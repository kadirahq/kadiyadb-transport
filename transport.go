package transport

import (
	"bufio"
	"bytes"
	"io"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/kadirahq/go-tools/hybrid"
)

const (
	// BufferSize ...
	BufferSize = 1024 * 1024
)

var (
	// WorkerCount ...
	WorkerCount = runtime.NumCPU()
)

// Message ...
type Message interface {
	MarshalTo([]byte) (int, error)
	proto.Unmarshaler
	proto.Sizer
}

// Conn ...
type Conn struct {
	conn    net.Conn
	buff    *bytes.Buffer
	buffAlt *bytes.Buffer
	reader  *bufio.Reader
	connMtx *sync.Mutex
	sendMtx *sync.Mutex
	recvMtx *sync.Mutex
	flshMtx *sync.Mutex
	sendSz  *hybrid.Uint32
	sendBuf []byte
	recvSz  *hybrid.Uint32
	recvBuf []byte
}

// Dial ...
func Dial(addr string) (c *Conn, err error) {
	nc, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	c = wrap(nc)
	return c, nil
}

// wrap ...
func wrap(nc net.Conn) (c *Conn) {
	c = &Conn{
		conn:    nc,
		buff:    bytes.NewBuffer(nil),
		buffAlt: bytes.NewBuffer(nil),
		reader:  bufio.NewReaderSize(nc, BufferSize),
		connMtx: &sync.Mutex{},
		sendMtx: &sync.Mutex{},
		recvMtx: &sync.Mutex{},
		flshMtx: &sync.Mutex{},
		sendSz:  hybrid.NewUint32(nil),
		sendBuf: make([]byte, 0, BufferSize),
		recvSz:  hybrid.NewUint32(nil),
		recvBuf: make([]byte, 0, BufferSize),
	}

	return c
}

// Send ...
func (c *Conn) Send(msg Message, now bool) (err error) {
	c.sendMtx.Lock()
	defer c.sendMtx.Unlock()

	var data []byte
	size := msg.Size()

	if size > len(c.sendBuf) {
		data = make([]byte, size)
	} else {
		data = c.sendBuf[:size]
	}

	n, err := msg.MarshalTo(data)
	if err != nil {
		return err
	} else if n != size {
		panic("MarshalTo failed")
	}

	*c.sendSz.Value = uint32(size)

	var dst io.Writer
	if now {
		dst = c.conn
		c.connMtx.Lock()
		defer c.connMtx.Unlock()
	} else {
		dst = c.buff
	}

	if err := c.write(dst, c.sendSz.Bytes); err != nil {
		return err
	}

	if err := c.write(dst, data); err != nil {
		return err
	}

	return nil
}

// Recv ...
func (c *Conn) Recv(msg Message) (err error) {
	c.recvMtx.Lock()
	defer c.recvMtx.Unlock()

	if *c.recvSz.Value == 0 {
		if err := c.read(c.reader, c.recvSz.Bytes); err != nil {
			return err
		}
	}

	var data []byte

	size := int(*c.recvSz.Value)
	if size > len(c.recvBuf) {
		data = make([]byte, size)
	} else {
		data = c.recvBuf[:size]
	}

	if err := c.read(c.reader, data); err != nil {
		return err
	}

	// reset size buffer
	*c.recvSz.Value = 0

	if err := msg.Unmarshal(data); err != nil {
		return err
	}

	return nil
}

// Flush ...
func (c *Conn) Flush() (err error) {
	c.flshMtx.Lock()
	defer c.flshMtx.Unlock()

	c.sendMtx.Lock()
	c.buff, c.buffAlt = c.buffAlt, c.buff
	c.sendMtx.Unlock()

	size := c.buffAlt.Len()

	c.connMtx.Lock()
	n, err := c.buffAlt.WriteTo(c.conn)
	c.connMtx.Unlock()

	if err != nil {
		return err
	} else if int(n) != size {
		panic("WriteTo failed")
	}

	if cap := c.buffAlt.Cap(); cap > BufferSize {
		c.buffAlt = bytes.NewBuffer(nil)
	} else {
		c.buffAlt.Reset()
	}

	return nil
}

// Close ...
func (c *Conn) Close() (err error) {
	c.sendMtx.Lock()
	defer c.sendMtx.Unlock()
	c.recvMtx.Lock()
	defer c.recvMtx.Unlock()
	c.flshMtx.Lock()
	defer c.flshMtx.Unlock()

	return c.conn.Close()
}

func (c *Conn) write(w io.Writer, d []byte) (err error) {
	for towrite := d[:]; len(towrite) > 0; {
		n, err := w.Write(towrite)
		if err != nil {
			return err
		}

		towrite = towrite[n:]
	}

	return nil
}

func (c *Conn) read(r io.Reader, d []byte) (err error) {
	for toread := d[:]; len(toread) > 0; {
		n, err := r.Read(toread)
		if err != nil {
			return err
		}

		toread = toread[n:]
	}

	return nil
}

// Handler ...
type Handler func(c *Conn) (err error)

// Listener ...
type Listener struct {
	handler    Handler
	clientsMap map[uint64]*Conn
	clientsMtx sync.Mutex
	clientsCtr uint64
}

// NewListener ...
func NewListener(handler Handler) (l *Listener) {
	l = &Listener{
		handler:    handler,
		clientsMap: map[uint64]*Conn{},
	}

	return l
}

// Listen ...
func (l *Listener) Listen(addr string) (err error) {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	// start all worker goroutines
	for i := 0; i < WorkerCount; i++ {
		go l.startWorker()
	}

	for {
		nc, err := listener.Accept()
		if err != nil {
			continue
		}

		go l.handleConnection(nc)
	}
}

// Flush ...
func (l *Listener) Flush() (err error) {
	// TODO now okay for a small number of client connections
	// better start goroutines and use a waitgroup later on
	for id, c := range l.clientsMap {
		if err := c.Flush(); err != nil {
			l.removeConnection(id)
			return err
		}
	}

	return nil
}

func (l *Listener) startWorker() {
	for {
		for len(l.clientsMap) == 0 {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		for id, c := range l.clientsMap {
			if err := l.handler(c); err != nil {
				l.removeConnection(id)
			}
		}
	}
}

func (l *Listener) handleConnection(nc net.Conn) {
	clientID := atomic.AddUint64(&l.clientsCtr, 1)
	l.clientsMtx.Lock()
	l.clientsMap[clientID] = wrap(nc)
	l.clientsMtx.Unlock()
}

func (l *Listener) removeConnection(id uint64) {
	l.clientsMtx.Lock()
	if c, ok := l.clientsMap[id]; ok {
		delete(l.clientsMap, id)
		c.Close()
	}
	l.clientsMtx.Unlock()
}
