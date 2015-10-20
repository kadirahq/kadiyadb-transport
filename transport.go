package transport

import (
	"bufio"
	"io"
	"net"
	"sync"

	"github.com/kadirahq/go-tools/hybrid"
)

const (
	defaultBufferSize = 8192
)

// Conn is a Transport connection
type Conn struct {
	writer *bufio.Writer
	reader *bufio.Reader
	closer io.Closer
}

// NewConn creates a new Transport connection
func NewConn(conn net.Conn) *Conn {
	return &Conn{
		writer: bufio.NewWriterSize(conn, defaultBufferSize),
		reader: bufio.NewReaderSize(conn, defaultBufferSize),
		closer: conn,
	}
}

// Dial creates a connection to given address
func Dial(addr string) (c *Conn, err error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	return NewConn(conn), nil
}

// Write writes to the connection
func (conn *Conn) Write(buffer []byte) error {
	toWrite := buffer[:]
	for len(toWrite) > 0 {
		n, err := conn.writer.Write(toWrite)
		if (err != nil) && (err != io.ErrShortWrite) {
			return err
		}

		toWrite = toWrite[n:]
	}

	return nil
}

// Read reads `n` number of bytes from the connection
func (conn *Conn) Read(n int) ([]byte, error) {
	buffer := make([]byte, n)

	toRead := buffer[:]
	for len(toRead) > 0 {
		read, err := conn.reader.Read(toRead)
		if err != nil {
			return nil, err
		}

		toRead = toRead[read:]
	}

	return buffer, nil
}

// Flush flushes the buffer
func (conn *Conn) Flush() error {
	err := conn.writer.Flush()
	for err == io.ErrShortWrite {
		err = conn.writer.Flush()
	}
	return err
}

// Close closes the connection
func (conn *Conn) Close() (err error) {
	conn.Flush()

	if err := conn.closer.Close(); err != nil {
		return err
	}

	return nil
}

// Server listens for new connections
type Server struct {
	lsnr net.Listener
}

// Serve creates a listener and accepts connections
func Serve(addr string) (s *Server, err error) {
	lsnr, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}

	return &Server{lsnr: lsnr}, nil
}

// Close stops accepting connections
func (s *Server) Close() (err error) {
	if err := s.lsnr.Close(); err != nil {
		return err
	}

	return nil
}

// Accept returns a channel of connections
func (s *Server) Accept() (c *Conn, err error) {
	conn, err := s.lsnr.Accept()
	if err != nil {
		return nil, err
	}

	return NewConn(conn), nil
}

// Transport is used to wrap and send Responses
type Transport struct {
	conn      *Conn
	writeLock *sync.Mutex
	readLock  *sync.Mutex
	buf       []byte
}

// New creates a new Transport for a connection
func New(conn *Conn) (t *Transport) {
	return &Transport{
		conn:      conn,
		writeLock: new(sync.Mutex),
		readLock:  new(sync.Mutex),
		buf:       make([]byte, 13),
	}
}

// SendBatch writes data to the connection
func (t *Transport) SendBatch(batch [][]byte, id uint32, msgType uint8) error {
	t.writeLock.Lock()
	defer t.writeLock.Unlock()

	sz := uint32(len(batch))
	hybrid.EncodeUint32(t.buf[:4], &id)
	hybrid.EncodeUint8(t.buf[4:5], &msgType)
	hybrid.EncodeUint32(t.buf[5:9], &sz)

	err := t.conn.Write(t.buf[:9])
	if err != nil {
		return err
	}

	for _, req := range batch {
		sz := uint32(len(req))
		hybrid.EncodeUint32(t.buf[:4], &sz)
		err = t.conn.Write(t.buf[:4])
		if err != nil {
			return err
		}

		err = t.conn.Write(req)
		if err != nil {
			return err
		}
	}

	return t.conn.Flush()
}

// ReceiveBatch reads data from the connection
func (t *Transport) ReceiveBatch() ([][]byte, uint32, uint8, error) {
	t.readLock.Lock()
	defer t.readLock.Unlock()

	var resBatch [][]byte
	var id uint32
	var msgType uint8

	bytes, err := t.conn.Read(9) // Read the header
	if err != nil {
		return resBatch, id, msgType, err
	}

	var uiSize uint32
	hybrid.DecodeUint32(bytes[:4], &id)
	hybrid.DecodeUint8(bytes[4:5], &msgType)
	hybrid.DecodeUint32(bytes[5:9], &uiSize)

	size := int(uiSize)
	resBatch = make([][]byte, size)

	var uiMsgSize uint32
	for i := 0; i < size; i++ {
		bytes, err := t.conn.Read(4)
		if err != nil {
			return resBatch, id, msgType, err
		}
		hybrid.DecodeUint32(bytes, &uiMsgSize)

		resBatch[i], err = t.conn.Read(int(uiMsgSize))
		if err != nil {
			return resBatch, id, msgType, err
		}
	}

	return resBatch, id, msgType, err
}
