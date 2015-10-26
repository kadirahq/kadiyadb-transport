package transport

import (
	"reflect"
	"testing"
	"time"

	"github.com/kadirahq/kadiyadb-transport/test"
)

const (
	address = "localhost:1234"
)

func init() {
	l := NewListener(func(c *Conn) (err error) {
		msg := &test.Test{}

		if err := c.Recv(msg); err != nil {
			return err
		}

		if err := c.Send(msg); err != nil {
			return err
		}

		return nil
	})

	go l.Listen(address)

	go func() {
		for {
			time.Sleep(10 * time.Millisecond)
			l.Flush()
		}
	}()

	c, err := Dial(address)
	if err != nil {
		c, err = Dial(address)
	}

	c.Close()
}

func TestEcho(t *testing.T) {
	c, err := Dial(address)
	if err != nil {
		t.Fatal(err)
	}

	req := &test.Test{N: 1}
	res := &test.Test{}

	if err := c.Send(req); err != nil {
		t.Fatal(err)
	}
	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := c.Recv(res); err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(req, res) {
		t.Fatal("wrong echo")
	}
}

func BenchmarkEcho(b *testing.B) {
	c, err := Dial(address)
	if err != nil {
		b.Fatal(err)
	}

	req := &test.Test{N: 1}
	res := &test.Test{}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := c.Send(req); err != nil {
			b.Fatal(err)
		}
		if i%1000 == 0 {
			if err := c.Flush(); err != nil {
				b.Fatal(err)
			}
		}
	}

	if err := c.Flush(); err != nil {
		b.Fatal(err)
	}

	for i := 0; i < b.N; i++ {
		if err := c.Recv(res); err != nil {
			b.Fatal(err)
		}
	}
}
