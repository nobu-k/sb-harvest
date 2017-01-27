package harvest

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"testing"
	"time"

	"gopkg.in/sensorbee/sensorbee.v0/core"
	"gopkg.in/sensorbee/sensorbee.v0/data"
)

func TestSink(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		// don't write this as defer l.Close() because l will be re-assigned.
		l.Close()
	}()

	ch := make(chan []byte, 1)
	discardConn := make(chan struct{})
	go listenThread(l, ch, discardConn)

	_, port, err := net.SplitHostPort(l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}

	ctx := core.NewContext(nil)
	s, err := newSink(ctx, &SinkOptions{
		Protocol:           "tcp",
		Address:            fmt.Sprintf("127.0.0.1:%v", port),
		BackoffInitialWait: time.Millisecond,
		BackoffMaxWait:     time.Millisecond,
		WriteTimeout:       time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close(ctx)

	t.Run("write one tuple", func(t *testing.T) {
		writeTest(t, ctx, s, ch)
	})

	t.Run("reconnection", func(t *testing.T) {
		l.Close()
		discardConn <- struct{}{}
		go func() {
			time.Sleep(10 * time.Millisecond)
			newL, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%v", port))
			if err != nil {
				t.Fatal(err)
			}
			l = newL
			go listenThread(l, ch, discardConn)
		}()

		// To avoid writing to the old loop.
		time.Sleep(5 * time.Millisecond)
		writeTest(t, ctx, s, ch)
	})
}

func listenThread(l net.Listener, ch chan<- []byte, discardConn <-chan struct{}) {
	for {
		a, err := l.Accept()
		if err != nil {
			break
		}

		func() { // doesn't have to be async
			defer a.Close()
			b := make([]byte, 1024)
			for {
				readCh := make(chan []byte, 1)
				go func() {
					n, err := a.Read(b)
					if err != nil {
						if err == io.EOF {
							return
						}
						log.Printf("cannot read data: %v\n", err)
						readCh <- nil
						return
					}
					a.Write([]byte("201\n"))
					readCh <- b[:n]
				}()

				select {
				case b := <-readCh:
					ch <- b
				case <-discardConn:
					a.Close()
					return
				}
			}
		}()
	}
}

func writeTest(t *testing.T, ctx *core.Context, s *sink, ch <-chan []byte) {
	err := s.Write(ctx, core.NewTuple(data.Map{"a": data.Int(1)}))
	if err != nil {
		t.Fatalf("cannot write a tuple: %v", err)
	}

	b := <-ch
	if b == nil {
		t.Fatal("cannot receive the result")
	}

	m := data.Map{}
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("cannot unmarshal json: %v", err)
	}

	if m.String() != (data.Map{"a": data.Int(1)}).String() {
		t.Fatal("wrong data has been sent")
	}
}
