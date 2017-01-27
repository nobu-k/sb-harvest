package harvest

import (
	"context"
	"io"
	"net"
	"time"

	"gopkg.in/sensorbee/sensorbee.v0/core"
)

type Sink struct {
	dial            func() (net.Conn, error)
	gctx            context.Context
	cancelWriteLoop context.CancelFunc

	tuples       chan *core.Tuple
	initialWait  time.Duration
	maxWait      time.Duration
	writeTimeout time.Duration
}

func (s *Sink) Write(ctx *core.Context, t *core.Tuple) error {
	// This sink blocks when the connect is lost until it is established.
	// Therefore tuples are dropped on connect failure until SensorBee supports
	// BUFFER SIZE/DROP configuration in INSERT INTO statement.
	s.tuples <- t
	return nil
}

func (s *Sink) reconnectWithRetry(ctx *core.Context) (net.Conn, error) {
	w := s.initialWait
	for {
		c, err := s.dial()
		if err == nil {
			return c, nil
		}

		ctx.ErrLog(err).Error("cannot connect to the host. retrying...")
		select {
		case <-time.After(w):
		case <-s.gctx.Done():
			return nil, err
		}

		w *= 2
		if w > s.maxWait {
			w = s.maxWait
		}
	}
}

func (s *Sink) writeLoop(ctx *core.Context) {
	c, err := s.reconnectWithRetry(ctx)
	if err != nil {
		ctx.ErrLog(err).Error("connection failed")
		return
	}
	defer func() {
		if c != nil {
			c.Close()
		}
	}()

	ack := make([]byte, 1024)
	for {
		var t *core.Tuple
		select {
		case <-s.gctx.Done():
			return
		case t = <-s.tuples:
		}

		js := t.Data.String()
		for {
			c.SetWriteDeadline(time.Now().Add(s.writeTimeout))
			_, err := io.WriteString(c, js)
			if err == nil {
				n, err := c.Read(ack)
				if err != nil {
					ctx.ErrLog(err).Warn("cannot receive the ack from harvest.soracom.io")
					continue
				}
				if !((n == 3 || n == 4) && string(ack[:3]) == "201") {
					ctx.Log().WithField("ack", string(ack[:n])).
						Warn("unknown ack message from harvest.soracom.io. the data might have been lost.")
				}
				break
			}
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}

			c.SetReadDeadline(time.Now())
			if _, re := c.Read([]byte{}); re != io.EOF {
				ctx.ErrLog(err).Warn("the write error might not be due to disconnect but reconnect anyway")
			}

			// connection lost
			c.Close()
			c, err = s.reconnectWithRetry(ctx)
			if err != nil {
				ctx.ErrLog(err).Error("connection failed")
				return
			}
		}
	}
}

func (s *Sink) Close(ctx *core.Context) error {
	s.cancelWriteLoop()
	return nil
}

type SinkOption struct {
	Protocol string
	Address  string

	BackoffInitialWait time.Duration
	BackoffMaxWait     time.Duration
	WriteTimeout       time.Duration
}

func newSink(ctx *core.Context, opt *SinkOption) (*Sink, error) {
	dial := func() (net.Conn, error) {
		return net.Dial(opt.Protocol, opt.Address)
	}
	gctx, cancel := context.WithCancel(context.Background())
	s := &Sink{
		dial:            dial,
		gctx:            gctx,
		cancelWriteLoop: cancel,
		tuples:          make(chan *core.Tuple),
		initialWait:     opt.BackoffInitialWait,
		maxWait:         opt.BackoffMaxWait,
		writeTimeout:    opt.WriteTimeout,
	}

	go s.writeLoop(ctx)
	return s, nil
}
