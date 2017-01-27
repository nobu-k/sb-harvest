package harvest

import (
	"context"
	"io"
	"net"
	"time"

	"gopkg.in/sensorbee/sensorbee.v0/bql"
	"gopkg.in/sensorbee/sensorbee.v0/core"
	"gopkg.in/sensorbee/sensorbee.v0/data"
)

type sink struct {
	dial            func() (net.Conn, error)
	gctx            context.Context
	cancelWriteLoop context.CancelFunc

	tuples       chan *core.Tuple
	initialWait  time.Duration
	maxWait      time.Duration
	writeTimeout time.Duration
}

func (s *sink) Write(ctx *core.Context, t *core.Tuple) error {
	// This sink blocks when the connect is lost until it is established.
	// Therefore tuples are dropped on connect failure until SensorBee supports
	// BUFFER SIZE/DROP configuration in INSERT INTO statement.
	s.tuples <- t
	return nil
}

func (s *sink) reconnectWithRetry(ctx *core.Context) (net.Conn, error) {
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

func (s *sink) writeLoop(ctx *core.Context) {
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

func (s *sink) Close(ctx *core.Context) error {
	s.cancelWriteLoop()
	return nil
}

// SinkOptions is the options of soracom_harvest sink.
type SinkOptions struct {
	// Protocol currently only supports "tcp". "udp" can be used but not tested.
	// The default value is "tcp".
	Protocol string

	// Address is address of soracom harvest in "host:port" format.
	// The default value is "harvest.soracom.io:8514".
	Address string

	// BackoffInitialWait is the duration that the sink waits for reconnection
	// after the first disconnect. The default value is "5s".
	BackoffInitialWait time.Duration

	// BackofMaxWait is the maximum duration of exponential backoff. The default
	// value is "1m".
	BackoffMaxWait time.Duration

	// WriteTimeout is timeout on sending data to the server. The default value
	// is "1m".
	WriteTimeout time.Duration
}

func newSink(ctx *core.Context, opt *SinkOptions) (*sink, error) {
	dial := func() (net.Conn, error) {
		return net.Dial(opt.Protocol, opt.Address)
	}
	gctx, cancel := context.WithCancel(context.Background())
	s := &sink{
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

// CreateSink creates a new soracom_harvest sink.
func CreateSink(ctx *core.Context, ioParams *bql.IOParams, params data.Map) (core.Sink, error) {
	opt := SinkOptions{
		Protocol:           "tcp",
		Address:            "harvest.soracom.io:8514",
		BackoffInitialWait: 5 * time.Second,
		BackoffMaxWait:     time.Minute,
		WriteTimeout:       time.Minute,
	}
	if err := data.Decode(params, &opt); err != nil {
		return nil, err
	}
	return newSink(ctx, &opt)
}
