package fusion

import (
	"context"
	"errors"
	"time"
)

// Runner represents a fusion streaming pipeline. A fusion instance has a
// stream and a proc for processing it. If the proc is not set, a no-op
// proc will be used.
type Runner struct {
	// Proc to use for processing the messages.
	Proc Proc

	// Stream to read messages from.
	Stream Stream

	// DrainTime is the timeout for draining the messages from the stream
	// channel when the Proc exits pre-maturely. If not set, channel will
	// not be drained.
	DrainTime time.Duration

	// Logger to be used by the Runner. If not set, a no-op logger will be
	// used.
	Logger Logger
}

// Run spawns all the worker goroutines and blocks until all of them exit.
// Worker threads exit when context is cancelled or when source closes. It
// returns any error that was returned from the source.
func (fu Runner) Run(ctx context.Context) error {
	if err := fu.init(); err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	streamCh, err := fu.Stream.Out(ctx)
	if err != nil {
		return err
	}

	if err := fu.Proc.Run(ctx, streamCh); err != nil {
		fu.drainAll(streamCh)
		return err
	}

	return nil
}

func (fu *Runner) drainAll(ch <-chan Msg) {
	if fu.DrainTime == 0 {
		return
	}

	for {
		select {
		case <-time.After(fu.DrainTime):
			return

		case msg, open := <-ch:
			if !open {
				return
			}
			msg.Ack(Retry)
		}
	}
}

func (fu *Runner) init() error {
	if fu.Stream == nil {
		return errors.New("stream must not be nil")
	}
	if fu.Proc == nil {
		fu.Proc = &Fn{}
	}
	if fu.Logger == nil {
		fu.Logger = noOpLogger{}
	}
	return nil
}

// Logger implementations provide logging facilities for Actor.
type Logger interface {
	Debugf(msg string, args ...interface{})
	Infof(msg string, args ...interface{})
	Warnf(msg string, args ...interface{})
	Errorf(msg string, args ...interface{})
}

type noOpLogger struct{}

func (g noOpLogger) Debugf(msg string, args ...interface{}) {}
func (g noOpLogger) Infof(msg string, args ...interface{})  {}
func (g noOpLogger) Warnf(msg string, args ...interface{})  {}
func (g noOpLogger) Errorf(msg string, args ...interface{}) {}
