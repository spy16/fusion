package fusion

import (
	"context"
	"errors"
	"fmt"
	"io"
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

	// Log to be used by the Runner. If not set, a no-op value will be
	// used.
	Log Log
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
	} else if streamCh == nil {
		return io.EOF
	}

	if err := fu.Proc.Run(ctx, streamCh); err != nil {
		fu.Log(map[string]interface{}{
			"level":   "warn",
			"message": fmt.Sprintf("proc exited with error: %v", err),
		})
		fu.drainAll(streamCh)
		return err
	}

	return nil
}

func (fu *Runner) drainAll(ch <-chan Msg) {
	if fu.DrainTime == 0 {
		fu.Log(map[string]interface{}{
			"level":   "warn",
			"message": "drain time is not set, not draining stream",
		})
		return
	}

	fu.Log(map[string]interface{}{
		"level":   "info",
		"message": fmt.Sprintf("drain time is set, waiting for %s", fu.DrainTime),
	})
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
	if fu.Log == nil {
		fu.Log = func(_ map[string]interface{}) {}
	}

	if fu.Stream == nil {
		return errors.New("stream must not be nil")
	}

	if fu.Proc == nil {
		fu.Log(map[string]interface{}{
			"level":   "warn",
			"message": "proc is not set, using no-op",
		})
		fu.Proc = &Fn{}
	}
	return nil
}

// Log implementation provides structured logging facilities for fusion
// components.
type Log func(_ map[string]interface{})
