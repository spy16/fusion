package fusion

import (
	"context"
	"fmt"
	"sync"
)

var _ Proc = (*Fn)(nil)

// Proc represents a processor in the stream pipeline.
type Proc interface {
	// Run should spawn the worker threads that consume from 'stream' and
	// process messages. Run should block until all workers exit. Proc is
	// responsible for acknowledging the message based on success/failure
	// in handling. Proc must stop all workers when ctx is cancelled or
	// 'stream' is closed.
	Run(ctx context.Context, stream <-chan Msg) error
}

// ProcFn implements Proc using a simple Go function.
type ProcFn func(ctx context.Context, stream <-chan Msg) error

// Run dispatches the msg to the wrapped function.
func (pf ProcFn) Run(ctx context.Context, stream <-chan Msg) error { return pf(ctx, stream) }

// Fn implements a concurrent Proc using a custom processor function.
type Fn struct {
	// Number of worker threads to launch for processing messages.
	// If not set, defaults to 1.
	Workers int

	// Func is the function to invoke for each message. If not set,
	// uses a no-op func.
	Func func(ctx context.Context, msg Msg) error
}

// Run spawns the configured number of worker threads.
func (fn *Fn) Run(ctx context.Context, stream <-chan Msg) error {
	log := LogFrom(ctx)
	fn.init()

	wg := &sync.WaitGroup{}
	for i := 0; i < fn.Workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			for msg := range stream {
				err := fn.Func(ctx, msg)
				msg.Ack(err)
			}
			log(map[string]interface{}{
				"level":   "info",
				"message": fmt.Sprintf("stream closed, worker %d exiting", id),
			})
		}(i)
	}
	wg.Wait()

	log(map[string]interface{}{
		"level":   "info",
		"message": "all workers exited",
	})
	return nil
}

func (fn *Fn) init() {
	if fn.Func == nil {
		fn.Func = func(_ context.Context, _ Msg) error {
			return Skip
		}
	}
	if fn.Workers == 0 {
		fn.Workers = 1
	}
}
