package fusion

import (
	"context"
	"errors"
	"sync"
)

var _ Proc = (*Fn)(nil)

var (
	// Skip can be passed as argument to the Ack method of Msg to signal
	// that the message should be skipped.
	Skip = errors.New("skip message")

	// Fail can be passed as argument to the Ack method of Msg to signal
	// that the message should be failed immediately without retries.
	Fail = errors.New("fail message")

	// Retry can be returned from a proc implementations when processing
	// a message failed and should be retried later sometime.
	Retry = errors.New("retry message")
)

// Proc represents a processor in the stream pipeline.
type Proc interface {
	// Run should spawn the worker threads that consume from 'stream' and
	// process messages. Run should block until all workers exit. Proc is
	// responsible for acknowledging the message based on success/failure
	// in handling. Proc must stop all workers when ctx is cancelled or
	// 'stream' is closed.
	Run(ctx context.Context, stream <-chan Msg) error
}

// Fn implements a concurrent Proc using a custom processor function.
type Fn struct {
	Logger

	// Func is the function to invoke for each message. If not set,
	// uses a no-op func.
	Func func(ctx context.Context, msg Msg) error

	// Number of worker threads to launch for processing messages.
	// If not set, defaults to 1.
	Workers int
}

// Run spawns the configured number of worker threads.
func (fn *Fn) Run(ctx context.Context, stream <-chan Msg) error {
	fn.init()

	wg := &sync.WaitGroup{}
	for i := 0; i < fn.Workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			fn.work(ctx, stream)
		}(i)
	}
	wg.Wait()

	return nil
}

func (fn *Fn) work(ctx context.Context, stream <-chan Msg) {
	for {
		select {
		case <-ctx.Done():
			return

		case msg, open := <-stream:
			if !open {
				return
			}
			msg.Ack(fn.Func(ctx, msg))
		}
	}
}

func (fn *Fn) init() {
	if fn.Func == nil {
		fn.Func = func(_ context.Context, _ Msg) error {
			return Skip
		}
	}
	if fn.Logger == nil {
		fn.Logger = noOpLogger{}
	}
	if fn.Workers == 0 {
		fn.Workers = 1
	}
}
