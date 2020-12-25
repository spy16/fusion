package retry

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/spy16/fusion"
)

var _ fusion.Proc = (*Retrier)(nil)

// Retrier is a fusion Proc that can wrap other Proc implementation to provide
// automatic retries.
type Retrier struct {
	// Proc is the fusion Proc that must be executed for each message.
	// This field must be set.
	Proc fusion.Proc

	// Queue can be set to use a delay queue. If not set, an in-memory
	// queue will be used.
	Queue DelayQueue

	// Backoff can be set to configure a backoff strategy to be used
	// during retries. If not set, constant backoff strategy will be
	// used with 1s intervals.
	Backoff Backoff

	// MaxRetries can be set to control how many retries should be done
	// before returning Fail status. Defaults to 3.
	MaxRetries int

	// EnqueueWorkers is the number of worker threads to use for moving
	// messages from stream to the queue.
	EnqueueWorkers int

	// ProcWorkers is the number of main worker threads to use for running
	// proc.
	ProcWorkers int

	// OnFailure is called when a message fails and exhausts all retries.
	// If not set, such messages will be logged and discarded.
	OnFailure func(item Item)

	// Log can be set to customise logging mechanism used by retrier. If
	// not set, logging will be disabled.
	Log fusion.Log
}

// Run starts the proc workers and the retry worker threads and blocks until
// all the workers return.
func (ret *Retrier) Run(ctx context.Context, stream <-chan fusion.Msg) error {
	if err := ret.init(); err != nil {
		return err
	}

	wg := &sync.WaitGroup{}
	for i := 0; i < ret.EnqueueWorkers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			ret.enqueueWorker(ctx, stream)
		}(i)
	}

	// TODO: co-ordinate these 2 workers better.

	for i := 0; i < ret.ProcWorkers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			ret.procWorker(ctx)
		}(i)
	}
	wg.Wait()

	return nil
}

func (ret *Retrier) procWorker(ctx context.Context) {
	_ = ret.Queue.Dequeue(ctx, func(ctx context.Context, item Item) error {
		if item.Attempts >= ret.MaxRetries {
			ret.OnFailure(item)
		}

		// TODO: invoke proc.
		return nil
	})
}

func (ret *Retrier) enqueueWorker(ctx context.Context, stream <-chan fusion.Msg) {
	for {
		select {
		case <-ctx.Done():
			return

		case msg, open := <-stream:
			if !open {
				if closer, ok := ret.Queue.(io.Closer); ok {
					_ = closer.Close()
				}
				return
			}

			// move this item to the delay queue and acknowledge the
			// stream if it succeeded/failed.
			msg.Ack(ret.Queue.Enqueue(Item{
				Message:     msg,
				NextAttempt: time.Now(), // queue for immediate attempt.
			}))
		}
	}
}

func (ret *Retrier) init() error {
	if ret.Proc == nil {
		return errors.New("proc must be set")
	}

	if ret.Log == nil {
		ret.Log = func(_ map[string]interface{}) {}
	}

	if ret.EnqueueWorkers <= 0 {
		ret.EnqueueWorkers = 1
	}

	if ret.Queue == nil {
		ret.Queue = &InMemQ{}
	}

	if ret.Backoff == nil {
		ret.Backoff = ConstBackoff(1 * time.Second)
	}

	if ret.MaxRetries == 0 {
		ret.MaxRetries = 3
	}

	if ret.OnFailure == nil {
		ret.OnFailure = func(item Item) {
			ret.Log(map[string]interface{}{
				"level":   "warn",
				"item":    item,
				"message": "retry exhausted, discarding item.",
			})
		}
	}
	return nil
}
