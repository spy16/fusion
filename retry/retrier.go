package retry

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/spy16/fusion"
)

var _ fusion.Proc = (*Proc)(nil)

// Proc is a fusion Proc that can wrap other Proc implementation to provide
// automatic retries.
type Proc struct {
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

	// Workers is the number of main worker threads to use.
	Workers int
}

// Run starts the proc workers and the retry worker threads and blocks until
// all the workers return.
func (ret *Proc) Run(ctx context.Context, stream <-chan fusion.Msg) error {
	if err := ret.init(); err != nil {
		return err
	}

	wg := &sync.WaitGroup{}
	for i := 0; i < ret.Workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			ret.enqueue(ctx, stream)
		}(i)
	}
	wg.Wait()

	return nil
}

func (ret *Proc) queueWorker(ctx context.Context) {
	_ = ret.Queue.Dequeue(ctx, func(ctx context.Context, item Item) error {
		if item.Attempts >= ret.MaxRetries {
			// TODO: Push this message to a Dead Queue or failure storage.
			return nil
		}

		return nil
	})
}

func (ret *Proc) enqueue(ctx context.Context, stream <-chan fusion.Msg) {
	for {
		select {
		case <-ctx.Done():
			return

		case msg, open := <-stream:
			if !open {
				// TODO: signal the delay queue to close?
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

func (ret *Proc) init() error {
	if ret.Proc == nil {
		return errors.New("proc must be set")
	}

	if ret.Workers <= 0 {
		ret.Workers = 1
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
	return nil
}
