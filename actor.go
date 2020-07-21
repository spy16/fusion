package fusion

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"
)

// Actor represents an entity that consumes messages from the stream and acts on it.
// If processing message from the stream fails, it is queued in the configured delay
// queue if retries are enabled. If retries are not enabled, messages are passed to
// the configured OnFailure handler.
type Actor struct {
	Logger
	dq        DelayQueue
	proc      Processor
	opts      Options
	stream    Stream
	streamEOF bool
}

// Run spawns all the worker goroutines and blocks until all of them return. Workers
// will exit when context is cancelled or when stream and delay-queue both return EOF.
func (actor *Actor) Run(ctx context.Context) error {
	if actor.stream == nil {
		return errors.New("stream is nil, nothing to do")
	}

	actor.Debugf("spawning %d workers", actor.opts.Workers)
	wg := &sync.WaitGroup{}
	for i := 0; i < actor.opts.Workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			if err := actor.worker(ctx, id); err != nil {
				actor.Errorf("worker %d exited with error: %v", id, err)
				return
			}
			actor.Infof("worker %d finished", id)
		}(i)
	}
	wg.Wait()

	actor.closeAll()
	return nil
}

func (actor *Actor) worker(ctx context.Context, id int) error {
	for ctx.Err() == nil {
		err := actor.read(ctx, actor.readFn)
		if err != nil {
			if err == io.EOF {
				actor.Warnf("end of stream reached, worker %d exiting", id)
				return nil
			} else if err == ErrNoMessage {
				select {
				case <-ctx.Done():
					return nil

				case <-time.After(actor.opts.PollInterval):
					continue
				}
			}
			return err
		}
	}
	return nil
}

func (actor *Actor) read(ctx context.Context, fn ReadFn) (err error) {
	err = actor.dq.Dequeue(ctx, fn)
	if err == nil {
		return nil
	} else if err != ErrNoMessage && err != io.EOF {
		actor.Errorf("queue returned unknown error: %v", err)
		return nil
	}
	if actor.stream == nil || actor.streamEOF {
		if err == io.EOF {
			// stream is not available and queue is fully drained. we can
			// signal EOF to stop the worker.
			return io.EOF
		}
		return ErrNoMessage
	}
	err = actor.stream.Read(ctx, fn)
	if err == io.EOF {
		actor.streamEOF = true
		return nil
	}
	return err
}

func (actor *Actor) readFn(ctx context.Context, msg Message) (err error) {
	defer func() {
		if v := recover(); v != nil {
			actor.Errorf("recovered a panic: %v", v)
			if e, ok := v.(error); ok {
				err = e
			}
			err = fmt.Errorf("panic: %v", v)
		}
	}()

	err = actor.process(ctx, msg)
	if err != nil && !errors.Is(err, Skip) {
		if errors.Is(err, Failed) {
			return actor.opts.OnFailure(msg, err)
		}
		return err
	}

	return nil
}

func (actor *Actor) process(ctx context.Context, msg Message) error {
	msg.Attempts++
	actor.Debugf("processing (attempt=%d): key=\"%s\", val=\"%s\"", msg.Attempts, msg.Key, msg.Val)

	procErr := actor.proc(ctx, msg)
	if procErr != nil {
		if errors.Is(procErr, Skip) || errors.Is(procErr, Failed) {
			return procErr
		}

		// processing failed with unhandled error. queue for retry.
		if err := actor.queueForRetry(msg); err != nil {
			// failed to queue for retry. try to call the OnFailure handler.
			return actor.opts.OnFailure(msg, procErr)
		}
	}

	return nil
}

func (actor *Actor) queueForRetry(msg Message) error {
	retriesDone := msg.Attempts - 1
	if actor.opts.Backoff == nil {
		return errors.New("retries are disabled")
	} else if retriesDone >= actor.opts.MaxRetries {
		return errors.New("retries exhausted")
	}

	tryAfter := actor.opts.Backoff.RetryAfter(msg)
	msg.Time = msg.Time.Add(tryAfter)
	return actor.dq.Enqueue(msg)
}

func (actor *Actor) closeAll() {
	if closer, ok := actor.stream.(io.Closer); ok {
		_ = closer.Close()
	}

	if closer, ok := actor.dq.(io.Closer); ok {
		_ = closer.Close()
	}
}
