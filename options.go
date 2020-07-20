package fusion

import (
	"context"
	"fmt"
	"log"
	"time"
)

// Options represents the configuration options for an actor instance.
type Options struct {
	// Stream is the primary source of messages for the actor. Worker
	// threads read from stream continuously and process the messages.
	// If stream is not set, actor will rely entirely on the queue and
	// manually enqueued messages using actor.Enqueue().
	Stream Stream

	// Queue is used for retries and for manually enqueuing messages
	// for the actor. If queue is not set, an in-memory queue will be
	// used.
	Queue DelayQueue

	// Processor function to use for processing the messages. If not
	// set, a no-op processor will be used that skips everything.
	Processor Processor

	// Workers is the number of worker goroutines to spawn when actor
	// starts. Defaults to 1.
	Workers int

	// MaxRetries is the number of retry attempts allowed. If not set,
	// retries are disabled.
	MaxRetries int

	// Backoff strategy to be used for retries. Messages that need to
	// be retried are re-queued to a future time based on the delay
	// returned by backoff. If not set, retries are disabled.
	Backoff Backoff

	// OnFailure if set, will be called when all retries are exhausted
	// and the processing has not succeeded. If not set, such messages
	// will simply be logged and ignored. err argument will have error
	// that occurred in processor in the last retry attempt.
	OnFailure func(msg Message, err error)

	// Logger can be overridden to use custom logger.
	Logger Logger

	// PollInterval to use when non-blocking queues/streams return ErrNoMessage.
	// Defaults to 300ms.
	PollInterval time.Duration
}

func (opts *Options) defaults() {
	if opts.Processor == nil {
		opts.Processor = noOpProcessor
	}

	if opts.Queue == nil {
		opts.Queue = &InMemQ{}
	}

	if opts.Workers <= 0 {
		opts.Workers = 1
	}

	if opts.Logger == nil {
		opts.Logger = goLogger{}
	}

	if opts.PollInterval <= 0 {
		opts.PollInterval = 300 * time.Millisecond
	}

	if opts.OnFailure == nil {
		opts.OnFailure = func(msg Message, err error) {
			opts.Logger.Errorf("failed to process: %+v", msg)
		}
	}
}

func noOpProcessor(_ context.Context, _ Message) error { return Skip }

type goLogger struct{}

func (g goLogger) Debugf(msg string, args ...interface{}) {
	log.Printf("[DEBUG] %s", fmt.Sprintf(msg, args...))
}

func (g goLogger) Infof(msg string, args ...interface{}) {
	log.Printf("[INFO ] %s", fmt.Sprintf(msg, args...))
}

func (g goLogger) Warnf(msg string, args ...interface{}) {
	log.Printf("[WARN ] %s", fmt.Sprintf(msg, args...))
}

func (g goLogger) Errorf(msg string, args ...interface{}) {
	log.Printf("[ERROR] %s", fmt.Sprintf(msg, args...))
}
