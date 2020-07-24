package retrier

import "time"

// Options is used to specify configuration options for the retrier.
type Options struct {
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
}

func (opts *Options) setDefaults() {
	if opts.Queue == nil {
		opts.Queue = &InMemQ{}
	}

	if opts.Backoff == nil {
		opts.Backoff = ConstBackoff(1 * time.Second)
	}

	if opts.MaxRetries == 0 {
		opts.MaxRetries = 3
	}
}
