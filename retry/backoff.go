package retry

import (
	"math"
	"time"
)

// Backoff represents a backoff strategy to be used by the Retrier.
type Backoff interface {
	// RetryAfter should return the time duration which should be
	// elapsed before the next queueForRetry.
	RetryAfter(attempts int) time.Duration
}

// ExpBackoff provides a simple exponential Backoff strategy for retries.
func ExpBackoff(base float64, initialTimeout, maxTimeout time.Duration) Backoff {
	return backoffFunc(func(attempt int) time.Duration {
		if attempt == 0 {
			return time.Duration(0)
		}
		waitTime := time.Duration(
			float64(initialTimeout.Nanoseconds()) * math.Pow(base, float64(attempt)),
		)
		if waitTime > maxTimeout {
			return maxTimeout
		}
		return waitTime
	})
}

// ConstBackoff implements a constant interval Backoff strategy.
func ConstBackoff(interval time.Duration) Backoff {
	return backoffFunc(func(_ int) time.Duration {
		return interval
	})
}

// backoffFunc is an adaptor to allow using ordinary Go functions as Backoff
// strategy.
type backoffFunc func(retriesDone int) time.Duration

func (bf backoffFunc) RetryAfter(attempts int) time.Duration {
	return bf(attempts)
}
