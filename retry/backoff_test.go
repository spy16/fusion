package retry_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/spy16/fusion/retry"
)

func TestExpBackoff(t *testing.T) {
	backoff := retry.ExpBackoff(2, 100*time.Millisecond, 500*time.Millisecond)

	assert.Equal(t, time.Duration(0), backoff.RetryAfter(0))
	assert.Equal(t, 200*time.Millisecond, backoff.RetryAfter(1))
	assert.Equal(t, 400*time.Millisecond, backoff.RetryAfter(2))
	assert.Equal(t, 500*time.Millisecond, backoff.RetryAfter(3))
	assert.Equal(t, 500*time.Millisecond, backoff.RetryAfter(4))
	assert.Equal(t, 500*time.Millisecond, backoff.RetryAfter(5))
}

func TestConstBackoff(t *testing.T) {
	backoff := retry.ConstBackoff(2 * time.Second)
	assert.Equal(t, 2*time.Second, backoff.RetryAfter(0))
	assert.Equal(t, 2*time.Second, backoff.RetryAfter(1))
	assert.Equal(t, 2*time.Second, backoff.RetryAfter(2))
	assert.Equal(t, 2*time.Second, backoff.RetryAfter(3))
}
