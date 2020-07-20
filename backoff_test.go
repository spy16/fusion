package fusion_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/spy16/fusion"
)

func TestExpBackoff(t *testing.T) {
	backoff := fusion.ExpBackoff(2, 100*time.Millisecond, 500*time.Millisecond)

	assert.Equal(t, time.Duration(0), backoff.RetryAfter(fusion.Message{Attempts: 0}))
	assert.Equal(t, 200*time.Millisecond, backoff.RetryAfter(fusion.Message{Attempts: 1}))
	assert.Equal(t, 400*time.Millisecond, backoff.RetryAfter(fusion.Message{Attempts: 2}))
	assert.Equal(t, 500*time.Millisecond, backoff.RetryAfter(fusion.Message{Attempts: 3}))
	assert.Equal(t, 500*time.Millisecond, backoff.RetryAfter(fusion.Message{Attempts: 4}))
	assert.Equal(t, 500*time.Millisecond, backoff.RetryAfter(fusion.Message{Attempts: 5}))
}

func TestConstBackoff(t *testing.T) {
	backoff := fusion.ConstBackoff(2 * time.Second)
	assert.Equal(t, 2*time.Second, backoff.RetryAfter(fusion.Message{Attempts: 0}))
	assert.Equal(t, 2*time.Second, backoff.RetryAfter(fusion.Message{Attempts: 1}))
	assert.Equal(t, 2*time.Second, backoff.RetryAfter(fusion.Message{Attempts: 2}))
	assert.Equal(t, 2*time.Second, backoff.RetryAfter(fusion.Message{Attempts: 3}))
}
