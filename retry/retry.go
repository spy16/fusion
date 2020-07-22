package retry

import (
	"context"
	"time"

	"github.com/spy16/fusion"
)

// Retrier implements a fusion processor stage that provides automatic
// retries.
type Retrier struct {
	queue DelayQueue
}

// Process
func (r *Retrier) Process(ctx context.Context, msg fusion.Message) (*fusion.Message, error) {
	panic("implement me")
}

// Item is maintained by the delay queue and tracks the retries done etc.
type Item struct {
	Message     fusion.Message `json:"message"`
	Attempts    int            `json:"attempts"`
	NextAttempt time.Time      `json:"next_attempt"`
	LastAttempt time.Time      `json:"last_attempt"`
}
