package fusion

import (
	"context"
	"errors"
)

var (
	// Skip can be returned from proc implementations to signal that the
	// message has been skipped from processing but should still be acked.
	Skip = errors.New("skip message")

	// Fail can be returned from proc implementations to signal that the
	// message should be marked as failed but should be acked. This can
	// be useful when Proc knows the message can never successfully be
	// processed.
	Fail = errors.New("fail message")
)

// Proc represents a processor in the stream pipeline. Proc can return
// nil or Skip or Fail to indicate a terminal state in which case the
// msg will be acknowledged. If proc returns any other error, it will
// be nAcked.
type Proc func(ctx context.Context, msg Msg) error

func noOpProc(ctx context.Context, msg Msg) error {
	return Skip
}
