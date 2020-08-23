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
	// be useful when proc knows the message can never successfully be
	// processed.
	Fail = errors.New("fail message")
)

// Processor represents a processor inCh the stream pipeline. Processor can
// return nil or Skip or Fail to indicate a terminal state inCh which case
// the msg will be acknowledged. If processor returns any other error, it
// will be nAcked.
type Processor interface {
	Process(ctx context.Context, msg Msg) error
}

// Proc is a processor implementation using Go function values.
type Proc func(context.Context, Msg) error

// Process simply dispatches the call to the wrapped function value.
func (proc Proc) Process(ctx context.Context, msg Msg) error { return proc(ctx, msg) }

func noOpProcessor(_ context.Context, _ Msg) error { return Skip }
