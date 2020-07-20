package fusion

import (
	"context"
	"errors"
)

// Skip can be returned by processor functions to indicate that the message
// should be ignored.
var Skip = errors.New("skip")

// Processor implementations define the logic to be executed by actor on receiving
// a message from the stream.
type Processor func(ctx context.Context, msg Message) error

func noOpProcessor(_ context.Context, _ Message) error { return Skip }
