package fusion

import "context"

// Stream represents an immutable stream of messages.
type Stream interface {
	// Read should read the next message available in  the stream and
	// call readFn with it. Success/Failure of the readFn invocation
	// should be used as ACK/NACK respectively.
	Read(ctx context.Context, readFn ReadFn) error
}
