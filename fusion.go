package fusion

import (
	"context"
	"errors"
	"time"
)

var (
	// ErrNoMessage can be returned from stream/queue implementations to indicate that
	// the stream/queue currently has no message. Actor might switch to polling mode in
	// this case.
	ErrNoMessage = errors.New("no message available")

	// Skip can be returned by processor functions to indicate that the message
	// should be ignored.
	Skip = errors.New("skip message")

	// Failed can be returned from processor functions to indicate that the message
	// must be failed immediately (i.e., skip retries even if available).
	Failed = errors.New("fail message")
)

// New returns a new actor with given configurations. If proc is nil, actor will
// consume from queue and skip everything. If queue is nil, in-memory queue will
// be used if retries are enabled.
func New(opts Options) *Actor {
	opts.defaults()
	return &Actor{
		stream: opts.Stream,
		dq:     opts.Queue,
		proc:   opts.Processor,
		opts:   opts,
		Logger: opts.Logger,
	}
}

// Processor implementations define the logic to be executed by actor on receiving
// a message from the stream.
type Processor func(ctx context.Context, msg Message) error

// Stream represents an immutable stream of messages.
type Stream interface {
	// Read should read the next message available in  the stream and
	// call readFn with it. Success/Failure of the readFn invocation
	// should be used as ACK/NACK respectively.
	Read(ctx context.Context, readFn ReadFn) error
}

// DelayQueue implementation maintains the messages in a timestamp based order.
// This is used by actor for retries and manual enqueuing messages.
type DelayQueue interface {
	// Enqueue must save the message with priority based on the timestamp set.
	// If no timestamp is set, current timestamp should be assumed.
	Enqueue(msg Message) error

	// Dequeue should read one message that has an expired timestamp and call
	// readFn with it. Success/failure from readFn must be considered as ACK
	// or nACK respectively. When message is not available, Dequeue should not
	// block but return ErrNoMessage. Queue can return EOF to indicate that the
	// queue is fully drained. Other errors from the queue will be logged and
	// ignored.
	Dequeue(ctx context.Context, readFn ReadFn) error
}

// ReadFn implementation is called by the message queue to handle a message.
type ReadFn func(ctx context.Context, msg Message) error

// Message represents a message from the stream. Contents of key and value are
// not validated by the framework itself, but may be validated by the processor
// functions.
type Message struct {
	Key []byte `json:"key" xml:"key"`
	Val []byte `json:"val" xml:"val"`

	// Time at which message arrived or should be processed when scheduled by
	// retrying logic. (Managed by the actor)
	Time time.Time `json:"time" xml:"time"`

	// Attempts is incremented by the actor every time an attempt is done to
	// process the message.
	Attempts int `json:"attempts" xml:"attempts"`
}

// Backoff represents a backoff strategy to be used by the actor.
type Backoff interface {
	// RetryAfter should return the time duration which should be
	// elapsed before the next queueForRetry.
	RetryAfter(msg Message) time.Duration
}

// Logger implementations provide logging facilities for Actor.
type Logger interface {
	Debugf(msg string, args ...interface{})
	Infof(msg string, args ...interface{})
	Warnf(msg string, args ...interface{})
	Errorf(msg string, args ...interface{})
}
