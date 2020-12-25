package fusion

import "errors"

var (
	// Skip can be passed as argument to the Ack method of Msg to signal
	// that the message should be skipped.
	Skip = errors.New("skip message")

	// Fail can be passed as argument to the Ack method of Msg to signal
	// that the message should be failed immediately without retries.
	Fail = errors.New("fail message")

	// Retry can be returned from a proc implementations when processing
	// a message failed and should be retried later sometime.
	Retry = errors.New("retry message")
)

// Msg represents a message with one or more payloads.
type Msg struct {
	Key     []byte            `json:"key"`
	Val     []byte            `json:"val"`
	Attribs map[string]string `json:"attribs"`

	// Ack will be used to signal an ACK/nACK when message has passed
	// through the pipeline. A no-op value must be set when there is
	// no need for ack. Ack must be idempotent. If message was handled
	// successfully, then Ack will be called without error.
	Ack func(err error)
}

// Clone returns a clone of the original message. Ack function will
// be set to no-op in the clone.
func (msg *Msg) Clone() Msg {
	var clone Msg
	clone.Key = append([]byte(nil), msg.Key...)
	clone.Val = append([]byte(nil), msg.Val...)
	clone.Ack = func(_ error) {}
	return clone
}
