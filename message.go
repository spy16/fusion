package fusion

// Msg represents a message with one or more payloads.
type Msg struct {
	// Payloads can contain one or more message payloads.
	Payloads []Payload `json:"payloads"`

	// Ack will be used to signal an ACK/nACK when message has passed
	// through the pipeline. A no-op value must be set when there is
	// no need for ack. Ack must be idempotent. If acknowledge fails,
	// source is free to re-send the message through normal means.
	// Cause can be set when success=false to send the information
	// about the reason for failure.
	Ack func(success bool, cause error)
}

// Payload represents a key-value pair of arbitrary data.
type Payload struct {
	Key []byte `json:"key"`
	Val []byte `json:"val"`
}

// Clone returns a deep clone of the original message. Ack function will
// be set to no-op in the clone.
func (msg *Msg) Clone() Msg {
	var clone Msg
	for _, p := range msg.Payloads {
		clone.Payloads = append(clone.Payloads, p.Clone())
	}
	clone.Ack = func(_ bool, _ error) {}
	return clone
}

// Clone returns a depp clone of the payload.
func (p Payload) Clone() Payload {
	return Payload{
		Key: append([]byte(nil), p.Key...),
		Val: append([]byte(nil), p.Val...),
	}
}
