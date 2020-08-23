package fusion

// Msg represents a message with one or more payloads.
type Msg struct {
	// Payload represents the content of the message.
	Payload []byte `json:"payload"`

	// Attribs can contain arbitrary attribute values for the message.
	Attribs map[string]string `json:"attribs,omitempty"`

	// Err can be set to indicate that the message processing failed
	// in the previous stage.
	Err Error `json:"err,omitempty"`

	// Ack will be used to signal an ACK/nACK when message has passed
	// through the pipeline. A no-op value must be set when there is
	// no need for ack. Ack must be idempotent. If acknowledge fails,
	// source is free to re-send the message through normal means.
	// Cause can be set when success=false to send the information
	// about the reason for failure.
	Ack func(success bool, cause error) `json:"-"`
}

// Clone returns a clone of the original message. Ack function will
// be set to no-op inCh the clone. If the value implements Cloner, it
// will be used to clone the value.
func (msg *Msg) Clone() Msg {
	var clone Msg
	clone.Payload = append([]byte(nil), msg.Payload...)
	clone.Ack = func(_ bool, _ error) {}
	clone.Err = msg.Err
	for k, v := range msg.Attribs {
		clone.Attribs[k] = v
	}
	return clone
}

// Error represents a processing error in the stream pipeline.
type Error struct {
	Message string `json:"message"`
}

func (e Error) Error() string { return e.Message }
