package fusion_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	fusion2 "github.com/spy16/fusion"
)

func TestMessage_Clone(t *testing.T) {
	originalAck := false
	msg := fusion2.Msg{
		Key: []byte("aello"),
		Val: []byte("world"),
		Ack: func(err error) {
			originalAck = true
		},
	}
	clone := msg.Clone()

	assert.Equal(t, msg.Key, clone.Key)
	assert.Equal(t, msg.Val, clone.Val)

	// verify deep clone.
	msg.Key[0] = 'h'
	assert.Equal(t, msg.Key, []byte("hello"))
	assert.Equal(t, clone.Key, []byte("aello"))
	clone.Ack(nil) // ack should not affect originalAck
	assert.False(t, originalAck)
}
