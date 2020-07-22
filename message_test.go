package fusion_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	fusion2 "github.com/spy16/fusion"
)

func TestMessage_Clone(t *testing.T) {
	msg := fusion2.Message{
		Payloads: []fusion2.Payload{
			{
				Key: []byte("aello"),
				Val: []byte("world"),
			},
		},
	}
	clone := msg.Clone()
	assert.Equal(t, msg.Payloads, clone.Payloads)

	// verify deep clone.
	msg.Payloads[0].Key[0] = 'h'
	assert.Equal(t, msg.Payloads[0].Key, []byte("hello"))
	assert.Equal(t, clone.Payloads[0].Key, []byte("aello"))
}
