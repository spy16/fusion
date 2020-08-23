package fusion_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/spy16/fusion"
)

func TestMessage_Clone(t *testing.T) {
	msg := fusion.Msg{
		Key:     []byte("aello"),
		Payload: []byte("world"),
	}
	clone := msg.Clone()

	assert.Equal(t, msg.Key, clone.Key)
	assert.Equal(t, msg.Payload, clone.Payload)

	// verify deep clone.
	msg.Key[0] = 'h'
	assert.Equal(t, msg.Key, []byte("hello"))
	assert.Equal(t, clone.Key, []byte("aello"))
}
