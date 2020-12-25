package fusion_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/spy16/fusion"
)

func TestFn_Run(t *testing.T) {
	t.Run("NoProc", func(t *testing.T) {
		fn := fusion.Fn{}

		acked := false
		msg := fusion.Msg{
			Key: []byte("key"),
			Val: []byte("val"),
			Ack: func(err error) {
				assert.Equal(t, err, fusion.Skip)
				acked = true
			},
		}
		ch := make(chan fusion.Msg, 1)
		ch <- msg
		close(ch)

		err := fn.Run(context.Background(), ch)
		assert.NoError(t, err)
		assert.True(t, acked)
	})

	t.Run("Fail", func(t *testing.T) {
		acked := false
		msg := fusion.Msg{
			Key: []byte("key"),
			Val: []byte("val"),
			Ack: func(err error) {
				assert.NoError(t, err)
				acked = true
			},
		}
		ch := make(chan fusion.Msg, 1)
		ch <- msg
		close(ch)

		fn := fusion.Fn{
			Workers: 1,
			Func: func(ctx context.Context, msg fusion.Msg) error {
				return nil
			},
		}

		err := fn.Run(context.Background(), ch)
		assert.NoError(t, err)
		assert.True(t, acked)
	})
}
