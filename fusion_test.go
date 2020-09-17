package fusion_test

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	fusion2 "github.com/spy16/fusion"
)

func TestNew(t *testing.T) {
	t.Parallel()

	t.Run("Nil Stream", func(t *testing.T) {
		fu := fusion2.Runner{
			Stream: nil,
		}
		err := fu.Run(context.Background())
		assert.Error(t, err)
	})

	t.Run("Success", func(t *testing.T) {
		fu := fusion2.Runner{
			Stream: &fusion2.LineStream{
				From: strings.NewReader("hello\n"),
			},
		}
		err := fu.Run(context.Background())
		assert.NoError(t, err)
	})
}

func TestFusion_Run(t *testing.T) {
	t.Parallel()

	t.Run("Success", func(t *testing.T) {
		counter := int64(0)
		lineStream := &fusion2.LineStream{From: strings.NewReader("msg1\nmsg2\nmsg3")}

		fu := fusion2.Runner{
			Stream: lineStream,
			Proc: &fusion2.Fn{
				Func: func(ctx context.Context, msg fusion2.Msg) error {
					atomic.AddInt64(&counter, 1)
					return nil
				},
			},
		}

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		require.NoError(t, fu.Run(ctx))

		done := false
		go func() {
			assert.NoError(t, fu.Run(ctx))
			done = true
			cancel()
		}()
		<-ctx.Done()
		require.True(t, done)

		assert.Equal(t, int64(3), counter)
		assert.NoError(t, lineStream.Err())
	})

	t.Run("ContextCancelled", func(t *testing.T) {
		src := fusion2.StreamFn(func(ctx context.Context) (*fusion2.Msg, error) {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return &fusion2.Msg{Ack: func(_ error) {}}, nil
		})

		fu := &fusion2.Runner{
			Stream: src,
		}

		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()

		require.NoError(t, fu.Run(ctx))

		done := false
		go func() {
			assert.NoError(t, fu.Run(ctx))
			done = true
			cancel()
		}()
		<-ctx.Done()
		time.Sleep(2 * time.Second)
		require.True(t, done)
	})
}
