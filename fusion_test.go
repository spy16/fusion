package fusion_test

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/spy16/fusion"
)

func TestNew(t *testing.T) {
	t.Parallel()

	t.Run("NilSource", func(t *testing.T) {
		fu, err := fusion.New(nil, nil, fusion.Options{})
		assert.Error(t, err)
		assert.Nil(t, fu)
	})

	t.Run("Success", func(t *testing.T) {
		src := &fusion.LineStream{}
		noOp := []fusion.Proc{fusion.NoOpProcessor{}}
		fu, err := fusion.New(src, noOp, fusion.Options{})
		assert.NoError(t, err)
		assert.NotNil(t, fu)
	})
}

func TestFusion_Run(t *testing.T) {
	t.Parallel()

	t.Run("Success", func(t *testing.T) {
		counter := int64(0)
		proc := fusion.ProcFunc(func(ctx context.Context, msg fusion.Message) (*fusion.Message, error) {
			atomic.AddInt64(&counter, 1)
			return nil, nil
		})

		src := &fusion.LineStream{From: strings.NewReader("msg1\nmsg2\nmsg3")}
		fu, err := fusion.New(src, []fusion.Proc{proc}, fusion.Options{})
		require.NoError(t, err)
		require.NotNil(t, fu)

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		done := false
		go func() {
			assert.NoError(t, fu.Run(ctx))
			done = true
			cancel()
		}()
		<-ctx.Done()
		require.True(t, done)

		assert.Equal(t, int64(3), counter)
	})

	t.Run("ContextCancelled", func(t *testing.T) {
		proc := &fusion.NoOpProcessor{}

		src := fusion.SourceFunc(func(ctx context.Context) (*fusion.Message, error) {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return &fusion.Message{Ack: func(_ bool, _ error) {}}, nil
		})

		fu, err := fusion.New(src, []fusion.Proc{proc}, fusion.Options{})
		require.NoError(t, err)
		require.NotNil(t, fu)

		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()

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
