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

	t.Run("NilSource", func(t *testing.T) {
		fu, err := fusion2.New(nil, nil, fusion2.Options{})
		assert.Error(t, err)
		assert.Nil(t, fu)
	})

	t.Run("NoProcessor", func(t *testing.T) {
		src := &fusion2.LineStream{}
		fu, err := fusion2.New(src, nil, fusion2.Options{})
		assert.Error(t, err)
		assert.Nil(t, fu)
	})

	t.Run("Success", func(t *testing.T) {
		src := &fusion2.LineStream{}
		noOp := []fusion2.Processor{fusion2.NoOpProcessor{}}
		fu, err := fusion2.New(src, noOp, fusion2.Options{})
		assert.NoError(t, err)
		assert.NotNil(t, fu)
	})
}

func TestFusion_Run(t *testing.T) {
	t.Parallel()

	t.Run("Success", func(t *testing.T) {
		counter := int64(0)
		proc := fusion2.ProcessorFunc(func(ctx context.Context, msg fusion2.Message) (*fusion2.Message, error) {
			atomic.AddInt64(&counter, 1)
			return nil, nil
		})

		src := &fusion2.LineStream{From: strings.NewReader("msg1\nmsg2\nmsg3")}
		fu, err := fusion2.New(src, []fusion2.Processor{proc}, fusion2.Options{})
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
		proc := &fusion2.NoOpProcessor{}

		src := fusion2.SourceFunc(func(ctx context.Context) (*fusion2.Message, error) {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return &fusion2.Message{Ack: func(_ bool, _ error) {}}, nil
		})

		fu, err := fusion2.New(src, []fusion2.Processor{proc}, fusion2.Options{})
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
