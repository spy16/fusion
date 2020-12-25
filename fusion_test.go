package fusion

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRunner_Run(t *testing.T) {
	t.Parallel()

	t.Run("NilStream", func(t *testing.T) {
		fu := Runner{}
		err := fu.Run(context.Background())
		assert.Error(t, err)
	})

	t.Run("StreamErr", func(t *testing.T) {
		fu := Runner{
			Stream: &fakeStream{
				OutFunc: func(ctx context.Context) (<-chan Msg, error) {
					return nil, errors.New("failed")
				},
			},
		}
		err := fu.Run(context.Background())
		assert.Error(t, err)
	})

	t.Run("NilChanFromStream", func(t *testing.T) {
		fu := Runner{
			Stream: &fakeStream{
				OutFunc: func(ctx context.Context) (<-chan Msg, error) {
					return nil, nil
				},
			},
		}
		err := fu.Run(context.Background())
		assert.Error(t, err)
		assert.Equal(t, io.EOF, err)
	})

	t.Run("StreamErr", func(t *testing.T) {
		fu := Runner{
			Stream: StreamFn(func(ctx context.Context) (*Msg, error) {
				return nil, errors.New("stream error")
			}),
			Proc: ProcFn(func(ctx context.Context, stream <-chan Msg) error {
				return errors.New("failed")
			}),
			DrainTime: 1 * time.Second,
		}
		err := fu.Run(context.Background())
		assert.Error(t, err)
	})

	t.Run("Success", func(t *testing.T) {
		fu := Runner{
			Stream: StreamFn(func(ctx context.Context) (*Msg, error) {
				return nil, io.EOF
			}),
			Proc: ProcFn(func(ctx context.Context, stream <-chan Msg) error {
				return nil
			}),
			DrainTime: 1 * time.Second,
		}
		err := fu.Run(context.Background())
		assert.NoError(t, err)
	})
}

type fakeStream struct {
	OutFunc func(ctx context.Context) (<-chan Msg, error)
}

func (f fakeStream) Out(ctx context.Context) (<-chan Msg, error) { return f.OutFunc(ctx) }
