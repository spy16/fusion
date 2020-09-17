package fusion_test

import (
	"context"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	fusion2 "github.com/spy16/fusion"
)

func TestStreamFn_Out(t *testing.T) {
	n := int64(2)
	sf := fusion2.StreamFn(func(ctx context.Context) (*fusion2.Msg, error) {
		if n <= 0 {
			return nil, io.EOF
		}
		atomic.AddInt64(&n, -1)
		return &fusion2.Msg{}, nil
	})
	messages, err := sf.Out(context.Background())
	require.NoError(t, err)

	count := countStream(messages)
	assert.Equal(t, 2, count)
}

func TestLineStream_Out(t *testing.T) {
	t.Parallel()

	t.Run("BeginningToEOF", func(t *testing.T) {
		ls := &fusion2.LineStream{From: strings.NewReader("msg1\nmsg2\nmsg3\n")}
		messages, err := ls.Out(context.Background())
		require.NoError(t, err)
		count := countStream(messages)
		assert.Equal(t, 3, count)
	})

	t.Run("FromOffset", func(t *testing.T) {
		ls := &fusion2.LineStream{
			From:   strings.NewReader("msg1\nmsg2\nmsg3\n"),
			Offset: 1,
		}
		messages, err := ls.Out(context.Background())
		require.NoError(t, err)
		count := countStream(messages)
		assert.Equal(t, 2, count)
	})

	t.Run("FromOffsetWithSize", func(t *testing.T) {
		ls := &fusion2.LineStream{
			From:   strings.NewReader("msg1\nmsg2\nmsg3\n"),
			Offset: 1,
			Size:   1,
		}
		messages, err := ls.Out(context.Background())
		require.NoError(t, err)
		count := countStream(messages)
		assert.Equal(t, 1, count)
	})
}

func countStream(s <-chan fusion2.Msg) int {
	upperBound := time.NewTimer(1 * time.Second)
	defer upperBound.Stop()

	count := 0
	for {
		select {
		case <-upperBound.C:
			return count

		case _, open := <-s:
			if !open {
				return count
			}
			count++
		}
	}
}
