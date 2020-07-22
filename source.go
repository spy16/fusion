package fusion

import "context"

// Source implementation is the source of data in a pipeline.
type Source interface {
	// ConsumeFrom should return a channel to which it independently writes
	// the data stream to. It is the responsibility of this Source to close
	// the returned channel once the data is exhausted. goroutines spawned
	// by the source must be tied to the given context and exit when context
	// is cancelled.
	ConsumeFrom(ctx context.Context) (<-chan Message, error)
}

// SourceFunc implements a source using a Go function value.
type SourceFunc func(ctx context.Context) (*Message, error)

// ConsumeFrom launches a goroutine that continuously calls the wrapped
// function and writes the return message to the channel. Stops when ctx
// is cancelled or the function returns an error.
func (sf SourceFunc) ConsumeFrom(ctx context.Context) (<-chan Message, error) {
	stream := make(chan Message)
	go func() {
		defer close(stream)
		for {
			msg, err := sf(ctx)
			if err != nil {
				return
			}

			select {
			case <-ctx.Done():
				return
			case stream <- *msg:
			}
		}
	}()
	return stream, nil
}
