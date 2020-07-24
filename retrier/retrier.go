package retrier

import (
	"context"
	"errors"

	"github.com/spy16/fusion"
)

var _ fusion.Processor = (*Retrier)(nil)

// New returns a new retrier wrapping the given processor.
func New(proc fusion.Processor, opts Options) (*Retrier, error) {
	if proc == nil {
		return nil, errors.New("proc must not be nil")
	}
	opts.setDefaults()

	return &Retrier{
		proc: proc,
		opts: opts,
	}, nil
}

// Retrier is a fusion Processor that can wrap other processor implementation
// to provide automatic retries.
type Retrier struct {
	proc fusion.Processor
	opts Options
}

// Process
func (ret *Retrier) Process(ctx context.Context, msg fusion.Msg) error {
	panic("implement me")
}
