package fusion

import "context"

// Stage represents a processing stage in the fusion pipeline. Concurrency
// should be managed by the stage implementations.
type Stage interface {
	// Spawn should spawn the workers that read from inCh and write to the
	// returned channel after processing. All stage workers should exit when
	// the context is cancelled or when the inCh is closed. Stage must also
	// close the returned channel when workers exit.
	Spawn(ctx context.Context, inCh <-chan Msg) (<-chan Msg, error)
}
