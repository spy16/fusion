package fusion

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
)

// New initialises an instance of Fusion pipeline with given source.
func New(src Source) *Fusion {
	return &Fusion{
		Logger:   noOpLogger{},
		OnFinish: func(_ Msg) {},
		source:   src,
		stages:   nil,
	}
}

// Source implementation is the source of data in fusion pipeline.
type Source interface {
	// Out should return a channel to which the source independently writes
	// the data stream to. It is the responsibility of this Source to close
	// the returned channel once the data is exhausted. goroutines spawned
	// by the source must be tied to the given context and exit when context
	// is cancelled.
	Out(ctx context.Context) (<-chan Msg, error)
}

// Stage represents a processing stage in the fusion pipeline. Concurrency
// should be managed by the stage implementations.
type Stage interface {
	// Run should spawn the workers that read from inCh and write to outCh
	// and block until all workers exit. All stage workers should exit when
	// the context is cancelled or when the inCh is closed. Stage must also
	// close the outCh while exiting.
	Run(ctx context.Context, inCh <-chan Msg, outCh chan<- Msg) error
}

// Fusion represents a fusion stream pipeline. A fusion pipeline has a source
// from which messages originate and has stages that map, filter and process
// those messages.
type Fusion struct {
	// Logger be overridden to use a custom logger.
	Logger

	// OnFinish can be set to receive messages that have passed through
	// the pipeline.
	OnFinish func(msg Msg)

	source Source
	stages []stageEntry
}

// Run sets up the plumbing between stages and starts all the stages. Run blocks
// until all stages exit.
func (fu *Fusion) Run(ctx context.Context) error {
	if fu.source == nil {
		return errors.New("source is nil, nothing to do")
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	stream, err := fu.source.Out(ctx)
	if err != nil {
		return err
	}

	var out chan Msg
	wg := &sync.WaitGroup{}
	for _, stg := range fu.stages {
		out = make(chan Msg)

		wg.Add(1)
		go func(stage stageEntry) {
			defer wg.Done()
			stg.run(ctx, stream, out)
		}(stg)

		stream = out
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for msg := range out {
			fu.OnFinish(msg)
		}
	}()

	wg.Wait()
	return nil
}

// Add adds a stage to the fusion instance. If 'workers' is negative/zero, it
// will be set to 1. If 'stage' is nil, this is a no-op.
func (fu *Fusion) Add(name string, workers int, stage Stage) {
	if stage == nil {
		return
	}
	if workers <= 0 {
		workers = 1
	}
	fu.stages = append(fu.stages, stageEntry{
		Logger:  fu.Logger,
		Stage:   stage,
		Name:    strings.TrimSpace(name),
		Workers: workers,
	})
}

type stageEntry struct {
	Logger
	Stage
	Name    string
	Workers int
}

func (se *stageEntry) run(ctx context.Context, inCh <-chan Msg, outCh chan<- Msg) {
	wg := &sync.WaitGroup{}
	for i := 0; i < se.Workers; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			if err := se.Run(ctx, inCh, outCh); err != nil {
				se.Errorf("stage thread '%s#%d' (type='%s') exited with error: %v",
					se.Name, idx, reflect.TypeOf(se.Stage), err)
			}
		}(i)
	}
	wg.Wait()
}
