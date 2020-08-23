package stage

import (
	"context"
	"errors"
	"sync"

	"github.com/spy16/fusion"
)

// Map implements a stage that maps incoming messages using a mapper function
// and writes to the output.
type Map struct {
	// Mapper is called for every message received by the stage and the return
	// value will be passed to the next stage.
	Mapper func(ctx context.Context, msg fusion.Msg) (*fusion.Msg, error)

	// Workers can be set to configure the number of mapper threads. Defaults
	// to 1 thread/goroutine.
	Workers int

	inCh   <-chan fusion.Msg
	mapped chan fusion.Msg
}

func (m *Map) Spawn(ctx context.Context, inCh <-chan fusion.Msg) (<-chan fusion.Msg, error) {
	m.inCh = inCh
	if err := m.init(); err != nil {
		return nil, err
	}
	go m.runWorkersAndWait(ctx)
	return m.mapped, nil
}

func (m *Map) runWorkersAndWait(ctx context.Context) {
	defer close(m.mapped)

	wg := &sync.WaitGroup{}
	for i := 0; i < m.Workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			m.worker(ctx, id)
		}(i)
	}
	wg.Wait()
}

func (m *Map) worker(ctx context.Context, id int) {
	for {
		select {
		case <-ctx.Done():
			return

		case msg, open := <-m.inCh:
			if !open {
				return
			}
			res, err := m.Mapper(ctx, msg)
			if err != nil {

			}
		}
	}
}

func (m *Map) init() error {
	if m.Mapper == nil {
		return errors.New("field Mapper must be set")
	}

	if m.Workers <= 0 {
		m.Workers = 1
	}

	m.mapped = make(chan fusion.Msg)
	return nil
}
