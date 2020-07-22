package fusion

import (
	"context"
	"errors"
	"sync"
	"time"
)

// New returns a new fusion stream processing pipeline  instance with given
// source and processor stages. If no stage is added, pipeline simply drains
// the source.
func New(source Source, opts Options) (*Fusion, error) {
	if source == nil {
		return nil, errors.New("source must not be nil")
	}

	opts.setDefaults()
	return &Fusion{
		source:  source,
		stages:  opts.Stages,
		logger:  opts.Logger,
		workers: opts.Workers,
		drainT:  opts.DrainWithin,
	}, nil
}

// Fusion represents a fusion streaming pipeline. A fusion instance has a
// stream source and one or more processing stages.
type Fusion struct {
	logger  Logger
	workers int
	source  Source
	stages  []Proc
	stream  <-chan Msg
	drainT  time.Duration
}

// Proc represents a processor stage in the stream pipeline. It can apply
// some logic to message received frm upstream stage and send the results
// downstream. If the returned message is nil or has no payload, fusion
// will assume end of the pipeline (i.e., a sink) and call the Ack() on
// the original message.
type Proc func(ctx context.Context, msg Msg) (*Msg, error)

// Run spawns all the worker goroutines and blocks until all of them exit.
// Worker threads exit when context is cancelled or when source closes. It
// returns any error that was returned from the source.
func (fu *Fusion) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	stream, err := fu.source.ConsumeFrom(ctx)
	if err != nil {
		return err
	}
	fu.stream = stream

	wg := &sync.WaitGroup{}
	for i := 0; i < fu.workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			if err := fu.worker(ctx); err != nil {
				fu.logger.Warnf("worker %d exited due to error: %v", id, err)
				return
			}
			fu.logger.Debugf("worker %d exited normally", id)
		}(i)
	}
	wg.Wait()

	fu.logger.Debugf("all workers returned")
	if se, ok := fu.source.(interface{ Err() error }); ok {
		return se.Err()
	}
	return nil
}

func (fu *Fusion) worker(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			fu.drainAll(fu.drainT)
			return nil

		case msg, open := <-fu.stream:
			if !open {
				return nil
			}
			if err := fu.process(ctx, msg); err != nil {
				fu.logger.Warnf("failed to process, will NACK: %v", err)
				msg.Ack(false, err)
			} else {
				fu.logger.Infof("processed successfully, will ACK")
				msg.Ack(true, nil)
			}
		}
	}
}

func (fu *Fusion) process(ctx context.Context, msg Msg) error {
	fu.logger.Debugf("message received: %+v", msg)
	var err error
	var res *Msg

	res = &msg
	for _, proc := range fu.stages {
		res, err = proc(ctx, msg)
		if err != nil {
			return err
		}
		if res == nil {
			// message was filtered out. stop here.
			return nil
		}
	}
	return nil
}

func (fu *Fusion) drainAll(timeout time.Duration) {
	for {
		select {
		case <-time.After(timeout):
			fu.logger.Warnf("could not drain the stream within timeout")
			return
		case msg, open := <-fu.stream:
			if !open {
				return
			}
			msg.Ack(false, nil)
		}
	}
}
