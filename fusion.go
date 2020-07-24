package fusion

import (
	"context"
	"errors"
	"sync"
	"time"
)

// New returns a new fusion stream processing pipeline configured using the
// given options.
func New(source Source, opts ...Options) (*Fusion, error) {
	if source == nil {
		return nil, errors.New("source must not be nil")
	}

	opts = append(opts, Options{})
	opt := opts[0]

	opt.setDefaults()
	return &Fusion{
		source:    source,
		workers:   opt.Workers,
		drainT:    opt.DrainWithin,
		logger:    opt.Logger,
		onFinish:  opt.OnFinish,
		processor: opt.Processor,
	}, nil
}

// Fusion represents a fusion streaming pipeline. A fusion instance has a
// stream source and one or more processing stages.
type Fusion struct {
	logger    Logger
	source    Source
	stream    <-chan Msg
	workers   int
	drainT    time.Duration
	onFinish  func(Msg, error)
	processor Processor
}

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
			fu.drainAll()
			return nil

		case msg, open := <-fu.stream:
			if !open {
				return nil
			}
			fu.process(ctx, msg)
		}
	}
}

func (fu *Fusion) process(ctx context.Context, msg Msg) {
	fu.logger.Debugf("message received: %+v", msg)

	err := fu.processor.Process(ctx, msg)
	if err != nil {
		switch err {
		case Skip, Fail:
			fu.logger.Infof("proc returned Skip/Fail, will ACK")
			msg.Ack(true, err)
			fu.onFinish(msg, err)

		default:
			fu.logger.Warnf("proc returned unknown err, will NACK: %v", err)
			msg.Ack(false, err)
		}
		return
	}

	fu.logger.Infof("proc finished successfully, will ACK: %v", err)
	fu.onFinish(msg, nil)
	msg.Ack(true, nil)
}

func (fu *Fusion) drainAll() {
	if fu.drainT == 0 {
		fu.logger.Infof("no drain timeout set, not draining the channel")
		return
	}

	for {
		select {
		case <-time.After(fu.drainT):
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
