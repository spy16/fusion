package fusion

import (
	"context"
	"time"
)

// Options represents optional configuration values for the fusion instance.
type Options struct {
	Workers int

	// Stages represents the processing stages to be executed for each
	// message from the source. If not set, fusion instance simply drains
	// the source.
	Stages []Proc

	// Logger to use for the fusion instance. If not set, no-op logger
	// will be set.
	Logger Logger

	// DrainWithin is the timeout to wait to properly drain the channel
	// and send NACKs for all messages. If not set, stream will not be
	// drained.
	DrainWithin time.Duration
}

func (opts *Options) setDefaults() {
	if opts.Workers <= 0 {
		opts.Workers = 1
	}

	if opts.Logger == nil {
		opts.Logger = noOpLogger{}
	}
}

// Logger implementations provide logging facilities for Actor.
type Logger interface {
	Debugf(msg string, args ...interface{})
	Infof(msg string, args ...interface{})
	Warnf(msg string, args ...interface{})
	Errorf(msg string, args ...interface{})
}

type noOpLogger struct{}

func (g noOpLogger) Debugf(msg string, args ...interface{}) {}
func (g noOpLogger) Infof(msg string, args ...interface{})  {}
func (g noOpLogger) Warnf(msg string, args ...interface{})  {}
func (g noOpLogger) Errorf(msg string, args ...interface{}) {}

// NoOpProcessor consumes the messages and simply ignores them.
type NoOpProcessor struct{}

func (NoOpProcessor) Process(_ context.Context, _ Message) (*Message, error) { return nil, nil }
