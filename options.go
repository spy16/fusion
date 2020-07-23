package fusion

import "time"

// Options represents optional configuration values for the fusion instance.
type Options struct {
	// workers represents the number of workers to launch for reading
	// from the source and invoking stages.
	Workers int

	// Proc represents the processor to be used by the fusion pipeline to
	// process the incoming messages. If not set, a default no-op will be
	// used.
	Proc Proc

	// OnFinish when set, will be called when a proc successfully processes
	// a message or fails by returning Fail or skips by returning Skip.
	OnFinish func(msg Msg, err error)

	// Logger to use for the fusion instance. If not set, no-op logger
	// will be set.
	Logger Logger

	// drainT is the timeout to wait to properly drain the channel
	// and send NACKs for all messages when fusion instance is exiting
	// before stream is exhausted due to a context cancellation etc. If
	// not set, stream will not be drained.
	DrainWithin time.Duration
}

func (opts *Options) setDefaults() {
	if opts.Workers <= 0 {
		opts.Workers = 1
	}

	if opts.Logger == nil {
		opts.Logger = noOpLogger{}
	}

	if opts.Proc == nil {
		opts.Proc = noOpProc
	}

	if opts.OnFinish == nil {
		opts.OnFinish = func(_ Msg, _ error) {}
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
