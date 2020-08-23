package fusion

// Option represents a configuration option for the fusion initialisation.
type Option func(fu *Fusion) error

// WithOnFinish sets the OnFinish handler to be called when a lifetime of
// message ends in the pipeline. If nil, a no-op handler will be set.
func WithOnFinish(f func(msg Msg)) Option {
	return func(fu *Fusion) error {
		if f == nil {
			f = func(_ Msg) {}
		}
		fu.OnFinish = f
		return nil
	}
}

// WithLogger sets the logger to be used by the fusion instance. If nil,
// a no-op logger will be used.
func WithLogger(lg Logger) Option {
	return func(fu *Fusion) error {
		if lg == nil {
			lg = noOpLogger{}
		}
		fu.Logger = lg
		return nil
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

func (g noOpLogger) Debugf(_ string, _ ...interface{}) {}
func (g noOpLogger) Infof(_ string, _ ...interface{})  {}
func (g noOpLogger) Warnf(_ string, _ ...interface{})  {}
func (g noOpLogger) Errorf(_ string, _ ...interface{}) {}
