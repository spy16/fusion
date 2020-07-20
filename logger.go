package fusion

import (
	"fmt"
	"log"
)

// Logger implementations provide logging facilities for Actor.
type Logger interface {
	Debugf(msg string, args ...interface{})
	Infof(msg string, args ...interface{})
	Warnf(msg string, args ...interface{})
	Errorf(msg string, args ...interface{})
}

type goLogger struct{}

func (g goLogger) Debugf(msg string, args ...interface{}) {
	log.Printf("[DEBUG] %s", fmt.Sprintf(msg, args...))
}

func (g goLogger) Infof(msg string, args ...interface{}) {
	log.Printf("[INFO ] %s", fmt.Sprintf(msg, args...))
}

func (g goLogger) Warnf(msg string, args ...interface{}) {
	log.Printf("[WARN ] %s", fmt.Sprintf(msg, args...))
}

func (g goLogger) Errorf(msg string, args ...interface{}) {
	log.Printf("[ERROR] %s", fmt.Sprintf(msg, args...))
}
