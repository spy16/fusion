package fusion

import (
	"testing"
)

func TestNew(t *testing.T) {
	actor := New(Options{})
	if actor == nil {
		t.Errorf("actor should be non-nil")
	}
}
