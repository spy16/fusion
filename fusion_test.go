package fusion

import (
	"context"
	"testing"
)

func TestNew(t *testing.T) {
	actor := New(Options{})
	if actor == nil {
		t.Errorf("actor should be non-nil")
	}
}

func Test_noOpProcessor(t *testing.T) {
	err := noOpProcessor(context.Background(), Message{})
	if err != Skip {
		t.Errorf("no-op processor must return Skip, got: %v", err)
	}
}
