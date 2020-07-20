package fusion

import (
	"context"
	"testing"
)

func Test_noOpProcessor(t *testing.T) {
	err := noOpProcessor(context.Background(), Message{})
	if err != Skip {
		t.Errorf("no-op processor must return Skip, got: %v", err)
	}
}
