package retry

import (
	"context"
	"io"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	fusion2 "github.com/spy16/fusion"
)

func TestInMemQ_Enqueue(t *testing.T) {
	q := &InMemQ{}
	err := q.Dequeue(context.Background(), nil)
	if err != io.EOF {
		t.Errorf("InMemQ.Dequeue() expected ErrNoMessage from empty queue, got '%v'", err)
	}

	at := time.Now()

	item1 := Item{Message: fusion2.Message{}, NextAttempt: at}
	item2 := Item{Message: fusion2.Message{}, NextAttempt: at.Add(1 * time.Hour)}

	noErr(t, q.Enqueue(item2))
	noErr(t, q.Enqueue(item1))

	err = q.Dequeue(context.Background(), func(ctx context.Context, item Item) error {
		if !reflect.DeepEqual(item, item1) {
			t.Errorf("InMemQ.Dequeue()\nwant=%+v\ngot=%+v", item1, item)
		}
		return nil
	})
	noErr(t, err)

	err = q.Dequeue(context.Background(), nil)
	assert.Error(t, err)
}

func noErr(t *testing.T, err error) {
	if err != nil {
		t.Fatalf("expected no error, got '%v'", err)
	}
}
