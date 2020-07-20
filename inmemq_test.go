package fusion

import (
	"context"
	"io"
	"reflect"
	"testing"
	"time"
)

func TestInMemQ_Enqueue(t *testing.T) {
	q := &InMemQ{}
	err := q.Dequeue(context.Background(), nil)
	if err != io.EOF {
		t.Errorf("InMemQ.Dequeue() expected ErrNoMessage from empty queue, got '%v'", err)
	}

	msg1 := Message{Time: time.Now()}
	msg2 := Message{Time: msg1.Time.Add(1 * time.Hour)}

	noErr(t, q.Enqueue(msg2))
	noErr(t, q.Enqueue(msg1))

	err = q.Dequeue(context.Background(), func(ctx context.Context, msg Message) error {
		if !reflect.DeepEqual(msg, msg1) {
			t.Errorf("InMemQ.Dequeue()\nwant=%+v\ngot=%+v", msg1, msg)
		}
		return nil
	})
	noErr(t, err)

	err = q.Dequeue(context.Background(), nil)
	if err != ErrNoMessage {
		t.Errorf("InMemQ.Dequeue() expected ErrNoMessage from queue with "+
			"no expired messages, got '%v'", err)
	}
}

func noErr(t *testing.T, err error) {
	if err != nil {
		t.Fatalf("expected no error, got '%v'", err)
	}
}
