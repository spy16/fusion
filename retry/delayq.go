package retry

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/spy16/fusion"
)

var _ DelayQueue = (*InMemQ)(nil)

// Item is maintained by the delay queue and tracks the retries done etc.
type Item struct {
	Message     fusion.Msg `json:"message"`
	Attempts    int        `json:"attempts"`
	NextAttempt time.Time  `json:"next_attempt"`
	LastAttempt time.Time  `json:"last_attempt"`
}

// DelayQueue implementation maintains the messages in a timestamp based order.
// This is used by retrier for retries.
type DelayQueue interface {
	// Enqueue must save the message with priority based on the timestamp set.
	// If no timestamp is set, current timestamp should be assumed.
	Enqueue(item Item) error

	// Dequeue should read one message that has an expired timestamp and call
	// readFn with it. Success/failure from readFn must be considered as ACK
	// or nACK respectively. When message is not available, Dequeue should not
	// block but return ErrNoMessage. Queue can return EOF to indicate that the
	// queue is fully drained. Other errors from the queue will be logged and
	// ignored.
	Dequeue(ctx context.Context, readFn ReadFn) error
}

// ReadFn implementation is called by the message queue to handle a message.
type ReadFn func(ctx context.Context, item Item) error

// InMemQ implements an in-memory min-heap based message queue.
type InMemQ struct {
	mu    sync.Mutex
	items []Item
}

// Enqueue pushes the message into the in-mem heap with timestamp as its priority.
// If timestamp is not set, current timestamp will be assumed.
func (q *InMemQ) Enqueue(item Item) error {
	if item.NextAttempt.IsZero() {
		item.NextAttempt = time.Now()
	}
	q.push(item)
	return nil
}

// Dequeue reads a message from the in-mem heap if available and calls readFn with
// it. Otherwise returns ErrNoMessage.
func (q *InMemQ) Dequeue(ctx context.Context, readFn ReadFn) error {
	if q.size() == 0 {
		return io.EOF
	}

	item := q.pop()
	if item == nil {
		return errors.New("no message")
	}

	err := readFn(ctx, *item)
	if err != nil {
		q.push(*item) // failed to read. put it back.
	}
	return nil
}

func (q *InMemQ) push(item Item) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.items = append(q.items, item)
	q.heapifyUp(q.size() - 1)
}

func (q *InMemQ) pop() *Item {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.size() == 0 || !q.items[0].NextAttempt.Before(time.Now()) {
		return nil
	}

	m := q.items[0]
	q.swap(0, q.size()-1)
	q.items = q.items[:q.size()-1]
	q.heapifyDown(0)
	return &m
}

func (q *InMemQ) size() int { return len(q.items) }

func (q *InMemQ) heapifyUp(index int) {
	parentAt := q.parent(index)
	if index > 0 {
		child := q.items[index]
		parent := q.items[q.parent(index)]
		if child.NextAttempt.Before(parent.NextAttempt) {
			q.swap(index, parentAt)
		}
		q.heapifyUp(parentAt)
	}
}

func (q *InMemQ) heapifyDown(index int) {
	rightChildAt := q.rightChild(index)
	leftChildAt := q.leftChild(index)

	if index < q.size() && leftChildAt < q.size() && rightChildAt < q.size() {
		parent := q.items[index]
		if parent.NextAttempt.After(q.items[rightChildAt].NextAttempt) {
			q.swap(rightChildAt, index)
			q.heapifyDown(rightChildAt)
		} else if parent.NextAttempt.After(q.items[leftChildAt].NextAttempt) {
			q.swap(leftChildAt, index)
			q.heapifyDown(leftChildAt)
		}
	}
}

func (q *InMemQ) swap(i, j int) {
	tmp := q.items[i]
	q.items[i] = q.items[j]
	q.items[j] = tmp
}

func (*InMemQ) parent(index int) int {
	if index == 0 {
		return 0
	}
	return (index - 1) / 2
}

func (*InMemQ) leftChild(index int) int { return 2*index + 1 }

func (*InMemQ) rightChild(index int) int { return 2*index + 2 }
