package fusion

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"
)

var _ DelayQueue = (*InMemQ)(nil)

// ErrNoMessage can be returned from stream/queue implementations to indicate that
// the stream/queue currently has no message. Actor might switch to polling mode in
// this case.
var ErrNoMessage = errors.New("no message available")

// DelayQueue implementation maintains the messages in a timestamp based order.
// This is used by actor for retries and manual enqueuing messages.
type DelayQueue interface {
	// Enqueue must save the message with priority based on the timestamp set.
	// If no timestamp is set, current timestamp should be assumed.
	Enqueue(msg Message) error

	// Dequeue should read one message that has an expired timestamp and call
	// readFn with it. Success/failure from readFn must be considered as ACK
	// or nACK respectively. When message is not available, Dequeue should not
	// block but return ErrNoMessage. Queue can return EOF to indicate that the
	// queue is fully drained. Other errors from the queue will be logged and
	// ignored.
	Dequeue(ctx context.Context, readFn ReadFn) error
}

// ReadFn implementation is called by the message queue to handle a message.
type ReadFn func(ctx context.Context, msg Message) error

// Message represents a message from the stream. Contents of key and value are
// not validated by the framework itself, but may be validated by the processor
// functions.
type Message struct {
	Key []byte `json:"key" xml:"key"`
	Val []byte `json:"val" xml:"val"`

	// Time at which message arrived or should be processed when scheduled by
	// retrying logic. (Managed by the actor)
	Time time.Time `json:"time" xml:"time"`

	// Attempts is incremented by the actor every time an attempt is done to
	// process the message.
	Attempts int `json:"attempts" xml:"attempts"`
}

// InMemQ implements an in-memory min-heap based message queue.
type InMemQ struct {
	heap messageHeap
}

// Dequeue reads a message from the in-mem heap if available and calls readFn with
// it. Otherwise returns ErrNoMessage.
func (q *InMemQ) Dequeue(ctx context.Context, readFn ReadFn) error {
	if q.heap.Size() == 0 {
		return io.EOF
	}

	m := q.heap.Pop()
	if m == nil {
		return ErrNoMessage
	}

	err := readFn(ctx, *m)
	if err != nil {
		q.heap.Push(*m) // failed to read. put it back.
	}
	return nil
}

// Enqueue pushes the message into the in-mem heap with timestamp as its priority.
// If timestamp is not set, current timestamp will be assumed.
func (q *InMemQ) Enqueue(msg Message) error {
	if msg.Time.IsZero() {
		msg.Time = time.Now()
	}
	q.heap.Push(msg)
	return nil
}

type messageHeap struct {
	mu    sync.Mutex
	items []Message
}

func (h *messageHeap) Push(msg Message) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.items = append(h.items, msg)
	h.heapifyUp(h.Size() - 1)
}

func (h *messageHeap) Pop() *Message {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.Size() == 0 || !h.items[0].Time.Before(time.Now()) {
		return nil
	}

	m := h.items[0]
	h.swap(0, h.Size()-1)
	h.items = h.items[:h.Size()-1]
	h.heapifyDown(0)
	return &m
}

func (h *messageHeap) Size() int { return len(h.items) }

func (h *messageHeap) heapifyUp(index int) {
	parentAt := h.parent(index)
	if index > 0 {
		child := h.items[index]
		parent := h.items[h.parent(index)]
		if child.Time.Before(parent.Time) {
			h.swap(index, parentAt)
		}
		h.heapifyUp(parentAt)
	}
}

func (h *messageHeap) heapifyDown(index int) {
	rightChildAt := h.rightChild(index)
	leftChildAt := h.leftChild(index)

	if index < h.Size() && leftChildAt < h.Size() && rightChildAt < h.Size() {
		parent := h.items[index]
		if parent.Time.After(h.items[rightChildAt].Time) {
			h.swap(rightChildAt, index)
			h.heapifyDown(rightChildAt)
		} else if parent.Time.After(h.items[leftChildAt].Time) {
			h.swap(leftChildAt, index)
			h.heapifyDown(leftChildAt)
		}
	}
}

func (h *messageHeap) swap(i, j int) {
	tmp := h.items[i]
	h.items[i] = h.items[j]
	h.items[j] = tmp
}

func (*messageHeap) parent(index int) int {
	if index == 0 {
		return 0
	}
	return (index - 1) / 2
}

func (*messageHeap) leftChild(index int) int { return 2*index + 1 }

func (*messageHeap) rightChild(index int) int { return 2*index + 2 }
