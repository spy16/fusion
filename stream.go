package fusion

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"sync"
)

var _ Stream = (*LineStream)(nil)

// Stream implementation is the source of data in a pipeline.
type Stream interface {
	// Out should return a channel to which it independently writes the data
	// stream to. Stream is responsible for closing the returned channel once
	// the data is exhausted or when the stream worker exits. All goroutines
	// spawned by the stream must exit when the given context is cancelled.
	Out(ctx context.Context) (<-chan Msg, error)
}

// StreamFn implements a source using a Go function value.
type StreamFn func(ctx context.Context) (*Msg, error)

// Out launches a goroutine that continuously calls the wrapped function and
// writes the return message to the channel. Stops when ctx is cancelled or
// the function returns an error.
func (sf StreamFn) Out(ctx context.Context) (<-chan Msg, error) {
	stream := make(chan Msg)
	go func() {
		defer close(stream)

		for {
			msg, err := sf(ctx)
			if err != nil {
				return
			}

			select {
			case <-ctx.Done():
				return
			case stream <- *msg:
			}
		}
	}()
	return stream, nil
}

// LineStream implements a Stream using io.Reader. This implementation scans
// the reader line-by-line and streams each line as a message. If offset is
// set, 'offset' number of lines are read and skipped. If Size is set, only
// 'size' number of lines are read after which the source will return EOF.
type LineStream struct {
	From   io.Reader // From is the reader to use.
	Offset int       // Offset to start at.
	Size   int       // Number of lines (from offset) to stream.
	Buffer int       // Stream channel buffer size.

	// normal streaming states.
	curOffset  int
	eofReached bool
	reader     *bufio.Reader
	messages   chan Msg
	err        error

	// buffer for maintaining messages that got nAcked.
	mu     sync.Mutex
	nAcked []*Msg
}

// Out sets up the source channel and sets up goroutines for writing to it.
func (rd *LineStream) Out(ctx context.Context) (<-chan Msg, error) {
	if rd.From == nil {
		return nil, errors.New("field From must be set")
	}
	rd.reader = bufio.NewReader(rd.From)
	rd.messages = make(chan Msg, rd.Buffer)

	go rd.stream(ctx)
	return rd.messages, nil
}

// Err returns the error that caused the source to end.
func (rd *LineStream) Err() error { return rd.err }

func (rd *LineStream) stream(ctx context.Context) {
	defer close(rd.messages)

	for {
		msg, err := rd.readOne()
		if err != nil {
			if err == io.EOF {
				return
			}
			rd.err = err
			break
		}

		if rd.curOffset <= rd.Offset {
			continue
		}

		select {
		case <-ctx.Done():
			return

		case rd.messages <- *msg:
		}
	}
}

func (rd *LineStream) readOne() (*Msg, error) {
	if msg := rd.pop(); msg != nil {
		return msg, nil
	} else if rd.eofReached {
		return nil, io.EOF
	}

	if rd.Size > 0 && rd.curOffset >= rd.Size+rd.Offset {
		return nil, io.EOF
	}

	msg, err := rd.readLine()
	if err != nil {
		return nil, err
	}

	msg.Ack = func(err error) {
		if err == nil || err == Fail || err == Skip {
			return
		}

		rd.mu.Lock()
		defer rd.mu.Unlock()
		rd.nAcked = append(rd.nAcked, msg)
	}
	return msg, nil
}

func (rd *LineStream) pop() *Msg {
	rd.mu.Lock()
	defer rd.mu.Unlock()

	if len(rd.nAcked) == 0 {
		return nil
	}

	msg := rd.nAcked[0]
	rd.nAcked = rd.nAcked[1:]
	return msg
}

func (rd *LineStream) readLine() (*Msg, error) {
	line, err := rd.reader.ReadBytes('\n')
	if err != nil && err != io.EOF {
		return nil, err
	} else if err == io.EOF {
		if string(line) == "" {
			return nil, err
		}
		rd.eofReached = true
	}
	rd.curOffset++

	var key [8]byte
	binary.LittleEndian.PutUint64(key[:], uint64(rd.curOffset+rd.Offset-1))

	return &Msg{
		Key: key[:],
		Val: line,
	}, nil
}
