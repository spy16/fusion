package fusion

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"sync"
)

var (
	_ Source = (SourceFunc)(nil)
	_ Source = (*LineStream)(nil)
)

// Source implementation is the source of data in a pipeline.
type Source interface {
	// ConsumeFrom should return a channel to which it independently writes
	// the data stream to. It is the responsibility of this Source to close
	// the returned channel once the data is exhausted. goroutines spawned
	// by the source must be tied to the given context and exit when context
	// is cancelled.
	ConsumeFrom(ctx context.Context) (<-chan Msg, error)
}

// SourceFunc implements a source using a Go function value.
type SourceFunc func(ctx context.Context) (*Msg, error)

// ConsumeFrom launches a goroutine that continuously calls the wrapped
// function and writes the return message to the channel. Stops when ctx
// is cancelled or the function returns an error.
func (sf SourceFunc) ConsumeFrom(ctx context.Context) (<-chan Msg, error) {
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

// LineStream implements a source using io.Reader. This implementation scans
// the reader line-by-line and streams each line as a message. If offset is
// set, 'offset' number of lines are read and skipped. If Size is set, only
// 'size' number of lines are read after which the source will return EOF.
type LineStream struct {
	From   io.Reader // From is the reader to use.
	Offset int       // Offset to start at.
	Size   int       // Number of lines to stream.
	Buffer int       // Stream channel buffer size.

	// normal streaming states.
	curOffset int
	scanner   *bufio.Scanner
	messages  chan Msg
	err       error

	// buffer for maintaining messages that got nAcked.
	mu     sync.Mutex
	nAcked []*Msg
}

// ConsumeFrom sets up the source channel and sets up goroutines for writing
// to it.
func (rd *LineStream) ConsumeFrom(ctx context.Context) (<-chan Msg, error) {
	if rd.From == nil {
		return nil, errors.New("field From must be set")
	}
	rd.scanner = bufio.NewScanner(rd.From)
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
	}

	if rd.Size > 0 && rd.curOffset >= rd.Size+rd.Offset {
		return nil, io.EOF
	}

	msg, err := rd.readLine()
	if err != nil {
		return nil, err
	}

	msg.Ack = func(success bool, _ error) {
		if !success {
			rd.mu.Lock()
			defer rd.mu.Unlock()
			rd.nAcked = append(rd.nAcked, msg)
		}
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
	if !rd.scanner.Scan() {
		if rd.scanner.Err() == nil {
			return nil, io.EOF
		}
		return nil, rd.scanner.Err()
	}
	rd.curOffset++

	var key [8]byte
	binary.LittleEndian.PutUint64(key[:], uint64(rd.curOffset+rd.Offset-1))

	return &Msg{
		Key: key[:],
		Val: rd.scanner.Bytes(),
	}, nil
}
