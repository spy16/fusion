package fusion

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"sync"
)

// LineStream implements a source using io.Reader. This implementation scans
// the reader line-by-line and streams each line as a message. If offset is
// set, 'offset' number of lines are read and skipped. If Size is set, only
// 'size' number of lines are read after which the source will return EOF.
type LineStream struct {
	From   io.Reader // From is the reader to use.
	Offset int       // Offset to start at.
	Size   int       // Number of lines to stream.

	// normal streaming states.
	count int
	sc    *bufio.Scanner
	ch    chan Message
	err   error

	// buffer for maintaining messages that got nAcked.
	mu     sync.Mutex
	nAcked []*Message
}

// ConsumeFrom sets up the source channel and sets up goroutines for writing
// to it.
func (rd *LineStream) ConsumeFrom(ctx context.Context) (<-chan Message, error) {
	if rd.From == nil {
		return nil, errors.New("from must be set")
	}
	rd.sc = bufio.NewScanner(rd.From)
	rd.ch = make(chan Message)

	go rd.stream(ctx)

	return rd.ch, nil
}

// Err returns the error that caused the source to end.
func (rd *LineStream) Err() error {
	return rd.err
}

func (rd *LineStream) stream(ctx context.Context) {
	defer close(rd.ch)

	for {
		msg, err := rd.readOne()
		if err != nil {
			if err == io.EOF {
				return
			}
			rd.err = err
			break
		}

		if rd.count < rd.Offset {
			continue
		}

		select {
		case <-ctx.Done():
			return

		case rd.ch <- *msg:
		}
	}
}

func (rd *LineStream) readOne() (*Message, error) {
	if msg := rd.pop(); msg != nil {
		return msg, nil
	}

	if rd.Size > 0 && rd.count > rd.Size+rd.Offset {
		return nil, io.EOF
	}

	p, err := rd.readLine()
	if err != nil {
		return nil, err
	}

	msg := &Message{Payloads: []Payload{p}}
	msg.Ack = func(success bool, _ error) {
		if !success {
			rd.mu.Lock()
			defer rd.mu.Unlock()
			rd.nAcked = append(rd.nAcked, msg)
		}
	}
	return msg, nil
}

func (rd *LineStream) pop() *Message {
	rd.mu.Lock()
	defer rd.mu.Unlock()

	if len(rd.nAcked) == 0 {
		return nil
	}

	msg := rd.nAcked[0]
	rd.nAcked = rd.nAcked[1:]
	return msg
}

func (rd *LineStream) readLine() (Payload, error) {
	var payload Payload
	if !rd.sc.Scan() {
		if rd.sc.Err() == nil {
			return payload, io.EOF
		}
		return payload, rd.sc.Err()
	}
	rd.count++

	payload.Key = make([]byte, 8, 8)
	binary.LittleEndian.PutUint64(payload.Key[:], uint64(rd.count+rd.Offset-1))

	payload.Val = []byte(rd.sc.Text())
	return payload, nil
}
