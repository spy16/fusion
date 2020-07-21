package stream

import (
	"bufio"
	"context"
	"io"
	"sync"

	"github.com/spy16/fusion"
)

// Lines implements a stream using io.Reader. This implementation scans the
// reader line-by-line and streams each line as a message.
type Lines struct {
	From   io.Reader // From is the reader to use.
	Offset int       // Offset to start at.
	Size   int       // Number of lines to stream.

	count int
	mu    sync.Mutex
	lines []string
	sc    *bufio.Scanner
	once  sync.Once
}

// Read reads a line from the file and passes it to the readFn and advances.
func (rd *Lines) Read(ctx context.Context, readFn fusion.ReadFn) error {
	line, err := rd.readLine()
	if err != nil {
		return err
	}

	err = readFn(ctx, fusion.Message{
		Key: nil, // change this to line offset
		Val: []byte(line),
	})
	if err != nil {
		rd.mu.Lock()
		defer rd.mu.Unlock()
		rd.lines = append(rd.lines, line)
		return nil
	}

	return nil
}

// Close closes the underlying reader if supported.
func (rd *Lines) Close() error {
	if closer, ok := rd.From.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

func (rd *Lines) readLine() (string, error) {
	if err := rd.init(); err != nil {
		return "", err
	}
	rd.mu.Lock()
	defer rd.mu.Unlock()

	if rd.Size > 0 && rd.count >= rd.Size {
		return "", io.EOF
	}
	rd.count++

	if len(rd.lines) > 0 {
		line := rd.lines[0]
		rd.lines = rd.lines[1:]
		return line, nil
	}

	if !rd.sc.Scan() {
		if rd.sc.Err() == nil {
			return "", io.EOF
		}
		return "", rd.sc.Err()
	}
	return rd.sc.Text(), nil
}

func (rd *Lines) init() (err error) {
	rd.once.Do(func() {
		rd.sc = bufio.NewScanner(rd.From)

		for i := 0; i < rd.Offset-1; i++ {
			if !rd.sc.Scan() {
				err = rd.sc.Err()
				break
			}
		}
	})
	return nil
}
