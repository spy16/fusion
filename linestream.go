package fusion

import (
	"bufio"
	"context"
	"io"
	"sync"
)

// LineStream implements a stream using io.Reader. This implementation scans
// the reader line-by-line and streams each line as a message. If offset is
// set, 'offset' number of lines are read and skipped. If Size is set, only
// 'size' number of lines are read after which the stream will return EOF.
type LineStream struct {
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
func (rd *LineStream) Read(ctx context.Context, readFn ReadFn) error {
	line, err := rd.readLine()
	if err != nil {
		return err
	}

	// TODO(spy16): change key to have line offset
	err = readFn(ctx, Message{
		Key: nil,
		Val: []byte(line),
	})
	if err != nil {
		rd.mu.Lock()
		defer rd.mu.Unlock()
		rd.lines = append(rd.lines, line)
	}

	return nil
}

// Close closes the underlying reader if supported.
func (rd *LineStream) Close() error {
	if closer, ok := rd.From.(io.ReadCloser); ok {
		return closer.Close()
	}
	return nil
}

func (rd *LineStream) readLine() (string, error) {
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

func (rd *LineStream) init() (err error) {
	rd.once.Do(func() {
		rd.sc = bufio.NewScanner(rd.From)

		for i := 0; i < rd.Offset; i++ {
			if !rd.sc.Scan() {
				err = rd.sc.Err()
				break
			}
		}
	})
	return nil
}
