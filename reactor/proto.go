// +build protobuf

package main

import (
	"errors"
	"fmt"

	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/desc/protoparse"
)

var ErrNotFound = errors.New("not found")

// Parse parses protobuf files from given files and returns the message descriptor.
// Returns parse error or ErrNotFound if the message is not found.
func Parse(msgType string, files, importDirs []string) (*desc.MessageDescriptor, error) {
	p := &protoparse.Parser{
		InferImportPaths: true,
		ImportPaths:      importDirs,
	}

	descriptors, err := p.ParseFiles(files...)
	if err != nil {
		return nil, fmt.Errorf("parse failed: %w", err)
	}

	for _, fd := range descriptors {
		msgDesc := fd.FindMessage(msgType)
		if msgDesc != nil {
			return msgDesc, nil
		}
	}

	return nil, fmt.Errorf("%w: '%s'", ErrNotFound, msgType)
}
