// Package fatalwriter provides an io.Writer that terminates the process
// on write failure.
//
// When hotelier cannot write logs, it should stop processing tasks rather
// than silently dropping log output. This writer enforces that policy:
// any write error triggers a fatal message to stderr followed by os.Exit(1).
//
// Usage:
//
//	logger := log.New(fatalwriter.New(os.Stdout), "prefix ", log.LstdFlags)
package fatalwriter

import (
	"fmt"
	"io"
	"os"
)

// FatalMsgFormat is the format string used for fatal error messages.
// It is exported so that callers can use a consistent message format.
const FatalMsgFormat = "FATAL: log write failed: %v\n"

// Writer wraps an io.Writer and exits the process on any write failure.
type Writer struct {
	w        io.Writer
	exitFunc func(int)
}

// New creates a Writer that wraps the given writer.
// If a write to the underlying writer fails, the process exits with status 1.
// Optional configuration can be passed via Option values (e.g. WithExitFunc).
func New(w io.Writer, opts ...Option) *Writer {
	fw := &Writer{
		w:        w,
		exitFunc: os.Exit,
	}
	for _, opt := range opts {
		opt(fw)
	}
	return fw
}

// Option configures a Writer.
type Option func(*Writer)

// WithExitFunc sets a custom exit function. By default, os.Exit is used.
// This is primarily useful for testing.
func WithExitFunc(f func(int)) Option {
	return func(fw *Writer) {
		fw.exitFunc = f
	}
}

// Write delegates to the underlying writer. On failure, it prints the error
// to os.Stderr and calls os.Exit(1).
func (fw *Writer) Write(p []byte) (int, error) {
	n, err := fw.w.Write(p)
	if err != nil {
		fmt.Fprintf(os.Stderr, FatalMsgFormat, err)
		fw.exitFunc(1)
	}
	return n, nil
}
