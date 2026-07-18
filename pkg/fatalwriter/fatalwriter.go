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

// Writer wraps an io.Writer and exits the process on any write failure.
type Writer struct {
	w io.Writer
}

// New creates a Writer that wraps the given writer.
// If a write to the underlying writer fails, the process exits with status 1.
func New(w io.Writer) *Writer {
	return &Writer{w: w}
}

// Write delegates to the underlying writer. On failure, it prints the error
// to os.Stderr and calls os.Exit(1).
func (fw *Writer) Write(p []byte) (int, error) {
	n, err := fw.w.Write(p)
	if err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: log write failed: %v\n", err)
		os.Exit(1)
	}
	return n, nil
}
