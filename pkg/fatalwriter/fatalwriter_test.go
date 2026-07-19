package fatalwriter

import (
	"errors"
	"io"
	"testing"
)

// failingWriter is an io.Writer that fails on the Nth write.
type failingWriter struct {
	failAt int
	calls  int
}

func (f *failingWriter) Write(p []byte) (int, error) {
	f.calls++
	if f.calls >= f.failAt {
		return 0, errors.New("disk full")
	}
	return len(p), nil
}

func TestWriter_Success(t *testing.T) {
	var buf failingWriter
	buf.failAt = 10 // never fail

	fw := New(&buf)
	n, err := fw.Write([]byte("hello"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 5 {
		t.Errorf("expected 5 bytes written, got %d", n)
	}
	if buf.calls != 1 {
		t.Errorf("expected 1 call, got %d", buf.calls)
	}
}

func TestWriter_FailureExits(t *testing.T) {
	var exitedWith int
	var didExit bool
	exitFn := func(code int) {
		didExit = true
		exitedWith = code
	}

	var buf failingWriter
	buf.failAt = 1 // fail immediately

	fw := New(&buf, WithExitFunc(exitFn))
	_, _ = fw.Write([]byte("trigger failure"))

	if !didExit {
		t.Fatal("expected exit function to be called")
	}
	if exitedWith != 1 {
		t.Errorf("expected exit code 1, got %d", exitedWith)
	}
	if buf.calls != 1 {
		t.Errorf("expected 1 call to underlying writer, got %d", buf.calls)
	}
}

func TestWriter_MultipleWrites(t *testing.T) {
	var buf failingWriter
	buf.failAt = 5 // fail on 5th write

	fw := New(&buf)

	for i := 0; i < 3; i++ {
		n, err := fw.Write([]byte("x"))
		if err != nil {
			t.Fatalf("write %d: unexpected error: %v", i, err)
		}
		if n != 1 {
			t.Errorf("write %d: expected 1 byte, got %d", i, buf.calls)
		}
	}

	if buf.calls != 3 {
		t.Errorf("expected 3 calls, got %d", buf.calls)
	}
}

func TestWriter_New(t *testing.T) {
	discard := io.Discard
	fw := New(discard)
	if fw.w != discard {
		t.Error("expected underlying writer to be io.Discard")
	}
}
