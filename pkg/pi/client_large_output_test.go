package pi

import (
	"fmt"
	"io"
	"strings"
	"testing"
	"time"
)

// These are regression tests for issue #92: pi emits newline-delimited JSON
// on stdout, and a single line (e.g. a tool_execution_end carrying a large
// tool result) can exceed the previous bufio.Scanner token limits (1 MiB for
// stdout, 64 KiB for stderr). The scanner then failed with
// "bufio.Scanner: token too long", the event stream was closed, and the guest
// handler marked the task as an abnormal exit. The reader must deliver lines
// of any length.

const largeLineSize = 2 * 1024 * 1024 // 2 MiB — above both old token limits

// waitForEventChClosed waits until eventCh is closed, failing the test after
// the timeout.
func waitForEventChClosed(t *testing.T, ch chan Event, timeout time.Duration) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		for range ch {
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(timeout):
		t.Fatal("event channel was not closed within timeout")
	}
}

// TestPiClient_readEvents_DeliversLargeLine verifies that a stdout line far
// larger than the old 1 MiB scanner limit is delivered intact as an event.
func TestPiClient_readEvents_DeliversLargeLine(t *testing.T) {
	var logBuf strings.Builder
	c := NewClient(PiClientConfig{CWD: "/tmp", Log: newTestLogger(&logBuf)})

	pr, pw := io.Pipe()
	c.stdout = pr

	go c.readEvents()

	payload := strings.Repeat("a", largeLineSize)
	line := fmt.Sprintf(`{"type":"tool_execution_end","toolName":"read","result":"%s"}`, payload)
	go func() {
		_, _ = pw.Write([]byte(line + "\n"))
		_ = pw.Close()
	}()

	select {
	case event, ok := <-c.eventCh:
		if !ok {
			t.Fatalf("event channel closed without delivering the large event; log: %s", logBuf.String())
		}
		if event.Type != "tool_execution_end" {
			t.Errorf("event type = %q, want %q", event.Type, "tool_execution_end")
		}
		if len(event.Result) < largeLineSize {
			t.Errorf("result length = %d, want >= %d (line was truncated)", len(event.Result), largeLineSize)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("large event was not delivered; log: %s", logBuf.String())
	}

	waitForEventChClosed(t, c.eventCh, 5*time.Second)

	for _, marker := range []string{"scan error", "read error"} {
		if strings.Contains(logBuf.String(), marker) {
			t.Errorf("log contains %q: %s", marker, logBuf.String())
		}
	}
}

// TestPiClient_readStderr_CapturesLargeLine verifies that a stderr line far
// larger than the old 64 KiB default scanner limit is captured in full.
func TestPiClient_readStderr_CapturesLargeLine(t *testing.T) {
	var logBuf strings.Builder
	c := NewClient(PiClientConfig{CWD: "/tmp", Log: newTestLogger(&logBuf)})

	pr, pw := io.Pipe()
	c.stderr = pr

	go c.readStderr()

	line := strings.Repeat("x", largeLineSize)
	go func() {
		_, _ = pw.Write([]byte(line + "\n"))
		_ = pw.Close()
	}()

	deadline := time.Now().Add(10 * time.Second)
	for {
		lines := c.GetStderrLines()
		if len(lines) == 1 && lines[0] == line {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("large stderr line was not captured (got %d lines, first length %d); log: %s",
				len(lines), firstLineLen(lines), logBuf.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func firstLineLen(lines []string) int {
	if len(lines) == 0 {
		return 0
	}
	return len(lines[0])
}

// TestPiClient_readEvents_FinalLineWithoutNewline verifies that a final line
// without a trailing newline (e.g. if pi is killed mid-write) is still
// delivered. This guards the io.EOF handling of the line reader.
func TestPiClient_readEvents_FinalLineWithoutNewline(t *testing.T) {
	var logBuf strings.Builder
	c := NewClient(PiClientConfig{CWD: "/tmp", Log: newTestLogger(&logBuf)})

	pr, pw := io.Pipe()
	c.stdout = pr

	go c.readEvents()

	go func() {
		_, _ = pw.Write([]byte(`{"type":"agent_end"}`)) // no trailing newline
		_ = pw.Close()
	}()

	select {
	case event, ok := <-c.eventCh:
		if !ok {
			t.Fatalf("event channel closed without delivering the final event; log: %s", logBuf.String())
		}
		if event.Type != "agent_end" {
			t.Errorf("event type = %q, want %q", event.Type, "agent_end")
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("final event was not delivered; log: %s", logBuf.String())
	}

	waitForEventChClosed(t, c.eventCh, 5*time.Second)
}

// TestPiClient_readEvents_DeliversMultipleEvents pins the basic behaviour:
// multiple lines are delivered in order and empty lines are skipped.
func TestPiClient_readEvents_DeliversMultipleEvents(t *testing.T) {
	var logBuf strings.Builder
	c := NewClient(PiClientConfig{CWD: "/tmp", Log: newTestLogger(&logBuf)})

	pr, pw := io.Pipe()
	c.stdout = pr

	go c.readEvents()

	go func() {
		_, _ = pw.Write([]byte(`{"type":"message_start"}` + "\n" +
			"\n" +
			`{"type":"message_end"}` + "\n"))
		_ = pw.Close()
	}()

	want := []string{"message_start", "message_end"}
	for i, wantType := range want {
		select {
		case event, ok := <-c.eventCh:
			if !ok {
				t.Fatalf("event channel closed after %d events, want %d; log: %s", i, len(want), logBuf.String())
			}
			if event.Type != wantType {
				t.Errorf("event[%d].type = %q, want %q", i, event.Type, wantType)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("event[%d] was not delivered; log: %s", i, logBuf.String())
		}
	}

	waitForEventChClosed(t, c.eventCh, 5*time.Second)
}
