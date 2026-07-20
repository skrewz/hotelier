package pi

import (
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"
)

// TestGetExitError_NotStarted verifies that GetExitError returns nil
// when the client has not been started.
func TestGetExitError_NotStarted(t *testing.T) {
	c := NewClient(PiClientConfig{CWD: "/tmp"})
	err := c.GetExitError()
	if err != nil {
		t.Errorf("expected nil exit error before start, got: %v", err)
	}
}

// TestGetExitCode_NotStarted verifies that GetExitCode returns -1
// when the client has not been started.
func TestGetExitCode_NotStarted(t *testing.T) {
	c := NewClient(PiClientConfig{CWD: "/tmp"})
	code := c.GetExitCode()
	if code != -1 {
		t.Errorf("expected exit code -1 before start, got: %d", code)
	}
}

// TestGetStderrLines_Empty verifies that GetStderrLines returns an empty
// slice when no stderr has been captured.
func TestGetStderrLines_Empty(t *testing.T) {
	c := NewClient(PiClientConfig{CWD: "/tmp"})
	lines := c.GetStderrLines()
	if lines == nil {
		t.Error("expected empty slice, got nil")
	}
	if len(lines) != 0 {
		t.Errorf("expected 0 stderr lines, got %d", len(lines))
	}
}

// TestGetEventHistory_Empty verifies that GetEventHistory returns an empty
// slice when no events have been captured.
func TestGetEventHistory_Empty(t *testing.T) {
	c := NewClient(PiClientConfig{CWD: "/tmp"})
	events := c.GetEventHistory()
	if events == nil {
		t.Error("expected empty slice, got nil")
	}
	if len(events) != 0 {
		t.Errorf("expected 0 events, got %d", len(events))
	}
}

// TestAddStderrLine_CapturesLines verifies that stderr lines are captured
// and returned in chronological order.
func TestAddStderrLine_CapturesLines(t *testing.T) {
	c := NewClient(PiClientConfig{CWD: "/tmp"})

	c.addStderrLine("line 1")
	c.addStderrLine("line 2")
	c.addStderrLine("line 3")

	lines := c.GetStderrLines()
	if len(lines) != 3 {
		t.Fatalf("expected 3 lines, got %d", len(lines))
	}
	if lines[0] != "line 1" {
		t.Errorf("expected 'line 1', got %q", lines[0])
	}
	if lines[1] != "line 2" {
		t.Errorf("expected 'line 2', got %q", lines[1])
	}
	if lines[2] != "line 3" {
		t.Errorf("expected 'line 3', got %q", lines[2])
	}
}

// TestAddStderrLine_RingBuffer verifies that stderr lines are capped at
// maxStderrLines, with oldest lines dropped.
func TestAddStderrLine_RingBuffer(t *testing.T) {
	c := NewClient(PiClientConfig{CWD: "/tmp"})

	// Add more lines than the buffer can hold
	for i := 0; i < maxStderrLines+50; i++ {
		c.addStderrLine("line")
	}

	lines := c.GetStderrLines()
	if len(lines) != maxStderrLines {
		t.Errorf("expected %d lines (capped), got %d", maxStderrLines, len(lines))
	}
}

// TestAddEventToHistory_CapturesEvents verifies that events are captured
// and returned in chronological order.
func TestAddEventToHistory_CapturesEvents(t *testing.T) {
	c := NewClient(PiClientConfig{CWD: "/tmp"})

	c.addEventToHistory(Event{Type: "text_delta"})
	c.addEventToHistory(Event{Type: "tool_execution_start"})
	c.addEventToHistory(Event{Type: "guest_end"})

	events := c.GetEventHistory()
	if len(events) != 3 {
		t.Fatalf("expected 3 events, got %d", len(events))
	}
	if events[0].Type != "text_delta" {
		t.Errorf("expected 'text_delta', got %q", events[0].Type)
	}
	if events[1].Type != "tool_execution_start" {
		t.Errorf("expected 'tool_execution_start', got %q", events[1].Type)
	}
	if events[2].Type != "guest_end" {
		t.Errorf("expected 'guest_end', got %q", events[2].Type)
	}
}

// TestAddEventToHistory_RingBuffer verifies that event history is capped at
// maxEventHistory, with oldest events dropped.
func TestAddEventToHistory_RingBuffer(t *testing.T) {
	c := NewClient(PiClientConfig{CWD: "/tmp"})

	// Add more events than the buffer can hold
	for i := 0; i < maxEventHistory+20; i++ {
		c.addEventToHistory(Event{Type: "text_delta"})
	}

	events := c.GetEventHistory()
	if len(events) != maxEventHistory {
		t.Errorf("expected %d events (capped), got %d", maxEventHistory, len(events))
	}
}

// TestGetStderrLines_ReturnsCopy verifies that GetStderrLines returns a
// copy of the internal slice, so mutations to the returned slice don't
// affect the internal state.
func TestGetStderrLines_ReturnsCopy(t *testing.T) {
	c := NewClient(PiClientConfig{CWD: "/tmp"})

	c.addStderrLine("original")
	lines := c.GetStderrLines()

	// Mutate the returned slice
	lines[0] = "mutated"

	// Internal state should be unchanged
	internalLines := c.GetStderrLines()
	if internalLines[0] != "original" {
		t.Errorf("internal state was mutated: got %q, want 'original'", internalLines[0])
	}
}

// TestGetEventHistory_ReturnsCopy verifies that GetEventHistory returns a
// copy of the internal slice.
func TestGetEventHistory_ReturnsCopy(t *testing.T) {
	c := NewClient(PiClientConfig{CWD: "/tmp"})

	c.addEventToHistory(Event{Type: "original"})
	events := c.GetEventHistory()

	// Mutate the returned slice
	events[0].Type = "mutated"

	// Internal state should be unchanged
	internalEvents := c.GetEventHistory()
	if internalEvents[0].Type != "original" {
		t.Errorf("internal state was mutated: got %q, want 'original'", internalEvents[0].Type)
	}
}

// TestStderrAndEventCapture_Concurrent verifies that stderr and event
// capture are safe under concurrent access.
func TestStderrAndEventCapture_Concurrent(t *testing.T) {
	c := NewClient(PiClientConfig{CWD: "/tmp"})

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(3)
		go func(n int) {
			defer wg.Done()
			c.addStderrLine("stderr")
		}(i)
		go func(n int) {
			defer wg.Done()
			c.addEventToHistory(Event{Type: "event"})
		}(i)
		go func(n int) {
			defer wg.Done()
			_ = c.GetStderrLines()
			_ = c.GetEventHistory()
		}(i)
	}
	wg.Wait()

	lines := c.GetStderrLines()
	events := c.GetEventHistory()
	if len(lines) != 10 {
		t.Errorf("expected 10 stderr lines, got %d", len(lines))
	}
	if len(events) != 10 {
		t.Errorf("expected 10 events, got %d", len(events))
	}
}

// TestEventHistory_PreservesFullEvent verifies that the full event data
// is preserved in history, not just the type.
func TestEventHistory_PreservesFullEvent(t *testing.T) {
	c := NewClient(PiClientConfig{CWD: "/tmp"})

	event := Event{
		Type:                  "tool_execution_end",
		ToolName:              "bash",
		ToolCallId:            "call-123",
		IsError:               true,
		AssistantMessageEvent: json.RawMessage(`{"type":"text_delta","delta":"hello"}`),
	}
	c.addEventToHistory(event)

	events := c.GetEventHistory()
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if events[0].Type != event.Type {
		t.Errorf("type mismatch: got %q, want %q", events[0].Type, event.Type)
	}
	if events[0].ToolName != event.ToolName {
		t.Errorf("tool name mismatch: got %q, want %q", events[0].ToolName, event.ToolName)
	}
	if events[0].ToolCallId != event.ToolCallId {
		t.Errorf("tool call id mismatch: got %q, want %q", events[0].ToolCallId, event.ToolCallId)
	}
	if events[0].IsError != event.IsError {
		t.Errorf("isError mismatch: got %v, want %v", events[0].IsError, event.IsError)
	}
}

// TestSpawnLineCount_IndependentOfDiagnostics verifies that the spawn
// line counter is independent of the diagnostic capture mechanisms.
func TestSpawnLineCount_IndependentOfDiagnostics(t *testing.T) {
	c := NewClient(PiClientConfig{
		CWD:         "/tmp",
		SpawnOutput: func(line string) {},
	})

	// Simulate some spawn output
	for i := int32(0); i < 15; i++ {
		c.trySpawnOutput("line")
	}

	// Spawn line count should be 15
	count := atomic.LoadInt32(&c.spawnLineCount)
	if count != 15 {
		t.Errorf("expected spawnLineCount 15, got %d", count)
	}

	// But stderr lines should be empty (spawn output bypasses stderr capture)
	lines := c.GetStderrLines()
	if len(lines) != 0 {
		t.Errorf("expected 0 stderr lines (spawn output bypasses capture), got %d", len(lines))
	}
}
