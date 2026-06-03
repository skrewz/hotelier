package pi

import (
	"context"
	"encoding/json"
	"os/exec"
	"testing"
	"time"
)

// TestPiClient_StopActuallyTerminatesProcess verifies that Stop() causes the
// pi subprocess to actually exit within a reasonable time. This is a regression
// test: pi is a persistent RPC server that stays alive after stdin is closed,
// so we must kill the process rather than relying on stdin.Close() + cmd.Wait().
func TestPiClient_StopActuallyTerminatesProcess(t *testing.T) {
	if _, err := exec.LookPath("pi"); err != nil {
		t.Skip("pi not installed")
	}

	// Use a long-lived context so exec.CommandContext doesn't kill the process
	ctx := context.Background()

	c := NewClient(PiClientConfig{CWD: "/tmp"})

	if err := c.Start(ctx); err != nil {
		t.Fatalf("start failed: %v", err)
	}

	if !c.IsRunning() {
		t.Fatal("client should be running after Start")
	}

	// Send a prompt (will hang in plan mode, so give it a generous timeout)
	go func() {
		_ = c.Prompt(ctx, "say hi")
	}()

	// Wait a bit for the process to settle
	time.Sleep(2 * time.Second)

	// Verify process is still alive
	state := c.GetProcessState()
	if state != nil {
		t.Fatal("process should not have exited yet")
	}

	// Now try to stop — this is the critical part
	stopDone := make(chan error, 1)
	go func() {
		stopDone <- c.Stop(context.Background())
	}()

	select {
	case err := <-stopDone:
		t.Logf("Stop returned: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("Stop() did not return within 10 seconds — pi subprocess is not exiting " +
			"even with the force-kill fallback.")
	}

	// Verify process actually exited
	time.Sleep(200 * time.Millisecond)
	state = c.GetProcessState()
	if state == nil {
		t.Fatal("process state should not be nil after Stop")
	}
	if !state.Exited() {
		t.Fatal("pi subprocess should have exited after Stop")
	}
}

func TestIsThinkingDelta(t *testing.T) {
	tests := []struct {
		name     string
		event    Event
		expected bool
	}{
		{
			name:     "nil assistantMessageEvent",
			event:    Event{},
			expected: false,
		},
		{
			name:     "thinking_delta type",
			event:    Event{AssistantMessageEvent: json.RawMessage(`{"type":"thinking_delta","delta":"let me think"}`)},
			expected: true,
		},
		{
			name:     "text_delta type",
			event:    Event{AssistantMessageEvent: json.RawMessage(`{"type":"text_delta","delta":"hello"}`)},
			expected: false,
		},
		{
			name:     "unknown type",
			event:    Event{AssistantMessageEvent: json.RawMessage(`{"type":"image_delta"}`)},
			expected: false,
		},
		{
			name:     "malformed JSON",
			event:    Event{AssistantMessageEvent: json.RawMessage(`not json`)},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsThinkingDelta(tt.event)
			if got != tt.expected {
				t.Errorf("IsThinkingDelta() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestExtractThinkingDelta(t *testing.T) {
	tests := []struct {
		name     string
		event    Event
		expected string
	}{
		{
			name:     "nil assistantMessageEvent",
			event:    Event{},
			expected: "",
		},
		{
			name:     "thinking_delta with content",
			event:    Event{AssistantMessageEvent: json.RawMessage(`{"type":"thinking_delta","delta":"let me think about this"}`)},
			expected: "let me think about this",
		},
		{
			name:     "thinking_delta empty delta",
			event:    Event{AssistantMessageEvent: json.RawMessage(`{"type":"thinking_delta","delta":""}`)},
			expected: "",
		},
		{
			name:     "text_delta should return empty",
			event:    Event{AssistantMessageEvent: json.RawMessage(`{"type":"text_delta","delta":"hello"}`)},
			expected: "",
		},
		{
			name:     "malformed JSON",
			event:    Event{AssistantMessageEvent: json.RawMessage(`not json`)},
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ExtractThinkingDelta(tt.event)
			if got != tt.expected {
				t.Errorf("ExtractThinkingDelta() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestIsTextDelta(t *testing.T) {
	tests := []struct {
		name     string
		event    Event
		expected bool
	}{
		{
			name:     "nil assistantMessageEvent",
			event:    Event{},
			expected: false,
		},
		{
			name:     "text_delta type",
			event:    Event{AssistantMessageEvent: json.RawMessage(`{"type":"text_delta","delta":"hello"}`)},
			expected: true,
		},
		{
			name:     "thinking_delta type",
			event:    Event{AssistantMessageEvent: json.RawMessage(`{"type":"thinking_delta","delta":"thinking"}`)},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsTextDelta(tt.event)
			if got != tt.expected {
				t.Errorf("IsTextDelta() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestExtractTextDelta(t *testing.T) {
	tests := []struct {
		name     string
		event    Event
		expected string
	}{
		{
			name:     "text_delta with content",
			event:    Event{AssistantMessageEvent: json.RawMessage(`{"type":"text_delta","delta":"hello world"}`)},
			expected: "hello world",
		},
		{
			name:     "thinking_delta should return empty",
			event:    Event{AssistantMessageEvent: json.RawMessage(`{"type":"thinking_delta","delta":"thinking"}`)},
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ExtractTextDelta(tt.event)
			if got != tt.expected {
				t.Errorf("ExtractTextDelta() = %q, want %q", got, tt.expected)
			}
		})
	}
}
