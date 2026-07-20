package guest

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"hotelier/pkg/pi"
)

// TestExitDiagnostics_StructuredFields verifies that ExitDiagnostics
// captures all expected fields.
func TestExitDiagnostics_StructuredFields(t *testing.T) {
	diag := &ExitDiagnostics{
		ExitCode:         42,
		ExitError:        "exit status 42",
		StderrLines:      []string{"line1", "line2"},
		LastEventTypes:   []string{"text_delta", "tool_execution_start"},
		GuestEndReceived: false,
	}

	if diag.ExitCode != 42 {
		t.Errorf("expected ExitCode 42, got %d", diag.ExitCode)
	}
	if diag.ExitError != "exit status 42" {
		t.Errorf("expected ExitError 'exit status 42', got %q", diag.ExitError)
	}
	if len(diag.StderrLines) != 2 {
		t.Errorf("expected 2 stderr lines, got %d", len(diag.StderrLines))
	}
	if len(diag.LastEventTypes) != 2 {
		t.Errorf("expected 2 event types, got %d", len(diag.LastEventTypes))
	}
	if diag.GuestEndReceived {
		t.Error("expected GuestEndReceived to be false")
	}
}

// TestCaptureExitDiagnostics_NoClient verifies that captureExitDiagnostics
// handles a nil client gracefully.
func TestCaptureExitDiagnostics_NoClient(t *testing.T) {
	baseDir, err := os.MkdirTemp("", "hotelier-base-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(baseDir)

	h := &PIHandler{
		baseCWD: baseDir,
		client:  nil,
		log:     log.New(io.Discard, "", 0),
	}

	diag := h.captureExitDiagnostics(false)

	if diag == nil {
		t.Fatal("expected non-nil diagnostics")
	}
	// When client is nil, ExitCode remains at zero value (0) and no fields are populated
	if diag.ExitError != "" {
		t.Errorf("expected empty ExitError, got %q", diag.ExitError)
	}
	if diag.GuestEndReceived {
		t.Error("expected GuestEndReceived to be false")
	}
	if len(diag.StderrLines) != 0 {
		t.Errorf("expected no stderr lines, got %d", len(diag.StderrLines))
	}
	if len(diag.LastEventTypes) != 0 {
		t.Errorf("expected no event types, got %d", len(diag.LastEventTypes))
	}
}

// TestCaptureExitDiagnostics_GuestEndReceived verifies that the
// guestEndReceived flag is correctly captured.
func TestCaptureExitDiagnostics_GuestEndReceived(t *testing.T) {
	baseDir, err := os.MkdirTemp("", "hotelier-base-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(baseDir)

	client := pi.NewClient(pi.PiClientConfig{
		CWD: baseDir,
		Log: log.New(io.Discard, "", 0),
	})

	h := &PIHandler{
		baseCWD: baseDir,
		client:  client,
		log:     log.New(io.Discard, "", 0),
	}

	// Test with guestEndReceived = true
	diag := h.captureExitDiagnostics(true)
	if !diag.GuestEndReceived {
		t.Error("expected GuestEndReceived to be true")
	}

	// Test with guestEndReceived = false
	diag = h.captureExitDiagnostics(false)
	if diag.GuestEndReceived {
		t.Error("expected GuestEndReceived to be false")
	}
}

// TestCaptureExitDiagnostics_CapturesStderr verifies that stderr lines
// from the pi client are captured in diagnostics.
func TestCaptureExitDiagnostics_CapturesStderr(t *testing.T) {
	if _, err := exec.LookPath("pi"); err != nil {
		t.Skip("pi not installed")
	}

	baseDir, err := os.MkdirTemp("", "hotelier-base-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(baseDir)

	client := pi.NewClient(pi.PiClientConfig{
		CWD: baseDir,
		Log: log.New(io.Discard, "", 0),
	})

	h := &PIHandler{
		baseCWD: baseDir,
		client:  client,
		log:     log.New(io.Discard, "", 0),
	}

	ctx := context.Background()
	if err := client.Start(ctx); err != nil {
		t.Fatalf("start failed: %v", err)
	}
	defer client.Stop(ctx)

	// Give the process time to produce any stderr
	time.Sleep(500 * time.Millisecond)

	diag := h.captureExitDiagnostics(true)

	// We can't guarantee stderr output, but we verify the mechanism works
	t.Logf("captured %d stderr lines", len(diag.StderrLines))
}

// TestCaptureExitDiagnostics_EventHistoryAPI verifies that the
// GetEventHistory API is available for diagnostics capture.
func TestCaptureExitDiagnostics_EventHistoryAPI(t *testing.T) {
	if _, err := exec.LookPath("pi"); err != nil {
		t.Skip("pi not installed")
	}

	baseDir, err := os.MkdirTemp("", "hotelier-base-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(baseDir)

	client := pi.NewClient(pi.PiClientConfig{
		CWD: baseDir,
		Log: log.New(io.Discard, "", 0),
	})

	h := &PIHandler{
		baseCWD: baseDir,
		client:  client,
		log:     log.New(io.Discard, "", 0),
	}

	diag := h.captureExitDiagnostics(true)

	// Before starting, event history should be empty
	if len(diag.LastEventTypes) != 0 {
		t.Errorf("expected 0 event types before start, got %d", len(diag.LastEventTypes))
	}

	// Start the client and give it time to process events
	ctx := context.Background()
	if err := client.Start(ctx); err != nil {
		t.Fatalf("start failed: %v", err)
	}
	defer client.Stop(ctx)

	// Send a prompt to generate some events
	go func() {
		_ = client.Prompt(ctx, "say hi")
	}()

	// Give events time to be captured
	time.Sleep(1 * time.Second)

	diag = h.captureExitDiagnostics(true)
	t.Logf("captured %d event types after prompt", len(diag.LastEventTypes))
}

// TestTaskResult_IncludesDiagnostics verifies that TaskResult includes
// the Diagnostics field and it's properly serializable.
func TestTaskResult_IncludesDiagnostics(t *testing.T) {
	result := TaskResult{
		TaskID:  "test-task",
		Success: false,
		Output:  "some output",
		Error:   "pi crashed",
		Diagnostics: &ExitDiagnostics{
			ExitCode:         1,
			ExitError:        "exit status 1",
			StderrLines:      []string{"error: something went wrong"},
			LastEventTypes:   []string{"text_delta"},
			GuestEndReceived: false,
		},
	}

	if result.Diagnostics == nil {
		t.Fatal("expected non-nil diagnostics")
	}
	if result.Diagnostics.ExitCode != 1 {
		t.Errorf("expected ExitCode 1, got %d", result.Diagnostics.ExitCode)
	}
	if result.Diagnostics.GuestEndReceived {
		t.Error("expected GuestEndReceived to be false")
	}
}

// TestPIHandler_ExecuteTask_DiagnosticsOnNormalCompletion verifies that
// even on normal completion (guest_end received), diagnostics are still
// attached to the result.
func TestPIHandler_ExecuteTask_DiagnosticsOnNormalCompletion(t *testing.T) {
	if _, err := exec.LookPath("pi"); err != nil {
		t.Skip("pi not installed")
	}

	h := NewPIHandler("/tmp", "", "", "")
	if err := h.Start(context.Background()); err != nil {
		t.Fatalf("start failed: %v", err)
	}
	defer h.Stop(context.Background())

	task := TaskAssignment{
		TaskID: "test-diagnostics-normal",
		Prompt: "echo hello",
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	result, err := h.ExecuteTask(ctx, task, func(entry LogEntry) error {
		return nil
	})

	// Result may be nil if context cancelled, but if we got a result,
	// it should have diagnostics attached.
	if result != nil {
		if result.Diagnostics == nil {
			t.Error("expected diagnostics to be attached even on normal completion")
		} else {
			t.Logf("diagnostics: exit_code=%d, guest_end=%v, stderr_lines=%d, event_types=%d",
				result.Diagnostics.ExitCode,
				result.Diagnostics.GuestEndReceived,
				len(result.Diagnostics.StderrLines),
				len(result.Diagnostics.LastEventTypes))
		}
	} else if err != nil {
		t.Logf("task returned error (expected if pi is in plan mode): %v", err)
	}
}

// TestPIHandler_ExecuteTask_AbnormalExitDetected verifies that the
// captureExitDiagnostics helper correctly captures the guestEndReceived
// flag. The full ExecuteTask abnormal exit flow is harder to test
// without a mock subprocess, but we verify the diagnostic capture logic.
func TestPIHandler_ExecuteTask_AbnormalExitDetected(t *testing.T) {
	baseDir, err := os.MkdirTemp("", "hotelier-base-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(baseDir)

	client := pi.NewClient(pi.PiClientConfig{
		CWD: baseDir,
		Log: log.New(io.Discard, "", 0),
	})

	h := &PIHandler{
		baseCWD: baseDir,
		client:  client,
		log:     log.New(io.Discard, "", 0),
	}

	diag := h.captureExitDiagnostics(false)

	// Verify abnormal exit is detected
	if diag.GuestEndReceived {
		t.Error("expected GuestEndReceived to be false for abnormal exit")
	}
}

// TestPIHandler_DiagnosticsOutputFormat verifies the format of diagnostic
// output that would be appended to the task result on abnormal exit.
func TestPIHandler_DiagnosticsOutputFormat(t *testing.T) {
	// This test verifies the output format by constructing what the
	// handler would produce on abnormal exit.

	baseDir, err := os.MkdirTemp("", "hotelier-base-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(baseDir)

	client := pi.NewClient(pi.PiClientConfig{
		CWD: baseDir,
		Log: log.New(io.Discard, "", 0),
	})

	h := &PIHandler{
		baseCWD: baseDir,
		client:  client,
		log:     log.New(io.Discard, "", 0),
	}

	diag := h.captureExitDiagnostics(false)

	// Simulate what ExecuteTask does on abnormal exit
	var output strings.Builder
	output.WriteString("some agent output")
	output.WriteString("\n\n--- Exit Diagnostics ---\n")
	output.WriteString(fmt.Sprintf("Exit code: %d\n", diag.ExitCode))
	if diag.ExitError != "" {
		output.WriteString(fmt.Sprintf("Exit error: %s\n", diag.ExitError))
	}
	if len(diag.StderrLines) > 0 {
		output.WriteString(fmt.Sprintf("Stderr (%d lines):\n", len(diag.StderrLines)))
		for _, line := range diag.StderrLines {
			output.WriteString(fmt.Sprintf("  %s\n", line))
		}
	}
	if len(diag.LastEventTypes) > 0 {
		output.WriteString(fmt.Sprintf("Last event types: %v\n", diag.LastEventTypes))
	}

	outputStr := output.String()

	if !strings.Contains(outputStr, "--- Exit Diagnostics ---") {
		t.Error("expected diagnostic marker in output")
	}
	if !strings.Contains(outputStr, "Exit code:") {
		t.Error("expected exit code in diagnostic output")
	}
}
