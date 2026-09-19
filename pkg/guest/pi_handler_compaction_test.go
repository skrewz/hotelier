package guest

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// runFakePiTask runs ExecuteTask against a fake `pi` subprocess that emits
// the given JSON event lines (after 10 noise lines to clear the spawn-output
// capture window) and returns the task result and collected log entries.
//
// Note: eventLines are embedded verbatim into a Python script as dict
// literals, so use Python literals (True/False/None), not JSON ones.
func runFakePiTask(t *testing.T, eventLines []string) (*TaskResult, []LogEntry) {
	t.Helper()

	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 not installed, cannot run fake pi")
	}

	fakeBinDir, err := os.MkdirTemp("", "hotelier-fakepi-bin-*")
	if err != nil {
		t.Fatalf("create fake bin dir: %v", err)
	}
	defer os.RemoveAll(fakeBinDir)

	var script strings.Builder
	script.WriteString(`#!/usr/bin/env python3
import sys, json

def emit(obj):
    sys.stdout.write(json.dumps(obj) + "\n")
    sys.stdout.flush()

try:
    sys.stdin.readline()
except Exception:
    pass

for _ in range(10):
    emit({"type": "queue_update", "queue": []})

`)
	for _, line := range eventLines {
		script.WriteString("emit(" + line + ")\n")
	}

	fakePi := filepath.Join(fakeBinDir, "pi")
	if err := os.WriteFile(fakePi, []byte(script.String()), 0o755); err != nil {
		t.Fatalf("write fake pi: %v", err)
	}

	origPath := os.Getenv("PATH")
	t.Cleanup(func() { os.Setenv("PATH", origPath) })
	os.Setenv("PATH", fakeBinDir+string(os.PathListSeparator)+origPath)

	baseDir, err := os.MkdirTemp("", "hotelier-base-*")
	if err != nil {
		t.Fatalf("create base dir: %v", err)
	}
	defer os.RemoveAll(baseDir)

	h := NewPIHandler(baseDir, "", "", "")
	h.chrootEnabled = false // non-chroot compaction flow; chroot covered by dedicated tests

	// The log callback is invoked from multiple goroutines (the event loop
	// and the spawn-output path), so guard the shared slice.
	var mu sync.Mutex
	var entries []LogEntry
	task := TaskAssignment{TaskID: "test-compaction", Prompt: "do the thing"}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	result, err := h.ExecuteTask(ctx, task, func(entry LogEntry) error {
		mu.Lock()
		entries = append(entries, entry)
		mu.Unlock()
		return nil
	})
	if err != nil {
		t.Fatalf("ExecuteTask returned error: %v", err)
	}
	if result == nil {
		t.Fatal("ExecuteTask returned nil result")
	}
	if !result.Success {
		t.Fatalf("expected task to succeed, got success=%v error=%q output=%q", result.Success, result.Error, result.Output)
	}
	return result, entries
}

func compactionEntries(entries []LogEntry) []LogEntry {
	var out []LogEntry
	for _, e := range entries {
		if e.Level == "compaction" {
			out = append(out, e)
		}
	}
	return out
}

// TestPIHandler_CompactionEvents verifies that pi compaction events are
// converted into structured "compaction" log entries (issue #53). The event
// sequence mirrors pi's real wire format: compaction_start, then
// compaction_end with a result containing the summary and token counts.
func TestPIHandler_CompactionEvents(t *testing.T) {
	_, entries := runFakePiTask(t, []string{
		`{"type": "agent_start"}`,
		`{"type": "message_update", "assistantMessageEvent": {"type": "text_delta", "delta": "working..."}}`,
		`{"type": "compaction_start", "reason": "threshold"}`,
		`{"type": "compaction_end", "reason": "threshold", "result": {"summary": "## Goal\nDo the thing.", "firstKeptEntryId": "abc", "tokensBefore": 150000, "estimatedTokensAfter": 32000}}`,
		`{"type": "message_update", "assistantMessageEvent": {"type": "text_delta", "delta": " done"}}`,
		`{"type": "agent_settled"}`,
	})

	comp := compactionEntries(entries)
	if len(comp) != 2 {
		t.Fatalf("expected 2 compaction log entries, got %d: %+v", len(comp), comp)
	}

	start := comp[0]
	if start.CompactionType != "start" {
		t.Errorf("start.CompactionType = %q, want %q", start.CompactionType, "start")
	}
	if start.CompactionReason != "threshold" {
		t.Errorf("start.CompactionReason = %q, want %q", start.CompactionReason, "threshold")
	}
	if start.Line != "[COMPACTION_START] (reason: threshold)" {
		t.Errorf("start.Line = %q, want %q", start.Line, "[COMPACTION_START] (reason: threshold)")
	}
	if start.TaskID != "test-compaction" {
		t.Errorf("start.TaskID = %q, want %q", start.TaskID, "test-compaction")
	}

	end := comp[1]
	if end.CompactionType != "end" {
		t.Errorf("end.CompactionType = %q, want %q", end.CompactionType, "end")
	}
	if end.CompactionReason != "threshold" {
		t.Errorf("end.CompactionReason = %q, want %q", end.CompactionReason, "threshold")
	}
	if end.CompactionSummary != "## Goal\nDo the thing." {
		t.Errorf("end.CompactionSummary = %q, want the summary", end.CompactionSummary)
	}
	if end.CompactionTokensBefore != 150000 {
		t.Errorf("end.CompactionTokensBefore = %d, want 150000", end.CompactionTokensBefore)
	}
	if end.CompactionTokensAfter != 32000 {
		t.Errorf("end.CompactionTokensAfter = %d, want 32000", end.CompactionTokensAfter)
	}
	if end.CompactionError {
		t.Error("end.CompactionError = true, want false for successful compaction")
	}
	wantLine := "[COMPACTION_END] (reason: threshold, 150000 -> 32000 tokens)"
	if end.Line != wantLine {
		t.Errorf("end.Line = %q, want %q", end.Line, wantLine)
	}
}

// TestPIHandler_FailedCompactionEvent verifies that a failed compaction
// (aborted with an error message) produces a compaction entry flagged as an
// error, so the UI can render a failed state.
func TestPIHandler_FailedCompactionEvent(t *testing.T) {
	_, entries := runFakePiTask(t, []string{
		`{"type": "agent_start"}`,
		`{"type": "compaction_start", "reason": "overflow"}`,
		`{"type": "compaction_end", "reason": "overflow", "aborted": True, "errorMessage": "LLM request failed"}`,
		`{"type": "agent_settled"}`,
	})

	comp := compactionEntries(entries)
	if len(comp) != 2 {
		t.Fatalf("expected 2 compaction log entries, got %d: %+v", len(comp), comp)
	}

	end := comp[1]
	if !end.CompactionError {
		t.Error("end.CompactionError = false, want true for failed compaction")
	}
	if end.CompactionSummary != "" {
		t.Errorf("end.CompactionSummary = %q, want empty for failed compaction", end.CompactionSummary)
	}
	wantLine := "[COMPACTION_END] (reason: overflow) [ERROR] LLM request failed"
	if end.Line != wantLine {
		t.Errorf("end.Line = %q, want %q", end.Line, wantLine)
	}
}
