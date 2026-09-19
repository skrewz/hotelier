package guest

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// TestPIHandler_ToolStartCarriesArgsJSON verifies that a tool_execution_start
// event produces a log entry whose ToolArgsJSON field carries the raw
// argument JSON verbatim, alongside the display string in ToolArgs. The UI
// needs the raw JSON to render a diff for edit tool calls (issue #2).
func TestPIHandler_ToolStartCarriesArgsJSON(t *testing.T) {
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 not installed, cannot run fake pi")
	}

	fakeBinDir, err := os.MkdirTemp("", "hotelier-fakepi-bin-*")
	if err != nil {
		t.Fatalf("create fake bin dir: %v", err)
	}
	defer os.RemoveAll(fakeBinDir)

	// The first 10 stdout lines of a freshly-spawned pi client are captured
	// by the SpawnOutput callback (as [spawn] logs) and NOT parsed as events.
	// Emit 10 harmless queue_update lines first so the tool events below fall
	// outside that window and are processed as events.
	script := `#!/usr/bin/env python3
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

emit({"type": "agent_start"})
args = {
    "path": "/tmp/example-file.py",
    "edits": [
        {"oldText": "x = 1\ny = 2", "newText": "x = 1\ny = 3\nz = 4"}
    ],
}
emit({"type": "tool_execution_start", "toolCallId": "call-edit-1", "toolName": "edit", "args": args})
emit({"type": "tool_execution_end", "toolCallId": "call-edit-1", "toolName": "edit",
      "result": {"content": [{"type": "text", "text": "Successfully replaced 1 block(s) in /tmp/example-file.py."}]},
      "isError": False})
emit({"type": "agent_end", "willRetry": False})
emit({"type": "agent_settled"})
`
	fakePi := filepath.Join(fakeBinDir, "pi")
	if err := os.WriteFile(fakePi, []byte(script), 0o755); err != nil {
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
	h.chrootEnabled = false // non-chroot tool-event flow; chroot covered by dedicated tests

	task := TaskAssignment{
		TaskID: "test-toolargs-json",
		Prompt: "edit the file",
	}

	var mu sync.Mutex
	var startEntries []LogEntry

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	result, err := h.ExecuteTask(ctx, task, func(entry LogEntry) error {
		mu.Lock()
		defer mu.Unlock()
		if entry.Level == "tool" && entry.ToolType == "start" {
			startEntries = append(startEntries, entry)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("ExecuteTask returned error: %v", err)
	}
	if result == nil || !result.Success {
		t.Fatalf("expected successful task, got result=%+v", result)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(startEntries) != 1 {
		t.Fatalf("expected 1 tool start entry, got %d", len(startEntries))
	}
	entry := startEntries[0]

	if entry.ToolName != "edit" {
		t.Errorf("expected tool name 'edit', got %q", entry.ToolName)
	}
	if entry.ToolID != "call-edit-1" {
		t.Errorf("expected tool id 'call-edit-1', got %q", entry.ToolID)
	}
	// Display string for the header (unchanged behaviour).
	if entry.ToolArgs != "path: /tmp/example-file.py" {
		t.Errorf("expected ToolArgs 'path: /tmp/example-file.py', got %q", entry.ToolArgs)
	}
	// Raw JSON for the UI diff renderer.
	if entry.ToolArgsJSON == "" {
		t.Fatal("expected non-empty ToolArgsJSON on tool start entry")
	}
	var parsed struct {
		Path  string `json:"path"`
		Edits []struct {
			OldText string `json:"oldText"`
			NewText string `json:"newText"`
		} `json:"edits"`
	}
	if err := json.Unmarshal([]byte(entry.ToolArgsJSON), &parsed); err != nil {
		t.Fatalf("ToolArgsJSON is not valid JSON: %v", err)
	}
	if parsed.Path != "/tmp/example-file.py" {
		t.Errorf("expected path '/tmp/example-file.py' in ToolArgsJSON, got %q", parsed.Path)
	}
	if len(parsed.Edits) != 1 || parsed.Edits[0].OldText != "x = 1\ny = 2" || parsed.Edits[0].NewText != "x = 1\ny = 3\nz = 4" {
		t.Errorf("expected edits [{x = 1\\ny = 2 -> x = 1\\ny = 3\\nz = 4}] in ToolArgsJSON, got %+v", parsed.Edits)
	}
}
