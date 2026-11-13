//go:build integration

// Package integration provides integration tests for the hotelier UI.
// Run with: go test -tags=integration ./test/integration/ -run TestValidateToolUI
package integration

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"hotelier/internal/server"
	"hotelier/pkg/config"
	"hotelier/pkg/logstore"
	"hotelier/pkg/persona"
	"hotelier/pkg/pi"
	"hotelier/pkg/queue"
	"hotelier/pkg/rpc"
)

// testLogEntry represents a single log entry used across both the RPC and
// Playwright validation paths.
type testLogEntry struct {
	line         string
	level        string
	toolType     string // "start", "output", "end"
	toolName     string // e.g. "bash", "read"
	toolID       string // unique tool call identifier
	toolArgs     string // arguments/parameters (display string, produced by pi.ToolArgs)
	toolArgsJSON string // raw argument JSON (mirrors LogEntry.ToolArgsJSON, issue #2)
	toolOutput   string // captured output
	toolError    bool   // true if tool ended with error
	// compaction fields (level "compaction")
	compactionType         string // "start", "end"
	compactionReason       string // "manual", "threshold", "overflow"
	compactionSummary      string // generated summary (end)
	compactionTokensBefore int    // context size before (end)
	compactionTokensAfter  int    // estimated size after (end)
	compactionError        bool   // true if compaction failed
}

// piRPCEvent captures the top-level type field from a Pi RPC wire event.
type piRPCEvent struct {
	Type string `json:"type"`
}

// piMessageUpdateEvent captures message_update events.
type piMessageUpdateEvent struct {
	Type                  string                  `json:"type"`
	AssistantMessageEvent piAssistantMessageEvent `json:"assistantMessageEvent"`
}

// piAssistantMessageEvent captures the nested event inside message_update.
type piAssistantMessageEvent struct {
	Type  string `json:"type"`
	Delta string `json:"delta"`
}

// piToolCallInfo extracts tool call details from a message_update partial.
type piToolCallInfo struct {
	Type      string `json:"type"`
	ID        string `json:"id"`
	Name      string `json:"name"`
	Arguments any    `json:"arguments"`
}

// piCompactionEvent captures compaction_start / compaction_end events.
type piCompactionEvent struct {
	Type         string `json:"type"`
	Reason       string `json:"reason"`
	Aborted      bool   `json:"aborted"`
	ErrorMessage string `json:"errorMessage"`
	Result       struct {
		Summary              string `json:"summary"`
		TokensBefore         int    `json:"tokensBefore"`
		EstimatedTokensAfter int    `json:"estimatedTokensAfter"`
	} `json:"result"`
}

// parsePiRPCCapture reads the Pi RPC log file and converts real wire events
// into testLogEntry format compatible with hotelier's guest.log RPC method.
func parsePiRPCCapture(filePath string) ([]testLogEntry, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", filePath, err)
	}

	var entries []testLogEntry
	lines := strings.Split(string(data), "\n")

	for _, line := range lines {
		// Each line starts with a prefix like "[STDOUT←pi] 2026-05-24T...: "
		// followed by a JSON payload. Anchor on the first '{' — using the
		// last ": " is unsafe because payloads may contain ": " inside
		// string values (e.g. compaction summaries).
		jsonStart := strings.Index(line, "{")
		if jsonStart == -1 {
			continue
		}
		payload := strings.TrimSpace(line[jsonStart:])
		if payload == "" {
			continue
		}

		// Quick type check to avoid full unmarshalling on non-interesting lines
		var evt piRPCEvent
		if err := json.Unmarshal([]byte(payload), &evt); err != nil {
			continue
		}

		switch evt.Type {
		case "tool_execution_start":
			var toolEvt pi.Event
			if err := json.Unmarshal([]byte(payload), &toolEvt); err != nil {
				continue
			}
			// Use the production extraction (pi.ToolArgs) so the test data
			// cannot drift from what the guest actually sends.
			displayArgs := pi.ToolArgs(toolEvt)
			entries = append(entries, testLogEntry{
				line:         fmt.Sprintf("[TOOL_START] %s: %s (id: %s)", toolEvt.ToolName, displayArgs, toolEvt.ToolCallId),
				level:        "tool",
				toolType:     "start",
				toolName:     toolEvt.ToolName,
				toolID:       toolEvt.ToolCallId,
				toolArgs:     displayArgs,
				toolArgsJSON: string(toolEvt.Args),
			})

		case "tool_execution_update":
			// Production emits a [TOOL_OUTPUT] entry for update events whose
			// partial result carries text: pkg/guest/pi_handler.go handles
			// tool_execution_update via pi.ToolPartialResult and skips events
			// with no text.
			var toolEvt pi.Event
			if err := json.Unmarshal([]byte(payload), &toolEvt); err != nil {
				continue
			}
			partial := pi.ToolPartialResult(toolEvt)
			if partial == "" {
				continue
			}
			entries = append(entries, testLogEntry{
				line:       fmt.Sprintf("[TOOL_OUTPUT] %s (id: %s): %s", toolEvt.ToolName, toolEvt.ToolCallId, partial),
				level:      "tool",
				toolType:   "output",
				toolName:   toolEvt.ToolName,
				toolID:     toolEvt.ToolCallId,
				toolOutput: partial,
			})

		case "tool_execution_end":
			var toolEvt pi.Event
			if err := json.Unmarshal([]byte(payload), &toolEvt); err != nil {
				continue
			}
			// The guest sends the extracted text (pi.ToolResult), not the raw
			// result JSON.
			resultText := pi.ToolResult(toolEvt)
			entries = append(entries, testLogEntry{
				line:       fmt.Sprintf("[TOOL_END] %s (id: %s): %s", toolEvt.ToolName, toolEvt.ToolCallId, resultText),
				level:      "tool",
				toolType:   "end",
				toolName:   toolEvt.ToolName,
				toolID:     toolEvt.ToolCallId,
				toolOutput: resultText,
				toolError:  toolEvt.IsError,
			})

		case "message_update":
			var msgEvt piMessageUpdateEvent
			if err := json.Unmarshal([]byte(payload), &msgEvt); err != nil {
				continue
			}
			switch msgEvt.AssistantMessageEvent.Type {
			case "thinking_delta":
				if msgEvt.AssistantMessageEvent.Delta != "" {
					entries = append(entries, testLogEntry{
						line:  msgEvt.AssistantMessageEvent.Delta,
						level: "thinking",
					})
				}
			case "text_delta":
				if msgEvt.AssistantMessageEvent.Delta != "" {
					entries = append(entries, testLogEntry{
						line:  msgEvt.AssistantMessageEvent.Delta,
						level: "text",
					})
				}
			}

		case "compaction_start", "compaction_end":
			var compEvt piCompactionEvent
			if err := json.Unmarshal([]byte(payload), &compEvt); err != nil {
				continue
			}
			if evt.Type == "compaction_start" {
				entries = append(entries, testLogEntry{
					line:             fmt.Sprintf("[COMPACTION_START] (reason: %s)", compEvt.Reason),
					level:            "compaction",
					compactionType:   "start",
					compactionReason: compEvt.Reason,
				})
				continue
			}
			// compaction_end
			errMsg := compEvt.ErrorMessage
			if compEvt.Aborted && errMsg == "" {
				errMsg = "aborted"
			}
			var line string
			if errMsg != "" {
				line = fmt.Sprintf("[COMPACTION_END] (reason: %s) [ERROR] %s", compEvt.Reason, errMsg)
			} else if compEvt.Result.TokensBefore > 0 {
				line = fmt.Sprintf("[COMPACTION_END] (reason: %s, %d -> %d tokens)", compEvt.Reason, compEvt.Result.TokensBefore, compEvt.Result.EstimatedTokensAfter)
			} else {
				line = fmt.Sprintf("[COMPACTION_END] (reason: %s)", compEvt.Reason)
			}
			entries = append(entries, testLogEntry{
				line:                   line,
				level:                  "compaction",
				compactionType:         "end",
				compactionReason:       compEvt.Reason,
				compactionSummary:      compEvt.Result.Summary,
				compactionTokensBefore: compEvt.Result.TokensBefore,
				compactionTokensAfter:  compEvt.Result.EstimatedTokensAfter,
				compactionError:        errMsg != "",
			})
		}
	}

	return entries, nil
}

// loadCaptureEntries parses a Pi RPC capture file and prepends the system/
// thinking/text messages that hotelier generates itself (not sent over RPC).
func loadCaptureEntries(logPath, taskID, prompt string) []testLogEntry {
	entries, err := parsePiRPCCapture(logPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "WARN: parsePiRPCCapture(%s) failed: %v\n", logPath, err)
		return nil
	}
	entries = append([]testLogEntry{
		{line: "Task started", level: "system"},
		{line: fmt.Sprintf("Executing task %s", taskID), level: "system"},
		{line: fmt.Sprintf("Cloning https://github.com/example/repo.git -> /tmp/hotelier/tasks/%s/repo", taskID), level: "system"},
		{line: "Cloned https://github.com/example/repo.git", level: "system"},
		{line: fmt.Sprintf("Spawning pi subprocess in: /tmp/hotelier/tasks/%s/repo", taskID), level: "system"},
		{line: "[spawn] Checking pi binary...", level: "system"},
		{line: "[spawn] Checking configuration...", level: "system"},
		{line: "[spawn] Starting RPC server...", level: "system"},
		{line: "Sending prompt to pi", level: "system"},
		{line: "Prompt sent, waiting for events", level: "system"},
	}, entries...)
	return entries
}

// testLogEntries returns the shared set of simulated log entries for the
// main (tool-call) task.  These are parsed directly from the real Pi RPC
// capture log to ensure the integration test uses the actual wire format.
func testLogEntries() []testLogEntry {
	return loadCaptureEntries(
		"test/integration/pi_rpc_capture_redacted.log",
		"ui-test-task-PLACEHOLDER",
		"Simulate tool calls and validate UI rendering",
	)
}

// testLogEntriesCompleted returns log entries for the completed-task flow,
// parsed from a real Pi RPC capture of a simple successful run.
func testLogEntriesCompleted(taskID string) []testLogEntry {
	return loadCaptureEntries(
		"test/integration/pi_rpc_capture_completed_redacted.log",
		taskID,
		"Reply with just the word done",
	)
}

// testLogEntriesFailed returns log entries for the failed-task flow,
// parsed from a real Pi RPC capture of an interrupted run.
func testLogEntriesFailed(taskID string) []testLogEntry {
	return loadCaptureEntries(
		"test/integration/pi_rpc_capture_failed_redacted.log",
		taskID,
		"Run a long computation that will be interrupted",
	)
}

// testLogEntriesCompaction returns log entries for a task that underwent
// context compaction, parsed from a real Pi RPC capture (issue #53). The
// capture contains two trivial prompt exchanges followed by a manual
// `compact` command, producing compaction_start and compaction_end events.
func testLogEntriesCompaction(taskID string) []testLogEntry {
	return loadCaptureEntries(
		"test/integration/pi_rpc_capture_compaction_redacted.log",
		taskID,
		"Reply with just the word done",
	)
}

// testLogEntriesTitleURL returns log entries for a task that carries a
// title and a reference URL (issue #229). The entries come from the same
// real Pi RPC capture as the completed-task flow — only the task metadata
// differs.
func testLogEntriesTitleURL(taskID string) []testLogEntry {
	return loadCaptureEntries(
		"test/integration/pi_rpc_capture_completed_redacted.log",
		taskID,
		"Investigate flaky integration tests and raise a fix",
	)
}

// testLogEntriesTitleOnly returns log entries for a task that carries a
// title but no reference URL (issue #229), parsed from the same real Pi
// RPC capture as the completed-task flow.
func testLogEntriesTitleOnly(taskID string) []testLogEntry {
	return loadCaptureEntries(
		"test/integration/pi_rpc_capture_completed_redacted.log",
		taskID,
		"Summarise the weekly queue report",
	)
}

// testLogEntriesTitleBadURL returns log entries for a task that carries a
// title and a non-http(s) reference URL (issue #229, review feedback on
// PR #233), parsed from the same real Pi RPC capture as the
// completed-task flow.
func testLogEntriesTitleBadURL(taskID string) []testLogEntry {
	return loadCaptureEntries(
		"test/integration/pi_rpc_capture_completed_redacted.log",
		taskID,
		"Render a title whose URL is not http(s)",
	)
}

// testLogEntriesEdit returns log entries for the edit-tool flow (issue #2),
// parsed from a real Pi RPC capture of a run that read and then edited a
// file. The capture contains a read tool call and an edit tool call whose
// args carry oldText/newText for the UI diff renderer.
func testLogEntriesEdit(taskID string) []testLogEntry {
	return loadCaptureEntries(
		"test/integration/pi_rpc_capture_edit_redacted.log",
		taskID,
		"Edit /tmp/example-edit-target.txt: change the word 'hello' to 'goodbye' in the return statement. Do not do anything else.",
	)
}

// sendLogEntries sends a slice of log entries to the server via guest.log RPC.
func sendLogEntries(t *testing.T, guestWS *websocket.Conn, taskID string, entries []testLogEntry) {
	t.Helper()
	for _, entry := range entries {
		logParams, _ := json.Marshal(map[string]interface{}{
			"task_id":                  taskID,
			"line":                     entry.line,
			"level":                    entry.level,
			"tool_type":                entry.toolType,
			"tool_name":                entry.toolName,
			"tool_id":                  entry.toolID,
			"tool_args":                entry.toolArgs,
			"tool_args_json":           entry.toolArgsJSON,
			"tool_output":              entry.toolOutput,
			"tool_error":               entry.toolError,
			"compaction_type":          entry.compactionType,
			"compaction_reason":        entry.compactionReason,
			"compaction_summary":       entry.compactionSummary,
			"compaction_tokens_before": entry.compactionTokensBefore,
			"compaction_tokens_after":  entry.compactionTokensAfter,
			"compaction_error":         entry.compactionError,
		})
		if err := rpc.WriteMessage(guestWS, &rpc.JSONRPCMessage{
			JSONRPC: "2.0", ID: jsonID(2), Method: "guest.log", Params: logParams,
		}); err != nil {
			t.Fatalf("send guest.log (%s): %v", entry.line[:min(40, len(entry.line))], err)
		}
		logResp, err := rpc.ReadMessage(guestWS)
		if err != nil || logResp.Error != nil {
			t.Fatalf("guest.log response: err=%v resp=%+v", err, logResp)
		}
		t.Logf("Sent log: [%s] %s", entry.level, entry.line[:min(60, len(entry.line))])
		time.Sleep(50 * time.Millisecond)
	}
}

// flushAccumulator forces the LogAccumulator to flush any buffered entries.
// This is needed because text deltas are batched and only flushed on level
// change or explicit FlushAll. Thinking deltas are emitted immediately,
// so they don't need flushing. When the last entries sent are text deltas,
// they remain buffered until this call.
func flushAccumulator(t *testing.T, srv *server.Server) {
	acc := srv.LogAccumulator()
	if acc == nil {
		return
	}
	acc.FlushAll(func(e server.TaskLogEntry) {
		// Write to both in-memory and disk stores, mirroring handleGuestLog.
		srv.LogStore().Add(e)
		if ds := srv.DiskLogStore(); ds != nil {
			_ = ds.Append(logstore.Entry{
				TaskID:       e.TaskID,
				Line:         e.Line,
				Level:        e.Level,
				Timestamp:    e.Timestamp,
				ToolType:     e.ToolType,
				ToolName:     e.ToolName,
				ToolID:       e.ToolID,
				ToolArgs:     e.ToolArgs,
				ToolArgsJSON: e.ToolArgsJSON,
				ToolOutput:   e.ToolOutput,
				ToolError:    e.ToolError,
			})
		}
	})
}

func TestValidateToolUI(t *testing.T) {
	// Ensure we're in the project root so relative paths (webDir, templateDir)
	// resolve correctly — this keeps us on the same code path as production.
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("get cwd: %v", err)
	}

	p := filepath.Clean(filepath.Join(wd, "..", ".."))
	if err := os.Chdir(p); err != nil {
		t.Fatalf("chdir to project root: %v", err)
	}
	projectRoot := p

	logDir := t.TempDir() + "/logs"
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		t.Fatalf("create log dir: %v", err)
	}

	cfg := config.ServerConfig{
		Host:      "127.0.0.1",
		Port:      0,
		MaxGuests: 0,
		LogDir:    logDir,
		Personas: []persona.Persona{
			{Name: "test-purple", Env: map[string]string{"ROLE": "implementer"}},
			{Name: "test-blue", Env: map[string]string{"ROLE": "reviewer"}},
			{Name: "test-green", Env: map[string]string{"ROLE": "architect"}},
		},
	}
	srv := server.New(cfg)
	hub := srv.Hub()
	go hub.Run()

	// Build the HTTP handler exactly as server.Run() does — this ensures we
	// exercise the same code paths as production.
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", srv.HandleWebSocket)
	mux.HandleFunc("/api/tasks", srv.HandleTasks)
	// The exact "/api/tasks/search" pattern is preferred over the
	// "/api/tasks/" subtree whatever the registration order, as
	// server.Routes() notes (issue #269); it is declared first only to
	// mirror that file. Keep the two in step if the set ever changes.
	mux.HandleFunc("/api/tasks/search", srv.HandleTaskSearch)
	mux.HandleFunc("/api/tasks/", srv.HandleTaskDetail)
	mux.HandleFunc("/api/guests", srv.HandleGuests)
	mux.HandleFunc("/api/guests/", srv.HandleGuestDetail)
	mux.HandleFunc("/api/concurrency", srv.HandleConcurrency)
	mux.HandleFunc("/api/health", srv.HandleHealth)
	mux.HandleFunc("/api/logs", srv.HandleLogs)
	mux.HandleFunc("/api/logs/", srv.HandleLogEntry)
	mux.HandleFunc("/", srv.HandleWebUI)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = http.Serve(ln, mux) }()
	time.Sleep(200 * time.Millisecond)

	port := ln.Addr().(*net.TCPAddr).Port
	baseURL := fmt.Sprintf("http://127.0.0.1:%d", port)
	wsURL := fmt.Sprintf("ws://127.0.0.1:%d/ws", port)
	t.Logf("Server running at %s (ws: %s)", baseURL, wsURL)

	// Step 1: Check in a guest via WebSocket RPC.
	guestWS, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial guest WS: %v", err)
	}
	defer guestWS.Close()

	regParams, _ := json.Marshal(map[string]interface{}{
		"id":   "ui-test-guest",
		"name": "UI Test Guest",
		"tags": []string{"business-default"},
		// The machine the guest runs on (issue #238). Fixed rather than the
		// test host's real hostname so the assertions below are stable.
		"host": "ui-test-host",
	})
	if err := rpc.WriteMessage(guestWS, &rpc.JSONRPCMessage{
		JSONRPC: "2.0", ID: jsonID(1), Method: "guest.register", Params: regParams,
	}); err != nil {
		t.Fatalf("send guest.register: %v", err)
	}
	regResp, err := rpc.ReadMessage(guestWS)
	if err != nil || regResp.Error != nil {
		t.Fatalf("guest.register: err=%v resp=%+v", err, regResp)
	}
	t.Logf("Guest registered: %s", string(regResp.Result))

	// Step 2: Create a task via the task queue.
	taskID := fmt.Sprintf("ui-test-task-%d", time.Now().UnixNano())
	taskObj := &queue.Task{
		ID:      taskID,
		Prompt:  "Simulate tool calls and validate UI rendering",
		Tags:    []string{"business-default"},
		Persona: "test-purple",
	}
	if err := srv.TaskQueue().Add(taskObj); err != nil {
		t.Fatalf("add task: %v", err)
	}
	t.Logf("Task created: %s", taskID)

	// Step 3: Assign task to guest, start it, and simulate tool call log entries.
	if err := srv.TaskQueue().Assign(taskID, "ui-test-guest"); err != nil {
		t.Fatalf("assign task: %v", err)
	}
	if err := srv.TaskQueue().Start(taskID); err != nil {
		t.Fatalf("start task: %v", err)
	}
	assignParams, _ := json.Marshal(map[string]interface{}{
		"id":          taskID,
		"prompt":      "Simulate tool calls and validate UI rendering",
		"tags":        []string{"business-default"},
		"assigned_to": "ui-test-guest",
	})
	hub.Broadcast(rpc.ConnectionRoleGuest, &rpc.JSONRPCMessage{
		JSONRPC: "2.0", Method: "task.assign", Params: assignParams,
	})
	time.Sleep(100 * time.Millisecond)

	logEntries := testLogEntries()
	sendLogEntries(t, guestWS, taskID, logEntries)

	// Flush any remaining buffered entries (thinking/text deltas that haven't
	// been flushed by a level change yet).
	flushAccumulator(t, srv)

	// Step 4: Validate server-side log store has correct entries.
	validateServerLogs(t, srv, taskID, logEntries)

	// Step 4b: Create a failed task to verify error display in the UI.
	// Log entries are sent via guest.log RPC from a real Pi RPC capture.
	failedTaskID := fmt.Sprintf("ui-test-failed-task-%d", time.Now().UnixNano())
	failedTask := &queue.Task{
		ID:     failedTaskID,
		Prompt: "Run a long computation that will be interrupted",
		Tags:   []string{"business-default"},
	}
	if err := srv.TaskQueue().Add(failedTask); err != nil {
		t.Fatalf("add failed task: %v", err)
	}
	if err := srv.TaskQueue().Assign(failedTaskID, "ui-test-guest"); err != nil {
		t.Fatalf("assign failed task: %v", err)
	}
	if err := srv.TaskQueue().Start(failedTaskID); err != nil {
		t.Fatalf("start failed task: %v", err)
	}
	failedLogEntries := testLogEntriesFailed(failedTaskID)
	sendLogEntries(t, guestWS, failedTaskID, failedLogEntries)
	const failureReason = "pi subprocess exited with code 1: compilation failed"
	if err := srv.TaskQueue().Fail(failedTaskID, failureReason); err != nil {
		t.Fatalf("fail task: %v", err)
	}
	t.Logf("Failed task created: %s (reason: %s)", failedTaskID, failureReason)

	// Step 4c: Create a completed task to verify the filter hides it by default.
	// Log entries are sent via guest.log RPC from a real Pi RPC capture.
	completedTaskID := fmt.Sprintf("ui-test-completed-task-%d", time.Now().UnixNano())
	completedTask := &queue.Task{
		ID:     completedTaskID,
		Prompt: "Reply with just the word done",
		Tags:   []string{"business-default"},
	}
	if err := srv.TaskQueue().Add(completedTask); err != nil {
		t.Fatalf("add completed task: %v", err)
	}
	if err := srv.TaskQueue().Assign(completedTaskID, "ui-test-guest"); err != nil {
		t.Fatalf("assign completed task: %v", err)
	}
	if err := srv.TaskQueue().Start(completedTaskID); err != nil {
		t.Fatalf("start completed task: %v", err)
	}
	completedLogEntries := testLogEntriesCompleted(completedTaskID)
	sendLogEntries(t, guestWS, completedTaskID, completedLogEntries)
	if err := srv.TaskQueue().Complete(completedTaskID, "Task completed successfully"); err != nil {
		t.Fatalf("complete task: %v", err)
	}
	t.Logf("Completed task created: %s", completedTaskID)

	// Step 4d: Create a pending task (not assigned) to test the
	// "N pending" expand button in the UI.
	pendingTaskID := fmt.Sprintf("ui-test-pending-task-%d", time.Now().UnixNano())
	pendingTask := &queue.Task{
		ID:     pendingTaskID,
		Prompt: "A task that remains pending waiting for a matching guest",
		Tags:   []string{"business-special"},
	}
	if err := srv.TaskQueue().Add(pendingTask); err != nil {
		t.Fatalf("add pending task: %v", err)
	}
	t.Logf("Pending task created: %s", pendingTaskID)

	// Step 4e: Create a second task with a different persona to test colour distribution.
	colouredTaskID := fmt.Sprintf("ui-test-coloured-task-%d", time.Now().UnixNano())
	colouredTask := &queue.Task{
		ID:      colouredTaskID,
		Prompt:  "A task with a distinct persona for colour testing",
		Tags:    []string{"business-default"},
		Persona: "test-blue",
	}
	if err := srv.TaskQueue().Add(colouredTask); err != nil {
		t.Fatalf("add coloured task: %v", err)
	}
	if err := srv.TaskQueue().Assign(colouredTaskID, "ui-test-guest"); err != nil {
		t.Fatalf("assign coloured task: %v", err)
	}
	if err := srv.TaskQueue().Start(colouredTaskID); err != nil {
		t.Fatalf("start coloured task: %v", err)
	}
	t.Logf("Coloured task created: %s", colouredTaskID)

	// Step 4f: Create a running task that underwent context compaction
	// (issue #53). Log entries come from a real Pi RPC capture containing
	// compaction_start and compaction_end events.
	compactionTaskID := fmt.Sprintf("ui-test-compaction-task-%d", time.Now().UnixNano())
	compactionTask := &queue.Task{
		ID:     compactionTaskID,
		Prompt: "Long-running task that triggers context compaction",
		Tags:   []string{"business-default"},
	}
	if err := srv.TaskQueue().Add(compactionTask); err != nil {
		t.Fatalf("add compaction task: %v", err)
	}
	if err := srv.TaskQueue().Assign(compactionTaskID, "ui-test-guest"); err != nil {
		t.Fatalf("assign compaction task: %v", err)
	}
	if err := srv.TaskQueue().Start(compactionTaskID); err != nil {
		t.Fatalf("start compaction task: %v", err)
	}
	compactionLogEntries := testLogEntriesCompaction(compactionTaskID)
	sendLogEntries(t, guestWS, compactionTaskID, compactionLogEntries)
	flushAccumulator(t, srv)
	t.Logf("Compaction task created: %s", compactionTaskID)

	// Step 4g: Create a completed task whose run used the edit tool, to
	// verify inline diff rendering (issue #2). Log entries are sent via
	// guest.log RPC from a real Pi RPC capture containing a read and an
	// edit tool call.
	editTaskID := fmt.Sprintf("ui-test-edit-task-%d", time.Now().UnixNano())
	editTask := &queue.Task{
		ID:     editTaskID,
		Prompt: "Edit /tmp/example-edit-target.txt: change the word 'hello' to 'goodbye' in the return statement. Do not do anything else.",
		Tags:   []string{"business-default"},
	}
	if err := srv.TaskQueue().Add(editTask); err != nil {
		t.Fatalf("add edit task: %v", err)
	}
	if err := srv.TaskQueue().Assign(editTaskID, "ui-test-guest"); err != nil {
		t.Fatalf("assign edit task: %v", err)
	}
	if err := srv.TaskQueue().Start(editTaskID); err != nil {
		t.Fatalf("start edit task: %v", err)
	}
	editLogEntries := testLogEntriesEdit(editTaskID)
	sendLogEntries(t, guestWS, editTaskID, editLogEntries)
	flushAccumulator(t, srv)
	if err := srv.TaskQueue().Complete(editTaskID, "Task completed successfully"); err != nil {
		t.Fatalf("complete edit task: %v", err)
	}
	t.Logf("Edit task created: %s", editTaskID)

	// Step 4h: Create three tasks that carry a title (issue #229): one with
	// a title and a reference URL, one with a title only, and one whose
	// reference URL carries a non-http(s) scheme to exercise the scheme
	// allow-list in taskTitleHTML (review feedback on PR #233). All stay
	// RUNNING so they are visible under the default filter. Log entries come
	// from a real Pi RPC capture and are delivered via the guest.log RPC,
	// like every other task.
	titleUrlTaskID := fmt.Sprintf("ui-test-title-url-task-%d", time.Now().UnixNano())
	titleUrlTaskTitle := "Investigate <b>flaky</b> tests & raise a fix"
	titleUrlTaskURL := "https://example.com/pulls/1"
	titleUrlTask := &queue.Task{
		ID:     titleUrlTaskID,
		Prompt: "Investigate flaky integration tests and raise a fix",
		Tags:   []string{"business-default"},
		Title:  titleUrlTaskTitle,
		URL:    titleUrlTaskURL,
	}
	if err := srv.TaskQueue().Add(titleUrlTask); err != nil {
		t.Fatalf("add title+URL task: %v", err)
	}
	if err := srv.TaskQueue().Assign(titleUrlTaskID, "ui-test-guest"); err != nil {
		t.Fatalf("assign title+URL task: %v", err)
	}
	if err := srv.TaskQueue().Start(titleUrlTaskID); err != nil {
		t.Fatalf("start title+URL task: %v", err)
	}
	sendLogEntries(t, guestWS, titleUrlTaskID, testLogEntriesTitleURL(titleUrlTaskID))
	flushAccumulator(t, srv)
	t.Logf("Title+URL task created: %s (title: %s, url: %s)", titleUrlTaskID, titleUrlTaskTitle, titleUrlTaskURL)

	titleOnlyTaskID := fmt.Sprintf("ui-test-title-only-task-%d", time.Now().UnixNano())
	titleOnlyTaskTitle := "Plain title with no reference URL"
	titleOnlyTask := &queue.Task{
		ID:     titleOnlyTaskID,
		Prompt: "Summarise the weekly queue report",
		Tags:   []string{"business-default"},
		Title:  titleOnlyTaskTitle,
	}
	if err := srv.TaskQueue().Add(titleOnlyTask); err != nil {
		t.Fatalf("add title-only task: %v", err)
	}
	if err := srv.TaskQueue().Assign(titleOnlyTaskID, "ui-test-guest"); err != nil {
		t.Fatalf("assign title-only task: %v", err)
	}
	if err := srv.TaskQueue().Start(titleOnlyTaskID); err != nil {
		t.Fatalf("start title-only task: %v", err)
	}
	sendLogEntries(t, guestWS, titleOnlyTaskID, testLogEntriesTitleOnly(titleOnlyTaskID))
	flushAccumulator(t, srv)
	t.Logf("Title-only task created: %s (title: %s)", titleOnlyTaskID, titleOnlyTaskTitle)

	// The URL deliberately uses mixed-case "JavaScript:" so the assertion
	// also covers the case-insensitivity of the /^https?:\/\//i guard: a
	// non-http(s) scheme must downgrade the title to plain text.
	titleBadURLTaskID := fmt.Sprintf("ui-test-title-bad-url-task-%d", time.Now().UnixNano())
	titleBadURLTaskTitle := "Task with a hostile reference URL"
	titleBadURLTaskURL := "JavaScript:alert(1)"
	titleBadURLTask := &queue.Task{
		ID:     titleBadURLTaskID,
		Prompt: "Render a title whose URL is not http(s)",
		Tags:   []string{"business-default"},
		Title:  titleBadURLTaskTitle,
		URL:    titleBadURLTaskURL,
	}
	if err := srv.TaskQueue().Add(titleBadURLTask); err != nil {
		t.Fatalf("add title+bad-URL task: %v", err)
	}
	if err := srv.TaskQueue().Assign(titleBadURLTaskID, "ui-test-guest"); err != nil {
		t.Fatalf("assign title+bad-URL task: %v", err)
	}
	if err := srv.TaskQueue().Start(titleBadURLTaskID); err != nil {
		t.Fatalf("start title+bad-URL task: %v", err)
	}
	sendLogEntries(t, guestWS, titleBadURLTaskID, testLogEntriesTitleBadURL(titleBadURLTaskID))
	flushAccumulator(t, srv)
	t.Logf("Title+bad-URL task created: %s (title: %s, url: %s)", titleBadURLTaskID, titleBadURLTaskTitle, titleBadURLTaskURL)

	validateUI(t, baseURL, taskID, compactionTaskID, failedTaskID, completedTaskID, pendingTaskID, failureReason, editTaskID, titleUrlTaskID, titleUrlTaskTitle, titleUrlTaskURL, titleOnlyTaskID, titleOnlyTaskTitle, titleBadURLTaskID, titleBadURLTaskTitle, projectRoot)
}

func validateServerLogs(t *testing.T, srv *server.Server, taskID string, expectedEntries []testLogEntry) {
	logStore := srv.LogStore()
	if logStore == nil {
		t.Skip("log store not configured, skipping server-side validation")
	}

	entries := logStore.Get(taskID)

	// Validate structural properties rather than exact entry-by-entry matching.
	// Text deltas are batched by the LogAccumulator, so stored text entries
	// are fewer than the raw capture entries. Thinking deltas are emitted
	// immediately (1:1 with raw deltas). The Playwright test validates the
	// rendered UI; this validates the server-side storage.

	// Count entries by level
	var systemCount, thinkingCount, toolCount, textCount int
	for _, e := range entries {
		switch e.Level {
		case "system":
			systemCount++
		case "thinking":
			thinkingCount++
		case "tool":
			toolCount++
		case "text":
			textCount++
		}
	}

	// System entries should match 1:1 (no batching)
	var expectedSystemCount int
	for _, e := range expectedEntries {
		if e.level == "system" {
			expectedSystemCount++
		}
	}
	if systemCount != expectedSystemCount {
		t.Errorf("expected %d system entries, got %d", expectedSystemCount, systemCount)
	}

	// Thinking entries should match 1:1 (emitted immediately, not batched)
	var expectedThinkingDeltas int
	for _, e := range expectedEntries {
		if e.level == "thinking" {
			expectedThinkingDeltas++
		}
	}
	if thinkingCount != expectedThinkingDeltas {
		t.Errorf("expected %d thinking entries (1:1 with raw deltas), got %d", expectedThinkingDeltas, thinkingCount)
	}
	t.Logf("Thinking: %d entries (1:1 with %d raw deltas)", thinkingCount, expectedThinkingDeltas)

	// Tool entries should match 1:1 (no batching)
	var expectedToolCount int
	for _, e := range expectedEntries {
		if e.level == "tool" {
			expectedToolCount++
		}
	}
	if toolCount != expectedToolCount {
		t.Errorf("expected %d tool entries, got %d", expectedToolCount, toolCount)
	}

	// The main capture contains a tool_execution_update event whose partial
	// result carries text ("devvm\n"). Production converts such events into
	// [TOOL_OUTPUT] entries (pkg/guest/pi_handler.go handles
	// tool_execution_update via pi.ToolPartialResult), so the parsed entries
	// must include at least one output entry — otherwise the UI's
	// tool_type === 'output' handlers go unexercised.
	var outputCount int
	for _, e := range entries {
		if e.Level == "tool" && e.ToolType == "output" {
			outputCount++
		}
	}
	if outputCount == 0 {
		t.Errorf("expected at least one tool output entry (tool_execution_update with partial text), got none")
	}

	// Print summary
	t.Logf("Server log entries: %d total (%d system, %d thinking, %d tool, %d text)",
		len(entries), systemCount, thinkingCount, toolCount, textCount)

	// Validate tool entries have correct structured fields
	toolIdx := 0
	for _, e := range entries {
		if e.Level != "tool" {
			continue
		}
		if toolIdx < len(expectedEntries) && expectedEntries[toolIdx].level == "tool" {
			exp := expectedEntries[toolIdx]
			if e.ToolType != exp.toolType {
				t.Errorf("tool entry: expected tool_type %q, got %q", exp.toolType, e.ToolType)
			}
			if e.ToolName != exp.toolName {
				t.Errorf("tool entry: expected tool_name %q, got %q", exp.toolName, e.ToolName)
			}
		}
		toolIdx++
	}
}

func validateUI(t *testing.T, baseURL, taskID, compactionTaskID, failedTaskID, completedTaskID, pendingTaskID, failureReason, editTaskID, titleUrlTaskID, titleUrlTaskTitle, titleUrlTaskURL, titleOnlyTaskID, titleOnlyTaskTitle, titleBadUrlTaskID, titleBadUrlTaskTitle, projectRoot string) {
	// Allow overriding the screenshot directory via env var for manual inspection.
	// When set, screenshots survive t.TempDir() cleanup.
	overrideDir := os.Getenv("SCREENSHOT_DIR")
	var screenshotDir string
	if overrideDir != "" {
		screenshotDir = overrideDir
		if err := os.MkdirAll(screenshotDir, 0o755); err != nil {
			t.Fatalf("create screenshot dir: %v", err)
		}
	} else {
		screenshotDir = t.TempDir() + "/screenshots"
		if err := os.MkdirAll(screenshotDir, 0o755); err != nil {
			t.Fatalf("create screenshot dir: %v", err)
		}
	}

	// Compute expected log date for assertions.
	// The disk log store writes entries with time.Now(), so the date directory
	// will match today's date in the server's timezone.
	logDate := time.Now().Format("2006-01-02")

	script := fmt.Sprintf(`
const { chromium } = require('playwright');
(async () => {
  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage({ viewport: { width: 1280, height: 900 } });
  const jsErrors = [];
  page.on('pageerror', err => jsErrors.push(err.message));

  const screenshotDir = '%s';
  const baseURL = '%s';
  const taskId = '%s';
  const compactionTaskId = '%s';
  const failedTaskId = '%s';
  const completedTaskId = '%s';
  const pendingTaskId = '%s';
  const failureReason = '%s';
  const editTaskId = '%s';
  const titleUrlTaskId = '%s';
  const titleUrlTaskTitle = '%s';
  const titleUrlTaskUrl = '%s';
  const titleOnlyTaskId = '%s';
  const titleOnlyTaskTitle = '%s';
  const titleBadUrlTaskId = '%s';
  const titleBadUrlTaskTitle = '%s';
  const expectedLogDate = '%s';
  const path = require('path');

  async function takeScreenshot(name) {
    const filePath = path.join(screenshotDir, name + '.png');
    await page.screenshot({ path: filePath, fullPage: true });
    console.log('Screenshot saved:', filePath);
  }

  function fail(msg) {
    console.error('FAIL:', msg);
    throw new Error(msg);
  }

  // Helper: click an element by its selector using native .click().
  // Playwright's page.click() does NOT fire onclick attribute handlers on
  // elements rendered via innerHTML (which is how the log view works).
  // Using native .click() via page.evaluate() fires the handler correctly,
  // exercising the same code path as a real user click.
  async function clickEl(selector) {
    await page.evaluate((sel) => {
      const el = document.querySelector(sel);
      if (!el) throw new Error('element not found: ' + sel);
      el.click();
    }, selector);
  }

  // =====================================================================
  // Phase 1: Front page — Tasks tab
  // =====================================================================
  console.log('=== Phase 1: Tasks tab ===');
  await page.goto(baseURL);
  await page.waitForLoadState('networkidle');

  if (jsErrors.length > 0) {
    console.error('JS errors on page load:', jsErrors);
    await takeScreenshot('00-js-errors');
    process.exit(1);
  }

  // Wait for task list to populate
  await page.waitForSelector('.task-item', { timeout: 10000 });

  // Verify Tasks tab is active by default
  const tasksTabActive = await page.evaluate(() => {
    const tabs = document.querySelectorAll('.tab');
    for (const tab of tabs) {
      if (tab.textContent.trim() === 'Tasks' && tab.classList.contains('active')) {
        return true;
      }
    }
    return false;
  });
  if (!tasksTabActive) fail('Tasks tab should be active on page load');
  console.log('PASS: Tasks tab active on load');

  // Verify task count matches
  const taskCount = await page.$$('.task-item');
  if (taskCount.length === 0) fail('No task items rendered');
  console.log('PASS:', taskCount.length, 'task(s) rendered');

  // Verify stats in header
  const statsOk = await page.evaluate(() => {
    const guestCount = document.getElementById('guest-count').textContent;
    const taskCountEl = document.getElementById('task-count').textContent;
    return guestCount !== '0' && taskCountEl !== '0';
  });
  if (!statsOk) fail('Header stats should show non-zero values');
  console.log('PASS: Header stats populated');

  // =====================================================================
  // Phase 1b: Concurrency header stat (issue #250)
  // =====================================================================
  console.log('=== Phase 1b: Concurrency stat ===');

  // The dashboard polls /api/guests on load, so the concurrency stat must
  // have moved off its placeholder by now (the same style of assertion as
  // the header stats above).
  const concurrencyLive = await page.evaluate(() => ({
    value: document.getElementById('concurrency-cap-input').value,
    label: document.getElementById('concurrency-label').textContent,
  }));
  if (concurrencyLive.value === '') fail('Concurrency cap field should be populated from the /api/guests poll');
  if (!concurrencyLive.label.startsWith('Concurrency')) fail('Concurrency label should start with "Concurrency", got: ' + concurrencyLive.label);
  console.log('PASS: concurrency stat reads', concurrencyLive.value, '\u00b7', concurrencyLive.label);

  // renderConcurrency's three branches, pinned with fixtures: the
  // next_change and waiting_on_cap segments, the unlimited cap, and the
  // absent-payload fallback. The Go tests assert the JSON; only these
  // assertions can fail if the line the operator reads stops composing.
  const concurrencyFixtures = await page.evaluate(() => {
    const read = () => ({
      value: document.getElementById('concurrency-cap-input').value,
      label: document.getElementById('concurrency-label').textContent,
    });
    const out = [];
    renderConcurrency({ cap: 2, in_flight: 1, waiting_on_cap: 3, next_change: { at: '23:00', cap: 4 } });
    out.push(read());
    renderConcurrency({ cap: 'unlimited', in_flight: 0, waiting_on_cap: 0, next_change: null });
    out.push(read());
    renderConcurrency(undefined);
    out.push(read());
    return out;
  });
  const expectStat = (fixture, value, label, what) => {
    if (fixture.value !== value) fail(what + ': expected cap field "' + value + '", got "' + fixture.value + '"');
    if (fixture.label !== label) fail(what + ': expected label "' + label + '", got "' + fixture.label + '"');
  };
  expectStat(concurrencyFixtures[0], '2', 'Concurrency \u00b7 1 in flight \u00b7 next 23:00 \u2192 4 \u00b7 3 waiting on cap', 'scheduled fixture');
  expectStat(concurrencyFixtures[1], 'unlimited', 'Concurrency \u00b7 0 in flight', 'unlimited fixture');
  expectStat(concurrencyFixtures[2], '', 'Concurrency', 'absent-payload fixture');
  console.log('PASS: renderConcurrency pins all three branches');

  // The cap field is editable (issue #264): typing a number and clicking
  // Set must move the enforced number — proven against /api/health, the
  // surface that states the cap the next assignment decision obeys. The
  // press is the interaction a human performs — focus the field, type
  // with real keystrokes, click the button with the mouse — because a
  // real click moves focus before it lands, and a DOM-level .click() on
  // an unfocused field would stay green even when that sequence posts
  // the stale cap (issue #264 review). Then back to "unlimited", which
  // is what this server's config resolves to and what the rest of this
  // test expects.
  const capInput = page.locator('#concurrency-cap-input');
  const capSet = page.locator('.concurrency-edit button');
  // typeCapField is the one real typing interaction — focus the field,
  // select all, delete, type — that every block below shares; typeAndSet
  // is that plus the Set click (issue #264 review: an interaction
  // hand-copied per block is how a shape nobody can perform creeps back
  // into the assertions).
  const typeCapField = async (text) => {
    await capInput.click();
    await page.keyboard.press('Control+a');
    await page.keyboard.press('Delete');
    await page.keyboard.type(text);
  };
  const typeAndSet = async (text) => {
    await typeCapField(text);
    await capSet.click();
    await page.waitForTimeout(400);
  };
  const healthCap = async () => await page.evaluate(async () => (await (await fetch('/api/health')).json()).concurrency_cap);
  await typeAndSet('1');
  const cap1 = await page.evaluate(async () => ({
    health: (await (await fetch('/api/health')).json()).concurrency_cap,
    label: document.getElementById('concurrency-label').textContent,
  }));
  if (cap1.health !== 1) fail('typing 1 and clicking Set should put 1 on /api/health, got ' + JSON.stringify(cap1.health));
  if (!cap1.label.includes('Concurrency')) fail('the label should still describe the concurrency picture after an update, got: ' + cap1.label);
  await typeAndSet('-1');
  if (await healthCap() !== 'unlimited') fail('a typed -1 lifts the cap server-side; the field posts it verbatim');
  await typeAndSet('unlimited');
  if (await healthCap() !== 'unlimited') fail('typing "unlimited" and clicking Set should lift the cap on /api/health');
  if (await capInput.inputValue() !== 'unlimited') fail('the field should show the cap the server now states, got: ' + await capInput.inputValue());
  console.log('PASS: the cap field updates the live concurrency cap end to end');

  // The mid-typing guard (issue #264): the polled refresh must not
  // clobber a value the operator is typing. Type into the focused field
  // as a user would, force the refresh that the header polls run, and
  // assert the field still holds what was typed. Without the guard in
  // renderConcurrency this fails.
  await typeCapField('5');
  const midTyping = await page.evaluate(async () => {
    await refresh();
    const el = document.getElementById('concurrency-cap-input');
    return {
      value: el.value,
      focused: document.activeElement === el,
    };
  });
  if (!midTyping.focused) fail('the cap field should still be focused after typing into it');
  if (midTyping.value !== '5') fail('a polled refresh must not clobber a cap being typed, field now reads: ' + midTyping.value);
  console.log('PASS: the polled refresh leaves a mid-typing cap field alone');

  // Refusals come from the server, which owns the cap grammar: invalid
  // text — and a blank field — alerts with the server's 400 and posts no
  // change. /api/health is the arbiter: the enforced cap must not move.
  // The dialog listener captures the alert instead of letting it block
  // the script.
  const capAlerts = [];
  const capDialogHandler = dialog => { capAlerts.push(dialog.message()); dialog.dismiss(); };
  page.on('dialog', capDialogHandler);
  const invalidCaps = await page.evaluate(async () => {
    const el = document.getElementById('concurrency-cap-input');
    const settle = () => new Promise(r => setTimeout(r, 400));
    const health = async () => (await (await fetch('/api/health')).json()).concurrency_cap;
    const before = await health();
    el.value = 'banana';
    document.querySelector('.concurrency-edit button').click();
    await settle();
    const afterBanana = await health();
    el.value = '';
    document.querySelector('.concurrency-edit button').click();
    await settle();
    const afterBlank = await health();
    return { before: before, afterBanana: afterBanana, afterBlank: afterBlank };
  });
  page.off('dialog', capDialogHandler);
  if (invalidCaps.afterBanana !== invalidCaps.before) fail('"banana" must alert and leave the cap untouched, health moved ' + invalidCaps.before + ' to ' + invalidCaps.afterBanana);
  if (invalidCaps.afterBlank !== invalidCaps.before) fail('a blank field must alert and leave the cap untouched, health moved ' + invalidCaps.before + ' to ' + invalidCaps.afterBlank);
  if (capAlerts.length !== 2) fail('each invalid cap value should alert once, saw ' + capAlerts.length + ' dialogs: ' + JSON.stringify(capAlerts));
  if (!capAlerts[0].includes('oncurrency cap')) fail('the alert should name the concurrency cap, got: ' + capAlerts[0]);
  console.log('PASS: invalid cap values alert and leave the live cap untouched');

  // The field must not keep displaying a value that was never applied:
  // the poll leaves a mid-typing field alone, but leaving focus restores
  // the enforced cap, and Enter applies the value (issue #264 review).
  // Without the blur restore this field would still read 9 while
  // /api/health says unlimited; without the Enter binding, Enter does
  // nothing.
  await typeCapField('9');
  const capBlur = await page.evaluate(async () => {
    document.getElementById('concurrency-cap-input').blur();
    const health = (await (await fetch('/api/health')).json()).concurrency_cap;
    return { shown: document.getElementById('concurrency-cap-input').value, health: health };
  });
  if (capBlur.shown !== String(capBlur.health)) fail('a blurred cap field must show the enforced cap again, field: ' + capBlur.shown + ', health: ' + JSON.stringify(capBlur.health));
  console.log('PASS: a blurred cap field shows the enforced cap again');

  // Enter in the field applies the value: the same contract the Set
  // button has, proven against /api/health, then restored the same way.
  await typeCapField('3');
  await page.keyboard.press('Enter');
  const capEnter = await page.evaluate(async () => {
    await new Promise(r => setTimeout(r, 400));
    return {
      health: (await (await fetch('/api/health')).json()).concurrency_cap,
      shown: document.getElementById('concurrency-cap-input').value,
    };
  });
  if (capEnter.health !== 3) fail('Enter in the cap field should apply the value, /api/health states ' + JSON.stringify(capEnter.health));
  if (capEnter.shown !== '3') fail('the field should show the applied cap even while focused, got ' + capEnter.shown);
  await typeCapField('unlimited');
  await page.keyboard.press('Enter');
  const capRestored = await page.evaluate(async () => {
    await new Promise(r => setTimeout(r, 400));
    return (await (await fetch('/api/health')).json()).concurrency_cap;
  });
  if (capRestored !== 'unlimited') fail('Enter should restore the unlimited cap, got ' + JSON.stringify(capRestored));
  await page.evaluate(() => document.activeElement.blur());
  console.log('PASS: Enter applies the cap field end to end');

  // The WebKit focus shape (issue #264 review): clicking the Set button
  // can drop focus to the body instead of moving it into the button
  // (https://bugs.webkit.org/show_bug.cgi?id=229895), so focusout
  // arrives with relatedTarget === null and a restore keyed on that
  // alone fires before the click posts what was typed. Chromium never
  // produces this order, so pin the shape by dispatching the events in
  // the order WebKit does: mousedown on the button, focusout with a
  // null relatedTarget on the field, then the click. The button's
  // mousedown must mark the submit as intended so the restore stands
  // down. This fails against markup that reads intent off the focus
  // event alone.
  await typeCapField('7');
  await capSet.dispatchEvent('mousedown');
  await capInput.dispatchEvent('focusout', { relatedTarget: null });
  await capSet.dispatchEvent('click');
  const capRebound = await page.evaluate(async () => {
    await new Promise(r => setTimeout(r, 400));
    return {
      health: (await (await fetch('/api/health')).json()).concurrency_cap,
      shown: document.getElementById('concurrency-cap-input').value,
    };
  });
  if (capRebound.health !== 7) fail('a WebKit-shaped Set click (focusout with a null relatedTarget) must still post the typed cap; /api/health states ' + JSON.stringify(capRebound.health));
  if (capRebound.shown !== '7') fail('the field should show the applied cap after a WebKit-shaped Set click, got ' + capRebound.shown);
  console.log('PASS: the Set button submits on mousedown intent, not on focus-event geography');

  // An abandoned Set press (issue #264 review): a mousedown that never
  // becomes a click — the operator presses the button, drags off and
  // releases — must not leave the submit mark armed, because an armed
  // mark switches the blur restore off on every later blur and the
  // field would then show a typed-but-never-applied cap again. The
  // field's own focus event clears the mark, and the gesture that
  // produces a typed value always starts by focusing the field — so
  // blur, refocus, blur after the abandoned press must put the
  // enforced number back. This fails against a mark only a successful
  // Set can clear.
  await typeCapField('8');
  await capSet.dispatchEvent('mousedown');
  const capAbandoned = await page.evaluate(async () => {
    const el = document.getElementById('concurrency-cap-input');
    el.blur();
    el.focus();
    el.blur();
    const health = (await (await fetch('/api/health')).json()).concurrency_cap;
    return { shown: el.value, health: health };
  });
  if (capAbandoned.shown !== String(capAbandoned.health)) fail('an abandoned Set press (mousedown without click) must not leave the blur restore disabled, field: ' + capAbandoned.shown + ', health: ' + JSON.stringify(capAbandoned.health));
  console.log('PASS: an abandoned Set press leaves the blur restore working for the next blur');
  await typeAndSet('unlimited');

  // Restore the live picture before any later screenshots (blur first:
  // a focused field is deliberately not refreshed by the poll).
  await page.evaluate(() => document.activeElement.blur());
  await page.evaluate(async () => { await refresh(); });

  // Verify the tab set is Tasks + Logs + Search and nothing else (the task
  // detail view is a state of the Logs tab, never a tab of its own; the
  // Search tab is issue #270).
  const tabs = await page.evaluate(() => Array.from(document.querySelectorAll('.tab')).map(t => t.textContent.trim()));
  if (tabs.length !== 3) fail('Should have exactly 3 tabs (Tasks + Logs + Search), got ' + tabs.join(', '));
  if (tabs.join(',') !== 'Tasks,Logs,Search') fail('Unexpected tab set: ' + tabs.join(','));
  console.log('PASS: Tabs are Tasks, Logs and Search');

  // =====================================================================
  // Phase 1a: Scroll fix — html/body must NOT have overflow:hidden
  // =====================================================================
  console.log('=== Phase 1a: Scroll fix ===');

  // Verify that html and body do NOT have overflow:hidden.
  // overflow:hidden on the root elements prevents scroll events from
  // reaching child containers, making the UI impossible to scroll.
  const scrollFixOk = await page.evaluate(() => {
    const htmlStyle = window.getComputedStyle(document.documentElement);
    const bodyStyle = window.getComputedStyle(document.body);
    return {
      htmlOverflow: htmlStyle.overflow,
      bodyOverflow: bodyStyle.overflow,
    };
  });
  if (scrollFixOk.htmlOverflow === 'hidden') fail('html must NOT have overflow:hidden (blocks scrolling)');
  if (scrollFixOk.bodyOverflow === 'hidden') fail('body must NOT have overflow:hidden (blocks scrolling)');
  console.log('PASS: html overflow is', scrollFixOk.htmlOverflow, '(not hidden)');
  console.log('PASS: body overflow is', scrollFixOk.bodyOverflow, '(not hidden)');

  // Verify that scrollable containers have overflow-y:auto (or scroll).
  // These are the elements that should handle scrolling.
  const scrollableContainers = await page.evaluate(() => {
    const results = {};
    const selectors = {
      sidebar: '.sidebar',
      taskList: '#task-list',
      taskDetailBody: '.task-detail-body',
      tabTasks: '#tab-tasks',
    };
    for (const [name, sel] of Object.entries(selectors)) {
      const el = document.querySelector(sel);
      if (el) {
        const style = window.getComputedStyle(el);
        results[name] = {
          found: true,
          overflowY: style.overflowY,
          scrollHeight: el.scrollHeight,
          clientHeight: el.clientHeight,
        };
      } else {
        results[name] = { found: false };
      }
    }
    return results;
  });

  // Sidebar should be present and scrollable
  if (!scrollableContainers.sidebar.found) fail('.sidebar not found');
  if (scrollableContainers.sidebar.overflowY !== 'auto' && scrollableContainers.sidebar.overflowY !== 'scroll') {
    fail('.sidebar should have overflow-y:auto or scroll, got ' + scrollableContainers.sidebar.overflowY);
  }
  console.log('PASS: .sidebar has overflow-y:', scrollableContainers.sidebar.overflowY);

  // Task list should be present and scrollable
  if (!scrollableContainers.taskList.found) fail('#task-list not found');
  if (scrollableContainers.taskList.overflowY !== 'auto' && scrollableContainers.taskList.overflowY !== 'scroll') {
    fail('#task-list should have overflow-y:auto or scroll, got ' + scrollableContainers.taskList.overflowY);
  }
  console.log('PASS: #task-list has overflow-y:', scrollableContainers.taskList.overflowY);

  // tab-tasks container should be present and scrollable (Issue #42 fix).
  // The parent tab container must NOT have overflow:hidden as that clips
  // the scrollable child elements and prevents scrolling.
  if (!scrollableContainers.tabTasks.found) fail('#tab-tasks not found');
  if (scrollableContainers.tabTasks.overflowY === 'hidden') {
    fail('#tab-tasks must NOT have overflow:hidden (clips scrollable children), got overflow-y: hidden');
  }
  if (scrollableContainers.tabTasks.overflowY !== 'auto' && scrollableContainers.tabTasks.overflowY !== 'scroll') {
    fail('#tab-tasks should have overflow-y:auto or scroll, got ' + scrollableContainers.tabTasks.overflowY);
  }
  console.log('PASS: #tab-tasks has overflow-y:', scrollableContainers.tabTasks.overflowY);

  // task-detail-body may not be visible yet (we're on Tasks tab), but check
  // the CSS rule is correct by inspecting the element's style attribute.
  if (scrollableContainers.taskDetailBody.found) {
    if (scrollableContainers.taskDetailBody.overflowY !== 'auto' && scrollableContainers.taskDetailBody.overflowY !== 'scroll') {
      fail('.task-detail-body should have overflow-y:auto or scroll, got ' + scrollableContainers.taskDetailBody.overflowY);
    }
    console.log('PASS: .task-detail-body has overflow-y:', scrollableContainers.taskDetailBody.overflowY);
  }

  // Verify the tab-tasks container can actually be scrolled (even if content fits,
  // the element should respond to scroll commands).
  const tabTasksScrollable = await page.evaluate(() => {
    const el = document.querySelector('#tab-tasks');
    if (!el) return { error: 'not found' };
    const before = el.scrollTop;
    el.scrollTop = 99999; // Scroll to bottom
    const after = el.scrollTop;
    return {
      acceptsScroll: true,
      scrollTopBefore: before,
      scrollTopAfter: after,
      scrollHeight: el.scrollHeight,
      clientHeight: el.clientHeight,
      canScroll: el.scrollHeight > el.clientHeight,
    };
  });
  if (tabTasksScrollable.error) fail(tabTasksScrollable.error);
  console.log('PASS: #tab-tasks accepts scroll commands (scrollTop: ' + tabTasksScrollable.scrollTopBefore + ' -> ' + tabTasksScrollable.scrollTopAfter + ', canScroll: ' + tabTasksScrollable.canScroll + ')');

  // Verify the task list can actually be scrolled (even if content fits,
  // the element should respond to scroll commands).
  const taskListScrollable = await page.evaluate(() => {
    const el = document.querySelector('#task-list');
    if (!el) return { error: 'not found' };
    const before = el.scrollTop;
    el.scrollTop = 99999; // Scroll to bottom
    const after = el.scrollTop;
    // scrollTop is always 0 when content fits, but the key is that
    // setting scrollTop doesn't throw and the element accepts scroll.
    // When content exceeds viewport, after > before proves scrolling works.
    return {
      acceptsScroll: true,
      scrollTopBefore: before,
      scrollTopAfter: after,
      scrollHeight: el.scrollHeight,
      clientHeight: el.clientHeight,
      canScroll: el.scrollHeight > el.clientHeight,
    };
  });
  if (taskListScrollable.error) fail(taskListScrollable.error);
  console.log('PASS: #task-list accepts scroll commands (scrollTop: ' + taskListScrollable.scrollTopBefore + ' -> ' + taskListScrollable.scrollTopAfter + ', canScroll: ' + taskListScrollable.canScroll + ')');

  await takeScreenshot('01a-scroll-fix');

  // =====================================================================
  // Phase 1b: Task summary and filter row
  // =====================================================================
  console.log('=== Phase 1b: Task summary and filter ===');

  // Verify task summary is rendered with coloured counts
  const summaryResult = await page.evaluate(() => {
    const summary = document.getElementById('task-summary');
    if (!summary) return { error: 'task-summary element not found' };
    const counts = summary.querySelectorAll('.task-summary-count');
    const countValues = Array.from(counts).map(c => c.textContent.trim());
    const countClasses = Array.from(counts).map(c => {
      const classes = c.className.split(' ');
      return classes.find(cls => cls !== 'task-summary-count');
    });
    return { countValues, countClasses };
  });
  if (summaryResult.error) fail(summaryResult.error);
  if (summaryResult.countValues.length < 4) fail('Task summary should have at least 4 count values, got ' + summaryResult.countValues.length);
  // First count is total, should be non-zero
  if (summaryResult.countValues[0] === '0') fail('Total task count should be non-zero');
  if (summaryResult.countClasses[0] !== 'total') fail('First summary count should have class "total"');
  console.log('PASS: Task summary rendered with', summaryResult.countValues.length, 'counts:', summaryResult.countValues.join(', '));

  // Verify filter row is rendered with toggle buttons
  const filterResult = await page.evaluate(() => {
    const row = document.getElementById('task-filter-row');
    if (!row) return { error: 'task-filter-row element not found' };
    const buttons = row.querySelectorAll('.task-filter-btn');
    const buttonStates = Array.from(buttons).map(b => ({
      status: b.dataset.status,
      active: b.classList.contains('active'),
      inactive: b.classList.contains('inactive'),
    }));
    return { buttonStates };
  });
  if (filterResult.error) fail(filterResult.error);
  if (filterResult.buttonStates.length < 6) fail('Filter row should have at least 6 buttons, got ' + filterResult.buttonStates.length);
  // Verify COMPLETED, PENDING and CANCELLED are inactive by default
  for (const hiddenStatus of ['COMPLETED', 'PENDING', 'CANCELLED']) {
    const btn = filterResult.buttonStates.find(b => b.status === hiddenStatus);
    if (!btn) fail(hiddenStatus + ' filter button not found');
    if (btn.active) fail(hiddenStatus + ' filter should be inactive by default');
    if (!btn.inactive) fail(hiddenStatus + ' filter should have inactive class by default');
  }
  // Verify other statuses are active by default
  for (const btn of filterResult.buttonStates) {
    if (btn.status !== 'COMPLETED' && btn.status !== 'PENDING' && btn.status !== 'CANCELLED' && !btn.active) {
      fail(btn.status + ' filter should be active by default');
    }
  }
  console.log('PASS: Filter row rendered with', filterResult.buttonStates.length, 'buttons, COMPLETED, PENDING and CANCELLED inactive by default');

  // Verify pending and completed tasks are hidden by default
  const visibleTaskCount = await page.evaluate(() => {
    return document.querySelectorAll('.task-item').length;
  });
  // With 9 tasks (1 RUNNING + 1 FAILED + 1 PENDING + 1 COMPLETED + 1 RUNNING with persona
  // + 1 RUNNING with compaction + 1 RUNNING with title+URL + 1 RUNNING with title only
  // + 1 RUNNING with title+non-http(s) URL)
  // and PENDING/COMPLETED hidden, only 7 should be visible
  if (visibleTaskCount !== 7) fail('Should show 7 tasks (PENDING and COMPLETED hidden by default), got ' + visibleTaskCount);
  console.log('PASS:', visibleTaskCount, 'task(s) visible with default filter (PENDING and COMPLETED hidden)');

  // Verify the "N pending" expand button is visible
  const pendingBtnResult = await page.evaluate(() => {
    const btn = document.querySelector('.pending-expand-btn');
    if (!btn) return { found: false };
    return { found: true, text: btn.textContent.trim() };
  });
  if (!pendingBtnResult.found) fail('"N pending" expand button should be visible when pending tasks are hidden');
  if (!pendingBtnResult.text.includes('pending')) fail('Pending button should contain "pending" text, got: ' + pendingBtnResult.text);
  if (!pendingBtnResult.text.includes('1')) fail('Pending button should show count 1, got: ' + pendingBtnResult.text);
  console.log('PASS: "N pending" button visible:', pendingBtnResult.text);

  // Verify log count is displayed in task meta line
  const logCountResult = await page.evaluate(() => {
    const items = document.querySelectorAll('.task-item');
    const results = [];
    for (const item of items) {
      const meta = item.querySelector('.task-meta');
      if (!meta) continue;
      const metaText = meta.textContent;
      const match = metaText.match(/(\d+)\s+logs?/);
      results.push({
        hasLogCount: !!match,
        count: match ? parseInt(match[1], 10) : -1,
        metaText: metaText,
      });
    }
    return results;
  });
  if (logCountResult.length === 0) fail('No task items found to check log count');
  for (const r of logCountResult) {
    if (!r.hasLogCount) fail('Task meta should contain log count, got: ' + r.metaText);
    if (r.count < 0) fail('Log count should be non-negative, got: ' + r.count);
  }
  // The RUNNING task should have logs (from testLogEntries), the FAILED task should too
  const runningTaskHasLogs = logCountResult.some(r => r.count > 0);
  if (!runningTaskHasLogs) fail('At least one visible task should have log entries > 0');
  console.log('PASS: Log counts displayed in task meta:', logCountResult.map(r => r.count + ' logs').join(', '));

  // Hourglass spinner for running tasks (issues #210, #246): each visible
  // task card shows an ⏳ next to its log count exactly while the task is
  // running. The overview angle is no longer keyed off the log count
  // (issue #246): it advances while the count changes between updates, so
  // the assertion here only checks the transform is a rotation.
  const spinnerListResult = await page.evaluate(() => {
    const items = [];
    for (const item of document.querySelectorAll('.task-item')) {
      const meta = item.querySelector('.task-meta');
      const badge = item.querySelector('.badge');
      const sp = meta ? meta.querySelector('.task-spinner') : null;
      const match = meta ? meta.textContent.match(/(\d+)\s+logs?/) : null;
      items.push({
        status: badge ? badge.textContent.trim() : '',
        count: match ? parseInt(match[1], 10) : -1,
        hasSpinner: !!sp,
        transform: sp ? sp.style.transform : null,
        glyph: sp ? sp.textContent : null,
      });
    }
    return items;
  });
  for (const r of spinnerListResult) {
    const running = r.status === 'RUNNING' || r.status === 'ASSIGNED';
    if (running && !r.hasSpinner) fail('Running task card (' + r.status + ') should show the ⏳ spinner');
    if (!running && r.hasSpinner) fail('Non-running task card (' + r.status + ') should not show a spinner');
    if (r.hasSpinner) {
      if (r.glyph !== '⏳') fail('Spinner glyph should be ⏳, got: ' + r.glyph);
      if (!/^rotate\(\d+(\.\d+)?deg\)$/.test(r.transform || '')) fail('Spinner rotation should be a rotate(Ndeg) transform, got: ' + r.transform);
    }
  }
  console.log('PASS: ⏳ spinner on task cards is keyed off status and log count');

  // =====================================================================
  // Phase 1c: Persona badge colours
  // =====================================================================
  console.log('=== Phase 1c: Persona badge colours ===');

  const personaResult = await page.evaluate(() => {
    const items = document.querySelectorAll('.task-item');
    const results = [];
    for (const item of items) {
      const badge = item.querySelector('.persona-badge');
      if (!badge) continue;
      results.push({
        id: item.textContent.substring(0, 40),
        className: badge.className,
        text: badge.textContent.trim(),
        hasColourClass: /persona-badge-[0-7]/.test(badge.className),
      });
    }
    return results;
  });

  if (personaResult.length === 0) fail('No persona badges found in task list');
  for (const p of personaResult) {
    if (!p.hasColourClass) fail('Persona badge should have a colour class, got: ' + p.className);
    console.log('PASS: Persona badge colour class for', p.id, ':', p.className);
  }

  // =====================================================================
  // Phase 1e: Task title rendering in the overview list (issue #229)
  // =====================================================================
  console.log('=== Phase 1e: Task titles in overview ===');

  const titleListResult = await page.evaluate((d) => {
    const itemFor = (tid) => document.querySelector('.task-item[onclick*="' + tid + '"]');
    const out = {};
    const urlItem = itemFor(d.titleUrlTaskId);
    if (urlItem) {
      const el = urlItem.querySelector('.task-title');
      const a = el ? el.querySelector('a') : null;
      const cs = el ? getComputedStyle(el) : null;
      const idLine = urlItem.querySelector('.task-id');
      out.url = {
        found: true,
        text: el ? el.textContent : null,
        hasAnchor: !!a,
        href: a ? a.getAttribute('href') : null,
        target: a ? a.getAttribute('target') : null,
        rel: a ? a.getAttribute('rel') : null,
        titleAttr: el ? el.getAttribute('title') : null,
        noRawHTML: el ? el.querySelector('b, script') === null : false,
        beforeId: idLine ? idLine.textContent.indexOf(el.textContent) < idLine.textContent.indexOf(d.titleUrlTaskId) : false,
        overflow: cs ? cs.overflow : null,
        textOverflow: cs ? cs.textOverflow : null,
        whiteSpace: cs ? cs.whiteSpace : null,
      };
    } else { out.url = { found: false }; }
    const onlyItem = itemFor(d.titleOnlyTaskId);
    if (onlyItem) {
      const el = onlyItem.querySelector('.task-title');
      out.only = {
        found: true,
        text: el ? el.textContent : null,
        hasAnchor: !!el && el.querySelector('a') !== null,
        titleAttr: el ? el.getAttribute('title') : null,
      };
    } else { out.only = { found: false }; }
    const badItem = itemFor(d.titleBadUrlTaskId);
    if (badItem) {
      const el = badItem.querySelector('.task-title');
      out.bad = {
        found: true,
        text: el ? el.textContent : null,
        hasAnchor: !!el && el.querySelector('a') !== null,
        hasAnchorAnywhere: badItem.querySelector('a[href^="java"]') !== null,
      };
    } else { out.bad = { found: false }; }
    const plainItem = itemFor(d.plainTaskId);
    out.plain = {
      found: !!plainItem,
      hasTitleEl: !!plainItem && plainItem.querySelector('.task-title') !== null,
      startsWithId: !!plainItem && plainItem.querySelector('.task-id').textContent.trim().startsWith(d.plainTaskId),
    };
    return out;
  }, { titleUrlTaskId, titleUrlTaskTitle, titleOnlyTaskId, titleBadUrlTaskId, plainTaskId: taskId });

  if (!titleListResult.url.found) fail('title+URL task row not found in task list');
  const tu = titleListResult.url;
  if (tu.text !== titleUrlTaskTitle) fail('overview title text should equal the raw title, got: ' + tu.text);
  if (!tu.hasAnchor) fail('title with a URL should render as an anchor in the overview');
  if (tu.href !== titleUrlTaskUrl) fail('title anchor href should be the task URL, got: ' + tu.href);
  if (tu.target !== '_blank') fail('title anchor should open in a new tab (target=_blank), got: ' + tu.target);
  if (!tu.rel || !tu.rel.includes('noopener')) fail('title anchor rel should include noopener, got: ' + tu.rel);
  if (tu.titleAttr !== titleUrlTaskTitle) fail('title element should expose the full title via the title attribute, got: ' + tu.titleAttr);
  if (!tu.noRawHTML) fail('title must be rendered as text, not raw HTML');
  if (!tu.beforeId) fail('title should render in front of the task ID');
  if (tu.overflow !== 'hidden' || tu.textOverflow !== 'ellipsis' || tu.whiteSpace !== 'nowrap') {
    fail('overview title should truncate with an ellipsis (got ' + tu.overflow + '/' + tu.textOverflow + '/' + tu.whiteSpace + ')');
  }
  if (!titleListResult.only.found) fail('title-only task row not found in task list');
  if (titleListResult.only.text !== titleOnlyTaskTitle) fail('title-only overview text mismatch, got: ' + titleListResult.only.text);
  if (titleListResult.only.hasAnchor) fail('title without a URL must render as plain text, not a link');
  if (titleListResult.only.titleAttr !== titleOnlyTaskTitle) fail('title-only element should expose the full title via the title attribute');
  if (!titleListResult.bad.found) fail('title+bad-URL task row not found in task list');
  if (titleListResult.bad.text !== titleBadUrlTaskTitle) fail('title+bad-URL overview text mismatch, got: ' + titleListResult.bad.text);
  if (titleListResult.bad.hasAnchor) fail('non-http(s) URL must be downgraded to plain text, not rendered as a link');
  if (titleListResult.bad.hasAnchorAnywhere) fail('no anchor with a javascript: href may appear in the task row');
  if (!titleListResult.plain.found) fail('untitled task row not found in task list');
  if (titleListResult.plain.hasTitleEl) fail('task with no title must not render a .task-title element');
  if (!titleListResult.plain.startsWithId) fail('untitled task row should lead with the ID exactly as before');
  console.log('PASS: overview titles render ahead of the ID — link, plain text, downgraded-scheme, and absent variants all correct');

  await takeScreenshot('01-front-page');

  // =====================================================================
  // Phase 1d: "N pending" expand button — pending tasks hidden/shown
  // =====================================================================
  console.log('=== Phase 1d: Pending expand button ===');

  // With 9 tasks (RUNNING + FAILED + PENDING + COMPLETED + RUNNING-with-persona
  // + RUNNING-with-compaction + title+URL + title-only + title+non-http(s)-URL)
  // and PENDING/COMPLETED hidden, only 7 should be visible
  const visibleBefore = await page.evaluate(() => {
    return document.querySelectorAll('.task-item').length;
  });
  if (visibleBefore !== 7) fail('Should show 7 tasks (PENDING and COMPLETED hidden by default), got ' + visibleBefore);
  console.log('PASS:', visibleBefore, 'tasks visible (PENDING and COMPLETED hidden)');

  // Verify the pending task is NOT in the list
  const pendingHidden = await page.evaluate((pid) => {
    const items = document.querySelectorAll('.task-item');
    for (const item of items) {
      if (item.textContent.includes(pid)) return false;
    }
    return true;
  }, pendingTaskId);
  if (!pendingHidden) fail('Pending task should be hidden by default');
  console.log('PASS: Pending task hidden by default');

  // Click the "N pending" expand button
  await clickEl('.pending-expand-btn');

  // Wait for re-render
  await page.waitForTimeout(500);

  // Now 8 tasks should be visible (everything except COMPLETED)
  const visibleAfter = await page.evaluate(() => {
    return document.querySelectorAll('.task-item').length;
  });
  if (visibleAfter !== 8) fail('Should show 8 tasks (PENDING now shown), got ' + visibleAfter);
  console.log('PASS:', visibleAfter, 'tasks visible (PENDING shown)');

  // Verify the pending task IS now in the list
  const pendingShown = await page.evaluate((pid) => {
    const items = document.querySelectorAll('.task-item');
    for (const item of items) {
      if (item.textContent.includes(pid)) return true;
    }
    return false;
  }, pendingTaskId);
  if (!pendingShown) fail('Pending task should be visible after clicking expand button');
  console.log('PASS: Pending task visible after expanding');

  // Verify the pending task shows "0 logs" (it has no log entries)
  const pendingLogCount = await page.evaluate((pid) => {
    const items = document.querySelectorAll('.task-item');
    for (const item of items) {
      if (item.textContent.includes(pid)) {
        const meta = item.querySelector('.task-meta');
        if (meta) {
          const match = meta.textContent.match(/(\d+)\s+logs?/);
          if (match) return parseInt(match[1], 10);
        }
      }
    }
    return -1;
  }, pendingTaskId);
  if (pendingLogCount !== 0) fail('Pending task should show 0 logs, got: ' + pendingLogCount);
  console.log('PASS: Pending task shows 0 logs');

  // Verify the "N pending" button is gone (no more hidden pending tasks)
  const pendingBtnGone = await page.evaluate(() => {
    return document.querySelector('.pending-expand-btn') === null;
  });
  if (!pendingBtnGone) fail('"N pending" button should disappear after expanding');
  console.log('PASS: "N pending" button disappeared after expanding');

  // Verify PENDING filter button is now active
  const pendingBtnActive = await page.evaluate(() => {
    const btn = document.querySelector('button[data-status="PENDING"]');
    return btn && btn.classList.contains('active') && !btn.classList.contains('inactive');
  });
  if (!pendingBtnActive) fail('PENDING filter button should be active after expanding');
  console.log('PASS: PENDING filter button active after expanding');

  // Toggle PENDING filter button back to hide pending tasks
  await clickEl('button[data-status="PENDING"]');
  await page.waitForTimeout(500);

  // Back to 7 visible tasks, "N pending" button reappears
  const visibleFinal = await page.evaluate(() => {
    return document.querySelectorAll('.task-item').length;
  });
  if (visibleFinal !== 7) fail('Should show 7 tasks (PENDING hidden again), got ' + visibleFinal);
  console.log('PASS:', visibleFinal, 'tasks visible (PENDING hidden again)');

  const pendingBtnReappeared = await page.evaluate(() => {
    return document.querySelector('.pending-expand-btn') !== null;
  });
  if (!pendingBtnReappeared) fail('"N pending" button should reappear when PENDING filter is toggled off');
  console.log('PASS: "N pending" button reappeared');

  await takeScreenshot('01b-pending-expand');

  // =====================================================================
  // Phase 2: Click task → Logs view with rich tool-block rendering
  // =====================================================================
  console.log('=== Phase 2: Click task → Logs view ===');
  // Must use clickEl() — page.click() does NOT fire onclick attribute
  // handlers on elements rendered via innerHTML (which is how task items work).
  await clickEl('.task-item');
  await page.waitForSelector('.task-detail-body .tool-block', { timeout: 5000 });

  // Verify URL updated to /task/:id (kept for backwards compatibility)
  const detailUrl = await page.evaluate(() => window.location.pathname);
  if (!detailUrl.startsWith('/task/')) fail('URL should start with /task/, got: ' + detailUrl);
  console.log('PASS: URL updated to', detailUrl);

  // Verify Logs tab is active
  const logsTabActive = await page.evaluate(() => {
    const tabs = document.querySelectorAll('.tab');
    for (const tab of tabs) {
      if (tab.textContent.trim() === 'Logs' && tab.classList.contains('active')) {
        return true;
      }
    }
    return false;
  });
  if (!logsTabActive) fail('Logs tab should be active after clicking task');
  console.log('PASS: Logs tab active');

  // Verify task detail header is present (no breadcrumb for task detail view)
  const headerOk = await page.evaluate(() => {
    return document.querySelector('.task-detail-header') !== null;
  });
  if (!headerOk) fail('Task detail header missing');
  console.log('PASS: Task detail header rendered');

  await takeScreenshot('02-logs-task-detail');

  // =====================================================================
  // Phase 3: DOM validation of logs view (tool blocks, log messages)
  // =====================================================================
  console.log('=== Phase 3: Logs view DOM validation ===');

  const detailResult = await page.evaluate(() => {
    const body = document.querySelector('.task-detail-body');
    if (!body) return { error: 'no .task-detail-body found' };

    const toolBlocks = body.querySelectorAll('.tool-block');
    const thinkingBlocks = body.querySelectorAll('.thinking-block');
    const logMsgs = body.querySelectorAll(':scope > .log-msg');

    // Check that no plain-text log-msg contains tool markers
    const toolMarkerLines = ['[TOOL_OUTPUT]', '[TOOL_END]', '[TOOL_START]'];
    const hasToolMarkersOutsideBlocks = Array.from(logMsgs).some(m =>
      toolMarkerLines.some(marker => m.textContent.includes(marker))
    );

    return {
      toolBlockCount: toolBlocks.length,
      toolBlockIds: Array.from(toolBlocks).map(b => b.id),
      toolStatuses: Array.from(toolBlocks).map(b => b.querySelector('.tool-status')?.textContent?.trim()),
      toolNames: Array.from(toolBlocks).map(b => b.querySelector('.tool-name')?.textContent?.trim()),
      toolOutputsHavePre: Array.from(toolBlocks).map(b => b.querySelector('.tool-output pre') !== null),
      toolOutputContents: Array.from(toolBlocks).map(b => {
        const pre = b.querySelector('.tool-output pre');
        return pre ? pre.textContent.substring(0, 80) : '';
      }),
      // Command line assertions: tool ID should NOT be present, command line should be
      toolHasCommandLine: Array.from(toolBlocks).map(b => b.querySelector('.command-line') !== null),
      toolHasNoToolId: Array.from(toolBlocks).map(b => b.querySelector('.tool-id') === null),
      toolCommandLineText: Array.from(toolBlocks).map(b => {
        const cl = b.querySelector('.command-line');
        return cl ? cl.textContent.trim() : '';
      }),
      thinkingBlockCount: thinkingBlocks.length,
      thinkingBlockContents: Array.from(thinkingBlocks).map(b => {
        const content = b.querySelector('.thinking-content');
        return content ? content.textContent.substring(0, 100) : '';
      }),
      thinkingHasHeader: Array.from(thinkingBlocks).map(b => b.querySelector('.thinking-block-header') !== null),
      // Spawn blocks (Issue #45: collapsed pre-run troubleshooting output)
      spawnBlocks: body.querySelectorAll('.spawn-block'),
      spawnBlockCount: body.querySelectorAll('.spawn-block').length,
      spawnBlockContents: Array.from(body.querySelectorAll('.spawn-block')).map(b => {
        const content = b.querySelector('.spawn-content');
        return content ? content.textContent.substring(0, 100) : '';
      }),
      spawnBlockBodiesOpen: Array.from(body.querySelectorAll('.spawn-block')).map(b => {
        const body = b.querySelector('.spawn-block-body');
        return body ? body.classList.contains('open') : false;
      }),
      spawnHasHeader: Array.from(body.querySelectorAll('.spawn-block')).map(b => b.querySelector('.spawn-block-header') !== null),
      logMsgCount: logMsgs.length,
      hasToolMarkersOutsideBlocks,
      // System-level operational messages (Executing task, Cloning, Spawning, etc.)
      // NOTE: [spawn] lines are now inside spawn-blocks, not plain .log-msg.system
      systemMsgCount: Array.from(logMsgs).filter(m => m.classList.contains('system')).length,
      systemMsgTexts: Array.from(logMsgs).filter(m => m.classList.contains('system')).map(m => m.textContent.trim()),
    };
  });

  if (detailResult.error) {
    fail('DOM validation error: ' + detailResult.error);
    await takeScreenshot('03-dom-error');
    process.exit(1);
  }

  // Real Pi RPC log has exactly 1 tool call: bash "hostname" → success
  const detailChecks = [
    { name: '1 tool block rendered', pass: detailResult.toolBlockCount === 1 },
    { name: 'tool named "bash"', pass: detailResult.toolNames[0] === 'bash' },
    { name: 'block status "done"', pass: detailResult.toolStatuses[0] === 'done' },
    { name: 'block has <pre> for output', pass: detailResult.toolOutputsHavePre[0] },
    { name: 'output contains "devvm"', pass: detailResult.toolOutputContents[0].includes('devvm') },
    // The capture carries a tool_execution_update with partial text "devvm\n"
    // followed by a tool_execution_end with the same text. Production emits
    // both a [TOOL_OUTPUT] and a [TOOL_END] entry, so the rendered output
    // must contain the text twice — this pins the UI's tool_type === 'output'
    // handler (the partial must be appended, not dropped).
    { name: 'output contains partial + final text (output handler exercised)', pass: (detailResult.toolOutputContents[0].match(/devvm/g) || []).length === 2 },
    { name: 'no tool markers outside blocks', pass: !detailResult.hasToolMarkersOutsideBlocks },
    { name: 'tool block has command-line span', pass: detailResult.toolHasCommandLine[0] === true },
    { name: 'tool block has NO tool-id span', pass: detailResult.toolHasNoToolId[0] === true },
    { name: 'command line shows "hostname"', pass: detailResult.toolCommandLineText[0] === 'hostname' },
    { name: '2 thinking blocks rendered', pass: detailResult.thinkingBlockCount === 2 },
    { name: 'thinking block 1 has header', pass: detailResult.thinkingHasHeader[0] === true },
    { name: 'thinking block 1 has non-empty content', pass: detailResult.thinkingBlockContents[0].length > 0 },
    { name: 'thinking block 2 has header', pass: detailResult.thinkingHasHeader[1] === true },
    { name: 'thinking block 2 has non-empty content', pass: detailResult.thinkingBlockContents[1].length > 0 },
    // Spawn blocks (Issue #45: collapsed pre-run troubleshooting output)
    { name: 'spawn blocks rendered', pass: detailResult.spawnBlockCount >= 1 },
    { name: 'spawn block 1 has header', pass: detailResult.spawnHasHeader[0] === true },
    { name: 'spawn block 1 has non-empty content', pass: detailResult.spawnBlockContents[0].length > 0 },
    { name: 'spawn block 1 content includes [spawn]', pass: detailResult.spawnBlockContents[0].includes('[spawn]') },
    { name: 'spawn block is collapsed by default', pass: detailResult.spawnBlockBodiesOpen[0] === false },
    // Operational system messages
    { name: 'system messages present (including operational)', pass: detailResult.systemMsgCount >= 2 },
    { name: 'operational message: Executing task', pass: detailResult.systemMsgTexts.some(t => t.includes('Executing task')) },
    { name: 'operational message: Cloning', pass: detailResult.systemMsgTexts.some(t => t.includes('Cloning')) },
    { name: 'operational message: Spawning pi subprocess', pass: detailResult.systemMsgTexts.some(t => t.includes('Spawning pi subprocess')) },
    { name: 'operational message: Prompt sent', pass: detailResult.systemMsgTexts.some(t => t.includes('Prompt sent')) },
  ];

  let detailFailed = false;
  for (const check of detailChecks) {
    if (!check.pass) { console.error('FAIL:', check.name); detailFailed = true; }
    else { console.log('PASS:', check.name); }
  }
  if (detailFailed) {
    await takeScreenshot('03-detail-validation-failed');
    process.exit(1);
  }

  // =====================================================================
  // Phase 3z: Issue #46 — UI polishes: counters, timestamps, previews
  // =====================================================================
  console.log('=== Phase 3z: Issue #46 UI polishes ===');

  const uiPolishResult = await page.evaluate(() => {
    const body = document.querySelector('.task-detail-body');
    if (!body) return { error: 'no .task-detail-body found' };

    // Check thinking blocks have preview, counter, and timestamp
    const thinkingBlocks = body.querySelectorAll('.thinking-block');
    const thinkingPreviews = Array.from(thinkingBlocks).map(b => {
      const preview = b.querySelector('.thinking-preview');
      return preview ? preview.textContent.trim() : '';
    });
    const thinkingCounters = Array.from(thinkingBlocks).map(b => {
      const counter = b.querySelector('.block-counter');
      return counter ? counter.textContent.trim() : '';
    });
    const thinkingTimestamps = Array.from(thinkingBlocks).map(b => {
      const ts = b.querySelector('.block-timestamp');
      return ts ? ts.textContent.trim() : '';
    });

    // Check tool blocks have counter and timestamp
    const toolBlocks = body.querySelectorAll('.tool-block');
    const toolCounters = Array.from(toolBlocks).map(b => {
      const counter = b.querySelector('.block-counter');
      return counter ? counter.textContent.trim() : '';
    });
    const toolTimestamps = Array.from(toolBlocks).map(b => {
      const ts = b.querySelector('.block-timestamp');
      return ts ? ts.textContent.trim() : '';
    });

    // Check tool blocks are collapsed when completed (Issue #46)
    const toolBodies = Array.from(toolBlocks).map(b => {
      const body = b.querySelector('.tool-block-body');
      return body ? body.classList.contains('open') : false;
    });

    // Check prompt preview
    const promptContainer = document.querySelector('.task-detail-prompt');
    const promptPreview = promptContainer?.querySelector('.prompt-preview')?.textContent?.trim() || '';

    return {
      thinkingPreviews,
      thinkingCounters,
      thinkingTimestamps,
      toolCounters,
      toolTimestamps,
      toolBodiesOpen: toolBodies,
      promptPreview,
    };
  });

  if (uiPolishResult.error) {
    fail('UI polish validation error: ' + uiPolishResult.error);
    await takeScreenshot('03z-ui-polish-error');
    process.exit(1);
  }

  const uiPolishChecks = [
    // Thinking blocks show first line as preview
    { name: 'thinking block 1 has preview text', pass: uiPolishResult.thinkingPreviews[0].length > 0 },
    { name: 'thinking block 2 has preview text', pass: uiPolishResult.thinkingPreviews[1].length > 0 },
    // Thinking blocks have counters
    { name: 'thinking block 1 has counter', pass: uiPolishResult.thinkingCounters[0].length > 0 },
    { name: 'thinking block 2 has counter', pass: uiPolishResult.thinkingCounters[1].length > 0 },
    // Thinking blocks have timestamps (HH:MM format)
    { name: 'thinking block 1 has timestamp', pass: /^\d{2}:\d{2}$/.test(uiPolishResult.thinkingTimestamps[0]) },
    { name: 'thinking block 2 has timestamp', pass: /^\d{2}:\d{2}$/.test(uiPolishResult.thinkingTimestamps[1]) },
    // Tool blocks have counters
    { name: 'tool block has counter', pass: uiPolishResult.toolCounters[0].length > 0 },
    // Tool blocks have timestamps
    { name: 'tool block has timestamp', pass: /^\d{2}:\d{2}$/.test(uiPolishResult.toolTimestamps[0]) },
    // Tool blocks are collapsed when completed (Issue #46)
    { name: 'completed tool block is collapsed', pass: uiPolishResult.toolBodiesOpen[0] === false },
    // Prompt has preview text when collapsed
    { name: 'prompt has preview text', pass: uiPolishResult.promptPreview.length > 0 },
  ];

  let uiPolishFailed = false;
  for (const check of uiPolishChecks) {
    if (!check.pass) { console.error('FAIL:', check.name); uiPolishFailed = true; }
    else { console.log('PASS:', check.name); }
  }
  if (uiPolishFailed) {
    await takeScreenshot('03z-ui-polish-failed');
    process.exit(1);
  }

  await takeScreenshot('03z-ui-polishes');

  // =====================================================================
  // Phase 3a: Prompt section — markdown rendering and collapsible
  // =====================================================================
  console.log('=== Phase 3a: Prompt section ===');

  const promptResult = await page.evaluate(() => {
    const promptContainer = document.querySelector('.task-detail-prompt');
    if (!promptContainer) return { error: 'no .task-detail-prompt found' };

    const header = promptContainer.querySelector('.task-detail-prompt-header');
    const body = promptContainer.querySelector('.task-detail-prompt-body');
    const mdRendered = promptContainer.querySelector('.md-rendered');
    const label = header?.querySelector('.prompt-label')?.textContent?.trim();
    const chevron = header?.querySelector('.prompt-chevron');
    const bodyIsOpen = body?.classList.contains('open');
    const chevronIsOpen = chevron?.classList.contains('open');
    const promptTextContent = mdRendered ? mdRendered.textContent.trim() : '';

    return {
      hasHeader: header !== null,
      hasBody: body !== null,
      hasMdRendered: mdRendered !== null,
      label,
      bodyIsOpen,
      chevronIsOpen,
      promptTextContent,
    };
  });

  if (promptResult.error) {
    fail('Prompt validation error: ' + promptResult.error);
    await takeScreenshot('03a-prompt-error');
    process.exit(1);
  }

  const promptChecks = [
    { name: 'prompt has header element', pass: promptResult.hasHeader },
    { name: 'prompt has body element', pass: promptResult.hasBody },
    { name: 'prompt has md-rendered element', pass: promptResult.hasMdRendered },
    { name: 'prompt label is "📝 Prompt"', pass: promptResult.label === '📝 Prompt' },
    { name: 'prompt body is collapsed by default (Issue #46)', pass: promptResult.bodyIsOpen === false },
    { name: 'prompt chevron is collapsed by default (Issue #46)', pass: promptResult.chevronIsOpen === false },
    { name: 'prompt text content rendered', pass: promptResult.promptTextContent.length > 0 },
    { name: 'prompt contains expected text', pass: promptResult.promptTextContent.includes('Simulate tool calls') },
  ];

  let promptFailed = false;
  for (const check of promptChecks) {
    if (!check.pass) { console.error('FAIL:', check.name); promptFailed = true; }
    else { console.log('PASS:', check.name); }
  }
  if (promptFailed) {
    await takeScreenshot('03a-prompt-validation-failed');
    process.exit(1);
  }

  // Verify prompt can be toggled open (starts collapsed by default — Issue #46)
  await clickEl('.task-detail-prompt-header');
  const promptOpened = await page.evaluate(() => {
    const body = document.querySelector('.task-detail-prompt-body');
    const chevron = document.querySelector('.prompt-chevron');
    return body?.classList.contains('open') && chevron?.classList.contains('open');
  });
  if (!promptOpened) fail('Prompt should be open after clicking header');
  console.log('PASS: Prompt opened after toggle');

  // Verify prompt can be toggled closed again
  await clickEl('.task-detail-prompt-header');
  const promptClosed = await page.evaluate(() => {
    const body = document.querySelector('.task-detail-prompt-body');
    const chevron = document.querySelector('.prompt-chevron');
    return !body?.classList.contains('open') && !chevron?.classList.contains('open');
  });
  if (!promptClosed) fail('Prompt should be closed after clicking header again');
  console.log('PASS: Prompt closed after toggle');

  await takeScreenshot('03a-prompt-collapsible');

  // =====================================================================
  // Phase 3b: Scroll behaviour — auto-scroll to bottom on new messages
  // =====================================================================
  console.log('=== Phase 3b: Scroll behaviour ===');

  // Ensure the body has enough content to be scrollable.
  // The test data may fit exactly in the viewport, so we inject
  // placeholder content to guarantee scrollHeight > clientHeight.
  const scrollSetup = await page.evaluate(() => {
    const body = document.querySelector('.task-detail-body');
    if (!body) return { error: 'no .task-detail-body' };

    // If already scrollable, nothing to do
    if (body.scrollHeight > body.clientHeight) {
      return { wasScrollable: true };
    }

    // Inject enough spacer content to make the body scrollable.
    // Each spacer is ~20px tall; add enough to exceed clientHeight.
    const needed = body.clientHeight + 100; // 100px of overflow
    const spacer = document.createElement('div');
    spacer.className = 'log-msg scroll-test-spacer';
    spacer.style.height = needed + 'px';
    spacer.style.background = '#1e293b';
    spacer.textContent = 'spacer';
    body.appendChild(spacer);

    return { wasScrollable: false, newScrollHeight: body.scrollHeight };
  });
  if (scrollSetup.error) fail(scrollSetup.error);
  console.log('Body scrollable:', scrollSetup.wasScrollable ? 'yes (already)' : 'yes (after spacer, height=' + scrollSetup.newScrollHeight + ')');

  // 1. Verify isScrolledToBottom helper works correctly
  const scrollHelpersOk = await page.evaluate(() => {
    const body = document.querySelector('.task-detail-body');
    if (!body) return { error: 'no .task-detail-body' };

    // Scroll to bottom first
    body.scrollTop = body.scrollHeight;
    const atBottom = isScrolledToBottom(body);

    // Scroll up by 50px (or as much as possible)
    const scrollUp = Math.min(50, body.scrollHeight - body.clientHeight);
    body.scrollTop = body.scrollHeight - body.clientHeight - scrollUp;
    const notAtBottom = !isScrolledToBottom(body);

    return { atBottom, notAtBottom };
  });
  if (scrollHelpersOk.error) fail(scrollHelpersOk.error);
  const helperChecks = [
    { name: 'isScrolledToBottom returns true when at bottom', pass: scrollHelpersOk.atBottom },
    { name: 'isScrolledToBottom returns false when scrolled up', pass: scrollHelpersOk.notAtBottom },
  ];
  for (const c of helperChecks) {
    if (!c.pass) fail(c.name);
    console.log('PASS:', c.name);
  }

  // 2. Verify scrollToBottomIfNeeded only scrolls when already at bottom
  const conditionalScrollOk = await page.evaluate(() => {
    const body = document.querySelector('.task-detail-body');
    if (!body) return { error: 'no .task-detail-body' };

    // When scrolled up, scrollToBottomIfNeeded should NOT change scrollTop
    const scrollUp = Math.min(50, body.scrollHeight - body.clientHeight);
    body.scrollTop = body.scrollHeight - body.clientHeight - scrollUp;
    const scrollTopBefore = body.scrollTop;
    scrollToBottomIfNeeded(body);
    const stayedPut = body.scrollTop === scrollTopBefore;

    // When at bottom, scrollToBottomIfNeeded should keep us at bottom
    body.scrollTop = body.scrollHeight;
    scrollToBottomIfNeeded(body);
    const stayedAtBottom = isScrolledToBottom(body);

    return { stayedPut, stayedAtBottom };
  });
  if (conditionalScrollOk.error) fail(conditionalScrollOk.error);
  const conditionalChecks = [
    { name: 'scrollToBottomIfNeeded stays put when scrolled up', pass: conditionalScrollOk.stayedPut },
    { name: 'scrollToBottomIfNeeded stays at bottom when already there', pass: conditionalScrollOk.stayedAtBottom },
  ];
  for (const c of conditionalChecks) {
    if (!c.pass) fail(c.name);
    console.log('PASS:', c.name);
  }

  // 3. Verify that appendLogToDetail respects scroll position
  //    Simulate a new log entry and check scroll behaviour.
  const appendScrollOk = await page.evaluate(() => {
    const body = document.querySelector('.task-detail-body');
    if (!body) return { error: 'no .task-detail-body' };

    // Create a fake log entry
    const fakeEntry = {
      task_id: 'test-task',
      line: '[SCROLL-TEST] This is a test message',
      level: 'system',
      timestamp: new Date().toISOString(),
    };

    // Test A: when scrolled up, appending should NOT scroll
    const scrollUp = Math.min(50, body.scrollHeight - body.clientHeight);
    body.scrollTop = body.scrollHeight - body.clientHeight - scrollUp;
    const scrollTopBefore = body.scrollTop;
    appendLogToDetail(fakeEntry);
    const stayedPut = Math.abs(body.scrollTop - scrollTopBefore) < 5;

    // Test B: when at bottom, appending SHOULD scroll
    body.scrollTop = body.scrollHeight;
    const oldHeight = body.scrollHeight;
    appendLogToDetail(fakeEntry);
    // scrollTop is capped at scrollHeight - clientHeight, so we check
    // that the element is at the bottom, not that scrollTop >= oldHeight.
    const scrolledDown = isScrolledToBottom(body);

    // Clean up test entries (remove last 2 elements added)
    for (let i = 0; i < 2; i++) {
      const lastChild = body.lastElementChild;
      if (lastChild && lastChild.textContent && lastChild.textContent.includes('[SCROLL-TEST]')) {
        body.removeChild(lastChild);
      }
    }

    return {
      stayedPut,
      scrolledDown,
    };
  });
  if (appendScrollOk.error) fail(appendScrollOk.error);
  const appendChecks = [
    { name: 'appendLogToDetail stays put when scrolled up', pass: appendScrollOk.stayedPut },
    { name: 'appendLogToDetail scrolls down when at bottom', pass: appendScrollOk.scrolledDown },
  ];
  for (const c of appendChecks) {
    if (!c.pass) fail(c.name);
    console.log('PASS:', c.name);
  }

  // Clean up spacer content
  await page.evaluate(() => {
    const spacer = document.querySelector('.scroll-test-spacer');
    if (spacer) spacer.remove();
  });

  // =====================================================================
  // Phase 3c: Streaming path — _appendThinkingToDetail creates new blocks
  //              after interruptions (tool calls, text, etc.)
  // =====================================================================
  console.log('=== Phase 3c: Streaming thinking blocks after interruptions ===');

  // Test the streaming path directly by calling _appendThinkingToDetail
  // in a controlled environment. This validates the DOM-position check
  // (lastBlock === body.lastElementChild) that determines whether to
  // append to an existing block or create a new one.
  const streamingResult = await page.evaluate(() => {
    // Create a hidden test container to simulate task-detail-body
    const testBody = document.createElement('div');
    testBody.className = 'task-detail-body';
    testBody.style.display = 'none';
    document.body.appendChild(testBody);

    // Simulate streaming sequence: thinking → tool block → thinking → text
    // Step 1: First thinking delta (creates block 1)
    _appendThinkingToDetail(testBody, 'Analyzing the request...', true);

    // Step 2: Tool block is appended (simulates tool start)
    const toolBlock = document.createElement('div');
    toolBlock.className = 'tool-block';
    toolBlock.id = 'tool-test-123';
    toolBlock.innerHTML = '<div class="tool-block-header"><span class="tool-name">bash</span></div>';
    testBody.appendChild(toolBlock);

    // Step 3: Second thinking delta AFTER tool block
    // Without the fix, this would append to block 1 (wrong).
    // With the fix, it creates block 2 because block 1 is no longer
    // the last child of the body (tool block follows it).
    _appendThinkingToDetail(testBody, 'Now I have the hostname result...', true);

    // Step 4: Text delta (creates a guest message)
    _appendTextToDetail(testBody, 'Your hostname is **devvm**.', true);

    // Step 5: Third thinking delta after text
    // Should create block 3 because the last thinking block (2) is
    // no longer the last child (text message follows it).
    _appendThinkingToDetail(testBody, 'Final analysis complete.', true);

    // Validate results
    const thinkingBlocks = testBody.querySelectorAll('.thinking-block');
    const toolBlocks = testBody.querySelectorAll('.tool-block');
    const guestMsgs = testBody.querySelectorAll('.log-msg.guest');

    // Clean up
    document.body.removeChild(testBody);

    return {
      thinkingBlockCount: thinkingBlocks.length,
      toolBlockCount: toolBlocks.length,
      guestMsgCount: guestMsgs.length,
      thinkingContents: Array.from(thinkingBlocks).map(b => {
        const content = b.querySelector('.thinking-content');
        return content ? content.textContent.trim() : '';
      }),
    };
  });

  const streamingChecks = [
    { name: '3 thinking blocks created (not 1 merged)', pass: streamingResult.thinkingBlockCount === 3 },
    { name: '1 tool block present', pass: streamingResult.toolBlockCount === 1 },
    { name: '1 guest message present', pass: streamingResult.guestMsgCount === 1 },
    { name: 'thinking block 1 has initial content', pass: streamingResult.thinkingContents[0].includes('Analyzing') },
    { name: 'thinking block 2 has post-tool content', pass: streamingResult.thinkingContents[1].includes('hostname result') },
    { name: 'thinking block 3 has post-text content', pass: streamingResult.thinkingContents[2].includes('Final analysis') },
  ];

  let streamingFailed = false;
  for (const check of streamingChecks) {
    if (!check.pass) { console.error('FAIL:', check.name); streamingFailed = true; }
    else { console.log('PASS:', check.name); }
  }
  if (streamingFailed) {
    await takeScreenshot('03c-streaming-failed');
    process.exit(1);
  }

  await takeScreenshot('03c-streaming-thinking-blocks');

  // =====================================================================
  // Phase 3d: Edit tool diff rendering (Issue #2)
  // =====================================================================
  console.log('=== Phase 3d: Edit tool diff rendering (Issue #2) ===');

  // --- Unit-style checks of the JS diff functions ---
  const diffUnitResult = await page.evaluate(() => {
    const out = {};

    // 1. Single-line word-level change (mirrors the real edit capture)
    const wordDiff = buildEditDiffHTML(JSON.stringify({
      path: '/tmp/example-edit-target.txt',
      edits: [{ oldText: 'return "hello " + name', newText: 'return "goodbye " + name' }]
    }));
    out.word = {
      hasDiff: wordDiff.indexOf('class="tool-diff"') !== -1,
      delLines: (wordDiff.match(/class="diff-line del"/g) || []).length,
      addLines: (wordDiff.match(/class="diff-line add"/g) || []).length,
      wordDel: /diff-word-del">[^<]*hello[^<]*</.test(wordDiff),
      wordAdd: /diff-word-add">[^<]*goodbye[^<]*</.test(wordDiff),
    };

    // 2. Multi-line change with unchanged context lines
    const multiDiff = buildEditDiffHTML(JSON.stringify({
      path: '/tmp/f.py',
      edits: [{ oldText: 'x = 1\ny = 2\nz = 3', newText: 'x = 1\ny = 99\nz = 3\nw = 4' }]
    }));
    out.multi = {
      sameLines: (multiDiff.match(/class="diff-line same"/g) || []).length,
      delLines: (multiDiff.match(/class="diff-line del"/g) || []).length,
      addLines: (multiDiff.match(/class="diff-line add"/g) || []).length,
      hasContext: multiDiff.indexOf('x = 1') !== -1 && multiDiff.indexOf('z = 3') !== -1,
    };

    // 3. Multiple edits in one call → one hunk per edit
    const multiEdit = buildEditDiffHTML(JSON.stringify({
      path: '/tmp/f.py',
      edits: [
        { oldText: 'a = 1', newText: 'a = 2' },
        { oldText: 'b = 1', newText: 'b = 2' }
      ]
    }));
    out.multiEdit = {
      hunks: (multiEdit.match(/class="diff-hunk"/g) || []).length,
    };

    // 4. Guard clauses — nothing diffable → empty string
    out.guards = {
      empty: buildEditDiffHTML('') === '',
      unparseable: buildEditDiffHTML('not json') === '',
      noEdits: buildEditDiffHTML(JSON.stringify({ path: '/tmp/f' })) === '',
      identical: buildEditDiffHTML(JSON.stringify({ path: '/tmp/f', edits: [{ oldText: 'x', newText: 'x' }] })) === '',
    };

    return out;
  });

  const diffUnitChecks = [
    { name: 'word diff: tool-diff container present', pass: diffUnitResult.word.hasDiff },
    { name: 'word diff: exactly 1 del line', pass: diffUnitResult.word.delLines === 1 },
    { name: 'word diff: exactly 1 add line', pass: diffUnitResult.word.addLines === 1 },
    { name: 'word diff: word-level del span on changed word', pass: diffUnitResult.word.wordDel },
    { name: 'word diff: word-level add span on changed word', pass: diffUnitResult.word.wordAdd },
    { name: 'multi-line diff: 2 context (same) lines', pass: diffUnitResult.multi.sameLines === 2 },
    { name: 'multi-line diff: 1 del line', pass: diffUnitResult.multi.delLines === 1 },
    { name: 'multi-line diff: 2 add lines', pass: diffUnitResult.multi.addLines === 2 },
    { name: 'multi-line diff: context lines present', pass: diffUnitResult.multi.hasContext },
    { name: 'multi-edit: one hunk per edit', pass: diffUnitResult.multiEdit.hunks === 2 },
    { name: 'guard: empty args → no diff', pass: diffUnitResult.guards.empty },
    { name: 'guard: unparseable args → no diff', pass: diffUnitResult.guards.unparseable },
    { name: 'guard: non-edit args → no diff', pass: diffUnitResult.guards.noEdits },
    { name: 'guard: identical old/new → no diff', pass: diffUnitResult.guards.identical },
  ];
  let diffUnitFailed = false;
  for (const check of diffUnitChecks) {
    if (!check.pass) { console.error('FAIL:', check.name); diffUnitFailed = true; }
    else { console.log('PASS:', check.name); }
  }
  if (diffUnitFailed) {
    await takeScreenshot('03d-diff-unit-failed');
    process.exit(1);
  }

  // --- User flow: open the completed edit task and verify the rendered diff ---
  // Back to the Tasks tab (real user journey)
  await page.locator('.tab').filter({ hasText: 'Tasks' }).click();
  await page.waitForFunction(() => {
    return document.getElementById('tab-tasks').style.display === 'block';
  }, { timeout: 5000 });

  // Enable the COMPLETED filter so the edit task becomes visible
  await clickEl('.task-filter-btn[data-status="COMPLETED"]');

  // toggleTaskFilter re-fetches and re-renders — wait for the edit task item
  await page.waitForFunction((id) => {
    const items = document.querySelectorAll('.task-item');
    for (const item of items) {
      const idEl = item.querySelector('.task-id');
      if (idEl && idEl.textContent.includes(id)) return true;
    }
    return false;
  }, editTaskId, { timeout: 10000 });

  // Click the edit task item
  await page.evaluate((id) => {
    const items = document.querySelectorAll('.task-item');
    for (const item of items) {
      const idEl = item.querySelector('.task-id');
      if (idEl && idEl.textContent.includes(id)) { item.click(); return; }
    }
    throw new Error('edit task item not found: ' + id);
  }, editTaskId);
  await page.waitForSelector('.task-detail-body .tool-block', { timeout: 5000 });

  const editDiffResult = await page.evaluate(() => {
    const body = document.querySelector('.task-detail-body');
    if (!body) return { error: 'no .task-detail-body found' };
    const blocks = Array.from(body.querySelectorAll('.tool-block'));
    const nameOf = b => (b.querySelector('.tool-name') ? b.querySelector('.tool-name').textContent : '').trim();
    const editBlock = blocks.find(b => nameOf(b) === 'edit');
    const readBlock = blocks.find(b => nameOf(b) === 'read');
    if (!editBlock) return { error: 'no edit tool block found' };

    const diff = editBlock.querySelector('.tool-diff');
    const textOf = sel => Array.from(editBlock.querySelectorAll(sel)).map(e => e.textContent);
    const outputPre = editBlock.querySelector('.tool-output pre');
    const headerPlain = editBlock.querySelector('.command-line-plain');

    return {
      blockCount: blocks.length,
      hasDiff: diff !== null,
      delLines: diff ? textOf('.diff-line.del .diff-text') : [],
      addLines: diff ? textOf('.diff-line.add .diff-text') : [],
      wordDels: diff ? textOf('.diff-word-del') : [],
      wordAdds: diff ? textOf('.diff-word-add') : [],
      readBlockHasDiff: readBlock ? readBlock.querySelector('.tool-diff') !== null : null,
      outputText: outputPre ? outputPre.textContent : '',
      headerArgs: headerPlain ? headerPlain.textContent.trim() : '',
    };
  });

  if (editDiffResult.error) {
    fail('Edit diff validation error: ' + editDiffResult.error);
    await takeScreenshot('03d-edit-diff-error');
    process.exit(1);
  }

  const editDiffChecks = [
    { name: '2 tool blocks rendered (read + edit)', pass: editDiffResult.blockCount === 2 },
    { name: 'edit block has inline diff', pass: editDiffResult.hasDiff },
    { name: 'diff del line shows old text', pass: editDiffResult.delLines.some(l => l.includes('hello')) },
    { name: 'diff add line shows new text', pass: editDiffResult.addLines.some(l => l.includes('goodbye')) },
    { name: 'word-level del span present', pass: editDiffResult.wordDels.some(w => w.includes('hello')) },
    { name: 'word-level add span present', pass: editDiffResult.wordAdds.some(w => w.includes('goodbye')) },
    { name: 'read block has NO diff', pass: editDiffResult.readBlockHasDiff === false },
    { name: 'edit output shows replacement message', pass: editDiffResult.outputText.includes('Successfully replaced 1 block(s)') },
    { name: 'edit header shows path display string', pass: editDiffResult.headerArgs === 'path: /tmp/example-edit-target.txt' },
  ];
  let editDiffFailed = false;
  for (const check of editDiffChecks) {
    if (!check.pass) { console.error('FAIL:', check.name); editDiffFailed = true; }
    else { console.log('PASS:', check.name); }
  }
  if (editDiffFailed) {
    await takeScreenshot('03d-edit-diff-failed');
    process.exit(1);
  }

  // Expand the edit tool block so the diff is visible in the screenshot
  await page.evaluate(() => {
    const body = document.querySelector('.task-detail-body');
    const editBlock = Array.from(body.querySelectorAll('.tool-block'))
      .find(b => (b.querySelector('.tool-name') ? b.querySelector('.tool-name').textContent : '').trim() === 'edit');
    if (!editBlock) throw new Error('edit block not found');
    editBlock.querySelector('.tool-block-header').click();
  });
  await page.waitForTimeout(300);
  await takeScreenshot('03d-edit-diff');

  // =====================================================================
  // Phase 4: Logs tab — dates, tasks, entries, breadcrumbs
  // =====================================================================
  console.log('=== Phase 4: Logs tab ===');

  // Click the Logs tab — fires onclick="showTab('logs')" which triggers loadLogDates()
  await page.locator('.tab').filter({ hasText: 'Logs' }).click();

  // Wait for log dates to load (the date items appear after the API fetch completes)
  await page.waitForSelector('#log-dates-list .log-task-item', { timeout: 10000 });

  // Verify URL is /logs (loadLogDates pushes /logs)
  const logsUrl = await page.evaluate(() => window.location.pathname);
  if (logsUrl !== '/logs') fail('URL should be /logs, got: ' + logsUrl);
  console.log('PASS: URL is /logs');

  // Verify #tab-logs has correct overflow (Issue #42 fix).
  // The tab container must NOT have overflow:hidden as that clips
  // the scrollable child elements and prevents scrolling.
  const tabLogsOverflow = await page.evaluate(() => {
    const el = document.getElementById('tab-logs');
    if (!el) return { error: 'not found' };
    const style = window.getComputedStyle(el);
    return {
      overflowY: style.overflowY,
      scrollHeight: el.scrollHeight,
      clientHeight: el.clientHeight,
      canScroll: el.scrollHeight > el.clientHeight,
    };
  });
  if (tabLogsOverflow.error) fail(tabLogsOverflow.error);
  if (tabLogsOverflow.overflowY === 'hidden') {
    fail('#tab-logs must NOT have overflow:hidden (clips scrollable children), got overflow-y: hidden');
  }
  if (tabLogsOverflow.overflowY !== 'auto' && tabLogsOverflow.overflowY !== 'scroll') {
    fail('#tab-logs should have overflow-y:auto or scroll, got ' + tabLogsOverflow.overflowY);
  }
  console.log('PASS: #tab-logs has overflow-y:', tabLogsOverflow.overflowY, '(canScroll:', tabLogsOverflow.canScroll + ')');

  // Verify log dates are present
  const logDatesOk = await page.evaluate(() => {
    const items = document.querySelectorAll('#log-dates-list .log-task-item');
    return items.length > 0;
  });
  if (!logDatesOk) fail('No log dates found');
  console.log('PASS: Log dates loaded');

  // Get the date string for later use
  const logDate = await page.evaluate(() => {
    const item = document.querySelector('.log-task-item .log-task-id');
    return item ? item.textContent.replace('📅 ', '').trim() : '';
  });
  if (logDate !== expectedLogDate) fail('Log date mismatch: expected ' + expectedLogDate + ', got ' + logDate);
  console.log('PASS: Log date:', logDate);

  await takeScreenshot('04-logs-dates');

  // --- Click the date item to load tasks for that date ---
  console.log('--- Clicking date item ---');
  await clickEl('#log-dates-list .log-task-item');

  // Wait for loadLogTasks to complete (it's async and fetches tasks from the API)
  await page.waitForFunction((date) => {
    const crumbs = document.querySelectorAll('.log-breadcrumb .crumb, .log-breadcrumb .current');
    return Array.from(crumbs).some(c => c.textContent.includes(date));
  }, logDate, { timeout: 10000 });

  // Verify breadcrumb shows date crumb
  const breadcrumbOk2 = await page.evaluate((date) => {
    const crumbs = document.querySelectorAll('.log-breadcrumb .crumb, .log-breadcrumb .current');
    const text = Array.from(crumbs).map(c => c.textContent).join(' ');
    return text.includes(date);
  }, logDate);
  if (!breadcrumbOk2) fail('Breadcrumb should contain date ' + logDate);
  console.log('PASS: Breadcrumb shows date');

  // Verify URL updated to /logs/:date
  const dateUrl = await page.evaluate(() => window.location.pathname);
  if (!dateUrl.startsWith('/logs/' + logDate)) fail('URL should start with /logs/' + logDate + ', got: ' + dateUrl);
  console.log('PASS: URL updated to', dateUrl);

  // Verify task items are listed (our test task should be present)
  const taskItems = await page.$$('.log-task-item');
  if (taskItems.length === 0) fail('No task items found under date ' + logDate);
  console.log('PASS:', taskItems.length, 'task(s) found under date');

  // Verify task items show start timestamps (not "click to view")
  const hasTimestamps = await page.evaluate(() => {
    const countEls = document.querySelectorAll('.log-task-item .log-task-count');
    if (countEls.length === 0) return false;
    // Timestamps should contain digits (date/time), not "click to view"
    return Array.from(countEls).every(el => /\d/.test(el.textContent));
  });
  if (!hasTimestamps) fail('Task items should display start timestamps, not "click to view"');
  console.log('PASS: Task items display start timestamps');

  // Verify breadcrumb has "All Dates" crumb for navigation back
  const allDatesCrumb = await page.evaluate(() => {
    const crumbs = document.querySelectorAll('.log-breadcrumb .crumb');
    return Array.from(crumbs).some(c => c.textContent === 'All Dates');
  });
  if (!allDatesCrumb) fail('Breadcrumb should have "All Dates" crumb');
  console.log('PASS: "All Dates" breadcrumb crumb present');

  await takeScreenshot('05-logs-tasks-under-date');

  // --- Click the main task item to load its log entries ---
  // Tasks are sorted by timestamp (newest first), so we click the one whose
  // ID starts with "ui-test-task-" (not "ui-test-failed-task-" or "ui-test-completed-task-").
  console.log('--- Clicking main task item ---');
  const mainTaskItem = await page.evaluateHandle(() => {
    const items = document.querySelectorAll('.log-task-item');
    for (const item of items) {
      const idEl = item.querySelector('.log-task-id');
      if (idEl && idEl.textContent.includes('ui-test-task-') && !idEl.textContent.includes('-failed-') && !idEl.textContent.includes('-completed-')) {
        return item;
      }
    }
    return null;
  });
  if (!mainTaskItem) fail('Could not find main task item in log list');
  await mainTaskItem.click();

  // Wait for log entries to render
  await page.waitForSelector('#log-entries-list .log-msg', { timeout: 10000 });

  // Verify log entries are rendered with correct structure
  const logEntryResult = await page.evaluate(() => {
    const entries = document.querySelectorAll('#log-entries-list .log-msg');
    if (entries.length === 0) return { error: 'no log entries found' };

    // Count breadcrumbs — should be exactly one (not duplicated)
    const allBreadcrumbs = document.querySelectorAll('.log-breadcrumb');
    const breadcrumbCrumbs = Array.from(document.querySelectorAll('.log-breadcrumb .crumb, .log-breadcrumb .current'))
      .map(c => c.textContent);

    // Check for tool blocks (should be present for tool-level entries)
    const toolBlocks = document.querySelectorAll('#log-entries-list .tool-block');
    const toolBlockNames = Array.from(toolBlocks).map(b => b.querySelector('.tool-name')?.textContent?.trim());
    const toolBlockStatuses = Array.from(toolBlocks).map(b => b.querySelector('.tool-status')?.textContent?.trim());
    const toolBlocksHavePre = Array.from(toolBlocks).map(b => b.querySelector('.tool-output pre') !== null);

    // Check for tool markers outside blocks (should NOT exist)
    const toolMarkerLines = ['[TOOL_OUTPUT]', '[TOOL_END]', '[TOOL_START]'];
    const hasToolMarkersOutsideBlocks = Array.from(entries).some(m =>
      toolMarkerLines.some(marker => m.textContent.includes(marker))
    );

    // Count total log-msg elements (system + guest, not tool markers)
    const systemMsgs = Array.from(entries).filter(e => e.classList.contains('system'));
    const guestMsgs = Array.from(entries).filter(e => e.classList.contains('guest'));
    const systemMsgTexts = systemMsgs.map(m => m.textContent.trim());

    // Check for thinking blocks — consecutive thinking entries should be
    // merged into single blocks by renderLogEntries' pendingThinkingLines
    // accumulator. The test data has thinking → tool → thinking → text,
    // so we expect thinking blocks to be present.
    const thinkingBlocks = document.querySelectorAll('#log-entries-list .thinking-block');
    const thinkingBlockContents = Array.from(thinkingBlocks).map(b => {
      const content = b.querySelector('.thinking-content');
      return content ? content.textContent.substring(0, 100) : '';
    });

    // Download button in the log breadcrumb (Issue #6)
    const downloadBtn = document.querySelector('.log-breadcrumb .download-btn');

    return {
      entryCount: entries.length,
      systemMsgCount: systemMsgs.length,
      guestMsgCount: guestMsgs.length,
      hasSystemMsg: systemMsgs.length > 0,
      hasGuestMsg: guestMsgs.length > 0,
      systemMsgTexts,
      breadcrumbCount: allBreadcrumbs.length,
      breadcrumbCrumbs,
      toolBlockCount: toolBlocks.length,
      toolBlockNames,
      toolBlockStatuses,
      toolBlocksHavePre,
      hasToolMarkersOutsideBlocks,
      thinkingBlockCount: thinkingBlocks.length,
      thinkingBlockContents,
      hasDownloadBtn: downloadBtn !== null,
      downloadBtnOnclick: downloadBtn ? (downloadBtn.getAttribute('onclick') || '') : '',
    };
  });

  if (logEntryResult.error) fail(logEntryResult.error);

  const logEntryChecks = [
    { name: 'log entries rendered', pass: logEntryResult.entryCount > 0 },
    { name: 'system messages present', pass: logEntryResult.hasSystemMsg },
    { name: 'guest messages present', pass: logEntryResult.hasGuestMsg },
    { name: 'breadcrumb has date crumb', pass: logEntryResult.breadcrumbCrumbs.some(c => c.includes('-')) },
    { name: 'breadcrumb has All Dates crumb', pass: logEntryResult.breadcrumbCrumbs.some(c => c === 'All Dates') },
    // --- Bug 1: content duplication ---
    { name: 'exactly 1 breadcrumb (no duplication)', pass: logEntryResult.breadcrumbCount === 1 },
    // --- Bug 2: tool blocks in log entries view ---
    { name: 'tool blocks rendered in log entries view', pass: logEntryResult.toolBlockCount > 0 },
    { name: 'tool block named "bash"', pass: logEntryResult.toolBlockNames.includes('bash') },
    { name: 'tool block status "done"', pass: logEntryResult.toolBlockStatuses.includes('done') },
    { name: 'tool block has <pre> for output', pass: logEntryResult.toolBlocksHavePre.some(Boolean) },
    { name: 'no tool markers outside blocks', pass: !logEntryResult.hasToolMarkersOutsideBlocks },
    // --- Thinking blocks in log entries view (renderLogEntries fix) ---
    // The test data has: thinking → tool → thinking → text
    // renderLogEntries should merge consecutive thinking entries into
    // single blocks. With the pendingThinkingLines accumulator, thinking
    // entries separated by non-thinking entries get separate blocks.
    { name: 'thinking blocks present in log entries view', pass: logEntryResult.thinkingBlockCount > 0 },
    { name: 'thinking blocks have non-empty content', pass: logEntryResult.thinkingBlockContents.every(c => c.length > 0) },
    // --- Download button in log breadcrumb (Issue #6) ---
    { name: 'download button present in log breadcrumb', pass: logEntryResult.hasDownloadBtn },
    { name: 'download button calls downloadLog()', pass: logEntryResult.downloadBtnOnclick.includes('downloadLog') },
    // Operational system messages in log entries view
    { name: 'operational message: Executing task', pass: logEntryResult.systemMsgTexts.some(t => t.includes('Executing task')) },
    { name: 'operational message: Cloning', pass: logEntryResult.systemMsgTexts.some(t => t.includes('Cloning')) },
    { name: 'operational message: Spawning pi subprocess', pass: logEntryResult.systemMsgTexts.some(t => t.includes('Spawning pi subprocess')) },
    { name: 'operational message: Prompt sent', pass: logEntryResult.systemMsgTexts.some(t => t.includes('Prompt sent')) },
  ];

  let logEntryFailed = false;
  for (const check of logEntryChecks) {
    if (!check.pass) { console.error('FAIL:', check.name); logEntryFailed = true; }
    else { console.log('PASS:', check.name); }
  }
  if (logEntryFailed) {
    await takeScreenshot('06-log-entries-failed');
    process.exit(1);
  }

  await takeScreenshot('06-log-entries');

  // --- Exercise the download button end-to-end (Issue #6) ---
  // Clicking the button must start a real browser download of the raw
  // JSONL file. This exercises downloadLog() (URL construction, anchor
  // click, download attribute) and the endpoint's attachment response.
  const [download] = await Promise.all([
    page.waitForEvent('download'),
    page.click('.log-breadcrumb .download-btn'),
  ]);
  const suggestedFilename = download.suggestedFilename();
  if (suggestedFilename !== taskId + '.jsonl') {
    fail('download filename should be ' + taskId + '.jsonl, got ' + suggestedFilename);
  }
  console.log('PASS: download button triggers download of ' + suggestedFilename);
  await download.cancel();

  // --- Click "All Dates" breadcrumb crumb to navigate back ---
  console.log('--- Clicking All Dates breadcrumb ---');
  await clickEl('.crumb');

  // Wait for dates list to reload
  await page.waitForSelector('#log-dates-list .log-task-item', { timeout: 10000 });

  const backToDateList = await page.evaluate(() => {
    const breadcrumb = document.querySelector('.log-breadcrumb .current');
    return breadcrumb && breadcrumb.textContent === 'All Dates';
  });
  if (!backToDateList) fail('Breadcrumb should show "All Dates" after clicking crumb');
  console.log('PASS: Breadcrumb navigation to All Dates works');

  const breadcrumbUrl = await page.evaluate(() => window.location.pathname);
  if (breadcrumbUrl !== '/logs') fail('URL should be /logs after breadcrumb nav, got: ' + breadcrumbUrl);
  console.log('PASS: URL is /logs after breadcrumb navigation');

  await takeScreenshot('07-breadcrumb-nav');

  // =====================================================================
  // Phase 5: Direct URL navigation
  // =====================================================================
  console.log('=== Phase 5: Direct URL navigation ===');

  // Navigate directly to /task/:id — should show task detail in Logs tab
  await page.goto(baseURL + '/task/' + taskId);
  await page.waitForLoadState('networkidle');
  await page.waitForSelector('.task-detail-body .tool-block', { timeout: 5000 });

  const directDetail = await page.evaluate(() => {
    return document.querySelector('.task-detail-body .tool-block') !== null &&
           document.getElementById('tab-logs').style.display === 'block';
  });
  if (!directDetail) fail('Direct nav to /task/:id should show task detail in Logs tab');
  console.log('PASS: Direct nav to /task/:id works');

  // Verify URL is /task/:id (kept for backwards compatibility)
  const directNavUrl = await page.evaluate(() => window.location.pathname);
  if (!directNavUrl.startsWith('/task/')) fail('URL should be /task/:id, got: ' + directNavUrl);
  console.log('PASS: URL is', directNavUrl, '(kept for backwards compat)');

  // --- Download button in the task detail header (Issue #208) ---
  // The task view must offer the whole trace as a downloadable JSONL file
  // without the user navigating to the date-partitioned Logs browser. The task
  // ID must reach downloadTaskLog() through a data attribute, not interpolated
  // into the onclick as a JS string literal — task IDs are caller-supplied.
  const taskDetailDownload = await page.evaluate(() => {
    const btn = document.querySelector('.task-detail-header .download-task-btn');
    return {
      present: btn !== null,
      onclick: btn ? (btn.getAttribute('onclick') || '') : '',
      dataset: btn ? btn.dataset.taskId : null,
    };
  });
  if (!taskDetailDownload.present) fail('task detail header must have a download button');
  if (!taskDetailDownload.onclick.includes('downloadTaskLog')) fail('download button should call downloadTaskLog(), got: ' + taskDetailDownload.onclick);
  if (taskDetailDownload.onclick.includes("'")) fail('download button onclick must not embed a quoted task id, got: ' + taskDetailDownload.onclick);
  if (taskDetailDownload.dataset !== taskId) fail('download button data-task-id should be ' + taskId + ', got: ' + taskDetailDownload.dataset);
  console.log('PASS: task detail download button passes the task id via data-task-id');

  const [taskTraceDownload] = await Promise.all([
    page.waitForEvent('download'),
    page.click('.task-detail-header .download-task-btn'),
  ]);
  const taskTraceFilename = taskTraceDownload.suggestedFilename();
  if (taskTraceFilename !== taskId + '.jsonl') {
    fail('task trace download should be ' + taskId + '.jsonl, got ' + taskTraceFilename);
  }
  console.log('PASS: task detail download button triggers download of ' + taskTraceFilename);
  await taskTraceDownload.cancel();

  await takeScreenshot('08-direct-task-nav');

  // Navigate directly to /logs
  await page.goto(baseURL + '/logs');
  await page.waitForLoadState('networkidle');
  await page.waitForFunction(() => {
    return document.getElementById('tab-logs').style.display === 'block';
  }, { timeout: 5000 });

  const directLogs = await page.evaluate(() => {
    return document.getElementById('tab-logs').style.display === 'block' &&
           document.querySelector('.log-task-item') !== null;
  });
  if (!directLogs) fail('Direct nav to /logs should show logs view with dates');
  console.log('PASS: Direct nav to /logs works');

  // Navigate directly to /logs/:date
  await page.goto(baseURL + '/logs/' + logDate);
  await page.waitForLoadState('networkidle');
  await page.waitForFunction(() => {
    const view = document.getElementById('log-view');
    return view.querySelector('.log-task-item') !== null ||
           view.querySelector('.empty-state') !== null;
  }, { timeout: 10000 });

  const directLogsDate = await page.evaluate(() => {
    return document.getElementById('tab-logs').style.display === 'block';
  });
  if (!directLogsDate) fail('Direct nav to /logs/:date should show logs tab');
  console.log('PASS: Direct nav to /logs/:date works');

  await takeScreenshot('09-direct-logs-nav');

  // =====================================================================
  // Phase 6: Browser back/forward
  // =====================================================================
  console.log('=== Phase 6: Browser back navigation ===');

  // Go back — should return to /logs
  await page.goBack();
  await page.waitForFunction((date) => {
    return window.location.pathname !== '/logs/' + date;
  }, logDate, { timeout: 5000 });

  const backPath = await page.evaluate(() => window.location.pathname);
  console.log('PASS: goBack() navigated to', backPath);

  // Note: goForward() is not tested because goBack() triggers a full page
  // reload (new WebSocket connection), which resets the SPA history stack.

  // =====================================================================
  // Phase 7: Navigate back to Tasks tab, then click into existing task
  // =====================================================================
  console.log('=== Phase 7: Back to Tasks, then click existing task ===');

  // Start from task detail (via direct URL)
  await page.goto(baseURL + '/task/' + taskId);
  await page.waitForLoadState('networkidle');
  await page.waitForSelector('.task-detail-body .tool-block', { timeout: 5000 });

  // Verify we're in Logs view with tool blocks
  const inLogsView = await page.evaluate(() => {
    return document.querySelector('.task-detail-body .tool-block') !== null &&
           document.getElementById('tab-logs').style.display === 'block';
  });
  if (!inLogsView) fail('Should be in Logs view');
  console.log('PASS: In Logs view');

  // Click the Tasks tab to navigate back
  await page.locator('.tab').filter({ hasText: 'Tasks' }).click();

  // Wait for Tasks tab to become active
  await page.waitForFunction(() => {
    const tabs = document.querySelectorAll('.tab');
    for (const tab of tabs) {
      if (tab.textContent.trim() === 'Tasks' && tab.classList.contains('active')) {
        return true;
      }
    }
    return false;
  }, { timeout: 5000 });

  // Verify Tasks tab is active
  const tasksTabActiveAfterNav = await page.evaluate(() => {
    return document.getElementById('tab-tasks').style.display === 'block' &&
           document.getElementById('tab-logs').style.display === 'none';
  });
  if (!tasksTabActiveAfterNav) fail('Tasks tab should be active after clicking Tasks tab');
  console.log('PASS: Tasks tab active after clicking Tasks tab');

  // Verify URL updated to /tasks
  const tasksUrl = await page.evaluate(() => window.location.pathname);
  if (tasksUrl !== '/tasks' && tasksUrl !== '/') fail('URL should be /tasks or / after clicking Tasks tab, got: ' + tasksUrl);
  console.log('PASS: URL is', tasksUrl, 'after clicking Tasks tab');

  await takeScreenshot('07-back-to-tasks');

  // Now click into an existing task from the task list
  console.log('--- Clicking existing task from task list ---');
  await clickEl('.task-item');
  await page.waitForSelector('.task-detail-body .tool-block', { timeout: 5000 });

  // Verify we're in Logs view with tool blocks
  const backInLogs = await page.evaluate(() => {
    return document.querySelector('.task-detail-body .tool-block') !== null &&
           document.getElementById('tab-logs').style.display === 'block';
  });
  if (!backInLogs) fail('Should be in Logs view after clicking task');
  console.log('PASS: In Logs view after clicking existing task');

  // Verify task content is loaded (tool blocks should be present)
  const taskContentLoaded = await page.evaluate(() => {
    const body = document.querySelector('.task-detail-body');
    return body !== null && body.querySelectorAll('.tool-block').length > 0;
  });
  if (!taskContentLoaded) fail('Task content should be loaded after clicking task');
  console.log('PASS: Task content loaded after clicking existing task');

  // Verify URL updated to /task/:id
  const taskDetailUrl = await page.evaluate(() => window.location.pathname);
  if (!taskDetailUrl.startsWith('/task/')) fail('URL should start with /task/, got: ' + taskDetailUrl);
  console.log('PASS: URL is', taskDetailUrl, 'after clicking existing task');

  await takeScreenshot('08-click-existing-task');

  // =====================================================================
  // Phase 8: Breadcrumb navigation back to date list
  // =====================================================================
  console.log('=== Phase 8: Breadcrumb navigation ===');

  // Navigate to logs view to test breadcrumb (task detail view has no breadcrumb)
  await page.goto(baseURL + '/logs/' + logDate + '/' + taskId);
  await page.waitForLoadState('networkidle');
  await page.waitForSelector('#log-entries-list .log-msg', { timeout: 5000 });

  // Click the date crumb to navigate back to date list
  await clickEl('.log-breadcrumb .crumb');

  // Wait for date list to reload
  await page.waitForSelector('#log-dates-list .log-task-item', { timeout: 10000 });

  const backToDateList2 = await page.evaluate(() => {
    const breadcrumb = document.querySelector('.log-breadcrumb .current');
    return breadcrumb && breadcrumb.textContent === 'All Dates';
  });
  if (!backToDateList2) fail('Breadcrumb should show "All Dates" after clicking crumb');
  console.log('PASS: Breadcrumb navigation to All Dates works');

  const breadcrumbUrl2 = await page.evaluate(() => window.location.pathname);
  if (breadcrumbUrl2 !== '/logs') fail('URL should be /logs after breadcrumb nav, got: ' + breadcrumbUrl2);
  console.log('PASS: URL is /logs after breadcrumb navigation');

  await takeScreenshot('09-breadcrumb-nav');

  // =====================================================================
  // Phase 9: Re-run button — creates a new task from an existing one
  // =====================================================================
  console.log('=== Phase 9: Re-run button ===');

  // Navigate to task detail (where the re-run button lives)
  await page.goto(baseURL + '/task/' + taskId);
  await page.waitForLoadState('networkidle');
  await page.waitForSelector('.task-detail-body .tool-block', { timeout: 5000 });

  // Verify the re-run button is present
  const rerunBtnPresent = await page.evaluate(() => {
    const btn = Array.from(document.querySelectorAll('.task-detail-header button'))
      .find(b => b.textContent.includes('Re-run'));
    return btn !== null;
  });
  if (!rerunBtnPresent) fail('Re-run button should be present in task detail header');
  console.log('PASS: Re-run button present');

  // Capture task count before re-run
  const taskCountBefore = await page.evaluate(() => {
    return document.querySelectorAll('.task-item').length;
  });
  console.log('Task count before re-run:', taskCountBefore);

  // Click the re-run button — fires onclick="rerunTask(taskId)"
  // Use attribute selector — clickEl() uses document.querySelector() which
  // does NOT support Playwright's :has-text() pseudo-selector.
  await clickEl('button[onclick^="rerunTask"]');

  // Wait for the task list to refresh (new task appears)
  await page.waitForFunction((count) => {
    return document.querySelectorAll('.task-item').length > count;
  }, taskCountBefore, { timeout: 10000 });

  // Verify task count increased
  const taskCountAfter = await page.evaluate(() => {
    return document.querySelectorAll('.task-item').length;
  });
  if (taskCountAfter <= taskCountBefore) fail('Task count should increase after re-run');
  console.log('PASS: Task count increased from', taskCountBefore, 'to', taskCountAfter);

  // Verify we navigated to the new task (detail view should show a different ID)
  const rerunNavOk = await page.evaluate((origId) => {
    const idEl = document.querySelector('.task-detail-id');
    if (!idEl) return false;
    // The ID element contains the task ID followed by a status badge
    const text = idEl.textContent.trim();
    const currentId = text.split(' ')[0]; // first token is the ID
    return currentId !== origId;
  }, taskId);
  if (!rerunNavOk) fail('Should navigate to new task after re-run');
  console.log('PASS: Navigated to new task after re-run');

  // Verify the new task has the same prompt
  const rerunPromptOk = await page.evaluate(() => {
    const promptEl = document.querySelector('.task-detail-prompt');
    return promptEl && promptEl.textContent.includes('Simulate tool calls');
  });
  if (!rerunPromptOk) fail('Re-run task should have the same prompt');
  console.log('PASS: Re-run task has same prompt');

  await takeScreenshot('10-rerun-task');

  // =====================================================================
  // Phase 10: Polling must NOT close open tool blocks
  // =====================================================================
  console.log('=== Phase 10: Polling preserves open tool-block state ===');

  // Navigate to task detail (where tool blocks are rendered)
  await page.goto(baseURL + '/task/' + taskId);
  await page.waitForLoadState('networkidle');
  await page.waitForSelector('.task-detail-body .tool-block', { timeout: 5000 });

  // Tool blocks are collapsed by default (Issue #46). Open one via click.
  await clickEl('.tool-block-header');

  // Verify the block is now open (body should have .open class)
  const blockOpen = await page.evaluate(() => {
    const body = document.querySelector('.tool-block-body');
    return body && body.classList.contains('open');
  });
  if (!blockOpen) fail('Tool block should be open after clicking header');
  console.log('PASS: Tool block open');

  // Trigger a refresh cycle (simulates the periodic /api/tasks poll).
  // Before the fix, this would call refreshTaskDetail() which rebuilds
  // the entire view and loses the open/closed state.
  await page.evaluate(() => refresh());

  // Allow the refresh to settle
  await page.waitForTimeout(1000);

  // Verify the tool block is STILL open — state must be preserved
  const blockStillOpen = await page.evaluate(() => {
    const body = document.querySelector('.tool-block-body');
    return body && body.classList.contains('open');
  });
  if (!blockStillOpen) fail('Tool block should remain open after polling refresh');
  console.log('PASS: Tool block state preserved after polling refresh');

  // Also verify the detail view is still intact (header, prompt, body)
  const detailIntact = await page.evaluate(() => {
    return document.querySelector('.task-detail-header') !== null &&
           document.querySelector('.task-detail-prompt') !== null &&
           document.querySelector('.task-detail-body .tool-block') !== null;
  });
  if (!detailIntact) fail('Task detail view should remain intact after refresh');
  console.log('PASS: Task detail view intact after refresh');

  await takeScreenshot('11-polling-state-preserved');

  // =====================================================================
  // Phase 11: Failed task — error reason displayed in task list and detail
  // =====================================================================
  console.log('=== Phase 11: Failed task error display ===');

  // Navigate to Tasks tab to see the failed task in the list
  await page.locator('.tab').filter({ hasText: 'Tasks' }).click();
  await page.waitForFunction(() => {
    return document.getElementById('tab-tasks').style.display === 'block';
  }, { timeout: 5000 });

  // Verify the failed task shows an error indicator in the task list
  const failedTaskInList = await page.evaluate((fid) => {
    const items = document.querySelectorAll('.task-item');
    for (const item of items) {
      const idEl = item.querySelector('.task-id');
      if (!idEl) continue;
      if (idEl.textContent.includes(fid)) {
        // Check for error indicator (⚠ with red text)
        const prompts = item.querySelectorAll('.task-prompt');
        for (const p of prompts) {
          if (p.textContent.includes('⚠')) {
            return { found: true, errorText: p.textContent.trim() };
          }
        }
        return { found: true, hasError: false };
      }
    }
    return { found: false };
  }, failedTaskId);

  if (!failedTaskInList.found) {
    fail('Failed task not found in task list');
  }
  if (!failedTaskInList.hasError && !failedTaskInList.errorText) {
    fail('Failed task should show error indicator (⚠) in task list');
  }
  console.log('PASS: Failed task shows error indicator in task list:', failedTaskInList.errorText || 'yes');

  await takeScreenshot('12-failed-task-list');

  // Click the failed task to see the detail view
  await page.evaluate((fid) => {
    const items = document.querySelectorAll('.task-item');
    for (const item of items) {
      const idEl = item.querySelector('.task-id');
      if (idEl && idEl.textContent.includes(fid)) {
        item.click();
        break;
      }
    }
  }, failedTaskId);

  // Wait for task detail to load
  await page.waitForSelector('.task-detail-header', { timeout: 5000 });

  // Verify the error banner is displayed in the detail view
  const failedTaskDetail = await page.evaluate((reason) => {
    const errorBanner = document.querySelector('.task-error');
    if (!errorBanner) return { hasBanner: false };
    const label = errorBanner.querySelector('.task-error-label')?.textContent?.trim();
    const text = errorBanner.querySelector('.task-error-text')?.textContent?.trim();
    return { hasBanner: true, label, text };
  }, failureReason);

  if (!failedTaskDetail.hasBanner) {
    fail('Failed task detail should show error banner (.task-error)');
    await takeScreenshot('13-failed-detail-no-banner');
    process.exit(1);
  }
  if (failedTaskDetail.label !== 'Failure Reason') {
    fail('Error banner label should be "Failure Reason", got: ' + failedTaskDetail.label);
  }
  if (!failedTaskDetail.text || !failedTaskDetail.text.includes('compilation failed')) {
    fail('Error banner should contain failure reason, got: ' + failedTaskDetail.text);
  }
  console.log('PASS: Failed task detail shows error banner with reason:', failedTaskDetail.text);

  await takeScreenshot('13-failed-task-detail');

  // =====================================================================
  // Phase 12: Priority badge for PENDING tasks
  // =====================================================================
  console.log('=== Phase 12: Priority badge ===');

  // Navigate to Tasks tab
  await page.locator('.tab').filter({ hasText: 'Tasks' }).click();
  await page.waitForFunction(() => {
    return document.getElementById('tab-tasks').style.display === 'block';
  }, { timeout: 5000 });

  // Ensure PENDING filter is active so the pending task is visible.
  // NOTE: return a serializable boolean — newer Playwright versions
  // serialize DOM elements returned from page.evaluate as ref strings.
  const pendingFilterInactive = await page.evaluate(() => {
    const btn = document.querySelector('button[data-status="PENDING"]');
    return btn !== null && btn.classList.contains('inactive');
  });
  if (pendingFilterInactive) {
    await clickEl('button[data-status="PENDING"]');
    await page.waitForTimeout(500);
  }

  // Verify the pending task is visible in the list
  const pendingTaskVisible = await page.evaluate((pid) => {
    const items = document.querySelectorAll('.task-item');
    for (const item of items) {
      if (item.textContent.includes(pid)) return true;
    }
    return false;
  }, pendingTaskId);
  if (!pendingTaskVisible) fail('Pending task should be visible in task list');
  console.log('PASS: Pending task visible in list');

  // Verify the priority badge is present on the pending task in the list
  // The badge has class "badge-priority" and shows 🔜{emoji}
  const priorityBadgeInList = await page.evaluate((pid) => {
    const items = document.querySelectorAll('.task-item');
    for (const item of items) {
      if (item.textContent.includes(pid)) {
        const badge = item.querySelector('.badge-priority');
        if (badge) return { found: true, text: badge.textContent.trim(), title: badge.title };
      }
    }
    return { found: false };
  }, pendingTaskId);
  if (!priorityBadgeInList.found) fail('Priority badge should be visible on pending task in list');
  console.log('PASS: Priority badge visible on pending task in list:', priorityBadgeInList.text);

  // Verify the priority badge is NOT present on non-pending tasks
  const noBadgeOnRunningTask = await page.evaluate((tid) => {
    const items = document.querySelectorAll('.task-item');
    for (const item of items) {
      if (item.textContent.includes(tid)) {
        const badge = item.querySelector('.badge-priority');
        return badge === null;
      }
    }
    return true; // task not found, pass
  }, taskId);
  if (!noBadgeOnRunningTask) fail('Priority badge should NOT be on non-pending tasks');
  console.log('PASS: Priority badge not present on running task');

  // Issue #266: orangutan is the lowest rung of the ladder, so the pending
  // row offers exactly one bump control — the up arrow towards teacher.
  const bumpButtonsInList = await page.evaluate((pid) => {
    const items = document.querySelectorAll('.task-item');
    for (const item of items) {
      if (item.textContent.includes(pid)) {
        const btns = [...item.querySelectorAll('.priority-bump')];
        return { count: btns.length, dirs: btns.map(b => b.dataset.dir).join(','), priority: btns[0] ? btns[0].dataset.priority : null };
      }
    }
    return { count: -1, dirs: '', priority: null };
  }, pendingTaskId);
  if (bumpButtonsInList.count !== 1) fail('Pending orangutan row should show exactly one bump button, got ' + bumpButtonsInList.count);
  if (bumpButtonsInList.dirs !== 'up') fail('Pending orangutan row should show only the up bump button, got: ' + bumpButtonsInList.dirs);
  if (bumpButtonsInList.priority !== 'orangutan') fail('Bump button should carry the current priority, got: ' + bumpButtonsInList.priority);
  console.log('PASS: Pending orangutan row shows only the up bump button');

  // Bump controls belong to pending tasks only.
  const noBumpOnRunningTask = await page.evaluate((tid) => {
    const items = document.querySelectorAll('.task-item');
    for (const item of items) {
      if (item.textContent.includes(tid)) {
        return item.querySelector('.priority-bump') === null;
      }
    }
    return true; // task not found, pass
  }, taskId);
  if (!noBumpOnRunningTask) fail('Bump buttons should NOT be on non-pending tasks');
  console.log('PASS: Bump buttons not present on running task');

  // Click into the pending task to see detail view
  await page.evaluate((pid) => {
    const items = document.querySelectorAll('.task-item');
    for (const item of items) {
      if (item.textContent.includes(pid)) {
        item.click();
        break;
      }
    }
  }, pendingTaskId);
  await page.waitForSelector('.task-detail-header', { timeout: 5000 });

  // Verify the priority badge is also present in the task detail header
  const priorityBadgeInDetail = await page.evaluate(() => {
    const header = document.querySelector('.task-detail-header');
    if (!header) return { found: false };
    const badge = header.querySelector('.badge-priority');
    if (badge) return { found: true, text: badge.textContent.trim(), title: badge.title };
    return { found: false };
  });
  if (!priorityBadgeInDetail.found) fail('Priority badge should be present in task detail header for pending task');
  console.log('PASS: Priority badge in detail header:', priorityBadgeInDetail.text);

  // Verify the priority badge is rendered with the expected format (🔜{emoji})
  if (!priorityBadgeInDetail.text.startsWith('🔜')) {
    fail('Priority badge should start with 🔜 prefix, got: ' + priorityBadgeInDetail.text);
  }
  console.log('PASS: Priority badge has correct format');

  await takeScreenshot('12-priority-badge');

  // Issue #266: bump the pending task along the ladder from the detail
  // header. orangutan -> teacher -> firefighter -> back down again,
  // asserting at each rung that the badge and the button set match the
  // ladder rule (top rung: down only; middle: both; bottom: up only).
  const detailBumpButtons = () => page.evaluate(() => {
    const header = document.querySelector('.task-detail-header');
    if (!header) return { count: -1, dirs: '', priority: null, badge: '' };
    const badge = header.querySelector('.badge-priority');
    const btns = [...header.querySelectorAll('.priority-bump')];
    return { count: btns.length, dirs: btns.map(b => b.dataset.dir).sort().join(','), priority: btns[0] ? btns[0].dataset.priority : null, badge: badge ? badge.textContent.trim() : '' };
  });
  const waitBadge = (emoji) => page.waitForFunction((e) => {
    const badge = document.querySelector('.task-detail-header .badge-priority');
    return badge && badge.textContent.includes(e);
  }, emoji, { timeout: 8000 });

  const detailBump0 = await detailBumpButtons();
  if (detailBump0.count !== 1 || detailBump0.dirs !== 'up') fail('Detail view of an orangutan task should show only the up bump button, got: ' + JSON.stringify(detailBump0));

  // Up: orangutan -> teacher (both buttons appear)
  await page.evaluate(() => document.querySelector('.task-detail-header .priority-bump-up').click());
  await waitBadge('🧑‍🏫'); // teacher
  const detailBump1 = await detailBumpButtons();
  if (detailBump1.count !== 2 || detailBump1.dirs !== 'down,up') fail('Detail view of a teacher task should show both bump buttons, got: ' + JSON.stringify(detailBump1));
  if (detailBump1.priority !== 'teacher') fail('Detail bump buttons should carry priority teacher, got: ' + detailBump1.priority);
  console.log('PASS: Bumping up moved orangutan -> teacher and shows both buttons');

  // Up: teacher -> firefighter (only the down button remains)
  await page.evaluate(() => document.querySelector('.task-detail-header .priority-bump-up').click());
  await waitBadge('🧑‍🚒'); // firefighter
  const detailBump2 = await detailBumpButtons();
  if (detailBump2.count !== 1 || detailBump2.dirs !== 'down') fail('Detail view of a firefighter task should show only the down bump button, got: ' + JSON.stringify(detailBump2));
  console.log('PASS: Bumping up moved teacher -> firefighter and shows only the down button');

  // Down: firefighter -> teacher (both buttons return)
  await page.evaluate(() => document.querySelector('.task-detail-header .priority-bump-down').click());
  await waitBadge('🧑‍🏫'); // teacher
  const detailBump3 = await detailBumpButtons();
  if (detailBump3.count !== 2 || detailBump3.dirs !== 'down,up') fail('Bumping down should restore both bump buttons, got: ' + JSON.stringify(detailBump3));
  console.log('PASS: Bumping down moved firefighter -> teacher and restores both buttons');

  // Down: teacher -> orangutan (only the up button remains)
  await page.evaluate(() => document.querySelector('.task-detail-header .priority-bump-down').click());
  await waitBadge('🦧'); // orangutan
  const detailBump4 = await detailBumpButtons();
  if (detailBump4.count !== 1 || detailBump4.dirs !== 'up') fail('Bumping down to orangutan should leave only the up button, got: ' + JSON.stringify(detailBump4));
  console.log('PASS: Bumping down returned to orangutan with only the up button');

  await takeScreenshot('12b-priority-bumped');

  // =====================================================================
  // Phase 13: Compaction task — compaction events rendered as a block
  // (issue #53)
  // =====================================================================
  console.log('=== Phase 13: Compaction block rendering ===');

  // --- Unit-style regression: compaction flushes a pending tool block ---
  // When a compaction event arrives while a tool block is still open, both
  // render paths must flush it with the full 8-argument
  // buildToolBlockHTML signature (argsJSON included). The compaction-flush
  // call sites once kept the pre-argsJSON 7-argument signature after a
  // rebase, which rendered the literal string "running" as the block's
  // output, dropped the real output and entry counter, and shifted the
  // timestamp to the epoch. The compaction capture contains no tool calls,
  // so the user-flow checks below cannot reach this branch — drive it
  // directly with a synthetic start → output → compaction-end sequence.
  const flushResult = await page.evaluate(() => {
    const startTs = '2026-09-15T10:00:00Z';
    const logs = [
      { level: 'tool', tool_type: 'start', tool_name: 'bash', tool_id: 'unit-flush-1',
        tool_args: 'command: ls', tool_args_json: '{"command":"ls"}',
        timestamp: startTs, line: '[TOOL_START] bash' },
      { level: 'tool', tool_type: 'output', tool_id: 'unit-flush-1',
        tool_output: 'real-output-here\n', timestamp: startTs,
        line: '[TOOL_OUTPUT] real-output-here' },
      { level: 'compaction', compaction_type: 'end', compaction_reason: 'manual',
        compaction_tokens_before: 100, compaction_tokens_after: 50,
        compaction_summary: 'unit summary', timestamp: startTs,
        line: '[COMPACTION_END] manual' },
    ];
    const task = { id: 'unit-flush-task', status: 'RUNNING', prompt: 'unit flush check' };

    function inspect(scope) {
      if (!scope) return { hasBlock: false, hasCompaction: false };
      const block = scope.querySelector('.tool-block');
      const pre = block ? block.querySelector('.tool-output pre') : null;
      const ts = block ? block.querySelector('.block-timestamp') : null;
      const counter = block ? block.querySelector('.block-counter') : null;
      const status = block ? block.querySelector('.tool-status') : null;
      return {
        hasBlock: !!block,
        output: pre ? pre.textContent.trim() : null,
        timestamp: ts ? ts.textContent.trim() : null,
        counter: counter ? counter.textContent.trim() : null,
        statusLabel: status ? status.textContent.trim() : null,
        hasCompaction: scope.querySelector('.compaction-block') !== null,
      };
    }

    const out = {};
    // Task-detail render path (renderTaskDetail)
    renderTaskDetail(task, logs, logs.length);
    out.detail = inspect(document.querySelector('#log-view .task-detail-body'));
    // Log-entries render path (renderLogEntries)
    renderLogEntries('2026-09-15', 'unit-flush-task', logs, logs.length);
    out.entries = inspect(document.querySelector('#log-entries-list'));
    out.expectedTimestamp = formatTimestamp(startTs);
    return out;
  });

  const flushChecks = [];
  for (const [pathName, r] of [['detail', flushResult.detail], ['entries', flushResult.entries]]) {
    flushChecks.push(
      { name: pathName + ': pending tool block flushed before compaction block', pass: r.hasBlock && r.hasCompaction },
      { name: pathName + ': flushed output is the real output, not "running"', pass: r.output === 'real-output-here' },
      { name: pathName + ': flushed status label is done', pass: r.statusLabel === 'done' },
      { name: pathName + ': flushed timestamp is the entry timestamp, not the epoch', pass: r.timestamp === flushResult.expectedTimestamp },
      { name: pathName + ': flushed entry counter is 2 (start + output)', pass: r.counter === '2' },
    );
  }
  let flushFailed = false;
  for (const check of flushChecks) {
    if (!check.pass) { console.error('FAIL:', check.name); flushFailed = true; }
    else { console.log('PASS:', check.name); }
  }
  if (flushFailed) {
    console.error('flush detail result:', JSON.stringify(flushResult.detail));
    console.error('flush entries result:', JSON.stringify(flushResult.entries));
    await takeScreenshot('13-compaction-flush-failed');
    process.exit(1);
  }
  await takeScreenshot('13-compaction-flush');

  // =====================================================================
  // Phase 13b: ⏳ hourglass spinner for running tasks (issue #210)
  // Unit-style: the task detail header shows a spinner rotated by the log
  // count modulo 360 degrees; streaming one more entry advances both the
  // counter and the rotation in place; a settled task has no spinner.
  // =====================================================================
  console.log('=== Phase 13b: hourglass spinner ===');

  // Step 1: spinnerHTML unit behaviour + a RUNNING task detail header with
  // 375 entries -> spinner present, rotated 375 %% 360 = 15 degrees.
  const spinnerRender = await page.evaluate(() => {
    const mkLogs = n => Array.from({ length: n }, (_, i) => ({
      level: 'info', line: 'spinner line ' + i, timestamp: '2026-09-15T10:00:00Z',
    }));
    const out = {};
    // spinnerHTML: angle = log count modulo 360; nothing for non-running tasks.
    out.helper375 = spinnerHTML(375, true);
    out.helperNotRunning = spinnerHTML(10, false);
    const runningTask = { id: 'unit-spinner-task', status: 'RUNNING', prompt: 'spinner check', assigned_to: 'guest-1' };
    const logs = mkLogs(375);
    renderTaskDetail(runningTask, logs, logs.length);
    // Seed the global accumulator the streaming path reads, mirroring the
    // WebSocket handler which pushes the entry before calling appendLogToDetail.
    taskLogs[runningTask.id] = logs.slice();
    const meta = document.querySelector('.task-detail-meta');
    const countEl = meta.querySelector('.meta-log-count');
    out.headerCount = countEl ? countEl.textContent : null;
    const sp = meta.querySelector('.task-spinner');
    out.headerTransform = sp ? sp.style.transform : null;
    return out;
  });
  await takeScreenshot('13b-spinner-running');

  // Step 2: streaming path — one more entry arrives via appendLogToDetail;
  // the counter and the spinner rotation must both advance in place.
  const spinnerStream = await page.evaluate(() => {
    const out = {};
    taskLogs['unit-spinner-task'].push({ level: 'info', line: 'spinner line 375', timestamp: '2026-09-15T10:00:01Z' });
    const spBefore = document.querySelector('.task-detail-meta .task-spinner');
    appendLogToDetail({ task_id: 'unit-spinner-task', level: 'info', line: 'spinner line 375', timestamp: '2026-09-15T10:00:01Z' });
    const spAfter = document.querySelector('.task-detail-meta .task-spinner');
    out.sameNode = spBefore === spAfter;
    const countEl = document.querySelector('.task-detail-meta .meta-log-count');
    out.count = countEl ? countEl.textContent : null;
    out.transform = spAfter ? spAfter.style.transform : null;
    return out;
  });

  // Step 3: a settled task renders no spinner at all.
  const spinnerSettled = await page.evaluate(() => {
    const mkLogs = n => Array.from({ length: n }, (_, i) => ({
      level: 'info', line: 'spinner line ' + i, timestamp: '2026-09-15T10:00:00Z',
    }));
    renderTaskDetail({ id: 'unit-spinner-done', status: 'COMPLETED', prompt: 'spinner check' }, mkLogs(5), 5);
    return { hasSpinner: document.querySelector('.task-detail-meta .task-spinner') !== null };
  });

  const spinnerChecks = [
    { name: 'spinnerHTML keys the angle off the count (375 -> rotate(15deg))', pass: spinnerRender.helper375.includes('⏳') && spinnerRender.helper375.includes('rotate(15deg)') },
    { name: 'spinnerHTML renders nothing for non-running tasks', pass: spinnerRender.helperNotRunning === '' },
    { name: 'running task header counts 375 log entries', pass: spinnerRender.headerCount === '375 log entries' },
    { name: 'running task header spinner rotated by 375 %% 360 = 15 degrees', pass: spinnerRender.headerTransform === 'rotate(15deg)' },
    { name: 'streaming entry advances the count to 376', pass: spinnerStream.count === '376 log entries' },
    { name: 'streaming entry advances rotation to 16 degrees in the same element', pass: spinnerStream.sameNode && spinnerStream.transform === 'rotate(16deg)' },
    { name: 'settled task header has no spinner', pass: !spinnerSettled.hasSpinner },
  ];
  let spinnerFailed = false;
  for (const check of spinnerChecks) {
    if (!check.pass) { console.error('FAIL:', check.name); spinnerFailed = true; }
    else { console.log('PASS:', check.name); }
  }
  if (spinnerFailed) {
    console.error('spinner render result:', JSON.stringify(spinnerRender));
    console.error('spinner stream result:', JSON.stringify(spinnerStream));
    console.error('spinner settled result:', JSON.stringify(spinnerSettled));
    await takeScreenshot('13b-spinner-failed');
    process.exit(1);
  }
  console.log('PASS: hourglass spinner rotation keyed off log count');
  await takeScreenshot('13b-spinner-settled');

  // =====================================================================
  // Phase 13e: overview hourglass rotation keyed off activity (issue #246)
  // The /tasks/ overview spinner spins smoothly while a task's log count
  // changes between frontend updates, pauses at its current angle when the
  // count is unchanged, and resumes on the next change. The within-task
  // detail spinner (phase 13b) is deliberately unchanged.
  // =====================================================================
  console.log('=== Phase 13e: overview activity-driven hourglass ===');

  // Show the Tasks tab so the overview list is visible for the DOM checks
  // and screenshots below.
  await page.locator('.tab').filter({ hasText: 'Tasks' }).click();
  await page.waitForFunction(() => document.getElementById('tab-tasks').style.display === 'block', { timeout: 5000 });

  const listSpinnerUnit = await page.evaluate(() => {
    const out = {};
    const uid = 'unit-list-spinner-task';
    const T = (count, status) => ({ id: uid, status: status || 'RUNNING', prompt: 'p', created_at: '2026-09-15T10:00:00Z', log_count: count });

    // State machine, driven directly the way the poller/WebSocket would.
    listSpinners.clear();
    updateListSpinners([T(10)]);
    out.restOnFirstSeen = listSpinners.get(uid).spinning === false && listSpinners.get(uid).angle === 0;
    updateListSpinners([T(11)]);
    out.spinsOnChange = listSpinners.get(uid).spinning === true;
    const a1 = listSpinners.get(uid).angle;
    tickListSpinners();
    out.ticksWhileActive = listSpinners.get(uid).angle === a1 + LIST_SPINNER_DEG_PER_TICK;
    updateListSpinners([T(11)]);
    out.pausesWhenUnchanged = listSpinners.get(uid).spinning === false;
    tickListSpinners();
    out.holdsAngleWhenPaused = listSpinners.get(uid).angle === a1 + LIST_SPINNER_DEG_PER_TICK;
    updateListSpinners([T(12)]);
    out.resumesOnNextChange = listSpinners.get(uid).spinning === true;
    updateListSpinners([{ id: uid, status: 'COMPLETED', prompt: 'p', log_count: 12 }]);
    out.prunedWhenSettled = !listSpinners.has(uid);

    // DOM: the overview renders the spinner at the angle held in state, the
    // ticker moves the live element between renders, and a re-render at an
    // unchanged count must not snap the spinner back to its old position.
    const domId = 'unit-list-spinner-dom';
    const D = (count) => ({ id: domId, status: 'RUNNING', prompt: 'dom spinner check', created_at: '2026-09-15T10:00:00Z', log_count: count });
    renderTasks({ tasks: [D(5)] });
    const el1 = document.querySelector('.task-item .list-spinner');
    out.domRendered = !!el1 && el1.textContent === '⏳';
    out.domStartsAtStateAngle = !!el1 && el1.style.transform === 'rotate(' + listSpinners.get(domId).angle + 'deg)';
    renderTasks({ tasks: [D(6)] });
    const angleBefore = listSpinners.get(domId).angle;
    tickListSpinners();
    const el2 = document.querySelector('.task-item .list-spinner');
    out.domTickerMovesElement = !!el2 && el2.style.transform === 'rotate(' + (angleBefore + LIST_SPINNER_DEG_PER_TICK) + 'deg)';
    const liveAngle = listSpinners.get(domId).angle;
    renderTasks({ tasks: [D(6)] });
    const el3 = document.querySelector('.task-item .list-spinner');
    out.domRerenderHoldsAngle = !!el3 && el3.style.transform === 'rotate(' + liveAngle + 'deg)';

    // The angle must grow monotonically rather than folding at every
    // revolution: CSS interpolates rotate() numerically, so a 357deg -> 0deg
    // fold would animate backwards through 357 degrees over one tick.
    const foldId = 'unit-list-spinner-fold';
    const F = (count) => ({ id: foldId, status: 'RUNNING', prompt: 'p', created_at: '2026-09-15T10:00:00Z', log_count: count });
    listSpinners.clear();
    if (listSpinnerTimer) { clearInterval(listSpinnerTimer); listSpinnerTimer = null; }
    updateListSpinners([F(1)]);
    updateListSpinners([F(2)]);
    let monotonic = true;
    let prevAngle = listSpinners.get(foldId).angle;
    // More than a full revolution of ticks, so any fold at 360deg is hit.
    const foldTicks = Math.floor(400 / LIST_SPINNER_DEG_PER_TICK);
    for (let i = 0; i < foldTicks; i++) {
        tickListSpinners();
        const angle = listSpinners.get(foldId).angle;
        if (angle <= prevAngle) monotonic = false;
        prevAngle = angle;
    }
    out.angleNeverSweepsBackwards = monotonic && listSpinners.get(foldId).angle === foldTicks * LIST_SPINNER_DEG_PER_TICK;

    // A running task the status filter hides renders no hourglass, so it must
    // not keep the 50ms ticker alive with nothing to animate.
    const hidId = 'unit-list-spinner-hidden';
    const H = (count) => ({ id: hidId, status: 'RUNNING', prompt: 'hidden spinner check', created_at: '2026-09-15T10:00:00Z', log_count: count });
    const savedRunningFilter = taskFilter.RUNNING;
    listSpinners.clear();
    if (listSpinnerTimer) { clearInterval(listSpinnerTimer); listSpinnerTimer = null; }
    updateListSpinners([H(1)]);
    updateListSpinners([H(2)]);
    out.tickerRunsWhileVisibleTaskActive = listSpinners.get(hidId).spinning === true && listSpinnerTimer !== null;
    taskFilter.RUNNING = false;
    renderTasks({ tasks: [H(3)] });
    out.hiddenTaskRendersNoSpinner = document.querySelector('.list-spinner[data-task-id="' + hidId + '"]') === null;
    out.hiddenTaskNotSpinning = listSpinners.get(hidId).spinning === false;
    tickListSpinners();
    out.tickerStopsWhenNothingVisible = listSpinnerTimer === null;
    taskFilter.RUNNING = savedRunningFilter;
    // Rendering the task again through the real path both resumes the spin
    // (its count changed while hidden) and leaves a visible hourglass in the
    // DOM for the screenshot below.
    renderTasks({ tasks: [H(4)] });
    out.visibleTaskResumesSpin = listSpinners.get(hidId).spinning === true;

    // Remove the fake entries so they cannot leak into later phases.
    listSpinners.clear();
    if (listSpinnerTimer) { clearInterval(listSpinnerTimer); listSpinnerTimer = null; }
    return out;
  });
  await takeScreenshot('13e-list-spinner');

  const listSpinnerChecks = [
    { name: 'overview spinner rests on first sighting of a task', pass: listSpinnerUnit.restOnFirstSeen },
    { name: 'count change between updates starts the spin', pass: listSpinnerUnit.spinsOnChange },
    { name: 'ticker advances the angle while active', pass: listSpinnerUnit.ticksWhileActive },
    { name: 'unchanged count pauses the spinner', pass: listSpinnerUnit.pausesWhenUnchanged },
    { name: 'paused spinner holds its angle across ticks', pass: listSpinnerUnit.holdsAngleWhenPaused },
    { name: 'next change resumes the spin', pass: listSpinnerUnit.resumesOnNextChange },
    { name: 'settled task state is pruned', pass: listSpinnerUnit.prunedWhenSettled },
    { name: 'overview renders the ⏳ list spinner', pass: listSpinnerUnit.domRendered },
    { name: 'rendered spinner starts at the state angle', pass: listSpinnerUnit.domStartsAtStateAngle },
    { name: 'ticker moves the live element between renders', pass: listSpinnerUnit.domTickerMovesElement },
    { name: 're-render at unchanged count keeps the live angle', pass: listSpinnerUnit.domRerenderHoldsAngle },
    { name: 'angle grows past a revolution without sweeping backwards', pass: listSpinnerUnit.angleNeverSweepsBackwards },
    { name: 'ticker runs while a visible task is active', pass: listSpinnerUnit.tickerRunsWhileVisibleTaskActive },
    { name: 'filtered-out task renders no spinner', pass: listSpinnerUnit.hiddenTaskRendersNoSpinner },
    { name: 'filtered-out task is not marked spinning', pass: listSpinnerUnit.hiddenTaskNotSpinning },
    { name: 'ticker stops when nothing rendered is turning', pass: listSpinnerUnit.tickerStopsWhenNothingVisible },
    { name: 'task visible again resumes the spin', pass: listSpinnerUnit.visibleTaskResumesSpin },
  ];
  let listSpinnerFailed = false;
  for (const check of listSpinnerChecks) {
    if (!check.pass) { console.error('FAIL:', check.name); listSpinnerFailed = true; }
    else { console.log('PASS:', check.name); }
  }
  if (listSpinnerFailed) {
    console.error('list spinner result:', JSON.stringify(listSpinnerUnit));
    await takeScreenshot('13e-list-spinner-failed');
    process.exit(1);
  }
  console.log('PASS: overview hourglass rotates while the log count changes');

  // The unit DOM checks replaced the task list with a fake task; restore the
  // real list before the following phases interact with it.
  await page.evaluate(() => refresh());
  await page.waitForFunction(() => document.querySelectorAll('.task-item').length > 0, { timeout: 5000 });
  await takeScreenshot('13e-list-spinner-restored');

  // Navigate to Tasks tab
  await page.locator('.tab').filter({ hasText: 'Tasks' }).click();
  await page.waitForFunction(() => {
    return document.getElementById('tab-tasks').style.display === 'block';
  }, { timeout: 5000 });

  // Click the compaction task
  await page.evaluate((cid) => {
    const items = document.querySelectorAll('.task-item');
    for (const item of items) {
      const idEl = item.querySelector('.task-id');
      if (idEl && idEl.textContent.includes(cid)) {
        item.click();
        break;
      }
    }
  }, compactionTaskId);

  // Wait for the compaction block to render, then let any pending
  // WebSocket updates settle.
  await page.waitForSelector('.compaction-block', { timeout: 5000 });
  await page.waitForTimeout(500);

  await takeScreenshot('13-compaction-task-detail');

  const compactionChecks = await page.evaluate(() => {
    const results = {};
    const blocks = document.querySelectorAll('.compaction-block');
    results.blockCount = blocks.length;
    if (blocks.length > 0) {
      const block = blocks[0];
      const header = block.querySelector('.compaction-block-header');
      results.headerText = header ? header.textContent : '';
      const reason = block.querySelector('.compaction-reason');
      results.reason = reason ? reason.textContent.trim() : null;
      const status = block.querySelector('.compaction-status');
      results.status = status ? status.textContent.trim() : null;
      const tokens = block.querySelector('.compaction-tokens');
      results.tokens = tokens ? tokens.textContent.trim() : null;
      const counter = block.querySelector('.block-counter');
      results.counter = counter ? counter.textContent.trim() : null;
      const body = block.querySelector('.compaction-block-body');
      results.bodyOpen = body ? body.classList.contains('open') : false;
      const summaryPre = block.querySelector('.compaction-block-body pre');
      results.hasSummaryPre = !!summaryPre;
      results.summaryText = summaryPre ? summaryPre.textContent : '';
    }
    // The raw [COMPACTION_*] lines must not leak as plain-text log lines.
    const leaked = Array.from(document.querySelectorAll('.log-msg'))
      .map(el => el.textContent)
      .filter(t => t.includes('[COMPACTION_START]') || t.includes('[COMPACTION_END]'));
    results.leakedPlainLines = leaked.length;
    return results;
  });

  if (compactionChecks.blockCount !== 1) {
    fail('Expected exactly 1 compaction block, got ' + compactionChecks.blockCount);
  }
  if (!compactionChecks.headerText.includes('Compaction')) {
    fail('Compaction block header should contain "Compaction"');
  }
  if (compactionChecks.reason !== 'manual') {
    fail('Compaction reason should be "manual", got: ' + compactionChecks.reason);
  }
  if (compactionChecks.status !== 'completed') {
    fail('Compaction status should be "completed", got: ' + compactionChecks.status);
  }
  if (!compactionChecks.tokens || !/^\d+ → \d+ tokens$/.test(compactionChecks.tokens)) {
    fail('Compaction token stats should be "<before> → <after> tokens", got: ' + compactionChecks.tokens);
  }
  if (compactionChecks.counter !== '2') {
    fail('Compaction block counter should be 2 (start + end entries), got: ' + compactionChecks.counter);
  }
  if (!compactionChecks.hasSummaryPre) {
    fail('Compaction summary should be inside a <pre> in the block body');
  }
  if (!compactionChecks.summaryText.includes('## Goal')) {
    fail('Compaction summary should contain the generated summary (## Goal)');
  }
  if (!compactionChecks.bodyOpen) {
    fail('Completed compaction block body should be open by default');
  }
  if (compactionChecks.leakedPlainLines !== 0) {
    fail('[COMPACTION_*] lines leaked as plain text: ' + compactionChecks.leakedPlainLines);
  }
  console.log('PASS: Compaction block rendered with reason, status, tokens and summary');

  // =====================================================================
  // Phase 13c: Task title in the drill-down header (issue #229)
  // =====================================================================
  console.log('=== Phase 13c: Task titles in drill-down ===');

  // Back to the Tasks tab, then follow the real user journey: click the
  // title+URL task item, which drives selectTask() → refreshTaskDetail()
  // → renderTaskDetail().
  await page.locator('.tab').filter({ hasText: 'Tasks' }).click();
  await page.waitForFunction(() => document.getElementById('tab-tasks').style.display === 'block', null, { timeout: 5000 });
  await page.evaluate((tid) => {
    const item = document.querySelector('.task-item[onclick*="' + tid + '"]');
    if (!item) throw new Error('title+URL task item not found');
    item.click();
  }, titleUrlTaskId);
  await page.waitForSelector('.task-detail-id .task-title a', { timeout: 10000 });
  await page.waitForTimeout(500);

  const detailUrlChecks = await page.evaluate((d) => {
    const idLine = document.querySelector('.task-detail-id');
    const el = idLine ? idLine.querySelector('.task-title') : null;
    const a = el ? el.querySelector('a') : null;
    const cs = el ? getComputedStyle(el) : null;
    return {
      found: !!el,
      text: el ? el.textContent : null,
      hasAnchor: !!a,
      href: a ? a.getAttribute('href') : null,
      target: a ? a.getAttribute('target') : null,
      rel: a ? a.getAttribute('rel') : null,
      noRawHTML: el ? el.querySelector('b, script') === null : false,
      beforeId: idLine ? idLine.textContent.indexOf(el.textContent) < idLine.textContent.indexOf(d.taskId) : false,
      notTruncated: cs ? (cs.overflow !== 'hidden' && cs.textOverflow !== 'ellipsis' && cs.whiteSpace !== 'nowrap') : false,
    };
  }, { taskId: titleUrlTaskId });

  if (!detailUrlChecks.found) fail('detail header should show the task title');
  if (detailUrlChecks.text !== titleUrlTaskTitle) fail('detail title text should equal the raw title, got: ' + detailUrlChecks.text);
  if (!detailUrlChecks.hasAnchor) fail('detail title with a URL should render as an anchor');
  if (detailUrlChecks.href !== titleUrlTaskUrl) fail('detail title anchor href should be the task URL, got: ' + detailUrlChecks.href);
  if (detailUrlChecks.target !== '_blank') fail('detail title anchor should open in a new tab, got target: ' + detailUrlChecks.target);
  if (!detailUrlChecks.rel || !detailUrlChecks.rel.includes('noopener')) fail('detail title anchor rel should include noopener, got: ' + detailUrlChecks.rel);
  if (!detailUrlChecks.noRawHTML) fail('detail title must be rendered as text, not raw HTML');
  if (!detailUrlChecks.beforeId) fail('detail title should render in front of the task ID');
  if (!detailUrlChecks.notTruncated) fail('drill-down should show the title in full, not truncated');
  console.log('PASS: drill-down shows the full linked title ahead of the ID');

  await takeScreenshot('13c-task-title-detail');

  // Now the title-only task: plain text in the drill-down, no anchor.
  await page.locator('.tab').filter({ hasText: 'Tasks' }).click();
  await page.waitForFunction(() => document.getElementById('tab-tasks').style.display === 'block', null, { timeout: 5000 });
  await page.evaluate((tid) => {
    const item = document.querySelector('.task-item[onclick*="' + tid + '"]');
    if (!item) throw new Error('title-only task item not found');
    item.click();
  }, titleOnlyTaskId);
  await page.waitForSelector('.task-detail-id .task-title', { timeout: 10000 });
  await page.waitForTimeout(500);

  const detailOnlyChecks = await page.evaluate(() => {
    const idLine = document.querySelector('.task-detail-id');
    const el = idLine ? idLine.querySelector('.task-title') : null;
    return {
      found: !!el,
      text: el ? el.textContent : null,
      hasAnchor: !!el && el.querySelector('a') !== null,
    };
  });
  if (!detailOnlyChecks.found) fail('detail header should show the title-only task title');
  if (detailOnlyChecks.text !== titleOnlyTaskTitle) fail('detail title-only text mismatch, got: ' + detailOnlyChecks.text);
  if (detailOnlyChecks.hasAnchor) fail('detail title without a URL must render as plain text, not a link');
  console.log('PASS: drill-down renders a URL-less title as plain text');

  await takeScreenshot('13c-task-title-only-detail');

  // Return to the Tasks tab so later phases start from the same state.
  await page.locator('.tab').filter({ hasText: 'Tasks' }).click();
  await page.waitForFunction(() => document.getElementById('tab-tasks').style.display === 'block', null, { timeout: 5000 });

  // =====================================================================
  // Phase 13d: Guest host and UID (issue #238)
  // =====================================================================
  // The guests list must show the UID a task is assigned to (not just its
  // display name), and every view of an assignment must name the machine the
  // guest is running on.
  console.log('=== Phase 13d: Guest host and UID ===');

  const guestCard = await page.evaluate(() => {
    const item = document.querySelector('#guest-list .guest-item');
    return item ? item.textContent : null;
  });
  if (!guestCard) fail('guests list rendered no guest card');
  if (!guestCard || !guestCard.includes('ui-test-guest')) {
    fail('guest card does not show the guest UID: ' + guestCard);
  }
  if (!guestCard || !guestCard.includes('ui-test-host')) {
    fail('guest card does not show the host the guest sits on: ' + guestCard);
  }
  console.log('PASS: guests list shows the UID and the host it sits on');

  // Check-in latency (issue #240): every guest card carries a "how long
  // since the last heartbeat" row, and the list order matches the server's
  // (sorted) order rather than flicking with map iteration. Heartbeat once
  // over a throwaway WS connection so the age assertion isn't at the mercy
  // of how long the earlier phases took, then wait for the next 5s poll to
  // pick the fresh age up.
  await page.evaluate((wsURL) => new Promise((resolve, reject) => {
    const ws = new WebSocket(wsURL);
    ws.onopen = () => ws.send(JSON.stringify({ jsonrpc: '2.0', id: 9990, method: 'guest.heartbeat', params: { id: 'ui-test-guest' } }));
    ws.onmessage = () => { ws.close(); resolve(); };
    ws.onerror = () => reject(new Error('heartbeat ws failed'));
    setTimeout(() => reject(new Error('heartbeat timed out')), 5000);
  }), baseURL.replace(/^http/, 'ws') + '/ws');
  await page.waitForFunction(() => {
    const row = document.querySelector('#guest-list .guest-item .guest-latency');
    return row && getComputedStyle(row).color === 'rgb(100, 116, 139)';
  }, null, { timeout: 15000 });
  const latencyRow = await page.evaluate(() => {
    const row = document.querySelector('#guest-list .guest-item .guest-latency');
    return row ? { text: row.textContent, colour: getComputedStyle(row).color } : null;
  });
  if (!latencyRow || !/\d+/.test(latencyRow.text) || !latencyRow.text.includes('ago')) {
    fail('guest card shows no heartbeat-age row: ' + JSON.stringify(latencyRow));
  }
  const orderMatches = await page.evaluate(async () => {
    const res = await fetch('/api/guests');
    const data = await res.json();
    const apiOrder = data.guests.map(g => g.id);
    const domOrder = Array.from(document.querySelectorAll('#guest-list .guest-item .guest-uid'))
      .map(el => el.textContent.trim());
    // Guests with no display name show the UID in the heading instead.
    const shown = domOrder.length ? domOrder : Array.from(document.querySelectorAll('#guest-list .guest-item .guest-name'))
      .map(el => el.textContent.replace(/[\u25cf\u00b7\s]+/g, '').trim());
    return JSON.stringify(shown.slice(0, apiOrder.length)) === JSON.stringify(apiOrder);
  });
  if (!orderMatches) fail('guests list order does not match the server list order');
  console.log('PASS: guest card shows heartbeat age; list order is stable and matches the API');

  // Colour tiers (issue #240): orange once a heartbeat is missed (2x the
  // interval), red at half the silence timeout. Injected renderGuests calls
  // are the only way to force an aged heartbeat without sleeping for
  // minutes; the list is restored straight after, as with the nameless
  // guest below.
  const latencyTiers = await page.evaluate(() => {
    const list = document.getElementById('guest-list');
    const keep = list.innerHTML;
    try {
      const probe = age => {
        renderGuests({ guests: [{ id: 'tier-guest', name: '', state: 'IDLE', tags: [], heartbeat_age_seconds: age }], heartbeat_interval: 30, silence_timeout: 180 });
        const row = document.querySelector('#guest-list .guest-latency');
        return getComputedStyle(row).color;
      };
      updateGuestLatencyThresholds({ heartbeat_interval: 30, silence_timeout: 180 });
      return { ok: probe(5), warn: probe(70), crit: probe(95) };
    } finally {
      list.innerHTML = keep;
    }
  });
  if (latencyTiers.ok !== 'rgb(100, 116, 139)') fail('latency tier ok: expected grey, got ' + latencyTiers.ok);
  if (latencyTiers.warn !== 'rgb(245, 158, 11)') fail('latency tier warn: expected orange, got ' + latencyTiers.warn);
  if (latencyTiers.crit !== 'rgb(239, 68, 68)') fail('latency tier crit: expected red, got ' + latencyTiers.crit);
  console.log('PASS: latency text turns orange past 2x interval and red past half the silence timeout');

  // A guest with no display name must not have its UID printed twice. The
  // sidebar is restored straight after so later phases see the real list.
  const nameless = await page.evaluate(() => {
    const list = document.getElementById('guest-list');
    const keep = list.innerHTML;
    try {
      renderGuests({ guests: [{ id: 'nameless-guest', name: '', state: 'IDLE', tags: [], host: 'ui-test-host' }] });
      const item = document.querySelector('#guest-list .guest-item');
      return { text: item ? item.textContent : '' };
    } finally {
      list.innerHTML = keep;
    }
  });
  const uidCount = (nameless.text.match(/nameless-guest/g) || []).length;
  if (uidCount !== 1) {
    fail('nameless guest prints its UID ' + uidCount + ' times: ' + JSON.stringify(nameless.text));
  }
  console.log('PASS: a guest without a display name prints its UID once');

  // Task list rows: an assigned task reads "→ uid @ host".
  const listNamesHost = await page.evaluate(() =>
    Array.from(document.querySelectorAll('.task-item .task-meta'))
      .some(el => el.textContent.includes('ui-test-guest @ ui-test-host'))
  );
  if (!listNamesHost) fail('no task list row names the guest host');
  console.log('PASS: task list rows name the guest host');

  // Drill into a task assigned to that guest — the detail header carries the
  // same "uid @ host" label.
  await page.evaluate(() => {
    const rows = Array.from(document.querySelectorAll('.task-item'));
    const row = rows.find(item => {
      const meta = item.querySelector('.task-meta');
      return meta && meta.textContent.includes('ui-test-guest @ ui-test-host');
    });
    if (!row) throw new Error('no task row showing a guest host');
    row.click();
  });
  await page.waitForSelector('.task-detail-meta', { timeout: 10000 });
  await page.waitForTimeout(500);

  const detailMeta = await page.evaluate(() => {
    const el = document.querySelector('.task-detail-meta');
    return el ? el.textContent : null;
  });
  if (!detailMeta || !detailMeta.includes('Guest: ui-test-guest @ ui-test-host')) {
    fail('task detail does not name the guest host: ' + detailMeta);
  }
  console.log('PASS: task detail names the guest host');

  // The polling path patches the header meta in place (it must not lose the
  // host, nor the log count it exists to update).
  await page.evaluate(async (tid) => { await patchTaskDetailMeta(tid); }, taskId);
  await page.waitForTimeout(500);
  const patchedMeta = await page.evaluate(() => {
    const el = document.querySelector('.task-detail-meta');
    return el ? el.textContent : null;
  });
  if (!patchedMeta || !patchedMeta.includes('ui-test-guest @ ui-test-host')) {
    fail('polled task detail meta lost the guest host: ' + patchedMeta);
  }
  if (!patchedMeta || !/log entries/.test(patchedMeta)) {
    fail('polled task detail meta lost the log count: ' + patchedMeta);
  }
  console.log('PASS: polled task detail meta keeps the guest host');

  await takeScreenshot('13d-guest-host');

  // =====================================================================
  // Phase 13f: Search task output from the web UI (issue #270)
  // =====================================================================
  // The search box is a thin client over GET /api/tasks/search: the page
  // supplies a query and a lookback window and the server decides what that
  // window may be. The journey below is the one a human follows — open the
  // Search tab, type, search, click a hit — with no DOM injection.
  console.log('=== Phase 13f: Task output search ===');

  const searchPageErrors = [];
  page.on('pageerror', err => searchPageErrors.push(err.message));

  await page.goto(baseURL + '/tasks');
  await page.waitForLoadState('networkidle');

  // Opening the Search tab shows the form, and the lookback control
  // defaults to the server's own default of 5 days.
  const searchForm = await page.evaluate(() => {
    const tab = Array.from(document.querySelectorAll('.tab'))
      .find(t => t.textContent.trim() === 'Search');
    if (!tab) return null;
    tab.click();
    return {
      active: tab.classList.contains('active'),
      panelShown: document.getElementById('tab-search').style.display !== 'none',
      days: document.getElementById('search-days').value,
      hasQueryField: document.getElementById('search-query') !== null,
      taskPanelHidden: document.getElementById('tab-tasks').style.display === 'none',
    };
  });
  if (!searchForm) fail('Search tab should be present in the tab bar');
  if (!searchForm.active) fail('Search tab should be active after clicking it');
  if (!searchForm.panelShown) fail('Search panel should be shown after clicking the tab');
  if (!searchForm.taskPanelHidden) fail('Tasks panel should be hidden on the Search tab');
  if (!searchForm.hasQueryField) fail('Search form should carry a query field');
  if (searchForm.days !== '5') fail('lookback should default to 5 days, got: ' + searchForm.days);
  console.log('PASS: Search tab renders with a 5-day default lookback');

  // An empty query must not reach the server at all.
  await page.fill('#search-query', '');
  await clickEl('#search-go');
  const emptyQuery = await page.evaluate(() => ({
    text: document.getElementById('search-results').textContent,
    url: window.location.pathname,
  }));
  if (!emptyQuery.text.toLowerCase().includes('query')) {
    fail('empty query should ask for a query, got: ' + emptyQuery.text);
  }
  if (emptyQuery.url !== '/search') fail('empty query should still be on /search, got: ' + emptyQuery.url);
  console.log('PASS: empty query asks for one instead of searching');
  await takeScreenshot('13f-search-empty-query');

  // Searching for a string present in a recent task's output returns that
  // task. The failed task's ID is the natural needle: it is unique per run,
  // it appears in that task's own trace ("Executing task <id>", its clone
  // path), and no other task's trace can contain it — so the match set is
  // exactly one task's hits.
  await page.fill('#search-query', failedTaskId);
  await clickEl('#search-go');
  await page.waitForSelector('#search-results .search-match', { timeout: 10000 });
  const searchChecks = await page.evaluate((needle) => {
    const rows = Array.from(document.querySelectorAll('#search-results .search-match'));
    const first = rows[0];
    return {
      rows: rows.length,
      foreignTasks: rows.filter(r => r.dataset.taskId !== needle).length,
      firstHasDate: /\d{4}-\d{2}-\d{2}/.test(first.querySelector('.search-match-date').textContent),
      firstHasStatus: first.querySelector('.search-match-status') !== null,
      contextLines: first.querySelectorAll('.search-match-line').length,
      marks: first.querySelectorAll('.search-match-line mark').length,
      markText: first.querySelector('.search-match-line mark')
        ? first.querySelector('.search-match-line mark').textContent : '',
      url: window.location.pathname + window.location.search,
    };
  }, failedTaskId);
  if (searchChecks.rows === 0) fail('search should return at least one match');
  if (searchChecks.foreignTasks !== 0) {
    fail('every match should belong to the searched-for task, ' + searchChecks.foreignTasks + ' did not');
  }
  if (!searchChecks.firstHasDate) fail('first match should carry the date it was found on');
  if (!searchChecks.firstHasStatus) fail('first match should carry the task status');
  if (searchChecks.contextLines < 2) {
    fail('a match should render context around the matching line, got ' + searchChecks.contextLines + ' lines');
  }
  if (searchChecks.marks === 0) fail('the matched substring should be highlighted');
  if (searchChecks.markText !== failedTaskId) {
    fail('highlight should carry the query verbatim, got: ' + searchChecks.markText);
  }
  if (!searchChecks.url.startsWith('/search?q=')) {
    fail('a search should be reflected in the URL, got: ' + searchChecks.url);
  }
  console.log('PASS: search returns the task with date, status, context and highlight');
  await takeScreenshot('13f-search-results');

  // Clicking a match lands on that task's log view — the existing detail /
  // download surface for the trace the match came from.
  await clickEl('#search-results .search-match');
  await page.waitForSelector('#log-entries-list .log-msg', { timeout: 10000 });
  const hitChecks = await page.evaluate((needle) => {
    return {
      url: window.location.pathname,
      logs: document.querySelectorAll('#log-entries-list .log-msg').length,
      showsNeedle: document.getElementById('log-view').textContent.includes(needle),
    };
  }, failedTaskId);
  if (hitChecks.url !== '/logs/' + logDate + '/' + failedTaskId) {
    fail('clicking a match should open its log view, expected /logs/' + logDate + '/' + failedTaskId + ', was: ' + hitChecks.url);
  }
  if (hitChecks.logs === 0) fail('the log view opened from a match should show entries');
  if (!hitChecks.showsNeedle) fail('the log view opened from a match should contain the matched text');
  console.log('PASS: a match links through to the task log view');
  await takeScreenshot('13f-search-hit-logs');

  // A window wider than the server's cap is the server's decision to make:
  // the page sends days=30 and shows the 400 body it gets back rather than
  // quietly clamping the number (issue #269 policy).
  await page.goto(baseURL + '/search?q=' + encodeURIComponent(failedTaskId) + '&days=30');
  await page.waitForLoadState('networkidle');
  const overCap = await page.evaluate(() => ({
    days: document.getElementById('search-days').value,
    text: document.getElementById('search-results').textContent,
  }));
  if (overCap.days !== '30') {
    fail('the page must not clamp the lookback client-side, field read back as: ' + overCap.days);
  }
  if (!overCap.text.includes('exceeds the maximum of 14 days')) {
    fail('the server rejection should be surfaced, got: ' + overCap.text);
  }
  console.log('PASS: an over-wide lookback surfaces the server rejection');
  await takeScreenshot('13f-search-over-cap');

  // A shared search URL replays itself: loading /search?q=... runs the
  // search without the user touching the form.
  await page.goto(baseURL + '/search?q=' + encodeURIComponent(failedTaskId));
  await page.waitForSelector('#search-results .search-match', { timeout: 10000 });
  const replayed = await page.evaluate((needle) => ({
    rows: document.querySelectorAll('#search-results .search-match').length,
    query: document.getElementById('search-query').value,
    firstTask: document.querySelector('#search-results .search-match').dataset.taskId,
  }), failedTaskId);
  if (replayed.rows === 0) fail('loading a /search URL should replay the search');
  if (replayed.query !== failedTaskId) fail('the query field should be filled from the URL, got: ' + replayed.query);
  if (replayed.firstTask !== failedTaskId) fail('replayed search should hit the same task');
  console.log('PASS: /search?q= replays the search on load');
  await takeScreenshot('13f-search-direct-url');

  // The URL, not the field, describes a replayed search. A search is pushed
  // without a days param whenever the field is empty at the time (empty means
  // "the server's default"), so a history step onto such an entry that left a
  // wider number in the field would send — and paint a header for — a window
  // the URL never named. Record what the page actually asks the server for,
  // because that request is the claim under test.
  const searchRequestLog = [];
  const logSearchRequest = (req) => {
    const u = new URL(req.url());
    if (u.pathname === '/api/tasks/search') searchRequestLog.push(u.search);
  };
  page.on('request', logSearchRequest);

  // Wait for the pane to stop saying "Searching" — it says so synchronously
  // when a search starts, so this cannot read a render that predates it.
  const searchSettled = () => page.waitForFunction(() => {
    const el = document.getElementById('search-results');
    return el && el.textContent.indexOf('Searching') === -1;
  }, null, { timeout: 15000 });

  // Wait for the request a history step provokes, and require it to be the
  // window that URL names.
  async function expectSearchRequest(from, want, label) {
    const deadline = Date.now() + 15000;
    while (searchRequestLog.length === from && Date.now() < deadline) {
      await page.waitForTimeout(50);
    }
    if (searchRequestLog.length === from) fail(label + ' should replay the search');
    const sent = searchRequestLog[searchRequestLog.length - 1];
    if (sent !== want) fail(label + ' should ask for ' + want + ', asked for: ' + sent);
    await searchSettled();
  }

  await page.goto(baseURL + '/search?q=' + encodeURIComponent(failedTaskId) + '&days=10');
  await searchSettled();
  const tenDayWindow = await page.evaluate(() => ({
    days: document.getElementById('search-days').value,
    note: document.querySelector('#search-results .search-note').textContent,
  }));
  if (tenDayWindow.days !== '10') {
    fail('a URL naming a 10-day window should fill the field with 10, got: ' + tenDayWindow.days);
  }

  // The same query searched with an empty field names no window at all.
  await page.fill('#search-days', '');
  const beforeNoWindow = searchRequestLog.length;
  await clickEl('#search-go');
  await expectSearchRequest(beforeNoWindow, '?q=' + failedTaskId,
    'a search with an empty lookback');
  const defaultWindow = await page.evaluate(() => ({
    url: window.location.pathname + window.location.search,
    note: document.querySelector('#search-results .search-note').textContent,
  }));
  if (defaultWindow.url !== '/search?q=' + encodeURIComponent(failedTaskId)) {
    fail('an empty lookback should push a URL with no days, got: ' + defaultWindow.url);
  }
  if (defaultWindow.note === tenDayWindow.note) {
    fail('the two windows must be distinguishable in the results header or this test proves nothing');
  }

  // Back onto the entry that names 10 days, then forward onto the entry that
  // names none: the field, the request and the header must all follow the URL.
  const beforeBack = searchRequestLog.length;
  await page.goBack();
  await expectSearchRequest(beforeBack, '?q=' + failedTaskId + '&days=10', 'going back to a days URL');
  const backWindow = await page.evaluate(() => document.getElementById('search-days').value);
  if (backWindow !== '10') fail('going back to a days URL should restore the field, got: ' + backWindow);

  const beforeForward = searchRequestLog.length;
  await page.goForward();
  await expectSearchRequest(beforeForward, '?q=' + failedTaskId, 'going forward to a URL with no days');
  const forwardWindow = await page.evaluate(() => ({
    days: document.getElementById('search-days').value,
    note: document.querySelector('#search-results .search-note').textContent,
  }));
  if (forwardWindow.days !== '') {
    fail('a URL naming no window must clear the lookback field, field read: ' + forwardWindow.days);
  }
  if (forwardWindow.note !== defaultWindow.note) {
    fail('a replayed search must paint the window its URL names, header read: ' + forwardWindow.note);
  }
  console.log('PASS: a replayed search takes its window from the URL, not the field');
  await takeScreenshot('13f-search-url-window');
  page.off('request', logSearchRequest);

  // Only the newest search may paint the results, because a search is the one
  // request here that can take seconds — the server walks up to 14 days of
  // traces — so two really can be in flight. Nothing else in this suite proves
  // it: every other search is issued one at a time, so deleting the guards
  // would stay green. Delay only the first response, fire both searches, and
  // require the pane, its header and the field to describe the second one even
  // after the slow first one lands.
  let firstSearchDelayed = true;
  await page.route('**/api/tasks/search**', async (route) => {
    const slow = firstSearchDelayed;
    firstSearchDelayed = false;
    if (slow) await new Promise(r => setTimeout(r, 3000));
    await route.continue();
  });
  await page.goto(baseURL + '/tasks');
  await page.waitForLoadState('networkidle');
  await page.evaluate(() => {
    const tab = Array.from(document.querySelectorAll('.tab'))
      .find(t => t.textContent.trim() === 'Search');
    tab.click();
  });
  await page.fill('#search-query', failedTaskId);
  await clickEl('#search-go');            // the slow one
  await page.fill('#search-query', completedTaskId);
  await clickEl('#search-go');            // the newest one
  await searchSettled();
  // Give the delayed response time to arrive and, if the guards are gone, to
  // overwrite the pane with a search the user already replaced.
  await page.waitForTimeout(4000);
  const newestWins = await page.evaluate((needle) => ({
    rows: document.querySelectorAll('#search-results .search-match').length,
    superseded: Array.from(document.querySelectorAll('#search-results .search-match'))
      .filter(r => r.dataset.taskId !== needle).length,
    note: document.querySelector('#search-results .search-note').textContent,
    query: document.getElementById('search-query').value,
  }), completedTaskId);
  if (newestWins.rows === 0) fail('the newest search should render its matches');
  if (newestWins.superseded !== 0) {
    fail('a superseded search must not paint the results, ' + newestWins.superseded + ' row(s) came from the earlier one');
  }
  if (!newestWins.note.includes(completedTaskId) || newestWins.query !== completedTaskId) {
    fail('the results header and the field must describe the newest search, header read: ' + newestWins.note);
  }
  console.log('PASS: the newest search owns the results pane');
  await takeScreenshot('13f-search-newest-wins');
  await page.unroute('**/api/tasks/search**');

  // Panel state: leaving a search for another view must not leave two panels
  // visible at once. This is the reviewer-reachable path — Back from a
  // search to an earlier task detail — driven without touching the DOM.
  await page.goto(baseURL + '/task/' + completedTaskId);
  await page.waitForLoadState('networkidle');
  await page.evaluate(() => {
    const tab = Array.from(document.querySelectorAll('.tab'))
      .find(t => t.textContent.trim() === 'Search');
    tab.click();
  });
  await page.fill('#search-query', failedTaskId);
  await clickEl('#search-go');
  await page.waitForSelector('#search-results .search-match', { timeout: 10000 });
  await page.goBack();
  await page.waitForFunction(() => window.location.pathname.indexOf('/task/') === 0, { timeout: 10000 });
  const backPanels = await page.evaluate(() => ({
    url: window.location.pathname,
    visible: ['tab-tasks', 'tab-logs', 'tab-search']
      .filter(id => document.getElementById(id).style.display !== 'none'),
  }));
  if (backPanels.url !== '/task/' + completedTaskId) {
    fail('going back should land on the earlier task detail, got: ' + backPanels.url);
  }
  if (backPanels.visible.length !== 1) {
    fail('exactly one panel should be visible after going back from a search, visible: ' + backPanels.visible.join(', '));
  }
  console.log('PASS: going back from a search leaves one panel visible');
  await takeScreenshot('13f-search-back-to-task');

  // The completeness banners are the only branch here a journey cannot
  // provoke: they render straight off the response's truncated and partial
  // flags, and whether those flags are set at all is the server's call —
  // internal/server/task_search_test.go asserts the cap sets truncated and
  // an unreadable trace sets partial. So drive the renderer one flag at a
  // time: a renamed field, an inverted condition or a banner swapped for the
  // other one fails here.
  const banners = await page.evaluate(() => {
    const shown = (flags) => {
      renderSearchResults(Object.assign(
        { query: 'flagged', from: '2026-05-01', to: '2026-05-06', matches: [] }, flags));
      const text = document.getElementById('search-results').textContent;
      return {
        truncated: text.indexOf('scan stopped at the server') !== -1,
        partial: text.indexOf('could not be read') !== -1,
      };
    };
    return {
      truncatedOnly: shown({ truncated: true }),
      partialOnly: shown({ partial: true }),
      neither: shown({}),
    };
  });
  if (!banners.truncatedOnly.truncated || banners.truncatedOnly.partial) {
    fail('a capped answer must show the truncation banner alone, got: ' + JSON.stringify(banners.truncatedOnly));
  }
  if (!banners.partialOnly.partial || banners.partialOnly.truncated) {
    fail('an unreadable window must show the partial banner alone, got: ' + JSON.stringify(banners.partialOnly));
  }
  if (banners.neither.truncated || banners.neither.partial) {
    fail('a complete answer must show no completeness banner, got: ' + JSON.stringify(banners.neither));
  }
  console.log('PASS: completeness banners follow the response flags');

  if (searchPageErrors.length > 0) {
    fail('search journey raised page JS errors: ' + searchPageErrors.join('; '));
  }
  console.log('PASS: search journey raised no page JS errors');

  // =====================================================================
  // Phase 14: Mobile viewport — mobile-friendly UI (issue #203)
  // =====================================================================
  // Re-drives the real user journey (navigate → task list → select task →
  // Logs tab) on a narrow phone-sized viewport and asserts that nothing
  // overflows horizontally and the key text stays readable. Uses a second
  // page so the desktop page from the earlier phases is undisturbed.
  console.log('=== Phase 14: Mobile viewport ===');
  const mobile = await browser.newPage({ viewport: { width: 390, height: 844 } });
  const mobileJsErrors = [];
  mobile.on('pageerror', err => mobileJsErrors.push(err.message));
  await mobile.goto(baseURL);
  await mobile.waitForLoadState('networkidle');
  await mobile.waitForSelector('.task-item', { timeout: 10000 });
  await mobile.waitForTimeout(500);

  // Containers that must never scroll horizontally (even on desktop they
  // wrap or clip rather than scroll). Deliberately excludes inner scroll
  // regions like .tool-output / .md-rendered pre, which may scroll
  // horizontally at any width.
  const NO_OVERFLOW_SELECTORS = '.header, .container, .sidebar, .main, .tab-bar, .task-summary, .task-filter-row, .task-item, .task-detail-header, .submit-form, .log-breadcrumb';

  async function assertNoHorizontalOverflow(context, label) {
    const overflow = await context.evaluate((selectors) => {
      const bad = [];
      if (document.documentElement.scrollWidth > document.documentElement.clientWidth + 1) {
        bad.push('page: scrollWidth=' + document.documentElement.scrollWidth + ' > ' + document.documentElement.clientWidth);
      }
      document.querySelectorAll(selectors).forEach(el => {
        if (el.scrollWidth > el.clientWidth + 1) {
          bad.push(el.className + ': scrollWidth=' + el.scrollWidth + ' > ' + el.clientWidth);
        }
      });
      return bad;
    }, NO_OVERFLOW_SELECTORS);
    if (overflow.length > 0) {
      fail(label + ': horizontal overflow — ' + overflow.join('; '));
    }
    console.log('PASS: ' + label + ' — no horizontal overflow');
  }

  // 14a: Front page — task list visible, header fits, readable title.
  await assertNoHorizontalOverflow(mobile, 'mobile front page');
  const mobileHeaderChecks = await mobile.evaluate(() => {
    const h1 = document.querySelector('.header h1');
    return {
      h1FontSize: h1 ? parseFloat(getComputedStyle(h1).fontSize) : 0,
      taskItems: document.querySelectorAll('.task-item').length,
      submitVisible: document.querySelector('.submit-form .btn') !== null,
    };
  });
  if (mobileHeaderChecks.taskItems === 0) fail('mobile: task list did not render');
  console.log('PASS: mobile task list rendered (' + mobileHeaderChecks.taskItems + ' tasks)');
  if (mobileHeaderChecks.h1FontSize < 16) {
    fail('mobile: header title too small to read (computed ' + mobileHeaderChecks.h1FontSize + 'px, want >= 16px)');
  }
  console.log('PASS: mobile header title readable (' + mobileHeaderChecks.h1FontSize + 'px)');
  if (!mobileHeaderChecks.submitVisible) fail('mobile: submit task button not present');
  console.log('PASS: mobile submit form reachable');
  await mobile.screenshot({ path: require('path').join(screenshotDir, '14a-mobile-tasks.png'), fullPage: true });
  console.log('Screenshot saved:', require('path').join(screenshotDir, '14a-mobile-tasks.png'));

  // 14b: Select the running task — detail view must render and fit.
  await mobile.evaluate((tid) => {
    const items = document.querySelectorAll('.task-item');
    for (const item of items) {
      if (item.textContent.includes(tid)) { item.click(); break; }
    }
  }, taskId);
  await mobile.waitForSelector('.task-detail-header', { timeout: 5000 });
  await mobile.waitForTimeout(500);
  await assertNoHorizontalOverflow(mobile, 'mobile task detail');
  const mobileDetailChecks = await mobile.evaluate(() => {
    const detail = document.querySelector('#log-view .task-detail');
    const header = document.querySelector('.task-detail-header');
    return {
      detailVisible: detail !== null && detail.offsetHeight > 0,
      headerFits: header ? header.scrollWidth <= header.clientWidth + 1 : false,
      hasLogBody: !!detail && detail.querySelector('.task-detail-body') !== null,
    };
  });
  if (!mobileDetailChecks.detailVisible) fail('mobile: task detail not visible after selecting task');
  if (!mobileDetailChecks.headerFits) fail('mobile: task detail header overflows');
  if (!mobileDetailChecks.hasLogBody) fail('mobile: task detail log body missing');
  console.log('PASS: mobile task detail renders and fits');
  await mobile.screenshot({ path: require('path').join(screenshotDir, '14b-mobile-task-detail.png'), fullPage: true });
  console.log('Screenshot saved:', require('path').join(screenshotDir, '14b-mobile-task-detail.png'));

  // 14c: Logs tab — follow the real journey: Logs tab → date → task →
  // entries. Breadcrumb and entry list must fit at mobile width.
  await mobile.evaluate(() => showTab('logs'));
  await mobile.waitForSelector('#log-dates-list .log-task-item', { timeout: 10000 });
  await mobile.evaluate(() => { document.querySelector('#log-dates-list .log-task-item').click(); });
  await mobile.waitForSelector('.log-breadcrumb', { timeout: 10000 });
  await mobile.waitForTimeout(300);
  await mobile.evaluate((tid) => {
    const items = document.querySelectorAll('.log-task-item');
    for (const item of items) {
      const idEl = item.querySelector('.log-task-id');
      if (idEl && idEl.textContent.includes(tid)) { item.click(); break; }
    }
  }, taskId);
  await mobile.waitForSelector('#log-entries-list .log-msg', { timeout: 10000 });
  await mobile.waitForTimeout(500);
  await assertNoHorizontalOverflow(mobile, 'mobile logs tab');
  const mobileLogsChecks = await mobile.evaluate(() => {
    const entries = document.querySelectorAll('#log-entries-list .log-msg');
    const toolBlocks = document.querySelectorAll('#log-entries-list .tool-block');
    return { entries: entries.length, toolBlocks: toolBlocks.length };
  });
  if (mobileLogsChecks.entries === 0) fail('mobile: log entries did not render in Logs tab');
  console.log('PASS: mobile Logs tab renders (' + mobileLogsChecks.entries + ' entries, ' + mobileLogsChecks.toolBlocks + ' tool blocks)');
  await mobile.screenshot({ path: require('path').join(screenshotDir, '14c-mobile-logs.png'), fullPage: true });
  console.log('Screenshot saved:', require('path').join(screenshotDir, '14c-mobile-logs.png'));

  if (mobileJsErrors.length > 0) fail('mobile: page JS errors: ' + mobileJsErrors.join('; '));
  console.log('PASS: mobile page raised no JS errors');
  await mobile.close();

  // =====================================================================
  // Phase 15: Full-screen task logs on mobile (issue #205)
  // =====================================================================
  // The mobile layout stacks the sidebar above the main pane (issue #203),
  // so the log stream of an open task only gets a fraction of the screen.
  // The task detail header carries a corner button that expands the logs to
  // the whole viewport and collapses them back to that split-screen view.
  // The affordance is narrow-viewport only: the desktop layout stays as it
  // was, per issue #203.
  console.log('=== Phase 15: Full-screen task logs (mobile) ===');

  // 15a: Big screens must be untouched — the toggle is hidden there.
  const desktopFs = await browser.newPage({ viewport: { width: 1280, height: 900 } });
  await desktopFs.goto(baseURL + '/task/' + taskId);
  await desktopFs.waitForSelector('.task-detail-header', { timeout: 10000 });
  const desktopFsBtn = await desktopFs.evaluate(() => {
    const btn = document.querySelector('.task-detail-fullscreen-btn');
    return btn ? getComputedStyle(btn).display : 'missing';
  });
  if (desktopFsBtn !== 'none') {
    fail('desktop: full-screen toggle should be hidden (display=' + desktopFsBtn + ')');
  }
  console.log('PASS: full-screen toggle hidden on a desktop viewport');
  await desktopFs.close();

  // 15b: Phone viewport — open a task, expand its logs, collapse again.
  const fsPage = await browser.newPage({ viewport: { width: 390, height: 844 } });
  const fsJsErrors = [];
  fsPage.on('pageerror', err => fsJsErrors.push(err.message));
  await fsPage.goto(baseURL);
  await fsPage.waitForLoadState('networkidle');
  await fsPage.waitForSelector('.task-item', { timeout: 10000 });
  await fsPage.evaluate((tid) => {
    for (const item of document.querySelectorAll('.task-item')) {
      if (item.textContent.includes(tid)) { item.click(); break; }
    }
  }, taskId);
  await fsPage.waitForSelector('#log-view .task-detail-body', { timeout: 10000 });
  await fsPage.waitForTimeout(500);

  // Since issue #234 the sidebar is collapsed behind a drawer toggle by
  // default on a phone. Open the drawer here so this phase still sees the
  // stacked split-screen view it was written against; the collapsed-by-
  // default behaviour is phase 16's subject.
  await fsPage.evaluate(() => document.querySelector('.sidebar-toggle').click());
  await fsPage.waitForTimeout(300);

  const fsBefore = await fsPage.evaluate(() => {
    const btn = document.querySelector('.task-detail-fullscreen-btn');
    return {
      btnVisible: btn !== null && btn.offsetHeight > 0,
      btnPressed: btn ? btn.getAttribute('aria-pressed') : null,
      mainH: document.querySelector('.main').offsetHeight,
      logBodyH: document.querySelector('#log-view .task-detail-body').offsetHeight,
      sidebarH: document.querySelector('.sidebar').offsetHeight,
      innerH: window.innerHeight,
    };
  });
  if (!fsBefore.btnVisible) fail('mobile: full-screen toggle not visible in the task detail header');
  if (fsBefore.btnPressed !== 'false') fail('mobile: toggle should start un-pressed, got ' + fsBefore.btnPressed);
  if (fsBefore.sidebarH === 0) fail('mobile: sidebar should be part of the split-screen view');
  // Sanity check the premise: in split-screen the main pane is squeezed.
  if (fsBefore.mainH >= fsBefore.innerH * 0.9) {
    fail('mobile: split-screen main pane already fills the viewport (' + fsBefore.mainH + '/' + fsBefore.innerH + ') — nothing to expand');
  }
  console.log('PASS: mobile split-screen detail shows the toggle (main pane ' + fsBefore.mainH + 'px of ' + fsBefore.innerH + 'px)');

  // 15c: Expand — header, sidebar and tab bar are gone, the log stream
  // fills the viewport and gains vertical room.
  await fsPage.evaluate(() => document.querySelector('.task-detail-fullscreen-btn').click());
  await fsPage.waitForTimeout(300);
  const fsExpanded = await fsPage.evaluate(() => {
    const mainRect = document.querySelector('.main').getBoundingClientRect();
    return {
      on: document.body.classList.contains('logs-fullscreen'),
      ariaPressed: document.querySelector('.task-detail-fullscreen-btn').getAttribute('aria-pressed'),
      headerH: document.querySelector('.header').offsetHeight,
      sidebarH: document.querySelector('.sidebar').offsetHeight,
      tabBarH: document.querySelector('.tab-bar').offsetHeight,
      mainW: mainRect.width, mainTop: mainRect.top, mainH: mainRect.height,
      logBodyH: document.querySelector('#log-view .task-detail-body').offsetHeight,
      innerW: window.innerWidth, innerH: window.innerHeight,
    };
  });
  if (!fsExpanded.on) fail('mobile: toggling should put the log stream in full screen');
  if (fsExpanded.ariaPressed !== 'true') fail('mobile: toggle should report aria-pressed=true in full screen');
  if (fsExpanded.headerH !== 0 || fsExpanded.sidebarH !== 0 || fsExpanded.tabBarH !== 0) {
    fail('mobile: full screen must hide the header, sidebar and tab bar (got ' + fsExpanded.headerH + '/' + fsExpanded.sidebarH + '/' + fsExpanded.tabBarH + 'px)');
  }
  if (fsExpanded.mainTop > 1) fail('mobile: main pane should start at the top of the viewport, top=' + fsExpanded.mainTop);
  if (fsExpanded.mainH < fsExpanded.innerH - 1) fail('mobile: main pane should fill the viewport height (' + fsExpanded.mainH + ' < ' + fsExpanded.innerH + ')');
  if (fsExpanded.mainW < fsExpanded.innerW - 1) fail('mobile: main pane should fill the viewport width (' + fsExpanded.mainW + ' < ' + fsExpanded.innerW + ')');
  if (fsExpanded.logBodyH <= fsBefore.logBodyH) {
    fail('mobile: log stream should get taller in full screen (' + fsExpanded.logBodyH + ' <= ' + fsBefore.logBodyH + ')');
  }
  await assertNoHorizontalOverflow(fsPage, 'mobile full-screen logs');
  console.log('PASS: full screen fills the viewport and grows the log stream (' + fsBefore.logBodyH + 'px -> ' + fsExpanded.logBodyH + 'px)');
  await fsPage.screenshot({ path: require('path').join(screenshotDir, '15a-mobile-logs-fullscreen.png') });
  console.log('Screenshot saved:', require('path').join(screenshotDir, '15a-mobile-logs-fullscreen.png'));

  // 15d: The same corner button goes back to the split-screen view.
  await fsPage.evaluate(() => document.querySelector('.task-detail-fullscreen-btn').click());
  await fsPage.waitForTimeout(300);
  const fsCollapsed = await fsPage.evaluate(() => ({
    on: document.body.classList.contains('logs-fullscreen'),
    ariaPressed: document.querySelector('.task-detail-fullscreen-btn').getAttribute('aria-pressed'),
    sidebarH: document.querySelector('.sidebar').offsetHeight,
    headerH: document.querySelector('.header').offsetHeight,
    mainH: document.querySelector('.main').offsetHeight,
    innerH: window.innerHeight,
  }));
  if (fsCollapsed.on) fail('mobile: corner button should leave full screen');
  if (fsCollapsed.ariaPressed !== 'false') fail('mobile: toggle should report aria-pressed=false in split screen');
  if (fsCollapsed.sidebarH === 0 || fsCollapsed.headerH === 0) fail('mobile: split-screen view should show the sidebar and header again');
  if (fsCollapsed.mainH >= fsCollapsed.innerH * 0.9) fail('mobile: main pane should return to the stacked split-screen height (' + fsCollapsed.mainH + '/' + fsCollapsed.innerH + ')');
  console.log('PASS: corner button returns to the split-screen view');
  await fsPage.screenshot({ path: require('path').join(screenshotDir, '15b-mobile-logs-split-screen.png') });
  console.log('Screenshot saved:', require('path').join(screenshotDir, '15b-mobile-logs-split-screen.png'));

  // 15f: Crossing the 768px breakpoint while full screen — a phone rotated to
  // landscape (844x390) — must self-heal. The toggle only exists at narrow
  // viewports, so a mode that outlived the breakpoint would leave the user
  // with no visible way back to the split-screen view.
  await fsPage.evaluate(() => document.querySelector('.task-detail-fullscreen-btn').click());
  await fsPage.waitForTimeout(200);
  if (!(await fsPage.evaluate(() => document.body.classList.contains('logs-fullscreen')))) {
    fail('mobile: precondition for the rotation check — should be full screen at 390x844');
  }
  await fsPage.setViewportSize({ width: 844, height: 390 });
  await fsPage.waitForTimeout(300);
  const fsRotated = await fsPage.evaluate(() => {
    const btn = document.querySelector('.task-detail-fullscreen-btn');
    return {
      on: document.body.classList.contains('logs-fullscreen'),
      ariaPressed: btn ? btn.getAttribute('aria-pressed') : null,
      btnVisible: btn !== null && btn.offsetHeight > 0,
      headerH: document.querySelector('.header').offsetHeight,
      sidebarH: document.querySelector('.sidebar').offsetHeight,
      tabBarH: document.querySelector('.tab-bar').offsetHeight,
    };
  });
  if (fsRotated.on) fail('rotation: crossing the breakpoint should drop out of full screen');
  if (fsRotated.ariaPressed !== 'false') fail('rotation: toggle should report aria-pressed=false after crossing the breakpoint, got ' + fsRotated.ariaPressed);
  if (fsRotated.btnVisible) fail('rotation: the toggle must stay hidden above the breakpoint');
  if (fsRotated.headerH === 0 || fsRotated.sidebarH === 0 || fsRotated.tabBarH === 0) {
    fail('rotation: layout must self-heal past the breakpoint (header/sidebar/tab bar = ' + fsRotated.headerH + '/' + fsRotated.sidebarH + '/' + fsRotated.tabBarH + 'px)');
  }
  await assertNoHorizontalOverflow(fsPage, 'rotated out of full screen');
  console.log('PASS: rotating past the breakpoint leaves full screen and restores the layout');

  // Rotating back must not silently put the user back in full screen — the
  // mode was dropped when the viewport widened.
  await fsPage.setViewportSize({ width: 390, height: 844 });
  await fsPage.waitForTimeout(300);
  const fsRotatedBack = await fsPage.evaluate(() => ({
    on: document.body.classList.contains('logs-fullscreen'),
    sidebarH: document.querySelector('.sidebar').offsetHeight,
    mainH: document.querySelector('.main').offsetHeight,
    innerH: window.innerHeight,
  }));
  if (fsRotatedBack.on) fail('rotation: rotating back should not re-enter full screen');
  if (fsRotatedBack.sidebarH === 0) fail('rotation: sidebar must be visible after rotating back to portrait');
  if (fsRotatedBack.mainH >= fsRotatedBack.innerH * 0.9) fail('rotation: main pane should be the stacked split-screen height after rotating back (' + fsRotatedBack.mainH + '/' + fsRotatedBack.innerH + ')');
  console.log('PASS: rotating back to portrait keeps the split-screen view');
  await fsPage.screenshot({ path: require('path').join(screenshotDir, '15c-mobile-logs-after-rotation.png') });
  console.log('Screenshot saved:', require('path').join(screenshotDir, '15c-mobile-logs-after-rotation.png'));

  // 15e: Leaving the task detail must never leave the dashboard without
  // its sidebar — full screen only exists for the detail view.
  await fsPage.evaluate(() => document.querySelector('.task-detail-fullscreen-btn').click());
  await fsPage.waitForTimeout(200);
  await fsPage.evaluate(() => document.querySelectorAll('.tab')[0].click());
  await fsPage.waitForTimeout(400);
  const fsAfterLeave = await fsPage.evaluate(() => ({
    on: document.body.classList.contains('logs-fullscreen'),
    sidebarH: document.querySelector('.sidebar').offsetHeight,
    tasksVisible: document.getElementById('tab-tasks').style.display !== 'none',
  }));
  if (fsAfterLeave.on) fail('mobile: navigating to the Tasks tab should drop out of full screen');
  if (fsAfterLeave.sidebarH === 0) fail('mobile: sidebar must be back after leaving full screen via the Tasks tab');
  if (!fsAfterLeave.tasksVisible) fail('mobile: Tasks tab should be shown after leaving full screen');
  console.log('PASS: leaving the task detail resets full screen');

  if (fsJsErrors.length > 0) fail('mobile full-screen: page JS errors: ' + fsJsErrors.join('; '));
  console.log('PASS: full-screen journey raised no JS errors');
  await fsPage.close();

  // =====================================================================
  // Phase 16: Sidebar drawer on mobile (issue #234)
  // =====================================================================
  // The Guests list and the Submit Task form are the least interesting part
  // of the UI, so on a phone they are collapsed behind a draw-down toggle in
  // the top bar (issue #234). By default only the top bar and the task area
  // fill the screen; the panels drop in on demand. Nothing is removed — the
  // drawer holds the very same markup the desktop sidebar shows.
  console.log('=== Phase 16: Mobile sidebar drawer ===');

  // 16a: Desktop is untouched — no toggle, sidebar always shown.
  const desktopDrawer = await browser.newPage({ viewport: { width: 1280, height: 900 } });
  await desktopDrawer.goto(baseURL);
  await desktopDrawer.waitForSelector('.task-item', { timeout: 10000 });
  const desktopDrawerState = await desktopDrawer.evaluate(() => {
    const btn = document.querySelector('.sidebar-toggle');
    const sidebar = document.querySelector('.sidebar');
    return {
      btnDisplay: btn ? getComputedStyle(btn).display : 'missing',
      sidebarH: sidebar.offsetHeight,
      panels: Array.from(sidebar.querySelectorAll('.panel h3')).map(h => h.textContent.trim()),
      guests: sidebar.querySelectorAll('#guest-list .guest-item').length,
    };
  });
  if (desktopDrawerState.btnDisplay !== 'none') {
    fail('desktop: drawer toggle should be hidden (display=' + desktopDrawerState.btnDisplay + ')');
  }
  if (desktopDrawerState.sidebarH === 0) fail('desktop: sidebar must stay visible on a big screen');
  console.log('PASS: drawer toggle hidden, sidebar always shown on a desktop viewport');
  await desktopDrawer.close();

  // 16b: Phone — collapsed by default. The toggle sits in the top bar, the
  // sidebar is out of the flow, and the task area starts directly under the
  // header instead of being pushed down by the panels.
  const drawer = await browser.newPage({ viewport: { width: 390, height: 844 } });
  const drawerJsErrors = [];
  drawer.on('pageerror', err => drawerJsErrors.push(err.message));
  await drawer.goto(baseURL);
  await drawer.waitForLoadState('networkidle');
  await drawer.waitForSelector('.task-item', { timeout: 10000 });
  await drawer.waitForTimeout(500);
  const drawerClosed = await drawer.evaluate(() => {
    const btn = document.querySelector('.sidebar-toggle');
    const sidebar = document.querySelector('.sidebar');
    const header = document.querySelector('.header');
    const main = document.querySelector('.main');
    return {
      on: document.body.classList.contains('sidebar-open'),
      btnVisible: btn !== null && btn.offsetHeight > 0,
      ariaExpanded: btn ? btn.getAttribute('aria-expanded') : null,
      ariaControls: btn ? btn.getAttribute('aria-controls') : null,
      sidebarH: sidebar.offsetHeight,
      headerH: header.offsetHeight,
      mainTop: Math.round(main.getBoundingClientRect().top),
      headerBottom: Math.round(header.getBoundingClientRect().bottom),
      taskItems: document.querySelectorAll('.task-item').length,
    };
  });
  if (drawerClosed.headerH === 0) fail('mobile: top bar must be visible with the drawer closed');
  if (drawerClosed.on) fail('mobile: drawer must start closed so only the top bar and the tasks are shown by default');
  if (!drawerClosed.btnVisible) fail('mobile: drawer toggle must live in the top bar');
  if (drawerClosed.ariaExpanded !== 'false') fail('mobile: toggle should report aria-expanded=false when closed, got ' + drawerClosed.ariaExpanded);
  if (drawerClosed.ariaControls !== 'sidebar') fail('mobile: toggle should control the sidebar, got aria-controls=' + drawerClosed.ariaControls);
  if (drawerClosed.sidebarH !== 0) fail('mobile: sidebar must be hidden by default (height ' + drawerClosed.sidebarH + 'px)');
  if (drawerClosed.mainTop > drawerClosed.headerBottom + 1) {
    fail('mobile: task area should sit directly under the top bar (main top ' + drawerClosed.mainTop + ', header bottom ' + drawerClosed.headerBottom + ')');
  }
  if (drawerClosed.taskItems === 0) fail('mobile: task list must be visible with the drawer closed');
  await assertNoHorizontalOverflow(drawer, 'mobile drawer closed');
  console.log('PASS: drawer starts closed, task area directly under the top bar');
  await drawer.screenshot({ path: require('path').join(screenshotDir, '16a-mobile-drawer-closed.png'), fullPage: true });
  console.log('Screenshot saved:', require('path').join(screenshotDir, '16a-mobile-drawer-closed.png'));

  // 16c: Open the drawer — the same panels drop in. Compare the panel
  // headings and the rendered guests against desktop so the issue's "we must
  // not lose any UI information" rule is checked, not just assumed.
  await drawer.evaluate(() => document.querySelector('.sidebar-toggle').click());
  await drawer.waitForTimeout(300);
  const drawerOpen = await drawer.evaluate(() => {
    const btn = document.querySelector('.sidebar-toggle');
    const sidebar = document.querySelector('.sidebar');
    return {
      on: document.body.classList.contains('sidebar-open'),
      ariaExpanded: btn.getAttribute('aria-expanded'),
      sidebarH: sidebar.offsetHeight,
      sidebarW: Math.round(sidebar.getBoundingClientRect().width),
      innerW: window.innerWidth,
      panels: Array.from(sidebar.querySelectorAll('.panel h3')).map(h => h.textContent.trim()),
      guests: sidebar.querySelectorAll('#guest-list .guest-item').length,
      guestListH: sidebar.querySelector('#guest-list').offsetHeight,
      submitBtnH: sidebar.querySelector('.submit-form .btn').offsetHeight,
      promptH: sidebar.querySelector('#task-prompt').offsetHeight,
      priorityInputs: sidebar.querySelectorAll('input[name="task-priority"]').length,
    };
  });
  if (!drawerOpen.on) fail('mobile: toggling should open the drawer');
  if (drawerOpen.ariaExpanded !== 'true') fail('mobile: toggle should report aria-expanded=true when open, got ' + drawerOpen.ariaExpanded);
  if (drawerOpen.sidebarH === 0) fail('mobile: opening the drawer must reveal the sidebar');
  if (drawerOpen.sidebarW > drawerOpen.innerW + 1) fail('mobile: drawer must fit the viewport (' + drawerOpen.sidebarW + ' > ' + drawerOpen.innerW + ')');
  if (drawerOpen.guestListH === 0) fail('mobile: guests list must be visible in the drawer');
  if (drawerOpen.submitBtnH === 0 || drawerOpen.promptH === 0) fail('mobile: submit form must be usable from the drawer');
  if (drawerOpen.priorityInputs !== 3) fail('mobile: priority choice should offer three options, got ' + drawerOpen.priorityInputs);
  if (JSON.stringify(drawerOpen.panels) !== JSON.stringify(desktopDrawerState.panels)) {
    fail('mobile: drawer must carry the same panels as the desktop sidebar — desktop ' + JSON.stringify(desktopDrawerState.panels) + ' vs mobile ' + JSON.stringify(drawerOpen.panels));
  }
  if (drawerOpen.guests !== desktopDrawerState.guests) fail('mobile: drawer shows ' + drawerOpen.guests + ' guests, desktop shows ' + desktopDrawerState.guests);
  await assertNoHorizontalOverflow(drawer, 'mobile drawer open');
  console.log('PASS: drawer drops in with the guests list and submit form intact (' + drawerOpen.panels.join(', ') + ')');
  await drawer.screenshot({ path: require('path').join(screenshotDir, '16b-mobile-drawer-open.png'), fullPage: true });
  console.log('Screenshot saved:', require('path').join(screenshotDir, '16b-mobile-drawer-open.png'));

  // 16d: The same button collapses it again — one control, both directions,
  // as with the full-screen toggle (issue #205).
  await drawer.evaluate(() => document.querySelector('.sidebar-toggle').click());
  await drawer.waitForTimeout(300);
  const drawerReclosed = await drawer.evaluate(() => ({
    on: document.body.classList.contains('sidebar-open'),
    ariaExpanded: document.querySelector('.sidebar-toggle').getAttribute('aria-expanded'),
    sidebarH: document.querySelector('.sidebar').offsetHeight,
  }));
  if (drawerReclosed.on) fail('mobile: toggling again should close the drawer');
  if (drawerReclosed.ariaExpanded !== 'false') fail('mobile: toggle should report aria-expanded=false after closing');
  if (drawerReclosed.sidebarH !== 0) fail('mobile: sidebar must be hidden again after closing the drawer');
  console.log('PASS: the same toggle closes the drawer again');

  if (drawerJsErrors.length > 0) fail('mobile drawer: page JS errors: ' + drawerJsErrors.join('; '));
  console.log('PASS: mobile drawer journey raised no JS errors');
  await drawer.close();

  console.log('All UI validation checks passed!');
  await browser.close();
})().catch(e => { console.error('Test failed:', e); process.exit(1); });
`, screenshotDir, baseURL, taskID, compactionTaskID, failedTaskID, completedTaskID, pendingTaskID, failureReason, editTaskID, titleUrlTaskID, titleUrlTaskTitle, titleUrlTaskURL, titleOnlyTaskID, titleOnlyTaskTitle, titleBadUrlTaskID, titleBadUrlTaskTitle, logDate)

	tmpScript := t.TempDir() + "/validate_ui.js"
	if err := os.WriteFile(tmpScript, []byte(script), 0o644); err != nil {
		t.Fatalf("write playwright script: %v", err)
	}

	cmd := exec.Command("node", tmpScript)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Dir = projectRoot

	nodePath := os.Getenv("HOME")
	if nodePath == "" {
		t.Fatal("HOME environment variable not set")
	}
	cmd.Env = append(os.Environ(), "NODE_PATH="+nodePath+"/node_modules")

	if err := cmd.Run(); err != nil {
		t.Fatalf("Playwright UI validation failed: %v", err)
	}

	// List screenshots taken — fail if none were produced
	files, _ := os.ReadDir(screenshotDir)
	if len(files) == 0 {
		t.Fatal("no screenshots produced — Playwright validation may not have run correctly")
	}
	t.Log("Screenshots taken:")
	for _, f := range files {
		t.Logf("  %s", filepath.Join(screenshotDir, f.Name()))
	}
}

func jsonID(id int64) *json.RawMessage {
	b, _ := json.Marshal(id)
	raw := json.RawMessage(b)
	return &raw
}

func toolID1() string { return "aBcD1234EfGh5678IjKlMnOpQrStUvWx" }
func toolID2() string { return "xYz789AbCdEf012GhIjKlMnOpQrStUv01" }

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
