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
	"hotelier/pkg/queue"
	"hotelier/pkg/rpc"
)

// testLogEntry represents a single log entry used across both the RPC and
// Playwright validation paths.
type testLogEntry struct {
	line       string
	level      string
	toolType   string // "start", "output", "end"
	toolName   string // e.g. "bash", "read"
	toolID     string // unique tool call identifier
	toolArgs   string // arguments/parameters
	toolOutput string // captured output
	toolError  bool   // true if tool ended with error
}

// piRPCEvent captures the top-level type field from a Pi RPC wire event.
type piRPCEvent struct {
	Type string `json:"type"`
}

// piToolExecEvent captures tool execution events from Pi RPC.
type piToolExecEvent struct {
	Type       string `json:"type"`
	ToolCallID string `json:"toolCallId"`
	ToolName   string `json:"toolName"`
	Args       any    `json:"args"`
	Result     any    `json:"result"`
	IsError    bool   `json:"isError"`
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
		// The JSON payload follows the last colon+space after the timestamp.
		jsonStart := strings.LastIndex(line, ": ")
		if jsonStart == -1 {
			continue
		}
		payload := strings.TrimSpace(line[jsonStart+2:])
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
			var toolEvt piToolExecEvent
			if err := json.Unmarshal([]byte(payload), &toolEvt); err != nil {
				continue
			}
			argsJSON, _ := json.Marshal(toolEvt.Args)
			entries = append(entries, testLogEntry{
				line:     fmt.Sprintf("[TOOL_START] %s: %s (id: %s)", toolEvt.ToolName, string(argsJSON), toolEvt.ToolCallID),
				level:    "tool",
				toolType: "start",
				toolName: toolEvt.ToolName,
				toolID:   toolEvt.ToolCallID,
				toolArgs: string(argsJSON),
			})

		case "tool_execution_end":
			var toolEvt piToolExecEvent
			if err := json.Unmarshal([]byte(payload), &toolEvt); err != nil {
				continue
			}
			resultJSON, _ := json.Marshal(toolEvt.Result)
			entries = append(entries, testLogEntry{
				line:       fmt.Sprintf("[TOOL_END] %s (id: %s): %s", toolEvt.ToolName, toolEvt.ToolCallID, string(resultJSON)),
				level:      "tool",
				toolType:   "end",
				toolName:   toolEvt.ToolName,
				toolID:     toolEvt.ToolCallID,
				toolOutput: string(resultJSON),
				toolError:  toolEvt.IsError,
			})

		case "tool_execution_update":
			var toolEvt piToolExecEvent
			if err := json.Unmarshal([]byte(payload), &toolEvt); err != nil {
				continue
			}
			// Only emit output entries when there's actual content
			var partialResult any
			if err := json.Unmarshal([]byte(payload), &struct {
				PartialResult any `json:"partialResult"`
			}{PartialResult: &partialResult}); err == nil && partialResult != nil {
				resultJSON, _ := json.Marshal(partialResult)
				if string(resultJSON) != "{}" && string(resultJSON) != "null" {
					entries = append(entries, testLogEntry{
						line:       fmt.Sprintf("[TOOL_OUTPUT] %s (id: %s): %s", toolEvt.ToolName, toolEvt.ToolCallID, string(resultJSON)),
						level:      "tool",
						toolType:   "output",
						toolName:   toolEvt.ToolName,
						toolID:     toolEvt.ToolCallID,
						toolOutput: string(resultJSON),
					})
				}
			}

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
		{"Task started", "system", "", "", "", "", "", false},
		{fmt.Sprintf("Executing task %s", taskID), "system", "", "", "", "", "", false},
		{fmt.Sprintf("Cloning https://github.com/example/repo.git -> /tmp/hotelier/tasks/%s/repo", taskID), "system", "", "", "", "", "", false},
		{"Cloned https://github.com/example/repo.git", "system", "", "", "", "", "", false},
		{fmt.Sprintf("Spawning pi subprocess in: /tmp/hotelier/tasks/%s/repo", taskID), "system", "", "", "", "", "", false},
		{"Sending prompt to pi", "system", "", "", "", "", "", false},
		{"Prompt sent, waiting for events", "system", "", "", "", "", "", false},
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

// sendLogEntries sends a slice of log entries to the server via guest.log RPC.
func sendLogEntries(t *testing.T, guestWS *websocket.Conn, taskID string, entries []testLogEntry) {
	t.Helper()
	for _, entry := range entries {
		logParams, _ := json.Marshal(map[string]interface{}{
			"task_id":     taskID,
			"line":        entry.line,
			"level":       entry.level,
			"tool_type":   entry.toolType,
			"tool_name":   entry.toolName,
			"tool_id":     entry.toolID,
			"tool_args":   entry.toolArgs,
			"tool_output": entry.toolOutput,
			"tool_error":  entry.toolError,
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
				TaskID:    e.TaskID,
				Line:      e.Line,
				Level:     e.Level,
				Timestamp: e.Timestamp,
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
	}
	srv := server.New(cfg)
	hub := srv.Hub()
	go hub.Run()

	// Build the HTTP handler exactly as server.Run() does — this ensures we
	// exercise the same code paths as production.
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", srv.HandleWebSocket)
	mux.HandleFunc("/api/tasks", srv.HandleTasks)
	mux.HandleFunc("/api/tasks/", srv.HandleTaskDetail)
	mux.HandleFunc("/api/guests", srv.HandleGuests)
	mux.HandleFunc("/api/guests/", srv.HandleGuestDetail)
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
		ID:     taskID,
		Prompt: "Simulate tool calls and validate UI rendering",
		Tags:   []string{"business-default"},
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

	// Step 5: Validate UI rendering via Playwright by exercising the real
	// user flow: navigate → task list → click task → detail view.
	validateUI(t, baseURL, taskID, failedTaskID, completedTaskID, pendingTaskID, failureReason, projectRoot)
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

func validateUI(t *testing.T, baseURL, taskID, failedTaskID, completedTaskID, pendingTaskID, failureReason, projectRoot string) {
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
  const failedTaskId = '%s';
  const completedTaskId = '%s';
  const pendingTaskId = '%s';
  const failureReason = '%s';
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

  // Verify only 2 tabs exist (Tasks + Logs, no Task Detail)
  const tabCount = await page.evaluate(() => document.querySelectorAll('.tab').length);
  if (tabCount !== 2) fail('Should have exactly 2 tabs (Tasks + Logs), got ' + tabCount);
  console.log('PASS: Only 2 tabs present (Task Detail removed)');

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

  // task-detail-body may not be visible yet (we're on Tasks tab), but check
  // the CSS rule is correct by inspecting the element's style attribute.
  if (scrollableContainers.taskDetailBody.found) {
    if (scrollableContainers.taskDetailBody.overflowY !== 'auto' && scrollableContainers.taskDetailBody.overflowY !== 'scroll') {
      fail('.task-detail-body should have overflow-y:auto or scroll, got ' + scrollableContainers.taskDetailBody.overflowY);
    }
    console.log('PASS: .task-detail-body has overflow-y:', scrollableContainers.taskDetailBody.overflowY);
  }

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
  // Verify COMPLETED and PENDING are inactive by default
  for (const hiddenStatus of ['COMPLETED', 'PENDING']) {
    const btn = filterResult.buttonStates.find(b => b.status === hiddenStatus);
    if (!btn) fail(hiddenStatus + ' filter button not found');
    if (btn.active) fail(hiddenStatus + ' filter should be inactive by default');
    if (!btn.inactive) fail(hiddenStatus + ' filter should have inactive class by default');
  }
  // Verify other statuses are active by default
  for (const btn of filterResult.buttonStates) {
    if (btn.status !== 'COMPLETED' && btn.status !== 'PENDING' && !btn.active) {
      fail(btn.status + ' filter should be active by default');
    }
  }
  console.log('PASS: Filter row rendered with', filterResult.buttonStates.length, 'buttons, COMPLETED and PENDING inactive by default');

  // Verify pending and completed tasks are hidden by default
  const visibleTaskCount = await page.evaluate(() => {
    return document.querySelectorAll('.task-item').length;
  });
  // With 4 tasks (1 RUNNING + 1 FAILED + 1 PENDING + 1 COMPLETED) and PENDING/COMPLETED hidden,
  // only 2 should be visible
  if (visibleTaskCount !== 2) fail('Should show 2 tasks (PENDING and COMPLETED hidden by default), got ' + visibleTaskCount);
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

  await takeScreenshot('01-front-page');

  // =====================================================================
  // Phase 1c: "N pending" expand button — pending tasks hidden/shown
  // =====================================================================
  console.log('=== Phase 1c: Pending expand button ===');

  // With 4 tasks (RUNNING + FAILED + PENDING + COMPLETED) and PENDING/COMPLETED hidden,
  // only 2 should be visible
  const visibleBefore = await page.evaluate(() => {
    return document.querySelectorAll('.task-item').length;
  });
  if (visibleBefore !== 2) fail('Should show 2 tasks (PENDING and COMPLETED hidden by default), got ' + visibleBefore);
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

  // Now 3 tasks should be visible (RUNNING + FAILED + PENDING)
  const visibleAfter = await page.evaluate(() => {
    return document.querySelectorAll('.task-item').length;
  });
  if (visibleAfter !== 3) fail('Should show 3 tasks (PENDING now shown), got ' + visibleAfter);
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

  // Back to 2 visible tasks, "N pending" button reappears
  const visibleFinal = await page.evaluate(() => {
    return document.querySelectorAll('.task-item').length;
  });
  if (visibleFinal !== 2) fail('Should show 2 tasks (PENDING hidden again), got ' + visibleFinal);
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
      logMsgCount: logMsgs.length,
      hasToolMarkersOutsideBlocks,
      // System-level operational messages (Executing task, Cloning, Spawning, etc.)
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
    { name: 'no tool markers outside blocks', pass: !detailResult.hasToolMarkersOutsideBlocks },
    { name: 'tool block has command-line span', pass: detailResult.toolHasCommandLine[0] === true },
    { name: 'tool block has NO tool-id span', pass: detailResult.toolHasNoToolId[0] === true },
    { name: 'command line shows "hostname"', pass: detailResult.toolCommandLineText[0] === 'hostname' },
    { name: '2 thinking blocks rendered', pass: detailResult.thinkingBlockCount === 2 },
    { name: 'thinking block 1 has header', pass: detailResult.thinkingHasHeader[0] === true },
    { name: 'thinking block 1 has non-empty content', pass: detailResult.thinkingBlockContents[0].length > 0 },
    { name: 'thinking block 2 has header', pass: detailResult.thinkingHasHeader[1] === true },
    { name: 'thinking block 2 has non-empty content', pass: detailResult.thinkingBlockContents[1].length > 0 },
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
    { name: 'prompt body is open by default', pass: promptResult.bodyIsOpen === true },
    { name: 'prompt chevron is open by default', pass: promptResult.chevronIsOpen === true },
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

  // Verify prompt can be toggled closed
  await clickEl('.task-detail-prompt-header');
  const promptClosed = await page.evaluate(() => {
    const body = document.querySelector('.task-detail-prompt-body');
    const chevron = document.querySelector('.prompt-chevron');
    return !body?.classList.contains('open') && !chevron?.classList.contains('open');
  });
  if (!promptClosed) fail('Prompt should be closed after clicking header');
  console.log('PASS: Prompt closed after toggle');

  // Verify prompt can be toggled open again
  await clickEl('.task-detail-prompt-header');
  const promptReopened = await page.evaluate(() => {
    const body = document.querySelector('.task-detail-prompt-body');
    const chevron = document.querySelector('.prompt-chevron');
    return body?.classList.contains('open') && chevron?.classList.contains('open');
  });
  if (!promptReopened) fail('Prompt should be open after clicking header again');
  console.log('PASS: Prompt reopened after toggle');

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

  await takeScreenshot('03b-scroll-behaviour');

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

  // Completed tool blocks are rendered open by default. Close one via click.
  await clickEl('.tool-block-header');

  // Verify the block is now closed (body should NOT have .open class)
  const blockClosed = await page.evaluate(() => {
    const body = document.querySelector('.tool-block-body');
    return body && !body.classList.contains('open');
  });
  if (!blockClosed) fail('Tool block should be closed after clicking header');
  console.log('PASS: Tool block closed');

  // Trigger a refresh cycle (simulates the periodic /api/tasks poll).
  // Before the fix, this would call refreshTaskDetail() which rebuilds
  // the entire view and loses the open/closed state.
  await page.evaluate(() => refresh());

  // Allow the refresh to settle
  await page.waitForTimeout(1000);

  // Verify the tool block is STILL closed — state must be preserved
  const blockStillClosed = await page.evaluate(() => {
    const body = document.querySelector('.tool-block-body');
    return body && !body.classList.contains('open');
  });
  if (!blockStillClosed) fail('Tool block should remain closed after polling refresh');
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
  // Phase 12: "Bring to top" button for PENDING tasks
  // =====================================================================
  console.log('=== Phase 12: Bring to top button ===');

  // Navigate to Tasks tab
  await page.locator('.tab').filter({ hasText: 'Tasks' }).click();
  await page.waitForFunction(() => {
    return document.getElementById('tab-tasks').style.display === 'block';
  }, { timeout: 5000 });

  // Ensure PENDING filter is active so the pending task is visible
  const pendingFilterBtn = await page.evaluate(() => {
    return document.querySelector('button[data-status="PENDING"]');
  });
  if (pendingFilterBtn && pendingFilterBtn.classList.contains('inactive')) {
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

  // Verify the "bring to top" button (🔝) is present on the pending task
  // The button is inside .task-btn-row and has onclick containing "bringToTop"
  const bringToTopBtnInList = await page.evaluate((pid) => {
    const items = document.querySelectorAll('.task-item');
    for (const item of items) {
      if (item.textContent.includes(pid)) {
        const btn = item.querySelector('button[onclick*="bringToTop"]');
        if (btn) return { found: true, text: btn.textContent.trim() };
      }
    }
    return { found: false };
  }, pendingTaskId);
  if (!bringToTopBtnInList.found) fail('Bring to top button should be visible on pending task in list');
  console.log('PASS: Bring to top button visible on pending task in list:', bringToTopBtnInList.text);

  // Verify the bring to top button is NOT present on non-pending tasks
  const noBtnOnRunningTask = await page.evaluate((tid) => {
    const items = document.querySelectorAll('.task-item');
    for (const item of items) {
      if (item.textContent.includes(tid)) {
        const btn = item.querySelector('button[onclick*="bringToTop"]');
        return btn === null;
      }
    }
    return true; // task not found, pass
  }, taskId);
  if (!noBtnOnRunningTask) fail('Bring to top button should NOT be on non-pending tasks');
  console.log('PASS: Bring to top button not present on running task');

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

  // Verify the "bring to top" button is also present in the task detail header
  const bringToTopBtnInDetail = await page.evaluate(() => {
    const header = document.querySelector('.task-detail-header');
    if (!header) return { found: false };
    const buttons = header.querySelectorAll('button');
    for (const btn of buttons) {
      if (btn.textContent.includes('Top')) {
        return { found: true, text: btn.textContent.trim() };
      }
    }
    return { found: false };
  });
  if (!bringToTopBtnInDetail.found) fail('Bring to top button should be present in task detail header for pending task');
  console.log('PASS: Bring to top button in detail header:', bringToTopBtnInDetail.text);

  // Click the "bring to top" button in detail view
  await clickEl('button[onclick*="bringToTop"]');
  await page.waitForTimeout(1000);

  // Navigate back to task list to verify the task is now at the top
  await page.locator('.tab').filter({ hasText: 'Tasks' }).click();
  await page.waitForFunction(() => {
    return document.getElementById('tab-tasks').style.display === 'block';
  }, { timeout: 5000 });
  await page.waitForTimeout(500);

  // Verify the pending task is now the first visible task item
  const pendingTaskIsFirst = await page.evaluate((pid) => {
    const items = document.querySelectorAll('.task-item');
    if (items.length === 0) return false;
    return items[0].textContent.includes(pid);
  }, pendingTaskId);
  if (!pendingTaskIsFirst) fail('Pending task should be first in list after bring-to-top');
  console.log('PASS: Pending task moved to top of queue');

  await takeScreenshot('14-bring-to-top');

  console.log('All UI validation checks passed!');
  await browser.close();
})().catch(e => { console.error('Test failed:', e); process.exit(1); });
`, screenshotDir, baseURL, taskID, failedTaskID, completedTaskID, pendingTaskID, failureReason, logDate)

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
