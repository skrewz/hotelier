//go:build integration

// Package test provides integration tests for the hotelier UI.
// Run with: go test -tags=integration ./test/ -run TestValidateToolUI
package test

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"hotelier/internal/server"
	"hotelier/pkg/config"
	"hotelier/pkg/queue"
	"hotelier/pkg/rpc"
)

// testLogEntry represents a single log entry used across both the RPC and
// Playwright validation paths.
type testLogEntry struct {
	line  string
	level string
}

// testLogEntries returns the shared set of simulated log entries.
func testLogEntries() []testLogEntry {
	return []testLogEntry{
		{"Task started", "system"},
		{fmt.Sprintf("[TOOL_START] bash: cat package.json (id: %s)", toolID1()), "tool"},
		{fmt.Sprintf("[TOOL_OUTPUT] bash (id: %s): {\"name\": \"example-app\", \"version\": \"1.0.0\"}", toolID1()), "tool"},
		{fmt.Sprintf("[TOOL_END] bash (id: %s): {\"name\": \"example-app\", \"version\": \"1.0.0\"}", toolID1()), "tool"},
		{"Let me check the API endpoint and the repo path more carefully.", "text"},
		{fmt.Sprintf("[TOOL_START] read: package.json (id: %s)", toolID2()), "tool"},
		{fmt.Sprintf("[TOOL_OUTPUT] read (id: %s): {\"name\": \"example-app\", \"version\": \"1.0.0\"}", toolID2()), "tool"},
		{fmt.Sprintf("[TOOL_END] read (id: %s) [ERROR]", toolID2()), "tool"},
		{"Task completed successfully", "system"},
	}
}

func TestValidateToolUI(t *testing.T) {
	// Ensure we're in the project root so relative paths (webDir, templateDir)
	// resolve correctly — this keeps us on the same code path as production.
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("get cwd: %v", err)
	}
	p := filepath.Clean(filepath.Join(wd, ".."))
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
		Repos:  []string{"/tmp/test-repo"},
		Prompt: "Simulate tool calls and validate UI rendering",
		Tags:   []string{"business-default"},
	}
	if err := srv.TaskQueue().Add(taskObj); err != nil {
		t.Fatalf("add task: %v", err)
	}
	t.Logf("Task created: %s", taskID)

	// Step 3: Assign task to guest and simulate tool call log entries.
	assignParams, _ := json.Marshal(map[string]interface{}{
		"id":          taskID,
		"repos":       []string{"/tmp/test-repo"},
		"prompt":      "Simulate tool calls and validate UI rendering",
		"tags":        []string{"business-default"},
		"assigned_to": "ui-test-guest",
	})
	hub.Broadcast(rpc.ConnectionRoleGuest, &rpc.JSONRPCMessage{
		JSONRPC: "2.0", Method: "task.assign", Params: assignParams,
	})
	time.Sleep(100 * time.Millisecond)

	logEntries := testLogEntries()

	for _, entry := range logEntries {
		logParams, _ := json.Marshal(map[string]interface{}{
			"task_id": taskID,
			"line":    entry.line,
			"level":   entry.level,
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

	// Step 4: Validate server-side log store has correct entries.
	validateServerLogs(t, srv, taskID, logEntries)

	// Step 5: Validate UI rendering via Playwright by exercising the real
	// user flow: navigate → task list → click task → detail view.
	validateUI(t, baseURL, taskID, projectRoot)
}

func validateServerLogs(t *testing.T, srv *server.Server, taskID string, expectedEntries []testLogEntry) {
	logStore := srv.LogStore()
	if logStore == nil {
		t.Skip("log store not configured, skipping server-side validation")
	}

	entries := logStore.Get(taskID)

	if len(entries) != len(expectedEntries) {
		t.Errorf("expected %d log entries, got %d", len(expectedEntries), len(entries))
		for i, e := range entries {
			t.Logf("  entry %d: [%s] %s", i, e.Level, e.Line[:min(50, len(e.Line))])
		}
		return
	}

	for i, expected := range expectedEntries {
		if entries[i].Line != expected.line {
			t.Errorf("entry %d: expected line %q, got %q", i, expected.line[:min(40, len(expected.line))], entries[i].Line[:min(40, len(entries[i].Line))])
		}
		if entries[i].Level != expected.level {
			t.Errorf("entry %d: expected level %q, got %q", i, expected.level, entries[i].Level)
		}
	}
}

func validateUI(t *testing.T, baseURL, taskID, projectRoot string) {
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

	script := fmt.Sprintf(`
const { chromium } = require('playwright');
(async () => {
  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage({ viewport: { width: 1280, height: 900 } });
  const errors = [];
  page.on('pageerror', err => errors.push(err.message));

  const screenshotDir = '%s';
  const path = require('path');

  async function takeScreenshot(name) {
    const filePath = path.join(screenshotDir, name + '.png');
    await page.screenshot({ path: filePath, fullPage: true });
    console.log('Screenshot saved:', filePath);
  }

  // --- Navigate and take front-page screenshot ---
  await page.goto('%s');
  await page.waitForLoadState('networkidle');

  if (errors.length > 0) {
    console.error('JS errors on page load:', errors);
    await takeScreenshot('01-page-load');
    process.exit(1);
  }

  // Wait for the task list to populate — the Go test has already sent
  // logs via WebSocket; the server broadcast should have triggered the
  // client-side refreshTaskList call that renders .task-item elements.
  await page.waitForSelector('.task-item', { timeout: 10000 });

  // Screenshot 1: front page (Tasks tab with the submitted task visible)
  await takeScreenshot('01-front-page');

  // Screenshot 2: front page after task submitted (same view, explicit label)
  await takeScreenshot('02-after-task-submitted');

  // Click the task item — this triggers selectTask() → refreshTaskDetail()
  // which fetches from /api/tasks/{taskId} and renders the detail view.
  await page.click('.task-item');
  await page.waitForSelector('.task-detail-header', { timeout: 5000 });

  // Screenshot 3: task detail view after clicking the task
  await takeScreenshot('03-task-clicked-detail');

  // Wait for any pending WebSocket updates to arrive and be processed.
  await new Promise(r => setTimeout(r, 1000));

  // --- Switch to Logs tab ---
  // Screenshot 4: logs page (showing dates)
  await page.evaluate(async () => {
    // Manually switch tab (avoid relying on implicit event global)
    document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
    document.querySelectorAll('.tab').forEach(t => {
      if (t.textContent.trim() === 'Logs') t.classList.add('active');
    });
    document.getElementById('tab-tasks').style.display = 'none';
    document.getElementById('tab-detail').style.display = 'none';
    document.getElementById('tab-logs').style.display = 'block';
    // Load the log dates
    await loadLogDates();
  });
  await new Promise(r => setTimeout(r, 1000));
  await takeScreenshot('04-logs-page');

  // Screenshot 5: logs page under the most recent date
  // Click the first (most recent) date entry
  const hasDateItem = await page.evaluate(() => {
    return document.querySelector('.log-task-item') !== null;
  });
  if (hasDateItem) {
    await page.click('.log-task-item');
    await new Promise(r => setTimeout(r, 1500));
    await takeScreenshot('05-logs-under-date');
  } else {
    console.log('No date items found in logs view — skipping date screenshot');
  }

  // --- Return to Task Detail tab for DOM validation ---
  await page.evaluate(() => {
    document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
    document.querySelectorAll('.tab').forEach(t => {
      if (t.textContent.trim() === 'Task Detail') t.classList.add('active');
    });
    document.getElementById('tab-tasks').style.display = 'none';
    document.getElementById('tab-detail').style.display = 'block';
    document.getElementById('tab-logs').style.display = 'none';
  });
  await new Promise(r => setTimeout(r, 500));

  // Validate the rendered DOM.
  const result = await page.evaluate(() => {
    const body = document.querySelector('.task-detail-body');
    if (!body) return { error: 'no .task-detail-body found' };

    const toolBlocks = body.querySelectorAll('.tool-block');
    const logMsgs = body.querySelectorAll(':scope > .log-msg');

    // Check that no plain-text log-msg contains tool markers — they should
    // be inside tool blocks, not duplicated as separate log lines.
    const toolMarkerLines = ['[TOOL_OUTPUT]', '[TOOL_END]', '[TOOL_START]'];
    const hasToolMarkersOutsideBlocks = Array.from(logMsgs).some(m =>
      toolMarkerLines.some(marker => m.textContent.includes(marker))
    );

    return {
      toolBlockCount: toolBlocks.length,
      toolBlockIds: Array.from(toolBlocks).map(b => b.id),
      toolStatuses: Array.from(toolBlocks).map(b => b.querySelector('.tool-status')?.textContent?.trim()),
      toolOutputsHavePre: Array.from(toolBlocks).map(b => b.querySelector('.tool-output pre') !== null),
      toolOutputContents: Array.from(toolBlocks).map(b => {
        const pre = b.querySelector('.tool-output pre');
        return pre ? pre.textContent.substring(0, 50) : '';
      }),
      logMsgCount: logMsgs.length,
      hasPlainMsg: Array.from(logMsgs).some(m => m.textContent?.includes("Let me check")),
      hasToolMarkersOutsideBlocks,
    };
  });

  if (result.error) {
    console.error('DOM validation error:', result.error);
    await takeScreenshot('06-dom-error');
    process.exit(1);
  }

  const checks = [
    { name: '2 tool blocks', pass: result.toolBlockCount === 2 },
    { name: 'unique tool block IDs', pass: result.toolBlockIds[0] !== result.toolBlockIds[1] },
    { name: 'first block status "done"', pass: result.toolStatuses[0] === 'done' },
    { name: 'second block status "error"', pass: result.toolStatuses[1] === 'error' },
    { name: 'first block has <pre> for output', pass: result.toolOutputsHavePre[0] },
    { name: 'first output contains JSON', pass: result.toolOutputContents[0] && result.toolOutputContents[0].includes("example-app") },
    { name: 'plain text msg after tools', pass: result.hasPlainMsg },
    { name: 'no tool markers outside blocks', pass: !result.hasToolMarkersOutsideBlocks },
  ];

  let failed = false;
  for (const check of checks) {
    if (!check.pass) {
      console.error('FAIL:', check.name);
      failed = true;
    } else {
      console.log('PASS:', check.name);
    }
  }

  if (failed) {
    await takeScreenshot('07-validation-failed');
    process.exit(1);
  }
  console.log('All UI validation checks passed!');
  await browser.close();
})().catch(e => { console.error('Test failed:', e); process.exit(1); });
`, screenshotDir, baseURL)

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

	// List any screenshots taken
	files, _ := os.ReadDir(screenshotDir)
	if len(files) > 0 {
		t.Log("Screenshots taken:")
		for _, f := range files {
			t.Logf("  %s", filepath.Join(screenshotDir, f.Name()))
		}
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
