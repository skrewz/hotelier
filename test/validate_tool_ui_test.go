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
	line       string
	level      string
	toolType   string // "start", "output", "end"
	toolName   string // e.g. "bash", "read"
	toolID     string // unique tool call identifier
	toolArgs   string // arguments/parameters
	toolOutput string // captured output
	toolError  bool   // true if tool ended with error
}

// testLogEntries returns the shared set of simulated log entries.
// These mirror real RPC log entries with structured fields as produced
// by the guest agent after the structured data refactor.
func testLogEntries() []testLogEntry {
	return []testLogEntry{
		{"Task started", "system", "", "", "", "", "", false},
		{"I'll help you with that. Let me start by checking the package.json file.", "text", "", "", "", "", "", false},
		{fmt.Sprintf("[TOOL_START] bash: cat package.json (id: %s)", toolID1()), "tool", "start", "bash", toolID1(), "cat package.json", "", false},
		{fmt.Sprintf("[TOOL_OUTPUT] bash (id: %s): {\"name\": \"example-app\", \"version\": \"1.0.0\"}", toolID1()), "tool", "output", "bash", toolID1(), "", "{\"name\": \"example-app\", \"version\": \"1.0.0\"}", false},
		{fmt.Sprintf("[TOOL_END] bash (id: %s): {\"name\": \"example-app\", \"version\": \"1.0.0\"}", toolID1()), "tool", "end", "bash", toolID1(), "", "{\"name\": \"example-app\", \"version\": \"1.0.0\"}", false},
		{"Let me check the API endpoint and the repo path more carefully.", "text", "", "", "", "", "", false},
		{fmt.Sprintf("[TOOL_START] read: package.json (id: %s)", toolID2()), "tool", "start", "read", toolID2(), "package.json", "", false},
		{fmt.Sprintf("[TOOL_OUTPUT] read (id: %s): {\"name\": \"example-app\", \"version\": \"1.0.0\"}", toolID2()), "tool", "output", "read", toolID2(), "", "{\"name\": \"example-app\", \"version\": \"1.0.0\"}", false},
		{fmt.Sprintf("[TOOL_END] read (id: %s)", toolID2()), "tool", "end", "read", toolID2(), "", "", true},
		{"Task completed successfully", "system", "", "", "", "", "", false},
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
		// Only check structured fields for tool entries
		if expected.level == "tool" {
			if entries[i].ToolType != expected.toolType {
				t.Errorf("entry %d: expected tool_type %q, got %q", i, expected.toolType, entries[i].ToolType)
			}
			if entries[i].ToolName != expected.toolName {
				t.Errorf("entry %d: expected tool_name %q, got %q", i, expected.toolName, entries[i].ToolName)
			}
			if entries[i].ToolID != expected.toolID {
				t.Errorf("entry %d: expected tool_id %q, got %q", i, expected.toolID, entries[i].ToolID)
			}
			if entries[i].ToolArgs != expected.toolArgs {
				t.Errorf("entry %d: expected tool_args %q, got %q", i, expected.toolArgs, entries[i].ToolArgs)
			}
			if entries[i].ToolOutput != expected.toolOutput {
				t.Errorf("entry %d: expected tool_output %q, got %q", i, expected.toolOutput, entries[i].ToolOutput)
			}
			if entries[i].ToolError != expected.toolError {
				t.Errorf("entry %d: expected tool_error %v, got %v", i, expected.toolError, entries[i].ToolError)
			}
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

  await takeScreenshot('01-front-page');

  // =====================================================================
  // Phase 2: Click task → Task Detail view
  // =====================================================================
  console.log('=== Phase 2: Task Detail ===');
  // Must use clickEl() — page.click() does NOT fire onclick attribute
  // handlers on elements rendered via innerHTML (which is how task items work).
  await clickEl('.task-item');
  await page.waitForSelector('.task-detail-header', { timeout: 5000 });

  // Verify URL updated
  const detailUrl = await page.evaluate(() => window.location.pathname);
  if (!detailUrl.startsWith('/task/')) fail('URL should start with /task/, got: ' + detailUrl);
  console.log('PASS: URL updated to', detailUrl);

  // Verify Task Detail tab is active
  const detailTabActive = await page.evaluate(() => {
    const tabs = document.querySelectorAll('.tab');
    for (const tab of tabs) {
      if (tab.textContent.trim() === 'Task Detail' && tab.classList.contains('active')) {
        return true;
      }
    }
    return false;
  });
  if (!detailTabActive) fail('Task Detail tab should be active after clicking task');
  console.log('PASS: Task Detail tab active');

  // Verify detail header shows task info
  const headerOk = await page.evaluate(() => {
    const header = document.querySelector('.task-detail-header');
    const meta = document.querySelector('.task-detail-meta');
    return header !== null && meta !== null;
  });
  if (!headerOk) fail('Task detail header/meta missing');
  console.log('PASS: Task detail header rendered');

  // Verify Close button exists
  const closeBtn = await page.evaluate(() => {
    return document.querySelector('.task-detail-header .btn-ghost') !== null;
  });
  if (!closeBtn) fail('Close button missing in task detail header');
  console.log('PASS: Close button present');

  await takeScreenshot('02-task-detail');

  // =====================================================================
  // Phase 3: DOM validation of task detail (tool blocks, log messages)
  // =====================================================================
  console.log('=== Phase 3: Task detail DOM validation ===');

  const detailResult = await page.evaluate(() => {
    const body = document.querySelector('.task-detail-body');
    if (!body) return { error: 'no .task-detail-body found' };

    const toolBlocks = body.querySelectorAll('.tool-block');
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
      logMsgCount: logMsgs.length,
      hasPlainMsg: Array.from(logMsgs).some(m => m.textContent?.includes("Let me check")),
      hasSystemMsg: Array.from(logMsgs).some(m => m.classList.contains('system')),
      hasIntroMsg: Array.from(logMsgs).some(m => m.textContent?.includes("I'll help you")),
      hasToolMarkersOutsideBlocks,
      promptVisible: document.querySelector('.task-detail-prompt') !== null,
    };
  });

  if (detailResult.error) {
    fail('DOM validation error: ' + detailResult.error);
    await takeScreenshot('03-dom-error');
    process.exit(1);
  }

  const detailChecks = [
    { name: '2 tool blocks rendered', pass: detailResult.toolBlockCount === 2 },
    { name: 'unique tool block IDs', pass: detailResult.toolBlockIds[0] !== detailResult.toolBlockIds[1] },
    { name: 'first tool named "bash"', pass: detailResult.toolNames[0] === 'bash' },
    { name: 'second tool named "read"', pass: detailResult.toolNames[1] === 'read' },
    { name: 'first block status "done"', pass: detailResult.toolStatuses[0] === 'done' },
    { name: 'second block status "error"', pass: detailResult.toolStatuses[1] === 'error' },
    { name: 'first block has <pre> for output', pass: detailResult.toolOutputsHavePre[0] },
    { name: 'second block has <pre> for output', pass: detailResult.toolOutputsHavePre[1] },
    { name: 'first output contains JSON', pass: detailResult.toolOutputContents[0].includes("example-app") },
    { name: 'second output contains JSON', pass: detailResult.toolOutputContents[1].includes("example-app") },
    { name: 'plain text message between tools', pass: detailResult.hasPlainMsg },
    { name: 'intro text message present', pass: detailResult.hasIntroMsg },
    { name: 'system messages present', pass: detailResult.hasSystemMsg },
    { name: 'no tool markers outside blocks', pass: !detailResult.hasToolMarkersOutsideBlocks },
    { name: 'prompt visible', pass: detailResult.promptVisible },
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
  const breadcrumbOk = await page.evaluate((date) => {
    const crumbs = document.querySelectorAll('.log-breadcrumb .crumb, .log-breadcrumb .current');
    const text = Array.from(crumbs).map(c => c.textContent).join(' ');
    return text.includes(date);
  }, logDate);
  if (!breadcrumbOk) fail('Breadcrumb should contain date ' + logDate);
  console.log('PASS: Breadcrumb shows date');

  // Verify URL updated to /logs/:date
  const dateUrl = await page.evaluate(() => window.location.pathname);
  if (!dateUrl.startsWith('/logs/' + logDate)) fail('URL should start with /logs/' + logDate + ', got: ' + dateUrl);
  console.log('PASS: URL updated to', dateUrl);

  // Verify task items are listed (our test task should be present)
  const taskItems = await page.$$('.log-task-item');
  if (taskItems.length === 0) fail('No task items found under date ' + logDate);
  console.log('PASS:', taskItems.length, 'task(s) found under date');

  // Verify breadcrumb has "All Dates" crumb for navigation back
  const allDatesCrumb = await page.evaluate(() => {
    const crumbs = document.querySelectorAll('.log-breadcrumb .crumb');
    return Array.from(crumbs).some(c => c.textContent === 'All Dates');
  });
  if (!allDatesCrumb) fail('Breadcrumb should have "All Dates" crumb');
  console.log('PASS: "All Dates" breadcrumb crumb present');

  await takeScreenshot('05-logs-tasks-under-date');

  // --- Click the task item to load its log entries ---
  console.log('--- Clicking task item ---');
  await clickEl('.log-task-item');

  // Wait for log entries to render
  await page.waitForSelector('.log-entry-line', { timeout: 10000 });

  // Verify log entries are rendered
  const logEntryResult = await page.evaluate(() => {
    const entries = document.querySelectorAll('.log-entry-line');
    if (entries.length === 0) return { error: 'no log entries found' };

    return {
      entryCount: entries.length,
      hasTimestamps: Array.from(entries).some(e => e.querySelector('.log-timestamp') !== null),
      hasLevelBadges: Array.from(entries).some(e => e.querySelector('.log-level-badge') !== null),
      hasContent: Array.from(entries).some(e => e.querySelector('.log-content') !== null),
      levels: Array.from(entries).map(e => e.dataset.level || ''),
      hasToolEntries: Array.from(entries).some(e => e.dataset.level === 'tool'),
      hasTextEntries: Array.from(entries).some(e => e.dataset.level === 'text'),
      hasSystemEntries: Array.from(entries).some(e => e.dataset.level === 'system'),
      breadcrumbCrumbs: Array.from(document.querySelectorAll('.log-breadcrumb .crumb, .log-breadcrumb .current'))
        .map(c => c.textContent),
    };
  });

  if (logEntryResult.error) fail(logEntryResult.error);

  const logEntryChecks = [
    { name: 'log entries rendered', pass: logEntryResult.entryCount > 0 },
    { name: 'entries have timestamps', pass: logEntryResult.hasTimestamps },
    { name: 'entries have level badges', pass: logEntryResult.hasLevelBadges },
    { name: 'entries have content', pass: logEntryResult.hasContent },
    { name: 'tool-level entries present', pass: logEntryResult.hasToolEntries },
    { name: 'text-level entries present', pass: logEntryResult.hasTextEntries },
    { name: 'system-level entries present', pass: logEntryResult.hasSystemEntries },
    { name: 'breadcrumb has date crumb', pass: logEntryResult.breadcrumbCrumbs.some(c => c.includes('-')) },
    { name: 'breadcrumb has All Dates crumb', pass: logEntryResult.breadcrumbCrumbs.some(c => c === 'All Dates') },
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

  // Navigate directly to /task/:id
  await page.goto(baseURL + '/task/' + taskId);
  await page.waitForLoadState('networkidle');
  await page.waitForSelector('.task-detail-header', { timeout: 5000 });

  const directDetail = await page.evaluate(() => {
    return document.querySelector('.task-detail-header') !== null &&
           document.querySelector('.task-detail-body') !== null;
  });
  if (!directDetail) fail('Direct nav to /task/:id should show detail view');
  console.log('PASS: Direct nav to /task/:id works');

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

  // Start from Task Detail (via direct URL)
  await page.goto(baseURL + '/task/' + taskId);
  await page.waitForLoadState('networkidle');
  await page.waitForSelector('.task-detail-header', { timeout: 5000 });

  // Verify we're in Task Detail view
  const inDetailView = await page.evaluate(() => {
    return document.querySelector('.task-detail-header') !== null &&
           document.getElementById('tab-detail').style.display === 'block';
  });
  if (!inDetailView) fail('Should be in Task Detail view');
  console.log('PASS: In Task Detail view');

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
           document.getElementById('tab-detail').style.display === 'none';
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
  await page.click('.task-item');
  await page.waitForSelector('.task-detail-header', { timeout: 5000 });

  // Verify we're back in Task Detail view
  const backInDetail = await page.evaluate(() => {
    return document.querySelector('.task-detail-header') !== null &&
           document.getElementById('tab-detail').style.display === 'block';
  });
  if (!backInDetail) fail('Should be in Task Detail view after clicking task');
  console.log('PASS: Back in Task Detail view after clicking existing task');

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
  // Phase 8: Close task detail and return to Tasks tab
  // =====================================================================
  console.log('=== Phase 8: Close task detail ===');

  // Navigate back to task detail
  await page.goto(baseURL + '/task/' + taskId);
  await page.waitForLoadState('networkidle');
  await page.waitForSelector('.task-detail-header', { timeout: 5000 });

  // Click Close button
  await page.click('.task-detail-header .btn-ghost');

  // Wait for Tasks tab to be active
  await page.waitForFunction(() => {
    const tabs = document.querySelectorAll('.tab');
    for (const tab of tabs) {
      if (tab.textContent.trim() === 'Tasks' && tab.classList.contains('active')) {
        return true;
      }
    }
    return false;
  }, { timeout: 5000 });

  const tasksAfterClose = await page.evaluate(() => {
    return document.getElementById('tab-tasks').style.display === 'block' &&
           document.getElementById('tab-detail').style.display === 'none';
  });
  if (!tasksAfterClose) fail('After Close, Tasks tab should be active');
  console.log('PASS: Close button returns to Tasks tab');

  const closeUrl = await page.evaluate(() => window.location.pathname);
  if (closeUrl !== '/tasks' && closeUrl !== '/') fail('URL should be /tasks or / after Close, got: ' + closeUrl);
  console.log('PASS: URL is', closeUrl, 'after Close');

  await takeScreenshot('09-after-close');

  console.log('All UI validation checks passed!');
  await browser.close();
})().catch(e => { console.error('Test failed:', e); process.exit(1); });
`, screenshotDir, baseURL, taskID, logDate)

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
