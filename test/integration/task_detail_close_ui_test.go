//go:build integration

// Task detail Close button test (issue #207). It clicks the detail header's
// Close button from both entry paths — through the task list and through a
// deep link to /task/:id — and asserts the dashboard lands back on /tasks,
// with the Tasks panel visible, the detail view off screen, full-screen mode
// left, and without a single page error.
package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"hotelier/internal/server"
	"hotelier/pkg/config"
)

func TestTaskDetailCloseUI(t *testing.T) {
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("get cwd: %v", err)
	}
	p := filepath.Clean(filepath.Join(wd, "..", ".."))
	if err := os.Chdir(p); err != nil {
		t.Fatalf("chdir to project root: %v", err)
	}
	// Other tests in this package derive the project root from the cwd they
	// were handed, so leave it as we found it.
	defer func() { _ = os.Chdir(wd) }()
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

	baseURL := fmt.Sprintf("http://127.0.0.1:%d", ln.Addr().(*net.TCPAddr).Port)
	t.Logf("Server running at %s", baseURL)

	// One pending task with no guest connected, so it stays PENDING and the
	// detail view has something to render.
	body, _ := json.Marshal(map[string]interface{}{
		"id":         "close-target",
		"prompt":     "close button fixture",
		"title":      "Close button target",
		"created_at": time.Now(),
	})
	res, err := http.Post(baseURL+"/api/tasks", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("create task: %v", err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusCreated {
		t.Fatalf("create task: status %d", res.StatusCode)
	}

	// Allow overriding the screenshot directory via env var for manual
	// inspection. When set, screenshots survive t.TempDir() cleanup.
	var screenshotDir string
	if overrideDir := os.Getenv("SCREENSHOT_DIR"); overrideDir != "" {
		screenshotDir = overrideDir
	} else {
		screenshotDir = t.TempDir()
	}
	if err := os.MkdirAll(screenshotDir, 0o755); err != nil {
		t.Fatalf("create screenshot dir: %v", err)
	}
	script := fmt.Sprintf(`
const { chromium } = require('playwright');
(async () => {
  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage({ viewport: { width: 1280, height: 900 } });
  const baseURL = '%s';
  const screenshotDir = '%s';
  // The reproduction in issue #207 is an uncaught ReferenceError, which
  // Playwright reports through pageerror, so pageerror is the hard gate.
  // Console errors from elsewhere in the dashboard (refresh retries,
  // WebSocket reconnect noise, a missing asset) are logged for context but
  // must not fail a Close-button test.
  const pageErrors = [];
  const armErrorGate = (p) => {
    p.on('pageerror', err => pageErrors.push(String(err)));
    p.on('console', msg => { if (msg.type() === 'error') console.log('console error (not fatal): ' + msg.text()); });
  };
  armErrorGate(page);

  // Click the detail header's Close button. It carries no class of its
  // own, so find it by its exact label among the header buttons; the real
  // elementHandle.click() is actionability-checked, like the clicks in
  // cancelled_filter_ui_test.go and preference_filter_ui_test.go.
  const clickClose = async (p) => {
    const btn = await p.$('.task-detail-header button:text-is("Close")');
    if (!btn) throw new Error('Close button not found in the task detail header');
    await btn.click();
  };

  // After Close: the route is /tasks, the Tasks panel is the visible one,
  // and no task detail header remains on screen.
  const assertBackOnList = async (p, label) => {
    await p.waitForFunction(() => location.pathname === '/tasks', { timeout: 10000 });
    const state = await p.evaluate(() => ({
      tasksVisible: getComputedStyle(document.getElementById('tab-tasks')).display !== 'none',
      logsVisible: getComputedStyle(document.getElementById('tab-logs')).display !== 'none',
      // The detail markup lives in the hidden Logs panel after a tab
      // switch, so 'gone' means not rendered, not removed from the DOM.
      detailHeaderVisible: (() => {
        const h = document.querySelector('.task-detail-header');
        return !!h && h.offsetParent !== null;
      })(),
      fullscreen: document.body.classList.contains('logs-fullscreen'),
    }));
    if (!state.tasksVisible) throw new Error(label + ': expected the Tasks panel to be visible');
    if (state.logsVisible) throw new Error(label + ': expected the Logs panel to be hidden');
    if (state.detailHeaderVisible) throw new Error(label + ': expected the task detail header to be off screen');
    if (state.fullscreen) throw new Error(label + ': expected full-screen mode to be left');
  };

  // Entry 1: task list → click the task item → detail → Close.
  await page.goto(baseURL);
  // The dashboard hides PENDING tasks by default; show them first.
  await page.evaluate(() => toggleTaskFilter('PENDING'));
  await page.waitForFunction(() => document.querySelectorAll('#task-list .task-item').length === 1, { timeout: 10000 });
  await page.evaluate(() => document.querySelector('.task-item[onclick*="close-target"]').click());
  await page.waitForSelector('.task-detail-header', { timeout: 10000 });
  await page.screenshot({ path: screenshotDir + '/01-detail-open.png', fullPage: true });

  // Enter full screen before closing, so the fullscreen check in
  // assertBackOnList has something to fail on: at this viewport the mode's
  // CSS never activates on its own, and Close must leave the mode.
  const enteredFullscreen = await page.evaluate(() => {
    toggleLogsFullscreen();
    return document.body.classList.contains('logs-fullscreen');
  });
  if (!enteredFullscreen) throw new Error('from the task list: could not enter full screen, so leaving it would go untested');

  await clickClose(page);
  await assertBackOnList(page, 'from the task list');
  await page.screenshot({ path: screenshotDir + '/02-closed-to-list.png', fullPage: true });

  // Entry 2: deep link straight to /task/:id in a fresh page, so the
  // history really is empty and history.back() would leave the app; the
  // route must be an explicit push to /tasks.
  const page2 = await browser.newPage({ viewport: { width: 1280, height: 900 } });
  armErrorGate(page2);
  await page2.goto(baseURL + '/task/close-target');
  await page2.waitForSelector('.task-detail-header', { timeout: 10000 });
  await page2.screenshot({ path: screenshotDir + '/03-deeplink-detail.png', fullPage: true });

  await clickClose(page2);
  await assertBackOnList(page2, 'from the deep link');
  await page2.screenshot({ path: screenshotDir + '/04-deeplink-closed.png', fullPage: true });

  // Any uncaught page error — the ReferenceError of issue #207 among
  // them — fails this test.
  if (pageErrors.length) {
    throw new Error('page errors observed: ' + pageErrors.join(' | '));
  }

  await browser.close();
  console.log('OK: task detail Close button validated (screenshots in %s)');
})().catch(err => { console.error(err); process.exit(1); });
`, baseURL, screenshotDir, screenshotDir)

	tmpScript := t.TempDir() + "/task_detail_close_ui.js"
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
		t.Fatalf("Playwright task detail Close validation failed: %v (screenshots: %s)", err, screenshotDir)
	}
}
