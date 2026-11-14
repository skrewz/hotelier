//go:build integration

// Cancelled-task default-visibility UI test (issue #289). Exercises the
// dashboard's status filter end-to-end: cancelled tasks are hidden by
// default (mirroring COMPLETED), the summary bar still counts them, and
// the CANCELLED toggle in the filter row reveals and hides them again.
// The default must survive a reload, since the status filter is page
// state with no persistence path.
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

func TestCancelledHiddenByDefaultUI(t *testing.T) {
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
	mux.HandleFunc("/api/tasks/search", srv.HandleTaskSearch)
	mux.HandleFunc("/api/tasks/filter", srv.HandleTaskFilter)
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

	// Two pending tasks with no guest connected. One is cancelled over the
	// API below, the other stays PENDING, so the default view hides both
	// statuses and the CANCELLED toggle's effect is observable on its own.
	type taskSpec struct {
		id    string
		title string
	}
	specs := []taskSpec{
		{"cancel-me", "Task destined for the bin"},
		{"stay-pending", "Task that stays queued"},
	}
	for i, s := range specs {
		body, _ := json.Marshal(map[string]interface{}{
			"id":         s.id,
			"prompt":     "cancelled default visibility fixture",
			"title":      s.title,
			"repo_ref":   "https://example.com/org/example-repo.git",
			"created_at": time.Now().Add(time.Duration(i) * time.Second),
		})
		res, err := http.Post(baseURL+"/api/tasks", "application/json", bytes.NewReader(body))
		if err != nil {
			t.Fatalf("create %s: %v", s.id, err)
		}
		res.Body.Close()
		if res.StatusCode != http.StatusCreated {
			t.Fatalf("create %s: status %d", s.id, res.StatusCode)
		}
	}

	// Server-side check independent of the browser: the fixture really is
	// CANCELLED, so a green UI run cannot be hiding a merely-PENDING task.
	res, err := http.Post(baseURL+"/api/tasks/cancel-me/cancel", "application/json", nil)
	if err != nil {
		t.Fatalf("cancel cancel-me: %v", err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("cancel cancel-me: status %d", res.StatusCode)
	}
	res, err = http.Get(baseURL + "/api/tasks/cancel-me")
	if err != nil {
		t.Fatalf("get cancel-me: %v", err)
	}
	var detail struct {
		Task struct {
			Status string `json:"status"`
		} `json:"task"`
	}
	if err := json.NewDecoder(res.Body).Decode(&detail); err != nil {
		t.Fatalf("decode cancel-me: %v", err)
	}
	res.Body.Close()
	if detail.Task.Status != "CANCELLED" {
		t.Fatalf("expected cancel-me to be CANCELLED, got %s", detail.Task.Status)
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
  const errors = [];
  page.on('pageerror', err => errors.push(String(err)));
  page.on('console', msg => { if (msg.type() === 'error') errors.push('console: ' + msg.text()); });

  await page.goto(baseURL);
  await page.waitForFunction(() => document.querySelectorAll('#task-filter-row .task-filter-btn').length === 6, { timeout: 10000 });

  // The default state: CANCELLED toggles off, and the cancelled task is
  // nowhere in the list while the summary bar still counts it.
  const cancelledBtnClass = await page.$eval('#task-filter-row .task-filter-btn[data-status="CANCELLED"]', e => e.className);
  if (!cancelledBtnClass.includes('inactive')) {
    throw new Error('expected the CANCELLED filter toggle to be inactive by default, got class: ' + cancelledBtnClass);
  }
  await page.waitForFunction(() => !document.querySelector('#task-list').textContent.includes('cancel-me'), { timeout: 10000 });
  const listHTML = await page.$eval('#task-list', e => e.textContent);
  if (listHTML.includes('stay-pending')) {
    throw new Error('expected PENDING hidden by default as well, list reads: ' + listHTML);
  }
  const summaryText = await page.$eval('#task-summary', e => e.textContent);
  if (!/\b1\b\s*cancelled/.test(summaryText)) {
    throw new Error('expected the summary bar to count the hidden cancelled task, got: ' + summaryText);
  }
  await page.screenshot({ path: screenshotDir + '/01-cancelled-hidden-by-default.png', fullPage: true });

  // Show PENDING so the list is not empty when the CANCELLED toggle
  // operates: the toggle's effect must be observable against visible rows.
  await page.click('#task-filter-row .task-filter-btn[data-status="PENDING"]');
  await page.waitForFunction(() => document.querySelectorAll('#task-list .task-item').length === 1, { timeout: 10000 });
  const pendingOnly = await page.$eval('#task-list .task-item .task-id', e => e.textContent);
  if (!pendingOnly.includes('stay-pending')) {
    throw new Error('expected the pending task alone, got: ' + pendingOnly);
  }

  // Clicking CANCELLED reveals the cancelled task alongside it.
  await page.click('#task-filter-row .task-filter-btn[data-status="CANCELLED"]');
  await page.waitForFunction(() => document.querySelectorAll('#task-list .task-item').length === 2, { timeout: 10000 });
  const ids = await page.$$eval('#task-list .task-item .task-id', els => els.map(e => e.textContent));
  if (!ids.some(t => t.includes('cancel-me'))) {
    throw new Error('expected the cancelled task after revealing CANCELLED, got: ' + JSON.stringify(ids));
  }
  const toggleClass = await page.$eval('#task-filter-row .task-filter-btn[data-status="CANCELLED"]', e => e.className);
  if (!toggleClass.includes('active')) {
    throw new Error('expected the CANCELLED toggle to read active once clicked, got class: ' + toggleClass);
  }
  await page.screenshot({ path: screenshotDir + '/02-cancelled-revealed.png', fullPage: true });

  // Clicking again hides it once more.
  await page.click('#task-filter-row .task-filter-btn[data-status="CANCELLED"]');
  await page.waitForFunction(() => document.querySelectorAll('#task-list .task-item').length === 1, { timeout: 10000 });
  const rehiding = await page.$eval('#task-list', e => e.textContent);
  if (rehiding.includes('cancel-me')) {
    throw new Error('expected the cancelled task hidden again, list reads: ' + rehiding);
  }

  // The status filter is page state, not persisted state, so a reload
  // must land back on the hidden-by-default view.
  await page.reload();
  await page.waitForFunction(() => document.querySelectorAll('#task-filter-row .task-filter-btn').length === 6, { timeout: 10000 });
  const reloadClass = await page.$eval('#task-filter-row .task-filter-btn[data-status="CANCELLED"]', e => e.className);
  if (!reloadClass.includes('inactive')) {
    throw new Error('expected the CANCELLED toggle inactive again after reload, got class: ' + reloadClass);
  }
  await page.waitForFunction(() => !document.querySelector('#task-list').textContent.includes('cancel-me'), { timeout: 10000 });
  await page.screenshot({ path: screenshotDir + '/03-cancelled-hidden-after-reload.png', fullPage: true });

  if (errors.length) {
    throw new Error('page errors observed: ' + errors.join(' | '));
  }

  await browser.close();
  console.log('OK: cancelled default visibility validated (screenshots in %s)');
})().catch(err => { console.error(err); process.exit(1); });
`, baseURL, screenshotDir, screenshotDir)

	tmpScript := t.TempDir() + "/cancelled_default_ui.js"
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
		t.Fatalf("Playwright cancelled default visibility validation failed: %v (screenshots: %s)", err, screenshotDir)
	}
}
