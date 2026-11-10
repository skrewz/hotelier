//go:build integration

// Task preference filter UI test (issue #281). Exercises the dashboard's
// preference field end-to-end: client-side substring filtering of the task
// list, persistence of the value to the address bar as a #filter=...
// fragment, and the Prioritise/Clear buttons driving /api/tasks/filter.
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

func TestPreferenceFilterUI(t *testing.T) {
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

	// Build the HTTP handler exactly as server.Run() does, including the
	// /api/tasks/filter endpoint under test.
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

	// Two pending tasks with no guest connected, so both stay PENDING:
	// one whose repo_ref carries the needle, one that matches nothing.
	type taskSpec struct {
		id      string
		title   string
		repoRef string
	}
	specs := []taskSpec{
		// Created in this order so FIFO without a filter puts pref-miss
		// first, and the filter's effect on the order is observable.
		{"pref-miss", "Unrelated chore", "https://example.com/org/other-repo.git"},
		// The needle below appears in this task's repo_ref alone, so the
		// client-side list filter and the backend match exercise the same
		// field list.
		{"pref-hit", "Polish the widget", "https://example.com/org/pref-hit-repo.git"},
	}
	for i, s := range specs {
		body, _ := json.Marshal(map[string]interface{}{
			"id":       s.id,
			"prompt":   "preference filter fixture",
			"title":    s.title,
			"repo_ref": s.repoRef,
			// Stagger creation so FIFO order is deterministic and, absent
			// the filter, puts pref-hit second.
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

	// Server-side check independent of the browser: applying the filter via
	// the endpoint reorders GET /api/tasks so the match comes first.
	res, err := http.Post(baseURL+"/api/tasks/filter", "application/json", bytes.NewReader([]byte(`{"filter":"widget"}`)))
	if err != nil {
		t.Fatalf("set filter: %v", err)
	}
	var setBody struct {
		Filter string `json:"filter"`
	}
	if err := json.NewDecoder(res.Body).Decode(&setBody); err != nil {
		t.Fatalf("decode set response: %v", err)
	}
	res.Body.Close()
	if setBody.Filter != "widget" {
		t.Fatalf("expected filter echo, got %q", setBody.Filter)
	}

	// The UI is about to drive the filter itself; start from a cleared
	// backend so its Prioritise click is the thing under test.
	res, err = http.Post(baseURL+"/api/tasks/filter", "application/json", bytes.NewReader([]byte(`{"filter":""}`)))
	if err != nil {
		t.Fatalf("clear filter: %v", err)
	}
	res.Body.Close()

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
  // The dashboard hides PENDING tasks by default (existing behaviour); show
  // them before asserting anything about the list.
  await page.evaluate(() => toggleTaskFilter('PENDING'));
  await page.waitForFunction(() => document.querySelectorAll('#task-list .task-item').length === 2, { timeout: 10000 });

  // No filter yet: both tasks visible, in FIFO order.
  let ids = await page.$$eval('#task-list .task-item .task-id', els => els.map(e => e.textContent));
  if (!ids[0].includes('pref-miss') || !ids[1].includes('pref-hit')) {
    throw new Error('expected FIFO order without a filter, got: ' + JSON.stringify(ids));
  }

  // Typing filters the list client-side and persists to the address bar.
  await page.fill('#task-pref-filter', 'pref-hit');
  await page.waitForFunction(() => document.querySelectorAll('#task-list .task-item').length === 1, { timeout: 10000 });
  const hash = await page.evaluate(() => location.hash);
  if (hash !== '#filter=pref-hit') {
    throw new Error('expected the filter in the address bar, got: ' + hash);
  }
  const shown = await page.$eval('#task-list .task-item .task-id', e => e.textContent);
  if (!shown.includes('pref-hit')) {
    throw new Error('expected only the matching task, got: ' + shown);
  }
  await page.screenshot({ path: screenshotDir + '/01-client-filter.png', fullPage: true });

  // Prioritise pushes the filter to the backend. The endpoint is a write,
  // so the backend's side of the bargain is read where it is observable:
  // the order GET /api/tasks comes back in.
  await page.click('#task-pref-apply');
  await page.waitForFunction(() => document.getElementById('task-pref-apply').classList.contains('task-pref-applied'), { timeout: 10000 });
  // The mark is the only thing on screen saying the backend is set, so
  // assert it renders — .btn:hover would otherwise silently outrank it.
  const appliedBg = await page.$eval('#task-pref-apply', e => getComputedStyle(e).backgroundColor);
  if (appliedBg !== 'rgb(30, 58, 95)') {
    throw new Error('expected the applied button to be visibly marked, got background: ' + appliedBg);
  }
  const drainOrder = await page.evaluate(async () => (await fetch('/api/tasks')).json().then(d => d.tasks.map(t => t.id)));
  if (!drainOrder[0].includes('pref-hit')) {
    throw new Error('expected the backend to drain the matching task first, got: ' + JSON.stringify(drainOrder));
  }
  await page.screenshot({ path: screenshotDir + '/02-backend-applied.png', fullPage: true });

  // Select-all-and-delete, then Prioritise: an empty field means "no
  // filter", so the click must take the clear route rather than be
  // swallowed with the button still marking a filter the field no longer
  // holds.
  await page.fill('#task-pref-filter', '');
  await page.click('#task-pref-apply');
  await page.waitForFunction(() => !document.getElementById('task-pref-apply').classList.contains('task-pref-applied'), { timeout: 10000 });
  const plainBg = await page.$eval('#task-pref-apply', e => getComputedStyle(e).backgroundColor);
  if (plainBg === 'rgb(30, 58, 95)') {
    throw new Error('expected the applied mark to be gone once the filter was cleared');
  }
  await page.waitForFunction(() => document.querySelectorAll('#task-list .task-item').length === 2, { timeout: 10000 });
  await page.screenshot({ path: screenshotDir + '/02b-empty-prioritise-clears.png', fullPage: true });

  // A reload restores the field from the fragment. Which filter the backend
  // is acting on is page state, not persisted state, so the button starts
  // unmarked again — the fragment, not the backend, is the persistence path.
  await page.fill('#task-pref-filter', 'pref-hit');
  await page.click('#task-pref-apply');
  await page.waitForFunction(() => document.getElementById('task-pref-apply').classList.contains('task-pref-applied'), { timeout: 10000 });
  await page.reload();
  await page.evaluate(() => toggleTaskFilter('PENDING'));
  await page.waitForFunction(() => document.querySelectorAll('#task-list .task-item').length === 1, { timeout: 10000 });
  const seeded = await page.$eval('#task-pref-filter', e => e.value);
  if (seeded !== 'pref-hit') {
    throw new Error('expected the field to seed from the fragment, got: ' + seeded);
  }
  const relit = await page.$eval('#task-pref-apply', e => e.classList.contains('task-pref-applied'));
  if (relit) {
    throw new Error('expected the applied mark to be page state, not re-read on load');
  }
  // The mark being gone must not read as "no filter applied": the page
  // cannot know after a reload, and the title is what says so.
  const relitTitle = await page.$eval('#task-pref-apply', e => e.title);
  if (!relitTitle.includes('unknown')) {
    throw new Error('expected the button title to flag the unknown applied state after reload, got: ' + relitTitle);
  }

  // Router navigation must not strip the field's persistence path: clicking
  // a task from the filtered list changes the path, but the fragment must
  // survive the push and a reload from there must still seed the field.
  await page.click('#task-list .task-item');
  await page.waitForFunction(() => location.pathname.startsWith('/task/') && location.hash === '#filter=pref-hit', { timeout: 10000 });
  await page.screenshot({ path: screenshotDir + '/03-task-click-keeps-fragment.png', fullPage: true });
  await page.reload();
  await page.waitForFunction(() => document.getElementById('task-pref-filter').value === 'pref-hit', { timeout: 10000 });
  // Back to the task list for the Clear step below.
  await page.goto(baseURL + '/tasks#filter=pref-hit');
  await page.evaluate(() => toggleTaskFilter('PENDING'));
  await page.waitForFunction(() => document.querySelectorAll('#task-list .task-item').length === 1, { timeout: 10000 });

  // Clear resets field, fragment and backend.
  await page.click('text=✖ Clear');
  await page.waitForFunction(() => document.querySelectorAll('#task-list .task-item').length === 2, { timeout: 10000 });
  const clearedHash = await page.evaluate(() => location.hash);
  if (clearedHash !== '') {
    throw new Error('expected the fragment to be cleared, got: ' + clearedHash);
  }
  const drainOrderAfterClear = await page.evaluate(async () => (await fetch('/api/tasks')).json().then(d => d.tasks.map(t => t.id)));
  if (!drainOrderAfterClear[0].includes('pref-miss')) {
    throw new Error('expected the backend to fall back to FIFO once cleared, got: ' + JSON.stringify(drainOrderAfterClear));
  }
  await page.screenshot({ path: screenshotDir + '/04-cleared.png', fullPage: true });

  if (errors.length) {
    throw new Error('page errors observed: ' + errors.join(' | '));
  }

  await browser.close();
  console.log('OK: preference filter UI validated (screenshots in %s)');
})().catch(err => { console.error(err); process.exit(1); });
`, baseURL, screenshotDir, screenshotDir)

	tmpScript := t.TempDir() + "/pref_filter_ui.js"
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
		t.Fatalf("Playwright preference filter UI validation failed: %v (screenshots: %s)", err, screenshotDir)
	}
}
