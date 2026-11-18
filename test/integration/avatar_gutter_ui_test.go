//go:build integration

// Face-as-row-gutter UI test (issue #277). Exercises the dashboard's task
// rows end-to-end: the identity's face renders as a fixed-size gutter beside
// everything below the headline, the persona pill is gone from the rows, the
// persona name remains in the detail pane, and the gutter box occupies the
// identical dimensions whether the tile loads or the proxy answers 502 — so
// a forge outage never reflows a row.
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
	"hotelier/pkg/persona"
)

func TestAvatarGutterUI(t *testing.T) {
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
		Personas: []persona.Persona{
			{Name: "cast-one", Env: map[string]string{"ROLE": "reviewer"}},
		},
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

	// One task carries the identity, one carries none, so the gutter's
	// presence and absence are both observable side by side.
	type taskSpec struct {
		id      string
		persona string
	}
	specs := []taskSpec{
		{"casted-task", "cast-one"},
		{"bare-task", ""},
	}
	for i, s := range specs {
		body, _ := json.Marshal(map[string]interface{}{
			"id":         s.id,
			"prompt":     "avatar gutter fixture",
			"repo_ref":   "https://example.com/org/example-repo.git",
			"persona":    s.persona,
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

	// Server-side check independent of the browser: the identity really is
	// on the task, so a green UI run cannot be resting on an empty field.
	res, err := http.Get(baseURL + "/api/tasks/casted-task")
	if err != nil {
		t.Fatalf("get casted-task: %v", err)
	}
	var detail struct {
		Task struct {
			Persona string `json:"persona"`
		} `json:"task"`
	}
	if err := json.NewDecoder(res.Body).Decode(&detail); err != nil {
		t.Fatalf("decode casted-task: %v", err)
	}
	res.Body.Close()
	if detail.Task.Persona != "cast-one" {
		t.Fatalf("expected casted-task persona cast-one, got %q", detail.Task.Persona)
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
  const baseURL = '%s';
  const screenshotDir = '%s';
  const errors = [];
  // A 1x1 opaque PNG; CSS scales it into the gutter box.
  const png = Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg==', 'base64');

  const pageErrors = page => {
    page.on('pageerror', err => errors.push(String(err)));
    page.on('console', msg => {
      if (msg.type() !== 'error') return;
      // A failed tile load is the outage path under test; browsers log it
      // as a bare 'Failed to load resource' with the URL in location().
      const loc = msg.location() || {};
      if (msg.text().includes('/avatar/') || (loc.url || '').includes('/avatar/')) return;
      errors.push('console: ' + msg.text());
    });
  };

  const openList = async page => {
    await page.goto(baseURL);
    await page.waitForFunction(() => document.querySelectorAll('#task-filter-row .task-filter-btn').length === 6, { timeout: 10000 });
    // PENDING rows are hidden by default (issue #289); reveal them.
    await page.click('#task-filter-row .task-filter-btn[data-status="PENDING"]');
    await page.waitForFunction(() => document.querySelectorAll('#task-list .task-item').length === 2, { timeout: 10000 });
    // Every gutter image has settled (loaded or failed) before measuring.
    await page.waitForFunction(() => [...document.querySelectorAll('.task-avatar img')].every(i => i.complete), { timeout: 10000 });
  };

  const rowFor = async (page, id) => page.evaluateHandle((tid) => {
    return [...document.querySelectorAll('#task-list .task-item')]
      .find(r => r.querySelector('.task-id').textContent.includes(tid));
  }, id);

  const metricsFor = async (page, id) => {
    const row = await rowFor(page, id);
    return row.evaluate(r => {
      const box = el => { const b = el.getBoundingClientRect(); return { x: b.x, y: b.y, w: b.width, h: b.height }; };
      // A block box spans the full width even beside a float; to see the
      // indent, measure where the first text actually starts.
      const textX = el => {
        const node = [...el.childNodes].find(n => n.nodeType === 3 && n.textContent.trim());
        if (!node) return el.getBoundingClientRect().x;
        const range = document.createRange();
        range.setStart(node, 0); range.setEnd(node, 1);
        return range.getBoundingClientRect().x;
      };
      const out = {
        row: box(r),
        headline: box(r.querySelector('.task-id')),
        headlineTextX: textX(r.querySelector('.task-id')),
        metaTextX: textX(r.querySelector('.task-meta')),
        meta: box(r.querySelector('.task-meta')),
        avatar: r.querySelector('.task-avatar') ? box(r.querySelector('.task-avatar')) : null,
      };
      const img = r.querySelector('.task-avatar img');
      if (img) {
        out.imgSrc = img.getAttribute('src');
        out.imgErrorClass = img.classList.contains('avatar-error');
        out.imgVisibility = getComputedStyle(img).visibility;
        out.glyph = getComputedStyle(r.querySelector('.task-avatar'), '::before').content;
      }
      return out;
    });
  };

  // --- Pass one: the forge answers; tiles load. ---
  const page = await browser.newPage({ viewport: { width: 1280, height: 900 } });
  pageErrors(page);
  await page.route('**/avatar/**', route => route.fulfill({ status: 200, contentType: 'image/png', body: png }));
  await openList(page);

  // The pill is gone from the rows: no persona badge markup anywhere in the
  // list, and the mod-8 palette with its hash function left with it.
  const pillCount = await page.evaluate(() => document.querySelectorAll('#task-list .persona-badge').length);
  if (pillCount !== 0) throw new Error('expected no persona pills in the rows, found ' + pillCount);
  const html = await page.content();
  if (html.includes('personaColourClass')) throw new Error('the mod-8 hash function is still on the page');
  if (/persona-badge-[0-7]/.test(html)) throw new Error('a palette class is still on the page');

  const casted = await metricsFor(page, 'casted-task');
  const bare = await metricsFor(page, 'bare-task');

  // The face renders from the proxy, keyed by the identity.
  if (casted.imgSrc !== '/avatar/cast-one') throw new Error('expected src /avatar/cast-one, got ' + casted.imgSrc);

  // The gutter is the token's square: 40 CSS px on the loaded edge.
  const token = await page.evaluate(() => getComputedStyle(document.documentElement).getPropertyValue('--task-avatar-gutter').trim());
  if (token !== '40px') throw new Error('expected --task-avatar-gutter to be 40px, got ' + token);
  if (!casted.avatar || Math.abs(casted.avatar.w - 40) > 1 || Math.abs(casted.avatar.h - 40) > 1) {
    throw new Error('expected a 40x40 gutter box, got ' + JSON.stringify(casted.avatar));
  }

  // The headline has not moved: its left edge matches the row that has no
  // gutter at all. Everything below it is indented by exactly the gutter.
  if (Math.abs(casted.headline.x - bare.headline.x) > 0.5) {
    throw new Error('headline moved: ' + casted.headline.x + ' vs ' + bare.headline.x);
  }
  if (Math.abs((casted.headlineTextX + 40) - casted.metaTextX) > 1) {
    throw new Error('expected the meta line indented by the gutter: ' + casted.metaTextX + ' vs headline ' + casted.headlineTextX);
  }
  if (Math.abs(bare.headlineTextX - bare.metaTextX) > 0.5) throw new Error('the gutterless row shifted');
  if (bare.avatar !== null) throw new Error('a task with no identity grew a gutter');
  await page.screenshot({ path: screenshotDir + '/01-task-list-gutters.png', fullPage: true });

  // The row is recognition; the selection is reading: the detail pane still
  // carries the persona name, without the glyph that stood in for a face.
  const castedRow = await rowFor(page, 'casted-task');
  await castedRow.click();
  await page.waitForSelector('.task-detail-id .persona-badge', { timeout: 10000 });
  const detailPersona = await page.$eval('.task-detail-id .persona-badge', e => e.textContent.trim());
  if (detailPersona !== 'cast-one') throw new Error('expected the detail pane to show the name cast-one, got ' + detailPersona);
  await page.screenshot({ path: screenshotDir + '/02-detail-persona-name.png', fullPage: true });
  const rowHeightWithTiles = casted.row.h;
  await page.close();

  // --- Pass two: the forge is unreachable; 502 and no cached tile. ---
  const page2 = await browser.newPage({ viewport: { width: 1280, height: 900 } });
  pageErrors(page2);
  await page2.route('**/avatar/**', route => route.fulfill({ status: 502, body: '' }));
  await openList(page2);
  const failed = await metricsFor(page2, 'casted-task');

  // Identical dimensions with a failed load: the fallback glyph shows, the
  // row geometry is unchanged, and the headline has not moved.
  if (!failed.imgErrorClass) throw new Error('a failed tile did not mark the image for fallback');
  if (failed.imgVisibility !== 'hidden') throw new Error('the broken image is still visible');
  if (!failed.glyph.includes('"?"')) throw new Error('expected the fallback glyph, got ' + failed.glyph);
  if (Math.abs(failed.avatar.w - 40) > 1 || Math.abs(failed.avatar.h - 40) > 1) {
    throw new Error('the fallback box is not 40x40: ' + JSON.stringify(failed.avatar));
  }
  if (Math.abs(failed.row.h - rowHeightWithTiles) > 1) {
    throw new Error('the row reflowed on an outage: ' + failed.row.h + ' vs ' + rowHeightWithTiles);
  }
  if (Math.abs(failed.headline.x - casted.headline.x) > 0.5) throw new Error('the headline moved during an outage');
  await page2.screenshot({ path: screenshotDir + '/03-avatar-gutter-forge-outage.png', fullPage: true });

  if (errors.length) throw new Error('page errors observed: ' + errors.join(' | '));

  await browser.close();
  console.log('OK: face-as-gutter validated (screenshots in %s)');
})().catch(err => { console.error(err); process.exit(1); });
`, baseURL, screenshotDir, screenshotDir)

	tmpScript := t.TempDir() + "/avatar_gutter_ui.js"
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
		t.Fatalf("Playwright avatar gutter validation failed: %v (screenshots: %s)", err, screenshotDir)
	}
}
