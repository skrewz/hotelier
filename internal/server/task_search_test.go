package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"hotelier/pkg/config"
	"hotelier/pkg/logstore"
	"hotelier/pkg/queue"
)

// logEntryWithTime builds a logstore entry for appendTrace-style helpers
// that need an explicit timestamp rather than a calendar date.
func logEntryWithTime(taskID, line string, ts time.Time) logstore.Entry {
	return logstore.Entry{TaskID: taskID, Line: line, Level: "text", Timestamp: ts}
}

// searchMatch mirrors one entry of the /api/tasks/search response's
// "matches" array.
type searchMatch struct {
	TaskID        string    `json:"task_id"`
	Date          string    `json:"date"`
	Title         string    `json:"title"`
	Status        string    `json:"status"`
	Timestamp     time.Time `json:"timestamp"`
	Line          string    `json:"line"`
	ContextBefore []string  `json:"context_before"`
	ContextAfter  []string  `json:"context_after"`
}

type searchResponse struct {
	Query   string        `json:"query"`
	From    string        `json:"from"`
	To      string        `json:"to"`
	Count   int           `json:"count"`
	Matches []searchMatch `json:"matches"`
}

// newSearchServer builds a server backed by a fresh log store directory.
func newSearchServer(t *testing.T) *Server {
	t.Helper()
	dir, err := os.MkdirTemp("", "hotelier-tasksearch-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return New(config.ServerConfig{Host: "127.0.0.1", Port: 0, LogDir: dir})
}

func doSearch(t *testing.T, srv *Server, target string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, target, nil)
	w := httptest.NewRecorder()
	srv.HandleTaskSearch(w, req)
	return w
}

func decodeSearch(t *testing.T, w *httptest.ResponseRecorder) searchResponse {
	t.Helper()
	var resp searchResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode search response: %v (body %q)", err, w.Body.String())
	}
	return resp
}

// addQueueTask registers a task with the orchestrator so search results can
// report its title and status.
func addQueueTask(t *testing.T, srv *Server, id, title string, status queue.TaskStatus) {
	t.Helper()
	task := &queue.Task{ID: id, Prompt: "search fixture " + id, Title: title, Status: status, CreatedAt: time.Now()}
	if err := srv.orchestrator.AddTask(task); err != nil {
		t.Fatalf("add task %s: %v", id, err)
	}
	// AddTask forces PENDING; set the fixture status on the stored task so
	// search reports it.
	stored, ok := srv.orchestrator.GetTask(id)
	if !ok {
		t.Fatalf("task %s not in queue", id)
	}
	stored.Status = status
}

// TestParseSearchWindowAnchorsOnTheHostCalendar pins the window to the same
// calendar the store partitions by. The store names its date directories
// after the entry timestamp's own date (logstore.dirForDate), which is
// host-local, so anchoring the window at the UTC day puts today's partition
// outside "to" on any host east of UTC and a default search silently finds
// none of today's output — the same anchoring PruneOlderThan already uses.
func TestParseSearchWindowAnchorsOnTheHostCalendar(t *testing.T) {
	east := time.FixedZone("utc+11", 11*60*60)
	now := time.Date(2026, 11, 10, 9, 0, 0, 0, east) // 2026-11-09T22:00Z

	from, to, err := parseSearchWindow(url.Values{}, now)
	if err != nil {
		t.Fatalf("default window: %v", err)
	}
	if got := to.Format("2006-01-02"); got != "2026-11-10" {
		t.Errorf("expected the host-local day as the upper bound, got %s", got)
	}
	if got := from.Format("2006-01-02"); got != "2026-11-06" {
		t.Errorf("expected a 5-day default window, got %s..%s", got, to.Format("2006-01-02"))
	}
}

// TestSearchAPI_DefaultWindowIncludesTodaysPartition is the user-visible face
// of the anchoring above: output appended a moment ago must come back from a
// search with no window, whatever calendar the host runs on. The store names
// that partition after the host-local day, so the response's "to" has to be
// that same day.
func TestSearchAPI_DefaultWindowIncludesTodaysPartition(t *testing.T) {
	srv := newSearchServer(t)
	now := time.Now()
	if err := srv.diskLogStore.Append(logEntryWithTime("task-now", "needle today", now)); err != nil {
		t.Fatalf("append: %v", err)
	}

	w := doSearch(t, srv, "/api/tasks/search?q=needle")
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (body %q)", w.Code, w.Body.String())
	}
	resp := decodeSearch(t, w)
	if resp.Count != 1 {
		t.Fatalf("expected today's output within the default window, got %d matches (%q)", resp.Count, w.Body.String())
	}
	if want := now.Format("2006-01-02"); resp.To != want {
		t.Errorf("expected the window to reach the store's newest partition %s, got %s", want, resp.To)
	}
}

// TestSearchAPI_MatchesWithContext verifies a hit comes back with the task's
// title and status, the matched line and context lines from the same trace.
func TestSearchAPI_MatchesWithContext(t *testing.T) {
	srv := newSearchServer(t)
	appendTrace(t, srv, "task-a", 2026, 5, 10, []string{
		"Task started",
		"reading config",
		"panic: nil pointer dereference",
		"goroutine 1 exited",
		"done",
	})
	addQueueTask(t, srv, "task-a", "Fix the crash", queue.TaskStatusCompleted)

	w := doSearch(t, srv, "/api/tasks/search?q=nil+pointer&from=2026-05-10&to=2026-05-10")
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (body %q)", w.Code, w.Body.String())
	}
	resp := decodeSearch(t, w)
	if resp.Count != 1 || len(resp.Matches) != 1 {
		t.Fatalf("expected 1 match, got %d (%q)", resp.Count, w.Body.String())
	}
	m := resp.Matches[0]
	if m.TaskID != "task-a" || m.Date != "2026-05-10" {
		t.Errorf("unexpected match identity: %+v", m)
	}
	if m.Title != "Fix the crash" || m.Status != "COMPLETED" {
		t.Errorf("expected title/status from the queue, got %q/%q", m.Title, m.Status)
	}
	if !strings.Contains(m.Line, "panic: nil pointer dereference") {
		t.Errorf("unexpected matched line %q", m.Line)
	}
	if strings.Join(m.ContextBefore, "|") != "Task started|reading config" {
		t.Errorf("expected two context lines before, got %v", m.ContextBefore)
	}
	if strings.Join(m.ContextAfter, "|") != "goroutine 1 exited|done" {
		t.Errorf("expected two context lines after, got %v", m.ContextAfter)
	}
}

// TestSearchAPI_DefaultWindowIsFiveDays verifies that with no window supplied
// only the last five days are searched (issue #269): a hit from today is
// found, a ten-day-old hit is not — but days=14 reaches it.
func TestSearchAPI_DefaultWindowIsFiveDays(t *testing.T) {
	srv := newSearchServer(t)
	now := time.Now()
	recent := now
	older := now.AddDate(0, 0, -10)
	for _, ts := range []time.Time{recent, older} {
		if err := srv.diskLogStore.Append(logEntryWithTime("task-x", "needle here", ts)); err != nil {
			t.Fatalf("append: %v", err)
		}
	}

	w := doSearch(t, srv, "/api/tasks/search?q=needle")
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (body %q)", w.Code, w.Body.String())
	}
	resp := decodeSearch(t, w)
	if resp.Count != 1 {
		t.Fatalf("expected only the recent match within the default 5-day window, got %d", resp.Count)
	}
	wantFrom := now.AddDate(0, 0, -4).Format("2006-01-02")
	if resp.From != wantFrom || resp.To != now.Format("2006-01-02") {
		t.Errorf("expected window %s..%s, got %s..%s", wantFrom, now.Format("2006-01-02"), resp.From, resp.To)
	}

	w = doSearch(t, srv, "/api/tasks/search?q=needle&days=14")
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 for days=14, got %d", w.Code)
	}
	if resp := decodeSearch(t, w); resp.Count != 2 {
		t.Errorf("expected 2 matches within 14 days, got %d", resp.Count)
	}
}

// TestSearchAPI_WiderThan14DaysRejected verifies the maximum lookback is
// enforced server-side with an explanatory 400, not silently clamped:
// the 14-day cap stands independently of retention pruning.
func TestSearchAPI_WiderThan14DaysRejected(t *testing.T) {
	srv := newSearchServer(t)

	w := doSearch(t, srv, "/api/tasks/search?q=x&days=15")
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for days=15, got %d", w.Code)
	}
	if body := w.Body.String(); !strings.Contains(body, "14") {
		t.Errorf("error body should explain the 14-day cap, got %q", body)
	}

	from := time.Now().AddDate(0, 0, -20).Format("2006-01-02")
	to := time.Now().Format("2006-01-02")
	w = doSearch(t, srv, "/api/tasks/search?q=x&from="+from+"&to="+to)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for a 21-day from/to window, got %d", w.Code)
	}
	if body := w.Body.String(); !strings.Contains(body, "14") {
		t.Errorf("error body should explain the 14-day cap, got %q", body)
	}
}

// TestSearchAPI_StatusFilter verifies an optional status parameter keeps only
// matches whose task currently holds that status (case-insensitive), and that
// matches for tasks the queue no longer knows about are excluded when a
// status is requested.
func TestSearchAPI_StatusFilter(t *testing.T) {
	srv := newSearchServer(t)
	appendTrace(t, srv, "task-done", 2026, 5, 10, []string{"matched output"})
	appendTrace(t, srv, "task-wait", 2026, 5, 10, []string{"matched output"})
	addQueueTask(t, srv, "task-done", "Done one", queue.TaskStatusCompleted)
	addQueueTask(t, srv, "task-wait", "Waiting one", queue.TaskStatusPending)

	w := doSearch(t, srv, "/api/tasks/search?q=matched&status=completed&from=2026-05-10&to=2026-05-10")
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (body %q)", w.Code, w.Body.String())
	}
	resp := decodeSearch(t, w)
	if resp.Count != 1 || resp.Matches[0].TaskID != "task-done" {
		t.Fatalf("expected only the COMPLETED task, got %d matches (%+v)", resp.Count, resp.Matches)
	}
}

// TestSearchAPI_UnknownTaskStillReturnedWithoutStatusFilter verifies log
// matches survive the queue forgetting their task: the entry is returned with
// an empty status rather than dropped.
func TestSearchAPI_UnknownTaskStillReturnedWithoutStatusFilter(t *testing.T) {
	srv := newSearchServer(t)
	appendTrace(t, srv, "task-gone", 2026, 5, 10, []string{"orphan output"})

	w := doSearch(t, srv, "/api/tasks/search?q=orphan&from=2026-05-10&to=2026-05-10")
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	resp := decodeSearch(t, w)
	if resp.Count != 1 || resp.Matches[0].Status != "" {
		t.Fatalf("expected one match with empty status, got %+v", resp.Matches)
	}
}

// TestSearchAPI_MissingQuery verifies 400 when q is absent or empty — an
// unbounded scan of the store is not a search.
func TestSearchAPI_MissingQuery(t *testing.T) {
	srv := newSearchServer(t)

	for _, target := range []string{"/api/tasks/search", "/api/tasks/search?q=", "/api/tasks/search?q=%20"} {
		w := doSearch(t, srv, target)
		if w.Code != http.StatusBadRequest {
			t.Errorf("%s: expected 400, got %d", target, w.Code)
		}
	}
}

// TestSearchAPI_InvalidWindowParams verifies 400 for malformed or ambiguous
// window parameters.
func TestSearchAPI_InvalidWindowParams(t *testing.T) {
	srv := newSearchServer(t)

	cases := []string{
		"/api/tasks/search?q=x&days=0",
		"/api/tasks/search?q=x&days=-3",
		"/api/tasks/search?q=x&days=many",
		"/api/tasks/search?q=x&days=7&from=2026-05-01&to=2026-05-05",
		"/api/tasks/search?q=x&from=2026-05-01",
		"/api/tasks/search?q=x&from=not-a-date&to=2026-05-01",
		"/api/tasks/search?q=x&from=2026-05-10&to=2026-05-01",
	}
	for _, target := range cases {
		w := doSearch(t, srv, target)
		if w.Code != http.StatusBadRequest {
			t.Errorf("%s: expected 400, got %d", target, w.Code)
		}
	}
}

// TestSearchAPI_Disabled verifies 503 when log_dir is not configured,
// matching the other log-backed endpoints.
func TestSearchAPI_Disabled(t *testing.T) {
	srv := New(config.ServerConfig{Host: "127.0.0.1", Port: 0})

	req := httptest.NewRequest(http.MethodGet, "/api/tasks/search?q=x", nil)
	w := httptest.NewRecorder()
	srv.HandleTaskSearch(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503 when log store disabled, got %d", w.Code)
	}
}

// TestSearchAPI_MethodNotAllowed verifies 405 for non-GET methods.
func TestSearchAPI_MethodNotAllowed(t *testing.T) {
	srv := newSearchServer(t)

	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete} {
		req := httptest.NewRequest(method, "/api/tasks/search?q=x", nil)
		w := httptest.NewRecorder()
		srv.HandleTaskSearch(w, req)
		if w.Code != http.StatusMethodNotAllowed {
			t.Errorf("%s: expected 405, got %d", method, w.Code)
		}
	}
}

// TestSearchAPI_ExcludesDatesOutsideWindow verifies date directories outside
// the requested window are never scanned — the lookback enforcement must be
// real, not just a filter on results.
func TestSearchAPI_ExcludesDatesOutsideWindow(t *testing.T) {
	srv := newSearchServer(t)
	appendTrace(t, srv, "task-in", 2026, 5, 10, []string{"needle in window"})
	appendTrace(t, srv, "task-out", 2026, 4, 10, []string{"needle out of window"})

	w := doSearch(t, srv, "/api/tasks/search?q=needle&from=2026-05-08&to=2026-05-11")
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	resp := decodeSearch(t, w)
	if resp.Count != 1 || resp.Matches[0].TaskID != "task-in" {
		t.Fatalf("expected only the in-window match, got %+v", resp.Matches)
	}
}
