package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"hotelier/pkg/queue"
	"hotelier/pkg/rpc"
)

// Task preference filter endpoint tests (issue #281). The endpoint is a
// write: POST sets or clears the live filter and echoes what is now in
// force. There is no read half — the dashboard's own persistence path for
// the field is the #filter= fragment in the address bar.

func postFilter(srv *Server, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/api/tasks/filter", bytes.NewReader([]byte(body)))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.HandleTaskFilter(w, req)
	return w
}

func TestHandleTaskFilter_SetAndClear(t *testing.T) {
	srv := newTestServer(t)

	w := postFilter(srv, `{"filter":"example-repo"}`)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if got := srv.Orchestrator().PreferenceFilter(); got != "example-repo" {
		t.Fatalf("expected filter on the orchestrator, got %q", got)
	}
	var echoed map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &echoed); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if echoed["filter"] != "example-repo" {
		t.Fatalf("expected the response to echo what is in force, got %v", echoed["filter"])
	}

	// An empty filter clears it (the UI's Clear button path).
	w = postFilter(srv, `{"filter":""}`)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if got := srv.Orchestrator().PreferenceFilter(); got != "" {
		t.Fatalf("expected cleared filter, got %q", got)
	}
}

// One operator action is one log line: the handler must not log alongside
// the queue.
func TestHandleTaskFilter_LogsOnce(t *testing.T) {
	srv := newTestServer(t)

	var logBuf bytes.Buffer
	out := srv.log.Writer()
	srv.log.SetOutput(&logBuf)
	w := postFilter(srv, `{"filter":"widget"}`)
	srv.log.SetOutput(out)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if n := strings.Count(logBuf.String(), "preference filter"); n != 1 {
		t.Fatalf("expected one log line for one set, got %d:\n%s", n, logBuf.String())
	}
}

func TestHandleTaskFilter_RejectsBadInput(t *testing.T) {
	srv := newTestServer(t)

	for _, body := range []string{"", "not json", `{}`, `{"filter":42}`} {
		w := postFilter(srv, body)
		if w.Code != http.StatusBadRequest {
			t.Errorf("body %q: expected 400, got %d", body, w.Code)
		}
	}
	if got := srv.Orchestrator().PreferenceFilter(); got != "" {
		t.Errorf("rejected input changed the filter: %q", got)
	}

	w := postFilter(srv, `{"filter":"keep"}`)
	if w.Code != http.StatusOK {
		t.Fatalf("setup failed: %d", w.Code)
	}
	for _, method := range []string{http.MethodGet, http.MethodDelete} {
		req := httptest.NewRequest(method, "/api/tasks/filter", nil)
		w = httptest.NewRecorder()
		srv.HandleTaskFilter(w, req)
		if w.Code != http.StatusMethodNotAllowed {
			t.Fatalf("%s: expected 405, got %d", method, w.Code)
		}
	}
	if got := srv.Orchestrator().PreferenceFilter(); got != "keep" {
		t.Errorf("unsupported method changed the filter: %q", got)
	}
}

// The filter is a fragment of a repository name or title, so a value past
// the bound is not a mistake but an attack: it must be refused, not stored
// in the queue and repeated into the log.
func TestHandleTaskFilter_RejectsOverlongFilter(t *testing.T) {
	srv := newTestServer(t)

	if w := postFilter(srv, `{"filter":"keep"}`); w.Code != http.StatusOK {
		t.Fatalf("setup failed: %d", w.Code)
	}

	body, err := json.Marshal(map[string]string{"filter": strings.Repeat("x", maxPreferenceFilterLen+1)})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if w := postFilter(srv, string(body)); w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for an over-long filter, got %d", w.Code)
	}
	if got := srv.Orchestrator().PreferenceFilter(); got != "keep" {
		t.Fatalf("rejected filter changed the queue: %q", got)
	}

	// The bound counts the filter text, not the JSON envelope around it.
	body, err = json.Marshal(map[string]string{"filter": strings.Repeat("y", maxPreferenceFilterLen)})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if w := postFilter(srv, string(body)); w.Code != http.StatusOK {
		t.Fatalf("expected a filter at the bound to be accepted, got %d", w.Code)
	}
	if got := srv.Orchestrator().PreferenceFilter(); got != strings.Repeat("y", maxPreferenceFilterLen) {
		t.Fatalf("expected the filter at the bound to stand, got %q", got)
	}
}

func TestHandleGetTasks_PendingOrderFollowsFilter(t *testing.T) {
	srv := newTestServer(t)

	mk := func(id, title, repo string, when time.Time) {
		task := &queue.Task{ID: id, Prompt: "x", Title: title, RepoRef: repo, CreatedAt: when}
		if err := srv.TaskQueue().Restore(task); err != nil {
			t.Fatalf("Restore %s: %v", id, err)
		}
	}
	base := time.Now().Add(-time.Hour)
	mk("plain", "Unrelated", "https://example.com/other.git", base)
	mk("hit", "Widget polish", "https://example.com/org/example-repo.git", base.Add(time.Minute))

	if w := postFilter(srv, `{"filter":"widget"}`); w.Code != http.StatusOK {
		t.Fatalf("set filter: %d %s", w.Code, w.Body.String())
	}

	req := httptest.NewRequest(http.MethodGet, "/api/tasks", nil)
	w := httptest.NewRecorder()
	srv.HandleTasks(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("list tasks: %d %s", w.Code, w.Body.String())
	}
	var list struct {
		Tasks []struct {
			ID string `json:"id"`
		} `json:"tasks"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &list); err != nil {
		t.Fatalf("unmarshal list: %v", err)
	}
	if len(list.Tasks) != 2 || list.Tasks[0].ID != "hit" {
		t.Fatalf("expected the preference-matched task first, got %v", list.Tasks)
	}
}

func TestTryAssignTask_PrefersPreferenceMatch(t *testing.T) {
	srv := newTestServer(t)
	hub := srv.Hub()
	go hub.Run()

	srv.Registry().Register("g1", "G1", nil)
	conn := rpc.NewTestConnection("conn-g1", hub)
	hub.Register(conn)
	hub.RegisterGuestConnection("g1", "conn-g1")
	time.Sleep(10 * time.Millisecond)

	base := time.Now().Add(-time.Hour)
	older := &queue.Task{ID: "older", Prompt: "unrelated", CreatedAt: base}
	matcher := &queue.Task{ID: "matcher", Title: "Widget polish", CreatedAt: base.Add(time.Minute)}
	if err := srv.TaskQueue().Restore(older); err != nil {
		t.Fatalf("Restore older: %v", err)
	}
	if err := srv.TaskQueue().Restore(matcher); err != nil {
		t.Fatalf("Restore matcher: %v", err)
	}

	if w := postFilter(srv, `{"filter":"widget"}`); w.Code != http.StatusOK {
		t.Fatalf("set filter: %d", w.Code)
	}

	// There is no WebSocket connection behind this test guest, so the
	// assignment is recorded and then pushed back to the queue when the
	// push fails. What is under test is the *pick*: the assignment log
	// line names the task tryAssignTask chose. The orchestrator logs
	// through the same logger as the server, so redirecting its output
	// captures the pick.
	var logBuf bytes.Buffer
	out := srv.log.Writer()
	srv.log.SetOutput(&logBuf)

	srv.tryAssignTask("g1")

	srv.log.SetOutput(out)

	matched, ok := srv.TaskQueue().Get("matcher")
	if !ok || matched.Status != queue.TaskStatusAssigned {
		t.Fatalf("expected the matching task to be assigned, got %v (found: %v) log:\n%s", matched, ok, logBuf.String())
	}
	if task, _ := srv.TaskQueue().Get("older"); task.Status != queue.TaskStatusPending {
		t.Fatalf("expected the non-matching task to stay pending, got %s", task.Status)
	}
}
