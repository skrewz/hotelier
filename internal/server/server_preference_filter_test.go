package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"hotelier/pkg/queue"
	"hotelier/pkg/rpc"
)

// Task preference filter endpoint tests (issue #281).

func getFilter(srv *Server) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, "/api/tasks/filter", nil)
	w := httptest.NewRecorder()
	srv.HandleTaskFilter(w, req)
	return w
}

func postFilter(srv *Server, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/api/tasks/filter", bytes.NewReader([]byte(body)))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.HandleTaskFilter(w, req)
	return w
}

func TestHandleTaskFilter_SetReadClear(t *testing.T) {
	srv := newTestServer(t)

	w := getFilter(srv)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var empty map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &empty); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if empty["filter"] != "" {
		t.Fatalf("expected empty filter by default, got %v", empty["filter"])
	}

	w = postFilter(srv, `{"filter":"s-habit-tracker"}`)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if got := srv.Orchestrator().PreferenceFilter(); got != "s-habit-tracker" {
		t.Fatalf("expected filter on the orchestrator, got %q", got)
	}

	w = getFilter(srv)
	var read map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &read); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if read["filter"] != "s-habit-tracker" {
		t.Fatalf("expected filter to read back, got %v", read["filter"])
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
	req := httptest.NewRequest(http.MethodDelete, "/api/tasks/filter", nil)
	w = httptest.NewRecorder()
	srv.HandleTaskFilter(w, req)
	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", w.Code)
	}
	if got := srv.Orchestrator().PreferenceFilter(); got != "keep" {
		t.Errorf("unsupported method changed the filter: %q", got)
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
	mk("hit", "Widget polish", "https://example.com/org/s-habit-tracker.git", base.Add(time.Minute))

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
