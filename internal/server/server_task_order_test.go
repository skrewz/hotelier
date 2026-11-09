package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"hotelier/pkg/queue"
)

// TestHandleGetTasks_OrderedByProcessing verifies that GET /api/tasks
// returns tasks in the order the queue will get processed: RUNNING first,
// then ASSIGNED, then PENDING by priority (FIFO within a priority), then
// settled tasks (issue #268).
func TestHandleGetTasks_OrderedByProcessing(t *testing.T) {
	srv := newTestServer(t)
	q := srv.TaskQueue()

	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("queue operation failed: %v", err)
		}
	}

	// Insertion order deliberately differs from processing order.
	createPendingTask(t, srv, "pending-orange-old", queue.PriorityOrangutan)
	createPendingTask(t, srv, "pending-firefighter", queue.PriorityFirefighter)
	createPendingTask(t, srv, "settle-running", queue.PriorityOrangutan)
	createPendingTask(t, srv, "pending-orange-new", queue.PriorityOrangutan)
	createPendingTask(t, srv, "settle-assigned", queue.PriorityOrangutan)
	createPendingTask(t, srv, "settle-completed", queue.PriorityOrangutan)

	must(q.Assign("settle-running", "guest-x"))
	must(q.Start("settle-running"))
	must(q.Assign("settle-assigned", "guest-y"))
	must(q.Assign("settle-completed", "guest-z"))
	must(q.Start("settle-completed"))
	must(q.Complete("settle-completed", "done"))

	w := httptest.NewRecorder()
	srv.HandleTasks(w, httptest.NewRequest(http.MethodGet, "/api/tasks", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var payload struct {
		Tasks []struct {
			ID string `json:"id"`
		} `json:"tasks"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &payload); err != nil {
		t.Fatalf("unmarshal tasks payload: %v", err)
	}

	got := make([]string, len(payload.Tasks))
	for i, task := range payload.Tasks {
		got[i] = task.ID
	}
	want := []string{
		"settle-running",
		"settle-assigned",
		"pending-firefighter",
		"pending-orange-old",
		"pending-orange-new",
		"settle-completed",
	}
	if len(got) != len(want) {
		t.Fatalf("expected %d tasks, got %d: %v", len(want), len(got), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("position %d: expected %s, got %s (full order: %v)", i, want[i], got[i], got)
		}
	}
}
