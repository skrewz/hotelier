package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"hotelier/pkg/queue"
)

// createPendingTask is a helper that creates a task through the HTTP
// endpoint and returns its id.
func createPendingTask(t *testing.T, srv *Server, id, priority string) string {
	t.Helper()
	body, _ := json.Marshal(map[string]interface{}{
		"id":       id,
		"prompt":   "priority test task",
		"priority": priority,
	})
	req := httptest.NewRequest(http.MethodPost, "/api/tasks", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.HandleTasks(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("task creation failed: %d %s", w.Code, w.Body.String())
	}
	var created queue.Task
	if err := json.Unmarshal(w.Body.Bytes(), &created); err != nil {
		t.Fatalf("unmarshal created task: %v", err)
	}
	return created.ID
}

func postPriority(srv *Server, taskID, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/api/tasks/"+taskID+"/priority", bytes.NewReader([]byte(body)))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.HandleTaskDetail(w, req)
	return w
}

// TestHandleTaskPriority_BumpUp verifies the happy path: a pending
// orangutan task is bumped to teacher (issue #266).
func TestHandleTaskPriority_BumpUp(t *testing.T) {
	srv := newTestServer(t)
	taskID := createPendingTask(t, srv, "prio-up", queue.PriorityOrangutan)

	w := postPriority(srv, taskID, `{"priority":"teacher"}`)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	task, ok := srv.TaskQueue().Get(taskID)
	if !ok {
		t.Fatal("task not found")
	}
	if task.Priority != queue.PriorityTeacher {
		t.Errorf("expected priority %q, got %q", queue.PriorityTeacher, task.Priority)
	}

	var resp map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if resp["priority"] != queue.PriorityTeacher || resp["status"] != "PENDING" {
		t.Errorf("unexpected response body: %v", resp)
	}
}

// TestHandleTaskPriority_BumpDown verifies the downward direction
// (issue #266).
func TestHandleTaskPriority_BumpDown(t *testing.T) {
	srv := newTestServer(t)
	taskID := createPendingTask(t, srv, "prio-down", queue.PriorityFirefighter)

	w := postPriority(srv, taskID, `{"priority":"teacher"}`)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	task, _ := srv.TaskQueue().Get(taskID)
	if task.Priority != queue.PriorityTeacher {
		t.Errorf("expected priority %q, got %q", queue.PriorityTeacher, task.Priority)
	}
}

// TestHandleTaskPriority_InvalidPriority verifies that an unknown priority
// is rejected with 400 and the task keeps its old value (issue #266).
func TestHandleTaskPriority_InvalidPriority(t *testing.T) {
	srv := newTestServer(t)
	taskID := createPendingTask(t, srv, "prio-invalid", queue.PriorityOrangutan)

	w := postPriority(srv, taskID, `{"priority":"captain"}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}

	task, _ := srv.TaskQueue().Get(taskID)
	if task.Priority != queue.PriorityOrangutan {
		t.Errorf("priority changed on rejected input: got %q", task.Priority)
	}
}

// TestHandleTaskPriority_BadBody verifies malformed bodies are rejected.
func TestHandleTaskPriority_BadBody(t *testing.T) {
	srv := newTestServer(t)
	taskID := createPendingTask(t, srv, "prio-badbody", queue.PriorityOrangutan)

	for _, body := range []string{"", "not json", `{}`, `{"priority":42}`} {
		w := postPriority(srv, taskID, body)
		if w.Code != http.StatusBadRequest {
			t.Errorf("body %q: expected 400, got %d", body, w.Code)
		}
	}
}

// TestHandleTaskPriority_NotFound verifies an unknown task id yields 404.
func TestHandleTaskPriority_NotFound(t *testing.T) {
	srv := newTestServer(t)

	w := postPriority(srv, "nonexistent", `{"priority":"teacher"}`)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", w.Code, w.Body.String())
	}
}

// TestHandleTaskPriority_NonPending verifies that a task which is no longer
// pending (here: cancelled) cannot be re-prioritised (issue #266).
func TestHandleTaskPriority_NonPending(t *testing.T) {
	srv := newTestServer(t)
	taskID := createPendingTask(t, srv, "prio-cancelled", queue.PriorityOrangutan)

	cancelReq := httptest.NewRequest(http.MethodPost, "/api/tasks/"+taskID+"/cancel", nil)
	cancelW := httptest.NewRecorder()
	srv.HandleTaskDetail(cancelW, cancelReq)
	if cancelW.Code != http.StatusOK {
		t.Fatalf("cancel failed: %d %s", cancelW.Code, cancelW.Body.String())
	}

	w := postPriority(srv, taskID, `{"priority":"firefighter"}`)
	if w.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d: %s", w.Code, w.Body.String())
	}
}

// TestHandleTaskPriority_MethodNotAllowed verifies only POST is accepted.
func TestHandleTaskPriority_MethodNotAllowed(t *testing.T) {
	srv := newTestServer(t)
	taskID := createPendingTask(t, srv, "prio-method", queue.PriorityOrangutan)

	req := httptest.NewRequest(http.MethodGet, "/api/tasks/"+taskID+"/priority", nil)
	w := httptest.NewRecorder()
	srv.HandleTaskDetail(w, req)
	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", w.Code)
	}
}
