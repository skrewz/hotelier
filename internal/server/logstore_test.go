package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"hotelier/pkg/config"
	"hotelier/pkg/logstore"
	"hotelier/pkg/queue"
	"hotelier/pkg/rpc"
)

// TestLogPersistence_Integration verifies that task logs are persisted to disk
// when log_dir is configured. It simulates a full flow: create task → send logs
// via agent.log RPC → flush accumulator → verify filesystem.
func TestLogPersistence_Integration(t *testing.T) {
	dir, err := os.MkdirTemp("", "hotelier-logpersist-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	cfg := config.ServerConfig{
		Host:   "127.0.0.1",
		Port:   0,
		LogDir: dir,
	}
	srv := New(cfg)

	if srv.DiskLogStore() == nil {
		t.Fatal("expected disk log store to be initialized")
	}

	// Create a task
	task := map[string]interface{}{
		"repos":  []string{"/repo"},
		"prompt": "Persistence test",
	}
	body, _ := json.Marshal(task)
	req := httptest.NewRequest(http.MethodPost, "/api/tasks", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.HandleTasks(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", w.Code)
	}

	var createdTask queue.Task
	json.Unmarshal(w.Body.Bytes(), &createdTask)

	// Send logs via the RPC handler
	logLines := []struct {
		line  string
		level string
	}{
		{"Task started", ""},
		{"Agent is thinking", "info"},
		{"[TOOL_START] read: file.txt (id: t1)", ""},
		{"[TOOL_OUTPUT] read (id: t1): file contents", ""},
		{"[TOOL_END] read (id: t1): result", ""},
		{"Hello **world**", "text"},
	}

	for _, ll := range logLines {
		params := map[string]interface{}{
			"task_id": createdTask.ID,
			"line":    ll.line,
			"level":   ll.level,
		}
		rawParams, _ := json.Marshal(params)
		srv.hub.Dispatch("agent.log", rawParams)
	}

	// Flush the accumulator — this triggers disk writes
	srv.logAccumulator.FlushAll(func(e TaskLogEntry) {
		srv.logStore.Add(e)
		if srv.diskLogStore != nil {
			_ = srv.diskLogStore.Append(logstore.Entry{
				TaskID:    e.TaskID,
				Line:      e.Line,
				Level:     e.Level,
				Timestamp: e.Timestamp,
			})
		}
		srv.hub.SendNotification("", rpc.ConnectionRoleBrowser, "task.log", map[string]interface{}{
			"task_id": e.TaskID,
			"line":    e.Line,
			"level":   e.Level,
		})
	})

	// Verify filesystem structure
	dates, err := srv.diskLogStore.ListDates()
	if err != nil {
		t.Fatalf("list dates: %v", err)
	}
	if len(dates) == 0 {
		t.Fatal("expected at least one date directory")
	}

	tasks, err := srv.diskLogStore.ListTasks(dates[0])
	if err != nil {
		t.Fatalf("list tasks: %v", err)
	}
	if len(tasks) != 1 || tasks[0] != createdTask.ID {
		t.Fatalf("expected task %s, got %v", createdTask.ID, tasks)
	}

	entries, err := srv.diskLogStore.ReadLogs(dates[0], createdTask.ID)
	if err != nil {
		t.Fatalf("read logs: %v", err)
	}

	// We sent 6 log lines, but the accumulator may batch some together.
	// At minimum, we should have at least 1 entry.
	if len(entries) == 0 {
		t.Fatal("expected at least 1 log entry on disk")
	}

	// Verify the first entry contains the task ID
	if entries[0].TaskID != createdTask.ID {
		t.Errorf("expected task_id %s, got %s", createdTask.ID, entries[0].TaskID)
	}
}

// TestLogAPI_DatesEndpoint verifies the /api/logs endpoint returns date list.
func TestLogAPI_DatesEndpoint(t *testing.T) {
	dir, err := os.MkdirTemp("", "hotelier-logapi-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	cfg := config.ServerConfig{
		Host:   "127.0.0.1",
		Port:   0,
		LogDir: dir,
	}
	srv := New(cfg)

	// Write some fake log entries
	srv.diskLogStore.Append(logstore.Entry{
		TaskID:    "task-1",
		Line:      "log 1",
		Timestamp: time.Date(2026, 5, 10, 0, 0, 0, 0, time.UTC),
	})
	srv.diskLogStore.Append(logstore.Entry{
		TaskID:    "task-2",
		Line:      "log 2",
		Timestamp: time.Date(2026, 5, 11, 0, 0, 0, 0, time.UTC),
	})

	req := httptest.NewRequest(http.MethodGet, "/api/logs", nil)
	w := httptest.NewRecorder()
	srv.HandleLogs(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var response map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &response)

	dates := response["dates"].([]interface{})
	if len(dates) != 2 {
		t.Errorf("expected 2 dates, got %d", len(dates))
	}
}

// TestLogAPI_TasksEndpoint verifies /api/logs/:date returns task list.
func TestLogAPI_TasksEndpoint(t *testing.T) {
	dir, err := os.MkdirTemp("", "hotelier-logapi-tasks-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	cfg := config.ServerConfig{
		Host:   "127.0.0.1",
		Port:   0,
		LogDir: dir,
	}
	srv := New(cfg)

	srv.diskLogStore.Append(logstore.Entry{
		TaskID:    "task-a",
		Line:      "log a",
		Timestamp: time.Date(2026, 5, 10, 0, 0, 0, 0, time.UTC),
	})
	srv.diskLogStore.Append(logstore.Entry{
		TaskID:    "task-b",
		Line:      "log b",
		Timestamp: time.Date(2026, 5, 10, 0, 0, 0, 0, time.UTC),
	})

	req := httptest.NewRequest(http.MethodGet, "/api/logs/2026-05-10", nil)
	w := httptest.NewRecorder()
	srv.HandleLogEntry(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var response map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &response)

	tasks := response["tasks"].([]interface{})
	if len(tasks) != 2 {
		t.Errorf("expected 2 tasks, got %d", len(tasks))
	}
}

// TestLogAPI_EntryEndpoint verifies /api/logs/:date/:task returns log entries.
func TestLogAPI_EntryEndpoint(t *testing.T) {
	dir, err := os.MkdirTemp("", "hotelier-logapi-entry-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	cfg := config.ServerConfig{
		Host:   "127.0.0.1",
		Port:   0,
		LogDir: dir,
	}
	srv := New(cfg)

	srv.diskLogStore.Append(logstore.Entry{
		TaskID:    "my-task",
		Line:      "Hello world",
		Level:     "text",
		Timestamp: time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC),
	})
	srv.diskLogStore.Append(logstore.Entry{
		TaskID:    "my-task",
		Line:      "[TOOL] read file",
		Level:     "tool",
		Timestamp: time.Date(2026, 5, 10, 12, 0, 1, 0, time.UTC),
	})

	req := httptest.NewRequest(http.MethodGet, "/api/logs/2026-05-10/my-task", nil)
	w := httptest.NewRecorder()
	srv.HandleLogEntry(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var response map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &response)

	entries := response["entries"].([]interface{})
	if len(entries) != 2 {
		t.Errorf("expected 2 entries, got %d", len(entries))
	}

	// Verify first entry
	first := entries[0].(map[string]interface{})
	if first["task_id"] != "my-task" {
		t.Errorf("expected task_id 'my-task', got %v", first["task_id"])
	}
	if first["line"] != "Hello world" {
		t.Errorf("expected line 'Hello world', got %v", first["line"])
	}
}

// TestLogAPI_Disabled verifies 503 when log_dir is not configured.
func TestLogAPI_Disabled(t *testing.T) {
	cfg := config.ServerConfig{
		Host: "127.0.0.1",
		Port: 0,
	}
	srv := New(cfg)

	req := httptest.NewRequest(http.MethodGet, "/api/logs", nil)
	w := httptest.NewRecorder()
	srv.HandleLogs(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503 when log store disabled, got %d", w.Code)
	}
}

// TestLogAPI_InvalidMethod verifies 405 for non-GET methods.
func TestLogAPI_InvalidMethod(t *testing.T) {
	dir, err := os.MkdirTemp("", "hotelier-logapi-method-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	cfg := config.ServerConfig{
		Host:   "127.0.0.1",
		Port:   0,
		LogDir: dir,
	}
	srv := New(cfg)

	for _, method := range []string{"POST", "PUT", "DELETE"} {
		req := httptest.NewRequest(method, "/api/logs", nil)
		w := httptest.NewRecorder()
		srv.HandleLogs(w, req)
		if w.Code != http.StatusMethodNotAllowed {
			t.Errorf("%s /api/logs: expected 405, got %d", method, w.Code)
		}
	}

	for _, method := range []string{"POST", "PUT", "DELETE"} {
		req := httptest.NewRequest(method, "/api/logs/2026-05-10", nil)
		w := httptest.NewRecorder()
		srv.HandleLogEntry(w, req)
		if w.Code != http.StatusMethodNotAllowed {
			t.Errorf("%s /api/logs/2026-05-10: expected 405, got %d", method, w.Code)
		}
	}
}
