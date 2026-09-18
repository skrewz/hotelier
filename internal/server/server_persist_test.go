package server

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"hotelier/pkg/config"
	"hotelier/pkg/queue"
)

// writeQueuedTask writes a task JSON file into a queue store directory
// (issue #190), as if a previous server instance had persisted it.
func writeQueuedTask(t *testing.T, dir string, task *queue.Task) {
	t.Helper()
	subdir := map[queue.TaskStatus]string{
		queue.TaskStatusPending:  "pending",
		queue.TaskStatusAssigned: "assigned",
		queue.TaskStatusRunning:  "running",
	}[task.Status]
	if subdir == "" {
		t.Fatalf("cannot pre-write terminal task %s", task.Status)
	}
	path := filepath.Join(dir, subdir, task.ID+".json")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	data, err := json.Marshal(task)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
}

func newPersistingTestServer(t *testing.T, queueDir string) *Server {
	t.Helper()
	cfg := config.ServerConfig{
		Host:     "127.0.0.1",
		Port:     0,
		QueueDir: queueDir,
	}
	return New(cfg)
}

func TestNew_WithQueueDir_RestoresPendingTask(t *testing.T) {
	dir := t.TempDir()
	created := time.Now().Add(-time.Hour)
	writeQueuedTask(t, dir, &queue.Task{
		ID:        "task-pending",
		Prompt:    "still waiting",
		Tags:      []string{"t1"},
		Priority:  queue.PriorityFirefighter,
		Status:    queue.TaskStatusPending,
		CreatedAt: created,
	})

	srv := newPersistingTestServer(t, dir)

	task, ok := srv.TaskQueue().Get("task-pending")
	if !ok {
		t.Fatal("expected pending task to be restored")
	}
	if task.Status != queue.TaskStatusPending {
		t.Errorf("expected PENDING, got %s", task.Status)
	}
	if !task.CreatedAt.Equal(created) {
		t.Errorf("expected CreatedAt to be preserved, got %v", task.CreatedAt)
	}
	if task.Priority != queue.PriorityFirefighter {
		t.Errorf("expected priority to be preserved, got %s", task.Priority)
	}
}

func TestNew_WithQueueDir_RestoresAssignedAndRunningAsPending(t *testing.T) {
	dir := t.TempDir()
	writeQueuedTask(t, dir, &queue.Task{
		ID:         "task-assigned",
		Prompt:     "was assigned",
		Status:     queue.TaskStatusAssigned,
		CreatedAt:  time.Now().Add(-time.Hour),
		AssignedTo: "guest-gone",
	})
	writeQueuedTask(t, dir, &queue.Task{
		ID:         "task-running",
		Prompt:     "was running",
		Status:     queue.TaskStatusRunning,
		CreatedAt:  time.Now().Add(-time.Hour),
		AssignedTo: "guest-gone",
	})

	srv := newPersistingTestServer(t, dir)

	for _, id := range []string{"task-assigned", "task-running"} {
		task, ok := srv.TaskQueue().Get(id)
		if !ok {
			t.Fatalf("expected %s to be restored", id)
		}
		if task.Status != queue.TaskStatusPending {
			t.Errorf("%s: expected PENDING after restore, got %s", id, task.Status)
		}
		if task.AssignedTo != "" {
			t.Errorf("%s: expected assignment cleared, got %q", id, task.AssignedTo)
		}
	}
}

func TestNew_WithQueueDir_RestoredTaskIsAssignable(t *testing.T) {
	dir := t.TempDir()
	writeQueuedTask(t, dir, &queue.Task{
		ID:        "task-1",
		Prompt:    "restore me",
		Status:    queue.TaskStatusRunning,
		CreatedAt: time.Now().Add(-time.Hour),
	})

	srv := newPersistingTestServer(t, dir)

	// The restored task must be assignable like any other pending task.
	if err := srv.Orchestrator().RegisterGuest("guest-1", "G", []string{}); err != nil {
		t.Fatalf("RegisterGuest failed: %v", err)
	}
	if err := srv.Orchestrator().AssignTask("task-1", "guest-1"); err != nil {
		t.Fatalf("AssignTask on restored task failed: %v", err)
	}
}

func TestNew_WithQueueDir_PersistsNewTasks(t *testing.T) {
	dir := t.TempDir()
	srv := newPersistingTestServer(t, dir)

	if err := srv.Orchestrator().AddTask(&queue.Task{ID: "task-1", Prompt: "new"}); err != nil {
		t.Fatalf("AddTask failed: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "pending", "task-1.json")); err != nil {
		t.Errorf("expected task to be persisted: %v", err)
	}

	// A second server instance on the same directory sees the task.
	srv2 := newPersistingTestServer(t, dir)
	if _, ok := srv2.TaskQueue().Get("task-1"); !ok {
		t.Error("expected task to be restored by the second server instance")
	}
}

func TestNew_WithoutQueueDir_NoPersistence(t *testing.T) {
	srv := newTestServer(t)

	if err := srv.Orchestrator().AddTask(&queue.Task{ID: "task-1", Prompt: "new"}); err != nil {
		t.Fatalf("AddTask failed: %v", err)
	}
	// No store, no directory, no error — existing in-memory behaviour.
	if _, ok := srv.TaskQueue().Get("task-1"); !ok {
		t.Error("expected task in queue")
	}
}

func TestNew_WithQueueDir_MissingDirIsCreated(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "does", "not", "exist", "yet")
	srv := newPersistingTestServer(t, dir)

	if err := srv.Orchestrator().AddTask(&queue.Task{ID: "task-1", Prompt: "new"}); err != nil {
		t.Fatalf("AddTask failed: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "pending", "task-1.json")); err != nil {
		t.Errorf("expected task file in created directory: %v", err)
	}
}
