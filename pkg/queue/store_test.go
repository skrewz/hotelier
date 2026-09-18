package queue

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	store, err := NewStore(t.TempDir(), func(format string, args ...interface{}) {})
	if err != nil {
		t.Fatalf("NewStore failed: %v", err)
	}
	return store
}

func testTask(id string, status TaskStatus) *Task {
	return &Task{
		ID:        id,
		Prompt:    "do the thing",
		Tags:      []string{"tag1"},
		Priority:  PriorityOrangutan,
		Status:    status,
		CreatedAt: time.Now().Add(-time.Hour),
	}
}

func fileExists(t *testing.T, path string) bool {
	t.Helper()
	_, err := os.Stat(path)
	if err == nil {
		return true
	}
	if os.IsNotExist(err) {
		return false
	}
	t.Fatalf("stat %s: %v", path, err)
	return false
}

func TestNewStore_CreatesDirectory(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "nested", "queue")
	store, err := NewStore(dir, func(format string, args ...interface{}) {})
	if err != nil {
		t.Fatalf("NewStore failed: %v", err)
	}
	if store == nil {
		t.Fatal("expected non-nil store")
	}
	if !fileExists(t, dir) {
		t.Errorf("expected directory %s to exist", dir)
	}
}

func TestStore_Save_PendingTask(t *testing.T) {
	store := newTestStore(t)
	task := testTask("task-1", TaskStatusPending)

	if err := store.Save(task); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	path := filepath.Join(store.dir, "pending", "task-1.json")
	if !fileExists(t, path) {
		t.Fatalf("expected file at %s", path)
	}

	loaded, err := store.Load()
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if len(loaded) != 1 {
		t.Fatalf("expected 1 task, got %d", len(loaded))
	}
	if loaded[0].ID != "task-1" || loaded[0].Status != TaskStatusPending ||
		loaded[0].Prompt != "do the thing" || len(loaded[0].Tags) != 1 {
		t.Errorf("unexpected loaded task: %+v", loaded[0])
	}
}

func TestStore_Save_AssignedAndRunningTasks(t *testing.T) {
	store := newTestStore(t)

	if err := store.Save(testTask("task-a", TaskStatusAssigned)); err != nil {
		t.Fatalf("Save assigned failed: %v", err)
	}
	if err := store.Save(testTask("task-r", TaskStatusRunning)); err != nil {
		t.Fatalf("Save running failed: %v", err)
	}

	if !fileExists(t, filepath.Join(store.dir, "assigned", "task-a.json")) {
		t.Error("expected file in assigned/")
	}
	if !fileExists(t, filepath.Join(store.dir, "running", "task-r.json")) {
		t.Error("expected file in running/")
	}
}

func TestStore_Save_TerminalStatusErrors(t *testing.T) {
	store := newTestStore(t)

	for _, status := range []TaskStatus{TaskStatusCompleted, TaskStatusFailed, TaskStatusCancelled} {
		if err := store.Save(testTask("task-x", status)); err == nil {
			t.Errorf("expected error saving task with status %s, got nil", status)
		}
	}
}

func TestStore_Save_MovesFileOnStatusChange(t *testing.T) {
	store := newTestStore(t)
	task := testTask("task-1", TaskStatusPending)
	if err := store.Save(task); err != nil {
		t.Fatalf("Save pending failed: %v", err)
	}

	task.Status = TaskStatusAssigned
	task.AssignedTo = "guest-1"
	if err := store.Save(task); err != nil {
		t.Fatalf("Save assigned failed: %v", err)
	}

	if fileExists(t, filepath.Join(store.dir, "pending", "task-1.json")) {
		t.Error("expected pending file to be removed after status change")
	}
	if !fileExists(t, filepath.Join(store.dir, "assigned", "task-1.json")) {
		t.Error("expected assigned file to exist")
	}

	loaded, err := store.Load()
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if len(loaded) != 1 {
		t.Fatalf("expected 1 task, got %d", len(loaded))
	}
	if loaded[0].Status != TaskStatusAssigned || loaded[0].AssignedTo != "guest-1" {
		t.Errorf("unexpected loaded task: %+v", loaded[0])
	}
}

func TestStore_Delete_RemovesFile(t *testing.T) {
	store := newTestStore(t)
	if err := store.Save(testTask("task-1", TaskStatusPending)); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	if err := store.Delete("task-1"); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	if fileExists(t, filepath.Join(store.dir, "pending", "task-1.json")) {
		t.Error("expected file to be removed")
	}

	// Deleting a non-existent task is not an error.
	if err := store.Delete("task-1"); err != nil {
		t.Errorf("expected nil error deleting missing task, got %v", err)
	}
}

func TestStore_Load_EmptyDir(t *testing.T) {
	store := newTestStore(t)
	loaded, err := store.Load()
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if len(loaded) != 0 {
		t.Errorf("expected 0 tasks, got %d", len(loaded))
	}
}

func TestStore_Load_SkipsCorruptFile(t *testing.T) {
	store := newTestStore(t)
	if err := store.Save(testTask("task-good", TaskStatusPending)); err != nil {
		t.Fatalf("Save failed: %v", err)
	}
	if err := os.WriteFile(filepath.Join(store.dir, "pending", "task-bad.json"), []byte("{not json"), 0o644); err != nil {
		t.Fatalf("write corrupt file: %v", err)
	}

	logs := make([]string, 0)
	store.logf = func(format string, args ...interface{}) {
		logs = append(logs, format)
	}

	loaded, err := store.Load()
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if len(loaded) != 1 || loaded[0].ID != "task-good" {
		t.Errorf("expected only task-good, got %+v", loaded)
	}
	if len(logs) == 0 {
		t.Error("expected a log message about the corrupt file")
	}
}

func TestStore_Load_DuplicateIDPrefersNewest(t *testing.T) {
	store := newTestStore(t)

	// Simulate a crash between writing the new file and deleting the old one:
	// the same task ID exists in two status directories.
	taskOld := testTask("task-1", TaskStatusPending)
	if err := store.Save(taskOld); err != nil {
		t.Fatalf("Save pending failed: %v", err)
	}
	taskNew := testTask("task-1", TaskStatusRunning)
	taskNew.AssignedTo = "guest-1"
	if err := store.Save(taskNew); err != nil {
		t.Fatalf("Save running failed: %v", err)
	}
	// Re-create the stale pending file with an older mtime.
	stale := filepath.Join(store.dir, "pending", "task-1.json")
	if err := os.WriteFile(stale, []byte(`{"id":"task-1","prompt":"do the thing","status":"PENDING"}`), 0o644); err != nil {
		t.Fatalf("write stale file: %v", err)
	}
	past := time.Now().Add(-time.Minute)
	if err := os.Chtimes(stale, past, past); err != nil {
		t.Fatalf("chtimes: %v", err)
	}

	loaded, err := store.Load()
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if len(loaded) != 1 {
		t.Fatalf("expected 1 task (deduped), got %d: %+v", len(loaded), loaded)
	}
	if loaded[0].Status != TaskStatusRunning || loaded[0].AssignedTo != "guest-1" {
		t.Errorf("expected the newer (running) copy to win, got %+v", loaded[0])
	}
}

func TestNewStore_ParentIsFileErrors(t *testing.T) {
	file := filepath.Join(t.TempDir(), "blocker")
	if err := os.WriteFile(file, []byte("x"), 0o644); err != nil {
		t.Fatalf("write blocker: %v", err)
	}
	if _, err := NewStore(filepath.Join(file, "sub"), func(format string, args ...interface{}) {}); err == nil {
		t.Error("expected error when parent path is a file")
	}
}

func TestStore_Save_WriteFailure(t *testing.T) {
	store := newTestStore(t)
	// Make the pending directory read-only so the temp-file write fails.
	if err := os.Chmod(filepath.Join(store.dir, "pending"), 0o500); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	t.Cleanup(func() { os.Chmod(filepath.Join(store.dir, "pending"), 0o755) })

	if err := store.Save(testTask("task-1", TaskStatusPending)); err == nil {
		t.Error("expected save to fail on read-only directory")
	}
}

func TestStore_Save_StaleRemovalFailure(t *testing.T) {
	store := newTestStore(t)
	if err := store.Save(testTask("task-1", TaskStatusPending)); err != nil {
		t.Fatalf("Save pending failed: %v", err)
	}
	// Make the pending directory read-only: the new (assigned) file is
	// written fine, but removing the stale pending copy fails.
	if err := os.Chmod(filepath.Join(store.dir, "pending"), 0o500); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	t.Cleanup(func() { os.Chmod(filepath.Join(store.dir, "pending"), 0o755) })

	task := testTask("task-1", TaskStatusAssigned)
	if err := store.Save(task); err == nil {
		t.Error("expected save to fail when the stale copy cannot be removed")
	}
}

func TestStore_Delete_Failure(t *testing.T) {
	store := newTestStore(t)
	if err := store.Save(testTask("task-1", TaskStatusPending)); err != nil {
		t.Fatalf("Save failed: %v", err)
	}
	if err := os.Chmod(filepath.Join(store.dir, "pending"), 0o500); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	t.Cleanup(func() { os.Chmod(filepath.Join(store.dir, "pending"), 0o755) })

	if err := store.Delete("task-1"); err == nil {
		t.Error("expected delete to fail on read-only directory")
	}
}

func TestStore_Load_ReadDirFailure(t *testing.T) {
	store := newTestStore(t)
	if err := os.Chmod(filepath.Join(store.dir, "pending"), 0o000); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	t.Cleanup(func() { os.Chmod(filepath.Join(store.dir, "pending"), 0o755) })

	if _, err := store.Load(); err == nil {
		t.Error("expected Load to fail when a status directory is unreadable")
	}
}

func TestStore_Load_SkipsEmptyID(t *testing.T) {
	store := newTestStore(t)
	if err := os.WriteFile(filepath.Join(store.dir, "pending", "empty-id.json"), []byte(`{"prompt":"no id"}`), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	loaded, err := store.Load()
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if len(loaded) != 0 {
		t.Errorf("expected 0 tasks, got %+v", loaded)
	}
}
