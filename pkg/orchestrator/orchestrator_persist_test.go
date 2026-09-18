package orchestrator

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"hotelier/pkg/queue"
)

// newPersistingOrchestrator creates an orchestrator wired to a queue store
// in a temp directory (issue #190).
func newPersistingOrchestrator(t *testing.T) (*Orchestrator, *queue.Store) {
	t.Helper()
	orch := New(func(format string, args ...interface{}) {})
	store, err := queue.NewStore(t.TempDir(), func(format string, args ...interface{}) {})
	if err != nil {
		t.Fatalf("NewStore failed: %v", err)
	}
	orch.SetStore(store)
	return orch, store
}

func storeTaskPath(t *testing.T, store *queue.Store, taskID string, status queue.TaskStatus) string {
	t.Helper()
	subdir := map[queue.TaskStatus]string{
		queue.TaskStatusPending:  "pending",
		queue.TaskStatusAssigned: "assigned",
		queue.TaskStatusRunning:  "running",
	}[status]
	return filepath.Join(store.Dir(), subdir, taskID+".json")
}

func assertFileExists(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); err != nil {
		t.Errorf("expected file at %s: %v", path, err)
	}
}

func assertFileGone(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); err == nil {
		t.Errorf("expected no file at %s", path)
	} else if !os.IsNotExist(err) {
		t.Errorf("stat %s: %v", path, err)
	}
}

func TestOrchestrator_AddTask_Persists(t *testing.T) {
	orch, store := newPersistingOrchestrator(t)

	if err := orch.AddTask(&queue.Task{ID: "task-1", Prompt: "test"}); err != nil {
		t.Fatalf("AddTask failed: %v", err)
	}
	assertFileExists(t, storeTaskPath(t, store, "task-1", queue.TaskStatusPending))
}

func TestOrchestrator_AddTaskOrDedup_Persists(t *testing.T) {
	orch, store := newPersistingOrchestrator(t)

	_, deduped, err := orch.AddTaskOrDedup(&queue.Task{ID: "task-1", Prompt: "test", DedupKey: "k1"})
	if err != nil || deduped {
		t.Fatalf("AddTaskOrDedup failed: %v (deduped: %v)", err, deduped)
	}
	assertFileExists(t, storeTaskPath(t, store, "task-1", queue.TaskStatusPending))

	// A squelched duplicate must not disturb the persisted file.
	existing, deduped, err := orch.AddTaskOrDedup(&queue.Task{ID: "task-2", Prompt: "test", DedupKey: "k1"})
	if err != nil || !deduped || existing.ID != "task-1" {
		t.Fatalf("expected dedup, got existing=%v deduped=%v err=%v", existing, deduped, err)
	}
	if _, err := os.Stat(storeTaskPath(t, store, "task-2", queue.TaskStatusPending)); !os.IsNotExist(err) {
		t.Error("expected no file for the squelched task")
	}
}

func TestOrchestrator_AssignTask_MovesToAssigned(t *testing.T) {
	orch, store := newPersistingOrchestrator(t)
	_ = orch.RegisterGuest("guest-1", "G", []string{})
	_ = orch.AddTask(&queue.Task{ID: "task-1", Prompt: "test"})

	if err := orch.AssignTask("task-1", "guest-1"); err != nil {
		t.Fatalf("AssignTask failed: %v", err)
	}
	assertFileGone(t, storeTaskPath(t, store, "task-1", queue.TaskStatusPending))
	assertFileExists(t, storeTaskPath(t, store, "task-1", queue.TaskStatusAssigned))
}

func TestOrchestrator_AcknowledgeTask_MovesToRunning(t *testing.T) {
	orch, store := newPersistingOrchestrator(t)
	_ = orch.RegisterGuest("guest-1", "G", []string{})
	_ = orch.AddTask(&queue.Task{ID: "task-1", Prompt: "test"})
	_ = orch.AssignTask("task-1", "guest-1")

	if err := orch.AcknowledgeTask("task-1", "guest-1"); err != nil {
		t.Fatalf("AcknowledgeTask failed: %v", err)
	}
	assertFileGone(t, storeTaskPath(t, store, "task-1", queue.TaskStatusAssigned))
	assertFileExists(t, storeTaskPath(t, store, "task-1", queue.TaskStatusRunning))
}

func TestOrchestrator_CompleteTask_RemovesFile(t *testing.T) {
	orch, store := newPersistingOrchestrator(t)
	_ = orch.RegisterGuest("guest-1", "G", []string{})
	_ = orch.AddTask(&queue.Task{ID: "task-1", Prompt: "test"})
	_ = orch.AssignTask("task-1", "guest-1")
	_ = orch.AcknowledgeTask("task-1", "guest-1")

	if err := orch.CompleteTask("task-1", "guest-1", "done"); err != nil {
		t.Fatalf("CompleteTask failed: %v", err)
	}
	assertFileGone(t, storeTaskPath(t, store, "task-1", queue.TaskStatusRunning))
}

func TestOrchestrator_FailTask_RemovesFile(t *testing.T) {
	orch, store := newPersistingOrchestrator(t)
	_ = orch.RegisterGuest("guest-1", "G", []string{})
	_ = orch.AddTask(&queue.Task{ID: "task-1", Prompt: "test"})
	_ = orch.AssignTask("task-1", "guest-1")

	if err := orch.FailTask("task-1", "guest-1", "boom"); err != nil {
		t.Fatalf("FailTask failed: %v", err)
	}
	assertFileGone(t, storeTaskPath(t, store, "task-1", queue.TaskStatusAssigned))
}

func TestOrchestrator_CancelTask_RemovesFile(t *testing.T) {
	orch, store := newPersistingOrchestrator(t)
	_ = orch.AddTask(&queue.Task{ID: "task-1", Prompt: "test"})

	if err := orch.CancelTask("task-1", ""); err != nil {
		t.Fatalf("CancelTask failed: %v", err)
	}
	assertFileGone(t, storeTaskPath(t, store, "task-1", queue.TaskStatusPending))
}

func TestOrchestrator_RequeueTask_MovesToPending(t *testing.T) {
	orch, store := newPersistingOrchestrator(t)
	_ = orch.RegisterGuest("guest-1", "G", []string{})
	_ = orch.AddTask(&queue.Task{ID: "task-1", Prompt: "test"})
	_ = orch.AssignTask("task-1", "guest-1")
	_ = orch.AcknowledgeTask("task-1", "guest-1")

	if err := orch.RequeueTask("task-1"); err != nil {
		t.Fatalf("RequeueTask failed: %v", err)
	}
	assertFileGone(t, storeTaskPath(t, store, "task-1", queue.TaskStatusRunning))
	assertFileExists(t, storeTaskPath(t, store, "task-1", queue.TaskStatusPending))
}

func TestOrchestrator_DeclineTask_MovesToPending(t *testing.T) {
	orch, store := newPersistingOrchestrator(t)
	_ = orch.RegisterGuest("guest-1", "G", []string{})
	_ = orch.AddTask(&queue.Task{ID: "task-1", Prompt: "test"})
	_ = orch.AssignTask("task-1", "guest-1")

	if err := orch.DeclineTask("task-1", "guest-1"); err != nil {
		t.Fatalf("DeclineTask failed: %v", err)
	}
	assertFileGone(t, storeTaskPath(t, store, "task-1", queue.TaskStatusAssigned))
	assertFileExists(t, storeTaskPath(t, store, "task-1", queue.TaskStatusPending))
}

func TestOrchestrator_TryAssignNext_PersistsAssignment(t *testing.T) {
	orch, store := newPersistingOrchestrator(t)
	_ = orch.RegisterGuest("guest-1", "G", []string{})
	_ = orch.AddTask(&queue.Task{ID: "task-1", Prompt: "test"})

	if !orch.TryAssignNext() {
		t.Fatal("expected TryAssignNext to assign a task")
	}
	assertFileExists(t, storeTaskPath(t, store, "task-1", queue.TaskStatusAssigned))
}

func TestOrchestrator_UnregisterGuestForce_PersistsRequeue(t *testing.T) {
	orch, store := newPersistingOrchestrator(t)
	_ = orch.RegisterGuest("guest-1", "G", []string{})
	_ = orch.AddTask(&queue.Task{ID: "task-1", Prompt: "test"})
	_ = orch.AssignTask("task-1", "guest-1")
	_ = orch.AcknowledgeTask("task-1", "guest-1")

	if err := orch.UnregisterGuestForce("guest-1"); err != nil {
		t.Fatalf("UnregisterGuestForce failed: %v", err)
	}
	assertFileGone(t, storeTaskPath(t, store, "task-1", queue.TaskStatusRunning))
	assertFileExists(t, storeTaskPath(t, store, "task-1", queue.TaskStatusPending))
}

func TestOrchestrator_CheckStuckTasks_PersistsRequeue(t *testing.T) {
	orch, store := newPersistingOrchestrator(t)
	_ = orch.RegisterGuest("guest-1", "G", []string{})
	_ = orch.AddTask(&queue.Task{ID: "task-1", Prompt: "test"})
	_ = orch.AssignTask("task-1", "guest-1")

	// Age the assignment and heartbeats past the timeout.
	task, _ := orch.GetTask("task-1")
	task.AssignedAt = time.Now().Add(-time.Hour)
	guest, _ := orch.GetGuest("guest-1")
	guest.LastHeartbeat = time.Now().Add(-time.Hour)
	guest.LastTaskHeartbeat = time.Now().Add(-time.Hour)

	requeued := orch.CheckStuckTasks(time.Minute)
	if len(requeued) != 1 || requeued[0].TaskID != "task-1" {
		t.Fatalf("expected task-1 to be re-queued, got %+v", requeued)
	}
	assertFileExists(t, storeTaskPath(t, store, "task-1", queue.TaskStatusPending))
}

func TestOrchestrator_CheckSilentGuests_RemovesFile(t *testing.T) {
	orch, store := newPersistingOrchestrator(t)
	_ = orch.RegisterGuest("guest-1", "G", []string{})
	_ = orch.AddTask(&queue.Task{ID: "task-1", Prompt: "test"})
	_ = orch.AssignTask("task-1", "guest-1")
	_ = orch.AcknowledgeTask("task-1", "guest-1")

	guest, _ := orch.GetGuest("guest-1")
	guest.LastHeartbeat = time.Now().Add(-time.Hour)

	failed := orch.CheckSilentGuests(time.Minute)
	if len(failed) != 1 || failed[0].TaskID != "task-1" {
		t.Fatalf("expected task-1 to be failed, got %+v", failed)
	}
	assertFileGone(t, storeTaskPath(t, store, "task-1", queue.TaskStatusRunning))
}

func TestOrchestrator_RemoveStaleGuests_RemovesFile(t *testing.T) {
	orch, store := newPersistingOrchestrator(t)
	_ = orch.RegisterGuest("guest-1", "G", []string{})
	_ = orch.AddTask(&queue.Task{ID: "task-1", Prompt: "test"})
	_ = orch.AssignTask("task-1", "guest-1")
	_ = orch.AcknowledgeTask("task-1", "guest-1")

	guest, _ := orch.GetGuest("guest-1")
	guest.LastHeartbeat = time.Now().Add(-time.Hour)

	removed := orch.RemoveStaleGuests(time.Minute)
	if len(removed) != 1 || removed[0].TaskID != "task-1" {
		t.Fatalf("expected task-1 to be failed and guest removed, got %+v", removed)
	}
	assertFileGone(t, storeTaskPath(t, store, "task-1", queue.TaskStatusRunning))
}

func TestOrchestrator_RestoreTask_RestoresAsPending(t *testing.T) {
	orch, store := newPersistingOrchestrator(t)

	created := time.Now().Add(-2 * time.Hour)
	task := &queue.Task{
		ID:         "task-1",
		Prompt:     "restored",
		Tags:       []string{"t1"},
		Priority:   queue.PriorityFirefighter,
		Status:     queue.TaskStatusRunning,
		CreatedAt:  created,
		AssignedTo: "guest-old",
	}

	if err := orch.RestoreTask(task); err != nil {
		t.Fatalf("RestoreTask failed: %v", err)
	}

	restored, ok := orch.GetTask("task-1")
	if !ok {
		t.Fatal("expected task to exist")
	}
	if restored.Status != queue.TaskStatusPending {
		t.Errorf("expected PENDING, got %s", restored.Status)
	}
	if !restored.CreatedAt.Equal(created) {
		t.Errorf("expected CreatedAt to be preserved, got %v", restored.CreatedAt)
	}
	if restored.AssignedTo != "" {
		t.Errorf("expected assignment cleared, got %q", restored.AssignedTo)
	}
	assertFileExists(t, storeTaskPath(t, store, "task-1", queue.TaskStatusPending))
}

func TestOrchestrator_RestoreTask_DuplicateErrors(t *testing.T) {
	orch, _ := newPersistingOrchestrator(t)
	_ = orch.AddTask(&queue.Task{ID: "task-1", Prompt: "original"})

	err := orch.RestoreTask(&queue.Task{ID: "task-1", Prompt: "duplicate"})
	if err == nil {
		t.Error("expected error restoring duplicate task, got nil")
	}
}

func TestOrchestrator_NoStore_OperationsSucceed(t *testing.T) {
	orch := New(func(format string, args ...interface{}) {})
	// No store set — everything must work as before.
	if err := orch.AddTask(&queue.Task{ID: "task-1", Prompt: "test"}); err != nil {
		t.Fatalf("AddTask failed: %v", err)
	}
	_ = orch.RegisterGuest("guest-1", "G", []string{})
	if err := orch.AssignTask("task-1", "guest-1"); err != nil {
		t.Fatalf("AssignTask failed: %v", err)
	}
	if err := orch.AcknowledgeTask("task-1", "guest-1"); err != nil {
		t.Fatalf("AcknowledgeTask failed: %v", err)
	}
	if err := orch.CompleteTask("task-1", "guest-1", "done"); err != nil {
		t.Fatalf("CompleteTask failed: %v", err)
	}
}

func TestOrchestrator_StoreFailure_DoesNotBreakTaskProcessing(t *testing.T) {
	orch, store := newPersistingOrchestrator(t)

	// Break the store: remove its root directory so every Save/Delete fails.
	if err := os.RemoveAll(store.Dir()); err != nil {
		t.Fatalf("RemoveAll: %v", err)
	}

	// Task processing must still succeed; store errors are logged only.
	if err := orch.AddTask(&queue.Task{ID: "task-1", Prompt: "test"}); err != nil {
		t.Fatalf("AddTask failed despite store failure: %v", err)
	}
	_ = orch.RegisterGuest("guest-1", "G", []string{})
	if err := orch.AssignTask("task-1", "guest-1"); err != nil {
		t.Fatalf("AssignTask failed despite store failure: %v", err)
	}
	if err := orch.AcknowledgeTask("task-1", "guest-1"); err != nil {
		t.Fatalf("AcknowledgeTask failed despite store failure: %v", err)
	}
	if err := orch.CompleteTask("task-1", "guest-1", "done"); err != nil {
		t.Fatalf("CompleteTask failed despite store failure: %v", err)
	}

	task, ok := orch.GetTask("task-1")
	if !ok || task.Status != queue.TaskStatusCompleted {
		t.Errorf("expected completed task, got %+v", task)
	}
}

func TestOrchestrator_StoreDeleteFailure_DoesNotBreakCancellation(t *testing.T) {
	orch, store := newPersistingOrchestrator(t)
	_ = orch.AddTask(&queue.Task{ID: "task-1", Prompt: "test"})

	// Make the pending directory read-only so Delete fails.
	pendingDir := filepath.Join(store.Dir(), "pending")
	if err := os.Chmod(pendingDir, 0o500); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	t.Cleanup(func() { os.Chmod(pendingDir, 0o755) })

	if err := orch.CancelTask("task-1", ""); err != nil {
		t.Fatalf("CancelTask failed despite store failure: %v", err)
	}
	task, ok := orch.GetTask("task-1")
	if !ok || task.Status != queue.TaskStatusCancelled {
		t.Errorf("expected cancelled task, got %+v", task)
	}
}
