package orchestrator

import (
	"testing"

	"hotelier/pkg/queue"
)

// TestOrchestrator_SetTaskPriority_Pending verifies that a pending task's
// priority can be bumped and the change is persisted (issue #266).
func TestOrchestrator_SetTaskPriority_Pending(t *testing.T) {
	orch, store := newPersistingOrchestrator(t)
	if err := orch.AddTask(&queue.Task{ID: "task-1", Prompt: "test"}); err != nil {
		t.Fatalf("AddTask failed: %v", err)
	}

	if err := orch.SetTaskPriority("task-1", queue.PriorityFirefighter); err != nil {
		t.Fatalf("SetTaskPriority failed: %v", err)
	}

	task, ok := orch.GetTask("task-1")
	if !ok {
		t.Fatal("task not found")
	}
	if task.Priority != queue.PriorityFirefighter {
		t.Errorf("expected priority %q, got %q", queue.PriorityFirefighter, task.Priority)
	}

	// The persisted copy must carry the new priority, not the original.
	restored, err := store.Load()
	if err != nil {
		t.Fatalf("store.Load failed: %v", err)
	}
	for _, r := range restored {
		if r.ID == "task-1" && r.Priority != queue.PriorityFirefighter {
			t.Errorf("persisted priority is %q, want %q", r.Priority, queue.PriorityFirefighter)
		}
	}
}

// TestOrchestrator_SetTaskPriority_NonPending verifies that only PENDING
// tasks accept priority changes: once a task is committed to a guest the
// priority no longer affects ordering (issue #266).
func TestOrchestrator_SetTaskPriority_NonPending(t *testing.T) {
	orch, _ := newPersistingOrchestrator(t)
	_ = orch.RegisterGuest("guest-1", "G", []string{})
	_ = orch.AddTask(&queue.Task{ID: "task-1", Prompt: "test"})
	_ = orch.AssignTask("task-1", "guest-1")

	if err := orch.SetTaskPriority("task-1", queue.PriorityFirefighter); err == nil {
		t.Error("expected error for ASSIGNED task")
	}

	_ = orch.AcknowledgeTask("task-1", "guest-1")
	if err := orch.SetTaskPriority("task-1", queue.PriorityFirefighter); err == nil {
		t.Error("expected error for RUNNING task")
	}

	_ = orch.CompleteTask("task-1", "guest-1", "done")
	if err := orch.SetTaskPriority("task-1", queue.PriorityTeacher); err == nil {
		t.Error("expected error for terminal task")
	}
}

// TestOrchestrator_SetTaskPriority_Invalid verifies input validation at the
// orchestration layer: unknown ids and unknown priorities are rejected and
// leave the task untouched (issue #266).
func TestOrchestrator_SetTaskPriority_Invalid(t *testing.T) {
	orch, _ := newPersistingOrchestrator(t)
	_ = orch.AddTask(&queue.Task{ID: "task-1", Prompt: "test", Priority: queue.PriorityTeacher})

	if err := orch.SetTaskPriority("nonexistent", queue.PriorityTeacher); err == nil {
		t.Error("expected error for unknown task id")
	}
	if err := orch.SetTaskPriority("task-1", "captain"); err == nil {
		t.Error("expected error for invalid priority")
	}

	task, _ := orch.GetTask("task-1")
	if task.Priority != queue.PriorityTeacher {
		t.Errorf("priority changed on rejected input: got %q", task.Priority)
	}
}
