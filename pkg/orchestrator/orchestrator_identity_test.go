package orchestrator

import (
	"testing"

	"hotelier/pkg/queue"
	"hotelier/pkg/registry"
)

// Issue #253: with a stable, configured guest id, "this id is already in the
// registry" no longer means "the same process is back". A fresh process holds
// no task, so reconciliation must drop the stale reference rather than hand
// the previous process's task back to it.

func TestReconcileFreshGuest_DropsStaleTaskReference(t *testing.T) {
	orch := newTestOrchestrator(t)
	_ = orch.RegisterGuest("guest-1", "Test Guest", []string{})

	task := &queue.Task{ID: "task-1", Prompt: "test"}
	_ = orch.AddTask(task)
	_ = orch.AssignTask("task-1", "guest-1")
	_ = orch.AcknowledgeTask("task-1", "guest-1")

	state := orch.ReconcileFreshGuest("guest-1")
	if state.Status != queue.TaskStatusPending {
		t.Errorf("expected PENDING for a fresh registration, got %s", state.Status)
	}
	if state.TaskID != "" {
		t.Errorf("a fresh process has no task, got %s", state.TaskID)
	}

	guest, _ := orch.GetGuest("guest-1")
	if guest.TaskID != "" {
		t.Errorf("expected the stale task reference to be cleared, got %s", guest.TaskID)
	}
	if guest.State != registry.GuestStateIdle {
		t.Errorf("expected guest IDLE, got %s", guest.State)
	}
}

// The task itself is left alone here. Revoking the claim the assignment leaves
// behind is issue #252; until that lands the stuck-task probe re-queues it
// within its timeout.
func TestReconcileFreshGuest_LeavesTaskAlone(t *testing.T) {
	orch := newTestOrchestrator(t)
	_ = orch.RegisterGuest("guest-1", "Test Guest", []string{})

	task := &queue.Task{ID: "task-1", Prompt: "test"}
	_ = orch.AddTask(task)
	_ = orch.AssignTask("task-1", "guest-1")

	_ = orch.ReconcileFreshGuest("guest-1")

	got, _ := orch.GetTask("task-1")
	if got.Status != queue.TaskStatusAssigned {
		t.Errorf("expected the task to stay ASSIGNED for the probe to re-queue, got %s", got.Status)
	}
}

func TestReconcileFreshGuest_UnknownGuest(t *testing.T) {
	orch := newTestOrchestrator(t)

	state := orch.ReconcileFreshGuest("guest-unknown")
	if state.Status != queue.TaskStatusPending || state.TaskID != "" {
		t.Errorf("expected an empty PENDING state, got %+v", state)
	}
}
