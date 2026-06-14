package orchestrator

import (
	"testing"
	"time"

	"hotelier/pkg/queue"
	"hotelier/pkg/registry"
)

func newTestOrchestrator(t *testing.T) *Orchestrator {
	t.Helper()
	return New(func(format string, args ...interface{}) {})
}

// --- RegisterGuest / UnregisterGuest ---

func TestRegisterGuest(t *testing.T) {
	orch := newTestOrchestrator(t)

	err := orch.RegisterGuest("guest-1", "Test Guest", []string{"tag1"})
	if err != nil {
		t.Fatalf("RegisterGuest failed: %v", err)
	}

	guest, ok := orch.GetGuest("guest-1")
	if !ok {
		t.Fatal("expected guest to exist")
	}
	if guest.ID != "guest-1" {
		t.Errorf("expected guest ID 'guest-1', got %s", guest.ID)
	}
	if guest.State != registry.GuestStateIdle {
		t.Errorf("expected guest state IDLE, got %s", guest.State)
	}
	if guest.Name != "Test Guest" {
		t.Errorf("expected name 'Test Guest', got %s", guest.Name)
	}
}

func TestRegisterDuplicateGuest(t *testing.T) {
	orch := newTestOrchestrator(t)

	_ = orch.RegisterGuest("guest-1", "Test Guest", []string{"tag1"})
	err := orch.RegisterGuest("guest-1", "Test Guest", []string{"tag1"})
	if err == nil {
		t.Error("expected error for duplicate guest, got nil")
	}
}

func TestUnregisterGuest(t *testing.T) {
	orch := newTestOrchestrator(t)
	_ = orch.RegisterGuest("guest-1", "Test Guest", []string{"tag1"})

	err := orch.UnregisterGuest("guest-1")
	if err != nil {
		t.Fatalf("UnregisterGuest failed: %v", err)
	}

	_, ok := orch.GetGuest("guest-1")
	if ok {
		t.Error("expected guest to be gone")
	}
}

func TestUnregisterRunningGuest_RequeuesTask(t *testing.T) {
	orch := newTestOrchestrator(t)
	_ = orch.RegisterGuest("guest-1", "Test Guest", []string{})

	task := &queue.Task{ID: "task-1", Prompt: "test"}
	_ = orch.AddTask(task)

	// Assign task to guest
	err := orch.AssignTask("task-1", "guest-1")
	if err != nil {
		t.Fatalf("AssignTask failed: %v", err)
	}

	// Guest is now RUNNING with the task
	guest, _ := orch.GetGuest("guest-1")
	if guest.State != registry.GuestStateRunning {
		t.Fatalf("expected guest RUNNING, got %s", guest.State)
	}

	// Unregister — should re-queue the task
	err = orch.UnregisterGuestForce("guest-1")
	if err != nil {
		t.Fatalf("UnregisterGuestForce failed: %v", err)
	}

	// Task should be back to PENDING
	task, ok := orch.GetTask("task-1")
	if !ok {
		t.Fatal("expected task to still exist")
	}
	if task.Status != queue.TaskStatusPending {
		t.Errorf("expected task PENDING after unregister, got %s", task.Status)
	}
	if task.AssignedTo != "" {
		t.Errorf("expected assigned_to cleared, got %s", task.AssignedTo)
	}
}

// --- AddTask ---

func TestAddTask(t *testing.T) {
	orch := newTestOrchestrator(t)

	task := &queue.Task{ID: "task-1", Prompt: "test task"}
	err := orch.AddTask(task)
	if err != nil {
		t.Fatalf("AddTask failed: %v", err)
	}

	got, ok := orch.GetTask("task-1")
	if !ok {
		t.Fatal("expected task to exist")
	}
	if got.Status != queue.TaskStatusPending {
		t.Errorf("expected PENDING, got %s", got.Status)
	}
}

// --- AssignTask (atomic: task PENDING→ASSIGNED, guest IDLE→RUNNING) ---

func TestAssignTask_AtomicStateChange(t *testing.T) {
	orch := newTestOrchestrator(t)
	_ = orch.RegisterGuest("guest-1", "Test Guest", []string{})

	task := &queue.Task{ID: "task-1", Prompt: "test"}
	_ = orch.AddTask(task)

	err := orch.AssignTask("task-1", "guest-1")
	if err != nil {
		t.Fatalf("AssignTask failed: %v", err)
	}

	// Task should be ASSIGNED
	task, ok := orch.GetTask("task-1")
	if !ok {
		t.Fatal("task not found")
	}
	if task.Status != queue.TaskStatusAssigned {
		t.Errorf("expected task ASSIGNED, got %s", task.Status)
	}
	if task.AssignedTo != "guest-1" {
		t.Errorf("expected assigned_to 'guest-1', got %s", task.AssignedTo)
	}

	// Guest should be RUNNING with the task
	guest, ok := orch.GetGuest("guest-1")
	if !ok {
		t.Fatal("guest not found")
	}
	if guest.State != registry.GuestStateRunning {
		t.Errorf("expected guest RUNNING, got %s", guest.State)
	}
	if guest.TaskID != "task-1" {
		t.Errorf("expected guest task_id 'task-1', got %s", guest.TaskID)
	}
}

func TestAssignTask_TaskNotPending(t *testing.T) {
	orch := newTestOrchestrator(t)
	_ = orch.RegisterGuest("guest-1", "Test Guest", []string{})

	task := &queue.Task{ID: "task-1", Prompt: "test"}
	_ = orch.AddTask(task)

	// Assign once
	_ = orch.AssignTask("task-1", "guest-1")

	// Try to assign again — should fail
	err := orch.AssignTask("task-1", "guest-1")
	if err == nil {
		t.Error("expected error assigning already-assigned task")
	}
}

func TestAssignTask_GuestNotIdle(t *testing.T) {
	orch := newTestOrchestrator(t)
	_ = orch.RegisterGuest("guest-1", "Test Guest", []string{})
	_ = orch.RegisterGuest("guest-2", "Test Guest 2", []string{})

	task1 := &queue.Task{ID: "task-1", Prompt: "test 1"}
	task2 := &queue.Task{ID: "task-2", Prompt: "test 2"}
	_ = orch.AddTask(task1)
	_ = orch.AddTask(task2)

	// Assign first task
	_ = orch.AssignTask("task-1", "guest-1")

	// Try to assign second task to same guest — should fail (guest is RUNNING)
	err := orch.AssignTask("task-2", "guest-1")
	if err == nil {
		t.Error("expected error assigning task to non-idle guest")
	}

	// Guest should still have only task-1
	guest, _ := orch.GetGuest("guest-1")
	if guest.TaskID != "task-1" {
		t.Errorf("expected guest task 'task-1', got %s", guest.TaskID)
	}
}

// --- AcknowledgeTask (ASSIGNED→RUNNING, explicit handshake) ---

func TestAcknowledgeTask(t *testing.T) {
	orch := newTestOrchestrator(t)
	_ = orch.RegisterGuest("guest-1", "Test Guest", []string{})

	task := &queue.Task{ID: "task-1", Prompt: "test"}
	_ = orch.AddTask(task)
	_ = orch.AssignTask("task-1", "guest-1")

	// Task is ASSIGNED, guest is RUNNING (from AssignTask)
	task, _ = orch.GetTask("task-1")
	if task.Status != queue.TaskStatusAssigned {
		t.Fatalf("expected ASSIGNED before ack, got %s", task.Status)
	}

	// Guest acknowledges — transitions to RUNNING
	err := orch.AcknowledgeTask("task-1", "guest-1")
	if err != nil {
		t.Fatalf("AcknowledgeTask failed: %v", err)
	}

	task, _ = orch.GetTask("task-1")
	if task.Status != queue.TaskStatusRunning {
		t.Errorf("expected task RUNNING after ack, got %s", task.Status)
	}
}

func TestAcknowledgeTask_WrongGuest(t *testing.T) {
	orch := newTestOrchestrator(t)
	_ = orch.RegisterGuest("guest-1", "Test Guest", []string{})
	_ = orch.RegisterGuest("guest-2", "Other Guest", []string{})

	task := &queue.Task{ID: "task-1", Prompt: "test"}
	_ = orch.AddTask(task)
	_ = orch.AssignTask("task-1", "guest-1")

	// Wrong guest tries to acknowledge
	err := orch.AcknowledgeTask("task-1", "guest-2")
	if err == nil {
		t.Error("expected error when wrong guest acknowledges")
	}

	// Task should still be ASSIGNED to guest-1
	task, _ = orch.GetTask("task-1")
	if task.Status != queue.TaskStatusAssigned {
		t.Errorf("expected task still ASSIGNED, got %s", task.Status)
	}
}

func TestAcknowledgeTask_Idempotent(t *testing.T) {
	orch := newTestOrchestrator(t)
	_ = orch.RegisterGuest("guest-1", "Test Guest", []string{})

	task := &queue.Task{ID: "task-1", Prompt: "test"}
	_ = orch.AddTask(task)
	_ = orch.AssignTask("task-1", "guest-1")
	_ = orch.AcknowledgeTask("task-1", "guest-1")

	// Acknowledge again — should be idempotent (no error)
	err := orch.AcknowledgeTask("task-1", "guest-1")
	if err != nil {
		t.Errorf("expected idempotent ack, got error: %v", err)
	}

	task, _ = orch.GetTask("task-1")
	if task.Status != queue.TaskStatusRunning {
		t.Errorf("expected task still RUNNING, got %s", task.Status)
	}
}

// --- CompleteTask (atomic: task→COMPLETED, guest RUNNING→IDLE) ---

func TestCompleteTask_AtomicStateChange(t *testing.T) {
	orch := newTestOrchestrator(t)
	_ = orch.RegisterGuest("guest-1", "Test Guest", []string{})

	task := &queue.Task{ID: "task-1", Prompt: "test"}
	_ = orch.AddTask(task)
	_ = orch.AssignTask("task-1", "guest-1")
	_ = orch.AcknowledgeTask("task-1", "guest-1")

	err := orch.CompleteTask("task-1", "guest-1", "success output")
	if err != nil {
		t.Fatalf("CompleteTask failed: %v", err)
	}

	// Task should be COMPLETED
	task, ok := orch.GetTask("task-1")
	if !ok {
		t.Fatal("task not found")
	}
	if task.Status != queue.TaskStatusCompleted {
		t.Errorf("expected task COMPLETED, got %s", task.Status)
	}
	if task.Result != "success output" {
		t.Errorf("expected result 'success output', got %s", task.Result)
	}

	// Guest should be IDLE with no task
	guest, ok := orch.GetGuest("guest-1")
	if !ok {
		t.Fatal("guest not found")
	}
	if guest.State != registry.GuestStateIdle {
		t.Errorf("expected guest IDLE, got %s", guest.State)
	}
	if guest.TaskID != "" {
		t.Errorf("expected guest task_id cleared, got %s", guest.TaskID)
	}
}

func TestCompleteTask_FromAssignedState(t *testing.T) {
	// Guest can complete a task even if it never acknowledged (ASSIGNED state).
	// This handles the case where the guest finishes quickly.
	orch := newTestOrchestrator(t)
	_ = orch.RegisterGuest("guest-1", "Test Guest", []string{})

	task := &queue.Task{ID: "task-1", Prompt: "test"}
	_ = orch.AddTask(task)
	_ = orch.AssignTask("task-1", "guest-1")
	// Skip AcknowledgeTask — go straight to complete

	err := orch.CompleteTask("task-1", "guest-1", "done")
	if err != nil {
		t.Fatalf("CompleteTask from ASSIGNED failed: %v", err)
	}

	task, _ = orch.GetTask("task-1")
	if task.Status != queue.TaskStatusCompleted {
		t.Errorf("expected COMPLETED, got %s", task.Status)
	}
}

func TestCompleteTask_Idempotent(t *testing.T) {
	orch := newTestOrchestrator(t)
	_ = orch.RegisterGuest("guest-1", "Test Guest", []string{})

	task := &queue.Task{ID: "task-1", Prompt: "test"}
	_ = orch.AddTask(task)
	_ = orch.AssignTask("task-1", "guest-1")
	_ = orch.AcknowledgeTask("task-1", "guest-1")
	_ = orch.CompleteTask("task-1", "guest-1", "done")

	// Complete again — should be idempotent
	err := orch.CompleteTask("task-1", "guest-1", "done again")
	if err != nil {
		t.Errorf("expected idempotent complete, got error: %v", err)
	}

	task, _ = orch.GetTask("task-1")
	if task.Status != queue.TaskStatusCompleted {
		t.Errorf("expected still COMPLETED, got %s", task.Status)
	}
	// Result should not be overwritten
	if task.Result != "done" {
		t.Errorf("expected result unchanged, got %s", task.Result)
	}
}

// --- FailTask (atomic: task→FAILED, guest RUNNING→IDLE) ---

func TestFailTask_AtomicStateChange(t *testing.T) {
	orch := newTestOrchestrator(t)
	_ = orch.RegisterGuest("guest-1", "Test Guest", []string{})

	task := &queue.Task{ID: "task-1", Prompt: "test"}
	_ = orch.AddTask(task)
	_ = orch.AssignTask("task-1", "guest-1")
	_ = orch.AcknowledgeTask("task-1", "guest-1")

	err := orch.FailTask("task-1", "guest-1", "something broke")
	if err != nil {
		t.Fatalf("FailTask failed: %v", err)
	}

	task, ok := orch.GetTask("task-1")
	if !ok {
		t.Fatal("task not found")
	}
	if task.Status != queue.TaskStatusFailed {
		t.Errorf("expected task FAILED, got %s", task.Status)
	}
	if task.Error != "something broke" {
		t.Errorf("expected error 'something broke', got %s", task.Error)
	}

	guest, ok := orch.GetGuest("guest-1")
	if !ok {
		t.Fatal("guest not found")
	}
	if guest.State != registry.GuestStateIdle {
		t.Errorf("expected guest IDLE, got %s", guest.State)
	}
	if guest.TaskID != "" {
		t.Errorf("expected guest task_id cleared, got %s", guest.TaskID)
	}
}

func TestFailTask_FromAssignedState(t *testing.T) {
	// Task can fail even from ASSIGNED (guest never started).
	orch := newTestOrchestrator(t)
	_ = orch.RegisterGuest("guest-1", "Test Guest", []string{})

	task := &queue.Task{ID: "task-1", Prompt: "test"}
	_ = orch.AddTask(task)
	_ = orch.AssignTask("task-1", "guest-1")
	// Skip ack — fail directly

	err := orch.FailTask("task-1", "guest-1", "stuck, never started")
	if err != nil {
		t.Fatalf("FailTask from ASSIGNED failed: %v", err)
	}

	task, _ = orch.GetTask("task-1")
	if task.Status != queue.TaskStatusFailed {
		t.Errorf("expected FAILED, got %s", task.Status)
	}
}

// --- CancelTask (atomic: task→CANCELLED, guest RUNNING→IDLE) ---

func TestCancelTask_FromRunning(t *testing.T) {
	orch := newTestOrchestrator(t)
	_ = orch.RegisterGuest("guest-1", "Test Guest", []string{})

	task := &queue.Task{ID: "task-1", Prompt: "test"}
	_ = orch.AddTask(task)
	_ = orch.AssignTask("task-1", "guest-1")
	_ = orch.AcknowledgeTask("task-1", "guest-1")

	err := orch.CancelTask("task-1", "guest-1")
	if err != nil {
		t.Fatalf("CancelTask from RUNNING failed: %v", err)
	}

	task, _ = orch.GetTask("task-1")
	if task.Status != queue.TaskStatusCancelled {
		t.Errorf("expected CANCELLED, got %s", task.Status)
	}

	guest, _ := orch.GetGuest("guest-1")
	if guest.State != registry.GuestStateIdle {
		t.Errorf("expected guest IDLE, got %s", guest.State)
	}
}

func TestCancelTask_FromPending(t *testing.T) {
	orch := newTestOrchestrator(t)
	_ = orch.RegisterGuest("guest-1", "Test Guest", []string{})

	task := &queue.Task{ID: "task-1", Prompt: "test"}
	_ = orch.AddTask(task)
	// Cancel before assignment

	err := orch.CancelTask("task-1", "")
	if err != nil {
		t.Fatalf("CancelTask from PENDING failed: %v", err)
	}

	task, _ = orch.GetTask("task-1")
	if task.Status != queue.TaskStatusCancelled {
		t.Errorf("expected CANCELLED, got %s", task.Status)
	}
}

func TestCancelTask_FromAssigned(t *testing.T) {
	orch := newTestOrchestrator(t)
	_ = orch.RegisterGuest("guest-1", "Test Guest", []string{})

	task := &queue.Task{ID: "task-1", Prompt: "test"}
	_ = orch.AddTask(task)
	_ = orch.AssignTask("task-1", "guest-1")

	err := orch.CancelTask("task-1", "guest-1")
	if err != nil {
		t.Fatalf("CancelTask from ASSIGNED failed: %v", err)
	}

	task, _ = orch.GetTask("task-1")
	if task.Status != queue.TaskStatusCancelled {
		t.Errorf("expected CANCELLED, got %s", task.Status)
	}

	guest, _ := orch.GetGuest("guest-1")
	if guest.State != registry.GuestStateIdle {
		t.Errorf("expected guest IDLE, got %s", guest.State)
	}
}

func TestCancelTask_Idempotent(t *testing.T) {
	orch := newTestOrchestrator(t)
	_ = orch.RegisterGuest("guest-1", "Test Guest", []string{})

	task := &queue.Task{ID: "task-1", Prompt: "test"}
	_ = orch.AddTask(task)
	_ = orch.AssignTask("task-1", "guest-1")
	_ = orch.CancelTask("task-1", "guest-1")

	// Cancel again — should be idempotent
	err := orch.CancelTask("task-1", "guest-1")
	if err != nil {
		t.Errorf("expected idempotent cancel, got error: %v", err)
	}
}

// --- RequeueTask (atomic: task→PENDING, guest RUNNING→IDLE) ---

func TestRequeueTask_AtomicStateChange(t *testing.T) {
	orch := newTestOrchestrator(t)
	_ = orch.RegisterGuest("guest-1", "Test Guest", []string{})

	task := &queue.Task{ID: "task-1", Prompt: "test"}
	_ = orch.AddTask(task)
	_ = orch.AssignTask("task-1", "guest-1")
	_ = orch.AcknowledgeTask("task-1", "guest-1")

	err := orch.RequeueTask("task-1")
	if err != nil {
		t.Fatalf("RequeueTask failed: %v", err)
	}

	task, ok := orch.GetTask("task-1")
	if !ok {
		t.Fatal("task not found")
	}
	if task.Status != queue.TaskStatusPending {
		t.Errorf("expected task PENDING, got %s", task.Status)
	}
	if task.AssignedTo != "" {
		t.Errorf("expected assigned_to cleared, got %s", task.AssignedTo)
	}

	guest, ok := orch.GetGuest("guest-1")
	if !ok {
		t.Fatal("guest not found")
	}
	if guest.State != registry.GuestStateIdle {
		t.Errorf("expected guest IDLE, got %s", guest.State)
	}
	if guest.TaskID != "" {
		t.Errorf("expected guest task_id cleared, got %s", guest.TaskID)
	}
}

// --- FindAvailableGuests / GetPendingTasks ---

func TestFindAvailableGuests(t *testing.T) {
	orch := newTestOrchestrator(t)
	_ = orch.RegisterGuest("guest-1", "Guest 1", []string{"tag1"})
	_ = orch.RegisterGuest("guest-2", "Guest 2", []string{"tag2"})

	// Both idle initially
	available := orch.FindAvailableGuests([]string{})
	if len(available) != 2 {
		t.Errorf("expected 2 available guests, got %d", len(available))
	}

	// Assign a task to guest-1
	task := &queue.Task{ID: "task-1", Prompt: "test"}
	_ = orch.AddTask(task)
	_ = orch.AssignTask("task-1", "guest-1")

	// Only guest-2 should be available
	available = orch.FindAvailableGuests([]string{})
	if len(available) != 1 {
		t.Errorf("expected 1 available guest, got %d", len(available))
	}
	if available[0].ID != "guest-2" {
		t.Errorf("expected guest-2 available, got %s", available[0].ID)
	}
}

func TestGetPendingTasks(t *testing.T) {
	orch := newTestOrchestrator(t)
	_ = orch.RegisterGuest("guest-1", "Test Guest", []string{})

	_ = orch.AddTask(&queue.Task{ID: "task-1", Prompt: "a"})
	_ = orch.AddTask(&queue.Task{ID: "task-2", Prompt: "b"})
	_ = orch.AddTask(&queue.Task{ID: "task-3", Prompt: "c"})

	// Assign task-1
	_ = orch.AssignTask("task-1", "guest-1")

	pending := orch.GetPendingTasks()
	if len(pending) != 2 {
		t.Errorf("expected 2 pending tasks, got %d", len(pending))
	}
}

// --- ReconcileGuest ---

func TestReconcileGuest_TaskRunningAssignedToThisGuest(t *testing.T) {
	orch := newTestOrchestrator(t)
	_ = orch.RegisterGuest("guest-1", "Test Guest", []string{})

	task := &queue.Task{ID: "task-1", Prompt: "test"}
	_ = orch.AddTask(task)
	_ = orch.AssignTask("task-1", "guest-1")
	_ = orch.AcknowledgeTask("task-1", "guest-1")

	state := orch.ReconcileGuest("guest-1")
	if state.Status != queue.TaskStatusRunning {
		t.Errorf("expected reconciliation to return RUNNING, got %s", state.Status)
	}
	if state.TaskID != "task-1" {
		t.Errorf("expected task_id 'task-1', got %s", state.TaskID)
	}
}

func TestReconcileGuest_TaskRequeued(t *testing.T) {
	orch := newTestOrchestrator(t)
	_ = orch.RegisterGuest("guest-1", "Test Guest", []string{})

	task := &queue.Task{ID: "task-1", Prompt: "test"}
	_ = orch.AddTask(task)
	_ = orch.AssignTask("task-1", "guest-1")

	// Simulate: task was re-queued (e.g. by checkStuckTasks)
	_ = orch.RequeueTask("task-1")

	state := orch.ReconcileGuest("guest-1")
	if state.Status != queue.TaskStatusPending {
		t.Errorf("expected reconciliation to return PENDING, got %s", state.Status)
	}

	// Guest should be set to IDLE
	guest, _ := orch.GetGuest("guest-1")
	if guest.State != registry.GuestStateIdle {
		t.Errorf("expected guest IDLE after reconcile, got %s", guest.State)
	}
	if guest.TaskID != "" {
		t.Errorf("expected guest task_id cleared, got %s", guest.TaskID)
	}
}

func TestReconcileGuest_TaskAssignedToOtherGuest(t *testing.T) {
	orch := newTestOrchestrator(t)
	_ = orch.RegisterGuest("guest-1", "Guest 1", []string{})
	_ = orch.RegisterGuest("guest-2", "Guest 2", []string{})

	task := &queue.Task{ID: "task-1", Prompt: "test"}
	_ = orch.AddTask(task)

	// Task assigned to guest-2 (guest-1 was originally assigned but stolen)
	_ = orch.AssignTask("task-1", "guest-2")

	state := orch.ReconcileGuest("guest-1")
	if state.Status != queue.TaskStatusPending {
		t.Errorf("expected reconciliation to return PENDING (no task), got %s", state.Status)
	}

	// Guest-1 should be IDLE
	guest, _ := orch.GetGuest("guest-1")
	if guest.State != registry.GuestStateIdle {
		t.Errorf("expected guest IDLE, got %s", guest.State)
	}
}

func TestReconcileGuest_NoTask(t *testing.T) {
	orch := newTestOrchestrator(t)
	_ = orch.RegisterGuest("guest-1", "Test Guest", []string{})

	state := orch.ReconcileGuest("guest-1")
	if state.Status != queue.TaskStatusPending {
		t.Errorf("expected PENDING (no task), got %s", state.Status)
	}
}

// --- TryAssignNext ---

func TestTryAssignNext(t *testing.T) {
	orch := newTestOrchestrator(t)
	_ = orch.RegisterGuest("guest-1", "Guest 1", []string{})
	_ = orch.RegisterGuest("guest-2", "Guest 2", []string{})

	_ = orch.AddTask(&queue.Task{ID: "task-1", Prompt: "a"})
	_ = orch.AddTask(&queue.Task{ID: "task-2", Prompt: "b"})

	// Assign task-1 to guest-1
	_ = orch.AssignTask("task-1", "guest-1")

	// TryAssignNext should find guest-2 and assign task-2
	assigned := orch.TryAssignNext()
	if !assigned {
		t.Fatal("expected TryAssignNext to assign a task")
	}

	task2, _ := orch.GetTask("task-2")
	if task2.Status != queue.TaskStatusAssigned {
		t.Errorf("expected task-2 ASSIGNED, got %s", task2.Status)
	}
	if task2.AssignedTo != "guest-2" {
		t.Errorf("expected task-2 assigned to guest-2, got %s", task2.AssignedTo)
	}
}

func TestTryAssignNext_NoPendingTasks(t *testing.T) {
	orch := newTestOrchestrator(t)
	_ = orch.RegisterGuest("guest-1", "Guest 1", []string{})

	assigned := orch.TryAssignNext()
	if assigned {
		t.Error("expected no assignment when no pending tasks")
	}
}

func TestTryAssignNext_NoAvailableGuests(t *testing.T) {
	orch := newTestOrchestrator(t)
	_ = orch.RegisterGuest("guest-1", "Guest 1", []string{})

	_ = orch.AddTask(&queue.Task{ID: "task-1", Prompt: "a"})
	_ = orch.AssignTask("task-1", "guest-1")

	// guest-1 is busy, no other guests
	assigned := orch.TryAssignNext()
	if assigned {
		t.Error("expected no assignment when no available guests")
	}
}

// --- Liveness: CheckStuckTasks ---

func TestCheckStuckTasks_RequeuesUnackedTask(t *testing.T) {
	orch := newTestOrchestrator(t)
	_ = orch.RegisterGuest("guest-1", "Test Guest", []string{})

	task := &queue.Task{ID: "task-1", Prompt: "test"}
	_ = orch.AddTask(task)
	_ = orch.AssignTask("task-1", "guest-1")

	// Simulate: task was assigned a long time ago
	task, _ = orch.GetTask("task-1")
	task.AssignedAt = time.Now().Add(-2 * time.Minute)

	// Task is ASSIGNED (not RUNNING = not acked), older than 1 minute
	requeued := orch.CheckStuckTasks(1 * time.Minute)
	if len(requeued) != 1 {
		t.Errorf("expected 1 task requeued, got %d", len(requeued))
	}

	task, _ = orch.GetTask("task-1")
	if task.Status != queue.TaskStatusPending {
		t.Errorf("expected task PENDING after requeue, got %s", task.Status)
	}

	// Guest should be IDLE
	guest, _ := orch.GetGuest("guest-1")
	if guest.State != registry.GuestStateIdle {
		t.Errorf("expected guest IDLE, got %s", guest.State)
	}
}

func TestCheckStuckTasks_SkipsRunningTask(t *testing.T) {
	orch := newTestOrchestrator(t)
	_ = orch.RegisterGuest("guest-1", "Test Guest", []string{})

	task := &queue.Task{ID: "task-1", Prompt: "test"}
	_ = orch.AddTask(task)
	_ = orch.AssignTask("task-1", "guest-1")
	_ = orch.AcknowledgeTask("task-1", "guest-1")

	// Simulate: task was assigned a long time ago (but it's RUNNING = acked)
	task, _ = orch.GetTask("task-1")
	task.AssignedAt = time.Now().Add(-10 * time.Minute)

	requeued := orch.CheckStuckTasks(1 * time.Minute)
	if len(requeued) != 0 {
		t.Errorf("expected no tasks requeued, got %d", len(requeued))
	}

	task, _ = orch.GetTask("task-1")
	if task.Status != queue.TaskStatusRunning {
		t.Errorf("expected task still RUNNING, got %s", task.Status)
	}
}

// --- Liveness: CheckSilentGuests ---

func TestCheckSilentGuests_FailsSilentTask(t *testing.T) {
	orch := newTestOrchestrator(t)
	_ = orch.RegisterGuest("guest-1", "Test Guest", []string{})

	task := &queue.Task{ID: "task-1", Prompt: "test"}
	_ = orch.AddTask(task)
	_ = orch.AssignTask("task-1", "guest-1")
	_ = orch.AcknowledgeTask("task-1", "guest-1")

	// Simulate: guest hasn't heartbeated in a long time
	guest, _ := orch.GetGuest("guest-1")
	guest.LastHeartbeat = time.Now().Add(-10 * time.Minute)

	failed := orch.CheckSilentGuests(1 * time.Minute)
	if len(failed) != 1 {
		t.Errorf("expected 1 task failed, got %d", len(failed))
	}

	task, _ = orch.GetTask("task-1")
	if task.Status != queue.TaskStatusFailed {
		t.Errorf("expected task FAILED, got %s", task.Status)
	}

	guest, _ = orch.GetGuest("guest-1")
	if guest.State != registry.GuestStateIdle {
		t.Errorf("expected guest IDLE, got %s", guest.State)
	}
}

func TestCheckSilentGuests_SkipsTerminalTask(t *testing.T) {
	orch := newTestOrchestrator(t)
	_ = orch.RegisterGuest("guest-1", "Test Guest", []string{})

	task := &queue.Task{ID: "task-1", Prompt: "test"}
	_ = orch.AddTask(task)
	_ = orch.AssignTask("task-1", "guest-1")
	_ = orch.AcknowledgeTask("task-1", "guest-1")
	_ = orch.CompleteTask("task-1", "guest-1", "done")

	// Guest has stale heartbeat (but task is already done)
	guest, _ := orch.GetGuest("guest-1")
	guest.LastHeartbeat = time.Now().Add(-10 * time.Minute)

	failed := orch.CheckSilentGuests(1 * time.Minute)
	if len(failed) != 0 {
		t.Errorf("expected no tasks failed (task already terminal), got %d", len(failed))
	}
}

func TestCheckSilentGuests_SkipsRequeuedTask(t *testing.T) {
	// checkSilentGuests and checkStuckTasks shouldn't race.
	// If a task was re-queued, checkSilentGuests should not try to fail it.
	orch := newTestOrchestrator(t)
	_ = orch.RegisterGuest("guest-1", "Test Guest", []string{})

	task := &queue.Task{ID: "task-1", Prompt: "test"}
	_ = orch.AddTask(task)
	_ = orch.AssignTask("task-1", "guest-1")

	// Simulate: task was re-queued (by checkStuckTasks)
	_ = orch.RequeueTask("task-1")

	// Guest still has stale heartbeat
	guest, _ := orch.GetGuest("guest-1")
	guest.LastHeartbeat = time.Now().Add(-10 * time.Minute)

	failed := orch.CheckSilentGuests(1 * time.Minute)
	if len(failed) != 0 {
		t.Errorf("expected no tasks failed (task already re-queued), got %d", len(failed))
	}
}

// --- DeclineTask ---

func TestDeclineTask(t *testing.T) {
	orch := newTestOrchestrator(t)
	_ = orch.RegisterGuest("guest-1", "Test Guest", []string{})

	task := &queue.Task{ID: "task-1", Prompt: "test"}
	_ = orch.AddTask(task)
	_ = orch.AssignTask("task-1", "guest-1")

	err := orch.DeclineTask("task-1", "guest-1")
	if err != nil {
		t.Fatalf("DeclineTask failed: %v", err)
	}

	// Task should be PENDING again
	task, _ = orch.GetTask("task-1")
	if task.Status != queue.TaskStatusPending {
		t.Errorf("expected task PENDING after decline, got %s", task.Status)
	}
	if task.AssignedTo != "" {
		t.Errorf("expected assigned_to cleared, got %s", task.AssignedTo)
	}

	// Guest should be IDLE
	guest, _ := orch.GetGuest("guest-1")
	if guest.State != registry.GuestStateIdle {
		t.Errorf("expected guest IDLE, got %s", guest.State)
	}
}
