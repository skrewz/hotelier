package queue

import (
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"
)

func newTestQueue(t *testing.T) *TaskQueue {
	t.Helper()
	return NewTaskQueue(func(format string, args ...interface{}) {})
}

func TestNewTaskQueue(t *testing.T) {
	q := newTestQueue(t)
	if q == nil {
		t.Fatal("expected non-nil task queue")
	}
	if q.Count() != 0 {
		t.Errorf("expected 0 tasks, got %d", q.Count())
	}
}

func TestAddTask(t *testing.T) {
	q := newTestQueue(t)

	task := &Task{
		ID:     "task-1",
		Prompt: "Build a feature",
		Tags:   []string{"business-default"},
	}

	err := q.Add(task)
	if err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	if q.Count() != 1 {
		t.Errorf("expected 1 task, got %d", q.Count())
	}

	if task.Status != TaskStatusPending {
		t.Errorf("expected status PENDING, got %s", task.Status)
	}
}

func TestAddDuplicateTask(t *testing.T) {
	q := newTestQueue(t)

	task := &Task{ID: "task-1", Prompt: "Build a feature"}
	if err := q.Add(task); err != nil {
		t.Fatalf("first add failed: %v", err)
	}

	err := q.Add(&Task{ID: "task-1", Prompt: "Duplicate"})
	if err == nil {
		t.Error("expected error for duplicate task ID, got nil")
	}
}

func TestGetTask(t *testing.T) {
	q := newTestQueue(t)

	task := &Task{ID: "task-1", Prompt: "Build a feature"}
	q.Add(task)

	got, ok := q.Get("task-1")
	if !ok {
		t.Fatal("expected task to exist")
	}
	if got.ID != "task-1" {
		t.Errorf("expected id task-1, got %s", got.ID)
	}

	_, ok = q.Get("nonexistent")
	if ok {
		t.Error("expected nonexistent task to not exist")
	}
}

func TestValidStatusTransitions(t *testing.T) {
	q := newTestQueue(t)

	task := &Task{ID: "task-1", Prompt: "Build a feature"}
	q.Add(task)

	// PENDING → ASSIGNED
	err := q.Assign("task-1", "guest-1")
	if err != nil {
		t.Fatalf("assign failed: %v", err)
	}

	// ASSIGNED → RUNNING
	err = q.Start("task-1")
	if err != nil {
		t.Fatalf("start failed: %v", err)
	}

	// RUNNING → COMPLETED
	err = q.Complete("task-1", "done")
	if err != nil {
		t.Fatalf("complete failed: %v", err)
	}

	if task.Status != TaskStatusCompleted {
		t.Errorf("expected COMPLETED, got %s", task.Status)
	}
}

func TestInvalidStatusTransition(t *testing.T) {
	q := newTestQueue(t)

	task := &Task{ID: "task-1", Prompt: "Build a feature"}
	q.Add(task)

	// Try PENDING → COMPLETED (invalid)
	err := q.Complete("task-1", "done")
	if err == nil {
		t.Error("expected error for invalid status transition, got nil")
	}

	// PENDING → ASSIGNED (valid)
	err = q.Assign("task-1", "guest-1")
	if err != nil {
		t.Fatalf("assign should succeed: %v", err)
	}

	// ASSIGNED → COMPLETED is now valid (guest can complete directly)
	err = q.Complete("task-1", "done")
	if err != nil {
		t.Fatalf("ASSIGNED→COMPLETED should be valid: %v", err)
	}
}

func TestAssignTask(t *testing.T) {
	q := newTestQueue(t)

	task := &Task{ID: "task-1", Prompt: "Build a feature", Tags: []string{"tag1"}}
	q.Add(task)

	err := q.Assign("task-1", "guest-1")
	if err != nil {
		t.Fatalf("assign failed: %v", err)
	}

	if task.Status != TaskStatusAssigned {
		t.Errorf("expected status ASSIGNED, got %s", task.Status)
	}
	if task.AssignedTo != "guest-1" {
		t.Errorf("expected assigned_to guest-1, got %s", task.AssignedTo)
	}

	// Try assigning again (should fail)
	err = q.Assign("task-1", "guest-2")
	if err == nil {
		t.Error("expected error for reassigning non-pending task, got nil")
	}
}

func TestAssignNonExistentTask(t *testing.T) {
	q := newTestQueue(t)
	err := q.Assign("nonexistent", "guest-1")
	if err == nil {
		t.Error("expected error for nonexistent task, got nil")
	}
}

func TestStartTask(t *testing.T) {
	q := newTestQueue(t)

	task := &Task{ID: "task-1", Prompt: "Build a feature"}
	q.Add(task)
	q.Assign("task-1", "guest-1")

	err := q.Start("task-1")
	if err != nil {
		t.Fatalf("start failed: %v", err)
	}

	if task.Status != TaskStatusRunning {
		t.Errorf("expected status RUNNING, got %s", task.Status)
	}

	// Try starting again (should fail)
	err = q.Start("task-1")
	if err == nil {
		t.Error("expected error for re-starting running task, got nil")
	}
}

func TestCompleteTask(t *testing.T) {
	q := newTestQueue(t)

	task := &Task{ID: "task-1", Prompt: "Build a feature"}
	q.Add(task)
	q.Assign("task-1", "guest-1")
	q.Start("task-1")

	err := q.Complete("task-1", "all tests pass")
	if err != nil {
		t.Fatalf("complete failed: %v", err)
	}

	if task.Status != TaskStatusCompleted {
		t.Errorf("expected status COMPLETED, got %s", task.Status)
	}
	if task.Result != "all tests pass" {
		t.Errorf("expected result 'all tests pass', got %s", task.Result)
	}

	// Try completing again (should fail)
	err = q.Complete("task-1", "again")
	if err == nil {
		t.Error("expected error for re-completing completed task, got nil")
	}
}

func TestFailTask(t *testing.T) {
	q := newTestQueue(t)

	task := &Task{ID: "task-1", Prompt: "Build a feature"}
	q.Add(task)
	q.Assign("task-1", "guest-1")
	q.Start("task-1")

	err := q.Fail("task-1", "build failed")
	if err != nil {
		t.Fatalf("fail failed: %v", err)
	}

	if task.Status != TaskStatusFailed {
		t.Errorf("expected status FAILED, got %s", task.Status)
	}
	if task.Error != "build failed" {
		t.Errorf("expected error 'build failed', got %s", task.Error)
	}
}

func TestFailAssignedTask(t *testing.T) {
	q := newTestQueue(t)

	task := &Task{ID: "task-1", Prompt: "Build a feature"}
	q.Add(task)
	q.Assign("task-1", "guest-1")

	err := q.Fail("task-1", "guest crashed")
	if err != nil {
		t.Fatalf("fail assigned task failed: %v", err)
	}

	if task.Status != TaskStatusFailed {
		t.Errorf("expected status FAILED, got %s", task.Status)
	}
}

func TestCancelTask(t *testing.T) {
	q := newTestQueue(t)

	task := &Task{ID: "task-1", Prompt: "Build a feature"}
	q.Add(task)

	err := q.Cancel("task-1")
	if err != nil {
		t.Fatalf("cancel failed: %v", err)
	}

	if task.Status != TaskStatusCancelled {
		t.Errorf("expected status CANCELLED, got %s", task.Status)
	}
}

func TestCancelAssignedTask(t *testing.T) {
	q := newTestQueue(t)

	task := &Task{ID: "task-1", Prompt: "Build a feature"}
	q.Add(task)
	q.Assign("task-1", "guest-1")

	err := q.Cancel("task-1")
	if err != nil {
		t.Fatalf("cancel assigned task failed: %v", err)
	}

	if task.Status != TaskStatusCancelled {
		t.Errorf("expected status CANCELLED, got %s", task.Status)
	}
}

func TestCancelRunningTask(t *testing.T) {
	q := newTestQueue(t)

	task := &Task{ID: "task-1", Prompt: "Build a feature"}
	q.Add(task)
	q.Assign("task-1", "guest-1")
	q.Start("task-1")

	// Cancel() only allows PENDING/ASSIGNED → CANCELLED.
	// RUNNING tasks must use UpdateStatus (guest confirms cancellation).
	err := q.Cancel("task-1")
	if err == nil {
		t.Error("expected error for cancelling running task via Cancel(), got nil")
	}

	// But UpdateStatus allows RUNNING → CANCELLED (guest confirmation path)
	err = q.UpdateStatus("task-1", TaskStatusCancelled)
	if err != nil {
		t.Fatalf("UpdateStatus RUNNING→CANCELLED should succeed: %v", err)
	}
	if task.Status != TaskStatusCancelled {
		t.Errorf("expected CANCELLED, got %s", task.Status)
	}
}

func TestGetPendingTasks(t *testing.T) {
	q := newTestQueue(t)

	q.Add(&Task{ID: "task-1", Prompt: "Task 1"})
	q.Add(&Task{ID: "task-2", Prompt: "Task 2"})
	q.Add(&Task{ID: "task-3", Prompt: "Task 3"})

	q.Assign("task-1", "guest-1")
	q.Start("task-1")
	q.Complete("task-1", "done")
	q.Cancel("task-2")

	pending := q.GetPendingTasks()
	if len(pending) != 1 {
		t.Errorf("expected 1 pending task, got %d", len(pending))
	}
	if pending[0].ID != "task-3" {
		t.Errorf("expected task-3, got %s", pending[0].ID)
	}
}

func TestGetTasksByStatus(t *testing.T) {
	q := newTestQueue(t)

	q.Add(&Task{ID: "task-1", Prompt: "Task 1"})
	q.Add(&Task{ID: "task-2", Prompt: "Task 2"})
	q.Add(&Task{ID: "task-3", Prompt: "Task 3"})

	q.Assign("task-1", "guest-1")
	q.Start("task-1")
	q.Complete("task-1", "done")
	q.Cancel("task-2")

	pending := q.GetTasksByStatus(TaskStatusPending)
	if len(pending) != 1 {
		t.Errorf("expected 1 pending, got %d", len(pending))
	}

	assigned := q.GetTasksByStatus(TaskStatusAssigned)
	if len(assigned) != 0 {
		t.Errorf("expected 0 assigned, got %d", len(assigned))
	}

	completed := q.GetTasksByStatus(TaskStatusCompleted)
	if len(completed) != 1 {
		t.Errorf("expected 1 completed, got %d", len(completed))
	}

	cancelled := q.GetTasksByStatus(TaskStatusCancelled)
	if len(cancelled) != 1 {
		t.Errorf("expected 1 cancelled, got %d", len(cancelled))
	}
}

func TestGetAllTasks(t *testing.T) {
	q := newTestQueue(t)

	q.Add(&Task{ID: "task-1", Prompt: "Task 1"})
	q.Add(&Task{ID: "task-2", Prompt: "Task 2"})
	q.Add(&Task{ID: "task-3", Prompt: "Task 3"})

	tasks := q.GetAllTasks()
	if len(tasks) != 3 {
		t.Errorf("expected 3 tasks, got %d", len(tasks))
	}
}

func TestGetGuestTasks(t *testing.T) {
	q := newTestQueue(t)

	q.Add(&Task{ID: "task-1", Prompt: "Task 1"})
	q.Add(&Task{ID: "task-2", Prompt: "Task 2"})
	q.Add(&Task{ID: "task-3", Prompt: "Task 3"})

	q.Assign("task-1", "guest-1")
	q.Assign("task-3", "guest-1")

	tasks := q.GetGuestTasks("guest-1")
	if len(tasks) != 2 {
		t.Errorf("expected 2 tasks for guest-1, got %d", len(tasks))
	}

	tasks = q.GetGuestTasks("guest-2")
	if len(tasks) != 0 {
		t.Errorf("expected 0 tasks for guest-2, got %d", len(tasks))
	}
}

func TestCountByStatus(t *testing.T) {
	q := newTestQueue(t)

	q.Add(&Task{ID: "task-1", Prompt: "Task 1"})
	q.Add(&Task{ID: "task-2", Prompt: "Task 2"})
	q.Add(&Task{ID: "task-3", Prompt: "Task 3"})

	q.Assign("task-1", "guest-1")
	q.Start("task-1")
	q.Complete("task-1", "done")
	q.Cancel("task-2")

	if q.CountByStatus(TaskStatusPending) != 1 {
		t.Errorf("expected 1 pending, got %d", q.CountByStatus(TaskStatusPending))
	}
	if q.CountByStatus(TaskStatusAssigned) != 0 {
		t.Errorf("expected 0 assigned, got %d", q.CountByStatus(TaskStatusAssigned))
	}
	if q.CountByStatus(TaskStatusRunning) != 0 {
		t.Errorf("expected 0 running, got %d", q.CountByStatus(TaskStatusRunning))
	}
	if q.CountByStatus(TaskStatusCompleted) != 1 {
		t.Errorf("expected 1 completed, got %d", q.CountByStatus(TaskStatusCompleted))
	}
	if q.CountByStatus(TaskStatusCancelled) != 1 {
		t.Errorf("expected 1 cancelled, got %d", q.CountByStatus(TaskStatusCancelled))
	}
}

func TestRemoveTask(t *testing.T) {
	q := newTestQueue(t)

	q.Add(&Task{ID: "task-1", Prompt: "Task 1"})
	q.Add(&Task{ID: "task-2", Prompt: "Task 2"})

	err := q.Remove("task-1")
	if err != nil {
		t.Fatalf("remove failed: %v", err)
	}

	if q.Count() != 1 {
		t.Errorf("expected 1 task, got %d", q.Count())
	}

	_, ok := q.Get("task-1")
	if ok {
		t.Error("expected task-1 to be removed")
	}

	err = q.Remove("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent task, got nil")
	}
}

func TestNextPendingTask(t *testing.T) {
	q := newTestQueue(t)

	q.Add(&Task{ID: "task-2", Prompt: "Task 2"})
	q.Add(&Task{ID: "task-1", Prompt: "Task 1"})
	q.Add(&Task{ID: "task-3", Prompt: "Task 3"})

	// First task should be task-2 (FIFO order)
	task := q.NextPendingTask()
	if task == nil {
		t.Fatal("expected a pending task")
	}
	if task.ID != "task-2" {
		t.Errorf("expected task-2 first, got %s", task.ID)
	}

	// Assign first task
	q.Assign("task-2", "guest-1")

	task = q.NextPendingTask()
	if task == nil {
		t.Fatal("expected next pending task")
	}
	if task.ID != "task-1" {
		t.Errorf("expected task-1 next, got %s", task.ID)
	}
}

func TestNextPendingTaskForTags(t *testing.T) {
	q := newTestQueue(t)

	q.Add(&Task{ID: "task-1", Prompt: "Task 1", Tags: []string{"business-default"}})
	q.Add(&Task{ID: "task-2", Prompt: "Task 2", Tags: []string{"android"}})
	q.Add(&Task{ID: "task-3", Prompt: "Task 3", Tags: []string{"business-default", "frontend"}})
	q.Add(&Task{ID: "task-4", Prompt: "Task 4"}) // no tags

	// No tag requirements - should get first pending (task-1)
	task := q.NextPendingTaskForTags([]string{})
	if task == nil {
		t.Fatal("expected a pending task")
	}
	if task.ID != "task-1" {
		t.Errorf("expected task-1, got %s", task.ID)
	}

	// Require android tag - should get task-2
	task = q.NextPendingTaskForTags([]string{"android"})
	if task == nil {
		t.Fatal("expected android task")
	}
	if task.ID != "task-2" {
		t.Errorf("expected task-2, got %s", task.ID)
	}

	// Require business-default - should get task-1 (still pending, matches)
	task = q.NextPendingTaskForTags([]string{"business-default"})
	if task == nil {
		t.Fatal("expected business-default task")
	}
	if task.ID != "task-1" {
		t.Errorf("expected task-1, got %s", task.ID)
	}

	// Assign task-1 so it's no longer pending
	q.Assign("task-1", "guest-1")

	// Require business-default again - should get task-3 now
	task = q.NextPendingTaskForTags([]string{"business-default"})
	if task == nil {
		t.Fatal("expected business-default task")
	}
	if task.ID != "task-3" {
		t.Errorf("expected task-3, got %s", task.ID)
	}

	// Require nonexistent tag - should get task-4 (no tags required)
	task = q.NextPendingTaskForTags([]string{"nonexistent"})
	if task == nil {
		t.Fatal("expected task-4 (no tags)")
	}
	if task.ID != "task-4" {
		t.Errorf("expected task-4, got %s", task.ID)
	}

	// All tasks done
	q.Assign("task-1", "guest-1")
	q.Assign("task-2", "guest-1")
	q.Assign("task-3", "guest-1")
	q.Assign("task-4", "guest-1")

	task = q.NextPendingTaskForTags([]string{})
	if task != nil {
		t.Errorf("expected nil, got %s", task.ID)
	}
}

func TestSetResult(t *testing.T) {
	q := newTestQueue(t)

	task := &Task{ID: "task-1", Prompt: "Task 1"}
	q.Add(task)

	err := q.SetResult("task-1", "result data")
	if err != nil {
		t.Fatalf("set result failed: %v", err)
	}

	if task.Result != "result data" {
		t.Errorf("expected result 'result data', got %s", task.Result)
	}
}

func TestTaskStatusMarshalJSON(t *testing.T) {
	tests := []struct {
		name     string
		status   TaskStatus
		expected string
	}{
		{"Pending", TaskStatusPending, `"PENDING"`},
		{"Assigned", TaskStatusAssigned, `"ASSIGNED"`},
		{"Running", TaskStatusRunning, `"RUNNING"`},
		{"Completed", TaskStatusCompleted, `"COMPLETED"`},
		{"Failed", TaskStatusFailed, `"FAILED"`},
		{"Cancelled", TaskStatusCancelled, `"CANCELLED"`},
		{"Unknown (99)", TaskStatus(99), `"UNKNOWN"`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := json.Marshal(tt.status)
			if err != nil {
				t.Fatalf("MarshalJSON failed: %v", err)
			}
			if string(data) != tt.expected {
				t.Errorf("expected %s, got %s", tt.expected, string(data))
			}
		})
	}
}

func TestTaskStatusUnmarshalJSON(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected TaskStatus
	}{
		{"PENDING", `"PENDING"`, TaskStatusPending},
		{"ASSIGNED", `"ASSIGNED"`, TaskStatusAssigned},
		{"RUNNING", `"RUNNING"`, TaskStatusRunning},
		{"COMPLETED", `"COMPLETED"`, TaskStatusCompleted},
		{"FAILED", `"FAILED"`, TaskStatusFailed},
		{"CANCELLED", `"CANCELLED"`, TaskStatusCancelled},
		{"Unknown string", `"UNKNOWN"`, TaskStatus(0)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var status TaskStatus
			if err := json.Unmarshal([]byte(tt.input), &status); err != nil {
				t.Fatalf("UnmarshalJSON failed: %v", err)
			}
			if status != tt.expected {
				t.Errorf("expected %d, got %d", tt.expected, status)
			}
		})
	}
}

func TestTaskStatusRoundTrip(t *testing.T) {
	statuses := []TaskStatus{
		TaskStatusPending,
		TaskStatusAssigned,
		TaskStatusRunning,
		TaskStatusCompleted,
		TaskStatusFailed,
		TaskStatusCancelled,
	}

	for _, original := range statuses {
		data, err := json.Marshal(original)
		if err != nil {
			t.Fatalf("Marshal failed for %s: %v", original, err)
		}

		var decoded TaskStatus
		if err := json.Unmarshal(data, &decoded); err != nil {
			t.Fatalf("Unmarshal failed for %s: %v", string(data), err)
		}

		if decoded != original {
			t.Errorf("round-trip failed: %s -> %s -> %s", original, string(data), decoded)
		}
	}
}

func TestTaskStatusUnmarshalJSON_Invalid(t *testing.T) {
	var status TaskStatus
	err := json.Unmarshal([]byte(`"INVALID_STATUS"`), &status)
	if err != nil {
		t.Fatalf("expected no error for invalid string, got: %v", err)
	}
	// Unknown strings should be stored as-is (zero value since "UNKNOWN" doesn't match any case)
	if status != TaskStatus(0) {
		t.Errorf("expected status 0 for unknown string, got %d", status)
	}
}

func TestTaskStatusUnmarshalJSON_NonString(t *testing.T) {
	var status TaskStatus
	err := json.Unmarshal([]byte("42"), &status)
	if err == nil {
		t.Error("expected error for non-string JSON value, got nil")
	}
}

func TestTaskStatusString(t *testing.T) {
	tests := []struct {
		status   TaskStatus
		expected string
	}{
		{TaskStatusPending, "PENDING"},
		{TaskStatusAssigned, "ASSIGNED"},
		{TaskStatusRunning, "RUNNING"},
		{TaskStatusCompleted, "COMPLETED"},
		{TaskStatusFailed, "FAILED"},
		{TaskStatusCancelled, "CANCELLED"},
		{TaskStatus(99), "UNKNOWN"},
	}

	for _, tt := range tests {
		if tt.status.String() != tt.expected {
			t.Errorf("TaskStatus(%d).String() = %s, want %s", tt.status, tt.status.String(), tt.expected)
		}
	}
}

func TestTaskCreatedAt(t *testing.T) {
	q := newTestQueue(t)

	before := time.Now()
	task := &Task{ID: "task-1", Prompt: "Task 1"}
	q.Add(task)
	after := time.Now()

	if task.CreatedAt.Before(before) || task.CreatedAt.After(after) {
		t.Errorf("expected created_at between %v and %v, got %v", before, after, task.CreatedAt)
	}
}

func TestQueueConcurrency(t *testing.T) {
	q := newTestQueue(t)
	var wg sync.WaitGroup

	// Concurrent adds
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			q.Add(&Task{
				ID:     fmt.Sprintf("task-%d", id),
				Prompt: fmt.Sprintf("Task %d", id),
				Tags:   []string{"tag"},
			})
		}(i)
	}

	// Concurrent gets
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			q.Get(fmt.Sprintf("task-%d", id))
		}(i)
	}

	// Concurrent status updates
	for i := 0; i < 25; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			q.Assign(fmt.Sprintf("task-%d", id), "guest-1")
		}(i)
	}

	wg.Wait()

	if q.Count() != 50 {
		t.Errorf("expected 50 tasks, got %d", q.Count())
	}
}

// TestTaskStatusTransition_AssignedToPending verifies that a task can be
// re-queued from ASSIGNED back to PENDING (for stuck task recovery).
func TestTaskStatusTransition_AssignedToPending(t *testing.T) {
	q := NewTaskQueue(func(format string, args ...interface{}) {})
	q.Add(&Task{ID: "task-1", Prompt: "test"})

	// PENDING → ASSIGNED
	err := q.Assign("task-1", "guest-1")
	if err != nil {
		t.Fatalf("assign failed: %v", err)
	}

	task, _ := q.Get("task-1")
	if task.Status != TaskStatusAssigned {
		t.Fatalf("expected ASSIGNED, got %s", task.Status)
	}

	// ASSIGNED → PENDING (re-queue)
	err = q.UpdateStatus("task-1", TaskStatusPending)
	if err != nil {
		t.Fatalf("re-queue failed: %v", err)
	}

	task, _ = q.Get("task-1")
	if task.Status != TaskStatusPending {
		t.Errorf("expected PENDING after re-queue, got %s", task.Status)
	}
}

// TestTaskStatusTransition_RunningToPending verifies that a running task
// can be re-queued (for guest unregister recovery).
func TestTaskStatusTransition_RunningToPending(t *testing.T) {
	q := NewTaskQueue(func(format string, args ...interface{}) {})
	q.Add(&Task{ID: "task-1", Prompt: "test"})

	// PENDING → ASSIGNED → RUNNING
	q.Assign("task-1", "guest-1")
	q.Start("task-1")

	task, _ := q.Get("task-1")
	if task.Status != TaskStatusRunning {
		t.Fatalf("expected RUNNING, got %s", task.Status)
	}

	// RUNNING → PENDING (re-queue)
	err := q.UpdateStatus("task-1", TaskStatusPending)
	if err != nil {
		t.Fatalf("re-queue failed: %v", err)
	}

	task, _ = q.Get("task-1")
	if task.Status != TaskStatusPending {
		t.Errorf("expected PENDING after re-queue, got %s", task.Status)
	}
}

// TestValidatePriority verifies that valid and invalid priorities are
// correctly identified.
func TestValidatePriority(t *testing.T) {
	tests := []struct {
		priority string
		valid    bool
	}{
		{PriorityFirefighter, true},
		{PriorityTeacher, true},
		{PriorityOrangutan, true},
		{"", false},
		{"🔥", false},
		{"random", false},
	}

	for _, tt := range tests {
		result := ValidatePriority(tt.priority)
		if result != tt.valid {
			t.Errorf("ValidatePriority(%q) = %v, want %v", tt.priority, result, tt.valid)
		}
	}
}

// TestPriorityValue verifies that priority values are ordered correctly
// (lower value = higher priority).
func TestPriorityValue(t *testing.T) {
	if priorityValue(PriorityFirefighter) >= priorityValue(PriorityTeacher) {
		t.Error("firefighter should have lower (higher) priority value than teacher")
	}
	if priorityValue(PriorityTeacher) >= priorityValue(PriorityOrangutan) {
		t.Error("teacher should have lower (higher) priority value than orangutan")
	}
	if priorityValue("unknown") <= priorityValue(PriorityOrangutan) {
		t.Error("unknown should have lower (lower) priority value than orangutan")
	}
}

// TestAddTask_DefaultPriority verifies that tasks without a priority
// default to orangutan (lowest).
func TestAddTask_DefaultPriority(t *testing.T) {
	q := newTestQueue(t)

	task := &Task{ID: "task-1", Prompt: "Task 1"}
	q.Add(task)

	if task.Priority != PriorityOrangutan {
		t.Errorf("expected default priority %q, got %q", PriorityOrangutan, task.Priority)
	}
}

// TestAddTask_InvalidPriority verifies that invalid priority values
// are normalised to orangutan.
func TestAddTask_InvalidPriority(t *testing.T) {
	q := newTestQueue(t)

	task := &Task{ID: "task-1", Prompt: "Task 1", Priority: "🔥"}
	q.Add(task)

	if task.Priority != PriorityOrangutan {
		t.Errorf("expected invalid priority normalised to %q, got %q", PriorityOrangutan, task.Priority)
	}
}

// TestAddTask_ValidPriority verifies that valid priority values are preserved.
func TestAddTask_ValidPriority(t *testing.T) {
	q := newTestQueue(t)

	for _, priority := range []string{PriorityFirefighter, PriorityTeacher, PriorityOrangutan} {
		task := &Task{ID: "task-priority-" + priority, Prompt: "Task", Priority: priority}
		q.Add(task)

		if task.Priority != priority {
			t.Errorf("expected priority %q, got %q", priority, task.Priority)
		}
	}
}

// TestGetPendingTasks_PriorityOrder verifies that pending tasks are returned
// sorted by priority (highest first), then by creation time (FIFO).
func TestGetPendingTasks_PriorityOrder(t *testing.T) {
	q := newTestQueue(t)

	// Add tasks in reverse priority order
	q.Add(&Task{ID: "task-orangutan", Prompt: "Low", Priority: PriorityOrangutan})
	q.Add(&Task{ID: "task-teacher", Prompt: "Medium", Priority: PriorityTeacher})
	q.Add(&Task{ID: "task-firefighter", Prompt: "High", Priority: PriorityFirefighter})

	pending := q.GetPendingTasks()
	if len(pending) != 3 {
		t.Fatalf("expected 3 pending tasks, got %d", len(pending))
	}

	if pending[0].ID != "task-firefighter" {
		t.Errorf("expected firefighter first, got %s", pending[0].ID)
	}
	if pending[1].ID != "task-teacher" {
		t.Errorf("expected teacher second, got %s", pending[1].ID)
	}
	if pending[2].ID != "task-orangutan" {
		t.Errorf("expected orangutan third, got %s", pending[2].ID)
	}
}

// TestGetPendingTasks_SamePriorityFIFO verifies that tasks with the same
// priority are returned in creation order (FIFO).
func TestGetPendingTasks_SamePriorityFIFO(t *testing.T) {
	q := newTestQueue(t)

	q.Add(&Task{ID: "task-1", Prompt: "First", Priority: PriorityOrangutan})
	q.Add(&Task{ID: "task-2", Prompt: "Second", Priority: PriorityOrangutan})
	q.Add(&Task{ID: "task-3", Prompt: "Third", Priority: PriorityOrangutan})

	pending := q.GetPendingTasks()
	if len(pending) != 3 {
		t.Fatalf("expected 3 pending tasks, got %d", len(pending))
	}

	if pending[0].ID != "task-1" {
		t.Errorf("expected task-1 first, got %s", pending[0].ID)
	}
	if pending[1].ID != "task-2" {
		t.Errorf("expected task-2 second, got %s", pending[1].ID)
	}
	if pending[2].ID != "task-3" {
		t.Errorf("expected task-3 third, got %s", pending[2].ID)
	}
}

// TestNextPendingTask_PriorityOrder verifies that NextPendingTask returns
// the highest-priority pending task.
func TestNextPendingTask_PriorityOrder(t *testing.T) {
	q := newTestQueue(t)

	q.Add(&Task{ID: "task-orangutan", Prompt: "Low", Priority: PriorityOrangutan})
	q.Add(&Task{ID: "task-firefighter", Prompt: "High", Priority: PriorityFirefighter})
	q.Add(&Task{ID: "task-teacher", Prompt: "Medium", Priority: PriorityTeacher})

	task := q.NextPendingTask()
	if task == nil {
		t.Fatal("expected a pending task")
	}
	if task.ID != "task-firefighter" {
		t.Errorf("expected firefighter first, got %s", task.ID)
	}

	// Assign firefighter, next should be teacher
	q.Assign("task-firefighter", "guest-1")
	task = q.NextPendingTask()
	if task == nil {
		t.Fatal("expected next pending task")
	}
	if task.ID != "task-teacher" {
		t.Errorf("expected teacher next, got %s", task.ID)
	}
}

// TestNextPendingTaskForTags_PriorityOrder verifies that tag-filtered
// tasks are also ordered by priority.
func TestNextPendingTaskForTags_PriorityOrder(t *testing.T) {
	q := newTestQueue(t)

	q.Add(&Task{ID: "task-low", Prompt: "Low", Tags: []string{"tag"}, Priority: PriorityOrangutan})
	q.Add(&Task{ID: "task-high", Prompt: "High", Tags: []string{"tag"}, Priority: PriorityFirefighter})
	q.Add(&Task{ID: "task-mid", Prompt: "Mid", Tags: []string{"tag"}, Priority: PriorityTeacher})

	task := q.NextPendingTaskForTags([]string{"tag"})
	if task == nil {
		t.Fatal("expected a matching task")
	}
	if task.ID != "task-high" {
		t.Errorf("expected high priority first, got %s", task.ID)
	}
}

// TestTask_RepoRefField verifies that the RepoRef field is correctly
// stored and retrieved on tasks.
func TestTask_RepoRefField(t *testing.T) {
	q := newTestQueue(t)

	task := &Task{
		ID:      "task-repo",
		Prompt:  "Build a feature",
		RepoRef: "https://github.com/user/repo.git",
	}

	if err := q.Add(task); err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	retrieved, ok := q.Get("task-repo")
	if !ok {
		t.Fatal("task not found")
	}

	if retrieved.RepoRef != "https://github.com/user/repo.git" {
		t.Errorf("expected RepoRef 'https://github.com/user/repo.git', got %q", retrieved.RepoRef)
	}
}

// TestTask_RepoRefEmpty verifies that tasks without RepoRef work correctly.
func TestTask_RepoRefEmpty(t *testing.T) {
	q := newTestQueue(t)

	task := &Task{
		ID:     "task-no-repo",
		Prompt: "Just a prompt",
	}

	if err := q.Add(task); err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	retrieved, ok := q.Get("task-no-repo")
	if !ok {
		t.Fatal("task not found")
	}

	if retrieved.RepoRef != "" {
		t.Errorf("expected empty RepoRef, got %q", retrieved.RepoRef)
	}
}
