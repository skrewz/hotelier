package queue

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// TaskStatus represents the status of a task.
type TaskStatus int

const (
	TaskStatusPending TaskStatus = iota
	TaskStatusAssigned
	TaskStatusRunning
	TaskStatusCompleted
	TaskStatusFailed
	TaskStatusCancelled
)

func (s TaskStatus) String() string {
	switch s {
	case TaskStatusPending:
		return "PENDING"
	case TaskStatusAssigned:
		return "ASSIGNED"
	case TaskStatusRunning:
		return "RUNNING"
	case TaskStatusCompleted:
		return "COMPLETED"
	case TaskStatusFailed:
		return "FAILED"
	case TaskStatusCancelled:
		return "CANCELLED"
	default:
		return "UNKNOWN"
	}
}

// MarshalJSON implements json.Marshaler for TaskStatus.
// It serializes the status as a string (e.g. "PENDING") rather than an int,
// so the frontend can use it directly without type coercion.
func (s TaskStatus) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.String())
}

// UnmarshalJSON implements json.Unmarshaler for TaskStatus.
func (s *TaskStatus) UnmarshalJSON(data []byte) error {
	var str string
	if err := json.Unmarshal(data, &str); err != nil {
		return err
	}
	switch str {
	case "PENDING":
		*s = TaskStatusPending
	case "ASSIGNED":
		*s = TaskStatusAssigned
	case "RUNNING":
		*s = TaskStatusRunning
	case "COMPLETED":
		*s = TaskStatusCompleted
	case "FAILED":
		*s = TaskStatusFailed
	case "CANCELLED":
		*s = TaskStatusCancelled
	default:
		*s = 0 // unknown status, store as zero value
	}
	return nil
}

// Task represents a unit of work to be executed by a guest.
type Task struct {
	ID         string     `json:"id"`
	Prompt     string     `json:"prompt"`
	Tags       []string   `json:"tags"`
	RepoRef    string     `json:"repo_ref,omitempty"` // git repository URL to clone (optional)
	Persona    string     `json:"persona,omitempty"`  // persona name to apply (optional)
	Status     TaskStatus `json:"status"`
	CreatedAt  time.Time  `json:"created_at"`
	AssignedTo string     `json:"assigned_to,omitempty"`
	AssignedAt time.Time  `json:"assigned_at,omitempty"` // when the task was assigned (for liveness probes)
	Timeout    int        `json:"timeout,omitempty"`     // seconds, 0 = unlimited
	Result     string     `json:"result,omitempty"`
	Error      string     `json:"error,omitempty"`
}

// TaskQueue manages pending and active tasks.
type TaskQueue struct {
	tasks   map[string]*Task
	ordered []*Task // maintains insertion order
	mu      sync.RWMutex
	logf    func(format string, args ...interface{})
}

// NewTaskQueue creates a new task queue.
func NewTaskQueue(logf func(format string, args ...interface{})) *TaskQueue {
	return &TaskQueue{
		tasks:   make(map[string]*Task),
		ordered: make([]*Task, 0),
		logf:    logf,
	}
}

// Add adds a new task to the queue.
func (q *TaskQueue) Add(task *Task) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if _, exists := q.tasks[task.ID]; exists {
		return fmt.Errorf("task %s already exists", task.ID)
	}

	task.Status = TaskStatusPending
	task.CreatedAt = time.Now()
	q.tasks[task.ID] = task
	q.ordered = append(q.ordered, task)
	q.logf("task added: %s (status: %s, tags: %v)", task.ID, task.Status, task.Tags)
	return nil
}

// Get returns a task by ID.
func (q *TaskQueue) Get(taskID string) (*Task, bool) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	task, exists := q.tasks[taskID]
	return task, exists
}

// UpdateStatus updates the status of a task.
func (q *TaskQueue) UpdateStatus(taskID string, status TaskStatus) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	task, exists := q.tasks[taskID]
	if !exists {
		return fmt.Errorf("task %s not found", taskID)
	}

	if !q.validTransition(task.Status, status) {
		return fmt.Errorf("invalid status transition: %s -> %s", task.Status, status)
	}

	task.Status = status
	if status == TaskStatusPending {
		task.AssignedTo = ""
	}
	q.logf("task %s status changed: %s -> %s", taskID, task.Status, status)
	return nil
}

// Assign assigns a task to a guest.
func (q *TaskQueue) Assign(taskID, guestID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	task, exists := q.tasks[taskID]
	if !exists {
		return fmt.Errorf("task %s not found", taskID)
	}

	if task.Status != TaskStatusPending {
		return fmt.Errorf("task %s is not pending (status: %s)", taskID, task.Status)
	}

	task.Status = TaskStatusAssigned
	task.AssignedTo = guestID
	task.AssignedAt = time.Now()
	q.logf("task %s assigned to guest %s", taskID, guestID)
	return nil
}

// Start marks a task as running.
func (q *TaskQueue) Start(taskID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	task, exists := q.tasks[taskID]
	if !exists {
		return fmt.Errorf("task %s not found", taskID)
	}

	if task.Status != TaskStatusAssigned {
		return fmt.Errorf("task %s is not assigned (status: %s)", taskID, task.Status)
	}

	task.Status = TaskStatusRunning
	q.logf("task %s started", taskID)
	return nil
}

// Complete marks a task as completed.
func (q *TaskQueue) Complete(taskID, result string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	task, exists := q.tasks[taskID]
	if !exists {
		return fmt.Errorf("task %s not found", taskID)
	}

	if task.Status != TaskStatusRunning && task.Status != TaskStatusAssigned {
		return fmt.Errorf("task %s is not running (status: %s)", taskID, task.Status)
	}

	task.Status = TaskStatusCompleted
	task.Result = result
	q.logf("task %s completed", taskID)
	return nil
}

// Fail marks a task as failed.
func (q *TaskQueue) Fail(taskID, errMsg string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	task, exists := q.tasks[taskID]
	if !exists {
		return fmt.Errorf("task %s not found", taskID)
	}

	if task.Status != TaskStatusRunning && task.Status != TaskStatusAssigned {
		return fmt.Errorf("task %s is not running or assigned (status: %s)", taskID, task.Status)
	}

	task.Status = TaskStatusFailed
	task.Error = errMsg
	q.logf("task %s failed: %s", taskID, errMsg)
	return nil
}

// Cancel marks a task as cancelled.
func (q *TaskQueue) Cancel(taskID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	task, exists := q.tasks[taskID]
	if !exists {
		return fmt.Errorf("task %s not found", taskID)
	}

	if task.Status != TaskStatusPending && task.Status != TaskStatusAssigned {
		return fmt.Errorf("task %s is not pending or assigned (status: %s)", taskID, task.Status)
	}

	task.Status = TaskStatusCancelled
	q.logf("task %s cancelled", taskID)
	return nil
}

// SetResult sets the result of a completed task.
func (q *TaskQueue) SetResult(taskID, result string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	task, exists := q.tasks[taskID]
	if !exists {
		return fmt.Errorf("task %s not found", taskID)
	}

	task.Result = result
	return nil
}

// GetPendingTasks returns all pending tasks.
func (q *TaskQueue) GetPendingTasks() []*Task {
	q.mu.RLock()
	defer q.mu.RUnlock()

	var result []*Task
	for _, task := range q.ordered {
		if task.Status == TaskStatusPending {
			result = append(result, task)
		}
	}
	return result
}

// GetTasksByStatus returns all tasks with the given status.
func (q *TaskQueue) GetTasksByStatus(status TaskStatus) []*Task {
	q.mu.RLock()
	defer q.mu.RUnlock()

	var result []*Task
	for _, task := range q.ordered {
		if task.Status == status {
			result = append(result, task)
		}
	}
	return result
}

// GetAllTasks returns all tasks.
func (q *TaskQueue) GetAllTasks() []*Task {
	q.mu.RLock()
	defer q.mu.RUnlock()

	result := make([]*Task, 0, len(q.tasks))
	for _, task := range q.ordered {
		result = append(result, task)
	}
	return result
}

// GetAssignedGuest returns the guest ID assigned to a task.
func (q *TaskQueue) GetAssignedGuest(taskID string) (string, bool) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	task, exists := q.tasks[taskID]
	if !exists {
		return "", false
	}
	return task.AssignedTo, true
}

// GetGuestTasks returns all tasks assigned to a guest.
func (q *TaskQueue) GetGuestTasks(guestID string) []*Task {
	q.mu.RLock()
	defer q.mu.RUnlock()

	var result []*Task
	for _, task := range q.tasks {
		if task.AssignedTo == guestID {
			result = append(result, task)
		}
	}
	return result
}

// Count returns the total number of tasks.
func (q *TaskQueue) Count() int {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return len(q.tasks)
}

// CountByStatus returns the number of tasks in a given status.
func (q *TaskQueue) CountByStatus(status TaskStatus) int {
	q.mu.RLock()
	defer q.mu.RUnlock()

	count := 0
	for _, task := range q.tasks {
		if task.Status == status {
			count++
		}
	}
	return count
}

// Remove removes a task from the queue.
func (q *TaskQueue) Remove(taskID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if _, exists := q.tasks[taskID]; !exists {
		return fmt.Errorf("task %s not found", taskID)
	}

	delete(q.tasks, taskID)

	// Remove from ordered list
	for i, task := range q.ordered {
		if task.ID == taskID {
			q.ordered = append(q.ordered[:i], q.ordered[i+1:]...)
			break
		}
	}

	q.logf("task removed: %s", taskID)
	return nil
}

// validTransition checks if a status transition is valid.
func (q *TaskQueue) validTransition(from, to TaskStatus) bool {
	validTransitions := map[TaskStatus][]TaskStatus{
		TaskStatusPending:   {TaskStatusAssigned, TaskStatusCancelled},
		TaskStatusAssigned:  {TaskStatusRunning, TaskStatusCancelled, TaskStatusPending},
		TaskStatusRunning:   {TaskStatusCompleted, TaskStatusFailed, TaskStatusCancelled, TaskStatusPending},
		TaskStatusCompleted: {},
		TaskStatusFailed:    {},
		TaskStatusCancelled: {},
	}

	allowed, ok := validTransitions[from]
	if !ok {
		return false
	}

	for _, s := range allowed {
		if s == to {
			return true
		}
	}
	return false
}

// MoveToTop moves a pending task to the front of the queue.
// Only PENDING tasks can be moved. Returns an error if the task
// does not exist or is not in PENDING status.
func (q *TaskQueue) MoveToTop(taskID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	task, exists := q.tasks[taskID]
	if !exists {
		return fmt.Errorf("task %s not found", taskID)
	}

	if task.Status != TaskStatusPending {
		return fmt.Errorf("task %s is not pending (status: %s)", taskID, task.Status)
	}

	// Remove from current position in ordered list
	for i, t := range q.ordered {
		if t.ID == taskID {
			q.ordered = append(q.ordered[:i], q.ordered[i+1:]...)
			break
		}
	}

	// Prepend to ordered list
	q.ordered = append([]*Task{task}, q.ordered...)
	q.logf("task %s moved to top of queue", taskID)
	return nil
}

// NextPendingTask returns the next pending task (FIFO).
func (q *TaskQueue) NextPendingTask() *Task {
	q.mu.RLock()
	defer q.mu.RUnlock()

	for _, task := range q.ordered {
		if task.Status == TaskStatusPending {
			return task
		}
	}
	return nil
}

// NextPendingTaskForTags returns the next pending task that matches the required tags.
func (q *TaskQueue) NextPendingTaskForTags(requiredTags []string) *Task {
	q.mu.RLock()
	defer q.mu.RUnlock()

	for _, task := range q.ordered {
		if task.Status != TaskStatusPending {
			continue
		}
		if !q.matchesTags(task.Tags, requiredTags) {
			continue
		}
		return task
	}
	return nil
}

// matchesTags checks if the task tags match all required tags.
// A task with no tags matches any required tags (no requirements).
func (q *TaskQueue) matchesTags(taskTags, requiredTags []string) bool {
	if len(taskTags) == 0 {
		return true
	}

	tagSet := make(map[string]struct{}, len(taskTags))
	for _, tag := range taskTags {
		tagSet[tag] = struct{}{}
	}

	for _, req := range requiredTags {
		if _, ok := tagSet[req]; !ok {
			return false
		}
	}
	return true
}
