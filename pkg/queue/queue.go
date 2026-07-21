package queue

import (
	"encoding/json"
	"fmt"
	"sort"
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

// Priority levels — derived from Futurama "The Problem with Popplers".
// Higher priority tasks are served first from the pending queue.
//
// API values are plain English strings. The UI maps these to emoji:
//   - firefighter → 🧑‍🚒
//   - teacher → 🧑‍🏫
//   - orangutan → 🦧
//
// Reference: https://futurama.fandom.com/wiki/The_Problem_with_Popplers/Transcript
const (
	// PriorityFirefighter is the highest priority level. Display: 🧑‍🚒
	PriorityFirefighter = "firefighter"

	// PriorityTeacher is the medium priority level. Display: 🧑‍🏫
	PriorityTeacher = "teacher"

	// PriorityOrangutan is the lowest (default) priority level. Display: 🦧
	PriorityOrangutan = "orangutan"
)

// validPriorities lists all accepted priority values in descending order
// (highest priority first).
var validPriorities = []string{PriorityFirefighter, PriorityTeacher, PriorityOrangutan}

// priorityValue returns a numeric sort key for a priority string.
// Lower values = higher priority (sorted ascending).
func priorityValue(priority string) int {
	for i, p := range validPriorities {
		if p == priority {
			return i
		}
	}
	// Unknown priorities sort after all known ones (lowest priority).
	return len(validPriorities)
}

// ValidatePriority returns true if the given priority string is one of the
// recognised levels.
func ValidatePriority(priority string) bool {
	for _, p := range validPriorities {
		if p == priority {
			return true
		}
	}
	return false
}

// Task represents a unit of work to be executed by a guest.
type Task struct {
	ID         string     `json:"id"`
	Prompt     string     `json:"prompt"`
	Tags       []string   `json:"tags"`
	Persona    string     `json:"persona,omitempty"`  // persona name to apply (optional)
	Priority   string     `json:"priority,omitempty"` // priority level: firefighter, teacher, or orangutan (default)
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
	// Default priority to orangutan if not set or invalid
	if task.Priority == "" || !ValidatePriority(task.Priority) {
		task.Priority = PriorityOrangutan
	}
	q.tasks[task.ID] = task
	q.ordered = append(q.ordered, task)
	q.logf("task added: %s (status: %s, tags: %v, priority: %s)", task.ID, task.Status, task.Tags, task.Priority)
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

// GetPendingTasks returns all pending tasks, sorted by priority
// (highest first) and then by creation time (FIFO within same priority).
func (q *TaskQueue) GetPendingTasks() []*Task {
	q.mu.RLock()
	defer q.mu.RUnlock()

	var result []*Task
	for _, task := range q.ordered {
		if task.Status == TaskStatusPending {
			result = append(result, task)
		}
	}
	return q.sortPendingByPriority(result)
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

// sortPendingByPriority sorts pending tasks by priority (highest first),
// then by creation time (FIFO within same priority). The caller must hold
// at least q.mu.RLock.
func (q *TaskQueue) sortPendingByPriority(tasks []*Task) []*Task {
	sort.SliceStable(tasks, func(i, j int) bool {
		vi, vj := priorityValue(tasks[i].Priority), priorityValue(tasks[j].Priority)
		if vi != vj {
			return vi < vj // lower value = higher priority
		}
		return tasks[i].CreatedAt.Before(tasks[j].CreatedAt)
	})
	return tasks
}

// NextPendingTask returns the next pending task, ordered by priority
// (highest first) and then by creation time (FIFO within same priority).
func (q *TaskQueue) NextPendingTask() *Task {
	q.mu.RLock()
	defer q.mu.RUnlock()

	var pending []*Task
	for _, task := range q.ordered {
		if task.Status == TaskStatusPending {
			pending = append(pending, task)
		}
	}
	if len(pending) == 0 {
		return nil
	}
	q.sortPendingByPriority(pending)
	return pending[0]
}

// NextPendingTaskForTags returns the next pending task that matches the
// required tags, ordered by priority (highest first) and then by creation
// time (FIFO within same priority).
func (q *TaskQueue) NextPendingTaskForTags(requiredTags []string) *Task {
	q.mu.RLock()
	defer q.mu.RUnlock()

	var pending []*Task
	for _, task := range q.ordered {
		if task.Status != TaskStatusPending {
			continue
		}
		if !q.matchesTags(task.Tags, requiredTags) {
			continue
		}
		pending = append(pending, task)
	}
	if len(pending) == 0 {
		return nil
	}
	q.sortPendingByPriority(pending)
	return pending[0]
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
