package orchestrator

import (
	"fmt"
	"sync"
	"time"

	"hotelier/pkg/queue"
	"hotelier/pkg/registry"
)

// Orchestrator is the single source of truth for task/guest lifecycle state.
//
// It wraps the TaskQueue and GuestRegistry with a single mutex so that all
// lifecycle mutations (assign, complete, fail, cancel, requeue) update both
// stores atomically. Task status is the authoritative source; guest state is
// derived from it.
//
// The orchestrator does NOT replace the queue and registry — it coordinates
// them. Read-only access to individual stores is still available via the
// delegation methods.
type Orchestrator struct {
	queue    *queue.TaskQueue
	registry *registry.GuestRegistry
	mu       sync.Mutex // single lock for all compound mutations
	logf     func(format string, args ...interface{})
}

// New creates a new Orchestrator wrapping the given queue and registry.
func New(logf func(format string, args ...interface{})) *Orchestrator {
	q := queue.NewTaskQueue(logf)
	r := registry.NewGuestRegistry(0, logf) // maxGuests set via SetMaxGuests
	return &Orchestrator{
		queue:    q,
		registry: r,
		logf:     logf,
	}
}

// NewWithExisting creates a new Orchestrator wrapping existing queue and registry.
// Useful for testing and migration (where queue/registry are created separately).
func NewWithExisting(q *queue.TaskQueue, r *registry.GuestRegistry, logf func(format string, args ...interface{})) *Orchestrator {
	return &Orchestrator{
		queue:    q,
		registry: r,
		logf:     logf,
	}
}

// --- Guest Lifecycle ---

// RegisterGuest registers a new guest. The guest starts in IDLE state.
func (o *Orchestrator) RegisterGuest(id, name string, tags []string) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	_, err := o.registry.Register(id, name, tags)
	if err != nil {
		return err
	}
	o.logf("guest registered: %s (name: %s, tags: %v)", id, name, tags)
	return nil
}

// UnregisterGuest unregisters a guest. Fails if the guest is RUNNING.
func (o *Orchestrator) UnregisterGuest(id string) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	return o.registry.Unregister(id)
}

// UnregisterGuestForce unregisters a guest even if it is RUNNING.
// If the guest has a task, the task is re-queued to PENDING.
func (o *Orchestrator) UnregisterGuestForce(id string) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	guest, ok := o.registry.GetGuest(id)
	if !ok {
		return fmt.Errorf("guest %s not found", id)
	}

	// If guest is running a task, re-queue it atomically.
	if guest.State == registry.GuestStateRunning && guest.TaskID != "" {
		o.requeueTaskInternal(guest.TaskID)
	}

	// Force-clear guest state so it's consistent.
	o.clearGuestTaskInternal(id)

	// Remove the guest from the registry. We do this inline because
	// registry.Unregister rejects RUNNING guests.
	o.registry.RemoveGuest(id)

	o.logf("guest force-unregistered: %s", id)
	return nil
}

// --- Task Lifecycle ---

// AddTask adds a new task to the queue in PENDING state.
func (o *Orchestrator) AddTask(task *queue.Task) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	return o.queue.Add(task)
}

// GetTask returns a task by ID.
func (o *Orchestrator) GetTask(taskID string) (*queue.Task, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()

	return o.queue.Get(taskID)
}

// GetGuest returns a guest by ID.
func (o *Orchestrator) GetGuest(guestID string) (*registry.Guest, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()

	return o.registry.GetGuest(guestID)
}

// AssignTask atomically assigns a pending task to an idle guest.
//
// Task: PENDING → ASSIGNED (with AssignedTo, AssignedAt)
// Guest: IDLE → RUNNING (with TaskID)
func (o *Orchestrator) AssignTask(taskID, guestID string) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	// Validate guest is IDLE
	guest, ok := o.registry.GetGuest(guestID)
	if !ok {
		return fmt.Errorf("guest %s not found", guestID)
	}
	if guest.State != registry.GuestStateIdle {
		return fmt.Errorf("guest %s is not idle (state: %s)", guestID, guest.State)
	}

	// Validate task is PENDING
	task, ok := o.queue.Get(taskID)
	if !ok {
		return fmt.Errorf("task %s not found", taskID)
	}
	if task.Status != queue.TaskStatusPending {
		return fmt.Errorf("task %s is not pending (status: %s)", taskID, task.Status)
	}

	// Atomic update: task + guest
	task.Status = queue.TaskStatusAssigned
	task.AssignedTo = guestID
	task.AssignedAt = time.Now()

	guest.TaskID = taskID
	guest.State = registry.GuestStateRunning
	guest.LastTaskHeartbeat = time.Time{} // reset — guest must confirm via heartbeat

	o.logf("task %s assigned to guest %s (atomic: PENDING→ASSIGNED, IDLE→RUNNING)", taskID, guestID)
	return nil
}

// AcknowledgeTask transitions an ASSIGNED task to RUNNING.
//
// This is the explicit handshake: the guest confirms it received the
// assignment and is executing the task.
//
// Task: ASSIGNED → RUNNING
// (Guest is already RUNNING from AssignTask)
//
// Idempotent: if task is already RUNNING, returns nil.
func (o *Orchestrator) AcknowledgeTask(taskID, guestID string) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	task, ok := o.queue.Get(taskID)
	if !ok {
		return fmt.Errorf("task %s not found", taskID)
	}

	// Idempotent: already RUNNING
	if task.Status == queue.TaskStatusRunning {
		return nil
	}

	if task.Status != queue.TaskStatusAssigned {
		return fmt.Errorf("task %s is not assigned (status: %s)", taskID, task.Status)
	}
	if task.AssignedTo != guestID {
		return fmt.Errorf("task %s is assigned to %s, not %s", taskID, task.AssignedTo, guestID)
	}

	task.Status = queue.TaskStatusRunning
	o.logf("task %s acknowledged by guest %s (ASSIGNED→RUNNING)", taskID, guestID)
	return nil
}

// CompleteTask atomically completes a task and clears the guest.
//
// Task: RUNNING|ASSIGNED → COMPLETED
// Guest: RUNNING → IDLE (TaskID cleared)
//
// Idempotent: if task is already terminal, returns nil.
func (o *Orchestrator) CompleteTask(taskID, guestID, result string) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	task, ok := o.queue.Get(taskID)
	if !ok {
		return fmt.Errorf("task %s not found", taskID)
	}

	// Idempotent: already terminal
	if task.Status == queue.TaskStatusCompleted || task.Status == queue.TaskStatusFailed || task.Status == queue.TaskStatusCancelled {
		return nil
	}

	if task.Status != queue.TaskStatusRunning && task.Status != queue.TaskStatusAssigned {
		return fmt.Errorf("task %s is not running or assigned (status: %s)", taskID, task.Status)
	}

	task.Status = queue.TaskStatusCompleted
	task.Result = result

	o.clearGuestTaskInternal(guestID)

	o.logf("task %s completed by guest %s (atomic: →COMPLETED, RUNNING→IDLE)", taskID, guestID)
	return nil
}

// FailTask atomically fails a task and clears the guest.
//
// Task: RUNNING|ASSIGNED → FAILED
// Guest: RUNNING → IDLE (TaskID cleared)
//
// Idempotent: if task is already terminal, returns nil.
func (o *Orchestrator) FailTask(taskID, guestID, errMsg string) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	task, ok := o.queue.Get(taskID)
	if !ok {
		return fmt.Errorf("task %s not found", taskID)
	}

	// Idempotent: already terminal
	if task.Status == queue.TaskStatusCompleted || task.Status == queue.TaskStatusFailed || task.Status == queue.TaskStatusCancelled {
		return nil
	}

	if task.Status != queue.TaskStatusRunning && task.Status != queue.TaskStatusAssigned {
		return fmt.Errorf("task %s is not running or assigned (status: %s)", taskID, task.Status)
	}

	task.Status = queue.TaskStatusFailed
	task.Error = errMsg

	o.clearGuestTaskInternal(guestID)

	o.logf("task %s failed by guest %s (atomic: →FAILED, RUNNING→IDLE): %s", taskID, guestID, errMsg)
	return nil
}

// CancelTask atomically cancels a task and clears the guest.
//
// Task: PENDING|ASSIGNED|RUNNING → CANCELLED
// Guest: RUNNING → IDLE (TaskID cleared, if guest provided)
//
// Idempotent: if task is already terminal, returns nil.
func (o *Orchestrator) CancelTask(taskID, guestID string) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	task, ok := o.queue.Get(taskID)
	if !ok {
		return fmt.Errorf("task %s not found", taskID)
	}

	// Idempotent: already terminal
	if task.Status == queue.TaskStatusCompleted || task.Status == queue.TaskStatusFailed || task.Status == queue.TaskStatusCancelled {
		return nil
	}

	if task.Status != queue.TaskStatusPending && task.Status != queue.TaskStatusAssigned && task.Status != queue.TaskStatusRunning {
		return fmt.Errorf("task %s cannot be cancelled (status: %s)", taskID, task.Status)
	}

	task.Status = queue.TaskStatusCancelled
	task.AssignedTo = ""

	if guestID != "" {
		o.clearGuestTaskInternal(guestID)
	}

	o.logf("task %s cancelled (atomic: →CANCELLED%s)", taskID,
		func() string {
			if guestID != "" {
				return ", RUNNING→IDLE"
			}
			return ""
		}())
	return nil
}

// RequeueTask atomically re-queues a task and clears the guest.
//
// Task: ASSIGNED|RUNNING → PENDING (AssignedTo cleared)
// Guest: RUNNING → IDLE (TaskID cleared)
func (o *Orchestrator) RequeueTask(taskID string) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	task, ok := o.queue.Get(taskID)
	if !ok {
		return fmt.Errorf("task %s not found", taskID)
	}

	if task.Status == queue.TaskStatusPending {
		// Already pending — nothing to do
		return nil
	}
	if task.Status == queue.TaskStatusCompleted || task.Status == queue.TaskStatusFailed || task.Status == queue.TaskStatusCancelled {
		return fmt.Errorf("task %s is terminal (status: %s), cannot re-queue", taskID, task.Status)
	}

	// Clear the guest before re-queuing (guest might be needed for logging)
	guestID := task.AssignedTo

	o.requeueTaskInternal(taskID)
	if guestID != "" {
		o.clearGuestTaskInternal(guestID)
	}

	return nil
}

// DeclineTask handles a guest declining an assigned task.
//
// Task: ASSIGNED → PENDING (AssignedTo cleared)
//
//	RUNNING → PENDING (AssignedTo cleared) — when guest acknowledged
//	  before ExecuteTask failed
//
// Guest: RUNNING → IDLE (TaskID cleared)
func (o *Orchestrator) DeclineTask(taskID, guestID string) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	task, ok := o.queue.Get(taskID)
	if !ok {
		return fmt.Errorf("task %s not found", taskID)
	}

	if task.AssignedTo != guestID {
		return fmt.Errorf("task %s is assigned to %s, not %s", taskID, task.AssignedTo, guestID)
	}

	switch task.Status {
	case queue.TaskStatusAssigned:
		o.requeueTaskInternal(taskID)
	case queue.TaskStatusRunning:
		// Guest acknowledged (RUNNING) but ExecuteTask failed and
		// guest declined. Transition back to PENDING so it can be
		// reassigned to another guest.
		o.requeueTaskInternal(taskID)
	default:
		return fmt.Errorf("task %s is not assignable (status: %s)", taskID, task.Status)
	}

	o.clearGuestTaskInternal(guestID)

	statusFrom := "ASSIGNED"
	if task.Status == queue.TaskStatusRunning {
		statusFrom = "RUNNING"
	}
	o.logf("task %s declined by guest %s (%s→PENDING, RUNNING→IDLE)", taskID, guestID, statusFrom)
	return nil
}

// TryAssignNext finds the next pending task and assigns it to an available
// guest. Returns true if a task was assigned.
func (o *Orchestrator) TryAssignNext() bool {
	o.mu.Lock()
	defer o.mu.Unlock()

	task := o.queue.NextPendingTask()
	if task == nil {
		return false
	}

	guests := o.registry.FindAvailableGuests(task.Tags)
	if len(guests) == 0 {
		return false
	}

	guest := guests[0]

	// Atomic assign (internal, lock already held)
	task.Status = queue.TaskStatusAssigned
	task.AssignedTo = guest.ID
	task.AssignedAt = time.Now()

	guest.TaskID = task.ID
	guest.State = registry.GuestStateRunning
	guest.LastTaskHeartbeat = time.Time{}

	o.logf("task %s auto-assigned to guest %s", task.ID, guest.ID)
	return true
}

// --- Read-Only Delegation ---

// GetPendingTasks returns all pending tasks.
func (o *Orchestrator) GetPendingTasks() []*queue.Task {
	o.mu.Lock()
	defer o.mu.Unlock()

	return o.queue.GetPendingTasks()
}

// GetTasksByStatus returns all tasks with the given status.
func (o *Orchestrator) GetTasksByStatus(status queue.TaskStatus) []*queue.Task {
	o.mu.Lock()
	defer o.mu.Unlock()

	return o.queue.GetTasksByStatus(status)
}

// GetAllTasks returns all tasks.
func (o *Orchestrator) GetAllTasks() []*queue.Task {
	o.mu.Lock()
	defer o.mu.Unlock()

	return o.queue.GetAllTasks()
}

// GetAllGuests returns all guests.
func (o *Orchestrator) GetAllGuests() []*registry.Guest {
	o.mu.Lock()
	defer o.mu.Unlock()

	return o.registry.GetAllGuests()
}

// FindAvailableGuests returns guests that can handle tasks with the given tags.
func (o *Orchestrator) FindAvailableGuests(requiredTags []string) []*registry.Guest {
	o.mu.Lock()
	defer o.mu.Unlock()

	return o.registry.FindAvailableGuests(requiredTags)
}

// Count returns the total number of tasks.
func (o *Orchestrator) Count() int {
	o.mu.Lock()
	defer o.mu.Unlock()

	return o.queue.Count()
}

// GuestCount returns the total number of registered guests.
func (o *Orchestrator) GuestCount() int {
	o.mu.Lock()
	defer o.mu.Unlock()

	return o.registry.Count()
}

// --- Heartbeat ---

// Heartbeat updates the last heartbeat time for a guest.
func (o *Orchestrator) Heartbeat(guestID string) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	return o.registry.Heartbeat(guestID)
}

// TaskHeartbeat updates the last heartbeat time and records the task_id.
func (o *Orchestrator) TaskHeartbeat(guestID, taskID string) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	return o.registry.TaskHeartbeat(guestID, taskID)
}

// --- Reconciliation ---

// ReconcileState represents the authoritative state after reconciliation.
type ReconcileState struct {
	Status queue.TaskStatus
	TaskID string
}

// ReconcileGuest checks the guest's task against the authoritative task queue
// and adjusts guest state accordingly. Used on guest reconnection.
//
// Returns the authoritative state so the server can notify the guest.
func (o *Orchestrator) ReconcileGuest(guestID string) ReconcileState {
	o.mu.Lock()
	defer o.mu.Unlock()

	guest, ok := o.registry.GetGuest(guestID)
	if !ok {
		return ReconcileState{Status: queue.TaskStatusPending}
	}

	if guest.TaskID == "" {
		// Guest has no task — state is consistent.
		guest.State = registry.GuestStateIdle
		return ReconcileState{Status: queue.TaskStatusPending}
	}

	task, ok := o.queue.Get(guest.TaskID)
	if !ok {
		// Task not found — stale reference.
		o.clearGuestTaskInternal(guestID)
		o.logf("guest %s reconciled: task %s not found, set IDLE", guestID, guest.TaskID)
		return ReconcileState{Status: queue.TaskStatusPending}
	}

	switch task.Status {
	case queue.TaskStatusRunning, queue.TaskStatusAssigned:
		// Task is active and assigned to this guest — consistent.
		if task.AssignedTo == guestID {
			guest.State = registry.GuestStateRunning
			guest.TaskID = task.ID
			return ReconcileState{Status: task.Status, TaskID: task.ID}
		}
		// Task assigned to another guest — guest was stolen.
		o.clearGuestTaskInternal(guestID)
		o.logf("guest %s reconciled: task %s assigned to %s, set IDLE", guestID, task.ID, task.AssignedTo)
		return ReconcileState{Status: queue.TaskStatusPending}

	case queue.TaskStatusPending:
		// Task was re-queued — guest's assignment was revoked.
		o.clearGuestTaskInternal(guestID)
		o.logf("guest %s reconciled: task %s re-queued, set IDLE", guestID, task.ID)
		return ReconcileState{Status: queue.TaskStatusPending}

	case queue.TaskStatusCompleted, queue.TaskStatusFailed, queue.TaskStatusCancelled:
		// Task is terminal — guest's work is done.
		o.clearGuestTaskInternal(guestID)
		o.logf("guest %s reconciled: task %s terminal (%s), set IDLE", guestID, task.ID, task.Status)
		return ReconcileState{Status: task.Status, TaskID: task.ID}
	}

	return ReconcileState{Status: queue.TaskStatusPending}
}

// --- Liveness Probes ---

// StuckTask represents a task that was re-queued by CheckStuckTasks.
type StuckTask struct {
	TaskID  string
	GuestID string
}

// CheckStuckTasks finds tasks that are ASSIGNED (not acknowledged as RUNNING)
// and older than the timeout. Re-queues them atomically.
// Also clears stale guest references for guests still pointing to terminal/PENDING tasks.
//
// Returns the list of re-queued tasks.
func (o *Orchestrator) CheckStuckTasks(timeout time.Duration) []StuckTask {
	o.mu.Lock()
	defer o.mu.Unlock()

	var requeued []StuckTask
	now := time.Now()

	for _, task := range o.queue.GetAllTasks() {
		if task.Status == queue.TaskStatusAssigned {
			if now.Sub(task.AssignedAt) <= timeout {
				continue // Not yet stuck
			}

			guestID := task.AssignedTo

			// Check if the guest is truly stuck or just transitioning.
			// A guest that has heartbeated with the task ID recently is
			// actively working — not stuck. A guest that previously
			// acknowledged (LastTaskHeartbeat set) but it's now stale may
			// have just finished the task and is about to clear its
			// internal reference. In that case, check the general heartbeat:
			// if the guest is alive, give it a grace period.
			// If the guest never acknowledged (LastTaskHeartbeat is zero),
			// it never received the assignment — true stuck task.
			if guest, ok := o.registry.GetGuest(guestID); ok {
				if now.Sub(guest.LastTaskHeartbeat) <= timeout {
					continue // Guest is actively working, not stuck
				}
				// Task-specific heartbeat is stale or zero.
				if !guest.LastTaskHeartbeat.IsZero() &&
					now.Sub(guest.LastHeartbeat) <= timeout {
					// Guest previously acknowledged but just finished.
					// Give it time to send a final heartbeat before re-queueing.
					continue
				}
			}

			requeued = append(requeued, StuckTask{TaskID: task.ID, GuestID: guestID})

			o.requeueTaskInternal(task.ID)
			if guestID != "" {
				o.clearGuestTaskInternal(guestID)
			}

			dur := now.Sub(task.AssignedAt)
			if dur < 0 {
				dur = 0
			}
			o.logf("stuck task %s re-queued (was ASSIGNED to %s for %v)", task.ID, guestID, dur)
			continue
		}

		// For terminal tasks, check if the assigned guest still has a stale reference.
		if task.AssignedTo != "" && o.isTerminalStatus(task.Status) {
			if guest, ok := o.registry.GetGuest(task.AssignedTo); ok && guest.TaskID == task.ID {
				o.clearGuestTaskInternal(guest.ID)
				o.logf("cleared stale guest reference: %s still pointed to terminal task %s (%s)",
					guest.ID, task.ID, task.Status)
			}
		}
	}

	// Also check guests that have a stale TaskID pointing to a task that
	// is no longer assigned to them (e.g. task was cancelled while PENDING).
	for _, guest := range o.registry.GetAllGuests() {
		if guest.TaskID == "" {
			continue
		}
		task, ok := o.queue.Get(guest.TaskID)
		if !ok {
			continue
		}
		// Guest has a TaskID but the task is not assigned to this guest.
		// Clear the stale reference.
		if task.AssignedTo != guest.ID {
			o.clearGuestTaskInternal(guest.ID)
			o.logf("cleared stale guest reference: %s pointed to task %s (assigned to %s)",
				guest.ID, task.ID, task.AssignedTo)
		}
	}

	return requeued
}

// SilentGuest represents a guest whose task was failed by CheckSilentGuests.
type SilentGuest struct {
	GuestID string
	TaskID  string
}

// StaleGuest represents a guest that was removed for being stale.
type StaleGuest struct {
	GuestID        string
	TaskID         string // task that was failed, if any
	TaskWasRunning bool
}

// CheckSilentGuests finds guests that are RUNNING but haven't heartbeated
// within the timeout. Fails their tasks atomically.
//
// Skips tasks that are already terminal or PENDING (re-queued by
// CheckStuckTasks). This prevents racing with CheckStuckTasks.
//
// Returns the list of failed tasks.
func (o *Orchestrator) CheckSilentGuests(timeout time.Duration) []SilentGuest {
	o.mu.Lock()
	defer o.mu.Unlock()

	var failed []SilentGuest
	now := time.Now()

	for _, guest := range o.registry.GetAllGuests() {
		if guest.TaskID == "" {
			continue // Not running a task
		}
		if now.Sub(guest.LastHeartbeat) <= timeout {
			continue // Not yet silent
		}

		task, ok := o.queue.Get(guest.TaskID)
		if !ok {
			// Task not found — stale reference, just clear guest.
			o.clearGuestTaskInternal(guest.ID)
			continue
		}

		// Skip terminal tasks (already completed/failed/cancelled)
		if task.Status == queue.TaskStatusCompleted || task.Status == queue.TaskStatusFailed || task.Status == queue.TaskStatusCancelled {
			o.clearGuestTaskInternal(guest.ID)
			continue
		}

		// Skip re-queued tasks (CheckStuckTasks already handled this)
		if task.Status == queue.TaskStatusPending {
			o.clearGuestTaskInternal(guest.ID)
			continue
		}

		// Task is RUNNING or ASSIGNED — fail it.
		task.Status = queue.TaskStatusFailed
		task.Error = fmt.Sprintf("guest %s silent for %v", guest.ID, now.Sub(guest.LastHeartbeat))

		o.clearGuestTaskInternal(guest.ID)
		failed = append(failed, SilentGuest{GuestID: guest.ID, TaskID: task.ID})

		o.logf("silent guest %s: task %s failed (silent for %v)", guest.ID, task.ID, now.Sub(guest.LastHeartbeat))
	}

	return failed
}

// RemoveStaleGuests removes guests that haven't heartbeated within the timeout.
// If a stale guest had a running task, the task is failed atomically before
// the guest is removed.
//
// Returns the list of removed guests and their failed tasks.
func (o *Orchestrator) RemoveStaleGuests(timeout time.Duration) []StaleGuest {
	o.mu.Lock()
	defer o.mu.Unlock()

	var removed []StaleGuest
	now := time.Now()

	// Collect stale guests first (can't delete during iteration)
	var staleIDs []string
	for _, guest := range o.registry.GetAllGuests() {
		if now.Sub(guest.LastHeartbeat) > timeout {
			staleIDs = append(staleIDs, guest.ID)
		}
	}

	for _, guestID := range staleIDs {
		guest, ok := o.registry.GetGuest(guestID)
		if !ok {
			continue
		}

		taskWasRunning := false
		taskID := ""

		// If the guest had a running task, fail it before removing the guest.
		if guest.TaskID != "" && guest.State == registry.GuestStateRunning {
			task, ok := o.queue.Get(guest.TaskID)
			if ok && !o.isTerminalStatus(task.Status) && task.Status != queue.TaskStatusPending {
				task.Status = queue.TaskStatusFailed
				task.Error = fmt.Sprintf("guest %s stale (no heartbeat for %v)", guest.ID, now.Sub(guest.LastHeartbeat))
				taskID = task.ID
				taskWasRunning = true
				o.logf("stale guest %s: task %s failed (no heartbeat for %v)",
					guest.ID, task.ID, now.Sub(guest.LastHeartbeat))
			}
		}

		// Remove the guest
		o.registry.RemoveGuest(guestID)
		removed = append(removed, StaleGuest{
			GuestID:        guest.ID,
			TaskID:         taskID,
			TaskWasRunning: taskWasRunning,
		})
		o.logf("stale guest removed: %s", guest.ID)
	}

	return removed
}

// --- Internal helpers (caller must hold o.mu) ---

// isTerminalStatus returns true if the task status is terminal
// (COMPLETED, FAILED, or CANCELLED).
func (o *Orchestrator) isTerminalStatus(status queue.TaskStatus) bool {
	return status == queue.TaskStatusCompleted ||
		status == queue.TaskStatusFailed ||
		status == queue.TaskStatusCancelled
}

func (o *Orchestrator) requeueTaskInternal(taskID string) {
	task, ok := o.queue.Get(taskID)
	if !ok {
		return
	}

	task.Status = queue.TaskStatusPending
	task.AssignedTo = ""
	task.AssignedAt = time.Time{}
}

func (o *Orchestrator) clearGuestTaskInternal(guestID string) {
	guest, ok := o.registry.GetGuest(guestID)
	if !ok {
		return
	}

	guest.TaskID = ""
	guest.State = registry.GuestStateIdle
	guest.LastTaskHeartbeat = time.Time{}
}

// --- Accessors for internal components (testing, migration) ---

// Queue returns the underlying TaskQueue. Use only for read-only access
// or when migrating existing code.
func (o *Orchestrator) Queue() *queue.TaskQueue {
	return o.queue
}

// Registry returns the underlying GuestRegistry. Use only for read-only
// access or when migrating existing code.
func (o *Orchestrator) Registry() *registry.GuestRegistry {
	return o.registry
}

// SetMaxGuests updates the maximum number of guests allowed (0 = unlimited).
func (o *Orchestrator) SetMaxGuests(max int) {
	o.mu.Lock()
	defer o.mu.Unlock()

	o.registry.SetMaxGuests(max)
}
