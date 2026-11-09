package orchestrator

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"hotelier/pkg/config"
	"hotelier/pkg/queue"
	"hotelier/pkg/registry"
)

// ErrConcurrencyCapReached is returned by AssignTask when the concurrency cap
// already covers everything in flight (issue #249). It is a distinct error so
// that "held back by the schedule" never reads like "no guest with the right
// tags is available": one is night mode resolving as designed, the other is a
// dead pool an operator has to fix.
var ErrConcurrencyCapReached = errors.New("concurrency cap reached")

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
	store    *queue.Store // optional: persists non-terminal tasks to disk (issue #190)
	mu       sync.Mutex   // single lock for all compound mutations
	logf     func(format string, args ...interface{})
	// concurrencyCap is the cap on how many tasks may be in flight, pushed in
	// by the server whenever the schedule or the config says it changed
	// (issue #249). Negative means unlimited, in the vocabulary
	// config.EffectiveConcurrency uses (config.ConcurrencyUnlimited), and the
	// constructors start there, so an orchestrator nobody configures assigns
	// exactly as it did before the cap existed.
	//
	// The cap is a value rather than a callback on purpose: the server takes
	// its own lock before the orchestrator's (see Server.Reload), so pulling
	// the cap through a closure here would invert that order and deadlock.
	concurrencyCap int
	// lastHoldLog deduplicates the "held by the cap" line: guests poll for
	// work every heartbeat, so night mode would otherwise repeat the same
	// sentence thousands of times a night (issue #249). Keyed by task ID:
	// a single global slot would alternate its key between two tasks held
	// at once (two polling guests, say) and log every poll.
	lastHoldLog map[string]string
}

// New creates a new Orchestrator wrapping the given queue and registry.
func New(logf func(format string, args ...interface{})) *Orchestrator {
	q := queue.NewTaskQueue(logf)
	r := registry.NewGuestRegistry(0, logf) // maxGuests set via SetMaxGuests
	return &Orchestrator{
		queue:    q,
		registry: r,
		logf:     logf,
		// Uncapped until the server pushes a cap in (issue #249).
		concurrencyCap: config.ConcurrencyUnlimited,
	}
}

// NewWithExisting creates a new Orchestrator wrapping existing queue and registry.
// Useful for testing and migration (where queue/registry are created separately).
func NewWithExisting(q *queue.TaskQueue, r *registry.GuestRegistry, logf func(format string, args ...interface{})) *Orchestrator {
	return &Orchestrator{
		queue:    q,
		registry: r,
		logf:     logf,
		// Uncapped until the server pushes a cap in (issue #249).
		concurrencyCap: config.ConcurrencyUnlimited,
	}
}

// SetStore wires in a queue store so that every lifecycle mutation is
// persisted to disk (issue #190). A nil store disables persistence.
// Call this before tasks are added.
func (o *Orchestrator) SetStore(store *queue.Store) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.store = store
}

// RestoreTask re-adds a previously persisted task at startup (issue #190).
// The task's attributes (prompt, tags, priority, created_at) are preserved,
// but it always enters the queue as PENDING with its assignment cleared:
// guests do not survive a restart, so ASSIGNED and RUNNING tasks re-enter
// the queue as they were and are (re-)assigned afresh.
func (o *Orchestrator) RestoreTask(task *queue.Task) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	if err := o.queue.Restore(task); err != nil {
		return err
	}
	o.persistTask(task)
	return nil
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

	if err := o.queue.Add(task); err != nil {
		return err
	}
	o.persistTask(task)
	return nil
}

// AddTaskOrDedup adds a new task to the queue in PENDING state, with
// deduplication on the task's DedupKey. See TaskQueue.AddOrDedup for the
// deduplication semantics.
func (o *Orchestrator) AddTaskOrDedup(task *queue.Task) (*queue.Task, bool, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	existing, deduplicated, err := o.queue.AddOrDedup(task)
	if err != nil {
		return nil, false, err
	}
	if !deduplicated {
		o.persistTask(existing)
	}
	return existing, deduplicated, nil
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
//
// It is the choke point every assignment path runs through, which is where the
// concurrency cap is enforced: with as many tasks in flight as the cap allows,
// the task stays PENDING and ErrConcurrencyCapReached is returned (issue #249).
func (o *Orchestrator) AssignTask(taskID, guestID string) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	// Validate the guest is available — a fact about the guest, not merely
	// its nominal state (issue #251).
	guest, ok := o.registry.GetGuest(guestID)
	if !ok {
		return fmt.Errorf("guest %s not found", guestID)
	}
	if !registry.AvailableForAssignment(guest) {
		return fmt.Errorf("guest %s is not available (state: %s, task: %s, reporting: %s)",
			guestID, guest.State, guest.TaskID, guest.HeartbeatTaskID)
	}

	// Validate task is PENDING
	task, ok := o.queue.Get(taskID)
	if !ok {
		return fmt.Errorf("task %s not found", taskID)
	}
	if task.Status != queue.TaskStatusPending {
		return fmt.Errorf("task %s is not pending (status: %s)", taskID, task.Status)
	}

	// The cap is checked here, under the same lock that flips the task and
	// the guest, so the count and the assignment are one atomic step and no
	// caller path can slip past it (issue #249).
	if !o.admitsNewWorkInternal(taskID) {
		return o.capErrorInternal(taskID)
	}

	// Atomic update: task + guest
	task.Status = queue.TaskStatusAssigned
	task.AssignedTo = guestID
	task.AssignedAt = time.Now()
	task.HeldReason = ""

	guest.TaskID = taskID
	guest.State = registry.GuestStateRunning
	guest.LastTaskHeartbeat = time.Time{} // reset — guest must confirm via heartbeat

	o.persistTask(task)
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
	o.persistTask(task)
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

	o.releaseGuestInternal(guestID)
	o.forgetTask(taskID)

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

	o.releaseGuestInternal(guestID)
	o.forgetTask(taskID)

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
		o.releaseGuestInternal(guestID)
	}
	o.forgetTask(taskID)

	o.logf("task %s cancelled (atomic: →CANCELLED%s)", taskID,
		func() string {
			if guestID != "" {
				return ", RUNNING→IDLE"
			}
			return ""
		}())
	return nil
}

// SetTaskPriority changes the priority of a pending task (issue #266).
//
// Priority only orders the pending queue, so a task that has left PENDING
// is rejected: its place in the queue no longer matters, and changing it
// would be misleading.
func (o *Orchestrator) SetTaskPriority(taskID, priority string) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	task, ok := o.queue.Get(taskID)
	if !ok {
		return fmt.Errorf("task %s not found", taskID)
	}
	if task.Status != queue.TaskStatusPending {
		return fmt.Errorf("task %s is not pending (status: %s)", taskID, task.Status)
	}

	if err := o.queue.SetPriority(taskID, priority); err != nil {
		return err
	}
	o.persistTask(task)

	o.logf("task %s priority set to %s (PENDING)", taskID, priority)
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
		o.releaseGuestInternal(guestID)
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

	o.releaseGuestInternal(guestID)

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

	// Same gate as AssignTask, under the same lock (issue #249).
	if !o.admitsNewWorkInternal(task.ID) {
		return false
	}

	guest := guests[0]

	// Atomic assign (internal, lock already held)
	task.Status = queue.TaskStatusAssigned
	task.AssignedTo = guest.ID
	task.AssignedAt = time.Now()
	task.HeldReason = ""

	guest.TaskID = task.ID
	guest.State = registry.GuestStateRunning
	guest.LastTaskHeartbeat = time.Time{}

	o.persistTask(task)
	o.logf("task %s auto-assigned to guest %s", task.ID, guest.ID)
	return true
}

// SetConcurrencyCap sets the cap on how many tasks may be in flight at once
// (issue #249). A negative value (config.ConcurrencyUnlimited) means no cap;
// 0 means start nothing, which is how an overnight schedule entry pauses the
// queue. It gates *starting* work only: tasks already ASSIGNED or RUNNING
// finish normally whatever the cap says now.
//
// The caller — the server — is responsible for re-resolving the cap against
// the schedule and pushing it here on reload and on every schedule boundary.
func (o *Orchestrator) SetConcurrencyCap(newCap int) {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.concurrencyCap == newCap {
		return
	}
	o.concurrencyCap = newCap
	// A different cap makes the previous "held" lines stale, so the next
	// hold of each task is worth logging again.
	o.lastHoldLog = nil
}

// ConcurrencyCap returns the cap currently in force. Negative means no cap.
func (o *Orchestrator) ConcurrencyCap() int {
	o.mu.Lock()
	defer o.mu.Unlock()

	return o.concurrencyCap
}

// InFlightCount returns the number of tasks in flight: ASSIGNED or RUNNING.
// That is the number the concurrency cap is compared against (issue #249).
func (o *Orchestrator) InFlightCount() int {
	o.mu.Lock()
	defer o.mu.Unlock()

	return o.inFlightCountInternal()
}

// AdmitsNewWork reports whether the cap leaves room for a task to start now,
// recording the hold in the task and the log when it does not, and clearing
// any stale hold reason when it does (issue #249). Callers use it to answer
// "why is this still PENDING?" on a path that never reaches AssignTask because
// no guest looked eligible, and so must not report a cap hold it did not
// attempt.
func (o *Orchestrator) AdmitsNewWork(taskID string) bool {
	o.mu.Lock()
	defer o.mu.Unlock()

	return o.admitsNewWorkInternal(taskID)
}

// inFlightCountInternal counts the tasks the cap must leave room for. The
// caller must hold o.mu.
func (o *Orchestrator) inFlightCountInternal() int {
	n := 0
	for _, task := range o.queue.GetAllTasks() {
		if task.Status == queue.TaskStatusAssigned || task.Status == queue.TaskStatusRunning {
			n++
		}
	}
	return n
}

// admitsNewWorkInternal reports whether a new task may start now, and records
// the refusal in the task and the log when it may not. An admitted task has
// nothing left to be held back by, so any reason recorded by an earlier,
// stricter cap is cleared — otherwise a task that goes on to wait on guest
// availability would keep advertising the released cap (issue #249). The
// caller must hold o.mu, which is what makes the count and the assignment
// atomic.
func (o *Orchestrator) admitsNewWorkInternal(taskID string) bool {
	clearHeldReason := func() {
		if task, ok := o.queue.Get(taskID); ok {
			task.HeldReason = ""
		}
	}

	// Negative is config.ConcurrencyUnlimited: no cap in force, so work
	// starts exactly as it did before the cap existed (issue #249).
	if o.concurrencyCap < 0 {
		clearHeldReason()
		return true
	}

	inFlight := o.inFlightCountInternal()
	if inFlight < o.concurrencyCap {
		clearHeldReason()
		return true
	}

	// Legible in state, so "waiting on the schedule" is distinguishable from
	// "no guest with the right tags", which leaves the field empty.
	if task, ok := o.queue.Get(taskID); ok {
		task.HeldReason = fmt.Sprintf("concurrency cap %d reached (%d in flight)", o.concurrencyCap, inFlight)
	}

	// Legible in the log, once per distinct (task, cap, in-flight) state
	// rather than once per poll. Keyed per task so tasks held at once cannot
	// alternate a single global key past the dedup and log every poll.
	key := fmt.Sprintf("%d:%d", o.concurrencyCap, inFlight)
	if o.lastHoldLog == nil {
		o.lastHoldLog = map[string]string{}
	}
	if o.lastHoldLog[taskID] != key {
		o.lastHoldLog[taskID] = key
		o.logf("task %s held back by the concurrency cap: %d in flight, cap %d (queue is waiting on the schedule, not on guest availability)",
			taskID, inFlight, o.concurrencyCap)
	}
	return false
}

// capErrorInternal states the refusal for a specific task. The caller must
// hold o.mu.
func (o *Orchestrator) capErrorInternal(taskID string) error {
	return fmt.Errorf("%w: task %s, %d in flight, cap %d", ErrConcurrencyCapReached,
		taskID, o.inFlightCountInternal(), o.concurrencyCap)
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

// TaskHeartbeat records the guest's liveness heartbeat. It never changes
// the guest's assignment: a report of a task other than the one assigned
// is a mismatch to surface, resolved through the reconcile path
// (issue #251).
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
		o.releaseGuestInternal(guestID)
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
		o.releaseGuestInternal(guestID)
		o.logf("guest %s reconciled: task %s assigned to %s, set IDLE", guestID, task.ID, task.AssignedTo)
		return ReconcileState{Status: queue.TaskStatusPending}

	case queue.TaskStatusPending:
		// Task was re-queued — guest's assignment was revoked.
		o.releaseGuestInternal(guestID)
		o.logf("guest %s reconciled: task %s re-queued, set IDLE", guestID, task.ID)
		return ReconcileState{Status: queue.TaskStatusPending}

	case queue.TaskStatusCompleted, queue.TaskStatusFailed, queue.TaskStatusCancelled:
		// Task is terminal — guest's work is done.
		o.releaseGuestInternal(guestID)
		o.logf("guest %s reconciled: task %s terminal (%s), set IDLE", guestID, task.ID, task.Status)
		return ReconcileState{Status: task.Status, TaskID: task.ID}
	}

	return ReconcileState{Status: queue.TaskStatusPending}
}

// ReconcileFreshGuest reconciles a registration from a process that has
// never registered before — the case a stable, configured guest id (issue
// #253) makes possible: the registry still holds an id a dead process left
// behind, and the process now registering under it holds no task at all.
//
// ReconcileGuest must not be used there. It reads the registry's task
// reference as fact and would report the previous process's task as this
// one's, so the fresh guest would be handed work it is not running.
//
// The task itself is deliberately left alone: revoking the claim an
// assignment leaves behind is issue #252. The stuck-task probe re-queues it
// within its timeout, and the guest is IDLE and available meanwhile.
//
// Returns the authoritative state (always no task) so the server can answer
// the registration.
func (o *Orchestrator) ReconcileFreshGuest(guestID string) ReconcileState {
	o.mu.Lock()
	defer o.mu.Unlock()

	guest, ok := o.registry.GetGuest(guestID)
	if !ok {
		return ReconcileState{Status: queue.TaskStatusPending}
	}

	if guest.TaskID != "" {
		o.logf("guest %s registered fresh: dropped stale reference to task %s", guestID, guest.TaskID)
		o.clearGuestTaskInternal(guestID)
	}
	guest.State = registry.GuestStateIdle

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
// Guests still holding a reference to a terminal task, or to a task assigned
// elsewhere, are logged as failed assertions but left untouched — recovery
// here was the re-queue loop of issue #251; the reconcile path resolves it.
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

			// Capture the elapsed assignment time *before* the re-queue:
			// requeueTaskInternal clears AssignedAt on this very task
			// pointer, and subtracting the zero time from now saturates at
			// math.MaxInt64 nanoseconds — every line used to read
			// "for 2562047h47m16.854775807s". Issue #253.
			dur := now.Sub(task.AssignedAt)
			if dur < 0 {
				dur = 0
			}

			o.requeueTaskInternal(task.ID)
			if guestID != "" {
				o.releaseGuestInternal(guestID)
			}

			o.logf("stuck task %s re-queued (was ASSIGNED to %s for %v)", task.ID, guestID, dur)
			continue
		}

		// For terminal tasks, check if the assigned guest still has a stale
		// reference. Once availability is a fact about the guest this must
		// never happen: it is an assertion, not a recovery heuristic. A
		// hit is logged and left alone — mutating the state here is what
		// turned a regression into a re-queue loop (issue #251). The
		// reference is resolved through the reconcile path instead.
		if task.AssignedTo != "" && o.isTerminalStatus(task.Status) {
			if guest, ok := o.registry.GetGuest(task.AssignedTo); ok && guest.TaskID == task.ID {
				o.logf("stale guest reference (assertion failed, not correcting): %s still points to terminal task %s (%s)",
					guest.ID, task.ID, task.Status)
			}
		}
	}

	// Also check guests that have a stale TaskID pointing to a task that
	// is not assigned to them (e.g. a heartbeat once again rewriting
	// assignments). Assertion, not recovery: log and leave the state
	// alone so a regression shows up as a message rather than as another
	// loop (issue #251).
	for _, guest := range o.registry.GetAllGuests() {
		if guest.TaskID == "" {
			continue
		}
		task, ok := o.queue.Get(guest.TaskID)
		if !ok {
			continue
		}
		if task.AssignedTo != guest.ID {
			o.logf("stale guest reference (assertion failed, not correcting): %s points to task %s (assigned to %s)",
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
			// Task not found — stale reference, release the guest.
			o.releaseGuestInternal(guest.ID)
			continue
		}

		// Skip terminal tasks (already completed/failed/cancelled)
		if task.Status == queue.TaskStatusCompleted || task.Status == queue.TaskStatusFailed || task.Status == queue.TaskStatusCancelled {
			o.releaseGuestInternal(guest.ID)
			continue
		}

		// Skip re-queued tasks (CheckStuckTasks already handled this)
		if task.Status == queue.TaskStatusPending {
			o.releaseGuestInternal(guest.ID)
			continue
		}

		// Task is RUNNING or ASSIGNED — fail it.
		task.Status = queue.TaskStatusFailed
		task.Error = fmt.Sprintf("guest %s silent for %v", guest.ID, now.Sub(guest.LastHeartbeat))

		o.releaseGuestInternal(guest.ID)
		o.forgetTask(task.ID)
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
				o.forgetTask(task.ID)
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
	o.persistTask(task)
}

// persistTask writes a non-terminal task to the queue store. The caller
// must hold o.mu. A nil store (persistence disabled) is a no-op; store
// errors are logged, not returned — a persistence failure must not break
// task processing.
func (o *Orchestrator) persistTask(task *queue.Task) {
	if o.store == nil {
		return
	}
	if err := o.store.Save(task); err != nil {
		o.logf("queue store: failed to persist task %s: %v", task.ID, err)
	}
}

// forgetTask removes a (now terminal) task from the queue store. The caller
// must hold o.mu. A nil store is a no-op; store errors are logged.
func (o *Orchestrator) forgetTask(taskID string) {
	if o.store == nil {
		return
	}
	if err := o.store.Delete(taskID); err != nil {
		o.logf("queue store: failed to forget task %s: %v", taskID, err)
	}
}

func (o *Orchestrator) clearGuestTaskInternal(guestID string) {
	guest, ok := o.registry.GetGuest(guestID)
	if !ok {
		return
	}

	// The guest's heartbeat report of the very task being cleared is
	// obsolete only when the task itself reached a terminal state — the
	// fact that ended the task ended the execution too. A re-queue keeps
	// the report: the process may still be running, and the report is
	// what keeps the guest unavailable (issue #251).
	if ref := guest.TaskID; ref != "" && guest.HeartbeatTaskID == ref {
		if task, ok := o.queue.Get(ref); ok && o.isTerminalStatus(task.Status) {
			guest.HeartbeatTaskID = ""
		}
	}

	guest.TaskID = ""
	guest.LastTaskHeartbeat = time.Time{}
	// Deliberately does not set the guest's state: availability is a fact
	// about the guest, never a side effect of this bookkeeping. Paths that
	// know the guest is finished use releaseGuestInternal.
}

// releaseGuestInternal clears the guest's task reference and sets it IDLE.
// Only valid where the guest really is free — it reported completion or
// failure, declined, had its claim revoked by a re-queue, was reconciled
// on reconnect, or went silent — i.e. where IDLE is a fact, not a
// bookkeeping assumption (issue #251). Note that availability still
// consults the guest's own heartbeat report: a released guest that
// continues to report a running task is not handed more work.
func (o *Orchestrator) releaseGuestInternal(guestID string) {
	guest, ok := o.registry.GetGuest(guestID)
	if !ok {
		return
	}
	o.clearGuestTaskInternal(guestID)
	guest.State = registry.GuestStateIdle
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
