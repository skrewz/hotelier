package orchestrator

import (
	"math"
	"testing"

	"hotelier/pkg/queue"
)

// recordedEvent is one Observe call captured by fakeObserver.
type recordedEvent struct {
	name    string
	seconds float64
}

// fakeObserver records every event the orchestrator emits so the tests can
// assert the funnel fires from exactly the methods that change state — and
// from nowhere else (issue #262).
type fakeObserver struct {
	events []recordedEvent
}

func (f *fakeObserver) Observe(event string, seconds float64) {
	f.events = append(f.events, recordedEvent{name: event, seconds: seconds})
}

func (f *fakeObserver) names() []string {
	out := make([]string, len(f.events))
	for i, e := range f.events {
		out[i] = e.name
	}
	return out
}

func (f *fakeObserver) last(t *testing.T) recordedEvent {
	t.Helper()
	if len(f.events) == 0 {
		t.Fatal("no events observed")
	}
	return f.events[len(f.events)-1]
}

func wired(t *testing.T) (*Orchestrator, *fakeObserver) {
	t.Helper()
	o := newTestOrchestrator(t)
	obs := &fakeObserver{}
	o.SetTaskObserver(obs)
	return o, obs
}

func mustTask(t *testing.T, o *Orchestrator, id string) {
	t.Helper()
	if err := o.AddTask(&queue.Task{ID: id, Prompt: "do the thing"}); err != nil {
		t.Fatalf("AddTask %s: %v", id, err)
	}
}

func mustAssign(t *testing.T, o *Orchestrator, taskID, guestID string) {
	t.Helper()
	if err := o.RegisterGuest(guestID, guestID, []string{"default"}); err != nil {
		t.Fatalf("RegisterGuest %s: %v", guestID, err)
	}
	if err := o.AssignTask(taskID, guestID); err != nil {
		t.Fatalf("AssignTask %s: %v", taskID, err)
	}
}

func TestAddTaskObservesSubmitted(t *testing.T) {
	o, obs := wired(t)
	mustTask(t, o, "t1")
	if got := obs.names(); len(got) != 1 || got[0] != EventSubmitted {
		t.Fatalf("events = %v, want exactly [submitted]", got)
	}
}

func TestAddTaskFailureObservesNothing(t *testing.T) {
	o, obs := wired(t)
	mustTask(t, o, "t1")
	if err := o.AddTask(&queue.Task{ID: "t1", Prompt: "dupe"}); err == nil {
		t.Fatal("expected duplicate AddTask to fail")
	}
	if len(obs.events) != 1 {
		t.Fatalf("events = %v, want only the one from the accepted add", obs.names())
	}
}

func TestAddTaskOrDedupAcceptedObservesSubmitted(t *testing.T) {
	o, obs := wired(t)
	if _, dup, err := o.AddTaskOrDedup(&queue.Task{ID: "t1", Prompt: "p", DedupKey: "k"}); err != nil || dup {
		t.Fatalf("AddTaskOrDedup: %v %v", dup, err)
	}
	if got := obs.names(); len(got) != 1 || got[0] != EventSubmitted {
		t.Fatalf("events = %v, want exactly [submitted]", got)
	}
}

func TestAddTaskOrDedupSquelchedObservesDeduplicatedOnly(t *testing.T) {
	o, obs := wired(t)
	if _, dup, err := o.AddTaskOrDedup(&queue.Task{ID: "t1", Prompt: "p", DedupKey: "k"}); err != nil || dup {
		t.Fatalf("AddTaskOrDedup: %v %v", dup, err)
	}
	if _, dup, err := o.AddTaskOrDedup(&queue.Task{ID: "t2", Prompt: "p", DedupKey: "k"}); err != nil || !dup {
		t.Fatalf("AddTaskOrDedup second: dup=%v err=%v", dup, err)
	}
	if got := obs.names(); len(got) != 2 || got[0] != EventSubmitted || got[1] != EventDeduplicated {
		t.Fatalf("events = %v, want [submitted deduplicated]", got)
	}
}

func TestAssignTaskObservesQueueWait(t *testing.T) {
	o, obs := wired(t)
	mustTask(t, o, "t1")
	mustAssign(t, o, "t1", "g1")
	if len(obs.events) != 2 {
		t.Fatalf("events = %v, want [submitted queue_wait]", obs.names())
	}
	last := obs.last(t)
	if last.name != EventQueueWait {
		t.Fatalf("event = %s, want queue_wait", last.name)
	}
	if math.IsNaN(last.seconds) || last.seconds < 0 {
		t.Errorf("queue_wait seconds = %v, want a real non-negative duration", last.seconds)
	}
}

func TestAssignTaskRefusedByCapObservesNothing(t *testing.T) {
	o, obs := wired(t)
	o.SetConcurrencyCap(0)
	mustTask(t, o, "t1")
	if err := o.RegisterGuest("g1", "g1", nil); err != nil {
		t.Fatalf("RegisterGuest: %v", err)
	}
	if err := o.AssignTask("t1", "g1"); err == nil {
		t.Fatal("expected AssignTask to be refused by the cap")
	}
	if len(obs.events) != 1 || obs.events[0].name != EventSubmitted {
		t.Fatalf("events = %v, want only the submission", obs.names())
	}
}

func TestCompleteTaskObservesCompletedWithRunDuration(t *testing.T) {
	o, obs := wired(t)
	mustTask(t, o, "t1")
	mustAssign(t, o, "t1", "g1")
	if err := o.AcknowledgeTask("t1", "g1"); err != nil {
		t.Fatalf("AcknowledgeTask: %v", err)
	}
	if err := o.CompleteTask("t1", "g1", "done"); err != nil {
		t.Fatalf("CompleteTask: %v", err)
	}
	last := obs.last(t)
	if last.name != EventCompleted {
		t.Fatalf("event = %s, want completed", last.name)
	}
	if math.IsNaN(last.seconds) || last.seconds < 0 {
		t.Errorf("duration seconds = %v, want a real non-negative duration", last.seconds)
	}
	// Idempotence must not double-count: completing again observes nothing.
	if err := o.CompleteTask("t1", "g1", "again"); err != nil {
		t.Fatalf("second CompleteTask: %v", err)
	}
	if obs.last(t).name != EventCompleted || len(obs.events) != 3 {
		t.Fatalf("events = %v, want no second completed event", obs.names())
	}
}

func TestFailTaskObservesFailed(t *testing.T) {
	o, obs := wired(t)
	mustTask(t, o, "t1")
	mustAssign(t, o, "t1", "g1")
	if err := o.FailTask("t1", "g1", "boom"); err != nil {
		t.Fatalf("FailTask: %v", err)
	}
	last := obs.last(t)
	if last.name != EventFailed {
		t.Fatalf("event = %s, want failed", last.name)
	}
	if math.IsNaN(last.seconds) || last.seconds < 0 {
		t.Errorf("duration seconds = %v, want a real non-negative duration", last.seconds)
	}
}

func TestCancelTaskObservesCancelled(t *testing.T) {
	o, obs := wired(t)
	mustTask(t, o, "t1")
	mustAssign(t, o, "t1", "g1")
	if err := o.CancelTask("t1", "g1"); err != nil {
		t.Fatalf("CancelTask: %v", err)
	}
	last := obs.last(t)
	if last.name != EventCancelled {
		t.Fatalf("event = %s, want cancelled", last.name)
	}
	if math.IsNaN(last.seconds) || last.seconds < 0 {
		t.Errorf("duration seconds = %v, want a real non-negative duration", last.seconds)
	}
}

// A PENDING task never left the queue, so its cancellation counts as a
// finish but has no run window to observe (issue #262).
func TestCancelPendingTaskObservesCancelledWithoutDuration(t *testing.T) {
	o, obs := wired(t)
	mustTask(t, o, "t1")
	if err := o.CancelTask("t1", ""); err != nil {
		t.Fatalf("CancelTask: %v", err)
	}
	last := obs.last(t)
	if last.name != EventCancelled {
		t.Fatalf("event = %s, want cancelled", last.name)
	}
	if !math.IsNaN(last.seconds) {
		t.Errorf("duration seconds = %v, want NoDuration (NaN) for a task that was never assigned", last.seconds)
	}
}

func TestRequeueTaskObservesRequeued(t *testing.T) {
	o, obs := wired(t)
	mustTask(t, o, "t1")
	mustAssign(t, o, "t1", "g1")
	if err := o.RequeueTask("t1"); err != nil {
		t.Fatalf("RequeueTask: %v", err)
	}
	if obs.last(t).name != EventRequeued {
		t.Fatalf("event = %s, want requeued", obs.last(t).name)
	}
	// Already PENDING — requeueing is a no-op and must not count again.
	if err := o.RequeueTask("t1"); err != nil {
		t.Fatalf("second RequeueTask: %v", err)
	}
	if obs.last(t).name != EventRequeued || len(obs.events) != 3 {
		t.Fatalf("events = %v, want no second requeued event", obs.names())
	}
}

func TestDeclineTaskObservesRequeued(t *testing.T) {
	o, obs := wired(t)
	mustTask(t, o, "t1")
	mustAssign(t, o, "t1", "g1")
	if err := o.DeclineTask("t1", "g1"); err != nil {
		t.Fatalf("DeclineTask: %v", err)
	}
	if obs.last(t).name != EventRequeued {
		t.Fatalf("event = %s, want requeued", obs.last(t).name)
	}
}

// Every existing call site constructs an orchestrator with no metrics wired
// (issue #262); the whole funnel must stay nil-safe.
func TestLifecycleWithoutObserver(t *testing.T) {
	o := newTestOrchestrator(t)
	mustTask(t, o, "t1")
	mustAssign(t, o, "t1", "g1")
	if err := o.AcknowledgeTask("t1", "g1"); err != nil {
		t.Fatalf("AcknowledgeTask: %v", err)
	}
	if err := o.CompleteTask("t1", "g1", "done"); err != nil {
		t.Fatalf("CompleteTask: %v", err)
	}
	task, ok := o.GetTask("t1")
	if !ok || task.Status != queue.TaskStatusCompleted {
		t.Fatalf("task = %v %v, want COMPLETED", task, ok)
	}
}
