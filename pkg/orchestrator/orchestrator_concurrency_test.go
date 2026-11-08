package orchestrator

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"hotelier/pkg/config"
	"hotelier/pkg/queue"
	"hotelier/pkg/registry"
)

// newCappedOrchestrator returns an orchestrator whose assignment decisions
// are logged into logLines and which admits at most cap tasks at once
// (issue #249). A negative cap is the unlimited case, stated in the same
// vocabulary config.EffectiveConcurrency uses.
func newCappedOrchestrator(t *testing.T, limit int) (*Orchestrator, *[]string) {
	t.Helper()
	var lines []string
	orch := New(func(format string, args ...interface{}) {
		lines = append(lines, fmt.Sprintf(format, args...))
	})
	orch.SetConcurrencyCap(limit)
	return orch, &lines
}

func addPending(t *testing.T, orch *Orchestrator, id string) {
	t.Helper()
	if err := orch.AddTask(&queue.Task{ID: id, Prompt: id}); err != nil {
		t.Fatalf("AddTask %s: %v", id, err)
	}
}

// TestAssignTask_AtCap_StaysPending is the core rule: with the cap already
// met, an assignment attempt must be refused and the task must remain
// PENDING rather than going out to the idle guest.
func TestAssignTask_AtCap_StaysPending(t *testing.T) {
	orch, _ := newCappedOrchestrator(t, 1)
	_ = orch.RegisterGuest("g1", "G1", nil)
	_ = orch.RegisterGuest("g2", "G2", nil)
	addPending(t, orch, "t1")
	addPending(t, orch, "t2")

	if err := orch.AssignTask("t1", "g1"); err != nil {
		t.Fatalf("first assignment must succeed under the cap: %v", err)
	}

	err := orch.AssignTask("t2", "g2")
	if !errors.Is(err, ErrConcurrencyCapReached) {
		t.Fatalf("expected ErrConcurrencyCapReached, got %v", err)
	}

	task, ok := orch.GetTask("t2")
	if !ok {
		t.Fatal("t2 missing from queue")
	}
	if task.Status != queue.TaskStatusPending {
		t.Errorf("expected t2 to stay PENDING, got %s", task.Status)
	}
	guest, _ := orch.GetGuest("g2")
	if guest.State != registry.GuestStateIdle || guest.TaskID != "" {
		t.Errorf("expected g2 to stay free, got state %s task %q", guest.State, guest.TaskID)
	}
}

// TestAssignTask_ASSIGNEDCountsTowardCap: the cap counts tasks in flight,
// which includes ASSIGNED ones — an ASSIGNED task is work that has started.
func TestAssignTask_ASSIGNEDCountsTowardCap(t *testing.T) {
	orch, _ := newCappedOrchestrator(t, 1)
	_ = orch.RegisterGuest("g1", "G1", nil)
	_ = orch.RegisterGuest("g2", "G2", nil)
	addPending(t, orch, "t1")
	addPending(t, orch, "t2")

	if err := orch.AssignTask("t1", "g1"); err != nil {
		t.Fatalf("assign t1: %v", err)
	}
	// t1 is ASSIGNED, not RUNNING yet: still one task in flight.
	if got := orch.InFlightCount(); got != 1 {
		t.Fatalf("expected 1 task in flight, got %d", got)
	}
	if err := orch.AssignTask("t2", "g2"); !errors.Is(err, ErrConcurrencyCapReached) {
		t.Fatalf("expected ASSIGNED tasks to count toward the cap, got %v", err)
	}
}

// TestAssignTask_CompletionFreesSlot: a task finishing must let the next one
// out with no other event.
func TestAssignTask_CompletionFreesSlot(t *testing.T) {
	orch, _ := newCappedOrchestrator(t, 1)
	_ = orch.RegisterGuest("g1", "G1", nil)
	_ = orch.RegisterGuest("g2", "G2", nil)
	addPending(t, orch, "t1")
	addPending(t, orch, "t2")

	if err := orch.AssignTask("t1", "g1"); err != nil {
		t.Fatalf("assign t1: %v", err)
	}
	if err := orch.AssignTask("t2", "g2"); !errors.Is(err, ErrConcurrencyCapReached) {
		t.Fatalf("expected t2 held at the cap, got %v", err)
	}

	if err := orch.CompleteTask("t1", "g1", "done"); err != nil {
		t.Fatalf("complete t1: %v", err)
	}
	if got := orch.InFlightCount(); got != 0 {
		t.Fatalf("expected no tasks in flight after completion, got %d", got)
	}
	if err := orch.AssignTask("t2", "g2"); err != nil {
		t.Fatalf("expected the freed slot to admit t2, got %v", err)
	}
}

// TestAssignTask_CapDropLeavesRunningTaskAlone: the cap gates starting work
// only. Tightening it mid-flight must not disturb a task already running.
func TestAssignTask_CapDropLeavesRunningTaskAlone(t *testing.T) {
	orch, _ := newCappedOrchestrator(t, 4)
	_ = orch.RegisterGuest("g1", "G1", nil)
	addPending(t, orch, "t1")

	if err := orch.AssignTask("t1", "g1"); err != nil {
		t.Fatalf("assign t1: %v", err)
	}
	if err := orch.AcknowledgeTask("t1", "g1"); err != nil {
		t.Fatalf("ack t1: %v", err)
	}

	// The 23:00 entry lands: start nothing.
	orch.SetConcurrencyCap(0)

	task, _ := orch.GetTask("t1")
	if task.Status != queue.TaskStatusRunning {
		t.Errorf("expected t1 to keep running after the cap dropped, got %s", task.Status)
	}
	if err := orch.CompleteTask("t1", "g1", "done"); err != nil {
		t.Fatalf("a paused cap must not block completion: %v", err)
	}
}

// TestAssignTask_UnlimitedCapIsTodayBehaviour: the unconfigured deployment
// (config.ConcurrencyUnlimited) must assign freely, exactly as before the
// cap existed.
func TestAssignTask_UnlimitedCapIsTodayBehaviour(t *testing.T) {
	orch, _ := newCappedOrchestrator(t, config.ConcurrencyUnlimited)
	for _, id := range []string{"g1", "g2", "g3"} {
		_ = orch.RegisterGuest(id, id, nil)
		addPending(t, orch, id)
	}
	for _, id := range []string{"g1", "g2", "g3"} {
		if err := orch.AssignTask(id, id); err != nil {
			t.Fatalf("unlimited cap must assign %s: %v", id, err)
		}
	}
	if got := orch.InFlightCount(); got != 3 {
		t.Fatalf("expected 3 tasks in flight, got %d", got)
	}
}

// TestAssignTask_HeldReasonIsLegibleAndCleared: a task held by the cap must
// say so in its own state — distinct from a task waiting on guest
// availability — and the reason must not outlive the hold.
func TestAssignTask_HeldReasonIsLegibleAndCleared(t *testing.T) {
	orch, _ := newCappedOrchestrator(t, 1)
	_ = orch.RegisterGuest("g1", "G1", nil)
	_ = orch.RegisterGuest("g2", "G2", nil)
	addPending(t, orch, "t1")
	addPending(t, orch, "t2")

	if err := orch.AssignTask("t1", "g1"); err != nil {
		t.Fatalf("assign t1: %v", err)
	}
	if err := orch.AssignTask("t2", "g2"); !errors.Is(err, ErrConcurrencyCapReached) {
		t.Fatalf("expected t2 held, got %v", err)
	}
	held, _ := orch.GetTask("t2")
	if held.HeldReason == "" {
		t.Fatal("expected t2 to carry a held_reason naming the cap")
	}
	if !strings.Contains(held.HeldReason, "concurrency cap") {
		t.Errorf("expected held_reason to name the concurrency cap, got %q", held.HeldReason)
	}
	if strings.Contains(held.HeldReason, "guest") {
		t.Errorf("held_reason must not read like a guest-availability problem: %q", held.HeldReason)
	}

	// Freeing the slot and assigning must clear the reason.
	if err := orch.CompleteTask("t1", "g1", "done"); err != nil {
		t.Fatalf("complete t1: %v", err)
	}
	if err := orch.AssignTask("t2", "g2"); err != nil {
		t.Fatalf("assign t2 after the slot freed: %v", err)
	}
	assigned, _ := orch.GetTask("t2")
	if assigned.HeldReason != "" {
		t.Errorf("expected held_reason to clear on assignment, got %q", assigned.HeldReason)
	}
}

// TestAssignTask_HoldIsLoggedOncePerState: the guest poll calls AssignTask
// every heartbeat, so night mode must not flood the log — but the first hold
// must be there, and it must say the cap is the reason.
func TestAssignTask_HoldIsLoggedOncePerState(t *testing.T) {
	orch, lines := newCappedOrchestrator(t, 1)
	_ = orch.RegisterGuest("g1", "G1", nil)
	_ = orch.RegisterGuest("g2", "G2", nil)
	addPending(t, orch, "t1")
	addPending(t, orch, "t2")

	if err := orch.AssignTask("t1", "g1"); err != nil {
		t.Fatalf("assign t1: %v", err)
	}
	for i := 0; i < 5; i++ {
		_ = orch.AssignTask("t2", "g2")
	}

	holds := 0
	for _, l := range *lines {
		if strings.Contains(l, "concurrency cap") {
			holds++
		}
	}
	if holds != 1 {
		t.Fatalf("expected one cap-hold log line for the repeated attempts, got %d (%v)", holds, *lines)
	}
}

// TestAssignTask_TwoHeldTasksDedupPerTask: two tasks held at once (two
// differently-tagged guests polling while the cap is met) must each be
// deduplicated against their own last-logged state. A single global dedup
// slot would alternate its key between the two tasks on every attempt and
// log every poll — the per-heartbeat flood the dedup exists to prevent
// (issue #249).
func TestAssignTask_TwoHeldTasksDedupPerTask(t *testing.T) {
	orch, lines := newCappedOrchestrator(t, 1)
	_ = orch.RegisterGuest("g1", "G1", nil)
	_ = orch.RegisterGuest("g2", "G2", nil)
	_ = orch.RegisterGuest("g3", "G3", nil)
	addPending(t, orch, "t1")
	addPending(t, orch, "t2")
	addPending(t, orch, "t3")

	if err := orch.AssignTask("t1", "g1"); err != nil {
		t.Fatalf("assign t1: %v", err)
	}
	// Alternate the held attempts the way two polling guests would.
	for i := 0; i < 5; i++ {
		_ = orch.AssignTask("t2", "g2")
		_ = orch.AssignTask("t3", "g3")
	}

	holds := 0
	for _, l := range *lines {
		if strings.Contains(l, "concurrency cap") {
			holds++
		}
	}
	if holds != 2 {
		t.Fatalf("expected one cap-hold line per held task, got %d (%v)", holds, *lines)
	}
}

// TestAdmitsNewWork_CapRiseClearsHeldReason: a task held by the cap that the
// cap later releases must stop advertising the cap the moment admission is
// granted, even if it still cannot start because no guest is available.
// Otherwise /api/tasks keeps saying "concurrency cap N reached" while the log
// correctly says "no available guest" — the cap-vs-pool misreading, inverted
// (issue #249).
func TestAdmitsNewWork_CapRiseClearsHeldReason(t *testing.T) {
	orch, _ := newCappedOrchestrator(t, 0)
	_ = orch.RegisterGuest("g1", "G1", nil)
	addPending(t, orch, "t1")

	if err := orch.AssignTask("t1", "g1"); !errors.Is(err, ErrConcurrencyCapReached) {
		t.Fatalf("expected t1 held at cap 0, got %v", err)
	}
	held, _ := orch.GetTask("t1")
	if held.HeldReason == "" {
		t.Fatal("expected t1 to carry a held_reason naming the cap")
	}

	// The cap rises: admission is granted, so the cap is no longer the reason.
	orch.SetConcurrencyCap(2)
	if !orch.AdmitsNewWork("t1") {
		t.Fatal("expected admission after the cap rose")
	}
	after, _ := orch.GetTask("t1")
	if after.HeldReason != "" {
		t.Errorf("expected held_reason to clear once the cap admits, got %q", after.HeldReason)
	}

	// The same holds when the cap becomes unlimited.
	orch.SetConcurrencyCap(0)
	addPending(t, orch, "t2")
	if err := orch.AssignTask("t2", "g1"); !errors.Is(err, ErrConcurrencyCapReached) {
		t.Fatalf("expected t2 held at cap 0, got %v", err)
	}
	orch.SetConcurrencyCap(config.ConcurrencyUnlimited)
	if !orch.AdmitsNewWork("t2") {
		t.Fatal("expected admission once the cap is unlimited")
	}
	if after, _ := orch.GetTask("t2"); after.HeldReason != "" {
		t.Errorf("expected held_reason to clear once the cap is unlimited, got %q", after.HeldReason)
	}
}

// TestTryAssignNext_AtCap_AssignsNothing: the bulk pass obeys the same rule
// as the point-to-point assignment.
func TestTryAssignNext_AtCap_AssignsNothing(t *testing.T) {
	orch, _ := newCappedOrchestrator(t, 1)
	_ = orch.RegisterGuest("g1", "G1", nil)
	_ = orch.RegisterGuest("g2", "G2", nil)
	addPending(t, orch, "t1")
	addPending(t, orch, "t2")

	if !orch.TryAssignNext() {
		t.Fatal("expected the first auto-assign to succeed")
	}
	if orch.TryAssignNext() {
		t.Fatal("expected TryAssignNext to assign nothing at the cap")
	}
	tasks := orch.GetTasksByStatus(queue.TaskStatusPending)
	if len(tasks) != 1 {
		t.Fatalf("expected t2 to stay PENDING, got %d pending tasks", len(tasks))
	}
}

// TestTryAssignNext_UnlimitedCap is today's behaviour for the bulk pass.
func TestTryAssignNext_UnlimitedCap(t *testing.T) {
	orch, _ := newCappedOrchestrator(t, config.ConcurrencyUnlimited)
	_ = orch.RegisterGuest("g1", "G1", nil)
	_ = orch.RegisterGuest("g2", "G2", nil)
	addPending(t, orch, "t1")
	addPending(t, orch, "t2")

	if !orch.TryAssignNext() || !orch.TryAssignNext() {
		t.Fatal("expected both auto-assigns to succeed with no cap in force")
	}
}

// TestSetConcurrencyCap_DefaultsToUnlimited: an orchestrator built without a
// cap call behaves as it did before the cap existed.
func TestSetConcurrencyCap_DefaultsToUnlimited(t *testing.T) {
	orch := newTestOrchestrator(t)
	_ = orch.RegisterGuest("g1", "G1", nil)
	_ = orch.RegisterGuest("g2", "G2", nil)
	addPending(t, orch, "t1")
	addPending(t, orch, "t2")

	if err := orch.AssignTask("t1", "g1"); err != nil {
		t.Fatalf("assign t1: %v", err)
	}
	if err := orch.AssignTask("t2", "g2"); err != nil {
		t.Fatalf("a fresh orchestrator must be uncapped: %v", err)
	}
}

// TestPendingHeldByCapCount pins the distinction issue #250 exists to make:
// work the cap cannot start must be countable separately from work the pool
// could take. The count follows the cap and the queue, not the holds the last
// assignment pass happened to record.
func TestPendingHeldByCapCount(t *testing.T) {
	// The cap has room for everything the queue holds, so nothing in it is
	// waiting on the cap.
	roomy, _ := newCappedOrchestrator(t, 3)
	for _, id := range []string{"a1", "a2"} {
		addPending(t, roomy, id)
	}
	if got := roomy.PendingHeldByCapCount(); got != 0 {
		t.Errorf("expected 0 with room in the cap, got %d", got)
	}

	// A cap of 1 with nothing in flight still lets one task start, so two of
	// the three are waiting on the cap rather than on guest matching.
	full, _ := newCappedOrchestrator(t, 1)
	_ = full.RegisterGuest("g1", "G1", nil)
	for _, id := range []string{"b1", "b2", "b3"} {
		addPending(t, full, id)
	}
	if got := full.PendingHeldByCapCount(); got != 2 {
		t.Errorf("expected 2 waiting on the cap with the slot free, got %d", got)
	}

	// The slot fills: now none of the queue can start, whatever the pool does.
	if err := full.AssignTask("b1", "g1"); err != nil {
		t.Fatalf("AssignTask b1: %v", err)
	}
	if got := full.PendingHeldByCapCount(); got != 2 {
		t.Errorf("expected 2 waiting on a full cap, got %d", got)
	}

	// An overnight cap of 0 holds the whole queue.
	full.SetConcurrencyCap(0)
	if got := full.PendingHeldByCapCount(); got != 3 {
		t.Errorf("expected all 3 pending tasks held by a cap of 0, got %d", got)
	}

	// An unlimited cap holds nothing, however long the queue is.
	full.SetConcurrencyCap(config.ConcurrencyUnlimited)
	if got := full.PendingHeldByCapCount(); got != 0 {
		t.Errorf("expected an unlimited cap to hold nothing back, got %d", got)
	}
}
