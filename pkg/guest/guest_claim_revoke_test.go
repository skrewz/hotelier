package guest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"hotelier/pkg/rpc"
)

// logRecorder collects a guest's own process log behind a mutex, so a test can
// assert on it while the dispatcher goroutine is still writing to it.
type logRecorder struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *logRecorder) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

func (l *logRecorder) contains(needle string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return strings.Contains(l.buf.String(), needle)
}

func (l *logRecorder) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

// cancelCapture records the guest.cancelled confirmations a guest sends, so a
// test can assert which task each confirmation named and what the guest
// actually did with it (issue #252).
type cancelCapture struct {
	mu    sync.Mutex
	calls []map[string]interface{}
}

func (c *cancelCapture) record(params json.RawMessage) {
	var m map[string]interface{}
	_ = json.Unmarshal(params, &m)
	c.mu.Lock()
	c.calls = append(c.calls, m)
	c.mu.Unlock()
}

func (c *cancelCapture) all() []map[string]interface{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]map[string]interface{}, len(c.calls))
	copy(out, c.calls)
	return out
}

// captureCancels replaces the server's guest.cancelled handler with one that
// records every confirmation and accepts it.
func captureCancels(hub *rpc.Hub, cap *cancelCapture) {
	hub.RegisterMethod("guest.cancelled",
		func(_ context.Context, params json.RawMessage) (interface{}, *rpc.RPCError) {
			cap.record(params)
			return map[string]interface{}{"status": "accepted"}, nil
		})
}

// waitForTaskRunning blocks until the guest reports the named task as the one
// executing, or fails the test after the deadline.
func waitForTaskRunning(t *testing.T, g *Guest, taskID string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		g.mu.Lock()
		running, current := g.running, g.currentTaskID
		g.mu.Unlock()
		if running && current == taskID {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("task %s never became the running task (running=%v current=%q)", taskID, running, current)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// waitFor blocks until cond holds, failing the test after a generous deadline.
// It replaces the sleeps that used to stand between an action and a positive
// assertion: the check is made as soon as the state arrives, not after an
// arbitrary pause (issue #252 review).
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("%s never happened", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// neverWindow is how long a negative assertion watches for something that must
// not happen. Unlike the fixed sleeps it replaces it is not a guess at how long
// "never" takes: the condition is polled throughout the window, so the test
// fails the instant the forbidden state appears and passes only once the whole
// window has passed without it — the shape a select on a channel gives, which a
// sync.Map offers no handle on (issue #252 review; the class of flake a7abc90
// fixed).
const neverWindow = 2 * time.Second

func assertNever(t *testing.T, what string, within time.Duration, appeared func() bool) {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		if appeared() {
			t.Errorf("%s happened within %s, but must never happen", what, within)
			return
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return
		}
		if remaining < 10*time.Millisecond {
			time.Sleep(remaining)
		} else {
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func assignParams(t *testing.T, task TaskAssignment) json.RawMessage {
	t.Helper()
	params, err := json.Marshal(task)
	if err != nil {
		t.Fatalf("marshal assignment: %v", err)
	}
	return params
}

func cancelParams(t *testing.T, cancel TaskCancel) json.RawMessage {
	t.Helper()
	params, err := json.Marshal(cancel)
	if err != nil {
		t.Fatalf("marshal cancel: %v", err)
	}
	return params
}

// TestGuest_RevokedQueuedClaimNeverStarts covers the first "Verify" bullet of
// issue #252: a task the guest holds but has not started must be dropped when
// the host revokes the claim, never executed later. Before the fix the cancel
// handler only tore down the *running* task's context, so the buffered
// assignment survived and ran as soon as the guest freed up — work the host
// had already closed.
func TestGuest_RevokedQueuedClaimNeverStarts(t *testing.T) {
	srv, wsURL := startTestServer(t)
	cap := &cancelCapture{}
	captureCancels(srv.Hub(), cap)

	release1 := make(chan struct{})
	var started sync.Map

	g := newConnectedGuest(t, wsURL, func(_ context.Context, task TaskAssignment, _ LogCallback) (*TaskResult, error) {
		started.Store(task.TaskID, true)
		if task.TaskID == "task-1" {
			<-release1
		}
		return &TaskResult{TaskID: task.TaskID, Success: true}, nil
	})
	logs := &logRecorder{}
	g.log = log.New(logs, "", 0)
	go g.taskDispatcher(context.Background())

	// task-1 is running; task-2 is held in the queue, unstarted.
	if !g.hub.InvokeNotificationHandler("task.assign", assignParams(t, TaskAssignment{TaskID: "task-1", Prompt: "long task"})) {
		t.Fatal("no task.assign handler registered")
	}
	waitForTaskRunning(t, g, "task-1")

	if !g.hub.InvokeNotificationHandler("task.assign", assignParams(t, TaskAssignment{TaskID: "task-2", Prompt: "queued task"})) {
		t.Fatal("no task.assign handler registered")
	}
	if !g.holdsClaim("task-2") {
		t.Fatal("expected task-2 to be held by the guest before it starts")
	}

	// The host re-queues task-2 and revokes the claim.
	if !g.hub.InvokeNotificationHandler("task.cancel", cancelParams(t, TaskCancel{TaskID: "task-2", Reason: "re-queued"})) {
		t.Fatal("no task.cancel handler registered")
	}

	close(release1)
	assertNever(t, "revoked task-2", neverWindow, func() bool {
		_, ok := started.Load("task-2")
		return ok
	})
	if !logs.contains("cancelled before start") {
		t.Errorf("expected the drop to be logged as 'cancelled before start', got:\n%s", logs.String())
	}

	calls := cap.all()
	if len(calls) != 1 {
		t.Fatalf("expected one guest.cancelled confirmation, got %d", len(calls))
	}
	if got := calls[0]["task_id"]; got != "task-2" {
		t.Errorf("confirmation must name the task it acted on, got %v", got)
	}
	if got := calls[0]["action"]; got != cancelActionBeforeStart {
		t.Errorf("expected action %q, got %v", cancelActionBeforeStart, got)
	}
}

// TestGuest_CancelNamesAnotherTask_DoesNotAbortTheRunningOne covers the third
// "Verify" bullet: a cancel naming task X while an unrelated task Y runs must
// not abort Y, and the guest must answer that it does not hold X. Before the
// fix the handler cancelled "whatever is running" and then confirmed the
// task named in the notification, so the host was told the wrong thing.
func TestGuest_CancelNamesAnotherTask_DoesNotAbortTheRunningOne(t *testing.T) {
	srv, wsURL := startTestServer(t)
	cap := &cancelCapture{}
	captureCancels(srv.Hub(), cap)

	ctxDone := make(chan struct{}, 1)
	g := newConnectedGuest(t, wsURL, func(ctx context.Context, task TaskAssignment, _ LogCallback) (*TaskResult, error) {
		if task.TaskID == "task-running" {
			<-ctx.Done()
			ctxDone <- struct{}{}
		}
		return &TaskResult{TaskID: task.TaskID, Success: true}, nil
	})
	go g.taskDispatcher(context.Background())

	if !g.hub.InvokeNotificationHandler("task.assign", assignParams(t, TaskAssignment{TaskID: "task-running", Prompt: "p"})) {
		t.Fatal("no task.assign handler registered")
	}
	waitForTaskRunning(t, g, "task-running")

	if !g.hub.InvokeNotificationHandler("task.cancel", cancelParams(t, TaskCancel{TaskID: "task-other", Reason: "re-queued"})) {
		t.Fatal("no task.cancel handler registered")
	}

	select {
	case <-ctxDone:
		t.Fatal("an unrelated task.cancel aborted the running task")
	case <-time.After(200 * time.Millisecond):
	}

	calls := cap.all()
	if len(calls) != 1 {
		t.Fatalf("expected one guest.cancelled confirmation, got %d", len(calls))
	}
	if got := calls[0]["task_id"]; got != "task-other" {
		t.Errorf("confirmation must name the task it was about, got %v", got)
	}
	if got := calls[0]["action"]; got != cancelActionNotHeld {
		t.Errorf("expected action %q, got %v", cancelActionNotHeld, got)
	}

	// The running task keeps its cancel context — nothing was torn down.
	g.mu.Lock()
	cancelCleared := g.cancel == nil
	g.mu.Unlock()
	if cancelCleared {
		t.Error("the running task's cancel context was cleared by a cancel for another task")
	}
}

// TestGuest_CancelTaskTheGuestDoesNotHoldIsANoOp asserts the no-op half of
// item 2 in issue #252: a cancel for a task the guest does not hold neither
// cancels nor starts anything, and is reported as such rather than as an
// error.
func TestGuest_CancelTaskTheGuestDoesNotHoldIsANoOp(t *testing.T) {
	srv, wsURL := startTestServer(t)
	cap := &cancelCapture{}
	captureCancels(srv.Hub(), cap)

	var ran sync.Map
	g := newConnectedGuest(t, wsURL, func(_ context.Context, task TaskAssignment, _ LogCallback) (*TaskResult, error) {
		ran.Store(task.TaskID, true)
		return &TaskResult{TaskID: task.TaskID, Success: true}, nil
	})
	logs := &logRecorder{}
	g.log = log.New(logs, "", 0)
	go g.taskDispatcher(context.Background())

	if !g.hub.InvokeNotificationHandler("task.cancel", cancelParams(t, TaskCancel{TaskID: "task-never-assigned", Reason: "re-queued"})) {
		t.Fatal("no task.cancel handler registered")
	}
	assertNever(t, "the unheld task", neverWindow, func() bool {
		_, ok := ran.Load("task-never-assigned")
		return ok
	})
	calls := cap.all()
	if len(calls) != 1 || calls[0]["action"] != cancelActionNotHeld {
		t.Errorf("expected a single not_held confirmation, got %v", calls)
	}
	if !logs.contains("does not hold") {
		t.Errorf("expected a log line stating the claim is not held, got:\n%s", logs.String())
	}
}

// TestGuest_ReassignedAfterRevocation_Runs closes the other side of the
// revocation marker: once the host hands the same task back, the guest must
// honour the new claim rather than dropping it on the stale revocation.
func TestGuest_ReassignedAfterRevocation_Runs(t *testing.T) {
	_, wsURL := startTestServer(t)

	var started sync.Map
	g := newConnectedGuest(t, wsURL, func(_ context.Context, task TaskAssignment, _ LogCallback) (*TaskResult, error) {
		started.Store(task.TaskID, true)
		return &TaskResult{TaskID: task.TaskID, Success: true}, nil
	})
	go g.taskDispatcher(context.Background())

	if !g.hub.InvokeNotificationHandler("task.assign", assignParams(t, TaskAssignment{TaskID: "task-1", Prompt: "p"})) {
		t.Fatal("no task.assign handler registered")
	}
	g.hub.InvokeNotificationHandler("task.cancel", cancelParams(t, TaskCancel{TaskID: "task-1", Reason: "re-queued"}))

	// The claim comes back to the same guest.
	if !g.hub.InvokeNotificationHandler("task.assign", assignParams(t, TaskAssignment{TaskID: "task-1", Prompt: "p"})) {
		t.Fatal("no task.assign handler registered")
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := started.Load("task-1"); ok {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Error("a task re-assigned after a revocation was never executed")
}

// TestGuest_DoesNotExecute_WhenAckRefused covers item 4 of issue #252: a
// refused acknowledgement is the host saying "this is not yours", so the
// guest must not run the task.
func TestGuest_DoesNotExecute_WhenAckRefused(t *testing.T) {
	srv, wsURL := startTestServer(t)
	srv.Hub().RegisterMethod("task.acknowledge",
		func(_ context.Context, _ json.RawMessage) (interface{}, *rpc.RPCError) {
			return nil, rpc.ClaimRefusedError("task task-1 is not assigned (status: COMPLETED)")
		})

	var mu sync.Mutex
	var ran bool
	g := newConnectedGuest(t, wsURL, func(_ context.Context, task TaskAssignment, _ LogCallback) (*TaskResult, error) {
		mu.Lock()
		ran = true
		mu.Unlock()
		return &TaskResult{TaskID: task.TaskID, Success: true}, nil
	})

	logs := &logRecorder{}
	g.log = log.New(logs, "", 0)

	if _, err := g.ExecuteTask(TaskAssignment{TaskID: "task-1", Prompt: "p"}); err == nil {
		t.Fatal("expected ExecuteTask to drop a claim the server refused")
	}
	mu.Lock()
	ranNow := ran
	mu.Unlock()
	if ranNow {
		t.Error("guest executed a task whose acknowledgement was refused")
	}
	if !logs.contains("not honoured") {
		t.Errorf("expected an info line stating the claim was not honoured, got:\n%s", logs.String())
	}
	if n := g.ackRefusals(); n != 1 {
		t.Errorf("expected 1 ack failure counted, got %d", n)
	}
}

// TestGuest_Executes_WhenAckTransportFails keeps the old behaviour for the
// transport case: a call that never reached the server is not a decision
// about the claim, and the reconnect/retry work owns that case.
func TestGuest_Executes_WhenAckTransportFails(t *testing.T) {
	srv, wsURL := startTestServer(t)
	srv.Hub().RegisterMethod("task.acknowledge",
		func(_ context.Context, _ json.RawMessage) (interface{}, *rpc.RPCError) {
			return nil, rpc.InternalError("task task-1 not found")
		})

	var mu sync.Mutex
	var ran bool
	g := newConnectedGuest(t, wsURL, func(_ context.Context, task TaskAssignment, _ LogCallback) (*TaskResult, error) {
		mu.Lock()
		ran = true
		mu.Unlock()
		return &TaskResult{TaskID: task.TaskID, Success: true}, nil
	})

	result, err := g.ExecuteTask(TaskAssignment{TaskID: "task-1", Prompt: "p"})
	if err != nil {
		t.Fatalf("ExecuteTask: %v", err)
	}
	if result == nil || !result.Success {
		t.Fatalf("expected the task to run, got %+v", result)
	}
	mu.Lock()
	ranNow := ran
	mu.Unlock()
	if !ranNow {
		t.Error("a transport-level ack failure must not drop the claim")
	}
	if n := g.ackRefusals(); n != 0 {
		t.Errorf("a transport failure is not an ack refusal, counted %d", n)
	}
}

// TestGuest_TwoGuestsOneTask_ExactlyOneExecutes walks the last "Verify"
// bullet of issue #252 end to end: the task is re-queued off a busy guest and
// handed to another. The guest that lost the claim must never start it, must
// never even try to acknowledge it, and only one guest may run it.
func TestGuest_TwoGuestsOneTask_ExactlyOneExecutes(t *testing.T) {
	srv, wsURL := startTestServer(t)

	var executed sync.Map
	var acked sync.Map
	srv.Hub().RegisterMethod("task.acknowledge",
		func(_ context.Context, params json.RawMessage) (interface{}, *rpc.RPCError) {
			var req struct {
				TaskID  string `json:"task_id"`
				GuestID string `json:"guest_id"`
			}
			_ = json.Unmarshal(params, &req)
			acked.Store(req.GuestID+"/"+req.TaskID, true)
			return map[string]interface{}{"status": "ok"}, nil
		})

	handler := func(_ context.Context, task TaskAssignment, _ LogCallback) (*TaskResult, error) {
		executed.Store(task.TaskID, true)
		if task.TaskID == "busy-1" {
			// Guest B is busy: the re-queued task sits in its queue.
			time.Sleep(2 * time.Second)
		}
		return &TaskResult{TaskID: task.TaskID, Success: true}, nil
	}

	guestA := newConnectedGuest(t, wsURL, handler)
	guestB := newConnectedGuest(t, wsURL, handler)
	defer guestA.Stop()
	defer guestB.Stop()
	go guestA.taskDispatcher(context.Background())
	go guestB.taskDispatcher(context.Background())

	// Guest B is mid-task when the assignment arrives, so it holds it queued.
	if !guestB.hub.InvokeNotificationHandler("task.assign", assignParams(t, TaskAssignment{TaskID: "busy-1", Prompt: "p"})) {
		t.Fatal("no task.assign handler registered")
	}
	waitForTaskRunning(t, guestB, "busy-1")
	if !guestB.hub.InvokeNotificationHandler("task.assign", assignParams(t, TaskAssignment{TaskID: "shared-1", Prompt: "p"})) {
		t.Fatal("no task.assign handler registered")
	}

	// The host re-queues the task and assigns it to guest A: the claim is
	// revoked from B first, then A is handed the assignment.
	if !guestB.hub.InvokeNotificationHandler("task.cancel", cancelParams(t, TaskCancel{TaskID: "shared-1", Reason: "re-queued", Intent: "revoke"})) {
		t.Fatal("no task.cancel handler registered")
	}
	if !guestA.hub.InvokeNotificationHandler("task.assign", assignParams(t, TaskAssignment{TaskID: "shared-1", Prompt: "p"})) {
		t.Fatal("no task.assign handler registered")
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := executed.Load("shared-1"); ok {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, ok := executed.Load("shared-1"); !ok {
		t.Fatal("the task was never executed by either guest")
	}
	assertNever(t, "guest B's acknowledgement of the revoked claim", neverWindow, func() bool {
		_, ok := acked.Load(guestB.ID() + "/shared-1")
		return ok
	})

	if _, ok := acked.Load(guestB.ID() + "/shared-1"); ok {
		t.Error("guest B acknowledged a claim the host had revoked — the ack-failure counter would climb")
	}
	if _, ok := acked.Load(guestA.ID() + "/shared-1"); !ok {
		t.Error("guest A never acknowledged the claim it was given")
	}
	guestB.mu.Lock()
	bRunning := guestB.currentTaskID
	guestB.mu.Unlock()
	if bRunning == "shared-1" {
		t.Error("guest B is running a task the host revoked from it")
	}
}

// TestGuest_CancelNamesTheRunningTask_AbortsItAndConfirms is the case the
// silence path already relied on, now reached through identity matching
// rather than "whatever is running": the named task is aborted and the
// confirmation says so (issue #252).
func TestGuest_CancelNamesTheRunningTask_AbortsItAndConfirms(t *testing.T) {
	srv, wsURL, results := startTestServerWithCapture(t)
	cap := &cancelCapture{}
	captureCancels(srv.Hub(), cap)

	aborted := make(chan bool, 1)
	g := newConnectedGuest(t, wsURL, func(ctx context.Context, task TaskAssignment, _ LogCallback) (*TaskResult, error) {
		<-ctx.Done()
		aborted <- true
		return &TaskResult{TaskID: task.TaskID, Success: false, Error: "cancelled"}, nil
	})
	logs := &logRecorder{}
	g.log = log.New(logs, "", 0)
	go g.taskDispatcher(context.Background())

	if !g.hub.InvokeNotificationHandler("task.assign", assignParams(t, TaskAssignment{TaskID: "task-run", Prompt: "p"})) {
		t.Fatal("no task.assign handler registered")
	}
	waitForTaskRunning(t, g, "task-run")

	if !g.hub.InvokeNotificationHandler("task.cancel", cancelParams(t, TaskCancel{TaskID: "task-run", Reason: "guest silence timeout"})) {
		t.Fatal("no task.cancel handler registered")
	}

	select {
	case <-aborted:
	case <-time.After(3 * time.Second):
		t.Fatal("the running task was not aborted by a cancel naming it")
	}

	deadline := time.Now().Add(3 * time.Second)
	for {
		calls := cap.all()
		if len(calls) > 0 {
			if calls[0]["action"] != cancelActionRunning {
				t.Errorf("expected action %q, got %v", cancelActionRunning, calls[0]["action"])
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no guest.cancelled confirmation arrived")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !logs.contains("cancelled while running") {
		t.Errorf("expected the abort to be logged as 'cancelled while running', got:\n%s", logs.String())
	}
	// The cancel carried no intent — the pre-existing "end the task"
	// meaning — so the aborted task still reports its result: the host
	// resolves the confirmation as a cancel and the result lands on a
	// task it has already ended. Only a revoke suppresses the result
	// (#252 review).
	//
	// The confirmation is sent by the cancel handler while this task's
	// handler is still unwinding, so it can arrive before the result it
	// does not suppress — wait for the result rather than reading it at
	// the moment the confirmation is seen.
	if !results.waitForResults(1, 3*time.Second) {
		t.Fatal("the cancelled task never reported its result")
	}
	if got := results.capturedResults(); len(got) != 1 || got[0].TaskID != "task-run" {
		t.Errorf("expected one guest.result for the cancelled task, got %+v", got)
	}
}

// TestGuest_RevokeAbortsRunningTask_ReportsNoResult is the revoke half of
// the same path. A revocation takes the claim back and hands the task's
// fate to the host — which has already re-queued it, possibly onto another
// guest. If the aborted guest still reported its failure, the host's
// FailTask would end a task another guest may be executing: the fate rule
// ("the host's acknowledgement is the authority on whether the work is
// still its own") not carried through to the abort path (issue #252
// review).
func TestGuest_RevokeAbortsRunningTask_ReportsNoResult(t *testing.T) {
	srv, wsURL, results := startTestServerWithCapture(t)
	cap := &cancelCapture{}
	captureCancels(srv.Hub(), cap)

	aborted := make(chan bool, 1)
	g := newConnectedGuest(t, wsURL, func(ctx context.Context, task TaskAssignment, _ LogCallback) (*TaskResult, error) {
		<-ctx.Done()
		aborted <- true
		return &TaskResult{TaskID: task.TaskID, Success: false, Error: "context canceled"}, ctx.Err()
	})
	logs := &logRecorder{}
	g.log = log.New(logs, "", 0)
	go g.taskDispatcher(context.Background())

	if !g.hub.InvokeNotificationHandler("task.assign", assignParams(t, TaskAssignment{TaskID: "task-run", Prompt: "p"})) {
		t.Fatal("no task.assign handler registered")
	}
	waitForTaskRunning(t, g, "task-run")

	if !g.hub.InvokeNotificationHandler("task.cancel", cancelParams(t, TaskCancel{TaskID: "task-run", Reason: "re-queued", Intent: "revoke"})) {
		t.Fatal("no task.cancel handler registered")
	}

	select {
	case <-aborted:
	case <-time.After(3 * time.Second):
		t.Fatal("the running task was not aborted by a revoke naming it")
	}

	deadline := time.Now().Add(3 * time.Second)
	for {
		calls := cap.all()
		if len(calls) > 0 {
			if calls[0]["action"] != cancelActionRunning {
				t.Errorf("expected action %q, got %v", cancelActionRunning, calls[0]["action"])
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no guest.cancelled confirmation arrived")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// No guest.result may arrive: the revoked guest reports nothing and
	// leaves the fate to the host. waitForResults fails the check the moment
	// one does rather than after a fixed pause.
	if results.waitForResults(1, neverWindow) {
		t.Errorf("a revoked-while-running task must not report a result, got %+v",
			results.capturedResults())
	}
	if !logs.contains("revoked while running; not reporting the result") {
		t.Errorf("expected the suppressed result to be logged, got:\n%s", logs.String())
	}
	g.mu.Lock()
	_, stillMarked := g.givenUp["task-run"]
	g.mu.Unlock()
	if stillMarked {
		t.Error("the given-up record survived the aborted task")
	}
}

// TestGuest_DuplicateAssignForQueuedTaskIsDropped covers the re-send half
// of the deduplication guard: before the claims map existed the guard keyed
// on the running task alone, so a re-sent assignment for a task that was
// queued but not yet started was buffered twice — the dispatcher ran one
// copy and the "already running" decline for the other handed the host a
// re-queue of work that was in flight (#252 review).
func TestGuest_DuplicateAssignForQueuedTaskIsDropped(t *testing.T) {
	_, wsURL := startTestServer(t)

	release1 := make(chan struct{})
	var started sync.Map
	g := newConnectedGuest(t, wsURL, func(_ context.Context, task TaskAssignment, _ LogCallback) (*TaskResult, error) {
		started.Store(task.TaskID, true)
		if task.TaskID == "task-1" {
			<-release1
		}
		return &TaskResult{TaskID: task.TaskID, Success: true}, nil
	})
	logs := &logRecorder{}
	g.log = log.New(logs, "", 0)
	go g.taskDispatcher(context.Background())

	// task-1 holds the dispatcher; task-2 sits in the queue, unstarted.
	if !g.hub.InvokeNotificationHandler("task.assign", assignParams(t, TaskAssignment{TaskID: "task-1", Prompt: "long task"})) {
		t.Fatal("no task.assign handler registered")
	}
	waitForTaskRunning(t, g, "task-1")
	if !g.hub.InvokeNotificationHandler("task.assign", assignParams(t, TaskAssignment{TaskID: "task-2", Prompt: "queued task"})) {
		t.Fatal("no task.assign handler registered")
	}

	// The host re-sends the assignment for the queued task — a reconnect
	// replay of what this guest already holds.
	if !g.hub.InvokeNotificationHandler("task.assign", assignParams(t, TaskAssignment{TaskID: "task-2", Prompt: "queued task"})) {
		t.Fatal("no task.assign handler registered")
	}
	if !logs.contains("ignoring duplicate task.assign for task-2") {
		t.Errorf("expected the re-sent queued assignment to be ignored, got:\n%s", logs.String())
	}

	close(release1)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := started.Load("task-2"); ok {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, ok := started.Load("task-2"); !ok {
		t.Fatal("the queued task never ran")
	}
	// The single held copy ran exactly once: nothing was buffered twice for
	// the dispatcher to decline and re-queue, and no second claim record
	// appeared for it either.
	assertNever(t, "task-2's claim record reappearing", neverWindow, func() bool {
		return g.holdsClaim("task-2")
	})
}

// TestGuest_QueuedClaimIsHeldTheMomentAssignIsHandled closes the window the
// claim record exists to close: the record must exist before the channel
// send, because the send is the moment the dispatcher can take the
// assignment. A cancel handled immediately after task.assign returns must
// find the claim and answer cancelled_before_start, never not_held (issue
// #252 review).
func TestGuest_QueuedClaimIsHeldTheMomentAssignIsHandled(t *testing.T) {
	srv, wsURL := startTestServer(t)
	cap := &cancelCapture{}
	captureCancels(srv.Hub(), cap)

	release1 := make(chan struct{})
	var started sync.Map
	g := newConnectedGuest(t, wsURL, func(_ context.Context, task TaskAssignment, _ LogCallback) (*TaskResult, error) {
		started.Store(task.TaskID, true)
		if task.TaskID == "task-1" {
			<-release1
		}
		return &TaskResult{TaskID: task.TaskID, Success: true}, nil
	})
	go g.taskDispatcher(context.Background())

	if !g.hub.InvokeNotificationHandler("task.assign", assignParams(t, TaskAssignment{TaskID: "task-1", Prompt: "long task"})) {
		t.Fatal("no task.assign handler registered")
	}
	waitForTaskRunning(t, g, "task-1")

	if !g.hub.InvokeNotificationHandler("task.assign", assignParams(t, TaskAssignment{TaskID: "task-2", Prompt: "queued task"})) {
		t.Fatal("no task.assign handler registered")
	}
	// No sleep: the cancel is handled immediately after the assign
	// returns, the earliest it could possibly arrive.
	if !g.hub.InvokeNotificationHandler("task.cancel", cancelParams(t, TaskCancel{TaskID: "task-2", Reason: "re-queued"})) {
		t.Fatal("no task.cancel handler registered")
	}

	calls := cap.all()
	if len(calls) != 1 {
		t.Fatalf("expected one guest.cancelled confirmation, got %d", len(calls))
	}
	if got := calls[0]["action"]; got != cancelActionBeforeStart {
		t.Errorf("a cancel immediately after the assign must find the claim, got action %v", got)
	}

	close(release1)
	assertNever(t, "task-2 after a cancelled_before_start confirmation", neverWindow, func() bool {
		_, ok := started.Load("task-2")
		return ok
	})
}

// TestGuest_QueueFullAssignmentLeavesNoClaim asserts the undo half of the
// same handler: an assignment the guest could not queue is not held, so a
// later cancel must not find a phantom claim for it.
func TestGuest_QueueFullAssignmentLeavesNoClaim(t *testing.T) {
	_, wsURL := startTestServer(t)

	release1 := make(chan struct{})
	g := newConnectedGuest(t, wsURL, func(_ context.Context, task TaskAssignment, _ LogCallback) (*TaskResult, error) {
		if task.TaskID == "task-1" {
			<-release1
		}
		return &TaskResult{TaskID: task.TaskID, Success: true}, nil
	})
	go g.taskDispatcher(context.Background())

	if !g.hub.InvokeNotificationHandler("task.assign", assignParams(t, TaskAssignment{TaskID: "task-1", Prompt: "long task"})) {
		t.Fatal("no task.assign handler registered")
	}
	waitForTaskRunning(t, g, "task-1")

	// The dispatcher stays inside ExecuteTask(task-1), so nothing drains
	// the 16-slot buffer: task-2..task-17 fill it and task-overflow does
	// not fit.
	for i := 2; i <= 17; i++ {
		id := fmt.Sprintf("task-%d", i)
		if !g.hub.InvokeNotificationHandler("task.assign", assignParams(t, TaskAssignment{TaskID: id, Prompt: "p"})) {
			t.Fatal("no task.assign handler registered")
		}
	}
	if !g.hub.InvokeNotificationHandler("task.assign", assignParams(t, TaskAssignment{TaskID: "task-overflow", Prompt: "p"})) {
		t.Fatal("no task.assign handler registered")
	}

	if g.holdsClaim("task-overflow") {
		t.Error("a dropped assignment must leave no claim behind")
	}
	if !g.holdsClaim("task-17") {
		t.Error("an assignment that fit the queue must be held")
	}
	close(release1)
}

// TestGuest_DispatcherExitPrunesQueuedClaims covers the pruning half of the
// issue #252 review: when the dispatcher exits — connection lost or the
// guest stopping — the claims for tasks still waiting on it must not
// survive it, or a later cancel answers cancelled_before_start for work the
// guest no longer has anywhere to run.
func TestGuest_DispatcherExitPrunesQueuedClaims(t *testing.T) {
	_, wsURL := startTestServer(t)

	var started sync.Map
	g := newConnectedGuest(t, wsURL, func(_ context.Context, task TaskAssignment, _ LogCallback) (*TaskResult, error) {
		started.Store(task.TaskID, true)
		return &TaskResult{TaskID: task.TaskID, Success: true}, nil
	})

	// Simulate the guest being busy on another connection so the
	// dispatcher parks in waitForIdle, however it picks up the assignment.
	g.mu.Lock()
	g.running = true
	g.mu.Unlock()

	dispatcherCtx, cancelDispatcher := context.WithCancel(context.Background())
	go g.taskDispatcher(dispatcherCtx)

	if !g.hub.InvokeNotificationHandler("task.assign", assignParams(t, TaskAssignment{TaskID: "task-pending", Prompt: "p"})) {
		t.Fatal("no task.assign handler registered")
	}
	if !g.holdsClaim("task-pending") {
		t.Fatal("expected the assignment to be claimed")
	}

	cancelDispatcher()
	waitFor(t, "the exited dispatcher to prune the queued claim", func() bool {
		return !g.holdsClaim("task-pending")
	})

	g.mu.Lock()
	g.running = false
	g.mu.Unlock()
	assertNever(t, "the dropped task", neverWindow, func() bool {
		_, ok := started.Load("task-pending")
		return ok
	})
}

// TestGuest_AlreadyRunningDeclinePrunesClaims is the second pruning hole:
// ExecuteTask returns "already running" before its own claim record is
// touched, and the decline that follows hands the task back to the host —
// so the claim must go with it.
func TestGuest_AlreadyRunningDeclinePrunesClaims(t *testing.T) {
	_, wsURL := startTestServer(t)
	g := newConnectedGuest(t, wsURL, func(_ context.Context, task TaskAssignment, _ LogCallback) (*TaskResult, error) {
		return &TaskResult{TaskID: task.TaskID, Success: true}, nil
	})

	g.mu.Lock()
	g.running = true
	g.currentTaskID = "task-running"
	g.claims["task-declined"] = struct{}{}
	g.revoked["task-declined"] = struct{}{}
	g.mu.Unlock()

	_, err := g.ExecuteTask(TaskAssignment{TaskID: "task-declined", Prompt: "p"})
	if err == nil || !strings.Contains(err.Error(), "already running") {
		t.Fatalf("expected an 'already running' error, got %v", err)
	}

	g.mu.Lock()
	_, heldClaims := g.claims["task-declined"]
	_, heldRevoked := g.revoked["task-declined"]
	g.mu.Unlock()
	if heldClaims || heldRevoked {
		t.Error("the declined task's claim records survived the 'already running' return")
	}

	g.mu.Lock()
	g.running = false
	g.currentTaskID = ""
	g.mu.Unlock()
}

// TestGuest_ReassignmentDuringRevokeAbort_RunsOnce closes the dedup hole the
// abort leaves open: a given-up record only exists while the aborted run is
// current, so a re-assignment of the re-queued task arriving while that run is
// still unwinding looks exactly like a duplicate assignment. Filing it as one
// drops the work the host just handed back and clears the record that keeps
// the aborted run quiet — the stray result the revocation is meant to
// suppress, back through a narrower door (issue #252 review).
func TestGuest_ReassignmentDuringRevokeAbort_RunsOnce(t *testing.T) {
	srv, wsURL, results := startTestServerWithCapture(t)
	cap := &cancelCapture{}
	captureCancels(srv.Hub(), cap)

	var runs atomic.Int64
	aborted := make(chan struct{})
	release := make(chan struct{})
	g := newConnectedGuest(t, wsURL, func(ctx context.Context, task TaskAssignment, _ LogCallback) (*TaskResult, error) {
		if runs.Add(1) == 1 {
			// First run: aborted by the revoke, and slow to unwind — the
			// re-assignment lands while this handler is still returning.
			<-ctx.Done()
			close(aborted)
			<-release
			return nil, ctx.Err()
		}
		return &TaskResult{TaskID: task.TaskID, Success: true}, nil
	})
	logs := &logRecorder{}
	g.log = log.New(logs, "", 0)
	go g.taskDispatcher(context.Background())

	if !g.hub.InvokeNotificationHandler("task.assign", assignParams(t, TaskAssignment{TaskID: "task-run", Prompt: "p"})) {
		t.Fatal("no task.assign handler registered")
	}
	waitForTaskRunning(t, g, "task-run")

	if !g.hub.InvokeNotificationHandler("task.cancel", cancelParams(t, TaskCancel{TaskID: "task-run", Reason: "re-queued", Intent: "revoke"})) {
		t.Fatal("no task.cancel handler registered")
	}
	select {
	case <-aborted:
	case <-time.After(3 * time.Second):
		t.Fatal("the running task was not aborted by the revoke")
	}

	// The host re-queued the task and the next heartbeat hands it back to
	// this same guest — the common single-guest case — before the aborted
	// handler has returned.
	if !g.hub.InvokeNotificationHandler("task.assign", assignParams(t, TaskAssignment{TaskID: "task-run", Prompt: "p"})) {
		t.Fatal("no task.assign handler registered")
	}
	if logs.contains("ignoring duplicate task.assign") {
		t.Errorf("a task whose claim the guest gave up is not one it holds; the re-assignment must be queued, got:\n%s", logs.String())
	}

	close(release)

	deadline := time.Now().Add(3 * time.Second)
	for runs.Load() < 2 {
		if time.Now().After(deadline) {
			t.Fatalf("the re-assigned task never ran after the abort unwound (runs=%d)", runs.Load())
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Exactly one result — the second run's. The aborted run must stay
	// silent: its error is a revoked claim, not the task's fate, and the
	// task is running again here.
	if !results.waitForResults(1, 3*time.Second) {
		t.Fatal("the re-assigned task never reported its result")
	}
	if got := results.capturedResults(); len(got) != 1 || !got[0].Success || got[0].Error != "" {
		t.Errorf("expected one successful result from the re-assigned run and silence from the aborted one, got %+v", got)
	}
}
