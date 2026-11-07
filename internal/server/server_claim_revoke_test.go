package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"hotelier/pkg/queue"
	"hotelier/pkg/rpc"
)

// connectTestGuest registers a guest in the registry and gives it a test
// connection, returning that connection so the test can read what the server
// pushed to the guest.
func connectTestGuest(t *testing.T, srv *Server, guestID string) *rpc.Connection {
	t.Helper()
	hub := srv.Hub()
	go hub.Run()

	if _, err := srv.Registry().Register(guestID, "Test Guest", []string{"default"}); err != nil {
		t.Fatalf("register guest: %v", err)
	}
	conn := rpc.NewTestConnection("conn-"+guestID, hub)
	hub.Register(conn)
	hub.RegisterGuestConnection(guestID, "conn-"+guestID)
	time.Sleep(10 * time.Millisecond)
	return conn
}

// recvCancel reads the next message the test connection received and decodes
// it as a task.cancel notification.
func recvCancel(t *testing.T, conn *rpc.Connection) map[string]interface{} {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		data, ok := conn.Recv()
		if ok {
			var msg rpc.JSONRPCMessage
			if err := json.Unmarshal(data, &msg); err != nil {
				t.Fatalf("unmarshal message: %v", err)
			}
			if msg.Method != "task.cancel" {
				continue
			}
			var params map[string]interface{}
			if err := json.Unmarshal(msg.Params, &params); err != nil {
				t.Fatalf("unmarshal cancel params: %v", err)
			}
			return params
		}
		if time.Now().After(deadline) {
			t.Fatal("expected a task.cancel notification, received none")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// addAssignedTask creates a task and assigns it to a guest without going
// through the assignment paths under test.
func addAssignedTask(t *testing.T, srv *Server, id, guestID string) *queue.Task {
	t.Helper()
	task := &queue.Task{ID: id, Prompt: "p", CreatedAt: time.Now()}
	if err := srv.Orchestrator().AddTask(task); err != nil {
		t.Fatalf("add task: %v", err)
	}
	if err := srv.Orchestrator().AssignTask(id, guestID); err != nil {
		t.Fatalf("assign task: %v", err)
	}
	return task
}

// TestCheckStuckTasks_RevokesClaimOnTheHolder is the core of issue #252: the
// stuck-task probe moves the task back to PENDING and clears its own record,
// but the holder still has the assignment queued and runs it later — work the
// host believes it is not doing. The probe must revoke the claim first.
func TestCheckStuckTasks_RevokesClaimOnTheHolder(t *testing.T) {
	srv := newTestServer(t)
	srv.cfg.TaskAssignmentTimeout = 1
	conn := connectTestGuest(t, srv, "guest-1")

	task := addAssignedTask(t, srv, "stuck-1", "guest-1")
	task.AssignedAt = time.Now().Add(-30 * time.Second)
	conn.Drain()

	srv.checkStuckTasks()

	params := recvCancel(t, conn)
	if params["task_id"] != "stuck-1" {
		t.Errorf("cancel must name the re-queued task, got %v", params["task_id"])
	}
	if params["reason"] != claimReasonRequeued {
		t.Errorf("expected reason %q, got %v", claimReasonRequeued, params["reason"])
	}
	if params["intent"] != cancelIntentRevoke {
		t.Errorf("expected intent %q, got %v", cancelIntentRevoke, params["intent"])
	}

	if got, _ := srv.TaskQueue().Get("stuck-1"); got.Status != queue.TaskStatusPending {
		t.Errorf("expected the stuck task re-queued to PENDING, got %s", got.Status)
	}
}

// TestHandleGuestDisconnect_RevokesClaimBeforeRequeue covers the disconnect
// re-queue path: the task goes back to PENDING, so the guest that still holds
// it must be told before it runs it.
func TestHandleGuestDisconnect_RevokesClaimBeforeRequeue(t *testing.T) {
	srv := newTestServer(t)
	conn := connectTestGuest(t, srv, "guest-1")

	addAssignedTask(t, srv, "disc-1", "guest-1")
	conn.Drain()

	srv.handleGuestDisconnect(conn.ID())

	params := recvCancel(t, conn)
	if params["task_id"] != "disc-1" {
		t.Errorf("cancel must name the re-queued task, got %v", params["task_id"])
	}
	if params["reason"] != claimReasonConnectionLost {
		t.Errorf("expected reason %q, got %v", claimReasonConnectionLost, params["reason"])
	}

	got, ok := srv.TaskQueue().Get("disc-1")
	if !ok {
		t.Fatal("task not found")
	}
	if got.Status != queue.TaskStatusPending {
		t.Errorf("expected the task re-queued to PENDING, got %s", got.Status)
	}
}

// TestHandleGuestCancelled_NotHeld_LeavesTaskAlone: a guest that does not hold
// the task must not be able to cancel it. Before the fix the guest confirmed
// every cancel it received — whatever it was running — and the host cancelled
// the task the confirmation named.
func TestHandleGuestCancelled_NotHeld_LeavesTaskAlone(t *testing.T) {
	srv := newTestServer(t)
	go srv.Hub().Run()
	if _, err := srv.Registry().Register("guest-1", "Test Guest", []string{"default"}); err != nil {
		t.Fatalf("register: %v", err)
	}
	if _, err := srv.Registry().Register("guest-2", "Other Guest", []string{"default"}); err != nil {
		t.Fatalf("register: %v", err)
	}

	addAssignedTask(t, srv, "claimed-1", "guest-2")

	params, _ := json.Marshal(map[string]interface{}{
		"task_id":  "claimed-1",
		"guest_id": "guest-1",
		"reason":   "re-queued",
		"action":   "not_held",
		"intent":   cancelIntentRevoke,
	})
	if _, rpcErr := srv.handleGuestCancelled(nil, params); rpcErr != nil {
		t.Fatalf("unexpected rpc error: %v", rpcErr)
	}

	task, ok := srv.TaskQueue().Get("claimed-1")
	if !ok {
		t.Fatal("task not found")
	}
	if task.Status != queue.TaskStatusAssigned {
		t.Errorf("a guest that does not hold the claim must not change its status, got %s", task.Status)
	}
	if task.AssignedTo != "guest-2" {
		t.Errorf("assignment must be untouched, got %q", task.AssignedTo)
	}
}

// TestHandleGuestCancelled_RevokeKeepsTaskPending: the host re-queued the task
// before revoking the claim, so the guest's confirmation must not cancel the
// task that is already back in the queue.
func TestHandleGuestCancelled_RevokeKeepsTaskPending(t *testing.T) {
	srv := newTestServer(t)
	go srv.Hub().Run()
	if _, err := srv.Registry().Register("guest-1", "Test Guest", []string{"default"}); err != nil {
		t.Fatalf("register: %v", err)
	}
	addAssignedTask(t, srv, "requeue-1", "guest-1")
	if err := srv.Orchestrator().RequeueTask("requeue-1"); err != nil {
		t.Fatalf("requeue: %v", err)
	}

	params, _ := json.Marshal(map[string]interface{}{
		"task_id":  "requeue-1",
		"guest_id": "guest-1",
		"reason":   "re-queued",
		"action":   "cancelled_before_start",
		"intent":   cancelIntentRevoke,
	})
	if _, rpcErr := srv.handleGuestCancelled(nil, params); rpcErr != nil {
		t.Fatalf("unexpected rpc error: %v", rpcErr)
	}

	task, ok := srv.TaskQueue().Get("requeue-1")
	if !ok {
		t.Fatal("task not found")
	}
	if task.Status != queue.TaskStatusPending {
		t.Errorf("a revoked claim must leave the re-queued task PENDING, got %s", task.Status)
	}

	stats := srv.claimStats()
	if stats.CancelledBeforeStart != 1 {
		t.Errorf("expected 1 'cancelled before start', got %d", stats.CancelledBeforeStart)
	}
	if stats.CancelledWhileRunning != 0 {
		t.Errorf("expected 0 'cancelled while running', got %d", stats.CancelledWhileRunning)
	}
}

// TestHandleGuestCancelled_CancelStillCancels keeps the original behaviour for
// an explicit cancellation: the guest is the authority on whether it stopped,
// and its confirmation ends the task.
func TestHandleGuestCancelled_CancelStillCancels(t *testing.T) {
	srv := newTestServer(t)
	go srv.Hub().Run()
	if _, err := srv.Registry().Register("guest-1", "Test Guest", []string{"default"}); err != nil {
		t.Fatalf("register: %v", err)
	}
	addAssignedTask(t, srv, "cancel-1", "guest-1")
	if err := srv.Orchestrator().Queue().Start("cancel-1"); err != nil {
		t.Fatalf("start: %v", err)
	}
	srv.Registry().SetGuestTask("guest-1", "cancel-1")

	params, _ := json.Marshal(map[string]interface{}{
		"task_id":  "cancel-1",
		"guest_id": "guest-1",
		"reason":   "user requested cancellation",
		"action":   "cancelled_while_running",
	})
	if _, rpcErr := srv.handleGuestCancelled(nil, params); rpcErr != nil {
		t.Fatalf("unexpected rpc error: %v", rpcErr)
	}

	task, ok := srv.TaskQueue().Get("cancel-1")
	if !ok {
		t.Fatal("task not found")
	}
	if task.Status != queue.TaskStatusCancelled {
		t.Errorf("expected CANCELLED, got %s", task.Status)
	}
	if stats := srv.claimStats(); stats.CancelledWhileRunning != 1 {
		t.Errorf("expected 1 'cancelled while running', got %d", stats.CancelledWhileRunning)
	}
}

// TestHandleTaskAcknowledge_CountsRefusal makes a recurrence of the issue #252
// failures one number instead of a forensic exercise: every ack the host
// refuses is counted.
func TestHandleTaskAcknowledge_CountsRefusal(t *testing.T) {
	srv := newTestServer(t)
	go srv.Hub().Run()
	if _, err := srv.Registry().Register("guest-1", "Test Guest", []string{"default"}); err != nil {
		t.Fatalf("register: %v", err)
	}
	addAssignedTask(t, srv, "ack-1", "guest-1")
	if err := srv.Orchestrator().Queue().Complete("ack-1", "done"); err != nil {
		t.Fatalf("complete: %v", err)
	}

	params, _ := json.Marshal(map[string]interface{}{
		"task_id":  "ack-1",
		"guest_id": "guest-1",
	})
	if _, rpcErr := srv.handleTaskAcknowledge(nil, params); rpcErr == nil {
		t.Fatal("expected the ack of a completed task to be refused")
	}

	if stats := srv.claimStats(); stats.AckRefusals != 1 {
		t.Errorf("expected 1 ack failure counted, got %d", stats.AckRefusals)
	}
}

// TestHandleGuests_ExposesClaimStats verifies the counters are readable
// without trawling the log.
func TestHandleGuests_ExposesClaimStats(t *testing.T) {
	srv := newTestServer(t)
	go srv.Hub().Run()
	if _, err := srv.Registry().Register("guest-1", "Test Guest", []string{"default"}); err != nil {
		t.Fatalf("register: %v", err)
	}

	// One revoked claim that had not started, and one ack refusal.
	addAssignedTask(t, srv, "revoke-1", "guest-1")
	if err := srv.Orchestrator().RequeueTask("revoke-1"); err != nil {
		t.Fatalf("requeue: %v", err)
	}
	confirmation, _ := json.Marshal(map[string]interface{}{
		"task_id":  "revoke-1",
		"guest_id": "guest-1",
		"action":   "cancelled_before_start",
		"intent":   cancelIntentRevoke,
	})
	if _, rpcErr := srv.handleGuestCancelled(nil, confirmation); rpcErr != nil {
		t.Fatalf("guest.cancelled: %v", rpcErr)
	}

	w := httptest.NewRecorder()
	srv.HandleGuests(w, httptest.NewRequest(http.MethodGet, "/api/guests", nil))

	var body struct {
		Stats struct {
			CancelledWhileRunning int `json:"cancelled_while_running"`
			CancelledBeforeStart  int `json:"cancelled_before_start"`
			AckRefusals           int `json:"ack_refusals"`
		} `json:"stats"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode /api/guests: %v (body: %s)", err, w.Body.String())
	}
	if body.Stats.CancelledBeforeStart != 1 {
		t.Errorf("expected cancelled_before_start 1, got %d", body.Stats.CancelledBeforeStart)
	}
}

// TestHandleGuestCancelled_NotHeldWithCancelIntent_EndsTask covers the user
// cancel that arrives as a not_held confirmation: the host believes the task
// is RUNNING, but the guest no longer holds it (it restarted, or dropped the
// claim after a refused acknowledgement). The cancel was requested by the
// HTTP caller, not by this guest, so the confirmation must end the task —
// returning early would leave it RUNNING with no holder until the stuck-task
// probe re-queued it, running work the user had explicitly cancelled.
func TestHandleGuestCancelled_NotHeldWithCancelIntent_EndsTask(t *testing.T) {
	srv := newTestServer(t)
	go srv.Hub().Run()
	if _, err := srv.Registry().Register("guest-1", "Test Guest", []string{"default"}); err != nil {
		t.Fatalf("register: %v", err)
	}
	addAssignedTask(t, srv, "cancel-1", "guest-1")
	if err := srv.Orchestrator().Queue().Start("cancel-1"); err != nil {
		t.Fatalf("start: %v", err)
	}
	srv.Registry().SetGuestTask("guest-1", "cancel-1")

	params, _ := json.Marshal(map[string]interface{}{
		"task_id":  "cancel-1",
		"guest_id": "guest-1",
		"reason":   "user requested cancellation",
		"action":   cancelActionNotHeld,
		"intent":   cancelIntentCancel,
	})
	if _, rpcErr := srv.handleGuestCancelled(nil, params); rpcErr != nil {
		t.Fatalf("unexpected rpc error: %v", rpcErr)
	}

	task, ok := srv.TaskQueue().Get("cancel-1")
	if !ok {
		t.Fatal("task not found")
	}
	if task.Status != queue.TaskStatusCancelled {
		t.Errorf("a host-requested cancel must end the task even when the guest no longer holds it, got %s", task.Status)
	}
}

// TestHandleGuestCancelled_OldGuestRevocationLeavesRequeuedTaskPending is the
// rolling-upgrade half of issue #252: a guest that predates the action and
// intent fields echoes neither, so a revocation confirmation must be resolved
// against the intent the host remembers sending — not against the "cancel"
// the absent fields used to default to, which would end a re-queued task
// another guest may already be running.
func TestHandleGuestCancelled_OldGuestRevocationLeavesRequeuedTaskPending(t *testing.T) {
	srv := newTestServer(t)
	conn := connectTestGuest(t, srv, "guest-1")

	addAssignedTask(t, srv, "revoke-1", "guest-1")
	if err := srv.Orchestrator().RequeueTask("revoke-1"); err != nil {
		t.Fatalf("requeue: %v", err)
	}
	conn.Drain()
	srv.revokeClaim("revoke-1", "guest-1", claimReasonRequeued)
	recvCancel(t, conn)

	// A pre-intent guest: no action, no intent in the confirmation.
	params, _ := json.Marshal(map[string]interface{}{
		"task_id":  "revoke-1",
		"guest_id": "guest-1",
		"reason":   "re-queued",
	})
	if _, rpcErr := srv.handleGuestCancelled(nil, params); rpcErr != nil {
		t.Fatalf("unexpected rpc error: %v", rpcErr)
	}

	task, ok := srv.TaskQueue().Get("revoke-1")
	if !ok {
		t.Fatal("task not found")
	}
	if task.Status == queue.TaskStatusCancelled {
		t.Error("an old guest's revocation confirmation cancelled a re-queued task — the host must resolve the intent-less confirmation against what it sent")
	}
	if task.Status != queue.TaskStatusPending {
		t.Errorf("expected the re-queued task to stay PENDING, got %s", task.Status)
	}
}

// TestHandleGuestCancelled_OldGuestUserCancelEndsTask checks the other arm of
// the same remembering: a user cancel of a RUNNING task sends intent cancel
// (or nothing at all), so an intent-less confirmation from an old guest must
// still end the task rather than be mistaken for a revocation.
func TestHandleGuestCancelled_OldGuestUserCancelEndsTask(t *testing.T) {
	srv := newTestServer(t)
	conn := connectTestGuest(t, srv, "guest-1")

	addAssignedTask(t, srv, "usercancel-1", "guest-1")
	if err := srv.Orchestrator().Queue().Start("usercancel-1"); err != nil {
		t.Fatalf("start: %v", err)
	}
	srv.Registry().SetGuestTask("guest-1", "usercancel-1")
	conn.Drain()

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/api/tasks/usercancel-1/cancel", nil)
	srv.handleTaskCancelHTTP(w, r, "usercancel-1")
	if w.Code != http.StatusOK {
		t.Fatalf("cancel endpoint: %d %s", w.Code, w.Body.String())
	}
	recvCancel(t, conn)

	params, _ := json.Marshal(map[string]interface{}{
		"task_id":  "usercancel-1",
		"guest_id": "guest-1",
		"reason":   "user requested cancellation",
	})
	if _, rpcErr := srv.handleGuestCancelled(nil, params); rpcErr != nil {
		t.Fatalf("unexpected rpc error: %v", rpcErr)
	}

	task, ok := srv.TaskQueue().Get("usercancel-1")
	if !ok {
		t.Fatal("task not found")
	}
	if task.Status != queue.TaskStatusCancelled {
		t.Errorf("an old guest's cancel confirmation must still end the task, got %s", task.Status)
	}
}
