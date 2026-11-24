package server

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"hotelier/pkg/queue"
	"hotelier/pkg/rpc"
)

// Issue #304: the queue is the single source of truth for who owns a task
// (task.AssignedTo). Every claim surface — the task-naming heartbeat and the
// completion/failure report — must reconcile the claim against the
// authoritative assignment: a contradicting claim never moves the assignment
// and is answered by idempotently revoking the claimant's claim.

// heartbeat dispatches guest.heartbeat over the socket named connID,
// naming guestID as the guest working on taskID. This is how a real
// guest's heartbeat arrives: the hub tags the dispatch with the
// connection the message came in on, so the socket is the identity and
// the name in the payload is an assertion about it.
func heartbeat(t *testing.T, srv *Server, connID, guestID, taskID string) {
	t.Helper()
	params, _ := json.Marshal(map[string]string{"id": guestID, "task_id": taskID})
	if _, rpcErr := srv.handleGuestHeartbeat(rpc.ContextWithConnection(context.Background(), connID), params); rpcErr != nil {
		t.Fatalf("guest.heartbeat: %v", rpcErr)
	}
}

// expectNoCancel asserts no task.cancel reaches conn within the window.
// Other messages (e.g. task.assign) are drained and ignored.
func expectNoCancel(t *testing.T, conn *rpc.Connection, window time.Duration) {
	t.Helper()
	deadline := time.Now().Add(window)
	for {
		if data, ok := conn.Recv(); ok {
			var msg rpc.JSONRPCMessage
			if err := json.Unmarshal(data, &msg); err != nil {
				t.Fatalf("unmarshal message: %v", err)
			}
			if msg.Method == "task.cancel" {
				t.Fatalf("unexpected task.cancel received: %s", string(data))
			}
			continue
		}
		if time.Now().After(deadline) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// confirmRevoke plays back the guest.cancelled confirmation an honest guest
// sends after receiving a revoke, consuming the outstanding-intent record.
func confirmRevoke(t *testing.T, srv *Server, taskID, guestID string) {
	t.Helper()
	params, _ := json.Marshal(map[string]interface{}{
		"task_id":  taskID,
		"guest_id": guestID,
		"action":   "cancelled_while_running",
		"intent":   "revoke",
	})
	if _, err := srv.handleGuestCancelled(context.Background(), params); err != nil {
		t.Fatalf("guest.cancelled: %v", err)
	}
}

func TestHeartbeat_RevokesClaim_WhenTaskIsUnknown(t *testing.T) {
	srv := newTestServer(t)
	conn := connectTestGuest(t, srv, "guest-a")

	heartbeat(t, srv, "conn-guest-a", "guest-a", "ghost-task")

	cancel := recvCancel(t, conn)
	if cancel["task_id"] != "ghost-task" {
		t.Errorf("expected cancel for ghost-task, got %v", cancel["task_id"])
	}
	if cancel["intent"] != "revoke" {
		t.Errorf("expected intent revoke, got %v", cancel["intent"])
	}
}

func TestHeartbeat_RevokesClaim_WhenTaskIsTerminal(t *testing.T) {
	srv := newTestServer(t)
	connectTestGuest(t, srv, "guest-a")
	connB := connectTestGuest(t, srv, "guest-b")

	task := addAssignedTask(t, srv, "task-x", "guest-a")
	if err := srv.TaskQueue().Start(task.ID); err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := srv.Orchestrator().CompleteTask(task.ID, "guest-a", "done"); err != nil {
		t.Fatalf("complete: %v", err)
	}

	// guest-b was never the owner; its claim of a completed task is a
	// contradiction and must fire the revoke.
	heartbeat(t, srv, "conn-guest-b", "guest-b", "task-x")

	cancel := recvCancel(t, connB)
	if cancel["task_id"] != "task-x" {
		t.Errorf("expected cancel for task-x, got %v", cancel["task_id"])
	}
	if cancel["intent"] != "revoke" {
		t.Errorf("expected intent revoke, got %v", cancel["intent"])
	}
}

func TestHeartbeat_RevokesClaim_WhenAssignedElsewhere(t *testing.T) {
	srv := newTestServer(t)
	connectTestGuest(t, srv, "guest-a")
	connB := connectTestGuest(t, srv, "guest-b")

	addAssignedTask(t, srv, "task-x", "guest-a")

	heartbeat(t, srv, "conn-guest-b", "guest-b", "task-x")

	cancel := recvCancel(t, connB)
	if cancel["task_id"] != "task-x" {
		t.Errorf("expected cancel for task-x, got %v", cancel["task_id"])
	}
	if cancel["intent"] != "revoke" {
		t.Errorf("expected intent revoke, got %v", cancel["intent"])
	}

	// The boundary from issue #251: a heartbeat never creates or changes
	// an assignment.
	task, ok := srv.Orchestrator().GetTask("task-x")
	if !ok {
		t.Fatal("task-x vanished")
	}
	if task.AssignedTo != "guest-a" {
		t.Errorf("assignment must survive the claim: AssignedTo=%q", task.AssignedTo)
	}
	if task.Status != queue.TaskStatusAssigned {
		t.Errorf("assignment must survive the claim: status=%s", task.Status)
	}
	guestB, _ := srv.Registry().GetGuest("guest-b")
	if guestB.TaskID != "" {
		t.Errorf("mismatched heartbeat must not assign, guest-b holds %q", guestB.TaskID)
	}
}

func TestHeartbeat_RevokesClaim_WhenTaskUnassigned(t *testing.T) {
	srv := newTestServer(t)
	connB := connectTestGuest(t, srv, "guest-b")

	// A re-queued task (e.g. after a proxy restart dropped the socket):
	// the queue says nobody works on it, so the orphan's claim must be
	// answered, not merely observed.
	task := &queue.Task{ID: "task-x", Prompt: "p", CreatedAt: time.Now()}
	if err := srv.Orchestrator().AddTask(task); err != nil {
		t.Fatalf("add task: %v", err)
	}

	heartbeat(t, srv, "conn-guest-b", "guest-b", "task-x")

	cancel := recvCancel(t, connB)
	if cancel["task_id"] != "task-x" {
		t.Errorf("expected cancel for task-x, got %v", cancel["task_id"])
	}
	got, _ := srv.Orchestrator().GetTask("task-x")
	if got.Status != queue.TaskStatusPending {
		t.Errorf("heartbeat must not move the task, status=%s", got.Status)
	}
}

func TestHeartbeat_OwnerIsNeverRevoked(t *testing.T) {
	srv := newTestServer(t)
	connA := connectTestGuest(t, srv, "guest-a")

	addAssignedTask(t, srv, "task-x", "guest-a")

	heartbeat(t, srv, "conn-guest-a", "guest-a", "task-x")

	expectNoCancel(t, connA, 150*time.Millisecond)

	guestA, _ := srv.Registry().GetGuest("guest-a")
	if guestA.LastTaskHeartbeat.IsZero() {
		t.Error("expected the matching heartbeat to confirm the claim")
	}
}

func TestHeartbeat_RevokeIsIdempotent(t *testing.T) {
	srv := newTestServer(t)
	connectTestGuest(t, srv, "guest-a")
	connB := connectTestGuest(t, srv, "guest-b")

	addAssignedTask(t, srv, "task-x", "guest-a")

	// Two claims while the revoke is outstanding: one ask, no repeat.
	heartbeat(t, srv, "conn-guest-b", "guest-b", "task-x")
	recvCancel(t, connB)
	heartbeat(t, srv, "conn-guest-b", "guest-b", "task-x")
	expectNoCancel(t, connB, 150*time.Millisecond)

	// Once the guest confirms, the next claim may fire a fresh revoke.
	confirmRevoke(t, srv, "task-x", "guest-b")
	heartbeat(t, srv, "conn-guest-b", "guest-b", "task-x")
	recvCancel(t, connB)
}

func TestGuestResult_NonOwnerCompletionRejectedAndRevoked(t *testing.T) {
	srv := newTestServer(t)
	connectTestGuest(t, srv, "guest-a")
	connB := connectTestGuest(t, srv, "guest-b")

	task := addAssignedTask(t, srv, "task-x", "guest-a")
	if err := srv.TaskQueue().Start(task.ID); err != nil {
		t.Fatalf("start: %v", err)
	}

	params, _ := json.Marshal(map[string]interface{}{
		"task_id": "task-x",
		"success": true,
		"output":  "I finished first",
	})
	ctx := rpc.ContextWithConnection(context.Background(), "conn-guest-b")
	_, rpcErr := srv.handleGuestResult(ctx, params)
	if rpcErr == nil {
		t.Fatal("expected the non-owner report to be rejected")
	}

	got, ok := srv.Orchestrator().GetTask("task-x")
	if !ok {
		t.Fatal("task-x vanished")
	}
	if got.Status != queue.TaskStatusRunning || got.AssignedTo != "guest-a" {
		t.Errorf("report must not move the assignment: status=%s assigned=%q", got.Status, got.AssignedTo)
	}
	if got.Result != "" {
		t.Errorf("rejected report must not store a result: %q", got.Result)
	}
	guestA, _ := srv.Registry().GetGuest("guest-a")
	if guestA.TaskID != "task-x" {
		t.Errorf("owner must stay engaged: guest-a holds %q", guestA.TaskID)
	}

	cancel := recvCancel(t, connB)
	if cancel["task_id"] != "task-x" || cancel["intent"] != "revoke" {
		t.Errorf("expected revoke for task-x, got %v", cancel)
	}
}

func TestGuestResult_NonOwnerFailureRejectedAndRevoked(t *testing.T) {
	srv := newTestServer(t)
	connectTestGuest(t, srv, "guest-a")
	connB := connectTestGuest(t, srv, "guest-b")

	task := addAssignedTask(t, srv, "task-x", "guest-a")
	if err := srv.TaskQueue().Start(task.ID); err != nil {
		t.Fatalf("start: %v", err)
	}

	params, _ := json.Marshal(map[string]interface{}{
		"task_id": "task-x",
		"success": false,
		"error":   "boom",
	})
	ctx := rpc.ContextWithConnection(context.Background(), "conn-guest-b")
	_, rpcErr := srv.handleGuestResult(ctx, params)
	if rpcErr == nil {
		t.Fatal("expected the non-owner report to be rejected")
	}

	got, _ := srv.Orchestrator().GetTask("task-x")
	if got.Status != queue.TaskStatusRunning {
		t.Errorf("rejected report must not fail the task: status=%s", got.Status)
	}

	cancel := recvCancel(t, connB)
	if cancel["task_id"] != "task-x" || cancel["intent"] != "revoke" {
		t.Errorf("expected revoke for task-x, got %v", cancel)
	}
}

func TestGuestResult_ReporterIdentityComesFromConnection(t *testing.T) {
	srv := newTestServer(t)
	connectTestGuest(t, srv, "guest-a")
	connB := connectTestGuest(t, srv, "guest-b")

	task := addAssignedTask(t, srv, "task-x", "guest-a")
	if err := srv.TaskQueue().Start(task.ID); err != nil {
		t.Fatalf("start: %v", err)
	}

	// A real guest omits guest_id (see guest.TaskResult): the connection
	// the report arrived on is the reporter's identity.
	params, _ := json.Marshal(map[string]interface{}{
		"task_id": "task-x",
		"success": true,
	})
	ctx := rpc.ContextWithConnection(context.Background(), "conn-guest-b")
	if _, rpcErr := srv.handleGuestResult(ctx, params); rpcErr == nil {
		t.Fatal("expected the non-owner report to be rejected")
	}

	got, _ := srv.Orchestrator().GetTask("task-x")
	if got.Status != queue.TaskStatusRunning || got.AssignedTo != "guest-a" {
		t.Errorf("report must not move the assignment: status=%s assigned=%q", got.Status, got.AssignedTo)
	}
	recvCancel(t, connB)
}

func TestGuestResult_OwnerCompletionAcceptedWithoutRevoke(t *testing.T) {
	srv := newTestServer(t)
	connA := connectTestGuest(t, srv, "guest-a")

	task := addAssignedTask(t, srv, "task-x", "guest-a")
	if err := srv.TaskQueue().Start(task.ID); err != nil {
		t.Fatalf("start: %v", err)
	}

	params, _ := json.Marshal(map[string]interface{}{
		"task_id": "task-x",
		"success": true,
		"output":  "done",
	})
	ctx := rpc.ContextWithConnection(context.Background(), "conn-guest-a")
	if _, rpcErr := srv.handleGuestResult(ctx, params); rpcErr != nil {
		t.Fatalf("owner report must be accepted: %v", rpcErr)
	}

	got, _ := srv.Orchestrator().GetTask("task-x")
	if got.Status != queue.TaskStatusCompleted {
		t.Errorf("expected COMPLETED, got %s", got.Status)
	}
	expectNoCancel(t, connA, 150*time.Millisecond)
}

func TestGuestResult_NonOwnerReportOnTerminalTaskRejectedAndRevoked(t *testing.T) {
	srv := newTestServer(t)
	connectTestGuest(t, srv, "guest-a")
	connB := connectTestGuest(t, srv, "guest-b")

	task := addAssignedTask(t, srv, "task-x", "guest-a")
	if err := srv.TaskQueue().Start(task.ID); err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := srv.Orchestrator().CompleteTask(task.ID, "guest-a", "done"); err != nil {
		t.Fatalf("complete: %v", err)
	}

	params, _ := json.Marshal(map[string]interface{}{
		"task_id": "task-x",
		"success": false,
		"error":   "I timed out waiting",
	})
	ctx := rpc.ContextWithConnection(context.Background(), "conn-guest-b")
	if _, rpcErr := srv.handleGuestResult(ctx, params); rpcErr == nil {
		t.Fatal("expected the non-owner report to be rejected")
	}
	recvCancel(t, connB)
}

// reportResult sends a guest.result from a socket the test names, which is
// how a real guest reports: guest.TaskResult carries no guest_id, so the
// connection the report arrived on is the reporter's identity.
func reportResult(t *testing.T, srv *Server, connID string, taskID string, success bool) *rpc.RPCError {
	t.Helper()
	params, _ := json.Marshal(map[string]interface{}{"task_id": taskID, "success": success})
	_, rpcErr := srv.handleGuestResult(rpc.ContextWithConnection(context.Background(), connID), params)
	return rpcErr
}

func TestGuestResult_ReportFromTheOwningSocketIsAccepted(t *testing.T) {
	srv := newTestServer(t)
	connA := connectTestGuest(t, srv, "guest-a")

	task := addAssignedTask(t, srv, "task-x", "guest-a")
	if err := srv.TaskQueue().Start(task.ID); err != nil {
		t.Fatalf("start: %v", err)
	}

	// The owner reporting over its own connection, naming nobody: the
	// connection is the identity, so the report is the owner's.
	if rpcErr := reportResult(t, srv, "conn-guest-a", "task-x", true); rpcErr != nil {
		t.Fatalf("owner report from its own socket must be accepted: %v", rpcErr)
	}
	got, _ := srv.Orchestrator().GetTask("task-x")
	if got.Status != queue.TaskStatusCompleted {
		t.Errorf("expected COMPLETED, got %s", got.Status)
	}
	expectNoCancel(t, connA, 150*time.Millisecond)
}

func TestGuestResult_UnidentifiableReporterIsRefused(t *testing.T) {
	srv := newTestServer(t)
	connA := connectTestGuest(t, srv, "guest-a")

	task := addAssignedTask(t, srv, "task-x", "guest-a")
	if err := srv.TaskQueue().Start(task.ID); err != nil {
		t.Fatalf("start: %v", err)
	}

	// A report the host cannot place — no connection to ask, no name
	// carried — must not be resolved into the assignee: attributing it
	// there is what made the ownership gate vacuous for any dispatch that
	// omitted the field (issue #304 review).
	if rpcErr := reportResult(t, srv, "", "task-x", true); rpcErr == nil {
		t.Fatal("expected a report with no identifiable reporter to be refused")
	}
	got, _ := srv.Orchestrator().GetTask("task-x")
	if got.Status != queue.TaskStatusRunning || got.AssignedTo != "guest-a" {
		t.Errorf("refused report must not move the task: status=%s assigned=%q", got.Status, got.AssignedTo)
	}
	if got.Result != "" {
		t.Errorf("refused report must not store a result: %q", got.Result)
	}
	expectNoCancel(t, connA, 150*time.Millisecond)
}

func TestGuestResult_UnmappedConnectionIsRefused(t *testing.T) {
	srv := newTestServer(t)
	connA := connectTestGuest(t, srv, "guest-a")

	task := addAssignedTask(t, srv, "task-x", "guest-a")
	if err := srv.TaskQueue().Start(task.ID); err != nil {
		t.Fatalf("start: %v", err)
	}

	// A socket the hub cannot place behind a guest — a browser session, a
	// guest whose re-registration moved the mapping on — says nothing about
	// who sent the report, so it must not be read as the assignee's.
	if rpcErr := reportResult(t, srv, "conn-browser-1", "task-x", true); rpcErr == nil {
		t.Fatal("expected a report from an unmapped connection to be refused")
	}
	got, _ := srv.Orchestrator().GetTask("task-x")
	if got.Status != queue.TaskStatusRunning || got.AssignedTo != "guest-a" {
		t.Errorf("refused report must not move the task: status=%s assigned=%q", got.Status, got.AssignedTo)
	}
	expectNoCancel(t, connA, 150*time.Millisecond)
}

func TestGuestResult_DeclaredNameYieldsToTheConnection(t *testing.T) {
	srv := newTestServer(t)
	connA := connectTestGuest(t, srv, "guest-a")
	connB := connectTestGuest(t, srv, "guest-b")

	task := addAssignedTask(t, srv, "task-x", "guest-a")
	if err := srv.TaskQueue().Start(task.ID); err != nil {
		t.Fatalf("start: %v", err)
	}

	// guest-b holds guest-a's name but not guest-a's socket. The name is a
	// claim about identity, not identity, so the report is guest-b's — and
	// guest-b does not own the task (issue #304 review).
	params, _ := json.Marshal(map[string]interface{}{
		"task_id":  "task-x",
		"guest_id": "guest-a",
		"success":  true,
		"output":   "typed from the wrong socket",
	})
	ctx := rpc.ContextWithConnection(context.Background(), "conn-guest-b")
	if _, rpcErr := srv.handleGuestResult(ctx, params); rpcErr == nil {
		t.Fatal("expected a report naming another guest to be refused")
	}
	got, _ := srv.Orchestrator().GetTask("task-x")
	if got.Status != queue.TaskStatusRunning || got.AssignedTo != "guest-a" {
		t.Errorf("refused report must not move the task: status=%s assigned=%q", got.Status, got.AssignedTo)
	}
	if got.Result != "" {
		t.Errorf("refused report must not store a result: %q", got.Result)
	}
	expectNoCancel(t, connA, 100*time.Millisecond)

	// The claimant is the socket behind the report, so that is who is told
	// to stop — not the name the report typed.
	cancel := recvCancel(t, connB)
	if cancel["task_id"] != "task-x" || cancel["intent"] != "revoke" {
		t.Errorf("expected revoke for task-x, got %v", cancel)
	}
}

func TestGuestResult_OwnerReportOnTerminalTaskNeedsNoRevoke(t *testing.T) {
	srv := newTestServer(t)
	connA := connectTestGuest(t, srv, "guest-a")

	task := addAssignedTask(t, srv, "task-x", "guest-a")
	if err := srv.TaskQueue().Start(task.ID); err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := srv.Orchestrator().CompleteTask(task.ID, "guest-a", "done"); err != nil {
		t.Fatalf("complete: %v", err)
	}

	// The owner reporting its own completed task again is its own task
	// winding down, not a contradiction (see claimContradiction): no revoke
	// on the surface that carries the owner's socket.
	if rpcErr := reportResult(t, srv, "conn-guest-a", "task-x", true); rpcErr != nil {
		t.Fatalf("owner's repeat report must not be read as a contradiction: %v", rpcErr)
	}
	expectNoCancel(t, connA, 150*time.Millisecond)
}

func TestGuestResult_UnknownTaskErrorWithoutRevoke(t *testing.T) {
	srv := newTestServer(t)
	connB := connectTestGuest(t, srv, "guest-b")

	// A task the host has no record of says nothing about the claim
	// (issue #252 precedent): error, but no revoke.
	params, _ := json.Marshal(map[string]interface{}{
		"task_id": "ghost-task",
		"success": true,
	})
	ctx := rpc.ContextWithConnection(context.Background(), "conn-guest-b")
	if _, rpcErr := srv.handleGuestResult(ctx, params); rpcErr == nil {
		t.Fatal("expected an error for the unknown task")
	}
	expectNoCancel(t, connB, 150*time.Millisecond)
}

func TestGuestResult_RevokeIsIdempotent(t *testing.T) {
	srv := newTestServer(t)
	connectTestGuest(t, srv, "guest-a")
	connB := connectTestGuest(t, srv, "guest-b")

	task := addAssignedTask(t, srv, "task-x", "guest-a")
	if err := srv.TaskQueue().Start(task.ID); err != nil {
		t.Fatalf("start: %v", err)
	}

	params, _ := json.Marshal(map[string]interface{}{
		"task_id": "task-x",
		"success": true,
	})
	ctx := rpc.ContextWithConnection(context.Background(), "conn-guest-b")
	for i := 0; i < 2; i++ {
		if _, rpcErr := srv.handleGuestResult(ctx, params); rpcErr == nil {
			t.Fatalf("report %d: expected rejection", i)
		}
	}
	recvCancel(t, connB)
	expectNoCancel(t, connB, 150*time.Millisecond)
}

func TestHeartbeat_ClaimInAnotherGuestsNameIsRefused(t *testing.T) {
	srv := newTestServer(t)
	connectTestGuest(t, srv, "guest-a")
	connB := connectTestGuest(t, srv, "guest-b")

	addAssignedTask(t, srv, "task-x", "guest-a")
	// Backdate the owner's liveness record: a heartbeat arriving on another
	// guest's socket must not move it, or holding any socket open would keep
	// a victim looking live to the liveness probe indefinitely (issue #304
	// review).
	stale := time.Now().Add(-time.Hour)
	if err := srv.Registry().SetLastHeartbeat("guest-a", stale); err != nil {
		t.Fatalf("backdate: %v", err)
	}

	heartbeat(t, srv, "conn-guest-b", "guest-a", "task-x")

	guestA, _ := srv.Registry().GetGuest("guest-a")
	if !guestA.LastHeartbeat.Equal(stale) {
		t.Errorf("guest-a's liveness was refreshed in its own name by another socket: %v", guestA.LastHeartbeat)
	}
	if guestA.TaskID != "task-x" {
		t.Errorf("assignment must survive the claim: guest-a holds %q", guestA.TaskID)
	}

	// The claimant is the socket behind the heartbeat, so that is who is
	// told to stop — a revoke aimed at the name typed here would let any
	// socket cancel a stranger's work.
	cancel := recvCancel(t, connB)
	if cancel["task_id"] != "task-x" || cancel["intent"] != "revoke" {
		t.Errorf("expected revoke for task-x, got %v", cancel)
	}
}

func TestHeartbeat_OwnerOnTerminalTaskIsNotRevoked(t *testing.T) {
	srv := newTestServer(t)
	connA := connectTestGuest(t, srv, "guest-a")

	task := addAssignedTask(t, srv, "task-x", "guest-a")
	if err := srv.TaskQueue().Start(task.ID); err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := srv.Orchestrator().CompleteTask(task.ID, "guest-a", "done"); err != nil {
		t.Fatalf("complete: %v", err)
	}

	// The owner's own heartbeat on its own task as it winds down is not a
	// contradiction, terminal or otherwise: revoking here would cancel work
	// the host already accepted.
	heartbeat(t, srv, "conn-guest-a", "guest-a", "task-x")
	expectNoCancel(t, connA, 150*time.Millisecond)

	got, _ := srv.Orchestrator().GetTask("task-x")
	if got.Status != queue.TaskStatusCompleted || got.AssignedTo != "guest-a" {
		t.Errorf("heartbeat must not move the task: status=%s assigned=%q", got.Status, got.AssignedTo)
	}
}

// TestHeartbeat_UnplaceableClaimLeavesATrace covers the claim surface issue
// #304 was filed from: a socket the hub cannot place — one that never
// registered, or an orphan whose re-registration moved the mapping on. Such a
// claim is refused before the liveness record is touched, and there is no
// claimant to send a revoke to, so the log is the only trace it leaves
// (#304 review).
func TestHeartbeat_UnplaceableClaimLeavesATrace(t *testing.T) {
	srv := newTestServer(t)
	connectTestGuest(t, srv, "guest-a")
	var logBuf bytes.Buffer
	srv.log.SetOutput(&logBuf)

	task := addAssignedTask(t, srv, "task-x", "guest-a")
	if err := srv.TaskQueue().Start(task.ID); err != nil {
		t.Fatalf("start: %v", err)
	}

	params, _ := json.Marshal(map[string]string{"id": "guest-a", "task_id": "task-x"})
	if _, rpcErr := srv.handleGuestHeartbeat(
		rpc.ContextWithConnection(context.Background(), "conn-unmapped"), params); rpcErr == nil {
		t.Fatal("a claim from a socket the host cannot place must be refused")
	}

	got, _ := srv.Orchestrator().GetTask("task-x")
	if got.Status != queue.TaskStatusRunning || got.AssignedTo != "guest-a" {
		t.Errorf("a refused claim must not move the task: status=%s assigned=%q", got.Status, got.AssignedTo)
	}

	log := logBuf.String()
	if !strings.Contains(log, "heartbeat claim mismatch") || !strings.Contains(log, "task-x") {
		t.Errorf("a refused claim must name the task it contradicted in the log, got: %s", log)
	}
	if !strings.Contains(log, "no revoke") {
		t.Errorf("the log must say no revoke went out, for a reader who assumes one did: %s", log)
	}
}

func TestClaimRevoke_DoesNotOverwriteAnOutstandingCancel(t *testing.T) {
	srv := newTestServer(t)
	connectTestGuest(t, srv, "guest-a")
	connB := connectTestGuest(t, srv, "guest-b")

	task := addAssignedTask(t, srv, "task-x", "guest-a")
	if err := srv.TaskQueue().Start(task.ID); err != nil {
		t.Fatalf("start: %v", err)
	}

	// The user cancelled: a task.cancel asking the holder to stop is in
	// flight, and its confirmation resolves against the ledger entry — an
	// old guest cannot echo the intent (issue #252).
	srv.rememberCancelIntent("task-x", cancelIntentCancel)

	// A second guest now claims the same task. Recording its revoke over
	// the user's cancel would make the holder's confirmation read as a
	// revocation, which the host must not act on — leaving a task the user
	// cancelled sitting RUNNING (issue #304 review).
	heartbeat(t, srv, "conn-guest-b", "guest-b", "task-x")
	expectNoCancel(t, connB, 150*time.Millisecond)

	params, _ := json.Marshal(map[string]interface{}{
		"task_id":  "task-x",
		"guest_id": "guest-a",
		"action":   "cancelled_while_running",
	})
	if _, err := srv.handleGuestCancelled(context.Background(), params); err != nil {
		t.Fatalf("guest.cancelled: %v", err)
	}

	got, _ := srv.Orchestrator().GetTask("task-x")
	if got.Status != queue.TaskStatusCancelled {
		t.Errorf("the user's cancel must survive a stranger's claim: status=%s", got.Status)
	}
}

// TestClaimRevoke_TheSendRecordsNothingAfterTheDecision pins the window
// a6bc856 shrank but did not close: claimRevokeDue decides and records under
// one lock, and the send that follows must write the ledger no second time,
// because a user's task.cancel recorded in that gap is the one the holder's
// confirmation resolves against (#304 review). The interleaving is forced
// the way it happens in the server: decide, cancel lands, send.
func TestClaimRevoke_TheSendRecordsNothingAfterTheDecision(t *testing.T) {
	srv := newTestServer(t)
	connectTestGuest(t, srv, "guest-a")
	connB := connectTestGuest(t, srv, "guest-b")

	task := addAssignedTask(t, srv, "task-x", "guest-a")
	if err := srv.TaskQueue().Start(task.ID); err != nil {
		t.Fatalf("start: %v", err)
	}

	// The decision half of a claim surface: record the ask under the lock.
	if !srv.claimRevokeDue("task-x") {
		t.Fatal("expected the first revoke ask to be due")
	}
	// The user's cancel lands in the gap the re-record used to leave open.
	srv.rememberCancelIntent("task-x", cancelIntentCancel)
	// The send half of revokeClaimOnce, with no ledger write of its own.
	srv.sendClaimRevoke("task-x", "guest-b", claimReasonClaimMismatch)
	recvCancel(t, connB)

	// The holder cannot echo the intent, so its confirmation resolves
	// against the ledger: if the send had rewritten the entry as a
	// revocation, the task the user cancelled would read as revoked, sit
	// RUNNING with a guest that stopped, and the stuck probe would re-queue
	// the cancelled work.
	params, _ := json.Marshal(map[string]interface{}{
		"task_id":  "task-x",
		"guest_id": "guest-a",
		"action":   "cancelled_while_running",
	})
	if _, err := srv.handleGuestCancelled(context.Background(), params); err != nil {
		t.Fatalf("guest.cancelled: %v", err)
	}
	got, _ := srv.Orchestrator().GetTask("task-x")
	if got.Status != queue.TaskStatusCancelled {
		t.Errorf("a cancel recorded between decision and send must survive it: status=%s", got.Status)
	}
}

// TestClaimRevoke_AFailedSendKeepsAnOutstandingCancel pins the other half of
// the window 797c250 closed: the send failed, so the ask it stood for is
// dropped from the ledger — but only while the entry still reads as a
// revocation. A user's cancel recorded between the decision and the send is
// what the holder's confirmation resolves against, and dropping it here would
// hand the same silent cancel back (#304 review). The failing send is the path
// an orphan whose socket is already gone actually takes.
func TestClaimRevoke_AFailedSendKeepsAnOutstandingCancel(t *testing.T) {
	srv := newTestServer(t)
	connectTestGuest(t, srv, "guest-a")

	task := addAssignedTask(t, srv, "task-x", "guest-a")
	if err := srv.TaskQueue().Start(task.ID); err != nil {
		t.Fatalf("start: %v", err)
	}

	// The decision half of a claim surface: record the ask under the lock.
	if !srv.claimRevokeDue("task-x") {
		t.Fatal("expected the first revoke ask to be due")
	}
	// The user's cancel lands before the send goes out.
	srv.rememberCancelIntent("task-x", cancelIntentCancel)
	// The send half, naming a guest the hub has no connection for: nothing
	// leaves, so the send reports the failure it must act on.
	srv.sendClaimRevoke("task-x", "guest-b", claimReasonClaimMismatch)

	srv.cancelIntentMu.Lock()
	entry, outstanding := srv.cancelIntentByTask["task-x"]
	srv.cancelIntentMu.Unlock()
	if !outstanding || entry.intent != cancelIntentCancel {
		t.Fatalf("a failed revoke send must leave the user's cancel recorded: outstanding=%v intent=%q",
			outstanding, entry.intent)
	}

	// And that entry is what the holder's intent-less confirmation resolves
	// against: the task the user cancelled ends, rather than merely losing a
	// claim no guest was ever told about.
	params, _ := json.Marshal(map[string]interface{}{
		"task_id":  "task-x",
		"guest_id": "guest-a",
		"action":   "cancelled_while_running",
	})
	if _, err := srv.handleGuestCancelled(context.Background(), params); err != nil {
		t.Fatalf("guest.cancelled: %v", err)
	}
	got, _ := srv.Orchestrator().GetTask("task-x")
	if got.Status != queue.TaskStatusCancelled {
		t.Errorf("a cancel the guest was never told about must still be honoured: status=%s", got.Status)
	}
}

func TestClaimRevoke_IsRetriedWhenNeverConfirmed(t *testing.T) {
	srv := newTestServer(t)
	connectTestGuest(t, srv, "guest-a")
	connB := connectTestGuest(t, srv, "guest-b")

	addAssignedTask(t, srv, "task-x", "guest-a")

	heartbeat(t, srv, "conn-guest-b", "guest-b", "task-x")
	recvCancel(t, connB)

	// The confirmation never arrives: a stalled executor, or a confirmation
	// lost on a socket that was already dying — the population issue #304
	// was filed from. Waiting for it forever would mean the claim surfaces
	// go silent for the rest of the task's life, so a revoke is only held
	// off for a while (issue #304 review).
	srv.cancelIntentMu.Lock()
	entry := srv.cancelIntentByTask["task-x"]
	entry.sentAt = time.Now().Add(-claimRevokeRetry - time.Second)
	srv.cancelIntentByTask["task-x"] = entry
	srv.cancelIntentMu.Unlock()

	heartbeat(t, srv, "conn-guest-b", "guest-b", "task-x")
	recvCancel(t, connB)
}
