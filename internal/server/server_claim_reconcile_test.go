package server

import (
	"context"
	"encoding/json"
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

// heartbeat names taskID from guestID through the real RPC handler.
func heartbeat(t *testing.T, srv *Server, guestID, taskID string) {
	t.Helper()
	params, _ := json.Marshal(map[string]string{"id": guestID, "task_id": taskID})
	if _, rpcErr := srv.handleGuestHeartbeat(nil, params); rpcErr != nil {
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
	if _, err := srv.handleGuestCancelled(nil, params); err != nil {
		t.Fatalf("guest.cancelled: %v", err)
	}
}

func TestHeartbeat_RevokesClaim_WhenTaskIsUnknown(t *testing.T) {
	srv := newTestServer(t)
	conn := connectTestGuest(t, srv, "guest-a")

	heartbeat(t, srv, "guest-a", "ghost-task")

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
	heartbeat(t, srv, "guest-b", "task-x")

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

	heartbeat(t, srv, "guest-b", "task-x")

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

	heartbeat(t, srv, "guest-b", "task-x")

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

	heartbeat(t, srv, "guest-a", "task-x")

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
	heartbeat(t, srv, "guest-b", "task-x")
	recvCancel(t, connB)
	heartbeat(t, srv, "guest-b", "task-x")
	expectNoCancel(t, connB, 150*time.Millisecond)

	// Once the guest confirms, the next claim may fire a fresh revoke.
	confirmRevoke(t, srv, "task-x", "guest-b")
	heartbeat(t, srv, "guest-b", "task-x")
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
		"task_id":  "task-x",
		"guest_id": "guest-b",
		"success":  true,
		"output":   "I finished first",
	})
	_, rpcErr := srv.handleGuestResult(nil, params)
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
		"task_id":  "task-x",
		"guest_id": "guest-b",
		"success":  false,
		"error":    "boom",
	})
	_, rpcErr := srv.handleGuestResult(nil, params)
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
		"task_id":  "task-x",
		"guest_id": "guest-a",
		"success":  true,
		"output":   "done",
	})
	if _, rpcErr := srv.handleGuestResult(nil, params); rpcErr != nil {
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
		"task_id":  "task-x",
		"guest_id": "guest-b",
		"success":  false,
		"error":    "I timed out waiting",
	})
	if _, rpcErr := srv.handleGuestResult(nil, params); rpcErr == nil {
		t.Fatal("expected the non-owner report to be rejected")
	}
	recvCancel(t, connB)
}

func TestGuestResult_AnonymousReportFallbackPreserved(t *testing.T) {
	srv := newTestServer(t)
	connectTestGuest(t, srv, "guest-a")

	task := addAssignedTask(t, srv, "task-x", "guest-a")
	if err := srv.TaskQueue().Start(task.ID); err != nil {
		t.Fatalf("start: %v", err)
	}

	// No guest_id and no connection (direct dispatch, e.g. a test
	// harness): the report is attributed to the assignee, as before.
	params, _ := json.Marshal(map[string]interface{}{
		"task_id": "task-x",
		"success": true,
	})
	if _, rpcErr := srv.handleGuestResult(nil, params); rpcErr != nil {
		t.Fatalf("anonymous report must keep working: %v", rpcErr)
	}
	got, _ := srv.Orchestrator().GetTask("task-x")
	if got.Status != queue.TaskStatusCompleted {
		t.Errorf("expected COMPLETED, got %s", got.Status)
	}
}

func TestGuestResult_UnknownTaskErrorWithoutRevoke(t *testing.T) {
	srv := newTestServer(t)
	connB := connectTestGuest(t, srv, "guest-b")

	// A task the host has no record of says nothing about the claim
	// (issue #252 precedent): error, but no revoke.
	params, _ := json.Marshal(map[string]interface{}{
		"task_id":  "ghost-task",
		"guest_id": "guest-b",
		"success":  true,
	})
	if _, rpcErr := srv.handleGuestResult(nil, params); rpcErr == nil {
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
		"task_id":  "task-x",
		"guest_id": "guest-b",
		"success":  true,
	})
	for i := 0; i < 2; i++ {
		if _, rpcErr := srv.handleGuestResult(nil, params); rpcErr == nil {
			t.Fatalf("report %d: expected rejection", i)
		}
	}
	recvCancel(t, connB)
	expectNoCancel(t, connB, 150*time.Millisecond)
}
