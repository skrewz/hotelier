package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"hotelier/pkg/config"
	"hotelier/pkg/guest"
	"hotelier/pkg/queue"
	"hotelier/pkg/registry"
	"hotelier/pkg/rpc"
)

// Issue #253: the guest configuration documents an `id`, and deployments
// template it (typically from the hostname) so a process restart keeps the
// same identity. Two things have to hold for that to be safe: the configured
// id must actually reach the registry, and a *fresh* process registering an id
// the registry already holds must not be reconciled into the task the previous
// process was given.

// TestGuestRegister_ConfiguredIDReachesAPI walks the id from the config into
// the registration and out through /api/guests.
func TestGuestRegister_ConfiguredIDReachesAPI(t *testing.T) {
	srv := newTestServer(t)
	hub := srv.Hub()
	go hub.Run()

	g := guest.New(config.GuestConfig{ID: "guest-producer-1", Name: "Producer", Tags: []string{"tag1"}}, nil)

	params, _ := json.Marshal(map[string]interface{}{
		"id":      g.ID(),
		"name":    "Producer",
		"tags":    []string{"tag1"},
		"resumed": false,
	})
	if _, err := hub.Dispatch("guest.register", params); err != nil {
		t.Fatalf("guest.register failed: %v", err)
	}

	w := httptest.NewRecorder()
	srv.HandleGuests(w, httptest.NewRequest(http.MethodGet, "/api/guests", nil))

	var body struct {
		Guests []struct {
			ID   string `json:"id"`
			Name string `json:"name"`
		} `json:"guests"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode /api/guests: %v", err)
	}
	if len(body.Guests) != 1 {
		t.Fatalf("expected 1 guest, got %d", len(body.Guests))
	}
	if body.Guests[0].ID != "guest-producer-1" {
		t.Errorf("registry holds %q, not the configured id", body.Guests[0].ID)
	}
}

// registerAndAssignTask registers a guest and gives it an ASSIGNED task, the
// state a previous process would have left behind.
func registerAndAssignTask(t *testing.T, srv *Server, hub *rpc.Hub, id string) {
	t.Helper()

	params, _ := json.Marshal(map[string]interface{}{
		"id": id, "name": "Producer", "tags": []string{"tag1"}, "resumed": true,
	})
	if _, err := hub.Dispatch("guest.register", params); err != nil {
		t.Fatalf("first register failed: %v", err)
	}

	task := &queue.Task{ID: "task-stale", Prompt: "leftover work", Tags: []string{"tag1"}}
	if err := srv.TaskQueue().Add(task); err != nil {
		t.Fatalf("add task: %v", err)
	}
	if err := srv.TaskQueue().Assign("task-stale", id); err != nil {
		t.Fatalf("assign task: %v", err)
	}
	if err := srv.Registry().SetGuestTask(id, "task-stale"); err != nil {
		t.Fatalf("set guest task: %v", err)
	}
}

func TestGuestRegister_FreshProcessDoesNotReclaimTask(t *testing.T) {
	srv := newTestServer(t)
	hub := srv.Hub()
	go hub.Run()

	registerAndAssignTask(t, srv, hub, "guest-producer-1")

	params, _ := json.Marshal(map[string]interface{}{
		"id": "guest-producer-1", "name": "Producer", "tags": []string{"tag1"}, "resumed": false,
	})
	resp, err := hub.Dispatch("guest.register", params)
	if err != nil {
		t.Fatalf("fresh register failed: %v", err)
	}

	result := resp.(map[string]interface{})
	if result["status"] != "re-registered" {
		t.Fatalf("expected status 're-registered', got %v", result["status"])
	}
	task := result["task"].(map[string]interface{})
	if task["id"] != "" {
		t.Errorf("a fresh process must not be handed a task, got %v", task["id"])
	}
	if task["status"] != queue.TaskStatusPending.String() {
		t.Errorf("expected the fresh registration to report %s, got %v", queue.TaskStatusPending, task["status"])
	}

	g, _ := srv.Registry().GetGuest("guest-producer-1")
	if g.TaskID != "" {
		t.Errorf("expected the stale task reference to be cleared, got %s", g.TaskID)
	}
	if g.State != registry.GuestStateIdle {
		t.Errorf("expected the fresh guest to be IDLE, got %s", g.State)
	}

	// The task keeps its claim until the stuck-task probe re-queues it (that
	// revocation is issue #252); nothing executes it twice in the meantime,
	// because the guest that held it is gone.
	got, _ := srv.TaskQueue().Get("task-stale")
	if got.Status != queue.TaskStatusAssigned {
		t.Errorf("expected the task to stay ASSIGNED for the probe, got %s", got.Status)
	}
}

func TestGuestRegister_ReconnectStillReconcilesRunningTask(t *testing.T) {
	srv := newTestServer(t)
	hub := srv.Hub()
	go hub.Run()

	registerAndAssignTask(t, srv, hub, "guest-producer-1")

	params, _ := json.Marshal(map[string]interface{}{
		"id": "guest-producer-1", "name": "Producer", "tags": []string{"tag1"}, "resumed": true,
	})
	resp, err := hub.Dispatch("guest.register", params)
	if err != nil {
		t.Fatalf("reconnect register failed: %v", err)
	}

	result := resp.(map[string]interface{})
	task := result["task"].(map[string]interface{})
	if task["id"] != "task-stale" {
		t.Errorf("a reconnect must reconcile to the task it is running, got %v", task["id"])
	}

	g, _ := srv.Registry().GetGuest("guest-producer-1")
	if g.State != registry.GuestStateRunning {
		t.Errorf("expected the reconnecting guest to stay RUNNING, got %s", g.State)
	}
}
