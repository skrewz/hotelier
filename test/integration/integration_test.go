package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"hotelier/internal/server"
	"hotelier/pkg/config"
	"hotelier/pkg/queue"
	"hotelier/pkg/registry"
	"hotelier/pkg/rpc"
)

func newTestServer(t *testing.T) *server.Server {
	t.Helper()
	cfg := config.ServerConfig{
		Host:      "127.0.0.1",
		Port:      0,
		MaxGuests: 0,
	}
	return server.New(cfg)
}

func TestFullLifecycle_RegisterGuest(t *testing.T) {
	srv := newTestServer(t)
	hub := srv.Hub()

	go hub.Run()

	// Simulate guest.register RPC
	params, _ := json.Marshal(map[string]interface{}{
		"id":   "test-guest-1",
		"name": "Test Guest",
		"tags": []string{"business-default", "frontend"},
	})

	resp, err := hub.Dispatch("guest.register", params)
	if err != nil {
		t.Fatalf("guest.register failed: %v", err)
	}

	result, ok := resp.(map[string]interface{})
	if !ok {
		t.Fatalf("expected map response, got %T", resp)
	}
	if result["status"] != "registered" {
		t.Errorf("expected status 'registered', got %v", result["status"])
	}
}

func TestFullLifecycle_SubmitAndRetrieveTask(t *testing.T) {
	srv := newTestServer(t)

	// Submit a task via REST API
	task := map[string]interface{}{
		"repos":  []string{"/path/to/repo"},
		"prompt": "Build a feature",
		"tags":   []string{"business-default"},
	}
	body, _ := json.Marshal(task)

	req := httptest.NewRequest(http.MethodPost, "/api/tasks", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	srv.HandleTasks(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", w.Code, w.Body.String())
	}

	var createdTask queue.Task
	if err := json.Unmarshal(w.Body.Bytes(), &createdTask); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}

	if createdTask.ID == "" {
		t.Error("expected non-empty task ID")
	}
	if createdTask.Status != queue.TaskStatusPending {
		t.Errorf("expected PENDING status, got %s", createdTask.Status)
	}

	// Retrieve the task
	req2 := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/tasks/%s", createdTask.ID), nil)
	w2 := httptest.NewRecorder()
	srv.HandleTaskDetail(w2, req2)

	if w2.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w2.Code, w2.Body.String())
	}

	// Response is now {task: ..., logs: ..., log_count: ...}
	var detailResponse map[string]interface{}
	if err := json.Unmarshal(w2.Body.Bytes(), &detailResponse); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	taskData, ok := detailResponse["task"]
	if !ok {
		t.Fatal("expected 'task' field in response")
	}

	var retrievedTask queue.Task
	taskBytes, _ := json.Marshal(taskData)
	if err := json.Unmarshal(taskBytes, &retrievedTask); err != nil {
		t.Fatalf("failed to unmarshal task: %v", err)
	}

	if retrievedTask.ID != createdTask.ID {
		t.Errorf("expected task ID %s, got %s", createdTask.ID, retrievedTask.ID)
	}
	if retrievedTask.Prompt != "Build a feature" {
		t.Errorf("expected prompt 'Build a feature', got %s", retrievedTask.Prompt)
	}
}

func TestFullLifecycle_ListTasks(t *testing.T) {
	srv := newTestServer(t)

	// Add multiple tasks
	for i := 0; i < 3; i++ {
		task := map[string]interface{}{
			"repos":  []string{"/repo"},
			"prompt": fmt.Sprintf("Task %d", i),
		}
		body, _ := json.Marshal(task)
		req := httptest.NewRequest(http.MethodPost, "/api/tasks", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		srv.HandleTasks(w, req)
	}

	// List all tasks
	req := httptest.NewRequest(http.MethodGet, "/api/tasks", nil)
	w := httptest.NewRecorder()
	srv.HandleTasks(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var response struct {
		Tasks []queue.Task `json:"tasks"`
		Count int          `json:"count"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	if response.Count != 3 {
		t.Errorf("expected 3 tasks, got %d", response.Count)
	}
	if len(response.Tasks) != 3 {
		t.Errorf("expected 3 task entries, got %d", len(response.Tasks))
	}
}

func TestFullLifecycle_TaskStatusTransitions(t *testing.T) {
	srv := newTestServer(t)

	// Create a task
	task := map[string]interface{}{
		"repos":  []string{"/repo"},
		"prompt": "Test task",
	}
	body, _ := json.Marshal(task)
	req := httptest.NewRequest(http.MethodPost, "/api/tasks", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.HandleTasks(w, req)

	var createdTask queue.Task
	json.Unmarshal(w.Body.Bytes(), &createdTask)

	// Assign the task
	err := srv.TaskQueue().Assign(createdTask.ID, "guest-1")
	if err != nil {
		t.Fatalf("assign failed: %v", err)
	}

	taskData, ok := srv.TaskQueue().Get(createdTask.ID)
	if !ok {
		t.Fatal("task not found after assign")
	}
	if taskData.Status != queue.TaskStatusAssigned {
		t.Errorf("expected ASSIGNED, got %s", taskData.Status)
	}
	if taskData.AssignedTo != "guest-1" {
		t.Errorf("expected assigned to guest-1, got %s", taskData.AssignedTo)
	}

	// Start the task
	err = srv.TaskQueue().Start(createdTask.ID)
	if err != nil {
		t.Fatalf("start failed: %v", err)
	}

	taskData, _ = srv.TaskQueue().Get(createdTask.ID)
	if taskData.Status != queue.TaskStatusRunning {
		t.Errorf("expected RUNNING, got %s", taskData.Status)
	}

	// Complete the task
	err = srv.TaskQueue().Complete(createdTask.ID, "all tests pass")
	if err != nil {
		t.Fatalf("complete failed: %v", err)
	}

	taskData, _ = srv.TaskQueue().Get(createdTask.ID)
	if taskData.Status != queue.TaskStatusCompleted {
		t.Errorf("expected COMPLETED, got %s", taskData.Status)
	}
	if taskData.Result != "all tests pass" {
		t.Errorf("expected result 'all tests pass', got %s", taskData.Result)
	}
}

func TestFullLifecycle_TaskFailure(t *testing.T) {
	srv := newTestServer(t)

	// Create and start a task
	task := map[string]interface{}{
		"repos":  []string{"/repo"},
		"prompt": "Failing task",
	}
	body, _ := json.Marshal(task)
	req := httptest.NewRequest(http.MethodPost, "/api/tasks", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.HandleTasks(w, req)

	var createdTask queue.Task
	json.Unmarshal(w.Body.Bytes(), &createdTask)

	srv.TaskQueue().Assign(createdTask.ID, "guest-1")
	srv.TaskQueue().Start(createdTask.ID)

	// Fail the task
	err := srv.TaskQueue().Fail(createdTask.ID, "build failed")
	if err != nil {
		t.Fatalf("fail failed: %v", err)
	}

	taskData, _ := srv.TaskQueue().Get(createdTask.ID)
	if taskData.Status != queue.TaskStatusFailed {
		t.Errorf("expected FAILED, got %s", taskData.Status)
	}
	if taskData.Error != "build failed" {
		t.Errorf("expected error 'build failed', got %s", taskData.Error)
	}
}

func TestFullLifecycle_CancelTask(t *testing.T) {
	srv := newTestServer(t)

	// Create a pending task
	task := map[string]interface{}{
		"repos":  []string{"/repo"},
		"prompt": "To be cancelled",
	}
	body, _ := json.Marshal(task)
	req := httptest.NewRequest(http.MethodPost, "/api/tasks", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.HandleTasks(w, req)

	var createdTask queue.Task
	json.Unmarshal(w.Body.Bytes(), &createdTask)

	// Cancel the task
	err := srv.TaskQueue().Cancel(createdTask.ID)
	if err != nil {
		t.Fatalf("cancel failed: %v", err)
	}

	taskData, _ := srv.TaskQueue().Get(createdTask.ID)
	if taskData.Status != queue.TaskStatusCancelled {
		t.Errorf("expected CANCELLED, got %s", taskData.Status)
	}
}

func TestFullLifecycle_InvalidTaskSubmission(t *testing.T) {
	srv := newTestServer(t)

	// Submit with empty body
	req := httptest.NewRequest(http.MethodPost, "/api/tasks", bytes.NewReader([]byte{}))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.HandleTasks(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for empty body, got %d", w.Code)
	}
}

func TestFullLifecycle_HealthCheck(t *testing.T) {
	srv := newTestServer(t)

	req := httptest.NewRequest(http.MethodGet, "/api/health", nil)
	w := httptest.NewRecorder()
	srv.HandleHealth(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var response map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &response)

	if response["status"] != "healthy" {
		t.Errorf("expected status 'healthy', got %v", response["status"])
	}
}

func TestFullLifecycle_GuestRegistrationViaREST(t *testing.T) {
	srv := newTestServer(t)

	// Register a guest
	guest, err := srv.Registry().Register("test-guest-1", "Test Guest", []string{"business-default"})
	if err != nil {
		t.Fatalf("register failed: %v", err)
	}

	if guest.ID != "test-guest-1" {
		t.Errorf("expected id test-guest-1, got %s", guest.ID)
	}
	if guest.State != registry.GuestStateIdle {
		t.Errorf("expected IDLE state, got %s", guest.State)
	}

	// List guests via REST
	req := httptest.NewRequest(http.MethodGet, "/api/guests", nil)
	w := httptest.NewRecorder()
	srv.HandleGuests(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var response struct {
		Guests []registry.Guest `json:"guests"`
		Count  int              `json:"count"`
	}
	json.Unmarshal(w.Body.Bytes(), &response)

	if response.Count != 1 {
		t.Errorf("expected 1 guest, got %d", response.Count)
	}
}

func TestFullLifecycle_TagBasedScheduling(t *testing.T) {
	srv := newTestServer(t)

	// Register two guests with different tags
	srv.Registry().Register("android-guest", "Android Guest", []string{"android"})
	srv.Registry().Register("frontend-guest", "Frontend Guest", []string{"frontend"})

	// Verify tag-based guest lookup works
	guests := srv.Registry().FindAvailableGuests([]string{"android"})
	if len(guests) != 1 {
		t.Errorf("expected 1 android guest, got %d", len(guests))
	}
	if len(guests) > 0 && guests[0].ID != "android-guest" {
		t.Errorf("expected android-guest, got %s", guests[0].ID)
	}

	guests = srv.Registry().FindAvailableGuests([]string{"frontend"})
	if len(guests) != 1 {
		t.Errorf("expected 1 frontend guest, got %d", len(guests))
	}
	if len(guests) > 0 && guests[0].ID != "frontend-guest" {
		t.Errorf("expected frontend-guest, got %s", guests[0].ID)
	}

	// No guests match nonexistent tag
	guests = srv.Registry().FindAvailableGuests([]string{"nonexistent"})
	if len(guests) != 0 {
		t.Errorf("expected 0 guests for nonexistent tag, got %d", len(guests))
	}

	// Submit a task with no tag requirements
	task := map[string]interface{}{
		"repos":  []string{"/repo"},
		"prompt": "Generic task",
	}
	body, _ := json.Marshal(task)
	req := httptest.NewRequest(http.MethodPost, "/api/tasks", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.HandleTasks(w, req)

	var createdTask queue.Task
	json.Unmarshal(w.Body.Bytes(), &createdTask)

	// Task should be created successfully
	if createdTask.ID == "" {
		t.Error("expected non-empty task ID")
	}
}

func TestFullLifecycle_ConcurrentTaskSubmission(t *testing.T) {
	srv := newTestServer(t)

	var wg sync.WaitGroup
	taskCount := 20

	for i := 0; i < taskCount; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			task := map[string]interface{}{
				"repos":  []string{"/repo"},
				"prompt": fmt.Sprintf("Concurrent task %d", id),
			}
			body, _ := json.Marshal(task)
			req := httptest.NewRequest(http.MethodPost, "/api/tasks", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()
			srv.HandleTasks(w, req)

			if w.Code != http.StatusCreated {
				t.Errorf("task %d: expected 201, got %d", id, w.Code)
			}
		}(i)
	}

	wg.Wait()

	// Verify all tasks were created
	req := httptest.NewRequest(http.MethodGet, "/api/tasks", nil)
	w := httptest.NewRecorder()
	srv.HandleTasks(w, req)

	var response struct {
		Count int `json:"count"`
	}
	json.Unmarshal(w.Body.Bytes(), &response)

	if response.Count != taskCount {
		t.Errorf("expected %d tasks, got %d", taskCount, response.Count)
	}
}

func TestFullLifecycle_DuplicateTaskID(t *testing.T) {
	srv := newTestServer(t)

	// Create first task
	task1 := map[string]interface{}{
		"id":     "unique-task-1",
		"repos":  []string{"/repo"},
		"prompt": "First task",
	}
	body1, _ := json.Marshal(task1)
	req1 := httptest.NewRequest(http.MethodPost, "/api/tasks", bytes.NewReader(body1))
	req1.Header.Set("Content-Type", "application/json")
	w1 := httptest.NewRecorder()
	srv.HandleTasks(w1, req1)

	if w1.Code != http.StatusCreated {
		t.Fatalf("expected 201 for first task, got %d", w1.Code)
	}

	// Try to create task with same ID
	task2 := map[string]interface{}{
		"id":     "unique-task-1",
		"repos":  []string{"/repo"},
		"prompt": "Duplicate task",
	}
	body2, _ := json.Marshal(task2)
	req2 := httptest.NewRequest(http.MethodPost, "/api/tasks", bytes.NewReader(body2))
	req2.Header.Set("Content-Type", "application/json")
	w2 := httptest.NewRecorder()
	srv.HandleTasks(w2, req2)

	if w2.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for duplicate task ID, got %d", w2.Code)
	}
}

func TestFullLifecycle_TaskNotFound(t *testing.T) {
	srv := newTestServer(t)

	req := httptest.NewRequest(http.MethodGet, "/api/tasks/nonexistent", nil)
	w := httptest.NewRecorder()
	srv.HandleTaskDetail(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", w.Code)
	}
}

func TestFullLifecycle_GuestNotFound(t *testing.T) {
	srv := newTestServer(t)

	req := httptest.NewRequest(http.MethodGet, "/api/guests/nonexistent", nil)
	w := httptest.NewRecorder()
	srv.HandleGuestDetail(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", w.Code)
	}
}

func TestFullLifecycle_MethodNotAllowed(t *testing.T) {
	srv := newTestServer(t)

	// PUT to /api/tasks should return 405
	req := httptest.NewRequest(http.MethodPut, "/api/tasks", nil)
	w := httptest.NewRecorder()
	srv.HandleTasks(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405, got %d", w.Code)
	}

	// DELETE to /api/guests should return 405
	req2 := httptest.NewRequest(http.MethodDelete, "/api/guests", nil)
	w2 := httptest.NewRecorder()
	srv.HandleGuests(w2, req2)

	if w2.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405, got %d", w2.Code)
	}
}

func TestFullLifecycle_Heartbeat(t *testing.T) {
	srv := newTestServer(t)

	// Register a guest
	srv.Registry().Register("heartbeat-guest", "Heartbeat Guest", []string{"test"})

	// Send heartbeat
	err := srv.Registry().Heartbeat("heartbeat-guest")
	if err != nil {
		t.Fatalf("heartbeat failed: %v", err)
	}

	// Verify guest still exists
	guest, ok := srv.Registry().GetGuest("heartbeat-guest")
	if !ok {
		t.Fatal("guest disappeared after heartbeat")
	}
	if guest.State != registry.GuestStateIdle {
		t.Errorf("expected IDLE, got %s", guest.State)
	}
}

func TestFullLifecycle_UnregisterGuest(t *testing.T) {
	srv := newTestServer(t)

	// Register a guest
	srv.Registry().Register("unreg-guest", "Unregister Guest", []string{"test"})

	// Unregister
	err := srv.Registry().Unregister("unreg-guest")
	if err != nil {
		t.Fatalf("unregister failed: %v", err)
	}

	// Verify guest is gone
	_, ok := srv.Registry().GetGuest("unreg-guest")
	if ok {
		t.Error("guest should not exist after unregister")
	}
}

func TestFullLifecycle_UnregisterRunningGuest(t *testing.T) {
	srv := newTestServer(t)

	// Register and assign a task to a guest
	srv.Registry().Register("running-guest", "Running Guest", []string{"test"})
	srv.Registry().SetGuestTask("running-guest", "task-1")

	// Try to unregister a running guest - should fail
	err := srv.Registry().Unregister("running-guest")
	if err == nil {
		t.Error("expected error when unregistering running guest")
	}
}

func TestFullLifecycle_StaleGuestDetection(t *testing.T) {
	srv := newTestServer(t)

	// Register guests
	srv.Registry().Register("stale-guest", "Stale Guest", []string{"test"})
	time.Sleep(10 * time.Millisecond)
	srv.Registry().Register("fresh-guest", "Fresh Guest", []string{"test"})

	// Remove stale guests with very short timeout
	stale := srv.Registry().RemoveStaleGuests(5 * time.Millisecond)
	if len(stale) != 1 {
		t.Errorf("expected 1 stale guest, got %d", len(stale))
	}
	if len(stale) > 0 && stale[0].ID != "stale-guest" {
		t.Errorf("expected stale-guest to be stale, got %s", stale[0].ID)
	}
}

func TestFullLifecycle_TaskQueueConcurrency(t *testing.T) {
	srv := newTestServer(t)

	var wg sync.WaitGroup
	numTasks := 50

	// Concurrent task creation
	for i := 0; i < numTasks; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			task := map[string]interface{}{
				"repos":  []string{"/repo"},
				"prompt": fmt.Sprintf("Concurrent task %d", id),
			}
			body, _ := json.Marshal(task)
			req := httptest.NewRequest(http.MethodPost, "/api/tasks", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()
			srv.HandleTasks(w, req)
		}(i)
	}

	wg.Wait()

	// Verify all tasks were created
	req := httptest.NewRequest(http.MethodGet, "/api/tasks", nil)
	w := httptest.NewRecorder()
	srv.HandleTasks(w, req)

	var response struct {
		Count int `json:"count"`
	}
	json.Unmarshal(w.Body.Bytes(), &response)

	if response.Count != numTasks {
		t.Errorf("expected %d tasks, got %d", numTasks, response.Count)
	}
}

func TestFullLifecycle_RPCDispatch(t *testing.T) {
	srv := newTestServer(t)
	hub := srv.Hub()

	go hub.Run()

	// Test guest.register
	params, _ := json.Marshal(map[string]interface{}{
		"id":   "rpc-guest-1",
		"name": "RPC Guest",
		"tags": []string{"test"},
	})
	resp, err := hub.Dispatch("guest.register", params)
	if err != nil {
		t.Fatalf("guest.register failed: %v", err)
	}
	result := resp.(map[string]interface{})
	if result["status"] != "registered" {
		t.Errorf("expected registered, got %v", result["status"])
	}

	// Test guest.heartbeat
	params, _ = json.Marshal(map[string]interface{}{"id": "rpc-guest-1"})
	resp, err = hub.Dispatch("guest.heartbeat", params)
	if err != nil {
		t.Fatalf("guest.heartbeat failed: %v", err)
	}
	if result, _ := resp.(map[string]interface{}); result["status"] != "ok" {
		t.Errorf("expected ok, got %v", result["status"])
	}

	// Test method not found
	_, err = hub.Dispatch("nonexistent.method", json.RawMessage("{}"))
	if err == nil {
		t.Error("expected error for nonexistent method")
	}
	if err.Code != rpc.CodeMethodNotFound {
		t.Errorf("expected CodeMethodNotFound, got %d", err.Code)
	}

	// Test invalid params
	params, _ = json.Marshal(map[string]interface{}{})
	_, err = hub.Dispatch("guest.register", params)
	if err == nil {
		t.Error("expected error for missing id")
	}
}

func TestFullLifecycle_GuestUnregister(t *testing.T) {
	srv := newTestServer(t)
	hub := srv.Hub()

	go hub.Run()

	// Register a guest
	params, _ := json.Marshal(map[string]interface{}{
		"id":   "unreg-guest",
		"name": "Unregister Guest",
		"tags": []string{"test"},
	})
	_, err := hub.Dispatch("guest.register", params)
	if err != nil {
		t.Fatalf("register failed: %v", err)
	}

	// Unregister the guest
	params, _ = json.Marshal(map[string]interface{}{"id": "unreg-guest"})
	resp, err := hub.Dispatch("guest.unregister", params)
	if err != nil {
		t.Fatalf("unregister failed: %v", err)
	}
	result := resp.(map[string]interface{})
	if result["status"] != "unregistered" {
		t.Errorf("expected unregistered, got %v", result["status"])
	}
}

func TestFullLifecycle_GuestLog(t *testing.T) {
	srv := newTestServer(t)
	hub := srv.Hub()

	go hub.Run()

	// Send a log entry
	params, _ := json.Marshal(map[string]interface{}{
		"task_id": "task-1",
		"line":    "Building project...",
		"level":   "info",
	})
	resp, err := hub.Dispatch("guest.log", params)
	if err != nil {
		t.Fatalf("guest.log failed: %v", err)
	}
	result := resp.(map[string]interface{})
	if result["status"] != "accepted" {
		t.Errorf("expected accepted, got %v", result["status"])
	}
}

func TestFullLifecycle_GuestStateIsString(t *testing.T) {
	srv := newTestServer(t)

	// Register guests with different states
	srv.Registry().Register("idle-guest", "Idle Guest", []string{"business-default"})
	srv.Registry().Register("running-guest", "Running Guest", []string{"android"})
	srv.Registry().SetGuestState("running-guest", registry.GuestStateRunning)

	// List guests via REST
	req := httptest.NewRequest(http.MethodGet, "/api/guests", nil)
	w := httptest.NewRecorder()
	srv.HandleGuests(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var response struct {
		Guests []map[string]interface{} `json:"guests"`
		Count  int                      `json:"count"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	if response.Count != 2 {
		t.Fatalf("expected 2 guests, got %d", response.Count)
	}

	// Build a map keyed by guest ID for deterministic lookups
	guestMap := make(map[string]map[string]interface{})
	for _, guest := range response.Guests {
		guestMap[guest["id"].(string)] = guest
	}

	// Verify each guest's state is a string (not an integer)
	for id, g := range guestMap {
		state, ok := g["state"]
		if !ok {
			t.Fatalf("guest %s: expected 'state' field", id)
		}
		if _, isString := state.(string); !isString {
			t.Errorf("guest %s: expected state to be a string, got %T: %v", id, state, state)
		}
	}

	// Verify the running guest specifically has state "RUNNING"
	runningGuest, ok := guestMap["running-guest"]
	if !ok {
		t.Fatal("expected running-guest in response")
	}
	if runningGuest["state"] != "RUNNING" {
		t.Errorf("expected running guest state 'RUNNING', got %v", runningGuest["state"])
	}

	// Verify the idle guest has a valid state string
	idleGuest, ok := guestMap["idle-guest"]
	if !ok {
		t.Fatal("expected idle-guest in response")
	}
	idleState, ok := idleGuest["state"].(string)
	if !ok {
		t.Fatalf("expected idle guest state to be a string, got %T", idleGuest["state"])
	}
	switch idleState {
	case "IDLE", "DISCONNECTED", "REGISTERED":
		// valid initial states
	default:
		t.Errorf("unexpected idle guest state: %s", idleState)
	}
}

func TestFullLifecycle_SubmitTaskThenList(t *testing.T) {
	srv := newTestServer(t)

	// Submit a task via REST API
	task := map[string]interface{}{
		"repos":  []string{"/path/to/repo"},
		"prompt": "Submit then list test",
		"tags":   []string{"business-default"},
	}
	body, _ := json.Marshal(task)
	req := httptest.NewRequest(http.MethodPost, "/api/tasks", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.HandleTasks(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", w.Code, w.Body.String())
	}

	var createdTask queue.Task
	if err := json.Unmarshal(w.Body.Bytes(), &createdTask); err != nil {
		t.Fatalf("failed to unmarshal created task: %v", err)
	}

	// Verify the task was created with correct status
	if createdTask.Status != queue.TaskStatusPending {
		t.Errorf("expected PENDING status, got %s", createdTask.Status)
	}
	if createdTask.ID == "" {
		t.Fatal("expected non-empty task ID")
	}
	if createdTask.Prompt != "Submit then list test" {
		t.Errorf("expected prompt 'Submit then list test', got %s", createdTask.Prompt)
	}

	// List all tasks and verify the submitted task appears
	req2 := httptest.NewRequest(http.MethodGet, "/api/tasks", nil)
	w2 := httptest.NewRecorder()
	srv.HandleTasks(w2, req2)

	if w2.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w2.Code)
	}

	var listResponse struct {
		Tasks []map[string]interface{} `json:"tasks"`
		Count int                      `json:"count"`
	}
	if err := json.Unmarshal(w2.Body.Bytes(), &listResponse); err != nil {
		t.Fatalf("failed to unmarshal list response: %v", err)
	}

	if listResponse.Count != 1 {
		t.Errorf("expected 1 task in list, got %d", listResponse.Count)
	}
	if len(listResponse.Tasks) != 1 {
		t.Fatalf("expected 1 task entry, got %d", len(listResponse.Tasks))
	}

	// Verify the task in the list has the correct ID and status as a string
	listTask := listResponse.Tasks[0]
	if listTask["id"] != createdTask.ID {
		t.Errorf("expected task id %s in list, got %s", createdTask.ID, listTask["id"])
	}

	// Status must be a string (not an integer) — this is what the frontend reads
	status, ok := listTask["status"]
	if !ok {
		t.Fatal("expected 'status' field in list response")
	}
	if statusStr, isString := status.(string); !isString {
		t.Errorf("expected status to be a string, got %T: %v", status, status)
	} else if statusStr != "PENDING" {
		t.Errorf("expected status 'PENDING', got %q", statusStr)
	}
}

func TestFullLifecycle_GuestResult(t *testing.T) {
	srv := newTestServer(t)

	// Create a task first
	task := map[string]interface{}{
		"repos":  []string{"/repo"},
		"prompt": "Test result submission",
	}
	body, _ := json.Marshal(task)
	req := httptest.NewRequest(http.MethodPost, "/api/tasks", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.HandleTasks(w, req)

	var createdTask queue.Task
	json.Unmarshal(w.Body.Bytes(), &createdTask)

	// Assign and start the task
	srv.TaskQueue().Assign(createdTask.ID, "guest-1")
	srv.TaskQueue().Start(createdTask.ID)

	// Submit success result
	hub := srv.Hub()
	go hub.Run()
	params, _ := json.Marshal(map[string]interface{}{
		"task_id": createdTask.ID,
		"success": true,
		"output":  "Build successful",
	})
	_, err := hub.Dispatch("guest.result", params)
	if err != nil {
		t.Fatalf("guest.result failed: %v", err)
	}

	// Verify task is completed
	taskData, _ := srv.TaskQueue().Get(createdTask.ID)
	if taskData.Status != queue.TaskStatusCompleted {
		t.Errorf("expected COMPLETED, got %s", taskData.Status)
	}
}
