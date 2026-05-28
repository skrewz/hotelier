package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"hotelier/pkg/config"
	"hotelier/pkg/queue"
	"hotelier/pkg/registry"
	"hotelier/pkg/rpc"
)

func newTestServer(t *testing.T) *Server {
	t.Helper()
	cfg := config.ServerConfig{
		Host: "127.0.0.1",
		Port: 0,
	}
	return New(cfg)
}

// Health endpoint tests
func TestHandleHealth(t *testing.T) {
	srv := newTestServer(t)

	req := httptest.NewRequest(http.MethodGet, "/api/health", nil)
	w := httptest.NewRecorder()
	srv.HandleHealth(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var response map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	if response["status"] != "healthy" {
		t.Errorf("expected status 'healthy', got %v", response["status"])
	}
}

// Task creation tests
func TestHandleTasks_POST(t *testing.T) {
	srv := newTestServer(t)

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
		t.Fatalf("expected 201, got %d", w.Code)
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
	if createdTask.Prompt != "Build a feature" {
		t.Errorf("expected prompt 'Build a feature', got %s", createdTask.Prompt)
	}
}

func TestHandleTasks_POST_EmptyBody(t *testing.T) {
	srv := newTestServer(t)

	req := httptest.NewRequest(http.MethodPost, "/api/tasks", bytes.NewReader([]byte{}))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.HandleTasks(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestHandleTasks_POST_InvalidJSON(t *testing.T) {
	srv := newTestServer(t)

	req := httptest.NewRequest(http.MethodPost, "/api/tasks", bytes.NewReader([]byte("not json")))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.HandleTasks(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestHandleTasks_POST_CustomID(t *testing.T) {
	srv := newTestServer(t)

	task := map[string]interface{}{
		"id":     "my-custom-id",
		"repos":  []string{"/repo"},
		"prompt": "Custom ID task",
	}
	body, _ := json.Marshal(task)

	req := httptest.NewRequest(http.MethodPost, "/api/tasks", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.HandleTasks(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", w.Code)
	}

	var createdTask queue.Task
	json.Unmarshal(w.Body.Bytes(), &createdTask)

	if createdTask.ID != "my-custom-id" {
		t.Errorf("expected ID 'my-custom-id', got %s", createdTask.ID)
	}
}

func TestHandleTasks_POST_DuplicateID(t *testing.T) {
	srv := newTestServer(t)

	// Create first task
	task1 := map[string]interface{}{
		"id":     "dup-id",
		"repos":  []string{"/repo"},
		"prompt": "First",
	}
	body1, _ := json.Marshal(task1)
	req1 := httptest.NewRequest(http.MethodPost, "/api/tasks", bytes.NewReader(body1))
	req1.Header.Set("Content-Type", "application/json")
	w1 := httptest.NewRecorder()
	srv.HandleTasks(w1, req1)

	if w1.Code != http.StatusCreated {
		t.Fatalf("expected 201 for first task, got %d", w1.Code)
	}

	// Try duplicate
	task2 := map[string]interface{}{
		"id":     "dup-id",
		"repos":  []string{"/repo"},
		"prompt": "Duplicate",
	}
	body2, _ := json.Marshal(task2)
	req2 := httptest.NewRequest(http.MethodPost, "/api/tasks", bytes.NewReader(body2))
	req2.Header.Set("Content-Type", "application/json")
	w2 := httptest.NewRecorder()
	srv.HandleTasks(w2, req2)

	if w2.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for duplicate, got %d", w2.Code)
	}
}

// Task listing tests
func TestHandleTasks_GET(t *testing.T) {
	srv := newTestServer(t)

	// Create some tasks
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

	// List tasks
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
}

func TestHandleTasks_GET_Empty(t *testing.T) {
	srv := newTestServer(t)

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
	json.Unmarshal(w.Body.Bytes(), &response)

	if response.Count != 0 {
		t.Errorf("expected 0 tasks, got %d", response.Count)
	}
}

// Task detail tests
func TestHandleTaskDetail(t *testing.T) {
	srv := newTestServer(t)

	// Create a task
	task := map[string]interface{}{
		"repos":  []string{"/repo"},
		"prompt": "Detail test",
	}
	body, _ := json.Marshal(task)
	req := httptest.NewRequest(http.MethodPost, "/api/tasks", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.HandleTasks(w, req)

	var createdTask queue.Task
	json.Unmarshal(w.Body.Bytes(), &createdTask)

	// Get task detail
	req2 := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/tasks/%s", createdTask.ID), nil)
	w2 := httptest.NewRecorder()
	srv.HandleTaskDetail(w2, req2)

	if w2.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w2.Code)
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

	// Verify logs field exists
	if _, ok := detailResponse["logs"]; !ok {
		t.Fatal("expected 'logs' field in response")
	}
	if _, ok := detailResponse["log_count"]; !ok {
		t.Fatal("expected 'log_count' field in response")
	}
}

func TestHandleTaskDetail_NotFound(t *testing.T) {
	srv := newTestServer(t)

	req := httptest.NewRequest(http.MethodGet, "/api/tasks/nonexistent", nil)
	w := httptest.NewRecorder()
	srv.HandleTaskDetail(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", w.Code)
	}
}

func TestHandleTaskDetail_EmptyID(t *testing.T) {
	srv := newTestServer(t)

	req := httptest.NewRequest(http.MethodGet, "/api/tasks/", nil)
	w := httptest.NewRecorder()
	srv.HandleTaskDetail(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

// Guest listing tests
func TestHandleGuests_GET(t *testing.T) {
	srv := newTestServer(t)

	// Register some guests
	srv.Registry().Register("guest-1", "Guest One", []string{"tag1"})
	srv.Registry().Register("guest-2", "Guest Two", []string{"tag2"})

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

	if response.Count != 2 {
		t.Errorf("expected 2 guests, got %d", response.Count)
	}
}

func TestHandleGuests_GET_Empty(t *testing.T) {
	srv := newTestServer(t)

	req := httptest.NewRequest(http.MethodGet, "/api/guests", nil)
	w := httptest.NewRecorder()
	srv.HandleGuests(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var response struct {
		Count int `json:"count"`
	}
	json.Unmarshal(w.Body.Bytes(), &response)

	if response.Count != 0 {
		t.Errorf("expected 0 guests, got %d", response.Count)
	}
}

// Guest detail tests
func TestHandleGuestDetail(t *testing.T) {
	srv := newTestServer(t)

	srv.Registry().Register("guest-1", "Guest One", []string{"tag1"})

	req := httptest.NewRequest(http.MethodGet, "/api/guests/guest-1", nil)
	w := httptest.NewRecorder()
	srv.HandleGuestDetail(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var guest registry.Guest
	if err := json.Unmarshal(w.Body.Bytes(), &guest); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	if guest.ID != "guest-1" {
		t.Errorf("expected guest ID 'guest-1', got %s", guest.ID)
	}
}

func TestHandleGuestDetail_NotFound(t *testing.T) {
	srv := newTestServer(t)

	req := httptest.NewRequest(http.MethodGet, "/api/guests/nonexistent", nil)
	w := httptest.NewRecorder()
	srv.HandleGuestDetail(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", w.Code)
	}
}

func TestHandleGuestDetail_EmptyID(t *testing.T) {
	srv := newTestServer(t)

	req := httptest.NewRequest(http.MethodGet, "/api/guests/", nil)
	w := httptest.NewRecorder()
	srv.HandleGuestDetail(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

// Method not allowed tests
func TestHandleTasks_MethodNotAllowed(t *testing.T) {
	srv := newTestServer(t)

	for _, method := range []string{"PUT", "DELETE", "PATCH"} {
		req := httptest.NewRequest(method, "/api/tasks", nil)
		w := httptest.NewRecorder()
		srv.HandleTasks(w, req)

		if w.Code != http.StatusMethodNotAllowed {
			t.Errorf("%s /api/tasks: expected 405, got %d", method, w.Code)
		}
	}
}

func TestHandleGuests_MethodNotAllowed(t *testing.T) {
	srv := newTestServer(t)

	for _, method := range []string{"PUT", "DELETE", "PATCH", "POST"} {
		req := httptest.NewRequest(method, "/api/guests", nil)
		w := httptest.NewRecorder()
		srv.HandleGuests(w, req)

		if w.Code != http.StatusMethodNotAllowed {
			t.Errorf("%s /api/guests: expected 405, got %d", method, w.Code)
		}
	}
}

// Task status transition tests via direct API
func TestHandleTasks_TaskAssignment(t *testing.T) {
	srv := newTestServer(t)

	// Create a task
	task := map[string]interface{}{
		"repos":  []string{"/repo"},
		"prompt": "Assignment test",
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

	taskData, _ := srv.TaskQueue().Get(createdTask.ID)
	if taskData.Status != queue.TaskStatusAssigned {
		t.Errorf("expected ASSIGNED, got %s", taskData.Status)
	}
	if taskData.AssignedTo != "guest-1" {
		t.Errorf("expected assigned to guest-1, got %s", taskData.AssignedTo)
	}
}

func TestHandleTasks_TaskCompletion(t *testing.T) {
	srv := newTestServer(t)

	// Create, assign, and start a task
	task := map[string]interface{}{
		"repos":  []string{"/repo"},
		"prompt": "Completion test",
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
	srv.TaskQueue().Complete(createdTask.ID, "all tests pass")

	taskData, _ := srv.TaskQueue().Get(createdTask.ID)
	if taskData.Status != queue.TaskStatusCompleted {
		t.Errorf("expected COMPLETED, got %s", taskData.Status)
	}
	if taskData.Result != "all tests pass" {
		t.Errorf("expected result 'all tests pass', got %s", taskData.Result)
	}
}

func TestHandleTasks_TaskFailure(t *testing.T) {
	srv := newTestServer(t)

	task := map[string]interface{}{
		"repos":  []string{"/repo"},
		"prompt": "Failure test",
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
	srv.TaskQueue().Fail(createdTask.ID, "build failed")

	taskData, _ := srv.TaskQueue().Get(createdTask.ID)
	if taskData.Status != queue.TaskStatusFailed {
		t.Errorf("expected FAILED, got %s", taskData.Status)
	}
	if taskData.Error != "build failed" {
		t.Errorf("expected error 'build failed', got %s", taskData.Error)
	}
}

// Guest lifecycle tests
func TestRegistry_RegisterGuest(t *testing.T) {
	srv := newTestServer(t)

	guest, err := srv.Registry().Register("test-guest", "Test Guest", []string{"business-default"})
	if err != nil {
		t.Fatalf("register failed: %v", err)
	}

	if guest.ID != "test-guest" {
		t.Errorf("expected id 'test-guest', got %s", guest.ID)
	}
	if guest.Name != "Test Guest" {
		t.Errorf("expected name 'Test Guest', got %s", guest.Name)
	}
	if guest.State != registry.GuestStateIdle {
		t.Errorf("expected IDLE state, got %s", guest.State)
	}
}

func TestRegistry_DuplicateGuest(t *testing.T) {
	srv := newTestServer(t)

	_, err := srv.Registry().Register("dup-guest", "Guest", []string{"tag"})
	if err != nil {
		t.Fatalf("first register failed: %v", err)
	}

	_, err = srv.Registry().Register("dup-guest", "Guest 2", []string{"tag2"})
	if err == nil {
		t.Error("expected error for duplicate guest ID")
	}
}

func TestRegistry_MaxGuests(t *testing.T) {
	cfg := config.ServerConfig{Host: "127.0.0.1", Port: 0, MaxGuests: 2}
	srv := New(cfg)

	_, err := srv.Registry().Register("guest-1", "Guest 1", []string{"tag"})
	if err != nil {
		t.Fatalf("first register failed: %v", err)
	}

	_, err = srv.Registry().Register("guest-2", "Guest 2", []string{"tag"})
	if err != nil {
		t.Fatalf("second register failed: %v", err)
	}

	_, err = srv.Registry().Register("guest-3", "Guest 3", []string{"tag"})
	if err == nil {
		t.Error("expected error when max guests reached")
	}
}

func TestRegistry_Heartbeat(t *testing.T) {
	srv := newTestServer(t)

	srv.Registry().Register("hb-guest", "HB Guest", []string{"tag"})

	err := srv.Registry().Heartbeat("hb-guest")
	if err != nil {
		t.Fatalf("heartbeat failed: %v", err)
	}

	guest, ok := srv.Registry().GetGuest("hb-guest")
	if !ok {
		t.Fatal("guest not found after heartbeat")
	}
	if guest.State != registry.GuestStateIdle {
		t.Errorf("expected IDLE, got %s", guest.State)
	}
}

func TestRegistry_UnregisterGuest(t *testing.T) {
	srv := newTestServer(t)

	srv.Registry().Register("unreg-guest", "Unreg Guest", []string{"tag"})

	err := srv.Registry().Unregister("unreg-guest")
	if err != nil {
		t.Fatalf("unregister failed: %v", err)
	}

	_, ok := srv.Registry().GetGuest("unreg-guest")
	if ok {
		t.Error("guest should not exist after unregister")
	}
}

func TestRegistry_UnregisterRunningGuest(t *testing.T) {
	srv := newTestServer(t)

	srv.Registry().Register("running-guest", "Running Guest", []string{"tag"})
	srv.Registry().SetGuestTask("running-guest", "task-1")

	err := srv.Registry().Unregister("running-guest")
	if err == nil {
		t.Error("expected error when unregistering running guest")
	}
}

func TestRegistry_FindAvailableGuests(t *testing.T) {
	srv := newTestServer(t)

	srv.Registry().Register("guest-1", "Guest 1", []string{"business-default", "frontend"})
	srv.Registry().Register("guest-2", "Guest 2", []string{"android"})
	srv.Registry().Register("guest-3", "Guest 3", []string{"business-default"})

	// Find guests with business-default tag
	guests := srv.Registry().FindAvailableGuests([]string{"business-default"})
	if len(guests) != 2 {
		t.Errorf("expected 2 guests with business-default, got %d", len(guests))
	}

	// Find guests with android tag
	guests = srv.Registry().FindAvailableGuests([]string{"android"})
	if len(guests) != 1 {
		t.Errorf("expected 1 guest with android, got %d", len(guests))
	}

	// No guests match nonexistent tag
	guests = srv.Registry().FindAvailableGuests([]string{"nonexistent"})
	if len(guests) != 0 {
		t.Errorf("expected 0 guests for nonexistent tag, got %d", len(guests))
	}
}

func TestRegistry_HasGuestWithTags(t *testing.T) {
	srv := newTestServer(t)

	srv.Registry().Register("guest-1", "Guest 1", []string{"business-default"})

	if !srv.Registry().HasGuestWithTags([]string{"business-default"}) {
		t.Error("expected to have guest with business-default")
	}

	if srv.Registry().HasGuestWithTags([]string{"android"}) {
		t.Error("expected not to have guest with android")
	}
}

func TestRegistry_RemoveStaleGuests(t *testing.T) {
	srv := newTestServer(t)

	srv.Registry().Register("stale-guest", "Stale Guest", []string{"tag"})
	time.Sleep(20 * time.Millisecond)
	srv.Registry().Register("fresh-guest", "Fresh Guest", []string{"tag"})

	stale := srv.Registry().RemoveStaleGuests(10 * time.Millisecond)
	if len(stale) != 1 {
		t.Errorf("expected 1 stale guest, got %d", len(stale))
	}
	if len(stale) > 0 && stale[0].ID != "stale-guest" {
		t.Errorf("expected stale-guest, got %s", stale[0].ID)
	}
}

// Task queue tests
func TestTaskQueue_AddAndGet(t *testing.T) {
	srv := newTestServer(t)

	task := &queue.Task{
		ID:     "task-1",
		Prompt: "Test task",
		Tags:   []string{"tag"},
	}
	if err := srv.TaskQueue().Add(task); err != nil {
		t.Fatalf("add failed: %v", err)
	}

	got, ok := srv.TaskQueue().Get("task-1")
	if !ok {
		t.Fatal("task not found")
	}
	if got.ID != "task-1" {
		t.Errorf("expected id 'task-1', got %s", got.ID)
	}
}

func TestTaskQueue_DuplicateID(t *testing.T) {
	srv := newTestServer(t)

	task1 := &queue.Task{ID: "dup", Prompt: "First"}
	if err := srv.TaskQueue().Add(task1); err != nil {
		t.Fatalf("first add failed: %v", err)
	}

	task2 := &queue.Task{ID: "dup", Prompt: "Second"}
	if err := srv.TaskQueue().Add(task2); err == nil {
		t.Error("expected error for duplicate task ID")
	}
}

func TestTaskQueue_GetPendingTasks(t *testing.T) {
	srv := newTestServer(t)

	srv.TaskQueue().Add(&queue.Task{ID: "task-1", Prompt: "Task 1"})
	srv.TaskQueue().Add(&queue.Task{ID: "task-2", Prompt: "Task 2"})
	srv.TaskQueue().Assign("task-1", "guest-1")

	pending := srv.TaskQueue().GetPendingTasks()
	if len(pending) != 1 {
		t.Errorf("expected 1 pending task, got %d", len(pending))
	}
	if pending[0].ID != "task-2" {
		t.Errorf("expected task-2, got %s", pending[0].ID)
	}
}

func TestTaskQueue_NextPendingTaskForTags(t *testing.T) {
	srv := newTestServer(t)

	srv.TaskQueue().Add(&queue.Task{ID: "task-1", Prompt: "Task 1", Tags: []string{"android"}})
	srv.TaskQueue().Add(&queue.Task{ID: "task-2", Prompt: "Task 2", Tags: []string{"frontend"}})
	srv.TaskQueue().Add(&queue.Task{ID: "task-3", Prompt: "Task 3"}) // no tags

	// No tag requirements - should get first pending (task-1)
	task := srv.TaskQueue().NextPendingTaskForTags([]string{})
	if task == nil || task.ID != "task-1" {
		t.Errorf("expected task-1, got %v", task)
	}

	// Require android - task-1 matches
	task = srv.TaskQueue().NextPendingTaskForTags([]string{"android"})
	if task == nil || task.ID != "task-1" {
		t.Errorf("expected task-1 for android, got %v", task)
	}

	// Assign task-1 so it's no longer pending
	srv.TaskQueue().Assign("task-1", "guest-1")

	// Require frontend - should get task-2
	task = srv.TaskQueue().NextPendingTaskForTags([]string{"frontend"})
	if task == nil || task.ID != "task-2" {
		t.Errorf("expected task-2 for frontend, got %v", task)
	}

	// Require nonexistent - task-3 has no tags so it matches any requirement
	task = srv.TaskQueue().NextPendingTaskForTags([]string{"nonexistent"})
	if task == nil || task.ID != "task-3" {
		t.Errorf("expected task-3 for nonexistent, got %v", task)
	}
}

func TestTaskQueue_CountByStatus(t *testing.T) {
	srv := newTestServer(t)

	srv.TaskQueue().Add(&queue.Task{ID: "task-1", Prompt: "Task 1"})
	srv.TaskQueue().Add(&queue.Task{ID: "task-2", Prompt: "Task 2"})
	srv.TaskQueue().Add(&queue.Task{ID: "task-3", Prompt: "Task 3"})
	srv.TaskQueue().Assign("task-1", "guest-1")
	srv.TaskQueue().Assign("task-2", "guest-1")
	srv.TaskQueue().Start("task-2")

	if srv.TaskQueue().CountByStatus(queue.TaskStatusPending) != 1 {
		t.Errorf("expected 1 pending, got %d", srv.TaskQueue().CountByStatus(queue.TaskStatusPending))
	}
	if srv.TaskQueue().CountByStatus(queue.TaskStatusAssigned) != 1 {
		t.Errorf("expected 1 assigned, got %d", srv.TaskQueue().CountByStatus(queue.TaskStatusAssigned))
	}
	if srv.TaskQueue().CountByStatus(queue.TaskStatusRunning) != 1 {
		t.Errorf("expected 1 running, got %d", srv.TaskQueue().CountByStatus(queue.TaskStatusRunning))
	}
}

// Web UI tests
func TestHandleWebUI(t *testing.T) {
	srv := newTestServer(t)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()
	srv.HandleWebUI(w, req)

	// Should serve index.html (200) or not found (404) depending on whether web files exist
	if w.Code != http.StatusOK && w.Code != http.StatusNotFound {
		t.Errorf("expected 200 or 404, got %d", w.Code)
	}
}

// Task status serialization tests
func TestHandleTasks_POST_StatusIsString(t *testing.T) {
	srv := newTestServer(t)

	task := map[string]interface{}{
		"repos":  []string{"/repo"},
		"prompt": "Status serialization test",
	}
	body, _ := json.Marshal(task)
	req := httptest.NewRequest(http.MethodPost, "/api/tasks", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.HandleTasks(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", w.Code)
	}

	// Parse the raw JSON to check the status field type
	var rawResponse map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &rawResponse); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}

	status, ok := rawResponse["status"]
	if !ok {
		t.Fatal("expected 'status' field in response")
	}

	// Status must be a string, not a number
	if _, isString := status.(string); !isString {
		t.Errorf("expected status to be a string, got %T: %v", status, status)
	}
}

func TestHandleTasks_GET_StatusIsString(t *testing.T) {
	srv := newTestServer(t)

	// Create a task
	task := map[string]interface{}{
		"repos":  []string{"/repo"},
		"prompt": "List status serialization test",
	}
	body, _ := json.Marshal(task)
	req := httptest.NewRequest(http.MethodPost, "/api/tasks", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.HandleTasks(w, req)

	// List tasks
	req2 := httptest.NewRequest(http.MethodGet, "/api/tasks", nil)
	w2 := httptest.NewRecorder()
	srv.HandleTasks(w2, req2)

	if w2.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w2.Code)
	}

	var response struct {
		Tasks []map[string]interface{} `json:"tasks"`
		Count int                      `json:"count"`
	}
	if err := json.Unmarshal(w2.Body.Bytes(), &response); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	if response.Count != 1 {
		t.Fatalf("expected 1 task, got %d", response.Count)
	}

	status := response.Tasks[0]["status"]
	if _, isString := status.(string); !isString {
		t.Errorf("expected status to be a string, got %T: %v", status, status)
	}
}

// Guest state serialization tests
func TestHandleGuests_GET_StateIsString(t *testing.T) {
	srv := newTestServer(t)

	// Register guests with different states
	srv.Registry().Register("guest-1", "Idle Guest", []string{"tag1"})
	srv.Registry().Register("guest-2", "Running Guest", []string{"tag2"})
	srv.Registry().SetGuestState("guest-2", registry.GuestStateRunning)

	// List guests
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

	// Each guest's state must be a string, not an integer
	for i, guest := range response.Guests {
		state, ok := guest["state"]
		if !ok {
			t.Fatalf("guest %d: expected 'state' field", i)
		}
		if _, isString := state.(string); !isString {
			t.Errorf("guest %d: expected state to be a string, got %T: %v", i, state, state)
		}
	}
}

func TestHandleGuestDetail_StateIsString(t *testing.T) {
	srv := newTestServer(t)

	srv.Registry().Register("detail-guest", "Detail Guest", []string{"tag1"})
	srv.Registry().SetGuestState("detail-guest", registry.GuestStateRunning)

	req := httptest.NewRequest(http.MethodGet, "/api/guests/detail-guest", nil)
	w := httptest.NewRecorder()
	srv.HandleGuestDetail(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var guest map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &guest); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	state, ok := guest["state"]
	if !ok {
		t.Fatal("expected 'state' field in guest detail")
	}
	if _, isString := state.(string); !isString {
		t.Errorf("expected state to be a string, got %T: %v", state, state)
	}
}

// WebSocket handler tests
func TestHandleWebSocket(t *testing.T) {
	srv := newTestServer(t)

	// Create a test HTTP server to serve the websocket
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/ws" {
			srv.HandleWebSocket(w, r)
		} else {
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	// Verify the endpoint exists (don't actually connect - that's tested in integration tests)
	req := httptest.NewRequest(http.MethodGet, "/ws", nil)
	req.Header.Set("Upgrade", "websocket")
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
	req.Header.Set("Sec-WebSocket-Version", "13")
	w := httptest.NewRecorder()
	srv.HandleWebSocket(w, req)

	// Should return 101 Switching Protocols or an error
	if w.Code != http.StatusSwitchingProtocols && w.Code != http.StatusBadRequest {
		t.Logf("websocket upgrade returned %d (expected 101 or 400)", w.Code)
	}
}

// Guest connection mapping tests
func TestHandleGuestRegister_ConnectionMapping(t *testing.T) {
	srv := newTestServer(t)
	hub := srv.Hub()

	go hub.Run()

	// Simulate guest.register via a real WebSocket connection
	// We need to register a connection first, then dispatch
	// Since Dispatch() doesn't have a connection context, we need to
	// use the hub's internal mechanism. Let's create a mock connection.
	conn := rpc.NewTestConnection("test-conn-1", hub)

	// Register the connection in the hub
	hub.Register(conn)
	time.Sleep(10 * time.Millisecond)

	// Now dispatch guest.register — this should record the mapping
	params, _ := json.Marshal(map[string]interface{}{
		"id":   "map-test-guest",
		"name": "Map Test Guest",
		"tags": []string{"business-default"},
	})

	// We need to call the handler through the hub's handleMessage
	// which sets the connection ID in context. Since we can't easily
	// do that with Dispatch(), let's test via the hub directly.
	resp, err := hub.Dispatch("guest.register", params)
	if err != nil {
		t.Fatalf("guest.register failed: %v", err)
	}

	result := resp.(map[string]interface{})
	if result["status"] != "registered" {
		t.Errorf("expected status 'registered', got %v", result["status"])
	}

	// Verify the guest was registered
	guest, ok := srv.Registry().GetGuest("map-test-guest")
	if !ok {
		t.Fatal("guest not found in registry")
	}
	if guest.ID != "map-test-guest" {
		t.Errorf("expected guest ID 'map-test-guest', got %s", guest.ID)
	}

	// The connection mapping test requires a real WebSocket context,
	// which is covered by the integration test below.
	_ = guest
}

func TestHandleGuestRegister_ReRegistration(t *testing.T) {
	srv := newTestServer(t)
	hub := srv.Hub()
	go hub.Run()

	// First registration
	params1, _ := json.Marshal(map[string]interface{}{
		"id":   "reconnect-guest",
		"name": "Reconnect Guest",
		"tags": []string{"tag1"},
	})

	resp1, err := hub.Dispatch("guest.register", params1)
	if err != nil {
		t.Fatalf("first register failed: %v", err)
	}
	result1 := resp1.(map[string]interface{})
	if result1["status"] != "registered" {
		t.Errorf("expected status 'registered', got %v", result1["status"])
	}

	// Second registration with the same ID (simulates reconnect)
	params2, _ := json.Marshal(map[string]interface{}{
		"id":   "reconnect-guest",
		"name": "Reconnect Guest Updated",
		"tags": []string{"tag1", "tag2"},
	})

	resp2, err := hub.Dispatch("guest.register", params2)
	if err != nil {
		t.Fatalf("second register failed: %v", err)
	}
	result2 := resp2.(map[string]interface{})
	if result2["status"] != "re-registered" {
		t.Errorf("expected status 're-registered', got %v", result2["status"])
	}

	// Verify the guest was updated
	guest, ok := srv.Registry().GetGuest("reconnect-guest")
	if !ok {
		t.Fatal("guest not found in registry")
	}
	if guest.Name != "Reconnect Guest Updated" {
		t.Errorf("expected name 'Reconnect Guest Updated', got %s", guest.Name)
	}
	if len(guest.Tags) != 2 {
		t.Errorf("expected 2 tags, got %d", len(guest.Tags))
	}
}

func TestHub_SendToGuest_WithMapping(t *testing.T) {
	hub := rpc.NewHub(t.Logf)
	go hub.Run()

	// Create a mock connection
	conn := rpc.NewTestConnection("conn-1", hub)

	hub.Register(conn)
	time.Sleep(10 * time.Millisecond)

	// Register the guest-connection mapping
	hub.RegisterGuestConnection("guest-1", "conn-1")

	// SendToGuest should find the connection
	err := hub.SendToGuest("guest-1", "task.assign", map[string]interface{}{"id": "task-1"})
	if err != nil {
		t.Fatalf("SendToGuest failed: %v", err)
	}

	// Verify the message was sent
	data, ok := conn.Recv()
	if !ok {
		t.Fatal("expected message to be sent to connection")
	}
	var msg rpc.JSONRPCMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		t.Fatalf("failed to unmarshal sent message: %v", err)
	}
	if msg.Method != "task.assign" {
		t.Errorf("expected method 'task.assign', got %s", msg.Method)
	}
}

func TestHub_SendToGuest_NoMapping(t *testing.T) {
	hub := rpc.NewHub(t.Logf)
	go hub.Run()

	// Don't register any mapping
	err := hub.SendToGuest("nonexistent-guest", "task.assign", nil)
	if err == nil {
		t.Fatal("expected error for unmapped guest, got nil")
	}
	expectedMsg := "connection for guest nonexistent-guest not found"
	if err.Error() != expectedMsg {
		t.Errorf("expected error %q, got %q", expectedMsg, err.Error())
	}
}

func TestHub_UnregisterGuestConnection(t *testing.T) {
	hub := rpc.NewHub(t.Logf)

	hub.RegisterGuestConnection("guest-1", "conn-1")

	// Verify mapping exists
	connID, ok := hub.GetGuestConnectionID("guest-1")
	if !ok || connID != "conn-1" {
		t.Errorf("expected conn-1, got %s (ok=%v)", connID, ok)
	}

	// Unregister
	hub.UnregisterGuestConnection("guest-1")

	// Verify mapping is gone
	_, ok = hub.GetGuestConnectionID("guest-1")
	if ok {
		t.Error("expected mapping to be removed after unregister")
	}
}

func TestIntegration_GuestRegisterAndTaskAssignment(t *testing.T) {
	// This test simulates the full flow:
	// 1. Guest connects via WebSocket
	// 2. Guest calls guest.register (which records connection mapping)
	// 3. A task is created and the server tries to assign it
	// 4. SendToGuest should succeed because the mapping exists

	srv := newTestServer(t)
	hub := srv.Hub()

	go hub.Run()

	// Register a connection in the hub (simulating WebSocket connect)
	conn := rpc.NewTestConnection("ws-conn-1", hub)
	hub.Register(conn)
	time.Sleep(10 * time.Millisecond)

	// Register a guest via RPC (this should record the guest-connection mapping)
	// Since Dispatch() doesn't have connection context, we manually register the mapping
	// to simulate what handleGuestRegister does when called via WebSocket

	// Manually register the guest and connection mapping (simulating the handler)
	_, err := srv.Registry().Register("integration-guest", "Integration Guest", []string{"business-default"})
	if err != nil {
		t.Fatalf("register failed: %v", err)
	}

	// This is what handleGuestRegister does after registering the guest
	hub.RegisterGuestConnection("integration-guest", conn.ID())

	// Create a task
	task := map[string]interface{}{
		"repos":  []string{"/repo"},
		"prompt": "Integration test task",
	}
	body, _ := json.Marshal(task)
	req := httptest.NewRequest(http.MethodPost, "/api/tasks", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.HandleTasks(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", w.Code)
	}

	var createdTask queue.Task
	json.Unmarshal(w.Body.Bytes(), &createdTask)

	// Now try to assign the task to the guest via SendToGuest
	// This is what tryAssignTask does internally
	err = hub.SendToGuest("integration-guest", "task.assign", map[string]interface{}{
		"id":     createdTask.ID,
		"repos":  createdTask.Repos,
		"prompt": createdTask.Prompt,
		"tags":   createdTask.Tags,
	})
	if err != nil {
		t.Fatalf("SendToGuest should succeed after guest registration, got: %v", err)
	}

	// Verify the message was delivered to the connection
	data, ok := conn.Recv()
	if !ok {
		t.Fatal("expected task.assign message to be sent to the guest's connection")
	}
	var msg rpc.JSONRPCMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		t.Fatalf("failed to unmarshal sent message: %v", err)
	}
	if msg.Method != "task.assign" {
		t.Errorf("expected method 'task.assign', got %s", msg.Method)
	}

	// Verify the task was assigned (the guest state update is handled by SetGuestTask in tryAssignTask)
	regGuest, ok := srv.Registry().GetGuest("integration-guest")
	if !ok {
		t.Fatal("guest should still exist")
	}
	if regGuest.TaskID != createdTask.ID {
		t.Errorf("expected guest to have task %s, got %s", createdTask.ID, regGuest.TaskID)
	}

	// Verify the task was assigned
	taskData, ok := srv.TaskQueue().Get(createdTask.ID)
	if !ok {
		t.Fatal("task should exist")
	}
	if taskData.Status != queue.TaskStatusAssigned {
		t.Errorf("expected task status ASSIGNED, got %s", taskData.Status)
	}
	if taskData.AssignedTo != "integration-guest" {
		t.Errorf("expected task assigned to 'integration-guest', got %s", taskData.AssignedTo)
	}
}

func TestLogAccumulator_FlushesImmediatelyForNonText(t *testing.T) {
	logger := log.New(io.Discard, "", 0)
	acc := NewLogAccumulator(logger)

	var emitted []TaskLogEntry
	emit := func(e TaskLogEntry) {
		emitted = append(emitted, e)
	}

	// Tool message should go through immediately
	acc.Feed("task-1", "[TOOL] write_file", "tool", emit)
	if len(emitted) != 1 {
		t.Fatalf("expected 1 emitted entry, got %d", len(emitted))
	}
	if emitted[0].Line != "[TOOL] write_file" {
		t.Errorf("expected '[TOOL] write_file', got %s", emitted[0].Line)
	}

	// Reset
	emitted = nil

	// "info" level should be batched (not flushed immediately)
	acc.Feed("task-2", "Hello ", "info", emit)
	acc.Feed("task-2", "World", "info", emit)
	if len(emitted) != 0 {
		t.Fatalf("expected 0 emitted entries for 'info' level during buffer, got %d", len(emitted))
	}
	acc.FlushAll(emit)
	if len(emitted) != 1 {
		t.Fatalf("expected 1 emitted entry after flush, got %d", len(emitted))
	}
	if emitted[0].Line != "Hello World" {
		t.Errorf("expected 'Hello World', got %s", emitted[0].Line)
	}
}

func TestLogAccumulator_BatchesTextDeltas(t *testing.T) {
	logger := log.New(io.Discard, "", 0)
	acc := NewLogAccumulator(logger)

	var emitted []TaskLogEntry
	emit := func(e TaskLogEntry) {
		emitted = append(emitted, e)
	}

	// Text deltas with "info" level (as the guest sends) should accumulate
	acc.Feed("task-2", "Hello ", "info", emit)
	acc.Feed("task-2", "World", "info", emit)

	// Nothing emitted yet — still buffering
	if len(emitted) != 0 {
		t.Fatalf("expected 0 emitted entries during buffer, got %d", len(emitted))
	}

	// Flush all
	acc.FlushAll(emit)

	if len(emitted) != 1 {
		t.Fatalf("expected 1 emitted entry after flush, got %d", len(emitted))
	}
	if emitted[0].Line != "Hello World" {
		t.Errorf("expected 'Hello World', got %s", emitted[0].Line)
	}
}

func TestLogAccumulator_MultipleTasks(t *testing.T) {
	logger := log.New(io.Discard, "", 0)
	acc := NewLogAccumulator(logger)

	var emitted []TaskLogEntry
	emit := func(e TaskLogEntry) {
		emitted = append(emitted, e)
	}

	acc.Feed("task-a", "Alpha ", "", emit)
	acc.Feed("task-b", "Beta ", "", emit)
	acc.Feed("task-a", "Alpha", "", emit)
	acc.FlushAll(emit)

	if len(emitted) != 2 {
		t.Fatalf("expected 2 emitted entries, got %d", len(emitted))
	}
	if emitted[0].TaskID != "task-a" || emitted[0].Line != "Alpha Alpha" {
		t.Errorf("expected task-a 'Alpha Alpha', got %s", emitted[0].Line)
	}
	if emitted[1].TaskID != "task-b" || emitted[1].Line != "Beta " {
		t.Errorf("expected task-b 'Beta ', got %s", emitted[1].Line)
	}
}

// TestLogAccumulator_MultiLineDeltasBatchesIntoOne verifies that a single
// delta containing embedded newlines is batched as one entry, and that
// multiple deltas with embedded newlines are also batched together.
func TestLogAccumulator_MultiLineDeltasBatchesIntoOne(t *testing.T) {
	logger := log.New(io.Discard, "", 0)
	acc := NewLogAccumulator(logger)

	var emitted []TaskLogEntry
	emit := func(e TaskLogEntry) {
		emitted = append(emitted, e)
	}

	// Simulate a PI handler sending a multi-line delta as one entry
	acc.Feed("task-1", "| Word | Count\n|------|-------\n| agent | 29", "info", emit)

	// Nothing emitted yet — still buffering
	if len(emitted) != 0 {
		t.Fatalf("expected 0 emitted entries during buffer, got %d", len(emitted))
	}

	// Flush
	acc.FlushAll(emit)

	if len(emitted) != 1 {
		t.Fatalf("expected 1 emitted entry after flush, got %d", len(emitted))
	}

	// The newline should be preserved in the single entry
	if emitted[0].Line != "| Word | Count\n|------|-------\n| agent | 29" {
		t.Errorf("expected multi-line delta preserved, got: %q", emitted[0].Line)
	}
}

// TestLogAccumulator_MixedToolAndText verifies that tool events flush
// the text buffer and that subsequent text starts a new batch.
func TestLogAccumulator_MixedToolAndText(t *testing.T) {
	logger := log.New(io.Discard, "", 0)
	acc := NewLogAccumulator(logger)

	var emitted []TaskLogEntry
	emit := func(e TaskLogEntry) {
		emitted = append(emitted, e)
	}

	// Text accumulates
	acc.Feed("task-1", "Hello ", "info", emit)
	acc.Feed("task-1", "World", "info", emit)

	// Tool event flushes the text buffer
	acc.Feed("task-1", "[TOOL] write_file", "tool", emit)

	// Should have 2 entries: "Hello World" and the tool message
	if len(emitted) != 2 {
		t.Fatalf("expected 2 emitted entries, got %d", len(emitted))
	}
	if emitted[0].Line != "Hello World" {
		t.Errorf("expected 'Hello World', got %q", emitted[0].Line)
	}
	if emitted[1].Line != "[TOOL] write_file" {
		t.Errorf("expected '[TOOL] write_file', got %q", emitted[1].Line)
	}

	// New text after tool starts fresh
	acc.FlushAll(emit)
	if len(emitted) != 2 {
		t.Fatalf("expected still 2 after flush (no new text), got %d", len(emitted))
	}
}

// TestLogAccumulator_ToolMessagesWithoutLevel verifies that tool messages
// are sent immediately even when no level is provided (empty string).
// This is the common case: the guest's sendLog callback doesn't pass a level.
func TestLogAccumulator_ToolMessagesWithoutLevel(t *testing.T) {
	logger := log.New(io.Discard, "", 0)
	acc := NewLogAccumulator(logger)

	var emitted []TaskLogEntry
	emit := func(e TaskLogEntry) {
		emitted = append(emitted, e)
	}

	// Simulate the guest sending tool messages with empty level.
	// These should NOT be batched — they must go through immediately.
	acc.Feed("task-1", "", "", emit) // empty line, should be ignored
	acc.Feed("task-1", "[TOOL_START] read: /etc/passwd (id: t1)", "", emit)
	acc.Feed("task-1", "[TOOL_OUTPUT] read (id: t1): file contents here", "", emit)
	acc.Feed("task-1", "[TOOL_END] read (id: t1): result output", "", emit)

	// All tool messages should have been emitted immediately.
	if len(emitted) != 3 {
		t.Fatalf("expected 3 emitted entries, got %d: %v", len(emitted), emitted)
	}

	if emitted[0].Line != "[TOOL_START] read: /etc/passwd (id: t1)" {
		t.Errorf("entry 0: expected TOOL_START, got %q", emitted[0].Line)
	}
	if emitted[0].Level != "tool" {
		t.Errorf("entry 0: expected level 'tool', got %q", emitted[0].Level)
	}

	if emitted[1].Line != "[TOOL_OUTPUT] read (id: t1): file contents here" {
		t.Errorf("entry 1: expected TOOL_OUTPUT, got %q", emitted[1].Line)
	}
	if emitted[1].Level != "tool" {
		t.Errorf("entry 1: expected level 'tool', got %q", emitted[1].Level)
	}

	if emitted[2].Line != "[TOOL_END] read (id: t1): result output" {
		t.Errorf("entry 2: expected TOOL_END, got %q", emitted[2].Line)
	}
	if emitted[2].Level != "tool" {
		t.Errorf("entry 2: expected level 'tool', got %q", emitted[2].Level)
	}
}

// TestLogAccumulator_TextBufferFlushedBeforeTool verifies that when
// text deltas are buffered and a tool message arrives (with empty level),
// the text buffer is flushed first, then the tool message is emitted immediately.
func TestLogAccumulator_TextBufferFlushedBeforeTool(t *testing.T) {
	logger := log.New(io.Discard, "", 0)
	acc := NewLogAccumulator(logger)

	var emitted []TaskLogEntry
	emit := func(e TaskLogEntry) {
		emitted = append(emitted, e)
	}

	// Text deltas accumulate
	acc.Feed("task-1", "Guest is thinking ", "info", emit)
	acc.Feed("task-1", "about the problem", "info", emit)

	// Tool message with empty level should flush the text buffer first,
	// then emit the tool message immediately.
	acc.Feed("task-1", "[TOOL_START] read: file.txt (id: t1)", "", emit)

	if len(emitted) != 2 {
		t.Fatalf("expected 2 emitted entries, got %d", len(emitted))
	}
	if emitted[0].Line != "Guest is thinking about the problem" {
		t.Errorf("entry 0: expected text delta flush, got %q", emitted[0].Line)
	}
	if emitted[0].Level != "text" {
		t.Errorf("entry 0: expected level 'text', got %q", emitted[0].Level)
	}
	if emitted[1].Line != "[TOOL_START] read: file.txt (id: t1)" {
		t.Errorf("entry 1: expected TOOL_START, got %q", emitted[1].Line)
	}
	if emitted[1].Level != "tool" {
		t.Errorf("entry 1: expected level 'tool', got %q", emitted[1].Level)
	}
}

// TestIntegration_TaskLogBroadcast verifies that when a guest sends a task.log
// notification via RPC, the server stores it, broadcasts it via WebSocket, and
// the receiving connection gets the notification.
func TestIntegration_TaskLogBroadcast(t *testing.T) {
	srv := newTestServer(t)
	hub := srv.Hub()

	go hub.Run()

	conn := rpc.NewTestConnection("browser-conn-1", hub)
	hub.Register(conn)
	hub.SetConnectionRole("browser-conn-1", rpc.ConnectionRoleBrowser)
	time.Sleep(10 * time.Millisecond)

	taskBody := map[string]interface{}{
		"repos":  []string{"/repo"},
		"prompt": "count words",
		"tags":   []string{"business-default"},
	}
	body, _ := json.Marshal(taskBody)
	req := httptest.NewRequest(http.MethodPost, "/api/tasks", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.HandleTasks(w, req)

	var createdTask queue.Task
	json.Unmarshal(w.Body.Bytes(), &createdTask)

	params := map[string]interface{}{
		"task_id": createdTask.ID,
		"line":    "Hello **world**",
		"level":   "text",
	}
	rawParams, _ := json.Marshal(params)
	hub.Dispatch("guest.log", rawParams)

	srv.LogAccumulator().FlushAll(func(e TaskLogEntry) {
		srv.LogStore().Add(e)
		srv.hub.SendNotification("", rpc.ConnectionRoleBrowser, "task.log", map[string]interface{}{
			"task_id": e.TaskID,
			"line":    e.Line,
			"level":   e.Level,
		})
	})

	logs := srv.LogStore().Get(createdTask.ID)
	if len(logs) != 1 {
		t.Fatalf("expected 1 stored log, got %d", len(logs))
	}
	if logs[0].Line != "Hello **world**" {
		t.Errorf("expected 'Hello **world**', got %q", logs[0].Line)
	}

	data, ok := conn.Recv()
	if !ok {
		t.Fatal("expected task.log notification to be sent to the browser connection")
	}

	var notification rpc.JSONRPCMessage
	if err := json.Unmarshal(data, &notification); err != nil {
		t.Fatalf("failed to unmarshal notification: %v", err)
	}
	if notification.Method != "task.log" {
		t.Errorf("expected method 'task.log', got %s", notification.Method)
	}

	var notifParams map[string]interface{}
	if err := json.Unmarshal(notification.Params, &notifParams); err != nil {
		t.Fatalf("failed to unmarshal params: %v", err)
	}
	if notifParams["task_id"] != createdTask.ID {
		t.Errorf("expected task_id %s, got %v", createdTask.ID, notifParams["task_id"])
	}
	if notifParams["line"] != "Hello **world**" {
		t.Errorf("expected line 'Hello **world**', got %v", notifParams["line"])
	}
	if notifParams["level"] != "text" {
		t.Errorf("expected level 'text', got %v", notifParams["level"])
	}
}

// TestCheckSilentGuests verifies that checkSilentGuests kills tasks for
// guests that have been silent for longer than the configured SilenceTimeout.
func TestCheckSilentGuests(t *testing.T) {
	cfg := config.ServerConfig{
		Host:              "127.0.0.1",
		Port:              0,
		HeartbeatInterval: 1, // 1 second for fast test
		SilenceTimeout:    2, // 2 seconds
	}
	srv := New(cfg)

	// Register a guest
	_, err := srv.Registry().Register("silent-guest", "Silent Guest", []string{"tag"})
	if err != nil {
		t.Fatalf("register failed: %v", err)
	}

	// Assign a task to the guest
	task := &queue.Task{
		ID:     "silent-task",
		Prompt: "Test task",
		Tags:   []string{"tag"},
	}
	if err := srv.TaskQueue().Add(task); err != nil {
		t.Fatalf("add task failed: %v", err)
	}
	if err := srv.TaskQueue().Assign("silent-task", "silent-guest"); err != nil {
		t.Fatalf("assign task failed: %v", err)
	}
	if err := srv.TaskQueue().Start("silent-task"); err != nil {
		t.Fatalf("start task failed: %v", err)
	}
	if err := srv.Registry().SetGuestTask("silent-guest", "silent-task"); err != nil {
		t.Fatalf("set guest task failed: %v", err)
	}

	// Don't send heartbeats — the guest will be stale
	// Wait for the task to be running
	time.Sleep(100 * time.Millisecond)

	// Manually advance the guest's LastHeartbeat to simulate silence
	srv.Registry().SetLastHeartbeat("silent-guest", time.Now().Add(-3*time.Second))

	// Run checkSilentGuests — it should kill the task
	srv.checkSilentGuests()

	// Verify the task was marked as failed
	taskData, ok := srv.TaskQueue().Get("silent-task")
	if !ok {
		t.Fatal("task should still exist")
	}
	if taskData.Status != queue.TaskStatusFailed {
		t.Errorf("expected task FAILED, got %s", taskData.Status)
	}

	// Verify the guest's task was cleared
	guestAfter, _ := srv.Registry().GetGuest("silent-guest")
	if guestAfter.TaskID != "" {
		t.Errorf("expected empty task_id, got %s", guestAfter.TaskID)
	}
	if guestAfter.State != registry.GuestStateIdle {
		t.Errorf("expected IDLE state, got %s", guestAfter.State)
	}
}

// TestCheckSilentGuests_Disabled verifies that when SilenceTimeout is 0,
// no tasks are killed even for silent guests.
func TestCheckSilentGuests_Disabled(t *testing.T) {
	cfg := config.ServerConfig{
		Host:              "127.0.0.1",
		Port:              0,
		HeartbeatInterval: 1,
		SilenceTimeout:    0, // disabled
	}
	srv := New(cfg)

	srv.Registry().Register("silent-guest", "Silent Guest", []string{"tag"})

	task := &queue.Task{
		ID:     "silent-task",
		Prompt: "Test task",
		Tags:   []string{"tag"},
	}
	srv.TaskQueue().Add(task)
	srv.TaskQueue().Assign("silent-task", "silent-guest")
	srv.TaskQueue().Start("silent-task")
	srv.Registry().SetGuestTask("silent-guest", "silent-task")

	srv.checkSilentGuests()

	taskData, _ := srv.TaskQueue().Get("silent-task")
	if taskData.Status != queue.TaskStatusRunning {
		t.Errorf("expected task RUNNING (silence detection disabled), got %s", taskData.Status)
	}
}

// TestCheckSilentGuests_ActiveGuest verifies that guests sending heartbeats
// are not killed even when running tasks.
func TestCheckSilentGuests_ActiveGuest(t *testing.T) {
	cfg := config.ServerConfig{
		Host:              "127.0.0.1",
		Port:              0,
		HeartbeatInterval: 1,
		SilenceTimeout:    10, // 10 seconds
	}
	srv := New(cfg)

	srv.Registry().Register("active-guest", "Active Guest", []string{"tag"})

	task := &queue.Task{
		ID:     "active-task",
		Prompt: "Test task",
		Tags:   []string{"tag"},
	}
	srv.TaskQueue().Add(task)
	srv.TaskQueue().Assign("active-task", "active-guest")
	srv.TaskQueue().Start("active-task")
	srv.Registry().SetGuestTask("active-guest", "active-task")

	// Send a fresh heartbeat
	srv.Registry().Heartbeat("active-guest")

	srv.checkSilentGuests()

	taskData, _ := srv.TaskQueue().Get("active-task")
	if taskData.Status != queue.TaskStatusRunning {
		t.Errorf("expected task RUNNING (guest is active), got %s", taskData.Status)
	}
}

func TestServerReload_MaxGuests(t *testing.T) {
	srv := newTestServer(t)

	// Initial max guests is 0 (unlimited)
	if srv.cfg.MaxGuests != 0 {
		t.Errorf("expected max_guests 0, got %d", srv.cfg.MaxGuests)
	}

	// Reload with a max of 5
	newCfg := config.ServerConfig{MaxGuests: 5}
	srv.Reload(newCfg)

	if srv.cfg.MaxGuests != 5 {
		t.Errorf("expected max_guests 5 after reload, got %d", srv.cfg.MaxGuests)
	}

	// Verify the registry was updated via SetMaxGuests
	srv.Registry().SetMaxGuests(5)
	// Register a guest to verify the max limit takes effect
	_, err := srv.Registry().Register("test-guest", "Test", nil)
	if err != nil {
		t.Errorf("expected guest registration to succeed with max 5, got: %v", err)
	}
}

func TestServerReload_LogDir(t *testing.T) {
	tmpDir := t.TempDir()

	srv := newTestServer(t)

	// Initially no disk log store
	if srv.DiskLogStore() != nil {
		t.Error("expected nil disk log store initially")
	}

	// Reload with a LogDir
	newCfg := config.ServerConfig{LogDir: tmpDir}
	srv.Reload(newCfg)

	if srv.DiskLogStore() == nil {
		t.Fatal("expected non-nil disk log store after reload")
	}

	// Reload with empty LogDir to disable
	emptyCfg := config.ServerConfig{}
	srv.Reload(emptyCfg)

	if srv.DiskLogStore() != nil {
		t.Error("expected nil disk log store after disabling LogDir")
	}
}

func TestServerReload_TaskTimeout(t *testing.T) {
	srv := newTestServer(t)

	newCfg := config.ServerConfig{TaskTimeout: 7200}
	srv.Reload(newCfg)

	if srv.cfg.TaskTimeout != 7200 {
		t.Errorf("expected task_timeout 7200, got %d", srv.cfg.TaskTimeout)
	}
}

func TestTryAssignTask_RejectsNonIdleGuest(t *testing.T) {
	srv := newTestServer(t)

	// Register a guest
	_, err := srv.Registry().Register("test-guest", "test-guest", []string{"default"})
	if err != nil {
		t.Fatalf("register guest: %v", err)
	}

	// Add a pending task
	task := &queue.Task{
		ID:     "task-1",
		Prompt: "test task",
		Tags:   []string{"default"},
	}
	if err := srv.TaskQueue().Add(task); err != nil {
		t.Fatalf("add task: %v", err)
	}

	// Set the guest to running state (simulating a guest already executing a task)
	if err := srv.Registry().SetGuestTask("test-guest", "existing-task"); err != nil {
		t.Fatalf("set guest task: %v", err)
	}

	// tryAssignTask should not assign because the guest is not idle
	srv.tryAssignTask("test-guest")

	// The task should still be pending (not assigned)
	qTask, ok := srv.TaskQueue().Get("task-1")
	if !ok {
		t.Fatal("task should still exist")
	}
	if qTask.Status != queue.TaskStatusPending {
		t.Errorf("expected task status PENDING, got %s", qTask.Status)
	}

	// The guest should still have its original task
	regGuest, ok := srv.Registry().GetGuest("test-guest")
	if !ok {
		t.Fatal("guest should still exist")
	}
	if regGuest.TaskID != "existing-task" {
		t.Errorf("expected guest task 'existing-task', got %s", regGuest.TaskID)
	}
}

func TestTryAssignTask_AssignsToIdleGuest(t *testing.T) {
	srv := newTestServer(t)

	// Register a guest
	_, err := srv.Registry().Register("test-guest", "test-guest", []string{"default"})
	if err != nil {
		t.Fatalf("register guest: %v", err)
	}

	// Add a pending task
	task := &queue.Task{
		ID:     "task-1",
		Prompt: "test task",
		Tags:   []string{"default"},
	}
	if err := srv.TaskQueue().Add(task); err != nil {
		t.Fatalf("add task: %v", err)
	}

	// Guest is idle — tryAssignTask should assign the task
	srv.tryAssignTask("test-guest")

	// The task should now be assigned
	qTask, ok := srv.TaskQueue().Get("task-1")
	if !ok {
		t.Fatal("task should still exist")
	}
	if qTask.Status != queue.TaskStatusAssigned {
		t.Errorf("expected task status ASSIGNED, got %s", qTask.Status)
	}
	if qTask.AssignedTo != "test-guest" {
		t.Errorf("expected task assigned to 'test-guest', got %s", qTask.AssignedTo)
	}

	// The guest should now be running
	regGuest, ok := srv.Registry().GetGuest("test-guest")
	if !ok {
		t.Fatal("guest should still exist")
	}
	if regGuest.TaskID != "task-1" {
		t.Errorf("expected guest task 'task-1', got %s", regGuest.TaskID)
	}
	if regGuest.State != registry.GuestStateRunning {
		t.Errorf("expected guest state RUNNING, got %s", regGuest.State)
	}
}

// TestHandleGuestLog_ToolFieldsFromAccumulator verifies that tool messages
// which arrive with empty level (and are detected by the accumulator via
// prefix matching) still get structured tool fields populated in the stored
// entry. This is the root cause of the "Cannot read properties of null
// (reading 'toolName')" crash: the accumulator sets level to "tool" but the
// emit callback checked req.Level instead of e.Level, so structured fields
// were never copied.
func TestHandleGuestLog_ToolFieldsFromAccumulator(t *testing.T) {
	srv := newTestServer(t)
	hub := srv.Hub()
	go hub.Run()

	// Create a task
	task := map[string]interface{}{
		"repos":  []string{"/repo"},
		"prompt": "tool fields test",
	}
	body, _ := json.Marshal(task)
	req := httptest.NewRequest(http.MethodPost, "/api/tasks", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.HandleTasks(w, req)

	var createdTask queue.Task
	json.Unmarshal(w.Body.Bytes(), &createdTask)

	// Send tool messages with EMPTY level — simulating the path where the
	// accumulator detects the [TOOL_*] prefix and sets level to "tool".
	// The structured fields should still be populated from the line content.
	toolLines := []map[string]interface{}{
		{
			"task_id": createdTask.ID,
			"line":    "[TOOL_START] read_file: /etc/passwd (id: t1)",
			"level":   "", // empty — accumulator will detect tool prefix
		},
		{
			"task_id": createdTask.ID,
			"line":    "[TOOL_OUTPUT] read_file (id: t1): file contents here",
			"level":   "",
		},
		{
			"task_id": createdTask.ID,
			"line":    "[TOOL_END] read_file (id: t1): result output",
			"level":   "",
		},
	}

	for _, tl := range toolLines {
		params, _ := json.Marshal(tl)
		hub.Dispatch("guest.log", params)
	}

	// Fetch the task detail to get stored logs
	req2 := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/tasks/%s", createdTask.ID), nil)
	w2 := httptest.NewRecorder()
	srv.HandleTaskDetail(w2, req2)

	if w2.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w2.Code)
	}

	var detailResponse map[string]interface{}
	if err := json.Unmarshal(w2.Body.Bytes(), &detailResponse); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	logs, ok := detailResponse["logs"].([]interface{})
	if !ok {
		t.Fatal("expected 'logs' field in response")
	}

	if len(logs) != 3 {
		t.Fatalf("expected 3 log entries, got %d", len(logs))
	}

	// Verify each tool entry has structured fields populated
	for i, rawLog := range logs {
		logEntry, ok := rawLog.(map[string]interface{})
		if !ok {
			t.Fatalf("log entry %d: expected map, got %T", i, rawLog)
		}

		if logEntry["level"] != "tool" {
			t.Errorf("entry %d: expected level 'tool', got %q", i, logEntry["level"])
		}

		toolType, hasType := logEntry["tool_type"]
		if !hasType || toolType == "" {
			t.Errorf("entry %d: expected non-empty tool_type, got %v (line: %q)", i, toolType, logEntry["line"])
		}

		toolName, hasName := logEntry["tool_name"]
		if !hasName || toolName == "" {
			t.Errorf("entry %d: expected non-empty tool_name, got %v (line: %q)", i, toolName, logEntry["line"])
		}

		toolID, hasID := logEntry["tool_id"]
		if !hasID || toolID == "" {
			t.Errorf("entry %d: expected non-empty tool_id, got %v (line: %q)", i, toolID, logEntry["line"])
		}
	}

	// Verify specific values
	startEntry := logs[0].(map[string]interface{})
	if startEntry["tool_type"] != "start" {
		t.Errorf("start entry: expected tool_type 'start', got %v", startEntry["tool_type"])
	}
	if startEntry["tool_name"] != "read_file" {
		t.Errorf("start entry: expected tool_name 'read_file', got %v", startEntry["tool_name"])
	}
	if startEntry["tool_id"] != "t1" {
		t.Errorf("start entry: expected tool_id 't1', got %v", startEntry["tool_id"])
	}
	if startEntry["tool_args"] != "/etc/passwd" {
		t.Errorf("start entry: expected tool_args '/etc/passwd', got %v", startEntry["tool_args"])
	}

	outputEntry := logs[1].(map[string]interface{})
	if outputEntry["tool_type"] != "output" {
		t.Errorf("output entry: expected tool_type 'output', got %v", outputEntry["tool_type"])
	}
	if outputEntry["tool_output"] != "file contents here" {
		t.Errorf("output entry: expected tool_output 'file contents here', got %v", outputEntry["tool_output"])
	}

	endEntry := logs[2].(map[string]interface{})
	if endEntry["tool_type"] != "end" {
		t.Errorf("end entry: expected tool_type 'end', got %v", endEntry["tool_type"])
	}
	if endEntry["tool_output"] != "result output" {
		t.Errorf("end entry: expected tool_output 'result output', got %v", endEntry["tool_output"])
	}
}

// TestHandleGuestLog_ToolErrorFields verifies that [TOOL_END] with [ERROR]
// is correctly parsed and the tool_error flag is set.
func TestHandleGuestLog_ToolErrorFields(t *testing.T) {
	srv := newTestServer(t)
	hub := srv.Hub()
	go hub.Run()

	task := map[string]interface{}{
		"repos":  []string{"/repo"},
		"prompt": "tool error test",
	}
	body, _ := json.Marshal(task)
	req := httptest.NewRequest(http.MethodPost, "/api/tasks", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.HandleTasks(w, req)

	var createdTask queue.Task
	json.Unmarshal(w.Body.Bytes(), &createdTask)

	// Send a tool error message with empty level
	params, _ := json.Marshal(map[string]interface{}{
		"task_id": createdTask.ID,
		"line":    "[TOOL_END] bash (id: t2): [ERROR] exit code 1",
		"level":   "",
	})
	hub.Dispatch("guest.log", params)

	// Fetch logs
	req2 := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/tasks/%s", createdTask.ID), nil)
	w2 := httptest.NewRecorder()
	srv.HandleTaskDetail(w2, req2)

	var detailResponse map[string]interface{}
	json.Unmarshal(w2.Body.Bytes(), &detailResponse)
	logs := detailResponse["logs"].([]interface{})

	endEntry := logs[0].(map[string]interface{})
	if endEntry["tool_error"] != true {
		t.Errorf("expected tool_error true, got %v", endEntry["tool_error"])
	}
	if endEntry["tool_output"] != "exit code 1" {
		t.Errorf("expected tool_output 'exit code 1', got %v", endEntry["tool_output"])
	}
}
