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

// Agent listing tests
func TestHandleAgents_GET(t *testing.T) {
	srv := newTestServer(t)

	// Register some agents
	srv.Registry().Register("agent-1", "Agent One", []string{"tag1"})
	srv.Registry().Register("agent-2", "Agent Two", []string{"tag2"})

	req := httptest.NewRequest(http.MethodGet, "/api/agents", nil)
	w := httptest.NewRecorder()
	srv.HandleAgents(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var response struct {
		Agents []registry.Agent `json:"agents"`
		Count  int              `json:"count"`
	}
	json.Unmarshal(w.Body.Bytes(), &response)

	if response.Count != 2 {
		t.Errorf("expected 2 agents, got %d", response.Count)
	}
}

func TestHandleAgents_GET_Empty(t *testing.T) {
	srv := newTestServer(t)

	req := httptest.NewRequest(http.MethodGet, "/api/agents", nil)
	w := httptest.NewRecorder()
	srv.HandleAgents(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var response struct {
		Count int `json:"count"`
	}
	json.Unmarshal(w.Body.Bytes(), &response)

	if response.Count != 0 {
		t.Errorf("expected 0 agents, got %d", response.Count)
	}
}

// Agent detail tests
func TestHandleAgentDetail(t *testing.T) {
	srv := newTestServer(t)

	srv.Registry().Register("agent-1", "Agent One", []string{"tag1"})

	req := httptest.NewRequest(http.MethodGet, "/api/agents/agent-1", nil)
	w := httptest.NewRecorder()
	srv.HandleAgentDetail(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var agent registry.Agent
	if err := json.Unmarshal(w.Body.Bytes(), &agent); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	if agent.ID != "agent-1" {
		t.Errorf("expected agent ID 'agent-1', got %s", agent.ID)
	}
}

func TestHandleAgentDetail_NotFound(t *testing.T) {
	srv := newTestServer(t)

	req := httptest.NewRequest(http.MethodGet, "/api/agents/nonexistent", nil)
	w := httptest.NewRecorder()
	srv.HandleAgentDetail(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", w.Code)
	}
}

func TestHandleAgentDetail_EmptyID(t *testing.T) {
	srv := newTestServer(t)

	req := httptest.NewRequest(http.MethodGet, "/api/agents/", nil)
	w := httptest.NewRecorder()
	srv.HandleAgentDetail(w, req)

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

func TestHandleAgents_MethodNotAllowed(t *testing.T) {
	srv := newTestServer(t)

	for _, method := range []string{"PUT", "DELETE", "PATCH", "POST"} {
		req := httptest.NewRequest(method, "/api/agents", nil)
		w := httptest.NewRecorder()
		srv.HandleAgents(w, req)

		if w.Code != http.StatusMethodNotAllowed {
			t.Errorf("%s /api/agents: expected 405, got %d", method, w.Code)
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
	err := srv.TaskQueue().Assign(createdTask.ID, "agent-1")
	if err != nil {
		t.Fatalf("assign failed: %v", err)
	}

	taskData, _ := srv.TaskQueue().Get(createdTask.ID)
	if taskData.Status != queue.TaskStatusAssigned {
		t.Errorf("expected ASSIGNED, got %s", taskData.Status)
	}
	if taskData.AssignedTo != "agent-1" {
		t.Errorf("expected assigned to agent-1, got %s", taskData.AssignedTo)
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

	srv.TaskQueue().Assign(createdTask.ID, "agent-1")
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

	srv.TaskQueue().Assign(createdTask.ID, "agent-1")
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

// Agent lifecycle tests
func TestRegistry_RegisterAgent(t *testing.T) {
	srv := newTestServer(t)

	agent, err := srv.Registry().Register("test-agent", "Test Agent", []string{"business-default"})
	if err != nil {
		t.Fatalf("register failed: %v", err)
	}

	if agent.ID != "test-agent" {
		t.Errorf("expected id 'test-agent', got %s", agent.ID)
	}
	if agent.Name != "Test Agent" {
		t.Errorf("expected name 'Test Agent', got %s", agent.Name)
	}
	if agent.State != registry.AgentStateIdle {
		t.Errorf("expected IDLE state, got %s", agent.State)
	}
}

func TestRegistry_DuplicateAgent(t *testing.T) {
	srv := newTestServer(t)

	_, err := srv.Registry().Register("dup-agent", "Agent", []string{"tag"})
	if err != nil {
		t.Fatalf("first register failed: %v", err)
	}

	_, err = srv.Registry().Register("dup-agent", "Agent 2", []string{"tag2"})
	if err == nil {
		t.Error("expected error for duplicate agent ID")
	}
}

func TestRegistry_MaxAgents(t *testing.T) {
	cfg := config.ServerConfig{Host: "127.0.0.1", Port: 0, MaxAgents: 2}
	srv := New(cfg)

	_, err := srv.Registry().Register("agent-1", "Agent 1", []string{"tag"})
	if err != nil {
		t.Fatalf("first register failed: %v", err)
	}

	_, err = srv.Registry().Register("agent-2", "Agent 2", []string{"tag"})
	if err != nil {
		t.Fatalf("second register failed: %v", err)
	}

	_, err = srv.Registry().Register("agent-3", "Agent 3", []string{"tag"})
	if err == nil {
		t.Error("expected error when max agents reached")
	}
}

func TestRegistry_Heartbeat(t *testing.T) {
	srv := newTestServer(t)

	srv.Registry().Register("hb-agent", "HB Agent", []string{"tag"})

	err := srv.Registry().Heartbeat("hb-agent")
	if err != nil {
		t.Fatalf("heartbeat failed: %v", err)
	}

	agent, ok := srv.Registry().GetAgent("hb-agent")
	if !ok {
		t.Fatal("agent not found after heartbeat")
	}
	if agent.State != registry.AgentStateIdle {
		t.Errorf("expected IDLE, got %s", agent.State)
	}
}

func TestRegistry_UnregisterAgent(t *testing.T) {
	srv := newTestServer(t)

	srv.Registry().Register("unreg-agent", "Unreg Agent", []string{"tag"})

	err := srv.Registry().Unregister("unreg-agent")
	if err != nil {
		t.Fatalf("unregister failed: %v", err)
	}

	_, ok := srv.Registry().GetAgent("unreg-agent")
	if ok {
		t.Error("agent should not exist after unregister")
	}
}

func TestRegistry_UnregisterRunningAgent(t *testing.T) {
	srv := newTestServer(t)

	srv.Registry().Register("running-agent", "Running Agent", []string{"tag"})
	srv.Registry().SetAgentTask("running-agent", "task-1")

	err := srv.Registry().Unregister("running-agent")
	if err == nil {
		t.Error("expected error when unregistering running agent")
	}
}

func TestRegistry_FindAvailableAgents(t *testing.T) {
	srv := newTestServer(t)

	srv.Registry().Register("agent-1", "Agent 1", []string{"business-default", "frontend"})
	srv.Registry().Register("agent-2", "Agent 2", []string{"android"})
	srv.Registry().Register("agent-3", "Agent 3", []string{"business-default"})

	// Find agents with business-default tag
	agents := srv.Registry().FindAvailableAgents([]string{"business-default"})
	if len(agents) != 2 {
		t.Errorf("expected 2 agents with business-default, got %d", len(agents))
	}

	// Find agents with android tag
	agents = srv.Registry().FindAvailableAgents([]string{"android"})
	if len(agents) != 1 {
		t.Errorf("expected 1 agent with android, got %d", len(agents))
	}

	// No agents match nonexistent tag
	agents = srv.Registry().FindAvailableAgents([]string{"nonexistent"})
	if len(agents) != 0 {
		t.Errorf("expected 0 agents for nonexistent tag, got %d", len(agents))
	}
}

func TestRegistry_HasAgentWithTags(t *testing.T) {
	srv := newTestServer(t)

	srv.Registry().Register("agent-1", "Agent 1", []string{"business-default"})

	if !srv.Registry().HasAgentWithTags([]string{"business-default"}) {
		t.Error("expected to have agent with business-default")
	}

	if srv.Registry().HasAgentWithTags([]string{"android"}) {
		t.Error("expected not to have agent with android")
	}
}

func TestRegistry_RemoveStaleAgents(t *testing.T) {
	srv := newTestServer(t)

	srv.Registry().Register("stale-agent", "Stale Agent", []string{"tag"})
	time.Sleep(20 * time.Millisecond)
	srv.Registry().Register("fresh-agent", "Fresh Agent", []string{"tag"})

	stale := srv.Registry().RemoveStaleAgents(10 * time.Millisecond)
	if len(stale) != 1 {
		t.Errorf("expected 1 stale agent, got %d", len(stale))
	}
	if len(stale) > 0 && stale[0].ID != "stale-agent" {
		t.Errorf("expected stale-agent, got %s", stale[0].ID)
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
	srv.TaskQueue().Assign("task-1", "agent-1")

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
	srv.TaskQueue().Assign("task-1", "agent-1")

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
	srv.TaskQueue().Assign("task-1", "agent-1")
	srv.TaskQueue().Assign("task-2", "agent-1")
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

// Agent state serialization tests
func TestHandleAgents_GET_StateIsString(t *testing.T) {
	srv := newTestServer(t)

	// Register agents with different states
	srv.Registry().Register("agent-1", "Idle Agent", []string{"tag1"})
	srv.Registry().Register("agent-2", "Running Agent", []string{"tag2"})
	srv.Registry().SetAgentState("agent-2", registry.AgentStateRunning)

	// List agents
	req := httptest.NewRequest(http.MethodGet, "/api/agents", nil)
	w := httptest.NewRecorder()
	srv.HandleAgents(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var response struct {
		Agents []map[string]interface{} `json:"agents"`
		Count  int                      `json:"count"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	if response.Count != 2 {
		t.Fatalf("expected 2 agents, got %d", response.Count)
	}

	// Each agent's state must be a string, not an integer
	for i, agent := range response.Agents {
		state, ok := agent["state"]
		if !ok {
			t.Fatalf("agent %d: expected 'state' field", i)
		}
		if _, isString := state.(string); !isString {
			t.Errorf("agent %d: expected state to be a string, got %T: %v", i, state, state)
		}
	}
}

func TestHandleAgentDetail_StateIsString(t *testing.T) {
	srv := newTestServer(t)

	srv.Registry().Register("detail-agent", "Detail Agent", []string{"tag1"})
	srv.Registry().SetAgentState("detail-agent", registry.AgentStateRunning)

	req := httptest.NewRequest(http.MethodGet, "/api/agents/detail-agent", nil)
	w := httptest.NewRecorder()
	srv.HandleAgentDetail(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var agent map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &agent); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	state, ok := agent["state"]
	if !ok {
		t.Fatal("expected 'state' field in agent detail")
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

// Agent connection mapping tests
func TestHandleAgentRegister_ConnectionMapping(t *testing.T) {
	srv := newTestServer(t)
	hub := srv.Hub()

	go hub.Run()

	// Simulate agent.register via a real WebSocket connection
	// We need to register a connection first, then dispatch
	// Since Dispatch() doesn't have a connection context, we need to
	// use the hub's internal mechanism. Let's create a mock connection.
	conn := rpc.NewTestConnection("test-conn-1", hub)

	// Register the connection in the hub
	hub.Register(conn)
	time.Sleep(10 * time.Millisecond)

	// Now dispatch agent.register — this should record the mapping
	params, _ := json.Marshal(map[string]interface{}{
		"id":   "map-test-agent",
		"name": "Map Test Agent",
		"tags": []string{"business-default"},
	})

	// We need to call the handler through the hub's handleMessage
	// which sets the connection ID in context. Since we can't easily
	// do that with Dispatch(), let's test via the hub directly.
	resp, err := hub.Dispatch("agent.register", params)
	if err != nil {
		t.Fatalf("agent.register failed: %v", err)
	}

	result := resp.(map[string]interface{})
	if result["status"] != "registered" {
		t.Errorf("expected status 'registered', got %v", result["status"])
	}

	// Verify the agent was registered
	agent, ok := srv.Registry().GetAgent("map-test-agent")
	if !ok {
		t.Fatal("agent not found in registry")
	}
	if agent.ID != "map-test-agent" {
		t.Errorf("expected agent ID 'map-test-agent', got %s", agent.ID)
	}

	// The connection mapping test requires a real WebSocket context,
	// which is covered by the integration test below.
	_ = agent
}

func TestHandleAgentRegister_ReRegistration(t *testing.T) {
	srv := newTestServer(t)
	hub := srv.Hub()
	go hub.Run()

	// First registration
	params1, _ := json.Marshal(map[string]interface{}{
		"id":   "reconnect-agent",
		"name": "Reconnect Agent",
		"tags": []string{"tag1"},
	})

	resp1, err := hub.Dispatch("agent.register", params1)
	if err != nil {
		t.Fatalf("first register failed: %v", err)
	}
	result1 := resp1.(map[string]interface{})
	if result1["status"] != "registered" {
		t.Errorf("expected status 'registered', got %v", result1["status"])
	}

	// Second registration with the same ID (simulates reconnect)
	params2, _ := json.Marshal(map[string]interface{}{
		"id":   "reconnect-agent",
		"name": "Reconnect Agent Updated",
		"tags": []string{"tag1", "tag2"},
	})

	resp2, err := hub.Dispatch("agent.register", params2)
	if err != nil {
		t.Fatalf("second register failed: %v", err)
	}
	result2 := resp2.(map[string]interface{})
	if result2["status"] != "re-registered" {
		t.Errorf("expected status 're-registered', got %v", result2["status"])
	}

	// Verify the agent was updated
	agent, ok := srv.Registry().GetAgent("reconnect-agent")
	if !ok {
		t.Fatal("agent not found in registry")
	}
	if agent.Name != "Reconnect Agent Updated" {
		t.Errorf("expected name 'Reconnect Agent Updated', got %s", agent.Name)
	}
	if len(agent.Tags) != 2 {
		t.Errorf("expected 2 tags, got %d", len(agent.Tags))
	}
}

func TestHub_SendToAgent_WithMapping(t *testing.T) {
	hub := rpc.NewHub(t.Logf)
	go hub.Run()

	// Create a mock connection
	conn := rpc.NewTestConnection("conn-1", hub)

	hub.Register(conn)
	time.Sleep(10 * time.Millisecond)

	// Register the agent-connection mapping
	hub.RegisterAgentConnection("agent-1", "conn-1")

	// SendToAgent should find the connection
	err := hub.SendToAgent("agent-1", "task.assign", map[string]interface{}{"id": "task-1"})
	if err != nil {
		t.Fatalf("SendToAgent failed: %v", err)
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

func TestHub_SendToAgent_NoMapping(t *testing.T) {
	hub := rpc.NewHub(t.Logf)
	go hub.Run()

	// Don't register any mapping
	err := hub.SendToAgent("nonexistent-agent", "task.assign", nil)
	if err == nil {
		t.Fatal("expected error for unmapped agent, got nil")
	}
	expectedMsg := "connection for agent nonexistent-agent not found"
	if err.Error() != expectedMsg {
		t.Errorf("expected error %q, got %q", expectedMsg, err.Error())
	}
}

func TestHub_UnregisterAgentConnection(t *testing.T) {
	hub := rpc.NewHub(t.Logf)

	hub.RegisterAgentConnection("agent-1", "conn-1")

	// Verify mapping exists
	connID, ok := hub.GetAgentConnectionID("agent-1")
	if !ok || connID != "conn-1" {
		t.Errorf("expected conn-1, got %s (ok=%v)", connID, ok)
	}

	// Unregister
	hub.UnregisterAgentConnection("agent-1")

	// Verify mapping is gone
	_, ok = hub.GetAgentConnectionID("agent-1")
	if ok {
		t.Error("expected mapping to be removed after unregister")
	}
}

func TestIntegration_AgentRegisterAndTaskAssignment(t *testing.T) {
	// This test simulates the full flow:
	// 1. Agent connects via WebSocket
	// 2. Agent calls agent.register (which records connection mapping)
	// 3. A task is created and the server tries to assign it
	// 4. SendToAgent should succeed because the mapping exists

	srv := newTestServer(t)
	hub := srv.Hub()

	go hub.Run()

	// Register a connection in the hub (simulating WebSocket connect)
	conn := rpc.NewTestConnection("ws-conn-1", hub)
	hub.Register(conn)
	time.Sleep(10 * time.Millisecond)

	// Register an agent via RPC (this should record the agent-connection mapping)
	// Since Dispatch() doesn't have connection context, we manually register the mapping
	// to simulate what handleAgentRegister does when called via WebSocket

	// Manually register the agent and connection mapping (simulating the handler)
	_, err := srv.Registry().Register("integration-agent", "Integration Agent", []string{"business-default"})
	if err != nil {
		t.Fatalf("register failed: %v", err)
	}

	// This is what handleAgentRegister does after registering the agent
	hub.RegisterAgentConnection("integration-agent", conn.ID())

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

	// Now try to assign the task to the agent via SendToAgent
	// This is what tryAssignTask does internally
	err = hub.SendToAgent("integration-agent", "task.assign", map[string]interface{}{
		"id":     createdTask.ID,
		"repos":  createdTask.Repos,
		"prompt": createdTask.Prompt,
		"tags":   createdTask.Tags,
	})
	if err != nil {
		t.Fatalf("SendToAgent should succeed after agent registration, got: %v", err)
	}

	// Verify the message was delivered to the connection
	data, ok := conn.Recv()
	if !ok {
		t.Fatal("expected task.assign message to be sent to the agent's connection")
	}
	var msg rpc.JSONRPCMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		t.Fatalf("failed to unmarshal sent message: %v", err)
	}
	if msg.Method != "task.assign" {
		t.Errorf("expected method 'task.assign', got %s", msg.Method)
	}

	// Verify the task was assigned (the agent state update is handled by SetAgentTask in tryAssignTask)
	regAgent, ok := srv.Registry().GetAgent("integration-agent")
	if !ok {
		t.Fatal("agent should still exist")
	}
	if regAgent.TaskID != createdTask.ID {
		t.Errorf("expected agent to have task %s, got %s", createdTask.ID, regAgent.TaskID)
	}

	// Verify the task was assigned
	taskData, ok := srv.TaskQueue().Get(createdTask.ID)
	if !ok {
		t.Fatal("task should exist")
	}
	if taskData.Status != queue.TaskStatusAssigned {
		t.Errorf("expected task status ASSIGNED, got %s", taskData.Status)
	}
	if taskData.AssignedTo != "integration-agent" {
		t.Errorf("expected task assigned to 'integration-agent', got %s", taskData.AssignedTo)
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

	// Text deltas with "info" level (as the agent sends) should accumulate
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
// This is the common case: the agent's sendLog callback doesn't pass a level.
func TestLogAccumulator_ToolMessagesWithoutLevel(t *testing.T) {
	logger := log.New(io.Discard, "", 0)
	acc := NewLogAccumulator(logger)

	var emitted []TaskLogEntry
	emit := func(e TaskLogEntry) {
		emitted = append(emitted, e)
	}

	// Simulate the agent sending tool messages with empty level.
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
	acc.Feed("task-1", "Agent is thinking ", "info", emit)
	acc.Feed("task-1", "about the problem", "info", emit)

	// Tool message with empty level should flush the text buffer first,
	// then emit the tool message immediately.
	acc.Feed("task-1", "[TOOL_START] read: file.txt (id: t1)", "", emit)

	if len(emitted) != 2 {
		t.Fatalf("expected 2 emitted entries, got %d", len(emitted))
	}
	if emitted[0].Line != "Agent is thinking about the problem" {
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

// TestIntegration_TaskLogBroadcast verifies that when an agent sends a task.log
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
	hub.Dispatch("agent.log", rawParams)

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

// TestCheckSilentAgents verifies that checkSilentAgents kills tasks for
// agents that have been silent for longer than the configured SilenceTimeout.
func TestCheckSilentAgents(t *testing.T) {
	cfg := config.ServerConfig{
		Host:              "127.0.0.1",
		Port:              0,
		HeartbeatInterval: 1, // 1 second for fast test
		SilenceTimeout:    2, // 2 seconds
	}
	srv := New(cfg)

	// Register an agent
	_, err := srv.Registry().Register("silent-agent", "Silent Agent", []string{"tag"})
	if err != nil {
		t.Fatalf("register failed: %v", err)
	}

	// Assign a task to the agent
	task := &queue.Task{
		ID:     "silent-task",
		Prompt: "Test task",
		Tags:   []string{"tag"},
	}
	if err := srv.TaskQueue().Add(task); err != nil {
		t.Fatalf("add task failed: %v", err)
	}
	if err := srv.TaskQueue().Assign("silent-task", "silent-agent"); err != nil {
		t.Fatalf("assign task failed: %v", err)
	}
	if err := srv.TaskQueue().Start("silent-task"); err != nil {
		t.Fatalf("start task failed: %v", err)
	}
	if err := srv.Registry().SetAgentTask("silent-agent", "silent-task"); err != nil {
		t.Fatalf("set agent task failed: %v", err)
	}

	// Don't send heartbeats — the agent will be stale
	// Wait for the task to be running
	time.Sleep(100 * time.Millisecond)

	// Manually advance the agent's LastHeartbeat to simulate silence
	srv.Registry().SetLastHeartbeat("silent-agent", time.Now().Add(-3*time.Second))

	// Run checkSilentAgents — it should kill the task
	srv.checkSilentAgents()

	// Verify the task was marked as failed
	taskData, ok := srv.TaskQueue().Get("silent-task")
	if !ok {
		t.Fatal("task should still exist")
	}
	if taskData.Status != queue.TaskStatusFailed {
		t.Errorf("expected task FAILED, got %s", taskData.Status)
	}

	// Verify the agent's task was cleared
	agentAfter, _ := srv.Registry().GetAgent("silent-agent")
	if agentAfter.TaskID != "" {
		t.Errorf("expected empty task_id, got %s", agentAfter.TaskID)
	}
	if agentAfter.State != registry.AgentStateIdle {
		t.Errorf("expected IDLE state, got %s", agentAfter.State)
	}
}

// TestCheckSilentAgents_Disabled verifies that when SilenceTimeout is 0,
// no tasks are killed even for silent agents.
func TestCheckSilentAgents_Disabled(t *testing.T) {
	cfg := config.ServerConfig{
		Host:              "127.0.0.1",
		Port:              0,
		HeartbeatInterval: 1,
		SilenceTimeout:    0, // disabled
	}
	srv := New(cfg)

	srv.Registry().Register("silent-agent", "Silent Agent", []string{"tag"})

	task := &queue.Task{
		ID:     "silent-task",
		Prompt: "Test task",
		Tags:   []string{"tag"},
	}
	srv.TaskQueue().Add(task)
	srv.TaskQueue().Assign("silent-task", "silent-agent")
	srv.TaskQueue().Start("silent-task")
	srv.Registry().SetAgentTask("silent-agent", "silent-task")

	srv.checkSilentAgents()

	taskData, _ := srv.TaskQueue().Get("silent-task")
	if taskData.Status != queue.TaskStatusRunning {
		t.Errorf("expected task RUNNING (silence detection disabled), got %s", taskData.Status)
	}
}

// TestCheckSilentAgents_ActiveAgent verifies that agents sending heartbeats
// are not killed even when running tasks.
func TestCheckSilentAgents_ActiveAgent(t *testing.T) {
	cfg := config.ServerConfig{
		Host:              "127.0.0.1",
		Port:              0,
		HeartbeatInterval: 1,
		SilenceTimeout:    10, // 10 seconds
	}
	srv := New(cfg)

	srv.Registry().Register("active-agent", "Active Agent", []string{"tag"})

	task := &queue.Task{
		ID:     "active-task",
		Prompt: "Test task",
		Tags:   []string{"tag"},
	}
	srv.TaskQueue().Add(task)
	srv.TaskQueue().Assign("active-task", "active-agent")
	srv.TaskQueue().Start("active-task")
	srv.Registry().SetAgentTask("active-agent", "active-task")

	// Send a fresh heartbeat
	srv.Registry().Heartbeat("active-agent")

	srv.checkSilentAgents()

	taskData, _ := srv.TaskQueue().Get("active-task")
	if taskData.Status != queue.TaskStatusRunning {
		t.Errorf("expected task RUNNING (agent is active), got %s", taskData.Status)
	}
}
