package test

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
		MaxAgents: 0,
	}
	return server.New(cfg)
}

func TestFullLifecycle_RegisterAgent(t *testing.T) {
	srv := newTestServer(t)
	hub := srv.Hub()

	go hub.Run()

	// Simulate agent.register RPC
	params, _ := json.Marshal(map[string]interface{}{
		"id":   "test-agent-1",
		"name": "Test Agent",
		"tags": []string{"business-default", "frontend"},
	})

	resp, err := hub.Dispatch("agent.register", params)
	if err != nil {
		t.Fatalf("agent.register failed: %v", err)
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
	err := srv.TaskQueue().Assign(createdTask.ID, "agent-1")
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
	if taskData.AssignedTo != "agent-1" {
		t.Errorf("expected assigned to agent-1, got %s", taskData.AssignedTo)
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

	srv.TaskQueue().Assign(createdTask.ID, "agent-1")
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

func TestFullLifecycle_AgentRegistrationViaREST(t *testing.T) {
	srv := newTestServer(t)

	// Register an agent
	agent, err := srv.Registry().Register("test-agent-1", "Test Agent", []string{"business-default"})
	if err != nil {
		t.Fatalf("register failed: %v", err)
	}

	if agent.ID != "test-agent-1" {
		t.Errorf("expected id test-agent-1, got %s", agent.ID)
	}
	if agent.State != registry.AgentStateIdle {
		t.Errorf("expected IDLE state, got %s", agent.State)
	}

	// List agents via REST
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

	if response.Count != 1 {
		t.Errorf("expected 1 agent, got %d", response.Count)
	}
}

func TestFullLifecycle_TagBasedScheduling(t *testing.T) {
	srv := newTestServer(t)

	// Register two agents with different tags
	srv.Registry().Register("android-agent", "Android Agent", []string{"android"})
	srv.Registry().Register("frontend-agent", "Frontend Agent", []string{"frontend"})

	// Verify tag-based agent lookup works
	agents := srv.Registry().FindAvailableAgents([]string{"android"})
	if len(agents) != 1 {
		t.Errorf("expected 1 android agent, got %d", len(agents))
	}
	if len(agents) > 0 && agents[0].ID != "android-agent" {
		t.Errorf("expected android-agent, got %s", agents[0].ID)
	}

	agents = srv.Registry().FindAvailableAgents([]string{"frontend"})
	if len(agents) != 1 {
		t.Errorf("expected 1 frontend agent, got %d", len(agents))
	}
	if len(agents) > 0 && agents[0].ID != "frontend-agent" {
		t.Errorf("expected frontend-agent, got %s", agents[0].ID)
	}

	// No agents match nonexistent tag
	agents = srv.Registry().FindAvailableAgents([]string{"nonexistent"})
	if len(agents) != 0 {
		t.Errorf("expected 0 agents for nonexistent tag, got %d", len(agents))
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

func TestFullLifecycle_AgentNotFound(t *testing.T) {
	srv := newTestServer(t)

	req := httptest.NewRequest(http.MethodGet, "/api/agents/nonexistent", nil)
	w := httptest.NewRecorder()
	srv.HandleAgentDetail(w, req)

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

	// DELETE to /api/agents should return 405
	req2 := httptest.NewRequest(http.MethodDelete, "/api/agents", nil)
	w2 := httptest.NewRecorder()
	srv.HandleAgents(w2, req2)

	if w2.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405, got %d", w2.Code)
	}
}

func TestFullLifecycle_Heartbeat(t *testing.T) {
	srv := newTestServer(t)

	// Register an agent
	srv.Registry().Register("heartbeat-agent", "Heartbeat Agent", []string{"test"})

	// Send heartbeat
	err := srv.Registry().Heartbeat("heartbeat-agent")
	if err != nil {
		t.Fatalf("heartbeat failed: %v", err)
	}

	// Verify agent still exists
	agent, ok := srv.Registry().GetAgent("heartbeat-agent")
	if !ok {
		t.Fatal("agent disappeared after heartbeat")
	}
	if agent.State != registry.AgentStateIdle {
		t.Errorf("expected IDLE, got %s", agent.State)
	}
}

func TestFullLifecycle_UnregisterAgent(t *testing.T) {
	srv := newTestServer(t)

	// Register an agent
	srv.Registry().Register("unreg-agent", "Unregister Agent", []string{"test"})

	// Unregister
	err := srv.Registry().Unregister("unreg-agent")
	if err != nil {
		t.Fatalf("unregister failed: %v", err)
	}

	// Verify agent is gone
	_, ok := srv.Registry().GetAgent("unreg-agent")
	if ok {
		t.Error("agent should not exist after unregister")
	}
}

func TestFullLifecycle_UnregisterRunningAgent(t *testing.T) {
	srv := newTestServer(t)

	// Register and assign a task to an agent
	srv.Registry().Register("running-agent", "Running Agent", []string{"test"})
	srv.Registry().SetAgentTask("running-agent", "task-1")

	// Try to unregister a running agent - should fail
	err := srv.Registry().Unregister("running-agent")
	if err == nil {
		t.Error("expected error when unregistering running agent")
	}
}

func TestFullLifecycle_StaleAgentDetection(t *testing.T) {
	srv := newTestServer(t)

	// Register agents
	srv.Registry().Register("stale-agent", "Stale Agent", []string{"test"})
	time.Sleep(10 * time.Millisecond)
	srv.Registry().Register("fresh-agent", "Fresh Agent", []string{"test"})

	// Remove stale agents with very short timeout
	stale := srv.Registry().RemoveStaleAgents(5 * time.Millisecond)
	if len(stale) != 1 {
		t.Errorf("expected 1 stale agent, got %d", len(stale))
	}
	if len(stale) > 0 && stale[0].ID != "stale-agent" {
		t.Errorf("expected stale-agent to be stale, got %s", stale[0].ID)
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

	// Test agent.register
	params, _ := json.Marshal(map[string]interface{}{
		"id":   "rpc-agent-1",
		"name": "RPC Agent",
		"tags": []string{"test"},
	})
	resp, err := hub.Dispatch("agent.register", params)
	if err != nil {
		t.Fatalf("agent.register failed: %v", err)
	}
	result := resp.(map[string]interface{})
	if result["status"] != "registered" {
		t.Errorf("expected registered, got %v", result["status"])
	}

	// Test agent.heartbeat
	params, _ = json.Marshal(map[string]interface{}{"id": "rpc-agent-1"})
	resp, err = hub.Dispatch("agent.heartbeat", params)
	if err != nil {
		t.Fatalf("agent.heartbeat failed: %v", err)
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
	_, err = hub.Dispatch("agent.register", params)
	if err == nil {
		t.Error("expected error for missing id")
	}
}

func TestFullLifecycle_AgentUnregister(t *testing.T) {
	srv := newTestServer(t)
	hub := srv.Hub()

	go hub.Run()

	// Register an agent
	params, _ := json.Marshal(map[string]interface{}{
		"id":   "unreg-agent",
		"name": "Unregister Agent",
		"tags": []string{"test"},
	})
	_, err := hub.Dispatch("agent.register", params)
	if err != nil {
		t.Fatalf("register failed: %v", err)
	}

	// Unregister the agent
	params, _ = json.Marshal(map[string]interface{}{"id": "unreg-agent"})
	resp, err := hub.Dispatch("agent.unregister", params)
	if err != nil {
		t.Fatalf("unregister failed: %v", err)
	}
	result := resp.(map[string]interface{})
	if result["status"] != "unregistered" {
		t.Errorf("expected unregistered, got %v", result["status"])
	}
}

func TestFullLifecycle_AgentLog(t *testing.T) {
	srv := newTestServer(t)
	hub := srv.Hub()

	go hub.Run()

	// Send a log entry
	params, _ := json.Marshal(map[string]interface{}{
		"task_id": "task-1",
		"line":    "Building project...",
		"level":   "info",
	})
	resp, err := hub.Dispatch("agent.log", params)
	if err != nil {
		t.Fatalf("agent.log failed: %v", err)
	}
	result := resp.(map[string]interface{})
	if result["status"] != "accepted" {
		t.Errorf("expected accepted, got %v", result["status"])
	}
}

func TestFullLifecycle_AgentStateIsString(t *testing.T) {
	srv := newTestServer(t)

	// Register agents with different states
	srv.Registry().Register("idle-agent", "Idle Agent", []string{"business-default"})
	srv.Registry().Register("running-agent", "Running Agent", []string{"android"})
	srv.Registry().SetAgentState("running-agent", registry.AgentStateRunning)

	// List agents via REST
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

	// Build a map keyed by agent ID for deterministic lookups
	agentMap := make(map[string]map[string]interface{})
	for _, agent := range response.Agents {
		agentMap[agent["id"].(string)] = agent
	}

	// Verify each agent's state is a string (not an integer)
	for id, agent := range agentMap {
		state, ok := agent["state"]
		if !ok {
			t.Fatalf("agent %s: expected 'state' field", id)
		}
		if _, isString := state.(string); !isString {
			t.Errorf("agent %s: expected state to be a string, got %T: %v", id, state, state)
		}
	}

	// Verify the running agent specifically has state "RUNNING"
	runningAgent, ok := agentMap["running-agent"]
	if !ok {
		t.Fatal("expected running-agent in response")
	}
	if runningAgent["state"] != "RUNNING" {
		t.Errorf("expected running agent state 'RUNNING', got %v", runningAgent["state"])
	}

	// Verify the idle agent has a valid state string
	idleAgent, ok := agentMap["idle-agent"]
	if !ok {
		t.Fatal("expected idle-agent in response")
	}
	idleState, ok := idleAgent["state"].(string)
	if !ok {
		t.Fatalf("expected idle agent state to be a string, got %T", idleAgent["state"])
	}
	switch idleState {
	case "IDLE", "DISCONNECTED", "REGISTERED":
		// valid initial states
	default:
		t.Errorf("unexpected idle agent state: %s", idleState)
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

func TestFullLifecycle_AgentResult(t *testing.T) {
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
	srv.TaskQueue().Assign(createdTask.ID, "agent-1")
	srv.TaskQueue().Start(createdTask.ID)

	// Submit success result
	hub := srv.Hub()
	go hub.Run()
	params, _ := json.Marshal(map[string]interface{}{
		"task_id": createdTask.ID,
		"success": true,
		"output":  "Build successful",
	})
	_, err := hub.Dispatch("agent.result", params)
	if err != nil {
		t.Fatalf("agent.result failed: %v", err)
	}

	// Verify task is completed
	taskData, _ := srv.TaskQueue().Get(createdTask.ID)
	if taskData.Status != queue.TaskStatusCompleted {
		t.Errorf("expected COMPLETED, got %s", taskData.Status)
	}
}
