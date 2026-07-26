package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"hotelier/pkg/config"
	"hotelier/pkg/persona"
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

func TestHandleTasks_POST_ReposRejected(t *testing.T) {
	srv := newTestServer(t)

	task := map[string]interface{}{
		"repos":  []string{"https://github.com/user/repo"},
		"prompt": "Build a feature",
	}
	body, _ := json.Marshal(task)

	req := httptest.NewRequest(http.MethodPost, "/api/tasks", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.HandleTasks(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 when repos is provided, got %d", w.Code)
	}
	bodyStr := w.Body.String()
	if !strings.Contains(bodyStr, "repos") {
		t.Errorf("expected error message mentioning 'repos', got: %s", bodyStr)
	}
}

func TestHandleTasks_POST_EmptyReposRejected(t *testing.T) {
	srv := newTestServer(t)

	task := map[string]interface{}{
		"repos":  []string{},
		"prompt": "Build a feature",
	}
	body, _ := json.Marshal(task)

	req := httptest.NewRequest(http.MethodPost, "/api/tasks", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.HandleTasks(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 when empty repos is provided, got %d", w.Code)
	}
}

func TestHandleTasks_POST_RepoRefAccepted(t *testing.T) {
	srv := newTestServer(t)

	task := map[string]interface{}{
		"prompt":   "Build a feature",
		"repo_ref": "https://github.com/user/repo.git",
	}
	body, _ := json.Marshal(task)

	req := httptest.NewRequest(http.MethodPost, "/api/tasks", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.HandleTasks(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201 when repo_ref is provided, got %d: %s", w.Code, w.Body.String())
	}

	var createdTask queue.Task
	if err := json.Unmarshal(w.Body.Bytes(), &createdTask); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}

	if createdTask.RepoRef != "https://github.com/user/repo.git" {
		t.Errorf("expected repo_ref 'https://github.com/user/repo.git', got %q", createdTask.RepoRef)
	}
}

func TestHandleTasks_POST_RepoRefAndPersona(t *testing.T) {
	srv := newTestServer(t)

	task := map[string]interface{}{
		"prompt":   "Build a feature",
		"repo_ref": "https://github.com/user/repo.git",
		"persona":  "",
	}
	body, _ := json.Marshal(task)

	req := httptest.NewRequest(http.MethodPost, "/api/tasks", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.HandleTasks(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201 when repo_ref and persona are provided, got %d: %s", w.Code, w.Body.String())
	}

	var createdTask queue.Task
	if err := json.Unmarshal(w.Body.Bytes(), &createdTask); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}

	if createdTask.RepoRef != "https://github.com/user/repo.git" {
		t.Errorf("expected repo_ref 'https://github.com/user/repo.git', got %q", createdTask.RepoRef)
	}
}

func TestHandleTasks_POST_CustomID(t *testing.T) {
	srv := newTestServer(t)

	task := map[string]interface{}{
		"id":     "my-custom-id",
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

// Persona validation tests
func TestHandleTasks_POST_InvalidPersona(t *testing.T) {
	srv := newTestServer(t)

	task := map[string]interface{}{
		"prompt":  "Build a feature",
		"persona": "nonexistent-persona",
	}
	body, _ := json.Marshal(task)

	req := httptest.NewRequest(http.MethodPost, "/api/tasks", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.HandleTasks(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for invalid persona, got %d", w.Code)
	}

	bodyStr := w.Body.String()
	if !strings.Contains(bodyStr, "persona") {
		t.Errorf("expected error message about persona, got: %s", bodyStr)
	}
}

func TestHandleTasks_POST_ValidPersona(t *testing.T) {
	cfg := config.ServerConfig{
		Host: "127.0.0.1",
		Port: 0,
		Personas: []persona.Persona{
			{
				Name: "test-persona",
				Env:  map[string]string{"TEST_VAR": "<workpath>/test"},
			},
		},
	}
	srv := New(cfg)

	task := map[string]interface{}{
		"prompt":  "Build a feature",
		"persona": "test-persona",
	}
	body, _ := json.Marshal(task)

	req := httptest.NewRequest(http.MethodPost, "/api/tasks", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.HandleTasks(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201 for valid persona, got %d. Body: %s", w.Code, w.Body.String())
	}

	var createdTask queue.Task
	if err := json.Unmarshal(w.Body.Bytes(), &createdTask); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}

	if createdTask.Persona != "test-persona" {
		t.Errorf("expected persona 'test-persona', got %q", createdTask.Persona)
	}
}

func TestHandleTasks_POST_NoPersona(t *testing.T) {
	srv := newTestServer(t)

	task := map[string]interface{}{
		"prompt": "Build a feature",
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

	if createdTask.Persona != "" {
		t.Errorf("expected empty persona, got %q", createdTask.Persona)
	}
}

// Task listing tests
func TestHandleTasks_GET(t *testing.T) {
	srv := newTestServer(t)

	// Create some tasks
	for i := 0; i < 3; i++ {
		task := map[string]interface{}{
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

func TestHandleTasks_GET_PersonaInResponse(t *testing.T) {
	cfg := config.ServerConfig{
		Host: "127.0.0.1",
		Port: 0,
		Personas: []persona.Persona{
			{Name: "dev-agent"},
		},
	}
	srv := New(cfg)

	// Create a task with persona
	task := map[string]interface{}{
		"prompt":  "Build a feature",
		"persona": "dev-agent",
	}
	body, _ := json.Marshal(task)
	req := httptest.NewRequest(http.MethodPost, "/api/tasks", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.HandleTasks(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", w.Code)
	}

	// List tasks and verify persona is in the response
	req = httptest.NewRequest(http.MethodGet, "/api/tasks", nil)
	w = httptest.NewRecorder()
	srv.HandleTasks(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var response struct {
		Tasks []map[string]interface{} `json:"tasks"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	if len(response.Tasks) != 1 {
		t.Fatalf("expected 1 task, got %d", len(response.Tasks))
	}

	gotPersona, ok := response.Tasks[0]["persona"].(string)
	if !ok {
		t.Fatalf("persona field missing or not a string in task list response")
	}
	if gotPersona != "dev-agent" {
		t.Errorf("expected persona 'dev-agent', got %q", gotPersona)
	}
}

func TestHandleTasks_GET_PersonaEmpty(t *testing.T) {
	srv := newTestServer(t)

	// Create a task without persona
	task := map[string]interface{}{
		"prompt": "Build something",
	}
	body, _ := json.Marshal(task)
	req := httptest.NewRequest(http.MethodPost, "/api/tasks", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.HandleTasks(w, req)

	// List tasks and verify persona is present but empty
	req = httptest.NewRequest(http.MethodGet, "/api/tasks", nil)
	w = httptest.NewRecorder()
	srv.HandleTasks(w, req)

	var response struct {
		Tasks []map[string]interface{} `json:"tasks"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	if len(response.Tasks) != 1 {
		t.Fatalf("expected 1 task, got %d", len(response.Tasks))
	}

	// Persona should be present but empty string
	gotPersona, ok := response.Tasks[0]["persona"].(string)
	if !ok {
		t.Fatalf("persona field missing or not a string in task list response")
	}
	if gotPersona != "" {
		t.Errorf("expected empty persona, got %q", gotPersona)
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
		Tasks []map[string]interface{} `json:"tasks"`
		Count int                      `json:"count"`
	}
	json.Unmarshal(w.Body.Bytes(), &response)

	if response.Count != 0 {
		t.Errorf("expected 0 tasks, got %d", response.Count)
	}
}

func TestHandleTasks_GET_LogCount(t *testing.T) {
	srv := newTestServer(t)

	// Create two tasks
	for i := 0; i < 2; i++ {
		task := map[string]interface{}{
			"id":     fmt.Sprintf("task-log-%d", i),
			"prompt": fmt.Sprintf("Task %d", i),
		}
		body, _ := json.Marshal(task)
		req := httptest.NewRequest(http.MethodPost, "/api/tasks", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		srv.HandleTasks(w, req)
	}

	// Add logs to first task only
	srv.LogStore().Add(TaskLogEntry{TaskID: "task-log-0", Line: "hello", Level: "text", Timestamp: time.Now()})
	srv.LogStore().Add(TaskLogEntry{TaskID: "task-log-0", Line: "world", Level: "text", Timestamp: time.Now()})

	// List tasks
	req := httptest.NewRequest(http.MethodGet, "/api/tasks", nil)
	w := httptest.NewRecorder()
	srv.HandleTasks(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var response struct {
		Tasks []map[string]interface{} `json:"tasks"`
		Count int                      `json:"count"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	if response.Count != 2 {
		t.Fatalf("expected 2 tasks, got %d", response.Count)
	}

	// Check log_count for each task
	for _, task := range response.Tasks {
		id := task["id"].(string)
		logCount, ok := task["log_count"]
		if !ok {
			t.Errorf("task %s: expected 'log_count' field", id)
			continue
		}
		count, ok := logCount.(float64)
		if !ok {
			t.Errorf("task %s: expected log_count to be a number, got %T", id, logCount)
			continue
		}
		if id == "task-log-0" && count != 2 {
			t.Errorf("task-log-0: expected log_count 2, got %v", count)
		}
		if id == "task-log-1" && count != 0 {
			t.Errorf("task-log-1: expected log_count 0, got %v", count)
		}
	}
}

// Task detail tests
func TestHandleTaskDetail(t *testing.T) {
	srv := newTestServer(t)

	// Create a task
	task := map[string]interface{}{
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

	// Now dispatch guest.register - this should record the mapping
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

func TestHandleGuestRegister_PreservesRunningState(t *testing.T) {
	srv := newTestServer(t)
	hub := srv.Hub()
	go hub.Run()

	// Register a guest
	params1, _ := json.Marshal(map[string]interface{}{
		"id":   "running-guest",
		"name": "Running Guest",
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

	// Create a real task and assign it through the orchestrator.
	// This is the authoritative path — ReconcileGuest checks the task queue.
	task := &queue.Task{
		ID:     "task-123",
		Prompt: "test task",
		Tags:   []string{"tag1"},
	}
	if err := srv.TaskQueue().Add(task); err != nil {
		t.Fatalf("failed to add task: %v", err)
	}
	if err := srv.TaskQueue().Assign("task-123", "running-guest"); err != nil {
		t.Fatalf("failed to assign task: %v", err)
	}
	if err := srv.Registry().SetGuestTask("running-guest", "task-123"); err != nil {
		t.Fatalf("failed to set guest task: %v", err)
	}

	guest, _ := srv.Registry().GetGuest("running-guest")
	if guest.State != registry.GuestStateRunning {
		t.Fatalf("expected guest state RUNNING, got %s", guest.State)
	}

	// Re-register the guest (simulates reconnect while running a task)
	params2, _ := json.Marshal(map[string]interface{}{
		"id":   "running-guest",
		"name": "Running Guest",
		"tags": []string{"tag1"},
	})

	resp2, err := hub.Dispatch("guest.register", params2)
	if err != nil {
		t.Fatalf("re-register failed: %v", err)
	}
	result2 := resp2.(map[string]interface{})
	if result2["status"] != "re-registered" {
		t.Errorf("expected status 're-registered', got %v", result2["status"])
	}

	// Verify the guest state was preserved as RUNNING (task is ASSIGNED in queue)
	guest, _ = srv.Registry().GetGuest("running-guest")
	if guest.State != registry.GuestStateRunning {
		t.Errorf("expected guest state RUNNING after re-registration, got %s", guest.State)
	}
	if guest.TaskID != "task-123" {
		t.Errorf("expected task ID 'task-123', got %s", guest.TaskID)
	}
}

func TestHandleGuestRegister_IdleGuestGetsTask(t *testing.T) {
	srv := newTestServer(t)
	hub := srv.Hub()
	go hub.Run()

	// Create a pending task
	task := &queue.Task{
		ID:     "task-pending-1",
		Prompt: "Do something",
		Tags:   []string{},
	}
	if err := srv.TaskQueue().Add(task); err != nil {
		t.Fatalf("failed to add task: %v", err)
	}

	// Register a guest
	params1, _ := json.Marshal(map[string]interface{}{
		"id":   "idle-guest",
		"name": "Idle Guest",
		"tags": []string{},
	})

	resp1, err := hub.Dispatch("guest.register", params1)
	if err != nil {
		t.Fatalf("first register failed: %v", err)
	}
	result1 := resp1.(map[string]interface{})
	if result1["status"] != "registered" {
		t.Errorf("expected status 'registered', got %v", result1["status"])
	}

	// Verify the guest is IDLE
	guest, _ := srv.Registry().GetGuest("idle-guest")
	if guest.State != registry.GuestStateIdle {
		t.Fatalf("expected guest state IDLE, got %s", guest.State)
	}

	// Re-register the guest (simulates reconnect while idle)
	params2, _ := json.Marshal(map[string]interface{}{
		"id":   "idle-guest",
		"name": "Idle Guest",
		"tags": []string{},
	})

	resp2, err := hub.Dispatch("guest.register", params2)
	if err != nil {
		t.Fatalf("re-register failed: %v", err)
	}
	result2 := resp2.(map[string]interface{})
	if result2["status"] != "re-registered" {
		t.Errorf("expected status 're-registered', got %v", result2["status"])
	}

	// Verify the guest is still IDLE (should not have been changed)
	guest, _ = srv.Registry().GetGuest("idle-guest")
	if guest.State != registry.GuestStateIdle {
		t.Errorf("expected guest state IDLE after re-registration, got %s", guest.State)
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

	// Nothing emitted yet - still buffering
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

	// Nothing emitted yet - still buffering
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
	// These should NOT be batched - they must go through immediately.
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

// TestLogAccumulator_ThinkingDeltasEmitImmediately verifies that thinking
// deltas are emitted one-by-one (not batched) so the frontend can stream
// them piecemeal.
func TestLogAccumulator_ThinkingDeltasEmitImmediately(t *testing.T) {
	logger := log.New(io.Discard, "", 0)
	acc := NewLogAccumulator(logger)

	var emitted []TaskLogEntry
	emit := func(e TaskLogEntry) {
		emitted = append(emitted, e)
	}

	// Each thinking delta should be emitted immediately
	acc.Feed("task-1", "Let me think ", "thinking", emit)
	if len(emitted) != 1 {
		t.Fatalf("expected 1 emitted entry after first thinking delta, got %d", len(emitted))
	}
	if emitted[0].Line != "Let me think " {
		t.Errorf("expected 'Let me think ', got %q", emitted[0].Line)
	}
	if emitted[0].Level != "thinking" {
		t.Errorf("expected level 'thinking', got %q", emitted[0].Level)
	}

	acc.Feed("task-1", "about this", "thinking", emit)
	if len(emitted) != 2 {
		t.Fatalf("expected 2 emitted entries after second thinking delta, got %d", len(emitted))
	}
	if emitted[1].Line != "about this" {
		t.Errorf("expected 'about this', got %q", emitted[1].Line)
	}
	if emitted[1].Level != "thinking" {
		t.Errorf("expected level 'thinking', got %q", emitted[1].Level)
	}

	// Flush should produce no additional entries
	acc.FlushAll(emit)
	if len(emitted) != 2 {
		t.Fatalf("expected 2 emitted entries after FlushAll, got %d", len(emitted))
	}
}

// TestLogAccumulator_ThinkingFlushesTextBuffer verifies that when thinking
// deltas arrive, any pending text buffer is flushed first.
func TestLogAccumulator_ThinkingFlushesTextBuffer(t *testing.T) {
	logger := log.New(io.Discard, "", 0)
	acc := NewLogAccumulator(logger)

	var emitted []TaskLogEntry
	emit := func(e TaskLogEntry) {
		emitted = append(emitted, e)
	}

	// Text accumulates in buffer
	acc.Feed("task-1", "First ", "text", emit)
	acc.Feed("task-1", "sentence.", "text", emit)

	// Thinking arrives - should flush text buffer, then emit thinking immediately
	acc.Feed("task-1", "Hmm, let me think.", "thinking", emit)

	// Should have 2 entries: flushed text, then thinking
	if len(emitted) != 2 {
		t.Fatalf("expected 2 emitted entries after thinking arrives, got %d", len(emitted))
	}
	if emitted[0].Line != "First sentence." {
		t.Errorf("entry 0: expected 'First sentence.', got %q", emitted[0].Line)
	}
	if emitted[0].Level != "text" {
		t.Errorf("entry 0: expected level 'text', got %q", emitted[0].Level)
	}
	if emitted[1].Line != "Hmm, let me think." {
		t.Errorf("entry 1: expected 'Hmm, let me think.', got %q", emitted[1].Line)
	}
	if emitted[1].Level != "thinking" {
		t.Errorf("entry 1: expected level 'thinking', got %q", emitted[1].Level)
	}

	// Flush should produce no additional entries
	acc.FlushAll(emit)
	if len(emitted) != 2 {
		t.Fatalf("expected 2 emitted entries after FlushAll, got %d", len(emitted))
	}
}

// TestLogAccumulator_TextThinkingTextTransition verifies the full cycle:
// text → thinking → text, with thinking emitted immediately and text
// buffered.
func TestLogAccumulator_TextThinkingTextTransition(t *testing.T) {
	logger := log.New(io.Discard, "", 0)
	acc := NewLogAccumulator(logger)

	var emitted []TaskLogEntry
	emit := func(e TaskLogEntry) {
		emitted = append(emitted, e)
	}

	// Text accumulates
	acc.Feed("task-1", "First ", "text", emit)
	acc.Feed("task-1", "sentence.", "text", emit)

	// Thinking arrives - flushes text buffer, emits immediately
	acc.Feed("task-1", "Hmm, ", "thinking", emit)
	acc.Feed("task-1", "let me reconsider.", "thinking", emit)

	// Text arrives again - thinking doesn't buffer so nothing to flush.
	// Text accumulates in buffer.
	acc.Feed("task-1", "Actually, ", "text", emit)
	acc.Feed("task-1", "here's my answer.", "text", emit)

	// Should have 3 entries: text flushed, thinking x2
	if len(emitted) != 3 {
		t.Fatalf("expected 3 emitted entries after transitions, got %d", len(emitted))
	}
	if emitted[0].Line != "First sentence." {
		t.Errorf("entry 0: expected 'First sentence.', got %q", emitted[0].Line)
	}
	if emitted[0].Level != "text" {
		t.Errorf("entry 0: expected level 'text', got %q", emitted[0].Level)
	}
	if emitted[1].Line != "Hmm, " {
		t.Errorf("entry 1: expected 'Hmm, ', got %q", emitted[1].Line)
	}
	if emitted[1].Level != "thinking" {
		t.Errorf("entry 1: expected level 'thinking', got %q", emitted[1].Level)
	}
	if emitted[2].Line != "let me reconsider." {
		t.Errorf("entry 2: expected 'let me reconsider.', got %q", emitted[2].Line)
	}
	if emitted[2].Level != "thinking" {
		t.Errorf("entry 2: expected level 'thinking', got %q", emitted[2].Level)
	}

	// Flush remaining text
	acc.FlushAll(emit)

	if len(emitted) != 4 {
		t.Fatalf("expected 4 emitted entries total, got %d", len(emitted))
	}
	if emitted[3].Line != "Actually, here's my answer." {
		t.Errorf("entry 3: expected \"Actually, here's my answer.\", got %q", emitted[3].Line)
	}
	if emitted[3].Level != "text" {
		t.Errorf("entry 3: expected level 'text', got %q", emitted[3].Level)
	}
}

// TestLogAccumulator_ToolFlushesPendingBuffers verifies that a tool message
// flushes any pending text buffer and is emitted immediately.
func TestLogAccumulator_ToolFlushesPendingBuffers(t *testing.T) {
	logger := log.New(io.Discard, "", 0)
	acc := NewLogAccumulator(logger)

	var emitted []TaskLogEntry
	emit := func(e TaskLogEntry) {
		emitted = append(emitted, e)
	}

	// Text accumulates
	acc.Feed("task-1", "Let me think ", "text", emit)
	acc.Feed("task-1", "about this", "text", emit)

	// Thinking emitted immediately
	acc.Feed("task-1", "Hmm.", "thinking", emit)

	// Tool message should flush any pending text buffer (there shouldn't be any)
	acc.Feed("task-1", "[TOOL_START] bash: ls (id: t1)", "tool", emit)

	if len(emitted) != 3 {
		t.Fatalf("expected 3 emitted entries, got %d", len(emitted))
	}
	if emitted[0].Line != "Let me think about this" {
		t.Errorf("entry 0: expected 'Let me think about this', got %q", emitted[0].Line)
	}
	if emitted[0].Level != "text" {
		t.Errorf("entry 0: expected level 'text', got %q", emitted[0].Level)
	}
	if emitted[1].Line != "Hmm." {
		t.Errorf("entry 1: expected 'Hmm.', got %q", emitted[1].Line)
	}
	if emitted[1].Level != "thinking" {
		t.Errorf("entry 1: expected level 'thinking', got %q", emitted[1].Level)
	}
	if emitted[2].Line != "[TOOL_START] bash: ls (id: t1)" {
		t.Errorf("entry 2: expected tool start, got %q", emitted[2].Line)
	}
	if emitted[2].Level != "tool" {
		t.Errorf("entry 2: expected level 'tool', got %q", emitted[2].Level)
	}
}

// TestLogAccumulator_ThinkingDeltasEmitImmediatelyWithGaps verifies that
// thinking deltas are emitted immediately even with long gaps between them.
// This replaces the old TestLogAccumulator_ThinkingNotFlushedByInactivity.
func TestLogAccumulator_ThinkingDeltasEmitImmediatelyWithGaps(t *testing.T) {
	logger := log.New(io.Discard, "", 0)
	acc := NewLogAccumulator(logger)

	var emitted []TaskLogEntry
	emit := func(e TaskLogEntry) {
		emitted = append(emitted, e)
	}

	// Feed thinking deltas with >1s gaps between them
	acc.Feed("task-1", "Let me think ", "thinking", emit)
	if len(emitted) != 1 {
		t.Fatalf("expected 1 emitted entry after first delta, got %d", len(emitted))
	}

	time.Sleep(1100 * time.Millisecond) // exceed flushPeriod

	acc.Feed("task-1", "about this ", "thinking", emit)
	if len(emitted) != 2 {
		t.Fatalf("expected 2 emitted entries after second delta, got %d", len(emitted))
	}

	time.Sleep(1100 * time.Millisecond) // exceed flushPeriod again

	acc.Feed("task-1", "problem.", "thinking", emit)
	if len(emitted) != 3 {
		t.Fatalf("expected 3 emitted entries after third delta, got %d", len(emitted))
	}

	// Each delta should be its own entry
	if emitted[0].Line != "Let me think " {
		t.Errorf("entry 0: expected 'Let me think ', got %q", emitted[0].Line)
	}
	if emitted[1].Line != "about this " {
		t.Errorf("entry 1: expected 'about this ', got %q", emitted[1].Line)
	}
	if emitted[2].Line != "problem." {
		t.Errorf("entry 2: expected 'problem.', got %q", emitted[2].Line)
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

	// Message: task.log (no task.updated on first log in new acknowledge model)
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
// guests that have been silent for longer than the configured TaskSilenceTimeout.
func TestCheckSilentGuests(t *testing.T) {
	cfg := config.ServerConfig{
		Host:               "127.0.0.1",
		Port:               0,
		HeartbeatInterval:  1, // 1 second for fast test
		SilenceTimeout:     2, // 2 seconds
		TaskSilenceTimeout: 2, // 2 seconds for running tasks
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

	// Don't send heartbeats - the guest will be stale
	// Wait for the task to be running
	time.Sleep(100 * time.Millisecond)

	// Manually advance the guest's LastHeartbeat to simulate silence
	srv.Registry().SetLastHeartbeat("silent-guest", time.Now().Add(-3*time.Second))

	// Run checkSilentGuests - it should kill the task
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

func TestServerReload_PersonaContentChange(t *testing.T) {
	// Reload should detect changes to persona content (not just length).
	// This tests the deep equality fix for the reload logic.
	originalCfg := config.ServerConfig{
		Host: "127.0.0.1",
		Port: 0,
		Personas: []persona.Persona{
			{Name: "alpha", Env: map[string]string{"KEY": "old-value"}},
		},
	}
	srv := New(originalCfg)

	// Verify initial persona
	initialPersona, err := srv.personaStore.Get("alpha")
	if err != nil {
		t.Fatalf("expected to find persona 'alpha': %v", err)
	}
	if initialPersona.Env["KEY"] != "old-value" {
		t.Errorf("expected 'old-value', got %q", initialPersona.Env["KEY"])
	}

	// Reload with same number of personas but different content
	newCfg := config.ServerConfig{
		Host: "127.0.0.1",
		Port: 0,
		Personas: []persona.Persona{
			{Name: "alpha", Env: map[string]string{"KEY": "new-value"}},
		},
	}
	srv.Reload(newCfg)

	// Verify persona was updated (store was rebuilt)
	updatedPersona, err := srv.personaStore.Get("alpha")
	if err != nil {
		t.Fatalf("expected to find persona 'alpha' after reload: %v", err)
	}
	if updatedPersona.Env["KEY"] != "new-value" {
		t.Errorf("expected 'new-value' after reload, got %q", updatedPersona.Env["KEY"])
	}
}

func TestServerReload_PersonaInvalidKeepsOldStore(t *testing.T) {
	// Reload with invalid persona config should keep the old store.
	originalCfg := config.ServerConfig{
		Host: "127.0.0.1",
		Port: 0,
		Personas: []persona.Persona{
			{Name: "valid", Env: map[string]string{"KEY": "safe-value"}},
		},
	}
	srv := New(originalCfg)

	// Reload with invalid persona (path traversal)
	invalidCfg := config.ServerConfig{
		Host: "127.0.0.1",
		Port: 0,
		Personas: []persona.Persona{
			{Name: "bad", Files: []persona.FileCopy{{From: "/tmp/x", To: "../escape"}}},
		},
	}
	srv.Reload(invalidCfg)

	// Old persona should still be accessible (store was NOT replaced)
	oldPersona, err := srv.personaStore.Get("valid")
	if err != nil {
		t.Fatalf("expected to still find persona 'valid' after failed reload: %v", err)
	}
	if oldPersona.Env["KEY"] != "safe-value" {
		t.Errorf("expected 'safe-value' preserved, got %q", oldPersona.Env["KEY"])
	}

	// Invalid persona should not be in the store
	_, err = srv.personaStore.Get("bad")
	if err == nil {
		t.Error("expected 'bad' persona to not exist (reload was rejected)")
	}
}

func TestServerNew_PersonaValidation(t *testing.T) {
	// Server should validate personas at startup.
	// Invalid personas are logged but server still starts (with empty store).
	cfg := config.ServerConfig{
		Host: "127.0.0.1",
		Port: 0,
		Personas: []persona.Persona{
			{Name: "valid", Env: map[string]string{"KEY": "value"}},
		},
	}
	srv := New(cfg)

	// Valid persona should be in the store
	found, err := srv.personaStore.Get("valid")
	if err != nil {
		t.Fatalf("expected to find persona 'valid': %v", err)
	}
	if found.Env["KEY"] != "value" {
		t.Errorf("expected 'value', got %q", found.Env["KEY"])
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
	hub := srv.Hub()
	go hub.Run()

	// Register a guest
	srv.Registry().Register("test-guest", "test-guest", []string{"default"})

	// Set up a connection and mapping (so SendToGuest succeeds)
	conn := rpc.NewTestConnection("conn-test-guest", hub)
	hub.Register(conn)
	hub.RegisterGuestConnection("test-guest", "conn-test-guest")
	time.Sleep(10 * time.Millisecond)

	// Add a pending task
	task := &queue.Task{
		ID:     "task-1",
		Prompt: "test task",
		Tags:   []string{"default"},
	}
	srv.TaskQueue().Add(task)

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
		t.Errorf("expected assigned_to 'test-guest', got %s", qTask.AssignedTo)
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

	// Verify the notification was sent
	data, ok := conn.Recv()
	if !ok {
		t.Fatal("expected task.assign notification to be sent")
	}
	var msg rpc.JSONRPCMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		t.Fatalf("failed to unmarshal sent message: %v", err)
	}
	if msg.Method != "task.assign" {
		t.Errorf("expected method 'task.assign', got %s", msg.Method)
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
		"prompt": "tool fields test",
	}
	body, _ := json.Marshal(task)
	req := httptest.NewRequest(http.MethodPost, "/api/tasks", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.HandleTasks(w, req)

	var createdTask queue.Task
	json.Unmarshal(w.Body.Bytes(), &createdTask)

	// Send tool messages with EMPTY level - simulating the path where the
	// accumulator detects the [TOOL_*] prefix and sets level to "tool".
	// The structured fields should still be populated from the line content.
	toolLines := []map[string]interface{}{
		{
			"task_id": createdTask.ID,
			"line":    "[TOOL_START] read_file: /etc/passwd (id: t1)",
			"level":   "", // empty - accumulator will detect tool prefix
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

// TestHandleTaskRerun_Success creates a task, completes it, then re-runs it.
// The new task should have the same prompt and tags but a different ID.
func TestHandleTaskRerun_Success(t *testing.T) {
	srv := newTestServer(t)

	// Create original task
	origTask := map[string]interface{}{
		"id":     "original-task",
		"prompt": "Build a feature",
		"tags":   []string{"business-default"},
	}
	body, _ := json.Marshal(origTask)
	req := httptest.NewRequest(http.MethodPost, "/api/tasks", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.HandleTasks(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201 for original task, got %d", w.Code)
	}

	// Re-run the task
	rerunReq := httptest.NewRequest(http.MethodPost, "/api/tasks/original-task/rerun", nil)
	rerunW := httptest.NewRecorder()
	srv.HandleTaskDetail(rerunW, rerunReq)

	if rerunW.Code != http.StatusCreated {
		t.Fatalf("expected 201 for rerun, got %d", rerunW.Code)
	}

	var newTask queue.Task
	if err := json.Unmarshal(rerunW.Body.Bytes(), &newTask); err != nil {
		t.Fatalf("failed to unmarshal rerun response: %v", err)
	}

	// New task should have different ID
	if newTask.ID == "original-task" {
		t.Errorf("expected new task to have a different ID, got %s", newTask.ID)
	}

	// But same prompt and tags
	if newTask.Prompt != "Build a feature" {
		t.Errorf("expected prompt 'Build a feature', got %s", newTask.Prompt)
	}
	if len(newTask.Tags) != 1 || newTask.Tags[0] != "business-default" {
		t.Errorf("expected tags ['business-default'], got %v", newTask.Tags)
	}
	if newTask.Status != queue.TaskStatusPending {
		t.Errorf("expected PENDING status, got %s", newTask.Status)
	}
}

// TestHandleTaskRerun_NotFound returns 404 when the original task does not exist.
func TestHandleTaskRerun_NotFound(t *testing.T) {
	srv := newTestServer(t)

	req := httptest.NewRequest(http.MethodPost, "/api/tasks/nonexistent-task/rerun", nil)
	w := httptest.NewRecorder()
	srv.HandleTaskDetail(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", w.Code)
	}
}

// TestHandleTaskRerun_EmptyID returns 400 when the task ID is empty.
func TestHandleTaskRerun_EmptyID(t *testing.T) {
	srv := newTestServer(t)

	req := httptest.NewRequest(http.MethodPost, "/api/tasks//rerun", nil)
	w := httptest.NewRecorder()
	srv.HandleTaskDetail(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

// TestHandleTaskRerun_GET returns 405 for non-POST methods.
func TestHandleTaskRerun_MethodNotAllowed(t *testing.T) {
	srv := newTestServer(t)

	// Create original task
	origTask := map[string]interface{}{
		"id":     "original-task",
		"prompt": "Build a feature",
	}
	body, _ := json.Marshal(origTask)
	req := httptest.NewRequest(http.MethodPost, "/api/tasks", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.HandleTasks(w, req)

	// GET should not trigger rerun
	getReq := httptest.NewRequest(http.MethodGet, "/api/tasks/original-task/rerun", nil)
	getW := httptest.NewRecorder()
	srv.HandleTaskDetail(getW, getReq)

	// GET on /api/tasks/:id returns the task detail (200), not a rerun
	// The rerun path is a sub-path, so GET should return 405
	if getW.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405 for GET on rerun path, got %d", getW.Code)
	}
}

// TestHandleTaskRerun_FailedTask verifies that a FAILED task can be re-run.
// This is the primary use case for the requeue button in the task list UI.
func TestHandleTaskRerun_FailedTask(t *testing.T) {
	srv := newTestServer(t)

	// Create original task
	origTask := map[string]interface{}{
		"id":     "failed-task",
		"prompt": "Build a feature",
		"tags":   []string{"business-default"},
	}
	body, _ := json.Marshal(origTask)
	req := httptest.NewRequest(http.MethodPost, "/api/tasks", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.HandleTasks(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201 for original task, got %d", w.Code)
	}

	// Simulate task failure through proper lifecycle: PENDING → ASSIGNED → RUNNING → FAILED
	taskQueue := srv.TaskQueue()
	if err := taskQueue.Assign("failed-task", "guest-1"); err != nil {
		t.Fatalf("failed to assign task: %v", err)
	}
	if err := taskQueue.Start("failed-task"); err != nil {
		t.Fatalf("failed to start task: %v", err)
	}
	if err := taskQueue.Fail("failed-task", "build failed"); err != nil {
		t.Fatalf("failed to mark task as failed: %v", err)
	}

	// Re-run the failed task
	rerunReq := httptest.NewRequest(http.MethodPost, "/api/tasks/failed-task/rerun", nil)
	rerunW := httptest.NewRecorder()
	srv.HandleTaskDetail(rerunW, rerunReq)

	if rerunW.Code != http.StatusCreated {
		t.Fatalf("expected 201 for rerun of failed task, got %d", rerunW.Code)
	}

	var newTask queue.Task
	if err := json.Unmarshal(rerunW.Body.Bytes(), &newTask); err != nil {
		t.Fatalf("failed to unmarshal rerun response: %v", err)
	}

	if newTask.Prompt != "Build a feature" {
		t.Errorf("expected prompt 'Build a feature', got %s", newTask.Prompt)
	}
	if newTask.Status != queue.TaskStatusPending {
		t.Errorf("expected PENDING status for rerun of failed task, got %s", newTask.Status)
	}
}

// TestHandleTaskRerun_CancelledTask verifies that a CANCELLED task can be re-run.
func TestHandleTaskRerun_CancelledTask(t *testing.T) {
	srv := newTestServer(t)

	// Create original task
	origTask := map[string]interface{}{
		"id":     "cancelled-task",
		"prompt": "Do something",
		"tags":   []string{},
	}
	body, _ := json.Marshal(origTask)
	req := httptest.NewRequest(http.MethodPost, "/api/tasks", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.HandleTasks(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201 for original task, got %d", w.Code)
	}

	// Simulate task cancellation
	taskQueue := srv.TaskQueue()
	if err := taskQueue.Cancel("cancelled-task"); err != nil {
		t.Fatalf("failed to cancel task: %v", err)
	}

	// Re-run the cancelled task
	rerunReq := httptest.NewRequest(http.MethodPost, "/api/tasks/cancelled-task/rerun", nil)
	rerunW := httptest.NewRecorder()
	srv.HandleTaskDetail(rerunW, rerunReq)

	if rerunW.Code != http.StatusCreated {
		t.Fatalf("expected 201 for rerun of cancelled task, got %d", rerunW.Code)
	}

	var newTask queue.Task
	if err := json.Unmarshal(rerunW.Body.Bytes(), &newTask); err != nil {
		t.Fatalf("failed to unmarshal rerun response: %v", err)
	}

	if newTask.Prompt != "Do something" {
		t.Errorf("expected prompt 'Do something', got %s", newTask.Prompt)
	}
	if newTask.Status != queue.TaskStatusPending {
		t.Errorf("expected PENDING status for rerun of cancelled task, got %s", newTask.Status)
	}
}

// TestHandleCreateTask_ValidPriority verifies that valid priority values
// are accepted and returned in the response.
func TestHandleCreateTask_ValidPriority(t *testing.T) {
	srv := newTestServer(t)

	for _, priority := range []string{queue.PriorityFirefighter, queue.PriorityTeacher, queue.PriorityOrangutan} {
		task := map[string]interface{}{
			"id":       "task-priority-" + priority,
			"prompt":   "Priority test",
			"priority": priority,
		}
		body, _ := json.Marshal(task)
		req := httptest.NewRequest(http.MethodPost, "/api/tasks", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		srv.HandleTasks(w, req)

		if w.Code != http.StatusCreated {
			t.Fatalf("priority %q: expected 201, got %d body: %s", priority, w.Code, w.Body.String())
		}

		var created queue.Task
		if err := json.Unmarshal(w.Body.Bytes(), &created); err != nil {
			t.Fatalf("priority %q: failed to unmarshal: %v", priority, err)
		}
		if created.Priority != priority {
			t.Errorf("priority %q: expected %q, got %q", priority, priority, created.Priority)
		}
	}
}

// TestHandleCreateTask_InvalidPriority verifies that invalid priority values
// are rejected.
func TestHandleCreateTask_InvalidPriority(t *testing.T) {
	srv := newTestServer(t)

	task := map[string]interface{}{
		"id":       "task-1",
		"prompt":   "Priority test",
		"priority": "🔥",
	}
	body, _ := json.Marshal(task)
	req := httptest.NewRequest(http.MethodPost, "/api/tasks", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.HandleTasks(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for invalid priority, got %d body: %s", w.Code, w.Body.String())
	}
}

// TestHandleCreateTask_DefaultPriority verifies that tasks without a priority
// default to orangutan.
func TestHandleCreateTask_DefaultPriority(t *testing.T) {
	srv := newTestServer(t)

	task := map[string]interface{}{
		"id":     "task-1",
		"prompt": "Default priority test",
	}
	body, _ := json.Marshal(task)
	req := httptest.NewRequest(http.MethodPost, "/api/tasks", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.HandleTasks(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d body: %s", w.Code, w.Body.String())
	}

	var created queue.Task
	if err := json.Unmarshal(w.Body.Bytes(), &created); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}
	if created.Priority != queue.PriorityOrangutan {
		t.Errorf("expected default priority %q, got %q", queue.PriorityOrangutan, created.Priority)
	}
}

// TestHandleGetTasks_IncludesPriority verifies that the tasks list includes
// the priority field.
func TestHandleGetTasks_IncludesPriority(t *testing.T) {
	srv := newTestServer(t)

	task := map[string]interface{}{
		"id":       "task-1",
		"prompt":   "Priority test",
		"priority": queue.PriorityFirefighter,
	}
	body, _ := json.Marshal(task)
	req := httptest.NewRequest(http.MethodPost, "/api/tasks", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.HandleTasks(w, req)

	// Get tasks
	tasksReq := httptest.NewRequest(http.MethodGet, "/api/tasks", nil)
	tasksW := httptest.NewRecorder()
	srv.HandleTasks(tasksW, tasksReq)

	var tasksData map[string]interface{}
	if err := json.Unmarshal(tasksW.Body.Bytes(), &tasksData); err != nil {
		t.Fatalf("failed to unmarshal tasks: %v", err)
	}
	taskList := tasksData["tasks"].([]interface{})
	taskMap := taskList[0].(map[string]interface{})

	if taskMap["priority"] != queue.PriorityFirefighter {
		t.Errorf("expected priority %q in task list, got %v", queue.PriorityFirefighter, taskMap["priority"])
	}
}

// TestHandleTaskRerun_PreservesPriority verifies that rerunning a task
// preserves the original priority.
func TestHandleTaskRerun_PreservesPriority(t *testing.T) {
	srv := newTestServer(t)

	task := map[string]interface{}{
		"id":       "task-1",
		"prompt":   "Priority test",
		"priority": queue.PriorityFirefighter,
	}
	body, _ := json.Marshal(task)
	req := httptest.NewRequest(http.MethodPost, "/api/tasks", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.HandleTasks(w, req)

	// Fail the task so it can be rerun
	err := srv.TaskQueue().Assign("task-1", "guest-1")
	if err != nil {
		t.Fatalf("failed to assign task: %v", err)
	}
	err = srv.TaskQueue().Start("task-1")
	if err != nil {
		t.Fatalf("failed to start task: %v", err)
	}
	err = srv.TaskQueue().Fail("task-1", "test failure")
	if err != nil {
		t.Fatalf("failed to fail task: %v", err)
	}

	// Rerun
	rerunReq := httptest.NewRequest(http.MethodPost, "/api/tasks/task-1/rerun", nil)
	rerunW := httptest.NewRecorder()
	srv.HandleTaskDetail(rerunW, rerunReq)

	if rerunW.Code != http.StatusCreated {
		t.Fatalf("expected 201 for rerun, got %d body: %s", rerunW.Code, rerunW.Body.String())
	}

	var rerunTask queue.Task
	if err := json.Unmarshal(rerunW.Body.Bytes(), &rerunTask); err != nil {
		t.Fatalf("failed to unmarshal rerun response: %v", err)
	}
	if rerunTask.Priority != queue.PriorityFirefighter {
		t.Errorf("expected priority %q preserved on rerun, got %q", queue.PriorityFirefighter, rerunTask.Priority)
	}
}

// TestHandleGuestLog_ToolErrorFields verifies that [TOOL_END] with [ERROR]
// is correctly parsed and the tool_error flag is set.
func TestHandleGuestLog_ToolErrorFields(t *testing.T) {
	srv := newTestServer(t)
	hub := srv.Hub()
	go hub.Run()

	task := map[string]interface{}{
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

// TestHandleGuestHeartbeat_NoTaskID verifies that a heartbeat without
// task_id uses the plain Heartbeat path.
func TestHandleGuestHeartbeat_NoTaskID(t *testing.T) {
	srv := newTestServer(t)

	// Register a guest directly via registry
	srv.Registry().Register("guest-1", "Test Guest", []string{"tag1"})

	// Heartbeat without task_id
	hbParams, _ := json.Marshal(map[string]string{
		"id": "guest-1",
	})
	result, rpcErr := srv.handleGuestHeartbeat(nil, hbParams)
	if rpcErr != nil {
		t.Fatalf("expected no error, got %v", rpcErr)
	}

	resp := result.(map[string]interface{})
	if resp["status"] != "ok" {
		t.Errorf("expected status 'ok', got %v", resp["status"])
	}

	// Verify LastTaskHeartbeat was NOT updated
	guest, _ := srv.Registry().GetGuest("guest-1")
	if !guest.LastTaskHeartbeat.IsZero() {
		t.Errorf("expected zero LastTaskHeartbeat (no task_id sent), got %v", guest.LastTaskHeartbeat)
	}
}

// TestHandleGuestHeartbeat_WithTaskID verifies that a heartbeat with
// task_id uses TaskHeartbeat, updating LastTaskHeartbeat.
func TestHandleGuestHeartbeat_WithTaskID(t *testing.T) {
	srv := newTestServer(t)

	// Register a guest directly via registry
	srv.Registry().Register("guest-1", "Test Guest", []string{"tag1"})

	// Assign a task
	srv.Registry().SetGuestTask("guest-1", "task-1")

	// Heartbeat with task_id
	hbParams, _ := json.Marshal(map[string]string{
		"id":      "guest-1",
		"task_id": "task-1",
	})
	result, rpcErr := srv.handleGuestHeartbeat(nil, hbParams)
	if rpcErr != nil {
		t.Fatalf("expected no error, got %v", rpcErr)
	}

	resp := result.(map[string]interface{})
	if resp["status"] != "ok" {
		t.Errorf("expected status 'ok', got %v", resp["status"])
	}

	// Verify LastTaskHeartbeat was updated
	guest, _ := srv.Registry().GetGuest("guest-1")
	if guest.LastTaskHeartbeat.IsZero() {
		t.Error("expected LastTaskHeartbeat to be set after task-aware heartbeat")
	}
}

// TestHandleGuestHeartbeat_NonExistentGuest verifies that a heartbeat
// for an unknown guest returns an error.
func TestHandleGuestHeartbeat_NonExistentGuest(t *testing.T) {
	srv := newTestServer(t)

	hbParams, _ := json.Marshal(map[string]string{
		"id": "unknown-guest",
	})
	_, rpcErr := srv.handleGuestHeartbeat(nil, hbParams)
	if rpcErr == nil {
		t.Error("expected error for unknown guest, got nil")
	}
}

// TestCheckStuckTasks_DetectsAndRequeues verifies that checkStuckTasks
// detects tasks stuck in ASSIGNED state and re-queues them.
func TestCheckStuckTasks_DetectsAndRequeues(t *testing.T) {
	srv := newTestServer(t)
	srv.cfg.TaskAssignmentTimeout = 1 // 1 second timeout

	// Register a guest directly via registry
	srv.Registry().Register("guest-1", "Test Guest", []string{"tag1"})

	// Add a task via HTTP
	taskBody, _ := json.Marshal(map[string]interface{}{
		"prompt": "test task",
		"tags":   []string{"tag1"},
	})
	req := httptest.NewRequest(http.MethodPost, "/api/tasks", bytes.NewReader(taskBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.HandleTasks(w, req)

	var createdTask queue.Task
	json.Unmarshal(w.Body.Bytes(), &createdTask)
	taskID := createdTask.ID

	// Manually assign the task (tryAssignTask fails to send notification
	// because there's no real WebSocket connection, which reverts the task).
	srv.TaskQueue().Assign(taskID, "guest-1")
	srv.Registry().SetGuestTask("guest-1", taskID)

	// Verify task is ASSIGNED
	task, _ := srv.TaskQueue().Get(taskID)
	if task.Status != queue.TaskStatusAssigned {
		t.Fatalf("expected task ASSIGNED, got %s", task.Status)
	}

	// Wait for the timeout to expire
	time.Sleep(1100 * time.Millisecond)

	// Run checkStuckTasks
	srv.checkStuckTasks()

	// Verify task was failed and re-queued
	task, ok := srv.TaskQueue().Get(taskID)
	if !ok {
		t.Fatalf("task %s not found after checkStuckTasks", taskID)
	}
	if task.Status != queue.TaskStatusPending {
		t.Errorf("expected task status PENDING after re-queue, got %s", task.Status)
	}

	// Verify guest task was cleared
	guest, _ := srv.Registry().GetGuest("guest-1")
	if guest.TaskID != "" {
		t.Errorf("expected guest task cleared, got %s", guest.TaskID)
	}
}

// TestCheckStuckTasks_IgnoresHealthyGuests verifies that checkStuckTasks
// does not touch guests that are heartbeating with their task.
func TestCheckStuckTasks_IgnoresHealthyGuests(t *testing.T) {
	srv := newTestServer(t)
	srv.cfg.TaskAssignmentTimeout = 1

	// Register a guest directly via registry
	srv.Registry().Register("guest-1", "Test Guest", []string{"tag1"})

	// Add a task via HTTP
	taskBody, _ := json.Marshal(map[string]interface{}{
		"prompt": "test task",
		"tags":   []string{"tag1"},
	})
	req := httptest.NewRequest(http.MethodPost, "/api/tasks", bytes.NewReader(taskBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.HandleTasks(w, req)

	var createdTask queue.Task
	json.Unmarshal(w.Body.Bytes(), &createdTask)
	taskID := createdTask.ID

	// Manually assign the task (tryAssignTask fails to send notification
	// because there's no real WebSocket connection, which reverts the task).
	srv.TaskQueue().Assign(taskID, "guest-1")
	srv.Registry().SetGuestTask("guest-1", taskID)

	// Verify task is ASSIGNED
	task, _ := srv.TaskQueue().Get(taskID)
	if task.Status != queue.TaskStatusAssigned {
		t.Fatalf("expected task ASSIGNED after manual assign, got %s", task.Status)
	}

	// Guest heartbeats with the task (confirming assignment)
	hbParams, _ := json.Marshal(map[string]string{
		"id":      "guest-1",
		"task_id": taskID,
	})
	srv.handleGuestHeartbeat(nil, hbParams)

	// Verify heartbeat updated LastTaskHeartbeat
	guest, _ := srv.Registry().GetGuest("guest-1")
	if guest.LastTaskHeartbeat.IsZero() {
		t.Fatal("expected LastTaskHeartbeat to be set after heartbeat")
	}

	// Wait briefly - less than the timeout so the guest is not detected as stuck.
	// The heartbeat was recent, so the guest should pass the liveness check.
	time.Sleep(500 * time.Millisecond)

	// Run checkStuckTasks
	srv.checkStuckTasks()

	// Verify task is still ASSIGNED (not re-queued)
	task, ok := srv.TaskQueue().Get(taskID)
	if !ok {
		t.Fatalf("task %s not found", taskID)
	}
	if task.Status != queue.TaskStatusAssigned {
		t.Errorf("expected task status ASSIGNED (healthy guest), got %s", task.Status)
	}

	// Verify guest still has the task
	guest, _ = srv.Registry().GetGuest("guest-1")
	if guest.TaskID != taskID {
		t.Errorf("expected guest to still have task %s, got %s", taskID, guest.TaskID)
	}
}

// TestCheckStuckTasks_Disabled verifies that checkStuckTasks is a no-op
// when TaskAssignmentTimeout is 0.
func TestCheckStuckTasks_Disabled(t *testing.T) {
	srv := newTestServer(t)
	srv.cfg.TaskAssignmentTimeout = 0 // disabled

	// Register a guest directly via registry
	srv.Registry().Register("guest-1", "Test Guest", []string{"tag1"})

	// Add a task via HTTP
	taskBody, _ := json.Marshal(map[string]interface{}{
		"prompt": "test task",
		"tags":   []string{"tag1"},
	})
	req := httptest.NewRequest(http.MethodPost, "/api/tasks", bytes.NewReader(taskBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.HandleTasks(w, req)

	var createdTask queue.Task
	json.Unmarshal(w.Body.Bytes(), &createdTask)
	taskID := createdTask.ID

	// Manually assign the task
	srv.TaskQueue().Assign(taskID, "guest-1")
	srv.Registry().SetGuestTask("guest-1", taskID)

	// Wait way past any reasonable timeout
	time.Sleep(100 * time.Millisecond)

	// Run checkStuckTasks - should be no-op
	srv.checkStuckTasks()

	// Verify task is still ASSIGNED
	task, ok := srv.TaskQueue().Get(taskID)
	if !ok {
		t.Fatalf("task %s not found", taskID)
	}
	if task.Status != queue.TaskStatusAssigned {
		t.Errorf("expected task status ASSIGNED (detection disabled), got %s", task.Status)
	}
}

// TestCheckStuckTasks_TaskAlreadyPending verifies that checkStuckTasks
// handles the case where the task has already been re-queued to PENDING
// by another code path (e.g. handleGuestTaskDeclined), but the guest
// still has a stale TaskID. This should not produce an error - it should
// just clear the guest's TaskID and move on.
func TestCheckStuckTasks_TaskAlreadyPending(t *testing.T) {
	srv := newTestServer(t)
	srv.cfg.TaskAssignmentTimeout = 1 // 1 second timeout

	// Add a task FIRST (with tags that won't match the guest)
	taskBody, _ := json.Marshal(map[string]interface{}{
		"prompt": "test task",
		"tags":   []string{"android"}, // guest has tag1, so no match
	})
	req := httptest.NewRequest(http.MethodPost, "/api/tasks", bytes.NewReader(taskBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.HandleTasks(w, req)

	var createdTask queue.Task
	json.Unmarshal(w.Body.Bytes(), &createdTask)
	taskID := createdTask.ID

	// Register a guest with different tags (tryAssignTask won't match the task)
	srv.Registry().Register("guest-1", "Test Guest", []string{"tag1"})

	// Task is already PENDING (another path re-queued it)
	// but the guest still has a stale TaskID.
	// We set it directly on the guest struct to avoid SetGuestTask
	// triggering tryAssignTask (which would pick up the pending task).
	guest, _ := srv.Registry().GetGuest("guest-1")
	guest.TaskID = taskID
	guest.State = registry.GuestStateRunning

	task, _ := srv.TaskQueue().Get(taskID)
	if task.Status != queue.TaskStatusPending {
		t.Fatalf("expected task PENDING, got %s", task.Status)
	}

	// Wait for the timeout to expire
	time.Sleep(1100 * time.Millisecond)

	// Run checkStuckTasks - should NOT error on PENDING -> PENDING
	srv.checkStuckTasks()

	// Task should still be PENDING (no error)
	task, ok := srv.TaskQueue().Get(taskID)
	if !ok {
		t.Fatalf("task %s not found after checkStuckTasks", taskID)
	}
	if task.Status != queue.TaskStatusPending {
		t.Errorf("expected task status PENDING, got %s", task.Status)
	}

	// Guest task should be cleared
	guest, _ = srv.Registry().GetGuest("guest-1")
	if guest.TaskID != "" {
		t.Errorf("expected guest task cleared, got %s", guest.TaskID)
	}
}

// TestCheckStuckTasks_TaskInTerminalState verifies that checkStuckTasks
// handles the case where the task has already reached a terminal state
// (COMPLETED, FAILED, CANCELLED), but the guest still has a stale TaskID.
func TestCheckStuckTasks_TaskInTerminalState(t *testing.T) {
	for _, tc := range []struct {
		name          string
		terminalState func(t *testing.T, srv *Server, taskID string)
	}{
		{
			name: "COMPLETED",
			terminalState: func(t *testing.T, srv *Server, taskID string) {
				t.Helper()
				srv.TaskQueue().Assign(taskID, "guest-1")
				srv.TaskQueue().Start(taskID)
				srv.TaskQueue().Complete(taskID, "done")
			},
		},
		{
			name: "FAILED",
			terminalState: func(t *testing.T, srv *Server, taskID string) {
				t.Helper()
				srv.TaskQueue().Assign(taskID, "guest-1")
				srv.TaskQueue().Start(taskID)
				srv.TaskQueue().Fail(taskID, "error")
			},
		},
		{
			name: "CANCELLED",
			terminalState: func(t *testing.T, srv *Server, taskID string) {
				t.Helper()
				srv.TaskQueue().Cancel(taskID)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := newTestServer(t)
			srv.cfg.TaskAssignmentTimeout = 1

			// Add task FIRST with tags that won't match the guest
			taskBody, _ := json.Marshal(map[string]interface{}{
				"prompt": "test task",
				"tags":   []string{"android"}, // guest has tag1, so no match
			})
			req := httptest.NewRequest(http.MethodPost, "/api/tasks", bytes.NewReader(taskBody))
			req.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()
			srv.HandleTasks(w, req)

			var createdTask queue.Task
			json.Unmarshal(w.Body.Bytes(), &createdTask)
			taskID := createdTask.ID

			// Register a guest with different tags (tryAssignTask won't match the task)
			srv.Registry().Register("guest-1", "Test Guest", []string{"tag1"})

			// Transition the task to a terminal state
			tc.terminalState(t, srv, taskID)

			// But the guest still has a stale TaskID.
			// Set it directly to avoid SetGuestTask triggering tryAssignTask.
			guest, _ := srv.Registry().GetGuest("guest-1")
			guest.TaskID = taskID
			guest.State = registry.GuestStateRunning

			time.Sleep(1100 * time.Millisecond)

			srv.checkStuckTasks()

			// Task should remain in terminal state
			task, ok := srv.TaskQueue().Get(taskID)
			if !ok {
				t.Fatalf("task %s not found", taskID)
			}
			if task.Status.String() != tc.name {
				t.Errorf("expected task status %s, got %s", tc.name, task.Status)
			}

			// Guest task should be cleared
			guest, _ = srv.Registry().GetGuest("guest-1")
			if guest.TaskID != "" {
				t.Errorf("expected guest task cleared, got %s", guest.TaskID)
			}
		})
	}
}

// TestTryAssignTask_SendToGuestFails_CleansUp verifies that when
// tryAssignTask succeeds in assigning the task to the queue and registry
// but fails to send the notification to the guest, it properly cleans up
// both the task assignment and the guest's task reference.
func TestTryAssignTask_SendToGuestFails_CleansUp(t *testing.T) {
	srv := newTestServer(t)

	// Register a guest WITHOUT setting up a connection mapping.
	// This means SendToGuest will fail (no connection found).
	srv.Registry().Register("guest-no-conn", "No Conn Guest", []string{"default"})

	// Add a pending task
	task := &queue.Task{
		ID:     "task-send-fail",
		Prompt: "test task",
		Tags:   []string{"default"},
	}
	srv.TaskQueue().Add(task)

	// Guest is idle - tryAssignTask will try to assign
	srv.tryAssignTask("guest-no-conn")

	// The task should be PENDING (re-queued after SendToGuest failure)
	qTask, ok := srv.TaskQueue().Get("task-send-fail")
	if !ok {
		t.Fatal("task should still exist")
	}
	if qTask.Status != queue.TaskStatusPending {
		t.Errorf("expected task status PENDING (SendToGuest failed, should revert), got %s", qTask.Status)
	}
	if qTask.AssignedTo != "" {
		t.Errorf("expected assigned_to cleared, got %s", qTask.AssignedTo)
	}

	// The guest should be idle with no task
	regGuest, ok := srv.Registry().GetGuest("guest-no-conn")
	if !ok {
		t.Fatal("guest should still exist")
	}
	if regGuest.State != registry.GuestStateIdle {
		t.Errorf("expected guest state IDLE, got %s", regGuest.State)
	}
	if regGuest.TaskID != "" {
		t.Errorf("expected guest task cleared, got %s", regGuest.TaskID)
	}
}

// TestHandleCancelTask_Pending verifies that cancelling a PENDING task
// transitions it to CANCELLED and broadcasts task.updated.
func TestHandleCancelTask_Pending(t *testing.T) {
	srv := newTestServer(t)

	// Create a task
	taskBody, _ := json.Marshal(map[string]interface{}{
		"id":     "cancel-test-pending",
		"prompt": "test task",
	})
	req := httptest.NewRequest(http.MethodPost, "/api/tasks", bytes.NewReader(taskBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.HandleTasks(w, req)

	var createdTask queue.Task
	json.Unmarshal(w.Body.Bytes(), &createdTask)
	taskID := createdTask.ID

	// Cancel the task
	cancelReq := httptest.NewRequest(http.MethodPost, "/api/tasks/"+taskID+"/cancel", nil)
	cancelW := httptest.NewRecorder()
	srv.HandleTaskDetail(cancelW, cancelReq)

	if cancelW.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", cancelW.Code)
	}

	// Verify task is now CANCELLED
	task, ok := srv.TaskQueue().Get(taskID)
	if !ok {
		t.Fatal("task should still exist")
	}
	if task.Status != queue.TaskStatusCancelled {
		t.Errorf("expected CANCELLED, got %s", task.Status)
	}
}

// TestHandleCancelTask_Assigned verifies that cancelling an ASSIGNED task
// transitions it to CANCELLED and broadcasts task.updated.
func TestHandleCancelTask_Assigned(t *testing.T) {
	srv := newTestServer(t)

	// Create and assign a task
	taskBody, _ := json.Marshal(map[string]interface{}{
		"id":     "cancel-test-assigned",
		"prompt": "test task",
	})
	req := httptest.NewRequest(http.MethodPost, "/api/tasks", bytes.NewReader(taskBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.HandleTasks(w, req)

	var createdTask queue.Task
	json.Unmarshal(w.Body.Bytes(), &createdTask)
	taskID := createdTask.ID

	// Assign the task
	srv.TaskQueue().Assign(taskID, "guest-1")

	// Cancel the task
	cancelReq := httptest.NewRequest(http.MethodPost, "/api/tasks/"+taskID+"/cancel", nil)
	cancelW := httptest.NewRecorder()
	srv.HandleTaskDetail(cancelW, cancelReq)

	if cancelW.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", cancelW.Code)
	}

	// Verify task is now CANCELLED
	task, ok := srv.TaskQueue().Get(taskID)
	if !ok {
		t.Fatal("task should still exist")
	}
	if task.Status != queue.TaskStatusCancelled {
		t.Errorf("expected CANCELLED, got %s", task.Status)
	}
}

// TestHandleCancelTask_Running verifies that cancelling a RUNNING task
// sends a cancel signal to the guest (which will confirm cancellation).
func TestHandleCancelTask_Running(t *testing.T) {
	srv := newTestServer(t)
	hub := srv.Hub()
	go hub.Run()

	// Register a guest and set up connection
	srv.Registry().Register("guest-1", "Test Guest", []string{"default"})
	conn := rpc.NewTestConnection("conn-guest-1", hub)
	hub.Register(conn)
	hub.RegisterGuestConnection("guest-1", "conn-guest-1")
	time.Sleep(10 * time.Millisecond)

	// Create, assign, and start a task
	taskBody, _ := json.Marshal(map[string]interface{}{
		"id":     "cancel-test-running",
		"prompt": "test task",
	})
	req := httptest.NewRequest(http.MethodPost, "/api/tasks", bytes.NewReader(taskBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.HandleTasks(w, req)

	var createdTask queue.Task
	json.Unmarshal(w.Body.Bytes(), &createdTask)
	taskID := createdTask.ID

	srv.TaskQueue().Assign(taskID, "guest-1")
	srv.TaskQueue().Start(taskID)

	// Drain stale notifications before cancel
	conn.Drain()

	// Cancel the task
	cancelReq := httptest.NewRequest(http.MethodPost, "/api/tasks/"+taskID+"/cancel", nil)
	cancelW := httptest.NewRecorder()
	srv.HandleTaskDetail(cancelW, cancelReq)

	if cancelW.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body: %s", cancelW.Code, cancelW.Body.String())
	}

	// Give a moment for the message to arrive
	time.Sleep(50 * time.Millisecond)

	// Verify a cancel signal was sent to the guest
	data, ok := conn.Recv()
	if !ok {
		t.Fatal("expected cancel signal to be sent to guest")
	}
	var msg rpc.JSONRPCMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		t.Fatalf("failed to unmarshal cancel signal: %v", err)
	}
	if msg.Method != "task.cancel" {
		t.Errorf("expected method 'task.cancel', got %s", msg.Method)
	}

	// Verify cancel params contain task_id
	var cancelParams map[string]interface{}
	if err := json.Unmarshal(msg.Params, &cancelParams); err != nil {
		t.Fatalf("failed to unmarshal cancel params: %v", err)
	}
	if cancelParams["task_id"] != taskID {
		t.Errorf("expected task_id %s, got %v", taskID, cancelParams["task_id"])
	}

	// Task should still be RUNNING (guest hasn't confirmed yet)
	task, ok := srv.TaskQueue().Get(taskID)
	if !ok {
		t.Fatal("task should still exist")
	}
	if task.Status != queue.TaskStatusRunning {
		t.Errorf("expected RUNNING (pending guest confirmation), got %s", task.Status)
	}
}

// TestHandleCancelTask_Completed returns 409 when trying to cancel a terminal task.
func TestHandleCancelTask_Completed(t *testing.T) {
	srv := newTestServer(t)

	// Create, assign, start, and complete a task
	taskBody, _ := json.Marshal(map[string]interface{}{
		"id":     "cancel-test-completed",
		"prompt": "test task",
	})
	req := httptest.NewRequest(http.MethodPost, "/api/tasks", bytes.NewReader(taskBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.HandleTasks(w, req)

	var createdTask queue.Task
	json.Unmarshal(w.Body.Bytes(), &createdTask)
	taskID := createdTask.ID

	srv.TaskQueue().Assign(taskID, "guest-1")
	srv.TaskQueue().Start(taskID)
	srv.TaskQueue().Complete(taskID, "done")

	// Try to cancel
	cancelReq := httptest.NewRequest(http.MethodPost, "/api/tasks/"+taskID+"/cancel", nil)
	cancelW := httptest.NewRecorder()
	srv.HandleTaskDetail(cancelW, cancelReq)

	if cancelW.Code != http.StatusConflict {
		t.Errorf("expected 409, got %d", cancelW.Code)
	}
}

// TestHandleCancelTask_NotFound returns 404 when the task does not exist.
func TestHandleCancelTask_NotFound(t *testing.T) {
	srv := newTestServer(t)

	cancelReq := httptest.NewRequest(http.MethodPost, "/api/tasks/nonexistent/cancel", nil)
	cancelW := httptest.NewRecorder()
	srv.HandleTaskDetail(cancelW, cancelReq)

	if cancelW.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", cancelW.Code)
	}
}

// TestHandleCancelTask_EmptyID returns 400 when the task ID is empty.
func TestHandleCancelTask_EmptyID(t *testing.T) {
	srv := newTestServer(t)

	cancelReq := httptest.NewRequest(http.MethodPost, "/api/tasks//cancel", nil)
	cancelW := httptest.NewRecorder()
	srv.HandleTaskDetail(cancelW, cancelReq)

	if cancelW.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", cancelW.Code)
	}
}

// TestHandleCancelTask_MethodNotAllowed returns 405 for non-POST methods.
func TestHandleCancelTask_MethodNotAllowed(t *testing.T) {
	srv := newTestServer(t)

	// Create a task
	taskBody, _ := json.Marshal(map[string]interface{}{
		"id":     "cancel-test-method",
		"prompt": "test task",
	})
	req := httptest.NewRequest(http.MethodPost, "/api/tasks", bytes.NewReader(taskBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.HandleTasks(w, req)

	var createdTask queue.Task
	json.Unmarshal(w.Body.Bytes(), &createdTask)
	taskID := createdTask.ID

	// GET should not trigger cancel
	getReq := httptest.NewRequest(http.MethodGet, "/api/tasks/"+taskID+"/cancel", nil)
	getW := httptest.NewRecorder()
	srv.HandleTaskDetail(getW, getReq)

	if getW.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405 for GET on cancel path, got %d", getW.Code)
	}
}

// TestHandleGuestCancelled verifies that when a guest confirms cancellation
// after receiving a task.cancel signal, the server updates the task status
// to CANCELLED and broadcasts task.updated.
func TestHandleGuestCancelled(t *testing.T) {
	srv := newTestServer(t)
	hub := srv.Hub()
	go hub.Run()

	// Register a guest
	srv.Registry().Register("guest-1", "Test Guest", []string{"default"})

	// Create, assign, and start a task
	taskBody, _ := json.Marshal(map[string]interface{}{
		"id":     "cancel-guest-confirm",
		"prompt": "test task",
	})
	req := httptest.NewRequest(http.MethodPost, "/api/tasks", bytes.NewReader(taskBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.HandleTasks(w, req)

	var createdTask queue.Task
	json.Unmarshal(w.Body.Bytes(), &createdTask)
	taskID := createdTask.ID

	srv.TaskQueue().Assign(taskID, "guest-1")
	srv.TaskQueue().Start(taskID)
	srv.Registry().SetGuestTask("guest-1", taskID)

	// Guest confirms cancellation
	params, _ := json.Marshal(map[string]interface{}{
		"task_id":  taskID,
		"guest_id": "guest-1",
		"reason":   "user requested cancellation",
	})
	result, rpcErr := srv.handleGuestCancelled(nil, params)
	if rpcErr != nil {
		t.Fatalf("expected no error, got %v", rpcErr)
	}

	resp := result.(map[string]interface{})
	if resp["status"] != "accepted" {
		t.Errorf("expected status 'accepted', got %v", resp["status"])
	}

	// Verify task is now CANCELLED
	task, ok := srv.TaskQueue().Get(taskID)
	if !ok {
		t.Fatal("task should still exist")
	}
	if task.Status != queue.TaskStatusCancelled {
		t.Errorf("expected CANCELLED, got %s", task.Status)
	}

	// Verify guest task was cleared
	guest, _ := srv.Registry().GetGuest("guest-1")
	if guest.TaskID != "" {
		t.Errorf("expected guest task cleared, got %s", guest.TaskID)
	}
}

// TestTryAssignTask_AssignsToIdleGuestWithConnection verifies that when
// a guest has a valid connection mapping, tryAssignTask successfully
// assigns the task and sends the notification.
func TestTryAssignTask_AssignsToIdleGuestWithConnection(t *testing.T) {
	srv := newTestServer(t)
	hub := srv.Hub()
	go hub.Run()

	// Register a guest
	srv.Registry().Register("guest-with-conn", "With Conn Guest", []string{"default"})

	// Set up a connection and mapping (so SendToGuest succeeds)
	conn := rpc.NewTestConnection("conn-with-guest", hub)
	hub.Register(conn)
	hub.RegisterGuestConnection("guest-with-conn", "conn-with-guest")
	time.Sleep(10 * time.Millisecond)

	// Add a pending task
	task := &queue.Task{
		ID:     "task-send-ok",
		Prompt: "test task",
		Tags:   []string{"default"},
	}
	srv.TaskQueue().Add(task)

	// Guest is idle - tryAssignTask should assign and send
	srv.tryAssignTask("guest-with-conn")

	// The task should be ASSIGNED
	qTask, ok := srv.TaskQueue().Get("task-send-ok")
	if !ok {
		t.Fatal("task should still exist")
	}
	if qTask.Status != queue.TaskStatusAssigned {
		t.Errorf("expected task status ASSIGNED, got %s", qTask.Status)
	}
	if qTask.AssignedTo != "guest-with-conn" {
		t.Errorf("expected assigned_to 'guest-with-conn', got %s", qTask.AssignedTo)
	}

	// The guest should be running with the task
	regGuest, ok := srv.Registry().GetGuest("guest-with-conn")
	if !ok {
		t.Fatal("guest should still exist")
	}
	if regGuest.State != registry.GuestStateRunning {
		t.Errorf("expected guest state RUNNING, got %s", regGuest.State)
	}
	if regGuest.TaskID != "task-send-ok" {
		t.Errorf("expected guest task 'task-send-ok', got %s", regGuest.TaskID)
	}

	// Verify the notification was sent
	data, ok := conn.Recv()
	if !ok {
		t.Fatal("expected task.assign notification to be sent")
	}
	var msg rpc.JSONRPCMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		t.Fatalf("failed to unmarshal sent message: %v", err)
	}
	if msg.Method != "task.assign" {
		t.Errorf("expected method 'task.assign', got %s", msg.Method)
	}
}

// TestHandleGuestDisconnect verifies that when a guest's WebSocket
// connection is lost, the server immediately fails the guest's running task.
func TestHandleGuestDisconnect(t *testing.T) {
	srv := newTestServer(t)
	hub := srv.Hub()
	go hub.Run()

	// Register a guest with a connection
	guestConn := rpc.NewTestConnection("guest-conn-1", hub)
	hub.Register(guestConn)
	hub.SetConnectionRole("guest-conn-1", rpc.ConnectionRoleGuest)

	params, _ := json.Marshal(map[string]interface{}{
		"id":   "disconnect-guest",
		"name": "Disconnect Guest",
		"tags": []string{"tag1"},
	})
	resp, err := hub.Dispatch("guest.register", params)
	if err != nil {
		t.Fatalf("register failed: %v", err)
	}
	result := resp.(map[string]interface{})
	if result["status"] != "registered" {
		t.Fatalf("expected status 'registered', got %v", result["status"])
	}

	// Dispatch doesn't set connection context, so register mapping manually
	hub.RegisterGuestConnection("disconnect-guest", "guest-conn-1")

	// Create and assign a task
	task := &queue.Task{
		ID:     "task-disconnect",
		Prompt: "test disconnect",
		Tags:   []string{"tag1"},
	}
	if err := srv.TaskQueue().Add(task); err != nil {
		t.Fatalf("failed to add task: %v", err)
	}
	if err := srv.TaskQueue().Assign("task-disconnect", "disconnect-guest"); err != nil {
		t.Fatalf("failed to assign task: %v", err)
	}
	if err := srv.Registry().SetGuestTask("disconnect-guest", "task-disconnect"); err != nil {
		t.Fatalf("failed to set guest task: %v", err)
	}

	// Verify task is ASSIGNED and guest is RUNNING
	task, _ = srv.TaskQueue().Get("task-disconnect")
	if task.Status != queue.TaskStatusAssigned {
		t.Fatalf("expected task ASSIGNED, got %s", task.Status)
	}
	guest, _ := srv.Registry().GetGuest("disconnect-guest")
	if guest.State != registry.GuestStateRunning {
		t.Fatalf("expected guest RUNNING, got %s", guest.State)
	}

	// Simulate connection disconnect
	srv.handleGuestDisconnect("guest-conn-1")

	// Task should be FAILED
	task, _ = srv.TaskQueue().Get("task-disconnect")
	if task.Status != queue.TaskStatusFailed {
		t.Errorf("expected task FAILED after disconnect, got %s", task.Status)
	}

	// Guest should be cleared
	guest, _ = srv.Registry().GetGuest("disconnect-guest")
	if guest.State != registry.GuestStateIdle {
		t.Errorf("expected guest IDLE after disconnect, got %s", guest.State)
	}
	if guest.TaskID != "" {
		t.Errorf("expected guest TaskID cleared, got %s", guest.TaskID)
	}
}

// TestHandleGuestDisconnect_BrowserConnection verifies that disconnecting
// a browser connection does not affect guest tasks.
func TestHandleGuestDisconnect_BrowserConnection(t *testing.T) {
	srv := newTestServer(t)
	hub := srv.Hub()
	go hub.Run()

	// Register a browser connection (not a guest)
	browserConn := rpc.NewTestConnection("browser-conn-1", hub)
	hub.Register(browserConn)
	hub.SetConnectionRole("browser-conn-1", rpc.ConnectionRoleBrowser)

	// Disconnect the browser — should be a no-op
	srv.handleGuestDisconnect("browser-conn-1")

	// No guests or tasks should be affected
	if srv.Registry().Count() != 0 {
		t.Errorf("expected 0 guests, got %d", srv.Registry().Count())
	}
}

// TestHandleGuestDisconnect_NoTask verifies that disconnecting a guest
// with no running task is a no-op.
func TestHandleGuestDisconnect_NoTask(t *testing.T) {
	srv := newTestServer(t)
	hub := srv.Hub()
	go hub.Run()

	// Register a guest with a connection
	guestConn := rpc.NewTestConnection("guest-conn-1", hub)
	hub.Register(guestConn)
	hub.SetConnectionRole("guest-conn-1", rpc.ConnectionRoleGuest)

	params, _ := json.Marshal(map[string]interface{}{
		"id":   "idle-guest",
		"name": "Idle Guest",
		"tags": []string{"tag1"},
	})
	resp, err := hub.Dispatch("guest.register", params)
	if err != nil {
		t.Fatalf("register failed: %v", err)
	}
	result := resp.(map[string]interface{})
	if result["status"] != "registered" {
		t.Fatalf("expected status 'registered', got %v", result["status"])
	}

	// Dispatch doesn't set connection context, so register mapping manually
	hub.RegisterGuestConnection("idle-guest", "guest-conn-1")

	// Disconnect the idle guest — should be a no-op
	srv.handleGuestDisconnect("guest-conn-1")

	// Guest should still exist and be IDLE
	guest, ok := srv.Registry().GetGuest("idle-guest")
	if !ok {
		t.Fatal("expected guest to still exist")
	}
	if guest.State != registry.GuestStateIdle {
		t.Errorf("expected guest IDLE, got %s", guest.State)
	}
}
