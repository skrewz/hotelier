package agent

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"hotelier/pkg/config"
)

func newTestAgent(t *testing.T) *Agent {
	t.Helper()
	cfg := config.AgentConfig{
		ID:   "test-agent-1",
		Name: "Test Agent",
		Tags: []string{"business-default", "frontend"},
	}
	handler := func(ctx context.Context, task TaskAssignment, _ LogCallback) (*TaskResult, error) {
		return &TaskResult{TaskID: task.TaskID, Success: true, Output: "test result"}, nil
	}
	return New(cfg, handler)
}

func TestNewAgent(t *testing.T) {
	ag := newTestAgent(t)
	if ag == nil {
		t.Fatal("expected non-nil agent")
	}
	if !strings.HasPrefix(ag.id, "agent-") {
		t.Errorf("expected ephemeral id starting with 'agent-', got %s", ag.id)
	}
	if len(ag.tags) != 2 {
		t.Errorf("expected 2 tags, got %d", len(ag.tags))
	}
}

func TestAgentConfig(t *testing.T) {
	cfg := config.AgentConfig{ID: "a2", TaskTimeout: 600, HeartbeatInterval: 10}
	ag := New(cfg, func(ctx context.Context, task TaskAssignment, _ LogCallback) (*TaskResult, error) {
		return &TaskResult{TaskID: task.TaskID, Success: true}, nil
	})
	if ag.config.TaskTimeout != 600 {
		t.Errorf("expected task_timeout 600, got %d", ag.config.TaskTimeout)
	}
}

func TestTaskAssignmentMarshal(t *testing.T) {
	task := TaskAssignment{TaskID: "t1", Repos: []string{"/repo1"}, Prompt: "Build", Tags: []string{"tag"}}
	data, err := json.Marshal(task)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	var parsed TaskAssignment
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if parsed.TaskID != "t1" {
		t.Error("round-trip failed")
	}
}

func TestTaskResultMarshal(t *testing.T) {
	result := TaskResult{TaskID: "t1", Success: true, Output: "ok"}
	data, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	var parsed TaskResult
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if parsed.TaskID != "t1" || !parsed.Success {
		t.Error("round-trip failed")
	}
}

func TestTaskResultFailureMarshal(t *testing.T) {
	result := TaskResult{TaskID: "t1", Success: false, Error: "fail"}
	data, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	var parsed TaskResult
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if parsed.Success {
		t.Error("round-trip failed")
	}
}

func TestLogEntryMarshal(t *testing.T) {
	entry := LogEntry{TaskID: "t1", Line: "test", Level: "info", Timestamp: time.Now()}
	data, err := json.Marshal(entry)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	var parsed LogEntry
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if parsed.TaskID != "t1" {
		t.Error("round-trip failed")
	}
}

func TestTaskCancelMarshal(t *testing.T) {
	cancel := TaskCancel{TaskID: "t1", Reason: "timeout"}
	data, err := json.Marshal(cancel)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	var parsed TaskCancel
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if parsed.TaskID != "t1" {
		t.Error("round-trip failed")
	}
}

// Additional agent tests for coverage
func TestNewAgentWithConfig(t *testing.T) {
	cfg := config.AgentConfig{
		ID:                "test-agent",
		Name:              "Test Agent",
		Tags:              []string{"business-default", "android"},
		TaskTimeout:       1800,
		HeartbeatInterval: 15,
		WorkingDir:        "/tmp/test",
		LogLevel:          "debug",
	}
	handler := func(ctx context.Context, task TaskAssignment, _ LogCallback) (*TaskResult, error) {
		return &TaskResult{TaskID: task.TaskID, Success: true, Output: "done"}, nil
	}
	ag := New(cfg, handler)
	if !strings.HasPrefix(ag.id, "agent-") {
		t.Errorf("expected ephemeral id starting with 'agent-', got %s", ag.id)
	}
	if ag.config.TaskTimeout != 1800 {
		t.Errorf("expected task_timeout 1800, got %d", ag.config.TaskTimeout)
	}
	if ag.config.HeartbeatInterval != 15 {
		t.Errorf("expected heartbeat_interval 15, got %d", ag.config.HeartbeatInterval)
	}
}

func TestNewAgentWithEmptyTags(t *testing.T) {
	cfg := config.AgentConfig{ID: "test-agent", Name: "Test Agent", Tags: []string{}}
	handler := func(ctx context.Context, task TaskAssignment, _ LogCallback) (*TaskResult, error) {
		return &TaskResult{TaskID: task.TaskID, Success: true}, nil
	}
	ag := New(cfg, handler)
	if len(ag.tags) != 0 {
		t.Errorf("expected 0 tags, got %d", len(ag.tags))
	}
}

func TestTaskAssignmentWithMultipleRepos(t *testing.T) {
	task := TaskAssignment{
		TaskID: "t1",
		Repos:  []string{"/repo1", "/repo2", "/repo3"},
		Prompt: "Build feature",
		Tags:   []string{"business-default", "frontend"},
	}
	data, err := json.Marshal(task)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	var parsed TaskAssignment
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if len(parsed.Repos) != 3 {
		t.Errorf("expected 3 repos, got %d", len(parsed.Repos))
	}
	if len(parsed.Tags) != 2 {
		t.Errorf("expected 2 tags, got %d", len(parsed.Tags))
	}
}

func TestTaskResultWithEmptyOutput(t *testing.T) {
	result := TaskResult{TaskID: "t1", Success: true, Output: ""}
	data, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	var parsed TaskResult
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if !parsed.Success {
		t.Error("expected success")
	}
}

func TestLogEntryWithLevel(t *testing.T) {
	entry := LogEntry{TaskID: "t1", Line: "error message", Level: "error", Timestamp: time.Now()}
	data, err := json.Marshal(entry)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	var parsed LogEntry
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if parsed.Level != "error" {
		t.Errorf("expected level error, got %s", parsed.Level)
	}
}

func TestLogEntryWithoutLevel(t *testing.T) {
	entry := LogEntry{TaskID: "t1", Line: "info message", Timestamp: time.Now()}
	data, err := json.Marshal(entry)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	var parsed LogEntry
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if parsed.Level != "" {
		t.Errorf("expected empty level, got %s", parsed.Level)
	}
}

func TestTaskCancelWithoutReason(t *testing.T) {
	cancel := TaskCancel{TaskID: "t1"}
	data, err := json.Marshal(cancel)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	var parsed TaskCancel
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if parsed.TaskID != "t1" {
		t.Error("round-trip failed")
	}
}

func TestTaskAssignmentWithEmptyPrompt(t *testing.T) {
	task := TaskAssignment{TaskID: "t1", Repos: []string{"/repo1"}, Prompt: "", Tags: []string{}}
	data, err := json.Marshal(task)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	var parsed TaskAssignment
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if parsed.Prompt != "" {
		t.Error("round-trip failed")
	}
}

// TestLogCallbackSendsLine verifies that a non-nil callback receives the log line.
func TestLogCallbackSendsLine(t *testing.T) {
	var mu sync.Mutex
	var lines []string
	cb := func(taskID, line string) error {
		mu.Lock()
		defer mu.Unlock()
		lines = append(lines, line)
		return nil
	}

	err := cb("task-1", "hello world")
	if err != nil {
		t.Fatalf("callback returned error: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(lines) != 1 {
		t.Fatalf("expected 1 line, got %d", len(lines))
	}
	if lines[0] != "hello world" {
		t.Errorf("expected 'hello world', got %q", lines[0])
	}
}

// TestAgentStop verifies that Stop can be called safely.
func TestAgentStop(t *testing.T) {
	cfg := config.AgentConfig{ID: "test-agent", Name: "Test Agent", Tags: []string{"test"}}
	handler := func(ctx context.Context, task TaskAssignment, _ LogCallback) (*TaskResult, error) {
		return &TaskResult{TaskID: task.TaskID, Success: true}, nil
	}
	ag := New(cfg, handler)
	ag.Stop()

	// Second Stop should be a no-op
	ag.Stop()
}

// TestAgentStopConcurrent verifies that concurrent Stop calls are safe.
func TestAgentStopConcurrent(t *testing.T) {
	cfg := config.AgentConfig{ID: "test-agent", Name: "Test Agent", Tags: []string{"test"}}
	handler := func(ctx context.Context, task TaskAssignment, _ LogCallback) (*TaskResult, error) {
		return &TaskResult{TaskID: task.TaskID, Success: true}, nil
	}
	ag := New(cfg, handler)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ag.Stop()
		}()
	}
	wg.Wait()
}
