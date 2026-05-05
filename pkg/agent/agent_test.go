package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
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

func TestDefaultHandlerSuccess(t *testing.T) {
	result, err := DefaultHandler(context.Background(), TaskAssignment{TaskID: "t1", Prompt: "echo hello"}, nil)
	if err != nil {
		t.Fatalf("failed: %v", err)
	}
	if !result.Success {
		t.Error("expected success")
	}
}

func TestDefaultHandlerOutput(t *testing.T) {
	result, err := DefaultHandler(context.Background(), TaskAssignment{TaskID: "t1", Prompt: "echo 'test output'"}, nil)
	if err != nil || result.Output == "" {
		t.Fatal("expected non-empty output")
	}
}

func TestDefaultHandlerFailure(t *testing.T) {
	result, err := DefaultHandler(context.Background(), TaskAssignment{TaskID: "t1", Prompt: "exit 1"}, nil)
	if err != nil {
		t.Fatalf("failed: %v", err)
	}
	if result.Success {
		t.Error("expected failure")
	}
	if result.Error == "" {
		t.Error("expected error message")
	}
}

func TestDefaultHandlerWithRepo(t *testing.T) {
	result, err := DefaultHandler(context.Background(), TaskAssignment{TaskID: "t1", Repos: []string{"/tmp"}, Prompt: "ls /tmp"}, nil)
	if err != nil || !result.Success {
		t.Fatal("expected success")
	}
}

func TestDefaultHandlerPipe(t *testing.T) {
	result, err := DefaultHandler(context.Background(), TaskAssignment{TaskID: "t1", Prompt: "echo hello | wc -w"}, nil)
	if err != nil || !result.Success {
		t.Fatal("expected success")
	}
}

func TestDefaultHandlerLoop(t *testing.T) {
	result, err := DefaultHandler(context.Background(), TaskAssignment{TaskID: "t1", Prompt: "for i in 1 2 3; do echo $i; done"}, nil)
	if err != nil || !result.Success {
		t.Fatal("expected success")
	}
}

func TestDefaultHandlerConditional(t *testing.T) {
	result, err := DefaultHandler(context.Background(), TaskAssignment{TaskID: "t1", Prompt: "if [ 1 -eq 1 ]; then echo 'equal'; fi"}, nil)
	if err != nil || !result.Success {
		t.Fatal("expected success")
	}
}

func TestDefaultHandlerEmptyPrompt(t *testing.T) {
	result, err := DefaultHandler(context.Background(), TaskAssignment{TaskID: "t1", Prompt: ""}, nil)
	if err != nil || !result.Success {
		t.Fatal("expected success")
	}
}

func TestDefaultHandlerLargeOutput(t *testing.T) {
	result, err := DefaultHandler(context.Background(), TaskAssignment{TaskID: "t1", Prompt: "for i in $(seq 1 100); do echo \"line $i\"; done"}, nil)
	if err != nil || !result.Success || result.Output == "" {
		t.Fatal("expected non-empty output")
	}
}

func TestDefaultHandlerWithEnvVars(t *testing.T) {
	result, err := DefaultHandler(context.Background(), TaskAssignment{TaskID: "t1", Prompt: "export V=hello && echo $V"}, nil)
	if err != nil || !result.Success {
		t.Fatal("expected success")
	}
}

func TestDefaultHandlerWithSubshell(t *testing.T) {
	result, err := DefaultHandler(context.Background(), TaskAssignment{TaskID: "t1", Prompt: "echo $(echo nested)"}, nil)
	if err != nil || !result.Success {
		t.Fatal("expected success")
	}
}

func TestDefaultHandlerWithFunction(t *testing.T) {
	result, err := DefaultHandler(context.Background(), TaskAssignment{TaskID: "t1", Prompt: "myfunc() { echo 'hello'; }; myfunc"}, nil)
	if err != nil || !result.Success {
		t.Fatal("expected success")
	}
}

func TestDefaultHandlerWithArithmetic(t *testing.T) {
	result, err := DefaultHandler(context.Background(), TaskAssignment{TaskID: "t1", Prompt: "echo $((2 + 3))"}, nil)
	if err != nil || !result.Success {
		t.Fatal("expected success")
	}
}

func TestDefaultHandlerWithGrep(t *testing.T) {
	result, err := DefaultHandler(context.Background(), TaskAssignment{TaskID: "t1", Prompt: "echo 'hello world' | grep 'world'"}, nil)
	if err != nil || !result.Success {
		t.Fatal("expected success")
	}
}

func TestDefaultHandlerWithDate(t *testing.T) {
	result, err := DefaultHandler(context.Background(), TaskAssignment{TaskID: "t1", Prompt: "date +%Y-%m-%d"}, nil)
	if err != nil || !result.Success {
		t.Fatal("expected success")
	}
}

func TestDefaultHandlerWithMkdir(t *testing.T) {
	result, err := DefaultHandler(context.Background(), TaskAssignment{TaskID: "t1", Prompt: "mkdir -p /tmp/hotelier-test && rmdir /tmp/hotelier-test"}, nil)
	if err != nil || !result.Success {
		t.Fatal("expected success")
	}
}

func TestDefaultHandlerWithTouch(t *testing.T) {
	result, err := DefaultHandler(context.Background(), TaskAssignment{TaskID: "t1", Prompt: "touch /tmp/hotelier-test-file && rm /tmp/hotelier-test-file"}, nil)
	if err != nil || !result.Success {
		t.Fatal("expected success")
	}
}

func TestDefaultHandlerWithCat(t *testing.T) {
	result, err := DefaultHandler(context.Background(), TaskAssignment{TaskID: "t1", Prompt: "echo 'test' > /tmp/hotelier-cat-test.txt && cat /tmp/hotelier-cat-test.txt && rm /tmp/hotelier-cat-test.txt"}, nil)
	if err != nil || !result.Success {
		t.Fatal("expected success")
	}
}

func TestDefaultHandlerWithHead(t *testing.T) {
	result, err := DefaultHandler(context.Background(), TaskAssignment{TaskID: "t1", Prompt: "echo -e 'line1\\nline2\\nline3' | head -2"}, nil)
	if err != nil || !result.Success {
		t.Fatal("expected success")
	}
}

func TestDefaultHandlerWithTail(t *testing.T) {
	result, err := DefaultHandler(context.Background(), TaskAssignment{TaskID: "t1", Prompt: "echo -e 'line1\\nline2\\nline3' | tail -1"}, nil)
	if err != nil || !result.Success {
		t.Fatal("expected success")
	}
}

func TestDefaultHandlerWithSort(t *testing.T) {
	result, err := DefaultHandler(context.Background(), TaskAssignment{TaskID: "t1", Prompt: "echo -e 'c\\na\\nb' | sort"}, nil)
	if err != nil || !result.Success {
		t.Fatal("expected success")
	}
}

func TestDefaultHandlerWithTr(t *testing.T) {
	result, err := DefaultHandler(context.Background(), TaskAssignment{TaskID: "t1", Prompt: "echo 'hello' | tr 'a-z' 'A-Z'"}, nil)
	if err != nil || !result.Success {
		t.Fatal("expected success")
	}
}

func TestDefaultHandlerWithPrintf(t *testing.T) {
	result, err := DefaultHandler(context.Background(), TaskAssignment{TaskID: "t1", Prompt: "printf '%s %s\\n' 'hello' 'world'"}, nil)
	if err != nil || !result.Success {
		t.Fatal("expected success")
	}
}

func TestDefaultHandlerWithHereDoc(t *testing.T) {
	result, err := DefaultHandler(context.Background(), TaskAssignment{TaskID: "t1", Prompt: "cat <<EOF\nhello\nworld\nEOF"}, nil)
	if err != nil || !result.Success {
		t.Fatal("expected success")
	}
}

func TestDefaultHandlerWithSetE(t *testing.T) {
	result, err := DefaultHandler(context.Background(), TaskAssignment{TaskID: "t1", Prompt: "set -e; echo 'set -e works'"}, nil)
	if err != nil || !result.Success {
		t.Fatal("expected success")
	}
}

func TestDefaultHandlerWithSetEuoPipefail(t *testing.T) {
	result, err := DefaultHandler(context.Background(), TaskAssignment{TaskID: "t1", Prompt: "set -euo pipefail; true"}, nil)
	if err != nil || !result.Success {
		t.Fatal("expected success")
	}
}

func TestDefaultHandlerWithSetEuoPipefailVar(t *testing.T) {
	result, err := DefaultHandler(context.Background(), TaskAssignment{TaskID: "t1", Prompt: "set -euo pipefail; MY_VAR='test'; echo ${MY_VAR}"}, nil)
	if err != nil || !result.Success {
		t.Fatal("expected success")
	}
}

func TestDefaultHandlerWithSetEuoPipefailDefault(t *testing.T) {
	result, err := DefaultHandler(context.Background(), TaskAssignment{TaskID: "t1", Prompt: "set -euo pipefail; echo ${UNSET_VAR:-default}"}, nil)
	if err != nil || !result.Success {
		t.Fatal("expected success")
	}
}

func TestDefaultHandlerWithSetEuoPipefailArray(t *testing.T) {
	result, err := DefaultHandler(context.Background(), TaskAssignment{TaskID: "t1", Prompt: "set -euo pipefail; ARR=(one two three); for item in \"${ARR[@]}\"; do echo \"$item\"; done"}, nil)
	if err != nil || !result.Success {
		t.Fatal("expected success")
	}
}

func TestDefaultHandlerWithSetEuoPipefailArrayDelete(t *testing.T) {
	result, err := DefaultHandler(context.Background(), TaskAssignment{TaskID: "t1", Prompt: "set -euo pipefail; ARR=(one two three); unset ARR[1]; echo ${#ARR[@]}"}, nil)
	if err != nil || !result.Success {
		t.Fatal("expected success")
	}
}

func TestDefaultHandlerWithSetEuoPipefailArrayAppend(t *testing.T) {
	result, err := DefaultHandler(context.Background(), TaskAssignment{TaskID: "t1", Prompt: "set -euo pipefail; ARR=(one two); ARR+=(three); echo ${#ARR[@]}"}, nil)
	if err != nil || !result.Success {
		t.Fatal("expected success")
	}
}

func TestDefaultHandlerWithSetEuoPipefailArraySlicing(t *testing.T) {
	result, err := DefaultHandler(context.Background(), TaskAssignment{TaskID: "t1", Prompt: "set -euo pipefail; ARR=(one two three four five); echo \"${ARR[@]:1:3}\""}, nil)
	if err != nil || !result.Success {
		t.Fatal("expected success")
	}
}

func TestDefaultHandlerWithSetEuoPipefailArrayPattern(t *testing.T) {
	result, err := DefaultHandler(context.Background(), TaskAssignment{TaskID: "t1", Prompt: "set -euo pipefail; ARR=(foo bar baz); echo ${ARR[@]/b/B}"}, nil)
	if err != nil || !result.Success {
		t.Fatal("expected success")
	}
}

func TestDefaultHandlerWithSetEuoPipefailArrayReplace(t *testing.T) {
	result, err := DefaultHandler(context.Background(), TaskAssignment{TaskID: "t1", Prompt: "set -euo pipefail; ARR=(foo bar baz); echo ${ARR[@]/ba/BAR}"}, nil)
	if err != nil || !result.Success {
		t.Fatal("expected success")
	}
}

func TestDefaultHandlerWithSetEuoPipefailArrayGlob(t *testing.T) {
	result, err := DefaultHandler(context.Background(), TaskAssignment{TaskID: "t1", Prompt: "set -euo pipefail; ARR=(foo bar baz); echo ${ARR[@]#f*}"}, nil)
	if err != nil || !result.Success {
		t.Fatal("expected success")
	}
}

func TestDefaultHandlerWithSetEuoPipefailArrayStrip(t *testing.T) {
	result, err := DefaultHandler(context.Background(), TaskAssignment{TaskID: "t1", Prompt: "set -euo pipefail; ARR=(foo bar baz); echo ${ARR[@]%%a*}"}, nil)
	if err != nil || !result.Success {
		t.Fatal("expected success")
	}
}

func TestDefaultHandlerWithSetEuoPipefailArrayEndStrip(t *testing.T) {
	result, err := DefaultHandler(context.Background(), TaskAssignment{TaskID: "t1", Prompt: "set -euo pipefail; ARR=(foo bar baz); echo ${ARR[@]%z}"}, nil)
	if err != nil || !result.Success {
		t.Fatal("expected success")
	}
}

func TestDefaultHandlerWithSetEuoPipefailArrayJoin(t *testing.T) {
	result, err := DefaultHandler(context.Background(), TaskAssignment{TaskID: "t1", Prompt: "set -euo pipefail; ARR=(one two three); IFS=-; echo \"${ARR[*]}\""}, nil)
	if err != nil || !result.Success {
		t.Fatal("expected success")
	}
}

func TestDefaultHandlerWithSetEuoPipefailArrayLength(t *testing.T) {
	result, err := DefaultHandler(context.Background(), TaskAssignment{TaskID: "t1", Prompt: "set -euo pipefail; ARR=(one two three); echo ${#ARR[@]}"}, nil)
	if err != nil || !result.Success {
		t.Fatal("expected success")
	}
}

func TestDefaultHandlerWithSetEuoPipefailArrayIndex(t *testing.T) {
	result, err := DefaultHandler(context.Background(), TaskAssignment{TaskID: "t1", Prompt: "set -euo pipefail; ARR=(one two three); echo ${ARR[0]}"}, nil)
	if err != nil || !result.Success {
		t.Fatal("expected success")
	}
}

func TestDefaultHandlerWithSetEuoPipefailArrayIterate(t *testing.T) {
	result, err := DefaultHandler(context.Background(), TaskAssignment{TaskID: "t1", Prompt: "set -euo pipefail; ARR=(one two three); for item in \"${ARR[@]}\"; do echo \"$item\"; done"}, nil)
	if err != nil || !result.Success {
		t.Fatal("expected success")
	}
}

func TestDefaultHandlerWithSetEuoPipefailArrayReplaceGlobal(t *testing.T) {
	result, err := DefaultHandler(context.Background(), TaskAssignment{TaskID: "t1", Prompt: "set -euo pipefail; ARR=(foo bar baz); echo ${ARR[@]//ba/BAR}"}, nil)
	if err != nil || !result.Success {
		t.Fatal("expected success")
	}
}

func TestDefaultHandlerWithSetEuoPipefailArrayReplaceFirst(t *testing.T) {
	result, err := DefaultHandler(context.Background(), TaskAssignment{TaskID: "t1", Prompt: "set -euo pipefail; ARR=(foo bar baz); echo ${ARR[@]/ba/BAR}"}, nil)
	if err != nil || !result.Success {
		t.Fatal("expected success")
	}
}

// JSON serialization
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

// Concurrency
func TestDefaultHandlerConcurrency(t *testing.T) {
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			_, err := DefaultHandler(context.Background(), TaskAssignment{TaskID: fmt.Sprintf("t%d", id), Prompt: "echo hello"}, nil)
			if err != nil {
				t.Errorf("task %d failed: %v", id, err)
			}
		}(i)
	}
	wg.Wait()
}

// Ensure os is used
var _ = os.Remove

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

func TestDefaultHandlerWithMultipleEcho(t *testing.T) {
	result, err := DefaultHandler(context.Background(), TaskAssignment{TaskID: "t1", Prompt: "echo 'a' && echo 'b' && echo 'c'"}, nil)
	if err != nil {
		t.Fatalf("failed: %v", err)
	}
	if !result.Success {
		t.Error("expected success")
	}
	if result.Output == "" {
		t.Error("expected non-empty output")
	}
}

// TestDefaultHandlerWithLocalRepo verifies that a local repo path is used
// as the working directory and the command runs inside it.
func TestDefaultHandlerWithLocalRepo(t *testing.T) {
	// Create a temp dir with a known file
	repoDir, err := os.MkdirTemp("", "hotelier-repo-*")
	if err != nil {
		t.Fatalf("create temp repo dir: %v", err)
	}
	defer os.RemoveAll(repoDir)

	if err := os.WriteFile(filepath.Join(repoDir, "test.txt"), []byte("hello world"), 0o644); err != nil {
		t.Fatalf("write test file: %v", err)
	}

	var mu sync.Mutex
	var lines []string

	result, err := DefaultHandler(context.Background(), TaskAssignment{
		TaskID: "t1",
		Repos:  []string{repoDir},
		Prompt: "cat test.txt",
	}, func(taskID, line string) error {
		mu.Lock()
		defer mu.Unlock()
		lines = append(lines, line)
		return nil
	})
	if err != nil {
		t.Fatalf("failed: %v", err)
	}
	if !result.Success {
		t.Error("expected success")
	}
	if !strings.Contains(result.Output, "hello world") {
		t.Errorf("expected output to contain 'hello world', got %q", result.Output)
	}

	// Verify working directory log was emitted
	mu.Lock()
	defer mu.Unlock()
	foundWorkdir := false
	for _, line := range lines {
		if strings.Contains(line, "[WORKDIR]") {
			foundWorkdir = true
			break
		}
	}
	if !foundWorkdir {
		t.Error("expected [WORKDIR] log line to be emitted")
	}

	// Verify command log was emitted
	foundCmd := false
	for _, line := range lines {
		if strings.Contains(line, "[SHELL]") {
			foundCmd = true
			break
		}
	}
	if !foundCmd {
		t.Error("expected [SHELL] log line to be emitted")
	}
}

// TestDefaultHandlerWithRemoteRepoCloning verifies that remote URLs trigger
// git clone and the repo is accessible in the working directory.
func TestDefaultHandlerWithRemoteRepoCloning(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}

	var lines []string
	cb := func(taskID, line string) error {
		lines = append(lines, line)
		return nil
	}

	result, err := DefaultHandler(context.Background(), TaskAssignment{
		TaskID: "t1",
		Repos:  []string{"https://github.com/hashicorp/vault.git"},
		Prompt: "ls",
	}, cb)
	if err != nil {
		t.Fatalf("failed: %v", err)
	}
	if !result.Success {
		t.Error("expected success")
	}
	// The 'ls' command should list the vault directory contents
	if result.Output == "" {
		t.Error("expected non-empty output from ls")
	}

	// Verify git clone log was emitted
	foundGit := false
	for _, line := range lines {
		if strings.Contains(line, "[GIT]") {
			foundGit = true
			break
		}
	}
	if !foundGit {
		t.Error("expected [GIT] log line to be emitted for remote repo")
	}

	// Verify working directory log was emitted
	foundWorkdir := false
	for _, line := range lines {
		if strings.Contains(line, "[WORKDIR]") {
			foundWorkdir = true
			break
		}
	}
	if !foundWorkdir {
		t.Error("expected [WORKDIR] log line to be emitted")
	}
}
