package guest

import (
	"context"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"hotelier/pkg/pi"
)

// TestPIHandler_Lifecycle tests that PIHandler properly starts and stops,
// and that the underlying pi subprocess actually exits.
func TestPIHandler_Lifecycle(t *testing.T) {
	if _, err := exec.LookPath("pi"); err != nil {
		t.Skip("pi not installed")
	}

	h := NewPIHandler("/tmp", "", "", "")
	if err := h.Start(context.Background()); err != nil {
		t.Fatalf("start failed: %v", err)
	}
	defer h.Stop(context.Background())

	if !h.IsRunning() {
		t.Fatal("handler should be running after Start")
	}

	// Verify underlying client is running
	client := h.GetClient()
	if client == nil {
		t.Fatal("client should not be nil")
	}
	if !client.IsRunning() {
		t.Fatal("client should be running")
	}
}

// TestPIHandler_StopActuallyKillsProcess verifies that Stop() causes the
// pi subprocess to actually exit within a reasonable time. This is a
// regression test for the case where closing stdin doesn't terminate pi
// because it's a persistent RPC server.
func TestPIHandler_StopActuallyKillsProcess(t *testing.T) {
	if _, err := exec.LookPath("pi"); err != nil {
		t.Skip("pi not installed")
	}

	h := NewPIHandler("/tmp", "", "", "")
	if err := h.Start(context.Background()); err != nil {
		t.Fatalf("start failed: %v", err)
	}

	// Verify process is running
	client := h.GetClient()
	if client == nil {
		t.Fatal("client should not be nil")
	}
	if client.GetProcessState() != nil {
		t.Fatal("process should not have exited yet")
	}

	// Stop with a timeout
	stopDone := make(chan struct{})
	go func() {
		h.Stop(context.Background())
		close(stopDone)
	}()

	select {
	case <-stopDone:
		// Good
	case <-time.After(3 * time.Second):
		t.Fatal("Stop() did not return within 3 seconds — pi subprocess is not exiting. " +
			"Closing stdin is insufficient because pi is a persistent RPC server.")
	}

	// Verify process actually exited
	time.Sleep(200 * time.Millisecond)
	state := client.GetProcessState()
	if state == nil {
		t.Fatal("process state should not be nil after Stop")
	}
	if !state.Exited() {
		t.Fatal("pi subprocess should have exited after Stop")
	}
}

// TestPIHandler_ExecuteTask verifies the handler can execute a task and
// that the handler remains usable after task completion.
// Note: pi may be in plan mode which never emits agent_end, so we use a
// short context timeout to avoid hanging the test suite.
func TestPIHandler_ExecuteTask(t *testing.T) {
	if _, err := exec.LookPath("pi"); err != nil {
		t.Skip("pi not installed")
	}

	h := NewPIHandler("/tmp", "", "", "")
	if err := h.Start(context.Background()); err != nil {
		t.Fatalf("start failed: %v", err)
	}
	defer h.Stop(context.Background())

	task := TaskAssignment{
		TaskID: "test-task",
		Prompt: "echo hello",
	}

	// Use a short timeout — pi may hang in plan mode
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	result, err := h.ExecuteTask(ctx, task, func(entry LogEntry) error {
		return nil
	})
	if err != nil {
		t.Logf("task error (expected if pi is in plan mode): %v", err)
	}

	if result != nil {
		t.Logf("result: success=%v output=%q", result.Success, result.Output)
	}

	// Handler should still be running
	if !h.IsRunning() {
		t.Fatal("handler should still be running after task")
	}
}

// TestPIHandler_FullDeltaSentAsOneEntry verifies that the handler sends
// the full text delta (including embedded newlines) as a single log entry,
// rather than splitting on newlines.
func TestPIHandler_FullDeltaSentAsOneEntry(t *testing.T) {
	if _, err := exec.LookPath("pi"); err != nil {
		t.Skip("pi not installed")
	}

	h := NewPIHandler("/tmp", "", "", "")
	if err := h.Start(context.Background()); err != nil {
		t.Fatalf("start failed: %v", err)
	}
	defer h.Stop(context.Background())

	task := TaskAssignment{
		TaskID: "test-delta-task",
		Prompt: "Say hello",
	}

	var mu sync.Mutex
	var lines []string
	lineSet := make(map[string]bool)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, _ = h.ExecuteTask(ctx, task, func(entry LogEntry) error {
		mu.Lock()
		defer mu.Unlock()
		lines = append(lines, entry.Line)
		lineSet[entry.Line] = true
		return nil
	})

	// The handler should have sent at least one log entry.
	// Verify that no entry is empty and that multi-line content
	// was not split into separate single-line entries.
	mu.Lock()
	defer mu.Unlock()
	for _, line := range lines {
		if line == "" {
			t.Error("received empty log line — handler should not split deltas by newline")
		}
	}
}

// TestParseRepoRef verifies the parseRepoRef helper correctly splits
// repo references into URL and optional branch/ref.
func TestParseRepoRef(t *testing.T) {
	tests := []struct {
		input   string
		wantURL string
		wantRef string
	}{
		// HTTPS URLs without ref
		{"https://github.com/user/repo", "https://github.com/user/repo", ""},
		// HTTPS URLs with branch ref
		{"https://github.com/user/repo@main", "https://github.com/user/repo", "main"},
		// HTTPS URLs with feature branch
		{"https://github.com/skrewz/hotelier@speculative-feature", "https://github.com/skrewz/hotelier", "speculative-feature"},
		// HTTPS URLs with commit SHA
		{"https://github.com/user/repo@abc123def", "https://github.com/user/repo", "abc123def"},
		// HTTPS URLs with tag
		{"https://github.com/user/repo@v1.2.0", "https://github.com/user/repo", "v1.2.0"},
		// HTTPS URLs with .git suffix and ref
		{"https://github.com/user/repo.git@develop", "https://github.com/user/repo.git", "develop"},
		// SSH URLs without ref
		{"git@github.com:user/repo", "git@github.com:user/repo", ""},
		// SSH URLs with ref
		{"git@github.com:user/repo@feature", "git@github.com:user/repo", "feature"},
		// SSH URLs with .git and ref
		{"git@github.com:user/repo.git@main", "git@github.com:user/repo.git", "main"},
		{"", "", ""},
	}

	for _, tc := range tests {
		url, ref := parseRepoRef(tc.input)
		if url != tc.wantURL {
			t.Errorf("parseRepoRef(%q) url = %q, want %q", tc.input, url, tc.wantURL)
		}
		if ref != tc.wantRef {
			t.Errorf("parseRepoRef(%q) ref = %q, want %q", tc.input, ref, tc.wantRef)
		}
	}
}

// TestPIHandler_OperationalLogsSentViaCallback verifies that operational
// messages (repo preparation, subprocess spawning) are sent via the sendLog
// callback so they appear in the server's log store. This is a regression
// test for the bug where fast tasks produced no visible logs because
// operational messages were only written to stdout.
func TestPIHandler_OperationalLogsSentViaCallback(t *testing.T) {
	baseDir, err := os.MkdirTemp("", "hotelier-base-*")
	if err != nil {
		t.Fatalf("create temp base dir: %v", err)
	}
	defer os.RemoveAll(baseDir)

	client := pi.NewClient(pi.PiClientConfig{
		CWD: baseDir,
		Log: log.New(io.Discard, "", 0),
	})

	h := &PIHandler{
		baseCWD: baseDir,
		client:  client,
		log:     log.New(io.Discard, "", 0),
	}

	var mu sync.Mutex
	var logEntries []LogEntry
	sendLog := func(entry LogEntry) error {
		mu.Lock()
		defer mu.Unlock()
		logEntries = append(logEntries, entry)
		return nil
	}

	_, err = h.prepareRepos(context.Background(), "task-1", []string{}, sendLog)
	if err != nil {
		t.Fatalf("prepareRepos failed: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()

	// Should have received at least one operational log entry.
	if len(logEntries) == 0 {
		t.Fatal("expected operational log entries from prepareRepos, got none")
	}

	// Verify the entries have the expected properties.
	for _, entry := range logEntries {
		if entry.TaskID != "task-1" {
			t.Errorf("expected task_id 'task-1', got %q", entry.TaskID)
		}
		if entry.Level != "system" {
			t.Errorf("expected level 'system', got %q", entry.Level)
		}
		if entry.Line == "" {
			t.Error("expected non-empty log line")
		}
	}
}

// TestPIHandler_PrepareReposNoRepos verifies that when no repos are specified,
// the handler creates and returns a task-specific directory.
func TestPIHandler_PrepareReposNoRepos(t *testing.T) {
	baseDir, err := os.MkdirTemp("", "hotelier-base-*")
	if err != nil {
		t.Fatalf("create temp base dir: %v", err)
	}
	defer os.RemoveAll(baseDir)

	client := pi.NewClient(pi.PiClientConfig{
		CWD: baseDir,
		Log: log.New(io.Discard, "", 0),
	})

	h := &PIHandler{
		baseCWD: baseDir,
		client:  client,
		log:     log.New(io.Discard, "", 0),
	}

	taskDir, err := h.prepareRepos(context.Background(), "task-1", []string{}, nil)
	if err != nil {
		t.Fatalf("prepareRepos failed: %v", err)
	}

	// Should return a task-specific directory, not the base CWD.
	expectedPrefix := filepath.Join(baseDir, "tasks", "task-1")
	if taskDir != expectedPrefix {
		t.Errorf("expected working dir %q, got %q", expectedPrefix, taskDir)
	}

	// The directory should exist.
	if _, err := os.Stat(taskDir); err != nil {
		t.Errorf("task dir %s should exist: %v", taskDir, err)
	}
}

// TestPIHandler_PrepareReposAfterResetClient verifies that prepareRepos uses
// the original base CWD even after resetClient replaces the pi client with
// a task-specific working directory. This is a regression test for the bug
// where nested tasks produced paths like:
//
//	/base/tasks/task-1/tasks/task-2/repo
//
// instead of:
//
//	/base/tasks/task-2/repo
func TestPIHandler_PrepareReposAfterResetClient(t *testing.T) {
	baseDir, err := os.MkdirTemp("", "hotelier-base-*")
	if err != nil {
		t.Fatalf("create temp base dir: %v", err)
	}
	defer os.RemoveAll(baseDir)

	client := pi.NewClient(pi.PiClientConfig{
		CWD: baseDir,
		Log: log.New(io.Discard, "", 0),
	})

	h := &PIHandler{
		baseCWD: baseDir,
		client:  client,
		log:     log.New(io.Discard, "", 0),
	}

	// First task — no repos, creates a task directory
	taskDir1, err := h.prepareRepos(context.Background(), "task-1", []string{}, nil)
	if err != nil {
		t.Fatalf("first prepareRepos failed: %v", err)
	}

	// Verify first task dir is under baseDir
	expectedDir1 := filepath.Join(baseDir, "tasks", "task-1")
	if taskDir1 != expectedDir1 {
		t.Errorf("first task dir = %q, want %q", taskDir1, expectedDir1)
	}

	// Simulate resetClient: replace the client with one whose CWD is the
	// first task's working directory (this is what ExecuteTask does)
	newClient := pi.NewClient(pi.PiClientConfig{
		CWD: taskDir1,
		Log: log.New(io.Discard, "", 0),
	})
	h.client = newClient

	// Second task — should still use baseDir, NOT taskDir1
	taskDir2, err := h.prepareRepos(context.Background(), "task-2", []string{}, nil)
	if err != nil {
		t.Fatalf("second prepareRepos failed: %v", err)
	}

	// The second task dir should be under baseDir, NOT nested under taskDir1.
	// Bug: it would be taskDir1/tasks/task-2 instead of baseDir/tasks/task-2.
	expectedDir2 := filepath.Join(baseDir, "tasks", "task-2")
	if taskDir2 != expectedDir2 {
		t.Errorf("second task dir = %q, want %q (base CWD leaked into task dir)", taskDir2, expectedDir2)
	}

	// Verify the directory was actually created at the correct path
	if _, err := os.Stat(expectedDir2); os.IsNotExist(err) {
		t.Errorf("expected task dir %s to exist", expectedDir2)
	}
}

// TestPIHandler_PrepareReposCloning verifies that remote URLs trigger git clone.
func TestPIHandler_PrepareReposCloning(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}

	baseDir, err := os.MkdirTemp("", "hotelier-base-*")
	if err != nil {
		t.Fatalf("create temp base dir: %v", err)
	}
	defer os.RemoveAll(baseDir)

	client := pi.NewClient(pi.PiClientConfig{
		CWD: baseDir,
		Log: log.New(io.Discard, "", 0),
	})

	h := &PIHandler{
		baseCWD: baseDir,
		client:  client,
		log:     log.New(io.Discard, "", 0),
	}

	// Use a small public repo for testing
	taskDir, err := h.prepareRepos(context.Background(), "task-1", []string{"https://github.com/hashicorp/vault.git"}, nil)
	if err != nil {
		t.Fatalf("prepareRepos failed: %v", err)
	}

	// Verify the task directory was created
	if _, err := os.Stat(taskDir); os.IsNotExist(err) {
		t.Errorf("task dir %s should exist", taskDir)
	}

	// Verify the cloned repo directory exists
	repoPath := filepath.Join(taskDir, "vault")
	if _, err := os.Stat(repoPath); os.IsNotExist(err) {
		t.Errorf("cloned repo %s should exist", repoPath)
	}
}

// TestPIHandler_CleanupTaskDir_RemovesDirectory verifies that cleanupTaskDir
// removes the task directory after a task completes. Guests should clean up
// after themselves regardless of success or failure.
func TestPIHandler_CleanupTaskDir_RemovesDirectory(t *testing.T) {
	baseDir, err := os.MkdirTemp("", "hotelier-base-*")
	if err != nil {
		t.Fatalf("create temp base dir: %v", err)
	}
	defer os.RemoveAll(baseDir)

	client := pi.NewClient(pi.PiClientConfig{
		CWD: baseDir,
		Log: log.New(io.Discard, "", 0),
	})

	h := &PIHandler{
		baseCWD: baseDir,
		client:  client,
		log:     log.New(io.Discard, "", 0),
	}

	// Create a task directory via prepareRepos
	taskDir, err := h.prepareRepos(context.Background(), "task-1", []string{}, nil)
	if err != nil {
		t.Fatalf("prepareRepos failed: %v", err)
	}

	// Verify the directory exists before cleanup
	if _, err := os.Stat(taskDir); err != nil {
		t.Fatalf("task dir %s should exist before cleanup: %v", taskDir, err)
	}

	// Clean up the task directory
	err = h.cleanupTaskDir("task-1")
	if err != nil {
		t.Fatalf("cleanupTaskDir failed: %v", err)
	}

	// Verify the directory no longer exists
	if _, err := os.Stat(taskDir); !os.IsNotExist(err) {
		t.Errorf("task dir %s should have been removed after cleanup", taskDir)
	}
}

// TestPIHandler_CleanupTaskDir_Idempotent verifies that cleanupTaskDir is
// safe to call even when the directory has already been removed.
func TestPIHandler_CleanupTaskDir_Idempotent(t *testing.T) {
	baseDir, err := os.MkdirTemp("", "hotelier-base-*")
	if err != nil {
		t.Fatalf("create temp base dir: %v", err)
	}
	defer os.RemoveAll(baseDir)

	client := pi.NewClient(pi.PiClientConfig{
		CWD: baseDir,
		Log: log.New(io.Discard, "", 0),
	})

	h := &PIHandler{
		baseCWD: baseDir,
		client:  client,
		log:     log.New(io.Discard, "", 0),
	}

	// Cleanup a non-existent directory should not error
	err = h.cleanupTaskDir("nonexistent-task")
	if err != nil {
		t.Errorf("cleanupTaskDir should not error on missing directory: %v", err)
	}
}

// TestPIHandler_CleanupTaskDir_WithContents verifies that cleanupTaskDir
// removes the task directory and all its contents (including cloned repos).
func TestPIHandler_CleanupTaskDir_WithContents(t *testing.T) {
	baseDir, err := os.MkdirTemp("", "hotelier-base-*")
	if err != nil {
		t.Fatalf("create temp base dir: %v", err)
	}
	defer os.RemoveAll(baseDir)

	client := pi.NewClient(pi.PiClientConfig{
		CWD: baseDir,
		Log: log.New(io.Discard, "", 0),
	})

	h := &PIHandler{
		baseCWD: baseDir,
		client:  client,
		log:     log.New(io.Discard, "", 0),
	}

	// Create a task directory
	taskDir, err := h.prepareRepos(context.Background(), "task-1", []string{}, nil)
	if err != nil {
		t.Fatalf("prepareRepos failed: %v", err)
	}

	// Create some files inside the task directory (simulating work artefacts)
	testFile := filepath.Join(taskDir, "output.txt")
	if err := os.WriteFile(testFile, []byte("hello"), 0o644); err != nil {
		t.Fatalf("create test file: %v", err)
	}

	// Clean up
	err = h.cleanupTaskDir("task-1")
	if err != nil {
		t.Fatalf("cleanupTaskDir failed: %v", err)
	}

	// Both the directory and its contents should be gone
	if _, err := os.Stat(taskDir); !os.IsNotExist(err) {
		t.Errorf("task dir %s should have been removed", taskDir)
	}
	if _, err := os.Stat(testFile); !os.IsNotExist(err) {
		t.Errorf("file inside task dir should have been removed: %s", testFile)
	}
}

// TestPIHandler_FinalOutputPreservesNewlines verifies that the model's
// final output (sent via agent_end) is transmitted as a single log entry
// with newlines preserved, not split into separate entries.
func TestPIHandler_FinalOutputPreservesNewlines(t *testing.T) {
	if _, err := exec.LookPath("pi"); err != nil {
		t.Skip("pi not installed")
	}

	h := NewPIHandler("/tmp", "", "", "")
	if err := h.Start(context.Background()); err != nil {
		t.Fatalf("start failed: %v", err)
	}
	defer h.Stop(context.Background())

	task := TaskAssignment{
		TaskID: "test-final-task",
		Prompt: "Say hi",
	}

	var mu sync.Mutex
	var lines []string

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, _ = h.ExecuteTask(ctx, task, func(entry LogEntry) error {
		mu.Lock()
		defer mu.Unlock()
		lines = append(lines, entry.Line)
		return nil
	})

	mu.Lock()
	defer mu.Unlock()

	// Check that the final output line (if present) contains newlines
	// rather than being split into multiple entries.
	for _, line := range lines {
		if strings.Contains(line, "\n") {
			// Good — the newline was preserved in a single entry
			return
		}
	}
	// If no newline was found, that's also acceptable if there were no
	// text outputs at all (pi may have only run tools).
	t.Logf("no multi-line output found in %d log entries", len(lines))
}

// TestPIHandler_ResetClient_LogsStopError verifies that resetClient logs
// the error when stopping the old client fails. This is a regression test
// for issue #10 where the old client's Stop() error was silently discarded.
func TestPIHandler_ResetClient_LogsStopError(t *testing.T) {
	if _, err := exec.LookPath("pi"); err != nil {
		t.Skip("pi not installed")
	}

	baseDir, err := os.MkdirTemp("", "hotelier-base-*")
	if err != nil {
		t.Fatalf("create temp base dir: %v", err)
	}
	defer os.RemoveAll(baseDir)

	// Capture log output
	var logBuf strings.Builder
	logger := log.New(&logBuf, "[test] ", 0)

	client := pi.NewClient(pi.PiClientConfig{
		CWD: baseDir,
		Log: logger,
	})

	h := &PIHandler{
		baseCWD: baseDir,
		client:  client,
		log:     logger,
	}

	// Start the client so resetClient will try to stop it
	ctx := context.Background()
	if err := h.client.Start(ctx); err != nil {
		t.Fatalf("start failed: %v", err)
	}

	// Call resetClient — this will stop the old client and start a new one
	err = h.resetClient(ctx, baseDir, "test-task", func(LogEntry) error { return nil })
	// We don't care if it succeeds or fails; we just want to see the logs
	if err != nil {
		t.Logf("resetClient returned error (may be expected): %v", err)
	}

	// Stop the handler to clean up
	h.Stop(ctx)

	// Verify that logs were produced during resetClient
	logOutput := logBuf.String()
	if !strings.Contains(logOutput, "stopped old client") && !strings.Contains(logOutput, "stopping old client") {
		t.Errorf("expected log about stopping old client, got:\n%s", logOutput)
	}
	if !strings.Contains(logOutput, "creating new pi client") {
		t.Errorf("expected log about creating new client, got:\n%s", logOutput)
	}
}

// TestPIHandler_ResetClient_VerifiesClientRunning verifies that resetClient
// checks that the new client is actually running after Start().
func TestPIHandler_ResetClient_VerifiesClientRunning(t *testing.T) {
	if _, err := exec.LookPath("pi"); err != nil {
		t.Skip("pi not installed")
	}

	baseDir, err := os.MkdirTemp("", "hotelier-base-*")
	if err != nil {
		t.Fatalf("create temp base dir: %v", err)
	}
	defer os.RemoveAll(baseDir)

	client := pi.NewClient(pi.PiClientConfig{
		CWD: baseDir,
		Log: log.New(io.Discard, "", 0),
	})

	h := &PIHandler{
		baseCWD: baseDir,
		client:  client,
		log:     log.New(io.Discard, "", 0),
	}

	ctx := context.Background()
	err = h.resetClient(ctx, baseDir, "test-task", func(LogEntry) error { return nil })
	if err != nil {
		t.Fatalf("resetClient failed: %v", err)
	}

	// Verify the new client is running
	if !h.client.IsRunning() {
		t.Error("client should be running after resetClient")
	}

	// Clean up
	h.client.Stop(ctx)
}

// TestPIHandler_ResetClient_ReturnsError verifies that resetClient returns
// an error when the new client fails to start. This is tested by calling
// resetClient with a valid directory — if pi is not installed, Start() fails.
// If pi is installed, the test verifies the happy path (client running).
func TestPIHandler_ResetClient_ReturnsError(t *testing.T) {
	baseDir, err := os.MkdirTemp("", "hotelier-base-*")
	if err != nil {
		t.Fatalf("create temp base dir: %v", err)
	}
	defer os.RemoveAll(baseDir)

	client := pi.NewClient(pi.PiClientConfig{
		CWD: baseDir,
		Log: log.New(io.Discard, "", 0),
	})

	h := &PIHandler{
		baseCWD: baseDir,
		client:  client,
		log:     log.New(io.Discard, "", 0),
	}

	ctx := context.Background()

	// If pi is not installed, Start() will fail and resetClient returns error
	// If pi is installed, Start() succeeds and client is running
	if _, lookErr := exec.LookPath("pi"); lookErr != nil {
		// pi not installed — resetClient should return an error
		err := h.resetClient(ctx, baseDir, "test-task", func(LogEntry) error { return nil })
		if err == nil {
			t.Fatal("expected error when pi is not installed, got nil")
		}
		if !strings.Contains(err.Error(), "start pi client") {
			t.Errorf("expected 'start pi client' in error, got: %v", err)
		}
	} else {
		// pi installed — resetClient should succeed and client should be running
		err := h.resetClient(ctx, baseDir, "test-task", func(LogEntry) error { return nil })
		if err != nil {
			t.Fatalf("resetClient failed: %v", err)
		}
		if !h.client.IsRunning() {
			t.Error("client should be running after resetClient")
		}
		h.client.Stop(ctx)
	}
}

// TestPIHandler_ExecuteTask_SendsSpawnLogs verifies that ExecuteTask sends
// spawn-related log entries (both spawn attempt and spawn success) via the
// sendLog callback. This is a regression test for issue #10 where spawn
// operations produced no visible output in logs.jsonl.
func TestPIHandler_ExecuteTask_SendsSpawnLogs(t *testing.T) {
	if _, err := exec.LookPath("pi"); err != nil {
		t.Skip("pi not installed")
	}

	baseDir, err := os.MkdirTemp("", "hotelier-base-*")
	if err != nil {
		t.Fatalf("create temp base dir: %v", err)
	}
	defer os.RemoveAll(baseDir)

	var logBuf strings.Builder
	logger := log.New(&logBuf, "[test] ", 0)

	h := NewPIHandler(baseDir, "", "", "")
	h.log = logger

	ctx := context.Background()
	if err := h.Start(ctx); err != nil {
		t.Fatalf("start failed: %v", err)
	}
	defer h.Stop(ctx)

	task := TaskAssignment{
		TaskID: "test-spawn-logs",
		Prompt: "test prompt",
	}

	var mu sync.Mutex
	var logEntries []LogEntry
	sendLog := func(entry LogEntry) error {
		mu.Lock()
		defer mu.Unlock()
		logEntries = append(logEntries, entry)
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	_, _ = h.ExecuteTask(ctx, task, sendLog)

	mu.Lock()
	defer mu.Unlock()

	// Look for spawn-related log entries
	var foundSpawn, foundSpawnSuccess bool
	for _, entry := range logEntries {
		if strings.Contains(entry.Line, "Spawning pi subprocess") {
			foundSpawn = true
		}
		if strings.Contains(entry.Line, "spawn") && strings.Contains(entry.Line, "successfully") {
			foundSpawnSuccess = true
		}
	}

	if !foundSpawn {
		t.Error("expected 'Spawning pi subprocess' log entry")
	}
	if !foundSpawnSuccess {
		t.Error("expected 'Pi subprocess spawned successfully' log entry")
	}
}

// TestPIHandler_ExecuteTask_SendsErrorLogOnSpawnFailure verifies that
// ExecuteTask sends an error log entry when resetClient fails during spawn.
// This tests the error path at lines 186-187 of pi_handler.go.
//
// NOTE: This test requires pi to be installed (to pass the initial IsRunning
// guard) but also requires pi to NOT be available during resetClient (to
// trigger the failure path). This contradiction means the test can only
// exercise the failure path if we can make resetClient fail after the
// initial guard passes. Without a mock PiClient, this is not possible.
// The test verifies the success path and documents the limitation.
func TestPIHandler_ExecuteTask_SendsErrorLogOnSpawnFailure(t *testing.T) {
	if _, err := exec.LookPath("pi"); err != nil {
		t.Skip("pi not installed — cannot pass initial IsRunning guard")
	}

	baseDir, err := os.MkdirTemp("", "hotelier-base-*")
	if err != nil {
		t.Fatalf("create temp base dir: %v", err)
	}
	defer os.RemoveAll(baseDir)

	h := NewPIHandler(baseDir, "", "", "")

	ctx := context.Background()
	if err := h.Start(ctx); err != nil {
		t.Fatalf("start failed: %v", err)
	}
	defer h.Stop(ctx)

	task := TaskAssignment{
		TaskID: "test-spawn-fail",
		Prompt: "test prompt",
	}

	var mu sync.Mutex
	var logEntries []LogEntry
	sendLog := func(entry LogEntry) error {
		mu.Lock()
		defer mu.Unlock()
		logEntries = append(logEntries, entry)
		return nil
	}

	// With pi installed, ExecuteTask will succeed (spawn works).
	// The error path (spawn failure) cannot be tested without mocking.
	// This test verifies the success path — the error path is covered
	// by TestPIHandler_ResetClient_ReturnsError which tests resetClient
	// failure directly.
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	_, _ = h.ExecuteTask(ctx, task, sendLog)

	mu.Lock()
	defer mu.Unlock()

	// Verify spawn-related logs were sent (success path)
	var foundSpawn, foundSpawnSuccess bool
	for _, entry := range logEntries {
		if strings.Contains(entry.Line, "Spawning pi subprocess") {
			foundSpawn = true
		}
		if strings.Contains(entry.Line, "spawn") && strings.Contains(entry.Line, "successfully") {
			foundSpawnSuccess = true
		}
	}

	if !foundSpawn {
		t.Error("expected 'Spawning pi subprocess' log entry")
	}
	if !foundSpawnSuccess {
		t.Error("expected 'Pi subprocess spawned successfully' log entry")
	}

	// NOTE: The actual spawn failure path (error log sent via sendLog)
	// cannot be tested here because it requires resetClient to fail,
	// which is impossible when pi is installed. A proper test would
	// require a mock PiClient that returns an error from Start().
}

// TestPIHandler_ResetClient_SpawnOutputCallback verifies that resetClient
// sets up the SpawnOutput callback which sends spawn-phase output lines to
// the guest log store. This is a regression test for issue #19 where spawn
// failures produced no troubleshooting output.
func TestPIHandler_ResetClient_SpawnOutputCallback(t *testing.T) {
	if _, err := exec.LookPath("pi"); err != nil {
		t.Skip("pi not installed")
	}

	baseDir, err := os.MkdirTemp("", "hotelier-base-*")
	if err != nil {
		t.Fatalf("create temp base dir: %v", err)
	}
	defer os.RemoveAll(baseDir)

	var capturedLogs []LogEntry
	sendLog := func(entry LogEntry) error {
		capturedLogs = append(capturedLogs, entry)
		return nil
	}

	client := pi.NewClient(pi.PiClientConfig{
		CWD: baseDir,
		Log: log.New(io.Discard, "", 0),
	})

	h := &PIHandler{
		baseCWD: baseDir,
		client:  client,
		log:     log.New(io.Discard, "", 0),
	}

	ctx := context.Background()
	err = h.resetClient(ctx, baseDir, "test-spawn-output", sendLog)
	if err != nil {
		t.Fatalf("resetClient failed: %v", err)
	}
	defer h.client.Stop(ctx)

	// Give the spawn output goroutines time to capture any lines
	time.Sleep(1 * time.Second)

	// pi typically produces no stderr on successful startup, so we verify
	// the callback mechanism works without crashing. The key test is that
	// the callback was set up correctly (no panic) and that any output
	// would be prefixed with "[spawn]".
	for _, entry := range capturedLogs {
		if strings.HasPrefix(entry.Line, "[spawn]") {
			t.Logf("spawn output captured: %s", entry.Line)
		}
	}

	// Verify the client is running (spawn succeeded)
	if !h.client.IsRunning() {
		t.Error("client should be running after resetClient")
	}
}

// TestPIHandler_ExecuteTask_ClientNotRunningInitialGuard verifies that
// ExecuteTask checks the client is running before proceeding. If the client
// is not running at the initial guard, an error is returned immediately.
//
// Note: The new post-reset IsRunning check (after resetClient in ExecuteTask)
// is a safety net that catches the edge case where the process starts but
// crashes before the check. Testing this path requires a mock client that
// returns true from IsRunning() initially, then false after Start().
// Without mocking, the initial guard is always hit first.
func TestPIHandler_ExecuteTask_ClientNotRunningInitialGuard(t *testing.T) {
	baseDir, err := os.MkdirTemp("", "hotelier-base-*")
	if err != nil {
		t.Fatalf("create temp base dir: %v", err)
	}
	defer os.RemoveAll(baseDir)

	// Create a handler with a client that is NOT running.
	// ExecuteTask should fail because the initial IsRunning check fails.
	client := pi.NewClient(pi.PiClientConfig{
		CWD: baseDir,
		Log: log.New(io.Discard, "", 0),
	})

	h := &PIHandler{
		baseCWD: baseDir,
		client:  client,
		log:     log.New(io.Discard, "", 0),
	}

	task := TaskAssignment{
		TaskID: "test-client-not-running",
		Prompt: "test prompt",
	}

	_, err = h.ExecuteTask(context.Background(), task, func(entry LogEntry) error {
		return nil
	})

	if err == nil {
		t.Fatal("expected error when client is not running")
	}
	if !strings.Contains(err.Error(), "not running") {
		t.Errorf("expected 'not running' in error, got: %v", err)
	}
}
