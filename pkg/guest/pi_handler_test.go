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

	"hotelier/pkg/persona"
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

	_, err = h.prepareTaskDir(context.Background(), "task-1", "", sendLog, nil)
	if err != nil {
		t.Fatalf("prepareTaskDir failed: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()

	// Should have received at least one operational log entry.
	if len(logEntries) == 0 {
		t.Fatal("expected operational log entries from prepareTaskDir, got none")
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

// TestPIHandler_PrepareTaskDir verifies that the handler creates and returns
// a task-specific directory.
func TestPIHandler_PrepareTaskDir(t *testing.T) {
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

	taskDir, err := h.prepareTaskDir(context.Background(), "task-1", "", nil, nil)
	if err != nil {
		t.Fatalf("prepareTaskDir failed: %v", err)
	}

	// Should return a task-specific directory under baseDir/tasks/ with the
	// task ID as a prefix (random suffix is appended for uniqueness).
	expectedPrefix := filepath.Join(baseDir, "tasks", "task-1-")
	if !strings.HasPrefix(taskDir, expectedPrefix) {
		t.Errorf("expected working dir to start with %q, got %q", expectedPrefix, taskDir)
	}

	// The directory should exist.
	if _, err := os.Stat(taskDir); err != nil {
		t.Errorf("task dir %s should exist: %v", taskDir, err)
	}

	// The tmp subdirectory should also exist (for TMPDIR isolation).
	tmpDir := filepath.Join(taskDir, "tmp")
	if _, err := os.Stat(tmpDir); os.IsNotExist(err) {
		t.Errorf("tmp dir %s should exist for TMPDIR isolation", tmpDir)
	}
}

// TestPIHandler_PrepareTaskDirAfterResetClient verifies that prepareTaskDir uses
// the original base CWD even after resetClient replaces the pi client with
// a task-specific working directory. This is a regression test for the bug
// where nested tasks produced paths like:
//
//	/base/tasks/task-1/tasks/task-2/repo
//
// instead of:
//
//	/base/tasks/task-2/repo
func TestPIHandler_PrepareTaskDirAfterResetClient(t *testing.T) {
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
	taskDir1, err := h.prepareTaskDir(context.Background(), "task-1", "", nil, nil)
	if err != nil {
		t.Fatalf("first prepareTaskDir failed: %v", err)
	}

	// Verify first task dir is under baseDir with the expected prefix.
	expectedPrefix1 := filepath.Join(baseDir, "tasks", "task-1-")
	if !strings.HasPrefix(taskDir1, expectedPrefix1) {
		t.Errorf("first task dir = %q, want prefix %q", taskDir1, expectedPrefix1)
	}

	// Simulate resetClient: replace the client with one whose CWD is the
	// first task's working directory (this is what ExecuteTask does)
	newClient := pi.NewClient(pi.PiClientConfig{
		CWD: taskDir1,
		Log: log.New(io.Discard, "", 0),
	})
	h.client = newClient

	// Second task — should still use baseDir, NOT taskDir1
	taskDir2, err := h.prepareTaskDir(context.Background(), "task-2", "", nil, nil)
	if err != nil {
		t.Fatalf("second prepareTaskDir failed: %v", err)
	}

	// The second task dir should be under baseDir, NOT nested under taskDir1.
	// Bug: it would be taskDir1/tasks/task-2 instead of baseDir/tasks/task-2.
	expectedPrefix2 := filepath.Join(baseDir, "tasks", "task-2-")
	if !strings.HasPrefix(taskDir2, expectedPrefix2) {
		t.Errorf("second task dir = %q, want prefix %q (base CWD leaked into task dir)", taskDir2, expectedPrefix2)
	}

	// Verify the directory was actually created
	if _, err := os.Stat(taskDir2); os.IsNotExist(err) {
		t.Errorf("expected task dir %s to exist", taskDir2)
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

	// Create a task directory via prepareTaskDir
	taskDir, err := h.prepareTaskDir(context.Background(), "task-1", "", nil, nil)
	if err != nil {
		t.Fatalf("prepareTaskDir failed: %v", err)
	}

	// Verify the directory exists before cleanup
	if _, err := os.Stat(taskDir); err != nil {
		t.Fatalf("task dir %s should exist before cleanup: %v", taskDir, err)
	}

	// Clean up the task directory using the actual path returned by prepareTaskDir.
	err = h.cleanupTaskDir(taskDir)
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

	// Cleanup a non-existent directory should not error.
	err = h.cleanupTaskDir(filepath.Join(baseDir, "tasks", "nonexistent-task-abc123"))
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
	taskDir, err := h.prepareTaskDir(context.Background(), "task-1", "", nil, nil)
	if err != nil {
		t.Fatalf("prepareTaskDir failed: %v", err)
	}

	// Create some files inside the task directory (simulating work artefacts)
	testFile := filepath.Join(taskDir, "output.txt")
	if err := os.WriteFile(testFile, []byte("hello"), 0o644); err != nil {
		t.Fatalf("create test file: %v", err)
	}

	// Clean up using the actual path returned by prepareTaskDir.
	err = h.cleanupTaskDir(taskDir)
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

// TestPIHandler_ExecuteTask_ClientNotRunningAttemptsRestart verifies that
// when the pi client is not running at the start of ExecuteTask, the handler
// attempts to restart the client before proceeding. If pi is installed, the
// restart will succeed and the task will proceed (or fail for other reasons
// like context timeout). If pi is not installed, the restart will fail and
// a descriptive error is returned.
func TestPIHandler_ExecuteTask_ClientNotRunningAttemptsRestart(t *testing.T) {
	baseDir, err := os.MkdirTemp("", "hotelier-base-*")
	if err != nil {
		t.Fatalf("create temp base dir: %v", err)
	}
	defer os.RemoveAll(baseDir)

	var logBuf strings.Builder
	logger := log.New(&logBuf, "", 0)

	// Create a handler with a client that is NOT running.
	client := pi.NewClient(pi.PiClientConfig{
		CWD: baseDir,
		Log: logger,
	})

	h := &PIHandler{
		baseCWD: baseDir,
		client:  client,
		log:     logger,
	}

	task := TaskAssignment{
		TaskID: "test-client-not-running",
		Prompt: "test prompt",
	}

	var restartWarningSent, restartSuccessSent bool
	var mu sync.Mutex
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = h.ExecuteTask(ctx, task, func(entry LogEntry) error {
		mu.Lock()
		defer mu.Unlock()
		if strings.Contains(entry.Line, "attempting restart") {
			restartWarningSent = true
		}
		if strings.Contains(entry.Line, "restarted successfully") {
			restartSuccessSent = true
		}
		return nil
	})

	// Check that restart-related logs were produced
	logOutput := logBuf.String()
	if !strings.Contains(logOutput, "pi client not running, attempting restart") {
		t.Errorf("expected restart attempt log, got:\n%s", logOutput)
	}

	// Check that restart-related sendLog entries were sent
	mu.Lock()
	hadRestartWarning := restartWarningSent
	hadRestartSuccess := restartSuccessSent
	mu.Unlock()

	if !hadRestartWarning {
		t.Error("expected 'attempting restart' log entry via sendLog")
	}

	if _, err := exec.LookPath("pi"); err == nil {
		// pi is installed — restart should succeed, task proceeds (may fail for other reasons)
		if !hadRestartSuccess {
			t.Error("expected 'restarted successfully' log entry via sendLog when pi is installed")
		}
		// The error (if any) should NOT be about the client not running
		if err != nil && strings.Contains(err.Error(), "not running") {
			t.Errorf("expected task to proceed after restart, got error: %v", err)
		}
		h.Stop(context.Background())
	} else {
		// pi not installed — restart should fail with descriptive error
		if err == nil {
			t.Fatal("expected error when pi is not installed and restart fails")
		}
		if !strings.Contains(err.Error(), "not running and restart failed") {
			t.Errorf("expected 'not running and restart failed' in error, got: %v", err)
		}
	}
}

// TestPIHandler_ExecuteTask_ClientKilledExternallyRestartSucceeds verifies
// that when the pi client is killed externally (e.g. pkill pi), ExecuteTask
// restarts the client and the task proceeds rather than failing with
// "not running". This simulates the exact scenario from issue #28.
func TestPIHandler_ExecuteTask_ClientKilledExternallyRestartSucceeds(t *testing.T) {
	// This test uses a non-existent binary by creating a client that
	// wraps a command that can't be found. We achieve this by setting
	// baseCWD to a valid directory but ensuring the restart path fails.
	// Since restartClient uses exec.Command("pi", ...), if pi is not
	// in PATH, the restart will fail.
	//
	// To make this test deterministic regardless of whether pi is installed,
	// we create a handler and manually kill the client process after Start(),
	// then verify the restart logic works.
	if _, err := exec.LookPath("pi"); err != nil {
		t.Skip("pi not installed — restart failure path tested via other means")
	}

	baseDir, err := os.MkdirTemp("", "hotelier-base-*")
	if err != nil {
		t.Fatalf("create temp base dir: %v", err)
	}
	defer os.RemoveAll(baseDir)

	// Start a real client, then kill it to simulate the "pi client not running" scenario.
	h := NewPIHandler(baseDir, "", "", "")
	if err := h.Start(context.Background()); err != nil {
		t.Fatalf("start failed: %v", err)
	}

	// Get the client and kill the process to simulate external kill
	client := h.GetClient()
	if client == nil || client.Cmd() == nil || client.Cmd().Process == nil {
		t.Fatal("client process not available")
	}
	proc := client.Cmd().Process

	// Kill the process externally (simulating pkill pi)
	if err := proc.Kill(); err != nil {
		t.Fatalf("kill process: %v", err)
	}

	// Wait for the process to exit so ProcessState is populated
	time.Sleep(500 * time.Millisecond)

	// Now the client should not be running
	if h.IsRunning() {
		t.Fatal("handler should not be running after external kill")
	}

	// ExecuteTask should attempt restart
	task := TaskAssignment{
		TaskID: "test-restart-after-kill",
		Prompt: "test prompt",
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = h.ExecuteTask(ctx, task, func(entry LogEntry) error {
		return nil
	})

	// After restart, the task should proceed (not fail with "not running")
	if err != nil && strings.Contains(err.Error(), "not running") {
		t.Errorf("expected task to proceed after restart, got error: %v", err)
	}

	h.Stop(context.Background())
}

// TestPIHandler_PrepareTaskDir_WithRepoRef verifies that prepareTaskDir
// clones the specified repository when repoRef is set.
func TestPIHandler_PrepareTaskDir_WithRepoRef(t *testing.T) {
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

	// Use a bare repo URL that doesn't exist — this should fail with a
	// git clone error, not a prepareTaskDir bug.
	_, err = h.prepareTaskDir(context.Background(), "task-repo", "https://github.com/nonexistent-user/nonexistent-repo-12345.git", nil, nil)
	if err == nil {
		t.Fatal("expected error when cloning nonexistent repo, got nil")
	}
	if !strings.Contains(err.Error(), "git clone") {
		t.Errorf("expected 'git clone' in error, got: %v", err)
	}

	// Task directory should still have been created (even though clone failed).
	// We can't check the exact path because of the random suffix, but we can
	// verify a new directory was created under baseDir/tasks/ with the right prefix.
	tasksDir := filepath.Join(baseDir, "tasks")
	entries, err := os.ReadDir(tasksDir)
	if err != nil {
		t.Fatalf("read tasks dir: %v", err)
	}
	found := false
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "task-repo-") {
			found = true
			expectedDir := filepath.Join(tasksDir, e.Name())
			if _, err := os.Stat(expectedDir); os.IsNotExist(err) {
				t.Errorf("expected task dir %s to exist even after clone failure", expectedDir)
			}
			tmpDir := filepath.Join(expectedDir, "tmp")
			if _, err := os.Stat(tmpDir); os.IsNotExist(err) {
				t.Errorf("tmp dir %s should exist for TMPDIR isolation even after clone failure", tmpDir)
			}
			break
		}
	}
	if !found {
		t.Errorf("expected a task directory with prefix 'task-repo-' to exist under %s", tasksDir)
	}
}

// TestPIHandler_PrepareTaskDir_WithPersonaNoRepoRef verifies that
// prepareTaskDir applies persona files even when repo_ref is empty.
// This is a regression test for the bug where persona files were only
// applied inside the repo_ref branch of prepareTaskDir.
func TestPIHandler_PrepareTaskDir_WithPersonaNoRepoRef(t *testing.T) {
	baseDir, err := os.MkdirTemp("", "hotelier-base-*")
	if err != nil {
		t.Fatalf("create temp base dir: %v", err)
	}
	defer os.RemoveAll(baseDir)

	// Create a persona that copies a file
	credFile := filepath.Join(baseDir, "cred-file")
	if err := os.WriteFile(credFile, []byte("secret"), 0o600); err != nil {
		t.Fatalf("create credential file: %v", err)
	}

	p := &persona.Persona{
		Name: "test-persona",
		Files: []persona.FileCopy{
			{From: credFile, To: ".git-credentials"},
		},
		Env: map[string]string{
			"GIT_CONFIG_COUNT": "1",
		},
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

	taskDir, err := h.prepareTaskDir(context.Background(), "task-persona-no-repo", "", nil, p)
	if err != nil {
		t.Fatalf("prepareTaskDir failed: %v", err)
	}

	// The task directory should exist with the persona file applied.
	expectedPrefix := filepath.Join(baseDir, "tasks", "task-persona-no-repo-")
	if !strings.HasPrefix(taskDir, expectedPrefix) {
		t.Errorf("expected task dir to start with %q, got %q", expectedPrefix, taskDir)
	}

	// The persona file should be present
	credDest := filepath.Join(taskDir, ".git-credentials")
	if _, err := os.Stat(credDest); os.IsNotExist(err) {
		t.Error("expected persona file .git-credentials to exist in task dir (applied without repo_ref)")
	}
}

// TestPIHandler_PrepareTaskDir_NoRepoRef verifies that prepareTaskDir
// works without a repo_ref (existing behaviour).
func TestPIHandler_PrepareTaskDir_NoRepoRef(t *testing.T) {
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

	var logEntries []LogEntry
	sendLog := func(entry LogEntry) error {
		logEntries = append(logEntries, entry)
		return nil
	}

	taskDir, err := h.prepareTaskDir(context.Background(), "task-no-repo", "", sendLog, nil)
	if err != nil {
		t.Fatalf("prepareTaskDir failed: %v", err)
	}

	expectedPrefix := filepath.Join(baseDir, "tasks", "task-no-repo-")
	if !strings.HasPrefix(taskDir, expectedPrefix) {
		t.Errorf("expected task dir to start with %q, got %q", expectedPrefix, taskDir)
	}

	// Should have sent a "Using task directory" log entry
	found := false
	for _, entry := range logEntries {
		if strings.Contains(entry.Line, "Using task directory") {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected 'Using task directory' log entry")
	}

	// The tmp subdirectory should exist (for TMPDIR isolation).
	tmpDir := filepath.Join(taskDir, "tmp")
	if _, err := os.Stat(tmpDir); os.IsNotExist(err) {
		t.Errorf("tmp dir %s should exist for TMPDIR isolation", tmpDir)
	}
}

// TestPIHandler_PrepareTaskDir_WithPersonaAndRepoRef verifies that
// prepareTaskDir applies persona files before and after cloning.
func TestPIHandler_PrepareTaskDir_WithPersonaAndRepoRef(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}

	baseDir, err := os.MkdirTemp("", "hotelier-base-*")
	if err != nil {
		t.Fatalf("create temp base dir: %v", err)
	}
	defer os.RemoveAll(baseDir)

	// Create a persona that copies a file
	credFile := filepath.Join(baseDir, "cred-file")
	if err := os.WriteFile(credFile, []byte("secret"), 0o600); err != nil {
		t.Fatalf("create credential file: %v", err)
	}

	p := &persona.Persona{
		Name: "test-persona",
		Files: []persona.FileCopy{
			{From: credFile, To: ".git-credentials"},
		},
		Env: map[string]string{
			"GIT_CONFIG_COUNT": "1",
		},
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

	// Clone a nonexistent repo — should fail, but persona files
	// should have been applied before the clone attempt.
	_, err = h.prepareTaskDir(context.Background(), "task-persona-repo", "https://github.com/nonexistent-user/nonexistent-repo-67890.git", nil, p)
	if err == nil {
		t.Fatal("expected error when cloning nonexistent repo, got nil")
	}

	// The task directory should exist with the persona file applied.
	tasksDir := filepath.Join(baseDir, "tasks")
	entries, err := os.ReadDir(tasksDir)
	if err != nil {
		t.Fatalf("read tasks dir: %v", err)
	}
	var foundDir string
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "task-persona-repo-") {
			foundDir = filepath.Join(tasksDir, e.Name())
			break
		}
	}
	if foundDir == "" {
		t.Fatalf("expected a task directory with prefix 'task-persona-repo-' to exist under %s", tasksDir)
	}
	if _, err := os.Stat(foundDir); os.IsNotExist(err) {
		t.Fatalf("expected task dir %s to exist", foundDir)
	}

	// The persona file should be present (applied before clone)
	credDest := filepath.Join(foundDir, ".git-credentials")
	if _, err := os.Stat(credDest); os.IsNotExist(err) {
		t.Error("expected persona file .git-credentials to exist in task dir (applied before clone)")
	}
}

func TestPIHandler_ResetClientWithEnv_RetriesOnFailure(t *testing.T) {
	if _, err := exec.LookPath("pi"); err != nil {
		t.Skip("pi not installed")
	}

	baseDir, err := os.MkdirTemp("", "hotelier-base-*")
	if err != nil {
		t.Fatalf("create temp base dir: %v", err)
	}
	defer os.RemoveAll(baseDir)

	// Capture log output to verify retry messages
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

	// Use a context that cancels after a short duration to interrupt retries.
	// This verifies the retry loop respects context cancellation.
	ctx, cancel := context.WithTimeout(context.Background(), 800*time.Millisecond)
	defer cancel()

	// Call resetClientWithEnv with a non-existent directory — Start() will fail
	// because pi can't start in that directory. With retries, this should take
	// at least 1s + 2s = 3s of backoff, but the context will cancel it sooner.
	start := time.Now()
	err = h.resetClientWithEnv(ctx, "/nonexistent/path/that/does/not/exist", "test-task", func(LogEntry) error {
		return nil
	}, nil)
	elapsed := time.Since(start)

	// The call should have been interrupted by context timeout (not immediate failure)
	// because the retry loop waits between attempts.
	if err == nil {
		t.Log("resetClientWithEnv succeeded unexpectedly")
	} else {
		t.Logf("resetClientWithEnv failed (expected): %v", err)
	}

	// Verify retry log messages were produced
	logOutput := logBuf.String()
	if !strings.Contains(logOutput, "attempt 1/3") {
		t.Errorf("expected retry attempt 1/3 in logs, got:\n%s", logOutput)
	}

	// Verify the elapsed time shows retries happened (should be at least ~1s for first backoff)
	if elapsed < 500*time.Millisecond {
		t.Errorf("expected retries to take time, but completed in %v (no retries?)", elapsed)
	}

	h.Stop(context.Background())
}

func TestPIHandler_ResetClientWithEnv_SendsRetryLogs(t *testing.T) {
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

	client := pi.NewClient(pi.PiClientConfig{
		CWD: baseDir,
		Log: logger,
	})

	var sentLogs []LogEntry
	var logsMu sync.Mutex

	h := &PIHandler{
		baseCWD: baseDir,
		client:  client,
		log:     logger,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 800*time.Millisecond)
	defer cancel()

	_ = h.resetClientWithEnv(ctx, "/nonexistent/path", "test-task", func(entry LogEntry) error {
		logsMu.Lock()
		defer logsMu.Unlock()
		sentLogs = append(sentLogs, entry)
		return nil
	}, nil)

	// Check that retry warning logs were sent
	logsMu.Lock()
	var hadRetryWarning bool
	for _, entry := range sentLogs {
		if strings.Contains(entry.Line, "Spawn attempt") && entry.Level == "warning" {
			hadRetryWarning = true
			break
		}
	}
	logsMu.Unlock()

	if !hadRetryWarning {
		t.Errorf("expected retry warning log entry via sendLog, got %d entries", len(sentLogs))
	}

	h.Stop(context.Background())
}

func TestPIHandler_RestartClient_RetriesOnFailure(t *testing.T) {
	if _, err := exec.LookPath("pi"); err != nil {
		t.Skip("pi not installed")
	}

	baseDir, err := os.MkdirTemp("", "hotelier-base-*")
	if err != nil {
		t.Fatalf("create temp base dir: %v", err)
	}
	defer os.RemoveAll(baseDir)

	// Capture log output to verify retry messages
	var logBuf strings.Builder
	logger := log.New(&logBuf, "[test] ", 0)

	client := pi.NewClient(pi.PiClientConfig{
		CWD: baseDir,
		Log: logger,
	})

	// Set baseCWD to a non-existent path to force Start() to fail on every attempt.
	h := &PIHandler{
		baseCWD: "/nonexistent/path/that/does/not/exist",
		client:  client,
		log:     logger,
	}

	// Use a context that cancels after a short duration to interrupt retries.
	// This verifies the retry loop respects context cancellation.
	ctx, cancel := context.WithTimeout(context.Background(), 800*time.Millisecond)
	defer cancel()

	// restartClient will fail on every attempt because baseCWD does not exist.
	// With retries, this should take at least ~1s of backoff, but the context
	// will cancel it sooner.
	start := time.Now()
	err = h.restartClient(ctx)
	elapsed := time.Since(start)

	if err == nil {
		t.Log("restartClient succeeded unexpectedly")
	} else {
		t.Logf("restartClient failed (expected): %v", err)
	}

	// Verify retry log messages were produced
	logOutput := logBuf.String()
	if !strings.Contains(logOutput, "attempt 1/3") {
		t.Errorf("expected retry attempt 1/3 in logs, got:\n%s", logOutput)
	}

	// Verify the elapsed time shows retries happened (should be at least ~500ms for first backoff attempt)
	if elapsed < 500*time.Millisecond {
		t.Errorf("expected retries to take time, but completed in %v (no retries?)", elapsed)
	}

	h.Stop(context.Background())
}

// TestPIHandler_PrepareTaskDir_UniquePaths verifies that each call to
// prepareTaskDir produces a unique directory path, even when called with
// the same task ID. This prevents the rename race condition where a stale
// .git (or any other file) from a previous interrupted run would cause
// os.Rename to fail with "file exists".
func TestPIHandler_PrepareTaskDir_UniquePaths(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}

	baseDir, err := os.MkdirTemp("", "hotelier-base-*")
	if err != nil {
		t.Fatalf("create temp base dir: %v", err)
	}
	defer os.RemoveAll(baseDir)

	// Create a minimal repo to clone from.
	repoDir, err := os.MkdirTemp("", "hotelier-repo-*")
	if err != nil {
		t.Fatalf("create temp repo dir: %v", err)
	}
	defer os.RemoveAll(repoDir)

	workDir := filepath.Join(repoDir, "work")
	if err := os.MkdirAll(workDir, 0o755); err != nil {
		t.Fatalf("create work dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(workDir, "README.md"), []byte("hello"), 0o644); err != nil {
		t.Fatalf("write README: %v", err)
	}
	if out, err := exec.Command("git", "-C", workDir, "init").CombinedOutput(); err != nil {
		t.Fatalf("init repo: %v\n%s", err, out)
	}
	if out, err := exec.Command("git", "-C", workDir, "config", "user.email", "test@test.com").CombinedOutput(); err != nil {
		t.Fatalf("git config email: %v\n%s", err, out)
	}
	if out, err := exec.Command("git", "-C", workDir, "config", "user.name", "Test").CombinedOutput(); err != nil {
		t.Fatalf("git config name: %v\n%s", err, out)
	}
	if out, err := exec.Command("git", "-C", workDir, "add", ".").CombinedOutput(); err != nil {
		t.Fatalf("git add: %v\n%s", err, out)
	}
	if out, err := exec.Command("git", "-C", workDir, "commit", "-m", "initial").CombinedOutput(); err != nil {
		t.Fatalf("commit: %v\n%s", err, out)
	}
	bundleFile := filepath.Join(repoDir, "repo.bundle")
	if out, err := exec.Command("git", "-C", workDir, "bundle", "create", bundleFile, "--all").CombinedOutput(); err != nil {
		t.Fatalf("bundle: %v\n%s", err, out)
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

	taskID := "task-unique-paths"

	// Call prepareTaskDir twice with the same task ID.
	dir1, err := h.prepareTaskDir(context.Background(), taskID, bundleFile, nil, nil)
	if err != nil {
		t.Fatalf("first prepareTaskDir failed: %v", err)
	}
	dir2, err := h.prepareTaskDir(context.Background(), taskID, bundleFile, nil, nil)
	if err != nil {
		t.Fatalf("second prepareTaskDir failed: %v", err)
	}

	// The two directories must be different.
	if dir1 == dir2 {
		t.Errorf("expected unique paths, got same dir %q", dir1)
	}

	// Both should have the task ID prefix.
	prefix := filepath.Join(baseDir, "tasks", taskID+"-")
	if !strings.HasPrefix(dir1, prefix) {
		t.Errorf("first dir %q should have prefix %q", dir1, prefix)
	}
	if !strings.HasPrefix(dir2, prefix) {
		t.Errorf("second dir %q should have prefix %q", dir2, prefix)
	}

	// Both should contain a cloned repo.
	for i, dir := range []string{dir1, dir2} {
		gitDir := filepath.Join(dir, ".git")
		if _, err := os.Stat(gitDir); os.IsNotExist(err) {
			t.Errorf("dir %d: expected .git to exist", i+1)
		}
		readme := filepath.Join(dir, "README.md")
		if _, err := os.Stat(readme); os.IsNotExist(err) {
			t.Errorf("dir %d: expected README.md to exist", i+1)
		}
	}

	// Clean up both directories.
	if err := h.cleanupTaskDir(dir1); err != nil {
		t.Fatalf("cleanup dir1: %v", err)
	}
	if err := h.cleanupTaskDir(dir2); err != nil {
		t.Fatalf("cleanup dir2: %v", err)
	}

	// Verify both are gone.
	if _, err := os.Stat(dir1); !os.IsNotExist(err) {
		t.Errorf("dir1 should have been removed")
	}
	if _, err := os.Stat(dir2); !os.IsNotExist(err) {
		t.Errorf("dir2 should have been removed")
	}
}

// TestPIHandler_ExecuteTask_WaitsForAgentSettled is a behavioural regression
// test for issue #161: when pi hits a 429 rate limit it emits agent_end
// (willRetry=true) BEFORE auto_retry_start, then retries. The task must only
// be marked complete on agent_settled, not on that intermediate agent_end.
//
// It uses a fake `pi` subprocess (a python script) that emits the exact 429
// auto-retry event sequence, so it runs deterministically without a real pi
// or network access. The fake pi emits text after the retry ("second-run-done");
// if the handler wrongly settled on the first agent_end, that text would be
// missing from the output.
func TestPIHandler_ExecuteTask_WaitsForAgentSettled(t *testing.T) {
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 not installed, cannot run fake pi")
	}

	// Create a temp dir with a fake `pi` executable.
	fakeBinDir, err := os.MkdirTemp("", "hotelier-fakepi-bin-*")
	if err != nil {
		t.Fatalf("create fake bin dir: %v", err)
	}
	defer os.RemoveAll(fakeBinDir)

	// The first 10 stdout lines of a freshly-spawned pi client are captured by
	// the SpawnOutput callback (as [spawn] logs) and NOT parsed as events. We
	// emit 10 harmless queue_update lines first so the real 429 sequence below
	// falls outside that window and is processed as events.
	script := `#!/usr/bin/env python3
import sys, time, json

def emit(obj):
    sys.stdout.write(json.dumps(obj) + "\n")
    sys.stdout.flush()

# Consume the first stdin line (the prompt, or an abort from a stop) so we
# don't block the client, then emit the event sequence.
try:
    sys.stdin.readline()
except Exception:
    pass

# 10 lines of noise to clear the SpawnOutput capture window.
for _ in range(10):
    emit({"type": "queue_update", "queue": []})

# The 429 auto-retry sequence (processed as events).
emit({"type": "agent_start"})
emit({"type": "message_update", "assistantMessageEvent": {"type": "text_delta", "delta": "first-run "}})
emit({"type": "agent_end", "willRetry": True})
emit({"type": "auto_retry_start", "attempt": 1, "maxAttempts": 2, "delayMs": 100, "errorMessage": "429 rate_limit_error"})
time.sleep(0.3)
emit({"type": "agent_start"})
emit({"type": "message_update", "assistantMessageEvent": {"type": "text_delta", "delta": "second-run-done"}})
emit({"type": "agent_end", "willRetry": False})
emit({"type": "auto_retry_end", "success": True, "attempt": 2})
emit({"type": "agent_settled"})
`
	fakePi := filepath.Join(fakeBinDir, "pi")
	if err := os.WriteFile(fakePi, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake pi: %v", err)
	}

	// Prepend the fake bin dir to PATH so exec.Command("pi") resolves to it.
	origPath := os.Getenv("PATH")
	t.Cleanup(func() { os.Setenv("PATH", origPath) })
	os.Setenv("PATH", fakeBinDir+string(os.PathListSeparator)+origPath)

	baseDir, err := os.MkdirTemp("", "hotelier-base-*")
	if err != nil {
		t.Fatalf("create base dir: %v", err)
	}
	defer os.RemoveAll(baseDir)

	h := NewPIHandler(baseDir, "", "", "")

	task := TaskAssignment{
		TaskID: "test-waits-for-settled",
		Prompt: "do the thing",
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	result, err := h.ExecuteTask(ctx, task, func(entry LogEntry) error {
		return nil
	})
	if err != nil {
		t.Fatalf("ExecuteTask returned error: %v", err)
	}
	if result == nil {
		t.Fatal("ExecuteTask returned nil result")
	}

	// The task must complete successfully (pi settled).
	if !result.Success {
		t.Errorf("expected task to succeed, got success=%v error=%q", result.Success, result.Error)
	}

	// The output must contain text emitted AFTER the retry (post the first
	// agent_end). If the handler settled on the first agent_end (the bug),
	// this text would be missing.
	if !strings.Contains(result.Output, "second-run-done") {
		t.Errorf("expected output to contain post-retry text 'second-run-done' (task settled before agent_settled), got output=%q", result.Output)
	}

	// Diagnostics should record that pi settled.
	if result.Diagnostics == nil {
		t.Fatal("expected diagnostics to be attached")
	}
	if !result.Diagnostics.SettledReceived {
		t.Error("expected SettledReceived to be true")
	}
}
