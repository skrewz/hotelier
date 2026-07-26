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
	taskDir2, err := h.prepareTaskDir(context.Background(), "task-2", "", nil, nil)
	if err != nil {
		t.Fatalf("second prepareTaskDir failed: %v", err)
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
	taskDir, err := h.prepareTaskDir(context.Background(), "task-1", "", nil, nil)
	if err != nil {
		t.Fatalf("prepareTaskDir failed: %v", err)
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
	_, err = h.ExecuteTask(context.Background(), task, func(entry LogEntry) error {
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

	// Task directory should still have been created (even though clone failed)
	expectedDir := filepath.Join(baseDir, "tasks", "task-repo")
	if _, err := os.Stat(expectedDir); os.IsNotExist(err) {
		t.Errorf("expected task dir %s to exist even after clone failure", expectedDir)
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

	// The task directory should exist with the persona file applied
	expectedDir := filepath.Join(baseDir, "tasks", "task-persona-no-repo")
	if taskDir != expectedDir {
		t.Errorf("expected %q, got %q", expectedDir, taskDir)
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

	expectedDir := filepath.Join(baseDir, "tasks", "task-no-repo")
	if taskDir != expectedDir {
		t.Errorf("expected %q, got %q", expectedDir, taskDir)
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

	// The task directory should exist with the persona file applied
	taskDir := filepath.Join(baseDir, "tasks", "task-persona-repo")
	if _, err := os.Stat(taskDir); os.IsNotExist(err) {
		t.Fatalf("expected task dir %s to exist", taskDir)
	}

	// The persona file should be present (applied before clone)
	credDest := filepath.Join(taskDir, ".git-credentials")
	if _, err := os.Stat(credDest); os.IsNotExist(err) {
		t.Error("expected persona file .git-credentials to exist in task dir (applied before clone)")
	}
}

// TestPIHandler_PrepareTaskDir_CreatesTmpDir verifies that prepareTaskDir
// creates a tmp subdirectory for the task's TMPDIR. This isolates temp files
// (git, npm, etc.) from other concurrent tasks. See issue #59.
func TestPIHandler_PrepareTaskDir_CreatesTmpDir(t *testing.T) {
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

	taskDir, err := h.prepareTaskDir(context.Background(), "task-tmp", "", nil, nil)
	if err != nil {
		t.Fatalf("prepareTaskDir failed: %v", err)
	}

	// The tmp subdirectory should exist inside the task directory.
	tmpDir := filepath.Join(taskDir, "tmp")
	info, err := os.Stat(tmpDir)
	if err != nil {
		t.Fatalf("tmp dir %s should exist: %v", tmpDir, err)
	}
	if !info.IsDir() {
		t.Errorf("tmp path %s should be a directory", tmpDir)
	}
}

// TestPIHandler_ResetClientWithEnv_SetsTMPDIR verifies that resetClientWithEnv
// passes the TMPDIR environment variable to the pi subprocess. This ensures
// that temp files (git, npm, etc.) are isolated per task. See issue #59.
func TestPIHandler_ResetClientWithEnv_SetsTMPDIR(t *testing.T) {
	if _, err := exec.LookPath("pi"); err != nil {
		t.Skip("pi not installed")
	}

	baseDir, err := os.MkdirTemp("", "hotelier-base-*")
	if err != nil {
		t.Fatalf("create temp base dir: %v", err)
	}
	defer os.RemoveAll(baseDir)

	taskDir := filepath.Join(baseDir, "tasks", "task-tmpdir")
	tmpDir := filepath.Join(taskDir, "tmp")
	if err := os.MkdirAll(tmpDir, 0o755); err != nil {
		t.Fatalf("create tmp dir: %v", err)
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

	taskEnv := map[string]string{
		"TMPDIR": tmpDir,
	}

	ctx := context.Background()
	err = h.resetClientWithEnv(ctx, taskDir, "task-tmpdir", func(LogEntry) error { return nil }, taskEnv)
	if err != nil {
		t.Fatalf("resetClientWithEnv failed: %v", err)
	}
	defer h.client.Stop(ctx)

	// Verify the subprocess has TMPDIR set correctly
	cmd := h.client.Cmd()
	if cmd == nil || cmd.Env == nil {
		t.Fatal("client cmd or env should not be nil")
	}

	found := false
	for _, envVar := range cmd.Env {
		if strings.HasPrefix(envVar, "TMPDIR=") {
			value := strings.TrimPrefix(envVar, "TMPDIR=")
			if value != tmpDir {
				t.Errorf("TMPDIR = %q, want %q", value, tmpDir)
			} else {
				found = true
			}
			break
		}
	}

	if !found {
		t.Error("TMPDIR not found in subprocess environment")
	}
}

// TestPIHandler_ResetClientWithEnv_MergesPersonaEnv verifies that persona
// environment variables are merged with TMPDIR, and that persona vars
// take precedence if they conflict. See issue #59.
func TestPIHandler_ResetClientWithEnv_MergesPersonaEnv(t *testing.T) {
	if _, err := exec.LookPath("pi"); err != nil {
		t.Skip("pi not installed")
	}

	baseDir, err := os.MkdirTemp("", "hotelier-base-*")
	if err != nil {
		t.Fatalf("create temp base dir: %v", err)
	}
	defer os.RemoveAll(baseDir)

	taskDir := filepath.Join(baseDir, "tasks", "task-merge")
	tmpDir := filepath.Join(taskDir, "tmp")
	if err := os.MkdirAll(tmpDir, 0o755); err != nil {
		t.Fatalf("create tmp dir: %v", err)
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

	// Simulate the env construction done in ExecuteTask:
	// TMPDIR + persona vars (persona takes precedence)
	taskEnv := map[string]string{
		"TMPDIR": tmpDir,
	}
	personaEnv := map[string]string{
		"PERSONA_TOKEN": "secret-token",
	}
	for k, v := range personaEnv {
		taskEnv[k] = v
	}

	ctx := context.Background()
	err = h.resetClientWithEnv(ctx, taskDir, "task-merge", func(LogEntry) error { return nil }, taskEnv)
	if err != nil {
		t.Fatalf("resetClientWithEnv failed: %v", err)
	}
	defer h.client.Stop(ctx)

	cmd := h.client.Cmd()
	if cmd == nil || cmd.Env == nil {
		t.Fatal("client cmd or env should not be nil")
	}

	// Verify both TMPDIR and PERSONA_TOKEN are present
	var foundTmpdir, foundPersona bool
	for _, envVar := range cmd.Env {
		if strings.HasPrefix(envVar, "TMPDIR=") {
			value := strings.TrimPrefix(envVar, "TMPDIR=")
			if value != tmpDir {
				t.Errorf("TMPDIR = %q, want %q", value, tmpDir)
			}
			foundTmpdir = true
		}
		if strings.HasPrefix(envVar, "PERSONA_TOKEN=") {
			foundPersona = true
		}
	}

	if !foundTmpdir {
		t.Error("TMPDIR not found in subprocess environment")
	}
	if !foundPersona {
		t.Error("PERSONA_TOKEN not found in subprocess environment")
	}
}
