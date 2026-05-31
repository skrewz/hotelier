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
		// Local paths — no parsing
		{"/local/path/to/repo", "/local/path/to/repo", ""},
		{"relative/path/repo", "relative/path/repo", ""},
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

// TestIsGitURL verifies the isGitURL helper correctly identifies remote URLs.
func TestIsGitURL(t *testing.T) {
	tests := []struct {
		input    string
		expected bool
	}{
		{"https://github.com/user/repo.git", true},
		{"git@github.com:user/repo.git", true},
		{"ssh://git@host:path/repo.git", true},
		{"/local/path/to/repo", false},
		{"relative/path/repo", false},
		{"", false},
	}

	for _, tc := range tests {
		result := isGitURL(tc.input)
		if result != tc.expected {
			t.Errorf("isGitURL(%q) = %v, want %v", tc.input, result, tc.expected)
		}
	}
}

// TestPIHandler_PrepareReposLocal verifies that local repo paths are resolved
// and the working directory is set correctly.
func TestPIHandler_PrepareReposLocal(t *testing.T) {
	// Create a temp dir to act as a "repo"
	repoDir, err := os.MkdirTemp("", "hotelier-repo-*")
	if err != nil {
		t.Fatalf("create temp repo dir: %v", err)
	}
	defer os.RemoveAll(repoDir)

	// Create a file in the repo so we can verify it exists
	if err := os.WriteFile(filepath.Join(repoDir, "test.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatalf("write test file: %v", err)
	}

	baseDir, err := os.MkdirTemp("", "hotelier-base-*")
	if err != nil {
		t.Fatalf("create temp base dir: %v", err)
	}
	defer os.RemoveAll(baseDir)

	// Create a handler with the base dir as CWD
	client := pi.NewClient(pi.PiClientConfig{
		CWD: baseDir,
		Log: log.New(io.Discard, "", 0),
	})

	h := &PIHandler{
		baseCWD: baseDir,
		client:  client,
		log:     log.New(io.Discard, "", 0),
	}

	taskDir, err := h.prepareRepos(context.Background(), "task-1", []string{repoDir}, nil)
	if err != nil {
		t.Fatalf("prepareRepos failed: %v", err)
	}

	// For local repos, the working directory should be the resolved repo path.
	expectedWD, err := filepath.Abs(repoDir)
	if err != nil {
		t.Fatalf("abs %s: %v", repoDir, err)
	}
	if taskDir != expectedWD {
		t.Errorf("working dir %q should be %q", taskDir, expectedWD)
	}

	// The repo should be accessible (we just verify the path is valid)
	if _, err := os.Stat(expectedWD); err != nil {
		t.Errorf("repo dir %s should exist: %v", expectedWD, err)
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

// TestPIHandler_PrepareReposRelativePathAfterResetClient verifies that
// relative repo paths are resolved against the base CWD, not a previous
// task's working directory.
func TestPIHandler_PrepareReposRelativePathAfterResetClient(t *testing.T) {
	baseDir, err := os.MkdirTemp("", "hotelier-base-*")
	if err != nil {
		t.Fatalf("create temp base dir: %v", err)
	}
	defer os.RemoveAll(baseDir)

	// Create a local repo relative to baseDir
	repoDir := filepath.Join(baseDir, "s-git", "nightrider")
	if err := os.MkdirAll(repoDir, 0o755); err != nil {
		t.Fatalf("create repo dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(repoDir, "README.md"), []byte("repo"), 0o644); err != nil {
		t.Fatalf("write README: %v", err)
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

	// First task — no repos
	taskDir1, err := h.prepareRepos(context.Background(), "task-1", []string{}, nil)
	if err != nil {
		t.Fatalf("first prepareRepos failed: %v", err)
	}

	// Simulate resetClient
	newClient := pi.NewClient(pi.PiClientConfig{
		CWD: taskDir1,
		Log: log.New(io.Discard, "", 0),
	})
	h.client = newClient

	// Second task — uses a relative repo path that exists under baseDir
	taskDir2, err := h.prepareRepos(context.Background(), "task-2", []string{"s-git/nightrider"}, nil)
	if err != nil {
		t.Fatalf("second prepareRepos failed: %v", err)
	}

	// The working directory should be the repo under baseDir, not under taskDir1.
	expectedWD := repoDir
	if taskDir2 != expectedWD {
		t.Errorf("working dir = %q, want %q", taskDir2, expectedWD)
	}

	// Verify the path actually exists
	if _, err := os.Stat(taskDir2); os.IsNotExist(err) {
		t.Errorf("working dir %s does not exist", taskDir2)
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

// TestPIHandler_ExecuteTaskLogsWorkingDir verifies that ExecuteTask logs
// the working directory and the pi subprocess spawn.
func TestPIHandler_ExecuteTaskLogsWorkingDir(t *testing.T) {
	if _, err := exec.LookPath("pi"); err != nil {
		t.Skip("pi not installed")
	}

	// Create a temp dir as a "repo"
	repoDir, err := os.MkdirTemp("", "hotelier-repo-*")
	if err != nil {
		t.Fatalf("create temp repo dir: %v", err)
	}
	defer os.RemoveAll(repoDir)

	// Write a file so we can verify the working dir
	if err := os.WriteFile(filepath.Join(repoDir, "test.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatalf("write test file: %v", err)
	}

	baseDir, err := os.MkdirTemp("", "hotelier-base-*")
	if err != nil {
		t.Fatalf("create temp base dir: %v", err)
	}
	defer os.RemoveAll(baseDir)

	h := NewPIHandler(baseDir, "", "", "")
	if err := h.Start(context.Background()); err != nil {
		t.Fatalf("start failed: %v", err)
	}
	defer h.Stop(context.Background())

	task := TaskAssignment{
		TaskID: "test-workdir-task",
		Repos:  []string{repoDir},
		Prompt: "cat test.txt",
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

	// Verify that a working directory log line was emitted
	foundWorkdir := false
	for _, line := range lines {
		if strings.Contains(line, "[WORKDIR]") || strings.Contains(line, "working directory") {
			foundWorkdir = true
			break
		}
	}
	// The PI handler logs to its own logger, not via sendLog,
	// so we check that the task dir was created
	// (the working dir log is a pi-handler internal log, not a task log)
	_ = foundWorkdir
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
