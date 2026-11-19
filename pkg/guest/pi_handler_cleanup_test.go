package guest

import (
	"bytes"
	"errors"
	"io"
	"log"
	"os"
	"path/filepath"
	"testing"

	"hotelier/pkg/jail"
)

// makeReadOnlySubtree creates a directory tree shaped like the Go module
// cache (0555 dirs, 0444 files) under root and returns the tree root.
func makeReadOnlySubtree(t *testing.T, root string) string {
	t.Helper()
	dir := filepath.Join(root, "go", "pkg", "mod", "example@v1.0.0")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("create %s: %v", dir, err)
	}
	if err := os.WriteFile(filepath.Join(dir, "README"), []byte("x"), 0o444); err != nil {
		t.Fatalf("write file: %v", err)
	}
	if err := os.Chmod(filepath.Join(root, "go", "pkg", "mod"), 0o555); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	if err := os.Chmod(dir, 0o555); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	return dir
}

// TestPIHandler_SweepRemovesStaleTaskDirWithReadOnlyTree verifies that the
// startup sweep removes a stale task dir containing read-only trees (issue
// #301).
func TestPIHandler_SweepRemovesStaleTaskDirWithReadOnlyTree(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("running as root; directory permissions are not enforced")
	}
	workDir := t.TempDir()
	stale := filepath.Join(workDir, "tasks", "task-123-abcdef.jail")
	if err := os.MkdirAll(stale, 0o755); err != nil {
		t.Fatalf("create stale jail root: %v", err)
	}
	makeReadOnlySubtree(t, stale)

	h := NewPIHandler(workDir, "", "", "")
	var logBuf bytes.Buffer
	h.log = log.New(&logBuf, "", 0)
	h.sweepStaleTaskDirs()

	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Errorf("stale jail root with read-only tree should be gone, stat err=%v; log: %s", err, logBuf.String())
	}
}

// TestPIHandler_CleanupTaskDir_RemovesReadOnlyTree verifies that end-of-task
// cleanup removes a task directory containing read-only trees (issue #301).
func TestPIHandler_CleanupTaskDir_RemovesReadOnlyTree(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("running as root; directory permissions are not enforced")
	}
	baseDir := t.TempDir()
	taskDir := filepath.Join(baseDir, "tasks", "task-9-abc123")
	if err := os.MkdirAll(taskDir, 0o755); err != nil {
		t.Fatalf("create task dir: %v", err)
	}
	makeReadOnlySubtree(t, taskDir)

	h := NewPIHandler(baseDir, "", "", "")
	if err := h.cleanupTaskDir(taskDir); err != nil {
		t.Fatalf("cleanupTaskDir failed: %v", err)
	}
	if _, err := os.Stat(taskDir); !os.IsNotExist(err) {
		t.Errorf("task dir with read-only tree should be gone, stat err=%v", err)
	}
}

// TestJailCleanupRemovesReadOnlyTree verifies that a jail root holding a
// read-only tree is removed (issue #301). It lives here rather than in
// pkg/jail to reuse the fixture above instead of rebuilding the tree.
func TestJailCleanupRemovesReadOnlyTree(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("running as root; directory permissions are not enforced")
	}
	root := filepath.Join(t.TempDir(), "task-1-abc.jail")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatalf("create jail root: %v", err)
	}
	makeReadOnlySubtree(t, filepath.Join(root, "home", "guestuser"))

	if err := jail.Cleanup(root); err != nil {
		t.Fatalf("jail.Cleanup failed: %v", err)
	}
	if _, err := os.Stat(root); !os.IsNotExist(err) {
		t.Errorf("jail root should be gone, stat err=%v", err)
	}
}

// TestPIHandler_CleanupArtifacts_EscalatesJailFailureToTaskLog verifies that
// the jail half of cleanup is escalated too. That half is the failure the
// issue actually logged — the leftovers were jail roots, holding the read-only
// home copies — so it must not be the half that can silently regress. A
// *jail.Jail built inside this package has an empty plan, so j.Cleanup()
// cannot fail there; jail removal is therefore passed in as a closure, and
// this test hands it one that fails.
func TestPIHandler_CleanupArtifacts_EscalatesJailFailureToTaskLog(t *testing.T) {
	var entries []LogEntry
	var journal bytes.Buffer
	h := &PIHandler{
		log:       log.New(&journal, "", 0),
		removeDir: os.RemoveAll,
	}
	jailRoot := filepath.Join(t.TempDir(), "task-3-abc.jail")
	if err := os.MkdirAll(jailRoot, 0o755); err != nil {
		t.Fatalf("create jail root: %v", err)
	}

	h.cleanupArtifacts("task-3", func() error { return errors.New("permission denied") }, jailRoot, "", func(e LogEntry) error {
		entries = append(entries, e)
		return nil
	})

	var escalated bool
	for _, e := range entries {
		if e.Level == "error" && e.TaskID == "task-3" &&
			bytes.Contains([]byte(e.Line), []byte("task-3-abc.jail")) {
			escalated = true
		}
	}
	if !escalated {
		t.Errorf("a failed jail removal should be sent as an error-level task log entry naming the jail root, got: %+v (journal: %s)", entries, journal.String())
	}
}

// TestPIHandler_CleanupArtifacts_EscalatesFailureToTaskLog verifies that a
// cleanup failure is not merely a journal line nobody reads: it is sent to
// the server as an error-level task log entry, so a guest whose disk is
// filling from undeletable leftovers is visible to the server (issue #301).
func TestPIHandler_CleanupArtifacts_EscalatesFailureToTaskLog(t *testing.T) {
	var entries []LogEntry
	h := &PIHandler{
		log:       log.New(io.Discard, "", 0),
		removeDir: func(string) error { return errors.New("permission denied") },
	}
	h.cleanupArtifacts("task-7", nil, "", filepath.Join(t.TempDir(), "tasks", "task-7-abc"), func(e LogEntry) error {
		entries = append(entries, e)
		return nil
	})
	var escalated bool
	for _, e := range entries {
		if e.Level == "error" && e.TaskID == "task-7" &&
			bytes.Contains([]byte(e.Line), []byte("task-7-abc")) {
			escalated = true
		}
	}
	if !escalated {
		t.Errorf("removal failure should be sent as an error-level task log entry naming the path, got: %+v", entries)
	}
}

// TestPIHandler_CleanupArtifacts_EscalationFailureIsJournalled verifies that
// a guest which cannot reach the server — sendLog fails between connections
// — still records the removal failure in its own journal, so the escalation
// is never dropped without a trace.
func TestPIHandler_CleanupArtifacts_EscalationFailureIsJournalled(t *testing.T) {
	var journal bytes.Buffer
	h := &PIHandler{
		log:       log.New(&journal, "", 0),
		removeDir: func(string) error { return errors.New("permission denied") },
	}
	h.cleanupArtifacts("task-9", nil, "", filepath.Join(t.TempDir(), "tasks", "task-9-abc"), func(LogEntry) error {
		return errors.New("send log: not connected")
	})
	if !bytes.Contains(journal.Bytes(), []byte("failed to escalate removal failure")) {
		t.Errorf("a failed escalation should be journalled, got: %s", journal.String())
	}
}

// TestPIHandler_CleanupArtifacts_SuccessIsSilent verifies the happy path
// adds no task-log noise: successful cleanup stays a journal-only event.
func TestPIHandler_CleanupArtifacts_SuccessIsSilent(t *testing.T) {
	baseDir := t.TempDir()
	taskDir := filepath.Join(baseDir, "tasks", "task-8-abc")
	if err := os.MkdirAll(taskDir, 0o755); err != nil {
		t.Fatalf("create task dir: %v", err)
	}

	var entries []LogEntry
	h := &PIHandler{
		log:       log.New(io.Discard, "", 0),
		baseCWD:   baseDir,
		removeDir: os.RemoveAll,
	}
	h.cleanupArtifacts("task-8", nil, "", taskDir, func(e LogEntry) error {
		entries = append(entries, e)
		return nil
	})
	if len(entries) != 0 {
		t.Errorf("successful cleanup should send no task log entries, got: %+v", entries)
	}
}
