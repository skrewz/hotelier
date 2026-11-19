package guest

import (
	"context"
	"errors"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// fakeGitOnPath puts a `git` on PATH that satisfies the one call cloneRepo
// makes: `git clone <repo> <dir>`, which it "satisfies" by writing
// payload/README into <dir>. cloneRepo then moves payload into the task
// directory, so the tests below can observe the move without a network or a
// real repository. Same PATH-shadowing trick pi_handler_toolargs_test.go uses
// for the pi executable.
func fakeGitOnPath(t *testing.T) {
	t.Helper()
	if _, err := os.Stat("/bin/sh"); err != nil {
		t.Skip("/bin/sh not available, cannot run fake git")
	}
	bin := t.TempDir()
	script := "#!/bin/sh\nmkdir -p \"$3/payload\" && echo cloned > \"$3/payload/README\"\n"
	if err := os.WriteFile(filepath.Join(bin, "git"), []byte(script), 0o755); err != nil {
		t.Fatalf("write fake git: %v", err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
}

// newCloneTestTaskDir returns a task directory whose payload entry already
// exists — the state cloneRepo's stale-destination branch handles, which in
// production only persona.ApplyFiles can create (it runs before the clone and
// writes fc.To's parents).
func newCloneTestTaskDir(t *testing.T, baseDir string) string {
	t.Helper()
	taskDir := filepath.Join(baseDir, "tasks", "task-4-abc123456789")
	if err := os.MkdirAll(filepath.Join(taskDir, "payload"), 0o755); err != nil {
		t.Fatalf("create stale destination: %v", err)
	}
	return taskDir
}

// TestPIHandler_CloneRepo_RemovesStaleDestinationThroughRemoveDir pins the
// stale-destination branch itself: the entry that is in the way goes through
// the same forceful seam as every other tree under a task directory, and if
// that removal fails the clone fails naming the path rather than leaving a
// half-populated task directory (issue #301).
func TestPIHandler_CloneRepo_RemovesStaleDestinationThroughRemoveDir(t *testing.T) {
	fakeGitOnPath(t)
	baseDir := t.TempDir()
	taskDir := newCloneTestTaskDir(t, baseDir)
	dst := filepath.Join(taskDir, "payload")

	var removed []string
	h := &PIHandler{
		log:     log.New(io.Discard, "", 0),
		baseCWD: baseDir,
		removeDir: func(p string) error {
			removed = append(removed, p)
			return errors.New("permission denied")
		},
	}

	err := h.cloneRepo(context.Background(), taskDir, "https://example.invalid/repo.git", nil, nil)
	if err == nil {
		t.Fatal("cloneRepo should fail when a stale destination cannot be removed")
	}
	if !strings.Contains(err.Error(), dst) {
		t.Errorf("error should name the path that could not be removed, got: %v", err)
	}
	if len(removed) != 1 || removed[0] != dst {
		t.Errorf("stale destination should be removed once through the seam, got: %v", removed)
	}
	if _, err := os.Stat(filepath.Join(dst, "README")); !os.IsNotExist(err) {
		t.Errorf("the clone must not be moved into a destination removal failed to clear")
	}
}

// TestPIHandler_CloneRepo_RemovesReadOnlyStaleDestination verifies the branch
// survives a stale destination whose contents cannot be unlinked without
// restoring the owner write bits — the shape issue #301 is about.
func TestPIHandler_CloneRepo_RemovesReadOnlyStaleDestination(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("running as root; directory permissions are not enforced")
	}
	fakeGitOnPath(t)
	baseDir := t.TempDir()
	taskDir := newCloneTestTaskDir(t, baseDir)
	makeReadOnlySubtree(t, filepath.Join(taskDir, "payload"))

	h := NewPIHandler(baseDir, "", "", "")
	h.log = log.New(io.Discard, "", 0)
	if err := h.cloneRepo(context.Background(), taskDir, "https://example.invalid/repo.git", nil, nil); err != nil {
		t.Fatalf("cloneRepo failed on a read-only stale destination: %v", err)
	}

	data, err := os.ReadFile(filepath.Join(taskDir, "payload", "README"))
	if err != nil {
		t.Fatalf("cloned payload should have replaced the stale destination: %v", err)
	}
	if strings.TrimSpace(string(data)) != "cloned" {
		t.Errorf("stale destination contents should be gone, got: %q", data)
	}
}
