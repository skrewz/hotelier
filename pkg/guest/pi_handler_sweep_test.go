package guest

import (
	"bytes"
	"context"
	"errors"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestPIHandler_Start_SweepsStaleTaskDirs verifies that Start() removes
// every entry under <baseCWD>/tasks/, including nested contents.
// Regression test for issue #180: orphaned task directories accumulate
// after a hard kill of the guest (SIGKILL, OOM) because ExecuteTask's
// deferred cleanup never runs.
func TestPIHandler_Start_SweepsStaleTaskDirs(t *testing.T) {
	if _, err := exec.LookPath("pi"); err != nil {
		t.Skip("pi not installed")
	}

	workDir := t.TempDir()

	// Pre-create stale entries as a hard-killed guest would leave them:
	// a task dir with contents (incl. a nested tmp/ scratch dir) and a
	// clone-* scratch dir from an interrupted cloneRepo.
	staleTask := filepath.Join(workDir, "tasks", "task-123-abcdef")
	staleTmp := filepath.Join(staleTask, "tmp")
	if err := os.MkdirAll(staleTmp, 0o755); err != nil {
		t.Fatalf("create stale task dir: %v", err)
	}
	for _, p := range []string{
		filepath.Join(staleTask, "AGENTS.md"),
		filepath.Join(staleTmp, "scratch.txt"),
	} {
		if err := os.WriteFile(p, []byte("stale"), 0o644); err != nil {
			t.Fatalf("write stale file %s: %v", p, err)
		}
	}
	staleClone := filepath.Join(workDir, "tasks", "clone-987654321")
	if err := os.MkdirAll(staleClone, 0o755); err != nil {
		t.Fatalf("create stale clone dir: %v", err)
	}

	h := NewPIHandler(workDir, "", "", "")
	if err := h.Start(context.Background()); err != nil {
		t.Fatalf("start failed: %v", err)
	}
	defer h.Stop(context.Background())

	if _, err := os.Stat(staleTask); !os.IsNotExist(err) {
		t.Errorf("stale task dir should be removed, stat err=%v", err)
	}
	if _, err := os.Stat(staleClone); !os.IsNotExist(err) {
		t.Errorf("stale clone dir should be removed, stat err=%v", err)
	}
}

// TestPIHandler_Start_SweepsStaleJails verifies that Start() also removes
// stale namespace jails (<taskDir>.jail) left behind by a hard-killed
// guest. Jails contain copies of the guest's credentials (~/.tokens,
// ~/.certs, ~/.forgejo-gitconfigs), so they must not survive a restart
// (review feedback on PR #174). The issue #180 sweep removes every entry
// under tasks/, which covers the jails — this test pins that down.
func TestPIHandler_Start_SweepsStaleJails(t *testing.T) {
	if _, err := exec.LookPath("pi"); err != nil {
		t.Skip("pi not installed")
	}

	workDir := t.TempDir()
	staleJail := filepath.Join(workDir, "tasks", "task-123-abcdef.jail")
	if err := os.MkdirAll(staleJail, 0o755); err != nil {
		t.Fatalf("create stale jail: %v", err)
	}
	// Credential-like content that must not survive a restart.
	if err := os.WriteFile(filepath.Join(staleJail, "tokens"), []byte("secret"), 0o600); err != nil {
		t.Fatalf("write stale credential: %v", err)
	}

	h := NewPIHandler(workDir, "", "", "")
	if err := h.Start(context.Background()); err != nil {
		t.Fatalf("start failed: %v", err)
	}
	defer h.Stop(context.Background())

	if _, err := os.Stat(staleJail); !os.IsNotExist(err) {
		t.Errorf("stale jail should be removed, stat err=%v", err)
	}
}

// TestPIHandler_Start_NoTasksDir verifies that Start() succeeds when
// <baseCWD>/tasks/ does not exist, and that the sweep does not create it.
// Documented choice for issue #180: the sweep only acts on an existing
// tasks/ directory; prepareTaskDir creates it on demand via MkdirAll.
func TestPIHandler_Start_NoTasksDir(t *testing.T) {
	if _, err := exec.LookPath("pi"); err != nil {
		t.Skip("pi not installed")
	}

	workDir := t.TempDir()
	tasksDir := filepath.Join(workDir, "tasks")
	if _, err := os.Stat(tasksDir); !os.IsNotExist(err) {
		t.Fatalf("expected tasks dir to be absent, stat err=%v", err)
	}

	h := NewPIHandler(workDir, "", "", "")
	if err := h.Start(context.Background()); err != nil {
		t.Fatalf("start failed: %v", err)
	}
	defer h.Stop(context.Background())

	if _, err := os.Stat(tasksDir); !os.IsNotExist(err) {
		t.Errorf("sweep must not create the tasks dir, stat err=%v", err)
	}
}

// TestPIHandler_Start_SweepRemovalErrorDoesNotFailStart verifies that a
// stale directory that cannot be removed does not fail Start(); the error
// is logged at info level and the guest starts anyway (issue #180). The
// removal failure is simulated via the injected removeDir seam — e.g. a
// read-only filesystem that enforces it in production.
func TestPIHandler_Start_SweepRemovalErrorDoesNotFailStart(t *testing.T) {
	if _, err := exec.LookPath("pi"); err != nil {
		t.Skip("pi not installed")
	}

	workDir := t.TempDir()
	stale := filepath.Join(workDir, "tasks", "task-456-fedcba")
	if err := os.MkdirAll(stale, 0o755); err != nil {
		t.Fatalf("create stale dir: %v", err)
	}

	h := NewPIHandler(workDir, "", "", "")
	h.removeDir = func(string) error { return errors.New("simulated removal failure") }

	// Capture the handler's log output to verify the error is logged.
	var logBuf bytes.Buffer
	h.log = log.New(&logBuf, "[pi-handler] ", log.LstdFlags)

	if err := h.Start(context.Background()); err != nil {
		t.Fatalf("start must not fail when a stale dir cannot be removed: %v", err)
	}
	defer h.Stop(context.Background())

	if _, err := os.Stat(stale); err != nil {
		t.Errorf("unremovable stale dir should still exist, stat err=%v", err)
	}
	if !strings.Contains(logBuf.String(), "simulated removal failure") {
		t.Errorf("removal error should be logged, got: %s", logBuf.String())
	}
}
