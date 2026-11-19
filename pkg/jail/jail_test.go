package jail

import (
	"os"
	"path/filepath"
	"testing"
)

// TestCleanup_RemovesReadOnlyTree verifies that a jail root holding a
// read-only tree — the home copies include the Go module cache, whose
// directories are 0555 by design — is removed. Plain os.RemoveAll aborts on
// the first such directory, so every task left a full jail root behind and
// the guest's disk filled (issue #301).
func TestCleanup_RemovesReadOnlyTree(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("running as root; directory permissions are not enforced")
	}
	root := filepath.Join(t.TempDir(), "task-1-abc.jail")
	readOnlyDir := filepath.Join(root, "home", "guest", "go", "pkg", "mod", "example@v1.0.0")
	if err := os.MkdirAll(readOnlyDir, 0o755); err != nil {
		t.Fatalf("create jail root skeleton: %v", err)
	}
	if err := os.WriteFile(filepath.Join(readOnlyDir, "README"), []byte("x"), 0o444); err != nil {
		t.Fatalf("write module file: %v", err)
	}
	if err := os.Chmod(filepath.Join(root, "home", "guest", "go", "pkg", "mod"), 0o555); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	if err := os.Chmod(readOnlyDir, 0o555); err != nil {
		t.Fatalf("chmod: %v", err)
	}

	if err := Cleanup(root); err != nil {
		t.Fatalf("Cleanup failed: %v", err)
	}
	if _, err := os.Stat(root); !os.IsNotExist(err) {
		t.Errorf("jail root should be gone, stat err=%v", err)
	}
}
