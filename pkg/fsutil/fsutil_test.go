package fsutil

import (
	"os"
	"path/filepath"
	"testing"
)

// skipIfRoot skips tests that rely on directory permissions being enforced;
// root bypasses them entirely.
func skipIfRoot(t *testing.T) {
	t.Helper()
	if os.Geteuid() == 0 {
		t.Skip("running as root; directory permissions are not enforced")
	}
}

// makeModuleCacheTree builds a directory tree shaped like a Go module
// cache — the layout that broke cleanup in issue #301: 0555 directories
// (read-only by design) holding 0444 files — under root, and returns the
// tree's root path.
func makeModuleCacheTree(t *testing.T, root string) string {
	t.Helper()
	tree := filepath.Join(root, "pkg")
	dirs := []string{
		filepath.Join(tree, "mod"),
		filepath.Join(tree, "mod", "example@v1.0.0"),
		filepath.Join(tree, "mod", "example@v1.0.0", ".circleci"),
	}
	for _, d := range dirs {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatalf("create %s: %v", d, err)
		}
		// Chmod explicitly: when rebuilding after a failed removal the
		// dirs already exist and still carry their 0555 modes, which
		// would deny the file writes below.
		if err := os.Chmod(d, 0o755); err != nil {
			t.Fatalf("chmod %s: %v", d, err)
		}
	}
	for _, f := range []string{
		filepath.Join(tree, "mod", "doc.txt"),
		filepath.Join(tree, "mod", "example@v1.0.0", ".circleci", "config.yml"),
	} {
		// Unlink first: on a rebuild after a failed removal the file still
		// exists with mode 0444, which os.WriteFile would refuse to open
		// for writing.
		_ = os.Remove(f)
		if err := os.WriteFile(f, []byte("read-only"), 0o444); err != nil {
			t.Fatalf("write %s: %v", f, err)
		}
	}
	for _, d := range dirs {
		if err := os.Chmod(d, 0o555); err != nil {
			t.Fatalf("chmod %s: %v", d, err)
		}
	}
	return tree
}

// TestRemoveAllForce_RemovesReadOnlyTree verifies that a tree shaped like a
// Go module cache is removed. Plain os.RemoveAll aborts on the first entry
// inside a 0555 directory with "permission denied" — unlinking an entry
// needs write permission on the directory it sits in — so the tree survives
// (issue #301: such leftovers accumulated once per task until the guest's
// disk filled).
func TestRemoveAllForce_RemovesReadOnlyTree(t *testing.T) {
	skipIfRoot(t)
	root := t.TempDir()
	tree := makeModuleCacheTree(t, root)

	// Pin the regression: os.RemoveAll must fail on this tree, otherwise
	// the assertion below could pass vacuously.
	if err := os.RemoveAll(tree); err == nil {
		t.Fatal("os.RemoveAll unexpectedly removed a read-only tree; environment does not reproduce the bug")
	}

	// The failed removal leaves a partial tree with read-only dirs still
	// in place; restore writability and rebuild it to its original shape.
	filepath.WalkDir(tree, func(p string, d os.DirEntry, err error) error {
		if err == nil && d.IsDir() {
			_ = os.Chmod(p, 0o755)
		}
		return nil
	})
	tree = makeModuleCacheTree(t, root)

	if err := RemoveAllForce(tree); err != nil {
		t.Fatalf("RemoveAllForce failed: %v", err)
	}
	if _, err := os.Stat(tree); !os.IsNotExist(err) {
		t.Errorf("read-only tree should be gone, stat err=%v", err)
	}
}

// TestRemoveAllForce_MissingPathIsNotAnError pins RemoveAll's
// nil-on-missing-path contract, which the cleanup paths rely on.
func TestRemoveAllForce_MissingPathIsNotAnError(t *testing.T) {
	if err := RemoveAllForce(filepath.Join(t.TempDir(), "absent")); err != nil {
		t.Errorf("missing path should not be an error, got: %v", err)
	}
}

// TestRemoveAllForce_RemovesOrdinaryTree verifies the fast path: a writable
// tree is removed without the chmod retry.
func TestRemoveAllForce_RemovesOrdinaryTree(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "a", "b")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("create tree: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "f"), []byte("x"), 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}
	if err := RemoveAllForce(root); err != nil {
		t.Fatalf("RemoveAllForce failed: %v", err)
	}
	if _, err := os.Stat(root); !os.IsNotExist(err) {
		t.Errorf("tree should be gone, stat err=%v", err)
	}
}

// TestRemoveAllForce_PropagatesUnfixableError verifies that a permission
// error chmod cannot fix — the tree's parent denies removal of the tree
// root itself — is still returned rather than swallowed.
func TestRemoveAllForce_PropagatesUnfixableError(t *testing.T) {
	skipIfRoot(t)
	parent := filepath.Join(t.TempDir(), "parent")
	tree := filepath.Join(parent, "tree")
	if err := os.MkdirAll(tree, 0o755); err != nil {
		t.Fatalf("create tree: %v", err)
	}
	if err := os.WriteFile(filepath.Join(tree, "f"), []byte("x"), 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}
	if err := os.Chmod(parent, 0o555); err != nil {
		t.Fatalf("chmod parent: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(parent, 0o755) })

	if err := RemoveAllForce(tree); err == nil {
		t.Error("expected a permission error when the parent denies removal")
	}
	if _, err := os.Stat(tree); err != nil {
		t.Errorf("tree root should still exist (only its contents could go), stat err=%v", err)
	}
}
