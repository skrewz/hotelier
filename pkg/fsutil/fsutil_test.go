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

// makeModuleCacheTree builds a directory tree shaped like a Go module cache —
// 0555 directories holding 0444 files — under root, and returns the tree's
// root path.
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
	}
	for _, f := range []string{
		filepath.Join(tree, "mod", "doc.txt"),
		filepath.Join(tree, "mod", "example@v1.0.0", ".circleci", "config.yml"),
	} {
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

// makeTreeWritable restores the owner write bit on every directory under
// root. A test that deliberately leaves an undeletable tree behind needs it,
// or t.TempDir's own cleanup fails on the leftovers.
func makeTreeWritable(t *testing.T, root string) {
	t.Helper()
	if err := filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
		if err == nil && d.IsDir() {
			_ = os.Chmod(p, 0o755)
		}
		return nil
	}); err != nil {
		t.Fatalf("chmod tree %s: %v", root, err)
	}
}

// TestRemoveAllForce_RemovesReadOnlyTree pins the issue #301 regression: a
// tree of read-only directories is removed, where plain os.RemoveAll is not
// enough. The first half asserts that plain os.RemoveAll really does fail on
// this shape, so the second half cannot pass vacuously.
func TestRemoveAllForce_RemovesReadOnlyTree(t *testing.T) {
	skipIfRoot(t)

	plainRoot := t.TempDir()
	t.Cleanup(func() { makeTreeWritable(t, plainRoot) })
	if err := os.RemoveAll(makeModuleCacheTree(t, plainRoot)); err == nil {
		t.Fatal("os.RemoveAll unexpectedly removed a read-only tree; environment does not reproduce the bug")
	}

	forceRoot := t.TempDir()
	tree := makeModuleCacheTree(t, forceRoot)
	if err := RemoveAllForce(tree); err != nil {
		t.Fatalf("RemoveAllForce failed: %v", err)
	}
	if _, err := os.Stat(tree); !os.IsNotExist(err) {
		t.Errorf("read-only tree should be gone, stat err=%v", err)
	}
}

// TestRemoveAllForce_PropagatesUnfixableError verifies that a permission
// error chmod cannot fix — the tree's parent denies removal of the tree root
// itself — is still returned rather than swallowed.
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
