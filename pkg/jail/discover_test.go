package jail

import (
	"os"
	"path/filepath"
	"testing"
)

// TestDiscover verifies host fact discovery against a synthetic layout:
// the pi root is the nearest node_modules ancestor, home dot-dirs are
// detected, and the usrmerge flag matches the actual host /bin.
func TestDiscover(t *testing.T) {
	base := t.TempDir()

	taskDir := filepath.Join(base, "task")
	if err := os.MkdirAll(taskDir, 0o755); err != nil {
		t.Fatal(err)
	}
	home := filepath.Join(base, "home")
	if err := os.MkdirAll(filepath.Join(home, ".pi"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(home, ".tokens"), 0o755); err != nil {
		t.Fatal(err)
	}
	// A fake pi under a node_modules tree.
	pi := filepath.Join(base, "install", "node_modules", "pi-coding-agent", "bin", "pi")
	if err := os.MkdirAll(filepath.Dir(pi), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(pi, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}

	f, err := Discover(taskDir, home, pi, nil)
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}

	if f.PiRoot != filepath.Join(base, "install", "node_modules") {
		t.Errorf("PiRoot = %q, want the node_modules ancestor", f.PiRoot)
	}
	// With no configured home copy set, the built-in default set is
	// probed (issue #201).
	if !contains(f.HomeCopyTrees, ".pi") || !contains(f.HomeCopyTrees, ".tokens") {
		t.Errorf("HomeCopyTrees = %v, want .pi and .tokens", f.HomeCopyTrees)
	}
	if contains(f.HomeCopyTrees, ".certs") {
		t.Errorf("HomeCopyTrees = %v, .certs was not created and must not appear", f.HomeCopyTrees)
	}
	// The usrmerge flag must match the actual host layout.
	fi, lerr := os.Lstat("/bin")
	hostUSRmerged := lerr != nil || fi.Mode()&os.ModeSymlink != 0
	if f.USRmerged != hostUSRmerged {
		t.Errorf("USRmerged = %v, host /bin says %v", f.USRmerged, hostUSRmerged)
	}
	// /usr must be among the bind dirs when it exists.
	if _, err := os.Stat("/usr"); err == nil && !contains(f.USRDirs, "/usr") {
		t.Errorf("USRDirs = %v, want /usr", f.USRDirs)
	}
	// /etc/hosts exists on any sane host and must be discovered.
	if _, err := os.Stat("/etc/hosts"); err == nil && !contains(f.EtcFiles, "/etc/hosts") {
		t.Errorf("EtcFiles = %v, want /etc/hosts", f.EtcFiles)
	}
	// /dev/null exists on any sane host and must be discovered (by name).
	if _, err := os.Stat("/dev/null"); err == nil && !contains(f.DevNodes, "null") {
		t.Errorf("DevNodes = %v, want null", f.DevNodes)
	}
}

// TestDiscover_PiRootFallback verifies that a pi outside any node_modules
// tree falls back to its parent directory as the bind root.
func TestDiscover_PiRootFallback(t *testing.T) {
	base := t.TempDir()
	taskDir := filepath.Join(base, "task")
	if err := os.MkdirAll(taskDir, 0o755); err != nil {
		t.Fatal(err)
	}
	binDir := filepath.Join(base, "bin")
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		t.Fatal(err)
	}
	pi := filepath.Join(binDir, "pi")
	if err := os.WriteFile(pi, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}

	f, err := Discover(taskDir, filepath.Join(base, "home"), pi, nil)
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if f.PiRoot != binDir {
		t.Errorf("PiRoot = %q, want the parent dir %q", f.PiRoot, binDir)
	}
}

// TestDiscover_RequiresTaskDir verifies the precondition.
func TestDiscover_RequiresTaskDir(t *testing.T) {
	if _, err := Discover("", "/home", "/usr/bin/pi", nil); err == nil {
		t.Error("Discover without a task dir should fail")
	}
}

// TestDiscover_RequiresPiPath verifies Discover fails when the pi path
// is empty (the handler resolves pi before calling Setup; this is the
// last line of defence).
func TestDiscover_RequiresPiPath(t *testing.T) {
	if _, err := Discover(t.TempDir(), "/home", "", nil); err == nil {
		t.Error("Discover without a pi path should fail")
	}
}

// TestDiscover_HomeCopiesConfigured verifies that a configured home copy
// set (issue #201) classifies entries by what they are on this guest:
// directories become tree copies, files (including dot-files and symlinks
// to files) become file copies, and entries that do not exist are skipped
// (best effort, as before).
func TestDiscover_HomeCopiesConfigured(t *testing.T) {
	base := t.TempDir()
	taskDir := filepath.Join(base, "task")
	if err := os.MkdirAll(taskDir, 0o755); err != nil {
		t.Fatal(err)
	}
	home := filepath.Join(base, "home")
	if err := os.MkdirAll(filepath.Join(home, ".pi"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(home, ".report-on-signal.yaml"), []byte("room: test\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	// A symlink to a file: probed via Stat (followed) and copied with
	// the target's content.
	if err := os.WriteFile(filepath.Join(base, "real-certs.conf"), []byte("certs\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(base, "real-certs.conf"), filepath.Join(home, ".client-cert.conf")); err != nil {
		t.Fatal(err)
	}
	// A fake pi (required, anything resolvable).
	binDir := filepath.Join(base, "bin")
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(binDir, "pi"), []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}

	f, err := Discover(taskDir, home, filepath.Join(binDir, "pi"),
		[]string{".pi", ".report-on-signal.yaml", ".client-cert.conf", "does-not-exist"})
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}

	if !contains(f.HomeCopyTrees, ".pi") {
		t.Errorf("HomeCopyTrees = %v, want .pi (a directory)", f.HomeCopyTrees)
	}
	for _, name := range []string{".report-on-signal.yaml", ".client-cert.conf"} {
		if !contains(f.HomeCopyFiles, name) {
			t.Errorf("HomeCopyFiles = %v, want %q (a file)", f.HomeCopyFiles, name)
		}
		if contains(f.HomeCopyTrees, name) {
			t.Errorf("HomeCopyTrees = %v, %q is a file and must not be a tree", f.HomeCopyTrees, name)
		}
	}
	// Best effort: a missing entry is skipped, not an error.
	if contains(f.HomeCopyFiles, "does-not-exist") || contains(f.HomeCopyTrees, "does-not-exist") {
		t.Error("a configured entry that does not exist must be skipped")
	}
	// An empty configured set is treated as "use the built-in default set".
	if err := os.MkdirAll(filepath.Join(home, ".tokens"), 0o755); err != nil {
		t.Fatal(err)
	}
	f2, err := Discover(taskDir, home, filepath.Join(binDir, "pi"), []string{})
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if !contains(f2.HomeCopyTrees, ".pi") || !contains(f2.HomeCopyTrees, ".tokens") {
		t.Errorf("HomeCopyTrees = %v, want the built-in default set for an empty config", f2.HomeCopyTrees)
	}
}
