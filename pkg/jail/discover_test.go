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

	f, err := Discover(taskDir, home, pi)
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}

	if f.PiRoot != filepath.Join(base, "install", "node_modules") {
		t.Errorf("PiRoot = %q, want the node_modules ancestor", f.PiRoot)
	}
	if !contains(f.HomeDotDirs, ".pi") || !contains(f.HomeDotDirs, ".tokens") {
		t.Errorf("HomeDotDirs = %v, want .pi and .tokens", f.HomeDotDirs)
	}
	if contains(f.HomeDotDirs, ".certs") {
		t.Errorf("HomeDotDirs = %v, .certs was not created and must not appear", f.HomeDotDirs)
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

	f, err := Discover(taskDir, filepath.Join(base, "home"), pi)
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if f.PiRoot != binDir {
		t.Errorf("PiRoot = %q, want the parent dir %q", f.PiRoot, binDir)
	}
}

// TestDiscover_RequiresTaskDir verifies the precondition.
func TestDiscover_RequiresTaskDir(t *testing.T) {
	if _, err := Discover("", "/home", "/usr/bin/pi"); err == nil {
		t.Error("Discover without a task dir should fail")
	}
}

// TestDiscover_RequiresPiPath verifies Discover fails when the pi path
// is empty (the handler resolves pi before calling Setup; this is the
// last line of defence).
func TestDiscover_RequiresPiPath(t *testing.T) {
	if _, err := Discover(t.TempDir(), "/home", ""); err == nil {
		t.Error("Discover without a pi path should fail")
	}
}
