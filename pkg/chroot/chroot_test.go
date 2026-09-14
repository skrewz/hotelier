package chroot

import (
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// newTestJail creates a jail in a temp dir with Setup() already called.
func newTestJail(t *testing.T) *Jail {
	t.Helper()
	root := filepath.Join(t.TempDir(), "jail")
	j := NewJail(root, log.New(io.Discard, "", 0))
	if err := j.Setup(); err != nil {
		t.Fatalf("Setup failed: %v", err)
	}
	return j
}

// writeHostFile writes a file on the "host" and returns its absolute path.
func writeHostFile(t *testing.T, dir, name, content string, mode os.FileMode) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(path, []byte(content), mode); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	return path
}

func TestJail_Setup_CreatesRoot(t *testing.T) {
	root := filepath.Join(t.TempDir(), "jail")
	j := NewJail(root, log.New(io.Discard, "", 0))
	if err := j.Setup(); err != nil {
		t.Fatalf("Setup failed: %v", err)
	}
	info, err := os.Stat(root)
	if err != nil {
		t.Fatalf("jail root should exist: %v", err)
	}
	if !info.IsDir() {
		t.Fatal("jail root should be a directory")
	}
	if j.Path() != root {
		t.Errorf("Path() = %q, want %q", j.Path(), root)
	}
}

func TestJail_Cleanup_RemovesRoot(t *testing.T) {
	j := newTestJail(t)
	writeHostFile(t, j.root, "etc/hostname", "host", 0o644)
	if err := j.Cleanup(); err != nil {
		t.Fatalf("Cleanup failed: %v", err)
	}
	if _, err := os.Stat(j.root); !os.IsNotExist(err) {
		t.Fatal("jail root should be removed by Cleanup")
	}
	// Idempotent — second call must not error.
	if err := j.Cleanup(); err != nil {
		t.Errorf("second Cleanup should be a no-op, got: %v", err)
	}
}

func TestJail_CopyFile_PreservesPermissions(t *testing.T) {
	j := newTestJail(t)
	src := writeHostFile(t, t.TempDir(), "bin/tool", "#!/bin/sh\necho hi\n", 0o755)
	if err := j.CopyFile(src, "/usr/bin/tool"); err != nil {
		t.Fatalf("CopyFile failed: %v", err)
	}
	dst := filepath.Join(j.root, "usr/bin/tool")
	info, err := os.Stat(dst)
	if err != nil {
		t.Fatalf("copied file should exist: %v", err)
	}
	if info.Mode().Perm() != 0o755 {
		t.Errorf("mode = %o, want 0755", info.Mode().Perm())
	}
	data, err := os.ReadFile(dst)
	if err != nil {
		t.Fatalf("read copied file: %v", err)
	}
	if string(data) != "#!/bin/sh\necho hi\n" {
		t.Errorf("content = %q", data)
	}
}

func TestJail_CopyFile_ResolvesSymlinks(t *testing.T) {
	j := newTestJail(t)
	hostDir := t.TempDir()
	real := writeHostFile(t, hostDir, "real/tool", "real-content", 0o755)
	link := filepath.Join(hostDir, "link/tool")
	if err := os.MkdirAll(filepath.Dir(link), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(real, link); err != nil {
		t.Fatal(err)
	}
	if err := j.CopyFile(link, "/usr/bin/tool"); err != nil {
		t.Fatalf("CopyFile on symlink failed: %v", err)
	}
	dst := filepath.Join(j.root, "usr/bin/tool")
	info, err := os.Lstat(dst)
	if err != nil {
		t.Fatalf("copied file should exist: %v", err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		t.Error("CopyFile should resolve the symlink and copy content, not the link")
	}
	data, _ := os.ReadFile(dst)
	if string(data) != "real-content" {
		t.Errorf("content = %q, want resolved target content", data)
	}
}

func TestJail_CopyFile_Errors(t *testing.T) {
	j := newTestJail(t)

	if err := j.CopyFile("/nonexistent/src/file", "/dst"); err == nil {
		t.Error("CopyFile with missing source should fail")
	}
	if err := j.CopyFile(t.TempDir(), "/dst"); err == nil {
		t.Error("CopyFile with a directory source should fail")
	}
	if err := j.CopyFile("/etc/hostname", "/"); err == nil {
		t.Error("CopyFile with root destination should fail")
	}
}

func TestJail_CopyDir_Recursive_PreservesSymlinks(t *testing.T) {
	j := newTestJail(t)
	hostDir := t.TempDir()
	writeHostFile(t, hostDir, "a/file1", "one", 0o644)
	writeHostFile(t, hostDir, "a/b/file2", "two", 0o600)
	// A relative symlink inside the tree must be preserved as a symlink.
	if err := os.Symlink("file1", filepath.Join(hostDir, "a/link-to-file1")); err != nil {
		t.Fatal(err)
	}

	if err := j.CopyDir(hostDir, "/task"); err != nil {
		t.Fatalf("CopyDir failed: %v", err)
	}

	dstA := filepath.Join(j.root, "task/a")
	data, err := os.ReadFile(filepath.Join(dstA, "file1"))
	if err != nil || string(data) != "one" {
		t.Errorf("file1: data=%q err=%v", data, err)
	}
	data, err = os.ReadFile(filepath.Join(dstA, "b/file2"))
	if err != nil || string(data) != "two" {
		t.Errorf("file2: data=%q err=%v", data, err)
	}
	info, err := os.Lstat(filepath.Join(dstA, "link-to-file1"))
	if err != nil {
		t.Fatalf("symlink should be preserved: %v", err)
	}
	if info.Mode()&os.ModeSymlink == 0 {
		t.Error("relative symlink should be preserved as a symlink")
	}
	if target, _ := os.Readlink(filepath.Join(dstA, "link-to-file1")); target != "file1" {
		t.Errorf("symlink target = %q, want %q", target, "file1")
	}
}

func TestJail_CopyDir_Errors(t *testing.T) {
	j := newTestJail(t)

	if err := j.CopyDir("/nonexistent/src", "/dst"); err == nil {
		t.Error("CopyDir with missing source should fail")
	}
	srcFile := writeHostFile(t, t.TempDir(), "file", "x", 0o644)
	if err := j.CopyDir(srcFile, "/dst"); err == nil {
		t.Error("CopyDir with a file source should fail")
	}
	if err := j.CopyDir(t.TempDir(), "/"); err == nil {
		t.Error("CopyDir with root destination should fail")
	}
}

func TestJail_CopyDirResolved_FollowsSymlinks(t *testing.T) {
	j := newTestJail(t)
	hostDir := t.TempDir()
	writeHostFile(t, hostDir, "real/target.txt", "target-content", 0o644)
	// Symlink to a file: resolved copy must contain the target's content.
	if err := os.Symlink(filepath.Join(hostDir, "real/target.txt"), filepath.Join(hostDir, "link.txt")); err != nil {
		t.Fatal(err)
	}
	// Symlink to a directory: resolved copy must contain the dir's contents.
	if err := os.Symlink(filepath.Join(hostDir, "real"), filepath.Join(hostDir, "dirlink")); err != nil {
		t.Fatal(err)
	}

	if err := j.CopyDirResolved(hostDir, "/etc/ssl/certs"); err != nil {
		t.Fatalf("CopyDirResolved failed: %v", err)
	}

	data, err := os.ReadFile(filepath.Join(j.root, "etc/ssl/certs/link.txt"))
	if err != nil || string(data) != "target-content" {
		t.Errorf("file symlink not resolved: data=%q err=%v", data, err)
	}
	data, err = os.ReadFile(filepath.Join(j.root, "etc/ssl/certs/dirlink/target.txt"))
	if err != nil || string(data) != "target-content" {
		t.Errorf("dir symlink not resolved: data=%q err=%v", data, err)
	}
}

// withFakePath prepends dir to PATH for the duration of the test.
func withFakePath(t *testing.T, dir string) {
	t.Helper()
	orig := os.Getenv("PATH")
	t.Cleanup(func() { os.Setenv("PATH", orig) })
	os.Setenv("PATH", dir+string(os.PathListSeparator)+orig)
}

func TestJail_CopyBinary_CopiesToSamePath(t *testing.T) {
	j := newTestJail(t)
	hostPath, err := exec.LookPath("env")
	if err != nil {
		t.Skip("env not in PATH")
	}
	got, err := j.CopyBinary("env")
	if err != nil {
		t.Fatalf("CopyBinary(env) failed: %v", err)
	}
	if got != hostPath {
		t.Errorf("CopyBinary returned %q, want %q", got, hostPath)
	}
	if _, err := os.Stat(filepath.Join(j.root, hostPath)); err != nil {
		t.Fatalf("binary should be copied to its host path in the jail: %v", err)
	}
	// Shared libraries must be copied to their host paths as well.
	out, err := exec.Command("ldd", hostPath).Output()
	if err != nil {
		t.Skipf("ldd failed on %s: %v", hostPath, err)
	}
	foundLib := false
	for _, line := range strings.Split(string(out), "\n") {
		if lib := extractLibraryPath(line); lib != "" {
			if _, err := os.Stat(filepath.Join(j.root, lib)); err == nil {
				foundLib = true
				break
			}
		}
	}
	if !foundLib {
		t.Error("expected at least one shared library to be copied into the jail")
	}
}

func TestJail_CopyBinary_NotFound(t *testing.T) {
	j := newTestJail(t)
	if _, err := j.CopyBinary("definitely-not-a-real-binary-xyz"); err == nil {
		t.Error("CopyBinary with unknown name should fail")
	}
}

func TestJail_PopulateEssentialBins(t *testing.T) {
	j := newTestJail(t)
	if err := j.PopulateEssentialBins(); err != nil {
		t.Fatalf("PopulateEssentialBins failed: %v", err)
	}
	for _, name := range []string{"env", "sh", "cat", "bash"} {
		hostPath, err := exec.LookPath(name)
		if err != nil {
			continue // not on this system — nothing to check
		}
		if _, err := os.Stat(filepath.Join(j.root, hostPath)); err != nil {
			t.Errorf("essential binary %s should be in the jail at %s", name, hostPath)
		}
	}
}

// makeFakePiPackage creates a fake npm-style pi installation:
//
//	bin/pi -> ../lib/node_modules/fakepkg/dist/cli.js
//	lib/node_modules/fakepkg/{package.json,dist/cli.js,dist/chunks/a.js}
//
// and returns the bin dir (to prepend to PATH).
func makeFakePiPackage(t *testing.T) string {
	t.Helper()
	base := t.TempDir()
	binDir := filepath.Join(base, "node_modules", "bin")
	pkgDir := filepath.Join(base, "node_modules", "lib", "node_modules", "fakepkg")
	distDir := filepath.Join(pkgDir, "dist")
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(distDir, "chunks"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(pkgDir, "package.json"), []byte(`{"name":"fakepkg","bin":{"pi":"dist/cli.js"}}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(distDir, "cli.js"), []byte("#!/usr/bin/env node\nconsole.log('fake pi')\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(distDir, "chunks", "a.js"), []byte("chunk-a\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("../lib/node_modules/fakepkg/dist/cli.js", filepath.Join(binDir, "pi")); err != nil {
		t.Fatal(err)
	}
	return binDir
}

func TestJail_PopulatePi_Package(t *testing.T) {
	j := newTestJail(t)
	binDir := makeFakePiPackage(t)
	withFakePath(t, binDir)

	if err := j.PopulatePi(); err != nil {
		t.Fatalf("PopulatePi failed: %v", err)
	}

	piHostPath := filepath.Join(binDir, "pi")
	piDst := filepath.Join(j.root, piHostPath)
	info, err := os.Lstat(piDst)
	if err != nil {
		t.Fatalf("pi bin should be copied to %s: %v", piDst, err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		t.Error("pi bin in the jail should be a regular file, not a symlink")
	}

	// The whole package must be copied at its host path.
	pkgInJail := filepath.Join(j.root, filepath.Clean(filepath.Join(binDir, "..", "lib", "node_modules", "fakepkg")))
	for _, rel := range []string{"package.json", "dist/cli.js", "dist/chunks/a.js"} {
		if _, err := os.Stat(filepath.Join(pkgInJail, rel)); err != nil {
			t.Errorf("package file %s should be in the jail: %v", rel, err)
		}
	}
}

func TestJail_PopulatePi_PlainScript(t *testing.T) {
	j := newTestJail(t)
	binDir := t.TempDir()
	writeHostFile(t, binDir, "pi", "#!/bin/sh\necho fake pi\n", 0o755)
	withFakePath(t, binDir)

	if err := j.PopulatePi(); err != nil {
		t.Fatalf("PopulatePi failed: %v", err)
	}
	dst := filepath.Join(j.root, binDir, "pi")
	if _, err := os.Stat(dst); err != nil {
		t.Fatalf("plain pi script should be copied to its host path: %v", err)
	}
}

func TestJail_PopulatePi_NotFound(t *testing.T) {
	j := newTestJail(t)
	// Point PATH at an empty dir so "pi" cannot be found.
	empty := t.TempDir()
	orig := os.Getenv("PATH")
	t.Cleanup(func() { os.Setenv("PATH", orig) })
	os.Setenv("PATH", empty)
	if err := j.PopulatePi(); err == nil {
		t.Error("PopulatePi should fail when pi is not in PATH")
	}
}

func TestJail_PopulateHome(t *testing.T) {
	j := newTestJail(t)
	home := t.TempDir()
	for _, dir := range []string{".pi", ".certs", ".forgejo-gitconfigs", ".tokens"} {
		writeHostFile(t, home, dir+"/file.txt", "content-of-"+dir, 0o600)
	}
	origHome := os.Getenv("HOME")
	t.Cleanup(func() { os.Setenv("HOME", origHome) })
	os.Setenv("HOME", home)

	if err := j.PopulateHome(); err != nil {
		t.Fatalf("PopulateHome failed: %v", err)
	}
	for _, dir := range []string{".pi", ".certs", ".forgejo-gitconfigs", ".tokens"} {
		dst := filepath.Join(j.root, home, dir, "file.txt")
		data, err := os.ReadFile(dst)
		if err != nil || string(data) != "content-of-"+dir {
			t.Errorf("%s not copied: data=%q err=%v", dir, data, err)
		}
	}
}

func TestJail_PopulateHome_MissingDirsSkipped(t *testing.T) {
	j := newTestJail(t)
	home := t.TempDir() // empty home — no dotdirs at all
	origHome := os.Getenv("HOME")
	t.Cleanup(func() { os.Setenv("HOME", origHome) })
	os.Setenv("HOME", home)
	if err := j.PopulateHome(); err != nil {
		t.Errorf("PopulateHome with empty home should not fail: %v", err)
	}
}

func TestJail_PopulateEtc(t *testing.T) {
	j := newTestJail(t)
	if err := j.PopulateEtc(); err != nil {
		t.Fatalf("PopulateEtc failed: %v", err)
	}
	// These files exist on any Linux system.
	for _, f := range []string{"/etc/resolv.conf", "/etc/hosts", "/etc/passwd"} {
		if _, err := os.Stat(f); err != nil {
			continue // not on host — nothing to check
		}
		if _, err := os.Stat(filepath.Join(j.root, f)); err != nil {
			t.Errorf("%s should be copied into the jail: %v", f, err)
		}
	}
}

func TestJail_PopulateDev(t *testing.T) {
	j := newTestJail(t)
	if err := j.PopulateDev(); err != nil {
		t.Fatalf("PopulateDev should be best-effort and not fail: %v", err)
	}
	// euid 0 is not a reliable proxy for mknod(2) permission: container
	// runtimes (e.g. rootless Podman) can hold euid 0 while blocking
	// mknod. Check the actual capability instead.
	if ok, err := CanMknod(); err == nil && ok {
		for _, dev := range []string{"/dev/null", "/dev/zero", "/dev/urandom"} {
			if _, err := os.Stat(filepath.Join(j.root, dev)); err != nil {
				t.Errorf("with CAP_MKNOD, %s should exist in the jail: %v", dev, err)
			}
		}
	}
}

func TestJail_CopyTaskDir(t *testing.T) {
	j := newTestJail(t)
	taskDir := filepath.Join(t.TempDir(), "tasks", "task-1", "abc123")
	writeHostFile(t, taskDir, "repo/main.go", "package main\n", 0o644)
	writeHostFile(t, taskDir, "tmp/.keep", "", 0o644)
	if err := os.Symlink("main.go", filepath.Join(taskDir, "repo/alias.go")); err != nil {
		t.Fatal(err)
	}

	if err := j.CopyTaskDir(taskDir); err != nil {
		t.Fatalf("CopyTaskDir failed: %v", err)
	}
	// Mirrored at the same absolute path inside the jail.
	if _, err := os.Stat(filepath.Join(j.root, taskDir, "repo/main.go")); err != nil {
		t.Errorf("task dir not mirrored at its host path: %v", err)
	}
	if info, err := os.Lstat(filepath.Join(j.root, taskDir, "repo/alias.go")); err != nil || info.Mode()&os.ModeSymlink == 0 {
		t.Errorf("symlink in task dir should be preserved: info=%v err=%v", info, err)
	}
}

// TestJail_PopulateEssentialBins_CopiesPythonStdlib verifies that the
// python3 standard library — which ldd never reports, because it is not a
// shared library — is copied into the jail. Without it, `python3 -c
// "import json"` fails inside the jail with ModuleNotFoundError (review
// feedback on PR #174).
func TestJail_PopulateEssentialBins_CopiesPythonStdlib(t *testing.T) {
	j := newTestJail(t)

	// A fake stdlib tree at an absolute host path.
	stdlib := filepath.Join(t.TempDir(), "lib", "python3.99")
	writeHostFile(t, stdlib, "json/__init__.py", "json\n", 0o644)
	writeHostFile(t, stdlib, "os.py", "os\n", 0o644)

	// A fake python3 that answers the sysconfig query with the stdlib path.
	binDir := t.TempDir()
	writeHostFile(t, binDir, "python3", "#!/bin/sh\necho "+stdlib+"\n", 0o755)
	withFakePath(t, binDir)

	if err := j.PopulateEssentialBins(); err != nil {
		t.Fatalf("PopulateEssentialBins failed: %v", err)
	}

	for _, rel := range []string{"json/__init__.py", "os.py"} {
		if _, err := os.Stat(filepath.Join(j.root, stdlib, rel)); err != nil {
			t.Errorf("python stdlib file %s should be in the jail: %v", rel, err)
		}
	}
}

// TestJail_PopulateEssentialBins_CopiesGitExecPath verifies that git's
// plumbing (the directory reported by `git --exec-path`, e.g.
// /usr/lib/git-core) is copied into the jail. The old sibling-directory
// heuristic looked for <git-dir>/git-core, which does not exist on either
// the Debian or the Fedora layout (review feedback on PR #174).
func TestJail_PopulateEssentialBins_CopiesGitExecPath(t *testing.T) {
	j := newTestJail(t)

	execPath := filepath.Join(t.TempDir(), "lib", "git-core")
	writeHostFile(t, execPath, "git-upload-pack", "#!/bin/sh\n", 0o755)
	writeHostFile(t, execPath, "git-gc", "#!/bin/sh\n", 0o755)

	// A fake git that answers --exec-path with the exec path.
	binDir := t.TempDir()
	writeHostFile(t, binDir, "git", "#!/bin/sh\necho "+execPath+"\n", 0o755)
	withFakePath(t, binDir)

	if err := j.PopulateEssentialBins(); err != nil {
		t.Fatalf("PopulateEssentialBins failed: %v", err)
	}

	for _, name := range []string{"git-upload-pack", "git-gc"} {
		if _, err := os.Stat(filepath.Join(j.root, execPath, name)); err != nil {
			t.Errorf("git exec-path file %s should be in the jail: %v", name, err)
		}
	}
}

// TestJail_CopyDirResolved_AncestorSymlinkTerminates verifies that
// following a symlink that points back at an ancestor of the tree being
// copied does not recurse unboundedly — a symlink to / inside ~/.pi or
// the pi package would otherwise copy the entire host filesystem into the
// jail (review feedback on PR #174).
func TestJail_CopyDirResolved_AncestorSymlinkTerminates(t *testing.T) {
	j := newTestJail(t)
	hostDir := t.TempDir()
	writeHostFile(t, hostDir, "top.txt", "top", 0o644)
	if err := os.MkdirAll(filepath.Join(hostDir, "a"), 0o755); err != nil {
		t.Fatal(err)
	}
	// a/loop points back at the tree root.
	if err := os.Symlink(hostDir, filepath.Join(hostDir, "a", "loop")); err != nil {
		t.Fatal(err)
	}

	if err := j.CopyDirResolved(hostDir, "/tree"); err != nil {
		t.Fatalf("CopyDirResolved failed: %v", err)
	}

	// Regular content is copied...
	if _, err := os.Stat(filepath.Join(j.root, "tree/top.txt")); err != nil {
		t.Errorf("top.txt should be copied: %v", err)
	}
	// ...and the cycle is detected and skipped, not re-expanded.
	if _, err := os.Stat(filepath.Join(j.root, "tree/a/loop")); !os.IsNotExist(err) {
		t.Errorf("ancestor symlink should be skipped to avoid unbounded recursion, stat err=%v", err)
	}
}

// TestJail_PopulateSSLCerts_RealDir verifies that when the certs path is
// already a real directory (Debian), its content is copied at its own path
// and no spurious symlink is created or logged (review feedback on PR #174).
func TestJail_PopulateSSLCerts_RealDir(t *testing.T) {
	j := newTestJail(t)
	base := t.TempDir()
	realDir := filepath.Join(base, "certs-real")
	writeHostFile(t, realDir, "crt1.pem", "cert", 0o644)

	if err := j.populateSSLCerts(realDir); err != nil {
		t.Fatalf("populateSSLCerts failed: %v", err)
	}
	if _, err := os.Stat(filepath.Join(j.root, realDir, "crt1.pem")); err != nil {
		t.Errorf("cert not copied at its real path: %v", err)
	}
	info, err := os.Lstat(filepath.Join(j.root, realDir))
	if err != nil || info.Mode()&os.ModeSymlink != 0 {
		t.Errorf("real dir must not be turned into a symlink: info=%v err=%v", info, err)
	}
}

// TestJail_PopulateSSLCerts_Symlink verifies that when the certs path is a
// symlink (most distros), the target's content is copied at the resolved
// path and the symlink itself is mirrored so the well-known path resolves.
func TestJail_PopulateSSLCerts_Symlink(t *testing.T) {
	j := newTestJail(t)
	base := t.TempDir()
	realDir := filepath.Join(base, "certs-real")
	writeHostFile(t, realDir, "crt1.pem", "cert", 0o644)
	link := filepath.Join(base, "certs-link")
	if err := os.Symlink(realDir, link); err != nil {
		t.Fatal(err)
	}

	if err := j.populateSSLCerts(link); err != nil {
		t.Fatalf("populateSSLCerts failed: %v", err)
	}
	if _, err := os.Stat(filepath.Join(j.root, realDir, "crt1.pem")); err != nil {
		t.Errorf("cert not copied at the resolved path: %v", err)
	}
	info, err := os.Lstat(filepath.Join(j.root, link))
	if err != nil || info.Mode()&os.ModeSymlink == 0 {
		t.Errorf("symlink should be mirrored in the jail: info=%v err=%v", info, err)
	}
}

func TestExtractLibraryPath(t *testing.T) {
	cases := []struct {
		line string
		want string
	}{
		{"\tlibssl.so.3 => /usr/lib/x86_64-linux-gnu/libssl.so.3 (0x00007f1234567890)", "/usr/lib/x86_64-linux-gnu/libssl.so.3"},
		{"\t/lib64/ld-linux-x86-64.so.2 (0x00007f1234560000)", "/lib64/ld-linux-x86-64.so.2"},
		{"\tlinux-vdso.so.1 (0x00007ffd00000000)", ""},
		{"\tlibmissing.so.1 => not found", ""},
		{"\tstatically linked", ""},
		{"", ""},
	}
	for _, tc := range cases {
		if got := extractLibraryPath(tc.line); got != tc.want {
			t.Errorf("extractLibraryPath(%q) = %q, want %q", tc.line, got, tc.want)
		}
	}
}

func TestCanChroot(t *testing.T) {
	ok, err := CanChroot()
	if err != nil {
		t.Fatalf("CanChroot returned error: %v", err)
	}
	if os.Geteuid() != 0 && ok {
		t.Error("CanChroot should be false for an unprivileged process")
	}
}

func TestCanMknod(t *testing.T) {
	ok, err := CanMknod()
	if err != nil {
		t.Fatalf("CanMknod returned error: %v", err)
	}
	if os.Geteuid() != 0 && ok {
		t.Error("CanMknod should be false for an unprivileged process")
	}
}

// TestJail_PopulatePi_StrayPackageJSON verifies that an unrelated
// package.json higher up the tree (without a "bin" entry pointing at pi)
// does not cause the parent directory to be copied into the jail.
func TestJail_PopulatePi_StrayPackageJSON(t *testing.T) {
	j := newTestJail(t)
	// Parent dir with an unrelated package.json (no bin field).
	parent := t.TempDir()
	if err := os.WriteFile(filepath.Join(parent, "package.json"), []byte(`{"name":"unrelated"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	binDir := filepath.Join(parent, "bin")
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		t.Fatal(err)
	}
	writeHostFile(t, binDir, "pi", "#!/bin/sh\necho fake pi\n", 0o755)
	withFakePath(t, binDir)

	if err := j.PopulatePi(); err != nil {
		t.Fatalf("PopulatePi failed: %v", err)
	}
	// The bin file is copied...
	if _, err := os.Stat(filepath.Join(j.root, binDir, "pi")); err != nil {
		t.Errorf("pi should be copied: %v", err)
	}
	// ...but the unrelated parent package.json is not.
	if _, err := os.Stat(filepath.Join(j.root, parent, "package.json")); !os.IsNotExist(err) {
		t.Errorf("unrelated parent package.json must NOT be copied into the jail (err=%v)", err)
	}
}

func TestJail_Setup_FailsWhenRootIsFile(t *testing.T) {
	root := filepath.Join(t.TempDir(), "jail")
	if err := os.WriteFile(root, []byte("file"), 0o644); err != nil {
		t.Fatal(err)
	}
	j := NewJail(root, log.New(io.Discard, "", 0))
	if err := j.Setup(); err == nil {
		t.Error("Setup should fail when the jail root path is a file")
	}
}

func TestJail_Cleanup_FailsOnUnremovable(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root can remove anything; needs non-root")
	}
	j := newTestJail(t)
	// A read-only subdirectory with a file inside cannot be removed by a
	// non-root process.
	sub := filepath.Join(j.root, "locked")
	if err := os.MkdirAll(sub, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sub, "f"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(sub, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(sub, 0o755) })
	if err := j.Cleanup(); err == nil {
		t.Error("Cleanup should fail when the jail contains an unremovable directory")
	}
}

func TestJail_CopyFile_DstIsDirectory(t *testing.T) {
	j := newTestJail(t)
	src := writeHostFile(t, t.TempDir(), "f", "x", 0o644)
	if err := os.MkdirAll(filepath.Join(j.root, "adir"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := j.CopyFile(src, "/adir"); err == nil {
		t.Error("CopyFile onto an existing directory should fail")
	}
}

func TestJail_CopyFile_RelativeDst(t *testing.T) {
	j := newTestJail(t)
	src := writeHostFile(t, t.TempDir(), "f", "x", 0o644)
	if err := j.CopyFile(src, "relative/path"); err == nil {
		t.Error("CopyFile with a relative destination should fail")
	}
}

func TestJail_CopyDirResolved_Errors(t *testing.T) {
	j := newTestJail(t)

	if err := j.CopyDirResolved("/nonexistent/src", "/dst"); err == nil {
		t.Error("CopyDirResolved with missing source should fail")
	}
	srcFile := writeHostFile(t, t.TempDir(), "file", "x", 0o644)
	if err := j.CopyDirResolved(srcFile, "/dst"); err == nil {
		t.Error("CopyDirResolved with a file source should fail")
	}
	if err := j.CopyDirResolved(t.TempDir(), "/"); err == nil {
		t.Error("CopyDirResolved with root destination should fail")
	}

	// A broken symlink in the tree: following it must fail.
	hostDir := t.TempDir()
	if err := os.Symlink(filepath.Join(hostDir, "no-such-target"), filepath.Join(hostDir, "broken")); err != nil {
		t.Fatal(err)
	}
	if err := j.CopyDirResolved(hostDir, "/dst"); err == nil {
		t.Error("CopyDirResolved with a broken symlink should fail")
	}
}

func TestJail_PopulateEssentialBins_EmptyPath(t *testing.T) {
	j := newTestJail(t)
	orig := os.Getenv("PATH")
	t.Cleanup(func() { os.Setenv("PATH", orig) })
	os.Setenv("PATH", t.TempDir()) // empty PATH — nothing to find
	if err := j.PopulateEssentialBins(); err != nil {
		t.Errorf("PopulateEssentialBins with empty PATH should not fail: %v", err)
	}
	entries, err := os.ReadDir(j.root)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Errorf("jail should be empty, got %d entries", len(entries))
	}
}

// TestJail_PopulatePi_StringBinForm verifies that a package.json using the
// string "bin" form is recognised as the owning package.
func TestJail_PopulatePi_StringBinForm(t *testing.T) {
	j := newTestJail(t)
	base := t.TempDir()
	binDir := filepath.Join(base, "bin")
	pkgDir := filepath.Join(base, "pkg")
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(pkgDir, "dist"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(pkgDir, "package.json"), []byte(`{"name":"strbin","bin":"dist/cli.js"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(pkgDir, "dist", "cli.js"), []byte("cli\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("../pkg/dist/cli.js", filepath.Join(binDir, "pi")); err != nil {
		t.Fatal(err)
	}
	withFakePath(t, binDir)

	if err := j.PopulatePi(); err != nil {
		t.Fatalf("PopulatePi failed: %v", err)
	}
	if _, err := os.Stat(filepath.Join(j.root, pkgDir, "dist", "cli.js")); err != nil {
		t.Errorf("string-bin package should be copied into the jail: %v", err)
	}
}

// TestJail_PopulatePi_BinPointsElsewhere verifies that a package.json whose
// bin entry does not point at pi is not treated as pi's package.
func TestJail_PopulatePi_BinPointsElsewhere(t *testing.T) {
	j := newTestJail(t)
	base := t.TempDir()
	binDir := filepath.Join(base, "bin")
	pkgDir := filepath.Join(base, "pkg")
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(pkgDir, "dist"), 0o755); err != nil {
		t.Fatal(err)
	}
	// bin points at a different (nonexistent) file.
	if err := os.WriteFile(filepath.Join(pkgDir, "package.json"), []byte(`{"name":"other","bin":{"other":"dist/other.js"}}`), 0o644); err != nil {
		t.Fatal(err)
	}
	writeHostFile(t, binDir, "pi", "#!/bin/sh\necho fake pi\n", 0o755)
	withFakePath(t, binDir)

	if err := j.PopulatePi(); err != nil {
		t.Fatalf("PopulatePi failed: %v", err)
	}
	// pi is copied as a plain file...
	if _, err := os.Stat(filepath.Join(j.root, binDir, "pi")); err != nil {
		t.Errorf("pi should be copied: %v", err)
	}
	// ...but the unrelated package is not.
	if _, err := os.Stat(filepath.Join(j.root, pkgDir, "package.json")); !os.IsNotExist(err) {
		t.Errorf("unrelated package must NOT be copied (err=%v)", err)
	}
}

func TestParseCapBit(t *testing.T) {
	// CAP_SYS_CHROOT is bit 18: 1<<18 = 0x40000.
	// CAP_MKNOD is bit 27: 1<<27 = 0x8000000.
	cases := []struct {
		name    string
		status  string
		bit     int
		want    bool
		wantErr bool
	}{
		{"chroot bit set", "Name:\ttest\nCapEff:\t0000000000040000\n", capSysChroot, true, false},
		{"chroot bit clear", "Name:\ttest\nCapEff:\t0000000000000000\n", capSysChroot, false, false},
		{"full caps", "CapEff:\t000001ffffffffff\n", capSysChroot, true, false},
		{"other bit only", "CapEff:\t0000000008000000\n", capSysChroot, false, false},
		{"missing", "Name:\ttest\n", capSysChroot, false, true},
		{"malformed line", "CapEff:\n", capSysChroot, false, true},
		{"bad hex", "CapEff:\tnothex\n", capSysChroot, false, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseCapBit(tc.status, tc.bit)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tc.wantErr)
			}
			if got != tc.want {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}
