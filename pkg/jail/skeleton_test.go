package jail

import (
	"io"
	"log"
	"os"
	"path/filepath"
	"testing"
)

// buildTestPlan builds a plan whose sources all live under base (a temp
// dir), so the test is hermetic: CreateSkeleton only ever writes under
// the jail root, while the sources are plain temp-dir files and dirs.
func buildTestPlan(t *testing.T, base string) Plan {
	t.Helper()

	srcUSR := filepath.Join(base, "src", "usr")
	if err := os.MkdirAll(filepath.Join(srcUSR, "bin"), 0o755); err != nil {
		t.Fatal(err)
	}
	srcDevNull := filepath.Join(base, "src", "devnull")
	if err := os.WriteFile(srcDevNull, []byte(""), 0o644); err != nil {
		t.Fatal(err)
	}
	taskDir := filepath.Join(base, "task")
	if err := os.MkdirAll(taskDir, 0o755); err != nil {
		t.Fatal(err)
	}
	srcHosts := filepath.Join(base, "src", "hosts")
	if err := os.WriteFile(srcHosts, []byte("127.0.0.1 localhost\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	// A tree with a symlink that must be copied resolved.
	srcCerts := filepath.Join(base, "src", "certs")
	if err := os.MkdirAll(srcCerts, 0o755); err != nil {
		t.Fatal(err)
	}
	caFile := filepath.Join(base, "src", "ca.pem")
	if err := os.WriteFile(caFile, []byte("CERTDATA"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(caFile, filepath.Join(srcCerts, "abc1")); err != nil {
		t.Fatal(err)
	}

	return Plan{
		JailRoot: filepath.Join(base, "task", "task.jail"),
		Cwd:      taskDir,
		Mounts: []Mount{
			{Kind: MountBindRO, Path: "/usr", Src: srcUSR},
			{Kind: MountBindRO, Path: "/dev/null", Src: srcDevNull},
			{Kind: MountBindRW, Path: taskDir, Src: taskDir},
			{Kind: MountTmpfs, Path: "/tmp"},
			{Kind: MountProc, Path: "/proc"},
		},
		CopyFiles: []string{srcHosts},
		CopyTrees: []string{srcCerts},
		Symlinks:  []Link{{Old: "usr/bin", New: "bin"}},
	}
}

// TestCreateSkeleton verifies that the skeleton contains a mount point for
// every mount (directories for dir sources, a regular file for file
// sources), the usrmerge symlinks, and resolved copies of the /etc files
// and trees — and that it writes nothing outside the jail root.
func TestCreateSkeleton(t *testing.T) {
	base := t.TempDir()
	plan := buildTestPlan(t, base)

	if err := CreateSkeleton(plan); err != nil {
		t.Fatalf("CreateSkeleton: %v", err)
	}
	jr := plan.JailRoot

	for _, mp := range []string{"/usr", "/tmp", "/proc", plan.Cwd} {
		fi, err := os.Stat(filepath.Join(jr, mp))
		if err != nil {
			t.Errorf("mount point %s missing: %v", mp, err)
			continue
		}
		if !fi.IsDir() {
			t.Errorf("mount point %s is not a directory", mp)
		}
	}
	// A file bind source gets a regular-file mount point.
	fi, err := os.Stat(filepath.Join(jr, "/dev/null"))
	if err != nil {
		t.Errorf("/dev/null mount point missing: %v", err)
	} else if fi.IsDir() {
		t.Error("/dev/null mount point should be a regular file (file bind source)")
	}

	// The usrmerge symlink.
	link, err := os.Readlink(filepath.Join(jr, "/bin"))
	if err != nil {
		t.Errorf("/bin symlink missing: %v", err)
	} else if link != "usr/bin" {
		t.Errorf("/bin symlink = %q, want usr/bin", link)
	}

	// The /etc file copy has the source content.
	data, err := os.ReadFile(filepath.Join(jr, plan.CopyFiles[0]))
	if err != nil {
		t.Fatalf("copied hosts file missing: %v", err)
	}
	if string(data) != "127.0.0.1 localhost\n" {
		t.Errorf("copied hosts content = %q", data)
	}

	// The tree copy resolves symlinks: abc1 is a regular file with the
	// target's content, not a symlink.
	abc1 := filepath.Join(jr, plan.CopyTrees[0], "abc1")
	fi, err = os.Stat(abc1) // Stat follows symlinks
	if err != nil {
		t.Fatalf("copied cert entry missing: %v", err)
	}
	if fi.Mode()&os.ModeSymlink != 0 {
		t.Error("copied cert entry should be resolved (regular file)")
	}
	caData, err := os.ReadFile(abc1)
	if err != nil {
		t.Fatalf("read copied cert: %v", err)
	}
	if string(caData) != "CERTDATA" {
		t.Errorf("copied cert content = %q, want the resolved target content", caData)
	}
}

// TestWriteSpec_RoundTrip verifies the spec file is written at
// <jailRoot>/spec.json and round-trips through JSON.
func TestWriteSpec_RoundTrip(t *testing.T) {
	base := t.TempDir()
	plan := buildTestPlan(t, base)
	if err := CreateSkeleton(plan); err != nil {
		t.Fatalf("CreateSkeleton: %v", err)
	}

	if err := WriteSpec(plan); err != nil {
		t.Fatalf("WriteSpec: %v", err)
	}
	specPath := filepath.Join(plan.JailRoot, "spec.json")
	if plan.SpecPath() != specPath {
		t.Errorf("SpecPath() = %q, want %q", plan.SpecPath(), specPath)
	}
	spec, err := ReadSpec(specPath)
	if err != nil {
		t.Fatalf("ReadSpec: %v", err)
	}
	if spec.JailRoot != plan.JailRoot || spec.Cwd != plan.Cwd {
		t.Errorf("spec round-trip mismatch: %+v", spec)
	}
	if len(spec.Mounts) != len(plan.Mounts) {
		t.Fatalf("spec mounts = %d, want %d", len(spec.Mounts), len(plan.Mounts))
	}
	for i, m := range plan.Mounts {
		if spec.Mounts[i] != m {
			t.Errorf("spec mount[%d] = %+v, want %+v", i, spec.Mounts[i], m)
		}
	}
}

// TestJail_Facade verifies the Jail facade: Setup discovers the host facts,
// builds the plan, creates the skeleton and writes the spec; Cleanup
// removes the jail root.
func TestJail_Facade(t *testing.T) {
	base := t.TempDir()
	taskDir := filepath.Join(base, "tasks", "task-1")
	if err := os.MkdirAll(taskDir, 0o755); err != nil {
		t.Fatal(err)
	}
	home := filepath.Join(base, "home")
	if err := os.MkdirAll(filepath.Join(home, ".pi"), 0o755); err != nil {
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

	j := NewJail(log.New(io.Discard, "", 0))
	if err := j.Setup(taskDir, home, pi); err != nil {
		t.Fatalf("Setup: %v", err)
	}
	if j.Path() != taskDir+".jail" {
		t.Errorf("Path() = %q, want %q", j.Path(), taskDir+".jail")
	}
	if _, err := os.Stat(j.SpecPath()); err != nil {
		t.Errorf("spec not written: %v", err)
	}
	if cwd := j.Plan().Cwd; cwd != "/task" {
		t.Errorf("Plan().Cwd = %q, want /task", cwd)
	}
	// The pi install root (the node_modules ancestor) must be a bind mount
	// so the exec target is visible inside the jail.
	spec, err := ReadSpec(j.SpecPath())
	if err != nil {
		t.Fatalf("ReadSpec: %v", err)
	}
	wantPiRoot := filepath.Join(base, "install", "node_modules")
	found := false
	for _, m := range spec.Mounts {
		if m.Src == wantPiRoot && m.Kind == MountBindRO {
			found = true
		}
	}
	if !found {
		t.Errorf("spec missing bind-ro of the pi node_modules root %s: %+v", wantPiRoot, spec.Mounts)
	}
	// The home dot-dir was copied into the skeleton.
	if _, err := os.Stat(filepath.Join(j.Path(), home, ".pi")); err != nil {
		t.Errorf("home dot-dir not copied: %v", err)
	}

	if err := j.Cleanup(); err != nil {
		t.Fatalf("Cleanup: %v", err)
	}
	if _, err := os.Stat(j.Path()); !os.IsNotExist(err) {
		t.Errorf("jail root should be removed after Cleanup (err=%v)", err)
	}
}

// TestCleanup_Missing verifies Cleanup is a no-op for a missing root.
func TestCleanup_Missing(t *testing.T) {
	if err := Cleanup(t.TempDir() + "/nope"); err != nil {
		t.Errorf("Cleanup of a missing root should not fail: %v", err)
	}
}

// TestJail_Setup_RequiresTaskDir verifies that Setup fails when the
// task directory is missing (the Discover error branch).
func TestJail_Setup_RequiresTaskDir(t *testing.T) {
	j := NewJail(log.New(io.Discard, "", 0))
	if err := j.Setup("", t.TempDir(), t.TempDir()+"/pi"); err == nil {
		t.Fatal("Setup should fail without a task dir")
	}
}

// TestReadSpec_Malformed verifies ReadSpec rejects a corrupt spec.
func TestReadSpec_Malformed(t *testing.T) {
	p := filepath.Join(t.TempDir(), "spec.json")
	if err := os.WriteFile(p, []byte("{not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadSpec(p); err == nil {
		t.Error("ReadSpec should fail on malformed JSON")
	}
}

// TestReadSpec_Missing verifies ReadSpec fails on a missing file.
func TestReadSpec_Missing(t *testing.T) {
	if _, err := ReadSpec(filepath.Join(t.TempDir(), "nope.json")); err == nil {
		t.Error("ReadSpec should fail on a missing file")
	}
}

// TestReadSpec_MissingFields verifies ReadSpec rejects a spec without
// the required jailRoot and cwd.
func TestReadSpec_MissingFields(t *testing.T) {
	p := filepath.Join(t.TempDir(), "spec.json")
	if err := os.WriteFile(p, []byte(`{"mounts":[]}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadSpec(p); err == nil {
		t.Error("ReadSpec should fail when jailRoot/cwd are missing")
	}
}
