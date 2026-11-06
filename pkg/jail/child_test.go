package jail

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

// TestJail_RunChild is the probe-gated integration test for the in-namespace
// child: it re-execs the test binary as the jail child (RunChild), which
// performs the bind mounts, mounts /proc, pivots the root and execs a
// probe script. The script reports namespace evidence back over stdout.
//
// It skips when unprivileged user namespaces are unavailable (CanJail),
// mirroring the fail-soft probe at guest startup.
func TestJail_RunChild(t *testing.T) {
	if ok, reason := CanJail(); !ok {
		t.Skipf("unprivileged namespaces unavailable: %s", reason)
	}

	base := t.TempDir()
	taskDir := filepath.Join(base, "task")
	if err := os.MkdirAll(filepath.Join(taskDir, "bin"), 0o755); err != nil {
		t.Fatal(err)
	}

	// Probe script: pure commands from the real /usr (bind-mounted ro).
	// It runs inside the jail after pivot_root.
	probe := filepath.Join(taskDir, "bin", "probe")
	script := `#!/bin/sh
echo "UID=$(id -u)"
echo "PROC_PIDS=$(ls /proc | grep -c '^[0-9]')"
touch /usr/ro-test 2>/dev/null && echo RO_FAIL || echo RO_OK
df -T /tmp 2>/dev/null | tail -n 1 | grep -q tmpfs && echo TMPFS_OK || echo TMPFS_FAIL
[ -e /spec.json ] && echo SPEC_VISIBLE || echo SPEC_MISSING
readlink /proc/self/exe >/dev/null 2>&1 && echo PROC_EXE_OK || echo PROC_EXE_FAIL
# /dev: the shell redirect opens /dev/null with O_CREAT — this is the
# case that fails in a 1777 dir inside a user namespace, so the /dev
# tmpfs must be mode 755.
echo hi > /dev/null 2>/dev/null && echo DEVNULL_REDIRECT_OK || echo DEVNULL_REDIRECT_FAIL
head -c1 /dev/zero >/dev/null 2>&1 && echo ZERO_OK || echo ZERO_FAIL
df -T /dev/pts 2>/dev/null | tail -n 1 | grep -q devpts && echo DEVPTS_OK || echo DEVPTS_FAIL
[ -e /dev/ptmx ] && echo PTMX_OK || echo PTMX_FAIL
# The privilege hole (issue #198): before the nested-userns drop the
# probe runs as root in the outer user namespace A with full
# capabilities in A — it can umount the jail's mounts and rebind them
# read-write. /tmp is a free tmpfs, so umount it as the probe. After
# the drop the probe holds no capabilities in A (or in B): CapEff must
# be zero and the umount must fail.
echo "CAPEFF=$(sed -n 's/^CapEff:[[:space:]]*//p' /proc/self/status)"
umount /tmp 2>/dev/null && echo UMMOUNT_HOLE || echo UMMOUNT_CLOSED

`
	if err := os.WriteFile(probe, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}

	// The task dir is mounted at /task (its jail path), not at its host
	// path: the /tmp tmpfs would hide a mount under /tmp.
	plan := Plan{
		JailRoot: filepath.Join(base, "task.jail"),
		Cwd:      "/task",
		Mounts: []Mount{
			{Kind: MountTmpfs, Path: "/dev", Opts: "mode=755", Flags: int(unix.MS_NODEV)},
			{Kind: MountBindRO, Path: "/usr", Src: "/usr"},
			{Kind: MountBindRW, Path: "/task", Src: taskDir},
			{Kind: MountTmpfs, Path: "/tmp"},
			{Kind: MountProc, Path: "/proc"},
		},
		DevNodes: []string{"null", "zero", "urandom", "tty"},
		// usrmerge symlinks: dynamically linked binaries resolve their
		// interpreter (e.g. /lib64/ld-linux-x86-64.so.2) through these.
		Symlinks: []Link{
			{Old: "usr/bin", New: "bin"},
			{Old: "usr/sbin", New: "sbin"},
			{Old: "usr/lib", New: "lib"},
			{Old: "usr/lib64", New: "lib64"},
		},
	}
	if err := CreateSkeleton(plan); err != nil {
		t.Fatalf("CreateSkeleton: %v", err)
	}
	if err := WriteSpec(plan); err != nil {
		t.Fatalf("WriteSpec: %v", err)
	}
	t.Cleanup(func() { _ = Cleanup(plan.JailRoot) })

	// Re-exec the test binary as the jail child, under unshare exactly as
	// the production spawn does (see BuildJailCommand in pkg/pi). The
	// child runs TestJailChildRunner, which calls RunChild and execs the
	// probe. Without the user namespace the mounts would fail with EPERM.
	// Inside the jail the probe is at its /task path, not its host path.
	s, err := runJailChild(t, plan, taskDir, "/task/bin/probe")
	if err != nil {
		t.Fatalf("jail child failed: %v\noutput:\n%s", err, s)
	}
	for _, want := range []string{
		"UID=1000", // dropped to a non-root id in the nested user namespace (issue #198)
		"RO_OK",    // the /usr bind is read-only, even as ns-root
		"TMPFS_OK", // /tmp is a fresh tmpfs
		"SPEC_VISIBLE",
		"PROC_EXE_OK",             // /proc works (this broke rustup in the chroot)
		"DEVNULL_REDIRECT_OK",     // O_CREAT on a device node (the 1777 quirk)
		"ZERO_OK",                 // /dev/zero is a working bind
		"DEVPTS_OK",               // fresh devpts for PTYs
		"PTMX_OK",                 // /dev/ptmx symlink
		"CAPEFF=0000000000000000", // no capabilities in A or B after the drop
		"UMMOUNT_CLOSED",          // the umount/rebind hole is closed by the drop
	} {
		if !strings.Contains(s, want) {
			t.Errorf("probe output missing %q:\n%s", want, s)
		}
	}
	// /proc must be scoped to the pid namespace: a handful of pids, not
	// the host's hundreds.
	var pids int
	if _, err := fmt.Sscanf(extractLine(s, "PROC_PIDS="), "%d", &pids); err != nil {
		t.Fatalf("cannot parse PROC_PIDS from:\n%s", s)
	}
	if pids > 10 {
		t.Errorf("/proc shows %d pids — the pid namespace is not scoped", pids)
	}

	// The read-only bind must not have leaked a write to the host.
	if _, err := os.Stat("/usr/ro-test"); !os.IsNotExist(err) {
		t.Error("write to the ro /usr bind leaked to the host")
	}
}

// runJailChild re-execs the test binary as the jail child under unshare
// (exactly as the production spawn does; see BuildJailCommand in
// pkg/pi) and returns the child's combined output. The plan must already
// have its skeleton created and spec written; taskDirHost is the host
// path of the task directory (used for the child's coverage dir). The
// spec path is passed via the environment (the testing package has no
// -test.args flag); command is the in-jail path of the command to run.
// extraUnshareFlags are appended to the outer unshare invocation (e.g.
// --map-auto for the range-mapped mode, issue #236).
func runJailChild(t *testing.T, plan Plan, taskDirHost, command string, extraUnshareFlags ...string) (string, error) {
	t.Helper()
	testBin, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}
	childArgs := []string{"-test.run=TestJailChildRunner"}
	// Coverage plumbing: the re-exec'd child runs the RunChild code that
	// this profile would otherwise miss (the exec seam in the child is
	// non-replacing, so its covdata runtime flushes on exit). The child
	// gets its OWN gocoverdir — sharing the parent's fails, because the
	// covdata runtime wipes the directory on init — and its data files
	// are copied into the parent's directory so the go tool merges them
	// into the final profile.
	parentCoverDir := flag.Lookup("test.gocoverdir").Value.String()
	if parentCoverDir != "" {
		// The child's covdata dir must be visible INSIDE the jail (the
		// child flushes it after the pivot, where only the jail's paths
		// exist) — /task is the right place: taskDir on the host, /task
		// in the jail. It must not be under the host's /tmp (the jail's
		// /tmp tmpfs would hide it) or under $HOME (not mounted).
		childCoverHost := filepath.Join(taskDirHost, "cover")
		if err := os.MkdirAll(childCoverHost, 0o755); err != nil {
			t.Fatal(err)
		}
		childArgs = append(childArgs, "-test.gocoverdir=/task/cover")
		t.Cleanup(func() {
			entries, err := os.ReadDir(childCoverHost)
			if err != nil {
				return
			}
			for _, e := range entries {
				data, err := os.ReadFile(filepath.Join(childCoverHost, e.Name()))
				if err != nil {
					continue
				}
				_ = os.WriteFile(filepath.Join(parentCoverDir, e.Name()), data, 0o644)
			}
		})
	}
	childArgs = append(childArgs, command)
	flags := append([]string{"--map-root-user", "--mount", "--pid", "--fork"}, extraUnshareFlags...)
	cmd := exec.Command("unshare", append(
		append(flags, "--", testBin),
		childArgs...,
	)...,
	)
	cmd.Env = append(os.Environ(), "JAIL_CHILD_SPEC="+plan.SpecPath())
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// TestJail_RunChild_MountFailure verifies that a bad bind source inside
// the namespace surfaces as a child failure (non-zero exit, a bind error
// in the output) — covering the error branches of RunChild and mountOne
// that the success path cannot reach.
func TestJail_RunChild_MountFailure(t *testing.T) {
	if ok, reason := CanJail(); !ok {
		t.Skipf("unprivileged namespaces unavailable: %s", reason)
	}
	// Each sub-case builds a plan whose first mount is guaranteed to
	// fail, so the child exercises mountOne's error branches.
	t.Run("unknown kind", func(t *testing.T) {
		base := t.TempDir()
		taskDir := filepath.Join(base, "task")
		if err := os.MkdirAll(taskDir, 0o755); err != nil {
			t.Fatal(err)
		}
		plan := Plan{
			JailRoot: filepath.Join(base, "task.jail"),
			Cwd:      "/task",
			Mounts: []Mount{
				{Kind: MountKind("bogus"), Path: "/bad"},
				{Kind: MountBindRW, Path: "/task", Src: taskDir},
				{Kind: MountTmpfs, Path: "/tmp"},
				{Kind: MountProc, Path: "/proc"},
			},
		}
		if err := CreateSkeleton(plan); err != nil {
			t.Fatalf("CreateSkeleton: %v", err)
		}
		if err := WriteSpec(plan); err != nil {
			t.Fatalf("WriteSpec: %v", err)
		}
		t.Cleanup(func() { _ = Cleanup(plan.JailRoot) })

		s, err := runJailChild(t, plan, taskDir, "/task/bin/probe")
		if err == nil {
			t.Fatalf("jail child should fail on an unknown mount kind:\n%s", s)
		}
		if !strings.Contains(s, "unknown mount kind") {
			t.Errorf("expected an unknown-kind error in the child output:\n%s", s)
		}
	})
	t.Run("bad source", func(t *testing.T) {
		base := t.TempDir()
		taskDir := filepath.Join(base, "task")
		if err := os.MkdirAll(taskDir, 0o755); err != nil {
			t.Fatal(err)
		}
		plan := Plan{
			JailRoot: filepath.Join(base, "task.jail"),
			Cwd:      "/task",
			Mounts: []Mount{
				// A bind source that does not exist: mountOne must fail.
				{Kind: MountBindRO, Path: "/bad", Src: filepath.Join(base, "no-such-src")},
				{Kind: MountBindRW, Path: "/task", Src: taskDir},
				{Kind: MountTmpfs, Path: "/tmp"},
				{Kind: MountProc, Path: "/proc"},
			},
		}
		if err := CreateSkeleton(plan); err != nil {
			t.Fatalf("CreateSkeleton: %v", err)
		}
		if err := WriteSpec(plan); err != nil {
			t.Fatalf("WriteSpec: %v", err)
		}
		t.Cleanup(func() { _ = Cleanup(plan.JailRoot) })

		s, err := runJailChild(t, plan, taskDir, "/task/bin/probe")
		if err == nil {
			t.Fatalf("jail child should fail on a bad bind source:\n%s", s)
		}
		if !strings.Contains(s, "bind") {
			t.Errorf("expected a bind error in the child output:\n%s", s)
		}
	})
}

// TestJail_RunChild_DevFailure verifies that a device node that does not
// exist on the host surfaces as a child failure — covering setupDev's
// error branch, which the success path cannot reach.
func TestJail_RunChild_DevFailure(t *testing.T) {
	if ok, reason := CanJail(); !ok {
		t.Skipf("unprivileged namespaces unavailable: %s", reason)
	}
	base := t.TempDir()
	taskDir := filepath.Join(base, "task")
	if err := os.MkdirAll(taskDir, 0o755); err != nil {
		t.Fatal(err)
	}
	plan := Plan{
		JailRoot: filepath.Join(base, "task.jail"),
		Cwd:      "/task",
		Mounts: []Mount{
			{Kind: MountTmpfs, Path: "/dev", Opts: "mode=755", Flags: int(unix.MS_NODEV)},
			{Kind: MountBindRW, Path: "/task", Src: taskDir},
			{Kind: MountTmpfs, Path: "/tmp"},
			{Kind: MountProc, Path: "/proc"},
		},
		DevNodes: []string{"definitely-not-a-device"},
	}
	if err := CreateSkeleton(plan); err != nil {
		t.Fatalf("CreateSkeleton: %v", err)
	}
	if err := WriteSpec(plan); err != nil {
		t.Fatalf("WriteSpec: %v", err)
	}
	t.Cleanup(func() { _ = Cleanup(plan.JailRoot) })

	s, err := runJailChild(t, plan, taskDir, "/task/bin/probe")
	if err == nil {
		t.Fatalf("jail child should fail on a bad dev node:\n%s", s)
	}
	if !strings.Contains(s, "/dev/") {
		t.Errorf("expected a /dev error in the child output:\n%s", s)
	}
}

// TestMirroredMapArgs verifies the range-mode drop mappings (issue
// #236): each entry of the outer namespace's uid/gid maps is mirrored
// into a nested-namespace range "inner:inner:count" (the outer field
// of the parent's map names ids in the PARENT of the parent, which the
// child must not reference — only the parent's own inner ids are
// mappable downward). Entries with count 0 (the unmapped-id filler
// lines of uid_map) are skipped.
func TestMirroredMapArgs(t *testing.T) {
	dir := t.TempDir()
	uidPath := filepath.Join(dir, "uid_map")
	gidPath := filepath.Join(dir, "gid_map")
	if err := os.WriteFile(uidPath, []byte(
		"         0       1000          1\n         1     100000      65534\n4294967295 4294967295          0\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(gidPath, []byte(
		"         0       1000          1\n         1     100000      65534\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	uidArgs, gidArgs, err := mirroredMapArgs(uidPath, gidPath)
	if err != nil {
		t.Fatalf("mirroredMapArgs: %v", err)
	}
	wantUID := []string{"--map-users=0:0:1", "--map-users=1:1:65534"}
	wantGID := []string{"--map-groups=0:0:1", "--map-groups=1:1:65534"}
	if !reflect.DeepEqual(uidArgs, wantUID) {
		t.Errorf("uid args = %v, want %v", uidArgs, wantUID)
	}
	if !reflect.DeepEqual(gidArgs, wantGID) {
		t.Errorf("gid args = %v, want %v", gidArgs, wantGID)
	}
}

// TestMirroredMapArgs_BadInput verifies the range-mode drop fails
// closed: an unreadable, empty or malformed map file must produce an
// error rather than a silently weaker (single-entry or unmapped) drop.
func TestMirroredMapArgs_BadInput(t *testing.T) {
	dir := t.TempDir()
	cases := map[string]string{
		"empty":     "",
		"only-zero": "4294967295 4294967295          0\n",
		"malformed": "not a map line\n",
		"short":     "0 0\n",
	}
	for name, content := range cases {
		t.Run(name, func(t *testing.T) {
			p := filepath.Join(dir, "map-"+name)
			if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
				t.Fatal(err)
			}
			if _, _, err := mirroredMapArgs(p, p); err == nil {
				t.Errorf("mirroredMapArgs on %q should fail", content)
			}
		})
	}
	t.Run("missing file", func(t *testing.T) {
		if _, _, err := mirroredMapArgs(filepath.Join(dir, "nope"), filepath.Join(dir, "nope")); err == nil {
			t.Error("mirroredMapArgs on a missing file should fail")
		}
	})
}

// canMapGuestRange reports whether this host's unshare(1) supports
// --map-auto with the current user's subuid/subgid block resolvable
// (util-linux >= 2.40, the uidmap package, and a provisioned
// /etc/subuid entry and passwd record for this user — see the README
// jail section, issue #236). It gates the true range-mapped end-to-end
// test.
func canMapGuestRange() bool {
	return exec.Command("unshare", "--map-root-user", "--map-auto", "--", "true").Run() == nil
}

// TestJail_RunChild_RangeMapped is the probe-gated end-to-end test for
// the range-mapped drop (issue #236): with spec.MapGuestRange set the
// jail child must write a multi-entry map for the nested namespace
// that mirrors everything the outer namespace maps, instead of the
// single "<innerID>:0:1" entry. The command then runs as uid 0 in the
// nested namespace (root there, mapping its whole outer range) but
// still holds no capabilities in the outer namespace A: the #198
// umount/rebind hole must stay closed.
//
// The "mirror only" sub-case runs wherever unprivileged namespaces do
// (the outer map is the plain single-UID one; the child mirrors it —
// exercising the multi-entry drop code path). The "--map-auto" sub-case
// additionally needs a provisioned subuid range and skips without it.
func TestJail_RunChild_RangeMapped(t *testing.T) {
	if ok, reason := CanJail(); !ok {
		t.Skipf("unprivileged namespaces unavailable: %s", reason)
	}

	tests := []struct {
		name         string
		flags        []string
		needRange    bool // skip unless canMapGuestRange
		wantMinLines int
	}{
		{"mirror single-entry outer map", nil, false, 1},
		{"with --map-auto", []string{"--map-auto"}, true, 2},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.needRange && !canMapGuestRange() {
				t.Skip("--map-auto unavailable (util-linux < 2.40, no newuidmap/newgidmap, or no subuid/passwd entry for this user)")
			}
			base := t.TempDir()
			taskDir := filepath.Join(base, "task")
			if err := os.MkdirAll(filepath.Join(taskDir, "bin"), 0o755); err != nil {
				t.Fatal(err)
			}
			probe := filepath.Join(taskDir, "bin", "probe-range")
			script := `#!/bin/sh
echo "UID=$(id -u)"
echo "UIDMAP_LINES=$(grep -c '[0-9]' /proc/self/uid_map)"
# The #198 hole must stay closed even in range mode: the command is
# root in the nested namespace B, but the jail's mounts belong to A,
# so the umount must fail.
umount /tmp 2>/dev/null && echo UMMOUNT_HOLE || echo UMMOUNT_CLOSED
`
			if err := os.WriteFile(probe, []byte(script), 0o755); err != nil {
				t.Fatal(err)
			}
			plan := Plan{
				JailRoot: filepath.Join(base, "task.jail"),
				Cwd:      "/task",
				Mounts: []Mount{
					{Kind: MountTmpfs, Path: "/dev", Opts: "mode=755", Flags: int(unix.MS_NODEV)},
					{Kind: MountBindRO, Path: "/usr", Src: "/usr"},
					{Kind: MountBindRW, Path: "/task", Src: taskDir},
					{Kind: MountTmpfs, Path: "/tmp"},
					{Kind: MountProc, Path: "/proc"},
				},
				DevNodes: []string{"null", "zero", "urandom", "tty"},
				// usrmerge shims: the drop's unshare and the probe
				// resolve their ELF interpreter through these.
				Symlinks: []Link{
					{Old: "usr/bin", New: "bin"},
					{Old: "usr/sbin", New: "sbin"},
					{Old: "usr/lib", New: "lib"},
					{Old: "usr/lib64", New: "lib64"},
				},
				MapGuestRange: true,
			}
			if err := CreateSkeleton(plan); err != nil {
				t.Fatalf("CreateSkeleton: %v", err)
			}
			if err := WriteSpec(plan); err != nil {
				t.Fatalf("WriteSpec: %v", err)
			}
			t.Cleanup(func() { _ = Cleanup(plan.JailRoot) })

			s, err := runJailChild(t, plan, taskDir, "/task/bin/probe-range", tc.flags...)
			if err != nil {
				t.Fatalf("jail child failed: %v\noutput:\n%s", err, s)
			}
			// The multi-entry drop maps A:0 to 0 in B, so the command
			// is uid 0 there (unlike the single-entry drop's 1000).
			if !strings.Contains(s, "UID=0") {
				t.Errorf("probe output missing UID=0 (range-mode drop):\n%s", s)
			}
			lines, err := strconv.Atoi(strings.TrimSpace(extractLine(s, "UIDMAP_LINES=")))
			if err != nil {
				t.Fatalf("cannot parse UIDMAP_LINES from:\n%s", s)
			}
			if lines < tc.wantMinLines {
				t.Errorf("nested uid_map has %d entries, want >= %d (multi-entry map)\n%s", lines, tc.wantMinLines, s)
			}
			if !strings.Contains(s, "UMMOUNT_CLOSED") {
				t.Errorf("the umount/rebind hole opened in range mode:\n%s", s)
			}
		})
	}
}

// TestRunChildMain_EmptyCommand verifies RunChildMain returns non-zero
// when there is no command (flag.Args() is empty in a normal test run).
func TestRunChildMain_EmptyCommand(t *testing.T) {
	if code := RunChildMain(filepath.Join(t.TempDir(), "spec.json")); code == 0 {
		t.Error("RunChildMain should return non-zero with an empty command")
	}
}

// TestJailChildRunner is the re-exec entry point: it runs only when the
// test binary is launched as the jail child (by TestJail_RunChild, with
// JAIL_CHILD_SPEC set and -test.run=TestJailChildRunner). It calls
// RunChild, which execs the probe command and never returns. In a normal
// test run it skips.
func TestJailChildRunner(t *testing.T) {
	specPath := os.Getenv("JAIL_CHILD_SPEC")
	if specPath == "" {
		t.Skip("not a re-exec'd jail child")
	}
	// Substitute the final exec with a non-replacing run: the real exec
	// would replace this test binary's process image, killing its
	// coverage runtime before it can flush. The probe's output still
	// reaches the parent's pipe (inherited stdout), so the assertions
	// are unaffected.
	origExecve := execve
	execve = func(argv []string) error {
		out, err := exec.Command(argv[0], argv[1:]...).CombinedOutput()
		os.Stdout.Write(out)
		return err
	}
	defer func() { execve = origExecve }()
	if err := RunChild(specPath, flag.Args()); err != nil {
		fmt.Fprintf(os.Stderr, "jailchild: %v\n", err)
		os.Exit(1)
	}
}

// TestIDMapping verifies the unshare(1) --map-users/--map-groups argument
// the nested drop uses: a single mapping of the outer root (A:0) to the
// inner non-root id, "<innerID>:0:1" (issue #198).
func TestIDMapping(t *testing.T) {
	if got := idMapping(DropID); got != "1000:0:1" {
		t.Errorf("idMapping(%d) = %q, want %q", DropID, got, "1000:0:1")
	}
	if got := idMapping(4242); got != "4242:0:1" {
		t.Errorf("idMapping(4242) = %q, want %q", got, "4242:0:1")
	}
}

// TestDropAndExec_Argv pins the unshare(1) command line the nested drop
// constructs (issue #198). The execve seam is stubbed with a capturing
// function, so the test runs in any environment — including hosts without
// unprivileged user namespaces, where the CanJail()-gated probe tests skip.
func TestDropAndExec_Argv(t *testing.T) {
	unsharePath, err := exec.LookPath("unshare")
	if err != nil {
		t.Skipf("unshare(1) not on PATH: %v", err)
	}

	origExecve := execve
	defer func() { execve = origExecve }()

	run := func(innerID int, wantMapping string) {
		t.Helper()
		var captured []string
		execve = func(argv []string) error {
			captured = argv
			return nil
		}
		if err := dropAndExec(innerID, false, []string{"/bin/true"}); err != nil {
			t.Fatalf("dropAndExec(%d): %v", innerID, err)
		}
		want := []string{
			unsharePath, "--user",
			"--map-users=" + wantMapping, "--map-groups=" + wantMapping,
			"--", "/bin/true",
		}
		if !reflect.DeepEqual(captured, want) {
			t.Errorf("dropAndExec(%d) argv = %v, want %v", innerID, captured, want)
		}
	}

	run(0, "1000:0:1")    // innerID <= 0 falls back to DropID
	run(4242, "4242:0:1") // an explicit inner id is used as-is
}

// TestDropAndExec_Argv_RangeMap pins the range-mapped drop argv (issue
// #236): instead of the single "<innerID>:0:1" entry, unshare gets one
// --map-users/--map-groups range per entry the outer namespace maps
// (this process's uid/gid maps). The single-entry mapping must not
// appear.
func TestDropAndExec_Argv_RangeMap(t *testing.T) {
	unsharePath, err := exec.LookPath("unshare")
	if err != nil {
		t.Skipf("unshare(1) not on PATH: %v", err)
	}

	origExecve := execve
	defer func() { execve = origExecve }()
	var captured []string
	execve = func(argv []string) error {
		captured = argv
		return nil
	}
	if err := dropAndExec(0, true, []string{"/bin/true"}); err != nil {
		t.Fatalf("dropAndExec(range): %v", err)
	}
	if len(captured) < 5 || captured[0] != unsharePath || captured[1] != "--user" {
		t.Fatalf("unexpected drop argv: %v", captured)
	}
	if tail := captured[len(captured)-2:]; len(tail) != 2 || tail[0] != "--" || tail[1] != "/bin/true" {
		t.Errorf("drop argv tail = %v, want [-- /bin/true]", tail)
	}
	var users, groups int
	for _, a := range captured[2:] {
		switch {
		case a == "--map-users=1000:0:1" || a == "--map-groups=1000:0:1":
			t.Errorf("range drop must not carry the single-entry mapping: %v", captured)
		case strings.HasPrefix(a, "--map-users="):
			users++
		case strings.HasPrefix(a, "--map-groups="):
			groups++
		}
	}
	if users == 0 || groups == 0 {
		t.Errorf("range drop carries no id ranges (users=%d groups=%d): %v", users, groups, captured)
	}
}

// TestDropAndExec_UnshareMissing verifies the drop fails closed when
// unshare(1) cannot be located: the error is returned and the command is
// not exec'd.
func TestDropAndExec_UnshareMissing(t *testing.T) {
	origExecve := execve
	defer func() { execve = origExecve }()
	executed := false
	execve = func(argv []string) error {
		executed = true
		return nil
	}
	t.Setenv("PATH", filepath.Join(t.TempDir(), "no-such-dir"))
	if err := dropAndExec(0, false, []string{"/bin/true"}); err == nil {
		t.Fatal("dropAndExec should fail when unshare is not on PATH")
	}
	if executed {
		t.Error("the command must not be exec'd when the drop fails")
	}
}

func extractLine(s, prefix string) string {
	for _, line := range strings.Split(s, "\n") {
		if strings.HasPrefix(line, prefix) {
			return strings.TrimPrefix(line, prefix)
		}
	}
	return ""
}
