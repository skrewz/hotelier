package pi

import (
	"context"
	"flag"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"hotelier/pkg/jail"
)

// TestMain handles the -jail-child re-exec: when the test binary is
// launched as the jail child (by the client's BuildJailCommand, which
// re-execs os.Executable()), it runs the jail child instead of the test
// suite. This mirrors the guest binary's -jail-child flag.
func TestMain(m *testing.M) {
	jailChildSpec := flag.String("jail-child", "", "internal: run as the jail child (re-exec under unshare)")
	flag.Parse()
	if *jailChildSpec != "" {
		os.Exit(jail.RunChildMain(*jailChildSpec))
	}
	os.Exit(m.Run())
}

// TestBuildJailCommand verifies the pure command construction: the
// unshare flags, the guest re-exec, the spec path, the "--" separator
// and the pi command.
func TestBuildJailCommand(t *testing.T) {
	got := BuildJailCommand("/usr/local/bin/guest", "/jail/spec.json", "/usr/bin/pi",
		[]string{"--mode", "rpc", "--no-session", "--provider", "anthropic"})
	want := []string{
		"unshare",
		"--map-root-user", "--mount", "--pid", "--fork",
		"--forward-signals", "--kill-child=SIGKILL",
		"--", "/usr/local/bin/guest", "-jail-child", "/jail/spec.json",
		"--", "/usr/bin/pi",
		"--mode", "rpc", "--no-session", "--provider", "anthropic",
	}
	if len(got) != len(want) {
		t.Fatalf("command length = %d, want %d:\n%v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("command[%d] = %q, want %q\nfull: %v", i, got[i], want[i], got)
			break
		}
	}
}

// TestPiClient_Start_JailSpecMissing verifies that Start fails
// synchronously when the jail spec does not exist (no process is
// spawned).
func TestPiClient_Start_JailSpecMissing(t *testing.T) {
	ctx := context.Background()
	c := NewClient(PiClientConfig{
		CWD:          "/tmp",
		Log:          log.New(io.Discard, "", 0),
		JailSpecPath: "/nonexistent/jail/spec.json",
	})
	if err := c.Start(ctx); err == nil {
		c.Stop(ctx)
		t.Fatal("Start with a missing jail spec should fail")
	}
	if c.cmd != nil && c.cmd.Process != nil {
		t.Error("no process should have been spawned for a missing spec")
	}
}

// buildJailTestJail builds a real namespace jail (via pkg/jail) around a
// fake pi: a POSIX shell script that writes its cwd to jail-marker and
// then blocks on stdin. It returns the spec path, the host task dir and
// the fake pi's path (which is visible at the same absolute path inside
// the jail, because its parent dir is bind-mounted read-only).
func buildJailTestJail(t *testing.T) (specPath, taskDir, fakePi string) {
	t.Helper()
	// The base dir must not be under /tmp: the jail mounts a fresh
	// tmpfs at /tmp, which would hide the pi-root bind (a bind under
	// /tmp can never coexist with a /tmp tmpfs — the source lookup is
	// either hidden or the mount is). Production paths (the pi install,
	// the task dir) are never under /tmp.
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	base, err := os.MkdirTemp(home, "hotelier-pi-jail-test-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(base) })
	taskDir = filepath.Join(base, "task")
	if err := os.MkdirAll(taskDir, 0o755); err != nil {
		t.Fatal(err)
	}
	binDir := filepath.Join(base, "bin")
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		t.Fatal(err)
	}
	fakePi = filepath.Join(binDir, "pi")
	script := `#!/bin/sh
printf '%s' "$(pwd)" > jail-marker
read -r _ || true
`
	if err := os.WriteFile(fakePi, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}

	homeDir := filepath.Join(base, "home")
	if err := os.MkdirAll(homeDir, 0o755); err != nil {
		t.Fatal(err)
	}

	j := jail.NewJail(log.New(io.Discard, "", 0))
	if err := j.Setup(taskDir, homeDir, fakePi); err != nil {
		t.Fatalf("jail setup: %v", err)
	}
	t.Cleanup(func() { _ = j.Cleanup() })
	return j.SpecPath(), taskDir, fakePi
}

// TestPiClient_Start_JailRunsInsideJail is the end-to-end test: the
// client spawns pi inside the namespace jail (unshare -> jail child ->
// pivot_root -> exec). The fake pi writes its cwd to jail-marker, which
// must land in the host task dir (mounted at /task inside the jail) with
// the jail cwd /task — proving pi ran inside the jail. The pinned
// ExecPath must be used verbatim (not re-resolved via PATH).
func TestPiClient_Start_JailRunsInsideJail(t *testing.T) {
	if ok, reason := jail.CanJail(); !ok {
		t.Skipf("unprivileged namespaces unavailable: %s", reason)
	}

	specPath, taskDir, fakePi := buildJailTestJail(t)

	// A decoy pi resolvable via PATH: the spawn must use the pinned
	// ExecPath, not this one.
	pathDir := t.TempDir()
	pathPi := filepath.Join(pathDir, "pi")
	if err := os.WriteFile(pathPi, []byte("#!/bin/sh\nsleep 1\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	origPath := os.Getenv("PATH")
	t.Cleanup(func() { os.Setenv("PATH", origPath) })
	os.Setenv("PATH", pathDir+string(os.PathListSeparator)+origPath)

	ctx := context.Background()
	c := NewClient(PiClientConfig{
		CWD:          taskDir,
		Log:          log.New(io.Discard, "", 0),
		ExecPath:     fakePi,
		JailSpecPath: specPath,
	})
	if err := c.Start(ctx); err != nil {
		t.Fatalf("start in jail failed: %v", err)
	}
	defer c.Stop(ctx)

	// The spawned command must be the unshare wrapper with the pinned pi
	// path (the PATH decoy must not be re-resolved).
	args := strings.Join(c.cmd.Args, " ")
	if c.cmd.Args[0] != "unshare" {
		t.Errorf("spawn used %q, want unshare", c.cmd.Args[0])
	}
	if !strings.Contains(args, fakePi) {
		t.Errorf("spawn command does not contain the pinned pi %q:\n%s", fakePi, args)
	}
	if strings.Contains(args, pathPi) {
		t.Errorf("spawn command contains the PATH decoy pi %q:\n%s", pathPi, args)
	}

	// The marker must appear in the host task dir with the jail cwd.
	marker := filepath.Join(taskDir, "jail-marker")
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if data, err := os.ReadFile(marker); err == nil {
			if string(data) != "/task" {
				t.Errorf("marker cwd = %q, want /task", data)
			}
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("marker file never appeared in the task dir — pi did not run inside the jail")
}
