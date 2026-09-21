package jail

import (
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

// TestBuildPlan_USRmerged verifies the plan for a usr-merged host (the
// devvm layout): /usr is bind-mounted read-only, the pi install root is
// bind-mounted read-only, the task dir is the single read-write bind,
// /tmp is a fresh tmpfs, /proc is mounted, /etc files and home dot-dirs
// are copied, and usrmerge symlinks are created.
func TestBuildPlan_USRmerged(t *testing.T) {
	f := Facts{
		TaskDir:     "/home/guest/tasks/task-1-abc123",
		HomeDir:     "/home/guest",
		PiRoot:      "/usr/local/lib/node_modules",
		USRmerged:   true,
		USRDirs:     []string{"/usr"},
		DevNodes:    []string{"null", "zero", "urandom", "tty"},
		EtcFiles:    []string{"/etc/hosts", "/etc/resolv.conf"},
		HasCertDir:  true,
		HomeDotDirs: []string{".pi", ".tokens"},
	}
	p, err := BuildPlan(f)
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}

	if p.JailRoot != "/home/guest/tasks/task-1-abc123.jail" {
		t.Errorf("JailRoot = %q, want the task dir + \".jail\"", p.JailRoot)
	}
	if p.Cwd != "/task" {
		t.Errorf("Cwd = %q, want the fixed jail path /task", p.Cwd)
	}

	mounts := make(map[string]Mount, len(p.Mounts))
	for _, m := range p.Mounts {
		mounts[m.Path] = m
	}

	// /usr is read-only.
	if m := mounts["/usr"]; m.Kind != MountBindRO || m.Src != "/usr" {
		t.Errorf("/usr mount = %+v, want bind-ro of /usr", m)
	}
	// /dev is a fresh tmpfs, not mode 1777 (O_CREAT on device files is
	// denied in a sticky+world-writable dir inside a user namespace);
	// nodev is a flag, not a data option, inside a user namespace.
	if m := mounts["/dev"]; m.Kind != MountTmpfs || m.Opts != "mode=755" || m.Flags != int(unix.MS_NODEV) {
		t.Errorf("/dev mount = %+v, want tmpfs mode=755 with MS_NODEV", m)
	}
	// Device nodes are bound by the child, not planned as mounts.
	if len(p.DevNodes) != 4 || p.DevNodes[0] != "null" {
		t.Errorf("DevNodes = %v, want the discovered node names", p.DevNodes)
	}
	for path := range mounts {
		if strings.HasPrefix(path, "/dev/") {
			t.Errorf("device node %s planned as a mount: %+v (the child binds them)", path, mounts[path])
		}
	}
	// The pi install root is read-only (the exec target must be visible).
	if m := mounts["/usr/local/lib/node_modules"]; m.Kind != MountBindRO || m.Src != "/usr/local/lib/node_modules" {
		t.Errorf("pi root mount = %+v, want bind-ro", m)
	}
	// The task dir is the ONLY read-write bind, at its fixed jail path.
	if m := mounts["/task"]; m.Kind != MountBindRW || m.Src != "/home/guest/tasks/task-1-abc123" {
		t.Errorf("task dir mount = %+v, want bind-rw of the host task dir at /task", m)
	}
	for path, m := range mounts {
		if m.Kind == MountBindRW && path != "/task" {
			t.Errorf("unexpected read-write bind at %s: %+v (the task dir must be the only rw path)", path, m)
		}
	}
	// /tmp is a fresh tmpfs, /proc is mounted.
	if m := mounts["/tmp"]; m.Kind != MountTmpfs {
		t.Errorf("/tmp mount = %+v, want tmpfs", m)
	}
	if m := mounts["/proc"]; m.Kind != MountProc {
		t.Errorf("/proc mount = %+v, want proc", m)
	}

	// /etc files are copied (not mounted): the host's /etc/resolv.conf is a
	// symlink on systemd-resolved hosts and cannot be a bind target.
	for _, f := range []string{"/etc/hosts", "/etc/resolv.conf"} {
		if !contains(p.CopyFiles, f) {
			t.Errorf("CopyFiles missing %s: %v", f, p.CopyFiles)
		}
	}
	// The CA certificate bundle is copied (resolved).
	if !contains(p.CopyTrees, "/etc/ssl/certs") {
		t.Errorf("CopyTrees missing /etc/ssl/certs: %v", p.CopyTrees)
	}
	// Home dot-dirs are copied (per-task, no crosstalk between tasks).
	for _, d := range []string{"/home/guest/.pi", "/home/guest/.tokens"} {
		if !contains(p.CopyTrees, d) {
			t.Errorf("CopyTrees missing %s: %v", d, p.CopyTrees)
		}
	}

	// Usrmerge symlinks.
	wantLinks := map[string]string{"bin": "usr/bin", "sbin": "usr/sbin", "lib": "usr/lib", "lib64": "usr/lib64"}
	if len(p.Symlinks) != len(wantLinks) {
		t.Fatalf("Symlinks = %v, want %d entries", p.Symlinks, len(wantLinks))
	}
	for _, l := range p.Symlinks {
		if wantLinks[l.New] != l.Old {
			t.Errorf("symlink %s -> %s, want %s", l.New, l.Old, wantLinks[l.New])
		}
	}
}

// TestBuildPlan_NonUSRmerged verifies the plan for a non-usr-merged host:
// the legacy /bin, /lib, /sbin, /lib64 directories are bind-mounted in
// addition to /usr, and no usrmerge symlinks are created.
func TestBuildPlan_NonUSRmerged(t *testing.T) {
	f := Facts{
		TaskDir:   "/home/guest/tasks/task-2-def456",
		HomeDir:   "/home/guest",
		PiRoot:    "/opt/pi",
		USRmerged: false,
		USRDirs:   []string{"/usr", "/bin", "/lib", "/sbin", "/lib64"},
		DevNodes:  []string{"null"},
		EtcFiles:  []string{"/etc/hosts"},
	}
	p, err := BuildPlan(f)
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}
	if len(p.Symlinks) != 0 {
		t.Errorf("Symlinks = %v, want none on a non-usr-merged host", p.Symlinks)
	}
	for _, d := range []string{"/usr", "/bin", "/lib", "/sbin", "/lib64"} {
		found := false
		for _, m := range p.Mounts {
			if m.Path == d && m.Kind == MountBindRO {
				found = true
			}
		}
		if !found {
			t.Errorf("missing bind-ro mount for %s", d)
		}
	}
}

// TestBuildPlan_RequiresTaskDirAndPiRoot verifies the pure-function
// preconditions.
func TestBuildPlan_RequiresTaskDirAndPiRoot(t *testing.T) {
	if _, err := BuildPlan(Facts{PiRoot: "/usr"}); err == nil {
		t.Error("BuildPlan without a task dir should fail")
	}
	if _, err := BuildPlan(Facts{TaskDir: "/t"}); err == nil {
		t.Error("BuildPlan without a pi root should fail")
	}
}

func contains(ss []string, s string) bool {
	for _, x := range ss {
		if x == s {
			return true
		}
	}
	return false
}
