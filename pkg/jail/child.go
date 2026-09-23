package jail

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"
)

// RunChild performs the in-namespace side of the jail: it reads the
// spec, performs the bind mounts (read-only except the task dir),
// mounts the tmpfs /tmp and the scoped /proc, pivots the root to the
// jail, changes to the working directory, drops into a nested user
// namespace (so the command holds no capabilities in the outer
// namespace — issue #198) and execs the command.
//
// It must run inside the namespaces created by unshare(1) (user, mount,
// pid) — see the package docs. On success it never returns (exec
// replaces the process); an error means the command could not be execed.
func RunChild(specPath string, argv []string) error {
	if len(argv) == 0 {
		return fmt.Errorf("jailchild: no command")
	}
	spec, err := ReadSpec(specPath)
	if err != nil {
		return err
	}

	// pivot_root(2) requires that the mount containing the new root is
	// not shared with other mount namespaces (EINVAL otherwise). systemd
	// makes / (and often /tmp) shared by default, so detach from the
	// host's shared propagation first — the same step bubblewrap takes.
	// This only affects our private mount namespace.
	if err := unix.Mount("", "/", "", unix.MS_REC|unix.MS_PRIVATE, ""); err != nil {
		return fmt.Errorf("jailchild: make-rprivate /: %w", err)
	}

	// All mount points are addressed relative to the jail root: chdir
	// first, then the absolute mount paths (e.g. /usr) resolve to
	// <jailRoot>/usr. After the pivot the jail root becomes /, so the
	// same paths are valid inside the jail.
	if err := os.Chdir(spec.JailRoot); err != nil {
		return fmt.Errorf("jailchild: chdir %s: %w", spec.JailRoot, err)
	}

	// pivot_root(2) requires new_root to be a mount point; the jail root
	// is a plain directory, so self-bind it (the same trick bubblewrap
	// uses). Done after make-rprivate so the new mount is private.
	if err := unix.Mount(".", ".", "", unix.MS_BIND, ""); err != nil {
		return fmt.Errorf("jailchild: self-bind jail root: %w", err)
	}
	// The self-bind created a new mount, but the process's cwd still sits
	// on the old one; pivot_root would then see new_root as a plain
	// directory (EINVAL). Re-enter the directory so the cwd is on the
	// self-bind mount.
	if err := os.Chdir("/"); err != nil {
		return fmt.Errorf("jailchild: chdir /: %w", err)
	}
	if err := os.Chdir(spec.JailRoot); err != nil {
		return fmt.Errorf("jailchild: re-enter %s: %w", spec.JailRoot, err)
	}

	for _, m := range spec.Mounts {
		// The spec carries jail paths (e.g. /usr); the mounts are made
		// before the pivot, so the target must be relative to the jail
		// root (the current directory).
		target := strings.TrimPrefix(m.Path, "/")
		if err := mountOne(m, target); err != nil {
			return fmt.Errorf("jailchild: mount %s (%s): %w", m.Path, m.Kind, err)
		}
	}

	// Populate /dev: bind the device nodes from the host, mount a fresh
	// devpts and /dev/shm. Must run after the /dev tmpfs mount.
	if err := setupDev(spec.DevNodes); err != nil {
		return err
	}

	// pivot_root: make the jail root the new /. The old root is moved to
	// .oldroot and lazily unmounted, detaching the host filesystem.
	if err := os.MkdirAll(".oldroot", 0o755); err != nil {
		return fmt.Errorf("jailchild: create .oldroot: %w", err)
	}
	if err := unix.PivotRoot(".", ".oldroot"); err != nil {
		return fmt.Errorf("jailchild: pivot_root: %w", err)
	}
	if err := unix.Unmount(".oldroot", unix.MNT_DETACH); err != nil {
		return fmt.Errorf("jailchild: umount old root: %w", err)
	}

	if err := os.Chdir(spec.Cwd); err != nil {
		return fmt.Errorf("jailchild: chdir %s: %w", spec.Cwd, err)
	}

	// Drop into a nested user namespace before exec (issue #198):
	// everything the mounter execs would otherwise inherit root-in-A
	// and with it CAP_SYS_ADMIN in A, which can umount the jail's ro
	// mounts and rebind them read-write. After the drop the command
	// holds no capabilities in A (or in B) and cannot.
	return dropAndExec(spec.DropToUID, argv)
}

// dropAndExec drops into a nested (child) user namespace B and execs the
// command as the non-root id innerID inside it (issue #198). The drop is
// delegated to unshare(1) — a single-threaded helper that creates B and
// maps the command into it as innerID — because a Go process cannot create
// a user namespace itself: the kernel's check_unshare_flags makes
// CLONE_NEWUSER imply CLONE_THREAD, which requires a single-threaded
// process, and the Go runtime always runs a second (sysmon) thread, so
// unix.Unshare(CLONE_NEWUSER) from here fails with EINVAL. unshare(1) is
// already a hard dependency of the jail (the outer spawn uses it), so no
// new binary is introduced.
//
// unshare(1) writes the mapping from its parent side (its helper child,
// still in A, writes the main process's /proc/<pid>/uid_map). That
// parent-side write succeeds here: the mapping file's inode is owned by
// the target's uid in A (0, unchanged by the unshare), so the writer
// (A:0) is the owner and the DAC check passes. It requires /proc to be
// mounted in A (showing this pid namespace) — RunChild mounts it (the
// MountProc spec entry) before this runs, so the requirement is met.
//
// The mapping is "<innerID>:0:1": A:0 (the guest user on the host) becomes
// innerID in B, so the command's host identity is unchanged while it is
// non-root in B and holds no capabilities in A.
func dropAndExec(innerID int, argv []string) error {
	if innerID <= 0 {
		innerID = DropID
	}
	mapping := idMapping(innerID)
	// execve(2) does not search PATH, so the drop helper must be
	// referenced by absolute path. unshare lives under /usr, which the
	// jail bind-mounts read-only, so it is resolvable after the pivot.
	unsharePath, err := exec.LookPath("unshare")
	if err != nil {
		return fmt.Errorf("jailchild: locate unshare: %w", err)
	}
	cmd := append([]string{
		unsharePath, "--user",
		"--map-users=" + mapping, "--map-groups=" + mapping,
		"--",
	}, argv...)
	// Replace the process with the command (via the drop). The
	// environment is inherited from the guest (persona vars, TMPDIR,
	// PATH, ...).
	return execve(cmd)
}

// idMapping builds the unshare(1) --map-users/--map-groups argument that
// maps the outer root (A:0) to the inner non-root id: "<innerID>:0:1"
// (inside:outside:count).
func idMapping(innerID int) string {
	return fmt.Sprintf("%d:0:1", innerID)
}

// execve is the final exec. It is a variable so the in-namespace test
// child can substitute a non-replacing implementation: the real exec
// replaces the process image, which would kill the test binary's
// coverage runtime before it can flush its data.
var execve = func(argv []string) error {
	if err := unix.Exec(argv[0], argv, os.Environ()); err != nil {
		return fmt.Errorf("jailchild: exec %s: %w", argv[0], err)
	}
	return nil // unreachable
}

// mountOne performs a single mount from the spec. target is the mount
// point relative to the jail root (the current directory); m.Src is the
// host-side source (absolute).
func mountOne(m Mount, target string) error {
	switch m.Kind {
	case MountBindRO:
		if err := unix.Mount(m.Src, target, "", unix.MS_BIND|unix.MS_REC, ""); err != nil {
			return err
		}
		// Bind mounts inherit the source's mount options; remount to
		// enforce read-only.
		return unix.Mount("", target, "", unix.MS_BIND|unix.MS_REMOUNT|unix.MS_RDONLY, "")
	case MountBindRW:
		return unix.Mount(m.Src, target, "", unix.MS_BIND|unix.MS_REC, "")
	case MountTmpfs:
		return unix.Mount("tmpfs", target, "tmpfs", uintptr(m.Flags), m.Opts)
	case MountProc:
		return unix.Mount("proc", target, "proc", 0, "")
	default:
		return fmt.Errorf("unknown mount kind %q", m.Kind)
	}
}

// setupDev populates the fresh /dev tmpfs (mounted from the spec) with
// the device nodes the agent needs. The nodes are bind-mounted from the
// host onto placeholder files: mknod requires CAP_MKNOD in the init
// user namespace (kernel fs/namei.c do_mknod), which an unprivileged
// namespace never has — binding is the workaround bubblewrap uses.
//
// A fresh devpts (with the standard /dev/ptmx symlink) gives the agent
// working PTYs, and /dev/shm a private shared-memory area (node and
// friends fall back to /tmp if it is absent).
func setupDev(devNodes []string) error {
	for _, name := range devNodes {
		placeholder := filepath.Join("dev", name)
		f, err := os.OpenFile(placeholder, os.O_CREATE, 0o666)
		if err != nil {
			return fmt.Errorf("jailchild: create /dev/%s placeholder: %w", name, err)
		}
		_ = f.Close()
		if err := unix.Mount("/dev/"+name, placeholder, "", unix.MS_BIND, ""); err != nil {
			return fmt.Errorf("jailchild: bind /dev/%s: %w", name, err)
		}
	}
	if err := os.MkdirAll("dev/pts", 0o755); err != nil {
		return fmt.Errorf("jailchild: create /dev/pts: %w", err)
	}
	if err := unix.Mount("devpts", "dev/pts", "devpts", 0, "mode=620,ptmxmode=666"); err != nil {
		return fmt.Errorf("jailchild: mount devpts: %w", err)
	}
	if err := os.Symlink("pts/ptmx", "dev/ptmx"); err != nil && !os.IsExist(err) {
		return fmt.Errorf("jailchild: symlink /dev/ptmx: %w", err)
	}
	if err := os.MkdirAll("dev/shm", 0o1777); err != nil {
		return fmt.Errorf("jailchild: create /dev/shm: %w", err)
	}
	if err := unix.Mount("tmpfs", "dev/shm", "tmpfs", unix.MS_NOSUID, "mode=1777"); err != nil {
		return fmt.Errorf("jailchild: mount /dev/shm: %w", err)
	}
	return nil
}

// RunChildMain is the entry point for the guest binary's -jail-child
// flag: it runs the jail child with the command taken from the
// remaining command-line arguments (after "--", via flag.Args()). It
// exits the process with a non-zero status on failure.
//
// flag.Parse() must have been called by the caller (the guest binary's
// main, or a test's TestMain) before RunChildMain is invoked.
func RunChildMain(specPath string) int {
	argv := flag.Args()
	if err := RunChild(specPath, argv); err != nil {
		fmt.Fprintf(os.Stderr, "jailchild: %v\n", err)
		return 1
	}
	return 0
}
