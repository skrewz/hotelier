// Package jail builds and runs per-task filesystem jails for the pi
// subprocess using unprivileged user, mount and pid namespaces
// (unshare(1), user_namespaces(7)).
//
// The jail replaces the former chroot approach (issue #51). A chroot
// gives path confinement only: no /proc, no device nodes, and a
// hand-picked set of binaries and libraries that silently drifts as the
// host toolchain changes. The namespace jail instead reuses the host's
// real /usr (bind-mounted read-only), mounts a scoped /proc and the
// device nodes the agent needs, and gives the subprocess a private
// /tmp and a private copy of the /etc files and home dot-directories it
// relies on. This is the same mechanism bubblewrap/Flatpak, rootless
// podman and the AI-agent CLIs use.
//
// Write isolation: every mount into the jail is explicitly read-only
// except the task directory itself, which is the single intentional
// read-write path. Writes to /usr, the pi install, /etc and the home
// dot-dirs cannot reach the host: /usr and the pi install are ro binds,
// /etc files and the dot-dirs are per-task copies, and /tmp is a fresh
// tmpfs.
//
// Process model: the guest spawns
//
//	unshare --map-root-user --mount --pid --fork --forward-signals \
//	    --kill-child=SIGKILL -- <guest> -jail-child <spec.json> -- <pi> <args...>
//
// The unshare(1) binary creates the namespaces and maps the guest's uid
// to root inside them (no capabilities required). Its forked child —
// this package's RunChild, reached via the guest binary's -jail-child
// flag — performs the bind mounts, mounts /proc, pivots the root to the
// jail and execs pi. The --fork is mandatory: a process that created a
// pid namespace (and is not pid 1 in it) cannot fork after mounting
// inside it (verified on kernel 7.1 and 7.2), so the child must be
// forked into the namespace by unshare.
package jail

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
)

// MountKind is the kind of a jail mount.
type MountKind string

const (
	// MountBindRO bind-mounts a host path read-only at the same path.
	MountBindRO MountKind = "bind-ro"
	// MountBindRW bind-mounts a host path read-write at the same path.
	// The task directory is the only read-write mount.
	MountBindRW MountKind = "bind-rw"
	// MountTmpfs mounts a fresh tmpfs (e.g. /tmp).
	MountTmpfs MountKind = "tmpfs"
	// MountProc mounts a scoped /proc for the new pid namespace.
	MountProc MountKind = "proc"
)

// Mount is one mount performed by the jail child before pivot_root.
//
// Path is the mount point inside the jail (an absolute path; it is
// resolved against the jail root before the pivot, so /usr means
// <jailRoot>/usr). Src is the host source for bind mounts. In
// production Path equals Src (the jail mirrors host absolute paths);
// they may differ in tests. Opts carries comma-separated data options
// for tmpfs mounts (empty for the others); Flags carries MS_* mount
// flags (e.g. MS_NODEV for /dev).
//
// MS_NODEV must be passed as a flag, not in Opts: inside a user
// namespace the VFS rejects the nodev/noexec/nosuid data options with
// EINVAL (verified on kernels 7.1 and 7.2) while the corresponding
// flags are accepted.
type Mount struct {
	Kind  MountKind `json:"kind"`
	Path  string    `json:"path"`
	Src   string    `json:"src,omitempty"`
	Opts  string    `json:"opts,omitempty"`
	Flags int       `json:"flags,omitempty"`
}

// Link is a symlink created in the skeleton, relative to the jail root
// (e.g. the usrmerge shims bin -> usr/bin).
type Link struct {
	Old string `json:"old"`
	New string `json:"new"`
}

// Plan describes a complete jail: where its root lives, what the
// subprocess's working directory is, and how the root filesystem is
// assembled. It is built by BuildPlan from host facts (Discover) and is
// pure data — no privileges are needed to build it.
type Plan struct {
	JailRoot string  `json:"jailRoot"`
	Cwd      string  `json:"cwd"`
	Mounts   []Mount `json:"mounts"`
	// DevNodes are the device node names ("null", "zero", ...) the
	// jail child bind-mounts from the host into the fresh /dev tmpfs.
	// Device nodes cannot be mknod'ed without CAP_MKNOD in the init
	// user namespace (kernel fs/namei.c do_mknod), so they are bound
	// from the host — the same approach bubblewrap takes.
	DevNodes  []string `json:"devNodes,omitempty"`
	CopyFiles []string `json:"copyFiles,omitempty"`
	CopyTrees []string `json:"copyTrees,omitempty"`
	Symlinks  []Link   `json:"symlinks,omitempty"`
}

// SpecPath is the location of the spec file inside the jail root. The
// spec is written into the jail root so the jail child can read it at
// /spec.json after the pivot.
func (p Plan) SpecPath() string {
	return filepath.Join(p.JailRoot, "spec.json")
}

// Spec is what the jail child reads: everything it needs to assemble the
// root filesystem and exec the command. (The Plan's copy lists are
// consumed host-side by CreateSkeleton and are not needed by the child.)
type Spec struct {
	JailRoot string  `json:"jailRoot"`
	Cwd      string  `json:"cwd"`
	Mounts   []Mount `json:"mounts"`
	// DevNodes are the device node names bind-mounted into /dev by the
	// jail child (see Plan.DevNodes).
	DevNodes []string `json:"devNodes,omitempty"`
}

// ReadSpec reads and validates a spec file.
func ReadSpec(path string) (Spec, error) {
	var s Spec
	data, err := os.ReadFile(path)
	if err != nil {
		return s, fmt.Errorf("read jail spec %s: %w", path, err)
	}
	if err := json.Unmarshal(data, &s); err != nil {
		return s, fmt.Errorf("parse jail spec %s: %w", path, err)
	}
	if s.JailRoot == "" || s.Cwd == "" {
		return s, fmt.Errorf("jail spec %s: jailRoot and cwd are required", path)
	}
	return s, nil
}

// Jail is a per-task jail: a skeleton directory on the host plus the
// spec the jail child consumes. It mirrors the old chroot.Jail API so
// the handler wiring stays the same shape.
type Jail struct {
	plan Plan
	log  *log.Logger
}

// NewJail creates a Jail. A nil logger is defaulted to a discarding one.
func NewJail(logger *log.Logger) *Jail {
	if logger == nil {
		logger = log.New(io.Discard, "", 0)
	}
	return &Jail{log: logger}
}

// Setup discovers the host facts, builds the plan, creates the skeleton
// and writes the spec. The jail root is a sibling of the task directory
// (<taskDir>.jail), so the task-directory sweep covers it.
func (j *Jail) Setup(taskDir, homeDir, piPath string) error {
	facts, err := Discover(taskDir, homeDir, piPath)
	if err != nil {
		return err
	}
	plan, err := BuildPlan(facts)
	if err != nil {
		return err
	}
	if err := CreateSkeleton(plan); err != nil {
		// Remove the partial skeleton so a failed setup leaves no
		// debris behind (the handler also defers Cleanup).
		_ = Cleanup(plan.JailRoot)
		return err
	}
	if err := WriteSpec(plan); err != nil {
		_ = Cleanup(plan.JailRoot)
		return err
	}
	j.plan = plan
	j.log.Printf("jail ready: root=%s cwd=%s mounts=%d copies=%d symlinks=%d",
		plan.JailRoot, plan.Cwd, len(plan.Mounts),
		len(plan.CopyFiles)+len(plan.CopyTrees), len(plan.Symlinks))
	return nil
}

// Path is the jail root on the host.
func (j *Jail) Path() string { return j.plan.JailRoot }

// SpecPath is the spec file inside the jail root.
func (j *Jail) SpecPath() string { return j.plan.SpecPath() }

// Plan returns the built plan (for inspection and testing).
func (j *Jail) Plan() Plan { return j.plan }

// Cleanup removes the jail root. The namespace mounts die with the pi
// process tree, so this is a plain RemoveAll; if an orphaned process
// still pins the mounts the removal fails with EBUSY and is reported.
func (j *Jail) Cleanup() error { return Cleanup(j.plan.JailRoot) }

// Cleanup removes a jail root directory. A missing root is not an error.
func Cleanup(root string) error {
	if root == "" {
		return nil
	}
	if _, err := os.Stat(root); os.IsNotExist(err) {
		return nil
	}
	return os.RemoveAll(root)
}
