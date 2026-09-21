package jail

import (
	"fmt"
	"os"
	"path/filepath"
)

// Facts are the host facts a jail plan is built from. Discover probes
// the real host; BuildPlan consumes them purely.
type Facts struct {
	// TaskDir is the task's working directory (the only rw bind).
	TaskDir string
	// HomeDir is the guest's home directory (dot-dirs are copied).
	HomeDir string
	// PiRoot is the directory bind-mounted read-only so the pi
	// executable is visible inside the jail at its host path: the
	// nearest node_modules ancestor of the pi binary, or the binary's
	// parent directory.
	PiRoot string
	// USRmerged reports whether the host is usr-merged (/bin is a
	// symlink into /usr). Controls the usrmerge shims in the skeleton.
	USRmerged bool
	// USRDirs are the host directories bind-mounted read-only to
	// provide the toolchain: ["/usr"] on usr-merged hosts, plus the
	// legacy /bin /lib /sbin /lib64 on non-usr-merged hosts.
	USRDirs []string
	// DevNodes are the device node names ("null", "zero", ...) that the
	// jail child bind-mounts from the host into the fresh /dev tmpfs.
	// They cannot be mknod'ed inside the namespace (CAP_MKNOD is
	// required in the init user namespace), so the host's nodes are
	// bound instead — the same approach bubblewrap takes.
	DevNodes []string
	// EtcFiles are the /etc files copied (symlink-resolved) into the
	// skeleton. They are copied, not bind-mounted, because the host's
	// /etc/resolv.conf is a symlink on systemd-resolved hosts and a
	// symlink cannot be a bind target.
	EtcFiles []string
	// HasCertDir reports whether /etc/ssl/certs exists (it is copied,
	// resolved, so TLS works inside the jail).
	HasCertDir bool
	// HomeDotDirs are the guest home subdirectories copied into the
	// skeleton (per-task copies: no crosstalk between concurrent
	// tasks). ~/.ssh is deliberately excluded, as before: the git flow
	// authenticates over HTTPS.
	HomeDotDirs []string
}

// etcFiles are the /etc files copied into every jail (best effort —
// missing files are skipped). The list is the old chroot set plus
// os-release, services and hostname, which agents' tooling probes.
var etcFiles = []string{
	"/etc/resolv.conf",
	"/etc/hosts",
	"/etc/passwd",
	"/etc/group",
	"/etc/nsswitch.conf",
	"/etc/os-release",
	"/etc/services",
	"/etc/hostname",
}

// devNodeNames are the device nodes bind-mounted into every jail (best
// effort — missing nodes are skipped). /dev/ptmx and /dev/pts are not
// listed: the jail child mounts a fresh devpts and creates the /dev/ptmx
// symlink itself.
var devNodeNames = []string{
	"null",
	"zero",
	"full",
	"random",
	"urandom",
	"tty",
}

// homeDotDirs are the guest home subdirectories copied into every jail
// (best effort — missing directories are skipped). They carry the pi
// agent configuration, TLS certificates, git configs and API tokens
// that agents rely on.
var homeDotDirs = []string{".pi", ".certs", ".forgejo-gitconfigs", ".tokens"}

// Discover probes the host and returns the facts for building a jail
// plan for the given task.
func Discover(taskDir, homeDir, piPath string) (Facts, error) {
	if taskDir == "" {
		return Facts{}, fmt.Errorf("jail: task dir is required")
	}
	if piPath == "" {
		return Facts{}, fmt.Errorf("jail: pi path is required")
	}

	f := Facts{TaskDir: taskDir, HomeDir: homeDir, PiRoot: piRootFor(piPath)}

	// Usrmerge detection: /bin is a symlink (or absent) on usr-merged
	// systems.
	if fi, err := os.Lstat("/bin"); err == nil && fi.Mode()&os.ModeSymlink == 0 {
		f.USRmerged = false
	} else {
		f.USRmerged = true
	}

	// Toolchain directories: /usr always (when present); the legacy
	// directories on non-usr-merged hosts.
	if _, err := os.Stat("/usr"); err == nil {
		f.USRDirs = append(f.USRDirs, "/usr")
	}
	if !f.USRmerged {
		for _, d := range []string{"/bin", "/lib", "/sbin", "/lib64"} {
			if fi, err := os.Stat(d); err == nil && fi.IsDir() {
				f.USRDirs = append(f.USRDirs, d)
			}
		}
	}
	if len(f.USRDirs) == 0 {
		return f, fmt.Errorf("jail: no toolchain directory found (/usr missing?)")
	}

	for _, name := range devNodeNames {
		if _, err := os.Stat(filepath.Join("/dev", name)); err == nil {
			f.DevNodes = append(f.DevNodes, name)
		}
	}
	for _, e := range etcFiles {
		if _, err := os.Stat(e); err == nil {
			f.EtcFiles = append(f.EtcFiles, e)
		}
	}
	if _, err := os.Stat("/etc/ssl/certs"); err == nil {
		f.HasCertDir = true
	}
	for _, d := range homeDotDirs {
		if fi, err := os.Stat(filepath.Join(homeDir, d)); err == nil && fi.IsDir() {
			f.HomeDotDirs = append(f.HomeDotDirs, d)
		}
	}
	return f, nil
}

// piRootFor returns the directory that must be bind-mounted so the pi
// binary is visible inside the jail at its host path: the nearest
// node_modules ancestor (the npm install root), or the binary's parent
// directory when pi is not installed under node_modules.
func piRootFor(piPath string) string {
	dir := filepath.Dir(piPath)
	for {
		if filepath.Base(dir) == "node_modules" {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return filepath.Dir(piPath)
		}
		dir = parent
	}
}
