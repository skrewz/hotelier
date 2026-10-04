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
	// HomeDir is the guest's home directory (configured home paths are
	// copied, relative to it).
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
	// HomeCopyTrees are the guest home subdirectories copied into the
	// skeleton (per-task copies: no crosstalk between concurrent
	// tasks). Names are relative to HomeDir. ~/.ssh is deliberately
	// excluded from the default set: the git flow authenticates over
	// HTTPS.
	HomeCopyTrees []string
	// HomeCopyFiles are the guest home files (e.g. dot-files such as a
	// skill's config file) copied into the skeleton with resolved-copy
	// semantics, like the /etc files. Names are relative to HomeDir.
	HomeCopyFiles []string
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

// defaultHomeCopies is the guest home paths copied into every jail when
// the guest config declares no home_copies set (issue #201). They carry
// the pi agent configuration, TLS certificates, git configs and API
// tokens that agents rely on. A guest config may enumerate a different
// (and, unlike this built-in set, file-level) set.
var defaultHomeCopies = []string{".pi", ".certs", ".forgejo-gitconfigs", ".tokens"}

// Discover probes the host and returns the facts for building a jail
// plan for the given task. homeCopies is the configured set of home
// paths to copy (relative to homeDir); nil or empty selects
// defaultHomeCopies.
func Discover(taskDir, homeDir, piPath string, homeCopies []string) (Facts, error) {
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
	// Home copies are best effort: entries that do not exist on this
	// guest are skipped. Each entry is classified by what it is here —
	// a directory is copied as a tree, a file (Stat follows symlinks)
	// as a resolved file copy.
	if len(homeCopies) == 0 {
		homeCopies = defaultHomeCopies
	}
	for _, e := range homeCopies {
		fi, err := os.Stat(filepath.Join(homeDir, e))
		if err != nil {
			continue
		}
		if fi.IsDir() {
			f.HomeCopyTrees = append(f.HomeCopyTrees, e)
		} else {
			f.HomeCopyFiles = append(f.HomeCopyFiles, e)
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
