// Package chroot builds and manages per-task chroot jails for guest task
// isolation (issue #51).
//
// Every file the pi subprocess needs is copied into the jail at the same
// absolute path it has on the host. Mirroring host paths means existing
// absolute-path references — shebangs, shared library paths, git credential
// and TLS paths, persona <workpath> env vars — keep working unmodified
// inside the jail.
//
// The jail is created as a sibling of the task working directory
// (<taskDir>.chroot) so the task directory can be copied into the jail at
// its own absolute path without recursion.
package chroot

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
)

// essentialBins are the host binaries copied into every jail (best effort —
// missing binaries are logged and skipped). The list is intentionally broad:
// agents routinely shell out to common tools, and a missing tool fails the
// task with a visible "command not found" rather than a jail misconfiguration.
var essentialBins = []string{
	// Coreutils
	"bash", "sh", "env", "cat", "ls", "cp", "mv", "rm", "mkdir", "rmdir",
	"find", "grep", "sed", "awk", "tar", "gzip", "gunzip", "zip", "unzip",
	"chmod", "chown", "head", "tail", "wc", "touch", "ln", "date", "ps",
	"kill", "sleep", "xargs", "cut", "tr", "sort", "uniq", "du", "df",
	"basename", "dirname", "realpath", "echo", "pwd", "true", "false",
	"test", "id", "whoami", "hostname", "uname", "which",
	// Networking
	"curl", "wget", "ssh", "scp", "rsync",
	// Language runtimes / build tools
	"node", "npm", "npx", "python3", "make", "jq", "git",
}

// homeDotDirs are the guest home subdirectories copied into every jail
// (best effort — missing directories are skipped). They carry the pi agent
// configuration, TLS certificates, git configs and API tokens that agents
// rely on.
var homeDotDirs = []string{".pi", ".certs", ".forgejo-gitconfigs", ".tokens"}

// etcFiles are the /etc files copied into every jail (best effort).
var etcFiles = []string{
	"/etc/resolv.conf",
	"/etc/hosts",
	"/etc/passwd",
	"/etc/group",
	"/etc/nsswitch.conf",
}

// devNodes are the device nodes created in the jail (best effort — requires
// CAP_MKNOD, i.e. root).
var devNodes = []struct {
	path  string
	major uint32
	minor uint32
}{
	{"/dev/null", 1, 3},
	{"/dev/zero", 1, 5},
	{"/dev/full", 1, 7},
	{"/dev/random", 1, 8},
	{"/dev/urandom", 1, 9},
	{"/dev/tty", 5, 0},
}

// Jail is a chroot jail rooted at root on the host.
type Jail struct {
	root string
	log  *log.Logger
}

// NewJail creates a jail rooted at root. The root directory is created by
// Setup.
func NewJail(root string, log *log.Logger) *Jail {
	return &Jail{root: root, log: log}
}

// Path returns the host path of the jail root.
func (j *Jail) Path() string {
	return j.root
}

// Setup creates the jail root directory.
func (j *Jail) Setup() error {
	if err := os.MkdirAll(j.root, 0o755); err != nil {
		return fmt.Errorf("create jail root %s: %w", j.root, err)
	}
	return nil
}

// Cleanup removes the jail root and everything under it. It is idempotent.
func (j *Jail) Cleanup() error {
	if err := os.RemoveAll(j.root); err != nil {
		return fmt.Errorf("remove jail root %s: %w", j.root, err)
	}
	return nil
}

// dstInJail maps a host absolute path to its mirrored location inside the
// jail.
func (j *Jail) dstInJail(hostAbs string) (string, error) {
	if !filepath.IsAbs(hostAbs) {
		return "", fmt.Errorf("destination %q is not an absolute path", hostAbs)
	}
	if hostAbs == "/" {
		return "", fmt.Errorf("refusing to copy onto the jail root")
	}
	return filepath.Join(j.root, hostAbs), nil
}

// CopyFile copies src to dstAbs inside the jail, where dstAbs is the
// absolute path the file should have inside the jail (typically the same
// path as src on the host). Symlinks in src are resolved — the target's
// content is copied. File permissions are preserved.
func (j *Jail) CopyFile(src, dstAbs string) error {
	info, err := os.Stat(src) // follows symlinks
	if err != nil {
		return fmt.Errorf("stat %s: %w", src, err)
	}
	if info.IsDir() {
		return fmt.Errorf("source %s is a directory, use CopyDir", src)
	}
	dst, err := j.dstInJail(dstAbs)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return fmt.Errorf("create parent dirs for %s: %w", dst, err)
	}
	if err := copyFileContents(src, dst, info.Mode().Perm()); err != nil {
		return fmt.Errorf("copy %s -> %s: %w", src, dst, err)
	}
	return nil
}

// CopyDir recursively copies src to dstAbs inside the jail, preserving
// symlinks as symlinks (important for working trees such as git repos).
func (j *Jail) CopyDir(src, dstAbs string) error {
	info, err := os.Stat(src)
	if err != nil {
		return fmt.Errorf("stat %s: %w", src, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("source %s is not a directory, use CopyFile", src)
	}
	dst, err := j.dstInJail(dstAbs)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dst, 0o755); err != nil {
		return fmt.Errorf("create %s: %w", dst, err)
	}
	return copyDirTree(src, dst, false, nil, j.log)
}

// CopyDirResolved is like CopyDir but follows symlinks: a symlink to a file
// is copied as a file with the target's content, and a symlink to a
// directory is copied as a directory with the target's contents. Use this
// for trees full of hash links such as /etc/ssl/certs.
func (j *Jail) CopyDirResolved(src, dstAbs string) error {
	info, err := os.Stat(src)
	if err != nil {
		return fmt.Errorf("stat %s: %w", src, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("source %s is not a directory, use CopyFile", src)
	}
	dst, err := j.dstInJail(dstAbs)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dst, 0o755); err != nil {
		return fmt.Errorf("create %s: %w", dst, err)
	}
	return copyDirTree(src, dst, true, map[devIno]struct{}{}, j.log)
}

// CopyBinary copies a binary found on the host PATH (by name) into the jail
// at the same absolute path, along with the shared libraries reported by
// ldd. It returns the host path the binary was found at.
func (j *Jail) CopyBinary(name string) (string, error) {
	hostPath, err := exec.LookPath(name)
	if err != nil {
		return "", fmt.Errorf("look up %s: %w", name, err)
	}
	resolved, err := filepath.EvalSymlinks(hostPath)
	if err != nil {
		return "", fmt.Errorf("resolve %s: %w", hostPath, err)
	}
	if err := j.CopyFile(hostPath, hostPath); err != nil {
		return "", err
	}
	if err := j.copyLibraries(resolved); err != nil {
		return "", err
	}
	return hostPath, nil
}

// PopulateEssentialBins copies the essential host binaries (see
// essentialBins) into the jail. Missing binaries are logged and skipped.
func (j *Jail) PopulateEssentialBins() error {
	for _, name := range essentialBins {
		if _, err := j.CopyBinary(name); err != nil {
			j.log.Printf("chroot: %s not available for the jail: %v", name, err)
			continue
		}
		j.log.Printf("chroot: copied %s into jail", name)
	}
	// git's external plumbing (git-upload-pack, git gc, ...) lives in the
	// directory reported by `git --exec-path` (e.g. /usr/lib/git-core on
	// Debian, /usr/libexec/git-core on Fedora), which is not on the PATH;
	// copy it if present so git actually works (best effort).
	if hostPath, err := exec.LookPath("git"); err == nil {
		if out, err := exec.Command(hostPath, "--exec-path").Output(); err == nil {
			execPath := strings.TrimSpace(string(out))
			if filepath.IsAbs(execPath) {
				if err := j.CopyDir(execPath, execPath); err != nil {
					j.log.Printf("chroot: failed to copy git exec path %s: %v", execPath, err)
				} else {
					j.log.Printf("chroot: copied git exec path %s into jail", execPath)
				}
			}
		}
	}
	// python3's standard library is not a shared library, so ldd never
	// reports it — query the interpreter for its stdlib path(s) and copy
	// them (best effort). Without the stdlib, `python3 -c "import json"`
	// fails inside the jail with ModuleNotFoundError.
	if hostPath, err := exec.LookPath("python3"); err == nil {
		if err := j.copyPythonStdlib(hostPath); err != nil {
			j.log.Printf("chroot: failed to copy python3 stdlib: %v", err)
		}
	}
	// npm and npx are JS scripts, not binaries: ldd reports nothing and
	// CopyBinary copies only the script content, leaving the package's
	// lib/ and node_modules/ behind — `npm` inside the jail would fail
	// with MODULE_NOT_FOUND. When the script belongs to an npm package
	// (its package.json "bin" field resolves to it), copy the whole
	// package at its host path (best effort). npm and npx share one
	// package, so dedupe by package root.
	seenNodePackages := map[string]bool{}
	for _, name := range []string{"npm", "npx"} {
		hostPath, err := exec.LookPath(name)
		if err != nil {
			continue
		}
		resolved, err := filepath.EvalSymlinks(hostPath)
		if err != nil {
			continue
		}
		pkgRoot := findPackageRoot(resolved)
		if pkgRoot == "" || seenNodePackages[pkgRoot] {
			continue
		}
		seenNodePackages[pkgRoot] = true
		if err := j.CopyDirResolved(pkgRoot, pkgRoot); err != nil {
			j.log.Printf("chroot: failed to copy npm package %s: %v", pkgRoot, err)
		} else {
			j.log.Printf("chroot: copied npm package %s into jail", pkgRoot)
		}
	}
	return nil
}

// pythonStdlibQuery prints the interpreter's stdlib and platstdlib paths
// (one per line; they may be identical).
const pythonStdlibQuery = `import sysconfig
print(sysconfig.get_path("stdlib"))
print(sysconfig.get_path("platstdlib"))
`

// copyPythonStdlib queries the python3 interpreter at python3Path for its
// standard library paths and copies each into the jail at its host path.
func (j *Jail) copyPythonStdlib(python3Path string) error {
	out, err := exec.Command(python3Path, "-c", pythonStdlibQuery).Output()
	if err != nil {
		return fmt.Errorf("query stdlib paths: %w", err)
	}
	seen := []string{}
	for _, line := range strings.Split(string(out), "\n") {
		stdlib := strings.TrimSpace(line)
		// Skip paths nested inside an already-copied one: on RHEL/Fedora
		// layouts platstdlib is a subdirectory of stdlib, and copying it
		// again would duplicate the whole tree (review feedback on #174).
		if !filepath.IsAbs(stdlib) || isUnderAny(stdlib, seen) {
			continue
		}
		seen = append(seen, stdlib)
		if _, err := os.Stat(stdlib); err != nil {
			j.log.Printf("chroot: python3 stdlib %s not present, skipping", stdlib)
			continue
		}
		if err := j.CopyDirResolved(stdlib, stdlib); err != nil {
			return fmt.Errorf("copy stdlib %s: %w", stdlib, err)
		}
		j.log.Printf("chroot: copied python3 stdlib %s into jail", stdlib)
	}
	return nil
}

// isUnderAny reports whether p is identical to, or nested inside, any of
// the given paths. The separator suffix avoids the classic prefix trap
// ("/a/bx" is not under "/a/b").
func isUnderAny(p string, paths []string) bool {
	for _, s := range paths {
		if p == s || strings.HasPrefix(p, s+string(filepath.Separator)) {
			return true
		}
	}
	return false
}

// PopulatePi copies the pi executable (and, when pi is an npm package, the
// whole package) into the jail at their host paths. It fails if pi is not
// found on the host PATH — a guest without pi cannot run tasks.
func (j *Jail) PopulatePi() error {
	piPath, err := exec.LookPath("pi")
	if err != nil {
		return fmt.Errorf("look up pi: %w", err)
	}
	resolved, err := filepath.EvalSymlinks(piPath)
	if err != nil {
		return fmt.Errorf("resolve pi %s: %w", piPath, err)
	}
	// When pi is an npm package (a symlink into the package's dist/), the
	// package's own files are required at runtime (relative imports,
	// createRequire). Copy the whole package at its host path.
	if pkgRoot := findPackageRoot(resolved); pkgRoot != "" {
		j.log.Printf("chroot: copying pi package %s into jail", pkgRoot)
		if err := j.CopyDirResolved(pkgRoot, pkgRoot); err != nil {
			return fmt.Errorf("copy pi package %s: %w", pkgRoot, err)
		}
	}
	// Copy the bin path itself as a regular file (symlink resolved) so PATH
	// lookup inside the jail finds it.
	if err := j.CopyFile(piPath, piPath); err != nil {
		return err
	}
	// Best effort: a compiled pi binary would need its shared libraries.
	if err := j.copyLibraries(resolved); err != nil {
		j.log.Printf("chroot: failed to copy pi libraries: %v", err)
	}
	return nil
}

// PopulateHome copies the guest's home dot-directories (see homeDotDirs)
// into the jail at their host paths. Missing directories are skipped.
func (j *Jail) PopulateHome() error {
	home, err := os.UserHomeDir()
	if err != nil {
		return fmt.Errorf("determine home directory: %w", err)
	}
	for _, dir := range homeDotDirs {
		src := filepath.Join(home, dir)
		if _, err := os.Stat(src); err != nil {
			j.log.Printf("chroot: %s not present, skipping", src)
			continue
		}
		if err := j.CopyDirResolved(src, src); err != nil {
			return fmt.Errorf("copy %s: %w", src, err)
		}
		j.log.Printf("chroot: copied %s into jail", src)
	}
	return nil
}

// PopulateEtc copies the essential /etc files (see etcFiles) and the CA
// certificate bundle into the jail at their host paths. Missing files are
// skipped.
func (j *Jail) PopulateEtc() error {
	for _, f := range etcFiles {
		if _, err := os.Stat(f); err != nil {
			j.log.Printf("chroot: %s not present, skipping", f)
			continue
		}
		if err := j.CopyFile(f, f); err != nil {
			return fmt.Errorf("copy %s: %w", f, err)
		}
	}
	// CA certificates: /etc/ssl/certs is a symlink to the real directory
	// on most distros and contains per-cert hash symlinks — resolve both.
	return j.populateSSLCerts("/etc/ssl/certs")
}

// populateSSLCerts copies the CA certificate bundle at sslCerts into the
// jail. On most distros sslCerts is a symlink to the real directory and
// contains per-cert hash symlinks — both are resolved. When the canonical
// path differs from sslCerts, the symlink itself is mirrored so the
// well-known path resolves inside the jail.
func (j *Jail) populateSSLCerts(sslCerts string) error {
	resolved, err := filepath.EvalSymlinks(sslCerts)
	if err != nil {
		j.log.Printf("chroot: %s not present, skipping", sslCerts)
		return nil
	}
	if err := j.CopyDirResolved(resolved, resolved); err != nil {
		return fmt.Errorf("copy %s: %w", resolved, err)
	}
	// When the canonical path differs from the well-known one (i.e.
	// sslCerts is a symlink on the host), mirror the symlink itself so
	// the well-known path resolves inside the jail. On systems where it
	// is already a real directory (Debian) there is nothing to mirror.
	if resolved != sslCerts {
		if err := os.Symlink(resolved, filepath.Join(j.root, sslCerts)); err != nil {
			j.log.Printf("chroot: failed to mirror symlink %s: %v", sslCerts, err)
		}
	}
	return nil
}

// PopulateDev creates the basic device nodes (see devNodes) in the jail.
// Requires CAP_MKNOD (root); failures are logged and skipped so the jail
// can still be built in unprivileged test environments.
func (j *Jail) PopulateDev() error {
	for _, dev := range devNodes {
		dst := filepath.Join(j.root, dev.path)
		if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
			j.log.Printf("chroot: create %s: %v", filepath.Dir(dst), err)
			continue
		}
		if err := syscall.Mknod(dst, syscall.S_IFCHR|0o666, int(dev.major<<8|dev.minor)); err != nil {
			j.log.Printf("chroot: mknod %s: %v", dev.path, err)
			continue
		}
		j.log.Printf("chroot: created %s", dev.path)
	}
	return nil
}

// CopyTaskDir copies the task working directory into the jail at its own
// absolute host path, preserving symlinks (git working trees).
func (j *Jail) CopyTaskDir(taskDir string) error {
	return j.CopyDir(taskDir, taskDir)
}

// copyLibraries copies the shared libraries reported by ldd for binary into
// the jail at their host paths.
func (j *Jail) copyLibraries(binary string) error {
	out, err := exec.Command("ldd", binary).Output()
	if err != nil {
		// Statically linked binaries or scripts produce no ldd output —
		// nothing to copy.
		return nil
	}
	for _, line := range strings.Split(string(out), "\n") {
		lib := extractLibraryPath(line)
		if lib == "" {
			continue
		}
		if err := j.CopyFile(lib, lib); err != nil {
			return fmt.Errorf("copy library %s: %w", lib, err)
		}
	}
	return nil
}

// findPackageRoot walks up from start (at most 6 levels) and returns the
// first directory whose package.json declares a "bin" entry that resolves
// to start — i.e. the npm package that owns the binary. Unrelated
// package.json files (e.g. a stray /tmp/package.json) are ignored, so a
// plain script is never mistaken for a package installation.
func findPackageRoot(start string) string {
	dir := filepath.Dir(start)
	for i := 0; i < 6; i++ {
		if pkgDir := packageDirWithBin(filepath.Join(dir, "package.json"), start); pkgDir != "" {
			return pkgDir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return ""
}

// packageDirWithBin returns pkgJSON's directory if its "bin" field (string
// or object form) names a file that resolves to start, or "" otherwise.
func packageDirWithBin(pkgJSON, start string) string {
	pkgDir := filepath.Dir(pkgJSON)
	data, err := os.ReadFile(pkgJSON)
	if err != nil {
		return ""
	}
	var pkg struct {
		Bin json.RawMessage `json:"bin"`
	}
	if json.Unmarshal(data, &pkg) != nil || pkg.Bin == nil {
		return ""
	}
	// "bin": "dist/cli.js" (string form)
	var str string
	if json.Unmarshal(pkg.Bin, &str) == nil {
		if binResolvesTo(filepath.Join(pkgDir, str), start) {
			return pkgDir
		}
		return ""
	}
	// "bin": {"pi": "dist/bundle/cli.js", ...} (object form)
	var obj map[string]string
	if json.Unmarshal(pkg.Bin, &obj) != nil {
		return ""
	}
	for _, target := range obj {
		if binResolvesTo(filepath.Join(pkgDir, target), start) {
			return pkgDir
		}
	}
	return ""
}

// binResolvesTo reports whether candidate (a bin target from package.json)
// resolves to the same file as start.
func binResolvesTo(candidate, start string) bool {
	resolved, err := filepath.EvalSymlinks(candidate)
	if err != nil {
		return false
	}
	return resolved == start
}

// extractLibraryPath extracts the absolute library path from one line of
// ldd output. It returns "" for lines without a usable path (vdso,
// "not found", "statically linked", ...).
func extractLibraryPath(line string) string {
	line = strings.TrimSpace(line)
	if line == "" {
		return ""
	}
	// "libfoo.so.1 => /path/to/libfoo.so.1 (0x...)" — take the right side.
	if idx := strings.Index(line, " => "); idx >= 0 {
		line = line[idx+len(" => "):]
	}
	// Cut at the first " (0x..." annotation or stray space.
	if idx := strings.IndexAny(line, " ("); idx >= 0 {
		line = line[:idx]
	}
	line = strings.TrimSpace(line)
	if !strings.HasPrefix(line, "/") {
		return ""
	}
	return line
}

// copyFileContents copies the content of src to dst with the given mode.
func copyFileContents(src, dst string, mode os.FileMode) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}

// devIno identifies a directory on the host filesystem, used to detect
// symlink cycles when following links.
type devIno struct {
	dev uint64
	ino uint64
}

// devInoOf returns the (device, inode) pair of path (following symlinks),
// or ok=false if it cannot be determined.
func devInoOf(path string) (devIno, bool) {
	st, err := os.Stat(path)
	if err != nil {
		return devIno{}, false
	}
	sys, ok := st.Sys().(*syscall.Stat_t)
	if !ok {
		return devIno{}, false
	}
	return devIno{dev: uint64(sys.Dev), ino: sys.Ino}, true
}

// copyDirTree recursively copies srcDir to dstDir. When follow is true,
// symlinks are followed (their targets' content is copied, with the
// target's mode), dangling symlinks are logged and skipped, and
// directories are tracked by (device, inode) so a symlink pointing at an
// ancestor — or at / — terminates instead of recursing unboundedly;
// otherwise symlinks are preserved as symlinks and visited may be nil.
func copyDirTree(srcDir, dstDir string, follow bool, visited map[devIno]struct{}, log *log.Logger) error {
	if follow {
		if key, ok := devInoOf(srcDir); ok {
			if _, seen := visited[key]; seen {
				// Already copied elsewhere in this tree. Besides breaking
				// cycles, this means a second symlink pointing at the same
				// external directory is skipped with no entry created at
				// all (the path simply does not exist in the jail). That
				// dedup side effect is accepted: cycle protection takes
				// priority over duplicating an already-copied directory
				// (review feedback on PR #174).
				return nil
			}
			visited[key] = struct{}{}
		}
	}
	entries, err := os.ReadDir(srcDir)
	if err != nil {
		return fmt.Errorf("read %s: %w", srcDir, err)
	}
	for _, entry := range entries {
		src := filepath.Join(srcDir, entry.Name())
		dst := filepath.Join(dstDir, entry.Name())

		info, err := os.Lstat(src)
		if err != nil {
			return fmt.Errorf("lstat %s: %w", src, err)
		}
		mode := info.Mode()

		switch {
		case mode&os.ModeSymlink != 0:
			target, err := os.Readlink(src)
			if err != nil {
				return fmt.Errorf("readlink %s: %w", src, err)
			}
			if !follow {
				if err := os.Symlink(target, dst); err != nil {
					return fmt.Errorf("symlink %s: %w", dst, err)
				}
				continue
			}
			// Follow: resolve the target and copy its content.
			resolved := target
			if !filepath.IsAbs(resolved) {
				resolved = filepath.Join(filepath.Dir(src), resolved)
			}
			stat, err := os.Stat(resolved)
			if err != nil {
				// Dangling symlink: the target is gone, so there is nothing
				// worth copying. Log and skip instead of failing the whole
				// tree — one stale link in ~/.pi or the pi package must not
				// brick every task's jail setup (review feedback on PR #174).
				log.Printf("chroot: skipping dangling symlink %s -> %s: %v", src, target, err)
				continue
			}
			if stat.IsDir() {
				if key, ok := devInoOf(resolved); ok {
					if _, seen := visited[key]; seen {
						// Cycle (e.g. a symlink back at an ancestor): skip
						// entirely to avoid unbounded recursion.
						continue
					}
				}
				if err := os.MkdirAll(dst, stat.Mode().Perm()); err != nil {
					return err
				}
				if err := copyDirTree(resolved, dst, true, visited, log); err != nil {
					return err
				}
			} else {
				// Use the target's mode: a hard-coded 0644 would drop the
				// exec bit and would leave a credential reached via symlink
				// world-readable inside the 0755 jail (review feedback on
				// PR #174).
				if err := copyFileContents(resolved, dst, stat.Mode().Perm()); err != nil {
					return fmt.Errorf("copy symlink target %s -> %s: %w", src, dst, err)
				}
			}
		case mode.IsDir():
			if err := os.MkdirAll(dst, mode.Perm()); err != nil {
				return err
			}
			if err := copyDirTree(src, dst, follow, visited, log); err != nil {
				return err
			}
		default:
			if err := copyFileContents(src, dst, mode.Perm()); err != nil {
				return fmt.Errorf("copy %s -> %s: %w", src, dst, err)
			}
		}
	}
	return nil
}

// capSysChroot is the capability bit required to call chroot(2), per
// <linux/capability.h>.
const capSysChroot = 18

// CanChroot reports whether the current process can call chroot(2), i.e.
// whether CAP_SYS_CHROOT is set in the effective capability set.
func CanChroot() (bool, error) {
	data, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return false, fmt.Errorf("read /proc/self/status: %w", err)
	}
	return parseCapBit(string(data), capSysChroot)
}

// CanMknod reports whether the current process can create character
// device nodes. It probes with a real mknod(2) call in a temp directory
// rather than checking capability bits, because the bits alone are not
// sufficient: container runtimes (e.g. rootless Podman) can present the
// container's root with a full capability set while the device cgroup
// still denies mknod(2).
func CanMknod() (bool, error) {
	dir, err := os.MkdirTemp("", "hotelier-mknod-probe-*")
	if err != nil {
		return false, fmt.Errorf("create probe dir: %w", err)
	}
	defer os.RemoveAll(dir)
	// Probe with a /dev/null-equivalent node (major 1, minor 3).
	if err := syscall.Mknod(filepath.Join(dir, "probe"), syscall.S_IFCHR|0o600, 1<<8|3); err != nil {
		return false, nil
	}
	return true, nil
}

// parseCapBit extracts the given capability bit from the content of
// /proc/self/status.
func parseCapBit(status string, bit int) (bool, error) {
	for _, line := range strings.Split(status, "\n") {
		if !strings.HasPrefix(line, "CapEff:") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 2 {
			return false, fmt.Errorf("unexpected CapEff line %q", line)
		}
		caps, err := strconv.ParseUint(fields[1], 16, 64)
		if err != nil {
			return false, fmt.Errorf("parse CapEff %q: %w", fields[1], err)
		}
		return caps&(1<<bit) != 0, nil
	}
	return false, fmt.Errorf("CapEff not found in /proc/self/status")
}
