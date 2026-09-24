package jail

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"syscall"
)

// discardLog is used for skeleton-side copy operations: dangling-symlink
// skips are not worth surfacing (the old chroot code logged them, but
// the skeleton has no caller logger of its own).
var discardLog = log.New(io.Discard, "", 0)

// CreateSkeleton builds the jail root on the host: a mount point for
// every mount in the plan (a directory for directory sources, a regular
// file for file sources such as device nodes), the usrmerge symlinks,
// and the resolved copies of the /etc files and trees. It writes
// nothing outside the jail root and requires no privileges.
func CreateSkeleton(p Plan) error {
	if err := os.MkdirAll(p.JailRoot, 0o755); err != nil {
		return fmt.Errorf("create jail root %s: %w", p.JailRoot, err)
	}

	for _, m := range p.Mounts {
		mp := filepath.Join(p.JailRoot, m.Path)
		switch m.Kind {
		case MountTmpfs, MountProc:
			if err := os.MkdirAll(mp, 0o755); err != nil {
				return fmt.Errorf("create mount point %s: %w", mp, err)
			}
		default: // bind mounts: the point must exist before mount(2)
			if err := os.MkdirAll(filepath.Dir(mp), 0o755); err != nil {
				return fmt.Errorf("create parent of %s: %w", mp, err)
			}
			if fi, err := os.Stat(m.Src); err == nil && !fi.IsDir() {
				// File bind source (e.g. a device node): a regular file
				// is a valid bind target and is replaced by the mount.
				f, err := os.OpenFile(mp, os.O_CREATE, 0o644)
				if err != nil {
					return fmt.Errorf("create file mount point %s: %w", mp, err)
				}
				_ = f.Close()
			} else {
				if err := os.MkdirAll(mp, 0o755); err != nil {
					return fmt.Errorf("create mount point %s: %w", mp, err)
				}
			}
		}
	}

	for _, l := range p.Symlinks {
		dst := filepath.Join(p.JailRoot, l.New)
		if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
			return fmt.Errorf("create parent of %s: %w", dst, err)
		}
		if err := os.Symlink(l.Old, dst); err != nil {
			return fmt.Errorf("symlink %s -> %s: %w", l.New, l.Old, err)
		}
	}

	for _, src := range p.CopyFiles {
		dst := filepath.Join(p.JailRoot, src)
		if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
			return fmt.Errorf("create parent of %s: %w", dst, err)
		}
		if err := copyFileResolved(src, dst); err != nil {
			return fmt.Errorf("copy %s: %w", src, err)
		}
	}

	for _, src := range p.CopyTrees {
		dst := filepath.Join(p.JailRoot, src)
		if err := os.MkdirAll(dst, 0o755); err != nil {
			return fmt.Errorf("create %s: %w", dst, err)
		}
		if err := copyDirTree(src, dst, true, map[devIno]struct{}{}, discardLog); err != nil {
			return fmt.Errorf("copy %s: %w", src, err)
		}
	}
	return nil
}

// WriteSpec writes the spec file into the jail root so the jail child
// can read it at /spec.json after the pivot.
func WriteSpec(p Plan) error {
	spec := Spec{JailRoot: p.JailRoot, Cwd: p.Cwd, Mounts: p.Mounts, DevNodes: p.DevNodes, DropToUID: p.DropToUID}
	data, err := json.Marshal(spec)
	if err != nil {
		return fmt.Errorf("marshal jail spec: %w", err)
	}
	return os.WriteFile(p.SpecPath(), data, 0o644)
}

// copyFileResolved copies src to dst, following symlinks (the host's
// /etc/resolv.conf is a symlink on systemd-resolved hosts).
func copyFileResolved(src, dst string) error {
	info, err := os.Stat(src) // follows symlinks
	if err != nil {
		return err
	}
	if info.IsDir() {
		return fmt.Errorf("source %s is a directory", src)
	}
	return copyFileContents(src, dst, info.Mode().Perm())
}

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
// target's mode), dangling symlinks are skipped, and directories are
// tracked by (device, inode) so a symlink pointing at an ancestor
// terminates instead of recursing unboundedly. Ported from pkg/chroot.
func copyDirTree(srcDir, dstDir string, follow bool, visited map[devIno]struct{}, log *log.Logger) error {
	if follow {
		if key, ok := devInoOf(srcDir); ok {
			if _, seen := visited[key]; seen {
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
			resolved := target
			if !filepath.IsAbs(resolved) {
				resolved = filepath.Join(filepath.Dir(src), resolved)
			}
			stat, err := os.Stat(resolved)
			if err != nil {
				// Dangling symlink: skip rather than fail the whole tree.
				log.Printf("jail: skipping dangling symlink %s -> %s: %v", src, target, err)
				continue
			}
			if stat.IsDir() {
				if key, ok := devInoOf(resolved); ok {
					if _, seen := visited[key]; seen {
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
				// world-readable inside the jail.
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
