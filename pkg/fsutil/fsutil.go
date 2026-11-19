// Package fsutil provides filesystem helpers shared between the guest
// handler and the jail.
package fsutil

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
)

// RemoveAllForce removes a directory tree like os.RemoveAll, but survives
// read-only directories — unlinking an entry needs write permission on the
// directory it sits in, so os.RemoveAll aborts on trees that are read-only by
// design (the Go module cache: 0555 dirs, 0444 files), the leftovers of
// issue #301.
//
// It first tries os.RemoveAll; on a permission error it restores the owner
// read and write bits on every directory in the tree (best-effort: entries
// the caller does not own are skipped) and retries once. A missing path is
// not an error, matching os.RemoveAll. Errors chmod cannot fix — a read-only
// filesystem, directories owned by a uid outside the caller's reach — are
// returned. Only owner bits are added, and the walk does not follow
// symlinks. When the retry still fails the directories it made owner-writable
// stay that way, so a tree left behind is no longer read-only by design.
func RemoveAllForce(path string) error {
	err := os.RemoveAll(path)
	if err == nil || !errors.Is(err, fs.ErrPermission) {
		return err
	}
	_ = filepath.WalkDir(path, func(p string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			// Best effort: an unreadable subtree costs only the retry's
			// error, which RemoveAll reports anyway.
			return nil
		}
		if d.IsDir() {
			info, infoErr := d.Info()
			if infoErr == nil && info.Mode().Perm()&0o300 != 0o300 {
				_ = os.Chmod(p, info.Mode()|0o300)
			}
		}
		return nil
	})
	return os.RemoveAll(path)
}
