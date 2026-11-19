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
// read-only directories. os.RemoveAll needs write permission on every
// directory whose entries it unlinks, so it aborts on trees that are
// read-only by design — the Go module cache (0555 dirs, 0444 files) being
// the case that filled guest disks in issue #301: jail roots hold copies of
// the guest's home trees with those modes preserved, so once-per-task the
// cleanup failed and the whole tree survived.
//
// It first tries os.RemoveAll. If that fails with a permission error, it
// restores the owner read and write bits on every directory in the tree
// (best-effort: entries the caller does not own are skipped) and retries
// once. Errors that cannot be fixed this way — a read-only filesystem, or
// directories owned by a uid outside the caller's reach — are returned. A
// missing path is not an error, matching os.RemoveAll.
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
