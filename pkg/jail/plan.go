package jail

import (
	"fmt"
	"path/filepath"

	"golang.org/x/sys/unix"
)

// BuildPlan turns host facts into a jail plan. It is a pure function:
// no I/O, no privileges — the mount layout is data.
func BuildPlan(f Facts) (Plan, error) {
	if f.TaskDir == "" {
		return Plan{}, fmt.Errorf("jail: task dir is required")
	}
	if f.PiRoot == "" {
		return Plan{}, fmt.Errorf("jail: pi root is required")
	}

	p := Plan{
		JailRoot: f.TaskDir + ".jail",
		// The task dir is mounted at the fixed jail path /task (not at
		// its host path): a /tmp tmpfs would hide a mount under /tmp,
		// and /task is short and predictable for the agent.
		Cwd: "/task",
		// The child drops into a nested user namespace as this id before
		// exec, so the command holds no capabilities in the outer
		// namespace (issue #198).
		DropToUID: DropID,
	}

	// A fresh /dev tmpfs. The mode must not be the tmpfs default 1777:
	// inside a user namespace, O_CREAT on a device file in a
	// sticky+world-writable directory is denied (verified on kernels
	// 7.1 and 7.2), which would break shell redirections to /dev/null.
	// MS_NODEV keeps the placeholder files inert (the bound device
	// nodes sit on their own mounts, unaffected); it must be a flag,
	// not a data option, inside a user namespace. bubblewrap mounts its
	// /dev the same way (mode=755, nodev).
	// The device nodes themselves are bound onto it by the jail child
	// (Plan.DevNodes), as are a fresh devpts and /dev/shm.
	p.Mounts = append(p.Mounts, Mount{Kind: MountTmpfs, Path: "/dev", Opts: "mode=755", Flags: int(unix.MS_NODEV)})

	// Read-only toolchain mounts.
	for _, d := range f.USRDirs {
		p.Mounts = append(p.Mounts, Mount{Kind: MountBindRO, Path: d, Src: d})
	}
	p.DevNodes = f.DevNodes

	// The pi install root (read-only) and the task directory (the only
	// read-write path, at its fixed jail path).
	p.Mounts = append(p.Mounts,
		Mount{Kind: MountBindRO, Path: f.PiRoot, Src: f.PiRoot},
		Mount{Kind: MountBindRW, Path: "/task", Src: f.TaskDir},
	)

	// A private /tmp and a scoped /proc.
	p.Mounts = append(p.Mounts,
		Mount{Kind: MountTmpfs, Path: "/tmp"},
		Mount{Kind: MountProc, Path: "/proc"},
	)

	// Per-task copies: /etc files (symlink-resolved), the CA bundle and
	// the home dot-dirs.
	p.CopyFiles = append(p.CopyFiles, f.EtcFiles...)
	if f.HasCertDir {
		p.CopyTrees = append(p.CopyTrees, "/etc/ssl/certs")
	}
	for _, d := range f.HomeDotDirs {
		p.CopyTrees = append(p.CopyTrees, filepath.Join(f.HomeDir, d))
	}

	// Usrmerge shims so #!/bin/sh and friends resolve.
	if f.USRmerged {
		p.Symlinks = []Link{
			{Old: "usr/bin", New: "bin"},
			{Old: "usr/sbin", New: "sbin"},
			{Old: "usr/lib", New: "lib"},
			{Old: "usr/lib64", New: "lib64"},
		}
	}
	return p, nil
}
