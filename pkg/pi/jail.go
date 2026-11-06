package pi

// BuildJailCommand builds the command line that spawns pi inside the
// unshare-based jail (issue #51).
//
// The unshare(1) waiter creates the user, mount and pid namespaces
// (--map-root-user --mount --pid --fork) and re-execs the guest binary
// as the jail child (-jail-child <spec>). The jail child (pkg/jail
// RunChild) performs the bind mounts, pivots the root and execs pi.
//
// Signal handling:
//   - --forward-signals forwards SIGTERM/SIGINT received by the waiter
//     to the child, so Stop() can ask pi for a graceful shutdown.
//   - --kill-child=SIGKILL sends SIGKILL to the forked child (the
//     namespace's pid 1) when the waiter terminates; the kernel then
//     tears down the pid namespace, killing everything inside. Verified
//     to fire even when the waiter itself is SIGKILLed, so no process
//     is orphaned and no mount is pinned.
//
// The --fork is mandatory: a process that created a pid namespace (and
// is not pid 1 in it) cannot fork after mounting inside it.
//
// guestBin is the guest binary to re-exec (os.Executable()); specPath is
// the jail spec file on the host; piBin and piArgs are the command to
// exec inside the jail. mapGuestRange selects the range-mapped mode
// (issue #236): --map-auto then also maps the guest user's
// subuid/subgid block into the outer namespace, matching the multi-entry
// drop the jail child performs for such tasks. The functional floor is
// util-linux 2.40 (the drop passes repeated --map-users/--map-groups
// arguments and those only accumulate from 2.40; see unshare(1)); the
// supported floor is 2.41.6 (2.42.3 on the 2.42 series) because the
// spawn chain ends in an unshare exec and CVE-2026-78408 leaks
// descriptors across it on earlier releases — see README.md. Without the
// flag the command is byte-identical to the default spawn.
func BuildJailCommand(guestBin, specPath, piBin string, mapGuestRange bool, piArgs []string) []string {
	cmd := []string{
		"unshare",
		"--map-root-user",
	}
	if mapGuestRange {
		// --map-root-user takes precedence for id 0; the --map-auto
		// block is shifted to start at inner id 1 (unshare(1)), leaving
		// the single-UID mapping intact and the range above it.
		cmd = append(cmd, "--map-auto")
	}
	cmd = append(cmd,
		"--mount", "--pid", "--fork",
		"--forward-signals", "--kill-child=SIGKILL",
		"--", guestBin, "-jail-child", specPath,
		"--", piBin,
	)
	return append(cmd, piArgs...)
}
