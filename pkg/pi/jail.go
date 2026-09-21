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
// exec inside the jail.
func BuildJailCommand(guestBin, specPath, piBin string, piArgs []string) []string {
	cmd := []string{
		"unshare",
		"--map-root-user", "--mount", "--pid", "--fork",
		"--forward-signals", "--kill-child=SIGKILL",
		"--", guestBin, "-jail-child", specPath,
		"--", piBin,
	}
	return append(cmd, piArgs...)
}
