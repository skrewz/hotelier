package jail

import (
	"context"
	"fmt"
	"os/exec"
	"time"
)

// CanJail probes whether the current process can create the
// unprivileged namespaces the jail needs (user, mount, pid). It runs
// the same unshare(1) invocation shape the production spawn uses, with
// a trivial command, and reports success or a human-readable reason.
//
// The probe is fail-soft: when it fails (e.g. a distro restriction on
// unprivileged user namespaces), the guest disables the jail and tasks
// run without isolation, mirroring the old CanChroot behaviour.
func CanJail() (bool, string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "unshare",
		"--map-root-user", "--mount", "--pid", "--fork",
		"--", "true",
	).CombinedOutput()
	if err != nil {
		return false, fmt.Sprintf("unshare --map-root-user --mount --pid --fork true: %v: %s", err, string(out))
	}
	return true, ""
}
