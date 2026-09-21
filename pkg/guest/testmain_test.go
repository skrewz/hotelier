package guest

import (
	"flag"
	"os"
	"testing"

	"hotelier/pkg/jail"
)

// TestMain handles the -jail-child re-exec: when the test binary is
// launched as the jail child (by the pi client's BuildJailCommand, which
// re-execs os.Executable()), it runs the jail child instead of the test
// suite. This mirrors the guest binary's -jail-child flag (cmd/guest)
// and the pkg/pi TestMain.
func TestMain(m *testing.M) {
	jailChildSpec := flag.String("jail-child", "", "internal: run as the jail child (re-exec under unshare)")
	flag.Parse()
	if *jailChildSpec != "" {
		os.Exit(jail.RunChildMain(*jailChildSpec))
	}
	os.Exit(m.Run())
}
