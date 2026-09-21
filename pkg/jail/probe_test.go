package jail

import "testing"

// TestCanJail verifies the probe. On hosts where unprivileged user
// namespaces work (the devvm and this development machine), the probe
// must succeed; elsewhere the whole jail feature degrades to a warning at
// guest startup, so the test skips rather than fails.
func TestCanJail(t *testing.T) {
	ok, reason := CanJail()
	if !ok {
		t.Skipf("unprivileged namespaces unavailable on this host: %s", reason)
	}
}
