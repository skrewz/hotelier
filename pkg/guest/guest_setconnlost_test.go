package guest

import (
	"sync"
	"testing"
)

// TestSetConnLostRaceWithResetConn exercises the writer side of the
// connection-lost signal against a reconnect. setConnLost runs on the
// client's read goroutine while resetConn runs on Start's loop, so the
// channel it closes must be checked and closed under connMu — the
// unsynchronised read was surfaced by the keepalive reconnect test (issue
// #220). Run under -race this is the regression test for that data race;
// without the lock two callers can also both pass the "not yet closed"
// check and panic on the second close. setConnLost and resetConn both
// hold connMu (issue #218); this pins that discipline against regression.
func TestSetConnLostRaceWithResetConn(t *testing.T) {
	g := &Guest{connLost: make(chan struct{})}

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				g.setConnLost()
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for j := 0; j < 50; j++ {
			g.resetConn()
		}
	}()
	wg.Wait()
}
