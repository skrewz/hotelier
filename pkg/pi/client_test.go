package pi

import (
	"context"
	"os/exec"
	"testing"
	"time"
)

// TestPiClient_StopActuallyTerminatesProcess verifies that Stop() causes the
// pi subprocess to actually exit within a reasonable time. This is a regression
// test: pi is a persistent RPC server that stays alive after stdin is closed,
// so we must kill the process rather than relying on stdin.Close() + cmd.Wait().
func TestPiClient_StopActuallyTerminatesProcess(t *testing.T) {
	if _, err := exec.LookPath("pi"); err != nil {
		t.Skip("pi not installed")
	}

	// Use a long-lived context so exec.CommandContext doesn't kill the process
	ctx := context.Background()

	c := NewClient(PiClientConfig{CWD: "/tmp"})

	if err := c.Start(ctx); err != nil {
		t.Fatalf("start failed: %v", err)
	}

	if !c.IsRunning() {
		t.Fatal("client should be running after Start")
	}

	// Send a prompt (will hang in plan mode, so give it a generous timeout)
	go func() {
		_ = c.Prompt(ctx, "say hi")
	}()

	// Wait a bit for the process to settle
	time.Sleep(2 * time.Second)

	// Verify process is still alive
	state := c.GetProcessState()
	if state != nil {
		t.Fatal("process should not have exited yet")
	}

	// Now try to stop — this is the critical part
	stopDone := make(chan error, 1)
	go func() {
		stopDone <- c.Stop(context.Background())
	}()

	select {
	case err := <-stopDone:
		t.Logf("Stop returned: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("Stop() did not return within 10 seconds — pi subprocess is not exiting " +
			"even with the force-kill fallback.")
	}

	// Verify process actually exited
	time.Sleep(200 * time.Millisecond)
	state = c.GetProcessState()
	if state == nil {
		t.Fatal("process state should not be nil after Stop")
	}
	if !state.Exited() {
		t.Fatal("pi subprocess should have exited after Stop")
	}
}
