package pi

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// TestProcessStateAccessorsRaceFree hammers the process-state accessors
// (IsRunning, GetProcessState, GetExitCode) from multiple goroutines while a
// short-lived subprocess starts and exits.
//
// Under -race this fails if any accessor reads cmd.ProcessState without
// synchronising with the Wait() goroutine: cmd.Wait() writes
// cmd.ProcessState when the process exits, which is a data race against
// unsynchronised reads (pre-existing race, exposed flakily by
// TestPiClient_StopActuallyTerminatesProcess).
func TestProcessStateAccessorsRaceFree(t *testing.T) {
	// Fake pi binary that exits immediately, resolved via PATH.
	tmpDir := t.TempDir()
	fakePi := filepath.Join(tmpDir, "pi")
	if err := os.WriteFile(fakePi, []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatalf("write fake pi: %v", err)
	}
	t.Setenv("PATH", tmpDir+string(os.PathListSeparator)+os.Getenv("PATH"))

	c := NewClient(PiClientConfig{CWD: tmpDir})
	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("start failed: %v", err)
	}
	defer func() { _ = c.Stop(context.Background()) }()

	// Hammer the accessors while the fake pi exits and the Wait goroutine
	// populates the process state.
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					_ = c.IsRunning()
					_ = c.GetProcessState()
					_ = c.GetExitCode()
				}
			}
		}()
	}
	time.Sleep(500 * time.Millisecond)
	close(stop)
	wg.Wait()

	// After exit the accessors must report a consistent state.
	if c.IsRunning() {
		t.Error("client should not be running after fake pi exited")
	}
	if c.GetProcessState() == nil {
		t.Error("process state should be non-nil after exit")
	}
}
