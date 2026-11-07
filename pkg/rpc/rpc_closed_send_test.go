package rpc

import (
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"
)

// Regression tests for issue #242: a broadcast racing with a connection's
// unregistration could send on the closed send channel and panic, taking the
// whole server down.

func waitConnCount(t *testing.T, hub *Hub, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if hub.ConnectionCount() == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("connection count never reached %d", want)
}

// A connection that has been unregistered must reject subsequent sends with
// an error rather than panicking on the closed send channel.
func TestSendAfterUnregisterReturnsError(t *testing.T) {
	hub := newTestHub(t)
	go hub.Run()

	conn := NewTestConnection("conn-1", hub)
	hub.Register(conn)
	waitConnCount(t, hub, 1)

	hub.Unregister(conn)
	waitConnCount(t, hub, 0)

	// Broadcast holds no reference here, but SendTo/Send on a stale pointer
	// (exactly what a racing Broadcast does) must not panic.
	msg := &JSONRPCMessage{JSONRPC: "2.0", Method: "task.updated"}
	if err := conn.Send(msg); !errors.Is(err, ErrConnectionClosed) {
		t.Errorf("expected ErrConnectionClosed from an unregistered connection, got %v", err)
	}
}

// SendTo must likewise fail cleanly once the connection is gone from the hub.
func TestSendToAfterUnregisterReturnsError(t *testing.T) {
	hub := newTestHub(t)
	go hub.Run()

	conn := NewTestConnection("conn-1", hub)
	hub.Register(conn)
	waitConnCount(t, hub, 1)

	hub.Unregister(conn)
	waitConnCount(t, hub, 0)

	if err := hub.SendTo("conn-1", &JSONRPCMessage{JSONRPC: "2.0", Method: "task.updated"}); err == nil {
		t.Error("expected SendTo to fail for an unregistered connection")
	}
}

// TestHubUnregisterDoesNotCloseSendQueue mirrors the client-side
// TestClientHubUnregisterDoesNotCloseClientQueue (issue #217) on the server
// side. Unregistering a connection twice must not close its send queue twice,
// and an unregistered connection must keep refusing sends through the closed
// flag rather than through a closed channel. Nothing needs the queue closed:
// Close sets closed and closes closeCh before the hub ever sees the
// connection, so WriteLoop leaves through the closeCh case (issue #242).
func TestHubUnregisterDoesNotCloseSendQueue(t *testing.T) {
	hub := newTestHub(t)
	go hub.Run()

	conn := NewTestConnection("conn-double-unregister", hub)
	for i := 0; i < 50; i++ {
		hub.Register(conn)
		hub.Unregister(conn)
	}

	waitConnCount(t, hub, 0)

	if err := conn.Send(&JSONRPCMessage{JSONRPC: "2.0", Method: "task.updated"}); !errors.Is(err, ErrConnectionClosed) {
		t.Errorf("expected ErrConnectionClosed from an unregistered connection, got %v", err)
	}
}

// Concurrent broadcast and unregister churn — the pattern from the crash log
// (broadcastTaskUpdated racing with WriteLoop-triggered Close) — must never
// panic. A send on a closed channel would kill the test binary outright.
func TestConcurrentSendAndUnregisterDoesNotPanic(t *testing.T) {
	hub := newTestHub(t)
	go hub.Run()

	const rounds = 200
	var wg sync.WaitGroup

	conn := NewTestConnection("conn-churn", hub)
	hub.Register(conn)
	waitConnCount(t, hub, 1)

	wg.Add(3)
	go func() {
		defer wg.Done()
		msg := &JSONRPCMessage{JSONRPC: "2.0", Method: "task.updated", Params: json.RawMessage(`{}`)}
		for i := 0; i < rounds; i++ {
			_ = conn.Send(msg)
		}
	}()
	go func() {
		defer wg.Done()
		msg := &JSONRPCMessage{JSONRPC: "2.0", Method: "task.updated"}
		for i := 0; i < rounds; i++ {
			hub.Broadcast("", msg)
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < rounds; i++ {
			replacement := NewTestConnection("conn-churn", hub)
			hub.Register(replacement)
			hub.Unregister(replacement)
		}
	}()

	finished := make(chan struct{})
	go func() {
		wg.Wait()
		close(finished)
	}()
	// All three goroutines are waited for: returning while a sender was still
	// running would leave it broadcasting into this hub and could attribute a
	// race report to whichever test happened to run next.
	select {
	case <-finished:
	case <-time.After(30 * time.Second):
		t.Fatal("churn test timed out")
	}
}
