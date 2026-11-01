package guest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"hotelier/pkg/config"
	"hotelier/pkg/rpc"

	"github.com/gorilla/websocket"
)

// Tests for issue #230 (and its specification on issue #219): a heartbeat
// failure that indicates a broken transport must escalate to the reconnect
// loop instead of being logged forever. A guest whose heartbeats never
// reach the server is gone from the orchestrator's point of view; logging
// and retrying on the same dead connection wedges both the guest and its
// ASSIGNED tasks.

// quietHandler is a task handler that should never be invoked by these
// heartbeat tests.
func quietHandler() Handler {
	return func(ctx context.Context, task TaskAssignment, _ LogCallback) (*TaskResult, error) {
		return &TaskResult{TaskID: task.TaskID, Success: true}, nil
	}
}

// transportHeartbeatErr mimics what Heartbeat() returns when the socket is
// dead: the wrapped error is a plain transport error, not an RPCError.
func transportHeartbeatErr() error {
	return fmt.Errorf("heartbeat: all 3 attempts failed: %w",
		errors.New("use of closed network connection"))
}

// TestGuestHeartbeatLoop_TransportError_TriggersReconnect proves that a
// heartbeat failure whose error is not a server-side RPCError — the
// "use of closed network connection" pattern from issue #230 — signals
// connLost and exits the loop, handing reconnection to Start(). Before the
// fix the loop logged the failure and ticked forever.
func TestGuestHeartbeatLoop_TransportError_TriggersReconnect(t *testing.T) {
	cfg := config.GuestConfig{
		ID:                "test-guest-hb-transport",
		Name:              "Transport Heartbeat Guest",
		Tags:              []string{"test"},
		HeartbeatInterval: 1,
	}
	g := New(cfg, quietHandler())
	g.heartbeatForTest = transportHeartbeatErr

	done := make(chan struct{})
	go func() {
		g.heartbeatLoop(context.Background())
		close(done)
	}()

	select {
	case <-done:
		// expected — the loop must not keep retrying a dead transport
	case <-time.After(5 * time.Second):
		t.Fatal("heartbeatLoop still running after a transport heartbeat failure")
	}

	select {
	case <-g.connLost:
		// expected — Start() can now reconnect
	default:
		t.Error("expected connLost to be signalled after a transport heartbeat failure")
	}
}

// TestGuestHeartbeatLoop_NotConnected_TriggersReconnect covers the
// "heartbeat: not connected" path: Heartbeat() runs for real with a nil
// RPC client. That state means the guest is not on a working connection,
// so the loop must escalate rather than spin (issue #219 acceptance
// criteria). Before the fix this hit the log-and-continue branch forever.
func TestGuestHeartbeatLoop_NotConnected_TriggersReconnect(t *testing.T) {
	cfg := config.GuestConfig{
		ID:                "test-guest-hb-nilclient",
		Name:              "Nil Client Heartbeat Guest",
		Tags:              []string{"test"},
		HeartbeatInterval: 1,
	}
	g := New(cfg, quietHandler())
	// No Connect: g.client stays nil, so Heartbeat returns
	// "heartbeat: not connected" without any stubbing.

	done := make(chan struct{})
	go func() {
		g.heartbeatLoop(context.Background())
		close(done)
	}()

	select {
	case <-done:
		// expected
	case <-time.After(5 * time.Second):
		t.Fatal("heartbeatLoop still spinning with a nil RPC client")
	}

	select {
	case <-g.connLost:
		// expected
	default:
		t.Error("expected connLost to be signalled when the heartbeat reports not-connected")
	}
}

// TestGuestHeartbeatLoop_StaleFailure_DoesNotKillFreshConnection proves
// the escalation is bound to the connection the loop belongs to. A stale
// in-flight Heartbeat() from a replaced connection must not declare a
// fresh connection lost — the same discipline connLostCallback applies to
// onClose (issue #218), now applied to heartbeat escalation.
func TestGuestHeartbeatLoop_StaleFailure_DoesNotKillFreshConnection(t *testing.T) {
	cfg := config.GuestConfig{
		ID:                "test-guest-hb-stale",
		Name:              "Stale Heartbeat Guest",
		Tags:              []string{"test"},
		HeartbeatInterval: 1,
	}
	g := New(cfg, quietHandler())

	ran := make(chan struct{})
	var ranOnce sync.Once
	g.heartbeatForTest = func() error {
		// Simulate Start() having moved on to a new connection while
		// this call was in flight: resetConn hands the next attempt a
		// connLost channel of its own.
		ranOnce.Do(func() {
			g.resetConn()
			close(ran)
		})
		return transportHeartbeatErr()
	}

	done := make(chan struct{})
	go func() {
		g.heartbeatLoop(context.Background())
		close(done)
	}()

	select {
	case <-ran:
	case <-time.After(5 * time.Second):
		t.Fatal("heartbeat function was never called")
	}
	select {
	case <-done:
		// expected — the loop still exits on the failure
	case <-time.After(5 * time.Second):
		t.Fatal("heartbeatLoop did not exit after its stale failure")
	}

	fresh := g.connLostSignal()
	select {
	case <-fresh:
		t.Error("a stale heartbeat failure signalled the fresh connection's connLost channel")
	default:
	}
}

// TestGuest_Start_ReconnectsAfterHeartbeatTransportFailure is the end-to
// end case from issue #230: the server accepts the connection and then
// stops answering heartbeats (register still works). The guest must
// reconnect within a bounded time instead of wedging. Before the fix the
// heartbeatLoop logged "heartbeat failed" forever and never reconnected,
// so the upgrade count would stay at one.
func TestGuest_Start_ReconnectsAfterHeartbeatTransportFailure(t *testing.T) {
	upgrader := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}
	var upgrades atomic.Int64

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		upgrades.Add(1)
		go func() {
			defer conn.Close()
			for {
				_, data, err := conn.ReadMessage()
				if err != nil {
					return
				}
				var msg rpc.JSONRPCMessage
				if err := json.Unmarshal(data, &msg); err != nil {
					continue
				}
				// Answer requests, but never a guest.heartbeat —
				// the server went silent on heartbeats only.
				if msg.ID != nil && msg.Method != "guest.heartbeat" {
					resp := rpc.JSONRPCMessage{
						JSONRPC: "2.0",
						ID:      msg.ID,
						Result:  json.RawMessage("null"),
					}
					payload, err := json.Marshal(&resp)
					if err != nil {
						return
					}
					if err := conn.WriteMessage(websocket.TextMessage, payload); err != nil {
						return
					}
				}
			}
		}()
	})

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = http.Serve(ln, mux) }()
	t.Cleanup(func() { _ = ln.Close() })

	wsURL := fmt.Sprintf("ws://127.0.0.1:%d/ws", ln.Addr().(*net.TCPAddr).Port)
	cfg := config.GuestConfig{
		Name:              "Silent Heartbeat Host Guest",
		Tags:              []string{"test"},
		URL:               wsURL,
		HeartbeatInterval: 1,
	}
	g := New(cfg, quietHandler())
	go func() { _ = g.Start() }()
	t.Cleanup(func() { g.Stop() })

	// Wait for the first registration, then shrink the call timeout so
	// the heartbeat attempts fail fast instead of waiting out the
	// default 10s bound each.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if c := g.rpcClient(); c != nil && upgrades.Load() >= 1 {
			c.SetCallTimeout(50 * time.Millisecond)
			break
		}
		time.Sleep(time.Millisecond)
	}
	if upgrades.Load() < 1 {
		t.Fatal("guest never connected")
	}

	// Heartbeat ticks at 1s, retries 3 times with 1s+2s+4s backoff —
	// the escalation should land well inside the deadline. A pre-fix
	// guest would sit in the log-and-continue branch forever.
	deadline = time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if upgrades.Load() >= 2 {
			return // reconnected — success
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("guest never reconnected after persistent heartbeat failure (issue #230)")
}
