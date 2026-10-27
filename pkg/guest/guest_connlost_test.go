package guest

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"hotelier/internal/server"
	"hotelier/pkg/config"

	"github.com/gorilla/websocket"
)

// TestGuest_ReconnectLoop_ConnLostRaceFree reconnects the guest repeatedly
// against a real server, dropping the connection at the server end after
// every successful registration. Each drop makes the guest's dying read loop
// run setConnLost (through Client.fail) concurrently with the Start loop's
// resetConn — exactly the window issue #218 describes. Under -race this
// fails while setConnLost reads g.connLost without holding g.mu, and while
// Client.SetOnClose writes c.onClose after the read loop that reads it was
// spawned.
func TestGuest_ReconnectLoop_ConnLostRaceFree(t *testing.T) {
	srv, wsURL := startTestServer(t)

	cfg := config.GuestConfig{
		Name:              "Reconnect Race Guest",
		Tags:              []string{"test"},
		URL:               wsURL,
		HeartbeatInterval: 3600, // keep the heartbeat loop out of the picture
	}
	g := New(cfg, func(ctx context.Context, task TaskAssignment, _ LogCallback) (*TaskResult, error) {
		return &TaskResult{TaskID: task.TaskID, Success: true}, nil
	})
	go func() { _ = g.Start() }()
	t.Cleanup(func() { g.Stop() })

	prevConnID := ""
	for i := 0; i < 25; i++ {
		connID := awaitRegistration(t, g, srv, prevConnID)
		prevConnID = connID
		// Simulate the connection dropping by closing it at the far end.
		// A guest-side Client.Close is the graceful path and deliberately
		// reports no loss (issue #217), so the drop has to reach the guest
		// as a read error in its read loop. That dying read loop then
		// signals connLost while Start tears the connection down and calls
		// resetConn, racing the connLost field write.
		if conn, ok := srv.Hub().GetConnection(connID); ok {
			conn.Close()
		}
	}

	// The guest must still be functional after the churn.
	awaitRegistration(t, g, srv, prevConnID)
}

// awaitRegistration waits until the server has recorded a new connection
// for the guest (i.e. the guest.register round-trip has been processed)
// and returns that connection's id. Dropping the connection before
// registration completes would leave Register's Call awaiting a response
// that can no longer arrive, stalling the Start loop for reasons
// unrelated to issue #218.
func awaitRegistration(t *testing.T, g *Guest, srv *server.Server, prevConnID string) string {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if c := g.rpcClient(); c != nil {
			if connID, ok := srv.Hub().GetGuestConnectionID(g.id); ok && connID != prevConnID {
				// The register response has been queued by the server;
				// give the client a grace period to receive it before
				// dropping the socket.
				time.Sleep(50 * time.Millisecond)
				return connID
			}
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("timed out waiting for the guest to (re)register")
	return ""
}

// TestGuest_ImmediateCloseAfterUpgrade_SignalsConnLost connects the guest
// to a server that closes the socket immediately after the WebSocket
// upgrade. The guest must detect the loss (connLost is closed) instead of
// sitting in Start()'s select forever. Before the fix, the onClose
// callback was wired after the read loop had already been spawned, so a
// connection that died instantly could be seen as lost before the
// callback existed — the signal was then silently dropped (and reported
// as a data race by -race). The loop widens the window so at least one
// iteration hits it.
func TestGuest_ImmediateCloseAfterUpgrade_SignalsConnLost(t *testing.T) {
	upgrader := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}

	var upgrades atomic.Int64
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		upgrades.Add(1)
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		_ = conn.Close()
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	cfg := config.GuestConfig{
		Name:              "Immediate Close Guest",
		Tags:              []string{"test"},
		URL:               "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws",
		HeartbeatInterval: 3600,
	}
	g := New(cfg, func(ctx context.Context, task TaskAssignment, _ LogCallback) (*TaskResult, error) {
		return &TaskResult{TaskID: task.TaskID, Success: true}, nil
	})

	const iterations = 20
	for i := 0; i < iterations; i++ {
		if err := g.Connect(); err != nil {
			t.Fatalf("iteration %d: connect: %v", i, err)
		}

		g.mu.Lock()
		lost := g.connLost
		g.mu.Unlock()

		select {
		case <-lost:
			// expected — the loss was reported
		case <-time.After(2 * time.Second):
			t.Fatalf("iteration %d: server closed the connection immediately but connLost was never signalled", i)
		}

		// Tear down and reset for the next iteration, mirroring what
		// the Start loop does between reconnects.
		if c := g.rpcClient(); c != nil {
			c.Close()
			g.setRPCClient(nil)
		}
		g.resetConn()
	}

	if got := upgrades.Load(); got < iterations {
		t.Fatalf("expected at least %d WebSocket upgrades, got %d", iterations, got)
	}
}

// TestGuest_ConnLostDoubleCloseDoesNotPanic drives setConnLost from many
// goroutines while resetConn replaces the channel. Before the fix, two
// callers could both pass the "not yet closed" check on the same channel
// and the second close panicked, killing the guest process (issue #218).
func TestGuest_ConnLostDoubleCloseDoesNotPanic(t *testing.T) {
	cfg := config.GuestConfig{ID: "test", Name: "Test", Tags: []string{"test"}}
	g := New(cfg, func(ctx context.Context, task TaskAssignment, _ LogCallback) (*TaskResult, error) {
		return &TaskResult{TaskID: task.TaskID, Success: true}, nil
	})

	resetDone := make(chan struct{})
	go func() {
		defer close(resetDone)
		for i := 0; i < 200; i++ {
			g.resetConn()
		}
	}()

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
	wg.Wait()

	select {
	case <-resetDone:
	case <-time.After(5 * time.Second):
		t.Fatal("resetConn loop did not finish")
	}
	// Reaching here without a close-of-closed-channel panic is success.
}
