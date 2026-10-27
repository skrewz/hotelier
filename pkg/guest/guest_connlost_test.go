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

		lost := g.connLostSignal()
		select {
		case <-lost:
			// expected — the loss was reported
		case <-time.After(2 * time.Second):
			t.Fatalf("iteration %d: server closed the connection immediately but connLost was never signalled", i)
		}

		// Tear down for the next iteration, mirroring what the Start loop
		// does between reconnects (Connect installs the fresh channel).
		if c := g.rpcClient(); c != nil {
			c.Close()
			g.setRPCClient(nil)
		}
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

// TestGuest_StaleOnCloseDoesNotCloseTheFreshChannel pins the
// channel-identity guard in connLostCallback. A client that has already
// been replaced can still be winding down when its socket finally dies;
// its late onClose must not close the channel the *current* connection is
// waiting on, or Start() tears a healthy connection down and the guest
// reconnects in a loop (issue #218). The positive control at the end
// proves the reports really do reach the guest, so the test cannot pass by
// silently dropping every connection-lost signal.
func TestGuest_StaleOnCloseDoesNotCloseTheFreshChannel(t *testing.T) {
	srv, wsURL := startTestServer(t)

	cfg := config.GuestConfig{
		Name:              "Stale OnClose Guest",
		Tags:              []string{"test"},
		URL:               wsURL,
		HeartbeatInterval: 3600,
	}
	g := New(cfg, func(ctx context.Context, task TaskAssignment, _ LogCallback) (*TaskResult, error) {
		return &TaskResult{TaskID: task.TaskID, Success: true}, nil
	})
	t.Cleanup(func() {
		for _, id := range srv.Hub().GetAllConnectionIDs() {
			if c, ok := srv.Hub().GetConnection(id); ok {
				c.Close()
			}
		}
	})

	// First attempt: bound to the channel Connect installed.
	if err := g.Connect(); err != nil {
		t.Fatalf("first connect: %v", err)
	}
	firstIDs := srv.Hub().GetAllConnectionIDs()
	if len(firstIDs) != 1 {
		t.Fatalf("expected exactly one server connection after the first connect, got %d", len(firstIDs))
	}
	stale := g.connLostSignal()

	// Second attempt: Connect must hand it a channel of its own.
	if err := g.Connect(); err != nil {
		t.Fatalf("second connect: %v", err)
	}
	fresh := g.connLostSignal()
	if fresh == stale {
		t.Fatal("the second connect reused the first attempt's connLost channel")
	}

	// The first attempt's connection is the one that was already there.
	staleConn, ok := srv.Hub().GetConnection(firstIDs[0])
	if !ok {
		t.Fatalf("connection %s not found", firstIDs[0])
	}
	staleConn.Close()

	// The stale client's report must never close the fresh channel. Watch
	// for a whole second so a report that lands late is still caught.
	for i := 0; i < 100; i++ {
		select {
		case <-fresh:
			t.Fatal("a late onClose from a replaced client closed the fresh connLost channel")
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	// Positive control: losing the live connection must close that channel.
	for _, id := range srv.Hub().GetAllConnectionIDs() {
		if c, ok := srv.Hub().GetConnection(id); ok {
			c.Close()
		}
	}
	select {
	case <-fresh:
		// expected — the live connection reported its own loss
	case <-time.After(2 * time.Second):
		t.Fatal("losing the live connection did not signal connLost; the guard was never exercised")
	}
}

// TestGuest_FailedAttemptDoesNotPoisonTheNextConnection covers the path
// Start's error branch takes: the socket dies while guest.register is in
// flight, so the attempt reports its own loss and the loop retries without
// ever having had a successful cycle. The next attempt must not inherit
// that already-closed channel — if it did, Start's select would fire
// "connection lost" on a healthy connection the moment it connected.
func TestGuest_FailedAttemptDoesNotPoisonTheNextConnection(t *testing.T) {
	upgrader := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		_ = conn.Close()
	})
	dying := httptest.NewServer(mux)
	t.Cleanup(dying.Close)

	_, healthyURL := startTestServer(t)

	g := New(config.GuestConfig{
		Name:              "Failed Attempt Guest",
		Tags:              []string{"test"},
		URL:               "ws" + strings.TrimPrefix(dying.URL, "http") + "/ws",
		HeartbeatInterval: 3600,
	}, func(ctx context.Context, task TaskAssignment, _ LogCallback) (*TaskResult, error) {
		return &TaskResult{TaskID: task.TaskID, Success: true}, nil
	})

	if err := g.connectAndRegister(); err == nil {
		t.Fatal("expected the attempt against a host that drops the socket to fail")
	}

	// The healthy attempt the Start loop makes next.
	g.config.URL = healthyURL
	if err := g.connectAndRegister(); err != nil {
		t.Fatalf("connect and register on the healthy host: %v", err)
	}

	lost := g.connLostSignal()
	select {
	case <-lost:
		t.Fatal("the healthy connection inherited the failed attempt's closed connLost channel")
	case <-time.After(500 * time.Millisecond):
		// expected — the connection was not declared lost
	}
}

// TestGuest_ConnLostSignalIsNotGatedByTheGuestMutex pins the reason connMu
// exists. ExecuteTask holds g.mu across the task.acknowledge Call, and that
// Call can wait out the full RPC timeout on a dead socket; if reporting a
// lost connection had to queue behind g.mu, the reconnect would stall for
// that whole window — the "connected to nothing" symptom issue #218 is
// about, and invisible to -race.
func TestGuest_ConnLostSignalIsNotGatedByTheGuestMutex(t *testing.T) {
	g := New(config.GuestConfig{ID: "test", Name: "Test", Tags: []string{"test"}},
		func(ctx context.Context, task TaskAssignment, _ LogCallback) (*TaskResult, error) {
			return &TaskResult{TaskID: task.TaskID, Success: true}, nil
		})

	lostCh := g.connLostSignal()
	onClose := g.connLostCallback(lostCh)

	// Stand in for ExecuteTask: g.mu held, an RPC in flight.
	g.mu.Lock()
	defer g.mu.Unlock()

	reported := make(chan struct{})
	go func() {
		onClose()
		close(reported)
	}()

	select {
	case <-reported:
		// expected — reporting the loss does not wait on g.mu
	case <-time.After(2 * time.Second):
		t.Fatal("the connection-lost signal queued behind g.mu; reconnecting would stall behind the in-flight Call")
	}

	select {
	case <-lostCh:
		// expected
	default:
		t.Fatal("the connection loss was not signalled")
	}
}
