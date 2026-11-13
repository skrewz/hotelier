package rpc

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// silentPeerServer is an httptest server that completes the WebSocket
// handshake and then never reads from or writes to the connection again.
// The socket stays apparently open at the TCP level: frames are buffered by
// the kernel and ACKed, but no pong ever comes back. This is the "silently
// dead link" scenario from issue #220.
type silentPeerServer struct {
	server  *httptest.Server
	mu      sync.Mutex
	held    []*websocket.Conn // retained so the GC finalizer cannot close them
	release chan struct{}
}

func newSilentPeerServer(t *testing.T) *silentPeerServer {
	t.Helper()
	s := &silentPeerServer{release: make(chan struct{})}
	logf := testLogf(t)
	upgrader := NewUpgrader()
	s.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.ServeHTTP(w, r)
		if err != nil {
			logf("silent peer upgrade failed: %v", err)
			return
		}
		s.mu.Lock()
		s.held = append(s.held, conn)
		s.mu.Unlock()
		// Never read from this connection; hold the reference until the
		// test finishes so nothing closes the socket.
		<-s.release
	}))
	t.Cleanup(func() {
		close(s.release)
		s.server.Close()
	})
	return s
}

func wsAddr(url string) string {
	return "ws://" + strings.TrimPrefix(url, "http://") + "/ws"
}

// testLogf returns a logf suitable for hubs whose Run goroutines outlive
// the test: logging after the test completes would race with the testing
// framework's teardown.
func testLogf(t *testing.T) func(format string, args ...interface{}) {
	t.Helper()
	var mu sync.Mutex
	done := false
	t.Cleanup(func() {
		mu.Lock()
		done = true
		mu.Unlock()
	})
	return func(format string, args ...interface{}) {
		mu.Lock()
		defer mu.Unlock()
		if !done {
			t.Logf(format, args...)
		}
	}
}

// TestClientKeepaliveDetectsSilentPeer proves the client's read deadline
// fires when the peer stops responding at the application level without
// closing the socket: the failure must reach the onClose callback so the
// guest's Start loop can reconnect (issue #220 acceptance criterion 1).
func TestClientKeepaliveDetectsSilentPeer(t *testing.T) {
	srv := newSilentPeerServer(t)

	hub := NewClientHub(testLogf(t))
	client := NewClient("silent-peer-test", hub, testLogf(t))
	client.SetKeepAlive(50*time.Millisecond, 100*time.Millisecond)

	lost := make(chan struct{})
	var lostOnce sync.Once
	client.SetOnClose(func() { lostOnce.Do(func() { close(lost) }) })

	if err := client.Connect(wsAddr(srv.server.URL)); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer client.Close()

	// Detection must happen within a small multiple of the pong timeout,
	// not after TCP retransmission timeouts (or never).
	select {
	case <-lost:
	case <-time.After(3 * time.Second):
		t.Fatal("client did not detect the silently dead peer within the pong timeout")
	}
}

// TestClientKeepaliveKeepsIdleConnection proves pongs extend the read
// deadline: a healthy connection whose application layer is idle must not
// be torn down (issue #220 acceptance criterion 2).
func TestClientKeepaliveKeepsIdleConnection(t *testing.T) {
	hub := NewHub(testLogf(t))
	hub.RegisterMethod("test.ping", func(ctx context.Context, params json.RawMessage) (interface{}, *RPCError) {
		return map[string]bool{"ok": true}, nil
	})
	go hub.Run()

	upgrader := NewUpgrader()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.ServeHTTP(w, r)
		if err != nil {
			return
		}
		c := hub.NewConnection("idle-test-conn", conn, ConnectionRoleBrowser)
		go c.ReadLoop()
		go c.WriteLoop()
	}))
	defer srv.Close()

	clientHub := NewClientHub(testLogf(t))
	client := NewClient("idle-test", clientHub, testLogf(t))
	// Aggressive keepalive: the pong timeout is far shorter than the idle
	// period the test imposes, so only pongs can keep the connection up.
	client.SetKeepAlive(50*time.Millisecond, 100*time.Millisecond)

	lost := make(chan struct{})
	var lostOnce sync.Once
	client.SetOnClose(func() { lostOnce.Do(func() { close(lost) }) })

	if err := client.Connect(wsAddr(srv.URL)); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer client.Close()

	// Stay at the application level for 20 pong timeouts. Only WebSocket
	// control frames may flow during this window.
	time.Sleep(2 * time.Second)

	select {
	case <-lost:
		t.Fatal("healthy idle connection was torn down: pongs did not extend the read deadline")
	default:
	}

	// The connection must still carry application traffic.
	result, err := client.Call("test.ping", nil)
	if err != nil {
		t.Fatalf("call after idle period failed: %v", err)
	}
	var out struct {
		OK bool `json:"ok"`
	}
	if err := json.Unmarshal(result, &out); err != nil || !out.OK {
		t.Fatalf("unexpected call result: %s (err %v)", result, err)
	}
}

var idCounter atomic.Int64

func idCounterNext() int64 { return idCounter.Add(1) }

// hubWithSilentDropServer starts an httptest server whose /ws handler
// registers every upgraded connection with the given hub (already running)
// and runs its read/write loops. It returns the ws URL.
func hubWithSilentDropServer(t *testing.T, hub *Hub) string {
	t.Helper()
	upgrader := NewUpgrader()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.ServeHTTP(w, r)
		if err != nil {
			return
		}
		c := hub.NewConnection(fmt.Sprintf("srv-%d", idCounterNext()), conn, ConnectionRoleBrowser)
		go c.ReadLoop()
		go c.WriteLoop()
	}))
	t.Cleanup(srv.Close)
	return wsAddr(srv.URL)
}

// TestServerKeepaliveDropsSilentPeer proves the server side mirrors the
// client: a peer that answers nothing triggers the connection's read
// deadline, so the hub unregisters it and the disconnect callback fires.
func TestServerKeepaliveDropsSilentPeer(t *testing.T) {
	hub := NewHub(testLogf(t))
	hub.SetKeepAlive(50*time.Millisecond, 100*time.Millisecond)

	gone := make(chan string, 4)
	hub.SetOnDisconnect(func(id string) {
		select {
		case gone <- id:
		default:
		}
	})
	go hub.Run()

	url := hubWithSilentDropServer(t, hub)

	// A raw client that completes the handshake and then says nothing and
	// reads nothing (no read loop, so gorilla's automatic pong never runs).
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	select {
	case <-gone:
	case <-time.After(3 * time.Second):
		t.Fatal("server did not drop the silently dead peer within the pong timeout")
	}
}

// TestServerKeepaliveKeepsIdlePeerAlive proves the server-side pongs
// extend the deadline: a healthy client whose application layer is idle
// must survive far longer than the pong timeout.
func TestServerKeepaliveKeepsIdlePeerAlive(t *testing.T) {
	hub := NewHub(testLogf(t))
	hub.SetKeepAlive(50*time.Millisecond, 100*time.Millisecond)

	var disconnects atomic.Int64
	hub.SetOnDisconnect(func(string) { disconnects.Add(1) })
	go hub.Run()

	url := hubWithSilentDropServer(t, hub)

	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	// Draining reads let gorilla auto-answer the server's pings, which is
	// what a healthy browser or guest connection does.
	var readWG sync.WaitGroup
	readWG.Add(1)
	go func() {
		defer readWG.Done()
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}()

	// Idle at the application level for 20 pong timeouts.
	time.Sleep(2 * time.Second)

	if n := disconnects.Load(); n != 0 {
		t.Fatalf("healthy idle peer was disconnected %d time(s); pongs must extend the deadline", n)
	}
	conn.Close()
	readWG.Wait()
}

// TestKeepalivePingSurvivesTransientWriteError proves a ping that fails for
// a transient reason does not retire the ping goroutine for the rest of the
// connection's life. WriteControl's write-mutex timeout is the realistic
// transient: a data write holding the mutex past keepaliveWriteTimeout makes
// a ping fail on a perfectly healthy link. Retiring keepalive there would
// leave this endpoint's read deadline dependent on the peer's ping cadence —
// the very blind spot issue #220 removes.
func TestKeepalivePingSurvivesTransientWriteError(t *testing.T) {
	hub := NewHub(testLogf(t))
	hub.SetKeepAlive(50*time.Millisecond, 500*time.Millisecond)
	hub.RegisterMethod("test.ping", func(ctx context.Context, params json.RawMessage) (interface{}, *RPCError) {
		return map[string]bool{"ok": true}, nil
	})
	go hub.Run()
	url := hubWithSilentDropServer(t, hub)

	var attempts, failures atomic.Int64
	const injectedFailures = 3

	clientHub := NewClientHub(testLogf(t))
	client := NewClient("transient-ping-test", clientHub, testLogf(t))
	client.SetKeepAlive(50*time.Millisecond, 500*time.Millisecond)
	// Inject the failure into this client alone. A package-level hook would
	// also run for the server-side connection of this test — and for any
	// ping goroutine an earlier test left winding down, which is a data race
	// under -race.
	client.pingFn = func(conn *websocket.Conn, deadline time.Time) error {
		if attempts.Add(1) <= injectedFailures {
			failures.Add(1)
			// Stands in for gorilla's unexported write-timeout error.
			return fmt.Errorf("websocket: write timeout")
		}
		return conn.WriteControl(websocket.PingMessage, nil, deadline)
	}

	lost := make(chan struct{})
	var lostOnce sync.Once
	client.SetOnClose(func() { lostOnce.Do(func() { close(lost) }) })

	if err := client.Connect(url); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer client.Close()

	// Long enough for many pings, of which the first few fail.
	time.Sleep(time.Second)

	select {
	case <-lost:
		t.Fatal("connection was torn down after a transient ping failure")
	default:
	}

	if failures.Load() != injectedFailures {
		t.Fatalf("injected failures reached = %d, want %d (was the ping loop armed?)", failures.Load(), injectedFailures)
	}
	if attempts.Load() <= injectedFailures {
		t.Fatalf("ping goroutine stopped ticking after a transient failure (%d attempts)", attempts.Load())
	}

	// The link must still carry application traffic.
	result, err := client.Call("test.ping", nil)
	if err != nil {
		t.Fatalf("call after transient ping failures failed: %v", err)
	}
	var out struct {
		OK bool `json:"ok"`
	}
	if err := json.Unmarshal(result, &out); err != nil || !out.OK {
		t.Fatalf("unexpected call result: %s (err %v)", result, err)
	}
}

// TestKeepalivePingFailureLogIsBounded proves a ping that fails forever does
// not become one log line per tick. A peer that keeps the read side warm but
// never drains its own socket can make every ping time out on the write mutex
// for the life of the connection, and /ws upgrades anyone who can reach the
// port — so the retry must stay quiet (issue #220).
func TestKeepalivePingFailureLogIsBounded(t *testing.T) {
	hub := NewHub(testLogf(t))
	hub.SetKeepAlive(50*time.Millisecond, 100*time.Millisecond)
	go hub.Run()
	url := hubWithSilentDropServer(t, hub)

	var attempts atomic.Int64
	var pingLogs atomic.Int64
	count := func(format string, args ...interface{}) {
		if strings.Contains(fmt.Sprintf(format, args...), "keepalive ping") {
			pingLogs.Add(1)
		}
	}

	clientHub := NewClientHub(count)
	client := NewClient("ping-log-bound", clientHub, count)
	// The pong timeout stays far longer than the test, so the only thing
	// going wrong here is the ping write.
	client.SetKeepAlive(10*time.Millisecond, time.Second)
	client.pingFn = func(*websocket.Conn, time.Time) error {
		attempts.Add(1)
		return fmt.Errorf("websocket: write timeout")
	}

	if err := client.Connect(url); err != nil {
		t.Fatalf("connect: %v", err)
	}

	time.Sleep(500 * time.Millisecond)
	client.Close()

	got, tried := pingLogs.Load(), attempts.Load()
	if tried <= keepaliveFailureLogEvery {
		t.Fatalf("only %d pings in the window; the test cannot bound anything", tried)
	}
	if got == 0 {
		t.Fatal("a persistently failing ping logged nothing at all")
	}
	if limit := 1 + tried/keepaliveFailureLogEvery + 1; got > limit {
		t.Fatalf("ping failures logged %d lines for %d failures; want at most %d", got, tried, limit)
	}
}
