package rpc

import (
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// errSimulatedWrite stands in for a real socket error so tests can tell a
// simulated write failure apart from an incidental one.
var errSimulatedWrite = errors.New("simulated write failure")

// failingNetConn wraps a live connection and can be armed so that every
// subsequent Write fails while reads keep working. That is the half-open
// connection of issue #217: the read side stays quiet, only the write side
// knows the socket is gone.
type failingNetConn struct {
	net.Conn
	armed atomic.Bool
}

func (c *failingNetConn) Write(p []byte) (int, error) {
	if c.armed.Load() {
		return 0, errSimulatedWrite
	}
	return c.Conn.Write(p)
}

// newWriteFailClient returns a connected Client (registered with a running
// ClientHub, read loop running, onClose wired), a function that arms the
// write failure, and a counter of how often onClose fired.
func newWriteFailClient(t *testing.T) (*Client, func(), *atomic.Int32) {
	t.Helper()

	closes := &atomic.Int32{}
	client, arm := newWriteFailClientWithOnClose(t, func() { closes.Add(1) })
	return client, arm, closes
}

// newWriteFailClientWithOnClose is newWriteFailClient with a caller-supplied
// onClose, registered before the read loop starts (issue #218).
func newWriteFailClientWithOnClose(t *testing.T, onClose func()) (*Client, func()) {
	t.Helper()

	url, served := newTestWebsocketServer(t)

	fc := &failingNetConn{}
	dialer := &websocket.Dialer{NetDial: func(network, addr string) (net.Conn, error) {
		raw, err := net.Dial(network, addr)
		if err != nil {
			return nil, err
		}
		fc.Conn = raw
		return fc, nil
	}}

	conn, _, err := dialer.Dial(url, nil)
	if err != nil {
		t.Fatalf("dial test server: %v", err)
	}
	accepted := <-served
	if accepted == nil {
		t.Fatal("test server did not accept the connection")
	}
	t.Cleanup(func() { accepted.Close() })

	hub := NewClientHub(func(format string, args ...interface{}) {})
	go hub.Run()
	client := NewClient("write-fail-client", hub, func(format string, args ...interface{}) {})
	client.conn = conn
	client.SetOnClose(onClose)

	// Mirror what Connect does: register with the hub, then read.
	hub.register <- client
	go client.readLoop()
	t.Cleanup(func() { conn.Close() })

	return client, func() { fc.armed.Store(true) }
}

func newTestWebsocketServer(t *testing.T) (string, chan *websocket.Conn) {
	t.Helper()

	served := make(chan *websocket.Conn, 8)
	upgrader := NewUpgrader()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			served <- nil
			return
		}
		served <- conn
		// Drain so client writes never block on the socket.
		go func() {
			for {
				if _, _, err := conn.ReadMessage(); err != nil {
					return
				}
			}
		}()
	}))
	t.Cleanup(server.Close)

	return "ws://" + server.Listener.Addr().String() + "/rpc", served
}

func TestClientCallWriteFailureSignalsConnectionLoss(t *testing.T) {
	client, arm, closes := newWriteFailClient(t)
	arm()

	// A Call on a socket whose write side is dead …
	_, err := client.Call("guest.ping", map[string]string{"id": "x"})

	// … must surface the write error to the caller …
	if !errors.Is(err, errSimulatedWrite) {
		t.Fatalf("expected the write error to reach the caller, got %v", err)
	}
	// … and declare the connection lost, exactly once.
	if got := closes.Load(); got != 1 {
		t.Errorf("expected onClose to fire once after a failed write, got %d calls", got)
	}
	select {
	case <-client.closeCh:
	default:
		t.Error("expected closeCh to be closed after a failed write")
	}
}

func TestClientSendNotificationWriteFailureSignalsConnectionLoss(t *testing.T) {
	client, arm, closes := newWriteFailClient(t)
	arm()

	err := client.SendNotification("guest.log", map[string]string{"line": "hello"})

	if !errors.Is(err, errSimulatedWrite) {
		t.Fatalf("expected the write error to reach the caller, got %v", err)
	}
	if got := closes.Load(); got != 1 {
		t.Errorf("expected onClose to fire once after a failed notification write, got %d calls", got)
	}
	select {
	case <-client.closeCh:
	default:
		t.Error("expected closeCh to be closed after a failed write")
	}
}

func TestClientWriteFailureSignalsOnCloseOnlyOnce(t *testing.T) {
	client, arm, closes := newWriteFailClient(t)
	arm()

	// Two failing writes plus an explicit Close must still produce a single
	// loss notification: every loss path shares one guard.
	if _, err := client.Call("guest.ping", nil); err == nil {
		t.Error("expected Call to fail")
	}
	if err := client.SendNotification("guest.log", nil); err == nil {
		t.Error("expected SendNotification to fail")
	}
	client.Close()

	if got := closes.Load(); got != 1 {
		t.Errorf("expected exactly one onClose call, got %d", got)
	}
}

// TestClientWriteFailureWakesTheReconnectWaiter mirrors how Guest.Connect
// wires the client: onClose closes the channel Guest.Start waits on. A
// failed write must unblock that waiter, because a guest whose write failed
// has no other way to learn that the connection is gone (issue #217).
func TestClientWriteFailureWakesTheReconnectWaiter(t *testing.T) {
	connLost := make(chan struct{})
	var once sync.Once
	client, arm := newWriteFailClientWithOnClose(t, func() {
		once.Do(func() { close(connLost) })
	})
	arm()

	if err := client.SendNotification("guest.log", nil); err == nil {
		t.Fatal("expected the notification write to fail")
	}

	select {
	case <-connLost:
		// The reconnect loop would now dial again.
	case <-time.After(2 * time.Second):
		t.Error("a failed write did not wake the reconnect waiter")
	}
}

func TestClientGracefulCloseDoesNotSignalConnectionLoss(t *testing.T) {
	client, _, closes := newWriteFailClient(t)

	client.Close()

	if got := closes.Load(); got != 0 {
		t.Errorf("expected no onClose call for a graceful Close, got %d", got)
	}
}

// TestClientHubUnregisterDoesNotCloseClientQueue pins the cleanup of issue
// #217: the client hub closed the client's send queue on unregister, but no
// such queue exists any more. On main a second unregister of the same client
// panics with "close of closed channel"; with the dead path removed there is
// nothing left for the hub to close.
func TestClientHubUnregisterDoesNotCloseClientQueue(t *testing.T) {
	hub := NewClientHub(func(format string, args ...interface{}) {})
	go hub.Run()

	c := NewClient("unregister-client", hub, func(format string, args ...interface{}) {})
	for i := 0; i < 50; i++ {
		hub.register <- c
		hub.unregister <- c
		hub.register <- c
		hub.unregister <- c
	}

	deadline := time.Now().Add(2 * time.Second)
	for hub.ConnectionCount() != 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if hub.ConnectionCount() != 0 {
		t.Fatalf("expected the client to be unregistered, %d connected", hub.ConnectionCount())
	}
}

// TestConnectionWriteLoopWritesQueuedMessage keeps coverage of the
// server-side Connection.WriteLoop, which is live code and must not be
// caught in the client-side cleanup of issue #217.
func TestConnectionWriteLoopWritesQueuedMessage(t *testing.T) {
	hub := newTestHub(t)
	go hub.Run()

	url, served := newTestWebsocketServer(t)
	peer, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatalf("dial test server: %v", err)
	}
	defer peer.Close()

	accepted := <-served
	if accepted == nil {
		t.Fatal("test server did not accept the connection")
	}

	conn := &Connection{
		id:      "write-loop-conn",
		conn:    accepted,
		hub:     hub,
		send:    make(chan []byte, 256),
		closeCh: make(chan struct{}),
	}
	hub.Register(conn)

	go conn.WriteLoop()

	msg := &JSONRPCMessage{JSONRPC: "2.0", Method: "server.ping"}
	if err := conn.Send(msg); err != nil {
		t.Fatalf("queue message: %v", err)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, got, err := peer.ReadMessage()
		if err != nil {
			t.Errorf("read from WriteLoop: %v", err)
			return
		}
		var echoed JSONRPCMessage
		if err := json.Unmarshal(got, &echoed); err != nil {
			t.Errorf("unmarshal echoed message: %v", err)
			return
		}
		if echoed.Method != msg.Method {
			t.Errorf("expected %q, got %q", msg.Method, echoed.Method)
		}
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Connection.WriteLoop did not write the queued message")
	}

	conn.Close()
}

// TestConnectionWriteLoopClosesConnectionOnWriteError covers the failure
// branch of the same live loop: a write error must take the connection down
// rather than leave it half-alive.
func TestConnectionWriteLoopClosesConnectionOnWriteError(t *testing.T) {
	hub := newTestHub(t)
	go hub.Run()

	url, served := newTestWebsocketServer(t)
	peer, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatalf("dial test server: %v", err)
	}
	defer peer.Close()

	accepted := <-served
	if accepted == nil {
		t.Fatal("test server did not accept the connection")
	}

	conn := &Connection{
		id:      "write-loop-error-conn",
		conn:    accepted,
		hub:     hub,
		send:    make(chan []byte, 256),
		closeCh: make(chan struct{}),
	}
	hub.Register(conn)

	go conn.WriteLoop()

	// Close the socket from under the loop so the queued write fails.
	accepted.Close()
	if err := conn.Send(&JSONRPCMessage{JSONRPC: "2.0", Method: "server.ping"}); err != nil {
		t.Fatalf("queue message: %v", err)
	}

	select {
	case <-conn.closeCh:
	case <-time.After(2 * time.Second):
		t.Fatal("Connection.WriteLoop survived a write error without closing the connection")
	}
}
