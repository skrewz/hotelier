package rpc

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// startSilentWSServer starts a WebSocket server that accepts connections and
// reads messages but never sends any response. It simulates a host that
// accepts the connection and then goes quiet (issue #215).
func startSilentWSServer(t *testing.T) string {
	t.Helper()

	upgrader := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		go func() {
			defer conn.Close()
			for {
				if _, _, err := conn.ReadMessage(); err != nil {
					return
				}
			}
		}()
	}))
	t.Cleanup(srv.Close)

	return "ws" + strings.TrimPrefix(srv.URL, "http")
}

// startDelayedReplyWSServer starts a WebSocket server that answers each
// request with a successful result after the given delay.
func startDelayedReplyWSServer(t *testing.T, delay time.Duration) string {
	t.Helper()

	upgrader := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		go func() {
			defer conn.Close()
			for {
				_, raw, err := conn.ReadMessage()
				if err != nil {
					return
				}
				var req JSONRPCMessage
				if err := json.Unmarshal(raw, &req); err != nil {
					continue
				}
				if req.ID == nil {
					continue
				}
				resp := JSONRPCMessage{
					JSONRPC: "2.0",
					ID:      req.ID,
					Result:  json.RawMessage(`{"ok":true}`),
				}
				data, err := json.Marshal(resp)
				if err != nil {
					continue
				}
				time.Sleep(delay)
				if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
					return
				}
			}
		}()
	}))
	t.Cleanup(srv.Close)

	return "ws" + strings.TrimPrefix(srv.URL, "http")
}

func connectTestClient(t *testing.T, url string) *Client {
	t.Helper()

	hub := NewClientHub(t.Logf)
	c := NewClient("test-client", hub, t.Logf)
	if err := c.Connect(url); err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(c.Close)
	return c
}

// TestClientCallTimesOutWhenResponseNeverArrives proves that a Call whose
// response never arrives returns an error within the timeout instead of
// blocking forever (issue #215).
func TestClientCallTimesOutWhenResponseNeverArrives(t *testing.T) {
	url := startSilentWSServer(t)
	c := connectTestClient(t, url)

	const timeout = 100 * time.Millisecond
	c.SetCallTimeout(timeout)

	start := time.Now()
	_, err := c.Call("never.answered", nil)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}
	if want := "timeout waiting for never.answered response"; !strings.Contains(err.Error(), want) {
		t.Errorf("expected error to contain %q, got %v", want, err)
	}
	// Must return promptly after the timeout, not hang.
	if elapsed < timeout || elapsed > timeout+2*time.Second {
		t.Errorf("expected return around %v, took %v", timeout, elapsed)
	}
}

// TestClientCallReturnsErrorWhenClosedWhileWaiting proves that a Call in
// flight while the client is closed returns an error rather than blocking
// (issue #215).
func TestClientCallReturnsErrorWhenClosedWhileWaiting(t *testing.T) {
	url := startSilentWSServer(t)
	c := connectTestClient(t, url)

	// Long timeout so it is the close — not the timer — that ends the wait.
	c.SetCallTimeout(30 * time.Second)

	errCh := make(chan error, 1)
	go func() {
		_, err := c.Call("never.answered", nil)
		errCh <- err
	}()

	// Give Call time to write the request and enter the wait.
	time.Sleep(100 * time.Millisecond)
	c.Close()

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("expected error after close, got nil")
		}
		if want := "connection closed waiting for never.answered response"; !strings.Contains(err.Error(), want) {
			t.Errorf("expected error to contain %q, got %v", want, err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Call did not return after the client was closed")
	}
}

// TestClientCallSucceedsJustUnderTimeout proves the timeout is not so short
// that a healthy call whose response arrives just under it fails spuriously
// (issue #215).
func TestClientCallSucceedsJustUnderTimeout(t *testing.T) {
	url := startDelayedReplyWSServer(t, 80*time.Millisecond)
	c := connectTestClient(t, url)

	c.SetCallTimeout(1500 * time.Millisecond)

	result, err := c.Call("slow.but.healthy", nil)
	if err != nil {
		t.Fatalf("expected success, got error: %v", err)
	}
	if len(result) == 0 {
		t.Error("expected non-empty result")
	}
}

// TestNewClientUsesDefaultCallTimeout proves a fresh client starts with the
// package default timeout rather than zero (issue #215).
func TestNewClientUsesDefaultCallTimeout(t *testing.T) {
	c := NewClient("defaults", NewClientHub(t.Logf), t.Logf)

	if got := time.Duration(c.callTimeout.Load()); got != DefaultCallTimeout {
		t.Errorf("expected default call timeout %v, got %v", DefaultCallTimeout, got)
	}
	// The default heartbeat interval is 30s (pkg/config DefaultServerConfig).
	// The call timeout must be shorter, so a wedge surfaces as a heartbeat
	// failure rather than as silence (issue #215).
	const heartbeatInterval = 30 * time.Second
	if DefaultCallTimeout >= heartbeatInterval {
		t.Errorf("DefaultCallTimeout (%v) must be shorter than the heartbeat interval (%v)",
			DefaultCallTimeout, heartbeatInterval)
	}
}

// TestClientSetCallTimeoutResetsOnNonPositive proves SetCallTimeout with a
// non-positive duration restores the package default rather than disabling
// the bound (issue #215).
func TestClientSetCallTimeoutResetsOnNonPositive(t *testing.T) {
	c := NewClient("reset", NewClientHub(t.Logf), t.Logf)
	c.SetCallTimeout(50 * time.Millisecond)
	if got := time.Duration(c.callTimeout.Load()); got != 50*time.Millisecond {
		t.Fatalf("expected 50ms, got %v", got)
	}

	c.SetCallTimeout(0)
	if got := time.Duration(c.callTimeout.Load()); got != DefaultCallTimeout {
		t.Errorf("expected reset to %v, got %v", DefaultCallTimeout, got)
	}

	c.SetCallTimeout(-time.Second)
	if got := time.Duration(c.callTimeout.Load()); got != DefaultCallTimeout {
		t.Errorf("expected reset to %v for negative duration, got %v", DefaultCallTimeout, got)
	}
}
