package rpc

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// TestClientNotificationHandlerMayCall is the regression test for issue #214:
// a notification handler that performs a Call must not deadlock the client.
//
// readLoop used to dispatch notification handlers synchronously. A handler
// that called Client.Call then waited on the client's shared response
// channel (since removed) — a channel only readLoop drains — so both
// parked forever: the handler's Call never returned, no
// further message was ever read, and onClose (the reconnect signal) could
// never fire. This mirrors the guest's task.cancel handler answering with
// guest.cancelled.
func TestClientNotificationHandlerMayCall(t *testing.T) {
	upgrader := websocket.Upgrader{
		CheckOrigin: func(*http.Request) bool { return true },
	}

	// The server answers every request with a plain result, and exposes
	// its connection so the test can push notifications on demand.
	var srvMu sync.Mutex
	var srvConn *websocket.Conn
	connReady := make(chan struct{})
	readyOnce := sync.OnceFunc(func() { close(connReady) })

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade failed: %v", err)
			return
		}
		srvMu.Lock()
		srvConn = conn
		srvMu.Unlock()
		readyOnce()
		defer conn.Close()

		for {
			var msg JSONRPCMessage
			if err := conn.ReadJSON(&msg); err != nil {
				return
			}
			if msg.ID == nil {
				continue // notification from the client: no answer needed
			}
			resp := JSONRPCMessage{
				JSONRPC: "2.0",
				ID:      msg.ID,
				Result:  json.RawMessage(`{"ok":true}`),
			}
			srvMu.Lock()
			err := conn.WriteJSON(resp)
			srvMu.Unlock()
			if err != nil {
				return
			}
		}
	}))
	defer srv.Close()

	pushNotification := func(method, params string) {
		t.Helper()
		srvMu.Lock()
		defer srvMu.Unlock()
		if srvConn == nil {
			t.Fatalf("server connection not established")
		}
		raw := `{"jsonrpc":"2.0","method":"` + method + `","params":` + params + `}`
		if err := srvConn.WriteMessage(websocket.TextMessage, []byte(raw)); err != nil {
			t.Fatalf("push %s: %v", method, err)
		}
	}

	hub := NewClientHub(t.Logf)
	client := NewClient("deadlock-guest", hub, t.Logf)
	defer client.Close()

	connLost := make(chan struct{})
	client.SetOnClose(func() { close(connLost) })

	// handlerDone closes once the handler's own Call has returned. Before
	// the fix this never happens: the handler blocks readLoop, so the
	// response to guest.cancelled is never read.
	handlerDone := make(chan struct{})
	hub.RegisterNotificationHandler("task.cancel", func(method string, params json.RawMessage) {
		_, err := client.Call("guest.cancelled", map[string]any{
			"task_id": "task-1",
		})
		if err != nil {
			t.Errorf("Call from notification handler failed: %v", err)
		}
		close(handlerDone)
	})

	afterDone := make(chan struct{})
	hub.RegisterNotificationHandler("notify.after", func(method string, params json.RawMessage) {
		close(afterDone)
	})

	wsURL := "ws://" + strings.TrimPrefix(srv.URL, "http://")
	if err := client.Connect(wsURL); err != nil {
		t.Fatalf("connect: %v", err)
	}
	select {
	case <-connReady:
	case <-time.After(2 * time.Second):
		t.Fatal("server never saw a connection")
	}

	// Control: a Call from a goroutine that is not readLoop works even
	// before the fix. This proves the response plumbing itself is sound.
	if _, err := client.Call("control.ping", nil); err != nil {
		t.Fatalf("control call failed: %v", err)
	}

	// The deadlock trigger: a notification whose handler performs a Call.
	pushNotification("task.cancel", `{"task_id":"task-1","reason":"user requested cancellation"}`)
	select {
	case <-handlerDone:
	case <-time.After(3 * time.Second):
		t.Fatal("notification handler's Call never returned: readLoop is blocked by the handler (issue #214)")
	}

	// The client must still be reading: a later notification is dispatched.
	pushNotification("notify.after", `{}`)
	select {
	case <-afterDone:
	case <-time.After(3 * time.Second):
		t.Fatal("client stopped reading messages after the handler's Call")
	}

	// And the connection-lost signal must still be able to fire: readLoop
	// has to be back in ReadMessage for the socket close to surface.
	srvMu.Lock()
	srvConn.Close()
	srvMu.Unlock()
	select {
	case <-connLost:
	case <-time.After(3 * time.Second):
		t.Fatal("onClose never fired after the connection was lost")
	}
}
