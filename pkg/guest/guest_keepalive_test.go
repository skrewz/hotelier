package guest

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"hotelier/pkg/config"
	"hotelier/pkg/rpc"

	"github.com/gorilla/websocket"
)

// TestGuestReconnectsAfterSilentLinkLoss proves that when the peer stops
// responding at the application level without closing the socket, the
// guest's WebSocket keepalive notices within the pong timeout, the
// connection-lost signal fires, and Start() re-establishes the connection
// (issue #220 acceptance criterion 1, end to end).
func TestGuestReconnectsAfterSilentLinkLoss(t *testing.T) {
	var (
		mu            sync.Mutex
		registrations int
		held          []*websocket.Conn
		release       = make(chan struct{})
	)

	upgrader := rpc.NewUpgrader()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.ServeHTTP(w, r)
		if err != nil {
			return
		}

		mu.Lock()
		first := registrations == 0
		registrations++
		mu.Unlock()

		// Answer the guest.register request so the connection is fully
		// established on every attempt.
		_, msg, err := conn.ReadMessage()
		if err != nil {
			return
		}
		var req rpc.JSONRPCMessage
		if err := json.Unmarshal(msg, &req); err != nil || req.ID == nil {
			return
		}
		resp := rpc.JSONRPCMessage{JSONRPC: "2.0", ID: req.ID}
		if err := conn.WriteMessage(websocket.TextMessage, mustJSON(resp)); err != nil {
			return
		}

		if first {
			// Never touch this connection again: no reads (so no pongs),
			// no close. The link is "dead" while the socket stays open.
			mu.Lock()
			held = append(held, conn)
			mu.Unlock()
			<-release
			return
		}

		// Reconnect attempt: normal connection, drained until it ends.
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer srv.Close()
	defer close(release)

	cfg := config.GuestConfig{
		Name:                  "keepalive-test",
		Tags:                  []string{"test"},
		KeepalivePingInterval: 1,
		KeepalivePongTimeout:  1,
	}
	cfg.URL = "ws://" + srv.Listener.Addr().String() + "/ws"

	handler := func(_ context.Context, task TaskAssignment, _ LogCallback) (*TaskResult, error) {
		return &TaskResult{TaskID: task.TaskID, Success: true}, nil
	}

	g := New(cfg, handler)
	done := make(chan error, 1)
	go func() { done <- g.Start() }()

	// The first connection goes silent; detection takes at most
	// ping + pong = 2s and Start reconnects immediately afterwards.
	deadline := time.After(15 * time.Second)
	for {
		mu.Lock()
		n := registrations
		mu.Unlock()
		if n >= 2 {
			break
		}
		select {
		case <-deadline:
			g.Stop()
			t.Fatalf("guest never reconnected after silent link loss (registrations: %d)", n)
		case <-time.After(50 * time.Millisecond):
		}
	}

	g.Stop()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Start returned an error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Start did not return after Stop")
	}
}

func mustJSON(v interface{}) []byte {
	data, _ := json.Marshal(v)
	return data
}
