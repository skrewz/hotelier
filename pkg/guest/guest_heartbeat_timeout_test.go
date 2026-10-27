package guest

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	"hotelier/pkg/config"

	"github.com/gorilla/websocket"
)

// TestGuestHeartbeatFailsWhenServerNeverAnswers covers issue #215 end to
// end on the guest side: against a server that accepts the connection and
// then never answers, Heartbeat must return an error (via its 3-attempt
// backoff) rather than hanging forever. A hang would leave the guest
// looking alive to systemd while being functionally dead.
func TestGuestHeartbeatFailsWhenServerNeverAnswers(t *testing.T) {
	// A WebSocket server that upgrades and then discards every message —
	// it never sends a response to guest.heartbeat.
	upgrader := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
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
	})

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = http.Serve(ln, mux) }()
	t.Cleanup(func() { _ = ln.Close() })

	port := ln.Addr().(*net.TCPAddr).Port
	wsURL := fmt.Sprintf("ws://127.0.0.1:%d/ws", port)

	gcfg := config.GuestConfig{
		Name:              "Silent Host Guest",
		Tags:              []string{"test"},
		URL:               wsURL,
		HeartbeatInterval: 1,
	}
	g := New(gcfg, func(ctx context.Context, task TaskAssignment, _ LogCallback) (*TaskResult, error) {
		return &TaskResult{Success: true}, nil
	})

	if err := g.Connect(); err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() {
		if c := g.rpcClient(); c != nil {
			c.Close()
		}
	})

	// Shorten the per-call bound so the test does not wait 3x
	// DefaultCallTimeout; the production default is exercised elsewhere.
	g.rpcClient().SetCallTimeout(100 * time.Millisecond)

	start := time.Now()
	err = g.Heartbeat()
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected Heartbeat to fail against a silent server, got nil")
	}
	// Three timed-out attempts (3x100ms) plus the fixed 1s/2s/4s backoff
	// is roughly 7.3s. Anything approaching a hang is far beyond this.
	if elapsed > 30*time.Second {
		t.Errorf("Heartbeat took %v; expected it to give up promptly", elapsed)
	}
	t.Logf("Heartbeat failed as expected after %v: %v", elapsed.Round(time.Millisecond), err)
}
