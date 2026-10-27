package rpc

import (
	"time"

	"github.com/gorilla/websocket"
)

// WebSocket keepalive (issue #220).
//
// Without keepalive a connection can die "silently" — the TCP path is gone
// (NAT or firewall state expiry, a reverse proxy dropping an idle upstream)
// but no FIN or RST ever arrives — and both endpoints keep believing the
// socket is open. readLoop blocks in ReadMessage forever, onClose never
// fires, and the guest never reconnects.
//
// The cure follows the gorilla/websocket keepalive recipe
// (https://pkg.go.dev/github.com/gorilla/websocket#hdr-Control_Messages):
//
//   - A dedicated goroutine per connection sends a PingMessage via
//     WriteControl, the only write method that is safe to call alongside
//     WriteMessage from the write loop (see the package concurrency notes:
//     https://pkg.go.dev/github.com/gorilla/websocket@v1.5.3#hdr-Concurrency).
//   - The read goroutine sets a read deadline before every blocking read;
//     every successful read and every received pong pushes the deadline out
//     by pingInterval + pongTimeout.
//   - A peer that stops answering pongs therefore fails ReadMessage within
//     the deadline, and the existing failure paths run: the client's
//     onClose callback (→ guest.Start reconnects) or the server
//     connection's Close (→ hub unregister).
//
// Concurrency discipline: SetReadDeadline belongs to the read goroutine
// only — the pong handler runs inside ReadMessage, i.e. on that goroutine,
// so extending the deadline from it is correct. The ping goroutine touches
// the connection exclusively through WriteControl.

const (
	// DefaultKeepalivePingInterval is how often a ping control frame is
	// sent when the connection configuration does not override it. It is
	// noticeably shorter than the default heartbeat_interval (30s) so the
	// transport detects a dead path before the application layer notices
	// anything.
	DefaultKeepalivePingInterval = 10 * time.Second

	// DefaultKeepalivePongTimeout is how long a peer may stay silent — no
	// pong and no data frame — before the connection is declared dead.
	// Together with the ping interval it forms the detection window,
	// which stays well inside the nginx default 60s proxy idle timeout.
	DefaultKeepalivePongTimeout = 30 * time.Second

	// keepaliveWriteTimeout bounds a single ping control-frame write.
	keepaliveWriteTimeout = 10 * time.Second
)

// effectiveKeepalive resolves zero/unset knobs to the package defaults.
func effectiveKeepalive(ping, pong time.Duration) (time.Duration, time.Duration) {
	if ping <= 0 {
		ping = DefaultKeepalivePingInterval
	}
	if pong <= 0 {
		pong = DefaultKeepalivePongTimeout
	}
	return ping, pong
}

// configureKeepalive arms conn for keepalive: the initial read deadline
// and the pong handler that keeps pushing it out. Call it on the goroutine
// that creates the connection, before any read loop starts. The default
// ping handler (automatic pong replies) is deliberately left in place: it
// only writes a control frame, so it complies with the rule that control
// handlers must not block NextReader.
func configureKeepalive(conn *websocket.Conn, deadline time.Duration) {
	conn.SetReadDeadline(time.Now().Add(deadline))
	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(deadline))
	})
}

// startKeepalivePing launches the per-connection ping goroutine. It exits
// when closeCh is closed or a ping write fails (a failed write means the
// connection is broken; the read deadline will surface the failure through
// the normal failure path shortly after).
func startKeepalivePing(conn *websocket.Conn, interval time.Duration, closeCh chan struct{}, logf func(format string, args ...interface{}), prefix string) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-closeCh:
				return
			case <-ticker.C:
				if err := conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(keepaliveWriteTimeout)); err != nil {
					logf("%s keepalive ping failed: %v", prefix, err)
					return
				}
			}
		}
	}()
}
