package rpc

import (
	"errors"
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
//     by the keepalive window (see setKeepaliveDeadline).
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
	// sent when the connection configuration does not override it — the
	// config pair keepalive_ping_interval / keepalive_pong_timeout states
	// these two in seconds (pkg/config). It is noticeably shorter than the
	// default heartbeat_interval (30s) so the transport detects a dead path
	// before the application layer notices anything.
	DefaultKeepalivePingInterval = 10 * time.Second

	// DefaultKeepalivePongTimeout is how long a peer may stay silent — no
	// pong and no data frame — before the connection is declared dead.
	// Together with the ping interval it forms the detection window,
	// which stays well inside the nginx default 60s proxy idle timeout.
	DefaultKeepalivePongTimeout = 30 * time.Second

	// keepaliveWriteTimeout bounds a single ping control-frame write.
	keepaliveWriteTimeout = 10 * time.Second

	// keepaliveFailureLogEvery bounds the ping-failure log. A ping that
	// keeps failing says nothing about the peer (see startKeepalivePing),
	// so the goroutine keeps ticking — but it must not turn into free log
	// volume for a peer that never reads its socket either. The first
	// failure is logged, then every keepaliveFailureLogEvery'th consecutive
	// one, i.e. roughly once a hundred seconds at the default cadence.
	keepaliveFailureLogEvery = 10
)

// pingFunc writes one ping control frame. Production always passes
// (*websocket.Conn).WriteControl, the only write method that is safe to use
// alongside the write loop; a test may pass its own to inject a write
// failure into the one connection it is testing. It must be per connection,
// never a package global: ping goroutines from other connections still alive
// in the process would run the injected function too (issue #220).
type pingFunc func(conn *websocket.Conn, deadline time.Time) error

// writePing is the production pingFunc.
func writePing(conn *websocket.Conn, deadline time.Time) error {
	return conn.WriteControl(websocket.PingMessage, nil, deadline)
}

// effectiveKeepalive resolves zero/unset knobs to the package defaults. It
// is the single resolution point: call it once per connection, where the
// connection is created, and store the result.
func effectiveKeepalive(ping, pong time.Duration) (time.Duration, time.Duration) {
	if ping <= 0 {
		ping = DefaultKeepalivePingInterval
	}
	if pong <= 0 {
		pong = DefaultKeepalivePongTimeout
	}
	return ping, pong
}

// setKeepaliveDeadline pushes the read deadline out by the keepalive window:
// a peer that sends neither data nor a pong within ping + pong fails the next
// read, which runs the connection's existing failure path.
//
// This is the only home of the window and the only SetReadDeadline call in
// the package: both read loops and the pong handler push the deadline through
// it. It must be called from the goroutine performing the read, because
// SetReadDeadline is a read method under the gorilla concurrency rules and
// the pong handler runs inside ReadMessage on that same goroutine. A
// non-positive pair — a connection that was never armed — falls back to the
// package defaults so liveness is never silently off.
func setKeepaliveDeadline(conn *websocket.Conn, ping, pong time.Duration) error {
	ping, pong = effectiveKeepalive(ping, pong)
	return conn.SetReadDeadline(time.Now().Add(ping + pong))
}

// configureKeepalive arms conn for keepalive: the initial read deadline
// and the pong handler that keeps pushing it out. Call it on the goroutine
// that creates the connection, before any read loop starts. The default
// ping handler (automatic pong replies) is deliberately left in place: it
// only writes a control frame, so it complies with the rule that control
// handlers must not block NextReader.
//
// The error is the initial deadline failing, which on a live connection
// means it is already closed — a connection keepalive cannot work on, and
// therefore a result the caller must not discard.
func configureKeepalive(conn *websocket.Conn, ping, pong time.Duration) error {
	conn.SetPongHandler(func(string) error {
		return setKeepaliveDeadline(conn, ping, pong)
	})
	return setKeepaliveDeadline(conn, ping, pong)
}

// startKeepalivePing launches the per-connection ping goroutine. It exits
// when closeCh is closed, or when a ping reports the connection is being
// torn down. A failed ping on its own does *not* end the goroutine: see the
// comment at the write site for why a transient failure must be survived.
// sendPing is the control-frame write; nil selects the production one.
func startKeepalivePing(conn *websocket.Conn, interval time.Duration, closeCh chan struct{}, logf func(format string, args ...interface{}), prefix string, sendPing pingFunc) {
	if sendPing == nil {
		sendPing = writePing
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		failures := 0
		for {
			select {
			case <-closeCh:
				return
			case <-ticker.C:
				err := sendPing(conn, time.Now().Add(keepaliveWriteTimeout))
				if err == nil {
					failures = 0
					continue
				}
				failures++
				// A failed ping is not by itself evidence of a dead
				// connection. WriteControl waits for the connection's
				// write mutex and returns a timeout when another writer
				// holds it past keepaliveWriteTimeout (see
				// https://pkg.go.dev/github.com/gorilla/websocket@v1.5.3#hdr-Control_Messages),
				// so one large frame into a saturated TCP send buffer
				// would otherwise retire keepalive for the lifetime of a
				// healthy link — leaving liveness to the peer's own ping
				// cadence, which is the blind spot issue #220 exists to
				// close. Keep ticking: a genuinely broken connection fails
				// its read deadline shortly after, and that closes closeCh
				// and ends this goroutine.
				if errors.Is(err, websocket.ErrCloseSent) {
					logf("%s keepalive ping stopping: connection closing: %v", prefix, err)
					return
				}
				// Log the first failure and every
				// keepaliveFailureLogEvery'th one after it: an unbounded
				// one-line-per-tick log is volume an unauthenticated peer
				// can impose on us, and a peer that is truly gone fails
				// its read deadline and ends this goroutine anyway.
				switch {
				case failures == 1:
					logf("%s keepalive ping failed, will retry: %v", prefix, err)
				case failures%keepaliveFailureLogEvery == 0:
					logf("%s keepalive ping still failing after %d consecutive attempts: %v", prefix, failures, err)
				}
			}
		}
	}()
}
