package rpc

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// startMethodReplyWSServer starts a WebSocket server that answers every
// request with a result echoing the request's method. Each request is
// answered on its own goroutine so responses can arrive out of order.
// Replies to holdMethod are held until a token arrives on release — an
// empty holdMethod, which every test but the routing and stale ones
// uses, answers everything as it arrives. written, when non-nil, is
// closed once the held reply has been written, so a test can wait for
// that moment instead of sleeping.
func startMethodReplyWSServer(t *testing.T, holdMethod string, release chan struct{}, written chan struct{}) string {
	t.Helper()

	upgrader := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		// gorilla/websocket allows only one writer per connection at a
		// time (see the doc comment on Conn), so the held replies are
		// serialised against the immediate ones.
		var writeMu sync.Mutex
		write := func(data []byte) error {
			writeMu.Lock()
			defer writeMu.Unlock()
			return conn.WriteMessage(websocket.TextMessage, data)
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
				result, err := json.Marshal(map[string]string{"echo": req.Method})
				if err != nil {
					continue
				}
				data, err := json.Marshal(JSONRPCMessage{JSONRPC: "2.0", ID: req.ID, Result: result})
				if err != nil {
					continue
				}
				if holdMethod != "" && req.Method == holdMethod {
					go func() {
						// Blocking here also proves the request
						// reached the server.
						<-release
						if write(data) != nil {
							return
						}
						if written != nil {
							close(written)
						}
					}()
					continue
				}
				if err := write(data); err != nil {
					return
				}
			}
		}()
	}))
	t.Cleanup(srv.Close)

	return "ws" + strings.TrimPrefix(srv.URL, "http")
}

// TestClientCallRoutesResponsesToTheirOwnCallers proves two concurrent
// Calls for different methods each receive their own result (issue #216).
func TestClientCallRoutesResponsesToTheirOwnCallers(t *testing.T) {
	release := make(chan struct{})
	url := startMethodReplyWSServer(t, "call.slow.method", release, nil)
	c := connectTestClient(t, url)
	c.SetCallTimeout(5 * time.Second)

	// Repeat enough that a response handed to the wrong Call could not
	// plausibly hide from the assertions.
	const rounds = 20
	for i := 0; i < rounds; i++ {
		type outcome struct {
			result json.RawMessage
			err    error
		}
		slowCh := make(chan outcome, 1)
		go func() {
			r, err := c.Call("call.slow.method", nil)
			slowCh <- outcome{r, err}
		}()
		// Give the slow caller time to enter its wait so the fast reply
		// lands while both callers are parked. Nothing is asserted against
		// that delay: the slow reply is only let go below, once the fast
		// Call has returned.
		time.Sleep(20 * time.Millisecond)

		fastResult, err := c.Call("call.fast.method", nil)
		if err != nil {
			t.Fatalf("round %d: fast call: %v", i, err)
		}
		if !strings.Contains(string(fastResult), "call.fast.method") {
			t.Fatalf("round %d: fast call received %s — response misdelivered", i, fastResult)
		}

		// The held reply now lands while only the slow Call awaits it.
		release <- struct{}{}

		select {
		case o := <-slowCh:
			if o.err != nil {
				t.Fatalf("round %d: slow call: %v", i, o.err)
			}
			if !strings.Contains(string(o.result), "call.slow.method") {
				t.Fatalf("round %d: slow call received %s — response misdelivered", i, o.result)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("round %d: slow call never returned", i)
		}
	}
}

// TestClientIgnoresResponseForCompletedRequest proves that a response
// arriving after its caller has timed out is not handed to the next
// caller: the late reply is ignored and a later call still receives its
// own result (issue #216).
func TestClientIgnoresResponseForCompletedRequest(t *testing.T) {
	release := make(chan struct{})
	written := make(chan struct{})
	url := startMethodReplyWSServer(t, "stale.first", release, written)
	c := connectTestClient(t, url)

	// The reply is held until released below, so it is the caller's own
	// timeout that ends the call rather than a race against a fixed delay.
	c.SetCallTimeout(100 * time.Millisecond)
	if _, err := c.Call("stale.first", nil); err == nil {
		t.Fatal("expected the first call to time out")
	}

	// Wait until the late reply has actually been written, so the next
	// Call provably runs against a table whose late entry has already been
	// discarded. The ceiling is a hang detector, not a timing bet.
	release <- struct{}{}
	select {
	case <-written:
	case <-time.After(5 * time.Second):
		t.Fatal("the late reply was never written")
	}

	c.SetCallTimeout(3 * time.Second)
	result, err := c.Call("stale.second", nil)
	if err != nil {
		t.Fatalf("second call: %v", err)
	}
	if !strings.Contains(string(result), "stale.second") {
		t.Errorf("second call received %s — a stale response disturbed it", result)
	}
}

// startStrayReplyWSServer answers "stray.watcher" with an unsolicited
// response carrying an unrelated ID first, then answers the request
// itself. The stray response matches no pending request.
func startStrayReplyWSServer(t *testing.T) string {
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
				if req.ID == nil || req.Method != "stray.watcher" {
					continue
				}
				strayID := json.RawMessage(`999999999`)
				strayResult, _ := json.Marshal(map[string]string{"echo": "nobody.waited.for.me"})
				stray, _ := json.Marshal(JSONRPCMessage{JSONRPC: "2.0", ID: &strayID, Result: strayResult})
				if err := conn.WriteMessage(websocket.TextMessage, stray); err != nil {
					return
				}
				result, _ := json.Marshal(map[string]string{"echo": "stray.watcher"})
				data, _ := json.Marshal(JSONRPCMessage{JSONRPC: "2.0", ID: req.ID, Result: result})
				if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
					return
				}
			}
		}()
	}))
	t.Cleanup(srv.Close)

	return "ws" + strings.TrimPrefix(srv.URL, "http")
}

// TestClientIgnoresResponseForUnknownID proves a response whose ID
// matches no pending request is ignored rather than delivered, leaving
// the real response to reach its own caller (issue #216).
func TestClientIgnoresResponseForUnknownID(t *testing.T) {
	url := startStrayReplyWSServer(t)
	c := connectTestClient(t, url)
	c.SetCallTimeout(3 * time.Second)

	result, err := c.Call("stray.watcher", nil)
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	if !strings.Contains(string(result), "stray.watcher") {
		t.Errorf("call received %s — an unknown-ID response disturbed it", result)
	}
}

// captureLogf returns a logf that records every line it is handed, plus
// a reader for a snapshot of the recorded lines. Unlike testLogf, which
// guards calls from hubs and read loops that outlive their test, this
// recorder has no concurrency machinery: its only user below drives
// deliverResponse directly, so every call is on the test goroutine.
func captureLogf(t *testing.T) (func(string, ...interface{}), func() []string) {
	t.Helper()
	var logged []string
	logf := func(format string, args ...interface{}) {
		logged = append(logged, fmt.Sprintf(format, args...))
	}
	lines := func() []string { return append([]string(nil), logged...) }
	return logf, lines
}

// TestClientIgnoresSecondResponseForLiveRequest proves a second
// response for an ID whose Call has not yet woken is logged and
// dropped, not queued behind the first, whose receipt the Call keeps
// intact (issue #216). The branch is entered in production by a race
// between the Call waking and readLoop processing the second reply, so
// no server-side timing reaches it deterministically; the test calls
// deliverResponse directly against a pending entry whose channel
// already holds the first response — exactly the state an unwoken Call
// is in. deliverResponse reads only the pending table and logs through
// the stored logf, so no connection is involved.
func TestClientIgnoresSecondResponseForLiveRequest(t *testing.T) {
	logf, lines := captureLogf(t)
	hub := NewClientHub(testLogf(t))
	c := NewClient("dup-reply-test", hub, logf)

	id := json.RawMessage(`4242`)
	key := string(id)
	ch := make(chan *JSONRPCMessage, 1)
	first := &JSONRPCMessage{JSONRPC: "2.0", ID: &id}
	ch <- first // the Call has not woken: its channel still holds the reply
	c.pendingMu.Lock()
	c.pending[key] = ch
	c.pendingMu.Unlock()

	c.deliverResponse(&JSONRPCMessage{JSONRPC: "2.0", ID: &id})

	if len(ch) != 1 {
		t.Errorf("duplicate response was queued: channel holds %d, want 1", len(ch))
	}
	select {
	case got := <-ch:
		if got != first {
			t.Error("the pending channel holds the duplicate, not the Call's own first response")
		}
	default:
		t.Fatal("the pending channel was drained")
	}
	want := `second response for request id "4242" ignored`
	for _, line := range lines() {
		if strings.Contains(line, want) {
			return
		}
	}
	t.Errorf("the dropped duplicate left no log line; logged: %q", lines())
}

// pendingCount reports how many requests await their response.
func (c *Client) pendingCount() int {
	c.pendingMu.Lock()
	defer c.pendingMu.Unlock()
	return len(c.pending)
}

// waitForPending blocks until n Calls await their response, so a test can
// act on a Call being parked rather than guess when it will be.
func waitForPending(t *testing.T, c *Client, n int) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); c.pendingCount() != n; {
		if time.Now().After(deadline) {
			t.Fatalf("pending calls: got %d, want %d", c.pendingCount(), n)
		}
		time.Sleep(time.Millisecond)
	}
}

// TestClientCallLeavesNoPendingEntries proves every exit path — success,
// timeout and connection close — removes the pending entry, so the table
// does not grow without bound (issue #216).
func TestClientCallLeavesNoPendingEntries(t *testing.T) {
	c := connectTestClient(t, startMethodReplyWSServer(t, "", nil, nil))
	c.SetCallTimeout(2 * time.Second)

	for i := 0; i < 5; i++ {
		if _, err := c.Call("pending.ok", nil); err != nil {
			t.Fatalf("successful call %d: %v", i, err)
		}
	}
	if n := c.pendingCount(); n != 0 {
		t.Errorf("after successful calls: %d pending entries remain", n)
	}

	// Timed-out calls must deregister too. The deferred deregistration
	// runs before Call returns, so the table is empty by the time the
	// error is in hand.
	silentURL := startSilentWSServer(t)
	c2 := connectTestClient(t, silentURL)
	c2.SetCallTimeout(50 * time.Millisecond)
	for i := 0; i < 3; i++ {
		if _, err := c2.Call("pending.never", nil); err == nil {
			t.Fatal("expected timeout error")
		}
	}
	if n := c2.pendingCount(); n != 0 {
		t.Errorf("after timed-out calls: %d pending entries remain", n)
	}

	// A call interrupted by Close must not leak either.
	c3 := connectTestClient(t, silentURL)
	c3.SetCallTimeout(30 * time.Second)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, _ = c3.Call("pending.closed", nil)
	}()
	waitForPending(t, c3, 1)
	c3.Close()
	wg.Wait()
	if n := c3.pendingCount(); n != 0 {
		t.Errorf("after a close-interrupted call: %d pending entries remain", n)
	}
}
