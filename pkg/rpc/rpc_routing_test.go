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

// startMethodReplyWSServer starts a WebSocket server that answers every
// request with a result echoing the request's method. Calls to
// "call.slow.method" are answered after a delay so a concurrent fast
// call can overtake it; every other method is answered immediately.
// Each request is answered on its own goroutine so responses can arrive
// out of order — the case the shared channel cannot survive.
func startMethodReplyWSServer(t *testing.T, slowDelay time.Duration) string {
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
		// time (see the doc comment on Conn), so the delayed replies are
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
				resp := JSONRPCMessage{JSONRPC: "2.0", ID: req.ID, Result: result}
				data, err := json.Marshal(resp)
				if err != nil {
					continue
				}
				if req.Method == "call.slow.method" {
					go func() {
						time.Sleep(slowDelay)
						write(data)
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
// Calls for different methods each receive their own result. With the
// old shared callResp channel either caller could consume the other's
// response (issue #216).
func TestClientCallRoutesResponsesToTheirOwnCallers(t *testing.T) {
	url := startMethodReplyWSServer(t, 150*time.Millisecond)
	c := connectTestClient(t, url)
	c.SetCallTimeout(5 * time.Second)

	// Repeat enough that the old misdelivery (a response handed to an
	// arbitrary waiter) cannot plausibly hide from the assertion.
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
		// Let the slow caller enter its wait so the fast response can
		// arrive while both callers are parked.
		time.Sleep(20 * time.Millisecond)

		fastResult, err := c.Call("call.fast.method", nil)
		if err != nil {
			t.Fatalf("round %d: fast call: %v", i, err)
		}
		if !strings.Contains(string(fastResult), "call.fast.method") {
			t.Fatalf("round %d: fast call received %s — response misdelivered", i, fastResult)
		}

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

// startLateFirstReplyWSServer answers "stale.first" only after delayFirst
// (so the caller times out before it lands) and answers "stale.second"
// immediately.
func startLateFirstReplyWSServer(t *testing.T, delayFirst time.Duration) string {
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
				result, err := json.Marshal(map[string]string{"echo": req.Method})
				if err != nil {
					continue
				}
				resp := JSONRPCMessage{JSONRPC: "2.0", ID: req.ID, Result: result}
				if req.Method == "stale.first" {
					time.Sleep(delayFirst)
				}
				data, err := json.Marshal(resp)
				if err != nil {
					continue
				}
				if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
					return
				}
			}
		}()
	}))
	t.Cleanup(srv.Close)

	return "ws" + strings.TrimPrefix(srv.URL, "http")
}

// TestClientIgnoresResponseForCompletedRequest proves that a response
// arriving after its caller has timed out is not handed to the next
// caller: the late reply is ignored, and a later call still receives its
// own result. With the shared channel the late reply squats in the
// buffer and is misdelivered to — or wedges — the next Call (issue #216).
func TestClientIgnoresResponseForCompletedRequest(t *testing.T) {
	url := startLateFirstReplyWSServer(t, 300*time.Millisecond)
	c := connectTestClient(t, url)

	c.SetCallTimeout(100 * time.Millisecond)
	if _, err := c.Call("stale.first", nil); err == nil {
		t.Fatal("expected the first call to time out")
	}
	// Let the late reply land after the caller has given up.
	time.Sleep(300 * time.Millisecond)

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
// itself a moment later. The stray response matches no pending request.
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
				time.Sleep(50 * time.Millisecond)
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
// matches no pending request is ignored rather than delivered: with the
// shared channel it squatted in the buffer and the real response was
// silently dropped, timing the caller out (issue #216).
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

// pendingCount reports how many requests await their response.
func (c *Client) pendingCount() int {
	c.pendingMu.Lock()
	defer c.pendingMu.Unlock()
	return len(c.pending)
}

// TestClientCallLeavesNoPendingEntries proves every exit path — success,
// timeout and connection close — removes the pending entry, so the table
// does not grow without bound (issue #216).
func TestClientCallLeavesNoPendingEntries(t *testing.T) {
	replyURL := startMethodReplyWSServer(t, time.Millisecond)
	c := connectTestClient(t, replyURL)
	c.SetCallTimeout(2 * time.Second)

	for i := 0; i < 5; i++ {
		if _, err := c.Call("pending.ok", nil); err != nil {
			t.Fatalf("successful call %d: %v", i, err)
		}
	}
	if n := c.pendingCount(); n != 0 {
		t.Errorf("after successful calls: %d pending entries remain", n)
	}

	// Timed-out calls must deregister too.
	silentURL := startSilentWSServer(t)
	c2 := connectTestClient(t, silentURL)
	c2.SetCallTimeout(50 * time.Millisecond)
	for i := 0; i < 3; i++ {
		if _, err := c2.Call("pending.never", nil); err == nil {
			t.Fatal("expected timeout error")
		}
	}
	// The timed-out entries are removed by the deferred deregistration,
	// which runs before Call returns.
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
	time.Sleep(100 * time.Millisecond)
	c3.Close()
	wg.Wait()
	if n := c3.pendingCount(); n != 0 {
		t.Errorf("after a close-interrupted call: %d pending entries remain", n)
	}
}
