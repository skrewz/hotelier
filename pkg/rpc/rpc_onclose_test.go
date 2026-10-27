package rpc

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// TestClientOnCloseFiresOnImmediateClose covers issue #218: the read loop
// must observe the onClose callback that was registered before Connect.
// A server that closes the socket immediately after the upgrade can make
// the read loop fail before the caller of Connect has run anything else —
// if the callback is wired after the connection is established, the read
// loop may see a nil onClose and the loss is never reported.
func TestClientOnCloseFiresOnImmediateClose(t *testing.T) {
	upgrader := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		// Close immediately after the upgrade completes.
		_ = conn.Close()
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	hub := NewClientHub(func(format string, args ...interface{}) {})
	client := NewClient("issue218-client", hub, func(format string, args ...interface{}) {})
	t.Cleanup(client.Close)

	// The callback must be registered BEFORE connecting — that is the
	// contract issue #218 establishes.
	var once sync.Once
	fired := make(chan struct{})
	client.SetOnClose(func() {
		once.Do(func() { close(fired) })
	})

	url := "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws"
	if err := client.Connect(url); err != nil {
		t.Fatalf("connect: %v", err)
	}

	select {
	case <-fired:
		// expected — the loss was reported to the registered callback
	case <-time.After(2 * time.Second):
		t.Fatal("onClose registered before Connect was not invoked after the server closed the socket")
	}
}

// TestClientSetOnCloseRaceFree exercises SetOnClose concurrently with a
// live read loop. Under -race this fails if Client.onClose is read by the
// read loop without synchronisation (issue #218).
func TestClientSetOnCloseRaceFree(t *testing.T) {
	upgrader := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		// Keep the connection open long enough for the concurrent
		// SetOnClose calls below to overlap with the read loop, then
		// close it so the read loop observes the loss.
		time.Sleep(100 * time.Millisecond)
		_ = conn.Close()
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	hub := NewClientHub(func(format string, args ...interface{}) {})
	client := NewClient("issue218-race-client", hub, func(format string, args ...interface{}) {})
	t.Cleanup(client.Close)

	url := "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws"
	if err := client.Connect(url); err != nil {
		t.Fatalf("connect: %v", err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			client.SetOnClose(func() {})
		}()
	}
	wg.Wait()
}
