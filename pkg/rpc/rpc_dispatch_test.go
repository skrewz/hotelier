package rpc

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestHubDispatch_RegisteredMethod(t *testing.T) {
	hub := NewHub(t.Logf)

	hub.RegisterMethod("test.echo", func(ctx context.Context, params json.RawMessage) (interface{}, *RPCError) {
		var req struct {
			Message string `json:"message"`
		}
		if err := json.Unmarshal(params, &req); err != nil {
			return nil, InvalidParamsError("invalid params")
		}
		return map[string]interface{}{"echo": req.Message}, nil
	})

	resp, err := hub.Dispatch("test.echo", json.RawMessage(`{"message":"hello"}`))
	if err != nil {
		t.Fatalf("dispatch failed: %v", err)
	}

	result := resp.(map[string]interface{})
	if result["echo"] != "hello" {
		t.Errorf("expected echo 'hello', got %v", result["echo"])
	}
}

func TestHubDispatch_MethodNotFound(t *testing.T) {
	hub := NewHub(t.Logf)

	_, err := hub.Dispatch("nonexistent.method", json.RawMessage("{}"))
	if err == nil {
		t.Fatal("expected error for nonexistent method")
	}
	if err.Code != CodeMethodNotFound {
		t.Errorf("expected CodeMethodNotFound, got %d", err.Code)
	}
	if err.Message != "Method \"nonexistent.method\" not found" {
		t.Errorf("unexpected error message: %s", err.Message)
	}
}

func TestHubDispatch_HandlerError(t *testing.T) {
	hub := NewHub(t.Logf)

	hub.RegisterMethod("test.fail", func(ctx context.Context, params json.RawMessage) (interface{}, *RPCError) {
		return nil, InternalError("handler failed")
	})

	_, err := hub.Dispatch("test.fail", json.RawMessage("{}"))
	if err == nil {
		t.Fatal("expected error from handler")
	}
	if err.Code != CodeInternalError {
		t.Errorf("expected CodeInternalError, got %d", err.Code)
	}
	if err.Message != "handler failed" {
		t.Errorf("unexpected error message: %s", err.Message)
	}
}

func TestHubDispatch_NilResponse(t *testing.T) {
	hub := NewHub(t.Logf)

	hub.RegisterMethod("test.nil", func(ctx context.Context, params json.RawMessage) (interface{}, *RPCError) {
		return nil, nil
	})

	resp, err := hub.Dispatch("test.nil", json.RawMessage("{}"))
	if err != nil {
		t.Fatalf("dispatch failed: %v", err)
	}
	if resp != nil {
		t.Errorf("expected nil response, got %v", resp)
	}
}

func TestHubDispatch_InvalidParams(t *testing.T) {
	hub := NewHub(t.Logf)

	hub.RegisterMethod("test.validate", func(ctx context.Context, params json.RawMessage) (interface{}, *RPCError) {
		var req struct {
			Name string `json:"name"`
			Age  int    `json:"age"`
		}
		if err := json.Unmarshal(params, &req); err != nil {
			return nil, InvalidParamsError("name and age required")
		}
		if req.Name == "" {
			return nil, InvalidParamsError("name is required")
		}
		if req.Age <= 0 {
			return nil, InvalidParamsError("age must be positive")
		}
		return map[string]interface{}{"valid": true}, nil
	})

	// Missing fields
	_, err := hub.Dispatch("test.validate", json.RawMessage(`{}`))
	if err == nil {
		t.Fatal("expected error for missing fields")
	}
	if err.Code != CodeInvalidParams {
		t.Errorf("expected CodeInvalidParams, got %d", err.Code)
	}

	// Empty name
	_, err = hub.Dispatch("test.validate", json.RawMessage(`{"name":"","age":25}`))
	if err == nil {
		t.Fatal("expected error for empty name")
	}
	if err.Message != "name is required" {
		t.Errorf("expected 'name is required', got %s", err.Message)
	}

	// Negative age
	_, err = hub.Dispatch("test.validate", json.RawMessage(`{"name":"John","age":-1}`))
	if err == nil {
		t.Fatal("expected error for negative age")
	}
	if err.Message != "age must be positive" {
		t.Errorf("expected 'age must be positive', got %s", err.Message)
	}

	// Valid
	resp, err := hub.Dispatch("test.validate", json.RawMessage(`{"name":"John","age":25}`))
	if err != nil {
		t.Fatalf("dispatch failed: %v", err)
	}
	result := resp.(map[string]interface{})
	if !result["valid"].(bool) {
		t.Error("expected valid=true")
	}
}

func TestHubDispatch_BigParams(t *testing.T) {
	hub := NewHub(t.Logf)

	hub.RegisterMethod("test.big", func(ctx context.Context, params json.RawMessage) (interface{}, *RPCError) {
		return map[string]interface{}{"size": len(params)}, nil
	})

	largeParams := json.RawMessage(`{"data":"` + string(make([]byte, 10000)) + `"}`)
	resp, err := hub.Dispatch("test.big", largeParams)
	if err != nil {
		t.Fatalf("dispatch failed: %v", err)
	}
	result := resp.(map[string]interface{})
	size := result["size"].(int)
	if size < 10000 {
		t.Errorf("expected size >= 10000, got %d", size)
	}
}

func TestHubConnectionLifecycle(t *testing.T) {
	hub := NewHub(t.Logf)

	go hub.Run()
	defer func() {}() // Let goroutine run

	// Create a mock connection
	conn := &Connection{
		id:      "test-conn-1",
		send:    make(chan []byte, 256),
		closeCh: make(chan struct{}),
		hub:     hub,
	}

	// Register the connection
	hub.Register(conn)
	time.Sleep(10 * time.Millisecond)

	if hub.ConnectionCount() != 1 {
		t.Errorf("expected 1 connection, got %d", hub.ConnectionCount())
	}

	// Get the connection
	got, ok := hub.GetConnection("test-conn-1")
	if !ok {
		t.Fatal("expected connection to exist")
	}
	if got.id != "test-conn-1" {
		t.Errorf("expected id test-conn-1, got %s", got.id)
	}

	// Get all connection IDs
	ids := hub.GetAllConnectionIDs()
	if len(ids) != 1 {
		t.Errorf("expected 1 connection ID, got %d", len(ids))
	}

	// Unregister the connection
	hub.Unregister(conn)
	time.Sleep(10 * time.Millisecond)

	if hub.ConnectionCount() != 0 {
		t.Errorf("expected 0 connections, got %d", hub.ConnectionCount())
	}

	// Connection should no longer exist
	_, ok = hub.GetConnection("test-conn-1")
	if ok {
		t.Error("expected connection to not exist after unregister")
	}
}

func TestHubMultipleConnections(t *testing.T) {
	hub := NewHub(t.Logf)

	go hub.Run()

	// Register multiple connections
	conns := make([]*Connection, 5)
	for i := 0; i < 5; i++ {
		conns[i] = &Connection{
			id:      fmt.Sprintf("conn-%d", i),
			send:    make(chan []byte, 256),
			closeCh: make(chan struct{}),
			hub:     hub,
		}
		hub.Register(conns[i])
	}
	time.Sleep(10 * time.Millisecond)

	if hub.ConnectionCount() != 5 {
		t.Errorf("expected 5 connections, got %d", hub.ConnectionCount())
	}

	// Unregister half
	for i := 0; i < 3; i++ {
		hub.Unregister(conns[i])
	}
	time.Sleep(10 * time.Millisecond)

	if hub.ConnectionCount() != 2 {
		t.Errorf("expected 2 connections, got %d", hub.ConnectionCount())
	}
}

func TestHubSendTo(t *testing.T) {
	hub := NewHub(t.Logf)

	go hub.Run()

	conn := &Connection{
		id:      "send-to-conn",
		send:    make(chan []byte, 256),
		closeCh: make(chan struct{}),
		hub:     hub,
	}
	hub.Register(conn)
	time.Sleep(10 * time.Millisecond)

	// Send a message
	msg := &JSONRPCMessage{
		JSONRPC: "2.0",
		ID:      func() *json.RawMessage { x := json.RawMessage([]byte(`1`)); return &x }(),
		Result:  json.RawMessage(`{"status":"ok"}`),
	}
	err := hub.SendTo("send-to-conn", msg)
	if err != nil {
		t.Fatalf("SendTo failed: %v", err)
	}

	// Read the message from the connection's send channel
	select {
	case data := <-conn.send:
		var received JSONRPCMessage
		if err := json.Unmarshal(data, &received); err != nil {
			t.Fatalf("failed to unmarshal: %v", err)
		}
		if received.JSONRPC != "2.0" {
			t.Errorf("expected jsonrpc 2.0, got %s", received.JSONRPC)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timed out waiting for message")
	}

	// Send to nonexistent connection
	err = hub.SendTo("nonexistent", msg)
	if err == nil {
		t.Error("expected error for nonexistent connection")
	}
}

func TestHubBroadcast(t *testing.T) {
	hub := NewHub(t.Logf)

	go hub.Run()

	// Register multiple connections
	conns := make([]*Connection, 3)
	for i := 0; i < 3; i++ {
		conns[i] = &Connection{
			id:      fmt.Sprintf("broadcast-conn-%d", i),
			send:    make(chan []byte, 256),
			closeCh: make(chan struct{}),
			hub:     hub,
		}
		hub.Register(conns[i])
	}
	time.Sleep(10 * time.Millisecond)

	// Broadcast a message
	msg := &JSONRPCMessage{
		JSONRPC: "2.0",
		Method:  "test.broadcast",
		Params:  json.RawMessage(`{"data":"hello"}`),
	}
	hub.Broadcast(msg)

	// All connections should receive the message
	for i, conn := range conns {
		select {
		case data := <-conn.send:
			var received JSONRPCMessage
			if err := json.Unmarshal(data, &received); err != nil {
				t.Fatalf("connection %d: failed to unmarshal: %v", i, err)
			}
			if received.Method != "test.broadcast" {
				t.Errorf("connection %d: expected method test.broadcast, got %s", i, received.Method)
			}
		case <-time.After(100 * time.Millisecond):
			t.Fatalf("connection %d: timed out waiting for broadcast", i)
		}
	}
}

func TestHubConnectionConcurrency(t *testing.T) {
	hub := NewHub(t.Logf)

	go hub.Run()

	var wg sync.WaitGroup

	// Concurrent registrations
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			conn := &Connection{
				id:      fmt.Sprintf("concurrent-%d", id),
				send:    make(chan []byte, 256),
				closeCh: make(chan struct{}),
				hub:     hub,
			}
			hub.Register(conn)
			time.Sleep(time.Millisecond)
			hub.Unregister(conn)
		}(i)
	}

	// Concurrent connection count checks
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			hub.ConnectionCount()
		}()
	}

	// Concurrent GetConnection
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			hub.GetConnection(fmt.Sprintf("concurrent-%d", id))
		}(i)
	}

	wg.Wait()
}

func TestHubDispatch_Concurrency(t *testing.T) {
	hub := NewHub(t.Logf)

	hub.RegisterMethod("test.counter", func(ctx context.Context, params json.RawMessage) (interface{}, *RPCError) {
		return map[string]interface{}{"ok": true}, nil
	})

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			_, err := hub.Dispatch("test.counter", json.RawMessage(fmt.Sprintf(`{"id":%d}`, id)))
			if err != nil {
				t.Errorf("dispatch %d failed: %v", id, err)
			}
		}(i)
	}

	wg.Wait()
}

func TestHubSendTo_Concurrency(t *testing.T) {
	hub := NewHub(t.Logf)

	go hub.Run()

	// Register a connection
	conn := &Connection{
		id:      "concurrent-send",
		send:    make(chan []byte, 256),
		closeCh: make(chan struct{}),
		hub:     hub,
	}
	hub.Register(conn)
	time.Sleep(10 * time.Millisecond)

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			msg := &JSONRPCMessage{
				JSONRPC: "2.0",
				ID:      func() *json.RawMessage { x := json.RawMessage([]byte(fmt.Sprintf(`%d`, id))); return &x }(),
				Result:  json.RawMessage(`{"id":` + fmt.Sprintf("%d", id) + `}`),
			}
			hub.SendTo("concurrent-send", msg)
		}(i)
	}

	wg.Wait()

	// Verify all messages were received
	received := 0
	for {
		select {
		case <-conn.send:
			received++
		case <-time.After(50 * time.Millisecond):
			if received != 20 {
				t.Errorf("expected 20 messages, got %d", received)
			}
			return
		}
	}
}
