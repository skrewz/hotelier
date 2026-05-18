package rpc

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

func newTestHub(t *testing.T) *Hub {
	t.Helper()
	return NewHub(func(format string, args ...interface{}) {})
}

func TestNewHub(t *testing.T) {
	hub := newTestHub(t)
	if hub == nil {
		t.Fatal("expected non-nil hub")
	}
	if hub.ConnectionCount() != 0 {
		t.Errorf("expected 0 connections, got %d", hub.ConnectionCount())
	}
}

func TestRegisterMethod(t *testing.T) {
	hub := newTestHub(t)

	hub.RegisterMethod("test.method", func(ctx context.Context, params json.RawMessage) (interface{}, *RPCError) {
		return map[string]interface{}{"status": "ok"}, nil
	})

	hub.mu.RLock()
	_, ok := hub.methods["test.method"]
	hub.mu.RUnlock()

	if !ok {
		t.Error("expected method to be registered")
	}
}

func TestRPCError(t *testing.T) {
	err := ParseError()
	if err.Code != CodeParseError {
		t.Errorf("expected code %d, got %d", CodeParseError, err.Code)
	}
	if err.Message == "" {
		t.Error("expected non-empty message")
	}

	errStr := err.Error()
	if errStr == "" {
		t.Error("expected non-empty error string")
	}
}

func TestStandardErrorCodes(t *testing.T) {
	tests := []struct {
		name     string
		err      *RPCError
		expected int
	}{
		{"ParseError", ParseError(), CodeParseError},
		{"InvalidRequest", InvalidRequestError(), CodeInvalidRequest},
		{"MethodNotFound", MethodNotFoundError("test"), CodeMethodNotFound},
		{"InvalidParams", InvalidParamsError("bad params"), CodeInvalidParams},
		{"InternalError", InternalError("internal"), CodeInternalError},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.err.Code != tt.expected {
				t.Errorf("expected code %d, got %d", tt.expected, tt.err.Code)
			}
		})
	}
}

func TestMethodNotFoundError(t *testing.T) {
	err := MethodNotFoundError("guest.register")
	if err.Code != CodeMethodNotFound {
		t.Errorf("expected code %d, got %d", CodeMethodNotFound, err.Code)
	}
	if err.Message != "Method \"guest.register\" not found" {
		t.Errorf("expected method not found message, got %s", err.Message)
	}
}

func TestInvalidParamsError(t *testing.T) {
	err := InvalidParamsError("missing required field")
	if err.Code != CodeInvalidParams {
		t.Errorf("expected code %d, got %d", CodeInvalidParams, err.Code)
	}
	if err.Message != "missing required field" {
		t.Errorf("expected 'missing required field', got %s", err.Message)
	}
}

func TestInternalError(t *testing.T) {
	err := InternalError("something went wrong")
	if err.Code != CodeInternalError {
		t.Errorf("expected code %d, got %d", CodeInternalError, err.Code)
	}
	if err.Message != "something went wrong" {
		t.Errorf("expected 'something went wrong', got %s", err.Message)
	}
}

func TestJSONRPCMessageMarshal(t *testing.T) {
	idBytes := json.RawMessage(`1`)
	msg := &JSONRPCMessage{
		JSONRPC: "2.0",
		ID:      (*json.RawMessage)(&idBytes),
		Method:  "test.method",
		Params:  json.RawMessage(`{"key":"value"}`),
	}

	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	var parsed JSONRPCMessage
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if parsed.JSONRPC != "2.0" {
		t.Errorf("expected jsonrpc 2.0, got %s", parsed.JSONRPC)
	}
	if parsed.Method != "test.method" {
		t.Errorf("expected method test.method, got %s", parsed.Method)
	}
}

func TestJSONRPCMessageWithResult(t *testing.T) {
	idBytes := json.RawMessage(`1`)
	msg := &JSONRPCMessage{
		JSONRPC: "2.0",
		ID:      (*json.RawMessage)(&idBytes),
		Result:  json.RawMessage(`{"status":"ok"}`),
	}

	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	var parsed JSONRPCMessage
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if parsed.Result == nil {
		t.Error("expected result to be set")
	}
	if parsed.Error != nil {
		t.Error("expected error to be nil")
	}
}

func TestJSONRPCMessageWithError(t *testing.T) {
	idBytes := json.RawMessage(`1`)
	msg := &JSONRPCMessage{
		JSONRPC: "2.0",
		ID:      (*json.RawMessage)(&idBytes),
		Error:   &RPCError{Code: CodeInternalError, Message: "test error"},
	}

	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	var parsed JSONRPCMessage
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if parsed.Error == nil {
		t.Error("expected error to be set")
	}
	if parsed.Error.Code != CodeInternalError {
		t.Errorf("expected error code %d, got %d", CodeInternalError, parsed.Error.Code)
	}
}

func TestJSONRPCMessageNotification(t *testing.T) {
	msg := &JSONRPCMessage{
		JSONRPC: "2.0",
		Method:  "test.notification",
		Params:  json.RawMessage(`{}`),
	}

	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	var parsed JSONRPCMessage
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if parsed.ID != nil {
		t.Error("expected ID to be nil for notification")
	}
	if parsed.Method != "test.notification" {
		t.Errorf("expected method test.notification, got %s", parsed.Method)
	}
}

func TestHubConnectionCount(t *testing.T) {
	hub := newTestHub(t)
	if hub.ConnectionCount() != 0 {
		t.Errorf("expected 0 connections initially, got %d", hub.ConnectionCount())
	}
}

func TestHubGetConnection(t *testing.T) {
	hub := newTestHub(t)

	_, ok := hub.GetConnection("nonexistent")
	if ok {
		t.Error("expected nonexistent connection to not exist")
	}
}

func TestHubGetAllConnectionIDs(t *testing.T) {
	hub := newTestHub(t)
	ids := hub.GetAllConnectionIDs()
	if len(ids) != 0 {
		t.Errorf("expected 0 connection IDs, got %d", len(ids))
	}
}

func TestSendToNonexistent(t *testing.T) {
	hub := newTestHub(t)

	err := hub.SendTo("nonexistent", &JSONRPCMessage{
		JSONRPC: "2.0",
		ID:      func() *json.RawMessage { x := json.RawMessage([]byte(`1`)); return &x }(),
		Result:  json.RawMessage(`{}`),
	})
	if err == nil {
		t.Error("expected error for nonexistent connection, got nil")
	}
}

func TestBroadcastEmpty(t *testing.T) {
	hub := newTestHub(t)

	hub.Broadcast("", &JSONRPCMessage{
		JSONRPC: "2.0",
		Method:  "test",
	})
}

func TestSendToGuest(t *testing.T) {
	hub := newTestHub(t)

	err := hub.SendToGuest("nonexistent", "task.assign", map[string]string{"id": "task-1"})
	if err == nil {
		t.Error("expected error for nonexistent guest, got nil")
	}
}

func TestSendNotification(t *testing.T) {
	hub := newTestHub(t)

	err := hub.SendNotification("nonexistent", "", "task.log", map[string]string{"line": "test"})
	if err == nil {
		t.Error("expected error for nonexistent connection, got nil")
	}
}

func TestNewUpgrader(t *testing.T) {
	upgrader := NewUpgrader()
	if upgrader == nil {
		t.Fatal("expected non-nil upgrader")
	}
	if upgrader.ReadBufferSize != 1024 {
		t.Errorf("expected read buffer 1024, got %d", upgrader.ReadBufferSize)
	}
	if upgrader.WriteBufferSize != 1024 {
		t.Errorf("expected write buffer 1024, got %d", upgrader.WriteBufferSize)
	}
}

func TestNewClient(t *testing.T) {
	hub := NewClientHub(func(format string, args ...interface{}) {})
	client := NewClient("test-client", hub, func(format string, args ...interface{}) {})

	if client == nil {
		t.Fatal("expected non-nil client")
	}
	if client.id != "test-client" {
		t.Errorf("expected id test-client, got %s", client.id)
	}
}

func TestClientHubConnectionCount(t *testing.T) {
	hub := NewClientHub(func(format string, args ...interface{}) {})
	if hub.ConnectionCount() != 0 {
		t.Errorf("expected 0 connections, got %d", hub.ConnectionCount())
	}
}

func TestHubRun(t *testing.T) {
	hub := newTestHub(t)

	done := make(chan struct{})
	go func() {
		hub.Run()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Millisecond):
	}
}

func TestHubRegisterUnregister(t *testing.T) {
	hub := newTestHub(t)

	done := make(chan struct{})
	go func() {
		hub.Run()
		close(done)
	}()

	conn := &Connection{
		send:    make(chan []byte, 256),
		conn:    nil,
		id:      "test-conn",
		hub:     hub,
		closeCh: make(chan struct{}),
	}

	hub.register <- conn
	time.Sleep(10 * time.Millisecond)

	if hub.ConnectionCount() != 1 {
		t.Errorf("expected 1 connection after register, got %d", hub.ConnectionCount())
	}

	hub.unregister <- conn
	time.Sleep(10 * time.Millisecond)

	if hub.ConnectionCount() != 0 {
		t.Errorf("expected 0 connections after unregister, got %d", hub.ConnectionCount())
	}
}

func TestHubConcurrency(t *testing.T) {
	hub := newTestHub(t)

	done := make(chan struct{})
	go func() {
		hub.Run()
		close(done)
	}()

	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			conn := &Connection{
				send:    make(chan []byte, 256),
				conn:    nil,
				id:      fmt.Sprintf("conn-%d", id),
				hub:     hub,
				closeCh: make(chan struct{}),
			}
			hub.register <- conn
			time.Sleep(1 * time.Millisecond)
			hub.unregister <- conn
		}(i)
	}

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			hub.ConnectionCount()
		}()
	}

	wg.Wait()
}

// Additional RPC tests for coverage
func TestJSONRPCMessageWithNilID(t *testing.T) {
	msg := &JSONRPCMessage{
		JSONRPC: "2.0",
		Method:  "test",
		Params:  json.RawMessage(`{}`),
	}
	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	var parsed JSONRPCMessage
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if parsed.ID != nil {
		t.Error("expected nil ID")
	}
}

func TestJSONRPCMessageWithEmptyParams(t *testing.T) {
	idBytes := json.RawMessage([]byte(`1`))
	msg := &JSONRPCMessage{
		JSONRPC: "2.0",
		ID:      (*json.RawMessage)(&idBytes),
		Method:  "test",
		Params:  json.RawMessage(`{}`),
	}
	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	var parsed JSONRPCMessage
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if parsed.Params == nil {
		t.Error("expected non-nil Params")
	}
}

func TestJSONRPCMessageWithEmptyMethod(t *testing.T) {
	idBytes := json.RawMessage([]byte(`1`))
	msg := &JSONRPCMessage{
		JSONRPC: "2.0",
		ID:      (*json.RawMessage)(&idBytes),
		Method:  "",
		Params:  json.RawMessage(`{}`),
	}
	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	var parsed JSONRPCMessage
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if parsed.Method != "" {
		t.Error("expected empty method")
	}
}

func TestJSONRPCMessageWithEmptyResult(t *testing.T) {
	idBytes := json.RawMessage([]byte(`1`))
	msg := &JSONRPCMessage{
		JSONRPC: "2.0",
		ID:      (*json.RawMessage)(&idBytes),
		Result:  json.RawMessage(`{}`),
	}
	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	var parsed JSONRPCMessage
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if parsed.Result == nil {
		t.Error("expected non-nil Result")
	}
}

func TestJSONRPCMessageWithEmptyError(t *testing.T) {
	idBytes := json.RawMessage([]byte(`1`))
	msg := &JSONRPCMessage{
		JSONRPC: "2.0",
		ID:      (*json.RawMessage)(&idBytes),
		Error:   &RPCError{Code: CodeInternalError, Message: ""},
	}
	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	var parsed JSONRPCMessage
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if parsed.Error == nil {
		t.Error("expected non-nil Error")
	}
}

func TestJSONRPCMessageWithLongMethod(t *testing.T) {
	idBytes := json.RawMessage([]byte(`1`))
	method := "guest.register.with.very.long.method.name.that.goes.on.and.on"
	msg := &JSONRPCMessage{
		JSONRPC: "2.0",
		ID:      (*json.RawMessage)(&idBytes),
		Method:  method,
	}
	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	var parsed JSONRPCMessage
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if parsed.Method != method {
		t.Error("method mismatch")
	}
}

func TestJSONRPCMessageWithLongParams(t *testing.T) {
	idBytes := json.RawMessage([]byte(`1`))
	longParams := json.RawMessage(fmt.Sprintf(`{"data":"%s"}`, strings.Repeat("x", 1000)))
	msg := &JSONRPCMessage{
		JSONRPC: "2.0",
		ID:      (*json.RawMessage)(&idBytes),
		Params:  longParams,
	}
	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	var parsed JSONRPCMessage
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if len(parsed.Params) == 0 {
		t.Error("expected non-empty Params")
	}
}

func TestJSONRPCMessageWithLargeResult(t *testing.T) {
	idBytes := json.RawMessage([]byte(`1`))
	largeResult := json.RawMessage(fmt.Sprintf(`{"data":"%s"}`, strings.Repeat("x", 10000)))
	msg := &JSONRPCMessage{
		JSONRPC: "2.0",
		ID:      (*json.RawMessage)(&idBytes),
		Result:  largeResult,
	}
	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	var parsed JSONRPCMessage
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if len(parsed.Result) == 0 {
		t.Error("expected non-empty Result")
	}
}

func TestJSONRPCMessageWithLargeError(t *testing.T) {
	idBytes := json.RawMessage([]byte(`1`))
	msg := &JSONRPCMessage{
		JSONRPC: "2.0",
		ID:      (*json.RawMessage)(&idBytes),
		Error:   &RPCError{Code: CodeInternalError, Message: strings.Repeat("x", 1000)},
	}
	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	var parsed JSONRPCMessage
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if parsed.Error == nil || len(parsed.Error.Message) == 0 {
		t.Error("expected non-empty Error")
	}
}

func TestJSONRPCMessageWithSpecialCharsInMethod(t *testing.T) {
	idBytes := json.RawMessage([]byte(`1`))
	msg := &JSONRPCMessage{
		JSONRPC: "2.0",
		ID:      (*json.RawMessage)(&idBytes),
		Method:  "guest.register.with.dots",
	}
	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	var parsed JSONRPCMessage
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if parsed.Method != "guest.register.with.dots" {
		t.Error("method mismatch")
	}
}

func TestJSONRPCMessageWithSpecialCharsInParams(t *testing.T) {
	idBytes := json.RawMessage([]byte(`1`))
	msg := &JSONRPCMessage{
		JSONRPC: "2.0",
		ID:      (*json.RawMessage)(&idBytes),
		Params:  json.RawMessage(`{"key":"value with spaces"}`),
	}
	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	var parsed JSONRPCMessage
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if parsed.Params == nil {
		t.Error("expected non-nil Params")
	}
}

func TestJSONRPCMessageWithUnicodeInMethod(t *testing.T) {
	idBytes := json.RawMessage([]byte(`1`))
	msg := &JSONRPCMessage{
		JSONRPC: "2.0",
		ID:      (*json.RawMessage)(&idBytes),
		Method:  "guest.register.中文",
	}
	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	var parsed JSONRPCMessage
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if parsed.Method != "guest.register.中文" {
		t.Error("method mismatch")
	}
}

func TestJSONRPCMessageWithUnicodeInParams(t *testing.T) {
	idBytes := json.RawMessage([]byte(`1`))
	msg := &JSONRPCMessage{
		JSONRPC: "2.0",
		ID:      (*json.RawMessage)(&idBytes),
		Params:  json.RawMessage(`{"key":"中文"}`),
	}
	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	var parsed JSONRPCMessage
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if parsed.Params == nil {
		t.Error("expected non-nil Params")
	}
}

func TestJSONRPCMessageWithNullParams(t *testing.T) {
	idBytes := json.RawMessage([]byte(`1`))
	msg := &JSONRPCMessage{
		JSONRPC: "2.0",
		ID:      (*json.RawMessage)(&idBytes),
		Params:  json.RawMessage(`null`),
	}
	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	var parsed JSONRPCMessage
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if parsed.Params == nil {
		t.Error("expected non-nil Params")
	}
}

func TestJSONRPCMessageWithArrayParams(t *testing.T) {
	idBytes := json.RawMessage([]byte(`1`))
	msg := &JSONRPCMessage{
		JSONRPC: "2.0",
		ID:      (*json.RawMessage)(&idBytes),
		Params:  json.RawMessage(`[1, 2, 3]`),
	}
	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	var parsed JSONRPCMessage
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if parsed.Params == nil {
		t.Error("expected non-nil Params")
	}
}

func TestJSONRPCMessageWithNestedObjectParams(t *testing.T) {
	idBytes := json.RawMessage([]byte(`1`))
	msg := &JSONRPCMessage{
		JSONRPC: "2.0",
		ID:      (*json.RawMessage)(&idBytes),
		Params:  json.RawMessage(`{"outer":{"inner":{"value":42}}}`),
	}
	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	var parsed JSONRPCMessage
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if parsed.Params == nil {
		t.Error("expected non-nil Params")
	}
}

func TestJSONRPCMessageWithBooleanResult(t *testing.T) {
	idBytes := json.RawMessage([]byte(`1`))
	msg := &JSONRPCMessage{
		JSONRPC: "2.0",
		ID:      (*json.RawMessage)(&idBytes),
		Result:  json.RawMessage(`true`),
	}
	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	var parsed JSONRPCMessage
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if parsed.Result == nil {
		t.Error("expected non-nil Result")
	}
}

func TestJSONRPCMessageWithNumberResult(t *testing.T) {
	idBytes := json.RawMessage([]byte(`1`))
	msg := &JSONRPCMessage{
		JSONRPC: "2.0",
		ID:      (*json.RawMessage)(&idBytes),
		Result:  json.RawMessage(`42`),
	}
	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	var parsed JSONRPCMessage
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if parsed.Result == nil {
		t.Error("expected non-nil Result")
	}
}

func TestJSONRPCMessageWithStringResult(t *testing.T) {
	idBytes := json.RawMessage([]byte(`1`))
	msg := &JSONRPCMessage{
		JSONRPC: "2.0",
		ID:      (*json.RawMessage)(&idBytes),
		Result:  json.RawMessage(`"hello"`),
	}
	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	var parsed JSONRPCMessage
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if parsed.Result == nil {
		t.Error("expected non-nil Result")
	}
}

func TestJSONRPCMessageWithArrayResult(t *testing.T) {
	idBytes := json.RawMessage([]byte(`1`))
	msg := &JSONRPCMessage{
		JSONRPC: "2.0",
		ID:      (*json.RawMessage)(&idBytes),
		Result:  json.RawMessage(`[1, 2, 3]`),
	}
	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	var parsed JSONRPCMessage
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if parsed.Result == nil {
		t.Error("expected non-nil Result")
	}
}

func TestJSONRPCMessageWithNullResult(t *testing.T) {
	idBytes := json.RawMessage([]byte(`1`))
	msg := &JSONRPCMessage{
		JSONRPC: "2.0",
		ID:      (*json.RawMessage)(&idBytes),
		Result:  json.RawMessage(`null`),
	}
	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	var parsed JSONRPCMessage
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if parsed.Result == nil {
		t.Error("expected non-nil Result")
	}
}

func TestJSONRPCMessageWithLargeID(t *testing.T) {
	idBytes := json.RawMessage([]byte(`999999999999999999999999999999`))
	msg := &JSONRPCMessage{
		JSONRPC: "2.0",
		ID:      (*json.RawMessage)(&idBytes),
		Method:  "test",
	}
	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	var parsed JSONRPCMessage
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if parsed.ID == nil {
		t.Error("expected non-nil ID")
	}
}

func TestJSONRPCMessageWithNegativeID(t *testing.T) {
	idBytes := json.RawMessage([]byte(`-1`))
	msg := &JSONRPCMessage{
		JSONRPC: "2.0",
		ID:      (*json.RawMessage)(&idBytes),
		Method:  "test",
	}
	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	var parsed JSONRPCMessage
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if parsed.ID == nil {
		t.Error("expected non-nil ID")
	}
}

func TestJSONRPCMessageWithFloatID(t *testing.T) {
	idBytes := json.RawMessage([]byte(`1.5`))
	msg := &JSONRPCMessage{
		JSONRPC: "2.0",
		ID:      (*json.RawMessage)(&idBytes),
		Method:  "test",
	}
	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	var parsed JSONRPCMessage
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if parsed.ID == nil {
		t.Error("expected non-nil ID")
	}
}

func TestJSONRPCMessageWithEmptyJSONRPC(t *testing.T) {
	idBytes := json.RawMessage([]byte(`1`))
	msg := &JSONRPCMessage{
		JSONRPC: "",
		ID:      (*json.RawMessage)(&idBytes),
		Method:  "test",
	}
	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	var parsed JSONRPCMessage
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if parsed.JSONRPC != "" {
		t.Error("expected empty JSONRPC")
	}
}

func TestJSONRPCMessageWithInvalidJSONRPC(t *testing.T) {
	idBytes := json.RawMessage([]byte(`1`))
	msg := &JSONRPCMessage{
		JSONRPC: "1.0",
		ID:      (*json.RawMessage)(&idBytes),
		Method:  "test",
	}
	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	var parsed JSONRPCMessage
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if parsed.JSONRPC != "1.0" {
		t.Error("JSONRPC mismatch")
	}
}

func TestJSONRPCMessageWithWhitespaceInMethod(t *testing.T) {
	idBytes := json.RawMessage([]byte(`1`))
	msg := &JSONRPCMessage{
		JSONRPC: "2.0",
		ID:      (*json.RawMessage)(&idBytes),
		Method:  " test.method ",
	}
	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	var parsed JSONRPCMessage
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if parsed.Method != " test.method " {
		t.Error("method mismatch")
	}
}

func TestJSONRPCMessageWithWhitespaceInParams(t *testing.T) {
	idBytes := json.RawMessage([]byte(`1`))
	msg := &JSONRPCMessage{
		JSONRPC: "2.0",
		ID:      (*json.RawMessage)(&idBytes),
		Params:  json.RawMessage(`  {"key":"value"}  `),
	}
	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	var parsed JSONRPCMessage
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if parsed.Params == nil {
		t.Error("expected non-nil Params")
	}
}

func TestJSONRPCMessageWithWhitespaceInResult(t *testing.T) {
	idBytes := json.RawMessage([]byte(`1`))
	msg := &JSONRPCMessage{
		JSONRPC: "2.0",
		ID:      (*json.RawMessage)(&idBytes),
		Result:  json.RawMessage(`  {"key":"value"}  `),
	}
	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	var parsed JSONRPCMessage
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if parsed.Result == nil {
		t.Error("expected non-nil Result")
	}
}

func TestJSONRPCMessageWithWhitespaceInError(t *testing.T) {
	idBytes := json.RawMessage([]byte(`1`))
	msg := &JSONRPCMessage{
		JSONRPC: "2.0",
		ID:      (*json.RawMessage)(&idBytes),
		Error:   &RPCError{Code: CodeInternalError, Message: "  error message  "},
	}
	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	var parsed JSONRPCMessage
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if parsed.Error == nil {
		t.Error("expected non-nil Error")
	}
	if parsed.Error.Message != "  error message  " {
		t.Error("error message mismatch")
	}
}

func TestJSONRPCMessageWithMultipleFields(t *testing.T) {
	idBytes := json.RawMessage([]byte(`1`))
	msg := &JSONRPCMessage{
		JSONRPC: "2.0",
		ID:      (*json.RawMessage)(&idBytes),
		Method:  "test.method",
		Params:  json.RawMessage(`{"key":"value"}`),
		Result:  json.RawMessage(`{"status":"ok"}`),
		Error:   &RPCError{Code: CodeInternalError, Message: "error"},
	}
	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	var parsed JSONRPCMessage
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if parsed.JSONRPC != "2.0" {
		t.Error("JSONRPC mismatch")
	}
	if parsed.Method != "test.method" {
		t.Error("method mismatch")
	}
}
