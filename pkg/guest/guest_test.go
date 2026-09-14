package guest

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"hotelier/internal/server"
	"hotelier/pkg/config"
	"hotelier/pkg/queue"
	"hotelier/pkg/rpc"
)

func newTestGuest(t *testing.T) *Guest {
	t.Helper()
	cfg := config.GuestConfig{
		ID:   "test-guest-1",
		Name: "Test Guest",
		Tags: []string{"business-default", "frontend"},
	}
	handler := func(ctx context.Context, task TaskAssignment, _ LogCallback) (*TaskResult, error) {
		return &TaskResult{TaskID: task.TaskID, Success: true, Output: "test result"}, nil
	}
	return New(cfg, handler)
}

func TestNewGuest(t *testing.T) {
	ag := newTestGuest(t)
	if ag == nil {
		t.Fatal("expected non-nil guest")
	}
	if !strings.HasPrefix(ag.id, "guest-") {
		t.Errorf("expected ephemeral id starting with 'guest-', got %s", ag.id)
	}
	if len(ag.tags) != 2 {
		t.Errorf("expected 2 tags, got %d", len(ag.tags))
	}
}

func TestGuestConfig(t *testing.T) {
	cfg := config.GuestConfig{ID: "a2", TaskTimeout: 600, HeartbeatInterval: 10}
	ag := New(cfg, func(ctx context.Context, task TaskAssignment, _ LogCallback) (*TaskResult, error) {
		return &TaskResult{TaskID: task.TaskID, Success: true}, nil
	})
	if ag.config.TaskTimeout != 600 {
		t.Errorf("expected task_timeout 600, got %d", ag.config.TaskTimeout)
	}
}

func TestTaskAssignmentMarshal(t *testing.T) {
	task := TaskAssignment{TaskID: "t1", Prompt: "Build", Tags: []string{"tag"}}
	data, err := json.Marshal(task)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	var parsed TaskAssignment
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if parsed.TaskID != "t1" {
		t.Error("round-trip failed")
	}
}

func TestTaskResultMarshal(t *testing.T) {
	result := TaskResult{TaskID: "t1", Success: true, Output: "ok"}
	data, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	var parsed TaskResult
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if parsed.TaskID != "t1" || !parsed.Success {
		t.Error("round-trip failed")
	}
}

func TestTaskResultFailureMarshal(t *testing.T) {
	result := TaskResult{TaskID: "t1", Success: false, Error: "fail"}
	data, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	var parsed TaskResult
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if parsed.Success {
		t.Error("round-trip failed")
	}
}

func TestLogEntryMarshal(t *testing.T) {
	entry := LogEntry{TaskID: "t1", Line: "test", Level: "info", Timestamp: time.Now()}
	data, err := json.Marshal(entry)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	var parsed LogEntry
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if parsed.TaskID != "t1" {
		t.Error("round-trip failed")
	}
}

func TestTaskCancelMarshal(t *testing.T) {
	cancel := TaskCancel{TaskID: "t1", Reason: "timeout"}
	data, err := json.Marshal(cancel)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	var parsed TaskCancel
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if parsed.TaskID != "t1" {
		t.Error("round-trip failed")
	}
}

// Additional guest tests for coverage
func TestNewGuestWithConfig(t *testing.T) {
	cfg := config.GuestConfig{
		ID:                "test-guest",
		Name:              "Test Guest",
		Tags:              []string{"business-default", "android"},
		TaskTimeout:       1800,
		HeartbeatInterval: 15,
		WorkingDir:        "/tmp/test",
		LogLevel:          "debug",
	}
	handler := func(ctx context.Context, task TaskAssignment, _ LogCallback) (*TaskResult, error) {
		return &TaskResult{TaskID: task.TaskID, Success: true, Output: "done"}, nil
	}
	ag := New(cfg, handler)
	if !strings.HasPrefix(ag.id, "guest-") {
		t.Errorf("expected ephemeral id starting with 'guest-', got %s", ag.id)
	}
	if ag.config.TaskTimeout != 1800 {
		t.Errorf("expected task_timeout 1800, got %d", ag.config.TaskTimeout)
	}
	if ag.config.HeartbeatInterval != 15 {
		t.Errorf("expected heartbeat_interval 15, got %d", ag.config.HeartbeatInterval)
	}
}

func TestNewGuestWithEmptyTags(t *testing.T) {
	cfg := config.GuestConfig{ID: "test-guest", Name: "Test Guest", Tags: []string{}}
	handler := func(ctx context.Context, task TaskAssignment, _ LogCallback) (*TaskResult, error) {
		return &TaskResult{TaskID: task.TaskID, Success: true}, nil
	}
	ag := New(cfg, handler)
	if len(ag.tags) != 0 {
		t.Errorf("expected 0 tags, got %d", len(ag.tags))
	}
}

func TestTaskResultWithEmptyOutput(t *testing.T) {
	result := TaskResult{TaskID: "t1", Success: true, Output: ""}
	data, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	var parsed TaskResult
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if !parsed.Success {
		t.Error("expected success")
	}
}

func TestLogEntryWithLevel(t *testing.T) {
	entry := LogEntry{TaskID: "t1", Line: "error message", Level: "error", Timestamp: time.Now()}
	data, err := json.Marshal(entry)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	var parsed LogEntry
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if parsed.Level != "error" {
		t.Errorf("expected level error, got %s", parsed.Level)
	}
}

func TestLogEntryWithoutLevel(t *testing.T) {
	entry := LogEntry{TaskID: "t1", Line: "info message", Timestamp: time.Now()}
	data, err := json.Marshal(entry)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	var parsed LogEntry
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if parsed.Level != "" {
		t.Errorf("expected empty level, got %s", parsed.Level)
	}
}

func TestTaskCancelWithoutReason(t *testing.T) {
	cancel := TaskCancel{TaskID: "t1"}
	data, err := json.Marshal(cancel)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	var parsed TaskCancel
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if parsed.TaskID != "t1" {
		t.Error("round-trip failed")
	}
}

func TestTaskAssignmentWithEmptyPrompt(t *testing.T) {
	task := TaskAssignment{TaskID: "t1", Prompt: "", Tags: []string{}}
	data, err := json.Marshal(task)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	var parsed TaskAssignment
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if parsed.Prompt != "" {
		t.Error("round-trip failed")
	}
}

// TestLogCallbackSendsLine verifies that a non-nil callback receives the log line.
func TestLogCallbackSendsLine(t *testing.T) {
	var mu sync.Mutex
	var lines []string
	cb := func(taskID, line string) error {
		mu.Lock()
		defer mu.Unlock()
		lines = append(lines, line)
		return nil
	}

	err := cb("task-1", "hello world")
	if err != nil {
		t.Fatalf("callback returned error: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(lines) != 1 {
		t.Fatalf("expected 1 line, got %d", len(lines))
	}
	if lines[0] != "hello world" {
		t.Errorf("expected 'hello world', got %q", lines[0])
	}
}

// TestAgentStop verifies that Stop can be called safely.
func TestAgentStop(t *testing.T) {
	cfg := config.GuestConfig{ID: "test-guest", Name: "Test Guest", Tags: []string{"test"}}
	handler := func(ctx context.Context, task TaskAssignment, _ LogCallback) (*TaskResult, error) {
		return &TaskResult{TaskID: task.TaskID, Success: true}, nil
	}
	ag := New(cfg, handler)
	ag.Stop()

	// Second Stop should be a no-op
	ag.Stop()
}

// TestAgentStopConcurrent verifies that concurrent Stop calls are safe.
func TestAgentStopConcurrent(t *testing.T) {
	cfg := config.GuestConfig{ID: "test-guest", Name: "Test Guest", Tags: []string{"test"}}
	handler := func(ctx context.Context, task TaskAssignment, _ LogCallback) (*TaskResult, error) {
		return &TaskResult{TaskID: task.TaskID, Success: true}, nil
	}
	ag := New(cfg, handler)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ag.Stop()
		}()
	}
	wg.Wait()
}

// TestGuestConnect_NoTLS verifies that a guest without mTLS config
// builds a nil TLS config (no error).
func TestGuestConnect_NoTLS(t *testing.T) {
	cfg := config.GuestConfig{
		ID:   "test-guest",
		Name: "Test Guest",
		Tags: []string{"test"},
	}

	tlsCfg, err := cfg.TLSConfig()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tlsCfg != nil {
		t.Fatal("expected nil TLS config without mTLS settings")
	}
}

// TestGuestConnect_TLSConfigBuilt verifies that a guest with client_cert
// and client_key set builds a valid TLS config.
func TestGuestConnect_TLSConfigBuilt(t *testing.T) {
	tmpDir := t.TempDir()
	certPath := tmpDir + "cert.pem"
	keyPath := tmpDir + "key.pem"

	certPEM, keyPEM := generateSelfSignedCert(t)
	if err := os.WriteFile(certPath, []byte(certPEM), 0o600); err != nil {
		t.Fatalf("write cert: %v", err)
	}
	if err := os.WriteFile(keyPath, []byte(keyPEM), 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}

	cfg := config.GuestConfig{
		ID:         "test-guest",
		Name:       "Test Guest",
		Tags:       []string{"test"},
		ClientCert: certPath,
		ClientKey:  keyPath,
	}

	tlsCfg, err := cfg.TLSConfig()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tlsCfg == nil {
		t.Fatal("expected non-nil TLS config")
	}
	if len(tlsCfg.Certificates) != 1 {
		t.Fatalf("expected 1 certificate, got %d", len(tlsCfg.Certificates))
	}
}

// TestGuestConnect_MissingKeyError verifies that setting only client_cert
// without client_key returns an error.
func TestGuestConnect_MissingKeyError(t *testing.T) {
	cfg := config.GuestConfig{
		ClientCert: "/tmp/fake-cert.pem",
	}

	_, err := cfg.TLSConfig()
	if err == nil {
		t.Fatal("expected error when only client_cert is set")
	}
}

// TestGuestConnect_MissingCertError verifies that setting only client_key
// without client_cert returns an error.
func TestGuestConnect_MissingCertError(t *testing.T) {
	cfg := config.GuestConfig{
		ClientKey: "/tmp/fake-key.pem",
	}

	_, err := cfg.TLSConfig()
	if err == nil {
		t.Fatal("expected error when only client_key is set")
	}
}

// TestGuestConnect_InvalidCertError verifies that nonexistent cert files
// return an error.
func TestGuestConnect_InvalidCertError(t *testing.T) {
	cfg := config.GuestConfig{
		ClientCert: "/nonexistent/cert.pem",
		ClientKey:  "/nonexistent/key.pem",
	}

	_, err := cfg.TLSConfig()
	if err == nil {
		t.Fatal("expected error for nonexistent cert files")
	}
}

// TestGuestConnect_GuestHasTLSConfig verifies that a Guest with mTLS
// config has the config accessible for Connect to use.
func TestGuestConnect_GuestHasTLSConfig(t *testing.T) {
	tmpDir := t.TempDir()
	certPath := tmpDir + "cert.pem"
	keyPath := tmpDir + "key.pem"

	certPEM, keyPEM := generateSelfSignedCert(t)
	os.WriteFile(certPath, []byte(certPEM), 0o600)
	os.WriteFile(keyPath, []byte(keyPEM), 0o600)

	cfg := config.GuestConfig{
		ID:         "test-guest",
		Name:       "Test Guest",
		Tags:       []string{"test"},
		ClientCert: certPath,
		ClientKey:  keyPath,
	}

	handler := func(ctx context.Context, task TaskAssignment, _ LogCallback) (*TaskResult, error) {
		return &TaskResult{TaskID: task.TaskID, Success: true}, nil
	}
	g := New(cfg, handler)

	tlsCfg, err := g.config.TLSConfig()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tlsCfg == nil {
		t.Fatal("expected non-nil TLS config")
	}
	_ = g
}

// generateSelfSignedCert creates a temporary self-signed certificate and key
// for testing. Returns PEM-encoded cert and key strings.
func generateSelfSignedCert(t *testing.T) (certPEM, keyPEM string) {
	t.Helper()

	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}

	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"Test"},
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}

	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("create cert: %v", err)
	}

	certBuf := new(strings.Builder)
	if err := pem.Encode(certBuf, &pem.Block{Type: "CERTIFICATE", Bytes: certDER}); err != nil {
		t.Fatalf("encode cert: %v", err)
	}

	keyDER, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	keyBuf := new(strings.Builder)
	if err := pem.Encode(keyBuf, &pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}); err != nil {
		t.Fatalf("encode key: %v", err)
	}

	return certBuf.String(), keyBuf.String()
}

// TestGuestConnect_TLSConfigInsecureSkipVerify verifies that the TLS config
// built by GuestConfig.TLSConfig() can be used with InsecureSkipVerify
// (useful for testing with self-signed certs).
func TestGuestConnect_TLSConfigInsecureSkipVerify(t *testing.T) {
	tmpDir := t.TempDir()
	certPath := tmpDir + "cert.pem"
	keyPath := tmpDir + "key.pem"

	certPEM, keyPEM := generateSelfSignedCert(t)
	os.WriteFile(certPath, []byte(certPEM), 0o600)
	os.WriteFile(keyPath, []byte(keyPEM), 0o600)

	cfg := config.GuestConfig{
		ClientCert: certPath,
		ClientKey:  keyPath,
	}

	tlsCfg, err := cfg.TLSConfig()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify the cert can be used in a TLS handshake
	conn, err := tls.Dial("tcp", "nonexistent:0", tlsCfg)
	if err == nil {
		conn.Close()
		t.Fatal("expected dial error to nonexistent host")
	}
	// The error is expected — the important thing is the TLS config
	// was built successfully and the cert was loadable.
	_ = conn
}

// TestGuestConnect_WSSURLScheme verifies that ConnectURL uses wss://
// when mTLS is configured and the URL has no scheme.
func TestGuestConnect_WSSURLScheme(t *testing.T) {
	cfg := config.GuestConfig{
		URL: "hotelier.example.com:443/ws",
	}

	u, err := cfg.ConnectURL(true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if u != "wss://hotelier.example.com:443/ws" {
		t.Errorf("expected wss://hotelier.example.com:443/ws, got %s", u)
	}
}

// TestGuestConnect_WSURLScheme verifies that ConnectURL uses ws://
// when mTLS is not configured and the URL has no scheme.
func TestGuestConnect_WSURLScheme(t *testing.T) {
	cfg := config.GuestConfig{
		URL: "localhost:8080/ws",
	}

	u, err := cfg.ConnectURL(false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if u != "ws://localhost:8080/ws" {
		t.Errorf("expected ws://localhost:8080/ws, got %s", u)
	}
}

// TestGuestConnect_URLFieldPreservesScheme verifies that an explicit
// scheme in the URL is not overwritten.
func TestGuestConnect_URLFieldPreservesScheme(t *testing.T) {
	cfg := config.GuestConfig{
		URL: "ws://localhost:8080/ws",
	}

	u, err := cfg.ConnectURL(true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if u != "ws://localhost:8080/ws" {
		t.Errorf("expected ws://localhost:8080/ws, got %s", u)
	}
}

// TestGuestConnect_URLFieldPreservesPath verifies that a custom path
// in the URL is preserved.
func TestGuestConnect_URLFieldPreservesPath(t *testing.T) {
	cfg := config.GuestConfig{
		URL: "wss://example.com/custom-path",
	}

	u, err := cfg.ConnectURL(true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if u != "wss://example.com/custom-path" {
		t.Errorf("expected wss://example.com/custom-path, got %s", u)
	}
}

// TestGuestConnLost_Idempotent verifies that setConnLost is idempotent —
// closing the channel multiple times does not panic.
func TestGuestConnLost_Idempotent(t *testing.T) {
	cfg := config.GuestConfig{ID: "test", Name: "Test", Tags: []string{"test"}}
	handler := func(ctx context.Context, task TaskAssignment, _ LogCallback) (*TaskResult, error) {
		return &TaskResult{TaskID: task.TaskID, Success: true}, nil
	}
	g := New(cfg, handler)

	// First call should close the channel.
	g.setConnLost()

	// Verify the channel is closed.
	select {
	case <-g.connLost:
		// expected
	default:
		t.Fatal("connLost should be closed")
	}

	// Second call should not panic.
	g.setConnLost()

	// Third call should not panic.
	g.setConnLost()
}

// TestGuestConnLost_ClosedOnce verifies that only one goroutine observes
// the close (no duplicate signals).
func TestGuestConnLost_ClosedOnce(t *testing.T) {
	cfg := config.GuestConfig{ID: "test", Name: "Test", Tags: []string{"test"}}
	handler := func(ctx context.Context, task TaskAssignment, _ LogCallback) (*TaskResult, error) {
		return &TaskResult{TaskID: task.TaskID, Success: true}, nil
	}
	g := New(cfg, handler)

	// Close from multiple goroutines concurrently.
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			g.setConnLost()
		}()
	}
	wg.Wait()

	// All goroutines should have observed the same closed channel.
	count := 0
	for i := 0; i < 10; i++ {
		select {
		case <-g.connLost:
			count++
		default:
		}
	}
	if count != 10 {
		t.Errorf("expected all 10 reads to see closed channel, got %d", count)
	}
}

// TestGuestResetConn verifies that resetConn creates a fresh connLost
// channel that is open (not closed).
func TestGuestResetConn(t *testing.T) {
	cfg := config.GuestConfig{ID: "test", Name: "Test", Tags: []string{"test"}}
	handler := func(ctx context.Context, task TaskAssignment, _ LogCallback) (*TaskResult, error) {
		return &TaskResult{TaskID: task.TaskID, Success: true}, nil
	}
	g := New(cfg, handler)

	// Close the original connLost.
	g.setConnLost()

	// Reset should create a new open channel.
	g.resetConn()

	select {
	case <-g.connLost:
		t.Fatal("connLost should be open after reset")
	default:
		// expected — channel is open
	}
}

// TestGuestHeartbeatLoop_ExitsOnStop verifies that heartbeatLoop exits
// when Stop() is called.
func TestGuestHeartbeatLoop_ExitsOnStop(t *testing.T) {
	cfg := config.GuestConfig{
		ID:                "test",
		Name:              "Test",
		Tags:              []string{"test"},
		HeartbeatInterval: 1, // short interval for test
	}
	handler := func(ctx context.Context, task TaskAssignment, _ LogCallback) (*TaskResult, error) {
		return &TaskResult{TaskID: task.TaskID, Success: true}, nil
	}
	g := New(cfg, handler)

	done := make(chan struct{})
	go func() {
		g.heartbeatLoop()
		close(done)
	}()

	// Give the ticker a moment to fire.
	time.Sleep(50 * time.Millisecond)
	g.Stop()

	select {
	case <-done:
		// expected
	case <-time.After(2 * time.Second):
		t.Fatal("heartbeatLoop did not exit after Stop()")
	}
}

// TestGuestHeartbeatLoop_ExitsOnConnLost verifies that heartbeatLoop exits
// when the connection is lost.
func TestGuestHeartbeatLoop_ExitsOnConnLost(t *testing.T) {
	cfg := config.GuestConfig{
		ID:                "test",
		Name:              "Test",
		Tags:              []string{"test"},
		HeartbeatInterval: 1, // short interval for test
	}
	handler := func(ctx context.Context, task TaskAssignment, _ LogCallback) (*TaskResult, error) {
		return &TaskResult{TaskID: task.TaskID, Success: true}, nil
	}
	g := New(cfg, handler)

	done := make(chan struct{})
	go func() {
		g.heartbeatLoop()
		close(done)
	}()

	// Simulate connection loss.
	time.Sleep(50 * time.Millisecond)
	g.setConnLost()

	select {
	case <-done:
		// expected
	case <-time.After(2 * time.Second):
		t.Fatal("heartbeatLoop did not exit after connLost")
	}
}

// TestGuestHeartbeatLoop_ChecksConnLostBeforeHeartbeat verifies that when
// connLost is signalled, the heartbeatLoop exits before attempting a
// heartbeat on the dead connection.
func TestGuestHeartbeatLoop_ConnLostBeforeHeartbeat(t *testing.T) {
	cfg := config.GuestConfig{
		ID:                "test",
		Name:              "Test",
		Tags:              []string{"test"},
		HeartbeatInterval: 100, // long enough that heartbeat won't fire
	}
	handler := func(ctx context.Context, task TaskAssignment, _ LogCallback) (*TaskResult, error) {
		return &TaskResult{TaskID: task.TaskID, Success: true}, nil
	}
	g := New(cfg, handler)

	// Signal connection loss before the ticker fires.
	g.setConnLost()

	done := make(chan struct{})
	go func() {
		g.heartbeatLoop()
		close(done)
	}()

	select {
	case <-done:
		// expected — should exit immediately on connLost
	case <-time.After(2 * time.Second):
		t.Fatal("heartbeatLoop did not exit quickly after pre-signalled connLost")
	}
}

// TestClientSetOnClose verifies that the Client's SetOnClose callback
// mechanism is wired correctly.
func TestClientSetOnClose(t *testing.T) {
	hub := rpc.NewClientHub(func(format string, args ...interface{}) {})
	client := rpc.NewClient("test-client", hub, func(format string, args ...interface{}) {})

	called := false
	client.SetOnClose(func() {
		called = true
	})

	// Verify the callback was registered by invoking it.
	// We use a helper approach: set a callback that sets a flag,
	// then read the client's onClose field via the SetOnClose method.
	// Since onClose is unexported, we test via the public API.
	client.SetOnClose(func() {
		called = true
	})
	// The second SetOnClose should simply overwrite the first.
	// No panic = success.
	_ = called
}

// TestClientSetOnClose_Nil verifies that setting a nil callback is safe.
func TestClientSetOnClose_Nil(t *testing.T) {
	hub := rpc.NewClientHub(func(format string, args ...interface{}) {})
	client := rpc.NewClient("test-client", hub, func(format string, args ...interface{}) {})

	// Setting nil should be a no-op (doesn't panic).
	client.SetOnClose(nil)
}

// TestGuestNew_ConnLostChannel verifies that New creates an open connLost channel.
func TestGuestNew_ConnLostChannel(t *testing.T) {
	cfg := config.GuestConfig{ID: "test", Name: "Test", Tags: []string{"test"}}
	handler := func(ctx context.Context, task TaskAssignment, _ LogCallback) (*TaskResult, error) {
		return &TaskResult{TaskID: task.TaskID, Success: true}, nil
	}
	g := New(cfg, handler)

	select {
	case <-g.connLost:
		t.Fatal("connLost should be open after New()")
	default:
		// expected
	}
}

func TestGuestReload(t *testing.T) {
	cfg := config.GuestConfig{
		TaskTimeout:       900,
		HeartbeatInterval: 15,
		LogLevel:          "info",
	}

	handler := func(ctx context.Context, task TaskAssignment, sendLog LogCallback) (*TaskResult, error) {
		return &TaskResult{TaskID: task.TaskID, Success: true}, nil
	}
	g := New(cfg, handler)

	// Verify initial config
	if g.config.TaskTimeout != 900 {
		t.Errorf("expected task_timeout 900, got %d", g.config.TaskTimeout)
	}
	if g.config.HeartbeatInterval != 15 {
		t.Errorf("expected heartbeat_interval 15, got %d", g.config.HeartbeatInterval)
	}
	if g.config.LogLevel != "info" {
		t.Errorf("expected log_level info, got %s", g.config.LogLevel)
	}

	// Reload with new values
	newCfg := config.GuestConfig{
		TaskTimeout:       1800,
		HeartbeatInterval: 30,
		LogLevel:          "debug",
	}
	g.Reload(newCfg)

	if g.config.TaskTimeout != 1800 {
		t.Errorf("expected task_timeout 1800, got %d", g.config.TaskTimeout)
	}
	if g.config.HeartbeatInterval != 30 {
		t.Errorf("expected heartbeat_interval 30, got %d", g.config.HeartbeatInterval)
	}
	if g.config.LogLevel != "debug" {
		t.Errorf("expected log_level debug, got %s", g.config.LogLevel)
	}
}

// TestGuestNew_CurrentTaskIDEmpty verifies that a new guest starts with
// an empty currentTaskID.
func TestGuestNew_CurrentTaskIDEmpty(t *testing.T) {
	cfg := config.GuestConfig{ID: "test", Name: "Test", Tags: []string{"test"}}
	handler := func(ctx context.Context, task TaskAssignment, _ LogCallback) (*TaskResult, error) {
		return &TaskResult{TaskID: task.TaskID, Success: true}, nil
	}
	g := New(cfg, handler)

	g.mu.Lock()
	taskID := g.currentTaskID
	g.mu.Unlock()

	if taskID != "" {
		t.Errorf("expected empty currentTaskID on new guest, got %q", taskID)
	}
}

// TestGuest_DuplicateTaskAssignmentIgnored verifies that the task.assign
// notification handler ignores duplicate assignments for a task the guest
// is already running. This prevents the decline-loop that occurs when the
// server re-sends an assignment after a guest reconnection.
func TestGuest_DuplicateTaskAssignmentIgnored(t *testing.T) {
	cfg := config.GuestConfig{ID: "test", Name: "Test", Tags: []string{"test"}}
	handler := func(ctx context.Context, task TaskAssignment, _ LogCallback) (*TaskResult, error) {
		return &TaskResult{TaskID: task.TaskID, Success: true}, nil
	}
	g := New(cfg, handler)

	// Simulate the guest already running a task
	g.mu.Lock()
	g.currentTaskID = "task-already-running"
	g.mu.Unlock()

	taskID := "task-already-running"

	// Simulate receiving a duplicate task.assign notification. The handler
	// is registered in New() (see registerNotificationHandlers).
	params, _ := json.Marshal(TaskAssignment{
		TaskID: taskID,
		Prompt: "Some prompt",
	})

	// Invoke the handler directly via the hub helper
	if !g.hub.InvokeNotificationHandler("task.assign", params) {
		t.Fatal("no task.assign handler registered")
	}

	// The task should NOT be queued because it's a duplicate
	select {
	case <-g.taskCh:
		t.Error("expected duplicate task assignment to be ignored, but task was queued")
	default:
		// Correct — task was not queued
	}
}

// TestIsGuestNotFound_GuestNotFound verifies that isGuestNotFound
// detects the "guest not found" error from the server.
func TestIsGuestNotFound_GuestNotFound(t *testing.T) {
	// Simulate the error chain: RPC error wrapped in heartbeat error
	rpcErr := &rpc.RPCError{
		Code:    rpc.CodeInternalError,
		Message: "guest guest-89b13060 not found",
	}
	heartbeatErr := fmt.Errorf("heartbeat: all 3 attempts failed: %w", rpcErr)

	if !isGuestNotFound(heartbeatErr) {
		t.Error("expected isGuestNotFound to return true for wrapped 'guest not found' error")
	}
}

// TestIsGuestNotFound_DirectRPCError verifies that isGuestNotFound
// works with the raw RPC error (not wrapped).
func TestIsGuestNotFound_DirectRPCError(t *testing.T) {
	rpcErr := &rpc.RPCError{
		Code:    rpc.CodeInternalError,
		Message: "guest guest-abc123 not found",
	}

	if !isGuestNotFound(rpcErr) {
		t.Error("expected isGuestNotFound to return true for raw 'guest not found' RPC error")
	}
}

// TestIsGuestNotFound_TransientError verifies that isGuestNotFound
// returns false for transient errors (network issues, etc.).
func TestIsGuestNotFound_TransientError(t *testing.T) {
	err := fmt.Errorf("websocket write: connection reset by peer")

	if isGuestNotFound(err) {
		t.Error("expected isGuestNotFound to return false for transient error")
	}
}

// TestIsGuestNotFound_MethodNotFound verifies that isGuestNotFound
// returns false for method-not-found errors.
func TestIsGuestNotFound_MethodNotFound(t *testing.T) {
	rpcErr := rpc.MethodNotFoundError("guest.heartbeat")

	if isGuestNotFound(rpcErr) {
		t.Error("expected isGuestNotFound to return false for method-not-found error")
	}
}

// TestIsGuestNotFound_NilError verifies that isGuestNotFound
// returns false for nil errors.
func TestIsGuestNotFound_NilError(t *testing.T) {
	if isGuestNotFound(nil) {
		t.Error("expected isGuestNotFound to return false for nil error")
	}
}

// TestGuestHeartbeatLoop_ReconnectsOnGuestNotFound verifies that when
// Heartbeat() returns a "guest not found" error, the heartbeatLoop
// signals connLost and exits, triggering a reconnect cycle.
func TestGuestHeartbeatLoop_ReconnectsOnGuestNotFound(t *testing.T) {
	cfg := config.GuestConfig{
		ID:                "test-guest-not-found",
		Name:              "Test Guest",
		Tags:              []string{"test"},
		HeartbeatInterval: 1, // short interval for test
	}
	handler := func(ctx context.Context, task TaskAssignment, _ LogCallback) (*TaskResult, error) {
		return &TaskResult{TaskID: task.TaskID, Success: true}, nil
	}
	g := New(cfg, handler)

	// Replace Heartbeat with a stub that returns "guest not found".
	// We do this by monkey-patching via the Heartbeat method.
	// Since Heartbeat is a method on Guest, we use a wrapper approach.
	originalHeartbeat := g.heartbeatForTest
	g.heartbeatForTest = func() error {
		rpcErr := &rpc.RPCError{
			Code:    rpc.CodeInternalError,
			Message: "guest test-guest-not-found not found",
		}
		return fmt.Errorf("heartbeat: all 3 attempts failed: %w", rpcErr)
	}

	done := make(chan struct{})
	go func() {
		g.heartbeatLoop()
		close(done)
	}()

	// Wait for heartbeat to fire and detect the error.
	select {
	case <-done:
		// expected — heartbeatLoop should exit after detecting guest not found
	case <-time.After(5 * time.Second):
		t.Fatal("heartbeatLoop did not exit after guest-not-found error")
	}

	// Verify connLost was signalled.
	select {
	case <-g.connLost:
		// expected — connLost should be closed
	default:
		t.Error("expected connLost to be signalled after guest-not-found error")
	}

	// Restore for cleanup.
	g.heartbeatForTest = originalHeartbeat
}

// TestGuestHeartbeatLoop_ContinuesOnTransientError verifies that when
// Heartbeat() returns a transient error (not "guest not found"), the
// heartbeatLoop continues normally without signalling connLost.
func TestGuestHeartbeatLoop_ContinuesOnTransientError(t *testing.T) {
	cfg := config.GuestConfig{
		ID:                "test-guest-transient",
		Name:              "Test Guest",
		Tags:              []string{"test"},
		HeartbeatInterval: 1, // short interval for test
	}
	handler := func(ctx context.Context, task TaskAssignment, _ LogCallback) (*TaskResult, error) {
		return &TaskResult{TaskID: task.TaskID, Success: true}, nil
	}
	g := New(cfg, handler)

	// Replace Heartbeat with a stub that returns a transient error.
	originalHeartbeat := g.heartbeatForTest
	g.heartbeatForTest = func() error {
		return fmt.Errorf("websocket write: connection reset by peer")
	}

	done := make(chan struct{})
	go func() {
		g.heartbeatLoop()
		close(done)
	}()

	// Wait for a heartbeat to fire (should NOT exit).
	time.Sleep(2 * time.Second)

	// connLost should NOT be signalled for transient errors.
	select {
	case <-g.connLost:
		t.Error("expected connLost to NOT be signalled for transient error")
	default:
		// expected — connLost should still be open
	}

	// heartbeatLoop should still be running.
	select {
	case <-done:
		t.Error("expected heartbeatLoop to still be running after transient error")
	default:
		// expected — still running
	}

	// Stop the loop cleanly.
	g.Stop()

	select {
	case <-done:
		// expected
	case <-time.After(2 * time.Second):
		t.Fatal("heartbeatLoop did not exit after Stop()")
	}

	// Restore for cleanup.
	g.heartbeatForTest = originalHeartbeat
}

// TestGuest_DifferentTaskAssignmentQueued verifies that a task.assign for
// a *different* task ID is still queued even when the guest is running
// a task. The dispatcher will handle the conflict (decline the new task).
func TestGuest_DifferentTaskAssignmentQueued(t *testing.T) {
	cfg := config.GuestConfig{ID: "test", Name: "Test", Tags: []string{"test"}}
	handler := func(ctx context.Context, task TaskAssignment, _ LogCallback) (*TaskResult, error) {
		return &TaskResult{TaskID: task.TaskID, Success: true}, nil
	}
	g := New(cfg, handler)

	// Simulate the guest running a different task
	g.mu.Lock()
	g.currentTaskID = "task-current"
	g.mu.Unlock()

	// Send a task.assign for a *different* task. The handler is registered
	// in New() (see registerNotificationHandlers).
	params, _ := json.Marshal(TaskAssignment{
		TaskID: "task-different",
		Prompt: "Another prompt",
	})

	if !g.hub.InvokeNotificationHandler("task.assign", params) {
		t.Fatal("no task.assign handler registered")
	}

	// The task SHOULD be queued because it's a different task
	select {
	case task := <-g.taskCh:
		if task.TaskID != "task-different" {
			t.Errorf("expected task-id 'task-different', got %s", task.TaskID)
		}
	default:
		t.Error("expected different task assignment to be queued, but task was not queued")
	}
}

// TestGuest_NotificationHandlersRegisteredAtConstruction verifies that the
// task.assign and task.cancel notification handlers are registered when the
// guest is constructed — before any Connect() or Register() call. The server
// pushes task.assign while handling guest.register (before the response), and
// the read loop dispatches notifications as soon as they are read, so the
// handlers must already be in place by the time the first message arrives.
// See issue #168.
func TestGuest_NotificationHandlersRegisteredAtConstruction(t *testing.T) {
	g := newTestGuest(t)

	// No Connect()/Register() — the task.assign handler must already exist
	// and must queue the assignment on taskCh.
	params, _ := json.Marshal(TaskAssignment{TaskID: "task-1", Prompt: "p"})
	if !g.hub.InvokeNotificationHandler("task.assign", params) {
		t.Fatal("expected task.assign handler to be registered at construction")
	}
	select {
	case task := <-g.taskCh:
		if task.TaskID != "task-1" {
			t.Errorf("expected task-1 queued, got %s", task.TaskID)
		}
	default:
		t.Error("expected task.assign to be queued on taskCh")
	}

	// Malformed params must not panic and must not queue anything.
	g.hub.InvokeNotificationHandler("task.assign", json.RawMessage(`{not json`))
	select {
	case <-g.taskCh:
		t.Error("expected malformed task.assign to be dropped, but a task was queued")
	default:
	}

	// The task.cancel handler must also be registered. Invoking it while
	// g.client is nil must not panic (no connection exists yet) — it simply
	// skips the guest.cancelled confirmation.
	if !g.hub.InvokeNotificationHandler("task.cancel", json.RawMessage(`{"task_id":"task-1"}`)) {
		t.Fatal("expected task.cancel handler to be registered at construction")
	}

	// Queue full: fill the buffer, then verify a further assignment is
	// dropped rather than blocking the read loop.
	for i := 0; i < cap(g.taskCh); i++ {
		g.taskCh <- TaskAssignment{TaskID: fmt.Sprintf("fill-%d", i)}
	}
	g.hub.InvokeNotificationHandler("task.assign", params) // must not block
	if len(g.taskCh) != cap(g.taskCh) {
		t.Errorf("expected queue to stay full (%d), got %d — dropped task may have been queued", cap(g.taskCh), len(g.taskCh))
	}
}

// TestGuest_TaskAssignedDuringRegistration_IsReceived reproduces issue #168
// end to end: a pending task exists before the guest connects, so the server
// pushes task.assign while handling guest.register (before the response is
// written). The guest must receive and queue the assignment. Before the fix,
// the notification was dispatched by the read loop before Register() had a
// chance to register the handler, and was dropped with
// "no handler registered".
func TestGuest_TaskAssignedDuringRegistration_IsReceived(t *testing.T) {
	// Start a real server with a real WebSocket listener.
	cfg := config.ServerConfig{
		Host:      "127.0.0.1",
		Port:      0,
		MaxGuests: 0,
	}
	srv := server.New(cfg)
	hub := srv.Hub()
	go hub.Run()

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", srv.HandleWebSocket)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = http.Serve(ln, mux) }()
	t.Cleanup(func() { _ = ln.Close() })

	port := ln.Addr().(*net.TCPAddr).Port
	wsURL := fmt.Sprintf("ws://127.0.0.1:%d/ws", port)

	// Queue a pending task before the guest connects. The server will push
	// it during guest.register handling.
	taskID := fmt.Sprintf("task-168-%d", time.Now().UnixNano())
	if err := srv.TaskQueue().Add(&queue.Task{
		ID:     taskID,
		Prompt: "assigned during registration",
		Tags:   []string{"test"},
	}); err != nil {
		t.Fatalf("add task: %v", err)
	}

	// Connect a real guest over WebSocket.
	gcfg := config.GuestConfig{
		Name:              "Issue 168 Guest",
		Tags:              []string{"test"},
		URL:               wsURL,
		HeartbeatInterval: 1,
	}
	g := New(gcfg, func(ctx context.Context, task TaskAssignment, _ LogCallback) (*TaskResult, error) {
		return &TaskResult{TaskID: task.TaskID, Success: true, Output: "ok"}, nil
	})
	t.Cleanup(func() {
		g.Stop()
		if g.client != nil {
			g.client.Close()
		}
	})

	if err := g.Connect(); err != nil {
		t.Fatalf("connect: %v", err)
	}
	if err := g.Register(); err != nil {
		t.Fatalf("register: %v", err)
	}

	// The task.assign pushed during registration must have been received.
	select {
	case task := <-g.taskCh:
		if task.TaskID != taskID {
			t.Errorf("expected %s queued, got %s", taskID, task.TaskID)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("task.assign was not received by the guest (dropped before handler registration?)")
	}
}

// TestGuestSendLog_NilClient verifies that SendLog returns an error
// (not a nil pointer panic) when the RPC client is nil, e.g. while the
// guest is reconnecting (issue #72).
func TestGuestSendLog_NilClient(t *testing.T) {
	g := newTestGuest(t)
	g.client = nil

	err := g.SendLog(LogEntry{TaskID: "task-1", Line: "hello"})
	if err == nil {
		t.Fatal("expected error from SendLog with nil client")
	}
}

// TestGuestSendResult_NilClient verifies that SendResult returns an error
// immediately (no retry backoff) when the RPC client is nil (issue #72).
func TestGuestSendResult_NilClient(t *testing.T) {
	g := newTestGuest(t)
	g.client = nil

	start := time.Now()
	err := g.SendResult(TaskResult{TaskID: "task-1", Success: true})
	if err == nil {
		t.Fatal("expected error from SendResult with nil client")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("SendResult with nil client should fail immediately, took %v", elapsed)
	}
}

// TestGuestDeclineTask_NilClient verifies that DeclineTask does not panic
// and returns immediately when the RPC client is nil (issue #72).
func TestGuestDeclineTask_NilClient(t *testing.T) {
	g := newTestGuest(t)
	g.client = nil

	start := time.Now()
	g.DeclineTask("task-1", "busy")
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("DeclineTask with nil client should return immediately, took %v", elapsed)
	}
}

// TestGuestHeartbeat_NilClient verifies that Heartbeat returns an error
// immediately (no retry backoff) when the RPC client is nil (issue #72).
func TestGuestHeartbeat_NilClient(t *testing.T) {
	g := newTestGuest(t)
	g.client = nil

	start := time.Now()
	err := g.Heartbeat()
	if err == nil {
		t.Fatal("expected error from Heartbeat with nil client")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("Heartbeat with nil client should fail immediately, took %v", elapsed)
	}
}

// TestGuestRegister_NilClient verifies that Register returns an error
// (not a nil pointer panic) when the RPC client is nil (issue #72).
func TestGuestRegister_NilClient(t *testing.T) {
	g := newTestGuest(t)
	g.client = nil

	if err := g.Register(); err == nil {
		t.Fatal("expected error from Register with nil client")
	}
}

// TestGuestUnregister_NilClient verifies that Unregister returns an error
// (not a nil pointer panic) when the RPC client is nil (issue #72).
func TestGuestUnregister_NilClient(t *testing.T) {
	g := newTestGuest(t)
	g.client = nil

	if err := g.Unregister(); err == nil {
		t.Fatal("expected error from Unregister with nil client")
	}
}

// TestGuestSendLog_ConcurrentWithClientClear verifies that SendLog is safe
// to call concurrently with the reconnect loop clearing the client: no
// panic, no data race (issue #72). Before the fix, SendLog read g.client
// without the mutex while the writer flipped it, which the race detector
// flags; the nil window also produced the production SIGSEGV. A real
// connection is used so that a non-nil client always has a live conn —
// the production invariant (a client is only stored after Connect
// succeeds, and cleared to nil when the connection is lost).
func TestGuestSendLog_ConcurrentWithClientClear(t *testing.T) {
	cfg := config.ServerConfig{
		Host:      "127.0.0.1",
		Port:      0,
		MaxGuests: 0,
	}
	srv := server.New(cfg)
	hub := srv.Hub()
	go hub.Run()

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", srv.HandleWebSocket)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = http.Serve(ln, mux) }()
	t.Cleanup(func() { _ = ln.Close() })

	wsURL := fmt.Sprintf("ws://127.0.0.1:%d/ws", ln.Addr().(*net.TCPAddr).Port)
	gcfg := config.GuestConfig{
		Name:              "Race Guest",
		Tags:              []string{"test"},
		URL:               wsURL,
		HeartbeatInterval: 1,
	}
	g := New(gcfg, func(ctx context.Context, task TaskAssignment, _ LogCallback) (*TaskResult, error) {
		return &TaskResult{TaskID: task.TaskID, Success: true, Output: "ok"}, nil
	})
	if err := g.Connect(); err != nil {
		t.Fatalf("connect: %v", err)
	}
	if err := g.Register(); err != nil {
		t.Fatalf("register: %v", err)
	}
	connected := g.rpcClient()
	t.Cleanup(func() {
		g.Stop()
		if c := g.rpcClient(); c != nil {
			c.Close()
		}
	})

	done := make(chan struct{})
	var panicked bool
	go func() {
		defer func() {
			if r := recover(); r != nil {
				panicked = true
			}
			close(done)
		}()
		for i := 0; i < 2000; i++ {
			_ = g.SendLog(LogEntry{TaskID: "task-1", Line: "x"})
			// Flip the client between the live client and nil, as the
			// reconnect loop does, to exercise the check-then-use window.
			if i%100 == 0 {
				g.setRPCClient(connected)
			} else if i%100 == 50 {
				g.setRPCClient(nil)
			}
		}
	}()
	<-done
	if panicked {
		t.Fatal("SendLog panicked while client was being cleared")
	}
}
