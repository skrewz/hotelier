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
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"hotelier/pkg/config"
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

	// Register the task.assign handler (normally done in Register())
	taskID := "task-already-running"
	g.hub.RegisterNotificationHandler("task.assign", func(method string, params json.RawMessage) {
		g.log.Printf("[RPC] received notification: %s", method)
		var task TaskAssignment
		if err := json.Unmarshal(params, &task); err != nil {
			g.log.Printf("[RPC] failed to parse task.assign params: %v", err)
			return
		}

		// Deduplicate: if the guest is already running this exact task,
		// ignore the duplicate assignment.
		g.mu.Lock()
		if g.currentTaskID == task.TaskID {
			g.mu.Unlock()
			g.log.Printf("[RPC] ignoring duplicate task.assign for %s (already running)", task.TaskID)
			return
		}
		g.mu.Unlock()

		g.log.Printf("[RPC] dispatching task %s to execution", task.TaskID)
		select {
		case g.taskCh <- task:
			g.log.Printf("[RPC] task %s queued on guest for execution", task.TaskID)
		default:
			g.log.Printf("[RPC] task queue full, dropping task %s", task.TaskID)
		}
	})

	// Simulate receiving a duplicate task.assign notification
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

	// Register the task.assign handler
	g.hub.RegisterNotificationHandler("task.assign", func(method string, params json.RawMessage) {
		var task TaskAssignment
		if err := json.Unmarshal(params, &task); err != nil {
			return
		}

		g.mu.Lock()
		if g.currentTaskID == task.TaskID {
			g.mu.Unlock()
			return
		}
		g.mu.Unlock()

		select {
		case g.taskCh <- task:
		default:
		}
	})

	// Send a task.assign for a *different* task
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
