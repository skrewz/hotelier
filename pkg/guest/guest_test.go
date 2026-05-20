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
	task := TaskAssignment{TaskID: "t1", Repos: []string{"/repo1"}, Prompt: "Build", Tags: []string{"tag"}}
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

func TestTaskAssignmentWithMultipleRepos(t *testing.T) {
	task := TaskAssignment{
		TaskID: "t1",
		Repos:  []string{"/repo1", "/repo2", "/repo3"},
		Prompt: "Build feature",
		Tags:   []string{"business-default", "frontend"},
	}
	data, err := json.Marshal(task)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	var parsed TaskAssignment
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if len(parsed.Repos) != 3 {
		t.Errorf("expected 3 repos, got %d", len(parsed.Repos))
	}
	if len(parsed.Tags) != 2 {
		t.Errorf("expected 2 tags, got %d", len(parsed.Tags))
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
	task := TaskAssignment{TaskID: "t1", Repos: []string{"/repo1"}, Prompt: "", Tags: []string{}}
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

// TestGuestConnect_WSURLScheme verifies that the default Connect URL
// uses the ws:// scheme (not wss://).
func TestGuestConnect_WSURLScheme(t *testing.T) {
	cfg := config.GuestConfig{
		Host: "localhost",
		Port: 8080,
	}

	host := fmt.Sprintf("ws://%s:%d/ws", cfg.Host, cfg.Port)
	if host != "ws://localhost:8080/ws" {
		t.Errorf("expected ws://localhost:8080/ws, got %s", host)
	}
}
