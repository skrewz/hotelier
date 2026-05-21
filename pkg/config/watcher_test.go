package config

import (
	"log"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func serverConfigReload(path string) (interface{}, error) {
	return LoadServerConfig(path)
}

func guestConfigReload(path string) (interface{}, error) {
	return LoadGuestConfig(path)
}

func TestConfigWatcher_ReloadsOnFileChange(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "server.yaml")

	// Initial content
	initial := `host: "127.0.0.1"
port: 9090
heartbeat_interval: 15
`
	if err := os.WriteFile(configPath, []byte(initial), 0o644); err != nil {
		t.Fatalf("write initial config: %v", err)
	}

	logger := log.Default()
	w, err := NewConfigWatcher(configPath, logger, serverConfigReload)
	if err != nil {
		t.Fatalf("NewConfigWatcher: %v", err)
	}
	defer w.Close()

	updateCh := make(chan interface{}, 10)
	go w.Run(updateCh)

	// Give the watcher time to start
	time.Sleep(100 * time.Millisecond)

	// Verify initial config (loaded by NewConfigWatcher, not sent on channel)
	cfg, err := LoadServerConfig(configPath)
	if err != nil {
		t.Fatalf("load initial config: %v", err)
	}

	if cfg.Host != "127.0.0.1" {
		t.Errorf("expected host 127.0.0.1, got %s", cfg.Host)
	}
	if cfg.Port != 9090 {
		t.Errorf("expected port 9090, got %d", cfg.Port)
	}
	if cfg.HeartbeatInterval != 15 {
		t.Errorf("expected heartbeat_interval 15, got %d", cfg.HeartbeatInterval)
	}

	// Modify the file
	updated := `host: "0.0.0.0"
port: 8080
heartbeat_interval: 30
max_guests: 5
`
	if err := os.WriteFile(configPath, []byte(updated), 0o644); err != nil {
		t.Fatalf("write updated config: %v", err)
	}

	// Wait for the reload
	var newCfg ServerConfig
	select {
	case updated := <-updateCh:
		newCfg = updated.(ServerConfig)
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for config reload")
	}

	if newCfg.Host != "0.0.0.0" {
		t.Errorf("expected host 0.0.0.0, got %s", newCfg.Host)
	}
	if newCfg.Port != 8080 {
		t.Errorf("expected port 8080, got %d", newCfg.Port)
	}
	if newCfg.HeartbeatInterval != 30 {
		t.Errorf("expected heartbeat_interval 30, got %d", newCfg.HeartbeatInterval)
	}
	if newCfg.MaxGuests != 5 {
		t.Errorf("expected max_guests 5, got %d", newCfg.MaxGuests)
	}
}

func TestConfigWatcher_SkipsDuplicateContent(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "server.yaml")

	content := `host: "127.0.0.1"
port: 9090
`
	if err := os.WriteFile(configPath, []byte(content), 0o644); err != nil {
		t.Fatalf("write initial config: %v", err)
	}

	logger := log.Default()
	w, err := NewConfigWatcher(configPath, logger, serverConfigReload)
	if err != nil {
		t.Fatalf("NewConfigWatcher: %v", err)
	}
	defer w.Close()

	updateCh := make(chan interface{}, 10)
	go w.Run(updateCh)

	// Wait a bit for the watcher to start
	time.Sleep(100 * time.Millisecond)

	// Write the same content again (simulating editor re-save)
	if err := os.WriteFile(configPath, []byte(content), 0o644); err != nil {
		t.Fatalf("rewrite same config: %v", err)
	}

	// Should NOT receive another update since content is identical
	select {
	case <-updateCh:
		t.Fatal("expected no update for identical content")
	case <-time.After(500 * time.Millisecond):
		// Good — no spurious update
	}
}

func TestConfigWatcher_GuestConfig(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "guest.yaml")

	// Initial content
	initial := `url: "wss://localhost:9090/ws"
id: "test-guest"
name: "Test Guest"
tags:
  - test
task_timeout: 900
heartbeat_interval: 10
log_level: "debug"
`
	if err := os.WriteFile(configPath, []byte(initial), 0o644); err != nil {
		t.Fatalf("write initial config: %v", err)
	}

	logger := log.Default()
	w, err := NewConfigWatcher(configPath, logger, guestConfigReload)
	if err != nil {
		t.Fatalf("NewConfigWatcher: %v", err)
	}
	defer w.Close()

	updateCh := make(chan interface{}, 10)
	go w.Run(updateCh)

	// Wait for the watcher to start
	time.Sleep(100 * time.Millisecond)

	// Verify initial config
	cfg, err := LoadGuestConfig(configPath)
	if err != nil {
		t.Fatalf("load initial config: %v", err)
	}
	if cfg.TaskTimeout != 900 {
		t.Errorf("expected task_timeout 900, got %d", cfg.TaskTimeout)
	}
	if cfg.LogLevel != "debug" {
		t.Errorf("expected log_level debug, got %s", cfg.LogLevel)
	}

	// Modify
	updated := `url: "wss://localhost:9090/ws"
id: "test-guest"
name: "Test Guest Updated"
tags:
  - test
task_timeout: 1800
heartbeat_interval: 5
log_level: "info"
`
	if err := os.WriteFile(configPath, []byte(updated), 0o644); err != nil {
		t.Fatalf("write updated config: %v", err)
	}

	var newCfg GuestConfig
	select {
	case updated := <-updateCh:
		newCfg = updated.(GuestConfig)
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for config reload")
	}

	if newCfg.Name != "Test Guest Updated" {
		t.Errorf("expected name 'Test Guest Updated', got %s", newCfg.Name)
	}
	if newCfg.TaskTimeout != 1800 {
		t.Errorf("expected task_timeout 1800, got %d", newCfg.TaskTimeout)
	}
	if newCfg.LogLevel != "info" {
		t.Errorf("expected log_level info, got %s", newCfg.LogLevel)
	}
}

func TestConfigWatcher_ConcurrentReloads(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "server.yaml")

	content := `host: "127.0.0.1"
port: 9090
`
	if err := os.WriteFile(configPath, []byte(content), 0o644); err != nil {
		t.Fatalf("write initial config: %v", err)
	}

	logger := log.Default()
	w, err := NewConfigWatcher(configPath, logger, serverConfigReload)
	if err != nil {
		t.Fatalf("NewConfigWatcher: %v", err)
	}
	defer w.Close()

	updateCh := make(chan interface{}, 100)
	go w.Run(updateCh)

	// Wait for the watcher to start
	time.Sleep(100 * time.Millisecond)

	// Rapidly write the file multiple times
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			host := "host-" + string(rune('a'+n))
			newContent := "host: \"" + host + "\"\nport: " + string(rune('0'+n)) + "\n"
			_ = os.WriteFile(configPath, []byte(newContent), 0o644)
			time.Sleep(10 * time.Millisecond)
		}(i)
	}
	wg.Wait()

	// Drain updates — we should get at least one successful reload
	timeout := time.After(3 * time.Second)
	reloads := 0
	for {
		select {
		case <-updateCh:
			reloads++
		case <-timeout:
			goto done
		}
	}
done:

	if reloads == 0 {
		t.Error("expected at least one config reload from rapid writes")
	}
}

func TestConfigWatcher_Close(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "server.yaml")

	content := `host: "127.0.0.1"
port: 9090
`
	if err := os.WriteFile(configPath, []byte(content), 0o644); err != nil {
		t.Fatalf("write initial config: %v", err)
	}

	logger := log.Default()
	w, err := NewConfigWatcher(configPath, logger, serverConfigReload)
	if err != nil {
		t.Fatalf("NewConfigWatcher: %v", err)
	}

	updateCh := make(chan interface{}, 1)
	done := make(chan struct{})
	go func() {
		w.Run(updateCh)
		close(done)
	}()

	// Close should stop the watcher
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	select {
	case <-done:
		// Good — watcher exited
	case <-time.After(2 * time.Second):
		t.Fatal("watcher did not exit after Close()")
	}
}
