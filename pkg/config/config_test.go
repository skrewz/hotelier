package config

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDefaultServerConfig(t *testing.T) {
	cfg := DefaultServerConfig()

	if cfg.Host != "0.0.0.0" {
		t.Errorf("expected host 0.0.0.0, got %s", cfg.Host)
	}
	if cfg.Port != 8080 {
		t.Errorf("expected port 8080, got %d", cfg.Port)
	}
	if cfg.ReadTimeout != 30 {
		t.Errorf("expected read_timeout 30, got %d", cfg.ReadTimeout)
	}
	if cfg.WriteTimeout != 30 {
		t.Errorf("expected write_timeout 30, got %d", cfg.WriteTimeout)
	}
	if cfg.MaxLogSize != 1024*1024 {
		t.Errorf("expected max_log_size 1048576, got %d", cfg.MaxLogSize)
	}
	if cfg.TaskTimeout != 3600 {
		t.Errorf("expected task_timeout 3600, got %d", cfg.TaskTimeout)
	}
	if cfg.HeartbeatInterval != 30 {
		t.Errorf("expected heartbeat_interval 30, got %d", cfg.HeartbeatInterval)
	}
	if cfg.SilenceTimeout != 1800 {
		t.Errorf("expected silence_timeout 1800, got %d", cfg.SilenceTimeout)
	}
	if cfg.MaxAgents != 0 {
		t.Errorf("expected max_agents 0, got %d", cfg.MaxAgents)
	}
}

func TestDefaultAgentConfig(t *testing.T) {
	cfg := DefaultAgentConfig()

	if cfg.Host != "localhost" {
		t.Errorf("expected host localhost, got %s", cfg.Host)
	}
	if cfg.Port != 8080 {
		t.Errorf("expected port 8080, got %d", cfg.Port)
	}
	if cfg.TaskTimeout != 0 {
		t.Errorf("expected task_timeout 0, got %d", cfg.TaskTimeout)
	}
	if cfg.LogLevel != "info" {
		t.Errorf("expected log_level info, got %s", cfg.LogLevel)
	}
}

func TestLoadServerConfig(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "server.yaml")

	content := `
host: "127.0.0.1"
port: 9090
read_timeout: 60
write_timeout: 60
max_log_size: 2097152
task_timeout: 7200
heartbeat_interval: 15
max_agents: 10
`
	if err := os.WriteFile(configPath, []byte(content), 0o644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	cfg, err := LoadServerConfig(configPath)
	if err != nil {
		t.Fatalf("LoadServerConfig failed: %v", err)
	}

	if cfg.Host != "127.0.0.1" {
		t.Errorf("expected host 127.0.0.1, got %s", cfg.Host)
	}
	if cfg.Port != 9090 {
		t.Errorf("expected port 9090, got %d", cfg.Port)
	}
	if cfg.ReadTimeout != 60 {
		t.Errorf("expected read_timeout 60, got %d", cfg.ReadTimeout)
	}
	if cfg.WriteTimeout != 60 {
		t.Errorf("expected write_timeout 60, got %d", cfg.WriteTimeout)
	}
	if cfg.MaxLogSize != 2097152 {
		t.Errorf("expected max_log_size 2097152, got %d", cfg.MaxLogSize)
	}
	if cfg.TaskTimeout != 7200 {
		t.Errorf("expected task_timeout 7200, got %d", cfg.TaskTimeout)
	}
	if cfg.HeartbeatInterval != 15 {
		t.Errorf("expected heartbeat_interval 15, got %d", cfg.HeartbeatInterval)
	}
	if cfg.MaxAgents != 10 {
		t.Errorf("expected max_agents 10, got %d", cfg.MaxAgents)
	}
}

func TestLoadServerConfigNotExists(t *testing.T) {
	_, err := LoadServerConfig("/nonexistent/path/config.yaml")
	if err == nil {
		t.Error("expected error for nonexistent file, got nil")
	}
}

func TestLoadServerConfigInvalidYAML(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "invalid.yaml")

	content := `
host: "127.0.0.1"
invalid: [unclosed
`
	if err := os.WriteFile(configPath, []byte(content), 0o644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	_, err := LoadServerConfig(configPath)
	if err == nil {
		t.Error("expected error for invalid YAML, got nil")
	}
}

func TestLoadAgentConfig(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "agent.yaml")

	content := `
host: "192.168.1.100"
port: 3000
id: "test-agent-1"
name: "Test Agent"
tags:
  - "business-default"
  - "android"
task_timeout: 900
heartbeat_interval: 10
working_dir: "/tmp/test"
log_level: "debug"
`
	if err := os.WriteFile(configPath, []byte(content), 0o644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	cfg, err := LoadAgentConfig(configPath)
	if err != nil {
		t.Fatalf("LoadAgentConfig failed: %v", err)
	}

	if cfg.Host != "192.168.1.100" {
		t.Errorf("expected host 192.168.1.100, got %s", cfg.Host)
	}
	if cfg.Port != 3000 {
		t.Errorf("expected port 3000, got %d", cfg.Port)
	}
	if cfg.ID != "test-agent-1" {
		t.Errorf("expected id test-agent-1, got %s", cfg.ID)
	}
	if cfg.Name != "Test Agent" {
		t.Errorf("expected name Test Agent, got %s", cfg.Name)
	}
	if len(cfg.Tags) != 2 {
		t.Errorf("expected 2 tags, got %d", len(cfg.Tags))
	}
	if cfg.Tags[0] != "business-default" {
		t.Errorf("expected first tag business-default, got %s", cfg.Tags[0])
	}
	if cfg.TaskTimeout != 900 {
		t.Errorf("expected task_timeout 900, got %d", cfg.TaskTimeout)
	}
	if cfg.HeartbeatInterval != 10 {
		t.Errorf("expected heartbeat_interval 10, got %d", cfg.HeartbeatInterval)
	}
	if cfg.WorkingDir != "/tmp/test" {
		t.Errorf("expected working_dir /tmp/test, got %s", cfg.WorkingDir)
	}
	if cfg.LogLevel != "debug" {
		t.Errorf("expected log_level debug, got %s", cfg.LogLevel)
	}
}

func TestConfigStore(t *testing.T) {
	store := NewConfigStore()

	// Test Set and Get
	store.Set("key1", "value1")
	val, ok := store.Get("key1")
	if !ok {
		t.Error("expected key1 to exist")
	}
	if val != "value1" {
		t.Errorf("expected value1, got %v", val)
	}

	// Test Get non-existent key
	_, ok = store.Get("nonexistent")
	if ok {
		t.Error("expected nonexistent key to not exist")
	}

	// Test different types
	store.Set("int_key", 42)
	val, ok = store.Get("int_key")
	if !ok {
		t.Error("expected int_key to exist")
	}
	if v, ok := val.(int); !ok || v != 42 {
		t.Errorf("expected int 42, got %v", val)
	}

	store.Set("bool_key", true)
	val, ok = store.Get("bool_key")
	if !ok {
		t.Error("expected bool_key to exist")
	}
	if v, ok := val.(bool); !ok || !v {
		t.Errorf("expected bool true, got %v", val)
	}
}

func TestConfigStoreConcurrency(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	store := NewConfigStore()
	done := make(chan bool)

	// Writer goroutines
	for i := 0; i < 10; i++ {
		go func(id int) {
			for j := 0; j < 100; j++ {
				store.Set(fmt.Sprintf("key-%d-%d", id, j), j)
			}
			done <- true
		}(i)
	}

	// Reader goroutines
	for i := 0; i < 5; i++ {
		go func() {
			for j := 0; j < 100; j++ {
				store.Get(fmt.Sprintf("key-%d-%d", rand.Intn(10), j))
			}
		}()
	}

	// Wait for all writers
	for i := 0; i < 10; i++ {
		<-done
	}
}

func TestDefaultAgentConfig_AutoClaimNextTask(t *testing.T) {
	cfg := DefaultAgentConfig()
	if cfg.AutoClaimNextTask {
		t.Error("expected AutoClaimNextTask to default to false")
	}
}

func TestLoadAgentConfig_AutoClaimNextTask(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := tmpDir + "/agent.yaml"

	data := `host: localhost
port: 9090
id: test-agent
name: Test Agent
tags:
  - test
auto_claim_next_task: true
`
	if err := os.WriteFile(configPath, []byte(data), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg, err := LoadAgentConfig(configPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	if !cfg.AutoClaimNextTask {
		t.Error("expected AutoClaimNextTask to be true from config")
	}
	if cfg.Port != 9090 {
		t.Errorf("expected port 9090, got %d", cfg.Port)
	}
}
