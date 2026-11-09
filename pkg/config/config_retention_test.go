package config

import (
	"os"
	"path/filepath"
	"testing"
)

// TestDefaultServerConfig_LogRetentionDays pins the safe-by-default stance
// of issue #263: a deployment that sets nothing still prunes after 14 days.
func TestDefaultServerConfig_LogRetentionDays(t *testing.T) {
	if got := DefaultServerConfig().LogRetentionDays; got != 14 {
		t.Errorf("default log_retention_days = %d, want 14", got)
	}
}

func TestLoadServerConfig_LogRetentionDaysOverride(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "server.yaml")
	if err := os.WriteFile(path, []byte("port: 8080\nlog_retention_days: 30\n"), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := LoadServerConfig(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.LogRetentionDays != 30 {
		t.Errorf("log_retention_days = %d, want 30", cfg.LogRetentionDays)
	}
}

// TestLoadServerConfig_LogRetentionDaysAbsentKeepsDefault and
// TestLoadServerConfig_LogRetentionDaysExplicitZeroKeep the two readings of
// the key apart: absent means the 14-day default, an explicit 0 opts out of
// pruning entirely (the max_guests convention, issue #263).
func TestLoadServerConfig_LogRetentionDaysAbsentKeepsDefault(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "server.yaml")
	if err := os.WriteFile(path, []byte("port: 8080\n"), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := LoadServerConfig(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.LogRetentionDays != 14 {
		t.Errorf("log_retention_days = %d, want default 14", cfg.LogRetentionDays)
	}
}

func TestLoadServerConfig_LogRetentionDaysExplicitZero(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "server.yaml")
	if err := os.WriteFile(path, []byte("port: 8080\nlog_retention_days: 0\n"), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := LoadServerConfig(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.LogRetentionDays != 0 {
		t.Errorf("log_retention_days = %d, want explicit 0 (pruning disabled)", cfg.LogRetentionDays)
	}
}
