package server

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"hotelier/pkg/config"
	"hotelier/pkg/logstore"
)

func newPruneTestServer(t *testing.T, retentionDays int) (*Server, string) {
	t.Helper()
	dir := t.TempDir()
	cfg := config.ServerConfig{
		Host:             "127.0.0.1",
		Port:             0,
		LogDir:           dir,
		LogRetentionDays: retentionDays,
	}
	s := New(cfg)
	if s.DiskLogStore() == nil {
		t.Fatal("expected disk log store to be initialized")
	}
	return s, dir
}

func appendLogOn(t *testing.T, store *logstore.LogStore, taskID string, ts time.Time) {
	t.Helper()
	if err := store.Append(logstore.Entry{TaskID: taskID, Line: "x", Timestamp: ts}); err != nil {
		t.Fatalf("append: %v", err)
	}
}

// TestPruneTaskLogsAtStartup verifies the startup sweep issue #263 asks for:
// a server whose disk log store holds an expired day drops it before it
// serves, while the current day survives.
func TestPruneTaskLogsAtStartup(t *testing.T) {
	s, dir := newPruneTestServer(t, 14)

	appendLogOn(t, s.DiskLogStore(), "old", time.Now().AddDate(0, 0, -20))
	appendLogOn(t, s.DiskLogStore(), "today", time.Now())

	s.pruneTaskLogs()

	oldDate := time.Now().AddDate(0, 0, -20).Format("2006-01-02")
	if _, err := os.Stat(filepath.Join(dir, oldDate)); !os.IsNotExist(err) {
		t.Errorf("expired day %s should have been pruned at startup (stat err = %v)", oldDate, err)
	}
	today := time.Now().Format("2006-01-02")
	if _, err := os.Stat(filepath.Join(dir, today)); err != nil {
		t.Errorf("current day %s must survive: %v", today, err)
	}
}

func TestPruneTaskLogsDisabledWithZeroRetention(t *testing.T) {
	s, dir := newPruneTestServer(t, 0)

	appendLogOn(t, s.DiskLogStore(), "old", time.Now().AddDate(0, 0, -400))

	s.pruneTaskLogs()

	oldDate := time.Now().AddDate(0, 0, -400).Format("2006-01-02")
	if _, err := os.Stat(filepath.Join(dir, oldDate)); err != nil {
		t.Errorf("log_retention_days: 0 must prune nothing: %v", err)
	}
}

// TestPruneTaskLogsNoDiskStore asserts the sweep is a no-op when log_dir is
// unset: an in-memory-only deployment has nothing on disk to prune and must
// not fail.
func TestPruneTaskLogsNoDiskStore(t *testing.T) {
	s := New(config.ServerConfig{Host: "127.0.0.1", Port: 0, LogRetentionDays: 14})
	if s.DiskLogStore() != nil {
		t.Fatal("expected no disk log store")
	}
	s.pruneTaskLogs() // must not panic
}

// TestReloadLogsRetentionChange covers the live-reload path: a new
// log_retention_days value is picked up by the next sweep without a restart.
func TestReloadPicksUpRetentionChange(t *testing.T) {
	dir := t.TempDir()
	cfg := config.ServerConfig{Host: "127.0.0.1", Port: 0, LogDir: dir, LogRetentionDays: 0}
	s := New(cfg)

	appendLogOn(t, s.DiskLogStore(), "old", time.Now().AddDate(0, 0, -30))

	s.pruneTaskLogs()
	oldDate := time.Now().AddDate(0, 0, -30).Format("2006-01-02")
	if _, err := os.Stat(filepath.Join(dir, oldDate)); err != nil {
		t.Fatalf("retention 0 must have pruned nothing: %v", err)
	}

	cfg.LogRetentionDays = 14
	s.Reload(cfg)
	s.pruneTaskLogs()

	if _, err := os.Stat(filepath.Join(dir, oldDate)); !os.IsNotExist(err) {
		t.Errorf("after reload with retention 14, expired day should be pruned (stat err = %v)", err)
	}
}
