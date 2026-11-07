package server

import (
	"io"
	"log"
	"os"
	"path/filepath"
	"testing"
	"time"

	"hotelier/pkg/config"
)

func TestServerReload_ConcurrencySchedule(t *testing.T) {
	srv := newTestServer(t)

	// Initially: no schedule, no static cap -> unlimited.
	if got := srv.EffectiveConcurrency(time.Now()); got != 0 {
		t.Fatalf("expected unlimited (0) initially, got %d", got)
	}

	// Reload carrying only the static cap.
	srv.Reload(config.ServerConfig{Host: "127.0.0.1", Port: 0, MaxConcurrentTasks: 5})
	if got := srv.EffectiveConcurrency(time.Now()); got != 5 {
		t.Fatalf("expected static cap 5 after reload, got %d", got)
	}

	// Reload carrying a schedule; it must take precedence over the static cap.
	srv.Reload(config.ServerConfig{
		Host:               "127.0.0.1",
		Port:               0,
		MaxConcurrentTasks: 5,
		ScheduleTimezone:   "UTC",
		ConcurrencySchedule: []config.ConcurrencyScheduleEntry{
			{At: "07:00", Concurrency: 2},
			{At: "23:00", Concurrency: 4},
		},
	})
	noon := time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)
	if got := srv.EffectiveConcurrency(noon); got != 2 {
		t.Fatalf("expected schedule cap 2 at noon UTC, got %d", got)
	}
}

// TestServerReload_ConcurrencySchedule_Watcher proves a schedule edit takes
// effect on the watcher channel without a restart, the same path max_guests
// takes (issue #248).
func TestServerReload_ConcurrencySchedule_Watcher(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "server.yaml")
	initial := "host: \"127.0.0.1\"\nport: 0\nmax_concurrent_tasks: 1\n"
	if err := os.WriteFile(configPath, []byte(initial), 0o644); err != nil {
		t.Fatalf("write initial config: %v", err)
	}

	w, err := config.NewConfigWatcher(configPath, log.New(io.Discard, "", 0), func(path string) (interface{}, error) {
		return config.LoadServerConfig(path)
	})
	if err != nil {
		t.Fatalf("NewConfigWatcher: %v", err)
	}
	defer w.Close()

	srv := New(config.DefaultServerConfig())
	srv.Reload(mustLoad(t, configPath))

	if got := srv.EffectiveConcurrency(time.Now()); got != 1 {
		t.Fatalf("expected static cap 1 initially, got %d", got)
	}

	updateCh := make(chan interface{}, 4)
	go w.Run(updateCh)

	updated := initial + "concurrency_schedule:\n  - at: \"06:00\"\n    concurrency: 6\n"
	if err := os.WriteFile(configPath, []byte(updated), 0o644); err != nil {
		t.Fatalf("write updated config: %v", err)
	}

	select {
	case newCfg := <-updateCh:
		srv.Reload(newCfg.(config.ServerConfig))
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for config reload on the watcher channel")
	}

	noon := time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)
	if got := srv.EffectiveConcurrency(noon); got != 6 {
		t.Fatalf("expected schedule cap 6 at noon after hot reload, got %d", got)
	}
}

func mustLoad(t *testing.T, path string) config.ServerConfig {
	t.Helper()
	cfg, err := config.LoadServerConfig(path)
	if err != nil {
		t.Fatalf("LoadServerConfig: %v", err)
	}
	return cfg
}
