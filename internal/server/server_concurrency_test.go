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
	if got := srv.EffectiveConcurrency(time.Now()); got != config.ConcurrencyUnlimited {
		t.Fatalf("expected unlimited initially, got %d", got)
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

// TestServerReload_ConcurrencyPauseIsDistinctFromUnlimited checks the same
// contract through the server: an overnight entry carrying concurrency 0
// must reach the enforcement member as "start nothing", never as the
// unlimited answer an unconfigured server gives.
func TestServerReload_ConcurrencyPauseIsDistinctFromUnlimited(t *testing.T) {
	srv := New(config.ServerConfig{Host: "127.0.0.1", Port: 0})
	if got := srv.EffectiveConcurrency(time.Now()); got != config.ConcurrencyUnlimited {
		t.Fatalf("expected unlimited before the schedule is installed, got %d", got)
	}

	srv.Reload(config.ServerConfig{
		Host:             "127.0.0.1",
		Port:             0,
		ScheduleTimezone: "UTC",
		ConcurrencySchedule: []config.ConcurrencyScheduleEntry{
			{At: "07:00", Concurrency: 2},
			{At: "23:00", Concurrency: 0},
		},
	})
	if got := srv.EffectiveConcurrency(time.Date(2026, 6, 15, 23, 30, 0, 0, time.UTC)); got != 0 {
		t.Errorf("expected the 23:00 pause (0) after reload, got %d", got)
	}
	if got := srv.EffectiveConcurrency(time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)); got != 2 {
		t.Errorf("expected cap 2 by day, got %d", got)
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
	replaceFile(t, configPath, updated)

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

// TestServerReload_ConcurrencyInvalidKeepsOldKeys proves a concurrency config
// assembled in code cannot install an invalid schedule: Reload keeps the
// previous concurrency keys, the way an invalid persona keeps the old store
// (issue #248).
func TestServerReload_ConcurrencyInvalidKeepsOldKeys(t *testing.T) {
	srv := New(config.ServerConfig{
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
		t.Fatalf("expected schedule cap 2 before the reload, got %d", got)
	}

	// A schedule with a zone that cannot be loaded must not be installed.
	srv.Reload(config.ServerConfig{
		Host:               "127.0.0.1",
		Port:               0,
		MaxConcurrentTasks: 7,
		ScheduleTimezone:   "Not/AZone",
		ConcurrencySchedule: []config.ConcurrencyScheduleEntry{
			{At: "03:00", Concurrency: 9},
		},
	})
	if got := srv.EffectiveConcurrency(noon); got != 2 {
		t.Errorf("expected the previous schedule (cap 2) to survive an invalid reload, got %d", got)
	}

	// Nor may one with a malformed "at".
	srv.Reload(config.ServerConfig{
		Host:               "127.0.0.1",
		Port:               0,
		MaxConcurrentTasks: 7,
		ScheduleTimezone:   "UTC",
		ConcurrencySchedule: []config.ConcurrencyScheduleEntry{
			{At: "3:00", Concurrency: 9},
		},
	})
	if got := srv.EffectiveConcurrency(noon); got != 2 {
		t.Errorf("expected the previous schedule (cap 2) to survive a malformed at, got %d", got)
	}
	if got := srv.EffectiveConcurrency(time.Date(2026, 6, 15, 3, 0, 0, 0, time.UTC)); got != 4 {
		t.Errorf("expected the previous schedule's midnight wrap (cap 4) to survive, got %d", got)
	}
}

// TestServerReload_ConcurrencyInvalid_WatcherKeepsLastGoodConfig backs the
// README claim that a bad edit is rejected and the running server keeps the
// last good configuration (issue #248).
func TestServerReload_ConcurrencyInvalid_WatcherKeepsLastGoodConfig(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "server.yaml")
	initial := "host: \"127.0.0.1\"\nport: 0\nmax_concurrent_tasks: 3\n"
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

	if got := srv.EffectiveConcurrency(time.Now()); got != 3 {
		t.Fatalf("expected static cap 3 initially, got %d", got)
	}

	updateCh := make(chan interface{}, 4)
	go w.Run(updateCh)

	// A schedule whose "at" is malformed: the watcher must reject the file
	// rather than hand the bad config to Reload.
	bad := initial + "concurrency_schedule:\n  - at: \"25:00\"\n    concurrency: 8\n"
	replaceFile(t, configPath, bad)

	select {
	case newCfg := <-updateCh:
		srv.Reload(newCfg.(config.ServerConfig))
		t.Fatalf("watcher delivered an invalid config (cap %d)", srv.EffectiveConcurrency(time.Now()))
	case <-time.After(time.Second):
	}

	if got := srv.EffectiveConcurrency(time.Now()); got != 3 {
		t.Errorf("expected the server to keep the last good cap 3, got %d", got)
	}
}

// replaceFile edits path by rename, the way the editors the watcher is built
// for do (it watches the containing directory precisely so that rename-based
// edits are seen). Writing in place would let the watcher observe the
// truncated file as an intermediate state, which parses as an empty config
// and reaches Reload as a valid one.
func replaceFile(t *testing.T, path, content string) {
	t.Helper()
	tmp := filepath.Join(filepath.Dir(path), ".staged-"+filepath.Base(path))
	if err := os.WriteFile(tmp, []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", tmp, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		t.Fatalf("rename into %s: %v", path, err)
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
