package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// writeServerConfig writes content to a temp server.yaml and returns its path.
func writeServerConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "server.yaml")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

func TestDefaultServerConfig_ConcurrencyDefaults(t *testing.T) {
	cfg := DefaultServerConfig()

	if cfg.MaxConcurrentTasks != 0 {
		t.Errorf("expected max_concurrent_tasks 0 (unlimited), got %d", cfg.MaxConcurrentTasks)
	}
	if len(cfg.ConcurrencySchedule) != 0 {
		t.Errorf("expected empty concurrency_schedule, got %d entries", len(cfg.ConcurrencySchedule))
	}
	if cfg.ScheduleTimezone != "UTC" {
		t.Errorf("expected schedule_timezone UTC, got %q", cfg.ScheduleTimezone)
	}
}

func TestLoadServerConfig_ConcurrencySchedule(t *testing.T) {
	path := writeServerConfig(t, `
max_concurrent_tasks: 2
schedule_timezone: "Australia/Sydney"
concurrency_schedule:
  - at: "07:00"
    concurrency: 2
  - at: "23:00"
    concurrency: 4
`)

	cfg, err := LoadServerConfig(path)
	if err != nil {
		t.Fatalf("LoadServerConfig failed: %v", err)
	}

	if cfg.MaxConcurrentTasks != 2 {
		t.Errorf("expected max_concurrent_tasks 2, got %d", cfg.MaxConcurrentTasks)
	}
	if cfg.ScheduleTimezone != "Australia/Sydney" {
		t.Errorf("expected schedule_timezone Australia/Sydney, got %q", cfg.ScheduleTimezone)
	}
	if len(cfg.ConcurrencySchedule) != 2 {
		t.Fatalf("expected 2 schedule entries, got %d", len(cfg.ConcurrencySchedule))
	}
	if cfg.ConcurrencySchedule[0].At != "07:00" || cfg.ConcurrencySchedule[0].Concurrency != 2 {
		t.Errorf("unexpected first entry: %+v", cfg.ConcurrencySchedule[0])
	}
	if cfg.ConcurrencySchedule[1].At != "23:00" || cfg.ConcurrencySchedule[1].Concurrency != 4 {
		t.Errorf("unexpected second entry: %+v", cfg.ConcurrencySchedule[1])
	}
}

func TestLoadServerConfig_ConcurrencyValidation(t *testing.T) {
	tests := []struct {
		name    string
		body    string
		wantErr string
	}{
		{
			name:    "unparseable timezone",
			body:    "schedule_timezone: \"Not/AZone\"\n",
			wantErr: "schedule_timezone",
		},
		{
			name:    "malformed at (single-digit hour)",
			body:    "concurrency_schedule:\n  - at: \"7:00\"\n    concurrency: 2\n",
			wantErr: "at",
		},
		{
			name:    "malformed at (hour out of range)",
			body:    "concurrency_schedule:\n  - at: \"24:00\"\n    concurrency: 2\n",
			wantErr: "at",
		},
		{
			name:    "malformed at (minute out of range)",
			body:    "concurrency_schedule:\n  - at: \"07:60\"\n    concurrency: 2\n",
			wantErr: "at",
		},
		{
			name:    "malformed at (not a clock time)",
			body:    "concurrency_schedule:\n  - at: \"7am\"\n    concurrency: 2\n",
			wantErr: "at",
		},
		{
			name:    "duplicate at",
			body:    "concurrency_schedule:\n  - at: \"07:00\"\n    concurrency: 2\n  - at: \"07:00\"\n    concurrency: 4\n",
			wantErr: "duplicate",
		},
		{
			name:    "negative static cap",
			body:    "max_concurrent_tasks: -2\n",
			wantErr: "max_concurrent_tasks",
		},
		{
			name:    "negative concurrency",
			body:    "concurrency_schedule:\n  - at: \"07:00\"\n    concurrency: -1\n",
			wantErr: "concurrency",
		},
		{
			name:    "zero concurrency is allowed (full pause)",
			body:    "concurrency_schedule:\n  - at: \"07:00\"\n    concurrency: 0\n",
			wantErr: "",
		},
		{
			name:    "unsorted entries are valid",
			body:    "concurrency_schedule:\n  - at: \"23:00\"\n    concurrency: 4\n  - at: \"07:00\"\n    concurrency: 2\n",
			wantErr: "",
		},
		{
			name:    "valid schedule with timezone",
			body:    "schedule_timezone: \"Australia/Sydney\"\nconcurrency_schedule:\n  - at: \"07:00\"\n    concurrency: 2\n",
			wantErr: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := writeServerConfig(t, tt.body)
			_, err := LoadServerConfig(path)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("expected no error, got: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected error mentioning %q, got nil", tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("expected error mentioning %q, got: %v", tt.wantErr, err)
			}
		})
	}
}

// utc builds a UTC instant on 2026-06-15 (a Sydney winter day, AEST/UTC+10).
func utc(hour, minute int, day int) time.Time {
	return time.Date(2026, 6, day, hour, minute, 0, 0, time.UTC)
}

func dayNightSchedule(tz string) ServerConfig {
	return ServerConfig{
		MaxConcurrentTasks: 9,
		ScheduleTimezone:   tz,
		ConcurrencySchedule: []ConcurrencyScheduleEntry{
			{At: "07:00", Concurrency: 2},
			{At: "23:00", Concurrency: 4},
		},
	}
}

func TestEffectiveConcurrency_EmptySchedule(t *testing.T) {
	cfg := ServerConfig{MaxConcurrentTasks: 7}
	if got := cfg.EffectiveConcurrency(utc(3, 0, 15)); got != 7 {
		t.Errorf("expected static cap 7 with empty schedule, got %d", got)
	}

	unlimited := ServerConfig{}
	if got := unlimited.EffectiveConcurrency(utc(3, 0, 15)); got != 0 {
		t.Errorf("expected 0 (unlimited) with empty config, got %d", got)
	}
}

func TestEffectiveConcurrency_OnBoundary(t *testing.T) {
	cfg := dayNightSchedule("UTC")
	// Exactly 07:00: the 07:00 entry takes effect at its wall-clock time.
	if got := cfg.EffectiveConcurrency(utc(7, 0, 15)); got != 2 {
		t.Errorf("expected 2 at exactly 07:00, got %d", got)
	}
	if got := cfg.EffectiveConcurrency(utc(23, 0, 15)); got != 4 {
		t.Errorf("expected 4 at exactly 23:00, got %d", got)
	}
}

func TestEffectiveConcurrency_BeforeFirstEntry_WrapsToLastEntry(t *testing.T) {
	cfg := dayNightSchedule("UTC")
	// 03:00 is before the first entry; the previous day's last entry (23:00 -> 4)
	// holds through midnight.
	if got := cfg.EffectiveConcurrency(utc(3, 0, 15)); got != 4 {
		t.Errorf("expected 4 at 03:00 (midnight wrap), got %d", got)
	}
	// 06:59 is still before 07:00.
	if got := cfg.EffectiveConcurrency(utc(6, 59, 15)); got != 4 {
		t.Errorf("expected 4 at 06:59, got %d", got)
	}
	// 23:59 is after the last entry.
	if got := cfg.EffectiveConcurrency(utc(23, 59, 15)); got != 4 {
		t.Errorf("expected 4 at 23:59, got %d", got)
	}
	// Between the two entries.
	if got := cfg.EffectiveConcurrency(utc(12, 0, 15)); got != 2 {
		t.Errorf("expected 2 at 12:00, got %d", got)
	}
}

func TestEffectiveConcurrency_UnsortedEntries(t *testing.T) {
	cfg := ServerConfig{
		ScheduleTimezone: "UTC",
		ConcurrencySchedule: []ConcurrencyScheduleEntry{
			{At: "23:00", Concurrency: 4},
			{At: "07:00", Concurrency: 2},
		},
	}
	if got := cfg.EffectiveConcurrency(utc(12, 0, 15)); got != 2 {
		t.Errorf("expected 2 with entries listed out of order, got %d", got)
	}
}

func TestEffectiveConcurrency_SingleEntry(t *testing.T) {
	cfg := ServerConfig{
		ConcurrencySchedule: []ConcurrencyScheduleEntry{
			{At: "12:00", Concurrency: 3},
		},
	}
	for _, hour := range []int{0, 11, 12, 23} {
		if got := cfg.EffectiveConcurrency(utc(hour, 30, 15)); got != 3 {
			t.Errorf("expected 3 for single-entry schedule at %02d:30, got %d", hour, got)
		}
	}
}

func TestEffectiveConcurrency_TimezoneFollowsClockAcrossDST(t *testing.T) {
	cfg := dayNightSchedule("Australia/Sydney")

	// Sydney winter (AEST, UTC+10): 20:30 UTC is 06:30 local — before the
	// 07:00 boundary, so the previous day's 23:00 entry (4) still holds.
	winter := time.Date(2026, 7, 6, 20, 30, 0, 0, time.UTC)
	if got := cfg.EffectiveConcurrency(winter); got != 4 {
		t.Errorf("expected 4 at 20:30Z in winter (06:30 AEST), got %d", got)
	}

	// Sydney summer (AEDT, UTC+11): the same UTC wall-clock instant is
	// 07:30 local — the 07:00 entry (2) is already in force. The boundary
	// moved with the clock rather than drifting an hour.
	summer := time.Date(2026, 1, 12, 20, 30, 0, 0, time.UTC)
	if got := cfg.EffectiveConcurrency(summer); got != 2 {
		t.Errorf("expected 2 at 20:30Z in summer (07:30 AEDT), got %d", got)
	}

	// A day that crosses the AEDT->AEST transition itself: Sydney turns its
	// clocks back on the first Sunday of April, 2026-04-05 at 03:00 local.
	before := time.Date(2026, 4, 4, 15, 30, 0, 0, time.UTC) // 02:30 AEDT, DST still on
	if got := cfg.EffectiveConcurrency(before); got != 4 {
		t.Errorf("expected 4 before the DST transition (02:30 AEDT), got %d", got)
	}
	after := time.Date(2026, 4, 5, 21, 30, 0, 0, time.UTC) // 07:30 AEST, DST over
	if got := cfg.EffectiveConcurrency(after); got != 2 {
		t.Errorf("expected 2 after the DST transition (07:30 AEST), got %d", got)
	}
}

func TestEffectiveConcurrency_UnknownTimezoneFallsBackToUTC(t *testing.T) {
	cfg := dayNightSchedule("Not/AZone")
	// The resolver cannot load the zone (validation would have rejected the
	// file at load time); fall back to UTC rather than guess.
	if got := cfg.EffectiveConcurrency(utc(3, 0, 15)); got != 4 {
		t.Errorf("expected UTC fallback (4 at 03:00Z), got %d", got)
	}
}
