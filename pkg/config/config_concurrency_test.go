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
func utc(day, hour, minute int) time.Time {
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
	if got := cfg.EffectiveConcurrency(utc(15, 3, 0)); got != 7 {
		t.Errorf("expected static cap 7 with empty schedule, got %d", got)
	}

	unlimited := ServerConfig{}
	if got := unlimited.EffectiveConcurrency(utc(15, 3, 0)); got != ConcurrencyUnlimited {
		t.Errorf("expected ConcurrencyUnlimited with empty config, got %d", got)
	}
}

// TestEffectiveConcurrency_UnlimitedIsDistinctFromPause pins the contract the
// enforcement member builds on: the resolver returns a single int, so the
// "no cap" answer must not be the 0 that a schedule entry uses to pause all
// work. A consumer that read a bare 0 as unlimited would leave the overnight
// window uncapped.
func TestEffectiveConcurrency_UnlimitedIsDistinctFromPause(t *testing.T) {
	unconfigured := ServerConfig{}
	if got := unconfigured.EffectiveConcurrency(utc(15, 3, 0)); got != ConcurrencyUnlimited {
		t.Errorf("expected ConcurrencyUnlimited for an unconfigured server, got %d", got)
	}

	// The same server with a schedule entry that pauses the overnight window.
	paused := ServerConfig{
		ScheduleTimezone: "UTC",
		ConcurrencySchedule: []ConcurrencyScheduleEntry{
			{At: "07:00", Concurrency: 2},
			{At: "23:00", Concurrency: 0},
		},
	}
	if got := paused.EffectiveConcurrency(utc(15, 23, 30)); got != 0 {
		t.Errorf("expected 0 (no new work may start) from the 23:00 entry, got %d", got)
	}
	if got := paused.EffectiveConcurrency(utc(15, 12, 0)); got != 2 {
		t.Errorf("expected the 07:00 cap 2 to still apply by day, got %d", got)
	}

	// The sentinel must never be a value a config can carry, otherwise the
	// two meanings would share the channel again. Validation rejects every
	// negative, which is what keeps ConcurrencyUnlimited unrepresentable.
	if ConcurrencyUnlimited >= 0 {
		t.Errorf("ConcurrencyUnlimited must be outside the range a config can hold, got %d", ConcurrencyUnlimited)
	}
	if err := (ServerConfig{MaxConcurrentTasks: ConcurrencyUnlimited}).ValidateConcurrency(); err == nil {
		t.Error("expected the unlimited sentinel to be rejected as a configuration value")
	}
	if err := (ServerConfig{ConcurrencySchedule: []ConcurrencyScheduleEntry{{At: "07:00", Concurrency: ConcurrencyUnlimited}}}).ValidateConcurrency(); err == nil {
		t.Error("expected the unlimited sentinel to be rejected as a schedule entry cap")
	}
}

// TestEffectiveConcurrency_StaticFallbackKeepsUnlimitedMeaning: a schedule
// the resolver cannot trust falls back to max_concurrent_tasks, and that key
// means unlimited when it is 0, so the fallback resolves to the sentinel
// rather than to a pause.
func TestEffectiveConcurrency_StaticFallbackKeepsUnlimitedMeaning(t *testing.T) {
	cfg := dayNightSchedule("Not/AZone")
	cfg.MaxConcurrentTasks = 0
	if got := cfg.EffectiveConcurrency(utc(15, 3, 0)); got != ConcurrencyUnlimited {
		t.Errorf("expected an uncapped static fallback, got %d", got)
	}
}

func TestEffectiveConcurrency_OnBoundary(t *testing.T) {
	cfg := dayNightSchedule("UTC")
	// Exactly 07:00: the 07:00 entry takes effect at its wall-clock time.
	if got := cfg.EffectiveConcurrency(utc(15, 7, 0)); got != 2 {
		t.Errorf("expected 2 at exactly 07:00, got %d", got)
	}
	if got := cfg.EffectiveConcurrency(utc(15, 23, 0)); got != 4 {
		t.Errorf("expected 4 at exactly 23:00, got %d", got)
	}
}

func TestEffectiveConcurrency_BeforeFirstEntry_WrapsToLastEntry(t *testing.T) {
	cfg := dayNightSchedule("UTC")
	// 03:00 is before the first entry; the previous day's last entry (23:00 -> 4)
	// holds through midnight.
	if got := cfg.EffectiveConcurrency(utc(15, 3, 0)); got != 4 {
		t.Errorf("expected 4 at 03:00 (midnight wrap), got %d", got)
	}
	// 06:59 is still before 07:00.
	if got := cfg.EffectiveConcurrency(utc(15, 6, 59)); got != 4 {
		t.Errorf("expected 4 at 06:59, got %d", got)
	}
	// 23:59 is after the last entry.
	if got := cfg.EffectiveConcurrency(utc(15, 23, 59)); got != 4 {
		t.Errorf("expected 4 at 23:59, got %d", got)
	}
	// Between the two entries.
	if got := cfg.EffectiveConcurrency(utc(15, 12, 0)); got != 2 {
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
	if got := cfg.EffectiveConcurrency(utc(15, 12, 0)); got != 2 {
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
		if got := cfg.EffectiveConcurrency(utc(15, hour, 30)); got != 3 {
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

func TestEffectiveConcurrency_UnknownTimezoneFallsBackToStaticCap(t *testing.T) {
	cfg := dayNightSchedule("Not/AZone")
	// The zone cannot be loaded (ValidateConcurrency rejects such a config,
	// so this only happens for a config built in code). Re-interpreting the
	// schedule in UTC would shift every boundary by the zone's offset, so
	// the static cap wins instead.
	if got := cfg.EffectiveConcurrency(utc(15, 3, 0)); got != cfg.MaxConcurrentTasks {
		t.Errorf("expected static cap %d when the zone cannot be loaded, got %d", cfg.MaxConcurrentTasks, got)
	}
}

func TestEffectiveConcurrency_MalformedAtFallsBackToStaticCap(t *testing.T) {
	cfg := ServerConfig{
		MaxConcurrentTasks: 9,
		ScheduleTimezone:   "UTC",
		ConcurrencySchedule: []ConcurrencyScheduleEntry{
			{At: "07:00", Concurrency: 2},
			{At: "7:30", Concurrency: 5},
		},
	}
	// An unparsable "at" must not silently inherit a neighbouring entry's
	// cap; the static cap is the documented, obvious fallback.
	if got := cfg.EffectiveConcurrency(utc(15, 12, 0)); got != 9 {
		t.Errorf("expected static cap 9 for an unparsable at, got %d", got)
	}
}

func TestEffectiveConcurrency_ScheduleEntryInSpringForwardGap(t *testing.T) {
	cfg := ServerConfig{
		MaxConcurrentTasks: 9,
		ScheduleTimezone:   "Australia/Sydney",
		ConcurrencySchedule: []ConcurrencyScheduleEntry{
			{At: "02:30", Concurrency: 1},
			{At: "07:00", Concurrency: 2},
			{At: "23:00", Concurrency: 4},
		},
	}

	// An ordinary day: 02:30 is a normal boundary.
	if got := cfg.EffectiveConcurrency(utc(15, 16, 30)); got != 1 { // 02:30 AEST
		t.Errorf("expected 1 from the 02:30 entry on an ordinary day, got %d", got)
	}

	// Sydney springs forward on the first Sunday of October, 2026-10-04:
	// local time jumps 02:00 -> 03:00, so 02:30 does not exist that day.
	before := time.Date(2026, 10, 3, 15, 30, 0, 0, time.UTC) // 01:30 AEST
	if got := cfg.EffectiveConcurrency(before); got != 4 {
		t.Errorf("expected 4 at 01:30 AEST (previous day's 23:00 entry), got %d", got)
	}
	// The first instant after the gap is 03:00 AEDT, past the missing 02:30
	// boundary, so the entry takes effect there rather than being skipped.
	gap := time.Date(2026, 10, 3, 16, 30, 0, 0, time.UTC) // 03:30 AEDT
	if got := cfg.EffectiveConcurrency(gap); got != 1 {
		t.Errorf("expected 1 at 03:30 AEDT (02:30 entry takes effect after the gap), got %d", got)
	}
	if got := cfg.EffectiveConcurrency(time.Date(2026, 10, 3, 20, 0, 0, 0, time.UTC)); got != 2 { // 07:00 AEDT
		t.Errorf("expected 2 at 07:00 AEDT, got %d", got)
	}
}

// TestLoadLocation_Memoised guards the cache the resolver relies on: the
// enforcement member calls EffectiveConcurrency on every assignment
// decision, so a repeated lookup must not re-read the zone file.
// TestEffectiveConcurrency_DuplicateAtFirstEntryWins: ValidateConcurrency
// rejects a duplicated "at", but server.New only logs such a failure and
// installs the config anyway, so the resolver needs a deterministic winner
// for a config assembled in code. The first entry listed for that minute
// wins, whatever order the sort happens to produce.
func TestEffectiveConcurrency_DuplicateAtFirstEntryWins(t *testing.T) {
	cfg := ServerConfig{
		ScheduleTimezone: "UTC",
		ConcurrencySchedule: []ConcurrencyScheduleEntry{
			{At: "23:00", Concurrency: 4},
			{At: "07:00", Concurrency: 1},
			{At: "07:00", Concurrency: 9},
		},
	}
	for i := 0; i < 64; i++ {
		if got := cfg.EffectiveConcurrency(utc(15, 7, 30)); got != 1 {
			t.Fatalf("expected the first 07:00 entry (1) to win, got %d", got)
		}
	}
	// The other entries keep their windows.
	if got := cfg.EffectiveConcurrency(utc(15, 12, 0)); got != 1 {
		t.Errorf("expected 1 to hold until the 23:00 entry, got %d", got)
	}
	if got := cfg.EffectiveConcurrency(utc(15, 3, 0)); got != 4 {
		t.Errorf("expected 4 from the 23:00 entry through midnight, got %d", got)
	}
}

func TestLoadLocation_Memoised(t *testing.T) {
	first, err := loadLocation("Australia/Sydney")
	if err != nil {
		t.Fatalf("loadLocation: %v", err)
	}
	second, err := loadLocation("Australia/Sydney")
	if err != nil {
		t.Fatalf("loadLocation (second call): %v", err)
	}
	if first != second {
		t.Error("expected the zone to be resolved once and memoised, got two distinct locations")
	}
}

func TestValidateConcurrency_Method(t *testing.T) {
	if err := (ServerConfig{MaxConcurrentTasks: 2}).ValidateConcurrency(); err != nil {
		t.Errorf("expected a plain static cap to validate, got %v", err)
	}
	if err := (ServerConfig{ScheduleTimezone: "Not/AZone"}).ValidateConcurrency(); err == nil {
		t.Error("expected an unparseable zone to be rejected")
	}
	if err := (ServerConfig{ConcurrencySchedule: []ConcurrencyScheduleEntry{{At: "7:00", Concurrency: 2}}}).ValidateConcurrency(); err == nil {
		t.Error("expected a malformed at to be rejected")
	}
	if err := (ServerConfig{MaxConcurrentTasks: -1}).ValidateConcurrency(); err == nil {
		t.Error("expected a negative static cap to be rejected")
	}
}

// --- ConcurrencyStatus (issue #250): the cap, where it came from, what is next ---

func TestConcurrencyStatus_NamesScheduleEntryInForce(t *testing.T) {
	cfg := dayNightSchedule("UTC")

	status := cfg.ConcurrencyStatus(utc(15, 12, 0))

	if status.Cap != 2 {
		t.Errorf("expected cap 2 from the 07:00 entry, got %d", status.Cap)
	}
	if status.Source != `concurrency_schedule at "07:00"` {
		t.Errorf("expected the source to name the 07:00 entry, got %q", status.Source)
	}
	if status.NextAt() != "23:00" || status.NextCap != 4 {
		t.Errorf("expected the next change at 23:00 to 4, got %q -> %d", status.NextAt(), status.NextCap)
	}
	if want := utc(15, 23, 0); !status.NextChangeAt.Equal(want) {
		t.Errorf("expected the next change at %s, got %s", want, status.NextChangeAt)
	}
	if status.Timezone != "UTC" {
		t.Errorf("expected the timezone to be reported, got %q", status.Timezone)
	}
}

func TestConcurrencyStatus_NextChangeWrapsPastMidnight(t *testing.T) {
	cfg := dayNightSchedule("UTC")

	// 23:30 is past the last entry, so the next change is tomorrow's first.
	status := cfg.ConcurrencyStatus(utc(15, 23, 30))

	if status.Cap != 4 {
		t.Errorf("expected cap 4 after 23:00, got %d", status.Cap)
	}
	if want := utc(16, 7, 0); !status.NextChangeAt.Equal(want) {
		t.Errorf("expected the next change to be tomorrow's 07:00 (%s), got %s", want, status.NextChangeAt)
	}
	if status.NextAt() != "07:00" || status.NextCap != 2 {
		t.Errorf("expected the next change to be 07:00 -> 2, got %q -> %d", status.NextAt(), status.NextCap)
	}
}

func TestConcurrencyStatus_StaticCapHasNoNextChange(t *testing.T) {
	status := (ServerConfig{MaxConcurrentTasks: 7}).ConcurrencyStatus(utc(15, 12, 0))

	if status.Cap != 7 {
		t.Errorf("expected the static cap 7, got %d", status.Cap)
	}
	if status.Source != "max_concurrent_tasks" {
		t.Errorf("expected the source to be max_concurrent_tasks, got %q", status.Source)
	}
	if !status.NextChangeAt.IsZero() {
		t.Errorf("expected no scheduled change without a schedule, got %s", status.NextChangeAt)
	}
}

func TestConcurrencyStatus_UnlimitedIsReportedAsUnlimited(t *testing.T) {
	status := (ServerConfig{}).ConcurrencyStatus(utc(15, 12, 0))

	if status.Cap != ConcurrencyUnlimited {
		t.Errorf("expected ConcurrencyUnlimited, got %d", status.Cap)
	}
	if status.Source != "max_concurrent_tasks" {
		t.Errorf("expected the source to be max_concurrent_tasks, got %q", status.Source)
	}
}

// TestConcurrencyStatus_BoundaryThatChangesNothingIsNotAChange: the "next
// scheduled change" is a change in the number, not a tick of the clock. A
// schedule whose next entry repeats the current cap must not be reported as a
// change, or the dashboard would keep promising a boundary that does nothing.
func TestConcurrencyStatus_BoundaryThatChangesNothingIsNotAChange(t *testing.T) {
	cfg := ServerConfig{
		ScheduleTimezone: "UTC",
		ConcurrencySchedule: []ConcurrencyScheduleEntry{
			{At: "07:00", Concurrency: 2},
			{At: "23:00", Concurrency: 2},
		},
	}

	status := cfg.ConcurrencyStatus(utc(15, 12, 0))

	if status.Cap != 2 {
		t.Fatalf("expected cap 2, got %d", status.Cap)
	}
	if !status.NextChangeAt.IsZero() {
		t.Errorf("expected no change while every entry carries the same cap, got %s", status.NextChangeAt)
	}
}

func TestConcurrencyStatus_InZoneAcrossDST(t *testing.T) {
	cfg := dayNightSchedule("Australia/Sydney")

	// 20:30Z is 06:30 AEST: the 23:00 entry still holds, and the 07:00
	// boundary is half an hour away in local time.
	status := cfg.ConcurrencyStatus(time.Date(2026, 7, 6, 20, 30, 0, 0, time.UTC))

	if status.Cap != 4 {
		t.Errorf("expected cap 4 at 06:30 AEST, got %d", status.Cap)
	}
	if want := time.Date(2026, 7, 6, 21, 0, 0, 0, time.UTC); !status.NextChangeAt.Equal(want) {
		t.Errorf("expected the 07:00 AEST boundary at %s, got %s", want, status.NextChangeAt)
	}
	if status.NextAt() != "07:00" || status.NextCap != 2 {
		t.Errorf("expected 07:00 -> 2, got %q -> %d", status.NextAt(), status.NextCap)
	}
	if status.Timezone != "Australia/Sydney" {
		t.Errorf("expected the reported timezone to be Australia/Sydney, got %q", status.Timezone)
	}
}

// TestConcurrencyStatus_FallsBackLikeEffectiveConcurrency: a schedule the
// resolver cannot trust reports the static cap and no change, exactly as
// EffectiveConcurrency does — the two must never disagree.
func TestConcurrencyStatus_FallsBackLikeEffectiveConcurrency(t *testing.T) {
	for _, cfg := range []ServerConfig{
		{MaxConcurrentTasks: 5, ScheduleTimezone: "Not/AZone", ConcurrencySchedule: []ConcurrencyScheduleEntry{{At: "07:00", Concurrency: 1}}},
		{MaxConcurrentTasks: 5, ConcurrencySchedule: []ConcurrencyScheduleEntry{{At: "7am", Concurrency: 1}}},
	} {
		status := cfg.ConcurrencyStatus(utc(15, 12, 0))
		if status.Cap != cfg.EffectiveConcurrency(utc(15, 12, 0)) {
			t.Errorf("status cap %d disagrees with EffectiveConcurrency %d for %+v",
				status.Cap, cfg.EffectiveConcurrency(utc(15, 12, 0)), cfg)
		}
		if !status.NextChangeAt.IsZero() {
			t.Errorf("expected no scheduled change from an untrusted schedule, got %s", status.NextChangeAt)
		}
	}
}

// TestConcurrencyStatus_AgreesWithEffectiveConcurrencyAcrossTheDay: one
// resolution, two readers. The status surface and the enforcement path must
// never state different caps.
func TestConcurrencyStatus_AgreesWithEffectiveConcurrencyAcrossTheDay(t *testing.T) {
	cfg := dayNightSchedule("Australia/Sydney")

	start := time.Date(2026, 7, 6, 0, 0, 0, 0, time.UTC)
	for minute := 0; minute < 24*60; minute++ {
		now := start.Add(time.Duration(minute) * time.Minute)
		if got, want := cfg.ConcurrencyStatus(now).Cap, cfg.EffectiveConcurrency(now); got != want {
			t.Fatalf("at %s: status cap %d, EffectiveConcurrency %d", now, got, want)
		}
	}
}

// TestConcurrencyStatus_NextChangeIsAlwaysInTheFuture is the property the
// dashboard depends on: a reported boundary must lie ahead of the reading
// it came from, must not promise the cap already in force, must be the
// instant EffectiveConcurrency actually flips at, and — the half a
// fall-back day breaks — nothing may change before it: the enforced cap
// stays at the reading's value all the way to the reported instant.
//
// Sydney's transitions fall at 16:00Z on their UTC days, so each window
// opens at 13:00Z three hours ahead of them: the autumn day 2026-04-04
// repeats the 02:00–02:59 hour (transitioning at 16:00Z from 03:00 AEDT
// back to 02:00 AEST), the spring day 2026-10-03 removes it (02:00 AEST up
// to 03:00 AEDT, again at 16:00Z). A 27-hour window is a day, the extra
// fall-back hour, and the run-up to the transition.
//
// The schedule carries entries inside the shifted hour — including a second
// one, so on the spring day two boundaries collapse onto the transition
// instant and the tie must be broken by the enforcement rule, not by a
// sort — and entries outside it, so ordinary days are crossed too.
func TestConcurrencyStatus_NextChangeIsAlwaysInTheFuture(t *testing.T) {
	cfg := ServerConfig{
		MaxConcurrentTasks: 9,
		ScheduleTimezone:   "Australia/Sydney",
		ConcurrencySchedule: []ConcurrencyScheduleEntry{
			{At: "02:15", Concurrency: 1},
			{At: "02:45", Concurrency: 3},
			{At: "07:00", Concurrency: 2},
			{At: "23:00", Concurrency: 4},
		},
	}

	const window = 27 * 60

	for _, start := range []time.Time{
		time.Date(2026, 4, 4, 13, 0, 0, 0, time.UTC),  // across the fall-back
		time.Date(2026, 10, 3, 13, 0, 0, 0, time.UTC), // across the spring-forward
	} {
		// The enforced cap across the window plus the horizon a reported
		// next change can reach into (at most 26 hours past the reading),
		// precomputed once so the loop below compares rather than
		// re-resolves the schedule millions of times.
		horizon := make([]int, window+26*60)
		for i := range horizon {
			horizon[i] = cfg.EffectiveConcurrency(start.Add(time.Duration(i) * time.Minute))
		}

		for minute := 0; minute < window; minute++ {
			now := start.Add(time.Duration(minute) * time.Minute)
			status := cfg.ConcurrencyStatus(now)
			if !status.HasNextChange() {
				continue
			}
			if !status.NextChangeAt.After(now) {
				t.Fatalf("at %s: reported a next change in the past (%s)", now, status.NextChangeAt)
			}
			if status.NextCap == status.Cap {
				t.Fatalf("at %s: reported a change to the cap already in force (%d)", now, status.NextCap)
			}
			if got := cfg.EffectiveConcurrency(status.NextChangeAt); got != status.NextCap {
				t.Fatalf("at %s: enforcement at the reported change (%s) gives %d, not %d",
					now, status.NextChangeAt, got, status.NextCap)
			}
			at := int(status.NextChangeAt.Sub(start).Round(time.Minute) / time.Minute)
			for m := minute; m < at && m < len(horizon); m++ {
				if horizon[m] != status.Cap {
					t.Fatalf("at %s: enforcement already changed to %d at %s, before the reported change to %d at %s",
						now, horizon[m], start.Add(time.Duration(m)*time.Minute), status.NextCap, status.NextChangeAt)
				}
			}
		}
	}
}

// TestConcurrencyStatus_SpringForwardBoundaryIsQuotedAtTheTransition: a
// schedule entry inside the hour a spring-forward gap removes does not exist
// on that day's clock. resolveSchedule's wall-clock comparison flips at the
// transition itself, so that is the instant the status must report — not the
// normalised instant half an hour later, and not a minute no clock showed.
func TestConcurrencyStatus_SpringForwardBoundaryIsQuotedAtTheTransition(t *testing.T) {
	cfg := ServerConfig{
		MaxConcurrentTasks: 9,
		ScheduleTimezone:   "Australia/Sydney",
		ConcurrencySchedule: []ConcurrencyScheduleEntry{
			{At: "02:30", Concurrency: 1},
			{At: "07:00", Concurrency: 2},
			{At: "23:00", Concurrency: 4},
		},
	}

	// 01:30 AEST on the spring-forward day: the previous 23:00 entry holds.
	status := cfg.ConcurrencyStatus(time.Date(2026, 10, 3, 15, 30, 0, 0, time.UTC))
	if status.Cap != 4 {
		t.Fatalf("expected cap 4 at 01:30 AEST, got %d", status.Cap)
	}

	transition := time.Date(2026, 10, 3, 16, 0, 0, 0, time.UTC) // 03:00 AEDT
	if !status.NextChangeAt.Equal(transition) {
		t.Errorf("expected the next change at the transition (%s), got %s", transition, status.NextChangeAt)
	}
	if status.NextAt() != "03:00" {
		t.Errorf("expected the next change quoted at 03:00, the first wall clock past the boundary, got %q", status.NextAt())
	}
	if status.NextCap != 1 {
		t.Errorf("expected the next change to the 02:30 entry's cap 1, got %d", status.NextCap)
	}

	// The quote and the enforcement path agree across the boundary.
	if got := cfg.EffectiveConcurrency(transition.Add(-time.Minute)); got != status.Cap {
		t.Errorf("expected cap %d until the transition, got %d", status.Cap, got)
	}
	if got := cfg.EffectiveConcurrency(transition); got != status.NextCap {
		t.Errorf("expected cap %d from the transition, got %d", status.NextCap, got)
	}
}
