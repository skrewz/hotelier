package logstore

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// appendOn writes one log entry for taskID on the given date, creating the
// date and task directories the real write path would create.
func appendOn(t *testing.T, s *LogStore, taskID string, date time.Time) {
	t.Helper()
	if err := s.Append(Entry{TaskID: taskID, Line: "x", Timestamp: date}); err != nil {
		t.Fatalf("append %s on %s: %v", taskID, date.Format("2006-01-02"), err)
	}
}

func TestLogStore_PruneOlderThanRemovesExpiredDates(t *testing.T) {
	s, dir := newTestLogStore(t)
	defer os.RemoveAll(dir)

	now := time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)
	appendOn(t, s, "old-task", time.Date(2026, 5, 31, 9, 0, 0, 0, time.UTC))
	appendOn(t, s, "recent-task", time.Date(2026, 6, 5, 9, 0, 0, 0, time.UTC))

	res, err := s.PruneOlderThan(now, 14)
	if err != nil {
		t.Fatalf("prune: %v", err)
	}

	if _, err := os.Stat(filepath.Join(dir, "2026-05-31")); !os.IsNotExist(err) {
		t.Errorf("expired date dir 2026-05-31 should have been removed (stat err = %v)", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "2026-06-05", "recent-task")); err != nil {
		t.Errorf("in-window date dir should remain: %v", err)
	}
	if len(res.DatesRemoved) != 1 || res.DatesRemoved[0] != "2026-05-31" {
		t.Errorf("DatesRemoved = %v, want [2026-05-31]", res.DatesRemoved)
	}
}

func TestLogStore_PruneOlderThanKeepsBoundaryAndToday(t *testing.T) {
	s, dir := newTestLogStore(t)
	defer os.RemoveAll(dir)

	now := time.Date(2026, 6, 15, 23, 59, 0, 0, time.UTC)
	// Exactly at the cutoff (today minus 14 days) — still within the window.
	appendOn(t, s, "boundary-task", time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC))
	// The current day must never be pruned, regardless of the clock time.
	appendOn(t, s, "today-task", now)

	if _, err := s.PruneOlderThan(now, 14); err != nil {
		t.Fatalf("prune: %v", err)
	}

	for _, date := range []string{"2026-06-01", "2026-06-15"} {
		if _, err := os.Stat(filepath.Join(dir, date)); err != nil {
			t.Errorf("date dir %s should be left completely alone: %v", date, err)
		}
	}
}

func TestLogStore_PruneDisabledWithZeroRetention(t *testing.T) {
	s, dir := newTestLogStore(t)
	defer os.RemoveAll(dir)

	now := time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)
	appendOn(t, s, "ancient-task", time.Date(2020, 1, 1, 9, 0, 0, 0, time.UTC))

	res, err := s.PruneOlderThan(now, 0)
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "2020-01-01")); err != nil {
		t.Errorf("retention 0 must prune nothing: %v", err)
	}
	if len(res.DatesRemoved) != 0 {
		t.Errorf("DatesRemoved = %v, want none", res.DatesRemoved)
	}
}

// TestLogStore_PruneNeverTouchesNonDateDirs covers the queue_dir acceptance
// criterion: only YYYY-MM-DD directories are candidates, so even a deployment
// that points queue_dir at the same tree as log_dir keeps its status dirs
// (pending/, running/) and any stray files untouched.
func TestLogStore_PruneNeverTouchesNonDateDirs(t *testing.T) {
	s, dir := newTestLogStore(t)
	defer os.RemoveAll(dir)

	now := time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)
	appendOn(t, s, "old-task", time.Date(2025, 1, 1, 9, 0, 0, 0, time.UTC))

	// Queue-store-like neighbours living beside the date dirs.
	for _, name := range []string{"pending", "running"} {
		if err := os.MkdirAll(filepath.Join(dir, name), 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", name, err)
		}
		if err := os.WriteFile(filepath.Join(dir, name, "task.json"), []byte("{}"), 0o644); err != nil {
			t.Fatalf("write queue entry: %v", err)
		}
	}
	stray := filepath.Join(dir, "README")
	if err := os.WriteFile(stray, []byte("keep me"), 0o644); err != nil {
		t.Fatalf("write stray file: %v", err)
	}

	if _, err := s.PruneOlderThan(now, 14); err != nil {
		t.Fatalf("prune: %v", err)
	}

	for _, name := range []string{"pending", "running"} {
		if _, err := os.Stat(filepath.Join(dir, name, "task.json")); err != nil {
			t.Errorf("queue dir %s must survive the sweep: %v", name, err)
		}
	}
	if _, err := os.Stat(stray); err != nil {
		t.Errorf("non-date file must survive the sweep: %v", err)
	}
}

func TestLogStore_PruneReportsFileCountsAndBytes(t *testing.T) {
	s, dir := newTestLogStore(t)
	defer os.RemoveAll(dir)

	now := time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)
	oldDate := time.Date(2026, 5, 20, 9, 0, 0, 0, time.UTC)
	appendOn(t, s, "task-a", oldDate)
	appendOn(t, s, "task-a", oldDate.Add(time.Minute)) // same task, second line
	appendOn(t, s, "task-b", oldDate)

	// Measure what the sweep is about to reclaim before it runs.
	var wantFiles int
	var wantBytes int64
	for _, task := range []string{"task-a", "task-b"} {
		info, err := os.Stat(filepath.Join(dir, "2026-05-20", task, "logs.jsonl.bz2"))
		if err != nil {
			t.Fatalf("stat log file: %v", err)
		}
		wantFiles++
		wantBytes += info.Size()
	}

	res, err := s.PruneOlderThan(now, 14)
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	if res.FilesRemoved != wantFiles {
		t.Errorf("FilesRemoved = %d, want %d", res.FilesRemoved, wantFiles)
	}
	if res.BytesReclaimed != wantBytes {
		t.Errorf("BytesReclaimed = %d, want %d", res.BytesReclaimed, wantBytes)
	}
	if res.FilesRemoved == 0 || res.BytesReclaimed == 0 {
		t.Error("prune of a non-empty expired dir must report nonzero files and bytes")
	}
}

func TestLogStore_PruneEmptyStoreIsNoError(t *testing.T) {
	s, dir := newTestLogStore(t)
	defer os.RemoveAll(dir)

	if _, err := s.PruneOlderThan(time.Now(), 14); err != nil {
		t.Errorf("prune of an empty store should succeed: %v", err)
	}

	// A store whose directory vanished underneath it is likewise not an error:
	// ListDates already treats a missing root as "no dates".
	if err := os.RemoveAll(dir); err != nil {
		t.Fatalf("remove store dir: %v", err)
	}
	if _, err := s.PruneOlderThan(time.Now(), 14); err != nil {
		t.Errorf("prune of a missing store dir should succeed: %v", err)
	}
}

// TestLogStore_PruneOlderThanContinuesAfterRemovalFailure covers the
// damaged-volume case: one expired directory the process cannot remove
// (permissions, an immutable attribute, a failing disk) must not freeze the
// rest of the window. The sweep keeps walking, reports the directories it
// did reclaim and surfaces the failure as an error alongside them.
func TestLogStore_PruneOlderThanContinuesAfterRemovalFailure(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("running as root: a read-only directory stays removable")
	}
	s, dir := newTestLogStore(t)
	defer os.RemoveAll(dir)

	now := time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)
	appendOn(t, s, "stuck-task", time.Date(2026, 5, 1, 9, 0, 0, 0, time.UTC))
	appendOn(t, s, "newer-task", time.Date(2026, 5, 20, 9, 0, 0, 0, time.UTC))

	// Read-only expired dir: RemoveAll cannot unlink its child task dir, so
	// this day cannot be deleted while the newer one can.
	stuck := filepath.Join(dir, "2026-05-01")
	if err := os.Chmod(stuck, 0o500); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	t.Cleanup(func() { os.Chmod(stuck, 0o755) })

	res, err := s.PruneOlderThan(now, 14)
	if err == nil {
		t.Error("prune must report the directory it could not remove")
	}
	if _, statErr := os.Stat(stuck); statErr != nil {
		t.Fatalf("unremovable dir should still exist: %v", statErr)
	}
	if _, statErr := os.Stat(filepath.Join(dir, "2026-05-20")); !os.IsNotExist(statErr) {
		t.Errorf("a newer expired dir must not be blocked by an older one (stat err = %v)", statErr)
	}
	os.Chmod(stuck, 0o755)

	// The partial result is kept, not discarded: the log line still carries
	// evidence of what the sweep did reclaim.
	if len(res.DatesRemoved) != 1 || res.DatesRemoved[0] != "2026-05-20" {
		t.Errorf("DatesRemoved = %v, want [2026-05-20]", res.DatesRemoved)
	}
	if res.FilesRemoved == 0 || res.BytesReclaimed == 0 {
		t.Errorf("removed dir must report files and bytes, got %d/%d", res.FilesRemoved, res.BytesReclaimed)
	}
}
