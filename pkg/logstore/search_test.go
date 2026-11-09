package logstore

import (
	"strings"
	"testing"
	"time"
)

// appendLines writes one entry per line for taskID on the given date.
func appendLines(t *testing.T, s *LogStore, taskID string, day time.Time, lines []string) {
	t.Helper()
	for i, line := range lines {
		entry := Entry{
			TaskID:    taskID,
			Line:      line,
			Timestamp: day.Add(time.Duration(i) * time.Second),
		}
		if err := s.Append(entry); err != nil {
			t.Fatalf("append %q: %v", line, err)
		}
	}
}

func mustStore(t *testing.T) *LogStore {
	t.Helper()
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("new log store: %v", err)
	}
	return s
}

var (
	dayOne = time.Date(2026, 5, 10, 9, 0, 0, 0, time.UTC)
	dayTwo = time.Date(2026, 5, 11, 9, 0, 0, 0, time.UTC)
)

// TestSearchDates_FindsMatchesWithContext verifies a hit reports the task,
// date and timestamp, and carries up to two context lines either side taken
// from the same trace.
func TestSearchDates_FindsMatchesWithContext(t *testing.T) {
	s := mustStore(t)
	appendLines(t, s, "task-a", dayOne, []string{"one", "two", "THREE hit", "four", "five", "six"})

	matches, err := s.SearchDates([]string{"2026-05-10"}, "hit", 2)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(matches) != 1 {
		t.Fatalf("expected 1 match, got %d", len(matches))
	}
	m := matches[0]
	if m.TaskID != "task-a" || m.Date != "2026-05-10" {
		t.Errorf("unexpected identity: %+v", m)
	}
	if m.Line != "THREE hit" {
		t.Errorf("unexpected line %q", m.Line)
	}
	if strings.Join(m.ContextBefore, "|") != "one|two" {
		t.Errorf("unexpected before-context %v", m.ContextBefore)
	}
	if strings.Join(m.ContextAfter, "|") != "four|five" {
		t.Errorf("unexpected after-context %v", m.ContextAfter)
	}
	if !m.Timestamp.Equal(dayOne.Add(2 * time.Second)) {
		t.Errorf("unexpected timestamp %v", m.Timestamp)
	}
}

// TestSearchDates_MultipleMatchesAndFiles verifies every matching entry in
// every task of every requested date comes back, ordered by date, task ID
// and position in the trace.
func TestSearchDates_MultipleMatchesAndFiles(t *testing.T) {
	s := mustStore(t)
	appendLines(t, s, "task-b", dayTwo, []string{"needle second day"})
	appendLines(t, s, "task-a", dayTwo, []string{"first needle", "clean", "second needle"})
	appendLines(t, s, "task-a", dayOne, []string{"needle first day"})

	matches, err := s.SearchDates([]string{"2026-05-10", "2026-05-11"}, "needle", 0)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(matches) != 4 {
		t.Fatalf("expected 4 matches, got %d (%+v)", len(matches), matches)
	}
	var got []string
	for _, m := range matches {
		got = append(got, m.Date+"/"+m.TaskID+":"+m.Line)
	}
	want := []string{
		"2026-05-10/task-a:needle first day",
		"2026-05-11/task-a:first needle",
		"2026-05-11/task-a:second needle",
		"2026-05-11/task-b:needle second day",
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("match %d: want %q, got %q", i, want[i], got[i])
		}
	}
}

// TestSearchDates_OnlyScansRequestedDates verifies dates outside the window
// are never read — the caller's window restriction is honoured, not widened.
func TestSearchDates_OnlyScansRequestedDates(t *testing.T) {
	s := mustStore(t)
	appendLines(t, s, "task-a", dayOne, []string{"needle here"})
	appendLines(t, s, "task-b", dayTwo, []string{"needle here"})

	matches, err := s.SearchDates([]string{"2026-05-11"}, "needle", 0)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(matches) != 1 || matches[0].TaskID != "task-b" {
		t.Fatalf("expected only the task in the requested date, got %+v", matches)
	}
}

// TestSearchDates_CaseInsensitive verifies substring matching ignores case.
func TestSearchDates_CaseInsensitive(t *testing.T) {
	s := mustStore(t)
	appendLines(t, s, "task-a", dayOne, []string{"Deploying Service"})

	matches, err := s.SearchDates([]string{"2026-05-10"}, "deploying SERVICE", 0)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(matches) != 1 {
		t.Fatalf("expected 1 case-insensitive match, got %d", len(matches))
	}
}

// TestSearchDates_ContextClippedAtEdges verifies context never reaches past
// either end of the trace.
func TestSearchDates_ContextClippedAtEdges(t *testing.T) {
	s := mustStore(t)
	appendLines(t, s, "task-a", dayOne, []string{"hit", "next"})

	matches, err := s.SearchDates([]string{"2026-05-10"}, "hit", 2)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(matches) != 1 {
		t.Fatalf("expected 1 match, got %d", len(matches))
	}
	if len(matches[0].ContextBefore) != 0 {
		t.Errorf("expected no before-context at the start of the trace, got %v", matches[0].ContextBefore)
	}
	if strings.Join(matches[0].ContextAfter, "|") != "next" {
		t.Errorf("expected one after-context line, got %v", matches[0].ContextAfter)
	}
}

// TestSearchDates_MissingDateIsNotAnError verifies a date directory that was
// pruned (or never existed) simply contributes no matches.
func TestSearchDates_MissingDateIsNotAnError(t *testing.T) {
	s := mustStore(t)
	appendLines(t, s, "task-a", dayOne, []string{"something"})

	matches, err := s.SearchDates([]string{"2025-01-01", "2026-05-10"}, "something", 0)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(matches) != 1 || matches[0].Date != "2026-05-10" {
		t.Fatalf("expected the one match from the existing date, got %+v", matches)
	}
}
