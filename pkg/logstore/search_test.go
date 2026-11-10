package logstore

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
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

	res := s.SearchDates([]string{"2026-05-10"}, "hit")
	if len(res.Matches) != 1 {
		t.Fatalf("expected 1 match, got %d", len(res.Matches))
	}
	m := res.Matches[0]
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

	res := s.SearchDates([]string{"2026-05-10", "2026-05-11"}, "needle")
	if len(res.Matches) != 4 {
		t.Fatalf("expected 4 matches, got %d (%+v)", len(res.Matches), res.Matches)
	}
	var got []string
	for _, m := range res.Matches {
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

	res := s.SearchDates([]string{"2026-05-11"}, "needle")
	if len(res.Matches) != 1 || res.Matches[0].TaskID != "task-b" {
		t.Fatalf("expected only the task in the requested date, got %+v", res.Matches)
	}
}

// TestSearchDates_CaseInsensitive verifies substring matching ignores case.
func TestSearchDates_CaseInsensitive(t *testing.T) {
	s := mustStore(t)
	appendLines(t, s, "task-a", dayOne, []string{"Deploying Service"})

	res := s.SearchDates([]string{"2026-05-10"}, "deploying SERVICE")
	if len(res.Matches) != 1 {
		t.Fatalf("expected 1 case-insensitive match, got %d", len(res.Matches))
	}
}

// TestSearchDates_ContextClippedAtEdges verifies context never reaches past
// either end of the trace.
func TestSearchDates_ContextClippedAtEdges(t *testing.T) {
	s := mustStore(t)
	appendLines(t, s, "task-a", dayOne, []string{"hit", "next"})

	res := s.SearchDates([]string{"2026-05-10"}, "hit")
	if len(res.Matches) != 1 {
		t.Fatalf("expected 1 match, got %d", len(res.Matches))
	}
	if len(res.Matches[0].ContextBefore) != 0 {
		t.Errorf("expected no before-context at the start of the trace, got %v", res.Matches[0].ContextBefore)
	}
	if strings.Join(res.Matches[0].ContextAfter, "|") != "next" {
		t.Errorf("expected one after-context line, got %v", res.Matches[0].ContextAfter)
	}
}

// TestSearchDates_TruncatesAtTheCap verifies a query that matches everything
// cannot return the whole window: the scan stops at the cap, says so, and the
// matches it did collect are the earliest ones in the usual order.
func TestSearchDates_TruncatesAtTheCap(t *testing.T) {
	s := mustStore(t)
	lines := make([]string, MaxSearchMatches+25)
	for i := range lines {
		lines[i] = "every line has a needle"
	}
	appendLines(t, s, "task-a", dayOne, lines)

	res := s.SearchDates([]string{"2026-05-10"}, "needle")
	if len(res.Matches) != MaxSearchMatches {
		t.Fatalf("expected the scan to stop at %d matches, got %d", MaxSearchMatches, len(res.Matches))
	}
	if !res.Truncated {
		t.Error("expected a capped scan to report itself truncated")
	}
	if res.Matches[0].Line != "every line has a needle" {
		t.Errorf("unexpected first match %q", res.Matches[0].Line)
	}
}

// TestSearchDates_KeepsWalkingPastAnUnreadablePartition mirrors the stance
// PruneOlderThan takes in this package: one directory the scan cannot read
// costs its own matches, not the whole search. The unreadable partition is
// reported so the caller can log it, and the error is not a search failure.
func TestSearchDates_KeepsWalkingPastAnUnreadablePartition(t *testing.T) {
	s := mustStore(t)
	appendLines(t, s, "task-bad", dayOne, []string{"needle unreadable"})
	appendLines(t, s, "task-good", dayTwo, []string{"needle readable"})

	if err := os.Chmod(filepath.Join(s.dir, "2026-05-10", "task-bad"), 0o000); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	t.Cleanup(func() { os.Chmod(filepath.Join(s.dir, "2026-05-10", "task-bad"), 0o755) })

	res := s.SearchDates([]string{"2026-05-10", "2026-05-11"}, "needle")
	if len(res.Matches) != 1 || res.Matches[0].TaskID != "task-good" {
		t.Fatalf("expected the readable partition's match, got %+v", res.Matches)
	}
	if len(res.Errors) == 0 {
		t.Error("expected the unreadable partition to be reported")
	}
}

// TestSearchDates_MissingDateIsNotAnError verifies a date directory that was
// pruned (or never existed) simply contributes no matches.
func TestSearchDates_MissingDateIsNotAnError(t *testing.T) {
	s := mustStore(t)
	appendLines(t, s, "task-a", dayOne, []string{"something"})

	res := s.SearchDates([]string{"2025-01-01", "2026-05-10"}, "something")
	if len(res.Matches) != 1 || res.Matches[0].Date != "2026-05-10" {
		t.Fatalf("expected the one match from the existing date, got %+v", res.Matches)
	}
}

// TestSearchDates_StopsWhenTheScannedBytesBudgetRunsOut verifies the read
// side of the search is bounded: a trace larger than the budget stops the
// scan with Truncated set, and the walk never reaches the traces after it.
// The budget seam stands in for the tens of megabytes of decompressed output
// that would exhaust the production constant.
func TestSearchDates_StopsWhenTheScannedBytesBudgetRunsOut(t *testing.T) {
	s := mustStore(t)
	s.searchBudget = 4096

	long := strings.Repeat("x", 60)
	var lines []string
	for range 200 {
		lines = append(lines, long+" needle here")
	}
	appendLines(t, s, "task-a", dayOne, lines)
	appendLines(t, s, "task-b", dayOne, []string{"needle elsewhere"})

	// task-b would be a scan error if the scan ever read it; a scan that
	// honours its budget never gets that far.
	if err := os.Chmod(filepath.Join(s.dir, "2026-05-10", "task-b"), 0o000); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	t.Cleanup(func() { os.Chmod(filepath.Join(s.dir, "2026-05-10", "task-b"), 0o755) })

	res := s.SearchDates([]string{"2026-05-10"}, "needle")
	if !res.Truncated {
		t.Error("expected a scan that exhausted its scanned-bytes budget to report itself truncated")
	}
	if len(res.Errors) != 0 {
		t.Errorf("expected the scan to stop before reading task-b, got errors %v", res.Errors)
	}
}

// TestSearchDates_KeepsMatchesReadBeforeACorruptLine verifies a trace that
// fails to parse partway through costs only its unreadable remainder: the
// matches already streamed in come back, and the failure is reported.
func TestSearchDates_KeepsMatchesReadBeforeACorruptLine(t *testing.T) {
	s := mustStore(t)
	taskDir := filepath.Join(s.dir, "2026-05-10", "task-a")
	if err := os.MkdirAll(taskDir, 0o755); err != nil {
		t.Fatalf("create task dir: %v", err)
	}
	f, err := os.Create(filepath.Join(taskDir, "logs.jsonl"))
	if err != nil {
		t.Fatalf("create log file: %v", err)
	}
	for _, line := range []string{"needle first", "clean"} {
		entry := Entry{TaskID: "task-a", Line: line, Timestamp: dayOne}
		raw, err := json.Marshal(entry)
		if err != nil {
			t.Fatalf("marshal entry: %v", err)
		}
		if _, err := fmt.Fprintf(f, "%s\n", raw); err != nil {
			t.Fatalf("write entry: %v", err)
		}
	}
	if _, err := f.WriteString("this is not a log entry\n"); err != nil {
		t.Fatalf("write corrupt line: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close log file: %v", err)
	}

	res := s.SearchDates([]string{"2026-05-10"}, "needle")
	if len(res.Matches) != 1 || res.Matches[0].Line != "needle first" {
		t.Fatalf("expected the match read before the corrupt line, got %+v", res.Matches)
	}
	if len(res.Errors) == 0 {
		t.Error("expected the corrupt line to be reported")
	}
}

// TestSearchDates_FallsBackToLegacyLogWhenBzip2Unreadable verifies the
// search agrees with ReadLogs about which file is a task's trace when a
// crash left a corrupt-but-non-empty bzip2 beside an intact legacy file
// (the state ReadRawJSONL's docblock names). ReadLogs prefers the bzip2
// only when it reads; so does the streaming scan, and the legacy entries
// come back rather than an empty partial answer.
func TestSearchDates_FallsBackToLegacyLogWhenBzip2Unreadable(t *testing.T) {
	s := mustStore(t)
	taskDir := filepath.Join(s.dir, "2026-05-10", "task-a")
	if err := os.MkdirAll(taskDir, 0o755); err != nil {
		t.Fatalf("create task dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(taskDir, "logs.jsonl.bz2"),
		[]byte("not a bzip2 stream at all"), 0o644); err != nil {
		t.Fatalf("write corrupt bzip2: %v", err)
	}
	f, err := os.Create(filepath.Join(taskDir, "logs.jsonl"))
	if err != nil {
		t.Fatalf("create legacy log: %v", err)
	}
	for _, line := range []string{"clean", "needle legacy"} {
		raw, err := json.Marshal(Entry{TaskID: "task-a", Line: line, Timestamp: dayOne})
		if err != nil {
			t.Fatalf("marshal entry: %v", err)
		}
		if _, err := f.Write(append(raw, '\n')); err != nil {
			t.Fatalf("write legacy entry: %v", err)
		}
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close legacy log: %v", err)
	}

	res := s.SearchDates([]string{"2026-05-10"}, "needle")
	if len(res.Matches) != 1 || res.Matches[0].Line != "needle legacy" {
		t.Fatalf("expected the legacy trace's match, got %+v", res.Matches)
	}
	if len(res.Errors) != 0 {
		t.Errorf("expected no errors once the intact legacy file answered, got %v", res.Errors)
	}
	entries, err := s.ReadLogs("2026-05-10", "task-a")
	if err != nil {
		t.Fatalf("ReadLogs: %v", err)
	}
	if len(entries) != 2 {
		t.Errorf("the endpoints disagree about the trace: ReadLogs read %d entries, the search matched %d of them", len(entries), len(res.Matches))
	}
}
