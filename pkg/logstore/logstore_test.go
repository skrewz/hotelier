package logstore

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func newTestLogStore(t *testing.T) (*LogStore, string) {
	t.Helper()
	dir, err := os.MkdirTemp("", "hotelier-logstore-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	s, err := New(dir)
	if err != nil {
		t.Fatalf("new logstore: %v", err)
	}
	return s, dir
}

func TestLogStore_AppendAndRead(t *testing.T) {
	s, dir := newTestLogStore(t)
	defer os.RemoveAll(dir)

	entries := []Entry{
		{TaskID: "task-1", Line: "Hello **world**", Level: "text", Timestamp: time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)},
		{TaskID: "task-1", Line: "[TOOL_START] read: file.txt (id: t1)", Level: "tool", Timestamp: time.Date(2026, 5, 10, 12, 0, 1, 0, time.UTC)},
		{TaskID: "task-1", Line: "Agent output", Level: "info", Timestamp: time.Date(2026, 5, 10, 12, 0, 2, 0, time.UTC)},
	}

	for _, e := range entries {
		if err := s.Append(e); err != nil {
			t.Fatalf("append: %v", err)
		}
	}

	got, err := s.ReadLogs("2026-05-10", "task-1")
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	if len(got) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(got))
	}

	for i, want := range entries {
		if got[i].TaskID != want.TaskID {
			t.Errorf("entry %d: task_id = %q, want %q", i, got[i].TaskID, want.TaskID)
		}
		if got[i].Line != want.Line {
			t.Errorf("entry %d: line = %q, want %q", i, got[i].Line, want.Line)
		}
		if got[i].Level != want.Level {
			t.Errorf("entry %d: level = %q, want %q", i, got[i].Level, want.Level)
		}
	}
}

func TestLogStore_DatePartitioning(t *testing.T) {
	s, dir := newTestLogStore(t)
	defer os.RemoveAll(dir)

	// Two entries on different dates
	s.Append(Entry{TaskID: "task-1", Line: "Day 1", Level: "info", Timestamp: time.Date(2026, 5, 9, 12, 0, 0, 0, time.UTC)})
	s.Append(Entry{TaskID: "task-2", Line: "Day 2", Level: "info", Timestamp: time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)})

	dates, err := s.ListDates()
	if err != nil {
		t.Fatalf("list dates: %v", err)
	}

	if len(dates) != 2 {
		t.Fatalf("expected 2 dates, got %d: %v", len(dates), dates)
	}
	if dates[0] != "2026-05-09" || dates[1] != "2026-05-10" {
		t.Errorf("expected sorted dates, got %v", dates)
	}

	// Each date should have exactly one task
	tasks1, err := s.ListTasks("2026-05-09")
	if err != nil {
		t.Fatalf("list tasks 05-09: %v", err)
	}
	if len(tasks1) != 1 || tasks1[0] != "task-1" {
		t.Errorf("2026-05-09: expected [task-1], got %v", tasks1)
	}

	tasks2, err := s.ListTasks("2026-05-10")
	if err != nil {
		t.Fatalf("list tasks 05-10: %v", err)
	}
	if len(tasks2) != 1 || tasks2[0] != "task-2" {
		t.Errorf("2026-05-10: expected [task-2], got %v", tasks2)
	}
}

func TestLogStore_MultipleTasksSameDate(t *testing.T) {
	s, dir := newTestLogStore(t)
	defer os.RemoveAll(dir)

	s.Append(Entry{TaskID: "task-a", Line: "A1", Level: "info", Timestamp: time.Date(2026, 5, 10, 10, 0, 0, 0, time.UTC)})
	s.Append(Entry{TaskID: "task-b", Line: "B1", Level: "info", Timestamp: time.Date(2026, 5, 10, 10, 0, 0, 0, time.UTC)})
	s.Append(Entry{TaskID: "task-a", Line: "A2", Level: "info", Timestamp: time.Date(2026, 5, 10, 10, 0, 0, 0, time.UTC)})

	tasks, err := s.ListTasks("2026-05-10")
	if err != nil {
		t.Fatalf("list tasks: %v", err)
	}
	if len(tasks) != 2 {
		t.Fatalf("expected 2 tasks, got %d", len(tasks))
	}

	// Each task should have the right number of entries
	entriesA, err := s.ReadLogs("2026-05-10", "task-a")
	if err != nil {
		t.Fatalf("read task-a: %v", err)
	}
	if len(entriesA) != 2 {
		t.Errorf("task-a: expected 2 entries, got %d", len(entriesA))
	}

	entriesB, err := s.ReadLogs("2026-05-10", "task-b")
	if err != nil {
		t.Fatalf("read task-b: %v", err)
	}
	if len(entriesB) != 1 {
		t.Errorf("task-b: expected 1 entry, got %d", len(entriesB))
	}
}

func TestLogStore_AppendToNonExistentDate(t *testing.T) {
	s, dir := newTestLogStore(t)
	defer os.RemoveAll(dir)

	// Append to a date that doesn't exist yet — should create directories
	err := s.Append(Entry{TaskID: "future-task", Line: "future log", Level: "info", Timestamp: time.Date(2027, 1, 15, 0, 0, 0, 0, time.UTC)})
	if err != nil {
		t.Fatalf("append to future date: %v", err)
	}

	dates, err := s.ListDates()
	if err != nil {
		t.Fatalf("list dates: %v", err)
	}
	if len(dates) != 1 || dates[0] != "2027-01-15" {
		t.Errorf("expected [2027-01-15], got %v", dates)
	}
}

func TestLogStore_AppendCreatesTaskDir(t *testing.T) {
	s, dir := newTestLogStore(t)
	defer os.RemoveAll(dir)

	err := s.Append(Entry{TaskID: "deep-task", Line: "log", Level: "info", Timestamp: time.Date(2026, 5, 10, 0, 0, 0, 0, time.UTC)})
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	// Verify the task directory exists
	taskDir := filepath.Join(dir, "2026-05-10", "deep-task")
	info, err := os.Stat(taskDir)
	if err != nil {
		t.Fatalf("task dir should exist: %v", err)
	}
	if !info.IsDir() {
		t.Fatal("task path should be a directory")
	}

	// Verify the JSONL file exists
	jsonl := filepath.Join(taskDir, "logs.jsonl")
	info, err = os.Stat(jsonl)
	if err != nil {
		t.Fatalf("logs.jsonl should exist: %v", err)
	}
	if info.Size() == 0 {
		t.Fatal("logs.jsonl should not be empty")
	}
}

func TestLogStore_ReadNonExistentTask(t *testing.T) {
	s, dir := newTestLogStore(t)
	defer os.RemoveAll(dir)

	entries, err := s.ReadLogs("2026-05-10", "nonexistent")
	if err != nil {
		t.Fatalf("read nonexistent: %v", err)
	}
	if entries != nil && len(entries) != 0 {
		t.Errorf("expected empty slice, got %d entries", len(entries))
	}
}

func TestLogStore_ListDatesEmpty(t *testing.T) {
	s, dir := newTestLogStore(t)
	defer os.RemoveAll(dir)

	dates, err := s.ListDates()
	if err != nil {
		t.Fatalf("list dates: %v", err)
	}
	if len(dates) != 0 {
		t.Errorf("expected 0 dates, got %d", len(dates))
	}
}

func TestLogStore_ListTasksEmptyDate(t *testing.T) {
	s, dir := newTestLogStore(t)
	defer os.RemoveAll(dir)

	tasks, err := s.ListTasks("2026-05-10")
	if err != nil {
		t.Fatalf("list tasks: %v", err)
	}
	if len(tasks) != 0 {
		t.Errorf("expected 0 tasks, got %d", len(tasks))
	}
}

func TestLogStore_AppendPreservesTimestamp(t *testing.T) {
	s, dir := newTestLogStore(t)
	defer os.RemoveAll(dir)

	ts := time.Date(2026, 5, 10, 14, 30, 45, 123000000, time.UTC)
	entry := Entry{TaskID: "ts-test", Line: "timestamp test", Level: "text", Timestamp: ts}

	if err := s.Append(entry); err != nil {
		t.Fatalf("append: %v", err)
	}

	read, err := s.ReadLogs("2026-05-10", "ts-test")
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(read) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(read))
	}
	if !read[0].Timestamp.Equal(ts) {
		t.Errorf("timestamp = %v, want %v", read[0].Timestamp, ts)
	}
}

func TestLogStore_JSONLFormat(t *testing.T) {
	s, dir := newTestLogStore(t)
	defer os.RemoveAll(dir)

	s.Append(Entry{TaskID: "jsonl-test", Line: "multi\nline", Level: "text", Timestamp: time.Date(2026, 5, 10, 0, 0, 0, 0, time.UTC)})

	// Read raw file content
	raw, err := os.ReadFile(filepath.Join(dir, "2026-05-10", "jsonl-test", "logs.jsonl"))
	if err != nil {
		t.Fatalf("read raw: %v", err)
	}

	lines := splitLines(raw)
	if len(lines) != 1 {
		t.Fatalf("expected 1 line, got %d", len(lines))
	}

	// Verify the JSON is valid and contains the newline in the line field
	var parsed Entry
	if err := json.Unmarshal(lines[0], &parsed); err != nil {
		t.Fatalf("unmarshal JSONL: %v", err)
	}
	if parsed.Line != "multi\nline" {
		t.Errorf("line preserved newline: got %q", parsed.Line)
	}
}
