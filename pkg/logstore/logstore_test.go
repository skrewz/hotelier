package logstore

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dsnet/compress/bzip2"
)

func TestLogStore_AppendWritesCompressed(t *testing.T) {
	s, dir := newTestLogStore(t)
	defer func() {
		s.CloseAll()
		os.RemoveAll(dir)
	}()

	ts := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
	s.Append(Entry{TaskID: "task-1", Line: "compressed log", Level: "info", Timestamp: ts})

	// Close writers to flush data to disk
	s.CloseAll()

	// Verify compressed file exists
	bz2Path := filepath.Join(dir, "2026-05-10", "task-1", "logs.jsonl.bz2")
	info, err := os.Stat(bz2Path)
	if err != nil {
		t.Fatalf("logs.jsonl.bz2 should exist: %v", err)
	}
	if info.Size() == 0 {
		t.Fatal("logs.jsonl.bz2 should not be empty")
	}

	// Verify we can decompress and read the entry
	data, err := os.ReadFile(bz2Path)
	if err != nil {
		t.Fatalf("read bz2: %v", err)
	}

	r, err := bzip2.NewReader(bytes.NewReader(data), nil)
	if err != nil {
		t.Fatalf("create bzip2 reader: %v", err)
	}
	decompressed, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("decompress bz2: %v", err)
	}

	lines := splitLines(decompressed)
	if len(lines) != 1 {
		t.Fatalf("expected 1 line, got %d", len(lines))
	}

	var entry Entry
	if err := json.Unmarshal(lines[0], &entry); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if entry.Line != "compressed log" {
		t.Errorf("expected line 'compressed log', got %q", entry.Line)
	}
}

func TestLogStore_AppendMultipleEntries(t *testing.T) {
	s, dir := newTestLogStore(t)
	defer func() {
		s.CloseAll()
		os.RemoveAll(dir)
	}()

	base := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
	for i := 0; i < 5; i++ {
		s.Append(Entry{TaskID: "task-1", Line: "entry " + string(rune('A'+i)), Level: "info", Timestamp: base.Add(time.Duration(i) * time.Second)})
	}

	s.CloseAll()

	// Read back via API
	entries, err := s.ReadLogs("2026-05-10", "task-1")
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(entries) != 5 {
		t.Fatalf("expected 5 entries, got %d", len(entries))
	}

	for i, e := range entries {
		expected := "entry " + string(rune('A'+i))
		if e.Line != expected {
			t.Errorf("entry %d: line = %q, want %q", i, e.Line, expected)
		}
	}
}

func TestLogStore_ReadBackwardCompat_Uncompressed(t *testing.T) {
	s, dir := newTestLogStore(t)
	defer os.RemoveAll(dir)

	// Manually create an uncompressed logs.jsonl file
	taskDir := filepath.Join(dir, "2026-05-10", "legacy-task")
	if err := os.MkdirAll(taskDir, 0o755); err != nil {
		t.Fatalf("create task dir: %v", err)
	}

	ts := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
	entry := Entry{TaskID: "legacy-task", Line: "legacy log", Level: "info", Timestamp: ts}
	data, _ := json.Marshal(entry)

	if err := os.WriteFile(filepath.Join(taskDir, "logs.jsonl"), append(data, '\n'), 0o644); err != nil {
		t.Fatalf("write legacy file: %v", err)
	}

	// ReadLogs should find the uncompressed file
	entries, err := s.ReadLogs("2026-05-10", "legacy-task")
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	if entries[0].Line != "legacy log" {
		t.Errorf("expected 'legacy log', got %q", entries[0].Line)
	}
}

func TestLogStore_ReadPrefersCompressed(t *testing.T) {
	s, dir := newTestLogStore(t)
	defer func() {
		s.CloseAll()
		os.RemoveAll(dir)
	}()

	// Write a compressed file
	ts := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
	s.Append(Entry{TaskID: "task-1", Line: "compressed", Level: "info", Timestamp: ts})
	s.CloseAll()

	// Also create an uncompressed file with different content
	taskDir := filepath.Join(dir, "2026-05-10", "task-1")
	legacyEntry := Entry{TaskID: "task-1", Line: "uncompressed", Level: "info", Timestamp: ts}
	legacyData, _ := json.Marshal(legacyEntry)
	os.WriteFile(filepath.Join(taskDir, "logs.jsonl"), append(legacyData, '\n'), 0o644)

	// ReadLogs should prefer the compressed file
	entries, err := s.ReadLogs("2026-05-10", "task-1")
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	if entries[0].Line != "compressed" {
		t.Errorf("expected 'compressed' (from bz2), got %q", entries[0].Line)
	}
}

func TestLogStore_ListTasksWithTimestamps_Compressed(t *testing.T) {
	s, dir := newTestLogStore(t)
	defer func() {
		s.CloseAll()
		os.RemoveAll(dir)
	}()

	tsA := time.Date(2026, 5, 10, 8, 30, 0, 0, time.UTC)
	tsB := time.Date(2026, 5, 10, 10, 15, 0, 0, time.UTC)

	s.Append(Entry{TaskID: "task-a", Line: "A1", Level: "info", Timestamp: tsA})
	s.Append(Entry{TaskID: "task-b", Line: "B1", Level: "info", Timestamp: tsB})
	s.CloseAll()

	summaries, err := s.ListTasksWithTimestamps("2026-05-10")
	if err != nil {
		t.Fatalf("list tasks with timestamps: %v", err)
	}
	if len(summaries) != 2 {
		t.Fatalf("expected 2 tasks, got %d", len(summaries))
	}

	if summaries[0].TaskID != "task-a" {
		t.Errorf("expected first task 'task-a', got %q", summaries[0].TaskID)
	}
	if !summaries[0].StartTimestamp.Equal(tsA) {
		t.Errorf("expected task-a start %v, got %v", tsA, summaries[0].StartTimestamp)
	}
}

func TestLogStore_CloseAll(t *testing.T) {
	s, dir := newTestLogStore(t)
	defer os.RemoveAll(dir)

	ts := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
	s.Append(Entry{TaskID: "task-1", Line: "before close", Level: "info", Timestamp: ts})
	s.Append(Entry{TaskID: "task-2", Line: "before close too", Level: "info", Timestamp: ts})

	// CloseAll should flush all writers
	s.CloseAll()

	// Verify both tasks have compressed files
	for _, taskID := range []string{"task-1", "task-2"} {
		bz2Path := filepath.Join(dir, "2026-05-10", taskID, "logs.jsonl.bz2")
		if _, err := os.Stat(bz2Path); err != nil {
			t.Errorf("%s: logs.jsonl.bz2 should exist: %v", taskID, err)
		}
	}
}

func TestLogStore_AppendAndRead(t *testing.T) {
	s, dir := newTestLogStore(t)
	defer func() {
		s.CloseAll()
		os.RemoveAll(dir)
	}()

	entries := []Entry{
		{TaskID: "task-1", Line: "Hello **world**", Level: "text", Timestamp: time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)},
		{TaskID: "task-1", Line: "[TOOL_START] read: file.txt (id: t1)", Level: "tool", Timestamp: time.Date(2026, 5, 10, 12, 0, 1, 0, time.UTC)},
		{TaskID: "task-1", Line: "Guest output", Level: "info", Timestamp: time.Date(2026, 5, 10, 12, 0, 2, 0, time.UTC)},
	}

	for _, e := range entries {
		if err := s.Append(e); err != nil {
			t.Fatalf("append: %v", err)
		}
	}

	s.CloseAll()

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

func TestLogStore_DatePartitioning(t *testing.T) {
	s, dir := newTestLogStore(t)
	defer func() {
		s.CloseAll()
		os.RemoveAll(dir)
	}()

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
	defer func() {
		s.CloseAll()
		os.RemoveAll(dir)
	}()

	s.Append(Entry{TaskID: "task-a", Line: "A1", Level: "info", Timestamp: time.Date(2026, 5, 10, 10, 0, 0, 0, time.UTC)})
	s.Append(Entry{TaskID: "task-b", Line: "B1", Level: "info", Timestamp: time.Date(2026, 5, 10, 10, 0, 0, 0, time.UTC)})
	s.Append(Entry{TaskID: "task-a", Line: "A2", Level: "info", Timestamp: time.Date(2026, 5, 10, 10, 0, 0, 0, time.UTC)})
	s.CloseAll()

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
	defer func() {
		s.CloseAll()
		os.RemoveAll(dir)
	}()

	// Append to a date that doesn't exist yet — should create directories
	if err := s.Append(Entry{TaskID: "future-task", Line: "future log", Level: "info", Timestamp: time.Date(2027, 1, 15, 0, 0, 0, 0, time.UTC)}); err != nil {
		t.Fatalf("append to future date: %v", err)
	}
	s.CloseAll()

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
	defer func() {
		s.CloseAll()
		os.RemoveAll(dir)
	}()

	err := s.Append(Entry{TaskID: "deep-task", Line: "log", Level: "info", Timestamp: time.Date(2026, 5, 10, 0, 0, 0, 0, time.UTC)})
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	// Flush writer to disk
	s.CloseAll()

	// Verify the task directory exists
	taskDir := filepath.Join(dir, "2026-05-10", "deep-task")
	info, err := os.Stat(taskDir)
	if err != nil {
		t.Fatalf("task dir should exist: %v", err)
	}
	if !info.IsDir() {
		t.Fatal("task path should be a directory")
	}

	// Verify the compressed file exists
	bz2 := filepath.Join(taskDir, "logs.jsonl.bz2")
	info, err = os.Stat(bz2)
	if err != nil {
		t.Fatalf("logs.jsonl.bz2 should exist: %v", err)
	}
	if info.Size() == 0 {
		t.Fatal("logs.jsonl.bz2 should not be empty")
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
	defer func() {
		s.CloseAll()
		os.RemoveAll(dir)
	}()

	ts := time.Date(2026, 5, 10, 14, 30, 45, 123000000, time.UTC)
	entry := Entry{TaskID: "ts-test", Line: "timestamp test", Level: "text", Timestamp: ts}

	if err := s.Append(entry); err != nil {
		t.Fatalf("append: %v", err)
	}
	s.CloseAll()

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

func TestLogStore_ListTasksWithTimestamps(t *testing.T) {
	s, dir := newTestLogStore(t)
	defer func() {
		s.CloseAll()
		os.RemoveAll(dir)
	}()

	tsA := time.Date(2026, 5, 10, 8, 30, 0, 0, time.UTC)
	tsB := time.Date(2026, 5, 10, 10, 15, 0, 0, time.UTC)

	s.Append(Entry{TaskID: "task-a", Line: "A1", Level: "info", Timestamp: tsA})
	s.Append(Entry{TaskID: "task-b", Line: "B1", Level: "info", Timestamp: tsB})
	s.Append(Entry{TaskID: "task-a", Line: "A2", Level: "info", Timestamp: tsA.Add(5 * time.Minute)})
	s.CloseAll()

	summaries, err := s.ListTasksWithTimestamps("2026-05-10")
	if err != nil {
		t.Fatalf("list tasks with timestamps: %v", err)
	}
	if len(summaries) != 2 {
		t.Fatalf("expected 2 tasks, got %d", len(summaries))
	}

	// Summaries are sorted by task ID
	if summaries[0].TaskID != "task-a" {
		t.Errorf("expected first task 'task-a', got %q", summaries[0].TaskID)
	}
	if !summaries[0].StartTimestamp.Equal(tsA) {
		t.Errorf("expected task-a start %v, got %v", tsA, summaries[0].StartTimestamp)
	}

	if summaries[1].TaskID != "task-b" {
		t.Errorf("expected second task 'task-b', got %q", summaries[1].TaskID)
	}
	if !summaries[1].StartTimestamp.Equal(tsB) {
		t.Errorf("expected task-b start %v, got %v", tsB, summaries[1].StartTimestamp)
	}
}

func TestLogStore_ListTasksWithTimestamps_Empty(t *testing.T) {
	s, dir := newTestLogStore(t)
	defer os.RemoveAll(dir)

	summaries, err := s.ListTasksWithTimestamps("2026-05-10")
	if err != nil {
		t.Fatalf("list tasks with timestamps on empty date: %v", err)
	}
	if len(summaries) != 0 {
		t.Errorf("expected 0 tasks, got %d", len(summaries))
	}
}

func TestLogStore_JSONLFormat(t *testing.T) {
	s, dir := newTestLogStore(t)
	defer func() {
		s.CloseAll()
		os.RemoveAll(dir)
	}()

	s.Append(Entry{TaskID: "jsonl-test", Line: "multi\nline", Level: "text", Timestamp: time.Date(2026, 5, 10, 0, 0, 0, 0, time.UTC)})
	s.CloseAll()

	// Read compressed file and decompress
	bz2Path := filepath.Join(dir, "2026-05-10", "jsonl-test", "logs.jsonl.bz2")
	compressed, err := os.ReadFile(bz2Path)
	if err != nil {
		t.Fatalf("read bz2: %v", err)
	}

	r, err := bzip2.NewReader(bytes.NewReader(compressed), nil)
	if err != nil {
		t.Fatalf("create bzip2 reader: %v", err)
	}
	raw, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("decompress: %v", err)
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
