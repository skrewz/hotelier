// Package logstore persists task logs to the filesystem.
//
// Directory structure:
//
//	<logDir>/
//	  2026-05-10/
//	    task-abc123/
//	      logs.jsonl
//
// Each log entry is stored as one JSON line (JSONL) for append efficiency.
package logstore

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Entry represents a single log line persisted to disk.
// For tool call events, the Line field contains the original formatted
// string for backwards compatibility, but structured fields (ToolType,
// ToolName, etc.) carry the machine-readable data.
type Entry struct {
	TaskID    string    `json:"task_id"`
	Line      string    `json:"line"`
	Level     string    `json:"level,omitempty"`
	Timestamp time.Time `json:"timestamp"`

	// Structured tool call fields (only set when Level == "tool")
	ToolType   string `json:"tool_type,omitempty"`   // "start", "output", "end"
	ToolName   string `json:"tool_name,omitempty"`   // e.g. "bash", "read"
	ToolID     string `json:"tool_id,omitempty"`     // unique tool call identifier
	ToolArgs   string `json:"tool_args,omitempty"`   // arguments/parameters
	ToolOutput string `json:"tool_output,omitempty"` // captured output
	ToolError  bool   `json:"tool_error,omitempty"`  // true if tool ended with error
}

// LogStore persists task logs to the filesystem in a date-partitioned structure.
type LogStore struct {
	dir string
	mu  sync.Mutex
}

// New creates a new LogStore backed by the given directory.
// The directory is created if it does not exist.
func New(dir string) (*LogStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create log store dir %s: %w", dir, err)
	}
	return &LogStore{dir: dir}, nil
}

// dirForDate returns the date-partitioned directory path for the given date.
func (s *LogStore) dirForDate(date time.Time) string {
	return filepath.Join(s.dir, date.Format("2006-01-02"))
}

// filePath returns the full path to the JSONL file for the given task ID.
func (s *LogStore) filePath(taskID string, date time.Time) string {
	return filepath.Join(s.dirForDate(date), taskID, "logs.jsonl")
}

// Append persists a single log entry to the filesystem.
// Creates parent directories as needed (date dir, task dir).
func (s *LogStore) Append(entry Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	date := entry.Timestamp
	dir := s.dirForDate(date)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create date dir %s: %w", dir, err)
	}

	taskDir := filepath.Join(dir, entry.TaskID)
	if err := os.MkdirAll(taskDir, 0o755); err != nil {
		return fmt.Errorf("create task dir %s: %w", taskDir, err)
	}

	path := filepath.Join(taskDir, "logs.jsonl")

	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("open log file %s: %w", path, err)
	}
	defer f.Close()

	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("marshal log entry: %w", err)
	}

	if _, err := f.Write(data); err != nil {
		return fmt.Errorf("write log entry: %w", err)
	}
	if _, err := f.Write([]byte("\n")); err != nil {
		return fmt.Errorf("write newline: %w", err)
	}

	return nil
}

// ListDates returns all date directories present in the log store, sorted ascending.
func (s *LogStore) ListDates() ([]string, error) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read log store dir: %w", err)
	}

	var dates []string
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		// Validate date format: YYYY-MM-DD
		if _, err := time.Parse("2006-01-02", e.Name()); err == nil {
			dates = append(dates, e.Name())
		}
	}
	return dates, nil
}

// ListTasks returns all task directories for a given date, sorted ascending.
func (s *LogStore) ListTasks(date string) ([]string, error) {
	dir := filepath.Join(s.dir, date)
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read date dir %s: %w", dir, err)
	}

	var tasks []string
	for _, e := range entries {
		if e.IsDir() {
			tasks = append(tasks, e.Name())
		}
	}
	return tasks, nil
}

// ReadLogs reads all log entries for a task from its JSONL file.
func (s *LogStore) ReadLogs(date, taskID string) ([]Entry, error) {
	path := filepath.Join(s.dir, date, taskID, "logs.jsonl")

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read log file %s: %w", path, err)
	}

	var entries []Entry
	for _, line := range splitLines(data) {
		if isEmptyLine(line) {
			continue
		}
		var entry Entry
		if err := json.Unmarshal(line, &entry); err != nil {
			return nil, fmt.Errorf("unmarshal log entry: %w", err)
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

// splitLines splits raw bytes into individual lines.
func splitLines(data []byte) [][]byte {
	var lines [][]byte
	var current []byte
	for _, b := range data {
		if b == '\n' {
			lines = append(lines, current)
			current = nil
		} else {
			current = append(current, b)
		}
	}
	if len(current) > 0 {
		lines = append(lines, current)
	}
	return lines
}

// isEmptyLine checks if a byte slice is empty or whitespace-only.
func isEmptyLine(b []byte) bool {
	for _, c := range b {
		if c != ' ' && c != '\t' && c != '\r' {
			return false
		}
	}
	return true
}
