// Package logstore persists task logs to the filesystem.
//
// Directory structure:
//
//	<logDir>/
//	  2026-05-10/
//	    task-abc123/
//	      logs.jsonl.bz2
//
// Each log entry is stored as one JSON line (JSONL), compressed with bzip2
// for efficient storage. Legacy uncompressed `.jsonl` files are still
// readable for backward compatibility.
package logstore

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/dsnet/compress/bzip2"
)

// ErrLogNotFound is returned when no log file exists for the requested task.
var ErrLogNotFound = errors.New("log file not found")

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
	ToolType     string `json:"tool_type,omitempty"`      // "start", "output", "end"
	ToolName     string `json:"tool_name,omitempty"`      // e.g. "bash", "read"
	ToolID       string `json:"tool_id,omitempty"`        // unique tool call identifier
	ToolArgs     string `json:"tool_args,omitempty"`      // arguments/parameters
	ToolArgsJSON string `json:"tool_args_json,omitempty"` // raw argument JSON (UI diff rendering, issue #2)
	ToolOutput   string `json:"tool_output,omitempty"`    // captured output
	ToolError    bool   `json:"tool_error,omitempty"`     // true if tool ended with error

	// Structured compaction fields (only set when Level == "compaction")
	CompactionType         string `json:"compaction_type,omitempty"`          // "start", "end"
	CompactionReason       string `json:"compaction_reason,omitempty"`        // "manual", "threshold", "overflow"
	CompactionSummary      string `json:"compaction_summary,omitempty"`       // generated summary (end)
	CompactionTokensBefore int    `json:"compaction_tokens_before,omitempty"` // context size before (end)
	CompactionTokensAfter  int    `json:"compaction_tokens_after,omitempty"`  // estimated size after (end)
	CompactionError        bool   `json:"compaction_error,omitempty"`         // true if compaction failed
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

// CloseAll is a no-op for the current implementation.
// It exists for API compatibility with callers that expect it.
func (s *LogStore) CloseAll() {
	// Nothing to close — writers are closed after each Append
}

// dirForDate returns the date-partitioned directory path for the given date.
func (s *LogStore) dirForDate(date time.Time) string {
	return filepath.Join(s.dir, date.Format("2006-01-02"))
}

// compressedPath returns the full path to the compressed JSONL file for the given task ID.
func (s *LogStore) compressedPath(taskID string, date time.Time) string {
	return filepath.Join(s.dirForDate(date), taskID, "logs.jsonl.bz2")
}

// uncompressedPath returns the full path to the legacy uncompressed JSONL file.
func (s *LogStore) uncompressedPath(taskID string, date time.Time) string {
	return filepath.Join(s.dirForDate(date), taskID, "logs.jsonl")
}

// Append persists a single log entry to the filesystem.
// Creates parent directories as needed (date dir, task dir).
// Data is written through a bzip2-compressed stream.
// Each append opens the file, appends a bzip2 stream block, and closes it.
// The bzip2 reader can handle concatenated streams transparently.
func (s *LogStore) Append(entry Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	date := entry.Timestamp
	path := s.compressedPath(entry.TaskID, date)

	// Create parent directories as needed
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create task dir: %w", err)
	}

	// Open file in append mode to add a new bzip2 stream block
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("open log file %s: %w", path, err)
	}

	writer, err := bzip2.NewWriter(f, &bzip2.WriterConfig{
		Level: 1, // fastest compression; storage efficiency is the goal
	})
	if err != nil {
		f.Close()
		return fmt.Errorf("create bzip2 writer: %w", err)
	}

	data, err := json.Marshal(entry)
	if err != nil {
		writer.Close()
		f.Close()
		return fmt.Errorf("marshal log entry: %w", err)
	}

	_, err = writer.Write(data)
	if err != nil {
		writer.Close()
		f.Close()
		return fmt.Errorf("write log entry: %w", err)
	}
	_, err = writer.Write([]byte("\n"))
	if err != nil {
		writer.Close()
		f.Close()
		return fmt.Errorf("write newline: %w", err)
	}

	// Close finalises the bzip2 stream block
	if err := writer.Close(); err != nil {
		f.Close()
		return fmt.Errorf("close bzip2 writer: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close file: %w", err)
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

// TaskSummary holds a task ID and its earliest (starting) timestamp.
type TaskSummary struct {
	TaskID         string    `json:"task_id"`
	StartTimestamp time.Time `json:"start_timestamp"`
}

// readFirstTimestamp reads the first log entry from a task's log file
// and returns its timestamp. Supports both compressed (.bz2) and
// uncompressed (.jsonl) formats for backward compatibility.
func readFirstTimestamp(taskDir string) (time.Time, bool) {
	// Try compressed file first
	bz2Path := filepath.Join(taskDir, "logs.jsonl.bz2")
	if ts, ok := readFirstTimestampFromFile(bz2Path, true); ok {
		return ts, true
	}

	// Fall back to uncompressed file
	jsonlPath := filepath.Join(taskDir, "logs.jsonl")
	if ts, ok := readFirstTimestampFromFile(jsonlPath, false); ok {
		return ts, true
	}

	return time.Time{}, false
}

// readFirstTimestampFromFile reads the first JSON line from a file
// and returns the timestamp from the Entry.
func readFirstTimestampFromFile(path string, compressed bool) (time.Time, bool) {
	f, err := os.Open(path)
	if err != nil {
		return time.Time{}, false
	}
	defer f.Close()

	var reader io.Reader = f
	if compressed {
		r, err := bzip2.NewReader(f, nil)
		if err != nil {
			return time.Time{}, false
		}
		reader = r
	}

	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		line := scanner.Bytes()
		if isEmptyLine(line) {
			continue
		}
		var entry Entry
		if err := json.Unmarshal(line, &entry); err != nil {
			return time.Time{}, false
		}
		return entry.Timestamp, true
	}
	return time.Time{}, false
}

// ListTasksWithTimestamps returns all tasks for a given date along with
// the earliest (starting) timestamp for each task, sorted by task ID.
func (s *LogStore) ListTasksWithTimestamps(date string) ([]TaskSummary, error) {
	entries, err := os.ReadDir(filepath.Join(s.dir, date))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read date dir %s: %w", date, err)
	}

	var summaries []TaskSummary
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		taskID := e.Name()
		taskDir := filepath.Join(s.dir, date, taskID)

		ts, ok := readFirstTimestamp(taskDir)
		if ok {
			summaries = append(summaries, TaskSummary{
				TaskID:         taskID,
				StartTimestamp: ts,
			})
		}
	}

	// Sort by task ID for deterministic output
	sort.Slice(summaries, func(i, j int) bool {
		return summaries[i].TaskID < summaries[j].TaskID
	})

	return summaries, nil
}

// ListTasks returns all task directories for a given date, sorted ascending.
func (s *LogStore) ListTasks(date string) ([]string, error) {
	dir := filepath.Join(s.dir, date)
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read date dir %s: %w", date, err)
	}

	var tasks []string
	for _, e := range entries {
		if e.IsDir() {
			tasks = append(tasks, e.Name())
		}
	}
	return tasks, nil
}

// ReadLogs reads all log entries for a task from its log file.
// Supports both compressed (.bz2) and uncompressed (.jsonl) formats
// for backward compatibility. Compressed files are preferred.
func (s *LogStore) ReadLogs(date, taskID string) ([]Entry, error) {
	// Try compressed file first
	bz2Path := filepath.Join(s.dir, date, taskID, "logs.jsonl.bz2")
	entries, err := readLogFile(bz2Path, true)
	if err == nil && len(entries) > 0 {
		return entries, nil
	}

	// Fall back to uncompressed file
	jsonlPath := filepath.Join(s.dir, date, taskID, "logs.jsonl")
	entries, err = readLogFile(jsonlPath, false)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read log file %s: %w", jsonlPath, err)
	}

	return entries, nil
}

// ReadRawJSONL reads the raw JSONL content from a task's log file.
// Returns the decompressed JSONL as a string. Supports both compressed
// (.bz2) and uncompressed (.jsonl) formats for backward compatibility;
// compressed files are preferred. An existing file is returned as-is,
// even when empty. Returns ErrLogNotFound only when neither file exists;
// a present but unreadable file (e.g. a truncated bzip2 stream left by a
// crash mid-append) yields the underlying I/O error so callers can
// distinguish "missing" from "corrupt".
func (s *LogStore) ReadRawJSONL(date, taskID string) (string, error) {
	// Try compressed file first.
	bz2Path := filepath.Join(s.dir, date, taskID, "logs.jsonl.bz2")
	if info, err := os.Stat(bz2Path); err == nil && info.Size() == 0 {
		// A zero-byte bz2 (e.g. left by a crash before the first stream
		// block was written) is an empty log, not a corrupt one — a
		// zero-byte bzip2 stream fails to decompress.
		return "", nil
	}
	data, err := readRawFile(bz2Path, true)
	switch {
	case err == nil:
		return string(data), nil
	case !os.IsNotExist(err):
		// The compressed file exists but is unreadable. Surface the I/O
		// error instead of masking it as a missing log.
		return "", fmt.Errorf("read log file %s: %w", bz2Path, err)
	}

	// Fall back to uncompressed file.
	jsonlPath := filepath.Join(s.dir, date, taskID, "logs.jsonl")
	data, err = readRawFile(jsonlPath, false)
	if err != nil {
		if os.IsNotExist(err) {
			return "", ErrLogNotFound
		}
		return "", fmt.Errorf("read log file %s: %w", jsonlPath, err)
	}

	return string(data), nil
}

// openLogFile opens a log file and optionally wraps it in a bzip2 reader.
// Returns an io.ReadCloser that the caller must close.
func openLogFile(path string, compressed bool) (io.ReadCloser, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	if !compressed {
		return f, nil
	}
	r, err := bzip2.NewReader(f, nil)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("create bzip2 reader: %w", err)
	}
	return &compressReadCloser{Reader: r, closer: f}, nil
}

// compressReadCloser pairs a decompression reader with the underlying
// file's Closer, so closing the returned ReadCloser releases the file.
type compressReadCloser struct {
	io.Reader
	closer io.Closer
}

func (c *compressReadCloser) Close() error { return c.closer.Close() }

// readRawFile reads the raw (decompressed) bytes from a log file.
func readRawFile(path string, compressed bool) ([]byte, error) {
	rc, err := openLogFile(path, compressed)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	return io.ReadAll(rc)
}

// readLogFile reads all entries from a log file.
func readLogFile(path string, compressed bool) ([]Entry, error) {
	rc, err := openLogFile(path, compressed)
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	data, err := io.ReadAll(rc)
	if err != nil {
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
