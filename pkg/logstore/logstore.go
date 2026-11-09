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
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
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
	// os.ReadDir returns lexical order, which happens to be chronological for
	// YYYY-MM-DD names; sort explicitly so the ascending guarantee does not
	// depend on that.
	sort.Strings(dates)
	return dates, nil
}

// PruneResult reports what one PruneOlderThan sweep removed, so the caller
// can log evidence (issue #263): the next operator staring at a full disk
// finds the reclaimed bytes in the log instead of du-ing the tree by hand.
type PruneResult struct {
	// DatesRemoved lists the date directories deleted, oldest first.
	DatesRemoved []string
	// FilesRemoved counts the files those directories held. Entries that
	// cannot be read (a damaged volume, a permission error mid-walk) are
	// skipped rather than counted, so this is a floor rather than an exact
	// reconciliation.
	FilesRemoved int
	// BytesReclaimed sums the removed files' sizes. As with FilesRemoved,
	// an entry that cannot be stat'd counts as nothing, so the total reads
	// as "at least this much came back" rather than "this is exactly what
	// was there".
	BytesReclaimed int64
}

// PruneOlderThan removes every date directory older than the retention
// window, anchored at now: a directory is deleted when its YYYY-MM-DD name
// names a day strictly earlier than now's date minus retentionDays. The
// comparison works on directory names, not file trees, so the sweep costs a
// ReadDir plus a RemoveAll per expired day — never a walk to decide what to
// drop (one WalkDir per *removed* day still runs, to report its size).
//
// retentionDays <= 0 disables pruning and removes nothing, mirroring the
// max_guests convention where 0 means "no limit". Directories whose names
// are not dates (a queue store's pending/ running/ dirs sharing the tree,
// stray files) are never touched, so a persisted queue entry survives the
// sweep regardless of its age even when queue_dir points at log_dir.
//
// A directory that cannot be removed does not stop the sweep: the error is
// collected, the remaining (newer) dates are still visited, and the fully
// populated PruneResult comes back alongside the joined errors. Returning on
// the first failure would let one stuck day block every newer one on this
// sweep and on every future tick, which is the failure mode the sweep exists
// to prevent.
func (s *LogStore) PruneOlderThan(now time.Time, retentionDays int) (PruneResult, error) {
	var res PruneResult
	if retentionDays <= 0 {
		return res, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	dates, err := s.ListDates()
	if err != nil {
		return res, err
	}

	var errs []error

	// Compare days, not instants: anchor the cutoff at midnight UTC of
	// now's calendar date so a non-UTC "now" cannot shift the boundary by
	// its clock time.
	y, m, d := now.Date()
	cutoff := time.Date(y, m, d, 0, 0, 0, 0, time.UTC).AddDate(0, 0, -retentionDays)

	for _, date := range dates {
		day, err := time.Parse("2006-01-02", date)
		if err != nil {
			// ListDates only returns parseable names; belt and braces.
			continue
		}
		if !day.Before(cutoff) {
			continue
		}
		path := filepath.Join(s.dir, date)
		files, bytes := dirSize(path)
		if err := os.RemoveAll(path); err != nil {
			// Per-directory failure, not a sweep failure: keep walking so one
			// stuck directory (permissions, an immutable attribute, a damaged
			// volume) cannot permanently block every newer expired day.
			errs = append(errs, fmt.Errorf("prune log dir %s: %w", path, err))
			continue
		}
		res.DatesRemoved = append(res.DatesRemoved, date)
		res.FilesRemoved += files
		res.BytesReclaimed += bytes
	}
	return res, errors.Join(errs...)
}

// dirSize counts the regular files under dir and sums their sizes. It is
// only ever called on a directory about to be removed, to report what the
// sweep reclaims. Unreadable entries are skipped rather than failing the
// sweep, so the counts are a lower bound: an entry that cannot be stat'd
// is reported as having reclaimed nothing, and an expired day of such
// entries is reported as 0 files / 0 bytes.
func dirSize(dir string) (files int, bytes int64) {
	filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || !d.Type().IsRegular() {
			return nil
		}
		if info, err := d.Info(); err == nil {
			files++
			bytes += info.Size()
		}
		return nil
	})
	return files, bytes
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

// FindTaskDates returns every date partition that holds a log file for the
// task, oldest first — a task whose trace spans midnight has one file per
// date directory. The slice is empty when no partition holds the task. It
// performs no validation of taskID; callers must reject anything that is not a
// plain task ID before handing it in.
func (s *LogStore) FindTaskDates(taskID string) ([]string, error) {
	dates, err := s.ListDates()
	if err != nil {
		return nil, err
	}

	var found []string
	for _, date := range dates {
		if hasLogFile(filepath.Join(s.dir, date, taskID)) {
			found = append(found, date)
		}
	}
	return found, nil
}

// ReadTaskRawJSONL reads a task's whole trace across every date partition it
// appears in, concatenated oldest segment first, so a task that ran across
// midnight downloads as one file rather than silently losing its earlier
// segments. It returns the raw JSONL together with the partitions it came
// from. Returns ErrLogNotFound when no partition holds the task.
func (s *LogStore) ReadTaskRawJSONL(taskID string) (string, []string, error) {
	dates, err := s.FindTaskDates(taskID)
	if err != nil {
		return "", nil, err
	}
	if len(dates) == 0 {
		return "", nil, ErrLogNotFound
	}

	var out strings.Builder
	segmentEndsNewline := true
	for _, date := range dates {
		raw, err := s.ReadRawJSONL(date, taskID)
		if err != nil {
			return "", nil, err
		}
		if raw == "" {
			continue
		}
		// Keep the concatenation line-delimited even if a segment was
		// written without a trailing newline.
		if out.Len() > 0 && !segmentEndsNewline {
			out.WriteByte('\n')
		}
		out.WriteString(raw)
		segmentEndsNewline = strings.HasSuffix(raw, "\n")
	}
	return out.String(), dates, nil
}

// hasLogFile reports whether a task directory holds a log file in either the
// current (bzip2) or the legacy uncompressed format.
func hasLogFile(taskDir string) bool {
	for _, name := range []string{"logs.jsonl.bz2", "logs.jsonl"} {
		if _, err := os.Stat(filepath.Join(taskDir, name)); err == nil {
			return true
		}
	}
	return false
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

// Match is one log line that satisfied a search query, together with up to
// ContextLines neighbouring lines from the same trace on either side.
type Match struct {
	TaskID        string    `json:"task_id"`
	Date          string    `json:"date"`
	Timestamp     time.Time `json:"timestamp"`
	Line          string    `json:"line"`
	ContextBefore []string  `json:"context_before"`
	ContextAfter  []string  `json:"context_after"`
}

// SearchDates scans the given date partitions for log lines containing the
// query (case-insensitive substring) and returns the matches ordered by
// date, then task ID, then position within the trace. Only the dates handed
// in are read — window enforcement is the caller's job (issue #269 keeps it
// server-side in the HTTP handler), and a date that has been pruned or never
// existed simply yields no matches. Context lines come from the same trace
// and are clipped at either end.
func (s *LogStore) SearchDates(dates []string, query string, contextLines int) ([]Match, error) {
	if query == "" {
		return nil, fmt.Errorf("search query must not be empty")
	}
	needle := strings.ToLower(query)

	var matches []Match
	for _, date := range dates {
		tasks, err := s.ListTasks(date)
		if err != nil {
			return nil, err
		}
		sort.Strings(tasks)
		for _, taskID := range tasks {
			entries, err := s.ReadLogs(date, taskID)
			if err != nil {
				return nil, err
			}
			for i, e := range entries {
				if !strings.Contains(strings.ToLower(e.Line), needle) {
					continue
				}
				matches = append(matches, Match{
					TaskID:        taskID,
					Date:          date,
					Timestamp:     e.Timestamp,
					Line:          e.Line,
					ContextBefore: linesBefore(entries, i, contextLines),
					ContextAfter:  linesAfter(entries, i, contextLines),
				})
			}
		}
	}
	return matches, nil
}

// linesBefore returns up to n entry lines immediately preceding index i,
// oldest first, clipped at the start of the trace.
func linesBefore(entries []Entry, i, n int) []string {
	start := i - n
	if start < 0 {
		start = 0
	}
	out := make([]string, 0, i-start)
	for _, e := range entries[start:i] {
		out = append(out, e.Line)
	}
	return out
}

// linesAfter returns up to n entry lines immediately following index i,
// in trace order, clipped at the end of the trace.
func linesAfter(entries []Entry, i, n int) []string {
	end := i + 1 + n
	if end > len(entries) {
		end = len(entries)
	}
	out := make([]string, 0, end-i-1)
	for _, e := range entries[i+1 : end] {
		out = append(out, e.Line)
	}
	return out
}
