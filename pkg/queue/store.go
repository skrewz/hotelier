package queue

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// statusDirs maps the non-terminal task statuses to the subdirectory in
// which their JSON files are stored. Terminal tasks are not persisted —
// only the unprocessed queue (issue #190).
var statusDirs = map[TaskStatus]string{
	TaskStatusPending:  "pending",
	TaskStatusAssigned: "assigned",
	TaskStatusRunning:  "running",
}

// statusDirNames is the fixed iteration order for Load.
var statusDirNames = []string{"pending", "assigned", "running"}

// Store persists non-terminal tasks to a directory of JSON files, one file
// per task, organised into per-status subdirectories:
//
//	<dir>/pending/<taskID>.json
//	<dir>/assigned/<taskID>.json
//	<dir>/running/<taskID>.json
//
// Each file contains the full task as JSON. Save is atomic per file
// (write to a temp file, then rename) and self-cleaning: saving a task in a
// new status removes any stale copy in another status directory. The new
// file is written before the old one is removed, so a crash in between
// leaves a duplicate rather than a lost task; Load resolves duplicates by
// preferring the most recently modified file.
type Store struct {
	dir  string
	logf func(format string, args ...interface{})
}

// NewStore creates a queue store rooted at dir, creating the directory (and
// its status subdirectories) if they do not exist.
func NewStore(dir string, logf func(format string, args ...interface{})) (*Store, error) {
	if logf == nil {
		logf = func(format string, args ...interface{}) {}
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create queue store dir: %w", err)
	}
	for _, name := range statusDirNames {
		if err := os.MkdirAll(filepath.Join(dir, name), 0o755); err != nil {
			return nil, fmt.Errorf("create queue store subdir %s: %w", name, err)
		}
	}
	return &Store{dir: dir, logf: logf}, nil
}

// Dir returns the store's root directory.
func (s *Store) Dir() string {
	return s.dir
}

// Save persists a non-terminal task, replacing any previously saved copy
// (regardless of which status directory it was in). Saving a task in a
// terminal status is an error — terminal tasks must be removed with Delete.
func (s *Store) Save(task *Task) error {
	subdir, ok := statusDirs[task.Status]
	if !ok {
		return fmt.Errorf("cannot persist task %s in terminal status %s", task.ID, task.Status)
	}

	data, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("marshal task %s: %w", task.ID, err)
	}

	// Write the new file first (atomic: temp + rename) so a crash before
	// the stale copies are removed leaves a duplicate, not a lost task.
	dest := filepath.Join(s.dir, subdir, task.ID+".json")
	tmp := dest + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("write task %s: %w", task.ID, err)
	}
	if err := os.Rename(tmp, dest); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("rename task %s: %w", task.ID, err)
	}

	// Remove stale copies in the other status directories.
	for _, name := range statusDirNames {
		if name == subdir {
			continue
		}
		stale := filepath.Join(s.dir, name, task.ID+".json")
		if err := os.Remove(stale); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove stale task %s from %s: %w", task.ID, name, err)
		}
	}
	return nil
}

// Delete removes a task from the store. Deleting a task that is not
// present is not an error.
func (s *Store) Delete(taskID string) error {
	for _, name := range statusDirNames {
		path := filepath.Join(s.dir, name, taskID+".json")
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove task %s from %s: %w", taskID, name, err)
		}
	}
	return nil
}

// Load reads every persisted task. Corrupt files are skipped (with a log
// message) so a single bad file cannot block startup. If the same task ID
// appears in more than one status directory (a crash between the write and
// the stale-file removal in Save), the most recently modified copy wins.
func (s *Store) Load() ([]*Task, error) {
	byID := make(map[string]*Task)
	var order []string

	for _, name := range statusDirNames {
		entries, err := os.ReadDir(filepath.Join(s.dir, name))
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return nil, fmt.Errorf("read %s dir: %w", name, err)
		}
		for _, entry := range entries {
			if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
				continue
			}
			path := filepath.Join(s.dir, name, entry.Name())

			data, err := os.ReadFile(path)
			if err != nil {
				s.logf("queue store: cannot read %s: %v", path, err)
				continue
			}
			var task Task
			if err := json.Unmarshal(data, &task); err != nil {
				s.logf("queue store: skipping corrupt file %s: %v", path, err)
				continue
			}
			if task.ID == "" {
				s.logf("queue store: skipping file %s: empty task id", path)
				continue
			}

			if existing, ok := byID[task.ID]; ok {
				// Duplicate (crash mid-Save): keep the newer copy.
				info, err := os.Stat(path)
				if err != nil {
					continue
				}
				existingInfo, _ := os.Stat(filepath.Join(s.dir, statusDirs[existing.Status], existing.ID+".json"))
				if existingInfo != nil && !info.ModTime().After(existingInfo.ModTime()) {
					continue
				}
				s.logf("queue store: duplicate entry for task %s (%s vs %s), keeping the newest", task.ID, name, statusDirs[existing.Status])
			}
			if _, exists := byID[task.ID]; !exists {
				order = append(order, task.ID)
			}
			byID[task.ID] = &task
		}
	}

	tasks := make([]*Task, 0, len(byID))
	for _, id := range order {
		tasks = append(tasks, byID[id])
	}
	return tasks, nil
}
