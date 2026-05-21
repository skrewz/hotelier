package config

import (
	"log"
	"os"
	"path/filepath"

	"github.com/fsnotify/fsnotify"
)

// ConfigWatcher watches a configuration file for changes and re-parses it
// whenever the file is modified. It exposes a channel that delivers updated
// configuration values.
type ConfigWatcher struct {
	watcher     *fsnotify.Watcher
	configPath  string
	logger      *log.Logger
	reloadFn    func(path string) (interface{}, error)
	lastContent []byte
}

// NewConfigWatcher creates a new ConfigWatcher that watches the file at
// configPath and re-parses it using reloadFn on every change.
//
// reloadFn should load defaults first, then unmarshal the file contents
// (matching the pattern used by LoadServerConfig / LoadGuestConfig).
func NewConfigWatcher(configPath string, logger *log.Logger, reloadFn func(path string) (interface{}, error)) (*ConfigWatcher, error) {
	absPath, err := filepath.Abs(configPath)
	if err != nil {
		return nil, err
	}

	w, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}

	// Watch the directory containing the file so that rename-based edits
	// (used by editors like vim, VS Code, etc.) are caught.
	dir := filepath.Dir(absPath)
	if err := w.Add(dir); err != nil {
		w.Close()
		return nil, err
	}

	cw := &ConfigWatcher{
		watcher:     w,
		configPath:  absPath,
		logger:      logger,
		reloadFn:    reloadFn,
		lastContent: nil,
	}

	// Do an initial load to establish a baseline and push it to the channel.
	if _, err := cw.reload(); err != nil {
		w.Close()
		return nil, err
	}

	return cw, nil
}

// ReloadAndSend reloads the config from disk and sends it on updateCh.
// This is useful for the initial load so callers get the first config value.
func (w *ConfigWatcher) ReloadAndSend(updateCh chan<- interface{}) (interface{}, error) {
	cfg, err := w.reload()
	if err != nil {
		return nil, err
	}
	if cfg != nil {
		updateCh <- cfg
	}
	return cfg, nil
}

// Run starts the watch loop. It blocks until the watcher is closed or an
// error occurs. Call this in a goroutine. Updates are sent on updateCh
// whenever the config file is successfully reloaded.
func (w *ConfigWatcher) Run(updateCh chan<- interface{}) {
	for {
		select {
		case event, ok := <-w.watcher.Events:
			if !ok {
				return
			}
			// Only act on write/close-write events for the watched file.
			if event.Name == w.configPath &&
				(event.Has(fsnotify.Write) || event.Has(fsnotify.Create) || event.Has(fsnotify.Chmod)) {
				updated, err := w.reload()
				if err != nil {
					w.logger.Printf("config reload error: %v", err)
					continue
				}
				// Skip sending if content is unchanged (duplicate events from
				// editors triggering WRITE+CHMOD for the same write).
				if updated == nil {
					continue
				}
				w.logger.Printf("config reloaded from %s", w.configPath)
				updateCh <- updated
			}
		case err, ok := <-w.watcher.Errors:
			if !ok {
				return
			}
			w.logger.Printf("config watcher error: %v", err)
		}
	}
}

// Close stops the watcher and releases resources.
func (w *ConfigWatcher) Close() error {
	return w.watcher.Close()
}

// reload re-reads the config file, parses it, and stores the result.
// Returns the parsed config value.
func (w *ConfigWatcher) reload() (interface{}, error) {
	data, err := os.ReadFile(w.configPath)
	if err != nil {
		return nil, err
	}

	// Skip if content hasn't changed (avoids redundant reloads on some
	// editors that trigger multiple events).
	if w.lastContent != nil && string(w.lastContent) == string(data) {
		return nil, nil
	}

	cfg, err := w.reloadFn(w.configPath)
	if err != nil {
		return nil, err
	}

	w.lastContent = data
	return cfg, nil
}
