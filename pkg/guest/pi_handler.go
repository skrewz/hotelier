package guest

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"hotelier/pkg/pi"
)

// parseRepoRef splits a repo reference into URL and optional branch/ref.
// Supported formats:
//
//	https://github.com/user/repo@branch-name
//	https://github.com/user/repo@abc123def  (commit SHA)
//	git@github.com:user/repo@branch-name    (SSH with ref)
//	https://github.com/user/repo            (no ref — default branch)
//	git@github.com:user/repo                (SSH, no ref)
//
// Returns (url, ref). If no ref is specified, ref is an empty string.
func parseRepoRef(repo string) (url, ref string) {
	// Handle SSH URLs first: git@host:path/repo@ref
	if strings.HasPrefix(repo, "git@") {
		colonIdx := strings.Index(repo, ":")
		if colonIdx < 0 {
			return repo, ""
		}
		pathPart := repo[colonIdx+1:]
		refIdx := strings.LastIndex(pathPart, "@")
		if refIdx >= 0 {
			return repo[:colonIdx+1] + pathPart[:refIdx], pathPart[refIdx+1:]
		}
		return repo, ""
	}

	// Handle HTTPS/HTTP URLs: https://host/user/repo@ref
	if idx := strings.LastIndex(repo, "@"); idx > 0 {
		ref = repo[idx+1:]
		url = repo[:idx]
		if ref != "" {
			return url, ref
		}
	}

	return repo, ""
}

// PIHandler executes tasks using the pi AI guest via RPC subprocess.
type PIHandler struct {
	baseCWD string // original working directory, used for path resolution
	client  *pi.PiClient
	log     *log.Logger
	debug   bool
	debugMu sync.Mutex
	mu      sync.Mutex
}

// NewPIHandler creates a new PIHandler.
func NewPIHandler(cwd string, provider, model, thinkingLevel string) *PIHandler {
	return NewPIHandlerDebug(cwd, provider, model, thinkingLevel, false)
}

// BaseCWD returns the original working directory set during construction.
func (h *PIHandler) BaseCWD() string {
	return h.baseCWD
}

// NewPIHandlerDebug creates a new PIHandler with optional RPC debug logging.
// When debug is true, all RPC communication is logged to stdout.
func NewPIHandlerDebug(cwd string, provider, model, thinkingLevel string, debug bool) *PIHandler {
	logger := log.New(os.Stdout, "[pi-handler] ", log.LstdFlags)
	cfg := pi.PiClientConfig{
		CWD:           cwd,
		Provider:      provider,
		Model:         model,
		ThinkingLevel: thinkingLevel,
		Log:           logger,
		Debug:         debug,
	}
	return &PIHandler{
		baseCWD: cwd,
		client:  pi.NewClient(cfg),
		log:     logger,
		debug:   debug,
	}
}

// Start initializes the pi RPC subprocess.
func (h *PIHandler) Start(ctx context.Context) error {
	if err := h.client.Start(ctx); err != nil {
		return fmt.Errorf("start pi client: %w", err)
	}
	h.log.Printf("pi client started")
	if h.debug {
		h.log.Printf("[DEBUG] RPC debug logging enabled")
	}
	return nil
}

// Stop terminates the pi RPC subprocess.
func (h *PIHandler) Stop(ctx context.Context) {
	h.log.Printf("stopping pi client")
	done := make(chan struct{})
	go func() {
		_ = h.client.Stop(ctx)
		close(done)
	}()

	select {
	case <-done:
		h.log.Printf("pi client stopped")
	case <-time.After(10 * time.Second):
		h.log.Printf("pi client stop timed out, forcing cleanup")
		// Force kill by terminating the process directly
		if h.client != nil && h.client.GetProcessState() == nil {
			_ = h.client.Cmd().Process.Kill()
		}
	}
}

// IsRunning returns whether the handler is running.
func (h *PIHandler) IsRunning() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.client != nil && h.client.IsRunning()
}

// GetClient returns the underlying pi client (for testing).
func (h *PIHandler) GetClient() *pi.PiClient {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.client
}

// ExecuteTask runs a task through the pi guest and streams logs back via the callback.
// It clones any remote repos into a per-task working directory, sets that as the
// CWD for the pi subprocess, and logs all commands it spawns.
func (h *PIHandler) ExecuteTask(ctx context.Context, task TaskAssignment, sendLog func(LogEntry) error) (*TaskResult, error) {
	h.mu.Lock()
	if h.client == nil || !h.client.IsRunning() {
		h.mu.Unlock()
		return nil, fmt.Errorf("pi client not running")
	}
	h.mu.Unlock()

	h.log.Printf("[PI] executing task %s", task.TaskID)
	h.log.Printf("[PI] repos: %v", task.Repos)
	h.log.Printf("[PI] prompt: %s", task.Prompt)

	// Send operational log that task execution is starting.
	if err := sendLog(LogEntry{TaskID: task.TaskID, Line: fmt.Sprintf("Executing task %s", task.TaskID), Level: "system"}); err != nil {
		h.log.Printf("[PI] failed to send operational log: %v", err)
	}

	// Clone any remote repos and determine the working directory.
	workDir, err := h.prepareRepos(ctx, task.TaskID, task.Repos, sendLog)
	if err != nil {
		return nil, fmt.Errorf("prepare repos: %w", err)
	}

	// Clean up the task directory when the task completes (success or failure).
	// Guests should clean up after themselves.
	defer func() {
		if err := h.cleanupTaskDir(task.TaskID); err != nil {
			h.log.Printf("[CLEANUP] failed to remove task directory: %v", err)
		}
	}()

	// Build the prompt for pi — include repo context
	prompt := h.buildPrompt(task)

	// Recreate the pi client with the task-specific working directory.
	// We need to do this because the client was created with the base CWD,
	// but we want the pi subprocess to run inside the cloned repo tree.
	h.log.Printf("[PI] spawning pi subprocess in: %s", workDir)
	if err := sendLog(LogEntry{TaskID: task.TaskID, Line: fmt.Sprintf("Spawning pi subprocess in: %s", workDir), Level: "system"}); err != nil {
		h.log.Printf("[PI] failed to send operational log: %v", err)
	}
	if err := h.resetClient(ctx, workDir); err != nil {
		h.log.Printf("[PI] spawn failed: %v", err)
		_ = sendLog(LogEntry{TaskID: task.TaskID, Line: fmt.Sprintf("Spawn failed: %v", err), Level: "error"})
		return nil, fmt.Errorf("reset pi client with working dir %s: %w", workDir, err)
	}
	h.log.Printf("[PI] spawn succeeded, client running: %v", h.client.IsRunning())
	if err := sendLog(LogEntry{TaskID: task.TaskID, Line: "Pi subprocess spawned successfully", Level: "system"}); err != nil {
		h.log.Printf("[PI] failed to send operational log: %v", err)
	}

	// Verify the client is still running before sending the prompt.
	// The process could have started and then immediately crashed.
	if !h.client.IsRunning() {
		err := fmt.Errorf("pi client not running after spawn")
		h.log.Printf("[PI] %v", err)
		_ = sendLog(LogEntry{TaskID: task.TaskID, Line: err.Error(), Level: "error"})
		return nil, err
	}

	// Send the prompt
	h.log.Printf("[PI] sending prompt to pi")
	if err := sendLog(LogEntry{TaskID: task.TaskID, Line: fmt.Sprintf("Prompt: %s", prompt), Level: "system"}); err != nil {
		h.log.Printf("[PI] failed to send operational log: %v", err)
	}
	if err := h.client.Prompt(ctx, prompt); err != nil {
		return nil, fmt.Errorf("send prompt: %w", err)
	}
	h.log.Printf("[PI] prompt sent, waiting for events")
	if err := sendLog(LogEntry{TaskID: task.TaskID, Line: "Prompt sent, waiting for events", Level: "system"}); err != nil {
		h.log.Printf("[PI] failed to send operational log: %v", err)
	}

	// Collect streaming output
	var mu sync.Mutex
	var output strings.Builder

	// Subscribe to events
	eventCh := h.client.Subscribe()

	done := make(chan struct{})

	// Track last activity for idle detection
	var lastActivityMu sync.Mutex
	lastActivity := time.Now()
	updateActivity := func() {
		lastActivityMu.Lock()
		defer lastActivityMu.Unlock()
		lastActivity = time.Now()
	}
	getLastActivity := func() time.Time {
		lastActivityMu.Lock()
		defer lastActivityMu.Unlock()
		return lastActivity
	}

	go func() {
		defer close(done)
		eventCount := 0
		for event := range eventCh {
			eventCount++
			updateActivity()

			// Log significant events; suppress message_update noise
			if event.Type != "message_update" {
				h.log.Printf("[PI] event #%d: type=%s", eventCount, event.Type)
			}

			if pi.IsGuestEnd(event) {
				// Text deltas have already been streamed via sendLog.
				// No need to re-send the final text — it would duplicate.
				return
			}

			if pi.IsThinkingDelta(event) {
				delta := pi.ExtractThinkingDelta(event)
				if delta != "" {
					// Send thinking deltas as "thinking" level.
					// The server-side accumulator will batch these into complete messages.
					if err := sendLog(LogEntry{TaskID: task.TaskID, Line: delta, Level: "thinking"}); err != nil {
						h.log.Printf("[PI] failed to send thinking log: %v", err)
					}
				}
				continue
			}

			if pi.IsTextDelta(event) {
				delta := pi.ExtractTextDelta(event)
				if delta != "" {
					mu.Lock()
					output.WriteString(delta)
					mu.Unlock()

					// Send the full delta as one log entry, preserving newlines.
					// The server-side accumulator will batch these into complete messages.
					if err := sendLog(LogEntry{TaskID: task.TaskID, Line: delta, Level: "text"}); err != nil {
						h.log.Printf("[PI] failed to send log: %v", err)
					}
				}
			}

			if pi.IsToolExecution(event) {
				toolName := pi.ToolName(event)
				toolID := pi.ToolCallId(event)

				switch event.Type {
				case "tool_execution_start":
					args := pi.ToolArgs(event)
					entry := LogEntry{
						TaskID:   task.TaskID,
						Line:     fmt.Sprintf("[TOOL_START] %s: %s (id: %s)", toolName, args, toolID),
						Level:    "tool",
						ToolType: "start",
						ToolName: toolName,
						ToolID:   toolID,
						ToolArgs: args,
					}
					if err := sendLog(entry); err != nil {
						h.log.Printf("[PI] failed to send tool log: %v", err)
					}
					h.log.Printf("[TOOL] %s started (id: %s)", toolName, toolID)

				case "tool_execution_update":
					partial := pi.ToolPartialResult(event)
					if partial != "" {
						entry := LogEntry{
							TaskID:     task.TaskID,
							Line:       fmt.Sprintf("[TOOL_OUTPUT] %s (id: %s): %s", toolName, toolID, partial),
							Level:      "tool",
							ToolType:   "output",
							ToolName:   toolName,
							ToolID:     toolID,
							ToolOutput: partial,
						}
						if err := sendLog(entry); err != nil {
							h.log.Printf("[PI] failed to send tool output: %v", err)
						}
					}

				case "tool_execution_end":
					result := pi.ToolResult(event)
					isErr := event.IsError
					var line string
					if isErr {
						line = fmt.Sprintf("[TOOL_END] %s (id: %s) [ERROR]", toolName, toolID)
					} else if result != "" {
						line = fmt.Sprintf("[TOOL_END] %s (id: %s): %s", toolName, toolID, result)
					} else {
						line = fmt.Sprintf("[TOOL_END] %s (id: %s)", toolName, toolID)
					}
					entry := LogEntry{
						TaskID:     task.TaskID,
						Line:       line,
						Level:      "tool",
						ToolType:   "end",
						ToolName:   toolName,
						ToolID:     toolID,
						ToolOutput: result,
						ToolError:  isErr,
					}
					if err := sendLog(entry); err != nil {
						h.log.Printf("[PI] failed to send tool end log: %v", err)
					}
					h.log.Printf("[TOOL] %s ended (id: %s) elapsed=%s", toolName, toolID, time.Since(lastActivity))
				}
			}
		}
	}()

	// Wait for completion or context cancellation.
	// The context is driven by the guest's task timeout (configurable) or
	// by a server-sent task.cancel RPC (via silence detection). No hardcoded
	// timeout here — the server's silence detection replaces the old fixed limit.
	idleCheck := time.NewTicker(30 * time.Second)
	defer idleCheck.Stop()

	for {
		select {
		case <-done:
			idleCheck.Stop()
			// Agent completed
			return &TaskResult{
				TaskID:  task.TaskID,
				Success: true,
				Output:  output.String(),
			}, nil
		case <-ctx.Done():
			idleCheck.Stop()
			h.log.Printf("[PI] task %s cancelled by context", task.TaskID)
			_ = h.client.Abort()
			return &TaskResult{
				TaskID:  task.TaskID,
				Success: false,
				Error:   "task cancelled",
			}, nil
		case <-idleCheck.C:
			idle := time.Since(getLastActivity())
			if idle > 3*time.Minute {
				h.log.Printf("[PI] task %s idle for %s — agent may be stuck", task.TaskID, idle)
				_ = h.client.Abort()
				return &TaskResult{
					TaskID:  task.TaskID,
					Success: false,
					Error:   fmt.Sprintf("agent idle for %s", idle.Round(time.Second)),
				}, nil
			}
		}
	}
}

// prepareRepos clones repos into a task-specific directory and
// returns the path to use as the working directory for the pi subprocess.
// All repos are treated as git URLs and cloned; local paths are not supported.
func (h *PIHandler) prepareRepos(ctx context.Context, taskID string, repos []string, sendLog func(LogEntry) error) (string, error) {
	if len(repos) == 0 {
		// No repos — create a task-specific directory so the guest has a
		// clean, isolated working directory instead of the base CWD.
		taskDir := filepath.Join(h.baseCWD, "tasks", taskID)
		if err := os.MkdirAll(taskDir, 0o755); err != nil {
			return "", fmt.Errorf("create task dir %s: %w", taskDir, err)
		}
		h.log.Printf("[WORKDIR] no repos specified, using task dir: %s", taskDir)
		if sendLog != nil {
			_ = sendLog(LogEntry{TaskID: taskID, Line: fmt.Sprintf("Using task directory: %s", taskDir), Level: "system"})
		}
		return taskDir, nil
	}

	// Create a task-specific directory under the base CWD.
	taskDir := filepath.Join(h.baseCWD, "tasks", taskID)
	if err := os.MkdirAll(taskDir, 0o755); err != nil {
		return "", fmt.Errorf("create task dir %s: %w", taskDir, err)
	}

	var workDir string

	for _, repo := range repos {
		repoURL, repoRef := parseRepoRef(repo)
		repoName := filepath.Base(strings.TrimSuffix(repoURL, ".git"))
		clonePath := filepath.Join(taskDir, repoName)
		h.log.Printf("[GIT] cloning %s -> %s", repo, clonePath)
		if sendLog != nil {
			_ = sendLog(LogEntry{TaskID: taskID, Line: fmt.Sprintf("Cloning %s -> %s", repo, clonePath), Level: "system"})
		}
		cloneArgs := []string{"clone", "--depth", "1"}
		if repoRef != "" {
			cloneArgs = append(cloneArgs, "--branch", repoRef)
		}
		cloneArgs = append(cloneArgs, repoURL, clonePath)
		cloneCmd := exec.CommandContext(ctx, "git", cloneArgs...)
		out, err := cloneCmd.CombinedOutput()
		if err != nil {
			return "", fmt.Errorf("git clone %s: %w (output: %s)", repo, err, string(out))
		}
		h.log.Printf("[GIT] cloned %s", repo)
		if sendLog != nil {
			_ = sendLog(LogEntry{TaskID: taskID, Line: fmt.Sprintf("Cloned %s", repo), Level: "system"})
		}
		if workDir == "" {
			// Use the cloned repo as the working directory.
			// If multiple repos are specified, pick the first one.
			workDir = clonePath
		}
	}

	// If no repos were cloned, use taskDir as the working directory.
	if workDir == "" {
		workDir = taskDir
	}

	return workDir, nil
}

// resetClient restarts the pi subprocess with a new working directory.
// This is needed per-task so the guest operates inside the cloned repo tree.
func (h *PIHandler) resetClient(ctx context.Context, workDir string) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	// Stop the old client if it's running
	if h.client != nil && h.client.IsRunning() {
		if err := h.client.Stop(ctx); err != nil {
			h.log.Printf("[PI] stopping old client: %v", err)
		}
	}

	// Create a new client with the task-specific working directory
	h.log.Printf("[PI] creating new pi client for working dir: %s", workDir)
	cfg := pi.PiClientConfig{
		CWD:           workDir,
		Provider:      "",
		Model:         "",
		ThinkingLevel: "",
		Log:           h.log,
	}
	h.client = pi.NewClient(cfg)
	if err := h.client.Start(ctx); err != nil {
		return fmt.Errorf("start pi client in %s: %w", workDir, err)
	}
	if !h.client.IsRunning() {
		return fmt.Errorf("pi client in %s: started but not running", workDir)
	}
	h.log.Printf("[PI] new pi subprocess started in: %s", workDir)
	return nil
}

// cleanupTaskDir removes the task directory and all its contents.
// Guests clean up after themselves so stale directories don't accumulate.
// This method is safe to call even if the directory does not exist.
func (h *PIHandler) cleanupTaskDir(taskID string) error {
	taskDir := filepath.Join(h.baseCWD, "tasks", taskID)
	h.log.Printf("[CLEANUP] removing task directory: %s", taskDir)
	if err := os.RemoveAll(taskDir); err != nil {
		return fmt.Errorf("cleanup task dir %s: %w", taskDir, err)
	}
	h.log.Printf("[CLEANUP] task directory removed: %s", taskDir)
	return nil
}

// buildPrompt constructs the prompt for pi with repo context.
// The user's prompt is always placed first so that template commands
// (e.g. /repo-ideation) start at the top of the message and are
// expanded by pi.  Context tidbits are appended after.
func (h *PIHandler) buildPrompt(task TaskAssignment) string {
	var parts []string

	parts = append(parts, task.Prompt)

	if len(task.Repos) > 0 {
		parts = append(parts, fmt.Sprintf("Working repositories: %s", strings.Join(task.Repos, ", ")))
	}
	if len(task.Tags) > 0 {
		parts = append(parts, fmt.Sprintf("Required tags: %s", strings.Join(task.Tags, ", ")))
	}

	return strings.Join(parts, "\n\n")
}
