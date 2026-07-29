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

	"hotelier/pkg/persona"
	"hotelier/pkg/pi"
)

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
// It creates a per-task working directory and sets that as the CWD for the pi subprocess.
// If the pi client is not running when ExecuteTask is called, it attempts to restart
// the client before proceeding. This handles the case where the subprocess was killed
// externally (e.g. pkill pi) or crashed between tasks.
//
// If the task has a persona, the persona's files are copied into the working directory
// and its environment variables are applied to the pi subprocess.
func (h *PIHandler) ExecuteTask(ctx context.Context, task TaskAssignment, sendLog func(LogEntry) error) (*TaskResult, error) {
	h.mu.Lock()
	clientRunning := h.client != nil && h.client.IsRunning()
	h.mu.Unlock()

	if !clientRunning {
		h.log.Printf("[PI] pi client not running, attempting restart")
		if err := sendLog(LogEntry{TaskID: task.TaskID, Line: "Pi client not running, attempting restart", Level: "warning"}); err != nil {
			h.log.Printf("[PI] failed to send restart warning log: %v", err)
		}
		if err := h.restartClient(ctx); err != nil {
			return nil, fmt.Errorf("pi client not running and restart failed: %w", err)
		}
		h.log.Printf("[PI] pi client restarted successfully")
		if err := sendLog(LogEntry{TaskID: task.TaskID, Line: "Pi client restarted successfully", Level: "system"}); err != nil {
			h.log.Printf("[PI] failed to send restart success log: %v", err)
		}
	}

	h.log.Printf("[PI] executing task %s", task.TaskID)
	h.log.Printf("[PI] prompt: %s", task.Prompt)

	// Send operational log that task execution is starting.
	if err := sendLog(LogEntry{TaskID: task.TaskID, Line: fmt.Sprintf("Executing task %s", task.TaskID), Level: "system"}); err != nil {
		h.log.Printf("[PI] failed to send operational log: %v", err)
	}

	// Apply persona logging (the actual file application happens inside
	// prepareTaskDir — before the clone for repo_ref, or unconditionally
	// otherwise — so credentials are available for git auth).
	if task.Persona != nil {
		h.log.Printf("[PERSONA] applying persona %q for task %s", task.Persona.Name, task.TaskID)
		if err := sendLog(LogEntry{TaskID: task.TaskID, Line: fmt.Sprintf("Applying persona: %s", task.Persona.Name), Level: "system"}); err != nil {
			h.log.Printf("[PERSONA] failed to send persona log: %v", err)
		}
	}

	// Create a task-specific working directory.
	// If the task has a repo_ref, the repo is cloned here.
	// If a persona is specified, its files are applied before the clone
	// (for credentials) and re-applied after (for overlays).
	// Clean up the task directory when the task completes (success or failure).
	// Guests should clean up after themselves. Set up the defer before
	// prepareTaskDir so the error path also gets cleaned up (e.g. if
	// cloneRepo fails and leaves an empty task directory behind).
	defer func() {
		if err := h.cleanupTaskDir(task.TaskID); err != nil {
			h.log.Printf("[CLEANUP] failed to remove task directory: %v", err)
		}
	}()

	workDir, err := h.prepareTaskDir(ctx, task.TaskID, task.RepoRef, sendLog, task.Persona)
	if err != nil {
		return nil, fmt.Errorf("prepare task dir: %w", err)
	}

	// Resolve persona env vars for the pi subprocess.
	// Files were already applied inside prepareTaskDir for both
	// the repo and non-repo paths.
	var personaEnv map[string]string
	if task.Persona != nil {
		personaEnv = task.Persona.ResolvedEnv(workDir)
		h.log.Printf("[PERSONA] resolved persona %q: %d env vars, %d file copies", task.Persona.Name, len(personaEnv), len(task.Persona.Files))
		if err := sendLog(LogEntry{TaskID: task.TaskID, Line: fmt.Sprintf("Persona %q applied: %d env vars, %d file copies", task.Persona.Name, len(personaEnv), len(task.Persona.Files)), Level: "system"}); err != nil {
			h.log.Printf("[PERSONA] failed to send apply log: %v", err)
		}
	}

	// Build the prompt for pi — include repo context
	prompt := h.buildPrompt(task)

	// Recreate the pi client with the task-specific working directory.
	// We need to do this because the client was created with the base CWD,
	// but we want the pi subprocess to run inside the cloned repo tree.
	h.log.Printf("[PI] spawning pi subprocess in: %s", workDir)
	if err := sendLog(LogEntry{TaskID: task.TaskID, Line: fmt.Sprintf("Spawning pi subprocess in: %s", workDir), Level: "system"}); err != nil {
		h.log.Printf("[PI] failed to send operational log: %v", err)
	}
	// Build the task environment: TMPDIR for isolation + persona env vars.
	// TMPDIR points to a per-task temp directory to prevent resource contention
	// between concurrent tasks. See issue #59.
	taskEnv := map[string]string{
		"TMPDIR": filepath.Join(workDir, "tmp"),
	}
	// Merge persona env vars (persona vars take precedence over defaults)
	for k, v := range personaEnv {
		taskEnv[k] = v
	}

	if err := h.resetClientWithEnv(ctx, workDir, task.TaskID, sendLog, taskEnv); err != nil {
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

	// Track whether pi sent a guest_end/agent_end event before exiting.
	// If not, the process crashed or was killed — an abnormal exit.
	// Ordering: goroutine sets guestEndReceived → returns → deferred close(done)
	// fires → main loop receives on done → reads guestEndReceived.
	// The channel close provides the happens-before guarantee.
	var guestEndReceived bool

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
				// Mark that pi completed normally.
				guestEndReceived = true
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

			// Capture exit diagnostics
			diagnostics := h.captureExitDiagnostics(guestEndReceived)

			// If pi did not send guest_end, it crashed or was killed.
			// Mark the task as failed with diagnostic information.
			if !guestEndReceived {
				h.log.Printf("[PI] task %s: pi exited without guest_end (abnormal exit)", task.TaskID)
				h.log.Printf("[PI] exit code: %d, exit error: %v", diagnostics.ExitCode, diagnostics.ExitError)
				if len(diagnostics.StderrLines) > 0 {
					start := max(0, len(diagnostics.StderrLines)-3)
					h.log.Printf("[PI] last stderr lines: %v", diagnostics.StderrLines[start:])
				}
				if len(diagnostics.LastEventTypes) > 0 {
					start := max(0, len(diagnostics.LastEventTypes)-3)
					h.log.Printf("[PI] last event types: %v", diagnostics.LastEventTypes[start:])
				}

				// Append diagnostics to output for visibility
				mu.Lock()
				output.WriteString("\n\n--- Exit Diagnostics ---\n")
				output.WriteString(fmt.Sprintf("Exit code: %d\n", diagnostics.ExitCode))
				if diagnostics.ExitError != "" {
					output.WriteString(fmt.Sprintf("Exit error: %s\n", diagnostics.ExitError))
				}
				if len(diagnostics.StderrLines) > 0 {
					output.WriteString(fmt.Sprintf("Stderr (%d lines):\n", len(diagnostics.StderrLines)))
					for _, line := range diagnostics.StderrLines {
						output.WriteString(fmt.Sprintf("  %s\n", line))
					}
				}
				if len(diagnostics.LastEventTypes) > 0 {
					output.WriteString(fmt.Sprintf("Last event types: %v\n", diagnostics.LastEventTypes))
				}
				mu.Unlock()

				return &TaskResult{
					TaskID:      task.TaskID,
					Success:     false,
					Output:      output.String(),
					Error:       "pi subprocess exited without completing (no guest_end received)",
					Diagnostics: diagnostics,
				}, nil
			}

			// Normal completion — still attach diagnostics for reference
			return &TaskResult{
				TaskID:      task.TaskID,
				Success:     true,
				Output:      output.String(),
				Diagnostics: diagnostics,
			}, nil
		case <-ctx.Done():
			idleCheck.Stop()
			h.log.Printf("[PI] task %s cancelled by context", task.TaskID)
			_ = h.client.Abort()

			// Capture diagnostics even on cancellation
			diagnostics := h.captureExitDiagnostics(guestEndReceived)
			return &TaskResult{
				TaskID:      task.TaskID,
				Success:     false,
				Error:       "task cancelled",
				Diagnostics: diagnostics,
			}, nil
		case <-idleCheck.C:
			idle := time.Since(getLastActivity())
			if idle > 10*time.Minute {
				h.log.Printf("[PI] task %s idle for %s — agent may be stuck", task.TaskID, idle)
				_ = h.client.Abort()

				// Capture diagnostics even on idle timeout
				diagnostics := h.captureExitDiagnostics(guestEndReceived)
				return &TaskResult{
					TaskID:      task.TaskID,
					Success:     false,
					Error:       fmt.Sprintf("agent idle for %s", idle.Round(time.Second)),
					Diagnostics: diagnostics,
				}, nil
			}
		}
	}
}

// prepareTaskDir creates a task-specific directory and returns
// the path to use as the working directory for the pi subprocess.
// If repoRef is non-empty, the repository is cloned into the task directory
// using git clone. If a persona is provided, its files are applied before
// the clone (for credentials) and re-applied after (for overlays like
// AGENTS.md). The persona's resolved environment variables are passed to
// the git clone command for authentication.
func (h *PIHandler) prepareTaskDir(ctx context.Context, taskID, repoRef string, sendLog func(LogEntry) error, persona *persona.Persona) (string, error) {
	taskDir := filepath.Join(h.baseCWD, "tasks", taskID)
	if err := os.MkdirAll(taskDir, 0o755); err != nil {
		return "", fmt.Errorf("create task dir %s: %w", taskDir, err)
	}

	// Create a tmp subdirectory for this task's TMPDIR.
	// This isolates temp files (git, npm, etc.) from other concurrent tasks.
	// See issue #59.
	tmpDir := filepath.Join(taskDir, "tmp")
	if err := os.MkdirAll(tmpDir, 0o755); err != nil {
		return "", fmt.Errorf("create task tmp dir %s: %w", tmpDir, err)
	}

	if repoRef != "" {
		// Apply persona files before clone so credentials are available.
		// (e.g. SSH keys, git configs)
		if persona != nil {
			if err := persona.ApplyFiles(taskDir); err != nil {
				return "", fmt.Errorf("apply persona files before clone: %w", err)
			}
		}

		// Resolve persona env for git clone credentials
		var personaEnv map[string]string
		if persona != nil {
			personaEnv = persona.ResolvedEnv(taskDir)
		}

		if err := h.cloneRepo(ctx, taskDir, repoRef, sendLog, personaEnv); err != nil {
			return "", err
		}

		// Diagnostic: log task dir state before re-applying persona.
		h.log.Printf("[WORKDIR] task dir state before re-apply persona:")
		filepath.Walk(taskDir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return nil
			}
			rel, _ := filepath.Rel(taskDir, path)
			h.log.Printf("[WORKDIR]   %s mode=%o", rel, info.Mode().Perm())
			return nil
		})

		// Re-apply persona files after clone. The clone may have overwritten
		// files that the persona wants to overlay (e.g. AGENTS.md).
		if persona != nil {
			if err := persona.ApplyFiles(taskDir); err != nil {
				return "", fmt.Errorf("apply persona files after clone: %w", err)
			}
		}

		return taskDir, nil
	}

	// Apply persona files when no repo is being cloned.
	// When repoRef is set, persona files are applied before and after
	// the clone inside the repoRef branch above.
	if persona != nil {
		if err := persona.ApplyFiles(taskDir); err != nil {
			return "", fmt.Errorf("apply persona files: %w", err)
		}
	}

	h.log.Printf("[WORKDIR] using task dir: %s", taskDir)
	if sendLog != nil {
		_ = sendLog(LogEntry{TaskID: taskID, Line: fmt.Sprintf("Using task directory: %s", taskDir), Level: "system"})
	}
	return taskDir, nil
}

// cloneRepo clones the specified git repository into the task directory.
// The personaEnv map is passed to the git clone command so that persona-
// provided credentials are available for authentication.
// After cloning, the task directory contains the repo contents and the
// pi subprocess will run inside it.
func (h *PIHandler) cloneRepo(ctx context.Context, taskDir, repoRef string, sendLog func(LogEntry) error, personaEnv map[string]string) error {
	h.log.Printf("[WORKDIR] cloning repo %q into %s", repoRef, taskDir)
	if sendLog != nil {
		_ = sendLog(LogEntry{TaskID: filepath.Base(taskDir), Line: fmt.Sprintf("Cloning repository: %s", repoRef), Level: "system"})
	}

	// Create a temporary directory for the clone, then move contents
	// into the task directory. This avoids issues with git refusing to
	// clone into a non-empty directory.
	tmpDir, err := os.MkdirTemp(filepath.Dir(taskDir), "clone-*")
	if err != nil {
		return fmt.Errorf("create temp dir for clone: %w", err)
	}
	defer os.RemoveAll(tmpDir)

	// Build git clone command with persona environment.
	// Start from the host environment and overlay persona vars so that
	// essential variables (HOME, USER, TERM, LANG) are preserved.
	cmd := exec.CommandContext(ctx, "git", "clone", repoRef, tmpDir)
	cmd.Env = os.Environ()
	for k, v := range personaEnv {
		cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%s", k, v))
	}

	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("git clone %s: %w (output: %s)", repoRef, err, string(output))
	}

	// Move cloned contents into the task directory.
	// git clone <url> <dir> puts contents directly in <dir>.
	entries, err := os.ReadDir(tmpDir)
	if err != nil {
		return fmt.Errorf("read cloned dir %s: %w", tmpDir, err)
	}

	for _, entry := range entries {
		src := filepath.Join(tmpDir, entry.Name())
		dst := filepath.Join(taskDir, entry.Name())
		entryInfo, _ := entry.Info()
		h.log.Printf("[WORKDIR] moving %s (mode=%o) -> %s", entry.Name(), entryInfo.Mode().Perm(), dst)
		if err := os.Rename(src, dst); err != nil {
			return fmt.Errorf("move %s to %s: %w", src, dst, err)
		}
	}

	h.log.Printf("[WORKDIR] cloned %q into %s", repoRef, taskDir)
	if sendLog != nil {
		_ = sendLog(LogEntry{TaskID: filepath.Base(taskDir), Line: fmt.Sprintf("Repository cloned: %s", repoRef), Level: "system"})
	}

	return nil
}

// restartClient restarts the pi subprocess using the handler's base CWD.
// It is called when ExecuteTask detects that the client is not running,
// e.g. because the subprocess was killed externally or crashed.
// The restarted client is a temporary one — resetClient will replace it
// with a task-specific client later in ExecuteTask.
// Retries up to 3 times with exponential backoff on failure.
func (h *PIHandler) restartClient(ctx context.Context) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	// Stop the dead client if it's still around
	if h.client != nil && h.client.IsRunning() {
		// Should not happen — we only call restartClient when IsRunning is false.
		// But handle it gracefully just in case.
		if err := h.client.Stop(ctx); err != nil {
			h.log.Printf("[PI] stopping dead client: %v", err)
		}
	}

	// Retry with exponential backoff: 1s, 2s, 4s
	maxRetries := 3
	var lastErr error
	for attempt := 1; attempt <= maxRetries; attempt++ {
		// Create a new client with the base working directory
		h.log.Printf("[PI] restarting pi client in base dir: %s (attempt %d/%d)", h.baseCWD, attempt, maxRetries)
		cfg := pi.PiClientConfig{
			CWD:   h.baseCWD,
			Log:   h.log,
			Debug: h.debug,
		}
		h.client = pi.NewClient(cfg)
		if err := h.client.Start(ctx); err != nil {
			lastErr = fmt.Errorf("start pi client: %w", err)
			h.log.Printf("[PI] restart attempt %d/%d failed: %v", attempt, maxRetries, err)
			if attempt < maxRetries {
				if err := backoffAndSleep(ctx, attempt, h.log); err != nil {
					return err
				}
			}
			continue
		}
		if !h.client.IsRunning() {
			lastErr = fmt.Errorf("pi client started but not running")
			h.log.Printf("[PI] restart attempt %d/%d failed: client not running", attempt, maxRetries)
			if attempt < maxRetries {
				if err := backoffAndSleep(ctx, attempt, h.log); err != nil {
					return err
				}
			}
			continue
		}
		if h.client.Cmd() != nil && h.client.Cmd().Process != nil {
			h.log.Printf("[PI] pi client restarted (pid %d)", h.client.Cmd().Process.Pid)
		}
		return nil
	}
	return fmt.Errorf("pi client restart failed after %d attempts: %w", maxRetries, lastErr)
}

// resetClient restarts the pi subprocess with a new working directory.
// This is needed per-task so the guest operates inside the cloned repo tree.
func (h *PIHandler) resetClient(ctx context.Context, workDir string, taskID string, sendLog func(LogEntry) error) error {
	return h.resetClientWithEnv(ctx, workDir, taskID, sendLog, nil)
}

// resetClientWithEnv restarts the pi subprocess with a new working directory
// and optional environment variables. The env vars are applied to the pi
// subprocess so that persona-specific configuration (e.g. token paths)
// is available to the agent.
// Retries up to 3 times with exponential backoff on failure.
func (h *PIHandler) resetClientWithEnv(ctx context.Context, workDir string, taskID string, sendLog func(LogEntry) error, env map[string]string) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	// Stop the old client if it's running
	if h.client != nil && h.client.IsRunning() {
		if err := h.client.Stop(ctx); err != nil {
			h.log.Printf("[PI] stopping old client: %v", err)
		} else {
			h.log.Printf("[PI] stopped old client")
		}
	}

	// Retry with exponential backoff: 1s, 2s, 4s
	maxRetries := 3
	var lastErr error
	for attempt := 1; attempt <= maxRetries; attempt++ {
		// Create a new client with the task-specific working directory
		h.log.Printf("[PI] creating new pi client for working dir: %s (attempt %d/%d)", workDir, attempt, maxRetries)
		cfg := pi.PiClientConfig{
			CWD:           workDir,
			Provider:      "",
			Model:         "",
			ThinkingLevel: "",
			Log:           h.log,
			Env:           env,
			SpawnOutput: func(line string) {
				// Echo spawn-phase output to guest logs for troubleshooting.
				// See issue #19: without this, spawn failures produce silence.
				_ = sendLog(LogEntry{TaskID: taskID, Line: fmt.Sprintf("[spawn] %s", line), Level: "system"})
			},
		}
		h.client = pi.NewClient(cfg)
		if err := h.client.Start(ctx); err != nil {
			lastErr = fmt.Errorf("start pi client in %s: %w", workDir, err)
			h.log.Printf("[PI] spawn attempt %d/%d failed: %v", attempt, maxRetries, err)
			if sendLog != nil {
				_ = sendLog(LogEntry{TaskID: taskID, Line: fmt.Sprintf("Spawn attempt %d/%d failed: %v", attempt, maxRetries, err), Level: "warning"})
			}
			if attempt < maxRetries {
				if err := backoffAndSleep(ctx, attempt, h.log); err != nil {
					return err
				}
			}
			continue
		}
		if !h.client.IsRunning() {
			lastErr = fmt.Errorf("pi client in %s: started but not running", workDir)
			h.log.Printf("[PI] spawn attempt %d/%d failed: client not running", attempt, maxRetries)
			if sendLog != nil {
				_ = sendLog(LogEntry{TaskID: taskID, Line: fmt.Sprintf("Spawn attempt %d/%d failed: client not running", attempt, maxRetries), Level: "warning"})
			}
			if attempt < maxRetries {
				if err := backoffAndSleep(ctx, attempt, h.log); err != nil {
					return err
				}
			}
			continue
		}
		h.log.Printf("[PI] new pi subprocess started in: %s", workDir)
		return nil
	}
	return fmt.Errorf("pi client spawn failed after %d attempts: %w", maxRetries, lastErr)
}

// backoffAndSleep calculates exponential backoff (1s, 2s, 4s, ...) and sleeps
// for the duration, respecting context cancellation.
// Returns ctx.Err() if the context was cancelled during the sleep.
func backoffAndSleep(ctx context.Context, attempt int, logger *log.Logger) error {
	backoff := time.Duration(1<<(attempt-1)) * time.Second
	logger.Printf("[PI] retrying in %v", backoff)
	select {
	case <-time.After(backoff):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
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

// captureExitDiagnostics gathers diagnostic information from the pi client
// at exit time. It captures the exit code, exit error, stderr lines, and
// the last event types received. This data is used for troubleshooting
// failed or abnormal task completions.
func (h *PIHandler) captureExitDiagnostics(guestEndReceived bool) *ExitDiagnostics {
	diag := &ExitDiagnostics{}
	diag.GuestEndReceived = guestEndReceived

	// Capture exit code
	if h.client != nil {
		exitCode := h.client.GetExitCode()
		diag.ExitCode = exitCode

		// Capture exit error
		if exitErr := h.client.GetExitError(); exitErr != nil {
			diag.ExitError = exitErr.Error()
		}

		// Capture stderr lines
		if stderrLines := h.client.GetStderrLines(); len(stderrLines) > 0 {
			diag.StderrLines = stderrLines
		}

		// Capture last event types
		if events := h.client.GetEventHistory(); len(events) > 0 {
			lastN := 10
			if len(events) < lastN {
				lastN = len(events)
			}
			diag.LastEventTypes = make([]string, lastN)
			for i := 0; i < lastN; i++ {
				diag.LastEventTypes[i] = events[len(events)-lastN+i].Type
			}
		}
	}

	return diag
}

// buildPrompt constructs the prompt for pi with context.
// The user's prompt is always placed first so that template commands
// (e.g. /repo-ideation) start at the top of the message and are
// expanded by pi.  Context tidbits are appended after.
func (h *PIHandler) buildPrompt(task TaskAssignment) string {
	var parts []string

	parts = append(parts, task.Prompt)

	if len(task.Tags) > 0 {
		parts = append(parts, fmt.Sprintf("Required tags: %s", strings.Join(task.Tags, ", ")))
	}

	return strings.Join(parts, "\n\n")
}
