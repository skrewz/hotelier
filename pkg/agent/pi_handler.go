package agent

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

// isGitURL returns true if the string looks like a git remote URL.
func isGitURL(s string) bool {
	// SSH-style: user@host:path or git@host:path
	if strings.Contains(s, "://") {
		return true
	}
	if strings.HasPrefix(s, "git@") && strings.Contains(s, ":") {
		return true
	}
	return false
}

// PIHandler executes tasks using the pi AI agent via RPC subprocess.
type PIHandler struct {
	client *pi.PiClient
	log    *log.Logger
	mu     sync.Mutex
}

// NewPIHandler creates a new PIHandler.
func NewPIHandler(cwd string, provider, model, thinkingLevel string) *PIHandler {
	logger := log.New(os.Stdout, "[pi-handler] ", log.LstdFlags)
	cfg := pi.PiClientConfig{
		CWD:           cwd,
		Provider:      provider,
		Model:         model,
		ThinkingLevel: thinkingLevel,
		Log:           logger,
	}
	return &PIHandler{
		client: pi.NewClient(cfg),
		log:    logger,
	}
}

// Start initializes the pi RPC subprocess.
func (h *PIHandler) Start(ctx context.Context) error {
	if err := h.client.Start(ctx); err != nil {
		return fmt.Errorf("start pi client: %w", err)
	}
	h.log.Printf("pi client started")
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

// ExecuteTask runs a task through the pi agent and streams logs back via the callback.
// It clones any remote repos into a per-task working directory, sets that as the
// CWD for the pi subprocess, and logs all commands it spawns.
func (h *PIHandler) ExecuteTask(ctx context.Context, task TaskAssignment, sendLog func(taskID, line string) error) (*TaskResult, error) {
	h.mu.Lock()
	if h.client == nil || !h.client.IsRunning() {
		h.mu.Unlock()
		return nil, fmt.Errorf("pi client not running")
	}
	h.mu.Unlock()

	h.log.Printf("[PI] executing task %s", task.TaskID)
	h.log.Printf("[PI] repos: %v", task.Repos)
	h.log.Printf("[PI] prompt: %s", task.Prompt)

	// Clone any remote repos and determine the working directory.
	workDir, err := h.prepareRepos(ctx, task.TaskID, task.Repos, sendLog)
	if err != nil {
		return nil, fmt.Errorf("prepare repos: %w", err)
	}

	// Build the prompt for pi — include repo context
	prompt := h.buildPrompt(task)

	// Recreate the pi client with the task-specific working directory.
	// We need to do this because the client was created with the base CWD,
	// but we want the pi subprocess to run inside the cloned repo tree.
	h.log.Printf("[PI] spawning pi subprocess in: %s", workDir)
	if err := h.resetClient(ctx, workDir); err != nil {
		return nil, fmt.Errorf("reset pi client with working dir %s: %w", workDir, err)
	}

	// Send the prompt
	if err := h.client.Prompt(ctx, prompt); err != nil {
		return nil, fmt.Errorf("send prompt: %w", err)
	}

	// Collect streaming output
	var mu sync.Mutex
	var output strings.Builder

	// Subscribe to events
	eventCh := h.client.Subscribe()

	done := make(chan struct{})
	go func() {
		defer close(done)
		for event := range eventCh {
			if pi.IsAgentEnd(event) {
				// Text deltas have already been streamed via sendLog.
				// No need to re-send the final text — it would duplicate.
				return
			}

			if pi.IsTextDelta(event) {
				delta := pi.ExtractTextDelta(event)
				if delta != "" {
					mu.Lock()
					output.WriteString(delta)
					mu.Unlock()

					// Send the full delta as one log entry, preserving newlines.
					// The server-side accumulator will batch these into complete messages.
					if err := sendLog(task.TaskID, delta); err != nil {
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
					var logMsg string
					if args != "" {
						logMsg = fmt.Sprintf("[TOOL_START] %s: %s (id: %s)", toolName, args, toolID)
					} else {
						logMsg = fmt.Sprintf("[TOOL_START] %s (id: %s)", toolName, toolID)
					}
					if err := sendLog(task.TaskID, logMsg); err != nil {
						h.log.Printf("[PI] failed to send tool log: %v", err)
					}

				case "tool_execution_update":
					partial := pi.ToolPartialResult(event)
					if partial != "" {
						logMsg := fmt.Sprintf("[TOOL_OUTPUT] %s (id: %s): %s", toolName, toolID, partial)
						if err := sendLog(task.TaskID, logMsg); err != nil {
							h.log.Printf("[PI] failed to send tool output: %v", err)
						}
					}

				case "tool_execution_end":
					result := pi.ToolResult(event)
					isErr := event.IsError
					var logMsg string
					if isErr {
						logMsg = fmt.Sprintf("[TOOL_END] %s (id: %s) [ERROR]", toolName, toolID)
					} else if result != "" {
						logMsg = fmt.Sprintf("[TOOL_END] %s (id: %s): %s", toolName, toolID, result)
					} else {
						logMsg = fmt.Sprintf("[TOOL_END] %s (id: %s)", toolName, toolID)
					}
					if err := sendLog(task.TaskID, logMsg); err != nil {
						h.log.Printf("[PI] failed to send tool end log: %v", err)
					}
				}
			}
		}
	}()

	// Wait for completion or context cancellation
	select {
	case <-done:
		// Agent completed
	case <-ctx.Done():
		h.log.Printf("[PI] task %s cancelled by context", task.TaskID)
		_ = h.client.Abort()
		return &TaskResult{
			TaskID:  task.TaskID,
			Success: false,
			Error:   "task cancelled",
		}, nil
	case <-time.After(10 * time.Minute): // Hard timeout
		h.log.Printf("[PI] task %s timed out", task.TaskID)
		_ = h.client.Abort()
		return &TaskResult{
			TaskID:  task.TaskID,
			Success: false,
			Error:   "task timed out after 10 minutes",
		}, nil
	}

	mu.Lock()
	result := output.String()
	mu.Unlock()

	h.log.Printf("[PI] task %s completed, output length: %d", task.TaskID, len(result))

	return &TaskResult{
		TaskID:  task.TaskID,
		Success: true,
		Output:  result,
	}, nil
}

// prepareRepos clones any remote repos into a task-specific directory and
// returns the path to use as the working directory for the pi subprocess.
// Local paths are resolved as-is; remote URLs are cloned.
func (h *PIHandler) prepareRepos(ctx context.Context, taskID string, repos []string, sendLog func(taskID, line string) error) (string, error) {
	if len(repos) == 0 {
		// No repos — use the handler's base CWD
		return h.client.CWD(), nil
	}

	// Create a task-specific directory under the base CWD.
	taskDir := filepath.Join(h.client.CWD(), "tasks", taskID)
	if err := os.MkdirAll(taskDir, 0o755); err != nil {
		return "", fmt.Errorf("create task dir %s: %w", taskDir, err)
	}

	for _, repo := range repos {
		if isGitURL(repo) {
			// Clone remote repo into taskDir
			repoName := filepath.Base(strings.TrimSuffix(repo, ".git"))
			clonePath := filepath.Join(taskDir, repoName)
			h.log.Printf("[GIT] cloning %s -> %s", repo, clonePath)
			cloneCmd := exec.CommandContext(ctx, "git", "clone", "--depth", "1", repo, clonePath)
			out, err := cloneCmd.CombinedOutput()
			if err != nil {
				return "", fmt.Errorf("git clone %s: %w (output: %s)", repo, err, string(out))
			}
			h.log.Printf("[GIT] cloned %s", repo)
		} else {
			// Local path — resolve relative to the task dir
			resolved := repo
			if !filepath.IsAbs(repo) {
				resolved = filepath.Join(taskDir, repo)
			}
			absPath, err := filepath.Abs(resolved)
			if err != nil {
				return "", fmt.Errorf("resolve local repo path %s: %w", repo, err)
			}
			h.log.Printf("[REPO] using local repo: %s", absPath)
		}
	}

	return taskDir, nil
}

// resetClient restarts the pi subprocess with a new working directory.
// This is needed per-task so the agent operates inside the cloned repo tree.
func (h *PIHandler) resetClient(ctx context.Context, workDir string) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	// Stop the old client if it's running
	if h.client != nil && h.client.IsRunning() {
		_ = h.client.Stop(ctx)
	}

	// Create a new client with the task-specific working directory
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
	h.log.Printf("[PI] pi subprocess started in: %s", workDir)
	return nil
}

// buildPrompt constructs the prompt for pi with repo context.
func (h *PIHandler) buildPrompt(task TaskAssignment) string {
	var parts []string

	if len(task.Repos) > 0 {
		parts = append(parts, fmt.Sprintf("Working repositories: %s", strings.Join(task.Repos, ", ")))
	}
	if len(task.Tags) > 0 {
		parts = append(parts, fmt.Sprintf("Required tags: %s", strings.Join(task.Tags, ", ")))
	}
	parts = append(parts, fmt.Sprintf("Task prompt: %s", task.Prompt))

	return strings.Join(parts, "\n\n")
}
