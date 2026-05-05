package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"hotelier/pkg/config"
	"hotelier/pkg/rpc"

	"github.com/google/uuid"
)

// LogEntry represents a log entry sent from an agent to the host.
type LogEntry struct {
	TaskID    string    `json:"task_id"`
	Line      string    `json:"line"`
	Level     string    `json:"level,omitempty"`
	Timestamp time.Time `json:"timestamp,omitempty"`
}

// TaskResult represents the result of a task execution.
type TaskResult struct {
	TaskID  string `json:"task_id"`
	Success bool   `json:"success"`
	Output  string `json:"output,omitempty"`
	Error   string `json:"error,omitempty"`
}

// TaskAssignment represents a task assigned to an agent.
type TaskAssignment struct {
	TaskID string   `json:"id"`
	Repos  []string `json:"repos"`
	Prompt string   `json:"prompt"`
	Tags   []string `json:"tags"`
}

// TaskCancel represents a task cancellation notification.
type TaskCancel struct {
	TaskID string `json:"task_id"`
	Reason string `json:"reason,omitempty"`
}

// LogCallback is a function that sends a log line to the host.
type LogCallback func(taskID, line string) error

// Handler is a function that handles task execution.
type Handler func(ctx context.Context, task TaskAssignment, log LogCallback) (*TaskResult, error)

// Agent is the client-side agent that connects to the Check-In Host.
type Agent struct {
	id       string
	name     string
	tags     []string
	config   config.AgentConfig
	client   *rpc.Client
	hub      *rpc.ClientHub
	log      *log.Logger
	handler  Handler
	mu       sync.Mutex
	running  bool
	stopCh   chan struct{}
	callResp chan *rpc.JSONRPCMessage
	taskCh   chan TaskAssignment // incoming task assignments
}

// New creates a new Agent instance.
func New(cfg config.AgentConfig, handler Handler) *Agent {
	// Generate an ephemeral ID so reconnects after crash don't conflict.
	id := "agent-" + uuid.New().String()[:8]

	logPrefix := fmt.Sprintf("[agent:%s]", id)
	logger := log.New(os.Stdout, logPrefix+" ", log.LstdFlags)

	hub := rpc.NewClientHub(func(format string, args ...interface{}) {
		logger.Printf(format, args...)
	})

	return &Agent{
		id:       id,
		name:     cfg.Name,
		tags:     cfg.Tags,
		config:   cfg,
		hub:      hub,
		log:      logger,
		handler:  handler,
		stopCh:   make(chan struct{}),
		callResp: make(chan *rpc.JSONRPCMessage, 1),
		taskCh:   make(chan TaskAssignment, 16),
	}
}

// Connect connects the agent to the Check-In Host.
func (a *Agent) Connect() error {
	host := fmt.Sprintf("ws://%s:%d/ws", a.config.Host, a.config.Port)
	a.log.Printf("connecting to host at %s", host)

	client := rpc.NewClient(a.id, a.hub, a.log.Printf)
	if err := client.Connect(host); err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	a.client = client
	a.log.Printf("connected to host")
	return nil
}

// Register registers the agent with the Check-In Host.
func (a *Agent) Register() error {
	params := map[string]interface{}{
		"id":   a.id,
		"name": a.name,
		"tags": a.tags,
	}

	_, err := a.client.Call("agent.register", params)
	if err != nil {
		return fmt.Errorf("register: %w", err)
	}

	// Register handler for server-initiated task.assign notifications
	a.hub.RegisterNotificationHandler("task.assign", func(method string, params json.RawMessage) {
		a.log.Printf("[RPC] received notification: %s", method)
		var task TaskAssignment
		if err := json.Unmarshal(params, &task); err != nil {
			a.log.Printf("[RPC] failed to parse task.assign params: %v", err)
			return
		}
		a.log.Printf("[RPC] dispatching task %s to execution", task.TaskID)
		select {
		case a.taskCh <- task:
			a.log.Printf("[RPC] task %s queued for execution", task.TaskID)
		default:
			a.log.Printf("[RPC] task queue full, dropping task %s", task.TaskID)
		}
	})

	// Register handler for task.cancel notifications
	a.hub.RegisterNotificationHandler("task.cancel", func(method string, params json.RawMessage) {
		a.log.Printf("[RPC] received notification: %s", method)
		var cancel TaskCancel
		if err := json.Unmarshal(params, &cancel); err != nil {
			a.log.Printf("[RPC] failed to parse task.cancel params: %v", err)
			return
		}
		a.log.Printf("[RPC] task %s cancelled: %s", cancel.TaskID, cancel.Reason)
	})

	a.log.Printf("registered with tags: %v", a.tags)
	return nil
}

// taskDispatcher runs tasks sequentially as they arrive.
func (a *Agent) taskDispatcher() {
	for {
		select {
		case <-a.stopCh:
			return
		case task := <-a.taskCh:
			a.log.Printf("[DISPATCH] starting task %s", task.TaskID)
			result, err := a.ExecuteTask(task)
			if err != nil {
				a.log.Printf("[DISPATCH] task %s error: %v", task.TaskID, err)
			} else {
				a.log.Printf("[DISPATCH] task %s finished: success=%v output=%q", task.TaskID, result.Success, result.Output)
			}

			// If configured, automatically claim the next pending task
			if a.config.AutoClaimNextTask {
				a.log.Printf("[DISPATCH] auto-claiming next pending task")
				a.tryClaimNextTask()
			}
		}
	}
}

// tryClaimNextTask sends a task.claim RPC to pick up the next pending task.
func (a *Agent) tryClaimNextTask() {
	if a.client == nil {
		return
	}

	params := map[string]interface{}{
		"id":   a.id,
		"tags": a.tags,
	}

	_, err := a.client.Call("task.claim", params)
	if err != nil {
		a.log.Printf("[DISPATCH] failed to claim next task: %v", err)
	}
}

// Unregister unregisters the agent from the Check-In Host.
func (a *Agent) Unregister() error {
	params := map[string]string{
		"id": a.id,
	}

	_, err := a.client.Call("agent.unregister", params)
	if err != nil {
		return fmt.Errorf("unregister: %w", err)
	}

	a.log.Printf("unregistered from host")
	return nil
}

// Heartbeat sends a heartbeat to the Check-In Host.
func (a *Agent) Heartbeat() error {
	params := map[string]string{
		"id": a.id,
	}

	_, err := a.client.Call("agent.heartbeat", params)
	if err != nil {
		return fmt.Errorf("heartbeat: %w", err)
	}
	return nil
}

// SendLog sends a log entry to the Check-In Host.
func (a *Agent) SendLog(taskID, line string) error {
	entry := LogEntry{
		TaskID:    taskID,
		Line:      line,
		Level:     "info",
		Timestamp: time.Now(),
	}

	err := a.client.SendNotification("agent.log", entry)
	if err != nil {
		return fmt.Errorf("send log: %w", err)
	}
	return nil
}

// SendResult submits the final result of a task to the Check-In Host.
func (a *Agent) SendResult(result TaskResult) error {
	err := a.client.SendNotification("agent.result", result)
	if err != nil {
		return fmt.Errorf("send result: %w", err)
	}
	return nil
}

// Start starts the agent's main loop.
func (a *Agent) Start() error {
	if err := a.Connect(); err != nil {
		return err
	}
	defer a.client.Close()

	if err := a.Register(); err != nil {
		return err
	}
	defer a.Unregister()

	// Start heartbeat goroutine
	go a.heartbeatLoop()

	// Start task dispatcher — handles incoming task.assign notifications
	go a.taskDispatcher()

	// Wait for shutdown
	a.log.Printf("agent ready, waiting for tasks...")
	<-a.stopCh

	a.log.Printf("agent shutting down")
	return nil
}

// Stop gracefully stops the agent.
func (a *Agent) Stop() {
	a.mu.Lock()
	defer a.mu.Unlock()

	select {
	case <-a.stopCh:
		// already stopped
	default:
		close(a.stopCh)
	}
}

func (a *Agent) heartbeatLoop() {
	interval := a.config.HeartbeatInterval
	if interval == 0 {
		interval = 30 // default 30 seconds
	}

	ticker := time.NewTicker(time.Duration(interval) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := a.Heartbeat(); err != nil {
				a.log.Printf("heartbeat failed: %v", err)
			}
		case <-a.stopCh:
			return
		}
	}
}

// ExecuteTask executes a task assigned to this agent.
func (a *Agent) ExecuteTask(task TaskAssignment) (*TaskResult, error) {
	a.mu.Lock()
	if a.running {
		a.mu.Unlock()
		return nil, fmt.Errorf("agent is already running a task")
	}
	a.running = true
	a.mu.Unlock()

	defer func() {
		a.mu.Lock()
		a.running = false
		a.mu.Unlock()
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Set up timeout
	if a.config.TaskTimeout > 0 {
		var cancelTimeout context.CancelFunc
		ctx, cancelTimeout = context.WithTimeout(ctx, time.Duration(a.config.TaskTimeout)*time.Second)
		defer cancelTimeout()
	}

	a.log.Printf("[TASK] executing task %s", task.TaskID)
	a.log.Printf("[TASK] repos: %v", task.Repos)
	a.log.Printf("[TASK] prompt: %s", task.Prompt)
	a.log.Printf("[TASK] tags: %v", task.Tags)
	a.log.Printf("[TASK] mode: %s", a.config.TaskMode)

	// Send log that task started
	if err := a.SendLog(task.TaskID, "Task started"); err != nil {
		a.log.Printf("failed to send task start log: %v", err)
	}

	// Execute the task using the handler
	result, err := a.handler(ctx, task, a.SendLog)
	if err != nil {
		a.log.Printf("task %s failed: %v", task.TaskID, err)
		failureResult := TaskResult{
			TaskID:  task.TaskID,
			Success: false,
			Error:   err.Error(),
		}
		if sendErr := a.SendResult(failureResult); sendErr != nil {
			a.log.Printf("failed to send failure result: %v", sendErr)
		}
		return nil, err
	}

	a.log.Printf("[TASK] task %s completed successfully", task.TaskID)
	if err := a.SendLog(task.TaskID, "Task completed successfully"); err != nil {
		a.log.Printf("failed to send task complete log: %v", err)
	}

	if err := a.SendResult(*result); err != nil {
		a.log.Printf("failed to send result: %v", err)
	}

	return result, nil
}

// DefaultHandler returns a default task handler that executes shell commands.
// It clones remote repos, logs the working directory, and logs each command it spawns.
func DefaultHandler(ctx context.Context, task TaskAssignment, cb LogCallback) (*TaskResult, error) {
	// Determine the working directory.
	// If there are remote repos, clone them into a temp dir and use that.
	// If there are only local repos, use the first one as the working directory.
	workDir, err := defaultHandlerPrepareRepos(ctx, task.Repos, cb)
	if err != nil {
		return nil, fmt.Errorf("prepare repos: %w", err)
	}

	// Log the working directory being used
	logLine := fmt.Sprintf("[WORKDIR] using working directory: %s", workDir)
	if cb != nil {
		_ = cb(task.TaskID, logLine)
	}

	// Execute the prompt as a shell command
	cmd := exec.CommandContext(ctx, "bash", "-c", task.Prompt)
	cmd.Dir = workDir

	// Log the command being spawned
	cmdLine := fmt.Sprintf("[SHELL] %s", cmd.String())
	if cb != nil {
		_ = cb(task.TaskID, cmdLine)
	}

	stdout, err := cmd.CombinedOutput()
	if err != nil {
		return &TaskResult{
			TaskID:  task.TaskID,
			Success: false,
			Output:  string(stdout),
			Error:   err.Error(),
		}, nil
	}

	return &TaskResult{
		TaskID:  task.TaskID,
		Success: true,
		Output:  string(stdout),
	}, nil
}

// defaultHandlerPrepareRepos clones any remote repos into a temp directory
// and returns the working directory. If there are only local repos, the first
// one is used as the working directory.
func defaultHandlerPrepareRepos(ctx context.Context, repos []string, cb LogCallback) (string, error) {
	if len(repos) == 0 {
		workDir, err := os.MkdirTemp("", "hotelier-task-*")
		if err != nil {
			return "", fmt.Errorf("create temp dir: %w", err)
		}
		return workDir, nil
	}

	// Check if any repo is a remote URL
	hasRemote := false
	for _, repo := range repos {
		if isGitURL(repo) {
			hasRemote = true
			break
		}
	}

	if hasRemote {
		// Create a task-specific directory and clone remote repos into it
		workDir, err := os.MkdirTemp("", "hotelier-task-*")
		if err != nil {
			return "", fmt.Errorf("create task dir: %w", err)
		}

		for _, repo := range repos {
			if isGitURL(repo) {
				repoName := filepath.Base(strings.TrimSuffix(repo, ".git"))
				clonePath := filepath.Join(workDir, repoName)
				cmdLine := fmt.Sprintf("[GIT] git clone --depth 1 %s %s", repo, clonePath)
				if cb != nil {
					_ = cb("", cmdLine)
				}
				cloneCmd := exec.CommandContext(ctx, "git", "clone", "--depth", "1", repo, clonePath)
				out, err := cloneCmd.CombinedOutput()
				if err != nil {
					return "", fmt.Errorf("git clone %s: %w (output: %s)", repo, err, string(out))
				}
			} else {
				// Local path — copy contents into the task directory
				resolved := repo
				if !filepath.IsAbs(repo) {
					resolved = filepath.Join(workDir, repo)
				}
				absPath, err := filepath.Abs(resolved)
				if err != nil {
					return "", fmt.Errorf("resolve local repo path %s: %w", repo, err)
				}
				cmdLine := fmt.Sprintf("[REPO] using local repo: %s", absPath)
				if cb != nil {
					_ = cb("", cmdLine)
				}
				// Copy the local repo contents into the task directory
				entries, err := os.ReadDir(absPath)
				if err != nil {
					return "", fmt.Errorf("read local repo %s: %w", absPath, err)
				}
				for _, entry := range entries {
					src := filepath.Join(absPath, entry.Name())
					dst := filepath.Join(workDir, entry.Name())
					if entry.IsDir() {
						if err := copyDir(src, dst); err != nil {
							return "", fmt.Errorf("copy dir %s: %w", entry.Name(), err)
						}
					} else {
						if err := copyFile(src, dst); err != nil {
							return "", fmt.Errorf("copy file %s: %w", entry.Name(), err)
						}
					}
				}
			}
		}

		return workDir, nil
	}

	// All local repos — use the first one as the working directory
	resolved := repos[0]
	if !filepath.IsAbs(resolved) {
		resolved = filepath.Join(".", resolved)
	}
	absPath, err := filepath.Abs(resolved)
	if err != nil {
		return "", fmt.Errorf("resolve local repo path %s: %w", repos[0], err)
	}
	cmdLine := fmt.Sprintf("[REPO] using local repo: %s", absPath)
	if cb != nil {
		_ = cb("", cmdLine)
	}
	return absPath, nil
}

// copyFile copies a single file from src to dst.
func copyFile(src, dst string) error {
	data, err := os.ReadFile(src)
	if err != nil {
		return err
	}
	return os.WriteFile(dst, data, 0o644)
}

// copyDir recursively copies a directory from src to dst.
func copyDir(src, dst string) error {
	entries, err := os.ReadDir(src)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dst, 0o755); err != nil {
		return err
	}
	for _, entry := range entries {
		srcPath := filepath.Join(src, entry.Name())
		dstPath := filepath.Join(dst, entry.Name())
		if entry.IsDir() {
			if err := copyDir(srcPath, dstPath); err != nil {
				return err
			}
		} else {
			if err := copyFile(srcPath, dstPath); err != nil {
				return err
			}
		}
	}
	return nil
}

// LoadConfig loads the agent configuration from a file.
func LoadConfig(path string) (config.AgentConfig, error) {
	return config.LoadAgentConfig(path)
}

// RunAgent is a convenience function to load config and run the agent.
func RunAgent(configPath string) error {
	cfg, err := LoadConfig(configPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	agent := New(cfg, DefaultHandler)
	return agent.Start()
}

// Ensure json is used
var _ = json.Marshal
