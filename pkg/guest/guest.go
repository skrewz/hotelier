package guest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"hotelier/pkg/config"
	"hotelier/pkg/rpc"

	"github.com/google/uuid"
)

// LogEntry represents a log entry sent from a guest to the host.
// For tool call events, structured fields (ToolType, ToolName, etc.)
// carry the machine-readable data alongside the formatted Line string.
type LogEntry struct {
	TaskID     string    `json:"task_id"`
	Line       string    `json:"line"`
	Level      string    `json:"level,omitempty"`
	Timestamp  time.Time `json:"timestamp,omitempty"`
	ToolType   string    `json:"tool_type,omitempty"`   // "start", "output", "end"
	ToolName   string    `json:"tool_name,omitempty"`   // e.g. "bash", "read"
	ToolID     string    `json:"tool_id,omitempty"`     // unique tool call identifier
	ToolArgs   string    `json:"tool_args,omitempty"`   // arguments/parameters
	ToolOutput string    `json:"tool_output,omitempty"` // captured output
	ToolError  bool      `json:"tool_error,omitempty"`  // true if tool ended with error
}

// TaskResult represents the result of a task execution.
type TaskResult struct {
	TaskID  string `json:"task_id"`
	Success bool   `json:"success"`
	Output  string `json:"output,omitempty"`
	Error   string `json:"error,omitempty"`
}

// TaskAssignment represents a task assigned to a guest.
type TaskAssignment struct {
	TaskID  string         `json:"id"`
	Prompt  string         `json:"prompt"`
	Tags    []string       `json:"tags"`
	Persona *PersonaData   `json:"persona,omitempty"` // persona data to apply (optional)
}

// PersonaData holds the persona configuration sent from the server.
// It includes environment variables and file copy mappings that are
// applied to the task's working directory.
type PersonaData struct {
	Name  string            `json:"name"`
	Env   map[string]string `json:"env"`
	Files []personaFileCopy `json:"files"`
}

// personaFileCopy represents a source-to-destination file copy.
type personaFileCopy struct {
	From string `json:"from"` // absolute source path on guest
	To   string `json:"to"`   // relative destination path (within workdir)
}

// TaskCancel represents a task cancellation notification.
type TaskCancel struct {
	TaskID string `json:"task_id"`
	Reason string `json:"reason,omitempty"`
}

// LogCallback is a function that sends a log entry to the host.
// For tool call events, the entry should include structured fields
// (ToolType, ToolName, etc.) alongside the formatted Line string.
type LogCallback func(entry LogEntry) error

// Handler is a function that handles task execution.
type Handler func(ctx context.Context, task TaskAssignment, sendLog LogCallback) (*TaskResult, error)

// Guest is the client-side guest that connects to the Check-In Host.
type Guest struct {
	id            string
	name          string
	tags          []string
	config        config.GuestConfig
	client        *rpc.Client
	hub           *rpc.ClientHub
	log           *log.Logger
	handler       Handler
	mu            sync.Mutex
	running       bool
	cancel        context.CancelFunc // cancels the current task's context
	stopCh        chan struct{}
	connLost      chan struct{} // closed when the connection drops
	callResp      chan *rpc.JSONRPCMessage
	taskCh        chan TaskAssignment // incoming task assignments
	currentTaskID string              // task currently being worked on (for task-aware heartbeat)

	// heartbeatForTest allows tests to stub the Heartbeat method.
	// When nil, heartbeatLoop calls g.Heartbeat() directly.
	heartbeatForTest func() error
}

// New creates a new Guest instance.
func New(cfg config.GuestConfig, handler Handler) *Guest {
	// Generate an ephemeral ID so reconnects after crash don't conflict.
	id := "guest-" + uuid.New().String()[:8]

	logPrefix := fmt.Sprintf("[guest:%s]", id)
	logger := log.New(os.Stdout, logPrefix+" ", log.LstdFlags)

	hub := rpc.NewClientHub(func(format string, args ...interface{}) {
		logger.Printf(format, args...)
	})

	return &Guest{
		id:       id,
		name:     cfg.Name,
		tags:     cfg.Tags,
		config:   cfg,
		hub:      hub,
		log:      logger,
		handler:  handler,
		stopCh:   make(chan struct{}),
		connLost: make(chan struct{}),
		callResp: make(chan *rpc.JSONRPCMessage, 1),
		taskCh:   make(chan TaskAssignment, 16),
	}
}

// Connect connects the guest to the Check-In Host.
func (g *Guest) Connect() error {
	client := rpc.NewClient(g.id, g.hub, g.log.Printf)

	// Build TLS config if mTLS is configured
	tlsConfig, err := g.config.TLSConfig()
	if err != nil {
		return fmt.Errorf("build TLS config: %w", err)
	}

	hasMTLS := tlsConfig != nil
	if hasMTLS {
		g.log.Printf("using mTLS client certificate for connection")
	}

	connectURL, err := g.config.ConnectURL(hasMTLS)
	if err != nil {
		return fmt.Errorf("resolve connect url: %w", err)
	}
	g.log.Printf("connecting to host at %s", connectURL)

	if hasMTLS {
		if err := client.ConnectWithTLS(connectURL, tlsConfig); err != nil {
			return fmt.Errorf("connect with TLS: %w", err)
		}
	} else {
		if err := client.Connect(connectURL); err != nil {
			return fmt.Errorf("connect: %w", err)
		}
	}
	g.client = client
	g.client.SetOnClose(g.setConnLost)
	g.log.Printf("connected to host")
	return nil
}

// setConnLost signals that the connection has been lost.
// This is called by the readLoop when the WebSocket closes unexpectedly.
func (g *Guest) setConnLost() {
	select {
	case <-g.connLost:
		// already lost
	default:
		close(g.connLost)
	}
}

// resetConn prepares for a new connection after a reconnect.
// It resets the connLost channel so future disconnections can be detected.
func (g *Guest) resetConn() {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.connLost = make(chan struct{})
}

// Register registers the guest with the Check-In Host.
func (g *Guest) Register() error {
	params := map[string]interface{}{
		"id":   g.id,
		"name": g.name,
		"tags": g.tags,
	}

	_, err := g.client.Call("guest.register", params)
	if err != nil {
		return fmt.Errorf("register: %w", err)
	}

	// Register handler for server-initiated task.assign notifications
	g.hub.RegisterNotificationHandler("task.assign", func(method string, params json.RawMessage) {
		g.log.Printf("[RPC] received notification: %s", method)
		var task TaskAssignment
		if err := json.Unmarshal(params, &task); err != nil {
			g.log.Printf("[RPC] failed to parse task.assign params: %v", err)
			return
		}

		// Deduplicate: if the guest is already running this exact task,
		// ignore the duplicate assignment. This can happen when the guest
		// reconnects and the server re-sends an assignment it already has.
		g.mu.Lock()
		if g.currentTaskID == task.TaskID {
			g.mu.Unlock()
			g.log.Printf("[RPC] ignoring duplicate task.assign for %s (already running)", task.TaskID)
			return
		}
		g.mu.Unlock()

		g.log.Printf("[RPC] dispatching task %s to execution", task.TaskID)
		select {
		case g.taskCh <- task:
			g.log.Printf("[RPC] task %s queued on guest for execution", task.TaskID)
		default:
			g.log.Printf("[RPC] task queue full, dropping task %s", task.TaskID)
		}
	})

	// Register handler for task.cancel notifications
	g.hub.RegisterNotificationHandler("task.cancel", func(method string, params json.RawMessage) {
		g.log.Printf("[RPC] received notification: %s", method)
		var cancel TaskCancel
		if err := json.Unmarshal(params, &cancel); err != nil {
			g.log.Printf("[RPC] failed to parse task.cancel params: %v", err)
			return
		}
		g.log.Printf("[RPC] task %s cancelled: %s", cancel.TaskID, cancel.Reason)

		// Cancel the running task's context, which will abort the pi subprocess.
		// This is the mechanism the server uses to kill a silent guest's task.
		g.mu.Lock()
		if g.cancel != nil {
			g.cancel()
			g.cancel = nil
		}
		g.mu.Unlock()

		// Confirm cancellation to the server. The guest is the authority on
		// whether the task was actually stopped.
		_, _ = g.client.Call("guest.cancelled", map[string]interface{}{
			"task_id":  cancel.TaskID,
			"guest_id": g.id,
			"reason":   cancel.Reason,
		})
	})

	g.log.Printf("registered with tags: %v", g.tags)
	return nil
}

// taskDispatcher runs tasks sequentially as they arrive.
func (g *Guest) taskDispatcher() {
	for {
		select {
		case <-g.stopCh:
			return
		case task := <-g.taskCh:
			g.log.Printf("[DISPATCH] starting task %s", task.TaskID)
			result, err := g.ExecuteTask(task)
			if err != nil {
				g.log.Printf("[DISPATCH] task %s error: %v", task.TaskID, err)
				// If the guest is already running a task, decline the assignment
				// so the server can re-queue it for another guest.
				if strings.Contains(err.Error(), "already running a task") {
					g.DeclineTask(task.TaskID, err.Error())
				}
			} else {
				g.log.Printf("[DISPATCH] task %s finished: success=%v output=%q", task.TaskID, result.Success, result.Output)
			}
		}
	}
}

// Unregister unregisters the guest from the Check-In Host.
func (g *Guest) Unregister() error {
	params := map[string]string{
		"id": g.id,
	}

	_, err := g.client.Call("guest.unregister", params)
	if err != nil {
		return fmt.Errorf("unregister: %w", err)
	}

	g.log.Printf("unregistered from host")
	return nil
}

// Heartbeat sends a heartbeat to the Check-In Host.
// If the guest is currently executing a task, the task_id is included
// so the server can verify the guest is heartbeating with its assigned task.
// Retries with exponential backoff (3 attempts) on failure.
func (g *Guest) Heartbeat() error {
	g.mu.Lock()
	taskID := g.currentTaskID
	g.mu.Unlock()

	params := map[string]string{
		"id":      g.id,
		"task_id": taskID,
	}

	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		_, err := g.client.Call("guest.heartbeat", params)
		if err == nil {
			return nil
		}
		lastErr = err
		backoff := time.Duration(1<<attempt) * time.Second // 1s, 2s, 4s
		g.log.Printf("heartbeat attempt %d/3 failed: %v, retrying in %v",
			attempt+1, err, backoff)
		time.Sleep(backoff)
	}
	return fmt.Errorf("heartbeat: all 3 attempts failed: %w", lastErr)
}

// SendLog sends a log entry to the Check-In Host.
// For tool call events, the entry should include structured fields
// (ToolType, ToolName, etc.) alongside the formatted Line string.
func (g *Guest) SendLog(entry LogEntry) error {
	entry.Timestamp = time.Now()
	if entry.Level == "" {
		entry.Level = "info"
	}

	err := g.client.SendNotification("guest.log", entry)
	if err != nil {
		return fmt.Errorf("send log: %w", err)
	}
	return nil
}

// SendResult submits the final result of a task to the Check-In Host.
// Retries with exponential backoff (3 attempts) on failure.
func (g *Guest) SendResult(result TaskResult) error {
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		err := g.client.SendNotification("guest.result", result)
		if err == nil {
			return nil
		}
		lastErr = err
		backoff := time.Duration(1<<attempt) * time.Second // 1s, 2s, 4s
		g.log.Printf("send result attempt %d/3 failed: %v, retrying in %v",
			attempt+1, err, backoff)
		time.Sleep(backoff)
	}
	return fmt.Errorf("send result: all 3 attempts failed: %w", lastErr)
}

// DeclineTask notifies the host that this guest cannot accept a task assignment.
// This is called when the guest is already running a task and receives a new
// assignment it cannot handle. Retries with exponential backoff (3 attempts).
func (g *Guest) DeclineTask(taskID, reason string) {
	params := map[string]string{
		"task_id":  taskID,
		"guest_id": g.id,
		"reason":   reason,
	}

	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		_, err := g.client.Call("guest.task_declined", params)
		if err == nil {
			g.log.Printf("[DISPATCH] declined task %s: %s", taskID, reason)
			return
		}
		lastErr = err
		backoff := time.Duration(1<<attempt) * time.Second // 1s, 2s, 4s
		g.log.Printf("[DISPATCH] decline task %s attempt %d/3 failed: %v, retrying in %v",
			taskID, attempt+1, err, backoff)
		time.Sleep(backoff)
	}
	g.log.Printf("[DISPATCH] failed to decline task %s after 3 attempts: %v", taskID, lastErr)
}

// Start starts the guest's main loop with automatic reconnection.
func (g *Guest) Start() error {
	for {
		if err := g.connectAndRegister(); err != nil {
			g.log.Printf("connection failed: %v, retrying in 5s...", err)
			select {
			case <-time.After(5 * time.Second):
			case <-g.stopCh:
				return nil
			}
			continue
		}

		// Start heartbeat goroutine
		go g.heartbeatLoop()

		// Start task dispatcher — handles incoming task.assign notifications
		go g.taskDispatcher()

		// Wait for shutdown or connection loss
		g.log.Printf("guest ready, waiting for tasks...")
		select {
		case <-g.stopCh:
			g.log.Printf("guest shutting down")
			return nil
		case <-g.connLost:
			g.log.Printf("connection lost, reconnecting...")
			// Clean up the dead connection
			if g.client != nil {
				g.client.Close()
				g.client = nil
			}
		}

		// Reset for reconnect
		g.resetConn()
	}
}

// connectAndRegister connects to the host and registers the guest.
// Returns nil on success, or an error if the connection fails.
func (g *Guest) connectAndRegister() error {
	if err := g.Connect(); err != nil {
		return err
	}

	if err := g.Register(); err != nil {
		g.client.Close()
		g.client = nil
		return err
	}

	return nil
}

// Stop gracefully stops the guest.
func (g *Guest) Stop() {
	g.mu.Lock()
	defer g.mu.Unlock()

	select {
	case <-g.stopCh:
		// already stopped
	default:
		close(g.stopCh)
	}
}

// isGuestNotFound checks whether an error indicates that the server
// no longer knows about this guest (e.g. server restart, guest removed).
// It matches RPC errors with code -32603 (InternalError) and a message
// containing "not found", which is the exact pattern the server returns
// when a guest ID is not in the registry.
func isGuestNotFound(err error) bool {
	if err == nil {
		return false
	}
	var rpcErr *rpc.RPCError
	if errors.As(err, &rpcErr) {
		// CodeInternalError (-32603) is used by the server for "guest not found".
		// CodeMethodNotFound (-32601) also contains "not found" but is unrelated.
		return rpcErr.Code == rpc.CodeInternalError && strings.Contains(rpcErr.Message, "not found")
	}
	return false
}

func (g *Guest) heartbeatLoop() {
	interval := g.config.HeartbeatInterval
	if interval == 0 {
		interval = 30 // default 30 seconds
	}

	ticker := time.NewTicker(time.Duration(interval) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			heartbeatFn := g.heartbeatForTest
			if heartbeatFn == nil {
				heartbeatFn = g.Heartbeat
			}
			if err := heartbeatFn(); err != nil {
				if isGuestNotFound(err) {
					g.log.Printf("guest not found on server, triggering reconnect: %v", err)
					g.setConnLost()
					return
				}
				g.log.Printf("heartbeat failed: %v", err)
			}
		case <-g.stopCh:
			return
		case <-g.connLost:
			// Connection lost — the Start loop will handle reconnection.
			return
		}
	}
}

// ExecuteTask executes a task assigned to this guest.
func (g *Guest) ExecuteTask(task TaskAssignment) (*TaskResult, error) {
	g.mu.Lock()
	if g.running {
		g.mu.Unlock()
		return nil, fmt.Errorf("guest is already running a task")
	}

	// Signal the server that we're about to execute this task so the
	// orchestrator's registry reflects the true state (RUNNING). This
	// prevents the server from reassigning the task to this guest if
	// ExecuteTask fails and the guest declines — FindAvailableGuests
	// will not return a guest whose registry state is RUNNING.
	_, err := g.client.Call("task.acknowledge", map[string]interface{}{
		"task_id":  task.TaskID,
		"guest_id": g.id,
	})
	if err != nil {
		g.log.Printf("[DISPATCH] failed to acknowledge task %s: %v",
			task.TaskID, err)
		// Continue anyway — the server may already know via heartbeat.
	}

	g.running = true
	g.currentTaskID = task.TaskID
	g.mu.Unlock()

	defer func() {
		g.mu.Lock()
		g.running = false
		taskID := g.currentTaskID
		g.currentTaskID = ""
		g.cancel = nil
		g.mu.Unlock()

		// Send a final heartbeat with the task ID before clearing it.
		// This gives the server a chance to see the task-specific heartbeat
		// and not mark the task as stuck while the guest is transitioning.
		if taskID != "" {
			if err := g.Heartbeat(); err != nil {
				g.log.Printf("failed to send final heartbeat for task %s: %v", taskID, err)
			}
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	g.mu.Lock()
	g.cancel = cancel
	g.mu.Unlock()

	// Set up timeout
	if g.config.TaskTimeout > 0 {
		var cancelTimeout context.CancelFunc
		ctx, cancelTimeout = context.WithTimeout(ctx, time.Duration(g.config.TaskTimeout)*time.Second)
		defer cancelTimeout()
	}

	g.log.Printf("[TASK] executing task %s", task.TaskID)
	g.log.Printf("[TASK] prompt: %s", task.Prompt)
	g.log.Printf("[TASK] tags: %v", task.Tags)

	// Send log that task started
	if err := g.SendLog(LogEntry{TaskID: task.TaskID, Line: "Task started", Level: "system"}); err != nil {
		g.log.Printf("failed to send task start log: %v", err)
	}

	// Execute the task using the handler
	result, err := g.handler(ctx, task, g.SendLog)
	if err != nil {
		g.log.Printf("task %s failed: %v", task.TaskID, err)
		failureResult := TaskResult{
			TaskID:  task.TaskID,
			Success: false,
			Error:   err.Error(),
		}
		if sendErr := g.SendResult(failureResult); sendErr != nil {
			g.log.Printf("failed to send failure result: %v", sendErr)
		}
		return nil, err
	}

	g.log.Printf("[TASK] task %s completed successfully", task.TaskID)
	if err := g.SendLog(LogEntry{TaskID: task.TaskID, Line: "Task completed successfully", Level: "system"}); err != nil {
		g.log.Printf("failed to send task complete log: %v", err)
	}

	if err := g.SendResult(*result); err != nil {
		g.log.Printf("failed to send result: %v", err)
	}

	return result, nil
}

// LoadConfig loads the guest configuration from a file.
func LoadConfig(path string) (config.GuestConfig, error) {
	return config.LoadGuestConfig(path)
}

// Reload updates the guest's runtime configuration from a new GuestConfig.
// It updates the config struct that controls heartbeat intervals, task timeouts,
// and log levels.
func (g *Guest) Reload(cfg config.GuestConfig) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if cfg.TaskTimeout != g.config.TaskTimeout {
		g.log.Printf("task_timeout updated: %ds", cfg.TaskTimeout)
	}
	if cfg.HeartbeatInterval != g.config.HeartbeatInterval {
		g.log.Printf("heartbeat_interval updated: %ds", cfg.HeartbeatInterval)
	}
	if cfg.SilenceTimeout != g.config.SilenceTimeout {
		g.log.Printf("silence_timeout updated: %ds", cfg.SilenceTimeout)
	}
	if cfg.LogLevel != g.config.LogLevel {
		g.log.Printf("log_level updated: %s", cfg.LogLevel)
	}
	g.config = cfg
}

// Ensure json is used
var _ = json.Marshal
