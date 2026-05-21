package guest

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"hotelier/pkg/config"
	"hotelier/pkg/rpc"

	"github.com/google/uuid"
)

// LogEntry represents a log entry sent from a guest to the host.
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

// TaskAssignment represents a task assigned to a guest.
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

// Guest is the client-side guest that connects to the Check-In Host.
type Guest struct {
	id       string
	name     string
	tags     []string
	config   config.GuestConfig
	client   *rpc.Client
	hub      *rpc.ClientHub
	log      *log.Logger
	handler  Handler
	mu       sync.Mutex
	running  bool
	cancel   context.CancelFunc // cancels the current task's context
	stopCh   chan struct{}
	connLost chan struct{} // closed when the connection drops
	callResp chan *rpc.JSONRPCMessage
	taskCh   chan TaskAssignment // incoming task assignments
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
		g.log.Printf("[RPC] dispatching task %s to execution", task.TaskID)
		select {
		case g.taskCh <- task:
			g.log.Printf("[RPC] task %s queued for execution", task.TaskID)
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
			} else {
				g.log.Printf("[DISPATCH] task %s finished: success=%v output=%q", task.TaskID, result.Success, result.Output)
			}

			// If configured, automatically claim the next pending task
			if g.config.AutoClaimNextTask {
				g.log.Printf("[DISPATCH] auto-claiming next pending task")
				g.tryClaimNextTask()
			}
		}
	}
}

// tryClaimNextTask sends a task.claim RPC to pick up the next pending task.
func (g *Guest) tryClaimNextTask() {
	if g.client == nil {
		return
	}

	params := map[string]interface{}{
		"id":   g.id,
		"tags": g.tags,
	}

	_, err := g.client.Call("task.claim", params)
	if err != nil {
		g.log.Printf("[DISPATCH] failed to claim next task: %v", err)
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
func (g *Guest) Heartbeat() error {
	params := map[string]string{
		"id": g.id,
	}

	_, err := g.client.Call("guest.heartbeat", params)
	if err != nil {
		return fmt.Errorf("heartbeat: %w", err)
	}
	return nil
}

// SendLog sends a log entry to the Check-In Host.
func (g *Guest) SendLog(taskID, line string) error {
	entry := LogEntry{
		TaskID:    taskID,
		Line:      line,
		Level:     "info",
		Timestamp: time.Now(),
	}

	err := g.client.SendNotification("guest.log", entry)
	if err != nil {
		return fmt.Errorf("send log: %w", err)
	}
	return nil
}

// SendResult submits the final result of a task to the Check-In Host.
func (g *Guest) SendResult(result TaskResult) error {
	err := g.client.SendNotification("guest.result", result)
	if err != nil {
		return fmt.Errorf("send result: %w", err)
	}
	return nil
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
			if err := g.Heartbeat(); err != nil {
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
	g.running = true
	g.mu.Unlock()

	defer func() {
		g.mu.Lock()
		g.running = false
		g.cancel = nil
		g.mu.Unlock()
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
	g.log.Printf("[TASK] repos: %v", task.Repos)
	g.log.Printf("[TASK] prompt: %s", task.Prompt)
	g.log.Printf("[TASK] tags: %v", task.Tags)

	// Send log that task started
	if err := g.SendLog(task.TaskID, "Task started"); err != nil {
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
	if err := g.SendLog(task.TaskID, "Task completed successfully"); err != nil {
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

// Ensure json is used
var _ = json.Marshal
