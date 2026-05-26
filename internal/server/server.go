package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"hotelier/pkg/config"
	"hotelier/pkg/logstore"
	"hotelier/pkg/queue"
	"hotelier/pkg/registry"
	"hotelier/pkg/rpc"
)

// TaskLogEntry is a single log line for a task.
// For tool call events, the Line field contains the original formatted
// string for backwards compatibility, but structured fields (ToolType,
// ToolName, etc.) carry the machine-readable data.
type TaskLogEntry struct {
	TaskID    string    `json:"task_id"`
	Line      string    `json:"line"`
	Level     string    `json:"level,omitempty"`
	Timestamp time.Time `json:"timestamp"`

	// Structured tool call fields (only set when Level == "tool")
	ToolType   string `json:"tool_type,omitempty"`   // "start", "output", "end"
	ToolName   string `json:"tool_name,omitempty"`   // e.g. "bash", "read"
	ToolID     string `json:"tool_id,omitempty"`     // unique tool call identifier
	ToolArgs   string `json:"tool_args,omitempty"`   // arguments/parameters
	ToolOutput string `json:"tool_output,omitempty"` // captured output
	ToolError  bool   `json:"tool_error,omitempty"`  // true if tool ended with error
}

// TaskLogStore stores log entries per task.
type TaskLogStore struct {
	logs map[string][]TaskLogEntry
	mu   sync.RWMutex
}

// NewTaskLogStore creates a new task log store.
func NewTaskLogStore() *TaskLogStore {
	return &TaskLogStore{
		logs: make(map[string][]TaskLogEntry),
	}
}

// Add appends a log entry for a task.
func (s *TaskLogStore) Add(entry TaskLogEntry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.logs[entry.TaskID] = append(s.logs[entry.TaskID], entry)
}

// Get returns all log entries for a task.
func (s *TaskLogStore) Get(taskID string) []TaskLogEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.logs[taskID]
}

// Count returns the number of log entries for a task.
func (s *TaskLogStore) Count(taskID string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.logs[taskID])
}

// LogAccumulator buffers guest text deltas and flushes them as complete messages.
// This prevents the UI from receiving hundreds of tiny fragments per response.
type LogAccumulator struct {
	mu          sync.Mutex
	buffer      map[string]string // taskID -> accumulated text
	lastFlush   map[string]time.Time
	flushPeriod time.Duration
	log         *log.Logger
}

// NewLogAccumulator creates a new log accumulator.
func NewLogAccumulator(logger *log.Logger) *LogAccumulator {
	return &LogAccumulator{
		buffer:      make(map[string]string),
		lastFlush:   make(map[string]time.Time),
		flushPeriod: 1 * time.Second, // flush every second of inactivity
		log:         logger,
	}
}

// Feed adds a log line to the accumulator and flushes if needed.
func (a *LogAccumulator) Feed(taskID, line, level string, emit func(entry TaskLogEntry)) {
	a.mu.Lock()
	defer a.mu.Unlock()

	now := time.Now()

	// Tool/system/error messages go through immediately.
	// "info" and empty level are treated as text deltas and batched,
	// unless the line is a tool message (detected by prefix).

	// Detect tool messages by prefix even when level is empty.
	isToolMsg := strings.HasPrefix(line, "[TOOL_START]") ||
		strings.HasPrefix(line, "[TOOL_OUTPUT]") ||
		strings.HasPrefix(line, "[TOOL_END]")

	if level != "" && level != "text" && level != "info" {
		// Explicit non-text level: send immediately.
		// Flush any pending text buffer first
		if buf, ok := a.buffer[taskID]; ok {
			a.emitNow(taskID, buf, "text", emit)
			delete(a.buffer, taskID)
		}
		a.emitNow(taskID, line, level, emit)
		return
	}

	// Tool messages bypass the buffer — send immediately.
	if isToolMsg {
		// Flush any pending text buffer first
		if buf, ok := a.buffer[taskID]; ok {
			a.emitNow(taskID, buf, "text", emit)
			delete(a.buffer, taskID)
		}
		a.emitNow(taskID, line, "tool", emit)
		return
	}

	// Check if we should flush due to inactivity
	if last, ok := a.lastFlush[taskID]; ok && now.Sub(last) > a.flushPeriod {
		// Flush existing buffer
		if buf, ok := a.buffer[taskID]; ok {
			a.emitNow(taskID, buf, "text", emit)
			delete(a.buffer, taskID)
		}
	}

	// Append to buffer
	a.buffer[taskID] += line
	a.lastFlush[taskID] = now
}

// FlushAll flushes all pending buffers immediately.
func (a *LogAccumulator) FlushAll(emit func(entry TaskLogEntry)) {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Sort task IDs for deterministic flush order (map iteration is non-deterministic).
	taskIDs := make([]string, 0, len(a.buffer))
	for taskID := range a.buffer {
		taskIDs = append(taskIDs, taskID)
	}
	sort.Strings(taskIDs)

	for _, taskID := range taskIDs {
		buf := a.buffer[taskID]
		if buf != "" {
			a.emitNow(taskID, buf, "text", emit)
		}
		delete(a.buffer, taskID)
	}
}

func (a *LogAccumulator) emitNow(taskID, line, level string, emit func(TaskLogEntry)) {
	if line == "" {
		return
	}
	a.lastFlush[taskID] = time.Now()
	entry := TaskLogEntry{
		TaskID:    taskID,
		Line:      line,
		Level:     level,
		Timestamp: time.Now(),
	}
	a.log.Printf("[task:%s] [%s] %s", taskID, level, line)
	emit(entry)
}

// Server is the Check-In Host that orchestrates guests and tasks.
type Server struct {
	cfg            config.ServerConfig
	registry       *registry.GuestRegistry
	taskQueue      *queue.TaskQueue
	hub            *rpc.Hub
	logStore       *TaskLogStore
	diskLogStore   *logstore.LogStore
	logAccumulator *LogAccumulator
	log            *log.Logger
	upgrader       *rpc.Upgrader
	mu             sync.RWMutex
	webDir         string
	templateDir    string
}

// Reload updates the server's runtime configuration from a new ServerConfig.
// It applies changes that can take effect without restarting:
//   - MaxGuests: updates the registry capacity
//   - LogDir: recreates the disk log store if the path changed
//   - TaskTimeout, HeartbeatInterval, SilenceTimeout, MaxLogSize: stored for future use
func (s *Server) Reload(cfg config.ServerConfig) {
	s.mu.Lock()
	defer s.mu.Unlock()

	old := s.cfg
	s.cfg = cfg

	// Update max guests if changed
	if cfg.MaxGuests != old.MaxGuests {
		s.registry.SetMaxGuests(cfg.MaxGuests)
		s.log.Printf("max_guests updated: %d", cfg.MaxGuests)
	}

	// Recreate disk log store if LogDir changed
	if cfg.LogDir != old.LogDir {
		if cfg.LogDir != "" {
			disk, err := logstore.New(cfg.LogDir)
			if err != nil {
				s.log.Printf("failed to create disk log store at %s: %v (keeping in-memory)", cfg.LogDir, err)
			} else {
				s.diskLogStore = disk
				s.log.Printf("disk log store enabled: %s", cfg.LogDir)
			}
		} else {
			s.diskLogStore = nil
			s.log.Printf("disk log store disabled")
		}
	}

	if cfg.TaskTimeout != old.TaskTimeout {
		s.log.Printf("task_timeout updated: %ds", cfg.TaskTimeout)
	}
	if cfg.HeartbeatInterval != old.HeartbeatInterval {
		s.log.Printf("heartbeat_interval updated: %ds", cfg.HeartbeatInterval)
	}
	if cfg.SilenceTimeout != old.SilenceTimeout {
		s.log.Printf("silence_timeout updated: %ds", cfg.SilenceTimeout)
	}
	if cfg.MaxLogSize != old.MaxLogSize {
		s.log.Printf("max_log_size updated: %d bytes", cfg.MaxLogSize)
	}
}

// New creates a new Server instance.
func New(cfg config.ServerConfig) *Server {
	logPrefix := "[hotelier]"
	logger := log.New(os.Stdout, logPrefix+" ", log.LstdFlags)

	s := &Server{
		cfg:            cfg,
		registry:       registry.NewGuestRegistry(cfg.MaxGuests, logger.Printf),
		taskQueue:      queue.NewTaskQueue(logger.Printf),
		hub:            rpc.NewHub(logger.Printf),
		logStore:       NewTaskLogStore(),
		logAccumulator: NewLogAccumulator(logger),
		log:            logger,
		upgrader:       rpc.NewUpgrader(),
		webDir:         "web/static",
		templateDir:    "web/templates",
	}

	// Initialize disk-backed log store if configured
	if cfg.LogDir != "" {
		disk, err := logstore.New(cfg.LogDir)
		if err != nil {
			logger.Printf("failed to create disk log store: %v (logs will be in-memory only)", err)
		} else {
			s.diskLogStore = disk
			logger.Printf("disk log store enabled: %s", cfg.LogDir)
		}
	}

	s.registerRPCMethods()

	return s
}

// Start starts the server.
func (s *Server) Start() error {
	s.log.Printf("starting hotelier on %s:%d", s.cfg.Host, s.cfg.Port)

	// Start the hub in the background
	go s.hub.Run()

	// Start stale guest cleanup
	go s.staleGuestCleanup()

	// Set up HTTP routes
	mux := http.NewServeMux()

	// WebSocket endpoint for guests
	mux.HandleFunc("/ws", s.HandleWebSocket)

	// REST API endpoints
	mux.HandleFunc("/api/tasks", s.HandleTasks)
	mux.HandleFunc("/api/tasks/", s.HandleTaskDetail)
	mux.HandleFunc("/api/guests", s.HandleGuests)
	mux.HandleFunc("/api/guests/", s.HandleGuestDetail)
	mux.HandleFunc("/api/health", s.HandleHealth)
	mux.HandleFunc("/api/logs", s.HandleLogs)
	mux.HandleFunc("/api/logs/", s.HandleLogEntry)

	// Web UI
	mux.HandleFunc("/", s.HandleWebUI)

	addr := fmt.Sprintf("%s:%d", s.cfg.Host, s.cfg.Port)
	s.log.Printf("server listening on %s", addr)

	return http.ListenAndServe(addr, mux)
}

// Stop gracefully stops the server.
func (s *Server) Stop(ctx context.Context) error {
	s.log.Printf("shutting down server")
	return nil
}

// Registry returns the guest registry (for testing).
func (s *Server) Registry() *registry.GuestRegistry {
	return s.registry
}

// TaskQueue returns the task queue (for testing).
func (s *Server) TaskQueue() *queue.TaskQueue {
	return s.taskQueue
}

// Hub returns the RPC hub (for testing).
func (s *Server) Hub() *rpc.Hub {
	return s.hub
}

// LogStore returns the task log store (for testing).
func (s *Server) LogStore() *TaskLogStore {
	return s.logStore
}

// LogAccumulator returns the log accumulator (for testing).
func (s *Server) LogAccumulator() *LogAccumulator {
	return s.logAccumulator
}

// DiskLogStore returns the disk-backed log store (for testing).
func (s *Server) DiskLogStore() *logstore.LogStore {
	return s.diskLogStore
}

func (s *Server) registerRPCMethods() {
	// Guest → Host methods
	s.hub.RegisterMethod("guest.register", s.handleGuestRegister)
	s.hub.RegisterMethod("guest.unregister", s.handleGuestUnregister)
	s.hub.RegisterMethod("guest.heartbeat", s.handleGuestHeartbeat)
	s.hub.RegisterMethod("guest.log", s.handleGuestLog)
	s.hub.RegisterMethod("guest.result", s.handleGuestResult)
	s.hub.RegisterMethod("guest.task_declined", s.handleGuestTaskDeclined)

	// Host → Guest methods (pushed by scheduler)
	s.hub.RegisterMethod("task.assign", s.handleTaskAssign)
	s.hub.RegisterMethod("task.cancel", s.handleTaskCancel)
}

// handleGuestRegister handles guest registration.
func (s *Server) handleGuestRegister(ctx context.Context, params json.RawMessage) (interface{}, *rpc.RPCError) {
	var req struct {
		ID   string   `json:"id"`
		Name string   `json:"name"`
		Tags []string `json:"tags"`
	}

	if err := json.Unmarshal(params, &req); err != nil {
		return nil, rpc.InvalidParamsError("invalid request parameters")
	}

	if req.ID == "" {
		return nil, rpc.InvalidParamsError("guest id is required")
	}

	guest, err := s.registry.Register(req.ID, req.Name, req.Tags)
	if err != nil {
		// Guest is already registered — update its connection and heartbeat.
		// This handles the case where a guest crashes and reconnects with
		// the same ephemeral ID, or a previous registration was missed.
		existing, exists := s.registry.GetGuest(req.ID)
		if exists {
			existing.Name = req.Name
			existing.Tags = req.Tags
			existing.ConnectedAt = time.Now()
			existing.LastHeartbeat = time.Now()
			existing.State = registry.GuestStateIdle

			// Update the connection mapping
			if connID, ok := rpc.ConnectionIDFromContext(ctx); ok {
				s.hub.RegisterGuestConnection(req.ID, connID)
				s.hub.SetConnectionRole(connID, rpc.ConnectionRoleGuest)
			}

			s.log.Printf("guest re-registered: %s (tags: %v)", existing.ID, existing.Tags)

			// Try to assign a pending task
			s.tryAssignTask(existing.ID)

			return map[string]interface{}{
				"status": "re-registered",
				"guest": map[string]interface{}{
					"id":   existing.ID,
					"name": existing.Name,
					"tags": existing.Tags,
				},
			}, nil
		}
		return nil, rpc.InternalError(err.Error())
	}

	// Record the guest-to-connection mapping so SendToGuest can find it
	if connID, ok := rpc.ConnectionIDFromContext(ctx); ok {
		s.hub.RegisterGuestConnection(req.ID, connID)
		s.hub.SetConnectionRole(connID, rpc.ConnectionRoleGuest)
	}

	s.log.Printf("guest registered: %s (tags: %v)", guest.ID, guest.Tags)

	// Try to assign a pending task
	s.tryAssignTask(guest.ID)

	return map[string]interface{}{
		"status": "registered",
		"guest": map[string]interface{}{
			"id":   guest.ID,
			"name": guest.Name,
			"tags": guest.Tags,
		},
	}, nil
}

// handleGuestUnregister handles guest unregistration.
func (s *Server) handleGuestUnregister(ctx context.Context, params json.RawMessage) (interface{}, *rpc.RPCError) {
	var req struct {
		ID string `json:"id"`
	}

	if err := json.Unmarshal(params, &req); err != nil {
		return nil, rpc.InvalidParamsError("invalid request parameters")
	}

	if err := s.registry.Unregister(req.ID); err != nil {
		return nil, rpc.InternalError(err.Error())
	}

	// Clean up the guest-connection mapping
	s.hub.UnregisterGuestConnection(req.ID)

	s.log.Printf("guest unregistered: %s", req.ID)

	// If the guest had a running task, re-queue it
	tasks := s.taskQueue.GetGuestTasks(req.ID)
	for _, task := range tasks {
		if task.Status == queue.TaskStatusRunning || task.Status == queue.TaskStatusAssigned {
			if err := s.taskQueue.UpdateStatus(task.ID, queue.TaskStatusPending); err != nil {
				s.log.Printf("failed to re-queue task %s: %v", task.ID, err)
			}
		}
	}

	return map[string]interface{}{
		"status": "unregistered",
	}, nil
}

// handleGuestHeartbeat handles guest heartbeat.
func (s *Server) handleGuestHeartbeat(ctx context.Context, params json.RawMessage) (interface{}, *rpc.RPCError) {
	var req struct {
		ID string `json:"id"`
	}

	if err := json.Unmarshal(params, &req); err != nil {
		return nil, rpc.InvalidParamsError("invalid request parameters")
	}

	if err := s.registry.Heartbeat(req.ID); err != nil {
		return nil, rpc.InternalError(err.Error())
	}

	return map[string]interface{}{
		"status": "ok",
	}, nil
}

// handleGuestLog handles incoming log entries from guests.
// For tool call events, the guest sends structured fields (tool_type,
// tool_name, tool_id, etc.) alongside the formatted line string.
func (s *Server) handleGuestLog(ctx context.Context, params json.RawMessage) (interface{}, *rpc.RPCError) {
	var req struct {
		TaskID     string `json:"task_id"`
		Line       string `json:"line"`
		Level      string `json:"level,omitempty"`
		ToolType   string `json:"tool_type,omitempty"`
		ToolName   string `json:"tool_name,omitempty"`
		ToolID     string `json:"tool_id,omitempty"`
		ToolArgs   string `json:"tool_args,omitempty"`
		ToolOutput string `json:"tool_output,omitempty"`
		ToolError  bool   `json:"tool_error,omitempty"`
	}

	if err := json.Unmarshal(params, &req); err != nil {
		return nil, rpc.InvalidParamsError("invalid request parameters")
	}

	if req.TaskID == "" || req.Line == "" {
		return nil, rpc.InvalidParamsError("task_id and line are required")
	}

	// Use the log accumulator to batch text deltas into complete messages.
	// Only non-text levels (tool, system, error) bypass the buffer.
	s.logAccumulator.Feed(
		req.TaskID,
		req.Line,
		req.Level,
		func(e TaskLogEntry) {
			// Populate structured tool call fields
			if req.Level == "tool" || req.ToolType != "" {
				e.ToolType = req.ToolType
				e.ToolName = req.ToolName
				e.ToolID = req.ToolID
				e.ToolArgs = req.ToolArgs
				e.ToolOutput = req.ToolOutput
				e.ToolError = req.ToolError
			}
			s.logStore.Add(e)
			// Persist to disk if configured
			if s.diskLogStore != nil {
				_ = s.diskLogStore.Append(logstore.Entry{
					TaskID:     e.TaskID,
					Line:       e.Line,
					Level:      e.Level,
					Timestamp:  e.Timestamp,
					ToolType:   e.ToolType,
					ToolName:   e.ToolName,
					ToolID:     e.ToolID,
					ToolArgs:   e.ToolArgs,
					ToolOutput: e.ToolOutput,
					ToolError:  e.ToolError,
				})
			}
			s.hub.SendNotification("", rpc.ConnectionRoleBrowser, "task.log", map[string]interface{}{
				"task_id":     e.TaskID,
				"line":        e.Line,
				"level":       e.Level,
				"tool_type":   e.ToolType,
				"tool_name":   e.ToolName,
				"tool_id":     e.ToolID,
				"tool_args":   e.ToolArgs,
				"tool_output": e.ToolOutput,
				"tool_error":  e.ToolError,
			})
		},
	)

	return map[string]interface{}{
		"status": "accepted",
	}, nil
}

// handleGuestResult handles task results from guests.
func (s *Server) handleGuestResult(ctx context.Context, params json.RawMessage) (interface{}, *rpc.RPCError) {
	var result struct {
		TaskID  string `json:"task_id"`
		Success bool   `json:"success"`
		Output  string `json:"output,omitempty"`
		Error   string `json:"error,omitempty"`
	}

	if err := json.Unmarshal(params, &result); err != nil {
		return nil, rpc.InvalidParamsError("invalid request parameters")
	}

	if result.TaskID == "" {
		return nil, rpc.InvalidParamsError("task_id is required")
	}

	if result.Success {
		if err := s.taskQueue.Complete(result.TaskID, result.Output); err != nil {
			return nil, rpc.InternalError(err.Error())
		}
	} else {
		if err := s.taskQueue.Fail(result.TaskID, result.Error); err != nil {
			return nil, rpc.InternalError(err.Error())
		}
	}

	// Flush any remaining accumulated logs for this task
	s.logAccumulator.FlushAll(func(e TaskLogEntry) {
		s.logStore.Add(e)
		s.hub.SendNotification("", rpc.ConnectionRoleBrowser, "task.log", map[string]interface{}{
			"task_id": e.TaskID,
			"line":    e.Line,
			"level":   e.Level,
		})
	})

	// Find the guest that submitted this result and clear its task assignment
	if guestID, exists := s.taskQueue.GetAssignedGuest(result.TaskID); exists {
		if err := s.registry.ClearGuestTask(guestID); err != nil {
			s.log.Printf("failed to clear guest task for %s: %v", guestID, err)
		}

		// Try to assign the next pending task to this now-idle guest
		s.tryAssignTask(guestID)
	}

	// Notify UI of task completion
	s.hub.Broadcast(rpc.ConnectionRoleBrowser, &rpc.JSONRPCMessage{
		JSONRPC: "2.0",
		Method:  "task.updated",
		Params: json.RawMessage(fmt.Sprintf(`{"task_id":"%s","status":"%s"}`,
			result.TaskID,
			map[bool]string{true: "completed", false: "failed"}[result.Success])),
	})

	return map[string]interface{}{
		"status": "accepted",
	}, nil
}

// handleGuestTaskDeclined handles a guest declining a task assignment.
// This is called when the guest is already running a task and cannot accept
// a new one. The server reverts the assignment and tries to assign the task
// to another eligible guest.
func (s *Server) handleGuestTaskDeclined(ctx context.Context, params json.RawMessage) (interface{}, *rpc.RPCError) {
	var req struct {
		TaskID  string `json:"task_id"`
		GuestID string `json:"guest_id"`
		Reason  string `json:"reason,omitempty"`
	}

	if err := json.Unmarshal(params, &req); err != nil {
		return nil, rpc.InvalidParamsError("invalid request parameters")
	}

	if req.TaskID == "" {
		return nil, rpc.InvalidParamsError("task_id is required")
	}

	s.log.Printf("task %s declined by guest %s: %s", req.TaskID, req.GuestID, req.Reason)

	// Revert the task assignment in the queue
	if err := s.taskQueue.UpdateStatus(req.TaskID, queue.TaskStatusPending); err != nil {
		s.log.Printf("failed to re-queue task %s: %v", req.TaskID, err)
	} else {
		// Clear the guest's task assignment in the registry
		if err := s.registry.ClearGuestTask(req.GuestID); err != nil {
			s.log.Printf("failed to clear guest task for %s: %v", req.GuestID, err)
		}

		// Notify UI of task re-queue
		s.hub.Broadcast(rpc.ConnectionRoleBrowser, &rpc.JSONRPCMessage{
			JSONRPC: "2.0",
			Method:  "task.updated",
			Params:  json.RawMessage(fmt.Sprintf(`{"task_id":"%s","status":"pending"}`, req.TaskID)),
		})

		// Try to assign the task to another eligible guest
		s.tryAssignTaskToEligible(req.TaskID)
	}

	return map[string]interface{}{
		"status": "accepted",
	}, nil
}

// handleTaskAssign is registered for host→guest task.assign (for completeness).
func (s *Server) handleTaskAssign(ctx context.Context, params json.RawMessage) (interface{}, *rpc.RPCError) {
	return map[string]interface{}{
		"status": "ok",
	}, nil
}

// handleTaskCancel is registered for host→guest task.cancel (for completeness).
func (s *Server) handleTaskCancel(ctx context.Context, params json.RawMessage) (interface{}, *rpc.RPCError) {
	return map[string]interface{}{
		"status": "ok",
	}, nil
}

// tryAssignTask tries to assign a pending task to the given guest.
// tryAssignTaskToEligible finds an idle guest that matches the given task's
// tags and assigns the task to that guest. It is used when a previously-
// assigned guest declines the task.
func (s *Server) tryAssignTaskToEligible(taskID string) {
	task, exists := s.taskQueue.Get(taskID)
	if !exists {
		return
	}

	guests := s.registry.FindAvailableGuests(task.Tags)
	if len(guests) == 0 {
		return
	}

	// Try each idle guest in order until one accepts
	for _, guest := range guests {
		// Skip the guest that already declined
		if guest.ID == task.AssignedTo {
			continue
		}

		if err := s.taskQueue.Assign(task.ID, guest.ID); err != nil {
			s.log.Printf("failed to assign task %s to guest %s: %v", task.ID, guest.ID, err)
			continue
		}

		if err := s.registry.SetGuestTask(guest.ID, task.ID); err != nil {
			s.log.Printf("failed to set guest task: %v", err)
			s.taskQueue.UpdateStatus(task.ID, queue.TaskStatusPending)
			continue
		}

		taskData := map[string]interface{}{
			"id":     task.ID,
			"repos":  task.Repos,
			"prompt": task.Prompt,
			"tags":   task.Tags,
		}

		if err := s.hub.SendToGuest(guest.ID, "task.assign", taskData); err != nil {
			s.log.Printf("failed to push task to guest %s: %v", guest.ID, err)
			s.registry.ClearGuestTask(guest.ID)
			s.taskQueue.UpdateStatus(task.ID, queue.TaskStatusPending)
			continue
		}

		s.log.Printf("task %s reassigned to guest %s", task.ID, guest.ID)
		return
	}

	// No guest accepted — re-queue the task
	if err := s.taskQueue.UpdateStatus(taskID, queue.TaskStatusPending); err != nil {
		s.log.Printf("failed to re-queue task %s after all guests declined: %v", taskID, err)
	}
}

// tryAssignTask tries to assign a pending task to the given guest.
func (s *Server) tryAssignTask(guestID string) {
	guest, exists := s.registry.GetGuest(guestID)
	if !exists {
		return
	}

	// Only assign to idle guests — a guest that is already running a task
	// on the client side should not receive another assignment.
	if guest.State != registry.GuestStateIdle {
		return
	}

	// Find a pending task that matches the guest's tags
	pending := s.taskQueue.GetPendingTasks()
	var matchedTask *queue.Task

	for _, task := range pending {
		if len(task.Tags) == 0 {
			matchedTask = task
			break
		}
		if s.matchesTags(guest.Tags, task.Tags) {
			matchedTask = task
			break
		}
	}

	if matchedTask == nil {
		return
	}

	// Assign the task
	if err := s.taskQueue.Assign(matchedTask.ID, guestID); err != nil {
		s.log.Printf("failed to assign task %s to guest %s: %v", matchedTask.ID, guestID, err)
		return
	}

	if err := s.registry.SetGuestTask(guestID, matchedTask.ID); err != nil {
		s.log.Printf("failed to set guest task: %v", err)
		return
	}

	// Push task to guest
	taskData := map[string]interface{}{
		"id":     matchedTask.ID,
		"repos":  matchedTask.Repos,
		"prompt": matchedTask.Prompt,
		"tags":   matchedTask.Tags,
	}

	if err := s.hub.SendToGuest(guestID, "task.assign", taskData); err != nil {
		s.log.Printf("failed to push task to guest %s: %v", guestID, err)
		return
	}

	s.log.Printf("task %s assigned to guest %s", matchedTask.ID, guestID)
}

// staleGuestCleanup periodically removes stale guests and kills their running tasks.
// When a guest's RPC connection has been silent for longer than SilenceTimeout,
// the server sends a task.cancel RPC to the guest to abort the pi subprocess,
// then marks the task as failed and re-queues it.
func (s *Server) staleGuestCleanup() {
	interval := time.Duration(s.cfg.HeartbeatInterval) * time.Second
	if interval == 0 {
		interval = 30 * time.Second
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// First: kill running tasks for guests that have been silent too long.
			// This runs before stale guest removal so we can send task.cancel.
			s.checkSilentGuests()

			// Then: Remove guests that have been completely silent (no heartbeat).
			stale := s.registry.RemoveStaleGuests(time.Duration(s.cfg.HeartbeatInterval) * time.Second)
			for _, guest := range stale {
				s.log.Printf("stale guest removed: %s", guest.ID)
			}
		}
	}
}

// checkSilentGuests finds guests that have been silent (no heartbeat) for longer
// than the configured SilenceTimeout and kills their running tasks.
func (s *Server) checkSilentGuests() {
	timeout := time.Duration(s.cfg.SilenceTimeout) * time.Second
	if timeout == 0 {
		return // Silence detection disabled
	}

	now := time.Now()
	for _, guest := range s.registry.GetAllGuests() {
		if guest.TaskID == "" {
			continue // Not running a task
		}

		if now.Sub(guest.LastHeartbeat) > timeout {
			s.log.Printf("guest %s silent for %v, killing task %s",
				guest.ID, now.Sub(guest.LastHeartbeat), guest.TaskID)

			// Mark the task as failed in the queue
			if err := s.taskQueue.Fail(guest.TaskID, fmt.Sprintf("guest went silent for %.0f seconds", now.Sub(guest.LastHeartbeat).Seconds())); err != nil {
				s.log.Printf("failed to mark task %s as failed: %v", guest.TaskID, err)
			}

			// Clear the guest's task assignment
			if err := s.registry.KillRunningGuestTask(guest.ID); err != nil {
				s.log.Printf("failed to kill guest %s task: %v", guest.ID, err)
			}

			// Notify the UI
			s.hub.Broadcast(rpc.ConnectionRoleBrowser, &rpc.JSONRPCMessage{
				JSONRPC: "2.0",
				Method:  "task.updated",
				Params: json.RawMessage(fmt.Sprintf(`{"task_id":"%s","status":"failed"}`,
					guest.TaskID)),
			})

			// Send task.cancel RPC to the guest to abort the pi subprocess.
			// This is best-effort — the guest may already be disconnected.
			cancelParams := map[string]interface{}{
				"task_id": guest.TaskID,
				"reason":  "guest silence timeout",
			}
			if err := s.hub.SendToGuest(guest.ID, "task.cancel", cancelParams); err != nil {
				s.log.Printf("failed to send task.cancel to guest %s: %v (guest may be disconnected)", guest.ID, err)
			}
		}
	}
}

// matchesTags checks if guest tags match required tags.
func (s *Server) matchesTags(guestTags, requiredTags []string) bool {
	if len(requiredTags) == 0 {
		return true
	}

	tagSet := make(map[string]struct{}, len(guestTags))
	for _, tag := range guestTags {
		tagSet[tag] = struct{}{}
	}

	for _, req := range requiredTags {
		if _, ok := tagSet[req]; !ok {
			return false
		}
	}
	return true
}

// handleWebSocket handles WebSocket connections.
// HandleWebSocket handles WebSocket connections.
func (s *Server) HandleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := s.upgrader.ServeHTTP(w, r)
	if err != nil {
		s.log.Printf("websocket upgrade failed: %v", err)
		return
	}

	// Generate a unique connection ID
	connID := fmt.Sprintf("conn-%d", time.Now().UnixNano())
	client := s.hub.NewConnection(connID, conn)
	// Default to browser role; guest role is set after guest.register RPC
	s.hub.SetConnectionRole(connID, rpc.ConnectionRoleBrowser)
	go client.ReadLoop()
	go client.WriteLoop()
}

// handleWebUI serves the web interface.
// HandleWebUI serves the web interface.
func (s *Server) HandleWebUI(w http.ResponseWriter, r *http.Request) {
	// Serve static files
	if r.URL.Path != "/" && r.URL.Path != "" {
		filePath := filepath.Join(s.webDir, r.URL.Path)
		if _, err := os.Stat(filePath); err == nil {
			http.ServeFile(w, r, filePath)
			return
		}
	}

	// Serve index.html for all other routes (SPA routing)
	indexPath := filepath.Join(s.webDir, "index.html")
	if _, err := os.Stat(indexPath); err == nil {
		http.ServeFile(w, r, indexPath)
		return
	}

	http.NotFound(w, r)
}

// handleHealth returns the server health status.
// HandleHealth returns the server health status.
func (s *Server) HandleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status": "healthy",
		"guests": s.registry.Count(),
		"tasks":  s.taskQueue.Count(),
	})
}

// handleTasks handles the /api/tasks endpoint.
// HandleTasks handles the /api/tasks endpoint.
func (s *Server) HandleTasks(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	switch r.Method {
	case http.MethodGet:
		s.handleGetTasks(w, r)
	case http.MethodPost:
		s.handleCreateTask(w, r)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleGetTasks(w http.ResponseWriter, r *http.Request) {
	tasks := s.taskQueue.GetAllTasks()
	json.NewEncoder(w).Encode(map[string]interface{}{
		"tasks": tasks,
		"count": len(tasks),
	})
}

func (s *Server) handleCreateTask(w http.ResponseWriter, r *http.Request) {
	var task queue.Task
	if err := json.NewDecoder(r.Body).Decode(&task); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if task.ID == "" {
		task.ID = fmt.Sprintf("task-%d", time.Now().UnixNano())
	}

	if err := s.taskQueue.Add(&task); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Try to assign to an available guest
	guests := s.registry.FindAvailableGuests(task.Tags)
	if len(guests) > 0 {
		s.tryAssignTask(guests[0].ID)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(task)
}

// handleTaskDetail handles the /api/tasks/:id endpoint.
// HandleTaskDetail handles the /api/tasks/:id endpoint.
func (s *Server) HandleTaskDetail(w http.ResponseWriter, r *http.Request) {
	taskID := strings.TrimPrefix(r.URL.Path, "/api/tasks/")
	if taskID == "" {
		http.Error(w, "task id required", http.StatusBadRequest)
		return
	}

	task, exists := s.taskQueue.Get(taskID)
	if !exists {
		http.Error(w, "task not found", http.StatusNotFound)
		return
	}

	logs := s.logStore.Get(taskID)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"task":      task,
		"logs":      logs,
		"log_count": len(logs),
	})
}

// handleGuests handles the /api/guests endpoint.
// HandleGuests handles the /api/guests endpoint.
func (s *Server) HandleGuests(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	switch r.Method {
	case http.MethodGet:
		guests := s.registry.GetAllGuests()
		json.NewEncoder(w).Encode(map[string]interface{}{
			"guests": guests,
			"count":  len(guests),
		})
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleGuestDetail handles the /api/guests/:id endpoint.
// HandleGuestDetail handles the /api/guests/:id endpoint.
func (s *Server) HandleGuestDetail(w http.ResponseWriter, r *http.Request) {
	guestID := r.URL.Path[len("/api/guests/"):]
	if guestID == "" {
		http.Error(w, "guest id required", http.StatusBadRequest)
		return
	}

	guest, exists := s.registry.GetGuest(guestID)
	if !exists {
		http.Error(w, "guest not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(guest)
}

// HandleLogs handles the /api/logs endpoint — returns a list of date directories.
func (s *Server) HandleLogs(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if s.diskLogStore == nil {
		http.Error(w, "log store not configured", http.StatusServiceUnavailable)
		return
	}

	dates, err := s.diskLogStore.ListDates()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(map[string]interface{}{
		"dates": dates,
		"count": len(dates),
	})
}

// HandleLogEntry handles /api/logs/:date/tasks and /api/logs/:date/:task.
func (s *Server) HandleLogEntry(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if s.diskLogStore == nil {
		http.Error(w, "log store not configured", http.StatusServiceUnavailable)
		return
	}

	path := strings.TrimPrefix(r.URL.Path, "/api/logs/")
	parts := strings.Split(path, "/")

	switch len(parts) {
	case 1:
		// /api/logs/:date → list tasks for this date
		if parts[0] == "" {
			http.Error(w, "date required", http.StatusBadRequest)
			return
		}
		tasks, err := s.diskLogStore.ListTasks(parts[0])
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		json.NewEncoder(w).Encode(map[string]interface{}{
			"date":  parts[0],
			"tasks": tasks,
			"count": len(tasks),
		})

	case 2:
		// /api/logs/:date/:task → return log entries
		date, taskID := parts[0], parts[1]
		if date == "" || taskID == "" {
			http.Error(w, "date and task id required", http.StatusBadRequest)
			return
		}
		entries, err := s.diskLogStore.ReadLogs(date, taskID)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		json.NewEncoder(w).Encode(map[string]interface{}{
			"date":    date,
			"task_id": taskID,
			"entries": entries,
			"count":   len(entries),
		})

	default:
		http.Error(w, "not found", http.StatusNotFound)
	}
}
