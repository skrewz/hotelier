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
type TaskLogEntry struct {
	TaskID    string    `json:"task_id"`
	Line      string    `json:"line"`
	Level     string    `json:"level,omitempty"`
	Timestamp time.Time `json:"timestamp"`
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

// LogAccumulator buffers agent text deltas and flushes them as complete messages.
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

// Server is the Check-In Host that orchestrates agents and tasks.
type Server struct {
	cfg            config.ServerConfig
	registry       *registry.AgentRegistry
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

// New creates a new Server instance.
func New(cfg config.ServerConfig) *Server {
	logPrefix := "[hotelier]"
	logger := log.New(os.Stdout, logPrefix+" ", log.LstdFlags)

	s := &Server{
		cfg:            cfg,
		registry:       registry.NewAgentRegistry(cfg.MaxAgents, logger.Printf),
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

	// Start stale agent cleanup
	go s.staleAgentCleanup()

	// Set up HTTP routes
	mux := http.NewServeMux()

	// WebSocket endpoint for agents
	mux.HandleFunc("/ws", s.HandleWebSocket)

	// REST API endpoints
	mux.HandleFunc("/api/tasks", s.HandleTasks)
	mux.HandleFunc("/api/tasks/", s.HandleTaskDetail)
	mux.HandleFunc("/api/agents", s.HandleAgents)
	mux.HandleFunc("/api/agents/", s.HandleAgentDetail)
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

// Registry returns the agent registry (for testing).
func (s *Server) Registry() *registry.AgentRegistry {
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
	// Agent → Host methods
	s.hub.RegisterMethod("agent.register", s.handleAgentRegister)
	s.hub.RegisterMethod("agent.unregister", s.handleAgentUnregister)
	s.hub.RegisterMethod("agent.heartbeat", s.handleAgentHeartbeat)
	s.hub.RegisterMethod("agent.log", s.handleAgentLog)
	s.hub.RegisterMethod("agent.result", s.handleAgentResult)
	s.hub.RegisterMethod("task.claim", s.handleTaskClaim)

	// Host → Agent methods (pushed by scheduler)
	s.hub.RegisterMethod("task.assign", s.handleTaskAssign)
	s.hub.RegisterMethod("task.cancel", s.handleTaskCancel)
}

// handleAgentRegister handles agent registration.
func (s *Server) handleAgentRegister(ctx context.Context, params json.RawMessage) (interface{}, *rpc.RPCError) {
	var req struct {
		ID   string   `json:"id"`
		Name string   `json:"name"`
		Tags []string `json:"tags"`
	}

	if err := json.Unmarshal(params, &req); err != nil {
		return nil, rpc.InvalidParamsError("invalid request parameters")
	}

	if req.ID == "" {
		return nil, rpc.InvalidParamsError("agent id is required")
	}

	agent, err := s.registry.Register(req.ID, req.Name, req.Tags)
	if err != nil {
		// Agent is already registered — update its connection and heartbeat.
		// This handles the case where an agent crashes and reconnects with
		// the same ephemeral ID, or a previous registration was missed.
		existing, exists := s.registry.GetAgent(req.ID)
		if exists {
			existing.Name = req.Name
			existing.Tags = req.Tags
			existing.ConnectedAt = time.Now()
			existing.LastHeartbeat = time.Now()
			existing.State = registry.AgentStateIdle

			// Update the connection mapping
			if connID, ok := rpc.ConnectionIDFromContext(ctx); ok {
				s.hub.RegisterAgentConnection(req.ID, connID)
				s.hub.SetConnectionRole(connID, rpc.ConnectionRoleAgent)
			}

			s.log.Printf("agent re-registered: %s (tags: %v)", existing.ID, existing.Tags)

			// Try to assign a pending task
			s.tryAssignTask(existing.ID)

			return map[string]interface{}{
				"status": "re-registered",
				"agent": map[string]interface{}{
					"id":   existing.ID,
					"name": existing.Name,
					"tags": existing.Tags,
				},
			}, nil
		}
		return nil, rpc.InternalError(err.Error())
	}

	// Record the agent-to-connection mapping so SendToAgent can find it
	if connID, ok := rpc.ConnectionIDFromContext(ctx); ok {
		s.hub.RegisterAgentConnection(req.ID, connID)
		s.hub.SetConnectionRole(connID, rpc.ConnectionRoleAgent)
	}

	s.log.Printf("agent registered: %s (tags: %v)", agent.ID, agent.Tags)

	// Try to assign a pending task
	s.tryAssignTask(agent.ID)

	return map[string]interface{}{
		"status": "registered",
		"agent": map[string]interface{}{
			"id":   agent.ID,
			"name": agent.Name,
			"tags": agent.Tags,
		},
	}, nil
}

// handleAgentUnregister handles agent unregistration.
func (s *Server) handleAgentUnregister(ctx context.Context, params json.RawMessage) (interface{}, *rpc.RPCError) {
	var req struct {
		ID string `json:"id"`
	}

	if err := json.Unmarshal(params, &req); err != nil {
		return nil, rpc.InvalidParamsError("invalid request parameters")
	}

	if err := s.registry.Unregister(req.ID); err != nil {
		return nil, rpc.InternalError(err.Error())
	}

	// Clean up the agent-connection mapping
	s.hub.UnregisterAgentConnection(req.ID)

	s.log.Printf("agent unregistered: %s", req.ID)

	// If the agent had a running task, re-queue it
	tasks := s.taskQueue.GetAgentTasks(req.ID)
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

// handleAgentHeartbeat handles agent heartbeat.
func (s *Server) handleAgentHeartbeat(ctx context.Context, params json.RawMessage) (interface{}, *rpc.RPCError) {
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

// handleAgentLog handles incoming log entries from agents.
func (s *Server) handleAgentLog(ctx context.Context, params json.RawMessage) (interface{}, *rpc.RPCError) {
	var entry struct {
		TaskID string `json:"task_id"`
		Line   string `json:"line"`
		Level  string `json:"level,omitempty"`
	}

	if err := json.Unmarshal(params, &entry); err != nil {
		return nil, rpc.InvalidParamsError("invalid request parameters")
	}

	if entry.TaskID == "" || entry.Line == "" {
		return nil, rpc.InvalidParamsError("task_id and line are required")
	}

	// Use the log accumulator to batch text deltas into complete messages.
	// Only non-text levels (tool, system, error) bypass the buffer.
	s.logAccumulator.Feed(
		entry.TaskID,
		entry.Line,
		entry.Level,
		func(e TaskLogEntry) {
			s.logStore.Add(e)
			// Persist to disk if configured
			if s.diskLogStore != nil {
				_ = s.diskLogStore.Append(logstore.Entry{
					TaskID:    e.TaskID,
					Line:      e.Line,
					Level:     e.Level,
					Timestamp: e.Timestamp,
				})
			}
			s.hub.SendNotification("", rpc.ConnectionRoleBrowser, "task.log", map[string]interface{}{
				"task_id": e.TaskID,
				"line":    e.Line,
				"level":   e.Level,
			})
		},
	)

	return map[string]interface{}{
		"status": "accepted",
	}, nil
}

// handleAgentResult handles task results from agents.
func (s *Server) handleAgentResult(ctx context.Context, params json.RawMessage) (interface{}, *rpc.RPCError) {
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

	// Find the agent that submitted this result and clear its task assignment
	if agentID, exists := s.taskQueue.GetAssignedAgent(result.TaskID); exists {
		if err := s.registry.ClearAgentTask(agentID); err != nil {
			s.log.Printf("failed to clear agent task for %s: %v", agentID, err)
		}
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

// handleTaskClaim handles an agent voluntarily claiming a pending task.
func (s *Server) handleTaskClaim(ctx context.Context, params json.RawMessage) (interface{}, *rpc.RPCError) {
	var req struct {
		ID   string   `json:"id"`
		Tags []string `json:"tags"`
	}

	if err := json.Unmarshal(params, &req); err != nil {
		return nil, rpc.InvalidParamsError("invalid request parameters")
	}

	// Find a pending task that matches the agent's tags
	agent, exists := s.registry.GetAgent(req.ID)
	if !exists {
		return nil, rpc.InvalidParamsError("agent not found")
	}

	// Get pending tasks and find the first one that matches
	pending := s.taskQueue.GetPendingTasks()
	var matchedTask *queue.Task

	for _, task := range pending {
		if len(task.Tags) == 0 {
			matchedTask = task
			break
		}
		// Check if agent has all required tags
		if s.matchesTags(agent.Tags, task.Tags) {
			matchedTask = task
			break
		}
	}

	if matchedTask == nil {
		return map[string]interface{}{
			"status": "no_task",
		}, nil
	}

	// Assign the task
	if err := s.taskQueue.Assign(matchedTask.ID, req.ID); err != nil {
		return nil, rpc.InternalError(err.Error())
	}

	if err := s.registry.SetAgentTask(req.ID, matchedTask.ID); err != nil {
		return nil, rpc.InternalError(err.Error())
	}

	// Push task to agent
	taskData := map[string]interface{}{
		"id":     matchedTask.ID,
		"repos":  matchedTask.Repos,
		"prompt": matchedTask.Prompt,
		"tags":   matchedTask.Tags,
	}

	if err := s.hub.SendToAgent(req.ID, "task.assign", taskData); err != nil {
		s.log.Printf("failed to push task to agent %s: %v", req.ID, err)
	}

	s.log.Printf("task %s claimed by agent %s", matchedTask.ID, req.ID)

	return map[string]interface{}{
		"status": "claimed",
		"task": map[string]interface{}{
			"id":     matchedTask.ID,
			"repos":  matchedTask.Repos,
			"prompt": matchedTask.Prompt,
			"tags":   matchedTask.Tags,
		},
	}, nil
}

// handleTaskAssign is registered for host→agent task.assign (for completeness).
func (s *Server) handleTaskAssign(ctx context.Context, params json.RawMessage) (interface{}, *rpc.RPCError) {
	return map[string]interface{}{
		"status": "ok",
	}, nil
}

// handleTaskCancel is registered for host→agent task.cancel (for completeness).
func (s *Server) handleTaskCancel(ctx context.Context, params json.RawMessage) (interface{}, *rpc.RPCError) {
	return map[string]interface{}{
		"status": "ok",
	}, nil
}

// tryAssignTask tries to assign a pending task to the given agent.
func (s *Server) tryAssignTask(agentID string) {
	agent, exists := s.registry.GetAgent(agentID)
	if !exists {
		return
	}

	// Find a pending task that matches the agent's tags
	pending := s.taskQueue.GetPendingTasks()
	var matchedTask *queue.Task

	for _, task := range pending {
		if len(task.Tags) == 0 {
			matchedTask = task
			break
		}
		if s.matchesTags(agent.Tags, task.Tags) {
			matchedTask = task
			break
		}
	}

	if matchedTask == nil {
		return
	}

	// Assign the task
	if err := s.taskQueue.Assign(matchedTask.ID, agentID); err != nil {
		s.log.Printf("failed to assign task %s to agent %s: %v", matchedTask.ID, agentID, err)
		return
	}

	if err := s.registry.SetAgentTask(agentID, matchedTask.ID); err != nil {
		s.log.Printf("failed to set agent task: %v", err)
		return
	}

	// Push task to agent
	taskData := map[string]interface{}{
		"id":     matchedTask.ID,
		"repos":  matchedTask.Repos,
		"prompt": matchedTask.Prompt,
		"tags":   matchedTask.Tags,
	}

	if err := s.hub.SendToAgent(agentID, "task.assign", taskData); err != nil {
		s.log.Printf("failed to push task to agent %s: %v", agentID, err)
		return
	}

	s.log.Printf("task %s assigned to agent %s", matchedTask.ID, agentID)
}

// staleAgentCleanup periodically removes stale agents.
func (s *Server) staleAgentCleanup() {
	interval := time.Duration(s.cfg.HeartbeatInterval) * time.Second
	if interval == 0 {
		interval = 30 * time.Second
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		time.Sleep(interval)
		stale := s.registry.RemoveStaleAgents(time.Duration(s.cfg.HeartbeatInterval) * time.Second)
		for _, agent := range stale {
			s.log.Printf("stale agent removed: %s", agent.ID)
		}
	}
}

// matchesTags checks if agent tags match required tags.
func (s *Server) matchesTags(agentTags, requiredTags []string) bool {
	if len(requiredTags) == 0 {
		return true
	}

	tagSet := make(map[string]struct{}, len(agentTags))
	for _, tag := range agentTags {
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
	// Default to browser role; agent role is set after agent.register RPC
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
		"agents": s.registry.Count(),
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

	// Try to assign to an available agent
	agents := s.registry.FindAvailableAgents(task.Tags)
	if len(agents) > 0 {
		s.tryAssignTask(agents[0].ID)
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

// handleAgents handles the /api/agents endpoint.
// HandleAgents handles the /api/agents endpoint.
func (s *Server) HandleAgents(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	switch r.Method {
	case http.MethodGet:
		agents := s.registry.GetAllAgents()
		json.NewEncoder(w).Encode(map[string]interface{}{
			"agents": agents,
			"count":  len(agents),
		})
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleAgentDetail handles the /api/agents/:id endpoint.
// HandleAgentDetail handles the /api/agents/:id endpoint.
func (s *Server) HandleAgentDetail(w http.ResponseWriter, r *http.Request) {
	agentID := r.URL.Path[len("/api/agents/"):]
	if agentID == "" {
		http.Error(w, "agent id required", http.StatusBadRequest)
		return
	}

	agent, exists := s.registry.GetAgent(agentID)
	if !exists {
		http.Error(w, "agent not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(agent)
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
