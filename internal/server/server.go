package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"hotelier/pkg/config"
	"hotelier/pkg/fatalwriter"
	"hotelier/pkg/logstore"
	"hotelier/pkg/orchestrator"
	"hotelier/pkg/persona"
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
//
// Delta types (text, thinking, info, empty) are batched. Non-delta types
// (tool, system, error) bypass the buffer and are emitted immediately.
// When a non-delta level arrives, any pending delta buffer is flushed first.
// When the delta level changes (e.g. text→thinking), the previous buffer
// is flushed with its level before accumulating the new level.
type LogAccumulator struct {
	mu          sync.Mutex
	buffer      map[string]string // taskID -> accumulated text
	bufferLevel map[string]string // taskID -> level of current buffer ("text", "thinking", etc.)
	lastFlush   map[string]time.Time
	flushPeriod time.Duration
	log         *log.Logger
}

// NewLogAccumulator creates a new log accumulator.
func NewLogAccumulator(logger *log.Logger) *LogAccumulator {
	return &LogAccumulator{
		buffer:      make(map[string]string),
		bufferLevel: make(map[string]string),
		lastFlush:   make(map[string]time.Time),
		flushPeriod: 1 * time.Second, // flush every second of inactivity
		log:         logger,
	}
}

// isDeltaLevel returns true if the level is a streaming delta type that
// should be batched (text, thinking, info, or empty).
func isDeltaLevel(level string) bool {
	return level == "text" || level == "thinking" || level == "info" || level == ""
}

// effectiveLevel returns the canonical level for a delta. Empty string
// and "info" are mapped to "text" for backwards compatibility.
// "thinking" is preserved so the frontend can render it distinctly.
func effectiveLevel(level string) string {
	switch level {
	case "", "info":
		return "text"
	default:
		return level
	}
}

// flushBuffer flushes the pending buffer for a task with its stored level.
// Must be called with a.mu held.
func (a *LogAccumulator) flushBuffer(taskID string, emit func(TaskLogEntry)) {
	buf, ok := a.buffer[taskID]
	if !ok || buf == "" {
		return
	}
	lvl := a.bufferLevel[taskID]
	if lvl == "" {
		lvl = "text"
	}
	a.emitNow(taskID, buf, lvl, emit)
	delete(a.buffer, taskID)
	delete(a.bufferLevel, taskID)
}

// Feed adds a log line to the accumulator and flushes if needed.
func (a *LogAccumulator) Feed(taskID, line, level string, emit func(entry TaskLogEntry)) {
	a.mu.Lock()
	defer a.mu.Unlock()

	now := time.Now()

	// Detect tool messages by prefix even when level is empty.
	isToolMsg := strings.HasPrefix(line, "[TOOL_START]") ||
		strings.HasPrefix(line, "[TOOL_OUTPUT]") ||
		strings.HasPrefix(line, "[TOOL_END]")

	if isToolMsg {
		// Tool messages bypass the buffer — flush any pending buffer first.
		a.flushBuffer(taskID, emit)
		a.emitNow(taskID, line, "tool", emit)
		return
	}

	if !isDeltaLevel(level) {
		// Explicit non-delta level (system, error): send immediately.
		// Flush any pending delta buffer first.
		a.flushBuffer(taskID, emit)
		a.emitNow(taskID, line, level, emit)
		return
	}

	// Thinking deltas are emitted immediately (not batched) so the frontend
	// can stream them piecemeal. Flush any pending text buffer first.
	if level == "thinking" {
		a.flushBuffer(taskID, emit)
		a.emitNow(taskID, line, "thinking", emit)
		return
	}

	// Delta type (text, info, or empty).
	effLevel := effectiveLevel(level)

	// If the delta level changed from the current buffer, flush the old buffer.
	if oldLevel, ok := a.bufferLevel[taskID]; ok && oldLevel != effLevel {
		a.flushBuffer(taskID, emit)
	}

	// Check if we should flush due to inactivity (only if buffer exists
	// and hasn't been flushed by level change above).
	if _, ok := a.buffer[taskID]; !ok {
		// Fresh buffer — no inactivity check needed.
	} else if last, ok := a.lastFlush[taskID]; ok && now.Sub(last) > a.flushPeriod {
		a.flushBuffer(taskID, emit)
	}

	// Append to buffer
	a.buffer[taskID] += line
	a.bufferLevel[taskID] = effLevel
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
		a.flushBuffer(taskID, emit)
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
	orchestrator   *orchestrator.Orchestrator
	hub            *rpc.Hub
	logStore       *TaskLogStore
	diskLogStore   *logstore.LogStore
	logAccumulator *LogAccumulator
	personaStore   *persona.Store
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
//   - Personas: rebuilds the persona store
//   - TaskTimeout, HeartbeatInterval, SilenceTimeout, MaxLogSize: stored for future use
func (s *Server) Reload(cfg config.ServerConfig) {
	s.mu.Lock()
	defer s.mu.Unlock()

	old := s.cfg
	s.cfg = cfg

	// Update max guests if changed
	if cfg.MaxGuests != old.MaxGuests {
		s.orchestrator.SetMaxGuests(cfg.MaxGuests)
		s.log.Printf("max_guests updated: %d", cfg.MaxGuests)
	}

	// Recreate persona store if personas changed (deep comparison catches
	// edits to existing personas, not just additions/removals).
	if !reflect.DeepEqual(cfg.Personas, old.Personas) {
		if err := persona.Validate(cfg.Personas); err != nil {
			s.log.Printf("invalid persona configuration after reload: %v (keeping old store)", err)
		} else {
			s.personaStore = persona.NewStore(cfg.Personas)
			if len(cfg.Personas) > 0 {
				s.log.Printf("personas updated: %v", s.personaStore.List())
			}
		}
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
	if cfg.TaskSilenceTimeout != old.TaskSilenceTimeout {
		s.log.Printf("task_silence_timeout updated: %ds", cfg.TaskSilenceTimeout)
	}
	if cfg.MaxLogSize != old.MaxLogSize {
		s.log.Printf("max_log_size updated: %d bytes", cfg.MaxLogSize)
	}
}

// New creates a new Server instance.
func New(cfg config.ServerConfig) *Server {
	logPrefix := "[hotelier]"
	// Use fatalwriter so the process exits if log writes fail (e.g. disk full).
	logger := log.New(fatalwriter.New(os.Stdout), logPrefix+" ", log.LstdFlags)

	s := &Server{
		cfg:            cfg,
		orchestrator:   orchestrator.New(logger.Printf),
		hub:            rpc.NewHub(logger.Printf),
		logStore:       NewTaskLogStore(),
		logAccumulator: NewLogAccumulator(logger),
		log:            logger,
		upgrader:       rpc.NewUpgrader(),
		webDir:         "web/static",
		templateDir:    "web/templates",
	}

	// Set max guests on orchestrator
	s.orchestrator.SetMaxGuests(cfg.MaxGuests)

	// Validate persona configuration at startup
	if err := persona.Validate(cfg.Personas); err != nil {
		logger.Printf("invalid persona configuration: %v", err)
	}

	// Initialize persona store
	s.personaStore = persona.NewStore(cfg.Personas)
	if len(cfg.Personas) > 0 {
		logger.Printf("loaded %d persona(s): %v", len(cfg.Personas), s.personaStore.List())
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

	// Handle guest disconnection: immediately fail the guest's task.
	s.hub.SetOnDisconnect(s.handleGuestDisconnect)

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

// Orchestrator returns the orchestrator (for testing).
func (s *Server) Orchestrator() *orchestrator.Orchestrator {
	return s.orchestrator
}

// Registry returns the guest registry (for testing, migration compat).
func (s *Server) Registry() *registry.GuestRegistry {
	return s.orchestrator.Registry()
}

// TaskQueue returns the task queue (for testing, migration compat).
func (s *Server) TaskQueue() *queue.TaskQueue {
	return s.orchestrator.Queue()
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
	s.hub.RegisterMethod("guest.cancelled", s.handleGuestCancelled)
	s.hub.RegisterMethod("task.acknowledge", s.handleTaskAcknowledge)

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

	// Record the guest-to-connection mapping so SendToGuest can find it.
	// Do this before registration so re-registration also updates the mapping.
	if connID, ok := rpc.ConnectionIDFromContext(ctx); ok {
		s.hub.RegisterGuestConnection(req.ID, connID)
		s.hub.SetConnectionRole(connID, rpc.ConnectionRoleGuest)
	}

	guest, err := s.orchestrator.Registry().Register(req.ID, req.Name, req.Tags)
	if err != nil {
		// Guest is already registered — reconcile its state against the
		// authoritative task queue. This handles reconnection after a
		// network blip, ensuring the guest's state matches reality.
		existing, exists := s.orchestrator.GetGuest(req.ID)
		if exists {
			// Update name and tags on re-registration
			existing.Name = req.Name
			existing.Tags = req.Tags
		}

		reconState := s.orchestrator.ReconcileGuest(req.ID)

		if exists {
			s.log.Printf("guest re-registered: %s (reconciled to %s, task: %s)",
				existing.ID, reconState.Status, reconState.TaskID)

			// If the guest is now IDLE, try to assign a pending task.
			if existing.State == registry.GuestStateIdle {
				s.tryAssignTask(req.ID)
			}

			return map[string]interface{}{
				"status": "re-registered",
				"guest": map[string]interface{}{
					"id":    existing.ID,
					"name":  existing.Name,
					"tags":  existing.Tags,
					"state": existing.State.String(),
				},
				"task": map[string]interface{}{
					"status": reconState.Status.String(),
					"id":     reconState.TaskID,
				},
			}, nil
		}
		return nil, rpc.InternalError(err.Error())
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

	if err := s.orchestrator.UnregisterGuestForce(req.ID); err != nil {
		return nil, rpc.InternalError(err.Error())
	}

	// Clean up the guest-connection mapping
	s.hub.UnregisterGuestConnection(req.ID)

	s.log.Printf("guest unregistered: %s", req.ID)

	// Orchestrator already re-queued any running task atomically.

	return map[string]interface{}{
		"status": "unregistered",
	}, nil
}

// handleGuestHeartbeat handles guest heartbeat.
// If task_id is present, it uses TaskHeartbeat to track the guest's
// current task for liveness probing.
func (s *Server) handleGuestHeartbeat(ctx context.Context, params json.RawMessage) (interface{}, *rpc.RPCError) {
	var req struct {
		ID     string `json:"id"`
		TaskID string `json:"task_id"`
	}

	if err := json.Unmarshal(params, &req); err != nil {
		return nil, rpc.InvalidParamsError("invalid request parameters")
	}

	if req.TaskID != "" {
		if err := s.orchestrator.TaskHeartbeat(req.ID, req.TaskID); err != nil {
			return nil, rpc.InternalError(err.Error())
		}
	} else {
		if err := s.orchestrator.Heartbeat(req.ID); err != nil {
			return nil, rpc.InternalError(err.Error())
		}
	}

	return map[string]interface{}{
		"status": "ok",
	}, nil
}

// parseToolLine extracts structured tool fields from a raw log line.
// Returns (populated, true) if the line matches a known tool format.
// Tool line formats:
//
//	[TOOL_START] tool_name: args (id: tool_id)
//	[TOOL_OUTPUT] tool_name (id: tool_id): output content
//	[TOOL_END] tool_name (id: tool_id): result output
//	[TOOL_END] tool_name (id: tool_id): [ERROR] error message
func parseToolLine(line string) (toolType, toolName, toolID, toolArgs, toolOutput string, toolError bool, ok bool) {
	var prefix string
	switch {
	case strings.HasPrefix(line, "[TOOL_START] "):
		prefix = "[TOOL_START] "
		toolType = "start"
	case strings.HasPrefix(line, "[TOOL_OUTPUT] "):
		prefix = "[TOOL_OUTPUT] "
		toolType = "output"
	case strings.HasPrefix(line, "[TOOL_END] "):
		prefix = "[TOOL_END] "
		toolType = "end"
	default:
		return "", "", "", "", "", false, false
	}

	rest := strings.TrimPrefix(line, prefix)

	// Find " (id: " to split name/args from id
	idIdx := strings.Index(rest, " (id: ")
	if idIdx < 0 {
		return "", "", "", "", "", false, false
	}

	// Everything before " (id: " is either "tool_name" or "tool_name: args"
	preID := rest[:idIdx]

	if toolType == "start" {
		// Format: "tool_name: args"
		colonIdx := strings.Index(preID, ": ")
		if colonIdx > 0 {
			toolName = preID[:colonIdx]
			toolArgs = preID[colonIdx+2:]
		} else {
			toolName = preID
		}
	} else {
		// Format: "tool_name" (no args for output/end)
		toolName = preID
	}

	// Extract tool_id: between "(id: " and ")"
	idStart := idIdx + len(" (id: ")
	idEnd := strings.Index(rest[idStart:], ")")
	if idEnd < 0 {
		return "", "", "", "", "", false, false
	}
	toolID = rest[idStart : idStart+idEnd]

	// After ")" there may be ": output" or ": [ERROR] output"
	afterID := rest[idStart+idEnd+1:] // skip ")"

	if strings.HasPrefix(afterID, ": ") {
		output := afterID[2:]
		if strings.HasPrefix(output, "[ERROR] ") {
			toolError = true
			toolOutput = strings.TrimPrefix(output, "[ERROR] ")
		} else {
			toolOutput = output
		}
	}

	return toolType, toolName, toolID, toolArgs, toolOutput, toolError, true
}

// broadcastTaskUpdated sends a task.updated notification to all browser
// connections so the UI can update the task status badge and detail view.
func (s *Server) broadcastTaskUpdated(taskID, status string) {
	s.hub.Broadcast(rpc.ConnectionRoleBrowser, &rpc.JSONRPCMessage{
		JSONRPC: "2.0",
		Method:  "task.updated",
		Params:  json.RawMessage(fmt.Sprintf(`{"task_id":"%s","status":"%s"}`, taskID, status)),
	})
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
			// Populate structured tool call fields.
			// First try the request fields (guest sent them directly).
			// Then fall back to parsing from the line (accumulator detected
			// the tool prefix but the original request had empty level).
			if req.Level == "tool" || req.ToolType != "" {
				e.ToolType = req.ToolType
				e.ToolName = req.ToolName
				e.ToolID = req.ToolID
				e.ToolArgs = req.ToolArgs
				e.ToolOutput = req.ToolOutput
				e.ToolError = req.ToolError
			}
			// If the entry has level "tool" (set by accumulator) but
			// structured fields are empty, parse them from the line.
			if e.Level == "tool" && e.ToolType == "" {
				e.ToolType, e.ToolName, e.ToolID, e.ToolArgs, e.ToolOutput, e.ToolError, _ = parseToolLine(e.Line)
			}
			s.logStore.Add(e)
			// Persist to disk if configured. Failure to write logs is fatal:
			// the system should not continue processing tasks if it cannot log.
			if s.diskLogStore != nil {
				if err := s.diskLogStore.Append(logstore.Entry{
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
				}); err != nil {
					fmt.Fprintf(os.Stderr, fatalwriter.FatalMsgFormat, err)
					os.Exit(1)
				}
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
		GuestID string `json:"guest_id"`
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

	// Find guest ID from task if not provided
	guestID := result.GuestID
	if guestID == "" {
		if task, ok := s.orchestrator.GetTask(result.TaskID); ok && task.AssignedTo != "" {
			guestID = task.AssignedTo
		}
	}

	var statusStr string
	if result.Success {
		if err := s.orchestrator.CompleteTask(result.TaskID, guestID, result.Output); err != nil {
			return nil, rpc.InternalError(err.Error())
		}
		statusStr = "COMPLETED"
	} else {
		if err := s.orchestrator.FailTask(result.TaskID, guestID, result.Error); err != nil {
			return nil, rpc.InternalError(err.Error())
		}
		statusStr = "FAILED"
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

	// Orchestrator already cleared the guest atomically.
	// Try to assign the next pending task to this now-idle guest.
	if guestID != "" {
		s.tryAssignTask(guestID)
	}

	// Notify UI of task completion
	s.broadcastTaskUpdated(result.TaskID, statusStr)

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

	// Decline atomically: task→PENDING, guest→IDLE
	if err := s.orchestrator.DeclineTask(req.TaskID, req.GuestID); err != nil {
		s.log.Printf("failed to decline task %s: %v", req.TaskID, err)
	} else {
		// Notify UI of task re-queue
		s.broadcastTaskUpdated(req.TaskID, "PENDING")

		// Try to assign the task to another eligible guest.
		// Pass the declining guest ID so we skip it — DeclineTask already
		// cleared task.AssignedTo, so the skip check inside would not work.
		s.tryAssignTaskToEligible(req.TaskID, req.GuestID)
	}

	return map[string]interface{}{
		"status": "accepted",
	}, nil
}

// handleGuestCancelled is called by a guest after it has stopped a task
// in response to a task.cancel signal. The guest is the authority on
// whether the task was actually aborted.
func (s *Server) handleGuestCancelled(ctx context.Context, params json.RawMessage) (interface{}, *rpc.RPCError) {
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

	s.log.Printf("task %s cancelled by guest %s: %s", req.TaskID, req.GuestID, req.Reason)

	// Cancel atomically: task→CANCELLED, guest→IDLE.
	// Orchestrator.CancelTask handles RUNNING→CANCELLED (was missing before).
	if err := s.orchestrator.CancelTask(req.TaskID, req.GuestID); err != nil {
		s.log.Printf("failed to cancel task %s: %v", req.TaskID, err)
		return nil, rpc.InternalError(err.Error())
	}

	// Flush any remaining accumulated logs for this task
	s.logAccumulator.FlushAll(func(e TaskLogEntry) {
		s.logStore.Add(e)
	})

	// Notify UI of task cancellation
	s.broadcastTaskUpdated(req.TaskID, "CANCELLED")

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

// handleTaskAcknowledge handles the guest's explicit acknowledgment of a task
// assignment. This is the handshake: the guest confirms it received the task
// and is executing it. The server transitions ASSIGNED→RUNNING.
func (s *Server) handleTaskAcknowledge(ctx context.Context, params json.RawMessage) (interface{}, *rpc.RPCError) {
	var req struct {
		TaskID  string `json:"task_id"`
		GuestID string `json:"guest_id"`
	}

	if err := json.Unmarshal(params, &req); err != nil {
		return nil, rpc.InvalidParamsError("invalid request parameters")
	}

	if req.TaskID == "" {
		return nil, rpc.InvalidParamsError("task_id is required")
	}

	// Find guest ID from context if not provided
	guestID := req.GuestID
	if guestID == "" {
		if connID, ok := rpc.ConnectionIDFromContext(ctx); ok {
			if gid, err := s.hub.GuestIDFromConnection(connID); err == nil {
				guestID = gid
			}
		}
	}

	if guestID == "" {
		return nil, rpc.InvalidParamsError("guest_id is required")
	}

	if err := s.orchestrator.AcknowledgeTask(req.TaskID, guestID); err != nil {
		s.log.Printf("task %s ack failed from guest %s: %v", req.TaskID, guestID, err)
		return nil, rpc.InternalError(err.Error())
	}

	s.log.Printf("task %s acknowledged by guest %s (ASSIGNED→RUNNING)", req.TaskID, guestID)
	s.broadcastTaskUpdated(req.TaskID, "RUNNING")

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

// handleGuestDisconnect is called when a WebSocket connection is lost.
// It immediately fails any running task for the guest associated with
// the connection, preventing orphaned tasks.
func (s *Server) handleGuestDisconnect(connectionID string) {
	guestID, err := s.hub.GuestIDFromConnection(connectionID)
	if err != nil || guestID == "" {
		return // Browser connection, not a guest
	}

	// Check if the guest has a running task
	guest, ok := s.orchestrator.GetGuest(guestID)
	if !ok || guest.TaskID == "" {
		return // No task to clean up
	}

	task, ok := s.orchestrator.GetTask(guest.TaskID)
	if !ok || task.Status == queue.TaskStatusPending || task.Status == queue.TaskStatusCompleted ||
		task.Status == queue.TaskStatusFailed || task.Status == queue.TaskStatusCancelled {
		return // Task not active or already handled
	}

	s.log.Printf("connection lost for guest %s: failing task %s (%s)",
		guestID, guest.TaskID, task.Status)

	// Fail the task atomically
	if err := s.orchestrator.FailTask(guest.TaskID, guestID, "connection lost"); err != nil {
		s.log.Printf("failed to fail task %s on disconnect: %v", guest.TaskID, err)
	}

	// Notify UI
	s.broadcastTaskUpdated(guest.TaskID, "FAILED")
}

// tryAssignPendingTasks iterates idle guests and tries to assign pending
// tasks to them. Used after stale guest removal to fill the gap.
func (s *Server) tryAssignPendingTasks() {
	for _, guest := range s.orchestrator.GetAllGuests() {
		if guest.State == registry.GuestStateIdle {
			s.tryAssignTask(guest.ID)
		}
	}
}

// tryAssignTaskToEligible finds an idle guest that matches the given task's
// tags and assigns the task to that guest. It is used when a previously-
// assigned guest declines the task. The skipGuestID parameter identifies
// the guest that declined and must be skipped (the task.AssignedTo field
// is already cleared by the time this function is called).
func (s *Server) tryAssignTaskToEligible(taskID, skipGuestID string) {
	task, ok := s.orchestrator.GetTask(taskID)
	if !ok {
		return
	}

	guests := s.orchestrator.FindAvailableGuests(task.Tags)
	if len(guests) == 0 {
		return
	}

	// Try each idle guest in order until one accepts
	for _, guest := range guests {
		// Skip the guest that already declined
		if guest.ID == skipGuestID {
			continue
		}

		if err := s.orchestrator.AssignTask(task.ID, guest.ID); err != nil {
			s.log.Printf("failed to assign task %s to guest %s: %v", task.ID, guest.ID, err)
			continue
		}

		taskData := map[string]interface{}{
			"id":       task.ID,
			"prompt":   task.Prompt,
			"tags":     task.Tags,
			"repo_ref": task.RepoRef,
		}

		if task.Persona != "" {
			p, err := s.personaStore.Get(task.Persona)
			if err != nil {
				s.log.Printf("failed to get persona %q for task %s: %v", task.Persona, task.ID, err)
				_ = s.orchestrator.RequeueTask(task.ID)
				continue
			}
			taskData["persona"] = map[string]interface{}{
				"name":  p.Name,
				"env":   p.Env,
				"files": p.Files,
			}
		}

		if err := s.hub.SendToGuest(guest.ID, "task.assign", taskData); err != nil {
			s.log.Printf("failed to push task to guest %s: %v", guest.ID, err)
			// Rollback: re-queue the task
			_ = s.orchestrator.RequeueTask(task.ID)
			continue
		}

		s.log.Printf("task %s reassigned to guest %s", task.ID, guest.ID)

		// Notify UI of task re-assignment
		s.broadcastTaskUpdated(task.ID, "ASSIGNED")
		return
	}
}

// tryAssignTask tries to assign a pending task to the given guest.
func (s *Server) tryAssignTask(guestID string) {
	guest, ok := s.orchestrator.GetGuest(guestID)
	if !ok {
		return
	}

	// Only assign to idle guests — a guest that is already running a task
	// on the client side should not receive another assignment.
	if guest.State != registry.GuestStateIdle {
		return
	}

	// Find a pending task that matches the guest's tags
	pending := s.orchestrator.GetPendingTasks()
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

	// Assign atomically: task→ASSIGNED, guest→RUNNING
	if err := s.orchestrator.AssignTask(matchedTask.ID, guestID); err != nil {
		s.log.Printf("failed to assign task %s to guest %s: %v", matchedTask.ID, guestID, err)
		return
	}

	// Push task to guest
	taskData := map[string]interface{}{
		"id":       matchedTask.ID,
		"prompt":   matchedTask.Prompt,
		"tags":     matchedTask.Tags,
		"repo_ref": matchedTask.RepoRef,
	}

	// Include persona data if specified
	if matchedTask.Persona != "" {
		p, err := s.personaStore.Get(matchedTask.Persona)
		if err != nil {
			s.log.Printf("failed to get persona %q for task %s: %v", matchedTask.Persona, matchedTask.ID, err)
			_ = s.orchestrator.RequeueTask(matchedTask.ID)
			return
		}
		taskData["persona"] = map[string]interface{}{
			"name":  p.Name,
			"env":   p.Env,
			"files": p.Files,
		}
	}

	if err := s.hub.SendToGuest(guestID, "task.assign", taskData); err != nil {
		s.log.Printf("failed to push task to guest %s: %v", guestID, err)
		// Rollback: re-queue the task atomically
		_ = s.orchestrator.RequeueTask(matchedTask.ID)
		return
	}

	s.log.Printf("task %s assigned to guest %s", matchedTask.ID, guestID)

	// Notify UI of task assignment
	s.broadcastTaskUpdated(matchedTask.ID, "ASSIGNED")
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
			// First: detect tasks stuck in ASSIGNED state (guest never confirmed).
			// This catches the race condition where task.assign was dropped.
			s.checkStuckTasks()

			// Second: kill running tasks for guests that have been silent too long.
			// This runs before stale guest removal so we can send task.cancel.
			s.checkSilentGuests()

			// Then: Remove guests that have been completely silent (no heartbeat).
			// The orchestrator fails any running tasks before removing the guest.
			stale := s.orchestrator.RemoveStaleGuests(time.Duration(s.cfg.HeartbeatInterval) * time.Second)
			for _, sg := range stale {
				s.log.Printf("stale guest removed: %s", sg.GuestID)
				if sg.TaskWasRunning {
					s.broadcastTaskUpdated(sg.TaskID, "FAILED")
				}
			}
			// After removing stale guests, try to assign pending tasks to remaining idle guests.
			s.tryAssignPendingTasks()
		}
	}
}

// checkSilentGuests finds guests that have been silent (no heartbeat) for longer
// than the configured TaskSilenceTimeout and kills their running tasks.
//
// Uses the orchestrator's atomic check which inspects task status before acting,
// preventing races with checkStuckTasks.
func (s *Server) checkSilentGuests() {
	timeout := time.Duration(s.cfg.TaskSilenceTimeout) * time.Second
	if timeout == 0 {
		return // Silence detection disabled
	}

	silent := s.orchestrator.CheckSilentGuests(timeout)
	for _, sg := range silent {
		s.log.Printf("guest %s silent, task %s failed", sg.GuestID, sg.TaskID)

		// Notify the UI
		s.broadcastTaskUpdated(sg.TaskID, "FAILED")

		// Send task.cancel RPC to the guest to abort the pi subprocess.
		// This is best-effort — the guest may already be disconnected.
		cancelParams := map[string]interface{}{
			"task_id": sg.TaskID,
			"reason":  "guest silence timeout",
		}
		if err := s.hub.SendToGuest(sg.GuestID, "task.cancel", cancelParams); err != nil {
			s.log.Printf("failed to send task.cancel to guest %s: %v (guest may be disconnected)", sg.GuestID, err)
		}
	}
}

// checkStuckTasks detects tasks that have been ASSIGNED to a guest but the
// guest has not heartbeated with that task_id. This catches the case where
// the server assigned a task but the guest never received the assignment
// (e.g. race condition in notification delivery).
// Stuck tasks are failed and re-queued for another guest.
func (s *Server) checkStuckTasks() {
	timeout := time.Duration(s.cfg.TaskAssignmentTimeout) * time.Second
	if timeout == 0 {
		return // Stuck task detection disabled
	}

	stuck := s.orchestrator.CheckStuckTasks(timeout)
	for _, st := range stuck {
		s.log.Printf("task %s stuck on guest %s for > %v, re-queuing",
			st.TaskID, st.GuestID, timeout)

		// Notify the UI
		s.broadcastTaskUpdated(st.TaskID, "PENDING")
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
	client := s.hub.NewConnection(connID, conn, rpc.ConnectionRoleBrowser)
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
		"guests": s.orchestrator.Registry().Count(),
		"tasks":  s.orchestrator.Queue().Count(),
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
	tasks := s.orchestrator.GetAllTasks()
	// Build task list with log_count annotation
	taskList := make([]map[string]interface{}, len(tasks))
	for i, t := range tasks {
		taskMap := map[string]interface{}{
			"id":          t.ID,
			"prompt":      t.Prompt,
			"tags":        t.Tags,
			"repo_ref":    t.RepoRef,
			"persona":     t.Persona,
			"status":      t.Status.String(),
			"created_at":  t.CreatedAt,
			"assigned_to": t.AssignedTo,
			"assigned_at": t.AssignedAt,
			"timeout":     t.Timeout,
			"result":      t.Result,
			"error":       t.Error,
			"log_count":   s.logStore.Count(t.ID),
		}
		taskList[i] = taskMap
	}
	json.NewEncoder(w).Encode(map[string]interface{}{
		"tasks": taskList,
		"count": len(taskList),
	})
}

func (s *Server) handleCreateTask(w http.ResponseWriter, r *http.Request) {
	// Read raw body to check for disallowed fields before decoding
	var raw map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	// Hard-fail if repos field is present
	if repos, ok := raw["repos"]; ok {
		if reposList, ok := repos.([]interface{}); ok && len(reposList) > 0 {
			http.Error(w, "repos field is not supported: tasks are credentialed to access their own resources", http.StatusBadRequest)
			return
		}
		// Also reject empty repos array to be strict
		http.Error(w, "repos field is not supported: tasks are credentialed to access their own resources", http.StatusBadRequest)
		return
	}

	// Reconstruct JSON without repos for task decoding
	bodyBytes, _ := json.Marshal(raw)
	var task queue.Task
	if err := json.Unmarshal(bodyBytes, &task); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	// Validate persona if specified
	if task.Persona != "" {
		if !s.personaStore.Exists(task.Persona) {
			http.Error(w, fmt.Sprintf("persona %q not found", task.Persona), http.StatusBadRequest)
			return
		}
	}

	if task.ID == "" {
		task.ID = fmt.Sprintf("task-%d", time.Now().UnixNano())
	}

	if err := s.orchestrator.AddTask(&task); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Try to assign to an available guest
	guests := s.orchestrator.FindAvailableGuests(task.Tags)
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
	path := strings.TrimPrefix(r.URL.Path, "/api/tasks/")

	// Check for rerun sub-path: /api/tasks/:id/rerun
	if strings.HasSuffix(path, "/rerun") {
		taskID := strings.TrimSuffix(path, "/rerun")
		s.handleTaskRerun(w, r, taskID)
		return
	}

	// Check for top sub-path: /api/tasks/:id/top
	if strings.HasSuffix(path, "/top") {
		taskID := strings.TrimSuffix(path, "/top")
		s.handleTaskTop(w, r, taskID)
		return
	}

	// Check for cancel sub-path: /api/tasks/:id/cancel
	if strings.HasSuffix(path, "/cancel") {
		taskID := strings.TrimSuffix(path, "/cancel")
		s.handleTaskCancelHTTP(w, r, taskID)
		return
	}

	taskID := path
	if taskID == "" {
		http.Error(w, "task id required", http.StatusBadRequest)
		return
	}

	task, ok := s.orchestrator.GetTask(taskID)
	if !ok {
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

// handleTaskRerun creates a new task from an existing one.
// POST /api/tasks/:id/rerun — clones the task's prompt and tags
// into a fresh task with a new ID.
func (s *Server) handleTaskRerun(w http.ResponseWriter, r *http.Request, taskID string) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if taskID == "" {
		http.Error(w, "task id required", http.StatusBadRequest)
		return
	}

	orig, ok := s.orchestrator.GetTask(taskID)
	if !ok {
		http.Error(w, "task not found", http.StatusNotFound)
		return
	}

	// Clone the task with a new ID
	newTask := &queue.Task{
		ID:      fmt.Sprintf("task-%d", time.Now().UnixNano()),
		Prompt:  orig.Prompt,
		Tags:    orig.Tags,
		RepoRef: orig.RepoRef,
		Persona: orig.Persona,
	}

	if err := s.orchestrator.AddTask(newTask); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Try to assign to an available guest
	guests := s.orchestrator.FindAvailableGuests(newTask.Tags)
	if len(guests) > 0 {
		s.tryAssignTask(guests[0].ID)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(newTask)
}

// handleTaskTop moves a pending task to the front of the queue.
// POST /api/tasks/:id/top — moves the task to the top of the pending queue
// so it will be assigned to the next available guest before other pending tasks.
func (s *Server) handleTaskTop(w http.ResponseWriter, r *http.Request, taskID string) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if taskID == "" {
		http.Error(w, "task id required", http.StatusBadRequest)
		return
	}

	task, ok := s.orchestrator.GetTask(taskID)
	if !ok {
		http.Error(w, "task not found", http.StatusNotFound)
		return
	}

	if task.Status != queue.TaskStatusPending {
		http.Error(w, fmt.Sprintf("task is not pending (status: %s)", task.Status), http.StatusConflict)
		return
	}

	if err := s.orchestrator.Queue().MoveToTop(taskID); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Broadcast task.position_changed so the UI can re-render the task list
	// in the new order.  Using a dedicated event avoids misleading consumers
	// who expect task.updated to signal a status change.
	s.hub.SendNotification("", rpc.ConnectionRoleBrowser, "task.position_changed", map[string]interface{}{
		"task_id": task.ID,
		"status":  task.Status.String(),
	})

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"task_id": task.ID,
		"status":  task.Status.String(),
	})
}

// handleTaskCancelHTTP handles the /api/tasks/:id/cancel endpoint.
// POST /api/tasks/:id/cancel — cancels a task that is PENDING or ASSIGNED.
// For RUNNING tasks, sends a cancel signal to the guest (guest confirms cancellation).
func (s *Server) handleTaskCancelHTTP(w http.ResponseWriter, r *http.Request, taskID string) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if taskID == "" {
		http.Error(w, "task id required", http.StatusBadRequest)
		return
	}

	task, ok := s.orchestrator.GetTask(taskID)
	if !ok {
		http.Error(w, "task not found", http.StatusNotFound)
		return
	}

	// Terminal states cannot be cancelled
	switch task.Status {
	case queue.TaskStatusCompleted, queue.TaskStatusFailed, queue.TaskStatusCancelled:
		http.Error(w, fmt.Sprintf("task already %s", task.Status), http.StatusConflict)
		return
	}

	switch task.Status {
	case queue.TaskStatusPending, queue.TaskStatusAssigned:
		// Cancel atomically — no guest needs to be notified.
		// Orchestrator.CancelTask handles guest cleanup.
		if err := s.orchestrator.CancelTask(taskID, task.AssignedTo); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

	case queue.TaskStatusRunning:
		// Send cancel signal to the guest — guest will confirm cancellation
		if task.AssignedTo == "" {
			http.Error(w, "task has no assigned guest", http.StatusBadRequest)
			return
		}

		if err := s.hub.SendToGuest(task.AssignedTo, "task.cancel", map[string]interface{}{
			"task_id": taskID,
			"reason":  "user requested cancellation",
		}); err != nil {
			s.log.Printf("failed to send task.cancel to guest %s: %v", task.AssignedTo, err)
			http.Error(w, fmt.Sprintf("failed to notify guest: %v", err), http.StatusServiceUnavailable)
			return
		}
	}

	// Broadcast task.updated to browsers
	task, _ = s.orchestrator.GetTask(taskID)
	s.hub.SendNotification("", rpc.ConnectionRoleBrowser, "task.updated", map[string]interface{}{
		"task_id": task.ID,
		"status":  task.Status.String(),
	})

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"task_id": task.ID,
		"status":  task.Status.String(),
	})
}

// handleGuests handles the /api/guests endpoint.
// HandleGuests handles the /api/guests endpoint.
func (s *Server) HandleGuests(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	switch r.Method {
	case http.MethodGet:
		guests := s.orchestrator.GetAllGuests()
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

	guest, ok := s.orchestrator.GetGuest(guestID)
	if !ok {
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
		// /api/logs/:date → list tasks for this date (with starting timestamps)
		if parts[0] == "" {
			http.Error(w, "date required", http.StatusBadRequest)
			return
		}
		summaries, err := s.diskLogStore.ListTasksWithTimestamps(parts[0])
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		json.NewEncoder(w).Encode(map[string]interface{}{
			"date":      parts[0],
			"summaries": summaries,
			"count":     len(summaries),
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
