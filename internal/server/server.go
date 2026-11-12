package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"mime"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"hotelier/pkg/config"
	"hotelier/pkg/fatalwriter"
	"hotelier/pkg/logstore"
	"hotelier/pkg/logutil"
	"hotelier/pkg/metrics"
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
	ToolType     string `json:"tool_type,omitempty"`      // "start", "output", "end"
	ToolName     string `json:"tool_name,omitempty"`      // e.g. "bash", "read"
	ToolID       string `json:"tool_id,omitempty"`        // unique tool call identifier
	ToolArgs     string `json:"tool_args,omitempty"`      // arguments/parameters
	ToolArgsJSON string `json:"tool_args_json,omitempty"` // raw argument JSON (UI diff rendering, issue #2)
	ToolOutput   string `json:"tool_output,omitempty"`    // captured output
	ToolError    bool   `json:"tool_error,omitempty"`     // true if tool ended with error

	// Structured compaction fields (only set when Level == "compaction")
	CompactionType         string `json:"compaction_type,omitempty"`          // "start", "end"
	CompactionReason       string `json:"compaction_reason,omitempty"`        // "manual", "threshold", "overflow"
	CompactionSummary      string `json:"compaction_summary,omitempty"`       // generated summary (end)
	CompactionTokensBefore int    `json:"compaction_tokens_before,omitempty"` // context size before (end)
	CompactionTokensAfter  int    `json:"compaction_tokens_after,omitempty"`  // estimated size after (end)
	CompactionError        bool   `json:"compaction_error,omitempty"`         // true if compaction failed
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
	// The journal records the activity and its size, never the content: every
	// agentic session used to land in the systemd journal (issue #241). The
	// content itself goes to the log store and the UI via the emit callback.
	//
	// Thinking deltas are skipped entirely: they arrive per token, so even a
	// size line would flood the journal (issue #194).
	if level != "thinking" {
		a.log.Printf("[task:%s] [%s] %s", taskID, level, logutil.Content(line))
	}
	emit(entry)
}

// Claim revocation. A task that leaves ASSIGNED/RUNNING for PENDING has to
// take its claim with it: until the holder is told, the assignment stays
// queued in that guest and runs later — work the host believes it is not
// doing, with side effects on the outside world (issue #252).
const (
	// cancelIntentRevoke tells a guest to give the claim up; the task itself
	// survives and is (re-)assigned elsewhere. cancelIntentCancel — the
	// default when the field is absent — ends the task.
	cancelIntentRevoke = "revoke"
	cancelIntentCancel = "cancel"

	// Why the claim went away, carried to the guest for its log.
	claimReasonRequeued       = "re-queued"
	claimReasonConnectionLost = "connection lost"
)

// What a guest reported having done with a task.cancel. They match the
// guest's cancelAction* values; the server counts them so a recurrence of
// issue #252 is one number rather than a forensic exercise.
const (
	cancelActionRunning     = "cancelled_while_running"
	cancelActionBeforeStart = "cancelled_before_start"
	cancelActionNotHeld     = "not_held"
)

// claimStats is the host's view of claim revocations: how many claims were
// given up while a task was running, how many before it started, and how many
// acknowledgements the host refused. It is served on /api/guests.
type claimStats struct {
	CancelledWhileRunning int `json:"cancelled_while_running"`
	CancelledBeforeStart  int `json:"cancelled_before_start"`
	AckRefusals           int `json:"ack_refusals"`
}

// claimCounters tallies those events. The zero value is ready to use.
type claimCounters struct {
	mu                    sync.Mutex
	cancelledWhileRunning int
	cancelledBeforeStart  int
	ackRefusals           int
}

func (c *claimCounters) record(action string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch action {
	case cancelActionRunning:
		c.cancelledWhileRunning++
	case cancelActionBeforeStart:
		c.cancelledBeforeStart++
	}
}

func (c *claimCounters) recordAckRefusal() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ackRefusals++
}

func (c *claimCounters) stats() claimStats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return claimStats{
		CancelledWhileRunning: c.cancelledWhileRunning,
		CancelledBeforeStart:  c.cancelledBeforeStart,
		AckRefusals:           c.ackRefusals,
	}
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
	claims         claimCounters
	// cancelIntentMu guards cancelIntentByTask, which remembers the intent
	// of the last task.cancel sent for each task. A guest that predates the
	// intent field cannot echo it back, so its confirmation is resolved
	// against what the host sent rather than against a default (issue #252
	// review). Entries are written *before* the send (so a confirmation
	// handled on another goroutine can never resolve against a record that
	// does not exist yet) and removed when the confirmation arrives, when a
	// later send for the same task overwrites it, or when the send fails.
	// A confirmation that never arrives — a guest with no connection skips
	// it, a socket dying between write and handling loses it — leaves the
	// entry behind: its lifetime is bounded by the task's, and since task
	// IDs are never reused the residue is a per-task entry at most, not a
	// growing set.
	cancelIntentMu     sync.Mutex
	cancelIntentByTask map[string]string
	// capOverride, when non-nil, replaces the configured or scheduled
	// concurrency cap with a value set at runtime through /api/concurrency
	// (issue #264). It is deliberately in-memory only — nothing persists
	// it — and expires at capOverrideExpires, the next scheduled boundary
	// after it was set, so the next shrink or expansion on the schedule is
	// still respected. With no schedule there is no boundary to expire
	// against and the override rules until the next config reload,
	// which clears it. Guarded by s.mu.
	capOverride        *int
	capOverrideExpires time.Time
	webDir             string
	templateDir        string
}

// claimStats returns the counters behind /api/guests.
func (s *Server) claimStats() claimStats {
	return s.claims.stats()
}

// revokeClaim tells the holder of a task that its claim is taken back, before
// the host re-queues the task. The notification is a no-op on a guest that
// does not hold the task, so it is safe to send on every path and safe to
// retry; a guest that cannot be reached is logged and left alone — the
// revocation is an optimisation over the guest's own ack refusal.
func (s *Server) revokeClaim(taskID, guestID, reason string) {
	if guestID == "" {
		return
	}
	s.log.Printf("revoking claim on task %s from guest %s: %s", taskID, guestID, reason)
	// Remember what we are about to ask for before the send: the
	// confirmation this send provokes may not carry the intent (an old
	// guest cannot echo it), and it can be handled on another goroutine
	// before the statement after the send would have run (#252 review).
	s.rememberCancelIntent(taskID, cancelIntentRevoke)
	if err := s.hub.SendToGuest(guestID, "task.cancel", map[string]interface{}{
		"task_id": taskID,
		"reason":  reason,
		"intent":  cancelIntentRevoke,
	}); err != nil {
		// Nothing was sent, so nothing is owed a resolution — drop the
		// record rather than leaving a revocation on file for a cancel no
		// guest was ever told about.
		s.forgetCancelIntent(taskID)
		s.log.Printf("failed to send task.cancel to guest %s: %v (guest may be disconnected)", guestID, err)
		return
	}
}

// rememberCancelIntent records the intent of a task.cancel about to be sent
// for a task, so an old guest's intent-less confirmation is resolved against
// what the host actually asked for. Every sender calls it before the send and
// pairs it with forgetCancelIntent on the error path.
func (s *Server) rememberCancelIntent(taskID, intent string) {
	s.cancelIntentMu.Lock()
	defer s.cancelIntentMu.Unlock()
	if s.cancelIntentByTask == nil {
		s.cancelIntentByTask = make(map[string]string)
	}
	s.cancelIntentByTask[taskID] = intent
}

// forgetCancelIntent drops the remembered intent for a task — used when the
// send it was recorded for failed, so the record never outlives the request
// it stands for.
func (s *Server) forgetCancelIntent(taskID string) {
	s.cancelIntentMu.Lock()
	defer s.cancelIntentMu.Unlock()
	delete(s.cancelIntentByTask, taskID)
}

// resolveCancelIntent decides what a guest.cancelled confirmation means:
// a guest that can echo the intent does so and consumes the remembered
// entry; a guest that cannot has its confirmation resolved against the last
// intent sent for that task, defaulting to cancel — the meaning task.cancel
// carried before the field existed.
//
// The entry is consumed here and nowhere else. Per-connection FIFO means a
// guest sends the confirmation for a task.cancel before it even reads an
// assignment sent after it, so clearing the entry on (re-)assignment could
// only ever lose it before the confirmation it is meant to resolve arrives.
func (s *Server) resolveCancelIntent(taskID, echoed string) string {
	s.cancelIntentMu.Lock()
	defer s.cancelIntentMu.Unlock()
	if echoed != "" {
		delete(s.cancelIntentByTask, taskID)
		return echoed
	}
	intent, ok := s.cancelIntentByTask[taskID]
	if !ok {
		return cancelIntentCancel
	}
	delete(s.cancelIntentByTask, taskID)
	return intent
}

// Reload updates the server's runtime configuration from a new ServerConfig.
// It applies changes that can take effect without restarting:
//   - MaxGuests: updates the registry capacity
//   - MaxConcurrentTasks, ConcurrencySchedule, ScheduleTimezone: validated
//     here and pushed to the orchestrator, so a cap edit — including one that
//     raises the cap and frees the queue — takes effect here too; an invalid
//     set of keys is rejected and the previous values are kept
//   - LogDir: recreates the disk log store if the path changed
//   - LogRetentionDays: read live by the log prune loop
//   - Personas: rebuilds the persona store
//   - TaskTimeout, HeartbeatInterval, MaxLogSize: stored for future use
//   - SilenceTimeout, TaskSilenceTimeout, TaskAssignmentTimeout: read live by the cleanup loop
func (s *Server) Reload(cfg config.ServerConfig) {
	if s.reload(cfg) {
		// The cap rose. A raised cap is itself the event the queue was
		// waiting on, so drain it here rather than waiting for a guest to
		// poll (issue #249). Outside the lock: the assignment pass talks to
		// guests and must not hold the config open.
		s.tryAssignPendingTasks()
	}
}

// reload applies cfg and reports whether the concurrency cap rose, which the
// caller turns into an assignment pass. It takes s.mu; the drain pass must not
// run under it.
func (s *Server) reload(cfg config.ServerConfig) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	old := s.cfg

	// The concurrency keys are validated here as well as by LoadServerConfig:
	// a config assembled in code reaches EffectiveConcurrency through this
	// path without ever passing the loader, and an invalid schedule must be
	// reported rather than resolved to a plausible-looking wrong cap (issue
	// #248). Keep the previous keys on failure, the way the personas branch
	// below keeps the old store.
	if err := cfg.ValidateConcurrency(); err != nil {
		s.log.Printf("invalid concurrency configuration after reload: %v (keeping previous concurrency settings)", err)
		cfg.MaxConcurrentTasks = old.MaxConcurrentTasks
		cfg.ConcurrencySchedule = old.ConcurrencySchedule
		cfg.ScheduleTimezone = old.ScheduleTimezone
	}
	// A trusted_hosts entry no Host header can ever carry is a configuration
	// error as well, and it matters more here than for most keys: the guard it
	// feeds refuses with "not the operator", so a dead entry and a broken guard
	// produce the same evidence. Keep the previous list, the way the concurrency
	// keys are kept above.
	if err := cfg.ValidateTrustedHosts(); err != nil {
		s.log.Printf("invalid trusted_hosts configuration after reload: %v (keeping previous trusted_hosts)", err)
		cfg.TrustedHosts = old.TrustedHosts
	}
	s.cfg = cfg

	// Update max guests if changed
	if cfg.MaxGuests != old.MaxGuests {
		s.orchestrator.SetMaxGuests(cfg.MaxGuests)
		s.log.Printf("max_guests updated: %d", cfg.MaxGuests)
	}

	// Concurrency cap changes are pushed to the orchestrator, which is where
	// assignments are decided: EffectiveConcurrency is the source of truth for
	// "what is the cap right now", and enforcement reads the value it holds
	// (issue #249). Log them the same way max_guests is logged.
	if cfg.MaxConcurrentTasks != old.MaxConcurrentTasks {
		s.log.Printf("max_concurrent_tasks updated: %d", cfg.MaxConcurrentTasks)
	}
	if !reflect.DeepEqual(cfg.ConcurrencySchedule, old.ConcurrencySchedule) || cfg.ScheduleTimezone != old.ScheduleTimezone {
		s.log.Printf("concurrency_schedule updated: %d entries (timezone %q)", len(cfg.ConcurrencySchedule), cfg.ScheduleTimezone)
	}
	// Say the same for trusted_hosts (issue #286): editing the list under a
	// running server otherwise lands silently, and this is the one key whose
	// failure looks like a broken guard rather than a bad reload.
	if !reflect.DeepEqual(cfg.TrustedHosts, old.TrustedHosts) {
		s.log.Printf("trusted_hosts updated: %d names %v", len(cfg.TrustedHosts), cfg.TrustedHosts)
	}
	// A config reload is an operator editing the file; the file regains
	// authority, so a live cap override (issue #264) does not survive it.
	if s.capOverride != nil {
		s.log.Printf("config reload cleared the live concurrency cap override")
		s.capOverride = nil
		s.capOverrideExpires = time.Time{}
	}
	_, drain := s.pushConcurrencyCapLocked(cfg, time.Now())

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
	if cfg.LogRetentionDays != old.LogRetentionDays {
		s.log.Printf("log_retention_days updated: %d", cfg.LogRetentionDays)
	}

	return drain
}

// concurrencySyncInterval is how often the server re-reads the concurrency
// schedule to see whether a boundary has been crossed (issue #249). Schedule
// entries are minute-granular "HH:MM" values, so a tick this short bounds the
// lag between a boundary and the queue draining to the new cap — without the
// enforcement side re-implementing the boundary arithmetic config owns.
const concurrencySyncInterval = 15 * time.Second

// pushConcurrencyCapLocked resolves the cap for cfg at now and hands it to the
// orchestrator, returning the number it enforced. The caller must hold s.mu. It
// also reports whether the cap rose — unlimited outranking any number — which
// means work the schedule was holding back may now start.
//
// Retirement and enforcement move together here on purpose: resolving an
// override that has expired at its boundary is a write, so it belongs on the
// path that also pushes the resolved value. A caller that resolved without
// pushing would leave the orchestrator enforcing a retired override while every
// status surface attributed it to the schedule (issue #264 review).
func (s *Server) pushConcurrencyCapLocked(cfg config.ServerConfig, now time.Time) (int, bool) {
	effective := s.effectiveCapLocked(cfg, now)
	rose := capIsGreater(effective, s.orchestrator.ConcurrencyCap())
	s.orchestrator.SetConcurrencyCap(effective)
	return effective, rose
}

// effectiveCapLocked resolves the cap in force at now, honouring a live
// runtime override of the kind /api/concurrency sets (issue #264). An
// override rules while it is installed and its expiry has not arrived —
// no expiry instant means no schedule was configured, and the override
// holds until a reload clears it — so the schedule's next shrink or
// expansion is still respected. The caller must hold s.mu.
func (s *Server) effectiveCapLocked(cfg config.ServerConfig, now time.Time) int {
	scheduled := cfg.EffectiveConcurrency(now)
	if s.capOverride != nil {
		if s.capOverrideExpires.IsZero() || now.Before(s.capOverrideExpires) {
			return *s.capOverride
		}
		s.log.Printf("concurrency cap override %v expired at %s: scheduled cap %v takes over",
			capLabel(*s.capOverride), s.capOverrideExpires.Format(time.RFC3339), capLabel(scheduled))
		s.capOverride = nil
		s.capOverrideExpires = time.Time{}
	}
	return scheduled
}

// syncConcurrencyCap re-resolves the cap against the schedule at now and
// pushes it into the orchestrator. It reports whether the cap rose, so the
// caller can drain the queue (issue #249). Crossing a 07:00 boundary is not an
// event the server receives from anyone; this is how the schedule is noticed.
// It takes the write lock, can log, and must never be called while the caller
// holds s.mu (the RWMutex is not reentrant) — the same rule EffectiveConcurrency
// states.
func (s *Server) syncConcurrencyCap(now time.Time) bool {
	// A write lock: resolving the cap may expire and clear a live override
	// (issue #264), which a read lock must not do.
	s.mu.Lock()
	defer s.mu.Unlock()
	_, rose := s.pushConcurrencyCapLocked(s.cfg, now)
	return rose
}

// setConcurrencyCapOverride installs newCap as the live concurrency cap
// until the next scheduled boundary after now (issue #264). Like a reload
// that raises the cap, it reports whether the cap rose, so the caller can
// drain the queue the old cap was holding back. remote names the peer the
// request came from, recorded on the log line so a cap change is
// attributable after the fact.
func (s *Server) setConcurrencyCapOverride(newCap int, remote string, now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.capOverride = &newCap
	until := "until the next config reload"
	if status := s.cfg.ConcurrencyStatus(now); status.HasNextChange() {
		s.capOverrideExpires = status.NextChangeAt
		until = fmt.Sprintf("until the next scheduled change at %s %s", status.NextAt(), status.Timezone)
	} else {
		s.capOverrideExpires = time.Time{}
	}

	rose := capIsGreater(newCap, s.orchestrator.ConcurrencyCap())
	s.orchestrator.SetConcurrencyCap(newCap)
	s.log.Printf("concurrency cap set to %v by the API from %s, %s", capLabel(newCap), remote, until)
	return rose
}

// refreshConcurrency re-resolves the cap at now and, when that raised the cap,
// runs an assignment pass: work the previous cap held back may start now, and
// nothing else is going to tell the server so (issue #249).
func (s *Server) refreshConcurrency(now time.Time) {
	if s.syncConcurrencyCap(now) {
		s.tryAssignPendingTasks()
	}
}

// concurrencySyncLoop watches the schedule so a cap that rises at a boundary
// drains the queue without waiting for a new task or a guest poll.
func (s *Server) concurrencySyncLoop() {
	ticker := time.NewTicker(concurrencySyncInterval)
	defer ticker.Stop()

	for range ticker.C {
		s.refreshConcurrency(time.Now())
	}
}

// capIsGreater orders caps by how much work they admit. The unlimited
// sentinel outranks every number and two equal caps are not "greater".
func capIsGreater(a, b int) bool {
	if a == b {
		return false
	}
	if a == config.ConcurrencyUnlimited {
		return true
	}
	if b == config.ConcurrencyUnlimited {
		return false
	}
	return a > b
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

	// Hand the cap to the orchestrator, the one place that decides
	// assignments, so the deployment starts under the cap its config states
	// (issue #249).
	s.orchestrator.SetConcurrencyCap(cfg.EffectiveConcurrency(time.Now()))

	// Validate the concurrency configuration at startup the way personas
	// are; the file path rejects such a config in LoadServerConfig, but a
	// config built in code would otherwise reach EffectiveConcurrency
	// unvalidated (issue #248). Unlike Reload, which keeps the previous keys,
	// this installs the config it was handed, so say which cap the process
	// is now running under: a schedule the resolver cannot read falls back
	// to max_concurrent_tasks.
	if err := cfg.ValidateConcurrency(); err != nil {
		fallback := fmt.Sprintf("%d", cfg.MaxConcurrentTasks)
		if cfg.MaxConcurrentTasks == 0 {
			fallback = "0 (unlimited)"
		}
		logger.Printf("invalid concurrency configuration: %v (a schedule the resolver cannot read falls back to max_concurrent_tasks=%s)", err, fallback)
	}

	// Same for trusted_hosts at startup: the file path refuses such a config
	// in LoadServerConfig, but a config built in code would otherwise install
	// an entry that matches nothing, and the only symptom is a 403 (issue #286).
	if err := cfg.ValidateTrustedHosts(); err != nil {
		logger.Printf("invalid trusted_hosts configuration: %v (an entry no Host header can carry admits nothing)", err)
	}

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

	// Initialize queue persistence if configured (issue #190): persist
	// unprocessed tasks to disk and restore them from a previous run.
	if cfg.QueueDir != "" {
		store, err := queue.NewStore(cfg.QueueDir, logger.Printf)
		if err != nil {
			logger.Printf("failed to create queue store at %s: %v (queue will be in-memory only)", cfg.QueueDir, err)
		} else {
			s.orchestrator.SetStore(store)
			if restored, err := s.restoreTasks(store); err != nil {
				logger.Printf("failed to restore tasks from %s: %v", cfg.QueueDir, err)
			} else if restored > 0 {
				logger.Printf("restored %d task(s) from %s", restored, cfg.QueueDir)
			}
		}
	}

	s.registerRPCMethods()

	return s
}

// restoreTasks loads persisted tasks from the queue store and re-adds them
// to the queue as PENDING (issue #190). Tasks that fail to restore are
// logged and skipped. Returns the number of tasks loaded from the store.
func (s *Server) restoreTasks(store *queue.Store) (int, error) {
	tasks, err := store.Load()
	if err != nil {
		return 0, err
	}
	for _, task := range tasks {
		if err := s.orchestrator.RestoreTask(task); err != nil {
			s.log.Printf("failed to restore task %s: %v", task.ID, err)
		}
	}
	return len(tasks), nil
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

	// Watch the concurrency schedule so a cap that rises at a boundary drains
	// the queue without waiting for an event (issue #249).
	go s.concurrencySyncLoop()

	// Prune the disk task log store at startup and on a tick, so neither a
	// first run nor a long-lived server can let the log tree grow into the
	// volume that hosts it (issue #263).
	go s.logPruneCleanup()

	// Set up HTTP routes
	mux := http.NewServeMux()

	// WebSocket endpoint for guests
	mux.HandleFunc("/ws", s.HandleWebSocket)

	// REST API endpoints
	mux.HandleFunc("/api/tasks", s.HandleTasks)
	// Exact match: ServeMux prefers "/api/tasks/search" over the
	// "/api/tasks/" subtree, so a search is never mistaken for a task ID.
	mux.HandleFunc("/api/tasks/search", s.HandleTaskSearch)
	// Exact match: as with /api/tasks/search, ServeMux prefers this over the
	// "/api/tasks/" subtree, so "filter" is never mistaken for a task ID.
	mux.HandleFunc("/api/tasks/filter", s.HandleTaskFilter)
	mux.HandleFunc("/api/tasks/", s.HandleTaskDetail)
	mux.HandleFunc("/api/guests", s.HandleGuests)
	mux.HandleFunc("/api/guests/", s.HandleGuestDetail)
	mux.HandleFunc("/api/concurrency", s.HandleConcurrency)
	mux.HandleFunc("/api/health", s.HandleHealth)
	mux.HandleFunc("/api/logs", s.HandleLogs)
	mux.HandleFunc("/api/logs/", s.HandleLogEntry)

	// Prometheus scrape endpoint. Like /api/health it is unauthenticated, and
	// it publishes only fixed-label gauges — no task IDs, guest IDs, personas
	// or repository URLs — so there is nothing private in it (issue #260).
	// One scrape at a time is exactly the semantics Prometheus wants anyway,
	// and a failed gather lands in the server log rather than only in a 500
	// nobody watches.
	metricsRegistry, err := metrics.NewRegistry(s.orchestrator)
	if err != nil {
		return fmt.Errorf("register metrics collector: %w", err)
	}
	mux.Handle("/metrics", quietScrapes(promhttp.HandlerFor(metricsRegistry, promhttp.HandlerOpts{
		ErrorLog:            s.log,
		MaxRequestsInFlight: 1,
		Timeout:             10 * time.Second,
	})))

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

// EffectiveConcurrency returns the task-concurrency cap in force at now,
// resolved from the currently loaded configuration (issue #248). It reads
// the config live, so a Reload carrying new concurrency keys takes effect
// without a restart, and it honours a live runtime override set through
// /api/concurrency (issue #264).
//
// It is the single source of truth for "what is the cap right now" and it
// leaves that statement true: it takes the write lock, because resolving may
// retire an override whose schedule boundary has arrived, and it hands the
// resolved number to the orchestrator on the way out — retirement and
// enforcement cannot be separated by a read, so the number it returns is the
// number in force the instant it returns (issue #264 review).
//
// A rise is no more separable from the drain it earns here than it is at the
// other sites that move the cap: Reload, refreshConcurrency and
// HandleConcurrency each take the verdict and drain outside the lock, and a
// site that installed a risen cap and dropped the verdict would leave the
// next sync tick comparing that number against an orchestrator already
// holding it, reporting no rise, and draining nothing.
//
// Because it can log, write and assign, it must never be called while the
// caller holds s.mu (the RWMutex is not reentrant).
func (s *Server) EffectiveConcurrency(now time.Time) int {
	s.mu.Lock()
	effective, rose := s.pushConcurrencyCapLocked(s.cfg, now)
	s.mu.Unlock()
	if rose {
		// A raised cap is itself the event the held queue was waiting on,
		// so drain it here rather than waiting for a guest to poll. Outside
		// the lock: the assignment pass talks to guests and must not hold
		// the config open (issue #249).
		s.tryAssignPendingTasks()
	}
	return effective
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
		// Host is the machine the guest process runs on (issue #238). Optional:
		// a guest that does not report one leaves the last known host untouched.
		Host string `json:"host"`
		// Resumed says whether the process registering has done so before —
		// i.e. whether "id already in the registry" is a reconnection or a
		// brand-new process reusing a stable id (issue #253). A pointer
		// because the field is optional: a guest binary that predates it
		// cannot know its id survives a restart, and for such a binary a
		// known id always *is* the same process, since it generated it.
		Resumed *bool `json:"resumed"`
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
			// The guest may have reconnected from a different machine.
			s.setGuestHost(req.ID, req.Host)
		}

		// A returning process still holds whatever it was given, so its
		// registration is reconciled against the task queue. A fresh one
		// holds nothing, and reconciling it would hand it a task it is not
		// running (issue #253).
		var reconState orchestrator.ReconcileState
		if resumed(req.Resumed) {
			reconState = s.orchestrator.ReconcileGuest(req.ID)
		} else {
			reconState = s.orchestrator.ReconcileFreshGuest(req.ID)
		}

		if exists {
			s.log.Printf("guest re-registered: %s (reconciled to %s, task: %s)",
				existing.ID, reconState.Status, reconState.TaskID)

			// If the guest is now available, try to assign a pending task.
			if registry.AvailableForAssignment(existing) {
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
	s.setGuestHost(guest.ID, req.Host)

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

// resumed interprets the optional resumed flag of a guest.register payload.
// Absent means a guest binary that predates the field, whose id is generated
// per process — for it, an id the registry already holds is always the same
// process coming back, so the pre-issue-#253 behaviour is preserved.
func resumed(flag *bool) bool {
	return flag == nil || *flag
}

// setGuestHost records the machine a guest reported at registration (issue
// #238). A guest binary that predates the field sends no host, so the last
// known host for that guest ID survives rather than being blanked out.
func (s *Server) setGuestHost(guestID, host string) {
	if host == "" {
		return
	}
	if err := s.orchestrator.Registry().SetGuestHost(guestID, host); err != nil {
		s.log.Printf("could not record host for guest %s: %v", guestID, err)
	}
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
		TaskID       string `json:"task_id"`
		Line         string `json:"line"`
		Level        string `json:"level,omitempty"`
		ToolType     string `json:"tool_type,omitempty"`
		ToolName     string `json:"tool_name,omitempty"`
		ToolID       string `json:"tool_id,omitempty"`
		ToolArgs     string `json:"tool_args,omitempty"`
		ToolArgsJSON string `json:"tool_args_json,omitempty"`
		ToolOutput   string `json:"tool_output,omitempty"`
		ToolError    bool   `json:"tool_error,omitempty"`
		// Structured compaction fields (level "compaction").
		CompactionType         string `json:"compaction_type,omitempty"`
		CompactionReason       string `json:"compaction_reason,omitempty"`
		CompactionSummary      string `json:"compaction_summary,omitempty"`
		CompactionTokensBefore int    `json:"compaction_tokens_before,omitempty"`
		CompactionTokensAfter  int    `json:"compaction_tokens_after,omitempty"`
		CompactionError        bool   `json:"compaction_error,omitempty"`
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
				e.ToolArgsJSON = req.ToolArgsJSON
				e.ToolOutput = req.ToolOutput
				e.ToolError = req.ToolError
			}
			if req.Level == "compaction" || req.CompactionType != "" {
				e.CompactionType = req.CompactionType
				e.CompactionReason = req.CompactionReason
				e.CompactionSummary = req.CompactionSummary
				e.CompactionTokensBefore = req.CompactionTokensBefore
				e.CompactionTokensAfter = req.CompactionTokensAfter
				e.CompactionError = req.CompactionError
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
					TaskID:                 e.TaskID,
					Line:                   e.Line,
					Level:                  e.Level,
					Timestamp:              e.Timestamp,
					ToolType:               e.ToolType,
					ToolName:               e.ToolName,
					ToolID:                 e.ToolID,
					ToolArgs:               e.ToolArgs,
					ToolArgsJSON:           e.ToolArgsJSON,
					ToolOutput:             e.ToolOutput,
					ToolError:              e.ToolError,
					CompactionType:         e.CompactionType,
					CompactionReason:       e.CompactionReason,
					CompactionSummary:      e.CompactionSummary,
					CompactionTokensBefore: e.CompactionTokensBefore,
					CompactionTokensAfter:  e.CompactionTokensAfter,
					CompactionError:        e.CompactionError,
				}); err != nil {
					fmt.Fprintf(os.Stderr, fatalwriter.FatalMsgFormat, err)
					os.Exit(1)
				}
			}
			s.hub.SendNotification("", rpc.ConnectionRoleBrowser, "task.log", map[string]interface{}{
				"task_id":                  e.TaskID,
				"line":                     e.Line,
				"level":                    e.Level,
				"tool_type":                e.ToolType,
				"tool_name":                e.ToolName,
				"tool_id":                  e.ToolID,
				"tool_args":                e.ToolArgs,
				"tool_args_json":           e.ToolArgsJSON,
				"tool_output":              e.ToolOutput,
				"tool_error":               e.ToolError,
				"compaction_type":          e.CompactionType,
				"compaction_reason":        e.CompactionReason,
				"compaction_summary":       e.CompactionSummary,
				"compaction_tokens_before": e.CompactionTokensBefore,
				"compaction_tokens_after":  e.CompactionTokensAfter,
				"compaction_error":         e.CompactionError,
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
		// Action is what the guest did with the cancel: it names the task the
		// confirmation is about, so a cancel for a task the guest never held
		// no longer looks like a cancellation (issue #252).
		Action string `json:"action,omitempty"`
		// Intent echoes the intent the host sent: "revoke" means the host is
		// re-queuing the task and only wanted the claim back.
		Intent string `json:"intent,omitempty"`
	}

	if err := json.Unmarshal(params, &req); err != nil {
		return nil, rpc.InvalidParamsError("invalid request parameters")
	}

	if req.TaskID == "" {
		return nil, rpc.InvalidParamsError("task_id is required")
	}

	// Resolve the intent before anything else: it — not the guest's action —
	// decides who owns the task's fate. An echoed intent wins; a missing one
	// (an old guest) falls back to what the host last sent for this task.
	intent := s.resolveCancelIntent(req.TaskID, req.Intent)

	if req.Action == "" {
		// A guest that predates the action field confirms every cancel it
		// receives; treat the confirmation as one about a task it held.
		req.Action = cancelActionRunning
	}
	s.claims.record(req.Action)
	s.log.Printf("task %s cancel reported by guest %s (%s): %s", req.TaskID, req.GuestID, req.Action, req.Reason)

	if intent == cancelIntentRevoke {
		// The host re-queued the task and kept its fate in its own hands.
		// Whether the guest dropped a queued claim or no longer held the
		// task at all, the confirmation only says this guest will not run
		// it. Cancelling here would end a task that is queued, possibly on
		// another guest (issue #252) — the guest has no say in the fate of
		// a task it was only asked to give back.
		if task, ok := s.orchestrator.GetTask(req.TaskID); ok {
			s.broadcastTaskUpdated(req.TaskID, task.Status.String())
		}
		return map[string]interface{}{"status": "accepted", "action": req.Action}, nil
	}

	// The cancel was requested by the host — the user's cancel endpoint or
	// silence detection — not by this guest, so the task ends whether or not
	// this guest still holds it. A not_held confirmation used to return
	// early, which left a task the user had cancelled sitting RUNNING with
	// no holder until the stuck-task probe re-queued it — running work the
	// user had explicitly cancelled.
	if req.Action == cancelActionNotHeld {
		s.log.Printf("task %s not held by guest %s; ending it because the cancel was requested by the host", req.TaskID, req.GuestID)
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

	// A task the host has no record of says nothing about the claim: it is
	// the shape of a restarted host or a stale test harness, and the guest
	// must not read it as a refusal. Anything else the orchestrator rejects
	// is a statement about the claim — completed, failed, re-queued, or
	// assigned to somebody else (issue #252).
	if _, ok := s.orchestrator.GetTask(req.TaskID); !ok {
		s.log.Printf("task %s ack failed from guest %s: task not found", req.TaskID, guestID)
		return nil, rpc.InternalError(fmt.Sprintf("task %s not found", req.TaskID))
	}

	if err := s.orchestrator.AcknowledgeTask(req.TaskID, guestID); err != nil {
		// Every refused ack is counted, not just logged: these are the
		// "is not assigned (status: COMPLETED)" and "is assigned to guest-X,
		// not guest-Y" refusals issue #252 was written from.
		s.claims.recordAckRefusal()
		s.log.Printf("task %s ack refused for guest %s: %v", req.TaskID, guestID, err)
		return nil, rpc.ClaimRefusedError(err.Error())
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
// It re-queues any running task for the guest associated with the connection,
// allowing another guest (or the same guest after reconnecting) to pick it up.
// This prevents tasks from being permanently lost due to transient connection
// issues or pi subprocess failures.
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

	s.log.Printf("connection lost for guest %s: re-queuing task %s (%s)",
		guestID, guest.TaskID, task.Status)

	// Revoke the claim before the task goes back to the queue. The connection
	// is dying, so this usually goes nowhere — the guest will find the claim
	// refused when it tries to acknowledge it after reconnecting (issue #252).
	s.revokeClaim(guest.TaskID, guestID, claimReasonConnectionLost)

	// Re-queue the task atomically: task→PENDING, guest→IDLE.
	// This allows the task to be picked up by another guest (or the same
	// guest after it reconnects). See issue #4.
	if err := s.orchestrator.RequeueTask(guest.TaskID); err != nil {
		s.log.Printf("failed to re-queue task %s on disconnect: %v", guest.TaskID, err)
	}

	// Notify UI
	s.broadcastTaskUpdated(guest.TaskID, "PENDING")
}

// tryAssignPendingTasks iterates idle guests and tries to assign pending
// tasks to them. Used after stale guest removal to fill the gap.
func (s *Server) tryAssignPendingTasks() {
	for _, guest := range s.orchestrator.GetAllGuests() {
		if registry.AvailableForAssignment(guest) {
			s.tryAssignTask(guest.ID)
		}
	}
}

// noteWhyNotAssigned records why a task that could not be handed out is still
// PENDING. The cap is asked first and wins: when it has no room, the
// orchestrator has already written the reason into the task and logged it, and
// saying "no available guest" there would send an operator looking for a dead
// pool while night mode is holding the queue (issue #249).
func (s *Server) noteWhyNotAssigned(task *queue.Task) {
	if !s.orchestrator.AdmitsNewWork(task.ID) {
		return
	}
	s.log.Printf("task %s pending: no available guest with tags %v", task.ID, task.Tags)
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
		s.noteWhyNotAssigned(task)
		return
	}

	// Try each idle guest in order until one accepts
	for _, guest := range guests {
		// Skip the guest that already declined
		if guest.ID == skipGuestID {
			continue
		}

		if err := s.orchestrator.AssignTask(task.ID, guest.ID); err != nil {
			// A cap hold is already in the log, deduplicated, from the
			// orchestrator; repeating it per attempt would read like a
			// malfunction (issue #249).
			if !errors.Is(err, orchestrator.ErrConcurrencyCapReached) {
				s.log.Printf("failed to assign task %s to guest %s: %v", task.ID, guest.ID, err)
			}
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
				s.revokeClaim(task.ID, guest.ID, claimReasonRequeued)
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
			// No revoke here: the assignment never reached this guest, so it
			// holds nothing to give back.
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

	// Only assign to available guests — availability is a fact about the
	// guest (no claim held, no running task reported in its heartbeat), not
	// merely its nominal state (issue #251).
	if !registry.AvailableForAssignment(guest) {
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
		// The cap already said so once, at the orchestrator, in its own
		// words; logging it again on every poll would bury the log (issue #249).
		if !errors.Is(err, orchestrator.ErrConcurrencyCapReached) {
			s.log.Printf("failed to assign task %s to guest %s: %v", matchedTask.ID, guestID, err)
		}
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
			s.revokeClaim(matchedTask.ID, guestID, claimReasonRequeued)
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
		// No revoke here: the assignment never reached this guest, so it
		// holds nothing to give back (same reasoning as the push-failure
		// rollback in tryAssignTaskToEligible).
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
// the server marks any running task as failed and removes the guest from the
// registry.
func (s *Server) staleGuestCleanup() {
	interval := time.Duration(s.effectiveHeartbeatInterval()) * time.Second

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

			// Then: remove guests that have been completely silent (no heartbeat).
			// The orchestrator fails any running tasks before removing the guest.
			s.removeStaleGuests()

			// After removing stale guests, try to assign pending tasks to remaining idle guests.
			s.tryAssignPendingTasks()
		}
	}
}

// logPruneCleanup sweeps the disk task log store at startup and then on the
// heartbeat-interval tick, the way staleGuestCleanup ticks for guests.
// Startup-only would fix a one-off fill but let a long-running server outlive
// its own retention window, so the tick is what keeps the tree bounded
// (issue #263).
func (s *Server) logPruneCleanup() {
	s.pruneTaskLogs()

	interval := time.Duration(s.effectiveHeartbeatInterval()) * time.Second
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for range ticker.C {
		s.pruneTaskLogs()
	}
}

// pruneTaskLogs runs one retention sweep over the disk log store, honouring
// the log_retention_days value loaded right now (0 disables pruning; a
// Reload takes effect on the next tick without a restart). It logs what it
// pruned — dirs, file count and bytes — so the evidence outlives the disk
// it freed (issue #263). With no disk log store configured there is nothing
// on disk to prune and the sweep is a no-op.
func (s *Server) pruneTaskLogs() {
	s.mu.RLock()
	store := s.diskLogStore
	retention := s.cfg.LogRetentionDays
	s.mu.RUnlock()

	if store == nil {
		return
	}

	// PruneOlderThan keeps walking past a directory it cannot remove, so the
	// partial result is still worth logging before the error: the sweep that
	// could not free one stuck day has usually freed the rest (issue #263
	// review).
	res, err := store.PruneOlderThan(time.Now(), retention)
	if len(res.DatesRemoved) > 0 {
		s.log.Printf("pruned task logs: %d date dir(s) %v, %d file(s), %d bytes reclaimed",
			len(res.DatesRemoved), res.DatesRemoved, res.FilesRemoved, res.BytesReclaimed)
	}
	if err != nil {
		s.log.Printf("task log prune failed: %v", err)
	}
}

// removeStaleGuests removes guests that have been silent (no heartbeat) for
// longer than the configured SilenceTimeout. Any running task is failed
// atomically before the guest is removed. Set SilenceTimeout to 0 to disable
// stale-guest removal.
func (s *Server) removeStaleGuests() {
	timeout := time.Duration(s.cfg.SilenceTimeout) * time.Second
	if timeout == 0 {
		return // Stale-guest removal disabled
	}

	stale := s.orchestrator.RemoveStaleGuests(timeout)
	for _, sg := range stale {
		s.log.Printf("stale guest removed: %s", sg.GuestID)
		if sg.TaskWasRunning {
			s.broadcastTaskUpdated(sg.TaskID, "FAILED")
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
			"intent":  cancelIntentCancel,
		}
		// The intent is recorded before the send, as every sender of
		// task.cancel does: a confirmation handled on another goroutine must
		// never resolve against a record that does not exist yet.
		s.rememberCancelIntent(sg.TaskID, cancelIntentCancel)
		if err := s.hub.SendToGuest(sg.GuestID, "task.cancel", cancelParams); err != nil {
			s.forgetCancelIntent(sg.TaskID)
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

		// The probe moved the task back to PENDING; the guest may still hold
		// the assignment in its queue and run it later. Revoke that claim
		// (issue #252).
		s.revokeClaim(st.TaskID, st.GuestID, claimReasonRequeued)

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

// capLabel states a cap for a JSON surface: the unlimited case in words, so
// a bare -1 does not read as a broken number (issue #249). /api/health and
// the concurrency picture on /api/guests share it so the two cannot drift.
func capLabel(cap int) interface{} {
	if cap == config.ConcurrencyUnlimited {
		return "unlimited"
	}
	return cap
}

// handleHealth returns the server health status.
// HandleHealth returns the server health status.
func (s *Server) HandleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status": "healthy",
		"guests": s.orchestrator.Registry().Count(),
		"tasks":  s.orchestrator.Queue().Count(),
		// The cap and what is in flight against it, so "nothing is moving"
		// can be read as night mode rather than guessed at (issue #249).
		// Health reads only the two numbers it publishes: running the
		// pending-count scan ConcurrencyCounts adds for /api/guests here
		// would make every unauthenticated probe walk the whole queue
		// under the assignment lock to discard the answer. The cap is the
		// same enforced value ConcurrencyCounts states, so the two
		// surfaces state one number; the pair is read in two
		// acquisitions, so only an assignment landing between the
		// reads can widen the window between them.
		"in_flight":       s.orchestrator.InFlightCount(),
		"concurrency_cap": capLabel(s.orchestrator.ConcurrencyCap()),
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
	tasks := s.orchestrator.GetAllTasksOrdered()
	// Build task list with log_count annotation
	taskList := make([]map[string]interface{}, len(tasks))
	for i, t := range tasks {
		taskMap := map[string]interface{}{
			"id":          t.ID,
			"prompt":      t.Prompt,
			"tags":        t.Tags,
			"repo_ref":    t.RepoRef,
			"persona":     t.Persona,
			"title":       t.Title,
			"url":         t.URL,
			"priority":    t.Priority,
			"status":      t.Status.String(),
			"created_at":  t.CreatedAt,
			"assigned_to": t.AssignedTo,
			"assigned_at": t.AssignedAt,
			"timeout":     t.Timeout,
			"result":      t.Result,
			"error":       t.Error,
			"dedup_key":   t.DedupKey,
			"held_reason": t.HeldReason,
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

	// Validate priority if specified
	if task.Priority != "" && !queue.ValidatePriority(task.Priority) {
		http.Error(w, fmt.Sprintf("invalid priority %q (must be firefighter, teacher, or orangutan)", task.Priority), http.StatusBadRequest)
		return
	}

	if task.ID == "" {
		task.ID = fmt.Sprintf("task-%d", time.Now().UnixNano())
	}

	// Add with deduplication: if the task carries a dedup_key that matches a
	// PENDING task, the submission is squelched and the existing task is
	// returned instead (no new task is created or assigned).
	added, deduplicated, err := s.orchestrator.AddTaskOrDedup(&task)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if deduplicated {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"task":      added,
			"dedup":     true,
			"dedup_key": added.DedupKey,
		})
		return
	}

	// Try to assign to an available guest
	guests := s.orchestrator.FindAvailableGuests(task.Tags)
	if len(guests) > 0 {
		s.tryAssignTask(guests[0].ID)
	} else {
		// Say why the task is waiting, cap or pool (issue #249).
		s.noteWhyNotAssigned(&task)
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

	// Check for cancel sub-path: /api/tasks/:id/cancel
	if strings.HasSuffix(path, "/cancel") {
		taskID := strings.TrimSuffix(path, "/cancel")
		s.handleTaskCancelHTTP(w, r, taskID)
		return
	}

	// Check for priority sub-path: /api/tasks/:id/priority
	if strings.HasSuffix(path, "/priority") {
		taskID := strings.TrimSuffix(path, "/priority")
		s.handleTaskPriority(w, r, taskID)
		return
	}

	// Check for download sub-path: /api/tasks/:id/download
	if strings.HasSuffix(path, "/download") {
		taskID := strings.TrimSuffix(path, "/download")
		s.handleTaskLogDownload(w, r, taskID)
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
// POST /api/tasks/:id/rerun — clones the task's prompt, tags, title and
// reference URL
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
		ID:       fmt.Sprintf("task-%d", time.Now().UnixNano()),
		Prompt:   orig.Prompt,
		Tags:     orig.Tags,
		RepoRef:  orig.RepoRef,
		Persona:  orig.Persona,
		Title:    orig.Title,
		URL:      orig.URL,
		Priority: orig.Priority,
	}

	if err := s.orchestrator.AddTask(newTask); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Try to assign to an available guest
	guests := s.orchestrator.FindAvailableGuests(newTask.Tags)
	if len(guests) > 0 {
		s.tryAssignTask(guests[0].ID)
	} else {
		s.noteWhyNotAssigned(newTask)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(newTask)
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

		// Recorded before the send so a confirmation handled concurrently
		// can always resolve against it; dropped again if the send fails
		// (#252 review).
		s.rememberCancelIntent(taskID, cancelIntentCancel)
		if err := s.hub.SendToGuest(task.AssignedTo, "task.cancel", map[string]interface{}{
			"task_id": taskID,
			"reason":  "user requested cancellation",
			"intent":  cancelIntentCancel,
		}); err != nil {
			s.forgetCancelIntent(taskID)
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

// handleTaskPriority handles the /api/tasks/:id/priority endpoint.
// POST /api/tasks/:id/priority — changes the priority of a PENDING task
// (issue #266). The body must carry {"priority": "<level>"}. Only pending
// tasks accept priority changes: priority orders the pending queue, so a
// task already committed to a guest cannot be re-prioritised (409).
func (s *Server) handleTaskPriority(w http.ResponseWriter, r *http.Request, taskID string) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if taskID == "" {
		http.Error(w, "task id required", http.StatusBadRequest)
		return
	}

	var body struct {
		Priority string `json:"priority"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	if !queue.ValidatePriority(body.Priority) {
		http.Error(w, fmt.Sprintf("invalid priority %q (must be firefighter, teacher, or orangutan)", body.Priority), http.StatusBadRequest)
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

	// Re-checks existence and status under the orchestrator lock; a failure
	// here means the task moved between the pre-check and now.
	if err := s.orchestrator.SetTaskPriority(taskID, body.Priority); err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}

	// Broadcast task.updated to browsers so the list and any open detail
	// view re-render with the new priority.
	s.broadcastTaskUpdated(taskID, task.Status.String())

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"task_id":  taskID,
		"priority": body.Priority,
		"status":   task.Status.String(),
	})
}

// handleGuests handles the /api/guests endpoint.
// HandleGuests handles the /api/guests endpoint.
//
// Alongside the registry entries the response carries, per guest, the
// server-measured seconds since its last heartbeat (so the UI can render
// check-in latency without comparing its own clock to the server's) and, for
// the whole list, the effective heartbeat_interval / silence_timeout the UI
// derives its "unexpected" and "halfway to timeout" colour thresholds from
// (issue #240).
func (s *Server) HandleGuests(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	switch r.Method {
	case http.MethodGet:
		guests := s.orchestrator.GetAllGuests()
		now := time.Now()
		listed := make([]guestWithAge, len(guests))
		for i, g := range guests {
			listed[i] = guestWithAge{
				Guest:               g,
				HeartbeatAgeSeconds: now.Sub(g.LastHeartbeat).Seconds(),
				HeartbeatTaskMismatch: g.TaskID != "" && g.HeartbeatTaskID != "" &&
					g.HeartbeatTaskID != g.TaskID,
			}
		}
		json.NewEncoder(w).Encode(map[string]interface{}{
			"guests":             listed,
			"count":              len(listed),
			"heartbeat_interval": s.effectiveHeartbeatInterval(),
			"silence_timeout":    s.cfg.SilenceTimeout,
			"stats":              s.claimStats(),
			// The concurrency picture rides along on the poll the dashboard
			// already makes, so the cap is visible without a second request
			// (issue #250).
			"concurrency": s.concurrencyStatus(now),
		})
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// concurrencyStatus is the concurrency picture for the status surface
// (issue #250): the cap in force right now, the entry of the schedule that
// produced it, how much work is in flight against it, how much more the cap
// is holding back, and the next boundary at which the number changes.
//
// The cap and the counts come from the orchestrator, in a single lock
// acquisition (ConcurrencyCounts) — one response is one instant, and the
// number an operator sees is the number the next assignment decision obeys.
// The provenance (`source`) and the schedule picture (`next_change`,
// `timezone`) come from the config resolver, which reads the schedule
// directly; immediately after a boundary they can therefore lead the
// enforced cap by up to one concurrencySyncInterval (15s), the pre-existing
// sync lag, until syncConcurrencyCap pushes the resolver's answer into the
// orchestrator. /api/health reads the same enforced cap, so the two surfaces
// state one number.
//
// The HTTP API performs no authentication; bind to loopback or terminate
// behind an authenticating proxy (see the README server configuration
// section). Everything in this response — queue depth, throttle schedule,
// timezone — must be written for an untrusted reader.
func (s *Server) concurrencyStatus(now time.Time) map[string]interface{} {
	s.mu.RLock()
	status := s.cfg.ConcurrencyStatus(now)
	// While an override is installed, the number comes from the operator,
	// not the config, and the status says so (issue #264). The sync tick
	// clears the override when the schedule takes over, so the label and
	// the enforced number agree at every instant.
	if s.capOverride != nil {
		status.Source = config.CapSourceOverride
	}
	s.mu.RUnlock()

	var next interface{}
	if status.HasNextChange() {
		next = map[string]interface{}{
			"at":       status.NextAt(),
			"cap":      status.NextCap,
			"timezone": status.Timezone,
		}
	}

	capNow, inFlight, waitingOnCap := s.orchestrator.ConcurrencyCounts()

	return map[string]interface{}{
		"cap":    capLabel(capNow),
		"source": status.Source,
		// "in_flight" is the ASSIGNED or RUNNING count the cap is measured
		// against; "waiting_on_cap" is the pending queue there is no room
		// for under the cap.
		"in_flight":      inFlight,
		"waiting_on_cap": waitingOnCap,
		"next_change":    next,
	}
}

// concurrencyBodyLimit bounds the /api/concurrency request body: the whole
// vocabulary of the endpoint fits in a few dozen bytes, so anything longer
// is an attack or a mistake, not a cap.
const concurrencyBodyLimit = 1024

// isLoopbackPeer reports whether the peer address of r names a loopback
// address — the operator's own machine, or an authenticating proxy
// running beside the server. A peer address the transport does not carry
// (an empty or unparsable RemoteAddr) is not loopback: it fails closed.
func isLoopbackPeer(r *http.Request) bool {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return false
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// operatorAllowed is the identity check on the state-changing concurrency
// route (issue #264 review): whoever moves the fleet-wide cap must be the
// operator, not merely someone who can open a socket. A loopback peer is
// the operator by the posture this repository documents (bind to loopback
// or terminate authentication at a local proxy); anything else is refused
// outright — the route fails closed instead of trusting a network posture
// nothing enforces. A loopback peer must also name this server in Host—a
// loopback address (hostIsLoopbackName), or one of the configured
// trusted_hosts names (issue #286), which is how a deployment behind a
// local proxy that forwards the public Host stays reachable. The name
// half is what stops a DNS-rebound page from borrowing the operator's own
// browser: Host says which server the client meant, not who asked, so the
// peer half stays and a list entry only ever adds an admissible name.
// There are no wildcards: rebinding presents whatever name the attacker's
// own zone serves, and config.ValidateTrustedHosts refuses an entry that
// could match nothing rather than letting it fail closed in silence. What
// setting the list costs is worth naming here: the verdict stops being
// "this came from this machine" and becomes "this came through my proxy",
// so while any name is configured the proxy itself must deny this route to
// everyone but the operator—guest machines included—until #285 enforces
// operator identity at the service; the README and server.example.yaml say
// that to the operator. A shared-secret mechanism for genuinely remote
// operators is part of the same cross-route decision and lives with the
// rest of the posture question in issue #285, not here with one route's
// share of it.
func (s *Server) operatorAllowed(r *http.Request) bool {
	if isLoopbackPeer(r) {
		name := config.NormaliseHostName(r.Host)
		if hostIsLoopbackName(name) || s.hostIsTrusted(name) {
			return true
		}
	}
	s.log.Printf("concurrency cap request refused: peer %s, Host %s", r.RemoteAddr, r.Host)
	return false
}

// hostIsLoopbackName reports whether a host name names the loopback
// address — localhost, or an address in 127.0.0.0/8 or ::1. It normalises
// its argument first (config.NormaliseHostName is idempotent, so a caller
// that already normalised loses nothing), because issue #285 is announced
// as the next consumer of this mechanism and a caller that handed it a raw
// Host value must not compile into a silent 403 for Host: Localhost. It is
// the DNS-rebinding guard (issue #264 review): a page on a rebound domain
// reaches the server from a loopback peer address, but it carries its own
// domain in Host, and an admitted request addresses this server by the name
// its own address bar or proxy shows.
func hostIsLoopbackName(name string) bool {
	name = config.NormaliseHostName(name)
	if name == "localhost" {
		return true
	}
	ip := net.ParseIP(name)
	return ip != nil && ip.IsLoopback()
}

// hostIsTrusted reports whether a normalised host name is one of the
// configured trusted_hosts entries (issue #286), each read through the same
// normaliser as the request side so the two cannot disagree about what a
// name is. An empty name names nothing and never matches, so a blank entry
// cannot admit a Host-less request—an entry like that is refused at load by
// config.ValidateTrustedHosts, and stays inert here.
func (s *Server) hostIsTrusted(name string) bool {
	if name == "" {
		return false
	}
	for _, h := range s.cfg.TrustedHosts {
		if config.NormaliseHostName(h) == name {
			return true
		}
	}
	return false
}

// HandleConcurrency handles the /api/concurrency endpoint (issue #264):
// POST sets the live concurrency cap at runtime, with no persistence to
// disk. The value takes effect immediately — the same contract a reload
// has — and rules until the next boundary of the concurrency schedule,
// when the scheduled shrink or expansion takes over again; with no
// schedule it rules until the next config reload or restart. The response
// is the same concurrency picture the dashboard polls on /api/guests, so
// the caller sees the cap, its source and what it holds back after the
// change.
//
// The body is `{"cap": N}`: N >= 0 is the cap (0 pauses new assignments,
// the vocabulary a schedule entry uses for the overnight pause), and the
// string "unlimited" — like any negative number — lifts the cap entirely.
// A string that spells a number says the same as the number, which is how
// the dashboard posts the field.
// The request must say `Content-Type: application/json` (anything else is
// 415, a CSRF guard: this endpoint must never answer a cross-origin
// simple POST), and a body beyond a kilobyte is refused. The handler
// enforces operator identity first (operatorAllowed): a peer that is not
// loopback, or a loopback peer whose Host names neither a loopback address
// nor a configured trusted_hosts name (a DNS-rebinding attempt), gets 403
// before the cap moves.
//
// Every other surface here keeps the repository's standing posture — the
// HTTP API performs no authentication; bind to loopback or terminate
// behind an authenticating proxy — and extending this check to the whole
// API is tracked in issue #285.
func (s *Server) HandleConcurrency(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Identity before anything else (issue #264 review): reaching this
	// route is not itself a credential.
	if !s.operatorAllowed(r) {
		http.Error(w, "operator identity required: a loopback peer addressing this server by a loopback or configured host name", http.StatusForbidden)
		return
	}

	// CSRF defence (issue #264 review): this endpoint changes state, so
	// it refuses the cross-origin "simple" POSTs a browser can send
	// without a CORS preflight. The dashboard already posts
	// Content-Type: application/json; anything that cannot say JSON —
	// text/plain, a form encoding — is not a dashboard, and fail-closing
	// here stops a malicious web page from reaching a loopback-bound
	// server through the operator's own browser.
	if media, _, err := mime.ParseMediaType(r.Header.Get("Content-Type")); err != nil || media != "application/json" {
		http.Error(w, "Content-Type must be application/json", http.StatusUnsupportedMediaType)
		return
	}

	// The whole body is a few dozen bytes; bound the read so the
	// magnitude bound in parseCapValue cannot be preceded by buffering
	// an arbitrarily large number literal into memory.
	r.Body = http.MaxBytesReader(w, r.Body, concurrencyBodyLimit)

	var body struct {
		Cap *json.RawMessage `json:"cap"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid JSON body", http.StatusBadRequest)
		return
	}
	if body.Cap == nil {
		http.Error(w, `body must carry "cap": a whole number (0 pauses new work) or "unlimited"`, http.StatusBadRequest)
		return
	}
	newCap, err := parseCapValue(*body.Cap)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	now := time.Now()
	rose := s.setConcurrencyCapOverride(newCap, r.RemoteAddr, now)
	if rose {
		// A raised cap is itself the event the held queue was waiting on;
		// drain it outside the lock, as reload and the sync tick do
		// (issue #249).
		s.tryAssignPendingTasks()
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(s.concurrencyStatus(now))
}

// parseCapValue reads the "cap" member of a /api/concurrency body. The
// vocabulary of the cap is the one the cap has everywhere: a whole number
// >= 0 is that cap (0 pauses), any negative number is the unlimited
// sentinel, and the string "unlimited" says the same thing in the words
// the status surface uses. A string that spells a number — the shape the
// dashboard posts, so the field never needs its own copy of this grammar
// — is read by the JSON number grammar, the same one the number branch
// above applies, so a value cannot be refused through one door and
// applied through the other: "5_0", "+7" and "5." are not numbers and are
// refused whether or not they are quoted. Anything else — a fraction, a
// different string, null, a number too large to be a queue depth — is
// refused, so a typo cannot silently pause or un-cap the fleet.
func parseCapValue(raw json.RawMessage) (int, error) {
	var n float64
	if err := json.Unmarshal(raw, &n); err == nil {
		return capFromNumber(n)
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		s = strings.TrimSpace(s)
		if strings.EqualFold(s, "unlimited") {
			return config.ConcurrencyUnlimited, nil
		}
		// Decode into `any` rather than straight into a float64: encoding/json
		// reads the four-letter literal `null` as a successful decode into a
		// non-pointer number and leaves the target untouched, which here is
		// the cap 0 — a fleet-wide pause the function doc above says is
		// refused.
		// Through an `any` a null is a nil, the assertion fails, and the
		// refusal stands, so both doors keep answering one value one way.
		var v any
		if err := json.Unmarshal([]byte(s), &v); err == nil {
			if f, ok := v.(float64); ok {
				return capFromNumber(f)
			}
		}
	}
	return 0, errors.New(`cap must be a whole number >= 0 or "unlimited"`)
}

// capFromNumber maps a parsed value onto a cap: fractions and the
// infinities are not caps, any negative number is the unlimited sentinel,
// and anything beyond a plausible queue depth is refused.
func capFromNumber(n float64) (int, error) {
	if math.IsNaN(n) || math.IsInf(n, 0) || n != math.Trunc(n) {
		return 0, errors.New("cap must be a whole number")
	}
	if n < 0 {
		return config.ConcurrencyUnlimited, nil
	}
	if n > math.MaxInt32 {
		return 0, errors.New("cap is implausibly large")
	}
	return int(n), nil
}

// guestWithAge flattens a registry entry for /api/guests and adds the
// derived heartbeat age (issue #240), plus the heartbeat task mismatch flag:
// the guest's heartbeat reports a running task other than the one assigned
// — a mismatch to surface and resolve via reconciliation, never by
// rewriting the assignment (issue #251).
type guestWithAge struct {
	*registry.Guest
	HeartbeatAgeSeconds   float64 `json:"heartbeat_age_seconds"`
	HeartbeatTaskMismatch bool    `json:"heartbeat_task_mismatch"`
}

// defaultHeartbeatIntervalSeconds is the heartbeat interval assumed when the
// config leaves it unset, matching the fallback in staleGuestCleanup.
const defaultHeartbeatIntervalSeconds = 30

// effectiveHeartbeatInterval returns the heartbeat interval guests are held
// to, falling back to the default when the config leaves it zero.
func (s *Server) effectiveHeartbeatInterval() int {
	if s.cfg.HeartbeatInterval > 0 {
		return s.cfg.HeartbeatInterval
	}
	return defaultHeartbeatIntervalSeconds
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

	case 3:
		// /api/logs/:date/:task/download → download the raw JSONL log file
		if parts[2] == "download" {
			date, taskID := parts[0], parts[1]
			if date == "" || taskID == "" {
				http.Error(w, "date and task id required", http.StatusBadRequest)
				return
			}
			s.handleLogDownload(w, date, taskID)
			return
		}
		http.Error(w, "not found", http.StatusNotFound)
		return

	default:
		http.Error(w, "not found", http.StatusNotFound)
	}
}

// logDatePattern and logTaskIDPattern constrain the path segments that
// handleLogDownload reflects into the Content-Disposition response header.
// Task IDs are server-generated as "task-<unixnano>"; the pattern is
// deliberately permissive (alphanumerics, dot, dash, underscore) but
// rejects quotes, whitespace and other characters that could produce a
// malformed header value.
var (
	logDatePattern   = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}$`)
	logTaskIDPattern = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)
)

// handleLogDownload serves the raw JSONL log file for a task as a
// downloadable attachment (Issue #6).
func (s *Server) handleLogDownload(w http.ResponseWriter, date, taskID string) {
	// Validate the path segments before reflecting taskID into the
	// Content-Disposition header.
	if !logDatePattern.MatchString(date) || !logTaskIDPattern.MatchString(taskID) {
		http.Error(w, "invalid date or task id", http.StatusBadRequest)
		return
	}

	raw, err := s.diskLogStore.ReadRawJSONL(date, taskID)
	if err != nil {
		if errors.Is(err, logstore.ErrLogNotFound) {
			http.Error(w, "log not found", http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Override the JSON Content-Type set at the top of HandleLogEntry.
	writeLogAttachment(w, taskID+".jsonl", raw)
}

// handleTaskLogDownload serves a task's whole execution trace as a
// downloadable JSONL attachment without the caller having to know which date
// partitions the trace lives in (Issue #208). Segments spread over several
// partitions are concatenated oldest first. It backs the "Download" button in
// the task view, which only knows the task ID.
func (s *Server) handleTaskLogDownload(w http.ResponseWriter, r *http.Request, taskID string) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Validate before the ID is used to build a path and reflected into the
	// Content-Disposition header.
	if !logTaskIDPattern.MatchString(taskID) {
		http.Error(w, "invalid task id", http.StatusBadRequest)
		return
	}

	if s.diskLogStore == nil {
		http.Error(w, "log store not configured", http.StatusServiceUnavailable)
		return
	}

	raw, dates, err := s.diskLogStore.ReadTaskRawJSONL(taskID)
	if err != nil {
		if errors.Is(err, logstore.ErrLogNotFound) {
			http.Error(w, "log not found", http.StatusNotFound)
			return
		}
		// Log-store errors embed absolute filesystem paths; keep them
		// server-side rather than echoing them to the client.
		s.log.Printf("task trace download %s: %v", taskID, err)
		http.Error(w, "failed to read log", http.StatusInternalServerError)
		return
	}

	writeLogAttachment(w, traceFilename(taskID, dates), raw)
}

// traceFilename names a task-trace download. A trace confined to one date
// partition is named after the task; one spread over several partitions says
// which span the file covers.
func traceFilename(taskID string, dates []string) string {
	if len(dates) < 2 {
		return taskID + ".jsonl"
	}
	return fmt.Sprintf("%s-%s_to_%s.jsonl", taskID, dates[0], dates[len(dates)-1])
}

// writeLogAttachment sends raw JSONL as an attachment with the given filename.
func writeLogAttachment(w http.ResponseWriter, filename, raw string) {
	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", filename))
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write([]byte(raw)); err != nil {
		log.Printf("write log download %s: %v", filename, err)
	}
}

// Lookback policy for task-output search (issue #269). The ideation issue
// fixed these numbers: a search with no window covers the last five days,
// and no request may scan more than fourteen. The cap is enforced
// server-side by rejecting wider windows outright — an explicit 400 rather
// than a silent clamp, so a client never believes it searched further than
// it did — and stands independently of log retention pruning (issue #263):
// pruning may already have removed older dates, but it never widens a
// window.
const (
	defaultSearchLookbackDays = 5
	maxSearchLookbackDays     = 14

	// maxSearchErrorsLogged bounds how many partial-scan failure messages
	// one log line quotes. The errors grow once per partition and once per
	// trace, so a window whose traces all fail to open — a permissions slip
	// after a deploy, a bad volume — would otherwise join a megabyte-scale
	// line that shippers truncate at the tail; the count next to the
	// excerpt carries the rest.
	maxSearchErrorsLogged = 10
)

// The task preference filter (issue #281) is a fragment of a repository
// name, title, prompt, persona or tag, so a few hundred bytes bounds any
// value an operator would type. The body bound carries the JSON envelope
// around the filter text; both bounds exist so an oversized request is
// refused rather than kept in the queue and repeated into the log.
const (
	maxPreferenceFilterLen       = 256
	maxPreferenceFilterBodyBytes = 8 * maxPreferenceFilterLen
)

// HandleTaskFilter handles POST /api/tasks/filter (issue #281): it sets or
// clears the live task preference filter. The body must carry
// {"filter": "<text>"}, and the empty string clears; the response echoes
// what is now in force, so the caller learns the trimmed value.
//
// While a filter is set the queue drains first the pending tasks carrying
// the substring in one of their fields (repository, title, prompt, persona
// or tags); once no pending task matches, normal queue
// semantics apply — the filter is a preference, never a suppression.
//
// There is no read half: the dashboard's persistence path for the field is
// the #filter= fragment in the address bar, so nothing needs the live value
// back and the endpoint is one write with no read.
//
// Like the concurrency cap this is a live operator control held in memory,
// so a server restart clears it. Reach worth stating plainly: like every
// other route on this server it is unauthenticated, and unlike the
// per-task controls it is a *global* one — one POST reorders the pending
// drain for every guest, and it stands until someone clears it. Because
// the effect is global and lasting, the route is also the first place a
// forged browser request would pay off, so it applies the fetch-metadata
// check in rejectsCrossSite before touching the queue: a request a
// browser cannot attribute to this server's own origin — scheme and host
// alike — is refused with 403 whatever its content type or submission
// style. The README's preference-filter
// section states what all this means operationally.
func (s *Server) HandleTaskFilter(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if rejectsCrossSite(r) {
		http.Error(w, "cross-site requests are refused", http.StatusForbidden)
		return
	}

	// The body bound is the filter bound plus slack for the JSON envelope
	// (escapes expand); a truncated read fails as a bad body below.
	var raw map[string]interface{}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxPreferenceFilterBodyBytes)).Decode(&raw); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	filter, ok := raw["filter"].(string)
	if !ok {
		http.Error(w, `body must carry {"filter": "<text>"}`, http.StatusBadRequest)
		return
	}
	if len(strings.TrimSpace(filter)) > maxPreferenceFilterLen {
		http.Error(w, "filter too long", http.StatusBadRequest)
		return
	}

	// The queue owns the one log line this action produces, and it says
	// whether the filter was set or cleared.
	s.orchestrator.SetPreferenceFilter(filter)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"filter": s.orchestrator.PreferenceFilter(),
	})
}

// rejectsCrossSite reports whether the request was forged by a third-party
// page — the CSRF case this route is uniquely exposed to, because its
// effect is global and lasting. A form submission is a *simple* request:
// the browser sends it cross-site without a CORS preflight, and the JSON
// decoder here would parse its body whatever the content type said, so
// content-type sniffing is not a defence. The check follows the
// fetch-metadata policy OWASP recommends for this case (see
// https://cheatsheetseries.owasp.org/cheatsheets/Cross_Site_Request_Forgery_Prevention_Cheat_Sheet.html#fetch-metadata-based-origin-validation):
// a browser-supplied Origin must name this server's own *origin* — scheme
// included, not just the host — and a request declaring
// Sec-Fetch-Site: cross-site is refused even if the Origin is withheld.
// Browsers always supply one of the two on a POST.
//
// The scheme matters because "site" in fetch-metadata terms is
// scheme-blind: a page served over plain HTTP under this server's own
// hostname is "same-site" to it (see
// https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Sec-Fetch-Site),
// so a host-only Origin match would let such a page forge the write
// through the Sec-Fetch-Site fallback. Binding the scheme closes that
// downgrade path.
// Non-browser clients (curl, service integrations, this server's own
// guests) send neither header and pass through: the route stays
// unauthenticated, as every route on this listener is.
func rejectsCrossSite(r *http.Request) bool {
	if origin := r.Header.Get("Origin"); origin != "" {
		u, err := url.Parse(origin)
		if err != nil {
			return true
		}
		return !strings.EqualFold(u.Host, r.Host) || !strings.EqualFold(u.Scheme, requestScheme(r))
	}
	return r.Header.Get("Sec-Fetch-Site") == "cross-site"
}

// requestScheme names the scheme the request arrived on: https when this
// server terminated TLS itself, when a TLS-terminating edge says so with
// X-Forwarded-Proto (first hop wins), or plain http otherwise. The server
// listens for TLS itself nowhere, so behind no edge that "otherwise" is the
// whole story; X-Forwarded-Proto only lets a fronting proxy state the
// client-facing scheme honestly. Forgery of that header by a non-browser
// client changes nothing — such a client can withhold Origin entirely and
// be judged by the Sec-Fetch-Site branch anyway.
func requestScheme(r *http.Request) string {
	if r.TLS != nil {
		return "https"
	}
	if proto := r.Header.Get("X-Forwarded-Proto"); proto != "" {
		if first, _, _ := strings.Cut(proto, ","); strings.EqualFold(strings.TrimSpace(first), "https") {
			return "https"
		}
	}
	return "http"
}

// HandleTaskSearch handles GET /api/tasks/search: a substring search over
// task output text, read through the date-partitioned log store so callers
// need not download whole traces (issue #269).
//
// Parameters: q (required, case-insensitive substring), status (optional,
// matched case-insensitively against the task's current status), and a
// lookback window — either days=N (the last N calendar days including
// today) or an explicit inclusive from=YYYY-MM-DD&to=YYYY-MM-DD pair.
// Neither window form nor days beyond the cap are accepted: see the
// constants above for the policy and the error bodies for the specifics.
//
// The response says how complete it is. "truncated" means the scan stopped
// early — at the store's match cap or its scanned-bytes budget — so newer
// hits in the window exist that are not listed (the walk goes oldest
// first). "partial" means a partition or trace could not be read (and was
// logged here) while the rest of the window was scanned normally. Like
// every other route on this server the endpoint is unauthenticated, and
// unlike /api/tasks/:id/download it searches every task's output at once,
// so it discloses the IDs, titles and statuses of whatever it matches;
// operators who would rather it did not must front it themselves.
func (s *Server) HandleTaskSearch(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if s.diskLogStore == nil {
		http.Error(w, "log store not configured", http.StatusServiceUnavailable)
		return
	}

	query := strings.TrimSpace(r.URL.Query().Get("q"))
	if query == "" {
		http.Error(w, "q parameter is required", http.StatusBadRequest)
		return
	}

	from, to, err := parseSearchWindow(r.URL.Query(), time.Now())
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	allDates, err := s.diskLogStore.ListDates()
	if err != nil {
		// Store errors wrap an *os.PathError naming the log directory, so
		// they stay server-side; echoing them would hand an anonymous
		// client the volume layout.
		s.log.Printf("task search list dates: %v", err)
		http.Error(w, "failed to read logs", http.StatusInternalServerError)
		return
	}

	// Restrict the scan to date directories inside the window; the store
	// then only ever opens files under those directories.
	window := make([]string, 0, len(allDates))
	for _, date := range allDates {
		d, err := time.Parse("2006-01-02", date)
		if err != nil {
			continue // not a date directory; ListDates would not have listed it
		}
		if !d.Before(from) && !d.After(to) {
			window = append(window, date)
		}
	}

	found := s.diskLogStore.SearchDates(window, query)
	if len(found.Errors) > 0 {
		// A partition the scan could not read cost its own matches, not the
		// search: say so where an operator will see it, once for the whole
		// scan rather than once per trace — the joined-failures stance
		// PruneOlderThan's caller takes — and let the response carry the
		// partial answer. Quote only the first maxSearchErrorsLogged
		// messages: the full join is unbounded in the all-traces-fail case,
		// and the count already states what the excerpt leaves out.
		quoted := found.Errors
		suffix := ""
		if len(quoted) > maxSearchErrorsLogged {
			suffix = fmt.Sprintf(" (and %d more)", len(quoted)-maxSearchErrorsLogged)
			quoted = quoted[:maxSearchErrorsLogged]
		}
		s.log.Printf("task search partial scan, window %s..%s: %d partition(s)/trace(s) unreadable: %s%s",
			from.Format("2006-01-02"), to.Format("2006-01-02"), len(found.Errors), strings.Join(quoted, "; "), suffix)
	}
	matches := found.Matches

	statusFilter := strings.TrimSpace(r.URL.Query().Get("status"))
	type decoratedMatch struct {
		logstore.Match
		Title  string `json:"title"`
		Status string `json:"status"`
	}
	results := make([]decoratedMatch, 0, len(matches))
	for _, m := range matches {
		task, ok := s.orchestrator.GetTask(m.TaskID)
		var title, status string
		if ok && task != nil {
			title, status = task.Title, task.Status.String()
		}
		// A status filter only matches tasks the queue still knows about
		// and currently holds that status; a pruned-from-memory task has
		// no status to satisfy the filter.
		if statusFilter != "" {
			if !ok || task == nil || !strings.EqualFold(status, statusFilter) {
				continue
			}
		}
		results = append(results, decoratedMatch{Match: m, Title: title, Status: status})
	}

	json.NewEncoder(w).Encode(map[string]interface{}{
		"query":     query,
		"from":      from.Format("2006-01-02"),
		"to":        to.Format("2006-01-02"),
		"count":     len(results),
		"truncated": found.Truncated,
		"partial":   len(found.Errors) > 0,
		"matches":   results,
	})
}

// parseSearchWindow resolves the lookback window to an inclusive date range,
// enforcing the cap server-side. It returns a descriptive error for any
// malformed, ambiguous or over-wide window, which the handler turns into a 400.
//
// The window is anchored at `now`'s own calendar date, exactly as
// LogStore.PruneOlderThan anchors its cutoff: the store names its date
// directories after the entry timestamp's date, which is host-local, so a
// UTC anchor would put the newest partition outside the window on any host
// east of UTC. The day is then built in UTC so the comparison against the
// parsed directory names stays a comparison of calendar days, not instants.
func parseSearchWindow(values url.Values, now time.Time) (from, to time.Time, err error) {
	y, mo, d := now.Date()
	today := time.Date(y, mo, d, 0, 0, 0, 0, time.UTC)

	daysStr := values.Get("days")
	fromStr, toStr := values.Get("from"), values.Get("to")

	switch {
	case daysStr != "" && (fromStr != "" || toStr != ""):
		return time.Time{}, time.Time{}, fmt.Errorf("specify either days or from/to, not both")

	case daysStr != "":
		n, err := strconv.Atoi(daysStr)
		if err != nil || n <= 0 {
			return time.Time{}, time.Time{}, fmt.Errorf("days must be a positive integer")
		}
		if n > maxSearchLookbackDays {
			return time.Time{}, time.Time{}, fmt.Errorf("lookback window of %d days exceeds the maximum of %d days", n, maxSearchLookbackDays)
		}
		return today.AddDate(0, 0, -(n - 1)), today, nil

	case fromStr != "" || toStr != "":
		if fromStr == "" || toStr == "" {
			return time.Time{}, time.Time{}, fmt.Errorf("both from and to are required")
		}
		from, err := time.Parse("2006-01-02", fromStr)
		if err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("from must be a YYYY-MM-DD date")
		}
		to, err := time.Parse("2006-01-02", toStr)
		if err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("to must be a YYYY-MM-DD date")
		}
		if from.After(to) {
			return time.Time{}, time.Time{}, fmt.Errorf("from must not be after to")
		}
		span := int(to.Sub(from).Hours()/24) + 1
		if span > maxSearchLookbackDays {
			return time.Time{}, time.Time{}, fmt.Errorf("lookback window of %d days exceeds the maximum of %d days", span, maxSearchLookbackDays)
		}
		return from, to, nil

	default:
		return today.AddDate(0, 0, -(defaultSearchLookbackDays - 1)), today, nil
	}
}
