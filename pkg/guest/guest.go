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
	"hotelier/pkg/logutil"
	"hotelier/pkg/persona"
	"hotelier/pkg/rpc"

	"github.com/google/uuid"
)

// LogEntry represents a log entry sent from a guest to the host.
// For tool call events, structured fields (ToolType, ToolName, etc.)
// carry the machine-readable data alongside the formatted Line string.
type LogEntry struct {
	TaskID       string    `json:"task_id"`
	Line         string    `json:"line"`
	Level        string    `json:"level,omitempty"`
	Timestamp    time.Time `json:"timestamp,omitempty"`
	ToolType     string    `json:"tool_type,omitempty"`      // "start", "output", "end"
	ToolName     string    `json:"tool_name,omitempty"`      // e.g. "bash", "read"
	ToolID       string    `json:"tool_id,omitempty"`        // unique tool call identifier
	ToolArgs     string    `json:"tool_args,omitempty"`      // arguments/parameters
	ToolArgsJSON string    `json:"tool_args_json,omitempty"` // raw argument JSON (for UI diff rendering, issue #2)
	ToolOutput   string    `json:"tool_output,omitempty"`    // captured output
	ToolError    bool      `json:"tool_error,omitempty"`     // true if tool ended with error
	// Compaction fields (level "compaction"): context compaction events
	// emitted by pi (compaction_start / compaction_end).
	CompactionType         string `json:"compaction_type,omitempty"`          // "start", "end"
	CompactionReason       string `json:"compaction_reason,omitempty"`        // "manual", "threshold", "overflow"
	CompactionSummary      string `json:"compaction_summary,omitempty"`       // generated summary (end)
	CompactionTokensBefore int    `json:"compaction_tokens_before,omitempty"` // context size before (end)
	CompactionTokensAfter  int    `json:"compaction_tokens_after,omitempty"`  // estimated size after (end)
	CompactionError        bool   `json:"compaction_error,omitempty"`         // true if compaction failed
}

// ExitDiagnostics contains diagnostic information captured when the pi
// subprocess exits. This is used for troubleshooting failed or abnormal
// task completions (e.g. 429 errors, process crashes).
type ExitDiagnostics struct {
	// ExitCode is the exit code of the pi subprocess (-1 if unavailable).
	ExitCode int `json:"exit_code,omitempty"`
	// ExitError is the error string from the subprocess exit, if any.
	ExitError string `json:"exit_error,omitempty"`
	// StderrLines are the last N lines of stderr output from the subprocess.
	StderrLines []string `json:"stderr_lines,omitempty"`
	// LastEventTypes are the types of the last N events received from pi.
	// This helps identify where the process was when it exited.
	LastEventTypes []string `json:"last_event_types,omitempty"`
	// SettledReceived indicates whether pi sent a settle event (agent_settled,
	// or the legacy guest_end) before exiting. False indicates an abnormal
	// exit (crash, kill, etc.). Note: agent_end does not count as settled —
	// pi emits it before an automatic retry. See issue #161.
	SettledReceived bool `json:"settled_received,omitempty"`
}

// TaskResult represents the result of a task execution.
type TaskResult struct {
	TaskID  string `json:"task_id"`
	Success bool   `json:"success"`
	Output  string `json:"output,omitempty"`
	Error   string `json:"error,omitempty"`
	// Diagnostics contains exit-time diagnostic information.
	// Populated for all task completions to aid troubleshooting.
	Diagnostics *ExitDiagnostics `json:"diagnostics,omitempty"`
}

// TaskAssignment represents a task assigned to a guest.
type TaskAssignment struct {
	TaskID  string           `json:"id"`
	Prompt  string           `json:"prompt"`
	Tags    []string         `json:"tags"`
	RepoRef string           `json:"repo_ref,omitempty"` // git repository URL to clone (optional)
	Persona *persona.Persona `json:"persona,omitempty"`  // persona data to apply (optional)
}

// TaskCancel represents a task cancellation notification.
type TaskCancel struct {
	TaskID string `json:"task_id"`
	Reason string `json:"reason,omitempty"`
	// Intent distinguishes why the host is revoking: "cancel" ends the task
	// (the user cancelled it, or it was failed), "revoke" only takes the
	// claim back because the host is re-queuing the task elsewhere. The
	// guest echoes it in its guest.cancelled confirmation so the host does
	// not treat a revocation as a request to cancel a task that is already
	// back in the queue. See issue #252.
	Intent string `json:"intent,omitempty"`
}

// cancelIntentRevoke is the task.cancel intent that takes the claim back
// without deciding the task's fate: the host re-queues the task elsewhere.
// The other intent, cancel, ends the task and is what task.cancel meant
// before the field existed (issue #252).
const cancelIntentRevoke = "revoke"

// What a guest did with a task.cancel it received, reported back to the host
// in the guest.cancelled confirmation. A cancel is resolved against the task
// it names — never against "whatever is running" (issue #252).
const (
	// cancelActionRunning: the named task was executing and its context was
	// aborted.
	cancelActionRunning = "cancelled_while_running"
	// cancelActionBeforeStart: the guest held the task but had not started
	// it; the claim is dropped and the task will never run here.
	cancelActionBeforeStart = "cancelled_before_start"
	// cancelActionNotHeld: the guest does not hold the named task, so the
	// cancel changed nothing. It is a no-op, not an error, so the host may
	// retry the notification safely.
	cancelActionNotHeld = "not_held"
)

// claimCounters are the guest's own counters for claims it gave up without
// executing them. They are logged rather than reported: the host counts the
// same events on its side, where they can be read without a live guest.
type claimCounters struct {
	AckRefusals int
}

// errClaimRevoked and errClaimRefused report a task the dispatcher must not
// run and must not decline either: the host has already taken the claim back
// (revoked) or refused it (a rejected task.acknowledge). Declining would hand
// the host a task it has already dealt with. See issue #252.
var (
	errClaimRevoked = errors.New("claim revoked before start")
	errClaimRefused = errors.New("claim refused by host")
)

// LogCallback is a function that sends a log entry to the host.
// For tool call events, the entry should include structured fields
// (ToolType, ToolName, etc.) alongside the formatted Line string.
type LogCallback func(entry LogEntry) error

// Handler is a function that handles task execution.
type Handler func(ctx context.Context, task TaskAssignment, sendLog LogCallback) (*TaskResult, error)

// Guest is the client-side guest that connects to the Check-In Host.
type Guest struct {
	id      string
	name    string
	host    string
	tags    []string
	config  config.GuestConfig
	client  *rpc.Client
	hub     *rpc.ClientHub
	log     *log.Logger
	handler Handler
	mu      sync.Mutex
	running bool
	cancel  context.CancelFunc // cancels the current task's context
	// claims holds the task IDs this guest has accepted and not yet started
	// or abandoned. The buffer on taskCh is invisible to a cancel, so the
	// set is what lets a task.cancel decide whether the guest holds the task
	// it names (issue #252).
	claims map[string]struct{}
	// revoked marks claims the host took back before they started. The
	// dispatcher drops the assignment when it reaches it instead of running
	// work the host has already closed.
	revoked map[string]struct{}
	// givenUp marks running tasks the host revoked mid-flight (a revoke-
	// intent task.cancel naming the in-flight task). The abort makes the
	// handler return an error, but that error is not the task's fate: the
	// host re-queued the task elsewhere and ExecuteTask must not report a
	// result for it, or the guest the host took the claim back from would
	// decide the fate after all (issue #252 review). Consumed by
	// claimRevokedWhileRunning when the aborted handler unwinds.
	givenUp  map[string]struct{}
	counters claimCounters
	stopCh   chan struct{}
	// connMu guards connLost alone. It is deliberately not g.mu: g.mu is
	// held across blocking RPC calls (see ExecuteTask), and the
	// connection-lost signal must never queue behind an in-flight call.
	// connMu is a leaf lock — never take g.mu or a Client lock while
	// holding it. See issue #218.
	connMu        sync.Mutex
	connLost      chan struct{} // closed when the connection drops (guarded by connMu)
	callResp      chan *rpc.JSONRPCMessage
	taskCh        chan TaskAssignment // incoming task assignments
	currentTaskID string              // task currently being worked on (for task-aware heartbeat)
	// registeredOnce reports whether this process has completed a
	// guest.register round-trip. It is the answer to "is this a reconnect?"
	// and travels with the registration so the server does not have to infer
	// it from the id alone — with a configured id (issue #253) an id the
	// registry already knows can equally be a brand-new process.
	// Guarded by mu.
	registeredOnce bool

	// heartbeatForTest allows tests to stub the Heartbeat method.
	// When nil, heartbeatLoop calls g.Heartbeat() directly.
	heartbeatForTest func() error
}

// New creates a new Guest instance.
func New(cfg config.GuestConfig, handler Handler) *Guest {
	// The configured id wins: it is the guest's identity in the registry, and
	// a deployment that templates it (typically from the hostname) expects a
	// process restart to keep the same identity (issue #253).
	id := cfg.ID
	if id == "" {
		// No configured identity: fall back to an ephemeral one so a
		// crash-reconnect cannot be mistaken for the registration the
		// previous process left behind.
		id = "guest-" + uuid.New().String()[:8]
	}

	logPrefix := fmt.Sprintf("[guest:%s]", id)
	logger := log.New(os.Stdout, logPrefix+" ", log.LstdFlags)

	hub := rpc.NewClientHub(func(format string, args ...interface{}) {
		logger.Printf(format, args...)
	})

	g := &Guest{
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
		claims:   make(map[string]struct{}),
		revoked:  make(map[string]struct{}),
		givenUp:  make(map[string]struct{}),
		// The machine this guest runs on, reported at registration so the
		// dashboard can say where a task is executing (issue #238). A failure
		// here just leaves the host unknown, which the UI renders as no host.
		host: hostOrEmpty(),
	}

	// Register the notification handlers at construction time, before any
	// connection exists. The server pushes task.assign while handling
	// guest.register (before the response is written), and the read loop
	// dispatches notifications as soon as they are read — so the handlers
	// must already be in place by the time the first message arrives.
	// Registering them in Register() (after the guest.register round-trip)
	// meant the assignment was deterministically dropped with
	// "no handler registered". See issue #168.
	g.registerNotificationHandlers()

	return g
}

// hostOrEmpty returns the machine's hostname, or an empty string when the
// kernel will not say (issue #238 treats an unknown host as "no host" rather
// than a fatal error — a guest with no hostname still runs tasks fine).
func hostOrEmpty() string {
	host, err := os.Hostname()
	if err != nil {
		return ""
	}
	return host
}

// registerNotificationHandlers registers the handlers for server-initiated
// notifications (task.assign, task.cancel). They are registered in New() so
// that no notification can arrive before a handler exists, on first connect
// or on reconnect alike.
func (g *Guest) registerNotificationHandlers() {
	// Handler for server-initiated task.assign notifications
	g.hub.RegisterNotificationHandler("task.assign", func(method string, params json.RawMessage) {
		g.log.Printf("[RPC] received notification: %s", method)
		var task TaskAssignment
		if err := json.Unmarshal(params, &task); err != nil {
			g.log.Printf("[RPC] failed to parse task.assign params: %v", err)
			return
		}

		// Record the claim before the dispatcher can pick it up: a cancel
		// that arrives while the assignment sits in the buffer must find it
		// (issue #252). A fresh assignment also clears any earlier
		// revocation of the same task, so a re-queue back to this guest
		// runs instead of being dropped on a stale marker. The record must
		// exist before the channel send below, because the send is the
		// moment the dispatcher can take the assignment: recording
		// afterwards left a window in which a cancel answered not_held for
		// a task the guest did hold.
		g.mu.Lock()
		_, alreadyHeld := g.claims[task.TaskID]
		if g.currentTaskID == task.TaskID || alreadyHeld {
			// Deduplicate: the guest already holds this exact task, running
			// or queued. This can happen when the guest reconnects and the
			// server re-sends an assignment it already has. The claims map is
			// the test for the queued half: keying only on currentTaskID
			// buffered a re-sent assignment twice, and the dispatcher's
			// "already running" decline for the second copy handed back work
			// that was in flight (#252 review).
			delete(g.revoked, task.TaskID)
			delete(g.givenUp, task.TaskID)
			g.mu.Unlock()
			g.log.Printf("[RPC] ignoring duplicate task.assign for %s (already held)", task.TaskID)
			return
		}
		g.claims[task.TaskID] = struct{}{}
		delete(g.revoked, task.TaskID)
		delete(g.givenUp, task.TaskID)
		g.mu.Unlock()

		g.log.Printf("[RPC] dispatching task %s to execution", task.TaskID)
		select {
		case g.taskCh <- task:
			g.log.Printf("[RPC] task %s queued on guest for execution", task.TaskID)
		default:
			// The queue is full, so the task is not held here after all:
			// undo the claim record, including any revocation a cancel may
			// have attached to it inside the send window, so neither map
			// keeps a task this guest never took.
			g.mu.Lock()
			delete(g.claims, task.TaskID)
			delete(g.revoked, task.TaskID)
			g.mu.Unlock()
			g.log.Printf("[RPC] task queue full, dropping task %s", task.TaskID)
		}
	})

	// Handler for task.cancel notifications
	g.hub.RegisterNotificationHandler("task.cancel", func(method string, params json.RawMessage) {
		g.log.Printf("[RPC] received notification: %s", method)
		var cancel TaskCancel
		if err := json.Unmarshal(params, &cancel); err != nil {
			g.log.Printf("[RPC] failed to parse task.cancel params: %v", err)
			return
		}

		action := g.revokeClaim(cancel.TaskID, cancel.Reason, cancel.Intent)

		// Confirm to the server, naming the task the guest acted on and what
		// it did with it. The guest is the authority on whether the task was
		// actually stopped; the host decides what the task's fate is, guided
		// by the intent it sent (issue #252). The client may be nil while no
		// connection exists (the handlers now live from construction time);
		// skip the confirmation rather than panic. The accessor makes the
		// check-then-use atomic with the reconnect loop clearing the client
		// (issue #72).
		if c := g.rpcClient(); c != nil {
			_, _ = c.Call("guest.cancelled", map[string]interface{}{
				"task_id":  cancel.TaskID,
				"guest_id": g.id,
				"reason":   cancel.Reason,
				"action":   action,
				"intent":   cancel.Intent,
			})
		} else {
			g.log.Printf("[RPC] no connection, skipping guest.cancelled confirmation for %s", cancel.TaskID)
		}
	})
}

// revokeClaim resolves a task.cancel against the task it names: the running
// task is aborted, a claim held but not started is dropped so it never runs,
// and a task the guest does not hold changes nothing (issue #252).
//
// Cancelling "whatever is running" was the original defect: a cancel for task
// X tore down whichever task happened to be in flight, and the confirmation
// then told the host about a task the guest had never seen.
//
// A revoke of the running task is also recorded in givenUp: the host has
// taken the claim back and re-queued the task elsewhere, so the aborted
// handler's error is not the task's fate and ExecuteTask reports no result
// for it, exactly as the drop paths report nothing for a revoked queue entry.
// A cancel-intent abort keeps reporting its result: there the task's fate is
// what the host's cancel asks for, and the result path is unchanged.
func (g *Guest) revokeClaim(taskID, reason, intent string) string {
	g.mu.Lock()
	switch {
	case taskID != "" && g.currentTaskID == taskID:
		// The task this cancel names is the one in flight. Its context may
		// already be gone (it is winding down), which is why the abort is
		// conditional and the action is not.
		if g.cancel != nil {
			g.cancel()
			g.cancel = nil
		}
		delete(g.claims, taskID)
		if intent == cancelIntentRevoke {
			g.givenUp[taskID] = struct{}{}
		}
		g.mu.Unlock()
		g.log.Printf("[CANCEL] task %s cancelled while running: %s", taskID, reason)
		return cancelActionRunning
	default:
		if _, held := g.claims[taskID]; !held {
			g.mu.Unlock()
			g.log.Printf("[CANCEL] task %s not running and not held here; ignoring cancel (does not hold the claim)", taskID)
			return cancelActionNotHeld
		}
		delete(g.claims, taskID)
		g.revoked[taskID] = struct{}{}
		g.mu.Unlock()
		g.log.Printf("[CANCEL] task %s cancelled before start (will not run): %s", taskID, reason)
		return cancelActionBeforeStart
	}
}

// claimRevokedWhileRunning reports whether the named task's claim was taken
// back while it was in flight, consuming the record. The aborted handler
// returns an error like any other failure, but a revocation is not a task
// failure: the host re-queued the task and its own acknowledgement owns the
// fate, so ExecuteTask must not send a result for it (issue #252 review).
func (g *Guest) claimRevokedWhileRunning(taskID string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if _, givenUp := g.givenUp[taskID]; !givenUp {
		return false
	}
	delete(g.givenUp, taskID)
	return true
}

// Connect connects the guest to the Check-In Host.
func (g *Guest) Connect() error {
	client := rpc.NewClient(g.id, g.hub, g.log.Printf)

	// Wire the connection-lost callback *before* connecting. Connect
	// spawns the client's read loop, which reports a closed socket through
	// this callback; registering it afterwards lets a connection that the
	// server closes immediately (restart, proxy hiccup, early
	// guest.register failure) be seen as lost before the callback exists —
	// the signal was then silently dropped and Start() waited on a
	// connLost nothing would ever close. resetConn hands *this* attempt a
	// channel of its own and the callback is bound to it, so a late report
	// from a previous client can never be mistaken for the loss of this
	// connection — on a successful reconnect or after a failed attempt.
	// The channel check runs under connMu — not g.mu, which ExecuteTask
	// holds across blocking RPC calls — so a dying socket is reported
	// immediately even while a Call is wedged. See issue #218.
	client.SetOnClose(g.connLostCallback(g.resetConn()))

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
	g.setRPCClient(client)
	g.log.Printf("connected to host")
	return nil
}

// rpcClient returns the current RPC client, or nil if the guest is not
// connected (e.g. while the Start loop is reconnecting). The read is
// mutex-protected so concurrent callers never race with the reconnect
// loop clearing the client (issue #72).
func (g *Guest) rpcClient() *rpc.Client {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.client
}

// setRPCClient stores the current RPC client. Pass nil to mark the guest
// as disconnected.
func (g *Guest) setRPCClient(c *rpc.Client) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.client = c
}

// connLostCallback builds the onClose callback Connect hands to an RPC
// client. The callback is bound to the connLost channel that was current
// when the client was created: if the guest has since moved on to a new
// channel (resetConn), a late report from this client is a no-op rather
// than a false "connection lost" for the fresh connection. The check runs
// under connMu — never g.mu, which ExecuteTask holds across blocking RPC
// calls — so a dying socket is always reported promptly. See issue #218.
func (g *Guest) connLostCallback(lostCh chan struct{}) func() {
	return func() {
		g.signalConnLostIfCurrent(lostCh)
	}
}

// setConnLost signals that the connection has been lost.
// This is called by the readLoop when the WebSocket closes unexpectedly,
// and by the heartbeat loop when the server reports the guest unknown.
// The check and the close happen under connMu, serialised against
// resetConn: two concurrent callers can no longer both pass the "not yet
// closed" check and double-close the channel, which panicked the whole
// guest process (issue #218).
func (g *Guest) setConnLost() {
	g.connMu.Lock()
	defer g.connMu.Unlock()
	signalChanLocked(g.connLost)
}

// signalConnLostIfCurrent closes ch if and only if ch is still the
// guest's current connLost channel. Callers that snapshot the channel for
// one connection (a heartbeat loop, an onClose callback) must escalate
// through this helper rather than setConnLost: if the guest has since run
// resetConn for a new connection, the signal belongs to a replaced
// connection and declaring the fresh one lost would drop a healthy
// socket. See issue #218 (callback binding) and #230 (heartbeat
// escalation).
func (g *Guest) signalConnLostIfCurrent(ch chan struct{}) {
	g.connMu.Lock()
	defer g.connMu.Unlock()
	if g.connLost == ch {
		signalChanLocked(ch)
	}
}

// connLostSignal returns the channel that signals the loss of the current
// connection. Callers must not assume it stays valid across a reconnect:
// snapshot it under connMu (i.e. through this helper) rather than reading
// the field in a select, which would race with resetConn.
func (g *Guest) connLostSignal() chan struct{} {
	g.connMu.Lock()
	defer g.connMu.Unlock()
	return g.connLost
}

// signalChanLocked closes ch unless it is already closed. The caller must
// hold connMu.
func signalChanLocked(ch chan struct{}) {
	select {
	case <-ch:
		// already lost
	default:
		close(ch)
	}
}

// resetConn gives the next connection attempt a connLost channel of its
// own and returns it, so a later disconnection can be detected and so a
// late onClose from a previous client cannot close the channel this
// attempt is waiting on. Every Connect runs it — whether the attempt ends
// up succeeding or not — which is what makes the channel-identity guard in
// connLostCallback hold on every path, including an attempt that failed
// after its own channel had already been closed (issue #218).
func (g *Guest) resetConn() chan struct{} {
	g.connMu.Lock()
	defer g.connMu.Unlock()

	g.connLost = make(chan struct{})
	return g.connLost
}

// ID returns the guest's identity as it is reported to the registry. It is
// fixed for the lifetime of the process: cfg.ID when configured, otherwise a
// generated one (issue #253). Reload never changes it — renaming a running
// guest would orphan whatever the registry has assigned to it.
func (g *Guest) ID() string {
	return g.id
}

// holdsClaim reports whether the guest holds an assignment for taskID that it
// has accepted but not started — the state a task.cancel has to be able to
// reach (issue #252).
func (g *Guest) holdsClaim(taskID string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	_, ok := g.claims[taskID]
	return ok
}

// ackRefusals is how many times the host refused this guest's
// task.acknowledge — the claims the guest dropped rather than executed
// (issue #252).
func (g *Guest) ackRefusals() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.counters.AckRefusals
}

// hasRegistered reports whether this process has registered before.
func (g *Guest) hasRegistered() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.registeredOnce
}

// markRegistered records that a registration round-trip completed, so the
// next one is reported as a reconnect rather than a fresh start.
func (g *Guest) markRegistered() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.registeredOnce = true
}

// registerParams builds the guest.register payload.
func (g *Guest) registerParams() map[string]interface{} {
	return map[string]interface{}{
		"id":   g.id,
		"name": g.name,
		"tags": g.tags,
		"host": g.host,
		// `resumed` distinguishes a reconnection (the same process after
		// a lost connection) from a fresh process reusing an identity the
		// registry still holds. Only the process knows which it is, and
		// the server must not reconcile a fresh one against a task it was
		// never given. Issue #253.
		"resumed": g.hasRegistered(),
	}
}

// Register registers the guest with the Check-In Host. The notification
// handlers (task.assign, task.cancel) are already registered in New() — see
// registerNotificationHandlers — so that a task.assign pushed while this
// call is in flight is not dropped. See issue #168.
func (g *Guest) Register() error {
	params := g.registerParams()

	c := g.rpcClient()
	if c == nil {
		return fmt.Errorf("register: not connected")
	}

	_, err := c.Call("guest.register", params)
	if err != nil {
		return fmt.Errorf("register: %w", err)
	}

	// Only a completed round-trip counts: a registration that died in
	// transport leaves the next one reported as fresh, which is the safe
	// direction — the server then drops rather than resurrects a task
	// reference.
	g.markRegistered()

	g.log.Printf("registered with tags: %v", g.tags)
	return nil
}

// taskDispatcher runs tasks sequentially as they arrive.
//
// The context is cancelled when the connection is lost (or the guest stops),
// which exits the dispatcher. Without this, each reconnection in Start()
// spawned a new dispatcher while the old one lingered on taskCh; an idle
// duplicate dispatcher would pick up a task while the guest was still busy
// with a task from the previous connection and decline it, driving the
// server into an assign→decline→reassign loop (issue #186).
func (g *Guest) taskDispatcher(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			g.dropBufferedClaims("connection lost")
			return
		case <-g.stopCh:
			g.dropBufferedClaims("guest stopping")
			return
		case task := <-g.taskCh:
			g.log.Printf("[DISPATCH] starting task %s", task.TaskID)
			// Wait for the guest to become idle before executing. The server
			// can assign a task to a guest it believes is idle while the
			// guest is still running a task from a previous connection (the
			// registry marks the guest IDLE when its connection drops).
			// Declining that assignment used to send the task back to the
			// server, which reassigned it to another busy guest that declined
			// it again — an infinite loop (issue #186). Running the task
			// after the current one completes is safe: the server's liveness
			// probe keys off the guest's heartbeat, not the exact task ID.
			if !g.waitForIdle(ctx) {
				// Exiting with this task in hand and possibly more queued:
				// prune their claim records so they do not outlive the
				// dispatcher (issue #252 review).
				g.dropBufferedClaims("dispatcher exiting", task.TaskID)
				return
			}
			result, err := g.ExecuteTask(task)
			if errors.Is(err, errClaimRevoked) || errors.Is(err, errClaimRefused) {
				// The host took the claim back, or refused it, while the task
				// waited. ExecuteTask logged which; do not decline — the host
				// has already dealt with the task (issue #252).
				continue
			}
			if err != nil {
				g.log.Printf("[DISPATCH] task %s error: %v", task.TaskID, err)
				// Defensive fallback: the guest became busy between the idle
				// check and ExecuteTask. Decline so the server can re-queue
				// the task. Only reachable if two dispatchers transiently
				// overlap during a reconnection; the decline is one-shot and
				// cannot re-form the #186 loop.
				if strings.Contains(err.Error(), "already running a task") {
					g.DeclineTask(task.TaskID, err.Error())
				}
			} else {
				g.log.Printf("[DISPATCH] task %s finished: success=%v output=%s", task.TaskID, result.Success, logutil.Content(result.Output))
			}
		}
	}
}

// waitForIdle blocks until the guest is no longer running a task. It
// returns false if the context is cancelled (connection lost) or the guest
// is stopping; the dispatcher then exits and the server re-queues any
// assigned task when the connection drops.
func (g *Guest) waitForIdle(ctx context.Context) bool {
	for {
		g.mu.Lock()
		running := g.running
		g.mu.Unlock()
		if !running {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-g.stopCh:
			return false
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// dropBufferedClaims prunes the claim bookkeeping for the named tasks and
// for assignments still sitting in the queue when the dispatcher exits, and
// removes the queued ones from the queue with it. The host re-queues a
// disconnected guest's tasks when the connection drops (and revokes their
// claims), so a buffered assignment is a stale copy of a task the host has
// already dealt with; leaving its claim recorded would tell a later cancel
// that this guest still has something to drop, and leaving it queued hands
// it to the next dispatcher — which is how revoked claims outlived their
// assignments in issue #252.
func (g *Guest) dropBufferedClaims(why string, ids ...string) {
	g.mu.Lock()
	for _, id := range ids {
		delete(g.claims, id)
		delete(g.revoked, id)
		delete(g.givenUp, id)
	}
	g.mu.Unlock()
	for {
		select {
		case task := <-g.taskCh:
			g.mu.Lock()
			delete(g.claims, task.TaskID)
			delete(g.revoked, task.TaskID)
			delete(g.givenUp, task.TaskID)
			g.mu.Unlock()
			g.log.Printf("[DISPATCH] dropped queued task %s (%s)", task.TaskID, why)
		default:
			return
		}
	}
}

// Unregister unregisters the guest from the Check-In Host.
func (g *Guest) Unregister() error {
	params := map[string]string{
		"id": g.id,
	}

	c := g.rpcClient()
	if c == nil {
		return fmt.Errorf("unregister: not connected")
	}

	_, err := c.Call("guest.unregister", params)
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
//
// Errors keep their type through the %w wraps so heartbeatLoop can tell a
// server-side RPCError (connection fine — log only) from a transport
// failure (connection gone — reconnect), as issue #219 requires. The
// retry loop here also retries application errors, which the loop-level
// escalation makes redundant but harmless: it only stretches the
// worst-case escalation time (3 call timeouts + 7s of backoff), and a
// transient blip on an otherwise healthy connection is worth retrying
// in place before the heavier reconnect path runs.
func (g *Guest) Heartbeat() error {
	g.mu.Lock()
	taskID := g.currentTaskID
	g.mu.Unlock()

	params := map[string]string{
		"id":      g.id,
		"task_id": taskID,
	}

	c := g.rpcClient()
	if c == nil {
		return fmt.Errorf("heartbeat: not connected")
	}

	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		_, err := c.Call("guest.heartbeat", params)
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

	c := g.rpcClient()
	if c == nil {
		// The guest is reconnecting; the log line is dropped rather than
		// crashing the process (issue #72).
		return fmt.Errorf("send log: not connected")
	}

	err := c.SendNotification("guest.log", entry)
	if err != nil {
		return fmt.Errorf("send log: %w", err)
	}
	return nil
}

// SendResult submits the final result of a task to the Check-In Host.
// Retries with exponential backoff (3 attempts) on failure.
func (g *Guest) SendResult(result TaskResult) error {
	c := g.rpcClient()
	if c == nil {
		return fmt.Errorf("send result: not connected")
	}

	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		err := c.SendNotification("guest.result", result)
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

	c := g.rpcClient()
	if c == nil {
		g.log.Printf("[DISPATCH] cannot decline task %s: not connected", taskID)
		return
	}

	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		_, err := c.Call("guest.task_declined", params)
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
		// Each attempt — successful or not — is bound to a connLost channel
		// of its own by Connect's resetConn, so a stale client winding down
		// can never close the channel a live connection is waiting on.
		// See issue #218.
		if err := g.connectAndRegister(); err != nil {
			g.log.Printf("connection failed: %v, retrying in 5s...", err)
			select {
			case <-time.After(5 * time.Second):
			case <-g.stopCh:
				return nil
			}
			continue
		}

		// Per-connection context: cancelled when the connection is lost so
		// the goroutines below exit instead of lingering and competing with
		// their replacements after reconnection.
		connCtx, cancelConn := context.WithCancel(context.Background())

		// Start heartbeat goroutine
		go g.heartbeatLoop(connCtx)

		// Start task dispatcher — handles incoming task.assign notifications
		go g.taskDispatcher(connCtx)

		// Wait for shutdown or connection loss. The channel is snapshotted
		// under connMu (as heartbeatLoop does) rather than read bare in the
		// select: it is the same channel Connect bound the onClose callback
		// to. See issue #218.
		lost := g.connLostSignal()
		g.log.Printf("guest ready, waiting for tasks...")
		select {
		case <-g.stopCh:
			cancelConn()
			g.log.Printf("guest shutting down")
			return nil
		case <-lost:
			g.log.Printf("connection lost, reconnecting...")
			// Cancel the connection context so the old goroutines exit.
			cancelConn()
			// Clean up the dead connection
			if c := g.rpcClient(); c != nil {
				c.Close()
				g.setRPCClient(nil)
			}
		}
	}
}

// connectAndRegister connects to the host and registers the guest.
// Returns nil on success, or an error if the connection fails.
func (g *Guest) connectAndRegister() error {
	if err := g.Connect(); err != nil {
		return err
	}

	if err := g.Register(); err != nil {
		if c := g.rpcClient(); c != nil {
			c.Close()
		}
		g.setRPCClient(nil)
		return err
	}

	return nil
}

// isStopping reports whether Stop has been called. Used to suppress
// failure reporting for in-flight tasks during shutdown: the failure is
// an artefact of the guest going away (the pi subprocess is killed before
// it can emit agent_settled), not an agent fault, and the server re-queues
// the task when the connection drops. See issue #184.
func (g *Guest) isStopping() bool {
	select {
	case <-g.stopCh:
		return true
	default:
		return false
	}
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

// isApplicationError reports whether err is (or wraps) a JSON-RPC error
// response from the server. Such an error proves the connection carried
// the request and the answer, so it must not be treated as a lost
// connection — unlike transport errors (failed writes, closed sockets,
// Call timeouts), which arrive as plain errors. rpc.Call returns
// *rpc.RPCError only for a JSON-RPC error response, and Heartbeat wraps
// with %w, so errors.As across the chain discriminates reliably.
func isApplicationError(err error) bool {
	if err == nil {
		return false
	}
	var rpcErr *rpc.RPCError
	return errors.As(err, &rpcErr)
}

func (g *Guest) heartbeatLoop(ctx context.Context) {
	interval := g.config.HeartbeatInterval
	if interval == 0 {
		interval = 30 // default 30 seconds
	}

	ticker := time.NewTicker(time.Duration(interval) * time.Second)
	defer ticker.Stop()

	for {
		// Snapshot the connLost channel under connMu: the Start loop
		// replaces the field in resetConn between reconnections, and an
		// unsynchronised read here (as part of the select below) would
		// race with that write. See issue #218.
		lost := g.connLostSignal()

		select {
		case <-ctx.Done():
			return
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
				if isApplicationError(err) {
					// The server answered the heartbeat with an
					// ordinary JSON-RPC error. The transport
					// demonstrably works, so tearing the
					// connection down would be wrong: log and
					// keep ticking (issue #219).
					g.log.Printf("heartbeat rejected by server: %v", err)
					continue
				}
				// Everything else — a dead socket, a nil client,
				// or exhausted Call timeouts — is a statement
				// about the transport. A guest whose heartbeats
				// never reach the server is, to the orchestrator,
				// indistinguishable from a dead one; logging and
				// retrying forever on the same dead connection
				// wedged guests with their tasks stuck ASSIGNED
				// (issues #230, #219). Escalate to the reconnect
				// loop Start() owns, signalling only *this*
				// loop's connection so a stale failure cannot
				// drop a fresh socket (#218 discipline).
				g.log.Printf("heartbeat failed, triggering reconnect: %v", err)
				g.signalConnLostIfCurrent(lost)
				return
			}
		case <-g.stopCh:
			return
		case <-lost:
			// Connection lost — the Start loop will handle reconnection.
			return
		}
	}
}

// ExecuteTask executes a task assigned to this guest.
func (g *Guest) ExecuteTask(task TaskAssignment) (*TaskResult, error) {
	g.mu.Lock()
	if g.running {
		// The decline that follows this error hands the task back to the
		// host, so this guest holds nothing: drop the claim record with it,
		// or a later cancel would answer cancelled_before_start for a task
		// that was never in the queue.
		delete(g.claims, task.TaskID)
		delete(g.revoked, task.TaskID)
		g.mu.Unlock()
		return nil, fmt.Errorf("guest is already running a task")
	}

	// A claim the host revoked while this task waited in the queue is
	// dropped here, never started: the host has already re-queued the task
	// and any work done now would be duplicated side effects (issue #252).
	if _, revoked := g.revoked[task.TaskID]; revoked {
		delete(g.revoked, task.TaskID)
		delete(g.claims, task.TaskID)
		g.mu.Unlock()
		g.log.Printf("[DISPATCH] claim for task %s was revoked before start; dropping it", task.TaskID)
		return nil, errClaimRevoked
	}

	// Signal the server that we're about to execute this task so the
	// orchestrator's registry reflects the true state (RUNNING). This
	// prevents the server from reassigning the task to this guest if
	// ExecuteTask fails and the guest declines — FindAvailableGuests
	// will not return a guest whose registry state is RUNNING.
	// g.mu is already held, so read the field directly instead of via
	// g.rpcClient() (which would deadlock). The client may be nil if the
	// connection was lost while a task was in flight (issue #72).
	if c := g.client; c != nil {
		_, err := c.Call("task.acknowledge", map[string]interface{}{
			"task_id":  task.TaskID,
			"guest_id": g.id,
		})
		if err != nil {
			// A refusal is the host saying "this is not yours": the task has
			// completed, failed or gone to another guest. Running it anyway
			// — the old "continue anyway" behaviour — is what produced
			// comments, commits and pull requests raised twice (issue #252).
			// Any other failure says nothing about the claim: the transport
			// case is already covered by the retry and reconnect work, so
			// carry on and let the heartbeat inform the server.
			if rpc.IsClaimRefused(err) {
				g.counters.AckRefusals++
				delete(g.claims, task.TaskID)
				delete(g.revoked, task.TaskID)
				g.mu.Unlock()
				g.log.Printf("[DISPATCH] claim for task %s not honoured: %v — not executing", task.TaskID, err)
				return nil, errClaimRefused
			}
			g.log.Printf("[DISPATCH] failed to acknowledge task %s: %v",
				task.TaskID, err)
			// Continue anyway — the server may already know via heartbeat.
		}
	} else {
		g.log.Printf("[DISPATCH] no connection, skipping task.acknowledge for %s", task.TaskID)
	}

	delete(g.claims, task.TaskID)
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
	// The journal is not a session log: log the prompt's size, not its text (issue #241).
	g.log.Printf("[TASK] prompt: %s", logutil.Content(task.Prompt))
	g.log.Printf("[TASK] tags: %v", task.Tags)

	// Send log that task started
	if err := g.SendLog(LogEntry{TaskID: task.TaskID, Line: "Task started", Level: "system"}); err != nil {
		g.log.Printf("failed to send task start log: %v", err)
	}

	// Execute the task using the handler
	result, err := g.handler(ctx, task, g.SendLog)
	// Consume the revocation record before the deferred cleanup below runs:
	// a revoke that aborted this task takes its fate with it, and neither
	// outcome below may report a result for it (issue #252 review).
	revokedWhileRunning := g.claimRevokedWhileRunning(task.TaskID)
	if err != nil {
		g.log.Printf("task %s failed: %v", task.TaskID, err)
		if g.isStopping() {
			// Shutdown artefact — do not report the failure. The server
			// re-queues the task when the connection drops (see issue #184).
			g.log.Printf("[TASK] task %s interrupted by guest shutdown; not reporting failure", task.TaskID)
			if sendErr := g.SendLog(LogEntry{TaskID: task.TaskID, Line: "Task interrupted by guest shutdown; will be re-queued", Level: "system"}); sendErr != nil {
				g.log.Printf("failed to send interruption log: %v", sendErr)
			}
			return nil, err
		}
		failureResult := TaskResult{
			TaskID:  task.TaskID,
			Success: false,
			Error:   err.Error(),
		}
		if revokedWhileRunning {
			// The host took this claim back mid-flight and re-queued the
			// task; the abort error is not a task failure. Reporting it
			// would fail a task another guest may already be running — the
			// revoked guest reports nothing, as the drop paths already do.
			g.log.Printf("[TASK] claim for task %s was revoked while running; not reporting the result", task.TaskID)
			return nil, err
		}
		if sendErr := g.SendResult(failureResult); sendErr != nil {
			g.log.Printf("failed to send failure result: %v", sendErr)
		}
		return nil, err
	}

	if !result.Success && g.isStopping() {
		// The guest is shutting down: the pi subprocess was killed before it
		// could emit agent_settled, so the failure is a shutdown artefact,
		// not an agent fault. Do not report it — the server re-queues the
		// task when the connection drops. See issue #184.
		g.log.Printf("[TASK] task %s interrupted by guest shutdown; not reporting failure", task.TaskID)
		if sendErr := g.SendLog(LogEntry{TaskID: task.TaskID, Line: "Task interrupted by guest shutdown; will be re-queued", Level: "system"}); sendErr != nil {
			g.log.Printf("failed to send interruption log: %v", sendErr)
		}
		return result, nil
	}

	if result.Success {
		g.log.Printf("[TASK] task %s completed successfully", task.TaskID)
		if err := g.SendLog(LogEntry{TaskID: task.TaskID, Line: "Task completed successfully", Level: "system"}); err != nil {
			g.log.Printf("failed to send task complete log: %v", err)
		}
	} else {
		g.log.Printf("[TASK] task %s failed: %s", task.TaskID, result.Error)
		if err := g.SendLog(LogEntry{TaskID: task.TaskID, Line: fmt.Sprintf("Task failed: %s", result.Error), Level: "error"}); err != nil {
			g.log.Printf("failed to send task failure log: %v", err)
		}
	}

	if revokedWhileRunning {
		// A successful handler says nothing about a claim the host already
		// took back: the side effects are done, but a result would decide a
		// fate this guest no longer owns.
		g.log.Printf("[TASK] claim for task %s was revoked while running; not reporting the result", task.TaskID)
		return result, nil
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
