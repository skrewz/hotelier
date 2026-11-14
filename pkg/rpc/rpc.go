package rpc

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

// ErrConnectionClosed is returned by Send when the connection has been torn
// down. Callers can branch on it with errors.Is to tell a routine teardown
// race apart from a real back-pressure failure such as a full send buffer
// (issue #242).
var ErrConnectionClosed = errors.New("connection closed")

// connectionIDKey is the context key for the current connection ID.
type connectionIDKey struct{}

// ConnectionIDFromContext returns the connection ID from a handler's context.
func ConnectionIDFromContext(ctx context.Context) (string, bool) {
	id, ok := ctx.Value(connectionIDKey{}).(string)
	return id, ok
}

// MessageType represents the type of a JSON-RPC message.
type MessageType int

const (
	MessageRequest MessageType = iota
	MessageResponse
	MessageNotification
)

// JSONRPCMessage represents a JSON-RPC 2.0 message.
type JSONRPCMessage struct {
	JSONRPC string           `json:"jsonrpc"`
	ID      *json.RawMessage `json:"id,omitempty"`
	Method  string           `json:"method"`
	Params  json.RawMessage  `json:"params,omitempty"`
	Result  json.RawMessage  `json:"result,omitempty"`
	Error   *RPCError        `json:"error,omitempty"`
}

// RPCError represents a JSON-RPC 2.0 error.
type RPCError struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

// Error implements the error interface.
func (e *RPCError) Error() string {
	return fmt.Sprintf("RPC error %d: %s", e.Code, e.Message)
}

// Standard JSON-RPC 2.0 error codes.
const (
	CodeParseError     = -32700
	CodeInvalidRequest = -32600
	CodeMethodNotFound = -32601
	CodeInvalidParams  = -32602
	CodeInternalError  = -32603

	// CodeClaimRefused is an application-defined error (JSON-RPC reserves
	// -32000 to -32099 for server errors) meaning "the claim you are
	// asserting is not yours": the task has moved on — completed, failed,
	// re-queued, or assigned to another guest. It is a decision about the
	// claim, not a transport failure, which is what distinguishes it from
	// CodeInternalError. See issue #252.
	CodeClaimRefused = -32004
)

// ParseError creates a parse error.
func ParseError() *RPCError {
	return &RPCError{Code: CodeParseError, Message: "Invalid JSON was received by the server."}
}

// InvalidRequestError creates an invalid request error.
func InvalidRequestError() *RPCError {
	return &RPCError{Code: CodeInvalidRequest, Message: "The JSON sent is not a valid Request object."}
}

// MethodNotFoundError creates a method not found error.
func MethodNotFoundError(method string) *RPCError {
	return &RPCError{Code: CodeMethodNotFound, Message: fmt.Sprintf("Method %q not found", method)}
}

// InvalidParamsError creates an invalid params error.
func InvalidParamsError(msg string) *RPCError {
	return &RPCError{Code: CodeInvalidParams, Message: msg}
}

// InternalError creates an internal error.
func InternalError(msg string) *RPCError {
	return &RPCError{Code: CodeInternalError, Message: msg}
}

// ClaimRefusedError creates an error stating that the claim the caller
// asserts is no longer its own — the task has moved on.
func ClaimRefusedError(msg string) *RPCError {
	return &RPCError{Code: CodeClaimRefused, Message: msg}
}

// IsClaimRefused reports whether err is (or wraps) a claim refusal: an
// explicit statement from the host that the task is not the caller's to
// execute. Anything else — a transport error, a method the server does not
// implement, an internal error — says nothing about the claim.
func IsClaimRefused(err error) bool {
	var rpcErr *RPCError
	return errors.As(err, &rpcErr) && rpcErr.Code == CodeClaimRefused
}

// Handler is a function that handles a JSON-RPC method call.
type Handler func(ctx context.Context, params json.RawMessage) (interface{}, *RPCError)

// NewTestConnection creates a test connection for use in tests.
func NewTestConnection(id string, hub *Hub) *Connection {
	return &Connection{
		id:      id,
		hub:     hub,
		send:    make(chan []byte, 256),
		closeCh: make(chan struct{}),
	}
}

// ConnectionRole identifies the type of a WebSocket connection.
type ConnectionRole string

const (
	ConnectionRoleBrowser ConnectionRole = "browser"
	ConnectionRoleGuest   ConnectionRole = "guest"
)

// Connection represents a connected JSON-RPC client over WebSocket.
type Connection struct {
	id      string
	conn    *websocket.Conn
	role    ConnectionRole
	send    chan []byte
	hub     *Hub
	mu      sync.Mutex
	closed  atomic.Bool
	closeCh chan struct{}

	// WebSocket keepalive knobs this connection was armed with, resolved once
	// in NewConnection (issue #220). Never mutated afterwards, so ReadLoop
	// reads them without a lock; a Connection built outside NewConnection
	// leaves them zero, which means the package defaults.
	keepalivePing time.Duration
	keepalivePong time.Duration
}

// ReadLoop reads messages from the connection.
func (c *Connection) ReadLoop() {
	defer c.Close()
	for {
		select {
		case <-c.closeCh:
			return
		default:
		}

		// Push the keepalive deadline out from the goroutine performing the
		// read; a peer that stops sending anything — data or pongs — fails
		// the read within the window (issue #220).
		if err := setKeepaliveDeadline(c.conn, c.keepalivePing, c.keepalivePong); err != nil {
			c.hub.logf("connection %s: set read deadline: %v", c.id, err)
			return
		}

		_, message, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err,
				websocket.CloseGoingAway,
				websocket.CloseNormalClosure,
				websocket.CloseAbnormalClosure) {
				c.hub.logf("connection %s read error: %v", c.id, err)
			}
			return
		}

		c.hub.handleMessage(c, message)
	}
}

// Send sends a JSON-RPC message to the connection.
func (c *Connection) Send(msg *JSONRPCMessage) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// A connection that is closed, or on its way out of the hub, must refuse
	// the send rather than queue into a queue nobody drains: before issue
	// #242 this was a send on a closed channel and a crashed process.
	if c.closed.Load() {
		return fmt.Errorf("%w: %s", ErrConnectionClosed, c.id)
	}

	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	select {
	case c.send <- data:
		return nil
	default:
		return fmt.Errorf("send buffer full")
	}
}

// Close closes the connection.
func (c *Connection) Close() {
	if c.closed.Swap(true) {
		return
	}
	close(c.closeCh)
	c.conn.Close()
	c.hub.removeConnection(c)
}

// WriteLoop reads from the send channel and writes to the connection.
func (c *Connection) WriteLoop() {
	defer c.Close()
	for {
		select {
		case <-c.closeCh:
			return
		case data, ok := <-c.send:
			if !ok {
				// Nothing on the live paths closes the queue any more;
				// this stays as cheap defence so a closed queue ends the
				// loop instead of writing a nil payload (issue #242).
				return
			}
			c.mu.Lock()
			err := c.conn.WriteMessage(websocket.TextMessage, data)
			c.mu.Unlock()
			if err != nil {
				c.hub.logf("connection %s: write error: %v", c.id, err)
				return
			}
		}
	}
}

// Recv reads a sent message from the connection's send channel (for testing).
func (c *Connection) Recv() ([]byte, bool) {
	select {
	case data := <-c.send:
		return data, true
	default:
		return nil, false
	}
}

// Drain removes all pending messages from the send channel. Useful in tests
// to clear stale notifications before asserting on specific messages.
func (c *Connection) Drain() {
	for {
		select {
		case <-c.send:
		default:
			return
		}
	}
}

// ID returns the connection's ID.
func (c *Connection) ID() string {
	return c.id
}

// Hub manages all connected JSON-RPC clients.
type Hub struct {
	connections      map[string]*Connection
	register         chan *Connection
	unregister       chan *Connection
	methods          map[string]Handler
	guestConnections map[string]string // guestID -> connectionID
	mu               sync.RWMutex
	logf             func(format string, args ...interface{})
	onDisconnect     func(connectionID string)
	nextID           atomic.Int64

	// WebSocket keepalive knobs for connections created after
	// SetKeepAlive (issue #220). Zero means the package defaults.
	keepalivePing time.Duration
	keepalivePong time.Duration
}

// NewHub creates a new Hub.
func NewHub(logf func(format string, args ...interface{})) *Hub {
	return &Hub{
		connections:      make(map[string]*Connection),
		register:         make(chan *Connection),
		unregister:       make(chan *Connection),
		methods:          make(map[string]Handler),
		guestConnections: make(map[string]string),
		logf:             logf,
	}
}

// SetOnDisconnect sets a callback invoked when a connection is lost.
// The callback receives the connection ID that was disconnected.
func (h *Hub) SetOnDisconnect(fn func(connectionID string)) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.onDisconnect = fn
}

// SetKeepAlive configures the WebSocket keepalive parameters applied to
// connections created afterwards via NewConnection (issue #220). A
// non-positive value selects the corresponding package default. Safe to
// call at any time, including from Server.Reload: it takes h.mu and
// NewConnection reads the values under h.mu.RLock. Connections already
// created keep the knobs they were armed with.
func (h *Hub) SetKeepAlive(pingInterval, pongTimeout time.Duration) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.keepalivePing = pingInterval
	h.keepalivePong = pongTimeout
}

// RegisterMethod registers a JSON-RPC method handler.
func (h *Hub) RegisterMethod(method string, handler Handler) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.methods[method] = handler
}

// NewConnection creates a new connection in the hub.
// The role is set atomically with connection creation to avoid races
// with Hub.Run() (the connection may not be in the map yet).
func (h *Hub) NewConnection(id string, conn *websocket.Conn, role ConnectionRole) *Connection {
	h.mu.RLock()
	ping, pong := effectiveKeepalive(h.keepalivePing, h.keepalivePong)
	h.mu.RUnlock()

	c := &Connection{
		id:            id,
		conn:          conn,
		role:          role,
		send:          make(chan []byte, 256),
		hub:           h,
		closeCh:       make(chan struct{}),
		keepalivePing: ping,
		keepalivePong: pong,
	}
	// Arm keepalive before the connection is registered and its read loop
	// starts (issue #220): a silently dead peer must be noticed even if
	// neither side ever sends application data. A deadline that cannot be
	// set means the socket is already gone; logging here is enough because
	// ReadLoop's own call fails a moment later and its deferred Close
	// removes the connection, so nothing is left unmonitored.
	if err := configureKeepalive(conn, ping, pong); err != nil {
		h.logf("connection %s: arm keepalive: %v", id, err)
	}
	startKeepalivePing(conn, ping, c.closeCh, h.logf, "connection "+id+":", nil)
	h.register <- c
	return c
}

func (h *Hub) handleMessage(c *Connection, message []byte) {
	var msg JSONRPCMessage
	if err := json.Unmarshal(message, &msg); err != nil {
		resp := &JSONRPCMessage{
			JSONRPC: "2.0",
			ID:      nil,
			Error:   ParseError(),
		}
		if msg.ID != nil {
			resp.ID = msg.ID
			if err := c.Send(resp); err != nil {
				h.logf("error sending parse error: %v", err)
			}
		}
		return
	}

	if msg.JSONRPC != "2.0" {
		resp := &JSONRPCMessage{
			JSONRPC: "2.0",
			ID:      msg.ID,
			Error:   InvalidRequestError(),
		}
		if msg.ID != nil {
			if err := c.Send(resp); err != nil {
				h.logf("error sending invalid request error: %v", err)
			}
		}
		return
	}

	h.mu.RLock()
	handler, ok := h.methods[msg.Method]
	h.mu.RUnlock()

	if !ok {
		resp := &JSONRPCMessage{
			JSONRPC: "2.0",
			ID:      msg.ID,
			Error:   MethodNotFoundError(msg.Method),
		}
		if msg.ID != nil {
			if err := c.Send(resp); err != nil {
				h.logf("error sending method not found: %v", err)
			}
		}
		return
	}

	ctx := context.WithValue(context.Background(), connectionIDKey{}, c.id)
	result, rpcErr := handler(ctx, msg.Params)
	if rpcErr != nil {
		resp := &JSONRPCMessage{
			JSONRPC: "2.0",
			ID:      msg.ID,
			Error:   rpcErr,
		}
		if err := c.Send(resp); err != nil {
			h.logf("error sending RPC error response: %v", err)
		}
		return
	}

	var resultData json.RawMessage
	if result != nil {
		resultData, _ = json.Marshal(result)
	}
	resp := &JSONRPCMessage{
		JSONRPC: "2.0",
		ID:      msg.ID,
		Result:  resultData,
	}
	if err := c.Send(resp); err != nil {
		h.logf("error sending response: %v", err)
	}
}

func (h *Hub) removeConnection(c *Connection) {
	h.unregister <- c
}

// Run runs the hub's main loop.
func (h *Hub) Run() {
	for {
		select {
		case c := <-h.register:
			h.mu.Lock()
			h.connections[c.id] = c
			h.mu.Unlock()
			h.logf("connection registered: %s (total: %d)", c.id, len(h.connections))
		case c := <-h.unregister:
			// Mark the connection closed before touching the map, and
			// without taking c.mu. c.mu is held by WriteLoop across a
			// socket write that carries no deadline, so locking it here
			// could stall this loop — and every h.mu reader with it —
			// for the duration of that write. The send queue is not
			// closed either: the closed flag is what makes Send refuse,
			// and closing it here would panic on a second unregister of
			// the same connection (issue #217 removed that path on the
			// client side; issue #242).
			c.closed.Store(true)
			h.mu.Lock()
			delete(h.connections, c.id)
			onDisconnect := h.onDisconnect
			h.mu.Unlock()
			h.logf("connection unregistered: %s (total: %d)", c.id, len(h.connections))
			// Call disconnect callback outside the lock to avoid deadlocks.
			if onDisconnect != nil {
				onDisconnect(c.id)
			}
		}
	}
}

// Register sends a connection to the hub's register channel (for testing).
func (h *Hub) Register(c *Connection) {
	h.register <- c
}

// Unregister sends a connection to the hub's unregister channel (for testing).
func (h *Hub) Unregister(c *Connection) {
	h.unregister <- c
}

// Dispatch calls a registered handler directly (for testing).
func (h *Hub) Dispatch(method string, params json.RawMessage) (interface{}, *RPCError) {
	h.mu.RLock()
	handler, ok := h.methods[method]
	h.mu.RUnlock()

	if !ok {
		return nil, MethodNotFoundError(method)
	}

	ctx := context.Background()
	return handler(ctx, params)
}

// ConnectionCount returns the number of connected clients.
func (h *Hub) ConnectionCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.connections)
}

// GetConnection returns a connection by ID.
func (h *Hub) GetConnection(id string) (*Connection, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	c, ok := h.connections[id]
	return c, ok
}

// GetGuestConnectionID returns the connection ID for a guest.
func (h *Hub) GetGuestConnectionID(guestID string) (string, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	connID, ok := h.guestConnections[guestID]
	return connID, ok
}

// GetAllConnectionIDs returns all connection IDs.
func (h *Hub) GetAllConnectionIDs() []string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	ids := make([]string, 0, len(h.connections))
	for id := range h.connections {
		ids = append(ids, id)
	}
	return ids
}

// SetConnectionRole sets the role of a connection (browser or guest).
// Used for post-registration role changes (e.g. browser → guest after
// guest.register RPC). The connection must already be in the hub's map.
func (h *Hub) SetConnectionRole(connID string, role ConnectionRole) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if c, ok := h.connections[connID]; ok {
		c.role = role
	}
}

// SendTo sends a message to a specific connection.
func (h *Hub) SendTo(id string, msg *JSONRPCMessage) error {
	c, ok := h.GetConnection(id)
	if !ok {
		return fmt.Errorf("connection %s not found", id)
	}
	return c.Send(msg)
}

// Broadcast sends a message to all connections matching the given role.
// Pass an empty string to broadcast to all roles.
func (h *Hub) Broadcast(role ConnectionRole, msg *JSONRPCMessage) {
	ids := h.GetAllConnectionIDs()
	for _, id := range ids {
		if c, ok := h.GetConnection(id); ok && c.role == role {
			if err := h.SendTo(id, msg); err != nil {
				// A connection that went away mid-broadcast is expected
				// teardown noise, not a failure worth a log line; anything
				// else (a full send buffer) still is.
				if !errors.Is(err, ErrConnectionClosed) {
					h.logf("broadcast failed to %s: %v", id, err)
				}
			}
		}
	}
}

// SendToGuest sends a task assignment to a specific guest.
func (h *Hub) SendToGuest(guestID string, method string, params interface{}) error {
	data, err := json.Marshal(params)
	if err != nil {
		return err
	}

	msg := &JSONRPCMessage{
		JSONRPC: "2.0",
		Method:  method,
		Params:  data,
	}

	h.mu.RLock()
	connID, ok := h.guestConnections[guestID]
	h.mu.RUnlock()

	if !ok {
		return fmt.Errorf("connection for guest %s not found", guestID)
	}

	return h.SendTo(connID, msg)
}

// RegisterGuestConnection records the mapping between a guest ID and its connection.
func (h *Hub) RegisterGuestConnection(guestID, connID string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.guestConnections[guestID] = connID
	h.logf("guest %s registered on connection %s", guestID, connID)
}

// UnregisterGuestConnection removes the mapping for a guest.
func (h *Hub) UnregisterGuestConnection(guestID string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.guestConnections, guestID)
}

// GuestIDFromConnection looks up the guest ID for a given connection ID.
// Returns the guest ID and true if found, or empty string and false otherwise.
func (h *Hub) GuestIDFromConnection(connID string) (string, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for guestID, id := range h.guestConnections {
		if id == connID {
			return guestID, nil
		}
	}
	return "", fmt.Errorf("no guest found for connection %s", connID)
}

// SendNotification sends a notification (no ID) to a specific connection
// or broadcasts to all connections matching the given role.
// If connID is empty, the message is broadcast to all connections with the
// specified role. Pass an empty role to broadcast to all connections.
func (h *Hub) SendNotification(connID string, role ConnectionRole, method string, params interface{}) error {
	data, err := json.Marshal(params)
	if err != nil {
		return err
	}

	msg := &JSONRPCMessage{
		JSONRPC: "2.0",
		Method:  method,
		Params:  data,
	}

	// Empty connID means broadcast to connections with the given role.
	if connID == "" {
		h.Broadcast(role, msg)
		return nil
	}

	return h.SendTo(connID, msg)
}

// Client is a JSON-RPC client that connects to a server.
type Client struct {
	id          string
	conn        *websocket.Conn
	hub         *ClientHub
	mu          sync.Mutex
	closed      atomic.Bool
	closeCh     chan struct{}
	logf        func(format string, args ...interface{})
	pendingMu   sync.Mutex                      // guards pending (issue #216)
	pending     map[string]chan *JSONRPCMessage // request ID -> waiting Call (issue #216)
	callTimeout atomic.Int64                    // per-call response bound in ns (issue #215)
	onClose     func()                          // called when the connection is lost unexpectedly; guarded by mu

	// Configured WebSocket keepalive knobs (issue #220), guarded by mu:
	// SetKeepAlive may be called at any time, including while a connection is
	// live. Zero means the package defaults.
	keepalivePing time.Duration
	keepalivePong time.Duration

	// The pair the connection now in flight was armed with, resolved once by
	// ConnectWithTLS before the read goroutine starts and never mutated
	// afterwards — so readLoop reads them without the lock and a later
	// SetKeepAlive cannot move the read window out from under the ping
	// cadence. It takes effect on the next Connect.
	armedPing time.Duration
	armedPong time.Duration

	// pingFn overrides the ping control-frame write for this client. It is
	// nil in production, where startKeepalivePing uses WriteControl, and set
	// by a test that needs to inject a write failure into this connection
	// alone (issue #220). Per instance rather than per package on purpose: a
	// package-level hook would be called by every ping goroutine in the
	// process and race with them under -race.
	pingFn pingFunc
}

// DefaultCallTimeout bounds how long Client.Call waits for a response.
// It is comfortably longer than the server's slowest legitimate handler,
// yet shorter than the default heartbeat interval of 30 seconds
// (pkg/config DefaultServerConfig), so a wedged connection surfaces as a
// heartbeat failure rather than as silence (issue #215).
const DefaultCallTimeout = 10 * time.Second

// SetCallTimeout bounds how long subsequent Call invocations wait for a
// response. A non-positive duration restores DefaultCallTimeout.
func (c *Client) SetCallTimeout(d time.Duration) {
	if d <= 0 {
		d = DefaultCallTimeout
	}
	c.callTimeout.Store(int64(d))
}

// ClientHub manages client connections.
type ClientHub struct {
	connections          map[string]*Client
	register             chan *Client
	unregister           chan *Client
	notificationHandlers map[string]func(method string, params json.RawMessage)
	mu                   sync.RWMutex
	logf                 func(format string, args ...interface{})
	nextID               atomic.Int64
}

// NewClientHub creates a new client hub.
func NewClientHub(logf func(format string, args ...interface{})) *ClientHub {
	return &ClientHub{
		connections:          make(map[string]*Client),
		register:             make(chan *Client),
		unregister:           make(chan *Client),
		notificationHandlers: make(map[string]func(method string, params json.RawMessage)),
		logf:                 logf,
	}
}

// Run runs the client hub's main loop.
func (h *ClientHub) Run() {
	for {
		select {
		case c := <-h.register:
			h.mu.Lock()
			h.connections[c.id] = c
			h.mu.Unlock()
		case c := <-h.unregister:
			h.mu.Lock()
			if _, ok := h.connections[c.id]; ok {
				delete(h.connections, c.id)
			}
			h.mu.Unlock()
		}
	}
}

// ConnectionCount returns the number of connected clients.
func (h *ClientHub) ConnectionCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.connections)
}

// RegisterNotificationHandler registers a handler for incoming RPC notifications.
func (h *ClientHub) RegisterNotificationHandler(method string, handler func(method string, params json.RawMessage)) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.notificationHandlers[method] = handler
}

// InvokeNotificationHandler calls the registered handler for the given method
// directly. Useful for testing notification handling without a live connection.
// Returns false if no handler is registered for the method.
func (h *ClientHub) InvokeNotificationHandler(method string, params json.RawMessage) bool {
	h.mu.RLock()
	handler, ok := h.notificationHandlers[method]
	h.mu.RUnlock()

	if !ok {
		return false
	}
	handler(method, params)
	return true
}

// NewClient creates a new JSON-RPC client.
func NewClient(id string, hub *ClientHub, logf func(format string, args ...interface{})) *Client {
	if logf == nil {
		logf = hub.logf
	}
	c := &Client{
		id:      id,
		hub:     hub,
		logf:    logf,
		closeCh: make(chan struct{}),
		pending: make(map[string]chan *JSONRPCMessage),
	}
	c.callTimeout.Store(int64(DefaultCallTimeout))
	return c
}

// SetOnClose sets a callback that is invoked when the connection is lost.
// The callback must be registered before Connect: Connect spawns the read
// loop, which can observe a socket the server closed immediately — if the
// callback were wired afterwards, that loss would be reported to nothing
// (see issue #218). The field itself is mutex-guarded so a later
// SetOnClose call is safe while the read loop is running.
func (c *Client) SetOnClose(fn func()) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.onClose = fn
}

// SetKeepAlive configures the WebSocket keepalive parameters used on the
// next Connect (issue #220). A non-positive value selects the corresponding
// package default. Safe to call at any time, including on a connected
// client: it takes mu, and the connection already in flight keeps the knobs
// it was armed with — matching Hub.SetKeepAlive.
func (c *Client) SetKeepAlive(pingInterval, pongTimeout time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.keepalivePing = pingInterval
	c.keepalivePong = pongTimeout
}

// Connect establishes a WebSocket connection to the server.
func (c *Client) Connect(url string) error {
	return c.ConnectWithTLS(url, nil)
}

// ConnectWithTLS establishes a WebSocket connection to the server with an
// optional TLS configuration. If tlsConfig is nil, the connection is
// plaintext (ws://). If tlsConfig is provided, the connection is upgraded
// to wss:// with the given TLS settings (use a wss:// URL).
func (c *Client) ConnectWithTLS(url string, tlsConfig *tls.Config) error {
	dialer := websocket.DefaultDialer

	if tlsConfig != nil {
		// Upgrade the URL scheme from ws:// to wss://
		if len(url) > 5 && url[:5] == "ws://" {
			url = "wss://" + url[5:]
		}
		dialer.TLSClientConfig = tlsConfig
	}

	conn, _, err := dialer.Dial(url, nil)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}

	// Resolve the knobs once for this connection (issue #220): the read loop
	// and the ping goroutine must agree on one pair, and a SetKeepAlive
	// during the connection's life must not move half of it.
	c.mu.Lock()
	ping, pong := effectiveKeepalive(c.keepalivePing, c.keepalivePong)
	c.mu.Unlock()

	// A socket whose read deadline cannot be set is one keepalive cannot
	// work on, so arming it is part of establishing the connection (issue
	// #220): fail the Connect rather than leave a link nothing watches.
	if err := configureKeepalive(conn, ping, pong); err != nil {
		conn.Close()
		return fmt.Errorf("arm keepalive: %w", err)
	}

	c.conn = conn
	c.armedPing, c.armedPong = ping, pong
	go c.hub.Run()
	c.hub.register <- c
	go c.readLoop()
	startKeepalivePing(conn, ping, c.closeCh, c.logf, "client "+c.id+":", c.pingFn)
	return nil
}

// shutdown tears the connection down exactly once and reports whether this
// call was the one that did it.
func (c *Client) shutdown() bool {
	if c.closed.Swap(true) {
		return false
	}
	close(c.closeCh)
	c.conn.Close()
	c.hub.unregister <- c
	return true
}

// fail declares the connection unusable: it closes closeCh, closes the
// socket, drops the client from the hub and notifies the owner via onClose,
// all exactly once.
//
// INVARIANT: every goroutine that discovers the connection is unusable — a
// failed read, a failed write, or any other hard error — must go through
// fail. Guest.Start() waits solely on the channel onClose closes, so a path
// that reports the error to its caller without calling fail leaves the guest
// convinced it is still connected (issue #217).
//
// onClose is invoked outside c.mu and must not call back into the client.
func (c *Client) fail(err error) {
	if !c.shutdown() {
		return
	}
	c.logf("client %s: connection lost: %v", c.id, err)

	c.mu.Lock()
	onClose := c.onClose
	c.mu.Unlock()
	if onClose != nil {
		onClose()
	}
}

func (c *Client) readLoop() {
	defer c.Close()
	for {
		select {
		case <-c.closeCh:
			return
		default:
		}

		// Push the keepalive deadline out from the goroutine performing the
		// read; a missed pong therefore fails ReadMessage within the window,
		// firing onClose so guest.Start reconnects (issue #220).
		if err := setKeepaliveDeadline(c.conn, c.armedPing, c.armedPong); err != nil {
			c.logf("client %s: set read deadline: %v", c.id, err)
			return
		}

		_, message, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err,
				websocket.CloseGoingAway,
				websocket.CloseNormalClosure,
				websocket.CloseAbnormalClosure) {
				c.logf("client %s read error: %v", c.id, err)
			}
			// Notify the guest that the connection is lost. fail loads
			// onClose under the mutex, so a concurrent SetOnClose cannot
			// race with this read (issues #217, #218).
			c.fail(err)
			return
		}

		var msg JSONRPCMessage
		if err := json.Unmarshal(message, &msg); err != nil {
			c.logf("client %s: failed to parse message: %v", c.id, err)
			continue
		}

		// Responses (ID set) go to the Call that made the request (issue #216).
		if msg.ID != nil {
			c.deliverResponse(&msg)
			continue
		}

		// Handle incoming notifications from server (no ID).
		// Dispatch off the readLoop: a handler that performs a Call would
		// otherwise wait for a response that only readLoop can deliver — a
		// self-deadlock that wedges the connection forever
		// (issue #214). Guests must therefore keep their handlers safe to
		// run concurrently.
		if msg.Method != "" {
			go c.handleNotification(msg)
		}
	}
}

// deliverResponse hands a response to the Call waiting under its request
// ID. A response that matches no pending Call is logged and ignored,
// never handed to an arbitrary waiter (issue #216). Both ways a response
// can arrive unclaimed — one the peer invented, and one whose caller gave
// up — share this line and carry the full ID, so a reader can tell them
// apart by correlating the ID with the callers' own timeout errors.
func (c *Client) deliverResponse(msg *JSONRPCMessage) {
	key := string(*msg.ID)

	c.pendingMu.Lock()
	ch, ok := c.pending[key]
	c.pendingMu.Unlock()

	if !ok {
		c.logf("client %s: response for request id %q ignored", c.id, key)
		return
	}

	// The channel has capacity 1 and only this path sends, so the send
	// never blocks even if the Call gave up (and deregistered) between
	// the lookup and the send; such a response simply becomes garbage.
	// The default branch is the duplicate: a second response for an ID
	// whose Call has not yet woken, dropped with a log line.
	select {
	case ch <- msg:
	default:
		c.logf("client %s: second response for request id %q ignored", c.id, key)
	}
}

func (c *Client) handleNotification(msg JSONRPCMessage) {
	c.hub.mu.RLock()
	handler, ok := c.hub.notificationHandlers[msg.Method]
	c.hub.mu.RUnlock()

	if ok {
		handler(msg.Method, msg.Params)
	} else {
		c.logf("client %s: received notification: %q (no handler registered)", c.id, msg.Method)
	}
}

// Call sends a JSON-RPC request and returns the response.
func (c *Client) Call(method string, params interface{}) (json.RawMessage, error) {
	var p json.RawMessage
	if params != nil {
		var err error
		p, err = json.Marshal(params)
		if err != nil {
			return nil, err
		}
	}

	id := c.hub.nextID.Add(1)
	idBytes, _ := json.Marshal(id)

	// Register this Call under the ID it allocated so the readLoop can
	// route the response back here and only here. The buffer of 1 means
	// the readLoop's send never blocks on a caller that has already gone
	// away; the deferred delete keeps the table empty once Call returns,
	// whatever path it takes (issue #216).
	key := string(idBytes)
	respCh := make(chan *JSONRPCMessage, 1)
	c.pendingMu.Lock()
	c.pending[key] = respCh
	c.pendingMu.Unlock()
	defer func() {
		c.pendingMu.Lock()
		delete(c.pending, key)
		c.pendingMu.Unlock()
	}()

	msg := &JSONRPCMessage{
		JSONRPC: "2.0",
		ID:      (*json.RawMessage)(&idBytes),
		Method:  method,
		Params:  p,
	}

	data, err := json.Marshal(msg)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	err = c.conn.WriteMessage(websocket.TextMessage, data)
	c.mu.Unlock()

	if err != nil {
		// The socket is gone: tell the reconnect loop, not just the caller.
		c.fail(err)
		return nil, err
	}

	// Wait for the response delivered by the readLoop goroutine to this
	// Call's own channel, but never block forever: a lost connection or a
	// response that never arrives must surface as an error, not a silent
	// hang (issues #215, #216).
	timer := time.NewTimer(time.Duration(c.callTimeout.Load()))
	defer timer.Stop()

	var resp *JSONRPCMessage
	select {
	case resp = <-respCh:
	case <-c.closeCh:
		return nil, fmt.Errorf("connection closed waiting for %s response", method)
	case <-timer.C:
		return nil, fmt.Errorf("timeout waiting for %s response", method)
	}

	if resp.Error != nil {
		return nil, resp.Error
	}

	return resp.Result, nil
}

// SendNotification sends a notification (no ID) to the server.
func (c *Client) SendNotification(method string, params interface{}) error {
	var p json.RawMessage
	if params != nil {
		var err error
		p, err = json.Marshal(params)
		if err != nil {
			return err
		}
	}

	msg := &JSONRPCMessage{
		JSONRPC: "2.0",
		Method:  method,
		Params:  p,
	}

	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	c.mu.Lock()
	err = c.conn.WriteMessage(websocket.TextMessage, data)
	c.mu.Unlock()
	if err != nil {
		// The socket is gone: tell the reconnect loop, not just the caller.
		c.fail(err)
		return err
	}

	return nil
}

// Close closes the client connection. It is the graceful path and therefore
// deliberately does not report a connection loss; unexpected failures go
// through fail instead.
func (c *Client) Close() {
	c.shutdown()
}

// Upgrader is a wrapper around the WebSocket upgrader with configurable options.
type Upgrader struct {
	websocket.Upgrader
}

// NewUpgrader creates a new WebSocket upgrader.
func NewUpgrader() *Upgrader {
	return &Upgrader{
		Upgrader: websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
			CheckOrigin: func(r *http.Request) bool {
				return true // In production, configure properly
			},
		},
	}
}

// ServeHTTP upgrades HTTP connections to WebSocket.
func (u *Upgrader) ServeHTTP(w http.ResponseWriter, r *http.Request) (*websocket.Conn, error) {
	conn, err := u.Upgrader.Upgrade(w, r, nil)
	if err != nil {
		return nil, fmt.Errorf("websocket upgrade: %w", err)
	}
	return conn, nil
}

// ReadJSON reads a JSON message from a WebSocket connection.
func ReadJSON(conn *websocket.Conn) ([]byte, error) {
	_, message, err := conn.ReadMessage()
	if err != nil {
		return nil, err
	}
	return message, nil
}

// WriteJSON writes a JSON message to a WebSocket connection.
func WriteJSON(conn *websocket.Conn, v interface{}) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return conn.WriteMessage(websocket.TextMessage, data)
}

// ReadMessage reads a JSON-RPC message from a connection.
func ReadMessage(conn *websocket.Conn) (*JSONRPCMessage, error) {
	data, err := ReadJSON(conn)
	if err != nil {
		return nil, err
	}

	var msg JSONRPCMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, fmt.Errorf("unmarshal: %w", err)
	}
	return &msg, nil
}

// WriteMessage writes a JSON-RPC message to a connection.
func WriteMessage(conn *websocket.Conn, msg *JSONRPCMessage) error {
	return WriteJSON(conn, msg)
}

// IsWebSocket checks if the HTTP request is a WebSocket upgrade request.
func IsWebSocket(r *http.Request) bool {
	// Check for the Upgrade header
	return strings.EqualFold(r.Header.Get("Upgrade"), "websocket")
}
