package rpc

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/gorilla/websocket"
)

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
		case data := <-c.send:
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

// RegisterMethod registers a JSON-RPC method handler.
func (h *Hub) RegisterMethod(method string, handler Handler) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.methods[method] = handler
}

// NewConnection creates a new connection in the hub.
func (h *Hub) NewConnection(id string, conn *websocket.Conn) *Connection {
	c := &Connection{
		id:      id,
		conn:    conn,
		send:    make(chan []byte, 256),
		hub:     h,
		closeCh: make(chan struct{}),
	}
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
			h.mu.Lock()
			if _, ok := h.connections[c.id]; ok {
				delete(h.connections, c.id)
				close(c.send)
			}
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
				h.logf("broadcast failed to %s: %v", id, err)
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
	id       string
	conn     *websocket.Conn
	send     chan []byte
	hub      *ClientHub
	mu       sync.Mutex
	closed   atomic.Bool
	closeCh  chan struct{}
	callResp chan *JSONRPCMessage
	onClose  func() // called when the connection is lost unexpectedly
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
				close(c.send)
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
	return &Client{
		id:       id,
		hub:      hub,
		send:     make(chan []byte, 256),
		closeCh:  make(chan struct{}),
		callResp: make(chan *JSONRPCMessage, 1),
	}
}

// SetOnClose sets a callback that is invoked when the connection is lost.
func (c *Client) SetOnClose(fn func()) {
	c.onClose = fn
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
	c.conn = conn
	go c.hub.Run()
	c.hub.register <- c
	go c.readLoop()
	go c.writeLoop()
	return nil
}

func (c *Client) readLoop() {
	defer c.Close()
	for {
		select {
		case <-c.closeCh:
			return
		default:
		}

		_, message, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err,
				websocket.CloseGoingAway,
				websocket.CloseNormalClosure,
				websocket.CloseAbnormalClosure) {
				c.hub.logf("client %s read error: %v", c.id, err)
			}
			// Notify the guest that the connection is lost.
			if c.onClose != nil {
				c.onClose()
			}
			return
		}

		var msg JSONRPCMessage
		if err := json.Unmarshal(message, &msg); err != nil {
			c.hub.logf("client %s: failed to parse message: %v", c.id, err)
			continue
		}

		// If this is a response to a Call() (has ID), deliver it to the caller
		if msg.ID != nil {
			select {
			case c.callResp <- &msg:
			default:
				// caller not waiting, discard
			}
			continue
		}

		// Handle incoming notifications from server (no ID)
		if msg.Method != "" {
			c.handleNotification(msg)
		}
	}
}

func (c *Client) writeLoop() {
	defer c.Close()
	for {
		select {
		case <-c.closeCh:
			return
		case data := <-c.send:
			c.mu.Lock()
			err := c.conn.WriteMessage(websocket.TextMessage, data)
			c.mu.Unlock()
			if err != nil {
				c.hub.logf("client %s: write error: %v", c.id, err)
				return
			}
		}
	}
}

func (c *Client) handleNotification(msg JSONRPCMessage) {
	c.hub.mu.RLock()
	handler, ok := c.hub.notificationHandlers[msg.Method]
	c.hub.mu.RUnlock()

	if ok {
		handler(msg.Method, msg.Params)
	} else {
		c.hub.logf("client %s: received notification: %s (no handler registered)", c.id, msg.Method)
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
		return nil, err
	}

	// Wait for response via the readLoop goroutine
	resp := <-c.callResp
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
	defer c.mu.Unlock()
	return c.conn.WriteMessage(websocket.TextMessage, data)
}

// Close closes the client connection.
func (c *Client) Close() {
	if c.closed.Swap(true) {
		return
	}
	close(c.closeCh)
	c.conn.Close()
	c.hub.unregister <- c
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
