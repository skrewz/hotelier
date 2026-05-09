package pi

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"sync"
	"time"
)

// Event represents a pi RPC event streamed to stdout.
type Event struct {
	Type                  string          `json:"type"`
	AssistantMessageEvent json.RawMessage `json:"assistantMessageEvent,omitempty"`
	Message               json.RawMessage `json:"message,omitempty"`
	Messages              json.RawMessage `json:"messages,omitempty"`
	ToolCallId            string          `json:"toolCallId,omitempty"`
	ToolName              string          `json:"toolName,omitempty"`
	IsError               bool            `json:"isError,omitempty"`
	Args                  json.RawMessage `json:"args,omitempty"`
	Result                json.RawMessage `json:"result,omitempty"`
	PartialResult         json.RawMessage `json:"partialResult,omitempty"`
}

// PiClient manages a pi RPC subprocess.
type PiClient struct {
	cmd           *exec.Cmd
	stdin         io.WriteCloser
	stdout        io.ReadCloser
	stderr        io.ReadCloser
	mu            sync.Mutex
	log           *log.Logger
	eventCh       chan Event
	doneCh        chan struct{}
	started       bool
	cwd           string
	provider      string
	model         string
	thinkingLevel string
	agentDir      string
	debug         bool
}

// PiClientConfig holds configuration for the pi client.
type PiClientConfig struct {
	// CWD is the working directory for pi.
	CWD string
	// Provider is the LLM provider (e.g. "anthropic", "openai").
	Provider string
	// Model is the model ID (e.g. "claude-sonnet-4-20250514").
	Model string
	// ThinkingLevel is the thinking level (off, minimal, low, medium, high, xhigh).
	ThinkingLevel string
	// AgentDir overrides pi's config directory.
	AgentDir string
	// Log is the logger for pi client events.
	Log *log.Logger
	// Debug enables verbose RPC logging to stdout.
	Debug bool
}

// NewClient creates a new pi RPC client.
func NewClient(cfg PiClientConfig) *PiClient {
	if cfg.Log == nil {
		cfg.Log = log.New(os.Stderr, "[pi] ", log.LstdFlags)
	}
	return &PiClient{
		log:           cfg.Log,
		cwd:           cfg.CWD,
		provider:      cfg.Provider,
		model:         cfg.Model,
		thinkingLevel: cfg.ThinkingLevel,
		agentDir:      cfg.AgentDir,
		debug:         cfg.Debug,
		eventCh:       make(chan Event, 256),
		doneCh:        make(chan struct{}),
	}
}

// Start launches the pi RPC subprocess.
func (c *PiClient) Start(ctx context.Context) error {
	args := []string{"--mode", "rpc", "--no-session"}
	if c.provider != "" {
		args = append(args, "--provider", c.provider)
	}
	if c.model != "" {
		args = append(args, "--model", c.model)
	}
	if c.thinkingLevel != "" {
		args = append(args, "--thinking", c.thinkingLevel)
	}
	if c.agentDir != "" {
		args = append(args, "--session-dir", c.agentDir)
	}

	c.cmd = exec.CommandContext(ctx, "pi", args...)
	c.cmd.Dir = c.cwd

	stdin, err := c.cmd.StdinPipe()
	if err != nil {
		return fmt.Errorf("stdin pipe: %w", err)
	}
	stdout, err := c.cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("stdout pipe: %w", err)
	}
	stderr, err := c.cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("stderr pipe: %w", err)
	}

	if err := c.cmd.Start(); err != nil {
		return fmt.Errorf("start pi: %w", err)
	}

	c.stdin = stdin
	c.stdout = stdout
	c.stderr = stderr
	c.started = true

	// Start reading stdout events
	go c.readEvents()

	// Capture stderr
	go c.readStderr()

	c.log.Printf("pi RPC subprocess started (pid %d)", c.cmd.Process.Pid)
	return nil
}

// Stop terminates the pi RPC subprocess.
func (c *PiClient) Stop(ctx context.Context) error {
	c.mu.Lock()
	if !c.started {
		c.mu.Unlock()
		return nil
	}

	if c.stdin != nil {
		// Send abort to clean up any in-progress work
		_ = c.sendCommandLocked(map[string]interface{}{"type": "abort"})
		_ = c.stdin.Close()
	}
	c.mu.Unlock()

	// Wait for process to exit, with a fallback kill if it doesn't exit gracefully.
	// pi is a persistent RPC server; closing stdin usually causes it to exit,
	// but in some cases (e.g., plan mode, extension dialogs) it may hang.
	done := make(chan error, 1)
	go func() {
		done <- c.cmd.Wait()
	}()

	select {
	case err := <-done:
		c.mu.Lock()
		c.started = false
		close(c.doneCh)
		c.mu.Unlock()
		if err != nil {
			c.log.Printf("pi subprocess exited: %v", err)
		} else {
			c.log.Printf("pi subprocess stopped cleanly")
		}
		return nil
	case <-time.After(5 * time.Second):
		// Process didn't exit gracefully — force kill
		c.log.Printf("pi subprocess did not exit within 5s, force killing")
		_ = c.cmd.Process.Kill()
		c.cmd.Wait()
		c.mu.Lock()
		c.started = false
		close(c.doneCh)
		c.mu.Unlock()
		return fmt.Errorf("pi subprocess killed after timeout")
	}
}

// Prompt sends a user prompt to pi and streams events back.
func (c *PiClient) Prompt(ctx context.Context, message string) error {
	return c.sendCommand(map[string]interface{}{
		"type":    "prompt",
		"message": message,
	})
}

// Abort sends an abort command to pi.
func (c *PiClient) Abort() error {
	return c.sendCommand(map[string]interface{}{"type": "abort"})
}

// Subscribe returns a channel that receives pi events.
func (c *PiClient) Subscribe() <-chan Event {
	return c.eventCh
}

// IsRunning returns whether the pi subprocess is running.
func (c *PiClient) IsRunning() bool {
	return c.started
}

// GetProcessState returns the process state, or nil if the process hasn't exited yet.
func (c *PiClient) GetProcessState() *os.ProcessState {
	return c.cmd.ProcessState
}

// Cmd returns the underlying exec.Cmd (for testing/advanced use).
func (c *PiClient) Cmd() *exec.Cmd {
	return c.cmd
}

// CWD returns the working directory configured for the pi subprocess.
func (c *PiClient) CWD() string {
	return c.cwd
}

func (c *PiClient) sendCommand(cmd map[string]interface{}) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sendCommandLocked(cmd)
}

func (c *PiClient) sendCommandLocked(cmd map[string]interface{}) error {
	if c.stdin == nil {
		return fmt.Errorf("client not started")
	}

	data, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("marshal command: %w", err)
	}

	// Log raw RPC commands to stdout when debug is enabled.
	if c.debug {
		fmt.Fprintf(os.Stdout, "[RPC] -> %s\n", string(data))
	}

	_, err = c.stdin.Write(append(data, '\n'))
	if err != nil {
		return fmt.Errorf("write command: %w", err)
	}

	return nil
}

func (c *PiClient) readEvents() {
	defer close(c.eventCh)

	scanner := bufio.NewScanner(c.stdout)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)

	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}

		// Log raw RPC events to stdout when debug is enabled.
		if c.debug {
			fmt.Fprintf(os.Stdout, "[RPC] <- %s\n", line)
		}

		var event Event
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			c.log.Printf("pi parse error: %v (line: %s)", err, line)
			continue
		}

		// Skip responses (they have an "id" field and "type": "response")
		if event.Type == "response" {
			continue
		}

		select {
		case c.eventCh <- event:
		case <-c.doneCh:
			return
		}
	}

	if err := scanner.Err(); err != nil {
		c.log.Printf("pi stdout scan error: %v", err)
	}
}

func (c *PiClient) readStderr() {
	scanner := bufio.NewScanner(c.stderr)
	for scanner.Scan() {
		c.log.Printf("pi stderr: %s", scanner.Text())
	}
}

// ExtractTextDelta extracts text content from message_update events.
func ExtractTextDelta(event Event) string {
	if event.AssistantMessageEvent == nil {
		return ""
	}

	var delta struct {
		Type  string `json:"type"`
		Delta string `json:"delta"`
	}
	if err := json.Unmarshal(event.AssistantMessageEvent, &delta); err != nil {
		return ""
	}

	if delta.Type == "text_delta" {
		return delta.Delta
	}

	return ""
}

// IsTextDelta checks if the event is a text delta.
func IsTextDelta(event Event) bool {
	if event.AssistantMessageEvent == nil {
		return false
	}

	var delta struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(event.AssistantMessageEvent, &delta); err != nil {
		return false
	}

	return delta.Type == "text_delta"
}

// IsAgentEnd checks if the event is an agent completion event.
func IsAgentEnd(event Event) bool {
	return event.Type == "agent_end"
}

// FinalText extracts the final assistant message text from an agent_end event.
// This is the model's complete output after all tool calls and processing.
func FinalText(event Event) string {
	if event.Messages == nil {
		return ""
	}

	var messages []struct {
		Role    string `json:"role"`
		Content []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"content"`
	}
	if err := json.Unmarshal(event.Messages, &messages); err != nil {
		return ""
	}

	// Find the last assistant message
	var lastAssistant string
	for _, msg := range messages {
		if msg.Role == "assistant" {
			for _, c := range msg.Content {
				if c.Type == "text" {
					lastAssistant += c.Text
				}
			}
		}
	}
	return lastAssistant
}

// IsToolExecution checks if the event is a tool execution event.
func IsToolExecution(event Event) bool {
	return event.Type == "tool_execution_start" ||
		event.Type == "tool_execution_update" ||
		event.Type == "tool_execution_end"
}

// ToolName extracts the tool name from tool execution events.
func ToolName(event Event) string {
	return event.ToolName
}

// ToolCallId extracts the tool call ID from tool execution events.
func ToolCallId(event Event) string {
	return event.ToolCallId
}

// ToolArgs extracts the arguments from a tool start/update event.
func ToolArgs(event Event) string {
	if event.Args == nil {
		return ""
	}
	var args struct {
		Command string `json:"command"`
		Content string `json:"content"`
		Path    string `json:"path"`
	}
	if err := json.Unmarshal(event.Args, &args); err != nil {
		return string(event.Args)
	}
	// Return the most relevant field
	if args.Command != "" {
		return args.Command
	}
	if args.Content != "" {
		return args.Content
	}
	if args.Path != "" {
		return "path: " + args.Path
	}
	return string(event.Args)
}

// ToolPartialResult extracts the partial result text from a tool update event.
func ToolPartialResult(event Event) string {
	if event.PartialResult == nil {
		return ""
	}
	var result struct {
		Content []struct {
			Type  string `json:"type"`
			Text  string `json:"text"`
			Image struct {
				Type string `json:"type"`
				Data string `json:"data"`
			} `json:"image"`
		} `json:"content"`
	}
	if err := json.Unmarshal(event.PartialResult, &result); err != nil {
		return ""
	}
	var text string
	for _, c := range result.Content {
		if c.Type == "text" {
			text += c.Text
		}
	}
	return text
}

// ToolResult extracts the final result text from a tool end event.
func ToolResult(event Event) string {
	if event.Result == nil {
		return ""
	}
	var result struct {
		Content []struct {
			Type  string `json:"type"`
			Text  string `json:"text"`
			Image struct {
				Type string `json:"type"`
				Data string `json:"data"`
			} `json:"image"`
		} `json:"content"`
	}
	if err := json.Unmarshal(event.Result, &result); err != nil {
		return ""
	}
	var text string
	for _, c := range result.Content {
		if c.Type == "text" {
			text += c.Text
		}
	}
	return text
}
