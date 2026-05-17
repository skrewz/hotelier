package registry

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// AgentState represents the state of an agent.
type AgentState int

const (
	AgentStateDisconnected AgentState = iota
	AgentStateRegistered
	AgentStateIdle
	AgentStateRunning
)

func (s AgentState) String() string {
	switch s {
	case AgentStateDisconnected:
		return "DISCONNECTED"
	case AgentStateRegistered:
		return "REGISTERED"
	case AgentStateIdle:
		return "IDLE"
	case AgentStateRunning:
		return "RUNNING"
	default:
		return "UNKNOWN"
	}
}

// MarshalJSON implements json.Marshaler for AgentState.
// It serializes the state as a string (e.g. "IDLE") rather than an int,
// so the frontend can use it directly without type coercion.
func (s AgentState) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.String())
}

// UnmarshalJSON implements json.Unmarshaler for AgentState.
func (s *AgentState) UnmarshalJSON(data []byte) error {
	var str string
	if err := json.Unmarshal(data, &str); err != nil {
		return err
	}
	switch str {
	case "DISCONNECTED":
		*s = AgentStateDisconnected
	case "REGISTERED":
		*s = AgentStateRegistered
	case "IDLE":
		*s = AgentStateIdle
	case "RUNNING":
		*s = AgentStateRunning
	default:
		*s = 0 // unknown state, store as zero value
	}
	return nil
}

// Agent represents a connected agent in the system.
type Agent struct {
	ID            string     `json:"id"`
	Name          string     `json:"name"`
	Tags          []string   `json:"tags"`
	State         AgentState `json:"state"`
	ConnectedAt   time.Time  `json:"connected_at"`
	LastHeartbeat time.Time  `json:"last_heartbeat"`
	TaskID        string     `json:"task_id,omitempty"`
}

// AgentRegistry manages the lifecycle of connected agents.
type AgentRegistry struct {
	agents    map[string]*Agent
	mu        sync.RWMutex
	maxAgents int
	logf      func(format string, args ...interface{})
}

// NewAgentRegistry creates a new agent registry.
func NewAgentRegistry(maxAgents int, logf func(format string, args ...interface{})) *AgentRegistry {
	return &AgentRegistry{
		agents:    make(map[string]*Agent),
		maxAgents: maxAgents,
		logf:      logf,
	}
}

// Register adds a new agent to the registry.
func (r *AgentRegistry) Register(agentID, name string, tags []string) (*Agent, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.maxAgents > 0 && len(r.agents) >= r.maxAgents {
		return nil, fmt.Errorf("maximum number of agents (%d) reached", r.maxAgents)
	}

	if _, exists := r.agents[agentID]; exists {
		return nil, fmt.Errorf("agent %s already registered", agentID)
	}

	agent := &Agent{
		ID:            agentID,
		Name:          name,
		Tags:          tags,
		State:         AgentStateIdle,
		ConnectedAt:   time.Now(),
		LastHeartbeat: time.Now(),
	}

	r.agents[agentID] = agent
	if r.logf != nil {
		r.logf("agent registered: %s (name: %s, tags: %v, total: %d)", agentID, name, tags, len(r.agents))
	}
	return agent, nil
}

// Unregister removes an agent from the registry.
func (r *AgentRegistry) Unregister(agentID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	agent, exists := r.agents[agentID]
	if !exists {
		return fmt.Errorf("agent %s not found", agentID)
	}

	if agent.State == AgentStateRunning {
		return fmt.Errorf("cannot unregister running agent %s (task: %s)", agentID, agent.TaskID)
	}

	delete(r.agents, agentID)
	if r.logf != nil {
		r.logf("agent unregistered: %s (total: %d)", agentID, len(r.agents))
	}
	return nil
}

// Heartbeat updates the last heartbeat time for an agent.
func (r *AgentRegistry) Heartbeat(agentID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	agent, exists := r.agents[agentID]
	if !exists {
		return fmt.Errorf("agent %s not found", agentID)
	}

	agent.LastHeartbeat = time.Now()
	return nil
}

// SetLastHeartbeat sets the last heartbeat time for an agent (for testing).
func (r *AgentRegistry) SetLastHeartbeat(agentID string, t time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	agent, exists := r.agents[agentID]
	if !exists {
		return fmt.Errorf("agent %s not found", agentID)
	}

	agent.LastHeartbeat = t
	return nil
}

// GetAgent returns an agent by ID.
func (r *AgentRegistry) GetAgent(agentID string) (*Agent, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	agent, exists := r.agents[agentID]
	return agent, exists
}

// GetAgents returns all agents matching the given state.
func (r *AgentRegistry) GetAgents(state AgentState) []*Agent {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var result []*Agent
	for _, agent := range r.agents {
		if agent.State == state {
			result = append(result, agent)
		}
	}
	return result
}

// GetAllAgents returns all agents.
func (r *AgentRegistry) GetAllAgents() []*Agent {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make([]*Agent, 0, len(r.agents))
	for _, agent := range r.agents {
		result = append(result, agent)
	}
	return result
}

// SetAgentState updates an agent's state.
func (r *AgentRegistry) SetAgentState(agentID string, state AgentState) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	agent, exists := r.agents[agentID]
	if !exists {
		return fmt.Errorf("agent %s not found", agentID)
	}

	oldState := agent.State
	agent.State = state
	if r.logf != nil {
		r.logf("agent %s state changed: %s -> %s", agentID, oldState, state)
	}
	return nil
}

// SetAgentTask assigns a task to an agent.
func (r *AgentRegistry) SetAgentTask(agentID, taskID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	agent, exists := r.agents[agentID]
	if !exists {
		return fmt.Errorf("agent %s not found", agentID)
	}

	agent.TaskID = taskID
	agent.State = AgentStateRunning
	return nil
}

// ClearAgentTask clears the task assignment from an agent.
func (r *AgentRegistry) ClearAgentTask(agentID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	agent, exists := r.agents[agentID]
	if !exists {
		return fmt.Errorf("agent %s not found", agentID)
	}

	agent.TaskID = ""
	agent.State = AgentStateIdle
	return nil
}

// KillRunningAgentTask clears the task assignment from a running agent.
// This is used when the server detects the agent has gone silent and needs
// to forcibly terminate its task execution.
func (r *AgentRegistry) KillRunningAgentTask(agentID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	agent, exists := r.agents[agentID]
	if !exists {
		return fmt.Errorf("agent %s not found", agentID)
	}

	if agent.TaskID == "" {
		return fmt.Errorf("agent %s has no running task", agentID)
	}

	taskID := agent.TaskID
	agent.TaskID = ""
	agent.State = AgentStateIdle

	if r.logf != nil {
		r.logf("agent %s task killed: %s (silence)", agentID, taskID)
	}

	return nil
}

// FindAvailableAgents returns agents that can handle tasks with the given tags.
func (r *AgentRegistry) FindAvailableAgents(requiredTags []string) []*Agent {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var result []*Agent
	for _, agent := range r.agents {
		if agent.State != AgentStateIdle {
			continue
		}
		if len(requiredTags) == 0 {
			result = append(result, agent)
			continue
		}
		if r.matchesTags(agent.Tags, requiredTags) {
			result = append(result, agent)
		}
	}
	return result
}

// HasAgentWithTags checks if there's at least one available agent with the required tags.
func (r *AgentRegistry) HasAgentWithTags(requiredTags []string) bool {
	return len(r.FindAvailableAgents(requiredTags)) > 0
}

// Count returns the total number of registered agents.
func (r *AgentRegistry) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.agents)
}

// CountByState returns the number of agents in a given state.
func (r *AgentRegistry) CountByState(state AgentState) int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	count := 0
	for _, agent := range r.agents {
		if agent.State == state {
			count++
		}
	}
	return count
}

// IsStale checks if an agent is stale (hasn't sent a heartbeat in the given duration).
func (r *AgentRegistry) IsStale(agentID string, timeout time.Duration) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	agent, exists := r.agents[agentID]
	if !exists {
		return false
	}

	return time.Since(agent.LastHeartbeat) > timeout
}

// RemoveStaleAgents removes agents that haven't sent a heartbeat within the timeout.
func (r *AgentRegistry) RemoveStaleAgents(timeout time.Duration) []*Agent {
	r.mu.Lock()
	defer r.mu.Unlock()

	var stale []*Agent
	now := time.Now()

	for id, agent := range r.agents {
		if now.Sub(agent.LastHeartbeat) > timeout {
			stale = append(stale, agent)
			delete(r.agents, id)
			if r.logf != nil {
				r.logf("stale agent removed: %s", id)
			}
		}
	}

	return stale
}

// matchesTags checks if the agent's tags match all required tags.
func (r *AgentRegistry) matchesTags(agentTags, requiredTags []string) bool {
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
