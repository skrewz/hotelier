package registry

import (
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"
)

func newTestRegistry(t *testing.T) *AgentRegistry {
	t.Helper()
	return NewAgentRegistry(0, func(format string, args ...interface{}) {})
}

func TestNewAgentRegistry(t *testing.T) {
	reg := NewAgentRegistry(0, nil)
	if reg == nil {
		t.Fatal("expected non-nil registry")
	}
	if reg.Count() != 0 {
		t.Errorf("expected 0 agents, got %d", reg.Count())
	}
}

func TestRegisterAgent(t *testing.T) {
	reg := newTestRegistry(t)

	agent, err := reg.Register("agent-1", "Test Agent", []string{"business-default", "frontend"})
	if err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	if agent.ID != "agent-1" {
		t.Errorf("expected id agent-1, got %s", agent.ID)
	}
	if agent.Name != "Test Agent" {
		t.Errorf("expected name Test Agent, got %s", agent.Name)
	}
	if len(agent.Tags) != 2 {
		t.Errorf("expected 2 tags, got %d", len(agent.Tags))
	}
	if agent.State != AgentStateIdle {
		t.Errorf("expected state IDLE, got %s", agent.State)
	}
	if reg.Count() != 1 {
		t.Errorf("expected 1 agent, got %d", reg.Count())
	}
}

func TestRegisterDuplicateAgent(t *testing.T) {
	reg := newTestRegistry(t)

	_, err := reg.Register("agent-1", "Test Agent", []string{"tag1"})
	if err != nil {
		t.Fatalf("first register failed: %v", err)
	}

	_, err = reg.Register("agent-1", "Another Agent", []string{"tag2"})
	if err == nil {
		t.Error("expected error for duplicate agent ID, got nil")
	}
}

func TestRegisterMaxAgents(t *testing.T) {
	reg := NewAgentRegistry(2, nil)

	_, err := reg.Register("agent-1", "Agent 1", []string{"tag1"})
	if err != nil {
		t.Fatalf("first register failed: %v", err)
	}

	_, err = reg.Register("agent-2", "Agent 2", []string{"tag2"})
	if err != nil {
		t.Fatalf("second register failed: %v", err)
	}

	_, err = reg.Register("agent-3", "Agent 3", []string{"tag3"})
	if err == nil {
		t.Error("expected error when max agents reached, got nil")
	}
	if reg.Count() != 2 {
		t.Errorf("expected 2 agents, got %d", reg.Count())
	}
}

func TestUnregisterAgent(t *testing.T) {
	reg := newTestRegistry(t)

	_, err := reg.Register("agent-1", "Test Agent", []string{"tag1"})
	if err != nil {
		t.Fatalf("register failed: %v", err)
	}

	err = reg.Unregister("agent-1")
	if err != nil {
		t.Fatalf("unregister failed: %v", err)
	}

	if reg.Count() != 0 {
		t.Errorf("expected 0 agents, got %d", reg.Count())
	}
}

func TestUnregisterNonExistentAgent(t *testing.T) {
	reg := newTestRegistry(t)
	err := reg.Unregister("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent agent, got nil")
	}
}

func TestUnregisterRunningAgent(t *testing.T) {
	reg := newTestRegistry(t)

	_, err := reg.Register("agent-1", "Test Agent", []string{"tag1"})
	if err != nil {
		t.Fatalf("register failed: %v", err)
	}

	if err := reg.SetAgentTask("agent-1", "task-1"); err != nil {
		t.Fatalf("set agent task failed: %v", err)
	}

	err = reg.Unregister("agent-1")
	if err == nil {
		t.Error("expected error for unregistering running agent, got nil")
	}
}

func TestHeartbeat(t *testing.T) {
	reg := newTestRegistry(t)

	_, err := reg.Register("agent-1", "Test Agent", []string{"tag1"})
	if err != nil {
		t.Fatalf("register failed: %v", err)
	}

	initialHeartbeat := time.Now()
	time.Sleep(10 * time.Millisecond)

	err = reg.Heartbeat("agent-1")
	if err != nil {
		t.Fatalf("heartbeat failed: %v", err)
	}

	agent, ok := reg.GetAgent("agent-1")
	if !ok {
		t.Fatal("agent not found after heartbeat")
	}
	if agent.LastHeartbeat.Before(initialHeartbeat) {
		t.Error("heartbeat should update last heartbeat time")
	}
}

func TestHeartbeatNonExistentAgent(t *testing.T) {
	reg := newTestRegistry(t)
	err := reg.Heartbeat("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent agent heartbeat, got nil")
	}
}

func TestGetAgent(t *testing.T) {
	reg := newTestRegistry(t)

	_, err := reg.Register("agent-1", "Test Agent", []string{"tag1", "tag2"})
	if err != nil {
		t.Fatalf("register failed: %v", err)
	}

	agent, ok := reg.GetAgent("agent-1")
	if !ok {
		t.Fatal("expected agent to exist")
	}
	if agent.ID != "agent-1" {
		t.Errorf("expected id agent-1, got %s", agent.ID)
	}

	_, ok = reg.GetAgent("nonexistent")
	if ok {
		t.Error("expected nonexistent agent to not exist")
	}
}

func TestGetAgentsByState(t *testing.T) {
	reg := newTestRegistry(t)

	reg.Register("agent-1", "Agent 1", []string{"tag1"})
	reg.Register("agent-2", "Agent 2", []string{"tag2"})
	reg.Register("agent-3", "Agent 3", []string{"tag3"})

	agents := reg.GetAgents(AgentStateIdle)
	if len(agents) != 3 {
		t.Errorf("expected 3 idle agents, got %d", len(agents))
	}

	reg.SetAgentState("agent-1", AgentStateRunning)

	running := reg.GetAgents(AgentStateRunning)
	if len(running) != 1 {
		t.Errorf("expected 1 running agent, got %d", len(running))
	}
	if running[0].ID != "agent-1" {
		t.Errorf("expected running agent-1, got %s", running[0].ID)
	}

	idle := reg.GetAgents(AgentStateIdle)
	if len(idle) != 2 {
		t.Errorf("expected 2 idle agents, got %d", len(idle))
	}
}

func TestSetAgentState(t *testing.T) {
	reg := newTestRegistry(t)

	_, err := reg.Register("agent-1", "Test Agent", []string{"tag1"})
	if err != nil {
		t.Fatalf("register failed: %v", err)
	}

	err = reg.SetAgentState("agent-1", AgentStateRunning)
	if err != nil {
		t.Fatalf("set state failed: %v", err)
	}

	agent, _ := reg.GetAgent("agent-1")
	if agent.State != AgentStateRunning {
		t.Errorf("expected state RUNNING, got %s", agent.State)
	}

	err = reg.SetAgentState("nonexistent", AgentStateIdle)
	if err == nil {
		t.Error("expected error for nonexistent agent, got nil")
	}
}

func TestSetAgentTask(t *testing.T) {
	reg := newTestRegistry(t)

	_, err := reg.Register("agent-1", "Test Agent", []string{"tag1"})
	if err != nil {
		t.Fatalf("register failed: %v", err)
	}

	err = reg.SetAgentTask("agent-1", "task-1")
	if err != nil {
		t.Fatalf("set task failed: %v", err)
	}

	agent, _ := reg.GetAgent("agent-1")
	if agent.TaskID != "task-1" {
		t.Errorf("expected task_id task-1, got %s", agent.TaskID)
	}
	if agent.State != AgentStateRunning {
		t.Errorf("expected state RUNNING, got %s", agent.State)
	}

	err = reg.SetAgentTask("nonexistent", "task-2")
	if err == nil {
		t.Error("expected error for nonexistent agent, got nil")
	}
}

func TestClearAgentTask(t *testing.T) {
	reg := newTestRegistry(t)

	_, err := reg.Register("agent-1", "Test Agent", []string{"tag1"})
	if err != nil {
		t.Fatalf("register failed: %v", err)
	}

	reg.SetAgentTask("agent-1", "task-1")
	err = reg.ClearAgentTask("agent-1")
	if err != nil {
		t.Fatalf("clear task failed: %v", err)
	}

	agent, _ := reg.GetAgent("agent-1")
	if agent.TaskID != "" {
		t.Errorf("expected empty task_id, got %s", agent.TaskID)
	}
	if agent.State != AgentStateIdle {
		t.Errorf("expected state IDLE, got %s", agent.State)
	}
}

func TestFindAvailableAgents(t *testing.T) {
	reg := newTestRegistry(t)

	reg.Register("agent-1", "Agent 1", []string{"business-default", "frontend"})
	reg.Register("agent-2", "Agent 2", []string{"business-default", "android"})
	reg.Register("agent-3", "Agent 3", []string{"frontend"})

	reg.SetAgentState("agent-2", AgentStateRunning)

	agents := reg.FindAvailableAgents([]string{"business-default"})
	if len(agents) != 1 {
		t.Errorf("expected 1 available agent with business-default, got %d", len(agents))
	}
	if agents[0].ID != "agent-1" {
		t.Errorf("expected agent-1, got %s", agents[0].ID)
	}

	agents = reg.FindAvailableAgents([]string{})
	if len(agents) != 2 {
		t.Errorf("expected 2 available agents with no tag requirements, got %d", len(agents))
	}

	agents = reg.FindAvailableAgents([]string{"nonexistent"})
	if len(agents) != 0 {
		t.Errorf("expected 0 available agents with nonexistent tag, got %d", len(agents))
	}

	agents = reg.FindAvailableAgents([]string{"business-default", "frontend"})
	if len(agents) != 1 {
		t.Errorf("expected 1 agent with both tags, got %d", len(agents))
	}
	if agents[0].ID != "agent-1" {
		t.Errorf("expected agent-1, got %s", agents[0].ID)
	}
}

func TestHasAgentWithTags(t *testing.T) {
	reg := newTestRegistry(t)

	reg.Register("agent-1", "Agent 1", []string{"business-default"})

	if !reg.HasAgentWithTags([]string{"business-default"}) {
		t.Error("expected to have agent with business-default tag")
	}

	if reg.HasAgentWithTags([]string{"android"}) {
		t.Error("expected not to have agent with android tag")
	}
}

func TestIsStale(t *testing.T) {
	reg := newTestRegistry(t)

	reg.Register("agent-1", "Test Agent", []string{"tag1"})

	if reg.IsStale("agent-1", 5*time.Second) {
		t.Error("expected agent not to be stale immediately")
	}

	// Wait for agent to become stale
	time.Sleep(50 * time.Millisecond)
	if !reg.IsStale("agent-1", 10*time.Millisecond) {
		t.Error("expected agent to be stale with 10ms timeout")
	}

	if reg.IsStale("nonexistent", 5*time.Second) {
		t.Error("expected nonexistent agent not to be stale")
	}
}

func TestRemoveStaleAgents(t *testing.T) {
	reg := newTestRegistry(t)

	reg.Register("agent-1", "Agent 1", []string{"tag1"})
	time.Sleep(50 * time.Millisecond)
	reg.Register("agent-2", "Agent 2", []string{"tag2"})

	stale := reg.RemoveStaleAgents(20 * time.Millisecond)
	if len(stale) != 1 {
		t.Errorf("expected 1 stale agent, got %d", len(stale))
	}
	if len(stale) > 0 && stale[0].ID != "agent-1" {
		t.Errorf("expected stale agent-1, got %s", stale[0].ID)
	}

	if reg.Count() != 1 {
		t.Errorf("expected 1 agent remaining, got %d", reg.Count())
	}
}

func TestCountByState(t *testing.T) {
	reg := newTestRegistry(t)

	reg.Register("agent-1", "Agent 1", []string{"tag1"})
	reg.Register("agent-2", "Agent 2", []string{"tag2"})
	reg.Register("agent-3", "Agent 3", []string{"tag3"})

	if reg.CountByState(AgentStateIdle) != 3 {
		t.Errorf("expected 3 idle agents, got %d", reg.CountByState(AgentStateIdle))
	}
	if reg.CountByState(AgentStateRunning) != 0 {
		t.Errorf("expected 0 running agents, got %d", reg.CountByState(AgentStateRunning))
	}

	reg.SetAgentState("agent-1", AgentStateRunning)
	reg.SetAgentState("agent-2", AgentStateRunning)

	if reg.CountByState(AgentStateIdle) != 1 {
		t.Errorf("expected 1 idle agent, got %d", reg.CountByState(AgentStateIdle))
	}
	if reg.CountByState(AgentStateRunning) != 2 {
		t.Errorf("expected 2 running agents, got %d", reg.CountByState(AgentStateRunning))
	}
}

func TestAgentStateMarshalJSON(t *testing.T) {
	tests := []struct {
		name     string
		state    AgentState
		expected string
	}{
		{"Disconnected", AgentStateDisconnected, `"DISCONNECTED"`},
		{"Registered", AgentStateRegistered, `"REGISTERED"`},
		{"Idle", AgentStateIdle, `"IDLE"`},
		{"Running", AgentStateRunning, `"RUNNING"`},
		{"Unknown (99)", AgentState(99), `"UNKNOWN"`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := json.Marshal(tt.state)
			if err != nil {
				t.Fatalf("MarshalJSON failed: %v", err)
			}
			if string(data) != tt.expected {
				t.Errorf("expected %s, got %s", tt.expected, string(data))
			}
		})
	}
}

func TestAgentStateUnmarshalJSON(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected AgentState
	}{
		{"DISCONNECTED", `"DISCONNECTED"`, AgentStateDisconnected},
		{"REGISTERED", `"REGISTERED"`, AgentStateRegistered},
		{"IDLE", `"IDLE"`, AgentStateIdle},
		{"RUNNING", `"RUNNING"`, AgentStateRunning},
		{"Unknown string", `"UNKNOWN"`, AgentState(0)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var state AgentState
			if err := json.Unmarshal([]byte(tt.input), &state); err != nil {
				t.Fatalf("UnmarshalJSON failed: %v", err)
			}
			if state != tt.expected {
				t.Errorf("expected %d, got %d", tt.expected, state)
			}
		})
	}
}

func TestAgentStateRoundTrip(t *testing.T) {
	states := []AgentState{
		AgentStateDisconnected,
		AgentStateRegistered,
		AgentStateIdle,
		AgentStateRunning,
	}

	for _, original := range states {
		data, err := json.Marshal(original)
		if err != nil {
			t.Fatalf("Marshal failed for %s: %v", original, err)
		}

		var decoded AgentState
		if err := json.Unmarshal(data, &decoded); err != nil {
			t.Fatalf("Unmarshal failed for %s: %v", string(data), err)
		}

		if decoded != original {
			t.Errorf("round-trip failed: %s -> %s -> %s", original, string(data), decoded)
		}
	}
}

func TestAgentStateUnmarshalJSON_Invalid(t *testing.T) {
	var state AgentState
	err := json.Unmarshal([]byte(`"INVALID_STATE"`), &state)
	if err != nil {
		t.Fatalf("expected no error for invalid string, got: %v", err)
	}
	// Unknown strings should fall through to zero value
	if state != AgentState(0) {
		t.Errorf("expected state 0 for unknown string, got %d", state)
	}
}

func TestAgentStateUnmarshalJSON_NonString(t *testing.T) {
	var state AgentState
	err := json.Unmarshal([]byte("42"), &state)
	if err == nil {
		t.Error("expected error for non-string JSON value, got nil")
	}
}

func TestAgentStateString(t *testing.T) {
	tests := []struct {
		state    AgentState
		expected string
	}{
		{AgentStateDisconnected, "DISCONNECTED"},
		{AgentStateRegistered, "REGISTERED"},
		{AgentStateIdle, "IDLE"},
		{AgentStateRunning, "RUNNING"},
		{AgentState(99), "UNKNOWN"},
	}

	for _, tt := range tests {
		if tt.state.String() != tt.expected {
			t.Errorf("AgentState(%d).String() = %s, want %s", tt.state, tt.state.String(), tt.expected)
		}
	}
}

func TestGetAllAgents(t *testing.T) {
	reg := newTestRegistry(t)

	reg.Register("agent-1", "Agent 1", []string{"tag1"})
	reg.Register("agent-2", "Agent 2", []string{"tag2"})
	reg.Register("agent-3", "Agent 3", []string{"tag3"})

	agents := reg.GetAllAgents()
	if len(agents) != 3 {
		t.Errorf("expected 3 agents, got %d", len(agents))
	}
}

func TestRegistryConcurrency(t *testing.T) {
	reg := NewAgentRegistry(100, nil)
	var wg sync.WaitGroup

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			reg.Register(fmt.Sprintf("agent-%d", id), fmt.Sprintf("Agent %d", id), []string{"tag"})
		}(i)
	}

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			reg.Heartbeat(fmt.Sprintf("agent-%d", id))
		}(i)
	}

	for i := 0; i < 25; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			reg.SetAgentState(fmt.Sprintf("agent-%d", id), AgentStateRunning)
		}(i)
	}

	wg.Wait()

	if reg.Count() != 50 {
		t.Errorf("expected 50 agents, got %d", reg.Count())
	}
}
