package registry

import (
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"
)

func newTestRegistry(t *testing.T) *GuestRegistry {
	t.Helper()
	return NewGuestRegistry(0, func(format string, args ...interface{}) {})
}

func TestNewGuestRegistry(t *testing.T) {
	reg := NewGuestRegistry(0, nil)
	if reg == nil {
		t.Fatal("expected non-nil registry")
	}
	if reg.Count() != 0 {
		t.Errorf("expected 0 guests, got %d", reg.Count())
	}
}

func TestRegisterGuest(t *testing.T) {
	reg := newTestRegistry(t)

	guest, err := reg.Register("guest-1", "Test Guest", []string{"business-default", "frontend"})
	if err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	if guest.ID != "guest-1" {
		t.Errorf("expected id guest-1, got %s", guest.ID)
	}
	if guest.Name != "Test Guest" {
		t.Errorf("expected name Test Guest, got %s", guest.Name)
	}
	if len(guest.Tags) != 2 {
		t.Errorf("expected 2 tags, got %d", len(guest.Tags))
	}
	if guest.State != GuestStateIdle {
		t.Errorf("expected state IDLE, got %s", guest.State)
	}
	if reg.Count() != 1 {
		t.Errorf("expected 1 guest, got %d", reg.Count())
	}
}

func TestRegisterDuplicateGuest(t *testing.T) {
	reg := newTestRegistry(t)

	_, err := reg.Register("guest-1", "Test Guest", []string{"tag1"})
	if err != nil {
		t.Fatalf("first register failed: %v", err)
	}

	_, err = reg.Register("guest-1", "Another Guest", []string{"tag2"})
	if err == nil {
		t.Error("expected error for duplicate Guest ID, got nil")
	}
}

func TestRegisterMaxGuests(t *testing.T) {
	reg := NewGuestRegistry(2, nil)

	_, err := reg.Register("guest-1", "Guest 1", []string{"tag1"})
	if err != nil {
		t.Fatalf("first register failed: %v", err)
	}

	_, err = reg.Register("guest-2", "Guest 2", []string{"tag2"})
	if err != nil {
		t.Fatalf("second register failed: %v", err)
	}

	_, err = reg.Register("guest-3", "Guest 3", []string{"tag3"})
	if err == nil {
		t.Error("expected error when max guests reached, got nil")
	}
	if reg.Count() != 2 {
		t.Errorf("expected 2 guests, got %d", reg.Count())
	}
}

func TestUnregisterGuest(t *testing.T) {
	reg := newTestRegistry(t)

	_, err := reg.Register("guest-1", "Test Guest", []string{"tag1"})
	if err != nil {
		t.Fatalf("register failed: %v", err)
	}

	err = reg.Unregister("guest-1")
	if err != nil {
		t.Fatalf("unregister failed: %v", err)
	}

	if reg.Count() != 0 {
		t.Errorf("expected 0 guests, got %d", reg.Count())
	}
}

func TestUnregisterNonExistentGuest(t *testing.T) {
	reg := newTestRegistry(t)
	err := reg.Unregister("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent guest, got nil")
	}
}

func TestUnregisterRunningGuest(t *testing.T) {
	reg := newTestRegistry(t)

	_, err := reg.Register("guest-1", "Test Guest", []string{"tag1"})
	if err != nil {
		t.Fatalf("register failed: %v", err)
	}

	if err := reg.SetGuestTask("guest-1", "task-1"); err != nil {
		t.Fatalf("set guest task failed: %v", err)
	}

	err = reg.Unregister("guest-1")
	if err == nil {
		t.Error("expected error for unregistering running guest, got nil")
	}
}

func TestHeartbeat(t *testing.T) {
	reg := newTestRegistry(t)

	_, err := reg.Register("guest-1", "Test Guest", []string{"tag1"})
	if err != nil {
		t.Fatalf("register failed: %v", err)
	}

	initialHeartbeat := time.Now()
	time.Sleep(10 * time.Millisecond)

	err = reg.Heartbeat("guest-1")
	if err != nil {
		t.Fatalf("heartbeat failed: %v", err)
	}

	guest, ok := reg.GetGuest("guest-1")
	if !ok {
		t.Fatal("guest not found after heartbeat")
	}
	if guest.LastHeartbeat.Before(initialHeartbeat) {
		t.Error("heartbeat should update last heartbeat time")
	}
}

func TestHeartbeatNonExistentGuest(t *testing.T) {
	reg := newTestRegistry(t)
	err := reg.Heartbeat("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent guest heartbeat, got nil")
	}
}

func TestGetGuest(t *testing.T) {
	reg := newTestRegistry(t)

	_, err := reg.Register("guest-1", "Test Guest", []string{"tag1", "tag2"})
	if err != nil {
		t.Fatalf("register failed: %v", err)
	}

	guest, ok := reg.GetGuest("guest-1")
	if !ok {
		t.Fatal("expected guest to exist")
	}
	if guest.ID != "guest-1" {
		t.Errorf("expected id guest-1, got %s", guest.ID)
	}

	_, ok = reg.GetGuest("nonexistent")
	if ok {
		t.Error("expected nonexistent guest to not exist")
	}
}

func TestGetGuestsByState(t *testing.T) {
	reg := newTestRegistry(t)

	reg.Register("guest-1", "Guest 1", []string{"tag1"})
	reg.Register("guest-2", "Guest 2", []string{"tag2"})
	reg.Register("guest-3", "Guest 3", []string{"tag3"})

	guests := reg.GetGuests(GuestStateIdle)
	if len(guests) != 3 {
		t.Errorf("expected 3 idle guests, got %d", len(guests))
	}

	reg.SetGuestState("guest-1", GuestStateRunning)

	running := reg.GetGuests(GuestStateRunning)
	if len(running) != 1 {
		t.Errorf("expected 1 running guest, got %d", len(running))
	}
	if running[0].ID != "guest-1" {
		t.Errorf("expected running guest-1, got %s", running[0].ID)
	}

	idle := reg.GetGuests(GuestStateIdle)
	if len(idle) != 2 {
		t.Errorf("expected 2 idle guests, got %d", len(idle))
	}
}

func TestSetGuestState(t *testing.T) {
	reg := newTestRegistry(t)

	_, err := reg.Register("guest-1", "Test Guest", []string{"tag1"})
	if err != nil {
		t.Fatalf("register failed: %v", err)
	}

	err = reg.SetGuestState("guest-1", GuestStateRunning)
	if err != nil {
		t.Fatalf("set state failed: %v", err)
	}

	guest, _ := reg.GetGuest("guest-1")
	if guest.State != GuestStateRunning {
		t.Errorf("expected state RUNNING, got %s", guest.State)
	}

	err = reg.SetGuestState("nonexistent", GuestStateIdle)
	if err == nil {
		t.Error("expected error for nonexistent guest, got nil")
	}
}

func TestSetGuestTask(t *testing.T) {
	reg := newTestRegistry(t)

	_, err := reg.Register("guest-1", "Test Guest", []string{"tag1"})
	if err != nil {
		t.Fatalf("register failed: %v", err)
	}

	err = reg.SetGuestTask("guest-1", "task-1")
	if err != nil {
		t.Fatalf("set task failed: %v", err)
	}

	guest, _ := reg.GetGuest("guest-1")
	if guest.TaskID != "task-1" {
		t.Errorf("expected task_id task-1, got %s", guest.TaskID)
	}
	if guest.State != GuestStateRunning {
		t.Errorf("expected state RUNNING, got %s", guest.State)
	}

	err = reg.SetGuestTask("nonexistent", "task-2")
	if err == nil {
		t.Error("expected error for nonexistent guest, got nil")
	}
}

func TestClearGuestTask(t *testing.T) {
	reg := newTestRegistry(t)

	_, err := reg.Register("guest-1", "Test Guest", []string{"tag1"})
	if err != nil {
		t.Fatalf("register failed: %v", err)
	}

	reg.SetGuestTask("guest-1", "task-1")
	err = reg.ClearGuestTask("guest-1")
	if err != nil {
		t.Fatalf("clear task failed: %v", err)
	}

	guest, _ := reg.GetGuest("guest-1")
	if guest.TaskID != "" {
		t.Errorf("expected empty task_id, got %s", guest.TaskID)
	}
	if guest.State != GuestStateIdle {
		t.Errorf("expected state IDLE, got %s", guest.State)
	}
}

func TestFindAvailableGuests(t *testing.T) {
	reg := newTestRegistry(t)

	reg.Register("guest-1", "Guest 1", []string{"business-default", "frontend"})
	reg.Register("guest-2", "Guest 2", []string{"business-default", "android"})
	reg.Register("guest-3", "Guest 3", []string{"frontend"})

	reg.SetGuestState("guest-2", GuestStateRunning)

	guests := reg.FindAvailableGuests([]string{"business-default"})
	if len(guests) != 1 {
		t.Errorf("expected 1 available guest with business-default, got %d", len(guests))
	}
	if guests[0].ID != "guest-1" {
		t.Errorf("expected guest-1, got %s", guests[0].ID)
	}

	guests = reg.FindAvailableGuests([]string{})
	if len(guests) != 2 {
		t.Errorf("expected 2 available guests with no tag requirements, got %d", len(guests))
	}

	guests = reg.FindAvailableGuests([]string{"nonexistent"})
	if len(guests) != 0 {
		t.Errorf("expected 0 available guests with nonexistent tag, got %d", len(guests))
	}

	guests = reg.FindAvailableGuests([]string{"business-default", "frontend"})
	if len(guests) != 1 {
		t.Errorf("expected 1 guest with both tags, got %d", len(guests))
	}
	if guests[0].ID != "guest-1" {
		t.Errorf("expected guest-1, got %s", guests[0].ID)
	}
}

func TestHasGuestWithTags(t *testing.T) {
	reg := newTestRegistry(t)

	reg.Register("guest-1", "Guest 1", []string{"business-default"})

	if !reg.HasGuestWithTags([]string{"business-default"}) {
		t.Error("expected to have guest with business-default tag")
	}

	if reg.HasGuestWithTags([]string{"android"}) {
		t.Error("expected not to have guest with android tag")
	}
}

func TestIsStale(t *testing.T) {
	reg := newTestRegistry(t)

	reg.Register("guest-1", "Test Guest", []string{"tag1"})

	if reg.IsStale("guest-1", 5*time.Second) {
		t.Error("expected guest not to be stale immediately")
	}

	// Wait for guest to become stale
	time.Sleep(50 * time.Millisecond)
	if !reg.IsStale("guest-1", 10*time.Millisecond) {
		t.Error("expected guest to be stale with 10ms timeout")
	}

	if reg.IsStale("nonexistent", 5*time.Second) {
		t.Error("expected nonexistent guest not to be stale")
	}
}

func TestRemoveStaleGuests(t *testing.T) {
	reg := newTestRegistry(t)

	reg.Register("guest-1", "Guest 1", []string{"tag1"})
	time.Sleep(50 * time.Millisecond)
	reg.Register("guest-2", "Guest 2", []string{"tag2"})

	stale := reg.RemoveStaleGuests(20 * time.Millisecond)
	if len(stale) != 1 {
		t.Errorf("expected 1 stale guest, got %d", len(stale))
	}
	if len(stale) > 0 && stale[0].ID != "guest-1" {
		t.Errorf("expected stale guest-1, got %s", stale[0].ID)
	}

	if reg.Count() != 1 {
		t.Errorf("expected 1 guest remaining, got %d", reg.Count())
	}
}

func TestCountByState(t *testing.T) {
	reg := newTestRegistry(t)

	reg.Register("guest-1", "Guest 1", []string{"tag1"})
	reg.Register("guest-2", "Guest 2", []string{"tag2"})
	reg.Register("guest-3", "Guest 3", []string{"tag3"})

	if reg.CountByState(GuestStateIdle) != 3 {
		t.Errorf("expected 3 idle guests, got %d", reg.CountByState(GuestStateIdle))
	}
	if reg.CountByState(GuestStateRunning) != 0 {
		t.Errorf("expected 0 running guests, got %d", reg.CountByState(GuestStateRunning))
	}

	reg.SetGuestState("guest-1", GuestStateRunning)
	reg.SetGuestState("guest-2", GuestStateRunning)

	if reg.CountByState(GuestStateIdle) != 1 {
		t.Errorf("expected 1 idle guest, got %d", reg.CountByState(GuestStateIdle))
	}
	if reg.CountByState(GuestStateRunning) != 2 {
		t.Errorf("expected 2 running guests, got %d", reg.CountByState(GuestStateRunning))
	}
}

func TestGuestStateMarshalJSON(t *testing.T) {
	tests := []struct {
		name     string
		state    GuestState
		expected string
	}{
		{"Disconnected", GuestStateDisconnected, `"DISCONNECTED"`},
		{"Registered", GuestStateRegistered, `"REGISTERED"`},
		{"Idle", GuestStateIdle, `"IDLE"`},
		{"Running", GuestStateRunning, `"RUNNING"`},
		{"Unknown (99)", GuestState(99), `"UNKNOWN"`},
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

func TestGuestStateUnmarshalJSON(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected GuestState
	}{
		{"DISCONNECTED", `"DISCONNECTED"`, GuestStateDisconnected},
		{"REGISTERED", `"REGISTERED"`, GuestStateRegistered},
		{"IDLE", `"IDLE"`, GuestStateIdle},
		{"RUNNING", `"RUNNING"`, GuestStateRunning},
		{"Unknown string", `"UNKNOWN"`, GuestState(0)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var state GuestState
			if err := json.Unmarshal([]byte(tt.input), &state); err != nil {
				t.Fatalf("UnmarshalJSON failed: %v", err)
			}
			if state != tt.expected {
				t.Errorf("expected %d, got %d", tt.expected, state)
			}
		})
	}
}

func TestGuestStateRoundTrip(t *testing.T) {
	states := []GuestState{
		GuestStateDisconnected,
		GuestStateRegistered,
		GuestStateIdle,
		GuestStateRunning,
	}

	for _, original := range states {
		data, err := json.Marshal(original)
		if err != nil {
			t.Fatalf("Marshal failed for %s: %v", original, err)
		}

		var decoded GuestState
		if err := json.Unmarshal(data, &decoded); err != nil {
			t.Fatalf("Unmarshal failed for %s: %v", string(data), err)
		}

		if decoded != original {
			t.Errorf("round-trip failed: %s -> %s -> %s", original, string(data), decoded)
		}
	}
}

func TestGuestStateUnmarshalJSON_Invalid(t *testing.T) {
	var state GuestState
	err := json.Unmarshal([]byte(`"INVALID_STATE"`), &state)
	if err != nil {
		t.Fatalf("expected no error for invalid string, got: %v", err)
	}
	// Unknown strings should fall through to zero value
	if state != GuestState(0) {
		t.Errorf("expected state 0 for unknown string, got %d", state)
	}
}

func TestGuestStateUnmarshalJSON_NonString(t *testing.T) {
	var state GuestState
	err := json.Unmarshal([]byte("42"), &state)
	if err == nil {
		t.Error("expected error for non-string JSON value, got nil")
	}
}

func TestGuestStateString(t *testing.T) {
	tests := []struct {
		state    GuestState
		expected string
	}{
		{GuestStateDisconnected, "DISCONNECTED"},
		{GuestStateRegistered, "REGISTERED"},
		{GuestStateIdle, "IDLE"},
		{GuestStateRunning, "RUNNING"},
		{GuestState(99), "UNKNOWN"},
	}

	for _, tt := range tests {
		if tt.state.String() != tt.expected {
			t.Errorf("GuestState(%d).String() = %s, want %s", tt.state, tt.state.String(), tt.expected)
		}
	}
}

func TestKillRunningGuestTask(t *testing.T) {
	reg := newTestRegistry(t)

	reg.Register("guest-1", "Guest 1", []string{"tag1"})
	reg.SetGuestTask("guest-1", "task-1")

	guest, _ := reg.GetGuest("guest-1")
	if guest.TaskID != "task-1" {
		t.Fatalf("expected task-1, got %s", guest.TaskID)
	}

	err := reg.KillRunningGuestTask("guest-1")
	if err != nil {
		t.Fatalf("KillRunningGuestTask failed: %v", err)
	}

	guest, _ = reg.GetGuest("guest-1")
	if guest.TaskID != "" {
		t.Errorf("expected empty task_id after kill, got %s", guest.TaskID)
	}
	if guest.State != GuestStateIdle {
		t.Errorf("expected IDLE state after kill, got %s", guest.State)
	}
}

func TestKillRunningGuestTask_NoTask(t *testing.T) {
	reg := newTestRegistry(t)

	reg.Register("guest-1", "Guest 1", []string{"tag1"})

	err := reg.KillRunningGuestTask("guest-1")
	if err == nil {
		t.Error("expected error when killing task for guest with no running task")
	}
}

func TestKillRunningGuestTask_NonExistent(t *testing.T) {
	reg := newTestRegistry(t)

	err := reg.KillRunningGuestTask("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent guest")
	}
}

func TestGetAllGuests(t *testing.T) {
	reg := newTestRegistry(t)

	reg.Register("guest-1", "Guest 1", []string{"tag1"})
	reg.Register("guest-2", "Guest 2", []string{"tag2"})
	reg.Register("guest-3", "Guest 3", []string{"tag3"})

	guests := reg.GetAllGuests()
	if len(guests) != 3 {
		t.Errorf("expected 3 guests, got %d", len(guests))
	}
}

func TestTaskHeartbeat(t *testing.T) {
	reg := newTestRegistry(t)

	_, err := reg.Register("guest-1", "Test Guest", []string{"tag1"})
	if err != nil {
		t.Fatalf("register failed: %v", err)
	}

	// Assign a task to the guest
	err = reg.SetGuestTask("guest-1", "task-1")
	if err != nil {
		t.Fatalf("set task failed: %v", err)
	}

	// LastTaskHeartbeat should be zero after assignment
	guest, _ := reg.GetGuest("guest-1")
	if !guest.LastTaskHeartbeat.IsZero() {
		t.Errorf("expected zero LastTaskHeartbeat after assignment, got %v", guest.LastTaskHeartbeat)
	}

	// Heartbeat with the task
	err = reg.TaskHeartbeat("guest-1", "task-1")
	if err != nil {
		t.Fatalf("TaskHeartbeat failed: %v", err)
	}

	// LastTaskHeartbeat should now be set
	guest, _ = reg.GetGuest("guest-1")
	if guest.LastTaskHeartbeat.IsZero() {
		t.Error("expected LastTaskHeartbeat to be set after TaskHeartbeat")
	}
	if guest.LastHeartbeat.IsZero() {
		t.Error("expected LastHeartbeat to be updated after TaskHeartbeat")
	}
}

func TestTaskHeartbeat_NonExistentGuest(t *testing.T) {
	reg := newTestRegistry(t)

	err := reg.TaskHeartbeat("nonexistent", "task-1")
	if err == nil {
		t.Error("expected error for nonexistent guest, got nil")
	}
}

func TestTaskHeartbeat_UpdatesTaskID(t *testing.T) {
	reg := newTestRegistry(t)

	reg.Register("guest-1", "Test Guest", []string{"tag1"})

	// Heartbeat with a task ID (simulating guest reporting its task)
	err := reg.TaskHeartbeat("guest-1", "task-1")
	if err != nil {
		t.Fatalf("TaskHeartbeat failed: %v", err)
	}

	guest, _ := reg.GetGuest("guest-1")
	if guest.TaskID != "task-1" {
		t.Errorf("expected task_id 'task-1', got %s", guest.TaskID)
	}
}

func TestGetStuckGuests_NoStuckGuests(t *testing.T) {
	reg := newTestRegistry(t)

	reg.Register("guest-1", "Guest 1", []string{"tag1"})
	reg.SetGuestTask("guest-1", "task-1")

	// Immediately heartbeat with the task — should not be stuck
	reg.TaskHeartbeat("guest-1", "task-1")

	stuck := reg.GetStuckGuests(1 * time.Second)
	if len(stuck) != 0 {
		t.Errorf("expected 0 stuck guests, got %d", len(stuck))
	}
}

func TestGetStuckGuests_DetectsStuckAssignment(t *testing.T) {
	reg := newTestRegistry(t)

	reg.Register("guest-1", "Guest 1", []string{"tag1"})
	reg.SetGuestTask("guest-1", "task-1")

	// Guest never heartbeated with the task — should be stuck
	time.Sleep(10 * time.Millisecond)

	stuck := reg.GetStuckGuests(5 * time.Millisecond)
	if len(stuck) != 1 {
		t.Errorf("expected 1 stuck guest, got %d", len(stuck))
	}
	if len(stuck) > 0 && stuck[0].ID != "guest-1" {
		t.Errorf("expected stuck guest-1, got %s", stuck[0].ID)
	}
	if len(stuck) > 0 && stuck[0].TaskID != "task-1" {
		t.Errorf("expected stuck task-1, got %s", stuck[0].TaskID)
	}
}

func TestGetStuckGuests_IgnoresGuestsWithoutTasks(t *testing.T) {
	reg := newTestRegistry(t)

	reg.Register("guest-1", "Guest 1", []string{"tag1"})
	// No task assigned

	stuck := reg.GetStuckGuests(1 * time.Millisecond)
	if len(stuck) != 0 {
		t.Errorf("expected 0 stuck guests, got %d", len(stuck))
	}
}

func TestGetStuckGuests_MultipleGuests(t *testing.T) {
	reg := newTestRegistry(t)

	reg.Register("guest-1", "Guest 1", []string{"tag1"})
	reg.Register("guest-2", "Guest 2", []string{"tag1"})
	reg.Register("guest-3", "Guest 3", []string{"tag1"})

	// guest-1: assigned task, never heartbeated (stuck)
	reg.SetGuestTask("guest-1", "task-1")

	// guest-2: assigned task, heartbeated recently (not stuck)
	reg.SetGuestTask("guest-2", "task-2")
	reg.TaskHeartbeat("guest-2", "task-2")

	// guest-3: no task (not stuck)

	// Sleep briefly so guest-1 (no heartbeat) is clearly stuck.
	// guest-2 heartbeated just before this, so use a timeout larger
	// than the sleep to ensure guest-2 is not detected as stuck.
	time.Sleep(10 * time.Millisecond)

	stuck := reg.GetStuckGuests(20 * time.Millisecond)
	if len(stuck) != 1 {
		t.Errorf("expected 1 stuck guest, got %d", len(stuck))
	}
	if len(stuck) > 0 && stuck[0].ID != "guest-1" {
		t.Errorf("expected stuck guest-1, got %s", stuck[0].ID)
	}
}

func TestSetGuestTask_ResetsLastTaskHeartbeat(t *testing.T) {
	reg := newTestRegistry(t)

	reg.Register("guest-1", "Guest 1", []string{"tag1"})

	// Heartbeat with a task
	reg.TaskHeartbeat("guest-1", "task-1")
	guest, _ := reg.GetGuest("guest-1")
	if guest.LastTaskHeartbeat.IsZero() {
		t.Fatal("expected LastTaskHeartbeat to be set")
	}

	// Assign a new task — should reset LastTaskHeartbeat
	reg.SetGuestTask("guest-1", "task-2")
	guest, _ = reg.GetGuest("guest-1")
	if !guest.LastTaskHeartbeat.IsZero() {
		t.Errorf("expected zero LastTaskHeartbeat after new assignment, got %v", guest.LastTaskHeartbeat)
	}
}

func TestClearGuestTask_ResetsLastTaskHeartbeat(t *testing.T) {
	reg := newTestRegistry(t)

	reg.Register("guest-1", "Guest 1", []string{"tag1"})
	reg.TaskHeartbeat("guest-1", "task-1")

	reg.ClearGuestTask("guest-1")
	guest, _ := reg.GetGuest("guest-1")
	if !guest.LastTaskHeartbeat.IsZero() {
		t.Errorf("expected zero LastTaskHeartbeat after clear, got %v", guest.LastTaskHeartbeat)
	}
}

func TestSetLastTaskHeartbeat(t *testing.T) {
	reg := newTestRegistry(t)

	reg.Register("guest-1", "Guest 1", []string{"tag1"})

	past := time.Now().Add(-1 * time.Minute)
	err := reg.SetLastTaskHeartbeat("guest-1", past)
	if err != nil {
		t.Fatalf("SetLastTaskHeartbeat failed: %v", err)
	}

	guest, _ := reg.GetGuest("guest-1")
	if guest.LastTaskHeartbeat.IsZero() {
		t.Error("expected LastTaskHeartbeat to be set")
	}
}

func TestSetLastTaskHeartbeat_NonExistent(t *testing.T) {
	reg := newTestRegistry(t)

	err := reg.SetLastTaskHeartbeat("nonexistent", time.Time{})
	if err == nil {
		t.Error("expected error for nonexistent guest, got nil")
	}
}

func TestRegistryConcurrency(t *testing.T) {
	reg := NewGuestRegistry(100, nil)
	var wg sync.WaitGroup

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			reg.Register(fmt.Sprintf("guest-%d", id), fmt.Sprintf("Guest %d", id), []string{"tag"})
		}(i)
	}

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			reg.Heartbeat(fmt.Sprintf("guest-%d", id))
		}(i)
	}

	for i := 0; i < 25; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			reg.SetGuestState(fmt.Sprintf("guest-%d", id), GuestStateRunning)
		}(i)
	}

	wg.Wait()

	if reg.Count() != 50 {
		t.Errorf("expected 50 guests, got %d", reg.Count())
	}
}
