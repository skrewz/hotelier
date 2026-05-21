package registry

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// GuestState represents the state of a guest.
type GuestState int

const (
	GuestStateDisconnected GuestState = iota
	GuestStateRegistered
	GuestStateIdle
	GuestStateRunning
)

func (s GuestState) String() string {
	switch s {
	case GuestStateDisconnected:
		return "DISCONNECTED"
	case GuestStateRegistered:
		return "REGISTERED"
	case GuestStateIdle:
		return "IDLE"
	case GuestStateRunning:
		return "RUNNING"
	default:
		return "UNKNOWN"
	}
}

// MarshalJSON implements json.Marshaler for GuestState.
// It serializes the state as a string (e.g. "IDLE") rather than an int,
// so the frontend can use it directly without type coercion.
func (s GuestState) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.String())
}

// UnmarshalJSON implements json.Unmarshaler for GuestState.
func (s *GuestState) UnmarshalJSON(data []byte) error {
	var str string
	if err := json.Unmarshal(data, &str); err != nil {
		return err
	}
	switch str {
	case "DISCONNECTED":
		*s = GuestStateDisconnected
	case "REGISTERED":
		*s = GuestStateRegistered
	case "IDLE":
		*s = GuestStateIdle
	case "RUNNING":
		*s = GuestStateRunning
	default:
		*s = 0 // unknown state, store as zero value
	}
	return nil
}

// Guest represents a connected guest in the system.
type Guest struct {
	ID            string     `json:"id"`
	Name          string     `json:"name"`
	Tags          []string   `json:"tags"`
	State         GuestState `json:"state"`
	ConnectedAt   time.Time  `json:"connected_at"`
	LastHeartbeat time.Time  `json:"last_heartbeat"`
	TaskID        string     `json:"task_id,omitempty"`
}

// GuestRegistry manages the lifecycle of connected guests.
type GuestRegistry struct {
	guests    map[string]*Guest
	mu        sync.RWMutex
	maxGuests int
	logf      func(format string, args ...interface{})
}

// NewGuestRegistry creates a new guest registry.
func NewGuestRegistry(maxGuests int, logf func(format string, args ...interface{})) *GuestRegistry {
	return &GuestRegistry{
		guests:    make(map[string]*Guest),
		maxGuests: maxGuests,
		logf:      logf,
	}
}

// SetMaxGuests updates the maximum number of guests allowed (0 = unlimited).
func (r *GuestRegistry) SetMaxGuests(maxGuests int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.maxGuests = maxGuests
}

// Register adds a new guest to the registry.
func (r *GuestRegistry) Register(guestID, name string, tags []string) (*Guest, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.maxGuests > 0 && len(r.guests) >= r.maxGuests {
		return nil, fmt.Errorf("maximum number of guests (%d) reached", r.maxGuests)
	}

	if _, exists := r.guests[guestID]; exists {
		return nil, fmt.Errorf("guest %s already registered", guestID)
	}

	guest := &Guest{
		ID:            guestID,
		Name:          name,
		Tags:          tags,
		State:         GuestStateIdle,
		ConnectedAt:   time.Now(),
		LastHeartbeat: time.Now(),
	}

	r.guests[guestID] = guest
	if r.logf != nil {
		r.logf("guest registered: %s (name: %s, tags: %v, total: %d)", guestID, name, tags, len(r.guests))
	}
	return guest, nil
}

// Unregister removes a guest from the registry.
func (r *GuestRegistry) Unregister(guestID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	guest, exists := r.guests[guestID]
	if !exists {
		return fmt.Errorf("guest %s not found", guestID)
	}

	if guest.State == GuestStateRunning {
		return fmt.Errorf("cannot unregister running guest %s (task: %s)", guestID, guest.TaskID)
	}

	delete(r.guests, guestID)
	if r.logf != nil {
		r.logf("guest unregistered: %s (total: %d)", guestID, len(r.guests))
	}
	return nil
}

// Heartbeat updates the last heartbeat time for a guest.
func (r *GuestRegistry) Heartbeat(guestID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	guest, exists := r.guests[guestID]
	if !exists {
		return fmt.Errorf("guest %s not found", guestID)
	}

	guest.LastHeartbeat = time.Now()
	return nil
}

// SetLastHeartbeat sets the last heartbeat time for a guest (for testing).
func (r *GuestRegistry) SetLastHeartbeat(guestID string, t time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	guest, exists := r.guests[guestID]
	if !exists {
		return fmt.Errorf("guest %s not found", guestID)
	}

	guest.LastHeartbeat = t
	return nil
}

// GetGuest returns a guest by ID.
func (r *GuestRegistry) GetGuest(guestID string) (*Guest, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	guest, exists := r.guests[guestID]
	return guest, exists
}

// GetGuests returns all guests matching the given state.
func (r *GuestRegistry) GetGuests(state GuestState) []*Guest {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var result []*Guest
	for _, guest := range r.guests {
		if guest.State == state {
			result = append(result, guest)
		}
	}
	return result
}

// GetAllGuests returns all guests.
func (r *GuestRegistry) GetAllGuests() []*Guest {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make([]*Guest, 0, len(r.guests))
	for _, guest := range r.guests {
		result = append(result, guest)
	}
	return result
}

// SetGuestState updates a guest's state.
func (r *GuestRegistry) SetGuestState(guestID string, state GuestState) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	guest, exists := r.guests[guestID]
	if !exists {
		return fmt.Errorf("guest %s not found", guestID)
	}

	oldState := guest.State
	guest.State = state
	if r.logf != nil {
		r.logf("guest %s state changed: %s -> %s", guestID, oldState, state)
	}
	return nil
}

// SetGuestTask assigns a task to a guest.
func (r *GuestRegistry) SetGuestTask(guestID, taskID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	guest, exists := r.guests[guestID]
	if !exists {
		return fmt.Errorf("guest %s not found", guestID)
	}

	guest.TaskID = taskID
	guest.State = GuestStateRunning
	return nil
}

// ClearGuestTask clears the task assignment from a guest.
func (r *GuestRegistry) ClearGuestTask(guestID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	guest, exists := r.guests[guestID]
	if !exists {
		return fmt.Errorf("guest %s not found", guestID)
	}

	guest.TaskID = ""
	guest.State = GuestStateIdle
	return nil
}

// KillRunningGuestTask clears the task assignment from a running guest.
// This is used when the server detects the guest has gone silent and needs
// to forcibly terminate its task execution.
func (r *GuestRegistry) KillRunningGuestTask(guestID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	guest, exists := r.guests[guestID]
	if !exists {
		return fmt.Errorf("guest %s not found", guestID)
	}

	if guest.TaskID == "" {
		return fmt.Errorf("guest %s has no running task", guestID)
	}

	taskID := guest.TaskID
	guest.TaskID = ""
	guest.State = GuestStateIdle

	if r.logf != nil {
		r.logf("guest %s task killed: %s (silence)", guestID, taskID)
	}

	return nil
}

// FindAvailableGuests returns guests that can handle tasks with the given tags.
func (r *GuestRegistry) FindAvailableGuests(requiredTags []string) []*Guest {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var result []*Guest
	for _, guest := range r.guests {
		if guest.State != GuestStateIdle {
			continue
		}
		if len(requiredTags) == 0 {
			result = append(result, guest)
			continue
		}
		if r.matchesTags(guest.Tags, requiredTags) {
			result = append(result, guest)
		}
	}
	return result
}

// HasGuestWithTags checks if there's at least one available guest with the required tags.
func (r *GuestRegistry) HasGuestWithTags(requiredTags []string) bool {
	return len(r.FindAvailableGuests(requiredTags)) > 0
}

// Count returns the total number of registered guests.
func (r *GuestRegistry) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.guests)
}

// CountByState returns the number of guests in a given state.
func (r *GuestRegistry) CountByState(state GuestState) int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	count := 0
	for _, guest := range r.guests {
		if guest.State == state {
			count++
		}
	}
	return count
}

// IsStale checks if a guest is stale (hasn't sent a heartbeat in the given duration).
func (r *GuestRegistry) IsStale(guestID string, timeout time.Duration) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	guest, exists := r.guests[guestID]
	if !exists {
		return false
	}

	return time.Since(guest.LastHeartbeat) > timeout
}

// RemoveStaleGuests removes guests that haven't sent a heartbeat within the timeout.
func (r *GuestRegistry) RemoveStaleGuests(timeout time.Duration) []*Guest {
	r.mu.Lock()
	defer r.mu.Unlock()

	var stale []*Guest
	now := time.Now()

	for id, guest := range r.guests {
		if now.Sub(guest.LastHeartbeat) > timeout {
			stale = append(stale, guest)
			delete(r.guests, id)
			if r.logf != nil {
				r.logf("stale guest removed: %s", id)
			}
		}
	}

	return stale
}

// matchesTags checks if the guest's tags match all required tags.
func (r *GuestRegistry) matchesTags(guestTags, requiredTags []string) bool {
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
