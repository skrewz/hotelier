package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"hotelier/pkg/config"
	"hotelier/pkg/queue"
)

// dayNightServer returns a server whose schedule runs cap 2 by day and cap 4
// by night in UTC — the night-mode shape issue #250 has to make visible.
func dayNightServer(t *testing.T) *Server {
	t.Helper()
	return New(config.ServerConfig{
		Host:               "127.0.0.1",
		Port:               0,
		MaxConcurrentTasks: 9,
		ScheduleTimezone:   "UTC",
		ConcurrencySchedule: []config.ConcurrencyScheduleEntry{
			{At: "07:00", Concurrency: 2},
			{At: "23:00", Concurrency: 4},
		},
	})
}

func addTask(t *testing.T, srv *Server, id string) {
	t.Helper()
	if err := srv.orchestrator.AddTask(&queue.Task{ID: id, Prompt: id}); err != nil {
		t.Fatalf("AddTask %s: %v", id, err)
	}
}

// TestConcurrencyStatus_FollowsTheScheduleWithAClock pins the verification the
// issue asks for: read the status either side of a boundary and the number,
// the entry it came from, and the next change all move on their own.
func TestConcurrencyStatus_FollowsTheScheduleWithAClock(t *testing.T) {
	srv := dayNightServer(t)

	before := srv.concurrencyStatus(time.Date(2026, 6, 15, 6, 0, 0, 0, time.UTC))
	if before["cap"] != 4 {
		t.Fatalf("expected cap 4 from the wrap-around 23:00 entry, got %v", before["cap"])
	}
	if before["source"] != `concurrency_schedule at "23:00"` {
		t.Errorf("expected the source to name the 23:00 entry, got %v", before["source"])
	}
	next, ok := before["next_change"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected a scheduled next change, got %v", before["next_change"])
	}
	if next["at"] != "07:00" || next["cap"] != 2 {
		t.Errorf("expected the next change to be 07:00 -> 2, got %v", next)
	}
	if next["timezone"] != "UTC" {
		t.Errorf("expected the next change to name its zone, got %v", next["timezone"])
	}

	after := srv.concurrencyStatus(time.Date(2026, 6, 15, 8, 0, 0, 0, time.UTC))
	if after["cap"] != 2 {
		t.Fatalf("expected cap 2 after the 07:00 boundary, got %v", after["cap"])
	}
	if after["source"] != `concurrency_schedule at "07:00"` {
		t.Errorf("expected the source to name the 07:00 entry, got %v", after["source"])
	}
	next, ok = after["next_change"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected a scheduled next change, got %v", after["next_change"])
	}
	if next["at"] != "23:00" || next["cap"] != 4 {
		t.Errorf("expected the next change to be 23:00 -> 4, got %v", next)
	}
}

// TestConcurrencyStatus_WaitingOnCapVersusIdleGuests is the distinction the
// issue exists to make: with the cap full and a queue behind it, the waiting
// work is named, so a dashboard that shows nothing running can still say
// "held behind the cap" rather than "idle".
func TestConcurrencyStatus_WaitingOnCapVersusIdleGuests(t *testing.T) {
	srv := New(config.ServerConfig{Host: "127.0.0.1", Port: 0, MaxConcurrentTasks: 1})
	if err := srv.orchestrator.RegisterGuest("g1", "G1", nil); err != nil {
		t.Fatalf("RegisterGuest: %v", err)
	}
	for _, id := range []string{"t1", "t2", "t3"} {
		addTask(t, srv, id)
	}

	if err := srv.orchestrator.AssignTask("t1", "g1"); err != nil {
		t.Fatalf("AssignTask t1: %v", err)
	}

	status := srv.concurrencyStatus(time.Now())
	if status["cap"] != 1 {
		t.Errorf("expected cap 1, got %v", status["cap"])
	}
	if status["in_flight"] != 1 {
		t.Errorf("expected 1 task in flight, got %v", status["in_flight"])
	}
	if status["waiting_on_cap"] != 2 {
		t.Errorf("expected the 2 pending tasks the full cap cannot start, got %v", status["waiting_on_cap"])
	}
	if status["source"] != "max_concurrent_tasks" {
		t.Errorf("expected the static cap to be named as the source, got %v", status["source"])
	}
	if status["next_change"] != nil {
		t.Errorf("expected no scheduled change without a schedule, got %v", status["next_change"])
	}

	// Freeing the slot hands the room to the queue: the work is no longer
	// waiting on the cap but on a guest being there to take it.
	if err := srv.orchestrator.CompleteTask("t1", "g1", "done"); err != nil {
		t.Fatalf("CompleteTask: %v", err)
	}
	if got := srv.concurrencyStatus(time.Now())["waiting_on_cap"]; got != 1 {
		t.Errorf("expected 1 waiting once the slot freed, got %v", got)
	}
}

// TestConcurrencyStatus_UnlimitedCapIsSaidInWords: a bare -1 in the JSON
// would read as a broken number rather than as "no cap" (same convention as
// /api/health, issue #249).
func TestConcurrencyStatus_UnlimitedCapIsSaidInWords(t *testing.T) {
	srv := newTestServer(t)

	status := srv.concurrencyStatus(time.Now())
	if status["cap"] != "unlimited" {
		t.Errorf("expected the unlimited cap to be stated in words, got %v", status["cap"])
	}
	if status["waiting_on_cap"] != 0 {
		t.Errorf("expected nothing waiting on a cap that is not in force, got %v", status["waiting_on_cap"])
	}
}

// TestHandleGuests_CarriesConcurrencyStatus: the dashboard polls /api/guests
// every cycle, so that is where the concurrency picture has to arrive — no
// second poll, no new page.
func TestHandleGuests_CarriesConcurrencyStatus(t *testing.T) {
	srv := dayNightServer(t)

	req := httptest.NewRequest(http.MethodGet, "/api/guests", nil)
	w := httptest.NewRecorder()
	srv.HandleGuests(w, req)

	var body struct {
		Concurrency struct {
			Cap          int    `json:"cap"`
			Source       string `json:"source"`
			InFlight     int    `json:"in_flight"`
			WaitingOnCap int    `json:"waiting_on_cap"`
			NextChange   *struct {
				At   string `json:"at"`
				Cap  int    `json:"cap"`
				Time string `json:"time"`
			} `json:"next_change"`
		} `json:"concurrency"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode /api/guests: %v", err)
	}

	if body.Concurrency.Source == "" {
		t.Fatal("expected /api/guests to carry the concurrency source")
	}
	if body.Concurrency.NextChange == nil {
		t.Fatal("expected /api/guests to carry the next scheduled change")
	}
	if _, err := time.Parse(time.RFC3339, body.Concurrency.NextChange.Time); err != nil {
		t.Errorf("expected the next change instant to be RFC3339, got %q (%v)", body.Concurrency.NextChange.Time, err)
	}
}
