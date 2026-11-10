package orchestrator

import (
	"testing"
	"time"

	"hotelier/pkg/queue"
)

// Preference filter delegation (issue #281): the server drives the filter
// through the orchestrator, and the bulk-assignment path must honour it.

func TestSetPreferenceFilter_DelegatesToQueue(t *testing.T) {
	orch := New(func(format string, args ...interface{}) {})

	if got := orch.PreferenceFilter(); got != "" {
		t.Fatalf("expected no filter by default, got %q", got)
	}

	orch.SetPreferenceFilter("s-habit-tracker")
	if got := orch.PreferenceFilter(); got != "s-habit-tracker" {
		t.Fatalf("expected filter to round-trip, got %q", got)
	}
	if got := orch.Queue().PreferenceFilter(); got != "s-habit-tracker" {
		t.Fatalf("expected the queue to hold the filter, got %q", got)
	}

	orch.SetPreferenceFilter("")
	if got := orch.PreferenceFilter(); got != "" {
		t.Fatalf("expected clear to round-trip, got %q", got)
	}
}

func TestTryAssignNext_PrefersPreferenceMatch(t *testing.T) {
	orch := New(func(format string, args ...interface{}) {})
	_ = orch.RegisterGuest("g1", "G1", nil)

	base := time.Now().Add(-time.Hour)
	older := &queue.Task{ID: "older", Prompt: "unrelated", CreatedAt: base}
	matcher := &queue.Task{ID: "matcher", Title: "Widget polish", CreatedAt: base.Add(time.Minute)}
	if err := orch.Queue().Restore(older); err != nil {
		t.Fatalf("Restore older: %v", err)
	}
	if err := orch.Queue().Restore(matcher); err != nil {
		t.Fatalf("Restore matcher: %v", err)
	}

	orch.SetPreferenceFilter("widget")

	if !orch.TryAssignNext() {
		t.Fatal("expected an assignment")
	}
	task, ok := orch.GetTask("matcher")
	if !ok || task.Status != queue.TaskStatusAssigned {
		t.Fatalf("expected the preference-matching task to be assigned, got %v %v", task, ok)
	}
}
