package queue

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"time"
)

// Preference filter tests (issue #281): while a filter is set, pending tasks
// whose text carries the substring are drained first; once no pending task
// matches, normal queue semantics apply.

func newRestoredQueue(t *testing.T, tasks []*Task) *TaskQueue {
	t.Helper()
	q := newTestQueue(t)
	for _, task := range tasks {
		if err := q.Restore(task); err != nil {
			t.Fatalf("Restore(%s): %v", task.ID, err)
		}
	}
	return q
}

func TestPreferenceFilterRoundTrip(t *testing.T) {
	q := newTestQueue(t)

	if got := q.PreferenceFilter(); got != "" {
		t.Fatalf("expected empty filter by default, got %q", got)
	}

	q.SetPreferenceFilter("  example-repo  ")
	if got := q.PreferenceFilter(); got != "example-repo" {
		t.Fatalf("expected trimmed filter, got %q", got)
	}

	q.SetPreferenceFilter("")
	if got := q.PreferenceFilter(); got != "" {
		t.Fatalf("expected cleared filter, got %q", got)
	}
}

// One operator action is one log line, and the line says whether the
// filter was set or cleared: "filter set to \"\"" is the last thing an
// operator wants to read after a restart mystery.
func TestSetPreferenceFilter_LogsOnceAndNamesTheAction(t *testing.T) {
	var log bytes.Buffer
	q := NewTaskQueue(func(format string, args ...interface{}) {
		fmt.Fprintf(&log, format+"\n", args...)
	})

	q.SetPreferenceFilter("example-repo")
	if got := log.String(); strings.Count(got, "preference filter") != 1 {
		t.Fatalf("expected one log line per set, got:\n%s", got)
	}

	log.Reset()
	q.SetPreferenceFilter("")
	if got := log.String(); !strings.Contains(got, "clear") {
		t.Fatalf("expected clearing to be named in the log, got:\n%s", got)
	}
	if strings.Count(log.String(), "preference filter") != 1 {
		t.Fatalf("expected one log line per clear, got:\n%s", log.String())
	}
}

// A settled task's text must not influence the pending band: only pending
// tasks are weighed against the filter.
func TestPreferenceFilter_IgnoresSettledTasks(t *testing.T) {
	base := time.Now().Add(-time.Hour)
	q := newRestoredQueue(t, []*Task{
		{ID: "settled-match", Title: "Widget polish", CreatedAt: base},
		{ID: "pending-plain", Prompt: "unrelated", CreatedAt: base.Add(time.Minute)},
	})
	if err := q.Assign("settled-match", "guest-1"); err != nil {
		t.Fatalf("Assign: %v", err)
	}
	if err := q.Start("settled-match"); err != nil {
		t.Fatalf("Start: %v", err)
	}

	q.SetPreferenceFilter("widget")

	ordered := q.GetAllTasksOrdered()
	if len(ordered) != 2 || ordered[0].ID != "settled-match" || ordered[1].ID != "pending-plain" {
		t.Fatalf("expected the running task first and the pending one after it, got %v", ordered)
	}
}

func TestNextPendingTask_PreferenceMatchFirst(t *testing.T) {
	base := time.Now().Add(-time.Hour)
	q := newRestoredQueue(t, []*Task{
		{ID: "older-other", Prompt: "unrelated work", CreatedAt: base},
		{ID: "newer-match", Prompt: "fix the widget", RepoRef: "https://example.com/org/example-repo.git", CreatedAt: base.Add(time.Minute)},
	})

	q.SetPreferenceFilter("example-repo")

	if got := q.NextPendingTask(); got == nil || got.ID != "newer-match" {
		t.Fatalf("expected preference-matching task first, got %v", got)
	}

	// Once the matching task leaves the pending queue, normal FIFO applies.
	if err := q.Assign("newer-match", "guest-1"); err != nil {
		t.Fatalf("Assign: %v", err)
	}
	if got := q.NextPendingTask(); got == nil || got.ID != "older-other" {
		t.Fatalf("expected normal queue semantics after matches are drained, got %v", got)
	}
}

func TestNextPendingTask_NoMatchFallsBackToPriority(t *testing.T) {
	base := time.Now().Add(-time.Hour)
	q := newRestoredQueue(t, []*Task{
		{ID: "first", Prompt: "alpha", CreatedAt: base},
		{ID: "second", Prompt: "beta", CreatedAt: base.Add(time.Minute)},
		{ID: "urgent", Prompt: "gamma", Priority: PriorityTeacher, CreatedAt: base.Add(2 * time.Minute)},
	})

	q.SetPreferenceFilter("nothing-matches-this")

	if got := q.NextPendingTask(); got == nil || got.ID != "urgent" {
		t.Fatalf("expected priority order when no pending task matches, got %v", got)
	}
}

func TestNextPendingTask_PreferenceOverPriority(t *testing.T) {
	base := time.Now().Add(-time.Hour)
	q := newRestoredQueue(t, []*Task{
		{ID: "high", Prompt: "burn the down", Priority: PriorityFirefighter, CreatedAt: base},
		{ID: "low-match", Title: "Widget polish", Prompt: "nice to have", CreatedAt: base.Add(time.Minute)},
	})

	q.SetPreferenceFilter("widget")

	if got := q.NextPendingTask(); got == nil || got.ID != "low-match" {
		t.Fatalf("expected preference to outrank priority while a match is pending, got %v", got)
	}
}

func TestPreferenceFilter_MatchesFieldsCaseInsensitively(t *testing.T) {
	cases := []struct {
		name   string
		task   *Task
		filter string
		match  bool
	}{
		{"repo", &Task{ID: "t", RepoRef: "https://example.com/Org/Example-Repo.git"}, "example", true},
		{"title", &Task{ID: "t", Title: "Fix Login Flow"}, "login", true},
		{"prompt", &Task{ID: "t", Prompt: "Refactor the parser"}, "PARSER", true},
		{"persona", &Task{ID: "t", Persona: "implementer"}, "plem", true},
		{"tags", &Task{ID: "t", Tags: []string{"android", "frontend"}}, "FRONTEND", true},
		{"no match", &Task{ID: "t", Prompt: "something else", RepoRef: "https://example.com/other.git"}, "habit", false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			q := newRestoredQueue(t, []*Task{tc.task})
			q.SetPreferenceFilter(tc.filter)

			got := q.NextPendingTask()
			if tc.match && (got == nil || got.ID != "t") {
				t.Fatalf("expected task to be preferred for filter %q", tc.filter)
			}
			if !tc.match {
				q.SetPreferenceFilter("will-match")
				if got := q.NextPendingTask(); got == nil {
					t.Fatalf("non-matching filter must not suppress tasks entirely")
				}
			}
		})
	}
}

func TestGetPendingTasks_And_OrderedFollowPreference(t *testing.T) {
	base := time.Now().Add(-time.Hour)
	q := newRestoredQueue(t, []*Task{
		{ID: "a", Prompt: "alpha", CreatedAt: base},
		{ID: "b", Prompt: "beta", CreatedAt: base.Add(time.Minute)},
		{ID: "c", Title: "Gamma widget", CreatedAt: base.Add(2 * time.Minute)},
	})

	q.SetPreferenceFilter("widget")

	pending := q.GetPendingTasks()
	if len(pending) != 3 || pending[0].ID != "c" {
		t.Fatalf("expected matching task first in GetPendingTasks, got %v", pending)
	}

	ordered := q.GetAllTasksOrdered()
	if len(ordered) != 3 || ordered[0].ID != "c" {
		t.Fatalf("expected displayed pending order to match drain order, got %v", ordered)
	}

	q.SetPreferenceFilter("")
	if pending := q.GetPendingTasks(); pending[0].ID != "a" {
		t.Fatalf("expected FIFO order once the filter is cleared, got %v", pending[0].ID)
	}
}
