package orchestrator

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"hotelier/pkg/queue"
)

// logCapture collects the lines an orchestrator logs, so a test can assert on
// what the operator would read in the server log.
type logCapture struct {
	mu    sync.Mutex
	lines []string
}

func (c *logCapture) logf(format string, args ...interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lines = append(c.lines, fmt.Sprintf(format, args...))
}

func (c *logCapture) all() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.lines...)
}

// overflowSentinel is what time.Duration renders to when the subtraction
// saturates at math.MaxInt64 nanoseconds — the value the stuck-task line used
// to report for every re-queue (issue #253).
const overflowSentinel = "2562047h47m16.854775807s"

// TestCheckStuckTasks_LogsRealDuration asserts the re-queue log line reports
// the time the task actually spent assigned. The duration used to be computed
// after requeueTaskInternal had cleared AssignedAt to the zero time on the
// same task pointer, so every line read for 2562047h47m16.854775807s.
func TestCheckStuckTasks_LogsRealDuration(t *testing.T) {
	capture := &logCapture{}
	orch := New(capture.logf)
	_ = orch.RegisterGuest("guest-1", "Test Guest", []string{})

	task := &queue.Task{ID: "task-1", Prompt: "test"}
	_ = orch.AddTask(task)
	_ = orch.AssignTask("task-1", "guest-1")

	assignedAgo := 2 * time.Minute
	task, _ = orch.GetTask("task-1")
	task.AssignedAt = time.Now().Add(-assignedAgo)

	timeout := 1 * time.Minute
	if requeued := orch.CheckStuckTasks(timeout); len(requeued) != 1 {
		t.Fatalf("expected 1 task requeued, got %d", len(requeued))
	}

	var line string
	for _, l := range capture.all() {
		if strings.Contains(l, "stuck task task-1 re-queued") {
			line = l
			break
		}
	}
	if line == "" {
		t.Fatalf("no stuck-task log line found in %q", capture.all())
	}
	if strings.Contains(line, overflowSentinel) {
		t.Fatalf("stuck-task log line reports the overflow sentinel: %q", line)
	}

	dur := reportedDuration(t, line)
	// The assignment predates the timeout, so the line must report at least
	// the timeout, and must stay in the neighbourhood of the real elapsed
	// time rather than an arbitrary sentinel.
	if dur < timeout {
		t.Errorf("reported duration %v is below the %v the task was assigned for", dur, timeout)
	}
	if dur > assignedAgo+30*time.Second {
		t.Errorf("reported duration %v exceeds the %v the task was actually assigned", dur, assignedAgo)
	}
}

// reportedDuration extracts the trailing "for <duration>)" from a stuck-task
// log line.
func reportedDuration(t *testing.T, line string) time.Duration {
	t.Helper()

	idx := strings.LastIndex(line, " for ")
	if idx < 0 {
		t.Fatalf("no duration in log line %q", line)
	}
	raw := strings.TrimSuffix(line[idx+len(" for "):], ")")
	dur, err := time.ParseDuration(raw)
	if err != nil {
		t.Fatalf("cannot parse duration %q in %q: %v", raw, line, err)
	}
	return dur
}
