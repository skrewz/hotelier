package metrics_test

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"hotelier/pkg/config"
	"hotelier/pkg/metrics"
	"hotelier/pkg/orchestrator"
	"hotelier/pkg/queue"
	"hotelier/pkg/registry"
)

func testLogf(format string, args ...interface{}) {}

// newFixture builds an orchestrator whose queue and registry the test drives
// directly, so the states under observation are set up rather than simulated.
func newFixture(t *testing.T) *orchestrator.Orchestrator {
	t.Helper()
	return orchestrator.NewWithExisting(
		queue.NewTaskQueue(testLogf),
		registry.NewGuestRegistry(0, testLogf),
		testLogf,
	)
}

// addTasks adds n tasks to the queue, running final on each so the caller can
// walk a task into the status the fixture wants.
func addTasks(t *testing.T, o *orchestrator.Orchestrator, prefix string, n int, final func(id string)) {
	t.Helper()
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("%s-%d", prefix, i)
		task := &queue.Task{ID: id, Prompt: "do the thing", Tags: []string{"default"}}
		if err := o.AddTask(task); err != nil {
			t.Fatalf("AddTask %s: %v", id, err)
		}
		if final != nil {
			final(id)
		}
	}
}

// serveState starts a server exposing the orchestrator's metrics the way the
// real server does, and returns its URL so a test can scrape it repeatedly.
func serveState(t *testing.T, o *orchestrator.Orchestrator) string {
	t.Helper()
	reg, err := metrics.NewRegistry(o)
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}
	srv := httptest.NewServer(promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	t.Cleanup(srv.Close)
	return srv.URL
}

// scrape fetches /metrics once and returns the response headers and body.
func scrape(t *testing.T, url string) (header http.Header, body string) {
	t.Helper()
	resp, err := http.Get(url + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /metrics: status %d", resp.StatusCode)
	}
	return resp.Header, string(raw)
}

// find returns the value of the first line whose series matches exactly (the
// metric name with its label set, as the exposition format writes it).
func find(t *testing.T, body, series string) string {
	t.Helper()
	for _, line := range strings.Split(body, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, value, ok := strings.Cut(line, " ")
		if !ok {
			continue
		}
		if name == series {
			return strings.TrimSpace(value)
		}
	}
	t.Fatalf("series %q not found in scrape:\n%s", series, body)
	return ""
}

func TestScrapePublishesEveryFamily(t *testing.T) {
	o := newFixture(t)

	addTasks(t, o, "pending", 3, nil)
	addTasks(t, o, "assigned", 2, func(id string) {
		if err := o.Queue().Assign(id, "guest-a"); err != nil {
			t.Fatalf("Assign %s: %v", id, err)
		}
	})
	addTasks(t, o, "running", 1, func(id string) {
		if err := o.Queue().Assign(id, "guest-a"); err != nil {
			t.Fatalf("Assign %s: %v", id, err)
		}
		if err := o.Queue().Start(id); err != nil {
			t.Fatalf("Start %s: %v", id, err)
		}
	})
	addTasks(t, o, "completed", 4, func(id string) {
		if err := o.Queue().Assign(id, "guest-a"); err != nil {
			t.Fatalf("Assign %s: %v", id, err)
		}
		if err := o.Queue().Start(id); err != nil {
			t.Fatalf("Start %s: %v", id, err)
		}
		if err := o.Queue().Complete(id, "done"); err != nil {
			t.Fatalf("Complete %s: %v", id, err)
		}
	})
	addTasks(t, o, "failed", 1, func(id string) {
		if err := o.Queue().Assign(id, "guest-a"); err != nil {
			t.Fatalf("Assign %s: %v", id, err)
		}
		if err := o.Queue().Fail(id, "boom"); err != nil {
			t.Fatalf("Fail %s: %v", id, err)
		}
	})
	addTasks(t, o, "cancelled", 1, func(id string) {
		if err := o.Queue().Cancel(id); err != nil {
			t.Fatalf("Cancel %s: %v", id, err)
		}
	})

	for _, id := range []string{"guest-a", "guest-b"} {
		if err := o.RegisterGuest(id, id, []string{"default"}); err != nil {
			t.Fatalf("RegisterGuest %s: %v", id, err)
		}
	}
	if err := o.Registry().SetGuestState("guest-b", registry.GuestStateRunning); err != nil {
		t.Fatalf("SetGuestState: %v", err)
	}

	o.SetConcurrencyCap(4)

	header, body := scrape(t, serveState(t, o))

	if ct := header.Get("Content-Type"); !strings.Contains(ct, "text/plain") {
		t.Errorf("Content-Type = %q, want the Prometheus text format", ct)
	}

	families := map[string]string{
		"hotelier_tasks":                    "gauge",
		"hotelier_guests":                   "gauge",
		"hotelier_tasks_in_flight":          "gauge",
		"hotelier_concurrency_cap":          "gauge",
		"hotelier_tasks_held_by_cap":        "gauge",
		"hotelier_tasks_submitted_total":    "counter",
		"hotelier_tasks_deduplicated_total": "counter",
		"hotelier_tasks_finished_total":     "counter",
		"hotelier_tasks_requeued_total":     "counter",
		"hotelier_task_queue_wait_seconds":  "histogram",
		"hotelier_task_duration_seconds":    "histogram",
	}
	for name, kind := range families {
		if !strings.Contains(body, "# HELP "+name+" ") {
			t.Errorf("no # HELP for %s in scrape:\n%s", name, body)
		}
		if !strings.Contains(body, "# TYPE "+name+" "+kind) {
			t.Errorf("no # TYPE %s %s in scrape:\n%s", name, kind, body)
		}
	}

	// Every label value is one of the fixed enumerations, including the states
	// with no guests in them, so a scrape always carries the whole set.
	want := map[string]string{
		`hotelier_tasks{status="PENDING"}`:      "3",
		`hotelier_tasks{status="ASSIGNED"}`:     "2",
		`hotelier_tasks{status="RUNNING"}`:      "1",
		`hotelier_tasks{status="COMPLETED"}`:    "4",
		`hotelier_tasks{status="FAILED"}`:       "1",
		`hotelier_tasks{status="CANCELLED"}`:    "1",
		`hotelier_guests{state="DISCONNECTED"}`: "0",
		`hotelier_guests{state="REGISTERED"}`:   "0",
		`hotelier_guests{state="IDLE"}`:         "1",
		`hotelier_guests{state="RUNNING"}`:      "1",
		"hotelier_tasks_in_flight":              "3",
		"hotelier_concurrency_cap":              "4",
		"hotelier_tasks_held_by_cap":            "0",
		// The state was walked through the queue directly, not through the
		// orchestrator funnel, so the transition counters are legitimately
		// zero — and still present, including the whole outcome set
		// (issue #262).
		"hotelier_tasks_submitted_total":                     "0",
		"hotelier_tasks_deduplicated_total":                  "0",
		`hotelier_tasks_finished_total{outcome="completed"}`: "0",
		`hotelier_tasks_finished_total{outcome="failed"}`:    "0",
		`hotelier_tasks_finished_total{outcome="cancelled"}`: "0",
		"hotelier_tasks_requeued_total":                      "0",
		"hotelier_task_queue_wait_seconds_count":             "0",
		"hotelier_task_duration_seconds_count":               "0",
	}
	for series, expected := range want {
		if got := find(t, body, series); got != expected {
			t.Errorf("%s = %s, want %s", series, got, expected)
		}
	}

	// The labelled families must sum to the whole population: if a status or
	// state is ever added to the state machines and the metrics lists fall
	// behind, work in it would vanish from every graph silently, with every
	// per-series expectation above still green (issue #260 review).
	if got := sumSeries(t, body, "hotelier_tasks"); got != o.Queue().Count() {
		t.Errorf("hotelier_tasks sums to %d, want the queue size %d", got, o.Queue().Count())
	}
	if got := sumSeries(t, body, "hotelier_guests"); got != o.Registry().Count() {
		t.Errorf("hotelier_guests sums to %d, want the registry size %d", got, o.Registry().Count())
	}
	if strings.Contains(body, `status="UNKNOWN"`) || strings.Contains(body, `state="UNKNOWN"`) {
		t.Errorf("a label fell through to the String() UNKNOWN fallback:\n%s", body)
	}
}

// sumSeries adds the values of every series of one family (the metric name
// with any label set).
func sumSeries(t *testing.T, body, family string) int {
	t.Helper()
	total := 0
	for _, line := range strings.Split(body, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, value, ok := strings.Cut(line, " ")
		if !ok {
			continue
		}
		if name == family || strings.HasPrefix(name, family+"{") {
			var n int
			if _, err := fmt.Sscanf(strings.TrimSpace(value), "%d", &n); err != nil {
				t.Fatalf("parse %q: %v", line, err)
			}
			total += n
		}
	}
	return total
}

// The label sets are maintained by hand beside the enumerations, so a new
// queue.TaskStatus or registry.GuestState can go unlisted there. Deriving
// the enumerations here — walking each type until String() hits its UNKNOWN
// fallback — makes that miss fail in CI instead of opening a gap in every
// graph (issue #260 review).
func TestLabelSetsCoverEnumerations(t *testing.T) {
	o := newFixture(t)
	_, body := scrape(t, serveState(t, o))

	for s := queue.TaskStatus(0); ; s++ {
		name := s.String()
		if name == "UNKNOWN" {
			break
		}
		if !strings.Contains(body, `hotelier_tasks{status="`+name+`"}`) {
			t.Errorf("task status %s has no hotelier_tasks series; the label set is stale", name)
		}
	}
	for s := registry.GuestState(0); ; s++ {
		name := s.String()
		if name == "UNKNOWN" {
			break
		}
		if !strings.Contains(body, `hotelier_guests{state="`+name+`"}`) {
			t.Errorf("guest state %s has no hotelier_guests series; the label set is stale", name)
		}
	}
}

// The decision to leave /metrics unauthenticated rests on the exposition
// carrying no identifiers at all. That negative is what this test defends:
// were a task, guest or persona label ever added "to make the dashboard
// more useful", CI fails here rather than quietly publishing identifiers
// to anonymous scrapers (issue #260 review).
func TestScrapePublishesNoIdentifiers(t *testing.T) {
	o := newFixture(t)
	addTasks(t, o, "pending", 1, nil)
	for _, id := range []string{"guest-a", "guest-b"} {
		if err := o.RegisterGuest(id, id, []string{"default"}); err != nil {
			t.Fatalf("RegisterGuest %s: %v", id, err)
		}
	}

	_, body := scrape(t, serveState(t, o))

	for _, forbidden := range []string{"pending-0", "guest-a", "guest-b", "do the thing"} {
		if strings.Contains(body, forbidden) {
			t.Errorf("scrape published %q to an anonymous caller:\n%s", forbidden, body)
		}
	}
}

// The cap sentinel is negative by design so it can never be mistaken for a
// count. Prometheus cannot say "no limit" as a number, so the gauge must read
// +Inf — and must never read -1.
func TestConcurrencyCapUnlimitedScrapesAsInf(t *testing.T) {
	o := newFixture(t)
	o.SetConcurrencyCap(config.ConcurrencyUnlimited)

	_, body := scrape(t, serveState(t, o))

	if got := find(t, body, "hotelier_concurrency_cap"); got != "+Inf" {
		t.Errorf("hotelier_concurrency_cap = %s, want +Inf", got)
	}
	if strings.Contains(body, "hotelier_concurrency_cap -1") {
		t.Errorf("unlimited cap published as -1:\n%s", body)
	}
}

// The collector must read the state when Prometheus scrapes, not a copy taken
// when the registry was built.
func TestScrapeReflectsLiveState(t *testing.T) {
	o := newFixture(t)
	addTasks(t, o, "running", 1, func(id string) {
		if err := o.Queue().Assign(id, "guest-a"); err != nil {
			t.Fatalf("Assign: %v", err)
		}
		if err := o.Queue().Start(id); err != nil {
			t.Fatalf("Start: %v", err)
		}
	})
	for _, id := range []string{"guest-a", "guest-b"} {
		if err := o.RegisterGuest(id, id, []string{"default"}); err != nil {
			t.Fatalf("RegisterGuest %s: %v", id, err)
		}
	}
	if err := o.Registry().SetGuestState("guest-b", registry.GuestStateRunning); err != nil {
		t.Fatalf("SetGuestState: %v", err)
	}
	o.SetConcurrencyCap(2)

	url := serveState(t, o)

	_, first := scrape(t, url)
	if got := find(t, first, `hotelier_tasks{status="RUNNING"}`); got != "1" {
		t.Fatalf("first scrape running = %s, want 1", got)
	}
	if got := find(t, first, "hotelier_tasks_in_flight"); got != "1" {
		t.Fatalf("first scrape in flight = %s, want 1", got)
	}

	if err := o.Queue().Complete("running-0", "done"); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if err := o.Registry().SetGuestState("guest-b", registry.GuestStateIdle); err != nil {
		t.Fatalf("SetGuestState: %v", err)
	}
	if err := o.RegisterGuest("guest-c", "guest-c", []string{"default"}); err != nil {
		t.Fatalf("RegisterGuest: %v", err)
	}

	_, second := scrape(t, url)
	if got := find(t, second, `hotelier_tasks{status="RUNNING"}`); got != "0" {
		t.Errorf("second scrape running = %s, want 0", got)
	}
	if got := find(t, second, `hotelier_tasks{status="COMPLETED"}`); got != "1" {
		t.Errorf("second scrape completed = %s, want 1", got)
	}
	if got := find(t, second, "hotelier_tasks_in_flight"); got != "0" {
		t.Errorf("second scrape in flight = %s, want 0", got)
	}
	if got := find(t, second, `hotelier_guests{state="IDLE"}`); got != "3" {
		t.Errorf("second scrape idle guests = %s, want 3", got)
	}
}

// A scrape concurrent with the assignment path must not race on the hold
// state: HeldReason is written under the orchestrator lock, and the
// collector must read it under that same lock. Meaningless without -race;
// under it, an unsynchronised read on either side shows up as a DATA RACE
// (issue #260 review).
func TestScrapeConcurrentWithHoldWrites(t *testing.T) {
	o := newFixture(t)
	addTasks(t, o, "pending", 4, nil)

	url := serveState(t, o)

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 200; i++ {
			// Alternate a cap that holds everything with one that admits,
			// so the poll both writes and clears hold reasons.
			if i%2 == 0 {
				o.SetConcurrencyCap(0)
			} else {
				o.SetConcurrencyCap(1)
			}
			for _, task := range o.Queue().GetAllTasks() {
				o.AdmitsNewWork(task.ID)
			}
		}
	}()

	for i := 0; i < 50; i++ {
		scrape(t, url)
	}
	<-done
}

// A PENDING task the cap is holding back must make the queue read differently
// from one it is simply waiting to place.
func TestHeldByCapCountsHeldPendingTasks(t *testing.T) {
	o := newFixture(t)
	addTasks(t, o, "held", 2, func(id string) {
		task, ok := o.Queue().Get(id)
		if !ok {
			t.Fatalf("task %s not in queue", id)
		}
		task.HeldReason = "concurrency cap 0 reached (0 in flight)"
	})
	addTasks(t, o, "waiting", 1, nil)

	_, body := scrape(t, serveState(t, o))
	if got := find(t, body, "hotelier_tasks_held_by_cap"); got != "2" {
		t.Errorf("hotelier_tasks_held_by_cap = %s, want 2", got)
	}
	if got := find(t, body, `hotelier_tasks{status="PENDING"}`); got != "3" {
		t.Errorf(`hotelier_tasks{status="PENDING"} = %s, want 3`, got)
	}
}

// --- Counters and histograms (issue #262) ---
//
// Seam ownership: this file asserts what the orchestrator's events turn into
// on /metrics; orchestrator_metrics_test.go asserts which method emits which
// event. Keep edits in the seam that owns them.

// submitAssign wires the orchestrator into a scrape endpoint first, then runs
// a task through submit → assign → acknowledge, so every event after the
// wiring is observed. It returns the scrape URL and stops at the point the
// test wants to finish the task.
func submitAssign(t *testing.T, o *orchestrator.Orchestrator, id string) string {
	t.Helper()
	url := serveState(t, o)
	if err := o.AddTask(&queue.Task{ID: id, Prompt: "do the thing"}); err != nil {
		t.Fatalf("AddTask %s: %v", id, err)
	}
	if err := o.RegisterGuest("guest-a", "guest-a", []string{"default"}); err != nil {
		t.Fatalf("RegisterGuest: %v", err)
	}
	if err := o.AssignTask(id, "guest-a"); err != nil {
		t.Fatalf("AssignTask: %v", err)
	}
	return url
}

// histogramCount reads a histogram's observation count from a scrape body.
func histogramCount(t *testing.T, body, family string) int {
	t.Helper()
	return atoi(t, find(t, body, family+"_count"))
}

func atoi(t *testing.T, s string) int {
	t.Helper()
	var n int
	if _, err := fmt.Sscanf(strings.TrimSpace(s), "%d", &n); err != nil {
		t.Fatalf("parse %q as a count: %v", s, err)
	}
	return n
}

func parseFloat(t *testing.T, s string) float64 {
	t.Helper()
	var f float64
	if _, err := fmt.Sscanf(strings.TrimSpace(s), "%g", &f); err != nil {
		t.Fatalf("parse %q as a number: %v", s, err)
	}
	return f
}

func TestCompletedTaskCountsAndObserves(t *testing.T) {
	o := newFixture(t)
	url := submitAssign(t, o, "t1")
	if err := o.AcknowledgeTask("t1", "guest-a"); err != nil {
		t.Fatalf("AcknowledgeTask: %v", err)
	}
	if err := o.CompleteTask("t1", "guest-a", "done"); err != nil {
		t.Fatalf("CompleteTask: %v", err)
	}
	_, body := scrape(t, url)

	if got := find(t, body, "hotelier_tasks_submitted_total"); got != "1" {
		t.Errorf("submitted_total = %s, want 1", got)
	}
	if got := find(t, body, `hotelier_tasks_finished_total{outcome="completed"}`); got != "1" {
		t.Errorf(`finished_total{outcome="completed"} = %s, want 1`, got)
	}
	if got := histogramCount(t, body, "hotelier_task_queue_wait_seconds"); got != 1 {
		t.Errorf("queue_wait_seconds_count = %d, want 1", got)
	}
	if got := histogramCount(t, body, "hotelier_task_duration_seconds"); got != 1 {
		t.Errorf("duration_seconds_count = %d, want 1", got)
	}
	// Both windows are seconds-to-hours by nature; a journey of test
	// wall-clock must land well inside the deliberate bucket set, never in
	// a default set's +Inf.
	assertBucketedWithin(t, body, "hotelier_task_queue_wait_seconds", 60)
	assertBucketedWithin(t, body, "hotelier_task_duration_seconds", 60)
}

// assertBucketedWithin checks the family saw its observations below bound
// seconds: the bucket at that boundary is cumulative and must equal the
// count, and the sum must sit under the bound.
func assertBucketedWithin(t *testing.T, body, family string, bound float64) {
	t.Helper()
	le := fmt.Sprintf("%g", bound)
	series := family + "_bucket{le=\"" + le + "\"}"
	count := histogramCount(t, body, family)
	if got := atoi(t, find(t, body, series)); got != count {
		t.Errorf("%s = %d, want the full count %d (observations leaked to a higher or +Inf bucket)", series, got, count)
	}
	if sum := parseFloat(t, find(t, body, family+"_sum")); sum < 0 || sum >= bound {
		t.Errorf("%s_sum = %v, want a plausible total below %v", family, sum, bound)
	}
}

func TestFailedTaskCountsOutcomeFailed(t *testing.T) {
	o := newFixture(t)
	url := submitAssign(t, o, "t1")
	if err := o.FailTask("t1", "guest-a", "boom"); err != nil {
		t.Fatalf("FailTask: %v", err)
	}
	_, body := scrape(t, url)

	if got := find(t, body, `hotelier_tasks_finished_total{outcome="failed"}`); got != "1" {
		t.Errorf(`finished_total{outcome="failed"} = %s, want 1`, got)
	}
	if got := find(t, body, `hotelier_tasks_finished_total{outcome="completed"}`); got != "0" {
		t.Errorf(`finished_total{outcome="completed"} = %s, want 0`, got)
	}
	if got := histogramCount(t, body, "hotelier_task_duration_seconds"); got != 1 {
		t.Errorf("duration_seconds_count = %d, want 1", got)
	}
}

func TestCancelledTaskCountsOutcomeCancelled(t *testing.T) {
	o := newFixture(t)
	url := submitAssign(t, o, "t1")
	if err := o.CancelTask("t1", "guest-a"); err != nil {
		t.Fatalf("CancelTask: %v", err)
	}
	_, body := scrape(t, url)

	if got := find(t, body, `hotelier_tasks_finished_total{outcome="cancelled"}`); got != "1" {
		t.Errorf(`finished_total{outcome="cancelled"} = %s, want 1`, got)
	}
	if got := histogramCount(t, body, "hotelier_task_duration_seconds"); got != 1 {
		t.Errorf("duration_seconds_count = %d, want 1", got)
	}
}

// A task cancelled before assignment finished without ever being in a
// guest's hands: it counts as a finish but contributes no duration.
func TestCancelPendingTaskCountsFinishWithoutDuration(t *testing.T) {
	o := newFixture(t)
	url := serveState(t, o)
	if err := o.AddTask(&queue.Task{ID: "t1", Prompt: "do the thing"}); err != nil {
		t.Fatalf("AddTask: %v", err)
	}
	if err := o.CancelTask("t1", ""); err != nil {
		t.Fatalf("CancelTask: %v", err)
	}
	_, body := scrape(t, url)

	if got := find(t, body, `hotelier_tasks_finished_total{outcome="cancelled"}`); got != "1" {
		t.Errorf(`finished_total{outcome="cancelled"} = %s, want 1`, got)
	}
	if got := histogramCount(t, body, "hotelier_task_duration_seconds"); got != 0 {
		t.Errorf("duration_seconds_count = %d, want 0 — there was no run window", got)
	}
}

// The requeue → re-assign journey is the one path where measuring the wait
// from submission would charge a task's first run to the queue. Rewinding the
// submission an hour leaves the requeue stamp as the only sane starting point:
// both observations must still land in the sub-minute buckets.
func TestReassignedTaskQueueWaitExcludesFirstRun(t *testing.T) {
	o := newFixture(t)
	url := submitAssign(t, o, "t1")
	if err := o.AcknowledgeTask("t1", "guest-a"); err != nil {
		t.Fatalf("AcknowledgeTask: %v", err)
	}
	time.Sleep(150 * time.Millisecond) // a run long enough to be miscounted
	if err := o.RequeueTask("t1"); err != nil {
		t.Fatalf("RequeueTask: %v", err)
	}
	task, ok := o.GetTask("t1")
	if !ok {
		t.Fatal("GetTask t1: not found")
	}
	task.CreatedAt = time.Now().Add(-time.Hour)
	if err := o.AssignTask("t1", "guest-a"); err != nil {
		t.Fatalf("AssignTask after requeue: %v", err)
	}
	_, body := scrape(t, url)

	if got := histogramCount(t, body, "hotelier_task_queue_wait_seconds"); got != 2 {
		t.Fatalf("queue_wait_seconds_count = %d, want one observation per assignment", got)
	}
	assertBucketedWithin(t, body, "hotelier_task_queue_wait_seconds", 60)
	if got := histogramCount(t, body, "hotelier_task_duration_seconds"); got != 0 {
		t.Errorf("duration_seconds_count = %d, want 0 — no transition ended this task yet", got)
	}
}

func TestRequeueAndDeclineCountRequeued(t *testing.T) {
	o := newFixture(t)
	url := serveState(t, o)
	if err := o.RegisterGuest("guest-a", "guest-a", []string{"default"}); err != nil {
		t.Fatalf("RegisterGuest: %v", err)
	}
	for _, id := range []string{"t1", "t2"} {
		if err := o.AddTask(&queue.Task{ID: id, Prompt: "do the thing"}); err != nil {
			t.Fatalf("AddTask %s: %v", id, err)
		}
	}
	if err := o.AssignTask("t1", "guest-a"); err != nil {
		t.Fatalf("AssignTask t1: %v", err)
	}
	if err := o.RequeueTask("t1"); err != nil {
		t.Fatalf("RequeueTask: %v", err)
	}
	if err := o.AssignTask("t2", "guest-a"); err != nil {
		t.Fatalf("AssignTask t2: %v", err)
	}
	if err := o.DeclineTask("t2", "guest-a"); err != nil {
		t.Fatalf("DeclineTask: %v", err)
	}
	_, body := scrape(t, url)

	if got := find(t, body, "hotelier_tasks_requeued_total"); got != "2" {
		t.Errorf("requeued_total = %s, want 2", got)
	}
}

func TestDeduplicatedSubmissionCountsDeduplicatedOnly(t *testing.T) {
	o := newFixture(t)
	url := serveState(t, o)
	if _, dup, err := o.AddTaskOrDedup(&queue.Task{ID: "t1", Prompt: "p", DedupKey: "k"}); err != nil || dup {
		t.Fatalf("AddTaskOrDedup: dup=%v err=%v", dup, err)
	}
	if _, dup, err := o.AddTaskOrDedup(&queue.Task{ID: "t2", Prompt: "p", DedupKey: "k"}); err != nil || !dup {
		t.Fatalf("AddTaskOrDedup second: dup=%v err=%v", dup, err)
	}
	_, body := scrape(t, url)

	if got := find(t, body, "hotelier_tasks_submitted_total"); got != "1" {
		t.Errorf("submitted_total = %s, want 1 — the squelched submission must not count as submitted", got)
	}
	if got := find(t, body, "hotelier_tasks_deduplicated_total"); got != "1" {
		t.Errorf("deduplicated_total = %s, want 1", got)
	}
}
