package metrics_test

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

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

	families := []string{
		"hotelier_tasks",
		"hotelier_guests",
		"hotelier_tasks_in_flight",
		"hotelier_concurrency_cap",
		"hotelier_tasks_held_by_cap",
	}
	for _, name := range families {
		if !strings.Contains(body, "# HELP "+name+" ") {
			t.Errorf("no # HELP for %s in scrape:\n%s", name, body)
		}
		if !strings.Contains(body, "# TYPE "+name+" gauge") {
			t.Errorf("no # TYPE %s gauge in scrape:\n%s", name, body)
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
	}
	for series, expected := range want {
		if got := find(t, body, series); got != expected {
			t.Errorf("%s = %s, want %s", series, got, expected)
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
