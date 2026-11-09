// Package metrics exposes Hotelier's working state in the Prometheus text
// exposition format (issue #260) and counts its task transitions and their
// durations (issue #262).
//
// The gauges are gathered at scrape time by a custom prometheus.Collector that
// reads the live orchestrator state. Nothing is stored twice and no background
// goroutine polls anything: the queue, the registry and the concurrency cap
// stay owned by the packages that keep them, and a scrape is a read of those
// (see the Prometheus Go guide linked below).
//
// The counters and histograms ride the same registry but not the scrape-time
// read: the orchestrator reports every lifecycle event through a one-method
// interface, and this package translates the events (issue #262).
//
// Cardinality is bounded on purpose. Every label value comes from a fixed
// enumeration — a task status or a guest state — so the series count cannot
// grow with the number of tasks, guests or personas. See the metric naming
// conventions linked below.
//
// https://prometheus.io/docs/guides/go-application/
// https://prometheus.io/docs/practices/naming/
package metrics

import (
	"math"

	"github.com/prometheus/client_golang/prometheus"

	"hotelier/pkg/orchestrator"
	"hotelier/pkg/queue"
	"hotelier/pkg/registry"
)

// namespace prefixes every family this package publishes.
const namespace = "hotelier"

// taskStatuses and guestStates are the complete label sets. Both lists are
// derived from the enumerations the state machines actually use, and
// TestLabelSetsCoverEnumerations walks the enumerations themselves to fail
// the day either list goes stale — a new status or state must not become a
// cardinality surprise that only shows up as a silently missing series.
var (
	taskStatuses = []queue.TaskStatus{
		queue.TaskStatusPending,
		queue.TaskStatusAssigned,
		queue.TaskStatusRunning,
		queue.TaskStatusCompleted,
		queue.TaskStatusFailed,
		queue.TaskStatusCancelled,
	}
	guestStates = []registry.GuestState{
		registry.GuestStateDisconnected,
		registry.GuestStateRegistered,
		registry.GuestStateIdle,
		registry.GuestStateRunning,
	}
)

// collector publishes the live server state as Prometheus gauges. It holds only
// a pointer to the orchestrator: every value is read when Prometheus scrapes,
// so the endpoint can never disagree with /api/health. The package exports
// NewRegistry alone (issues #260 and #262): nothing outside it builds one by
// hand.
type collector struct {
	orchestrator *orchestrator.Orchestrator

	tasksDesc    *prometheus.Desc
	guestsDesc   *prometheus.Desc
	inFlightDesc *prometheus.Desc
	capDesc      *prometheus.Desc
	heldDesc     *prometheus.Desc
}

// newCollector builds a collector over the given orchestrator.
func newCollector(o *orchestrator.Orchestrator) *collector {
	return &collector{
		orchestrator: o,
		tasksDesc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "tasks"),
			"Tasks currently held by the queue, by status. Terminal tasks stay counted until the process restarts.",
			[]string{"status"}, nil,
		),
		guestsDesc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "guests"),
			"Guests connected to this server, by state.",
			[]string{"state"}, nil,
		),
		inFlightDesc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "tasks_in_flight"),
			"Tasks in flight right now: ASSIGNED or RUNNING.",
			nil, nil,
		),
		capDesc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "concurrency_cap"),
			"Maximum number of tasks allowed in flight. +Inf when no cap is in force; the unlimited sentinel is never published as a number.",
			nil, nil,
		),
		heldDesc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "tasks_held_by_cap"),
			"PENDING tasks whose last assignment attempt was refused, i.e. tasks carrying a hold reason. The reason records the last refusal, so a task released by a rising cap stays counted until the next poll considers it.",
			nil, nil,
		),
	}
}

// Describe reports the families the collector can produce.
func (c *collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.tasksDesc
	ch <- c.guestsDesc
	ch <- c.inFlightDesc
	ch <- c.capDesc
	ch <- c.heldDesc
}

// Collect reads the live state and emits one gauge per series. The guest list
// includes the zero counts so a scrape always carries the whole fixed label
// set and a state dropping to zero reads as a flat line rather than a gap.
func (c *collector) Collect(ch chan<- prometheus.Metric) {
	q := c.orchestrator.Queue()
	r := c.orchestrator.Registry()

	for _, status := range taskStatuses {
		ch <- prometheus.MustNewConstMetric(c.tasksDesc, prometheus.GaugeValue,
			float64(q.CountByStatus(status)), status.String())
	}

	for _, state := range guestStates {
		ch <- prometheus.MustNewConstMetric(c.guestsDesc, prometheus.GaugeValue,
			float64(r.CountByState(state)), state.String())
	}

	ch <- prometheus.MustNewConstMetric(c.inFlightDesc, prometheus.GaugeValue,
		float64(c.orchestrator.InFlightCount()))
	ch <- prometheus.MustNewConstMetric(c.capDesc, prometheus.GaugeValue,
		capValue(c.orchestrator.ConcurrencyCap()))

	ch <- prometheus.MustNewConstMetric(c.heldDesc, prometheus.GaugeValue,
		float64(c.orchestrator.HeldPendingCount()))
}

// capValue turns the cap into a float for the gauge. config.ConcurrencyUnlimited
// is deliberately negative so that a bare -1 can never be mistaken for a number
// of tasks (issue #249), and orchestrator.Orchestrator.ConcurrencyCap documents
// any negative value as "no cap". Prometheus has no way to say "no limit" as a
// number except +Inf, which is also what a graph of the cap should read as: a
// line pinned at the top of the panel means nothing is being held back.
func capValue(limit int) float64 {
	if limit < 0 {
		return math.Inf(1)
	}
	return float64(limit)
}

// taskBuckets spans seconds to a couple of hours, deliberately rather than
// by default: these are agent runs, so the default histogram buckets (which
// top out around ten seconds) would put every task in +Inf and make the
// distribution unreadable. The ceiling sits above the one-hour task_timeout
// default so a task that times out is still placed in a finite bucket.
var taskBuckets = []float64{
	1, 5, 15, 30, 60, 120, 300, 600, 900, 1800, 3600, 7200,
}

// observers turns the orchestrator's task-lifecycle events (issue #262) into
// counters and histograms: what happened, as opposed to the collector above,
// which answers what is happening right now. It is wired into the
// orchestrator by NewRegistry, so the funnel in pkg/orchestrator and the
// registry cannot drift apart, and it implements orchestrator.TaskObserver
// with plain strings and floats — no Prometheus type reaches pkg/queue or
// the orchestrator itself.
//
// Counters reset when the process restarts; that is how Prometheus counters
// work and rate() accounts for it. Nothing here persists them, since
// queue_dir persists unprocessed tasks only and a persisted counter would
// need a recovery path nothing else in this server has.
type observers struct {
	submitted    prometheus.Counter
	deduplicated prometheus.Counter
	finished     *prometheus.CounterVec
	requeued     prometheus.Counter
	queueWait    prometheus.Histogram
	duration     prometheus.Histogram
}

// outcomeFinished is the complete outcome label set, exactly the three
// terminal transitions that exist. Like the gauge label sets it is a fixed
// enumeration, and the vector is pre-initialised to all three so a scrape
// carries the whole set whatever has finished yet.
var outcomeFinished = []string{"completed", "failed", "cancelled"}

func newObservers() *observers {
	finished := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: prometheus.BuildFQName(namespace, "", "tasks_finished_total"),
			Help: "Tasks that reached a terminal state, by outcome. Resets on process restart; use rate() for rates over time.",
		},
		[]string{"outcome"},
	)
	for _, outcome := range outcomeFinished {
		finished.WithLabelValues(outcome) // pin the zero series into every scrape
	}
	return &observers{
		submitted: prometheus.NewCounter(prometheus.CounterOpts{
			Name: prometheus.BuildFQName(namespace, "", "tasks_submitted_total"),
			Help: "Tasks accepted into the queue. Squelched duplicates count under tasks_deduplicated_total instead. Resets on process restart; use rate() for rates over time.",
		}),
		deduplicated: prometheus.NewCounter(prometheus.CounterOpts{
			Name: prometheus.BuildFQName(namespace, "", "tasks_deduplicated_total"),
			Help: "Submissions squelched against an already-pending task with the same dedup key. Resets on process restart; use rate() for rates over time.",
		}),
		finished: finished,
		requeued: prometheus.NewCounter(prometheus.CounterOpts{
			Name: prometheus.BuildFQName(namespace, "", "tasks_requeued_total"),
			Help: "Tasks re-entering the pending queue after being requeued or declined. Resets on process restart; use rate() for rates over time.",
		}),
		queueWait: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    prometheus.BuildFQName(namespace, "", "task_queue_wait_seconds"),
			Help:    "Seconds a task spent between submission and assignment, observed at each assignment. Resets on process restart.",
			Buckets: taskBuckets,
		}),
		duration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    prometheus.BuildFQName(namespace, "", "task_duration_seconds"),
			Help:    "Seconds a task spent between assignment and its terminal transition, observed at each finish. Resets on process restart.",
			Buckets: taskBuckets,
		}),
	}
}

// Observe implements orchestrator.TaskObserver. seconds is ignored (or
// tested for NaN) exactly where the event carries no window to measure.
func (ob *observers) Observe(event string, seconds float64) {
	switch event {
	case orchestrator.EventSubmitted:
		ob.submitted.Inc()
	case orchestrator.EventDeduplicated:
		ob.deduplicated.Inc()
	case orchestrator.EventRequeued:
		ob.requeued.Inc()
	case orchestrator.EventQueueWait:
		if !math.IsNaN(seconds) {
			ob.queueWait.Observe(seconds)
		}
	case orchestrator.EventCompleted, orchestrator.EventFailed, orchestrator.EventCancelled:
		ob.finished.WithLabelValues(event).Inc()
		// A task cancelled straight from PENDING finished without ever
		// being in a guest's hands: it counts, but has no duration.
		if !math.IsNaN(seconds) {
			ob.duration.Observe(seconds)
		}
	}
}

// Describe reports the families the observers can produce.
func (ob *observers) Describe(ch chan<- *prometheus.Desc) {
	ob.submitted.Describe(ch)
	ob.deduplicated.Describe(ch)
	ob.finished.Describe(ch)
	ob.requeued.Describe(ch)
	ob.queueWait.Describe(ch)
	ob.duration.Describe(ch)
}

// Collect emits the current value of every counter and histogram.
func (ob *observers) Collect(ch chan<- prometheus.Metric) {
	ob.submitted.Collect(ch)
	ob.deduplicated.Collect(ch)
	ob.finished.Collect(ch)
	ob.requeued.Collect(ch)
	ob.queueWait.Collect(ch)
	ob.duration.Collect(ch)
}

// NewRegistry returns a dedicated registry holding only Hotelier's
// gauges, counters and histograms, so the exposed set is a known list
// rather than whatever a dependency happened to register on the
// process-global one. As part of building it, NewRegistry wires the
// transition observers into the orchestrator: the registry and the funnel
// are one package's two halves, and a server that forgets to wire them
// would under-count forever (issue #262).
func NewRegistry(o *orchestrator.Orchestrator) (*prometheus.Registry, error) {
	reg := prometheus.NewRegistry()
	if err := reg.Register(newCollector(o)); err != nil {
		return nil, err
	}
	obs := newObservers()
	if err := reg.Register(obs); err != nil {
		return nil, err
	}
	o.SetTaskObserver(obs)
	return reg, nil
}
