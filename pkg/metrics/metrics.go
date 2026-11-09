// Package metrics exposes Hotelier's in-memory working state in the Prometheus
// text exposition format (issue #260).
//
// The gauges are gathered at scrape time by a custom prometheus.Collector that
// reads the live orchestrator state. Nothing is stored twice and no background
// goroutine polls anything: the queue, the registry and the concurrency cap
// stay owned by the packages that keep them, and a scrape is a read of those
// (see the Prometheus Go guide linked below).
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
// NewRegistry alone (issue #260): nothing outside it builds one by hand.
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

// NewRegistry returns a dedicated registry holding only Hotelier's gauges, so
// the exposed set is a known list rather than whatever a dependency happened to
// register on the process-global one.
func NewRegistry(o *orchestrator.Orchestrator) (*prometheus.Registry, error) {
	reg := prometheus.NewRegistry()
	if err := reg.Register(newCollector(o)); err != nil {
		return nil, err
	}
	return reg, nil
}
