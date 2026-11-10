package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"hotelier/pkg/orchestrator"
)

// exposedTotal adds up every value a registry would publish, so a test can ask
// the coarse question "did observing this event put anything anywhere?"
// without naming a family next to the event — which would be yet another copy
// of the enumeration to keep in step.
func exposedTotal(t *testing.T, reg *prometheus.Registry) float64 {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	total := 0.0
	for _, family := range families {
		for _, m := range family.GetMetric() {
			total += m.GetCounter().GetValue()
			total += m.GetGauge().GetValue()
			total += float64(m.GetHistogram().GetSampleCount())
			total += m.GetHistogram().GetSampleSum()
		}
	}
	return total
}

// TestObserveHandlesEveryTaskEvent ties the translation to the enumeration it
// translates: every event the orchestrator can emit must move a series. The
// events otherwise live twice — as Event* constants and as the case list in
// Observe — and nothing else fails the day the two part, which is how a new
// or renamed event would vanish from /metrics in silence. This is the same
// guard TestLabelSetsCoverEnumerations built for the gauge label sets
// (issue #262 review).
func TestObserveHandlesEveryTaskEvent(t *testing.T) {
	obs := newObservers()
	reg := prometheus.NewRegistry()
	for _, c := range obs.collectors() {
		if err := reg.Register(c); err != nil {
			t.Fatalf("Register: %v", err)
		}
	}

	events := orchestrator.TaskEvents()
	if len(events) == 0 {
		t.Fatal("orchestrator.TaskEvents() is empty — the guard would prove nothing")
	}
	for _, event := range events {
		before := exposedTotal(t, reg)
		obs.Observe(event, 1)
		if after := exposedTotal(t, reg); after <= before {
			t.Errorf("event %q changed nothing on the registry: it needs a case in Observe", event)
		}
	}

	// An event no case claims must stay inert — counted by nothing, but logged
	// rather than dropped quietly by the default branch.
	before := exposedTotal(t, reg)
	obs.Observe("event_nobody_declared", 1)
	if after := exposedTotal(t, reg); after != before {
		t.Errorf("an undeclared event moved the exposed total from %v to %v", before, after)
	}
}
