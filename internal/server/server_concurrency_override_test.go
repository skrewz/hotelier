package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"hotelier/pkg/config"
	"hotelier/pkg/queue"
)

// postConcurrency posts a raw body to the /api/concurrency endpoint and
// returns the recorder (issue #264).
func postConcurrency(t *testing.T, srv *Server, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/api/concurrency", bytes.NewReader([]byte(body)))
	req.Header.Set("Content-Type", "application/json")
	req.RemoteAddr = "127.0.0.1:1234" // the operator by the posture this route enforces
	req.Host = "127.0.0.1:8080"       // addressing the server by a loopback name, as an operator's request does
	w := httptest.NewRecorder()
	srv.HandleConcurrency(w, req)
	return w
}

// TestConcurrencyOverride_PostSetsCapAndIsEnforced: the endpoint's number is
// not a display value — it is the cap the orchestrator's next assignment
// decision obeys (the same contract reload and the sync tick have, issue
// #249). Lowering the cap below the queue's room holds work back at once.
func TestConcurrencyOverride_PostSetsCapAndIsEnforced(t *testing.T) {
	srv := capTestServer(t, config.ServerConfig{Host: "127.0.0.1", Port: 0, MaxConcurrentTasks: 2})
	submitTask(t, srv, "t1")
	if got := taskStatus(t, srv, "t1"); got != queue.TaskStatusAssigned {
		t.Fatalf("expected t1 assigned under the config cap, got %s", got)
	}

	w := postConcurrency(t, srv, `{"cap": 1}`)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if got := srv.orchestrator.ConcurrencyCap(); got != 1 {
		t.Fatalf("expected the override 1 at the orchestrator, got %d", got)
	}

	submitTask(t, srv, "t2")
	if got := taskStatus(t, srv, "t2"); got != queue.TaskStatusPending {
		t.Fatalf("expected t2 held PENDING by the override cap of 1, got %s", got)
	}
	if n := srv.orchestrator.InFlightCount(); n != 1 {
		t.Fatalf("expected 1 in flight under the cap of 1, got %d", n)
	}

	var status map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &status); err != nil {
		t.Fatalf("response is not the concurrency status: %v", err)
	}
	if status["cap"] != float64(1) {
		t.Errorf("expected the response to state cap 1, got %v", status["cap"])
	}
	if status["source"] != "manual override" {
		t.Errorf("expected the source to say the cap is a manual override, got %v", status["source"])
	}
}

// TestConcurrencyOverride_RaisingCapDrainsQueue: raising the cap is itself
// the event the held work is waiting on, so the POST must drain the queue
// without waiting for a guest poll or the next sync tick (issue #249's
// contract, applied to the new entry point by issue #264).
func TestConcurrencyOverride_RaisingCapDrainsQueue(t *testing.T) {
	srv := capTestServer(t, config.ServerConfig{Host: "127.0.0.1", Port: 0, MaxConcurrentTasks: 1})
	submitTask(t, srv, "t1")
	if got := taskStatus(t, srv, "t1"); got != queue.TaskStatusAssigned {
		t.Fatalf("expected t1 assigned at cap 1, got %s", got)
	}
	submitTask(t, srv, "t2")
	time.Sleep(100 * time.Millisecond)
	if got := taskStatus(t, srv, "t2"); got != queue.TaskStatusPending {
		t.Fatalf("expected t2 held at the cap, got %s", got)
	}

	if w := postConcurrency(t, srv, `{"cap": 2}`); w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if got := taskStatus(t, srv, "t2"); got != queue.TaskStatusAssigned {
		t.Fatalf("expected t2 assigned once the cap rose, got %s", got)
	}
}

// TestConcurrencyOverride_ZeroPausesTheQueue: 0 means "start nothing" here,
// the same vocabulary a schedule entry uses for the overnight pause — not
// "unlimited", which is what the static config key reads 0 as.
func TestConcurrencyOverride_ZeroPausesTheQueue(t *testing.T) {
	srv := capTestServer(t, config.ServerConfig{Host: "127.0.0.1", Port: 0, MaxConcurrentTasks: 2})
	if w := postConcurrency(t, srv, `{"cap": 0}`); w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if got := srv.orchestrator.ConcurrencyCap(); got != 0 {
		t.Fatalf("expected the pause cap 0 at the orchestrator, got %d", got)
	}
	submitTask(t, srv, "t1")
	time.Sleep(100 * time.Millisecond)
	if got := taskStatus(t, srv, "t1"); got != queue.TaskStatusPending {
		t.Fatalf("expected t1 held by the paused cap, got %s", got)
	}
}

// TestConcurrencyOverride_Unlimited: the endpoint speaks the same cap
// vocabulary as the status surface, so "unlimited" (and, defensively, any
// negative number) resolves to the ConcurrencyUnlimited sentinel and the
// response states it in words.
func TestConcurrencyOverride_Unlimited(t *testing.T) {
	srv := capTestServer(t, config.ServerConfig{Host: "127.0.0.1", Port: 0, MaxConcurrentTasks: 1})
	w := postConcurrency(t, srv, `{"cap": "unlimited"}`)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if got := srv.orchestrator.ConcurrencyCap(); got != config.ConcurrencyUnlimited {
		t.Fatalf("expected the unlimited sentinel, got %d", got)
	}
	var status map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &status); err != nil {
		t.Fatalf("response is not the concurrency status: %v", err)
	}
	if status["cap"] != "unlimited" {
		t.Errorf("expected the response to state cap \"unlimited\", got %v", status["cap"])
	}
}

// TestConcurrencyOverride_RejectsBadBodies: a cap is a number the queue is
// held to; a malformed or nonsensical body must be refused with 400 and
// must leave the cap in force untouched.
func TestConcurrencyOverride_RejectsBadBodies(t *testing.T) {
	srv := capTestServer(t, config.ServerConfig{Host: "127.0.0.1", Port: 0, MaxConcurrentTasks: 3})
	for _, body := range []string{
		``,
		`{}`,
		`{"cap": null}`,
		`{"cap": 1.5}`,
		`{"cap": "banana"}`,
		`{"cap": -3.5}`,
		`{"cap": 1e300}`,
		`not json`,
	} {
		w := postConcurrency(t, srv, body)
		if w.Code != http.StatusBadRequest {
			t.Errorf("body %q: expected 400, got %d", body, w.Code)
		}
	}
	if got := srv.orchestrator.ConcurrencyCap(); got != 3 {
		t.Fatalf("expected a refused body to leave the cap at 3, got %d", got)
	}
}

// TestConcurrencyOverride_MethodNotAllowed: the endpoint changes state and
// takes its value in the body; reads belong on /api/guests.
func TestConcurrencyOverride_MethodNotAllowed(t *testing.T) {
	srv := New(config.ServerConfig{Host: "127.0.0.1", Port: 0})
	for _, method := range []string{http.MethodGet, http.MethodDelete} {
		req := httptest.NewRequest(method, "/api/concurrency", nil)
		w := httptest.NewRecorder()
		srv.HandleConcurrency(w, req)
		if w.Code != http.StatusMethodNotAllowed {
			t.Errorf("%s: expected 405, got %d", method, w.Code)
		}
	}
}

// TestConcurrencyOverride_ScheduleBoundaryEndsOverride is the issue's
// central rule: the override rules now, but "the next shrink or expansion
// that will take place on the schedule is respected". A shrink is the sharp
// case — the operator raises the cap past what the schedule allows by day,
// and the 07:00 entry still takes over.
func TestConcurrencyOverride_ScheduleBoundaryEndsOverride(t *testing.T) {
	srv := dayNightServer(t) // cap 4 until 07:00, cap 2 from 07:00, UTC

	dayStart := time.Date(2026, 6, 15, 6, 0, 0, 0, time.UTC)
	srv.syncConcurrencyCap(dayStart)
	if !srv.setConcurrencyCapOverride(6, "test", dayStart) {
		t.Fatal("raising the cap from 4 to 6 is a rise the held queue should act on")
	}
	if got := srv.orchestrator.ConcurrencyCap(); got != 6 {
		t.Fatalf("expected the override 6 at the orchestrator, got %d", got)
	}

	// Before the boundary the override rules.
	if srv.syncConcurrencyCap(time.Date(2026, 6, 15, 6, 30, 0, 0, time.UTC)) {
		t.Fatal("no boundary was crossed, so nothing should report a rise")
	}
	if got := srv.orchestrator.ConcurrencyCap(); got != 6 {
		t.Fatalf("expected the override to hold before the boundary, got %d", got)
	}
	if src := srv.concurrencyStatus(time.Date(2026, 6, 15, 6, 30, 0, 0, time.UTC))["source"]; src != "manual override" {
		t.Errorf("expected the source to say the override is in force, got %v", src)
	}

	// The 07:00 shrink takes over: the schedule wins, and the source names
	// the entry again rather than the override.
	morning := time.Date(2026, 6, 15, 7, 30, 0, 0, time.UTC)
	if srv.syncConcurrencyCap(morning) {
		t.Fatal("restoring the schedule cap 2 over the override 6 lowered the cap, so no drain is due")
	}
	if got := srv.orchestrator.ConcurrencyCap(); got != 2 {
		t.Fatalf("expected the 07:00 schedule cap 2 after the boundary, got %d", got)
	}
	if src := srv.concurrencyStatus(morning)["source"]; src != `concurrency_schedule at "07:00"` {
		t.Errorf("expected the source back on the schedule after the boundary, got %v", src)
	}
}

// TestConcurrencyOverride_ScheduleExpansionIsRespected: the same rule from
// the other side — the operator drops the cap overnight, and the 23:00
// expansion still happens on its own.
func TestConcurrencyOverride_ScheduleExpansionIsRespected(t *testing.T) {
	srv := dayNightServer(t)

	night := time.Date(2026, 6, 15, 22, 0, 0, 0, time.UTC)
	srv.syncConcurrencyCap(night) // schedule cap 2 in force until 23:00
	srv.setConcurrencyCapOverride(1, "test", night)
	if got := srv.orchestrator.ConcurrencyCap(); got != 1 {
		t.Fatalf("expected the override 1 at the orchestrator, got %d", got)
	}

	later := time.Date(2026, 6, 15, 23, 30, 0, 0, time.UTC)
	if !srv.syncConcurrencyCap(later) {
		t.Fatal("the 23:00 expansion is an event the queue was waiting on; sync must report the rise")
	}
	if got := srv.orchestrator.ConcurrencyCap(); got != 4 {
		t.Fatalf("expected the scheduled expansion to 4 to be respected, got %d", got)
	}
}

// TestConcurrencyOverride_WithoutAScheduleLivesUntilReload: with no schedule
// there is no next boundary to expire against, so the override rules until
// a config reload reasserts the file — and never survives one.
func TestConcurrencyOverride_WithoutAScheduleLivesUntilReload(t *testing.T) {
	srv := New(config.ServerConfig{Host: "127.0.0.1", Port: 0, MaxConcurrentTasks: 3})
	now := time.Now()
	srv.setConcurrencyCapOverride(5, "test", now)
	srv.syncConcurrencyCap(now.Add(time.Hour))
	if got := srv.orchestrator.ConcurrencyCap(); got != 5 {
		t.Fatalf("expected the override to hold without a schedule, got %d", got)
	}

	if !srv.reload(config.ServerConfig{Host: "127.0.0.1", Port: 0, MaxConcurrentTasks: 7}) {
		t.Fatal("the reload raises the cap from the override 5 to 7; the queue should drain")
	}
	if got := srv.orchestrator.ConcurrencyCap(); got != 7 {
		t.Fatalf("expected the reloaded config cap 7 to clear the override, got %d", got)
	}
	if src := srv.concurrencyStatus(time.Now())["source"]; src != "max_concurrent_tasks" {
		t.Errorf("expected the source back on the config after reload, got %v", src)
	}
}

// TestConcurrencyOverride_ExpiryLabelsTheEnforcedCap: between a boundary
// and the next sync tick the orchestrator still enforces the override's
// number while the wall clock says it expired. The source must follow the
// enforced number — "manual override" until the schedule takes over in
// fact — so value and provenance agree at the one moment an operator is
// most likely to be reading them.
func TestConcurrencyOverride_ExpiryLabelsTheEnforcedCap(t *testing.T) {
	srv := dayNightServer(t) // cap 4 until 07:00, cap 2 from 07:00, UTC

	dayStart := time.Date(2026, 6, 15, 6, 0, 0, 0, time.UTC)
	srv.syncConcurrencyCap(dayStart)
	srv.setConcurrencyCapOverride(6, "test", dayStart)

	// Past the 07:00 boundary but before any tick has resolved it: the
	// orchestrator still states 6, so the source must still say so too.
	justAfter := time.Date(2026, 6, 15, 7, 0, 30, 0, time.UTC)
	status := srv.concurrencyStatus(justAfter)
	if status["cap"] != 6 {
		t.Fatalf("expected the still-enforced override 6 before the tick, got %v", status["cap"])
	}
	if status["source"] != "manual override" {
		t.Errorf("expected the source to follow the enforced cap until the tick hands over, got %v", status["source"])
	}

	// Once the tick has run, both the cap and its provenance are the
	// schedule's again.
	srv.syncConcurrencyCap(justAfter)
	status = srv.concurrencyStatus(justAfter)
	if status["cap"] != 2 {
		t.Fatalf("expected the scheduled cap 2 after the tick, got %v", status["cap"])
	}
	if status["source"] != `concurrency_schedule at "07:00"` {
		t.Errorf("expected the source back on the schedule after the tick, got %v", status["source"])
	}
}

// TestConcurrencyOverride_RejectsCrossSiteSimplePOSTs: the state-changing
// endpoint must not answer the cross-origin simple POSTs a browser sends
// without a CORS preflight, so anything that is not an explicit JSON
// request body is refused and leaves the cap in force untouched.
func TestConcurrencyOverride_RejectsCrossSiteSimplePOSTs(t *testing.T) {
	srv := capTestServer(t, config.ServerConfig{Host: "127.0.0.1", Port: 0, MaxConcurrentTasks: 3})
	for _, ct := range []string{"", "text/plain", "application/x-www-form-urlencoded", "application/xml"} {
		req := httptest.NewRequest(http.MethodPost, "/api/concurrency", strings.NewReader(`{"cap": 0}`))
		req.RemoteAddr = "127.0.0.1:1234" // identity settled; only the Content-Type is in question
		req.Host = "127.0.0.1:8080"
		if ct != "" {
			req.Header.Set("Content-Type", ct)
		}
		w := httptest.NewRecorder()
		srv.HandleConcurrency(w, req)
		if w.Code != http.StatusUnsupportedMediaType {
			t.Errorf("Content-Type %q: expected 415, got %d", ct, w.Code)
		}
	}
	if got := srv.orchestrator.ConcurrencyCap(); got != 3 {
		t.Fatalf("expected a refused request to leave the cap at 3, got %d", got)
	}

	// A parameterised but genuine JSON type still works (charset and the
	// like must not lock out a correct client).
	req := httptest.NewRequest(http.MethodPost, "/api/concurrency", strings.NewReader(`{"cap": 1}`))
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	req.RemoteAddr = "127.0.0.1:1234"
	req.Host = "localhost:8080"
	w := httptest.NewRecorder()
	srv.HandleConcurrency(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 for application/json with a parameter, got %d: %s", w.Code, w.Body.String())
	}
}

// TestConcurrencyOverride_RequiresOperatorIdentity: reaching the route is
// not a credential (reviews 1635 and 1671). A non-loopback peer cannot
// pause the fleet or lift its cap, and a loopback peer only speaks for
// the operator while it addresses the server by a loopback name: the
// DNS-rebinding attempt — a browser posting from 127.0.0.1 with a foreign
// domain in Host — is refused with the cap exactly where it stood.
func TestConcurrencyOverride_RequiresOperatorIdentity(t *testing.T) {
	postFrom := func(t *testing.T, srv *Server, remote, host string) *httptest.ResponseRecorder {
		t.Helper()
		req := httptest.NewRequest(http.MethodPost, "/api/concurrency", strings.NewReader(`{"cap": 1}`))
		req.Header.Set("Content-Type", "application/json")
		req.RemoteAddr = remote
		req.Host = host
		w := httptest.NewRecorder()
		srv.HandleConcurrency(w, req)
		return w
	}

	srv := capTestServer(t, config.ServerConfig{Host: "127.0.0.1", Port: 0, MaxConcurrentTasks: 3})
	if w := postFrom(t, srv, "203.0.113.9:5555", "127.0.0.1:8080"); w.Code != http.StatusForbidden {
		t.Fatalf("expected an anonymous remote POST to be refused with 403, got %d: %s", w.Code, w.Body.String())
	}
	if got := srv.orchestrator.ConcurrencyCap(); got != 3 {
		t.Fatalf("expected a refused request to leave the cap at 3, got %d", got)
	}
	if w := postFrom(t, srv, "127.0.0.1:4321", "attacker.example:8080"); w.Code != http.StatusForbidden {
		t.Fatalf("expected a DNS-rebinding POST (loopback peer, foreign Host) to be refused with 403, got %d: %s", w.Code, w.Body.String())
	}
	if got := srv.orchestrator.ConcurrencyCap(); got != 3 {
		t.Fatalf("expected the rebinding attempt to leave the cap at 3, got %d", got)
	}
	for _, host := range []string{"127.0.0.1:8080", "localhost:8080", "[::1]:8080"} {
		if w := postFrom(t, srv, "127.0.0.1:4321", host); w.Code != http.StatusOK {
			t.Fatalf("Host %q: expected a loopback POST to be admitted, got %d: %s", host, w.Code, w.Body.String())
		}
	}
	if got := srv.orchestrator.ConcurrencyCap(); got != 1 {
		t.Fatalf("expected the admitted request to set the cap to 1, got %d", got)
	}
}

// TestConcurrencyOverride_RejectsOversizedBody: the endpoint accepts a few
// dozen bytes; an unbounded body must be refused without buffering, and
// the cap in force must not move.
func TestConcurrencyOverride_RejectsOversizedBody(t *testing.T) {
	srv := capTestServer(t, config.ServerConfig{Host: "127.0.0.1", Port: 0, MaxConcurrentTasks: 3})
	w := postConcurrency(t, srv, `{"cap": `+strings.Repeat("9", 4096)+`}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected an oversized body to be refused with 400, got %d", w.Code)
	}
	if got := srv.orchestrator.ConcurrencyCap(); got != 3 {
		t.Fatalf("expected a refused body to leave the cap at 3, got %d", got)
	}
}

// TestConcurrencyOverride_EffectiveConcurrencyAgrees: the health surface,
// the status surface and EffectiveConcurrency must state the same cap:
// the override, not the config value it overrides.
func TestConcurrencyOverride_EffectiveConcurrencyAgrees(t *testing.T) {
	srv := New(config.ServerConfig{Host: "127.0.0.1", Port: 0, MaxConcurrentTasks: 3})
	now := time.Now()
	srv.setConcurrencyCapOverride(2, "test", now)
	if got := srv.EffectiveConcurrency(now); got != 2 {
		t.Fatalf("expected EffectiveConcurrency to honour the override, got %d", got)
	}
	if got := srv.concurrencyStatus(now)["cap"]; got != 2 {
		t.Fatalf("expected the status surface to state the override, got %v", got)
	}
	w := httptest.NewRecorder()
	srv.HandleHealth(w, httptest.NewRequest(http.MethodGet, "/api/health", nil))
	var health map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &health); err != nil {
		t.Fatalf("health is not JSON: %v", err)
	}
	if health["concurrency_cap"] != float64(2) {
		t.Fatalf("expected health to state the override cap, got %v", health["concurrency_cap"])
	}
}
