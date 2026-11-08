package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"hotelier/pkg/config"
	"hotelier/pkg/queue"
	"hotelier/pkg/rpc"
)

// capTestServer returns a server with two connected guests, each on a test
// connection, so an assignment can be observed without a real client and a cap
// below the guest count is the only thing holding the queue back (issue #249).
func capTestServer(t *testing.T, cfg config.ServerConfig) *Server {
	t.Helper()
	srv := New(cfg)
	go srv.hub.Run()
	// hub.Register is a channel send the hub goroutine picks up, so each
	// connection below is waited on before it can be assigned to.
	for i, id := range []string{"guest-1", "guest-2"} {
		conn := rpc.NewTestConnection(fmt.Sprintf("conn-%d", i+1), srv.hub)
		srv.hub.Register(conn)
		for srv.hub.ConnectionCount() < i+1 {
			time.Sleep(5 * time.Millisecond)
		}
		srv.hub.RegisterGuestConnection(id, conn.ID())
		if err := srv.orchestrator.RegisterGuest(id, "Guest "+id, nil); err != nil {
			t.Fatalf("RegisterGuest %s: %v", id, err)
		}
		t.Cleanup(func() { srv.hub.Unregister(conn) })
	}
	return srv
}

// assigneeOf returns the guest a task was handed to, since which of the two
// test guests picks work up is not part of what these tests pin down.
func assigneeOf(t *testing.T, srv *Server, id string) string {
	t.Helper()
	task, ok := srv.orchestrator.GetTask(id)
	if !ok {
		t.Fatalf("task %s not found", id)
	}
	if task.AssignedTo == "" {
		t.Fatalf("expected task %s to be assigned", id)
	}
	return task.AssignedTo
}

// submitTask posts a task to the HTTP API, mirroring what an operator or a
// forge integration does.
func submitTask(t *testing.T, srv *Server, id string) {
	t.Helper()
	body, _ := json.Marshal(map[string]interface{}{"id": id, "prompt": id})
	req := httptest.NewRequest(http.MethodPost, "/api/tasks", bytes.NewReader(body))
	w := httptest.NewRecorder()
	srv.HandleTasks(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("submit %s: expected 201, got %d: %s", id, w.Code, w.Body.String())
	}
}

func taskStatus(t *testing.T, srv *Server, id string) queue.TaskStatus {
	t.Helper()
	task, ok := srv.orchestrator.GetTask(id)
	if !ok {
		t.Fatalf("task %s not found", id)
	}
	return task.Status
}

// TestNew_PushesStaticCapIntoOrchestrator: the cap is not only a number the
// server can answer questions about — it must reach the choke point that
// decides assignments, at startup and without a reload.
func TestNew_PushesStaticCapIntoOrchestrator(t *testing.T) {
	srv := New(config.ServerConfig{Host: "127.0.0.1", Port: 0, MaxConcurrentTasks: 3})
	if got := srv.orchestrator.ConcurrencyCap(); got != 3 {
		t.Fatalf("expected the static cap 3 at the orchestrator, got %d", got)
	}

	// An unconfigured deployment must stay unlimited, i.e. today's behaviour.
	unlimited := New(config.ServerConfig{Host: "127.0.0.1", Port: 0})
	if got := unlimited.orchestrator.ConcurrencyCap(); got != config.ConcurrencyUnlimited {
		t.Fatalf("expected an unconfigured server to be unlimited, got %d", got)
	}
}

// TestReload_PushesCapIntoOrchestrator: a hot-reloaded cap takes effect
// without a restart, the way max_guests does.
func TestReload_PushesCapIntoOrchestrator(t *testing.T) {
	srv := New(config.ServerConfig{Host: "127.0.0.1", Port: 0})
	srv.Reload(config.ServerConfig{Host: "127.0.0.1", Port: 0, MaxConcurrentTasks: 2})
	if got := srv.orchestrator.ConcurrencyCap(); got != 2 {
		t.Fatalf("expected the reloaded cap 2 at the orchestrator, got %d", got)
	}
}

// capTestSchedule builds a two-entry UTC schedule that carries `before` from
// midnight until roughly an hour from now and `after` from that boundary on,
// so a test can observe a boundary being crossed without waiting for a fixed
// wall-clock hour. The boundary is clamped inside today because the resolver
// lets the last entry of the day hold through midnight.
func capTestSchedule(t *testing.T, before, after int) []config.ConcurrencyScheduleEntry {
	t.Helper()
	now := time.Now().UTC()
	if now.Hour() == 23 && now.Minute() >= 58 {
		t.Skip("within two minutes of midnight; the boundary cannot be placed inside today")
	}
	boundary := now.Add(time.Hour).UTC()
	if boundary.Day() != now.Day() {
		boundary = time.Date(now.Year(), now.Month(), now.Day(), 23, 59, 0, 0, time.UTC)
	}
	return []config.ConcurrencyScheduleEntry{
		{At: "00:00", Concurrency: before},
		{At: fmt.Sprintf("%02d:%02d", boundary.Hour(), boundary.Minute()), Concurrency: after},
	}
}

// TestSubmit_AtCap_TaskStaysPending is the issue's first verification, at the
// widest boundary available: two submissions, two idle guests and a cap of one
// must leave the second task PENDING — the cap, not guest availability.
func TestSubmit_AtCap_TaskStaysPending(t *testing.T) {
	srv := capTestServer(t, config.ServerConfig{Host: "127.0.0.1", Port: 0, MaxConcurrentTasks: 1})

	submitTask(t, srv, "t1")
	submitTask(t, srv, "t2")

	if got := taskStatus(t, srv, "t1"); got != queue.TaskStatusAssigned {
		t.Fatalf("expected t1 ASSIGNED, got %s", got)
	}
	if got := taskStatus(t, srv, "t2"); got != queue.TaskStatusPending {
		t.Fatalf("expected t2 to stay PENDING at the cap, got %s", got)
	}
	if got := srv.orchestrator.InFlightCount(); got != 1 {
		t.Fatalf("expected 1 task in flight, got %d", got)
	}
}

// TestCompleteTask_FreesSlotAndDrainsWithoutRestart is the issue's second
// verification: the task that was held must go out on the completion alone.
func TestCompleteTask_FreesSlotAndDrainsWithoutRestart(t *testing.T) {
	srv := capTestServer(t, config.ServerConfig{Host: "127.0.0.1", Port: 0, MaxConcurrentTasks: 1})

	submitTask(t, srv, "t1")
	submitTask(t, srv, "t2")
	if got := taskStatus(t, srv, "t2"); got != queue.TaskStatusPending {
		t.Fatalf("expected t2 held at the cap, got %s", got)
	}

	// The guest reporting the result is the event; no restart, no manual
	// pass, and no second guest polling.
	params, _ := json.Marshal(map[string]interface{}{"task_id": "t1", "success": true, "output": "done"})
	if _, rpcErr := srv.hub.Dispatch("guest.result", params); rpcErr != nil {
		t.Fatalf("guest.result: %v", rpcErr)
	}

	if got := taskStatus(t, srv, "t2"); got != queue.TaskStatusAssigned {
		t.Fatalf("expected t2 ASSIGNED once the slot freed, got %s", got)
	}
}

// TestReload_RaisingCapDrainsQueueWithoutAnEvent: raising the cap is itself
// the event. No guest may have to poll again for the queue to move.
func TestReload_RaisingCapDrainsQueueWithoutAnEvent(t *testing.T) {
	srv := capTestServer(t, config.ServerConfig{Host: "127.0.0.1", Port: 0, MaxConcurrentTasks: 1})

	submitTask(t, srv, "t1")
	submitTask(t, srv, "t2")
	if got := taskStatus(t, srv, "t2"); got != queue.TaskStatusPending {
		t.Fatalf("expected t2 held at the cap, got %s", got)
	}

	srv.Reload(config.ServerConfig{Host: "127.0.0.1", Port: 0, MaxConcurrentTasks: 2})

	if got := taskStatus(t, srv, "t2"); got != queue.TaskStatusAssigned {
		t.Fatalf("expected t2 ASSIGNED after the cap rose, got %s", got)
	}
}

// TestScheduleBoundary_RisingCapStartsPendingTask: a wall-clock boundary is
// not an event of any kind the server receives — no guest polls, no task is
// submitted — so the schedule sync must notice it on its own and drain the
// queue.
func TestScheduleBoundary_RisingCapStartsPendingTask(t *testing.T) {
	srv := capTestServer(t, config.ServerConfig{
		Host:                "127.0.0.1",
		Port:                0,
		ScheduleTimezone:    "UTC",
		ConcurrencySchedule: capTestSchedule(t, 1, 2),
	})

	submitTask(t, srv, "t1")
	submitTask(t, srv, "t2")
	if got := taskStatus(t, srv, "t2"); got != queue.TaskStatusPending {
		t.Fatalf("expected t2 held under the cap in force now, got %s", got)
	}

	// Before the boundary: the cap is unchanged, so nothing starts.
	srv.refreshConcurrency(time.Now().Add(30 * time.Second))
	if rose := srv.syncConcurrencyCap(time.Now().Add(30 * time.Second)); rose {
		t.Fatal("expected no cap change before the boundary")
	}
	if got := taskStatus(t, srv, "t2"); got != queue.TaskStatusPending {
		t.Fatalf("expected t2 still held before the boundary, got %s", got)
	}

	// After the boundary: the cap rose, so the queue drains.
	srv.refreshConcurrency(time.Now().Add(2 * time.Hour))
	if got := taskStatus(t, srv, "t2"); got != queue.TaskStatusAssigned {
		t.Fatalf("expected t2 ASSIGNED after the boundary, got %s", got)
	}
}

// TestSyncConcurrencyCap_PausingDoesNotDisturbRunningTask: the cap dropping
// to a full pause leaves the task already running alone.
func TestSyncConcurrencyCap_PausingDoesNotDisturbRunningTask(t *testing.T) {
	srv := capTestServer(t, config.ServerConfig{
		Host:                "127.0.0.1",
		Port:                0,
		ScheduleTimezone:    "UTC",
		ConcurrencySchedule: capTestSchedule(t, 2, 0),
	})

	submitTask(t, srv, "t1")
	if err := srv.orchestrator.AcknowledgeTask("t1", assigneeOf(t, srv, "t1")); err != nil {
		t.Fatalf("ack t1: %v", err)
	}

	srv.refreshConcurrency(time.Now().Add(2 * time.Hour))

	if got := taskStatus(t, srv, "t1"); got != queue.TaskStatusRunning {
		t.Fatalf("expected t1 to keep running through the pause, got %s", got)
	}
	if got := srv.orchestrator.ConcurrencyCap(); got != 0 {
		t.Fatalf("expected the pause (0) to reach the orchestrator, got %d", got)
	}
}

// TestHandleHealth_ReportsCapAndInFlight gives an operator one place to see
// that a queue is being held by the schedule.
func TestHandleHealth_ReportsCapAndInFlight(t *testing.T) {
	srv := capTestServer(t, config.ServerConfig{Host: "127.0.0.1", Port: 0, MaxConcurrentTasks: 2})
	submitTask(t, srv, "t1")

	req := httptest.NewRequest(http.MethodGet, "/api/health", nil)
	w := httptest.NewRecorder()
	srv.HandleHealth(w, req)

	var body map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal health: %v", err)
	}
	if body["concurrency_cap"] != float64(2) {
		t.Errorf("expected concurrency_cap 2, got %v", body["concurrency_cap"])
	}
	if body["in_flight"] != float64(1) {
		t.Errorf("expected in_flight 1, got %v", body["in_flight"])
	}

	// The unlimited case must read as unlimited, not as a cap of -1.
	free := New(config.ServerConfig{Host: "127.0.0.1", Port: 0})
	wFree := httptest.NewRecorder()
	free.HandleHealth(wFree, httptest.NewRequest(http.MethodGet, "/api/health", nil))
	var freeBody map[string]interface{}
	if err := json.Unmarshal(wFree.Body.Bytes(), &freeBody); err != nil {
		t.Fatalf("unmarshal health: %v", err)
	}
	if freeBody["concurrency_cap"] != "unlimited" {
		t.Errorf("expected concurrency_cap \"unlimited\", got %v", freeBody["concurrency_cap"])
	}
}

// TestAssignLog_DistinguishesCapFromDeadPool is the legibility requirement:
// an operator reading the log must be able to tell night mode from a pool
// with no matching guest.
func TestAssignLog_DistinguishesCapFromDeadPool(t *testing.T) {
	var capLog bytes.Buffer
	capped := capTestServer(t, config.ServerConfig{Host: "127.0.0.1", Port: 0, MaxConcurrentTasks: 1})
	capped.log.SetOutput(&capLog)
	submitTask(t, capped, "t1")
	submitTask(t, capped, "t2")
	if !strings.Contains(capLog.String(), "concurrency cap") {
		t.Errorf("expected a cap-hold line in the log, got: %s", capLog.String())
	}

	var poolLog bytes.Buffer
	empty := New(config.ServerConfig{Host: "127.0.0.1", Port: 0})
	empty.log.SetOutput(&poolLog)
	submitTask(t, empty, "t1")
	if !strings.Contains(poolLog.String(), "no available guest") {
		t.Errorf("expected a 'no available guest' line for a task with no matching guest, got: %s", poolLog.String())
	}
	if strings.Contains(poolLog.String(), "concurrency cap") {
		t.Errorf("a dead pool must not be reported as a cap hold: %s", poolLog.String())
	}
}

// TestTaskList_ExposesHeldReason: the reason a pending task is waiting must
// be readable from the task list, not just the log.
func TestTaskList_ExposesHeldReason(t *testing.T) {
	srv := capTestServer(t, config.ServerConfig{Host: "127.0.0.1", Port: 0, MaxConcurrentTasks: 1})
	submitTask(t, srv, "t1")
	submitTask(t, srv, "t2")

	w := httptest.NewRecorder()
	srv.HandleTasks(w, httptest.NewRequest(http.MethodGet, "/api/tasks", nil))
	var resp struct {
		Tasks []map[string]interface{} `json:"tasks"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal tasks: %v", err)
	}
	var held string
	for _, task := range resp.Tasks {
		if task["id"] == "t2" {
			held, _ = task["held_reason"].(string)
		}
	}
	if !strings.Contains(held, "concurrency cap") {
		t.Fatalf("expected held_reason naming the cap for t2, got %q", held)
	}
}
