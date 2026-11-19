package guest

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// Idle watchdog tests (issue #300). The watchdog's production window is
// 10 minutes with a 30-second tick; these tests shorten both through the
// handler's idleTimeout/idleCheckInterval seams so a silent period longer
// than the window costs about a second of wall clock.

const (
	idleTestTimeout = 400 * time.Millisecond
	idleTestTick    = 40 * time.Millisecond
	// idleTestQuiet is the silent period in the fake pi script, in seconds.
	// It is several times idleTestTimeout, so whether the watchdog fires
	// during the silence is not a race.
	idleTestQuiet = 1.5
	// idleTestCeiling is the ceiling on a disarmed watchdog in the ceiling
	// test: past the window, short enough to elapse inside idleTestQuiet.
	idleTestCeiling = 800 * time.Millisecond
)

// withFastIdleWatchdog shortens the watchdog window for tests.
func withFastIdleWatchdog(h *PIHandler) {
	h.idleTimeout = idleTestTimeout
	h.idleCheckInterval = idleTestTick
}

// emitLine turns a pi event literal into a fake-pi script line.
func emitLine(event string) string { return "emit(" + event + ")" }

// silent makes the fake pi go quiet for the given number of seconds without
// emitting any event — the situation the watchdog must tell apart depending
// on whether a tool call is in flight.
func silent(seconds float64) string { return fmt.Sprintf("time.sleep(%g)", seconds) }

const (
	toolStart = `{"type": "tool_execution_start", "toolCallId": "%s", "toolName": "bash", "args": {"command": "make -j"}}`
	toolEnd   = `{"type": "tool_execution_end", "toolCallId": "%s", "toolName": "bash", "result": "ok"}`
)

// TestIdleWatchdog_DisarmedWhileToolCallInFlight verifies that a silent
// period inside a tool call does not fail the task: pi only emits
// tool_execution_update when a tool reports a partial result, so a build or
// an ssh round trip running past the window looks identical to a wedged
// agent if the watchdog measures event silence alone.
func TestIdleWatchdog_DisarmedWhileToolCallInFlight(t *testing.T) {
	result, _ := runFakePiScript(t, "idle-tool-inflight", []string{
		emitLine(`{"type": "agent_start"}`),
		emitLine(fmt.Sprintf(toolStart, "call-1")),
		silent(idleTestQuiet),
		emitLine(fmt.Sprintf(toolEnd, "call-1")),
		emitLine(`{"type": "agent_settled"}`),
	}, withFastIdleWatchdog)

	if !result.Success {
		t.Fatalf("expected task to survive a silent tool call, got error=%q output=%q", result.Error, result.Output)
	}
	if strings.Contains(result.Error, "agent idle") {
		t.Fatalf("watchdog fired during an in-flight tool call: %q", result.Error)
	}
}

// TestIdleWatchdog_FiresWhenNothingRuns verifies the watchdog keeps doing
// its actual job: nothing has happened and nothing is running.
func TestIdleWatchdog_FiresWhenNothingRuns(t *testing.T) {
	result, _ := runFakePiScript(t, "idle-nothing-running", []string{
		emitLine(`{"type": "agent_start"}`),
		silent(idleTestQuiet),
		emitLine(`{"type": "agent_settled"}`),
	}, withFastIdleWatchdog)

	if result.Success {
		t.Fatalf("expected idle abort when no tool call is open, got success with output=%q", result.Output)
	}
	if !strings.Contains(result.Error, "agent idle for") {
		t.Fatalf("expected an 'agent idle for' failure, got %q", result.Error)
	}
}

// TestIdleWatchdog_RearmsAfterToolEnds verifies the window restarts when
// the last open call settles: the quiet period begins after the tool has
// ended, so the watchdog must fire again.
func TestIdleWatchdog_RearmsAfterToolEnds(t *testing.T) {
	result, _ := runFakePiScript(t, "idle-rearm", []string{
		emitLine(fmt.Sprintf(toolStart, "call-1")),
		emitLine(fmt.Sprintf(toolEnd, "call-1")),
		silent(idleTestQuiet),
		emitLine(`{"type": "agent_settled"}`),
	}, withFastIdleWatchdog)

	if result.Success {
		t.Fatalf("expected idle abort after the tool call ended, got success with output=%q", result.Output)
	}
	if !strings.Contains(result.Error, "agent idle for") {
		t.Fatalf("expected an 'agent idle for' failure, got %q", result.Error)
	}
}

// TestIdleWatchdog_ParallelCallsSettleTogether verifies that the quickest
// of two parallel tool calls does not re-arm the window while the slower
// one is still running: only returning to zero outstanding calls re-arms.
func TestIdleWatchdog_ParallelCallsSettleTogether(t *testing.T) {
	result, _ := runFakePiScript(t, "idle-parallel", []string{
		emitLine(fmt.Sprintf(toolStart, "call-fast")),
		emitLine(fmt.Sprintf(toolStart, "call-slow")),
		emitLine(fmt.Sprintf(toolEnd, "call-fast")),
		silent(idleTestQuiet),
		emitLine(fmt.Sprintf(toolEnd, "call-slow")),
		emitLine(`{"type": "agent_settled"}`),
	}, withFastIdleWatchdog)

	if !result.Success {
		t.Fatalf("expected task to survive while a parallel tool call is in flight, got error=%q output=%q", result.Error, result.Output)
	}
}

// TestIdleWatchdog_NestedChildEndKeepsWindowDisarmed verifies that a nested
// call (parentToolCallId, from extensions and codemode) finishing does not
// re-arm the parent's window.
func TestIdleWatchdog_NestedChildEndKeepsWindowDisarmed(t *testing.T) {
	result, _ := runFakePiScript(t, "idle-nested", []string{
		emitLine(fmt.Sprintf(toolStart, "call-parent")),
		emitLine(`{"type": "tool_execution_start", "toolCallId": "call-child", "parentToolCallId": "call-parent", "toolName": "bash", "args": {"command": "true"}}`),
		emitLine(`{"type": "tool_execution_end", "toolCallId": "call-child", "parentToolCallId": "call-parent", "toolName": "bash", "result": "ok"}`),
		silent(idleTestQuiet),
		emitLine(fmt.Sprintf(toolEnd, "call-parent")),
		emitLine(`{"type": "agent_settled"}`),
	}, withFastIdleWatchdog)

	if !result.Success {
		t.Fatalf("expected task to survive while the parent call is in flight, got error=%q output=%q", result.Error, result.Output)
	}
}

// TestIdleWatchdog_UnmatchedEndKeepsWindowDisarmed verifies that an end
// event for a tool call id that was never started cannot settle a call that
// is still running. Only the id's own end may re-arm the window: a plain
// counter would decrement to zero here and abort a healthy run, which is the
// bug this change exists to fix.
func TestIdleWatchdog_UnmatchedEndKeepsWindowDisarmed(t *testing.T) {
	result, _ := runFakePiScript(t, "idle-unmatched-end", []string{
		emitLine(fmt.Sprintf(toolStart, "call-open")),
		emitLine(`{"type": "tool_execution_end", "toolCallId": "call-never-started", "toolName": "bash", "result": "ok"}`),
		silent(idleTestQuiet),
		emitLine(fmt.Sprintf(toolEnd, "call-open")),
		emitLine(`{"type": "agent_settled"}`),
	}, withFastIdleWatchdog)

	if !result.Success {
		t.Fatalf("expected a stray end event to leave the watchdog disarmed, got error=%q output=%q", result.Error, result.Output)
	}
}

// TestIdleWatchdog_DuplicateEndKeepsWindowDisarmed is the same guarantee
// from the other side: a duplicated end for a call that already settled must
// not settle its slower sibling.
func TestIdleWatchdog_DuplicateEndKeepsWindowDisarmed(t *testing.T) {
	result, _ := runFakePiScript(t, "idle-duplicate-end", []string{
		emitLine(fmt.Sprintf(toolStart, "call-fast")),
		emitLine(fmt.Sprintf(toolStart, "call-slow")),
		emitLine(fmt.Sprintf(toolEnd, "call-fast")),
		emitLine(fmt.Sprintf(toolEnd, "call-fast")),
		silent(idleTestQuiet),
		emitLine(fmt.Sprintf(toolEnd, "call-slow")),
		emitLine(`{"type": "agent_settled"}`),
	}, withFastIdleWatchdog)

	if !result.Success {
		t.Fatalf("expected a duplicated end event to leave the watchdog disarmed, got error=%q output=%q", result.Error, result.Output)
	}
}

// TestIdleWatchdog_CeilingAbortsCallThatNeverReturns covers the failure mode
// the disarm accepts: an end that never arrives — pi wedged inside a tool
// call, or a call an untrusted prompt steered the agent into that never
// returns. Silence with a call in flight is survivable but not eternal, so
// the watchdog gives up at the ceiling rather than staying disarmed for the
// rest of the task (which, with no task deadline configured, would be forever).
func TestIdleWatchdog_CeilingAbortsCallThatNeverReturns(t *testing.T) {
	result, _ := runFakePiScript(t, "idle-ceiling", []string{
		emitLine(`{"type": "agent_start"}`),
		emitLine(fmt.Sprintf(toolStart, "call-hung")),
		silent(idleTestQuiet),
		emitLine(`{"type": "agent_settled"}`),
	}, func(h *PIHandler) {
		withFastIdleWatchdog(h)
		h.idleCeiling = idleTestCeiling
	})

	if result.Success {
		t.Fatalf("expected a call that never returns to be aborted at the ceiling, got success output=%q", result.Output)
	}
	if !strings.Contains(result.Error, "agent idle for") || !strings.Contains(result.Error, "in flight") {
		t.Fatalf("expected an idle abort naming the call in flight, got %q", result.Error)
	}
}

// TestIdleWatchdog_DisarmedDuringCompaction verifies compaction — which has
// the same shape as a silent tool call: start and end, nothing in between —
// also keeps the watchdog disarmed.
func TestIdleWatchdog_DisarmedDuringCompaction(t *testing.T) {
	result, _ := runFakePiScript(t, "idle-compaction", []string{
		emitLine(`{"type": "agent_start"}`),
		emitLine(`{"type": "compaction_start", "reason": "threshold"}`),
		silent(idleTestQuiet),
		emitLine(`{"type": "compaction_end", "reason": "threshold", "result": {"summary": "## Goal\nSummarised.", "firstKeptEntryId": "abc", "tokensBefore": 150000, "estimatedTokensAfter": 32000}}`),
		emitLine(`{"type": "agent_settled"}`),
	}, withFastIdleWatchdog)

	if !result.Success {
		t.Fatalf("expected task to survive a silent compaction, got error=%q output=%q", result.Error, result.Output)
	}
}
