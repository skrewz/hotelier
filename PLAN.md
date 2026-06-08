# Plan: Fix duplicate task assignment on guest reconnection

## Problem

Guest receives `task.assign` for the same task twice:

```
[RPC] received notification: task.assign
[RPC] dispatching task task-XXX to execution
[RPC] task task-XXX queued for execution
[DISPATCH] starting task task-XXX
[DISPATCH] task task-XXX error: guest is already running a task
[DISPATCH] declined task task-XXX: guest is already running a task
```

**Root cause:** When a guest reconnects (network blip), `handleGuestRegister`
detects it as a re-registration and **unconditionally resets**
`existing.State = GuestStateIdle`, then calls `tryAssignTask()`. But the
guest's client-side `running` flag is still `true` — it's mid-task. The server
sees IDLE, sends a duplicate assignment. The guest rejects it, declines it,
and the server re-queues it — potentially looping.

## Fixes

### 1. Server-side: preserve RUNNING state on re-registration

In `handleGuestRegister`, when a guest re-registers, preserve its existing
state instead of blindly resetting to IDLE. Update only the connection
metadata (name, tags, connectedAt, lastHeartbeat, connection mapping).

- If guest was RUNNING → keep RUNNING. `tryAssignTask` checks
  `State != Idle` so it will skip the guest.
- When the task completes and `handleGuestResult` fires, the guest is
  cleared to IDLE and gets the next task naturally.
- Edge case: if the task finished *before* reconnection but the result
  was lost in transit, the guest stays RUNNING on the server. The stuck-task
  detector eventually catches this. Safe, minor delay.

### 2. Guest-side: deduplicate task assignments

In the guest's `task.assign` notification handler, check if the task ID
matches `g.currentTaskID`. If so, silently ignore the duplicate — the guest
is already executing that task.

## Tests

- `TestHandleGuestRegister_PreservesRunningState` — guest re-registers while
  RUNNING; state preserved, no duplicate assignment sent.
- `TestHandleGuestRegister_IdleGuestGetsTask` — guest re-registers while IDLE;
  task assignment still works normally.
- `TestGuest_DuplicateTaskAssignmentIgnored` — guest receives `task.assign`
  for a task it's already running; duplicate is ignored, not queued.
