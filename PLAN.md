# Plan: Fix re-queue issues for stuck tasks

## Problem

```
task task-XXX stuck on guest guest-YYY for > 1m30s (guest never confirmed assignment), re-queuing
failed to re-queue stuck task task-XXX: invalid status transition: PENDING -> PENDING
```

`checkStuckTasks()` detects a guest with a TaskID but no task heartbeat. It calls
`UpdateStatus(taskID, PENDING)` unconditionally. If the task is already PENDING
(another path cleaned it up), the transition fails.

## Root causes

1. **`checkStuckTasks` doesn't handle already-PENDING tasks.** The task status
   and guest.TaskID can diverge: `handleGuestTaskDeclined` reverts the task to
   PENDING and clears guest.TaskID, but if clearing fails (or there's a race
   with re-registration setting TaskID again), the guest has a stale TaskID
   pointing to a PENDING task.

2. **`tryAssignTask` doesn't clean up on `SendToGuest` failure.** When the
   notification can't be delivered, the task is left ASSIGNED and the guest
   has TaskID set — but the guest never received the assignment. This is the
   primary source of stuck tasks.

## Changes

### 1. Fix `checkStuckTasks` (defensive)

Check the task's actual status before re-queuing:
- ASSIGNED or RUNNING → mark as FAILED with "stuck" error, then re-queue to PENDING
- Already PENDING → just clear guest.TaskID (no status change needed)
- Terminal state (COMPLETED/FAILED/CANCELLED) → just clear guest.TaskID

### 2. Fix `tryAssignTask` (root cause)

When `SendToGuest` fails, revert the assignment:
- Clear guest task in registry
- Re-queue task to PENDING

### 3. Tests

- Test: stuck task recovery when task is already PENDING (guest.TaskID stale)
- Test: stuck task recovery when task is ASSIGNED (normal case)
- Test: `tryAssignTask` cleans up when `SendToGuest` fails
- Test: `tryAssignTaskToEligible` cleans up when `SendToGuest` fails

## Implementation

### `checkStuckTasks` (server.go)

Before attempting `UpdateStatus(PENDING)`, fetch the task and check its current
status:
- **ASSIGNED** → re-queue to PENDING (existing behaviour)
- **PENDING** → task already cleaned up; just clear guest.TaskID
- **Terminal (COMPLETED/FAILED/CANCELLED)** → just clear guest.TaskID
- **Not found** → clear guest.TaskID (stale reference)

The UI broadcast is now sent only when the task was actually re-queued
(ASSIGNED → PENDING), avoiding spurious "task pending" notifications.

### `tryAssignTask` (server.go)

When `SendToGuest` fails after `Assign` and `SetGuestTask`, rollback both:
1. `taskQueue.UpdateStatus(taskID, PENDING)` — reverts the task
2. `registry.ClearGuestTask(guestID)` — clears guest state (sets IDLE)

### `UpdateStatus` (queue.go)

When transitioning to PENDING, clear `AssignedTo` so the task has no stale
assignee reference.

## Tests added

- `TestCheckStuckTasks_TaskAlreadyPending` — guest has stale TaskID, task is
  already PENDING. No error, guest.TaskID cleared.
- `TestCheckStuckTasks_TaskInTerminalState` — guest has stale TaskID, task is
  COMPLETED/FAILED/CANCELLED. No error, guest.TaskID cleared.
- `TestTryAssignTask_SendToGuestFails_CleansUp` — SendToGuest fails, task
  reverts to PENDING, guest reverts to IDLE.
- `TestTryAssignTask_AssignsToIdleGuestWithConnection` — happy path with
  connection mapping; notification is sent and verified.
- Updated `TestTryAssignTask_AssignsToIdleGuest` to use connection mapping
  (previously tested buggy behaviour where task stayed ASSIGNED).

## Status

✅ All tests pass (including race detection)
✅ Build succeeds
✅ Formatting clean (gofumpt)
✅ Coverage maintained at 61.5%
