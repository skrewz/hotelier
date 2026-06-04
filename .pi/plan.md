# Live event streaming fix

## Problem

When viewing a task detail page (e.g. `/task/task-1780569046361331539`), live events do not stream to the browser. The user must manually refresh the URL to see new tool/thinking/message outputs.

## Root cause analysis

Tracing the full event flow:

1. **Server → Browser `task.log` notifications**: These ARE sent correctly via WebSocket. The `handleGuestLog` handler broadcasts `task.log` to all browser connections. The frontend `appendLogToDetail()` handles them. **This part works.**

2. **Server → Browser `task.updated` notifications**: These are ONLY sent when a task completes or fails (`handleGuestResult`). They are NOT sent when:
   - Task is assigned to a guest (PENDING → ASSIGNED)
   - Task starts executing (ASSIGNED → RUNNING)
   - Task is re-queued after decline (RUNNING → PENDING)

3. **No fallback refresh for detail view**: The 5-second `refresh()` interval only updates the task list and stats. It does NOT refresh the active task detail. If WebSocket `task.log` events are lost or delayed, the user sees stale data.

## Plan

### 1. Broadcast `task.updated` on status transitions (server.go)

After every task status change, broadcast a `task.updated` notification to browser connections:
- After `tryAssignTask` succeeds (PENDING → ASSIGNED)
- After `tryAssignTaskToEligible` succeeds (PENDING → ASSIGNED)
- In `handleGuestTaskDeclined` when task is re-queued (already done ✓)
- On first log entry for a task (detect ASSIGNED → RUNNING transition)

### 2. Add fallback detail view refresh (index.html)

When a task detail is active, periodically refresh the task status from the API. This ensures the status badge and log count stay current even if WebSocket events are missed.

### 3. Add error logging to ws.onmessage (index.html)

Replace the empty `catch {}` block with proper error logging for debugging.
