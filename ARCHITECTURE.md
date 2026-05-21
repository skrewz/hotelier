# Hotelier — Ephemeral YOLO Guest Queue

> Autonomous AI development infrastructure with ephemeral guest queuing via JSON-RPC.



## Overview

Hotelier is a distributed task orchestration system that manages ephemeral
autonomous AI guests ("machines") for development work. Guests register
themselves with the system, declare their capabilities, receive tasks, execute
them against target repositories, stream logs back to the host, and submit
final results.


## High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        CHECK-IN HOST                            │
│                                                                 │
│  ┌───────────────┐    ┌──────────────────┐    ┌──────────────┐  │
│  │    JSON-RPC   │    │   Task Queue     │    │     UI       │  │
│  │   Endpoints   │◄──►│   & Scheduling   │◄──►│   (Web)      │  │
│  └───────┬───────┘    └──────────────────┘    └──────────────┘  │
│          │                                                      │
│          │  log streams, results                                │
│          ▼                                                      │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │                     Guest Registry                       │   │
│  └──────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
               │                │                │
               │                │                │
          ┌────┴──────┐    ┌────┴──────┐    ┌────┴──────┐
          │ Machine 1 │    │ Machine 2 │    │ Machine N │
          │  (Guest)  │    │  (Guest)  │    │  (Guest)  │
          └───────────┘    └───────────┘    └───────────┘
```


## Components

### 1. Check-In Host (Server)

The central orchestrator. All guests and clients communicate with it.

#### Responsibilities

- **Guest Registration**: Accept guest check-ins, store capabilities/tags,
  track liveness.
- **Task Scheduling**: Match incoming tasks to available guests based on tags.
- **Task Dispatch**: Push tasks to guests via JSON-RPC.
- **Log Ingestion**: Receive streaming logs from executing guests.
- **Result Collection**: Accept final task results from guests.
- **UI Hosting**: Serve a web interface for human operators to monitor and
  manage the system.

#### Sub-Components

| Sub-Component     | Description                                                  |
|-------------------|--------------------------------------------------------------|
| **JSON-RPC API**  | Bidirectional JSON-RPC endpoints for guest communication.   |
| **Task Queue**    | In-memory (or persistent) queue holding pending tasks.       |
| **Guest Registry**| Tracks connected guests, their tags, capabilities, status.  |
| **Web UI**        | Dashboard for humans to submit tasks, view guest status,    |
|                   | watch live logs, and review results.                         |

---

### 2. Guest Client Library

A library/sdk that guests use to connect to the Check-In Host. Any machine that
wants to participate in the queue runs this library.

#### Responsibilities

- **Register / Check In**: Connect to the host and announce available
  capabilities (tags).
- **Poll / Subscribe**: Listen for incoming tasks via JSON-RPC.
- **Execute Task**: Perform the work described in the task prompt against the
  target repositories.
- **Stream Logs**: Push execution logs back to the host in real time.
- **Submit Result**: Send the final output / patch / report back to the host
  upon completion (success or failure).
- **Heartbeat**: Periodically signal liveness to the host.

---

### 3. Guests ("Machines")

The actual autonomous AI guests — the worker processes that do the development work.

- Run the **Guest Client Library**.
- Each guest has a set of **tags** describing what it can do.
- Ephemeral by design: guests check in when available, take a task, complete
  it, and become idle (or disconnect).

---

## Tags & Capabilities

When a guest checks in, it declares one or more tags. The scheduler uses these
to route tasks.

| Tag               | Description                                              |
|-------------------|----------------------------------------------------------|
| `business-default`| Standard business-application development (default tag). |
| `android`         | Guest is equipped for Android development tasks.         |
| *(custom)*        | Any additional capability tag the guest supports.        |

Tags are flexible — new ones can be added as guests gain new capabilities.

---

## Task Schema

A task pushed to a guest contains at minimum:

| Field       | Type     | Description                                       |
|-------------|----------|---------------------------------------------------|
| `id`        | `string` | Unique task identifier.                           |
| `repos`     | `string[]` | List of repository URLs/paths the guest should work with. |
| `prompt`    | `string` | Natural-language instructions for the guest.      |
| `tags`      | `string[]` | Required tag(s) the assigned guest must have.     |
| `status`    | `string` | `pending` → `assigned` → `running` → `completed` / `failed` |
| `created_at`| `timestamp` | When the task was created.                      |
| `assigned_to`| `string` | ID of the guest assigned (after assignment).      |

*Additional fields may be added as the system evolves.*

---

## Guest Workflow

```
Guest starts
    │
    ▼
┌──────────────┐     ┌───────────────┐     ┌──────────────┐
│  Check In    │────►│  Register     │────►│  Declare     │
│  (connect)   │     │  capabilities │     │  tags        │
└──────────────┘     └───────────────┘     └──────────────┘
                                              │
                                              ▼
                                       ┌──────────────┐
                                       │  Wait for    │
                                       │  task        │
                                       └──────┬───────┘
                                              │
                    ┌─────────────────────────┘
                    ▼
            ┌──────────────┐
            │ Receive Task │
            │ (repos +     │
            │  prompt +    │
            │  tags)       │
            └──────┬───────┘
                   │
                   ▼
            ┌──────────────┐     ┌───────────────┐
            │  Execute     │────►│  Stream Logs  │
            │  Work        │     │  to Host      │
            └──────┬───────┘     └───────────────┘
                   │
          ┌────────┴────────┐
          │                 │
          ▼                 ▼
   ┌──────────────┐  ┌──────────────┐
   │  Success     │  │  Failure     │
   └──────┬───────┘  └──────┬───────┘
          │                 │
          ▼                 ▼
   ┌──────────────────────────────┐
   │  Submit Final Result to Host │
   └──────────────────────────────┘
          │
          ▼
   ┌──────────────┐
   │  Idle /      │
   │  Disconnect  │
   └──────────────┘
```

---

## JSON-RPC Interface

The Check-In Host exposes JSON-RPC methods for guest communication.

### Guest → Host Methods

| Method              | Direction   | Description                              |
|---------------------|-------------|------------------------------------------|
| `guest.register`    | Guest→Host  | Check in with tags and metadata.         |
| `guest.unregister`  | Guest→Host  | Disconnect / deregister.                 |
| `guest.heartbeat`   | Guest→Host  | Liveness ping.                           |
| `guest.log`         | Guest→Host  | Push a log entry (chunk).                |
| `guest.result`      | Guest→Host  | Submit final task result.                |

### Host → Guest Methods

| Method              | Direction   | Description                              |
|---------------------|-------------|------------------------------------------|
| `task.assign`       | Host→Guest  | Push a task to the guest.                |
| `task.cancel`       | Host→Guest  | Cancel an assigned task.                 |

### Host → Client (UI) Methods

| Method              | Direction   | Description                              |
|---------------------|-------------|------------------------------------------|
| `task.submit`       | Client→Host | Human operator submits a new task.       |
| `task.list`         | Client→Host | Query pending / active / completed tasks.|
| `guest.list`        | Client→Host | Query registered guests.                 |
| `task.logs`         | Client→Host | Subscribe to live logs for a task.       |

---

## Communication Flow Diagram

```
 Human Operator              Check-In Host                Guest Machine
 ──────────────             ──────────────               ─────────────
      │                          │                          │
      │  task.submit             │                          │
      ├─────────────────────────►│                          │
      │                          │                          │
      │                          │  task.assign             │
      │                          ├─────────────────────────►│
      │                          │                          │
      │                          │  guest.log (stream)      │
      │                          │◄─────────────────────────┤
      │  task.logs (subscribe)   │                          │
      ├─────────────────────────►│                          │
      │◄─────────────────────────┤                          │
      │  log chunks              │                          │
      │                          │                          │
      │                          │  guest.result            │
      │                          │◄─────────────────────────┤
      │                          │                          │
      │  task status updated     │                          │
      │◄─────────────────────────┤                          │
```

---

## State Model

### Guest State

```
DISCONNECTED → REGISTERED → IDLE → RUNNING → IDLE → (DISCONNECTED)
```

- **DISCONNECTED**: Not connected to the host.
- **REGISTERED**: Connected, heartbeating, but no active task.
- **IDLE**: Registered and available to accept tasks.
- **RUNNING**: Currently executing a task.

### Task State

```
PENDING → ASSIGNED → RUNNING → COMPLETED
                         └── FAILED
```

---

## Extensibility

- **New Tags**: Guests can declare arbitrary capability tags at check-in.
- **New Task Fields**: The task schema is open for extension.
- **New Guest Types**: Any machine running the client library can join the pool.
- **Persistent Storage**: The host's in-memory state can be backed by a database for durability.
