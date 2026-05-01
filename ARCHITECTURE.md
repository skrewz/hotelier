# Hotelier — Ephemeral YOLO Agent Queue

> Autonomous AI development infrastructure with ephemeral agent queuing via JSON-RPC.



## Overview

Hotelier is a distributed task orchestration system that manages ephemeral
autonomous AI agents ("machines") for development work. Agents register
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
│  │                     Agent Registry                       │   │
│  └──────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
               │                │                │
               │                │                │
          ┌────┴──────┐    ┌────┴──────┐    ┌────┴──────┐
          │ Machine 1 │    │ Machine 2 │    │ Machine N │
          │  (Agent)  │    │  (Agent)  │    │  (Agent)  │
          └───────────┘    └───────────┘    └───────────┘
```


## Components

### 1. Check-In Host (Server)

The central orchestrator. All agents and clients communicate with it.

#### Responsibilities

- **Agent Registration**: Accept agent check-ins, store capabilities/tags,
  track liveness.
- **Task Scheduling**: Match incoming tasks to available agents based on tags.
- **Task Dispatch**: Push tasks to agents via JSON-RPC.
- **Log Ingestion**: Receive streaming logs from executing agents.
- **Result Collection**: Accept final task results from agents.
- **UI Hosting**: Serve a web interface for human operators to monitor and
  manage the system.

#### Sub-Components

| Sub-Component     | Description                                                  |
|-------------------|--------------------------------------------------------------|
| **JSON-RPC API**  | Bidirectional JSON-RPC endpoints for agent communication.   |
| **Task Queue**    | In-memory (or persistent) queue holding pending tasks.       |
| **Agent Registry**| Tracks connected agents, their tags, capabilities, status.  |
| **Web UI**        | Dashboard for humans to submit tasks, view agent status,    |
|                   | watch live logs, and review results.                         |

---

### 2. Agent Client Library

A library/sdk that agents use to connect to the Check-In Host. Any machine that
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

### 3. Agents ("Machines")

The actual autonomous AI agents — the worker processes that do the development work.

- Run the **Agent Client Library**.
- Each agent has a set of **tags** describing what it can do.
- Ephemeral by design: agents check in when available, take a task, complete
  it, and become idle (or disconnect).

---

## Tags & Capabilities

When an agent checks in, it declares one or more tags. The scheduler uses these
to route tasks.

| Tag               | Description                                              |
|-------------------|----------------------------------------------------------|
| `business-default`| Standard business-application development (default tag). |
| `android`         | Agent is equipped for Android development tasks.         |
| *(custom)*        | Any additional capability tag the agent supports.        |

Tags are flexible — new ones can be added as agents gain new capabilities.

---

## Task Schema

A task pushed to an agent contains at minimum:

| Field       | Type     | Description                                       |
|-------------|----------|---------------------------------------------------|
| `id`        | `string` | Unique task identifier.                           |
| `repos`     | `string[]` | List of repository URLs/paths the agent should work with. |
| `prompt`    | `string` | Natural-language instructions for the agent.      |
| `tags`      | `string[]` | Required tag(s) the assigned agent must have.     |
| `status`    | `string` | `pending` → `assigned` → `running` → `completed` / `failed` |
| `created_at`| `timestamp` | When the task was created.                      |
| `assigned_to`| `string` | ID of the agent assigned (after assignment).      |

*Additional fields may be added as the system evolves.*

---

## Agent Workflow

```
Agent starts
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

The Check-In Host exposes JSON-RPC methods for agent communication.

### Agent → Host Methods

| Method              | Direction   | Description                              |
|---------------------|-------------|------------------------------------------|
| `agent.register`    | Agent→Host  | Check in with tags and metadata.         |
| `agent.unregister`  | Agent→Host  | Disconnect / deregister.                 |
| `agent.heartbeat`   | Agent→Host  | Liveness ping.                           |
| `agent.log`         | Agent→Host  | Push a log entry (chunk).                |
| `agent.result`      | Agent→Host  | Submit final task result.                |
| `task.claim`        | Agent→Host  | Voluntarily claim a pending task.        |

### Host → Agent Methods

| Method              | Direction   | Description                              |
|---------------------|-------------|------------------------------------------|
| `task.assign`       | Host→Agent  | Push a task to the agent.                |
| `task.cancel`       | Host→Agent  | Cancel an assigned task.                 |

### Host → Client (UI) Methods

| Method              | Direction   | Description                              |
|---------------------|-------------|------------------------------------------|
| `task.submit`       | Client→Host | Human operator submits a new task.       |
| `task.list`         | Client→Host | Query pending / active / completed tasks.|
| `agent.list`        | Client→Host | Query registered agents.                 |
| `task.logs`         | Client→Host | Subscribe to live logs for a task.       |

---

## Communication Flow Diagram

```
 Human Operator              Check-In Host                Agent Machine
 ──────────────             ──────────────               ─────────────
      │                          │                          │
      │  task.submit             │                          │
      ├─────────────────────────►│                          │
      │                          │                          │
      │                          │  task.assign             │
      │                          ├─────────────────────────►│
      │                          │                          │
      │                          │  agent.log (stream)      │
      │                          │◄─────────────────────────┤
      │  task.logs (subscribe)   │                          │
      ├─────────────────────────►│                          │
      │◄─────────────────────────┤                          │
      │  log chunks              │                          │
      │                          │                          │
      │                          │  agent.result            │
      │                          │◄─────────────────────────┤
      │                          │                          │
      │  task status updated     │                          │
      │◄─────────────────────────┤                          │
```

---

## State Model

### Agent State

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

- **New Tags**: Agents can declare arbitrary capability tags at check-in.
- **New Task Fields**: The task schema is open for extension.
- **New Agent Types**: Any machine running the client library can join the pool.
- **Persistent Storage**: The host's in-memory state can be backed by a database for durability.
