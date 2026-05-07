# Hotelier

Ephemeral YOLO agent queue — autonomous AI development infrastructure.

## Overview

Hotelier orchestrates ephemeral AI agents ("machines") that register, receive tasks, execute them against target repositories, stream logs, and submit results. Agents are configured entirely via YAML config files.

## Architecture

```
┌──────────────┐    WebSocket     ┌──────────────┐
│  Check-In    │◄────────────────►│    Agent     │
│  Host        │  JSON-RPC 2.0    │  (any machine│
│  :8080       │                  │   running the │
│              │                  │   agent CLI)  │
└──────────────┘                  └──────────────┘
  ├── REST API                      ├── agent.yaml
  ├── Web UI                        ├── Register
  ├── Task Queue                    ├── Execute tasks
  └── Agent Registry                ├── Stream logs
                                    └── Submit results
```

## Quick Start

```bash
make build

# Copy example configs and edit to suit
cp config/server.example.yaml config/server.yaml
cp config/agent.example.yaml config/agent.yaml

./bin/hotelier    # Server on :8080
./bin/agent       # Agent (runs tasks via the pi AI agent)
```

## Configuration

Sample configs are provided in `config/`:

| File | Description |
|------|-------------|
| `config/server.example.yaml` | Check-In Host (server) configuration |
| `config/agent.example.yaml` | Agent configuration |

Copy the examples to `config/server.yaml` and `config/agent.yaml`, then edit as needed.

### Server (`config/server.yaml`)

```yaml
host: "0.0.0.0"
port: 8080
read_timeout: 30
write_timeout: 30
max_log_size: 1048576
task_timeout: 3600
heartbeat_interval: 30
max_agents: 0
```

### Agent (`config/agent.yaml`)

```yaml
host: "localhost"
port: 8080
id: "agent-1"
name: "Dev Agent #1"
tags:
  - "business-default"
  - "frontend"
heartbeat_interval: 15
task_timeout: 1800
working_dir: "/tmp/hotelier"
log_level: "info"
```

| Field | Description | Default |
|-------|-------------|--------|
| `working_dir` | Base directory for task execution | `"/tmp/hotelier"` |
| `log_level` | Logging level (`debug`, `info`, `warn`, `error`) | `"info"` |

## Task Submission

Via REST API:

```bash
curl -X POST http://localhost:8080/api/tasks \
  -H 'Content-Type: application/json' \
  -d '{
    "repos": ["/path/to/repo"],
    "prompt": "Implement feature X",
    "tags": ["business-default"]
  }'
```

Or through the web UI dashboard.

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/tasks` | List all tasks |
| `POST` | `/api/tasks` | Submit a new task |
| `GET` | `/api/tasks/:id` | Get task details |
| `GET` | `/api/agents` | List connected agents |
| `GET` | `/api/agents/:id` | Get agent details |
| `GET` | `/api/health` | Health check |
| `WS` | `/ws` | WebSocket (JSON-RPC) |

## Tags

Agents declare capability tags at registration. Tasks specify required tags for routing:

- `business-default` — Standard development (default)
- `android` — Android-specific tasks
- Custom tags — Any capability your agent supports

## License

MIT
