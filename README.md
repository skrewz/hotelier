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
./bin/agent       # Agent (uses pi AI agent by default)
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
task_mode: "pi"
```

| Field | Description | Default |
|-------|-------------|--------|
| `task_mode` | Execution mode: `"pi"` (AI agent) or `"shell"` (raw bash) | `"pi"` |
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

## Execution Modes

Agents support two task execution modes, configured via `task_mode` in the agent config:

### `pi` (default)
Tasks are executed by the [pi](https://pi.dev) AI agent via its RPC interface. The agent streams output line-by-line back to the host as the AI works through the prompt. This is the recommended mode for development tasks.

### `shell`
Tasks are executed as raw bash commands in the target repository. This is useful for simple scripting tasks or when AI execution is not desired.

## Deployment

### Docker

```bash
docker build -t hotelier/server .
docker build -t hotelier/agent -f Dockerfile.agent .

docker run -d -p 8080:8080 -v $(pwd)/config/server.yaml:/etc/hotelier.yaml hotelier/server
docker run -v $(pwd)/config/agent.yaml:/etc/agent.yaml hotelier/agent
```

### systemd

```ini
# /etc/systemd/system/hotelier.service
[Unit]
Description=Hotelier Check-In Host
After=network.target

[Service]
ExecStart=/usr/local/bin/hotelier --config /etc/hotelier/config/server.yaml
Restart=always
User=hotelier

[Install]
WantedBy=multi-user.target
```

### Kubernetes

Agents run as pods with `config/agent.yaml` as a ConfigMap. Server runs as a Deployment with Ingress for the web UI.

## Tags

Agents declare capability tags at registration. Tasks specify required tags for routing:

- `business-default` — Standard development (default)
- `android` — Android-specific tasks
- Custom tags — Any capability your agent supports

## License

MIT
