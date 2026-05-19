# Hotelier

Ephemeral YOLO guest queue — autonomous AI development infrastructure.

***Warning***: this is alpha-grade software (read: made for myself, all bugs included). Use at your own risk.

## Overview

Hotelier orchestrates ephemeral AI guests ("machines") that register, receive tasks, execute them against target repositories, stream logs, and submit results. Guests are configured entirely via YAML config files.

## Screenshots

![Hotelier Web Dashboard](/docs/screenshot.png)

## Architecture

```
┌──────────────┐    WebSocket     ┌───────────────┐
│  Check-In    │◄────────────────►│    Guest      │
│  Host        │  JSON-RPC 2.0    │  (any machine │
│  :8080       │                  │   running the │
│              │                  │   guest CLI)  │
└──────────────┘                  └───────────────┘
  ├── REST API                      ├── guest.yaml
  ├── Web UI                        ├── Register
  ├── Task Queue                    ├── Execute tasks
  └── Guest Registry                ├── Stream logs
                                    └── Submit results
```

## Quick Start

```bash
make build

# Copy example configs and edit to suit
cp config/server.example.yaml config/server.yaml
cp config/guest.example.yaml config/guest.yaml

./bin/hotelier    # Server on :8080
./bin/guest       # Guest (runs tasks via the pi AI agent)
```

## Configuration

Sample configs are provided in `config/`:

| File | Description |
|------|-------------|
| `config/server.example.yaml` | Check-In Host (server) configuration |
| `config/guest.example.yaml` | Guest configuration |

Copy the examples to `config/server.yaml` and `config/guest.yaml`, then edit as needed.

### Server (`config/server.yaml`)

```yaml
host: "0.0.0.0"
port: 8080
read_timeout: 30
write_timeout: 30
max_log_size: 1048576
task_timeout: 3600
heartbeat_interval: 30
max_guests: 0
```

### Guest (`config/guest.yaml`)

```yaml
host: "localhost"
port: 8080
id: "guest-1"
name: "Dev Guest #1"
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
| `GET` | `/api/guests` | List connected guests |
| `GET` | `/api/guests/:id` | Get guest details |
| `GET` | `/api/health` | Health check |
| `WS` | `/ws` | WebSocket (JSON-RPC) |

## Tags

Guests declare capability tags at registration. Tasks specify required tags for routing:

- `business-default` — Standard development (default)
- `android` — Android-specific tasks
- Custom tags — Any capability your guest supports

## Deployment

### Container (Server Only)

Hotelier ships a multi-stage `Containerfile` for building the **server-side** binary as a container image. The guest is intentionally **not** containerized — agentic coding agents expect direct access to a machine's filesystem, shell, and tools (the `pi` agent runs a persistent RPC subprocess that expects a real host environment).

#### Build

```bash
make image
```

This uses `podman` to build the image. The build has two stages:

1. **builder** — a `golang:1.25-bookworm` image that compiles the `hotelier` binary with `-trimpath` and version ldflags.
2. **runtime** — a minimal `debian:12-slim` image with only `ca-certificates` and `git` installed (git is needed for guests to clone repos).

The resulting image runs as a non-root user (`hotelier`) on port 8080.

#### Run

```bash
podman run -d --name hotelier \
  -p 8080:8080 \
  -v /path/to/config:/etc/hotelier:Z \
  hotelier:latest
```

#### Configuration

The server reads its config from `/etc/hotelier/server.yaml` inside the container. Mount a host directory containing your config:

```bash
# On the host
cp config/server.example.yaml /path/to/config/server.yaml
# Edit /path/to/config/server.yaml as needed

# Run the container
podman run -d --name hotelier \
  -p 8080:8080 \
  -v /path/to/config:/etc/hotelier:Z \
  hotelier:latest
```

##### Volume Mounts

| Mount | Purpose |
|-------|--------|
| `/etc/hotelier` | Config files (read-only recommended) — mount a host directory containing `server.yaml` |
| `/var/log/hotelier` | Task log persistence — set `log_dir` in config to enable; mount a host directory |

Example with read-only config:

```bash
podman run -d --name hotelier \
  -p 8080:8080 \
  -v /path/to/config:/etc/hotelier:Z,ro \
  hotelier:latest
```

##### Environment Variables

No environment variables are required. The server is configured entirely through the YAML config file.

#### Task Log Persistence

Set `log_dir` in `server.yaml` to persist task logs to the mounted volume:

```yaml
log_dir: "/var/log/hotelier"
```

Logs are stored in a date-partitioned structure:

```
/var/log/hotelier/
  2026-05-10/
    task-abc123/
      logs.jsonl
    task-def456/
      logs.jsonl
  2026-05-11/
    task-ghi789/
      logs.jsonl
```

Each `logs.jsonl` file contains one JSON object per line (JSONL format), with fields:
`task_id`, `line`, `level`, `timestamp`.

The web UI includes a **Logs** tab for browsing persisted task logs.

Run with log persistence:

```bash
podman run -d --name hotelier \
  -p 8080:8080 \
  -v /path/to/config:/etc/hotelier:Z,ro \
  -v /path/to/logs:/var/log/hotelier:Z \
  hotelier:latest
```

##### Health Check

The container includes a built-in health check that pings the server's own binary every 30 seconds. Check status with:

```bash
podman inspect --format='{{.State.Health.Status}}' hotelier
```

### Guest Deployment

Guests run **natively on machines** — they are not containerized. Each guest machine needs:

1. The `bin/guest` binary (built via `make build`)
2. A `config/guest.yaml` pointing to the Check-In Host
3. Go installed (for the `pi` agent subprocess)
4. `pi` CLI installed (`go install github.com/mariozechner/pi-coding-agent@latest`)
5. `git` installed (for cloning repos)

```bash
# On the guest machine
make build
cp config/guest.example.yaml config/guest.yaml
# Edit config/guest.yaml with your server address
./bin/guest
```

## License

MIT
