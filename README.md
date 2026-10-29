# WorkshopBE

Coursework and a capstone project for a Go backend workshop. The project is **wikistream**, a service that consumes the Wikimedia recent-changes event stream, aggregates stats, and serves them over an authenticated HTTP API. It's built up across four versions, each one layering on the next set of modules.

## Repository layout

| Path | Contents |
|---|---|
| `go_developers_guide/` | Per-section exercises from the course (`section2` – `section7`) |
| `project/v1/` | Project version 1 |
| `project/v2/` | Project version 2 |
| `project/v3/` | Project version 3 |
| `project/v4/` | Project version 4 *(in progress)* |

Each version lives in its own directory with the application code under `cmd/`.

## Project versions

| Version | Modules | Focus | Status |
|---|---|---|---|
| **v1** | 1–2 | Initial implementation and first run at dockerization | Done |
| **v2** | 3–4 | Scylla DB, Docker Compose, CI/CD via GitHub Actions | Done |
| **v3** | 5–7 | RedPanda, Protobuf support, observability | Done |
| **v4** | 8–10 | Multi-threading, Kubernetes conversion, RedPanda Connect for the final presentation | In-Progress |

### v1: Core service

- Consumes the Wikimedia SSE stream with reconnect and exponential backoff
- Tracks processed events, unique users, bot vs. human counts, and per-wiki totals in memory
- Containerized with a Dockerfile

### v2: Persistence and automation

- Scylla as a persistent store for per-minute stats, alongside the in-memory store (`STORAGE_MODE=memory|scylla`)
- JWT-style bearer auth: `POST /login` issues a token, `GET /stats` requires it
- `docker-compose` setup for the app and Scylla
- GitHub Actions for CI/CD

### v3: Streaming and observability

- Producer / consumer split with RedPanda as the event backbone (6-partition topic, zstd, 7d retention)
- Protobuf `RecentChange` message, generated with `buf` and registered in the Schema Registry (BACKWARD compatibility)
- Consumer aggregates batches into Scylla with manual offset commits
- Prometheus metrics from both services, scraped into a provisioned Grafana dashboard
- Redpanda Console for inspecting topics and schemas

### v4: Scale-out and final project *(in progress)*

| Module | Topic |
|---|---|
| 8 | Multi-threaded event processing |
| 9 | Kubernetes manifests |
| 10 | RedPanda Connect for the final presentation |

## Running v3

From `project/v3`:

```sh
docker compose up --build
```

| Service | Port | Description |
|---|---|---|
| Consumer API | `7000` | `/status`, `/login`, `/stats`, `/metrics` |
| Redpanda Console | `8080` | Topics and schema registry UI |
| Prometheus | `9090` | Metrics and scrape targets (`/targets`) |
| Grafana | `3000` | `Wikistream` folder, pipeline dashboard (admin password from `GRAFANA_ADMIN_PASSWORD`) |

## Running v2

From `project/v2/cmd`:

```sh
docker compose up --build
```

| Endpoint | Auth | Description |
|---|---|---|
| `GET /status` | none | Liveness check |
| `POST /login` | none | Exchange `username` / `password` JSON for a bearer token |
| `GET /stats` | bearer token | Current aggregated stats |

The app listens on `:7000`. Configuration comes from `.env` (see `config.go` for the full list and defaults).

Scylla takes a little while to become healthy on first boot, and the app waits on its healthcheck. To check on it directly:

```sh
docker exec wikistream-scylla nodetool status   # look for "UN"
```
