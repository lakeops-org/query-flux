# <p align="center"><big>QueryFlux</big></p>

<p align="center">
  <img src="website/static/img/queryflux-hero-cover.png" alt="QueryFlux — multi-engine SQL query proxy and router in Rust" width="720" />
</p>

# <p align="center"><big>Universal SQL multi-engine query router and proxy in Rust</big></p>

QueryFlux sits between SQL clients and multiple backend query engines, providing protocol translation, intelligent routing, load balancing, and automatic SQL dialect conversion.

## Overview

QueryFlux lets you connect any SQL client using standard protocols (Trino HTTP, PostgreSQL wire, MySQL wire) and route queries to the right backend engine — Trino, DuckDB, StarRocks, Athena, or ClickHouse — based on flexible routing rules. SQL dialects are translated automatically when needed via [sqlglot](https://github.com/tobymao/sqlglot).

```
Client (Trino CLI / psql / mysql)
    ↓ native protocol
QueryFlux
    ↓ routing + dialect translation
Trino / DuckDB / StarRocks / ClickHouse
```

## Features

**Frontend Protocols**
- Trino HTTP (port 8080)
- PostgreSQL wire (port 5432)
- MySQL wire (port 3306)
- Arrow Flight SQL (query execution)

**Backend Engines**
- Trino — async HTTP polling
- DuckDB — embedded, in-process execution
- StarRocks — MySQL wire protocol
- Athena — AWS SDK, async polling
- ClickHouse — planned

**Routing**
- Protocol-based (route by client connection type)
- Header-based (HTTP header values)
- Query regex matching
- Client tags (Trino `X-Trino-Client-Tags`)
- Python script (custom routing logic)
- Compound (multiple conditions with AND/OR)
- Fallback group

**Other**
- SQL dialect translation via sqlglot (31+ dialects)
- Query queuing with per-cluster capacity limits
- In-memory (single-instance) or PostgreSQL-backed state
- Prometheus metrics + Grafana dashboards
- Admin REST API with OpenAPI spec + Basic auth
- QueryFlux Studio — web management UI (cluster monitoring, query history, config management)

## QueryFlux Studio

Studio is the web management UI, served on port `3000`. It connects to the Admin REST API on port `9000`.

**Default login:** username `admin`, password `admin`.

> **Security:** Change the default password immediately after first login. Go to **Security → Change password** in Studio. The new password is stored as a bcrypt hash in Postgres and the default credentials are no longer used.

You can also set bootstrap credentials via YAML or environment variables:

```yaml
queryflux:
  adminApi:
    port: 9000
    username: admin       # override with QUERYFLUX_ADMIN_USER
    password: admin       # override with QUERYFLUX_ADMIN_PASSWORD
```

Once the password has been changed through the UI, YAML/env credentials are ignored and the database record takes precedence. See the [Studio docs](website/docs/studio.md) for the full reference.

## Benchmark (proxy overhead)

End-to-end overhead is measured by [`queryflux-bench`](crates/queryflux-bench) (`cargo run --bin queryflux-bench` after `cargo build --bin queryflux`). It uses **mock** backends (Trino HTTP + MySQL wire for StarRocks), **50** warmup queries per path, then **500** timed iterations of `SELECT 1` — direct to the mock vs the same request through QueryFlux (Trino HTTP frontend). Numbers vary by machine; CI tracks trends via [`.github/workflows/benchmark.yml`](.github/workflows/benchmark.yml).

### Trino (mock HTTP coordinator)

| | p50 | p95 | p99 |
| --- | ---: | ---: | ---: |
| Direct | 0.21 ms | 0.29 ms | 0.35 ms |
| Via QueryFlux | 0.57 ms | 0.81 ms | 1.21 ms |
| **Overhead** | **0.36 ms** | **0.52 ms** | **0.86 ms** |

### StarRocks path (mock MySQL FE)

| | p50 | p95 | p99 |
| --- | ---: | ---: | ---: |
| Direct (MySQL `SELECT 1`) | 0.36 ms | 0.54 ms | 1.20 ms |
| Via QueryFlux | 0.70 ms | 1.21 ms | 4.20 ms |
| **Overhead** | **0.34 ms** | **0.67 ms** | **3.01 ms** |

## Getting Started

### Prerequisites

- Rust (stable)
- Python 3.10+ (for sqlglot SQL translation)
- Docker + Docker Compose (for local development stack)

### Setup

```bash
# Install Python dependencies (sqlglot)
make setup

# Start services (Trino, Postgres, Prometheus, Grafana) and run QueryFlux
make dev
```

This starts:
| Service | URL |
|---|---|
| QueryFlux (Trino HTTP) | http://localhost:8080 |
| Admin / Metrics | http://localhost:9000/metrics |
| Trino (direct) | http://localhost:8081 |
| PostgreSQL | localhost:5433 |
| Prometheus | http://localhost:9090 |
| Grafana | http://localhost:3000 (admin/admin) |

### Test it

```bash
curl -X POST http://localhost:8080/v1/statement \
  -H "X-Trino-User: dev" \
  -d "SELECT 42"
```

### Build

```bash
make build
# or
cargo build --release
./target/release/queryflux --config config.yaml
```

## Configuration

Copy `config.example.yaml` and adjust for your environment:

```yaml
queryflux:
  externalAddress: http://localhost:8080
  frontends:
    trinoHttp:
      enabled: true
      port: 8080
  persistence:
    type: inMemory  # or: postgres

clusterGroups:
  trino-default:
    engine: trino
    maxRunningQueries: 100
    clusters:
      - name: trino-1
        endpoint: http://trino-host:8080
        auth:
          type: basic
          username: user
          password: pass

  duckdb-local:
    engine: duckDb
    maxRunningQueries: 4
    clusters:
      - name: duckdb-1
        databasePath: /tmp/queryflux.duckdb

routers:
  - type: protocolBased
    trinoHttp: trino-default

  - type: header
    headerName: x-target-engine
    headerValueToGroup:
      duckdb: duckdb-local

routingFallback: trino-default
```

See `config.example.yaml` for the full reference including TLS, auth, query queuing, SQL translation, and Python script routing.

## Project Structure

```
queryflux/
├── crates/
│   ├── queryflux/                  # Main binary
│   ├── queryflux-core/             # Shared types and traits
│   ├── queryflux-config/           # Config loading
│   ├── queryflux-frontend/         # Protocol frontends (Trino HTTP, PG wire, ...)
│   ├── queryflux-engine-adapters/  # Backend engine adapters
│   ├── queryflux-cluster-manager/  # Load balancing and queueing
│   ├── queryflux-routing/          # Router implementations
│   ├── queryflux-persistence/      # State storage (in-memory / PostgreSQL)
│   ├── queryflux-translation/      # SQL dialect translation (sqlglot via PyO3)
│   ├── queryflux-metrics/          # Prometheus metrics
│   ├── queryflux-auth/             # Authentication and authorization
│   ├── queryflux-bench/            # Proxy overhead benchmarks
│   └── queryflux-e2e-tests/        # Integration tests
├── queryflux-studio/               # Management UI (Next.js — Studio)
├── examples/                       # Docker Compose quickstarts (see examples/README.md)
├── grafana/                        # Grafana dashboards
├── prometheus/                     # Prometheus config
├── config.example.yaml
├── docker/
│   ├── docker-compose.yml          # Local dev stack (`make dev`)
│   ├── fixtures/                   # SQL init, test data (shared with examples)
│   ├── test/                       # E2E stack: docker-compose.test.yml, fakesnow helpers
│   ├── queryflux/                  # QueryFlux Dockerfile
│   └── queryflux-studio/           # Studio Dockerfile
├── docs/                           # Architecture markdown
├── website/                        # Docusaurus documentation site
```

## Development

```bash
make dev      # Start all services and run QueryFlux
make stop     # Stop services
make logs     # View logs
make check    # Run tests and linting
make clean    # Remove build artifacts and Docker volumes
```

See [development.md](development.md) for environment variables, workspace layout, and how to run the binary locally. See [contribute.md](contribute.md) for pull request expectations.

## Architecture

See [docs/README.md](docs/README.md) for the full architecture doc set (motivation, query translation, routing and clusters). The high-level overview lives in [docs/architecture.md](docs/architecture.md).

**Docs website:** a Docusaurus mirror of this README and `docs/` lives under [`website/`](website/README.md); run `npm install` and `npm start` there for a local browseable site.

## License

Apache License 2.0 — see [LICENSE](LICENSE).
