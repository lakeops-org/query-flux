---
sidebar_position: 1
---

# QueryFlux

A universal SQL query proxy and router written in **Rust**. QueryFlux sits between SQL clients and multiple backend query engines, providing protocol translation, intelligent routing, load balancing, and automatic SQL dialect conversion (via [sqlglot](https://github.com/tobymao/sqlglot)).

```
Client (Trino CLI / psql / mysql)
    ↓ native protocol
QueryFlux
    ↓ routing + dialect translation
Trino / DuckDB / StarRocks / Athena / ClickHouse
```

## Features

**Frontend protocols**

- Trino HTTP (port 8080)
- PostgreSQL wire (port 5432)
- MySQL wire (port 3306)
- Arrow Flight SQL (query execution)

**Backend engines**

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
- Authentication (static, OIDC, LDAP) + authorization (allow-all, policy, OpenFGA)
- Prometheus metrics + Grafana dashboards
- Admin REST API with OpenAPI spec
- QueryFlux Studio — web management UI (cluster monitoring, query history, config management)

## Next steps

- **[Getting started](/docs/getting-started)** — Docker Compose **examples** (`minimal`, full stack, …), `curl`, and `make dev` for contributors
- **[Architecture](/docs/architecture/overview)** — motivation, system map, translation, routing, observability
- **[Development](/docs/development)** — workspace layout, `make check`, E2E tests

The same content lives in the repository **[README](https://github.com/lakeops-org/query-flux/blob/main/README.md)** and **`docs/`** tree; this site mirrors it for easier reading.
