---
sidebar_label: Overview
description: How QueryFlux frontends work — protocol listeners, shared dispatch, session context, and the available client protocols.
---

# Frontends

A **frontend** is the entry point for client traffic into QueryFlux. Each frontend speaks a specific wire or HTTP protocol, parses incoming SQL, builds a `SessionContext`, and hands the query to the shared dispatch layer. The client never knows which backend engine actually runs the query — the frontend translates results back into its native format before responding.

## Available frontends

| Frontend | Config key | Default port | Protocol | Dialect | Status |
|----------|------------|-------------|----------|---------|--------|
| [Trino HTTP](trino-http.md) | `trinoHttp` | 8080 | HTTP REST (JSON) | Trino | **Done** |
| [PostgreSQL wire](postgres-wire.md) | `postgresWire` | 5432 | PostgreSQL v3 wire | Postgres | **Done** |
| [MySQL wire](mysql-wire.md) | `mysqlWire` | 3306 | MySQL wire | MySQL | **Done** |
| [Arrow Flight SQL](flight-sql.md) | `flightSql` | 50051 | gRPC (Arrow Flight) | Generic | **Done** |
| ClickHouse HTTP | `clickhouseHttp` | 8123 | HTTP | ClickHouse | Planned |

## Shared architecture

All frontends converge on the same internal pipeline. The differences are only in how SQL enters and how results leave.

```
Client  ──(native protocol)──►  Frontend Listener
                                      │
                                SessionContext + SQL
                                      │
                                      ▼
                                 Router Chain  ──► ClusterGroupName
                                      │
                                      ▼
                              ClusterGroupManager  ──► ClusterName
                                      │
                                      ▼
                              Translation Service  ──► translated SQL
                                      │
                                      ▼
                                Engine Adapter  ──► results
                                      │
                                      ▼
                                 ResultSink  ──► native protocol response
```

### Dispatch paths

The dispatch layer offers two execution models:

| Path | Used by | Behavior |
|------|---------|----------|
| **`dispatch_query`** | Trino HTTP (async-capable groups) | Submit to engine, persist handle, return polling URL. Client follows `nextUri` to stream pages. |
| **`execute_to_sink`** | All other frontends + Trino HTTP sync fallback | Wait for cluster capacity (backoff), execute query to completion, stream Arrow batches through a `ResultSink` that encodes the native protocol response. |

### SessionContext

Each frontend builds a protocol-specific `SessionContext` that travels with the query through routing and into dispatch:

| Variant | Fields |
|---------|--------|
| `TrinoHttp` | `headers` (lowercased HTTP headers) |
| `PostgresWire` | `user`, `database`, `session_params` |
| `MySqlWire` | `user`, `schema`, `session_vars` |
| `ClickHouseHttp` | `headers`, `query_params` |

Flight SQL uses internal session metadata compatible with dispatch; see the [Flight SQL frontend](flight-sql.md).

### Authentication

All frontends extract credentials from their protocol's native mechanism (HTTP headers, wire auth packets, gRPC metadata) and pass them to the shared `auth_provider`. The auth result determines whether the query proceeds and provides context for authorization checks downstream.

### Routing

The `FrontendProtocol` enum identifies which frontend originated a query. The `protocolBased` router uses this to map traffic from different protocols to different cluster groups:

```yaml
routers:
  - type: protocolBased
    trinoHttp: trino-default
    postgresWire: trino-default
    mysqlWire: starrocks-group
    flightSql: flight-analytics
```

### SQL dialect and translation

Each frontend has a **default source dialect** (`protocol.default_dialect()`). When the target engine's dialect differs, sqlglot translates the SQL automatically. When dialects match, translation is skipped entirely.

## Configuration

Each frontend is enabled under `queryflux.frontends` in `config.yaml`:

```yaml
queryflux:
  frontends:
    trinoHttp:
      enabled: true
      port: 8080
    postgresWire:
      enabled: true
      port: 5432
    mysqlWire:
      enabled: true
      port: 3306
    flightSql:
      enabled: true
      port: 50051
```

Omitting a frontend block or setting `enabled: false` disables that listener entirely.
