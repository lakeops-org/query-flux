---
sidebar_position: 1
description:
  Universal SQL proxy and router in Rust — Trino, PostgreSQL, MySQL, and Flight on the wire;
  multi-engine backends, routing, metrics, and sqlglot dialect translation.
keywords:
  - QueryFlux
  - SQL proxy
  - Trino
  - Rust
  - query router
---

# QueryFlux

## What is QueryFlux?

Modern data stacks run multiple query engines — Trino for federated queries, DuckDB for embedded analytics, StarRocks for low-latency dashboards, Athena for pay-per-scan cold data. Open table formats like Apache Iceberg solved the storage problem: one copy of data in object storage, readable by any engine. But the **compute access problem** remained unsolved.

Every engine speaks its own wire protocol and its own SQL dialect. Every client — BI tool, notebook, application, CLI — needs its own driver, its own connection string, and its own retry logic. Multiply N clients by M engines and you get a wiring problem that grows combinatorially, with routing decisions, dialect differences, and capacity management reinvented in isolation at every edge.

**QueryFlux is the compute interoperability layer above the table format.** It is a universal SQL proxy and router, written in Rust, that sits between your clients and your engine fleet. Clients connect once, over a protocol they already know. QueryFlux routes each query to the right backend, translates dialects in flight, enforces concurrency limits, and exposes a unified observability surface — without any changes to client code.

> Iceberg unified the data. QueryFlux unifies the access.

---

## How does it work?

Every query passes through three stages inside QueryFlux:

### 1. Protocol ingestion

QueryFlux listens on multiple frontend ports simultaneously, each speaking a native client protocol:

| Frontend | Port | Clients |
|----------|------|---------|
| Trino HTTP | 8080 | Trino CLI, JDBC, `trino-python-client` |
| PostgreSQL wire | 5432 | `psql`, any Postgres-compatible driver |
| MySQL wire | 3306 | `mysql`, MySQL JDBC, most BI tools |
| Arrow Flight SQL | gRPC | Flight-native clients |

No driver changes, no custom SDK. Clients point at QueryFlux instead of a backend engine and everything works transparently.

### 2. Routing

Incoming queries are evaluated against an ordered chain of routing rules. The first rule that matches determines which cluster group handles the query. Rules can match on:

- **Protocol** — route all PostgreSQL wire clients to a low-latency StarRocks group
- **HTTP headers** — route by custom header values from the client
- **SQL text** — regex patterns on the query itself (`SELECT.*LIMIT 1`, `INSERT INTO`, …)
- **Client tags** — Trino's `X-Trino-Client-Tags` for caller-declared intent (`priority:interactive`, `workload:etl`)
- **Python script** — arbitrary routing logic: time of day, user identity, current cluster load, query cost estimate
- **Compound** — combine any of the above with AND/OR
- **Fallback** — catch-all for queries that match nothing

### 3. Dispatch and dialect translation

Once routed, QueryFlux selects a healthy cluster from the target group (using `roundRobin`, `leastLoaded`, `failover`, or `weighted` strategy), optionally **rewrites the SQL to the target engine's dialect** via [sqlglot](https://github.com/tobymao/sqlglot) (31+ dialects), and dispatches the query. If the group is at its concurrency limit, the query queues at the proxy — the client sees normal protocol behavior, not an error.

```
Client (psql / Trino CLI / mysql / BI tool)
    │  native protocol
    ▼
┌─────────────────────────────────────────────┐
│                 QueryFlux                   │
│                                             │
│  Frontend ──► Router ──► Dialect xlation    │
│                    │                        │
│              Cluster group                  │
│         (concurrency limit + queue)         │
└──────────────────┬──────────────────────────┘
                   │
      ┌────────────┼────────────┐
      ▼            ▼            ▼
   Trino       StarRocks      Athena  …
      └────────────┴────────────┘
          Apache Iceberg / Delta / Hudi
```

---

## Benefits

### Cut query costs by routing to the right engine

Cloud engines charge in fundamentally different ways. Compute-priced backends (Trino, StarRocks) charge for cluster uptime or CPU-seconds. Scan-priced backends (Athena, BigQuery) charge for bytes read. Without a routing layer, every query goes to the same engine regardless of its shape — CPU-heavy joins land on Athena, cold selective filters land on StarRocks, and you pay the wrong model each time.

In our own benchmarking, workload-aware routing — steering CPU-heavy work to compute-priced engines and selective cold-data queries to scan-priced ones — **reduced total workload cost by up to 56%**, with individual queries sometimes dropping by **up to 90%** compared with always using a single default.

### Enforce latency SLAs without touching clients

A batch ETL job competing with an interactive dashboard on the same Trino cluster degrades both. QueryFlux lets you encode performance intent in routing rules and apply it to all clients uniformly — no application changes, no conventions that drift:

- Route all PostgreSQL wire connections (typically interactive tooling) to a low-latency StarRocks pool
- Route queries tagged `workload:etl` to the Trino cluster reserved for batch
- Route queries matching `SELECT.*LIMIT \d+` to DuckDB for sub-10 ms response

### Absorb burst pressure with proxy-side queuing

When a cluster is saturated, the default behavior is engine-specific and invisible across engines. QueryFlux adds a controlled throttle per cluster group: queries queue at the proxy rather than hammering the backend, overflow spills to a secondary group via fallback routing, and queue depth is a first-class Prometheus metric. One pane of glass across all engines instead of fragmented per-engine UIs.

### Eliminate the N×M integration problem

One endpoint replaces N×M driver configurations. Clients connect to QueryFlux once; the backend topology — which engines exist, how they are grouped, how load is balanced — is config, not code. Add an engine, change a routing rule, swap a backend: no client changes, no deploys, no coordination.

### ~0.35 ms proxy overhead

QueryFlux is written in Rust. The measured p50 proxy overhead (routing + dialect translation, from the `queryflux-bench` suite) is approximately **0.35 ms**. For the typical analytical workload, the proxy is not on the critical path.

---

## Example use cases

### Multi-engine data platform

Your platform team runs Trino for federated queries, StarRocks for dashboard serving, and Athena for ad-hoc cold-data exploration. Today each team has its own connection string, its own driver, and its own capacity management story.

With QueryFlux: one endpoint, one set of routing rules. BI tools connect via MySQL wire and land on StarRocks. Scheduled Trino jobs stay on the Trino cluster. Ad-hoc `SELECT *` exploration queries are detected by regex and routed to Athena. Each engine gets the traffic it is designed for — without any team changing their tooling.

### Cost-aware workload dispatch

A data engineering team runs mixed workloads: heavy joins and window functions (CPU-bound) alongside selective filter queries on cold Iceberg tables (IO-bound). Every query today lands on the same Trino cluster.

With QueryFlux: a Python script router inspects the query, estimates whether it is CPU- or IO-skewed, and routes accordingly — CPU-bound to compute-priced Trino, IO-bound to scan-priced Athena. The cost model is encoded once in the routing script; every client benefits automatically.

### Dashboard SLA protection

A StarRocks cluster serves interactive dashboards with a 200 ms SLA. Ad-hoc analyst queries share the same cluster and occasionally saturate it, causing dashboard refreshes to miss SLA.

With QueryFlux: the StarRocks group gets a `maxRunningQueries` cap. When the group is full, ad-hoc queries queue at the proxy or spill to a Trino fallback group. Dashboards always find capacity; analysts get transparent queueing. The Grafana dashboard shows queue depth in real time.

### Transparent engine migration

A team wants to migrate their BI workload from Trino to StarRocks incrementally — routing 10% of traffic to StarRocks, comparing latency, then gradually increasing the split.

With QueryFlux: a `weighted` load balancing strategy on a cluster group with both engines handles the split. No client changes. The query history in QueryFlux Studio shows per-engine latency side by side. When the team is satisfied, they update the weights to 100% StarRocks. Zero flag day.

---

## Next steps

- **[Getting started](/docs/getting-started)** — Docker Compose examples (`minimal`, full stack), `curl`, and `make dev` for contributors
- **[Architecture](/docs/architecture/overview)** — system map, routing mechanics, dialect translation, observability
- **[Motivation and goals](/docs/architecture/motivation-and-goals)** — the full analysis: fragmented engine landscape, cost modeling, and the case for a proxy
- **[Configuration reference](/docs/configuration)** — complete YAML reference for clusters, routers, auth, and persistence
- **[Roadmap](/docs/roadmap)** — what is shipped, what is next (ClickHouse, cost-aware routing, Snowflake, BigQuery)
