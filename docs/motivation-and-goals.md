# Motivation and goals

## Why QueryFlux exists

Organizations rarely run a single SQL engine. You might use **Trino** for federated analytics, **DuckDB** for embedded or edge analytics, **StarRocks** or **ClickHouse** for warehousing or serving layers, and still want a **uniform client experience** (one URL, one familiar protocol, shared tooling).

Without a proxy, every combination of client and engine forces its own integration: different wire protocols, different SQL dialects, different connection management, and duplicated operational glue (routing, capacity limits, observability).

**QueryFlux** is a **universal SQL query proxy and router** written in Rust. It sits between SQL clients and multiple backend engines. It aims to:

1. **Speak the client’s protocol** — Trino HTTP, PostgreSQL wire, MySQL wire, and Arrow Flight SQL are all implemented.
2. **Route each query to the right backend pool** — using configurable rules (protocol, headers, regex on SQL, Trino client tags, Python scripts) instead of hard-coding one engine per deployment.
3. **Translate SQL when dialects differ** — so a Trino-oriented client can still hit DuckDB or StarRocks when routing sends the query there, using [sqlglot](https://github.com/tobymao/sqlglot) behind the scenes.
4. **Operate like a serious proxy** — per-group concurrency limits, queueing when clusters are full, health-aware selection, Prometheus metrics, and optional PostgreSQL-backed state for HA-style deployments.

In short: **one front door**, **many engines**, **explicit routing**, **automatic dialect bridging**, and **shared capacity and observability**.

## Relationship to trino-lb

The project README and high-level design acknowledge inspiration from [trino-lb](https://github.com/stackabletech/trino-lb): load-balanced Trino with routing in front. QueryFlux **generalizes** that idea:

- **Multiple engine types**, not only Trino.
- **Protocol translation** at the edge (Trino HTTP today; other protocols targeted).
- **SQL dialect translation** when the routed engine’s SQL differs from the client’s natural dialect.

If you think of trino-lb as “smart routing for Trino clusters,” QueryFlux is “smart routing and dialect bridging across heterogeneous query engines.”

## What success looks like for operators

- **Predictable routing**: Rules are data (YAML / DB-backed config), ordered and traceable.
- **Controlled blast radius**: Groups cap concurrent queries; full groups queue instead of overwhelming backends.
- **Observable behavior**: Metrics and admin APIs reflect group/cluster load and routing outcomes.
- **Incremental adoption**: You can start with a single group (e.g. one Trino pool) and add engines and routers as needs grow.

For the mechanics of translation and routing, see [query-translation.md](query-translation.md) and [routing-and-clusters.md](routing-and-clusters.md).
