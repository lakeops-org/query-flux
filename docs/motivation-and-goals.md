# Motivation and goals

## Why QueryFlux exists

### This is why QueryFlux

Modern data stacks are **fragmented by design**. Different engines exist because different problems demand different trade-offs — **Trino** for federation across sources, **DuckDB** for lightweight embedded analytics, **StarRocks** or **ClickHouse** for high-throughput serving layers. Using the right engine for the right job is good engineering.

That fragmentation has a cost that compounds quietly. Every engine speaks its own wire protocol, has its own SQL dialect quirks, and needs its own connection management. When you multiply engines by clients — BI tools, notebooks, application code, CLI tools — you do not get a linear integration problem; you get a **combinatorial** one (**N×M**). Each pairing needs its own driver, dialect handling, and retry logic. Operational concerns like routing, rate limiting, and observability get reinvented in isolation, over and over.

**Cost makes the picture worse.** Cloud analytical systems often charge either for **compute time** or for **data scanned**; query shapes skew toward **compute-bound** or **IO-bound** work, and each class tends to run cheaper under a different pricing model. In **our own benchmarking and cost modeling** while building QueryFlux — running the same analytical SQL across engines with different pricing models — we repeatedly saw a **poor match** between query shape and billing model inflate cost by large factors (on the order of **2–5×** versus a better-matched engine in those runs). When we prototyped **workload-aware routing** (steering CPU-skewed work toward compute-priced backends and scan-heavy work toward byte-priced backends where it helped), **total workload cost** in one representative suite fell by up to about **56%**, and **individual queries** sometimes dropped by up to about **90%** compared with always using a single default. That gap is not a rounding error — it is a structural inefficiency in any stack that treats every query the same.

Without a **single proxy layer**, capturing those savings is hard: there is no one place to observe query characteristics, apply routing logic, and dispatch to the right engine. Teams end up locked into one engine or maintaining bespoke glue that cannot reason about cost or capability in one place.

**QueryFlux** was created to cut through that. Instead of solving the N×M integration problem at the edges, it introduces **one proxy** that clients talk to over a familiar protocol. Behind it, QueryFlux handles **routing** to the right engine, **normalizes dialect** differences, and **centralizes** the operational glue that would otherwise scatter across the stack. That same layer is where **workload- and economics-aware routing** can live — matching queries to engines by capability and, over time, by how those engines bill for execution.

The result is a **uniform client experience** — one URL, shared tooling, consistent behavior — without forcing consolidation onto a single engine or leaving money on the table by ignoring how differently engines charge for the same work.

**QueryFlux** is a **universal SQL query proxy and router** written in Rust. It aims to:

1. **Speak the client’s protocol** — Trino HTTP, PostgreSQL wire, MySQL wire, and Arrow Flight SQL are all implemented.
2. **Route each query to the right backend pool** — using configurable rules (protocol, headers, regex on SQL, Trino client tags, Python scripts) instead of hard-coding one engine per deployment.
3. **Translate SQL when dialects differ** — so a Trino-oriented client can still hit DuckDB or StarRocks when routing sends the query there, using [sqlglot](https://github.com/tobymao/sqlglot) behind the scenes.
4. **Operate like a serious proxy** — per-group concurrency limits, queueing when clusters are full, health-aware selection, Prometheus metrics, and optional PostgreSQL-backed state for HA-style deployments.

In short: **one front door**, **many engines**, **explicit routing**, **automatic dialect bridging**, and **shared capacity and observability** — with a clear place to grow toward routing that respects **cost and workload shape**, grounded in what we measured above.

## Compared to Trino-only gateways

Some gateways optimize for **load-balanced Trino** behind one client protocol. QueryFlux targets **heterogeneous** deployments:

- **Multiple engine types** in one deployment (Trino, DuckDB, StarRocks, …).
- **Protocol choices at the edge** — Trino HTTP, PostgreSQL wire, MySQL wire, Arrow Flight SQL — not only Trino HTTP.
- **SQL dialect translation** when the routed engine’s SQL differs from what the client naturally speaks.

## What success looks like for operators

- **Predictable routing**: Rules are data (YAML / DB-backed config), ordered and traceable.
- **Controlled blast radius**: Groups cap concurrent queries; full groups queue instead of overwhelming backends.
- **Observable behavior**: Metrics and admin APIs reflect group/cluster load and routing outcomes.
- **Incremental adoption**: You can start with a single group (e.g. one Trino pool) and add engines and routers as needs grow.

For the mechanics of translation and routing, see [query-translation.md](query-translation.md) and [routing-and-clusters.md](routing-and-clusters.md).
