---
sidebar_label: Overview
---

# QueryFlux architecture documentation

This section describes how QueryFlux is put together: why it exists, how SQL is translated, and how traffic is routed to **cluster groups** and individual **clusters**.

| Document | What it covers |
|----------|----------------|
| [Motivation and goals](motivation-and-goals.md) | Problem statement, goals, and how QueryFlux fits multi-engine estates. |
| [System map](system-map.md) | End-to-end query lifecycle, major crates, and component status (high level). |
| [Query translation](query-translation.md) | Dialect detection, sqlglot integration, when translation runs, and schema-aware mode. |
| [Routing and clusters](routing-and-clusters.md) | Router chain, `routingFallback`, cluster groups, load-balancing strategies, and queueing. |
| [Observability](observability.md) | Prometheus metrics, Grafana dashboard, QueryFlux Studio, and the Admin REST API. |
| [Adding engine support](adding-engine-support.md) | Checklist for new **backend engines** (Rust adapters), **Studio** (`lib/studio-engines/` manifest + catalog slots), and **frontend wire protocols** (e.g. Postgres wire). |
| [Auth / authz design](auth-authz-design.md) | Authentication and authorization design notes. |

Start with [Motivation and goals](motivation-and-goals.md) if you are new to the project; use [System map](system-map.md) as the single-page map of the system.

The canonical Markdown sources live under [`docs/`](https://github.com/lakeops-org/query-flux/tree/main/docs) in the repository.
