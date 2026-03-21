# Routing, cluster groups, and clusters

QueryFlux separates **where a query should go logically** (cluster **group**) from **which physical backend instance** serves it (**cluster** / adapter). This document explains that split, how routers work, and how the cluster manager balances load and enforces limits.

## Vocabulary

| Term | Meaning |
|------|--------|
| **Cluster** | A named backend instance in config (`clusters.<name>`): one engine (Trino, DuckDB, StarRocks, …), endpoint or DB path, auth, etc. At runtime it has an **engine adapter** and a **`ClusterState`** (running count, health, limits). |
| **Cluster group** | A named **pool** (`clusterGroups.<name>`) listing **member** cluster names, a **per-group** `maxRunningQueries`, optional `maxQueuedQueries`, optional **selection strategy**, and `enabled`. Routing returns a **group**; the cluster manager then picks a **member** cluster. |
| **Router** | A rule that inspects the SQL string, session context, and frontend protocol and optionally returns a target group name. |
| **Router chain** | All routers in config order, plus **`routingFallback`** when every router returns “no match.” |

## Two-stage placement

1. **Routing (group selection)**  
   `RouterChain` evaluates each `RouterTrait` implementation in order. The **first** router that returns `Some(ClusterGroupName)` wins. If all return `None`, **`routingFallback`** is used.  
   Implementation: `queryflux_routing::chain::RouterChain` (`route`, `route_with_trace`).

2. **Cluster selection (member selection)**  
   `ClusterGroupManager::acquire_cluster(group)` considers only clusters in that group that are **enabled**, **healthy**, and **under** `max_running_queries`. It then uses the group’s **strategy** to pick one member and increments that cluster’s running count.  
   If **no** member is eligible (e.g. all at capacity or unhealthy), `acquire_cluster` returns **`None`** → the query is **queued** (Trino HTTP async path) or **retried with backoff** (sync `execute_to_sink` path).  
   Implementation: `queryflux_cluster_manager::simple::SimpleClusterGroupManager`.

When the query finishes (success, failure, or cancel), **`release_cluster`** decrements the running count on that cluster.

## Router types (config → code)

Configured under `routers:` in YAML (`queryflux_core::config::RouterConfig`). Wired in `queryflux/src/main.rs`.

| `type` | Behavior |
|--------|----------|
| `protocolBased` | Maps the active frontend (`trinoHttp`, `postgresWire`, `mysqlWire`, `flightSql`, `clickhouseHttp`) to a group name. |
| `header` | Matches a header value to a group (useful for Trino HTTP and similar). |
| `queryRegex` | Ordered rules: first regex match on the SQL text wins. |
| `clientTags` | Trino-style client tags header mapped to groups. |
| `pythonScript` | Embedded or file-backed Python `route(sql, user, protocol)` returning a group name or `None`. |

All five router types are implemented. Unknown `type` values in config are skipped at startup with a warning.

## Routing trace

`route_with_trace` records each router’s decision (`matched`, optional `result`) and whether the **fallback** group was used. This supports debugging and future UI/metrics (see `RoutingTrace` in `queryflux_routing::chain`).

## Cluster group configuration (actual shape)

Clusters are defined **once** at the top level; groups **reference** them by name:

```yaml
clusters:
  trino-1:
    engine: trino
    endpoint: http://trino:8080
    enabled: true
  duckdb-1:
    engine: duckDb
    enabled: true

clusterGroups:
  trino-default:
    enabled: true
    maxRunningQueries: 100
    members: [trino-1]
    strategy:
      type: leastLoaded
  duckdb-local:
    enabled: true
    maxRunningQueries: 4
    members: [duckdb-1]
```

Notes:

- **`maxRunningQueries`** on the group applies to **each** member cluster’s `ClusterState` when those states are built (see `main.rs` pass 2). It is the cap used for **acquire** / capacity checks.
- **`members`** can list multiple clusters, including **mixed engines** (e.g. Trino and DuckDB in one group). For that, **`engineAffinity`** (or another strategy) helps express preference order across engine types (`queryflux_cluster_manager::strategy`).

## Selection strategies

Configured as `strategy: { type: ... }` on a group. Implemented in `strategy.rs`:

| Strategy | Behavior |
|----------|----------|
| `roundRobin` | Rotates among eligible members (default when strategy omitted). |
| `leastLoaded` | Picks the member with the smallest `running_queries`. |
| `failover` | First eligible member in **member list order** (priority ordering in YAML). |
| `engineAffinity` | Ordered engine preference; within each engine, least loaded. |
| `weighted` | Distributes by configured weights (deterministic pseudo-random from load). |

Eligible candidates are always **healthy**, **enabled**, and **not at capacity** before the strategy runs.

## Health and runtime updates

Each `ClusterState` tracks health (`is_healthy`), updated by background health checks in the main binary. Unhealthy clusters are excluded from acquisition.

The `ClusterGroupManager` trait also supports **`update_cluster`** (enable/disable, change `max_running_queries`) for admin-driven changes.

## Frontend dispatch: async vs sync

After a group is chosen, the Trino HTTP handler (`post_statement`) branches:

- If the group is considered **async-capable** (e.g. Trino-style polling), it uses **`dispatch_query`**: acquire cluster → translate → `submit_query` → persist executing state → rewrite `nextUri` to point back at QueryFlux.
- Otherwise it uses **`execute_to_sink`**: wait for capacity (backoff loop), translate, stream Arrow batches, and synthesize a Trino-compatible JSON response.

So **routing and cluster selection** are shared concepts; the **result delivery** shape depends on engine and frontend capabilities.

## Mental model

- **Routers** answer: *which pool (group) should handle this query?*
- **Cluster manager + strategy** answer: *which replica/instance in that pool?*
- **Translation** (separate doc) then aligns SQL with that instance’s engine.

See [architecture.md](architecture.md) for the full component diagram and [query-translation.md](query-translation.md) for dialect conversion details.
