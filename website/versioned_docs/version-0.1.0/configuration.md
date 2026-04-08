---
sidebar_position: 1
sidebar_label: YAML reference
description: Complete reference for config.yaml — frontends, cluster groups, routing rules, concurrency limits, and observability settings.
---

# Configuration

Copy `config.example.yaml` in the repository root and adjust for your environment.

```yaml
queryflux:
  externalAddress: http://localhost:8080
  frontends:
    trinoHttp:
      enabled: true
      port: 8080
    flightSql:
      enabled: false
      port: 50051
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
    flightSql: trino-default

  - type: header
    headerName: x-target-engine
    headerValueToGroup:
      duckdb: duckdb-local

routingFallback: trino-default
```

`config.example.yaml`, `config.local.yaml`, and the serde types in `queryflux-core` (`config.rs`) are the authoritative reference. For routing semantics and `clusterGroups`, see **[Routing and clusters](/docs/architecture/routing-and-clusters)**.
