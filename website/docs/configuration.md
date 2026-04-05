---
sidebar_position: 2
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
    snowflakeHttp:
      enabled: true
      port: 8445
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
    snowflakeHttp: trino-default
    snowflakeSqlApi: trino-default

  - type: header
    headerName: x-target-engine
    headerValueToGroup:
      duckdb: duckdb-local

routingFallback: trino-default
```

## Admin API

```yaml
queryflux:
  adminApi:
    port: 9000            # Admin REST API + Studio proxy port (default: 9000)
    username: admin       # Bootstrap admin username — see note below (default: admin)
    password: admin       # Bootstrap admin password — see note below (default: admin)
```

`username` and `password` are the **bootstrap** credentials used on first boot. After you change the password through Studio's Security page, the new bcrypt hash is stored in Postgres and the YAML values are ignored.

Environment variables `QUERYFLUX_ADMIN_USER` and `QUERYFLUX_ADMIN_PASSWORD` override the YAML fields and follow the same bootstrap semantics.

See **[Studio & Admin Auth](/docs/studio)** for the full credential priority rules and password-change instructions.

---

`config.example.yaml`, `config.local.yaml`, and the serde types in `queryflux-core` (`config.rs`) are the authoritative reference. For routing semantics and `clusterGroups`, see **[Routing and clusters](/docs/architecture/routing-and-clusters)**.
