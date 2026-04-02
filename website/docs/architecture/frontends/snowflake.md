---
description: Snowflake-compatible frontend — HTTP wire protocol and SQL API v2, session management, query execution, and client connectivity.
---

# Snowflake

QueryFlux can accept connections from **Snowflake clients** — JDBC, ODBC, Python connector, Go driver, Node.js driver, and the SQL API — without any driver changes on the client side. Traffic is **not** proxied to a real Snowflake account; QueryFlux terminates the Snowflake protocol locally, authenticates via its own auth providers, routes through the standard router chain, and executes queries on whichever backend engine routing selects (Trino, StarRocks, DuckDB, etc.).

This is a **protocol bridge**, not a Snowflake proxy: the client believes it is talking to Snowflake, but the query runs on a different engine entirely.

## Configuration

```yaml
queryflux:
  frontends:
    snowflakeHttp:
      enabled: true
      port: 8445
```

Config key: `snowflakeHttp`. Protocol identifiers: `FrontendProtocol::SnowflakeHttp` (wire) and `FrontendProtocol::SnowflakeSqlApi` (SQL API). Default dialect: `SqlDialect::Generic` (both).

The `snowflakeHttp` frontend starts a single HTTP listener that serves **both** wire and SQL API routes. There is no separate port for `snowflakeSqlApi` — both protocol surfaces are merged onto the same listener.

## Two protocol flavors

The Snowflake frontend exposes two API surfaces on a **single port**:

| Protocol | Identifier | Typical clients | Auth model |
|----------|------------|-----------------|------------|
| **Snowflake HTTP wire** | `snowflakeHttp` | JDBC, ODBC, Python connector, Go driver, Node.js driver | Session-based (login → token) |
| **Snowflake SQL API v2** | `snowflakeSqlApi` | REST clients, service accounts, `curl` | Stateless Bearer token per request |

Both share the same listener; routing can target them independently via `protocolBased` rules:

```yaml
routers:
  - type: protocolBased
    snowflakeHttp: trino-default
    snowflakeSqlApi: analytics-group
```

---

## HTTP wire protocol

The wire protocol handles session lifecycle (login, logout, heartbeat, token refresh) and synchronous query execution. This is the API surface that JDBC, ODBC, and the Python/Go/Node connectors use internally.

### Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/session/v1/login-request` | Authenticate and create a session |
| `DELETE` | `/session` | Destroy a session (logout) |
| `GET` | `/session/heartbeat` | Validate that a session token is still alive |
| `POST` | `/session/token-request` | Refresh / validate a session token |
| `POST` | `/queries/v1/query-request` | Execute a SQL query |
| `GET` | `/queries/v1/query-monitoring-request` | Query monitoring (returns empty — sync execution) |
| `DELETE` | `/queries/v1/{query_id}` | Cancel a query (no-op — sync execution) |

### Login

`POST /session/v1/login-request` accepts JSON with `LOGIN_NAME` and `PASSWORD` fields. Optional `databaseName` and `schemaName` can be passed as query parameters or in `SESSION_PARAMETERS`.

On success, QueryFlux:

1. Authenticates via the configured `auth_provider`.
2. Resolves a **cluster group** by routing with `FrontendProtocol::SnowflakeHttp`.
3. Issues a QueryFlux-generated UUID as both `token` and `masterToken`.
4. Stores the session (auth context, group, database/schema hints, user) in an in-memory session store.

The returned token is used in subsequent requests via the `Authorization: Snowflake Token="<token>"` header.

### Query execution

`POST /queries/v1/query-request` reads the SQL from the `sqlText` JSON field. Gzip-compressed request bodies (`Content-Encoding: gzip`) are supported — the Python connector uses this by default.

Execution is **synchronous**: the query runs to completion via `execute_to_sink`, and the full result set is returned in a single response. The response includes:

- **Snowflake-compatible JSON** with `rowType` metadata (column names, types, nullability).
- **Base64-encoded Arrow IPC** in `rowsetBase64` for the Python connector's Arrow result path.
- Standard Snowflake response fields (`success`, `code`, `message`, `data`).

Errors are returned as **HTTP 200** with `"success": false` in the JSON body — matching Snowflake's convention so connectors handle errors correctly rather than treating non-200 responses as retryable infrastructure failures.

---

## SQL API v2

The SQL API follows Snowflake's [REST SQL API](https://docs.snowflake.com/en/developer-guide/sql-api) convention. This is the stateless, request-per-query interface typically used by service accounts and automation.

### Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/v2/statements` | Submit a SQL statement |
| `GET` | `/api/v2/statements/{handle}` | Get statement status/results |
| `DELETE` | `/api/v2/statements/{handle}` | Cancel a statement |

### Authentication

SQL API requests use `Authorization: Bearer <token>`. The token is validated against QueryFlux's `auth_provider` on every request — there is no session to maintain.

### Submit statement

`POST /api/v2/statements` accepts JSON with a `statement` field containing the SQL. QueryFlux authenticates the request, routes with `FrontendProtocol::SnowflakeSqlApi`, and executes synchronously.

The response is a Snowflake SQL API v2-compatible JSON object with:

- `statementHandle` — a unique identifier for the execution.
- `status` — `"00000"` on success.
- `rowType` — column metadata.
- `data` — rows as arrays of JSON string values.

### Get / cancel statement

Since execution is synchronous, `GET /api/v2/statements/{handle}` returns a **404** (no stored handles) and `DELETE /api/v2/statements/{handle}` returns a success stub.

---

## Session management

The HTTP wire protocol maintains sessions in an in-memory `SnowflakeSessionStore` (`DashMap`). Each session holds:

| Field | Description |
|-------|-------------|
| `token` | QueryFlux-issued UUID (the session key) |
| `auth_ctx` | Authentication context from the auth provider |
| `group` | Cluster group resolved at login time |
| `user` | Authenticated username |
| `database` / `schema` | Optional hints from the login request |
| `created_at` | Session creation timestamp |

Wire protocol requests resolve the session from the `Snowflake Token=` header. The cluster group is fixed at login — all queries in a session route to the same group.

The SQL API does **not** use the session store; each request is independently authenticated and routed.

---

## Connecting clients

### Python connector

```python
import snowflake.connector

conn = snowflake.connector.connect(
    user="dev",
    password="password",
    account="queryflux",       # any value — not used for routing
    host="localhost",
    port=8445,
    protocol="http",
    database="my_catalog",
    schema="my_schema",
)

cur = conn.cursor()
cur.execute("SELECT 1 AS num, 'hello' AS greeting")
for row in cur:
    print(row)
```

The Python connector sends requests to the wire protocol endpoints. Set `host` and `port` to point at the QueryFlux Snowflake listener. The `account` value is required by the driver but not used by QueryFlux for routing.

### JDBC

```
jdbc:snowflake://localhost:8445/?account=queryflux&ssl=off&db=my_catalog&schema=my_schema
```

### SQL API via curl

```bash
curl -X POST http://localhost:8445/api/v2/statements \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{"statement": "SELECT 42 AS answer"}'
```

---

## Dialect and translation

Both Snowflake protocol flavors map to `SqlDialect::Generic` as their default source dialect. When routing lands on a Trino cluster, for example, sqlglot translates the SQL from the generic dialect to Trino. If the target engine's dialect matches, translation is skipped.

## Related

- [Frontends overview](overview.md) — shared dispatch and session model
- [Configuration](/docs/configuration) — `frontends.snowflakeHttp` in `config.yaml`
- [Routing and clusters](/docs/architecture/routing-and-clusters) — `protocolBased` router with `snowflakeHttp` / `snowflakeSqlApi`
