---
description: MySQL wire protocol frontend — handshake, COM_QUERY, session variables, schema switching, and connecting with mysql clients.
---

# MySQL wire

The MySQL wire frontend lets standard MySQL clients (`mysql` CLI, JDBC, Python `mysql-connector`, Go `go-sql-driver/mysql`, etc.) connect to QueryFlux over the MySQL wire protocol. This is the natural entry point when routing traffic to MySQL-protocol backends like StarRocks.

## Configuration

```yaml
queryflux:
  frontends:
    mysqlWire:
      enabled: true
      port: 3306
```

Config key: `mysqlWire`. Protocol identifier: `FrontendProtocol::MySqlWire`. Default dialect: `SqlDialect::MySql`.

## Protocol support

| Feature | Status |
|---------|--------|
| Server handshake (protocol 10) | Supported — advertises `8.0.0-queryflux` |
| `mysql_native_password` auth | Supported |
| `COM_QUERY` | Supported |
| `COM_PING` | Supported |
| `COM_INIT_DB` (schema switch) | Supported |
| `COM_QUIT` | Supported |
| `COM_FIELD_LIST` | Stub (returns empty EOF) |
| SSL/TLS handshake | Detected and rejected — use `--ssl-mode=DISABLED` |
| Prepared statements (`COM_STMT_*`) | Not supported |

Clients must connect without TLS (`--ssl-mode=DISABLED` for `mysql` CLI, `useSSL=false` for JDBC). If the client sends an SSL request, QueryFlux returns an error and closes the connection.

## Handshake and authentication

1. QueryFlux sends a server handshake packet (protocol version 10, server version `8.0.0-queryflux`, `mysql_native_password`).
2. Client responds with username and optional database.
3. QueryFlux authenticates via the configured `auth_provider`.
4. On success, an OK packet is returned and the connection is ready.

## Execution model

Queries execute **synchronously** via `execute_to_sink`. Results stream as MySQL text protocol result sets:

1. Column count packet.
2. Column definition packets.
3. Row data packets (text values).
4. EOF / OK packet.

## Built-in query handling

The MySQL frontend intercepts several common queries that clients and drivers send during connection setup:

| Query pattern | Behavior |
|---------------|----------|
| `SET query_tags = '...'` / `SET SESSION query_tags = '...'` | Stores tags on the session for routing |
| `SET ...` (other) | Acknowledged without forwarding to backend |
| `USE <schema>` | Updates session schema |
| `SELECT @@version` | Returns `8.0.0-queryflux` |
| `SELECT DATABASE()` | Returns current session schema |
| `SHOW VARIABLES` / `SHOW STATUS` | Returns empty result set |

Leading `/* ... */` and `/*!...*/` conditional comments in SQL are stripped before processing.

## Session context

`SessionContext::MySqlWire` carries:

| Field | Source |
|-------|--------|
| `user` | Handshake response |
| `schema` | `COM_INIT_DB` or initial database from handshake |
| `session_vars` | Currently empty (generic `SET` is not stored) |
| `tags` | From `SET query_tags` / `SET SESSION query_tags` |

The `schema` and `user` fields are available to routers — the `pythonScript` router receives them in `ctx["schema"]` and `ctx["user"]`.

## Client examples

```bash
# mysql CLI
mysql -h 127.0.0.1 -P 3306 -u dev --ssl-mode=DISABLED -e "SELECT 1"

# With database
mysql -h 127.0.0.1 -P 3306 -u dev -D my_catalog --ssl-mode=DISABLED
```

```python
# Python (mysql-connector)
import mysql.connector
conn = mysql.connector.connect(host="127.0.0.1", port=3306, user="dev", ssl_disabled=True)
cur = conn.cursor()
cur.execute("SELECT 42 AS answer")
print(cur.fetchone())
```

## Related

- [Frontends overview](overview.md) — shared dispatch and session model
- [Routing and clusters](/docs/architecture/routing-and-clusters) — `protocolBased` router with `mysqlWire`
- [Query tags](/docs/architecture/query-tags) — setting tags via `SET query_tags`
