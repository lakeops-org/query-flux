---
sidebar_position: 3
description: How to connect AI agents to QueryFlux using the MCP server, agent context propagation, and query guardrails.
---

# AI Agent Integration

QueryFlux includes first-class support for AI agent workloads. Agents can query any connected engine through a standard interface, route automatically to the right backend, and stay within enforced guardrails — without knowing anything about the underlying infrastructure.

---

## How it works

The **MCP frontend** (`queryflux-frontend-mcp`) exposes QueryFlux as a set of tools and resources compatible with the [Model Context Protocol](https://modelcontextprotocol.io). Any agent framework that supports MCP — LangChain, LlamaIndex, custom Claude tools, OpenAI function calling — can connect directly.

```
Agent (LLM + tool calls)
        │
        ▼  MCP (port 8811)
queryflux-frontend-mcp
        │
        ▼
RouterChain  →  ClusterGroupManager  →  Engine Adapter
(routing)        (load balancing)        (Trino / DuckDB / StarRocks / …)
```

Agents issue queries through MCP tools. QueryFlux handles routing, translation, concurrency limits, and result streaming — identical to what a human analyst would get through the Trino or Postgres frontend.

---

## MCP tools

### `execute_query`

Execute a SQL query against any connected engine.

```json
{
  "tool": "execute_query",
  "arguments": {
    "sql": "SELECT region, SUM(revenue) FROM orders GROUP BY 1",
    "engine_hint": "trino",
    "max_rows": 1000
  }
}
```

| Parameter | Type | Description |
|-----------|------|-------------|
| `sql` | string | SQL to execute |
| `engine_hint` | string? | Preferred engine group (router can override) |
| `max_rows` | int? | Cap on returned rows (default: 500) |

Returns: column names, rows as arrays, execution time, engine used, and row count.

### `list_engines`

List available engine groups and their current status.

```json
{ "tool": "list_engines" }
```

Returns each group name, member clusters, health status, and running/queued query counts.

### `get_query_history`

Retrieve recent queries matching optional filters.

```json
{
  "tool": "get_query_history",
  "arguments": {
    "limit": 20,
    "engine_group": "duckdb-local"
  }
}
```

---

## Connecting to the MCP server

The MCP server starts on port `8811` by default. Configure it in your QueryFlux config:

```yaml
frontends:
  mcp:
    enabled: true
    port: 8811
    auth:
      type: static
      token: "${MCP_AUTH_TOKEN}"
```

### Claude Desktop

```json
{
  "mcpServers": {
    "queryflux": {
      "command": "npx",
      "args": ["-y", "@queryflux/mcp-client"],
      "env": {
        "QUERYFLUX_URL": "http://localhost:8811",
        "QUERYFLUX_TOKEN": "your-token"
      }
    }
  }
}
```

### LangChain / LlamaIndex

Any framework with an MCP adapter can connect using the server URL and token. Refer to your framework's MCP integration docs for the exact wiring.

---

## Agent context propagation

QueryFlux tracks which agent issued each query. Pass context headers so the query history and observability data are attributed correctly:

| Header | Description |
|--------|-------------|
| `X-Agent-Id` | Stable identifier for the agent (e.g. `revenue-analyst-v2`) |
| `X-Conversation-Id` | Current conversation or session ID |
| `X-Step-Index` | Step number within the conversation |
| `X-Tool-Call-Id` | Tool call identifier from the LLM response |

These flow into every `QueryRecord` and appear in QueryFlux Studio under the **Queries** tab. You can filter history by `agent_id` to see everything a specific agent has run.

---

## Query guardrails

Agents can issue arbitrary SQL — guardrails prevent unsafe queries from reaching the engine. They are configured in the `guardrails` block and evaluated in order before any query executes.

```yaml
guardrails:
  - type: readOnly          # block INSERT / UPDATE / DELETE / DDL
  - type: rowLimit
    max_rows: 100000        # rewrite queries that lack a LIMIT
  - type: costEstimate
    max_gb_scanned: 50      # reject before execution (Trino explain plan)
  - type: humanApproval
    patterns:
      - "DROP TABLE"
      - "TRUNCATE"
    webhook_url: "https://your-approval-system/approve"
    timeout_ms: 30000
```

Each guardrail can:

- **Allow** — pass the query through unchanged
- **Rewrite** — modify the SQL (e.g. inject a LIMIT clause)
- **Reject** — return an error immediately
- **Require approval** — pause execution and call a webhook; resume or cancel based on the response

The `readOnly` and `rowLimit` guardrails are recommended for all agent deployments. `humanApproval` is useful for destructive statements that agents might occasionally generate.

---

## Routing for agents

Agents often mix query types in a single session: fast lookups against a hot store (StarRocks), large scans against Trino, ad-hoc exploration against DuckDB. QueryFlux routes each query independently.

The built-in router types that work well for agent traffic:

- **`queryRegex`** — route by SQL pattern (e.g. queries touching `iceberg.` catalog go to Trino)
- **`tags`** — agents can attach tags to their session to express intent:

```json
{
  "tool": "execute_query",
  "arguments": {
    "sql": "...",
    "tags": { "workload": "batch", "agent": "pipeline-v1" }
  }
}
```

```yaml
routers:
  - type: tags
    rules:
      - tags: { workload: batch }
        targetGroup: trino-batch
      - tags: { workload: interactive }
        targetGroup: starrocks-hot
```

- **`protocolBased`** — all MCP traffic can be sent to a dedicated engine group, separate from BI tool traffic on the Postgres or Trino frontends.

---

## Example: multi-engine agent

A revenue analysis agent that routes automatically across engines:

```python
# Agent tool call (pseudocode)
result = mcp.call("execute_query", {
    "sql": """
        SELECT
            DATE_TRUNC('month', order_date) AS month,
            SUM(revenue)                   AS total_revenue
        FROM iceberg.sales.orders
        WHERE order_date >= DATE '2024-01-01'
        GROUP BY 1
        ORDER BY 1
    """,
    "tags": { "workload": "analytical" }
})
```

QueryFlux receives this query, evaluates the router chain:
1. `tags` router matches `workload: analytical` → routes to `trino-cluster`
2. Cluster manager picks the least-loaded Trino node
3. Query runs; result returned to the agent

If the agent follows up with a small lookup:

```python
result = mcp.call("execute_query", {
    "sql": "SELECT * FROM dim_customers WHERE customer_id = 42",
    "tags": { "workload": "interactive" }
})
```

The `tags` router routes this to `starrocks-hot`. Same agent session, two different engines — no configuration required in the agent code.

---

## Observability

Every agent query appears in:

- **QueryFlux Studio** — filter the Queries tab by `agent_id` or `conversation_id`
- **Prometheus** — `queryflux_queries_total` and `queryflux_query_duration_seconds` metrics include an `agent_id` label
- **Query history API** — `GET /api/v1/history?agent_id=revenue-analyst-v2`

This lets you audit what agents ran, how long queries took, which engines were used, and whether any guardrails fired.
