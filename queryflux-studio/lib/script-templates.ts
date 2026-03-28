/**
 * Starter bodies for user_scripts rows. Must match runtime contracts:
 * - translation_fixup: sqlglot AST transform
 * - routing: PythonScriptRouter `route(query, ctx)` (see docs/routing-and-clusters.md)
 */

export const TRANSLATION_FIXUP_TEMPLATE = `# Do not remove the imports below — they are required by the proxy.
from sqlglot.expressions import Expression
import sqlglot.expressions as exp


def transform(ast: Expression, src: str, dst: str) -> None:
    # src: source dialect (e.g. "trino"), dst: target dialect (e.g. "athena")
    pass
`;

export const ROUTING_SCRIPT_TEMPLATE = `def route(query: str, ctx: dict) -> str | None:
    """Pick a cluster group name, or None to let the next router decide.

    ctx always includes:
      - protocol: "trinoHttp" | "postgresWire" | "mysqlWire" | "clickHouseHttp" | "flightSql"
      - headers: dict[str, str] (empty {} for Postgres/MySQL wire; Trino uses lowercase keys)

    When authenticated, ctx may include:
      - auth: {"user": str, "groups": [str, ...], "roles": [str, ...]}

    Postgres wire also: database, user, sessionParams.
    MySQL wire: schema, user, sessionVars.
    ClickHouse HTTP: queryParams.
    """
    # Example:
    # if ctx.get("protocol") == "trinoHttp":
    #     user = (ctx.get("headers") or {}).get("x-trino-user")
    #     if user == "batch":
    #         return "my-heavy-group"
    return None
`;
