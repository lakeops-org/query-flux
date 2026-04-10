use std::sync::Arc;
use std::time::Instant;

use rmcp::handler::server::wrapper::Parameters;
use queryflux_auth::{AuthContext, Credentials};
use queryflux_core::{
    query::{ClusterGroupName, FrontendProtocol},
    session::SessionContext,
    tags::QueryTags,
};
use rmcp::model::{CallToolResult, Content, ErrorData, ErrorCode};
use rmcp::{tool, tool_handler, tool_router};
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::{json, Value};
use tracing::{debug, info, warn};

use crate::{
    dispatch::execute_to_sink,
    mcp::sink::JsonResultSink,
    mcp::BEARER_TOKEN,
    state::AppState,
};

const DEFAULT_MAX_ROWS: usize = 500;

#[derive(Debug, Deserialize, JsonSchema)]
struct ExecuteSqlParams {
    sql: String,
    engine_hint: Option<String>,
    max_rows: Option<usize>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct GetSchemaParams {
    table: String,
    engine_group: Option<String>,
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

fn mcp_err(msg: impl Into<String>) -> ErrorData {
    ErrorData::new(ErrorCode::INTERNAL_ERROR, msg.into(), None)
}

fn mcp_invalid(msg: impl Into<String>) -> ErrorData {
    ErrorData::new(ErrorCode::INVALID_PARAMS, msg.into(), None)
}

/// Some Trino DESCRIBE responses come back with a single tab-delimited string in
/// `col_name` and null `data_type`/`comment`. Normalize that shape into proper
/// `{ col_name, data_type, comment }` rows for MCP clients.
fn normalize_describe_rows(result: &mut Value) {
    let normalized_len = {
        let Some(rows) = result.get_mut("rows").and_then(Value::as_array_mut) else {
            return;
        };
        let mut normalized = Vec::with_capacity(rows.len());
        for row in rows.iter() {
            let Some(obj) = row.as_object() else {
                normalized.push(row.clone());
                continue;
            };
            let Some(col_name_raw) = obj.get("col_name").and_then(Value::as_str) else {
                normalized.push(row.clone());
                continue;
            };
            let data_type_is_null = obj.get("data_type").is_none_or(Value::is_null);
            let comment_is_null = obj.get("comment").is_none_or(Value::is_null);
            if !(data_type_is_null && comment_is_null && col_name_raw.contains('\t')) {
                normalized.push(row.clone());
                continue;
            }
            let mut parts = col_name_raw.split('\t');
            let c0 = parts.next().unwrap_or_default().trim();
            let c1 = parts.next().unwrap_or_default().trim();
            let c2 = parts.next().unwrap_or_default().trim();
            normalized.push(json!({
                "col_name": if c0.is_empty() { Value::Null } else { Value::String(c0.to_string()) },
                "data_type": if c1.is_empty() { Value::Null } else { Value::String(c1.to_string()) },
                "comment": if c2.is_empty() { Value::Null } else { Value::String(c2.to_string()) },
            }));
        }
        *rows = normalized;
        rows.len()
    };
    if let Some(row_count) = result.get_mut("row_count") {
        *row_count = json!(normalized_len);
    }
}

/// Build an `AuthContext` for the current MCP request by reading the bearer
/// token that was threaded via `BEARER_TOKEN` task-local from the axum middleware.
async fn auth_ctx(state: &AppState) -> Result<AuthContext, ErrorData> {
    let token = BEARER_TOKEN.try_with(Clone::clone).unwrap_or(None);
    debug!(has_bearer = token.is_some(), "MCP auth_ctx: validating bearer token");
    let creds = Credentials {
        username: None,
        password: None,
        bearer_token: token,
    };
    state
        .auth_provider
        .authenticate(&creds)
        .await
        .map_err(|e| {
            warn!(error = %e, "MCP auth_ctx: authentication failed");
            ErrorData::new(ErrorCode::INVALID_REQUEST, e.to_string(), None)
        })
}

/// Build a `SessionContext::Mcp` from optional agent headers.
fn mcp_session(user: Option<String>, agent_id: Option<String>, conversation_id: Option<String>) -> SessionContext {
    SessionContext::Mcp {
        user,
        agent_id,
        conversation_id,
        tags: QueryTags::new(),
    }
}

/// Resolve the target cluster group: use `engine_hint` if provided and valid,
/// otherwise route normally via the router chain.
async fn resolve_group(
    state: &AppState,
    sql: &str,
    session: &SessionContext,
    engine_hint: Option<&str>,
    auth_ctx: &AuthContext,
) -> Result<ClusterGroupName, ErrorData> {
    if let Some(hint) = engine_hint {
        let live = state.live.read().await;
        if live.group_members.contains_key(hint) {
            return Ok(ClusterGroupName(hint.to_string()));
        }
        return Err(mcp_invalid(format!("Unknown engine group: {hint}")));
    }

    let (group, _trace) = {
        let live = state.live.read().await;
        live.router_chain
            .route_with_trace(sql, session, &FrontendProtocol::Mcp, Some(auth_ctx))
            .await
    }
    .map_err(|e| mcp_err(e.to_string()))?;

    Ok(group)
}

// ---------------------------------------------------------------------------
// MCP Server — tool handlers
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct QueryFluxMcpServer {
    pub state: Arc<AppState>,
}

impl QueryFluxMcpServer {
    pub fn new(state: Arc<AppState>) -> Self {
        Self { state }
    }
}

#[tool_router]
impl QueryFluxMcpServer {
    /// Execute a SQL query against any connected QueryFlux engine.
    /// Returns result rows as JSON with column metadata.
    #[tool(description = "Execute a SQL query against any connected QueryFlux engine. Returns rows as JSON objects keyed by column name.")]
    async fn execute_sql(
        &self,
        Parameters(ExecuteSqlParams {
            sql,
            engine_hint,
            max_rows,
        }): Parameters<ExecuteSqlParams>,
    ) -> Result<CallToolResult, ErrorData> {
        let auth = auth_ctx(&self.state).await?;
        let session = mcp_session(Some(auth.user.clone()), None, None);
        let group = resolve_group(
            &self.state,
            &sql,
            &session,
            engine_hint.as_deref(),
            &auth,
        )
        .await?;

        debug!(sql = %sql, group = %group, "MCP execute_sql");

        let engine_name = group.0.clone();
        let limit = max_rows.unwrap_or(DEFAULT_MAX_ROWS).min(10_000);
        let mut sink = JsonResultSink::new(limit);
        let start = Instant::now();

        execute_to_sink(&self.state, sql, session, FrontendProtocol::Mcp, group, &mut sink, &auth)
            .await
            .map_err(|e| mcp_err(e.to_string()))?;

        if let Some(err) = &sink.error {
            return Ok(CallToolResult::error(vec![Content::text(err.clone())]));
        }

        let elapsed = start.elapsed().as_millis() as u64;
        let mut result = sink.into_result(elapsed, &engine_name);
        normalize_describe_rows(&mut result);
        Ok(CallToolResult::success(vec![Content::text(
            serde_json::to_string_pretty(&result).unwrap_or_default(),
        )]))
    }

    /// List all available engine groups, their member clusters, and current health status.
    #[tool(description = "List all available engine groups in QueryFlux with their member clusters and health status.")]
    async fn list_engines(&self) -> Result<CallToolResult, ErrorData> {
        let (group_order, group_members, cluster_manager) = {
            let live = self.state.live.read().await;
            (
                live.group_order.clone(),
                live.group_members.clone(),
                live.cluster_manager.clone(),
            )
        };

        let all_states = cluster_manager
            .all_cluster_states()
            .await
            .map_err(|e| mcp_err(e.to_string()))?;

        // Index states by cluster name for O(1) lookup
        let state_map: std::collections::HashMap<String, _> = all_states
            .into_iter()
            .map(|s| (s.cluster_name.0.clone(), s))
            .collect();

        let mut groups = Vec::new();
        for group_name in &group_order {
            let members = group_members.get(group_name).cloned().unwrap_or_default();
            let cluster_info: Vec<_> = members
                .iter()
                .map(|cluster_name| {
                    if let Some(s) = state_map.get(cluster_name) {
                        json!({
                            "name": cluster_name,
                            "engine": format!("{:?}", s.engine_type),
                            "running_queries": s.running_queries,
                            "max_running_queries": s.max_running_queries,
                            "healthy": s.is_healthy,
                            "enabled": s.enabled,
                        })
                    } else {
                        json!({ "name": cluster_name })
                    }
                })
                .collect();

            groups.push(json!({
                "group": group_name,
                "clusters": cluster_info,
            }));
        }

        let result = json!({ "engine_groups": groups });
        Ok(CallToolResult::success(vec![Content::text(
            serde_json::to_string_pretty(&result).unwrap_or_default(),
        )]))
    }

    /// Describe the columns of a table by executing DESCRIBE against the target engine.
    #[tool(description = "Describe the columns of a table (name, type, nullable). Executes DESCRIBE <table> against the target engine.")]
    async fn get_schema(
        &self,
        Parameters(GetSchemaParams {
            table,
            engine_group,
        }): Parameters<GetSchemaParams>,
    ) -> Result<CallToolResult, ErrorData> {
        let auth = auth_ctx(&self.state).await?;
        let sql = format!("DESCRIBE {table}");
        let session = mcp_session(Some(auth.user.clone()), None, None);
        let group = resolve_group(
            &self.state,
            &sql,
            &session,
            engine_group.as_deref(),
            &auth,
        )
        .await?;

        let engine_name = group.0.clone();
        let mut sink = JsonResultSink::new(1_000);
        let start = Instant::now();

        execute_to_sink(&self.state, sql, session, FrontendProtocol::Mcp, group, &mut sink, &auth)
            .await
            .map_err(|e| mcp_err(e.to_string()))?;

        if let Some(err) = &sink.error {
            return Ok(CallToolResult::error(vec![Content::text(err.clone())]));
        }

        let elapsed = start.elapsed().as_millis() as u64;
        let result = sink.into_result(elapsed, &engine_name);
        Ok(CallToolResult::success(vec![Content::text(
            serde_json::to_string_pretty(&result).unwrap_or_default(),
        )]))
    }
}

#[tool_handler(router = Self::tool_router())]
impl rmcp::ServerHandler for QueryFluxMcpServer {
    fn get_info(&self) -> rmcp::model::ServerInfo {
        let info = rmcp::model::ServerInfo::new(
            rmcp::model::ServerCapabilities::builder()
                .enable_tools()
                .build(),
        )
        .with_instructions(
            "QueryFlux MCP server. Tools: execute_sql, list_engines, get_schema.",
        );
        info!("MCP get_info called; advertising tools capability");
        info
    }
}
