use std::sync::Arc;
use std::time::Duration;

use axum::{
    body::Body,
    extract::{Path, State},
    http::{HeaderMap, HeaderValue, Response, StatusCode},
    response::IntoResponse,
};
use bytes::Bytes;
use chrono::Utc;
use queryflux_core::{
    query::{
        ClusterGroupName, ClusterName, ExecutingQuery, FrontendProtocol, IncomingQuery,
        ProxyQueryId, QueuedQuery, QueryExecution, QueryPollResult,
    },
    session::SessionContext,
};
use queryflux_engine_adapters::trino::api::{queued_response, TrinoResponse};
use serde_json::Value;
use tracing::{debug, info, warn};

use super::state::AppState;

fn json_response(body: impl serde::Serialize) -> Response<Body> {
    let json = serde_json::to_vec(&body).unwrap_or_default();
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/json")
        .body(Body::from(json))
        .unwrap()
}

fn raw_response_with_rewritten_next_uri(
    body_bytes: Bytes,
    proxy_next_uri: Option<String>,
) -> Response<Body> {
    // Parse the JSON, rewrite nextUri, re-serialize.
    let mut json: Value = match serde_json::from_slice(&body_bytes) {
        Ok(v) => v,
        Err(_) => return raw_bytes_response(body_bytes),
    };

    if let Some(uri) = proxy_next_uri {
        json["nextUri"] = Value::String(uri);
    } else {
        json.as_object_mut().map(|o| o.remove("nextUri"));
    }

    let out = serde_json::to_vec(&json).unwrap_or_default();
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/json")
        .body(Body::from(out))
        .unwrap()
}

fn raw_bytes_response(bytes: Bytes) -> Response<Body> {
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/json")
        .body(Body::from(bytes))
        .unwrap()
}

/// Exponential backoff delay for queued query polling.
/// Mirrors trino-lb: min(2^(seq+7) ms, 3000ms)
async fn queued_backoff_delay(sequence: u64) {
    if sequence > 0 {
        let ms = (2u64.saturating_pow((sequence + 7) as u32)).min(3000);
        tokio::time::sleep(Duration::from_millis(ms)).await;
    }
}

fn extract_session(headers: &HeaderMap) -> SessionContext {
    let mut h = std::collections::HashMap::new();
    for (k, v) in headers {
        if let Ok(s) = v.to_str() {
            h.insert(k.as_str().to_lowercase(), s.to_string());
        }
    }
    SessionContext::TrinoHttp { headers: h }
}

/// POST /v1/statement — client submits a new query.
pub async fn post_statement(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    let sql = match std::str::from_utf8(&body) {
        Ok(s) => s.to_string(),
        Err(_) => return StatusCode::BAD_REQUEST.into_response(),
    };

    let session = extract_session(&headers);
    let protocol = FrontendProtocol::TrinoHttp;

    // Route.
    let group = match state.router_chain.route(&sql, &session, &protocol).await {
        Ok(g) => g,
        Err(e) => {
            warn!("Routing error: {e}");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };

    let query_id = ProxyQueryId::new();
    info!(id = %query_id, group = %group, "New query submitted");

    queue_or_dispatch(&state, query_id, sql, session, protocol, group, false, 0).await
}

/// GET /v1/statement/qf/queued/{id}/{seq} — poll a query queued in QueryFlux.
pub async fn get_queued_statement(
    State(state): State<Arc<AppState>>,
    Path((id, seq)): Path<(String, u64)>,
) -> impl IntoResponse {
    let query_id = ProxyQueryId(id);

    let queued = match state.persistence.get_queued(&query_id).await {
        Ok(Some(q)) => q,
        Ok(None) => return StatusCode::NOT_FOUND.into_response(),
        Err(e) => {
            warn!("Persistence error: {e}");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };

    queued_backoff_delay(seq).await;

    let sql = queued.sql.clone();
    let session = queued.session.clone();
    let protocol = queued.frontend_protocol.clone();
    let group = queued.cluster_group.clone();

    queue_or_dispatch(&state, query_id, sql, session, protocol, group, true, seq).await
}

/// GET /v1/statement/qf/executing/{id} — poll an executing query.
pub async fn get_executing_statement(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let query_id = ProxyQueryId(id);

    let executing = match state.persistence.get(&query_id).await {
        Ok(Some(q)) => q,
        Ok(None) => return StatusCode::NOT_FOUND.into_response(),
        Err(e) => {
            warn!("Persistence error: {e}");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };

    let adapter = match state.adapter(&executing.cluster_group.0, &executing.cluster_name.0) {
        Some(a) => a,
        None => {
            warn!("No adapter for cluster {}/{}", executing.cluster_group, executing.cluster_name);
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };

    let backend_id = match &executing.backend_query_id {
        Some(id) => id.clone(),
        None => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };

    let poll_result = match adapter
        .poll_query(&backend_id, executing.next_uri.as_deref())
        .await
    {
        Ok(r) => r,
        Err(e) => {
            warn!("Poll error: {e}");
            let _ = state
                .cluster_manager
                .release_cluster(&executing.cluster_group, &executing.cluster_name)
                .await;
            let _ = state.persistence.delete(&query_id).await;
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };

    match poll_result {
        QueryPollResult::Raw { body, next_uri } => {
            if next_uri.is_none() {
                // Query is complete — release cluster, remove from persistence.
                let _ = state
                    .cluster_manager
                    .release_cluster(&executing.cluster_group, &executing.cluster_name)
                    .await;
                let _ = state.persistence.delete(&query_id).await;
                return raw_response_with_rewritten_next_uri(body, None).into_response();
            }

            // Update stored next_uri.
            let mut updated = executing.clone();
            updated.next_uri = next_uri.clone();
            updated.last_accessed = Utc::now();
            let _ = state.persistence.upsert(updated).await;

            let proxy_next_uri = Some(format!(
                "{}/v1/statement/qf/executing/{}",
                state.external_address, query_id
            ));
            raw_response_with_rewritten_next_uri(body, proxy_next_uri).into_response()
        }

        QueryPollResult::Failed { message, .. } => {
            let _ = state
                .cluster_manager
                .release_cluster(&executing.cluster_group, &executing.cluster_name)
                .await;
            let _ = state.persistence.delete(&query_id).await;
            warn!(id = %query_id, "Query failed: {message}");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }

        QueryPollResult::Pending { next_uri, .. } => {
            let mut updated = executing.clone();
            updated.next_uri = next_uri;
            updated.last_accessed = Utc::now();
            let _ = state.persistence.upsert(updated).await;

            let proxy_next_uri = format!(
                "{}/v1/statement/qf/executing/{}",
                state.external_address, query_id
            );
            let resp = queued_response(&query_id.0, 0, proxy_next_uri);
            json_response(&resp).into_response()
        }

        QueryPollResult::Complete { .. } => {
            let _ = state
                .cluster_manager
                .release_cluster(&executing.cluster_group, &executing.cluster_name)
                .await;
            let _ = state.persistence.delete(&query_id).await;
            // Complete without Raw — should not happen for Trino adapter.
            json_response(&serde_json::json!({ "id": query_id.0, "stats": { "state": "FINISHED" } }))
                .into_response()
        }
    }
}

/// DELETE /v1/statement/qf/executing/{id} — cancel a running query.
pub async fn delete_executing_statement(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let query_id = ProxyQueryId(id);

    if let Ok(Some(executing)) = state.persistence.get(&query_id).await {
        if let Some(next_uri) = &executing.next_uri {
            // Trino cancel = DELETE the nextUri.
            let client = reqwest::Client::new();
            let _ = client.delete(next_uri).send().await;
        }
        let _ = state
            .cluster_manager
            .release_cluster(&executing.cluster_group, &executing.cluster_name)
            .await;
        let _ = state.persistence.delete(&query_id).await;
    } else if let Ok(Some(_)) = state.persistence.get_queued(&query_id).await {
        let _ = state.persistence.delete_queued(&query_id).await;
    }

    StatusCode::NO_CONTENT.into_response()
}

/// Core logic: try to dispatch to a cluster, or queue if none available.
async fn queue_or_dispatch(
    state: &Arc<AppState>,
    query_id: ProxyQueryId,
    sql: String,
    session: SessionContext,
    protocol: FrontendProtocol,
    group: ClusterGroupName,
    queued_already_stored: bool,
    sequence: u64,
) -> axum::response::Response {
    // Try to acquire a cluster.
    let cluster_name = match state.cluster_manager.acquire_cluster(&group).await {
        Ok(Some(c)) => c,
        Ok(None) => {
            // No capacity — queue the query.
            return queue_query(state, query_id, sql, session, protocol, group, queued_already_stored, sequence).await;
        }
        Err(e) => {
            warn!("Cluster acquire error: {e}");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };

    // Get adapter.
    let adapter = match state.adapter(&group.0, &cluster_name.0) {
        Some(a) => a,
        None => {
            let _ = state.cluster_manager.release_cluster(&group, &cluster_name).await;
            warn!("No adapter for {}/{}", group, cluster_name);
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };

    // Submit query.
    let execution = match adapter.submit_query(&sql, &session).await {
        Ok(e) => e,
        Err(e) => {
            let _ = state.cluster_manager.release_cluster(&group, &cluster_name).await;
            warn!("Submit error: {e}");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };

    // Remove from queued persistence if it was stored there.
    if queued_already_stored {
        let _ = state.persistence.delete_queued(&query_id).await;
    }

    match execution {
        QueryExecution::Async { backend_query_id, next_uri, initial_body } => {
            // Store executing query.
            let now = Utc::now();
            let executing = ExecutingQuery {
                id: query_id.clone(),
                sql,
                translated_sql: None,
                cluster_group: group.clone(),
                cluster_name: cluster_name.clone(),
                backend_query_id: Some(backend_query_id),
                next_uri: next_uri.clone(),
                creation_time: now,
                last_accessed: now,
            };
            let _ = state.persistence.upsert(executing).await;

            info!(id = %query_id, cluster = %cluster_name, "Query handed to Trino");

            let proxy_next_uri = next_uri.as_ref().map(|_| {
                format!("{}/v1/statement/qf/executing/{}", state.external_address, query_id)
            });

            match initial_body {
                Some(body) => raw_response_with_rewritten_next_uri(body, proxy_next_uri).into_response(),
                None => {
                    let resp = queued_response(&query_id.0, 0, proxy_next_uri.unwrap_or_default());
                    json_response(&resp).into_response()
                }
            }
        }

        QueryExecution::Sync { result } => {
            let _ = state.cluster_manager.release_cluster(&group, &cluster_name).await;
            match result {
                QueryPollResult::Complete { .. } => {
                    json_response(&serde_json::json!({
                        "id": query_id.0,
                        "stats": { "state": "FINISHED" }
                    })).into_response()
                }
                QueryPollResult::Failed { message, .. } => {
                    warn!("Sync query failed: {message}");
                    StatusCode::INTERNAL_SERVER_ERROR.into_response()
                }
                _ => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
            }
        }
    }
}

async fn queue_query(
    state: &Arc<AppState>,
    query_id: ProxyQueryId,
    sql: String,
    session: SessionContext,
    protocol: FrontendProtocol,
    group: ClusterGroupName,
    already_stored: bool,
    sequence: u64,
) -> axum::response::Response {
    let now = Utc::now();
    let queued = QueuedQuery {
        id: query_id.clone(),
        sql,
        session,
        frontend_protocol: protocol,
        cluster_group: group,
        creation_time: now,
        last_accessed: now,
        sequence,
    };

    if already_stored {
        // Just update last_accessed (avoid hammering persistence).
        let _ = state.persistence.upsert_queued(queued).await;
    } else {
        let _ = state.persistence.upsert_queued(queued).await;
    }

    let next_seq = sequence + 1;
    let next_uri = format!(
        "{}/v1/statement/qf/queued/{}/{}",
        state.external_address, query_id, next_seq
    );
    let elapsed_ms = 0u64; // Simplified for now.
    let resp = queued_response(&query_id.0, elapsed_ms, next_uri);
    json_response(&resp).into_response()
}
