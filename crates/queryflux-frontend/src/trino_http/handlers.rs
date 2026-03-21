use std::sync::Arc;
use std::time::Duration;

use axum::{
    body::Body,
    extract::{Path, State},
    http::{HeaderMap, Response, StatusCode},
    response::IntoResponse,
};
use bytes::Bytes;
use chrono::Utc;
use queryflux_core::{
    query::{
        BackendQueryId, FrontendProtocol, ProxyQueryId, QueryPollResult, QueryStatus,
    },
    session::SessionContext,
};
use queryflux_engine_adapters::trino::api::{queued_response, TrinoError, TrinoResponse, TrinoStats};
use serde_json::Value;
use tracing::{info, warn};

use crate::dispatch::{dispatch_query, execute_to_sink, rewrite_trino_uri, DispatchOutcome};
use crate::state::AppState;
use super::result_sink::TrinoHttpResultSink;

fn json_response(body: impl serde::Serialize) -> Response<Body> {
    let json = serde_json::to_vec(&body).unwrap_or_default();
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/json")
        .body(Body::from(json))
        .unwrap()
}

/// Rewrite the `"nextUri":"..."` field in a raw JSON byte slice without a full parse/serialize.
fn raw_response_with_rewritten_next_uri(
    body_bytes: Bytes,
    proxy_next_uri: Option<String>,
) -> Response<Body> {
    let out = rewrite_next_uri_bytes(&body_bytes, proxy_next_uri.as_deref());
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/json")
        .body(Body::from(out))
        .unwrap()
}

fn rewrite_next_uri_bytes(src: &[u8], new_uri: Option<&str>) -> Bytes {
    const KEY: &[u8] = b"\"nextUri\"";
    if let Some(key_pos) = find_subsequence(src, KEY) {
        let after_key = &src[key_pos + KEY.len()..];
        let colon_offset = after_key.iter().position(|&b| b == b':').unwrap_or(0);
        let after_colon = &after_key[colon_offset + 1..];
        let value_start_offset = after_colon.iter().position(|&b| !b.is_ascii_whitespace()).unwrap_or(0);
        let value_start = key_pos + KEY.len() + colon_offset + 1 + value_start_offset;

        if src[value_start] == b'"' {
            if let Some(end_offset) = src[value_start + 1..].iter().position(|&b| b == b'"') {
                let value_end = value_start + 1 + end_offset + 1;
                let before = &src[..key_pos];
                let after = &src[value_end..];

                return match new_uri {
                    Some(uri) => {
                        let mut out = Vec::with_capacity(src.len() + uri.len());
                        out.extend_from_slice(before);
                        out.extend_from_slice(KEY);
                        out.extend_from_slice(b":");
                        out.push(b'"');
                        out.extend_from_slice(uri.as_bytes());
                        out.push(b'"');
                        out.extend_from_slice(after);
                        Bytes::from(out)
                    }
                    None => {
                        let mut out = Vec::with_capacity(src.len());
                        let trim_end = before.iter().rposition(|&b| b == b',').unwrap_or(before.len());
                        let has_preceding_comma = trim_end < before.len();
                        if has_preceding_comma {
                            out.extend_from_slice(&before[..trim_end]);
                        } else {
                            out.extend_from_slice(before);
                        }
                        let after_trimmed = if !has_preceding_comma {
                            let skip = after.iter().position(|&b| b != b',' && !b.is_ascii_whitespace()).unwrap_or(0);
                            &after[skip..]
                        } else {
                            after
                        };
                        out.extend_from_slice(after_trimmed);
                        Bytes::from(out)
                    }
                };
            }
        }
    }

    // Fallback: full serde parse/serialize.
    let mut json: Value = match serde_json::from_slice(src) {
        Ok(v) => v,
        Err(_) => return Bytes::copy_from_slice(src),
    };
    if let Some(uri) = new_uri {
        json["nextUri"] = Value::String(uri.to_string());
    } else {
        json.as_object_mut().map(|o| o.remove("nextUri"));
    }
    Bytes::from(serde_json::to_vec(&json).unwrap_or_default())
}

fn find_subsequence(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

/// Exponential backoff delay for queued query polling.
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

fn outcome_to_response(
    _state: &Arc<AppState>,
    query_id: &ProxyQueryId,
    outcome: DispatchOutcome,
) -> Response<Body> {
    match outcome {
        DispatchOutcome::Queued { queued_next_uri } => {
            let resp = queued_response(&query_id.0, 0, queued_next_uri);
            json_response(&resp).into_response()
        }
        DispatchOutcome::Async { initial_body, proxy_next_uri } => match initial_body {
            Some(body) => raw_response_with_rewritten_next_uri(body, proxy_next_uri).into_response(),
            None => {
                let resp = queued_response(&query_id.0, 0, proxy_next_uri.unwrap_or_default());
                json_response(&resp).into_response()
            }
        },
    }
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

    let (group, _trace) = match state.router_chain.route_with_trace(&sql, &session, &protocol).await {
        Ok(r) => r,
        Err(e) => {
            warn!("Routing error: {e}");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };

    let query_id = ProxyQueryId::new();
    info!(id = %query_id, group = %group, "New query submitted");

    // Trino backend: raw bytes forwarded, nextUri rewritten — zero Arrow.
    // Non-Trino backend (DuckDB, StarRocks): Arrow path → single-page Trino JSON response.
    if state.group_supports_async(&group.0) {
        match dispatch_query(&state, query_id.clone(), sql, session, protocol, group, false, 0).await {
            Ok(outcome) => outcome_to_response(&state, &query_id, outcome),
            Err(e) => {
                warn!("Dispatch error: {e}");
                StatusCode::INTERNAL_SERVER_ERROR.into_response()
            }
        }
    } else {
        let mut sink = TrinoHttpResultSink::new(&query_id.0);
        if let Err(e) = execute_to_sink(&state, sql, session, protocol, group, &mut sink).await {
            warn!(id = %query_id, "execute_to_sink error: {e}");
        }
        sink.into_response()
    }
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

    if state.group_supports_async(&group.0) {
        match dispatch_query(&state, query_id.clone(), sql, session, protocol, group, true, seq).await {
            Ok(outcome) => outcome_to_response(&state, &query_id, outcome),
            Err(e) => {
                warn!("Dispatch error: {e}");
                StatusCode::INTERNAL_SERVER_ERROR.into_response()
            }
        }
    } else {
        let _ = state.persistence.delete_queued(&query_id).await;
        let mut sink = TrinoHttpResultSink::new(&query_id.0);
        if let Err(e) = execute_to_sink(&state, sql, session, protocol, group, &mut sink).await {
            warn!(id = %query_id, "execute_to_sink error: {e}");
        }
        sink.into_response()
    }
}

/// GET /v1/statement/{*trino_path} — poll any Trino statement URL (queued or executing).
///
/// Trino's query lifecycle uses two path prefixes: `/v1/statement/queued/...` initially,
/// then `/v1/statement/executing/...` once running. Both are handled identically here.
///
/// The path is embedded verbatim in the client-facing URL. Any QueryFlux instance looks up
/// the stored `trino_endpoint` by trino_id (second path segment) and reconstructs the full
/// Trino URL — no persistence write needed between polls.
pub async fn get_executing_statement(
    State(state): State<Arc<AppState>>,
    Path(trino_path): Path<String>,
    headers: HeaderMap,
) -> impl IntoResponse {
    // trino_path = e.g. "queued/20260319_084733_00386_kqwci/1/token"
    //                 or "executing/20260319_084733_00386_kqwci/1/token"
    // Extract the Trino query ID (always the second segment).
    let trino_id = match trino_path.split('/').nth(1) {
        Some(id) => id.to_string(),
        None => return StatusCode::BAD_REQUEST.into_response(),
    };
    let backend_id = BackendQueryId(trino_id.clone());

    let executing = match state.persistence.get(&backend_id).await {
        Ok(Some(q)) => q,
        Ok(None) => return StatusCode::NOT_FOUND.into_response(),
        Err(e) => {
            warn!("Persistence error: {e}");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };

    let adapter = match state.adapter(&executing.cluster_name.0) {
        Some(a) => a,
        None => {
            warn!("No adapter for cluster {}/{}", executing.cluster_group, executing.cluster_name);
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };

    // Reconstruct the full Trino poll URL: stored endpoint + /v1/statement/ + captured path.
    let trino_url = format!(
        "{}/v1/statement/{}",
        executing.trino_endpoint.trim_end_matches('/'),
        trino_path
    );

    // Forward session headers to Trino.
    let session = extract_session(&headers);

    // Throttled last_accessed refresh: write to persistence at most every 120s per query.
    // This keeps the record "alive" for the zombie-cleanup task across all proxy instances,
    // without adding a persistence write on every poll.
    const LAST_ACCESSED_UPDATE_INTERVAL: i64 = 120;
    let now = Utc::now();
    if (now - executing.last_accessed).num_seconds() >= LAST_ACCESSED_UPDATE_INTERVAL {
        let mut refreshed = executing.clone();
        refreshed.last_accessed = now;
        let _ = state.persistence.upsert(refreshed).await;
    }

    let poll_result = match adapter.poll_query(&backend_id, Some(&trino_url)).await {
        Ok(r) => r,
        Err(e) => {
            warn!("Poll error: {e}");
            state.metrics.on_query_finished(&executing.cluster_group.0, &executing.cluster_name.0);
            let _ = state.cluster_manager.release_cluster(&executing.cluster_group, &executing.cluster_name).await;
            let _ = state.persistence.delete(&backend_id).await;
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };

    let elapsed_ms = (Utc::now() - executing.creation_time).num_milliseconds().max(0) as u64;

    match poll_result {
        QueryPollResult::Raw { body, next_uri, engine_stats } => {
            if next_uri.is_none() {
                // Final page — query complete.
                state.record_query(
                    &executing.id,
                    Some(backend_id.0.clone()),
                    // sql_preview: original SQL (translated_sql field holds the pre-translation original)
                    executing.translated_sql.as_deref().unwrap_or(&executing.sql),
                    &session,
                    &FrontendProtocol::TrinoHttp,
                    &executing.cluster_group,
                    &executing.cluster_name,
                    adapter.engine_type(),
                    FrontendProtocol::TrinoHttp.default_dialect(),
                    adapter.engine_type().dialect(),
                    executing.translated_sql.is_some(),
                    if executing.translated_sql.is_some() { Some(executing.sql.clone()) } else { None },
                    QueryStatus::Success,
                    elapsed_ms,
                    None,
                    None,
                    None,
                    engine_stats,
                );
                state.metrics.on_query_finished(&executing.cluster_group.0, &executing.cluster_name.0);
                let _ = state.cluster_manager.release_cluster(&executing.cluster_group, &executing.cluster_name).await;
                let _ = state.persistence.delete(&backend_id).await;
                return raw_response_with_rewritten_next_uri(body, None).into_response();
            }

            // Intermediate page — rewrite nextUri (swap Trino host → QueryFlux), no persistence write.
            let proxy_next_uri = next_uri.as_deref().map(|uri| {
                rewrite_trino_uri(uri, &state.external_address)
            });
            raw_response_with_rewritten_next_uri(body, proxy_next_uri).into_response()
        }

        QueryPollResult::Failed { message, .. } => {
            state.metrics.on_query_finished(&executing.cluster_group.0, &executing.cluster_name.0);
            state.record_query(
                &executing.id,
                Some(backend_id.0.clone()),
                executing.translated_sql.as_deref().unwrap_or(&executing.sql),
                &session,
                &FrontendProtocol::TrinoHttp,
                &executing.cluster_group,
                &executing.cluster_name,
                adapter.engine_type(),
                FrontendProtocol::TrinoHttp.default_dialect(),
                adapter.engine_type().dialect(),
                executing.translated_sql.is_some(),
                if executing.translated_sql.is_some() { Some(executing.sql.clone()) } else { None },
                QueryStatus::Failed,
                elapsed_ms,
                None,
                Some(message.clone()),
                None,
                None,
            );
            let _ = state.cluster_manager.release_cluster(&executing.cluster_group, &executing.cluster_name).await;
            warn!(id = %executing.id, "Query failed: {message}");
            let _ = state.persistence.delete(&backend_id).await;
            let error_resp = TrinoResponse {
                id: executing.id.0.clone(),
                next_uri: None,
                info_uri: format!("{}/ui/query.html", state.external_address),
                partial_cancel_uri: None,
                stats: TrinoStats {
                    state: "FAILED".to_string(),
                    queued: false,
                    scheduled: false,
                    elapsed_time_millis: elapsed_ms,
                    ..Default::default()
                },
                error: Some(TrinoError {
                    message: message.clone(),
                    error_code: Some(0),
                    error_name: Some("QUERY_FAILED".to_string()),
                    error_type: Some("USER_ERROR".to_string()),
                    failure_info: None,
                }),
                columns: None,
                data: None,
                update_type: None,
                update_count: None,
                warnings: vec![],
            };
            json_response(&error_resp).into_response()
        }

        QueryPollResult::Pending { next_uri, .. } => {
            // Still running — rewrite nextUri, no persistence write needed.
            let proxy_next_uri = next_uri.as_deref()
                .map(|uri| rewrite_trino_uri(uri, &state.external_address))
                .unwrap_or_else(|| format!("{}/v1/statement/{}", state.external_address, trino_path));
            let resp = queued_response(&executing.id.0, 0, proxy_next_uri);
            json_response(&resp).into_response()
        }

    }
}

/// DELETE /v1/statement/{*trino_path} — cancel a running query.
pub async fn delete_executing_statement(
    State(state): State<Arc<AppState>>,
    Path(trino_path): Path<String>,
) -> impl IntoResponse {
    let trino_id = match trino_path.split('/').nth(1) {
        Some(id) => id.to_string(),
        None => return StatusCode::NO_CONTENT.into_response(),
    };
    let backend_id = BackendQueryId(trino_id);

    if let Ok(Some(executing)) = state.persistence.get(&backend_id).await {
        let trino_url = format!(
            "{}/v1/statement/{}",
            executing.trino_endpoint.trim_end_matches('/'),
            trino_path
        );
        let client = reqwest::Client::new();
        let _ = client.delete(&trino_url).send().await;

        state.metrics.on_query_finished(&executing.cluster_group.0, &executing.cluster_name.0);
        let _ = state.cluster_manager.release_cluster(&executing.cluster_group, &executing.cluster_name).await;
        let _ = state.persistence.delete(&backend_id).await;
    }

    StatusCode::NO_CONTENT.into_response()
}
