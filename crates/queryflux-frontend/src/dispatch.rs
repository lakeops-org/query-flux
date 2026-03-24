use std::sync::Arc;
use std::time::Instant;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use bytes::Bytes;
use chrono::Utc;
use futures::StreamExt;
use queryflux_auth::AuthContext;
use queryflux_cluster_manager::ClusterGroupManager;
use queryflux_core::{
    error::{QueryFluxError, Result},
    query::{
        ClusterGroupName, ClusterName, ExecutingQuery, FrontendProtocol, ProxyQueryId,
        QueryExecution, QueryStats, QueryStatus, QueuedQuery,
    },
    session::SessionContext,
};
use queryflux_translation::SchemaContext;
use tracing::{info, warn};

use crate::state::AppState;

// ---------------------------------------------------------------------------
// ResultSink — universal streaming output interface
// ---------------------------------------------------------------------------

/// Implemented by each frontend protocol to receive query results.
///
/// `execute_to_sink` calls these in order:
///   on_schema (once) → on_batch (N times) → on_complete (once)
///   or on_error (once on failure).
///
/// Text-protocol sinks (MySQL, Postgres) format values as strings.
/// Arrow-native sinks (Flight SQL) pass RecordBatch through without inspection.
#[async_trait]
pub trait ResultSink: Send {
    async fn on_schema(&mut self, schema: &Schema) -> Result<()>;
    async fn on_batch(&mut self, batch: &RecordBatch) -> Result<()>;
    async fn on_complete(&mut self, stats: &QueryStats) -> Result<()>;
    async fn on_error(&mut self, message: &str) -> Result<()>;
}

/// Protocol-agnostic result of dispatching a query to an async (Trino) backend.
pub enum DispatchOutcome {
    /// No cluster capacity available — query was queued. Client should poll `queued_next_uri`.
    Queued { queued_next_uri: String },
    /// Query submitted to Trino; executing state stored in persistence.
    /// Client should poll `proxy_next_uri`. `initial_body` may contain the first response page.
    Async {
        initial_body: Option<Bytes>,
        proxy_next_uri: Option<String>,
    },
}

/// Rewrite a Trino-origin URL to point to QueryFlux instead, keeping the full path.
/// `http://trino:8080/v1/statement/executing/{id}/{token}` →
/// `http://queryflux:9000/v1/statement/executing/{id}/{token}`
///
/// Any instance can then reconstruct the Trino URL by looking up the stored
/// `trino_endpoint` and re-joining it with the path.
async fn cluster_db_ids(
    mgr: &std::sync::Arc<dyn ClusterGroupManager>,
    group: &ClusterGroupName,
    cluster: &ClusterName,
) -> (Option<i64>, Option<i64>) {
    match mgr.cluster_state(group, cluster).await {
        Ok(Some(s)) => (s.cluster_group_config_id, s.cluster_config_id),
        _ => (None, None),
    }
}

pub fn rewrite_trino_uri(trino_uri: &str, external_address: &str) -> String {
    // Find the path portion starting at /v1/
    if let Some(path_start) = trino_uri.find("/v1/") {
        format!(
            "{}{}",
            external_address.trim_end_matches('/'),
            &trino_uri[path_start..]
        )
    } else {
        trino_uri.to_string()
    }
}

/// Core dispatch logic shared across all frontend protocol implementations.
#[allow(clippy::too_many_arguments)]
pub async fn dispatch_query(
    state: &Arc<AppState>,
    query_id: ProxyQueryId,
    sql: String,
    session: SessionContext,
    protocol: FrontendProtocol,
    group: ClusterGroupName,
    already_queued: bool,
    sequence: u64,
    auth_ctx: &AuthContext,
) -> Result<DispatchOutcome> {
    // Authorization check — first gate before any resource acquisition.
    // Phase 1: AllowAllAuthorization always returns true (no behavior change).
    if !state.authorization.check(auth_ctx, &group.0).await {
        return Err(QueryFluxError::Unauthorized(format!(
            "user '{}' is not authorized to run queries on cluster group '{}'",
            auth_ctx.user, group.0
        )));
    }

    // Clone the manager and group translation fixups from one lock snapshot.
    let (cluster_manager, group_fixups) = {
        let live = state.live.read().await;
        (
            live.cluster_manager.clone(),
            live.group_translation_scripts
                .get(&group.0)
                .cloned()
                .unwrap_or_default(),
        )
    };

    let cluster_name = match cluster_manager.acquire_cluster(&group).await? {
        Some(c) => c,
        None => {
            let uri = persist_queued_query(
                state,
                query_id,
                sql,
                session,
                protocol,
                group,
                already_queued,
                sequence,
            )
            .await?;
            return Ok(DispatchOutcome::Queued {
                queued_next_uri: uri,
            });
        }
    };

    let (cluster_group_config_id, cluster_config_id) =
        cluster_db_ids(&cluster_manager, &group, &cluster_name).await;

    state.metrics.on_query_started(&group.0, &cluster_name.0);

    let cluster_cfg = state.cluster_config_cloned(&cluster_name.0).await;
    let credentials = state
        .identity_resolver
        .resolve(auth_ctx, cluster_cfg.as_ref())
        .await;

    let adapter = match state.adapter(&cluster_name.0).await {
        Some(a) => a,
        None => {
            state.metrics.on_query_finished(&group.0, &cluster_name.0);
            let _ = cluster_manager.release_cluster(&group, &cluster_name).await;
            return Err(QueryFluxError::Engine(format!(
                "No adapter for {group}/{cluster_name}"
            )));
        }
    };

    // The group-level `group_supports_async` check is a heuristic that can be wrong for
    // mixed-engine groups or stale DB state. Guard here so a sync cluster acquired via
    // round-robin doesn't reach `submit_query` and fail at runtime.
    if !adapter.supports_async() {
        state.metrics.on_query_finished(&group.0, &cluster_name.0);
        let _ = cluster_manager.release_cluster(&group, &cluster_name).await;
        return Err(QueryFluxError::SyncEngineRequired(cluster_name.0.clone()));
    }

    let src_dialect = protocol.default_dialect();
    let tgt_dialect = adapter.engine_type().dialect();
    let original_sql = sql.clone();
    let sql = match state
        .translation
        .maybe_translate(
            &sql,
            &src_dialect,
            &tgt_dialect,
            &SchemaContext::default(),
            &group_fixups,
        )
        .await
    {
        Ok(t) => t,
        Err(e) => {
            warn!(id = %query_id, "Translation error: {e}");
            state.metrics.on_query_finished(&group.0, &cluster_name.0);
            let _ = cluster_manager.release_cluster(&group, &cluster_name).await;
            return Err(e);
        }
    };
    let was_translated = sql != original_sql;
    if was_translated {
        info!(id = %query_id, src = ?src_dialect, tgt = ?tgt_dialect, "SQL translated");
    }

    let execution = match adapter.submit_query(&sql, &session, &credentials).await {
        Ok(e) => e,
        Err(e) => {
            state.metrics.on_query_finished(&group.0, &cluster_name.0);
            let _ = cluster_manager.release_cluster(&group, &cluster_name).await;
            warn!(id = %query_id, "Submit error: {e}");
            return Err(e);
        }
    };

    if already_queued {
        // Delete synchronously before marking as executing — prevents re-dispatch on restart.
        let _ = state.persistence.delete_queued(&query_id).await;
    }

    let QueryExecution::Async {
        backend_query_id,
        next_uri,
        initial_body,
    } = execution;
    let now = Utc::now();
    let executing = ExecutingQuery {
        id: query_id.clone(),
        sql,
        translated_sql: if was_translated {
            Some(original_sql)
        } else {
            None
        },
        cluster_group: group.clone(),
        cluster_name: cluster_name.clone(),
        cluster_group_config_id,
        cluster_config_id,
        backend_query_id: backend_query_id.clone(),
        trino_endpoint: adapter.base_url().to_string(),
        creation_time: now,
        last_accessed: now,
    };
    // Single write per query — no updates needed between polls.
    // Any QueryFlux instance can serve subsequent polls using this record.
    let _ = state.persistence.upsert(executing).await;
    info!(id = %query_id, backend = %backend_query_id, cluster = %cluster_name, "Query submitted (async)");

    // Rewrite nextUri: swap Trino host → QueryFlux external address, keep full path.
    let proxy_next_uri = next_uri
        .as_deref()
        .map(|uri| rewrite_trino_uri(uri, &state.external_address));
    Ok(DispatchOutcome::Async {
        initial_body,
        proxy_next_uri,
    })
}

#[allow(clippy::too_many_arguments)]
async fn persist_queued_query(
    state: &Arc<AppState>,
    query_id: ProxyQueryId,
    sql: String,
    session: SessionContext,
    protocol: FrontendProtocol,
    group: ClusterGroupName,
    _already_stored: bool,
    sequence: u64,
) -> Result<String> {
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
    let _ = state.persistence.upsert_queued(queued).await;
    let next_seq = sequence + 1;
    Ok(format!(
        "{}/v1/statement/qf/queued/{}/{}",
        state.external_address, query_id, next_seq
    ))
}

// ---------------------------------------------------------------------------
// execute_to_sink — shared Arrow execution driver for non-Trino-HTTP frontends
// ---------------------------------------------------------------------------

/// How long to wait between queue retries (exponential backoff, capped at 2s).
async fn queued_backoff_delay(seq: u64) {
    let ms = (100u64 * (1 << seq.min(4))).min(2000);
    tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
}

/// Execute a query against any backend and stream RecordBatches to `sink`.
///
/// Used by all non-Trino-HTTP frontends (MySQL wire, Postgres wire, Flight SQL).
/// The Trino HTTP frontend keeps its raw-bytes passthrough path unchanged.
///
/// - Waits for cluster capacity with exponential backoff (TCP connection stays open).
/// - Translates SQL dialect (frontend → backend).
/// - Calls `adapter.execute_as_arrow()` — each adapter owns its type mapping.
/// - Feeds the RecordBatch stream to `sink` with O(1 batch) memory.
pub async fn execute_to_sink(
    state: &Arc<AppState>,
    sql: String,
    session: SessionContext,
    protocol: FrontendProtocol,
    group: ClusterGroupName,
    sink: &mut impl ResultSink,
    auth_ctx: &AuthContext,
) -> Result<()> {
    // Authorization check.
    if !state.authorization.check(auth_ctx, &group.0).await {
        return Err(QueryFluxError::Unauthorized(format!(
            "user '{}' is not authorized to run queries on cluster group '{}'",
            auth_ctx.user, group.0
        )));
    }
    let query_id = ProxyQueryId::new();
    // 1. Queue loop: wait for cluster capacity.
    // Clone the manager out of the lock before entering the loop so we don't hold
    // the read lock across any of the I/O-bound await points inside.
    let (cluster_manager, group_fixups) = {
        let live = state.live.read().await;
        (
            live.cluster_manager.clone(),
            live.group_translation_scripts
                .get(&group.0)
                .cloned()
                .unwrap_or_default(),
        )
    };
    let mut seq: u64 = 0;
    let (cluster_name, adapter) = loop {
        match cluster_manager.acquire_cluster(&group).await? {
            Some(name) => match state.adapter(&name.0).await {
                Some(a) => break (name, a),
                None => {
                    let msg = format!("No adapter for {group}/{name}");
                    return sink.on_error(&msg).await;
                }
            },
            None => {
                queued_backoff_delay(seq).await;
                seq += 1;
            }
        }
    };
    let (cluster_group_config_id, cluster_config_id) =
        cluster_db_ids(&cluster_manager, &group, &cluster_name).await;
    info!(id = %query_id, group = %group, cluster = %cluster_name, "Query executing (sync)");

    // 2. Translate SQL dialect.
    let src_dialect = protocol.default_dialect();
    let tgt_dialect = adapter.engine_type().dialect();
    let engine_type = adapter.engine_type();
    let original_sql = sql.clone();
    let start = Instant::now();
    let translated = match state
        .translation
        .maybe_translate(
            &sql,
            &src_dialect,
            &tgt_dialect,
            &SchemaContext::default(),
            &group_fixups,
        )
        .await
    {
        Ok(t) => t,
        Err(e) => {
            let _ = cluster_manager.release_cluster(&group, &cluster_name).await;
            let elapsed_ms = start.elapsed().as_millis() as u64;
            let err_msg = e.to_string();
            state.record_query(
                &query_id,
                None,
                &original_sql,
                &session,
                &protocol,
                &group,
                &cluster_name,
                cluster_group_config_id,
                cluster_config_id,
                engine_type,
                src_dialect.clone(),
                tgt_dialect.clone(),
                false,
                None,
                QueryStatus::Failed,
                elapsed_ms,
                None,
                Some(err_msg.clone()),
                None,
                None,
            );
            return sink.on_error(&err_msg).await;
        }
    };
    let was_translated = translated != original_sql;

    let cluster_cfg = state.cluster_config_cloned(&cluster_name.0).await;
    let credentials = state
        .identity_resolver
        .resolve(auth_ctx, cluster_cfg.as_ref())
        .await;

    // 3. Execute as Arrow stream.
    let mut stream = match adapter
        .execute_as_arrow(&translated, &session, &credentials)
        .await
    {
        Ok(s) => s,
        Err(e) => {
            let _ = cluster_manager.release_cluster(&group, &cluster_name).await;
            let elapsed_ms = start.elapsed().as_millis() as u64;
            let err_msg = e.to_string();
            state.record_query(
                &query_id,
                None,
                &original_sql,
                &session,
                &protocol,
                &group,
                &cluster_name,
                cluster_group_config_id,
                cluster_config_id,
                engine_type,
                src_dialect.clone(),
                tgt_dialect.clone(),
                was_translated,
                if was_translated {
                    Some(translated.clone())
                } else {
                    None
                },
                QueryStatus::Failed,
                elapsed_ms,
                None,
                Some(err_msg.clone()),
                None,
                None,
            );
            return sink.on_error(&err_msg).await;
        }
    };

    // 4. Feed stream to sink — O(1 batch) memory.
    let mut schema_sent = false;
    let mut rows_returned = 0u64;
    while let Some(result) = stream.next().await {
        match result {
            Err(e) => {
                let stream_error = Some(e.to_string());
                let _ = cluster_manager.release_cluster(&group, &cluster_name).await;
                let elapsed_ms = start.elapsed().as_millis() as u64;
                state.record_query(
                    &query_id,
                    None,
                    &original_sql,
                    &session,
                    &protocol,
                    &group,
                    &cluster_name,
                    cluster_group_config_id,
                    cluster_config_id,
                    engine_type,
                    src_dialect.clone(),
                    tgt_dialect.clone(),
                    was_translated,
                    if was_translated {
                        Some(translated.clone())
                    } else {
                        None
                    },
                    QueryStatus::Failed,
                    elapsed_ms,
                    None,
                    stream_error.clone(),
                    None,
                    None,
                );
                return sink.on_error(stream_error.as_deref().unwrap()).await;
            }
            Ok(batch) => {
                if !schema_sent {
                    if let Err(e) = sink.on_schema(batch.schema_ref()).await {
                        let _ = cluster_manager.release_cluster(&group, &cluster_name).await;
                        return Err(e);
                    }
                    schema_sent = true;
                }
                rows_returned += batch.num_rows() as u64;
                if let Err(e) = sink.on_batch(&batch).await {
                    let _ = cluster_manager.release_cluster(&group, &cluster_name).await;
                    return Err(e);
                }
            }
        }
    }

    let _ = cluster_manager.release_cluster(&group, &cluster_name).await;

    let elapsed_ms = start.elapsed().as_millis() as u64;
    let stats = QueryStats {
        execution_duration_ms: elapsed_ms,
        rows_returned,
        ..Default::default()
    };

    state.record_query(
        &query_id,
        None,
        &original_sql,
        &session,
        &protocol,
        &group,
        &cluster_name,
        cluster_group_config_id,
        cluster_config_id,
        engine_type,
        src_dialect,
        tgt_dialect,
        was_translated,
        if was_translated {
            Some(translated)
        } else {
            None
        },
        QueryStatus::Success,
        elapsed_ms,
        Some(rows_returned),
        None,
        None,
        None,
    );

    // If no batches arrived (empty result), still send an empty schema if we have nothing.
    if !schema_sent {
        let empty_schema = Schema::empty();
        sink.on_schema(&empty_schema).await?;
    }

    sink.on_complete(&stats).await
}
