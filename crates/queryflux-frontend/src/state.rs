use std::collections::HashMap;
use std::sync::Arc;

use chrono::Utc;
use queryflux_cluster_manager::ClusterGroupManager;
use queryflux_core::{
    query::{
        ClusterGroupName, ClusterName, EngineType, FrontendProtocol, ProxyQueryId,
        QueryEngineStats, QueryStatus, SqlDialect,
    },
    session::SessionContext,
};
use queryflux_engine_adapters::EngineAdapterTrait;
use queryflux_metrics::{MetricsStore, QueryRecord};
use queryflux_persistence::Persistence;
use queryflux_routing::chain::{RouterChain, RoutingTrace};
use queryflux_translation::TranslationService;

/// Shared application state — passed to every handler via `axum::extract::State`.
/// Shared across all frontend protocol implementations (Trino HTTP, PG wire, etc.).
pub struct AppState {
    /// The external URL clients use to reach QueryFlux (used for nextUri rewriting).
    pub external_address: String,
    pub cluster_manager: Arc<dyn ClusterGroupManager>,
    /// cluster_name → adapter (one adapter per physical cluster, shared across groups).
    pub adapters: HashMap<String, Arc<dyn EngineAdapterTrait>>,
    /// group_name → ordered list of cluster names in that group.
    pub group_members: HashMap<String, Vec<String>>,
    pub persistence: Arc<dyn Persistence>,
    pub router_chain: RouterChain,
    pub translation: Arc<TranslationService>,
    pub metrics: Arc<dyn MetricsStore>,
}

impl AppState {
    pub fn adapter(&self, cluster: &str) -> Option<Arc<dyn EngineAdapterTrait>> {
        self.adapters.get(cluster).cloned()
    }

    /// Returns true if any cluster in the group supports async execution (e.g. Trino).
    pub fn group_supports_async(&self, group: &str) -> bool {
        self.group_members
            .get(group)
            .map(|members| {
                members.iter().any(|name| {
                    self.adapters
                        .get(name)
                        .map(|a| a.supports_async())
                        .unwrap_or(false)
                })
            })
            .unwrap_or(false)
    }

    /// Fire-and-forget: build a `QueryRecord` and write it to the metrics store asynchronously.
    /// Called once per query at completion (success, failure, or cancellation).
    #[allow(clippy::too_many_arguments)]
    pub fn record_query(
        &self,
        query_id: &ProxyQueryId,
        backend_query_id: Option<String>,
        sql: &str,
        session: &SessionContext,
        protocol: &FrontendProtocol,
        group: &ClusterGroupName,
        cluster: &ClusterName,
        engine_type: EngineType,
        src_dialect: SqlDialect,
        tgt_dialect: SqlDialect,
        was_translated: bool,
        translated_sql: Option<String>,
        status: QueryStatus,
        execution_ms: u64,
        rows: Option<u64>,
        error: Option<String>,
        routing_trace: Option<&RoutingTrace>,
        engine_stats: Option<QueryEngineStats>,
    ) {
        let record = QueryRecord {
            proxy_query_id: query_id.0.clone(),
            backend_query_id,
            cluster_group: group.clone(),
            cluster_name: cluster.clone(),
            engine_type,
            frontend_protocol: protocol.clone(),
            source_dialect: src_dialect,
            target_dialect: tgt_dialect,
            was_translated,
            translated_sql,
            user: session.user().map(|s| s.to_string()),
            catalog: session.database().map(|s| s.to_string()),
            database: None,
            sql_preview: sql.chars().take(500).collect(),
            status,
            routing_trace: routing_trace.and_then(|t| serde_json::to_value(t).ok()),
            queue_duration_ms: 0,
            execution_duration_ms: execution_ms,
            rows_returned: rows,
            error_message: error,
            created_at: Utc::now(),
            engine_stats,
        };
        let metrics = self.metrics.clone();
        tokio::spawn(async move {
            let _ = metrics.record_query(record).await;
        });
    }
}
