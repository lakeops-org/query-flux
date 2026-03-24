use std::collections::HashMap;
use std::sync::Arc;

use chrono::Utc;
use queryflux_auth::{AuthorizationChecker, AuthProvider, BackendIdentityResolver};
use queryflux_cluster_manager::{cluster_state::ClusterState, ClusterGroupManager};
use queryflux_core::{
    config::ClusterConfig,
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

/// Everything that can be hot-reloaded from the DB without restarting the proxy.
///
/// Wrapped in `Arc<tokio::sync::RwLock<LiveConfig>>` inside `AppState` so
/// that any handler can cheaply read a consistent snapshot, and a background
/// task can atomically swap the whole bundle on each reload tick.
pub struct LiveConfig {
    pub router_chain: RouterChain,
    pub cluster_manager: Arc<dyn ClusterGroupManager>,
    /// cluster_name → adapter (one adapter per physical cluster, shared across groups).
    pub adapters: HashMap<String, Arc<dyn EngineAdapterTrait>>,
    /// One `(adapter, ClusterState)` per physical cluster (first group membership wins).
    /// Used by background health / reconcile tasks so they track the **current** reload generation.
    pub health_check_targets: Vec<(Arc<dyn EngineAdapterTrait>, Arc<ClusterState>)>,
    /// Cluster configs keyed by cluster name — used by `BackendIdentityResolver` to
    /// look up `queryAuth` after a cluster is selected.
    pub cluster_configs: HashMap<String, ClusterConfig>,
    /// group_name → ordered list of cluster names in that group.
    pub group_members: HashMap<String, Vec<String>>,
    /// Ordered list of group names as they appear in config — used for authorization-aware
    /// first-fit when the router chain falls back to the static default.
    pub group_order: Vec<String>,
    /// group_name → ordered post-sqlglot Python fixup bodies (from `user_scripts` + group link).
    pub group_translation_scripts: HashMap<String, Vec<String>>,
}

/// Shared application state — passed to every handler via `axum::extract::State`.
/// Shared across all frontend protocol implementations (Trino HTTP, PG wire, etc.).
pub struct AppState {
    /// The external URL clients use to reach QueryFlux (used for nextUri rewriting).
    pub external_address: String,
    /// Hot-reloadable: routing rules + cluster registry.
    pub live: Arc<tokio::sync::RwLock<LiveConfig>>,
    // Static (never reloaded):
    pub persistence: Arc<dyn Persistence>,
    pub translation: Arc<TranslationService>,
    pub metrics: Arc<dyn MetricsStore>,
    /// Verifies client identity. Phase 1: `NoneAuthProvider` (network-trust, no crypto).
    pub auth_provider: Arc<dyn AuthProvider>,
    /// Checks whether an authenticated user may access a cluster group.
    /// Phase 1: `AllowAllAuthorization` (permit everything, today's behavior).
    pub authorization: Arc<dyn AuthorizationChecker>,
    /// Resolves per-user `QueryCredentials` from `AuthContext` + cluster `queryAuth` config.
    pub identity_resolver: Arc<BackendIdentityResolver>,
}

impl AppState {
    pub async fn adapter(&self, cluster: &str) -> Option<Arc<dyn EngineAdapterTrait>> {
        self.live.read().await.adapters.get(cluster).cloned()
    }

    pub async fn cluster_config_cloned(&self, cluster: &str) -> Option<ClusterConfig> {
        self.live.read().await.cluster_configs.get(cluster).cloned()
    }

    /// Returns true if any cluster in the group supports async execution (e.g. Trino).
    pub async fn group_supports_async(&self, group: &str) -> bool {
        let live = self.live.read().await;
        live.group_members
            .get(group)
            .map(|members| {
                members.iter().any(|name| {
                    live.adapters
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
        cluster_group_config_id: Option<i64>,
        cluster_config_id: Option<i64>,
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
            cluster_group_config_id,
            cluster_config_id,
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
