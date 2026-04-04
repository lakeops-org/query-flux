use std::collections::HashMap;
use std::sync::Arc;

use chrono::Utc;
use queryflux_auth::{AuthProvider, AuthorizationChecker, BackendIdentityResolver};
use queryflux_cluster_manager::{cluster_state::ClusterState, ClusterGroupManager};
use queryflux_core::{
    config::ClusterConfig,
    query::{
        ClusterGroupName, ClusterName, EngineType, FrontendProtocol, ProxyQueryId,
        QueryEngineStats, QueryStatus, SqlDialect,
    },
    session::SessionContext,
    tags::QueryTags,
};
use queryflux_engine_adapters::EngineAdapterTrait;
use queryflux_metrics::{MetricsStore, QueryRecord};
use queryflux_persistence::Persistence;
use queryflux_routing::chain::{RouterChain, RoutingTrace};
use queryflux_translation::TranslationService;

use crate::snowflake::http::session_store::SnowflakeSessionStore;

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
    /// group_name → default tags configured on the group.
    /// Merged with session tags at dispatch time; session tags win on key conflicts.
    pub group_default_tags: HashMap<String, QueryTags>,
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
    /// Active Snowflake **HTTP wire** sessions (Snowflake connector “Form 1”), keyed by the
    /// token issued at login.
    ///
    /// **Process-local only** — not shared across QueryFlux replicas. Multi-instance deployments
    /// must use load-balancer **session affinity** (sticky routing) so login and all follow-up
    /// requests hit the same instance, or sessions will fail with “not found”. Rolling restarts
    /// drop in-memory sessions unless clients reconnect. See `queryflux.enforceSnowflakeHttpSessionAffinity`
    /// and `frontends.snowflakeHttp.sessionAffinityAcknowledged` in config to assert affinity is configured.
    ///
    /// (Shared persistence for these sessions is not implemented yet.)
    ///
    /// Session lifetime is enforced in-process via `SnowflakeSessionStore::validate_snowflake_session`
    /// (max age + idle timeout; see `frontends.snowflakeHttp.snowflakeSessionMaxAgeSecs` /
    /// `snowflakeSessionIdleTimeoutSecs` in YAML).
    pub snowflake_sessions: Arc<SnowflakeSessionStore>,
}

/// Stable per-query metadata that does not change across the query's lifecycle.
/// Built once (after cluster selection and SQL translation) and passed to every
/// `record_query` call within the same dispatch function.
pub struct QueryContext<'a> {
    pub query_id: &'a ProxyQueryId,
    /// Original SQL as submitted by the client (pre-translation).
    pub sql: &'a str,
    pub session: &'a SessionContext,
    pub protocol: FrontendProtocol,
    pub group: &'a ClusterGroupName,
    pub cluster: &'a ClusterName,
    pub cluster_group_config_id: Option<i64>,
    pub cluster_config_id: Option<i64>,
    pub engine_type: EngineType,
    pub src_dialect: SqlDialect,
    pub tgt_dialect: SqlDialect,
    pub was_translated: bool,
    /// The translated SQL sent to the backend, when translation occurred.
    pub translated_sql: Option<String>,
    pub query_tags: QueryTags,
}

/// How the query ended — the fields that vary between success, failure, and cancellation.
pub struct QueryOutcome {
    /// Backend engine query ID (Trino query ID, Athena execution ID, etc.).
    pub backend_query_id: Option<String>,
    pub status: QueryStatus,
    pub execution_ms: u64,
    pub rows: Option<u64>,
    pub error: Option<String>,
    pub routing_trace: Option<RoutingTrace>,
    pub engine_stats: Option<QueryEngineStats>,
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
    pub fn record_query(&self, ctx: &QueryContext<'_>, outcome: QueryOutcome) {
        let record = QueryRecord {
            proxy_query_id: ctx.query_id.0.clone(),
            backend_query_id: outcome.backend_query_id,
            cluster_group: ctx.group.clone(),
            cluster_name: ctx.cluster.clone(),
            cluster_group_config_id: ctx.cluster_group_config_id,
            cluster_config_id: ctx.cluster_config_id,
            engine_type: ctx.engine_type.clone(),
            frontend_protocol: ctx.protocol.clone(),
            source_dialect: ctx.src_dialect.clone(),
            target_dialect: ctx.tgt_dialect.clone(),
            was_translated: ctx.was_translated,
            translated_sql: ctx.translated_sql.clone(),
            user: ctx.session.user().map(|s| s.to_string()),
            catalog: ctx.session.database().map(|s| s.to_string()),
            database: None,
            sql_preview: ctx.sql.chars().take(500).collect(),
            status: outcome.status,
            routing_trace: outcome
                .routing_trace
                .as_ref()
                .and_then(|t| serde_json::to_value(t).ok()),
            queue_duration_ms: 0,
            execution_duration_ms: outcome.execution_ms,
            rows_returned: outcome.rows,
            error_message: outcome.error,
            created_at: Utc::now(),
            engine_stats: outcome.engine_stats,
            query_tags: ctx.query_tags.clone(),
        };
        let metrics = self.metrics.clone();
        tokio::spawn(async move {
            let _ = metrics.record_query(record).await;
        });
    }
}
