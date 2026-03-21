use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::Value;

use queryflux_core::{
    error::Result,
    query::{
        ClusterGroupName, ClusterName, EngineType, FrontendProtocol, QueryEngineStats,
        QueryStatus, SqlDialect,
    },
};

/// A record of one completed (or failed/cancelled) query execution.
/// Written to the metrics store at the end of every query, regardless of outcome.
#[derive(Debug, Clone)]
pub struct QueryRecord {
    pub proxy_query_id: String,
    /// The query ID assigned by the backend engine (e.g. Trino's `20240319_123456_00001_xxxxx`).
    pub backend_query_id: Option<String>,
    pub cluster_group: ClusterGroupName,
    pub cluster_name: ClusterName,
    pub engine_type: EngineType,
    pub frontend_protocol: FrontendProtocol,
    pub source_dialect: SqlDialect,
    pub target_dialect: SqlDialect,
    pub was_translated: bool,
    /// The SQL after dialect translation. Only set when `was_translated` is true.
    pub translated_sql: Option<String>,
    pub user: Option<String>,
    pub catalog: Option<String>,
    pub database: Option<String>,
    /// First 500 chars of the original SQL.
    pub sql_preview: String,
    pub status: QueryStatus,
    /// Full routing trace serialized as JSON. Stored in the `routing_trace` JSONB column.
    pub routing_trace: Option<Value>,
    pub queue_duration_ms: u64,
    pub execution_duration_ms: u64,
    pub rows_returned: Option<u64>,
    pub error_message: Option<String>,
    pub created_at: DateTime<Utc>,
    /// Engine-reported execution stats (CPU, bytes scanned, memory, etc.).
    pub engine_stats: Option<QueryEngineStats>,
}

/// A periodic snapshot of one cluster's live utilization.
#[derive(Debug, Clone)]
pub struct ClusterSnapshot {
    pub cluster_name: ClusterName,
    pub group_name: ClusterGroupName,
    pub engine_type: EngineType,
    pub running_queries: u64,
    pub queued_queries: u64,
    pub max_running_queries: u64,
    pub recorded_at: DateTime<Utc>,
}

/// Write side of the metrics pipeline — records completed queries and cluster snapshots
/// for later display in the admin Studio UI.
///
/// Prometheus handles real-time alerting; this trait handles historical persistence.
/// Any persistence backend that wants to power the query history page must implement this.
#[async_trait]
pub trait MetricsStore: Send + Sync {
    async fn record_query(&self, record: QueryRecord) -> Result<()>;
    async fn record_cluster_snapshot(&self, snapshot: ClusterSnapshot) -> Result<()>;

    /// Called synchronously when a cluster slot is acquired (query starts executing).
    /// Used to maintain real-time running-query gauges in Prometheus.
    fn on_query_started(&self, _group: &str, _cluster: &str) {}

    /// Called synchronously when a cluster slot is released (query finished or failed).
    fn on_query_finished(&self, _group: &str, _cluster: &str) {}
}
