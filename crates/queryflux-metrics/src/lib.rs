use async_trait::async_trait;
use chrono::{DateTime, Utc};
use queryflux_core::{
    error::Result,
    query::{ClusterGroupName, ClusterName, EngineType, FrontendProtocol, QueryStatus, SqlDialect},
};

/// A record of one completed (or failed/cancelled) query execution.
/// Written to the metrics store at the end of every query, regardless of outcome.
#[derive(Debug, Clone)]
pub struct QueryRecord {
    pub proxy_query_id: String,
    pub cluster_group: ClusterGroupName,
    pub cluster_name: ClusterName,
    pub engine_type: EngineType,
    pub frontend_protocol: FrontendProtocol,
    pub source_dialect: SqlDialect,
    pub target_dialect: SqlDialect,
    pub was_translated: bool,
    pub user: Option<String>,
    pub catalog: Option<String>,
    pub database: Option<String>,
    /// First 500 chars of the original SQL.
    pub sql_preview: String,
    pub status: QueryStatus,
    pub queue_duration_ms: u64,
    pub execution_duration_ms: u64,
    pub rows_returned: Option<u64>,
    pub error_message: Option<String>,
    pub created_at: DateTime<Utc>,
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

/// Stores per-query and per-cluster metrics for later display in the management UI.
///
/// Prometheus handles real-time operational alerting.
/// This store handles historical data for query history, latency trends, and dashboards.
#[async_trait]
pub trait MetricsStore: Send + Sync {
    async fn record_query(&self, record: QueryRecord) -> Result<()>;
    async fn record_cluster_snapshot(&self, snapshot: ClusterSnapshot) -> Result<()>;
}

/// Discards all metrics — default for deployments that don't need the UI.
pub struct NoopMetricsStore;

#[async_trait]
impl MetricsStore for NoopMetricsStore {
    async fn record_query(&self, _record: QueryRecord) -> Result<()> {
        Ok(())
    }
    async fn record_cluster_snapshot(&self, _snapshot: ClusterSnapshot) -> Result<()> {
        Ok(())
    }
}
