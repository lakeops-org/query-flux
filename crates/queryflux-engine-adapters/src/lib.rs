pub mod adbc;
pub mod athena;
pub mod duckdb;
pub mod starrocks;
pub mod trino;

use std::pin::Pin;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use futures::Stream;
use queryflux_core::{
    config::ClusterConfig,
    engine_registry::EngineDescriptor,
    error::Result,
    query::{ClusterGroupName, ClusterName},
};

/// Implemented by each engine's typed config struct.
///
/// Parsing succeeds only when all required fields are present and valid for that engine —
/// construction IS validation. Engine-specific constraints (required fields, allowed auth types)
/// live here rather than in core.
pub trait EngineConfigParseable: Sized {
    /// Parse and validate from a DB config JSON blob.
    fn from_json(json: &serde_json::Value, cluster_name: &str) -> Result<Self>;
    /// Parse and validate from a YAML-loaded [`ClusterConfig`].
    fn from_cluster_config(cfg: &ClusterConfig, cluster_name: &str) -> Result<Self>;
}

/// A stream of Arrow RecordBatches — the universal output type for all adapters.
pub type ArrowStream = Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send>>;

/// Returned by `SyncAdapter::execute_as_arrow`.
///
/// Carries both the result stream and a post-completion stats channel.
/// Drive `stream` to exhaustion before reading `stats` — adapters send stats
/// into the oneshot only after all batches have been produced.
///
/// `stats` resolves to `None` when the engine does not expose structured
/// execution statistics (CPU time, bytes scanned, etc.).
pub struct SyncExecution {
    /// Arrow RecordBatch stream — drive to completion before reading stats.
    pub stream: ArrowStream,
    /// Engine-reported execution stats. Sent by the adapter once the stream ends.
    pub stats: tokio::sync::oneshot::Receiver<Option<queryflux_core::query::QueryEngineStats>>,
}

/// Sync engines: execute to completion, stream Arrow results.
/// Used by DuckDB (embedded + HTTP) and StarRocks.
#[async_trait]
pub trait SyncAdapter: Send + Sync {
    async fn execute_as_arrow(
        &self,
        sql: &str,
        session: &queryflux_core::session::SessionContext,
        credentials: &queryflux_auth::QueryCredentials,
        tags: &queryflux_core::tags::QueryTags,
    ) -> Result<SyncExecution>;
    fn engine_type(&self) -> queryflux_core::query::EngineType;
    /// Target dialect for SQL translation (may differ from `engine_type().dialect()`, e.g. Flight SQL + arbitrary sqlglot backend).
    fn translation_target_dialect(&self) -> queryflux_core::query::SqlDialect {
        self.engine_type().dialect()
    }
    async fn health_check(&self) -> bool;
    async fn fetch_running_query_count(&self) -> Option<u64> {
        None
    }
    async fn list_catalogs(&self) -> Result<Vec<String>>;
    async fn list_databases(&self, catalog: &str) -> Result<Vec<String>>;
    async fn list_tables(&self, catalog: &str, database: &str) -> Result<Vec<String>>;
    async fn describe_table(
        &self,
        catalog: &str,
        database: &str,
        table: &str,
    ) -> Result<Option<queryflux_core::catalog::TableSchema>>;
}

/// Async engines: submit-and-poll; lifecycle spans multiple HTTP requests.
/// Used by Trino and Athena.
#[async_trait]
pub trait AsyncAdapter: Send + Sync {
    async fn submit_query(
        &self,
        sql: &str,
        session: &queryflux_core::session::SessionContext,
        credentials: &queryflux_auth::QueryCredentials,
        tags: &queryflux_core::tags::QueryTags,
    ) -> Result<queryflux_core::query::QueryExecution>;
    async fn poll_query(
        &self,
        backend_id: &queryflux_core::query::BackendQueryId,
        next_uri: Option<&str>,
    ) -> Result<queryflux_core::query::QueryPollResult>;
    async fn cancel_query(&self, backend_id: &queryflux_core::query::BackendQueryId) -> Result<()>;
    fn engine_type(&self) -> queryflux_core::query::EngineType;
    fn translation_target_dialect(&self) -> queryflux_core::query::SqlDialect {
        self.engine_type().dialect()
    }
    fn base_url(&self) -> &str {
        ""
    }
    /// Execute a query synchronously by driving the internal submit+poll loop to completion.
    ///
    /// Enables MySQL/Postgres wire protocol clients to query async engines.
    /// Returns `Err(SyncEngineRequired)` by default — engines that support this path override it.
    async fn execute_as_arrow(
        &self,
        _sql: &str,
        _session: &queryflux_core::session::SessionContext,
        _credentials: &queryflux_auth::QueryCredentials,
        _tags: &queryflux_core::tags::QueryTags,
    ) -> Result<SyncExecution> {
        Err(queryflux_core::error::QueryFluxError::SyncEngineRequired(
            "this engine only supports the async (HTTP submit-poll) protocol".to_string(),
        ))
    }
    /// Extract engine-reported execution stats from a terminal submit-response body.
    ///
    /// Called by dispatch when the engine returns a terminal state on the initial POST
    /// (no `nextUri`). The body is the raw bytes returned by `submit_query`. Engines that
    /// embed stats in their response (e.g. Trino) override this; others return `None`.
    fn terminal_stats_from_body(
        &self,
        _body: &bytes::Bytes,
    ) -> Option<queryflux_core::query::QueryEngineStats> {
        None
    }
    async fn health_check(&self) -> bool;
    async fn fetch_running_query_count(&self) -> Option<u64> {
        None
    }
    async fn list_catalogs(&self) -> Result<Vec<String>>;
    async fn list_databases(&self, catalog: &str) -> Result<Vec<String>>;
    async fn list_tables(&self, catalog: &str, database: &str) -> Result<Vec<String>>;
    async fn describe_table(
        &self,
        catalog: &str,
        database: &str,
        table: &str,
    ) -> Result<Option<queryflux_core::catalog::TableSchema>>;
}

/// Type-safe adapter discriminant — replaces the `supports_async()` runtime flag.
///
/// Dispatch matches on this to route queries to the correct execution path:
/// `Sync` → `execute_to_sink`, `Async` → `dispatch_query`.
#[derive(Clone)]
pub enum AdapterKind {
    Sync(Arc<dyn SyncAdapter>),
    Async(Arc<dyn AsyncAdapter>),
}

impl AdapterKind {
    pub fn engine_type(&self) -> queryflux_core::query::EngineType {
        match self {
            Self::Sync(a) => a.engine_type(),
            Self::Async(a) => a.engine_type(),
        }
    }

    pub fn translation_target_dialect(&self) -> queryflux_core::query::SqlDialect {
        match self {
            Self::Sync(a) => a.translation_target_dialect(),
            Self::Async(a) => a.translation_target_dialect(),
        }
    }

    pub async fn health_check(&self) -> bool {
        match self {
            Self::Sync(a) => a.health_check().await,
            Self::Async(a) => a.health_check().await,
        }
    }

    pub async fn fetch_running_query_count(&self) -> Option<u64> {
        match self {
            Self::Sync(a) => a.fetch_running_query_count().await,
            Self::Async(a) => a.fetch_running_query_count().await,
        }
    }

    pub fn as_sync(&self) -> Option<Arc<dyn SyncAdapter>> {
        match self {
            Self::Sync(a) => Some(a.clone()),
            Self::Async(_) => None,
        }
    }

    pub fn as_async(&self) -> Option<Arc<dyn AsyncAdapter>> {
        match self {
            Self::Async(a) => Some(a.clone()),
            Self::Sync(_) => None,
        }
    }
}

/// Factory for constructing engine adapters from raw configuration.
///
/// Each engine provides a zero-sized factory struct implementing this trait.
/// This formalizes the contract that every adapter must support construction
/// from a DB config JSON blob and expose its descriptor metadata.
#[async_trait]
pub trait EngineAdapterFactory: Send + Sync {
    /// The engine key string matching the DB `engine_key` column (e.g. `"trino"`, `"duckDb"`).
    fn engine_key(&self) -> &'static str;

    /// Field-level schema used by the admin API and Studio UI.
    fn descriptor(&self) -> EngineDescriptor;

    /// Build an adapter instance from a raw DB config JSON blob.
    async fn build_from_config_json(
        &self,
        cluster_name: ClusterName,
        group: ClusterGroupName,
        json: &serde_json::Value,
    ) -> Result<AdapterKind>;
}
