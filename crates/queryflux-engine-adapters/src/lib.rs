pub mod athena;
pub mod duckdb;
pub mod starrocks;
pub mod trino;

use std::pin::Pin;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use futures::Stream;
use queryflux_auth::QueryCredentials;
use queryflux_core::{
    catalog::TableSchema,
    config::ClusterConfig,
    engine_registry::EngineDescriptor,
    error::{QueryFluxError, Result},
    query::{
        BackendQueryId, ClusterGroupName, ClusterName, EngineType, QueryExecution, QueryPollResult,
    },
    session::SessionContext,
    tags::QueryTags,
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
    ) -> Result<Arc<dyn EngineAdapterTrait>>;
}

/// Implemented by each query engine backend (Trino, DuckDB, StarRocks, ClickHouse, ...).
///
/// Engines that run queries synchronously return `QueryExecution::Sync` from `submit_query`.
/// Engines that run queries asynchronously return `QueryExecution::Async` and expect the
/// caller to poll via `poll_query` until `QueryPollResult::Complete` or `QueryPollResult::Failed`.
#[async_trait]
pub trait EngineAdapterTrait: Send + Sync {
    /// Submit a query for execution.
    ///
    /// Synchronous engines (DuckDB, StarRocks) return `QueryExecution::Sync` with the
    /// full result immediately. Async engines (Trino, ClickHouse) return `QueryExecution::Async`
    /// with a backend query ID to poll.
    ///
    /// `credentials` carries the resolved backend identity (Phase 1: always `ServiceAccount`).
    /// `session` carries unverified protocol metadata (headers, catalog hints, session properties)
    /// still needed for session setup until Phase 3b introduces `EngineConnectionOptions`.
    async fn submit_query(
        &self,
        sql: &str,
        session: &SessionContext,
        credentials: &QueryCredentials,
        tags: &QueryTags,
    ) -> Result<QueryExecution>;

    /// Poll a previously submitted async query for its current state.
    /// Only called when `submit_query` returned `QueryExecution::Async`.
    async fn poll_query(
        &self,
        backend_id: &BackendQueryId,
        next_uri: Option<&str>,
    ) -> Result<QueryPollResult>;

    /// Cancel a running or queued query.
    async fn cancel_query(&self, backend_id: &BackendQueryId) -> Result<()>;

    /// Check whether this cluster is reachable and healthy.
    async fn health_check(&self) -> bool;

    /// The engine type this adapter targets.
    fn engine_type(&self) -> EngineType;

    /// Whether this engine supports async polling.
    /// Sync engines always return false; their `poll_query` impl is unreachable.
    fn supports_async(&self) -> bool;

    /// The base URL of this engine instance (e.g. `http://trino:8080`).
    /// Used to reconstruct Trino poll URLs from the client-supplied path.
    /// Returns empty string for sync engines that don't have an HTTP endpoint.
    fn base_url(&self) -> &str {
        ""
    }

    /// Fetch the number of queries currently running on this engine instance,
    /// as reported by the engine itself. Used by the background reconciler to
    /// correct the in-memory `running_queries` counter after crashes or client disconnects.
    ///
    /// Return `None` if the engine does not expose this information.
    /// Default: `None` (unsupported). Engines that support it should override this.
    async fn fetch_running_query_count(&self) -> Option<u64> {
        None
    }

    /// Execute a query and return results as a stream of Arrow RecordBatches.
    ///
    /// This is the primary execution path for all non-Trino-HTTP frontends.
    /// Each adapter owns its type mapping (engine types → Arrow DataType) internally.
    /// The caller (execute_to_sink) feeds the stream to a ResultSink without
    /// inspecting individual types.
    ///
    /// Default: returns an error. Adapters that support Arrow execution override this.
    async fn execute_as_arrow(
        &self,
        _sql: &str,
        _session: &SessionContext,
        _credentials: &QueryCredentials,
        _tags: &QueryTags,
    ) -> Result<ArrowStream> {
        Err(QueryFluxError::Engine(format!(
            "Arrow execution not implemented for {:?} adapter",
            self.engine_type()
        )))
    }

    // --- Catalog discovery ---

    /// List all catalogs this engine instance is connected to.
    async fn list_catalogs(&self) -> Result<Vec<String>>;

    /// List all databases within a catalog.
    async fn list_databases(&self, catalog: &str) -> Result<Vec<String>>;

    /// List all tables within a catalog.database.
    async fn list_tables(&self, catalog: &str, database: &str) -> Result<Vec<String>>;

    /// Describe a specific table's columns and types.
    async fn describe_table(
        &self,
        catalog: &str,
        database: &str,
        table: &str,
    ) -> Result<Option<TableSchema>>;
}
