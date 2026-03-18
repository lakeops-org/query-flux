pub mod trino;

use async_trait::async_trait;
use queryflux_core::{
    catalog::TableSchema,
    error::Result,
    query::{BackendQueryId, EngineType, QueryExecution, QueryPollResult},
    session::SessionContext,
};

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
    async fn submit_query(
        &self,
        sql: &str,
        session: &SessionContext,
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
