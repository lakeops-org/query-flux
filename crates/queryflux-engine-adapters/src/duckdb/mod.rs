use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use duckdb::Connection;
use futures::stream;
use queryflux_core::{
    catalog::TableSchema,
    error::{QueryFluxError, Result},
    query::{
        BackendQueryId, ClusterGroupName, ClusterName, EngineType, QueryExecution, QueryPollResult,
    },
    session::SessionContext,
};
use tracing::debug;

use crate::EngineAdapterTrait;

/// DuckDB embedded engine adapter.
///
/// DuckDB is Arrow-native and runs in-process. All query execution goes through
/// `execute_as_arrow` — `submit_query` is not used for this adapter.
pub struct DuckDbAdapter {
    pub cluster_name: ClusterName,
    pub group_name: ClusterGroupName,
    /// Thread-safe DuckDB connection. Wrapped in Arc<Mutex> so it can be shared
    /// across `spawn_blocking` calls without moving out of `&self`.
    conn: Arc<Mutex<Connection>>,
}

impl DuckDbAdapter {
    pub fn new(
        cluster_name: ClusterName,
        group_name: ClusterGroupName,
        database_path: Option<String>,
    ) -> Result<Self> {
        let conn = match database_path.as_deref() {
            Some(path) => Connection::open(path),
            None => Connection::open_in_memory(),
        }
        .map_err(|e| QueryFluxError::Engine(format!("DuckDB open failed: {e}")))?;

        Ok(Self {
            cluster_name,
            group_name,
            conn: Arc::new(Mutex::new(conn)),
        })
    }
}

#[async_trait]
impl EngineAdapterTrait for DuckDbAdapter {
    /// Not used — DuckDB queries go through `execute_as_arrow`.
    async fn submit_query(&self, _sql: &str, _session: &SessionContext) -> Result<QueryExecution> {
        Err(QueryFluxError::Engine(
            "DuckDB requires execute_as_arrow; use the Arrow execution path".to_string(),
        ))
    }

    async fn poll_query(
        &self,
        _backend_id: &BackendQueryId,
        _next_uri: Option<&str>,
    ) -> Result<QueryPollResult> {
        Err(QueryFluxError::Engine(
            "DuckDB does not support async polling".to_string(),
        ))
    }

    async fn cancel_query(&self, _backend_id: &BackendQueryId) -> Result<()> {
        Ok(())
    }

    async fn health_check(&self) -> bool {
        let conn = Arc::clone(&self.conn);
        tokio::task::spawn_blocking(move || {
            let guard = conn.lock().unwrap_or_else(|e| e.into_inner());
            guard.execute_batch("SELECT 1").is_ok()
        })
        .await
        .unwrap_or(false)
    }

    fn engine_type(&self) -> EngineType {
        EngineType::DuckDb
    }

    fn supports_async(&self) -> bool {
        false
    }

    async fn execute_as_arrow(
        &self,
        sql: &str,
        _session: &SessionContext,
    ) -> Result<crate::ArrowStream> {
        debug!(cluster = %self.cluster_name, "Executing DuckDB query as Arrow");
        let conn = Arc::clone(&self.conn);
        let sql = sql.to_string();

        let batches = tokio::task::spawn_blocking(move || {
            let guard = conn.lock().unwrap_or_else(|e| e.into_inner());
            let mut stmt = guard
                .prepare(&sql)
                .map_err(|e| QueryFluxError::Engine(format!("DuckDB prepare failed: {e}")))?;
            let arrow = stmt
                .query_arrow([])
                .map_err(|e| QueryFluxError::Engine(format!("DuckDB query failed: {e}")))?;
            Ok::<_, QueryFluxError>(arrow.collect::<Vec<_>>())
        })
        .await
        .map_err(|e| QueryFluxError::Engine(format!("spawn_blocking failed: {e}")))??;

        Ok(Box::pin(stream::iter(batches.into_iter().map(Ok))))
    }

    // --- Catalog discovery ---

    async fn list_catalogs(&self) -> Result<Vec<String>> {
        let rows = self
            .run_show_query(
                "SELECT catalog_name FROM information_schema.schemata GROUP BY catalog_name",
            )
            .await?;
        if rows.is_empty() {
            Ok(vec!["memory".to_string()])
        } else {
            Ok(rows)
        }
    }

    async fn list_databases(&self, _catalog: &str) -> Result<Vec<String>> {
        self.run_show_query("SELECT schema_name FROM information_schema.schemata")
            .await
    }

    async fn list_tables(&self, _catalog: &str, database: &str) -> Result<Vec<String>> {
        self.run_show_query(&format!(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = '{database}'"
        ))
        .await
    }

    async fn describe_table(
        &self,
        catalog: &str,
        database: &str,
        table: &str,
    ) -> Result<Option<TableSchema>> {
        let sql = format!(
            "SELECT column_name, data_type, is_nullable \
             FROM information_schema.columns \
             WHERE table_schema = '{database}' AND table_name = '{table}' \
             ORDER BY ordinal_position"
        );
        let conn = Arc::clone(&self.conn);
        let rows: Vec<(String, String, bool)> = tokio::task::spawn_blocking(move || {
            let guard = conn.lock().unwrap_or_else(|e| e.into_inner());
            let mut stmt = guard
                .prepare(&sql)
                .map_err(|e| QueryFluxError::Engine(format!("DuckDB prepare failed: {e}")))?;
            let arrow = stmt
                .query_arrow([])
                .map_err(|e| QueryFluxError::Engine(format!("DuckDB query failed: {e}")))?;
            use duckdb::arrow::array::{Array, StringArray};
            let mut rows = Vec::new();
            for batch in arrow.collect::<Vec<_>>() {
                let names = batch.column(0).as_any().downcast_ref::<StringArray>();
                let types = batch.column(1).as_any().downcast_ref::<StringArray>();
                let nullables = batch.column(2).as_any().downcast_ref::<StringArray>();
                for i in 0..batch.num_rows() {
                    let name = names.and_then(|a| {
                        if !a.is_null(i) {
                            Some(a.value(i).to_string())
                        } else {
                            None
                        }
                    });
                    let data_type = types.and_then(|a| {
                        if !a.is_null(i) {
                            Some(a.value(i).to_uppercase())
                        } else {
                            None
                        }
                    });
                    let nullable = nullables
                        .map(|a| a.is_null(i) || a.value(i).to_uppercase() != "NO")
                        .unwrap_or(true);
                    if let (Some(name), Some(data_type)) = (name, data_type) {
                        rows.push((name, data_type, nullable));
                    }
                }
            }
            Ok::<_, QueryFluxError>(rows)
        })
        .await
        .map_err(|e| QueryFluxError::Engine(format!("spawn_blocking failed: {e}")))??;

        if rows.is_empty() {
            return Ok(None);
        }
        let columns = rows
            .into_iter()
            .map(
                |(name, data_type, nullable)| queryflux_core::catalog::ColumnDef {
                    name,
                    data_type,
                    nullable,
                },
            )
            .collect();
        Ok(Some(TableSchema {
            catalog: catalog.to_string(),
            database: database.to_string(),
            table: table.to_string(),
            columns,
        }))
    }
}

impl DuckDbAdapter {
    /// Execute a batch of setup statements (INSTALL, LOAD, ATTACH, CREATE SECRET, etc.).
    ///
    /// Used by the test harness to prepare the Iceberg catalog extension before
    /// queries run. Runs on a blocking thread since DuckDB is synchronous.
    pub async fn setup_batch(&self, sql: &str) -> Result<()> {
        let conn = Arc::clone(&self.conn);
        let sql = sql.to_string();
        tokio::task::spawn_blocking(move || {
            let guard = conn.lock().unwrap_or_else(|e| e.into_inner());
            guard
                .execute_batch(&sql)
                .map_err(|e| QueryFluxError::Engine(format!("DuckDB setup_batch failed: {e}")))
        })
        .await
        .map_err(|e| QueryFluxError::Engine(format!("spawn_blocking failed: {e}")))?
    }

    /// Run a query and collect the first column of each row as strings.
    /// Used internally for catalog discovery queries.
    async fn run_show_query(&self, sql: &str) -> Result<Vec<String>> {
        let conn = Arc::clone(&self.conn);
        let sql = sql.to_string();
        tokio::task::spawn_blocking(move || {
            let guard = conn.lock().unwrap_or_else(|e| e.into_inner());
            let mut stmt = guard
                .prepare(&sql)
                .map_err(|e| QueryFluxError::Engine(format!("DuckDB prepare failed: {e}")))?;
            let arrow = stmt
                .query_arrow([])
                .map_err(|e| QueryFluxError::Engine(format!("DuckDB query failed: {e}")))?;
            let mut results = Vec::new();
            for batch in arrow.collect::<Vec<_>>() {
                if batch.num_columns() == 0 {
                    continue;
                }
                let col = batch.column(0);
                use duckdb::arrow::array::{Array, StringArray};
                if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
                    for i in 0..arr.len() {
                        if !arr.is_null(i) {
                            results.push(arr.value(i).to_string());
                        }
                    }
                } else {
                    use duckdb::arrow::util::display::array_value_to_string;
                    for i in 0..col.len() {
                        if !col.is_null(i) {
                            results
                                .push(array_value_to_string(col.as_ref(), i).unwrap_or_default());
                        }
                    }
                }
            }
            Ok(results)
        })
        .await
        .map_err(|e| QueryFluxError::Engine(format!("spawn_blocking failed: {e}")))?
    }
}
