use std::time::SystemTime;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::session::SessionContext;

// --- Identifiers ---

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ProxyQueryId(pub String);

impl ProxyQueryId {
    pub fn new() -> Self {
        Self(Uuid::new_v4().to_string())
    }
}

impl Default for ProxyQueryId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for ProxyQueryId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BackendQueryId(pub String);

impl std::fmt::Display for BackendQueryId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ClusterGroupName(pub String);

impl std::fmt::Display for ClusterGroupName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ClusterName(pub String);

impl std::fmt::Display for ClusterName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

// --- Protocol & Engine ---

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum FrontendProtocol {
    TrinoHttp,
    PostgresWire,
    MySqlWire,
    ClickHouseHttp,
    FlightSql,
}

impl FrontendProtocol {
    /// The SQL dialect naturally associated with this protocol's clients.
    pub fn default_dialect(&self) -> SqlDialect {
        match self {
            FrontendProtocol::TrinoHttp => SqlDialect::Trino,
            FrontendProtocol::PostgresWire => SqlDialect::Postgres,
            FrontendProtocol::MySqlWire => SqlDialect::MySql,
            FrontendProtocol::ClickHouseHttp => SqlDialect::ClickHouse,
            FrontendProtocol::FlightSql => SqlDialect::Generic,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum EngineType {
    Trino,
    DuckDb,
    StarRocks,
    ClickHouse,
}

impl EngineType {
    pub fn dialect(&self) -> SqlDialect {
        match self {
            EngineType::Trino => SqlDialect::Trino,
            EngineType::DuckDb => SqlDialect::DuckDb,
            EngineType::StarRocks => SqlDialect::StarRocks,
            EngineType::ClickHouse => SqlDialect::ClickHouse,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum SqlDialect {
    Trino,
    DuckDb,
    StarRocks,
    ClickHouse,
    MySql,
    Postgres,
    Generic,
}

impl SqlDialect {
    /// Returns true if translating between these two dialects is a no-op.
    /// MySql and StarRocks share the same wire protocol and SQL syntax.
    pub fn is_compatible_with(&self, other: &SqlDialect) -> bool {
        self == other
            || matches!(
                (self, other),
                (SqlDialect::MySql, SqlDialect::StarRocks)
                    | (SqlDialect::StarRocks, SqlDialect::MySql)
            )
    }

    /// The dialect name as sqlglot expects it.
    pub fn sqlglot_name(&self) -> &'static str {
        match self {
            SqlDialect::Trino => "trino",
            SqlDialect::DuckDb => "duckdb",
            SqlDialect::StarRocks => "starrocks",
            SqlDialect::ClickHouse => "clickhouse",
            SqlDialect::MySql => "mysql",
            SqlDialect::Postgres => "postgres",
            SqlDialect::Generic => "",
        }
    }
}

// --- Incoming query (before routing) ---

#[derive(Debug, Clone)]
pub struct IncomingQuery {
    pub id: ProxyQueryId,
    pub sql: String,
    pub session: SessionContext,
    pub frontend_protocol: FrontendProtocol,
    pub creation_time: SystemTime,
}

impl IncomingQuery {
    pub fn new(sql: String, session: SessionContext, frontend_protocol: FrontendProtocol) -> Self {
        Self {
            id: ProxyQueryId::new(),
            sql,
            session,
            frontend_protocol,
            creation_time: SystemTime::now(),
        }
    }
}

// --- Executing query (after routing, being dispatched) ---

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutingQuery {
    pub id: ProxyQueryId,
    pub sql: String,
    pub translated_sql: Option<String>,
    pub cluster_group: ClusterGroupName,
    pub cluster_name: ClusterName,
    /// The backend engine's query ID (e.g. Trino's `20260319_084733_00386_kqwci`).
    /// Used as the persistence key and embedded in the client-facing poll URL.
    pub backend_query_id: BackendQueryId,
    /// The Trino cluster base URL (e.g. `http://trino:8080`).
    /// Used to reconstruct the Trino poll URL from the client-supplied path.
    /// Never changes after submit — no updates needed between polls.
    pub trino_endpoint: String,
    pub creation_time: DateTime<Utc>,
    pub last_accessed: DateTime<Utc>,
}

// --- Query execution result model ---

/// A query waiting for cluster capacity to become available.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueuedQuery {
    pub id: ProxyQueryId,
    pub sql: String,
    pub session: SessionContext,
    pub frontend_protocol: FrontendProtocol,
    pub cluster_group: ClusterGroupName,
    pub creation_time: DateTime<Utc>,
    pub last_accessed: DateTime<Utc>,
    /// How many times the client has polled. Used for exponential backoff.
    pub sequence: u64,
}

/// Returned by `EngineAdapterTrait::submit_query` for async (Trino) backends.
/// Sync backends (DuckDB, StarRocks) use `execute_as_arrow` instead.
#[derive(Debug)]
pub enum QueryExecution {
    Async {
        backend_query_id: BackendQueryId,
        /// The backend's next polling URL (stored in persistence, never sent to client).
        next_uri: Option<String>,
        /// Raw response bytes from the first submit call (e.g. Trino JSON).
        /// When present, the frontend rewrites nextUri and returns this directly.
        initial_body: Option<Bytes>,
    },
}

/// Returned by `EngineAdapterTrait::poll_query` for async (Trino) backends.
#[derive(Debug)]
pub enum QueryPollResult {
    Pending {
        progress: Option<f32>,
        next_uri: Option<String>,
    },
    Failed {
        message: String,
        error_code: Option<String>,
    },
    /// Raw response bytes for transparent protocol forwarding (Trino → Trino).
    /// The frontend rewrites nextUri and returns the bytes directly to the client.
    Raw {
        body: Bytes,
        /// The backend's next polling URL (None means query is complete).
        next_uri: Option<String>,
        /// Engine stats extracted from the final response (only set when next_uri is None).
        engine_stats: Option<QueryEngineStats>,
    },
}

// --- Query stats ---

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct QueryStats {
    pub queue_duration_ms: u64,
    pub execution_duration_ms: u64,
    pub rows_returned: u64,
    pub bytes_returned: Option<u64>,
}

/// Engine-level execution statistics captured from the final query response.
/// Fields are optional since different engines expose different metrics.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct QueryEngineStats {
    /// Elapsed (wall-clock) time as reported by the engine (ms).
    /// Comparing this against QueryFlux's own `execution_duration_ms` gives the proxy overhead.
    pub engine_elapsed_time_ms: Option<u64>,
    /// CPU time consumed by the query across all workers (ms).
    pub cpu_time_ms: Option<u64>,
    /// Number of rows read/processed by the engine.
    pub processed_rows: Option<u64>,
    /// Logical bytes processed (in-memory representation).
    pub processed_bytes: Option<u64>,
    /// Physical bytes read from storage (I/O cost).
    pub physical_input_bytes: Option<u64>,
    /// Peak memory usage across all workers (bytes).
    pub peak_memory_bytes: Option<u64>,
    /// Data spilled to disk during execution (bytes).
    pub spilled_bytes: Option<u64>,
    /// Number of execution splits/tasks.
    pub total_splits: Option<u32>,
}

// --- Query status (for metrics) ---

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum QueryStatus {
    Success,
    Failed,
    Cancelled,
}
