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
}

impl FrontendProtocol {
    /// The SQL dialect naturally associated with this protocol's clients.
    pub fn default_dialect(&self) -> SqlDialect {
        match self {
            FrontendProtocol::TrinoHttp => SqlDialect::Trino,
            FrontendProtocol::PostgresWire => SqlDialect::Postgres,
            FrontendProtocol::MySqlWire => SqlDialect::MySql,
            FrontendProtocol::ClickHouseHttp => SqlDialect::ClickHouse,
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
    pub backend_query_id: Option<BackendQueryId>,
    pub next_uri: Option<String>,
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

/// Returned by `EngineAdapterTrait::submit_query`.
/// Sync engines return the result immediately; async engines return a handle to poll.
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
    Sync {
        result: QueryPollResult,
    },
}

/// Returned by `EngineAdapterTrait::poll_query` or embedded in `QueryExecution::Sync`.
#[derive(Debug)]
pub enum QueryPollResult {
    Pending {
        progress: Option<f32>,
        next_uri: Option<String>,
    },
    Complete {
        columns: Vec<ColumnDef>,
        data: Vec<Vec<QueryValue>>,
        stats: QueryStats,
    },
    Failed {
        message: String,
        error_code: Option<String>,
    },
    /// Raw response bytes for transparent protocol forwarding (e.g. Trino → Trino).
    /// The frontend rewrites nextUri and returns the bytes directly to the client.
    Raw {
        body: Bytes,
        /// The backend's next polling URL (None means query is complete).
        next_uri: Option<String>,
    },
}

// --- Result value types ---

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum QueryValue {
    Null,
    Bool(bool),
    Int64(i64),
    Float64(f64),
    String(String),
    Bytes(Vec<u8>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct QueryStats {
    pub queue_duration_ms: u64,
    pub execution_duration_ms: u64,
    pub rows_returned: u64,
    pub bytes_returned: Option<u64>,
}

// --- Query status (for metrics) ---

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum QueryStatus {
    Success,
    Failed,
    Cancelled,
}
