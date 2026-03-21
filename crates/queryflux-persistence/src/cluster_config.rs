//! Persisted cluster and cluster-group configuration records.
//!
//! When Postgres persistence is configured, QueryFlux stores the cluster / group
//! config in `cluster_configs` and `cluster_group_configs` and reads from there
//! instead of the YAML file.  The YAML is only used to seed the tables on the
//! very first run (when both tables are empty).

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

// ---------------------------------------------------------------------------
// Cluster config
// ---------------------------------------------------------------------------

/// Full cluster configuration record as stored in Postgres.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, sqlx::FromRow)]
#[serde(rename_all = "camelCase")]
pub struct ClusterConfigRecord {
    pub name: String,
    /// YAML / registry engine key: `"trino"`, `"duckDb"`, `"starRocks"`, `"clickHouse"`.
    pub engine_key: String,
    /// HTTP(S) or mysql:// endpoint URL. Null for embedded engines (DuckDB).
    pub endpoint: Option<String>,
    /// DuckDB database file path. Null for all other engines.
    pub database_path: Option<String>,
    /// `"basic"` | `"bearer"` | null
    pub auth_type: Option<String>,
    pub auth_username: Option<String>,
    /// Stored as-is; mask in UI when displaying.
    pub auth_password: Option<String>,
    pub auth_token: Option<String>,
    pub tls_insecure_skip_verify: bool,
    pub enabled: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Request body for creating or fully replacing a cluster config.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct UpsertClusterConfig {
    pub engine_key: String,
    pub endpoint: Option<String>,
    pub database_path: Option<String>,
    pub auth_type: Option<String>,
    pub auth_username: Option<String>,
    pub auth_password: Option<String>,
    pub auth_token: Option<String>,
    #[serde(default)]
    pub tls_insecure_skip_verify: bool,
    #[serde(default = "default_true")]
    pub enabled: bool,
}

// ---------------------------------------------------------------------------
// Cluster group config
// ---------------------------------------------------------------------------

/// Full cluster group configuration record as stored in Postgres.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, sqlx::FromRow)]
#[serde(rename_all = "camelCase")]
pub struct ClusterGroupConfigRecord {
    pub name: String,
    pub enabled: bool,
    /// Ordered list of cluster names that are members of this group.
    pub members: Vec<String>,
    pub max_running_queries: i64,
    pub max_queued_queries: Option<i64>,
    /// Serialised `StrategyConfig`. `null` means RoundRobin (the default).
    #[schema(value_type = Option<Object>)]
    pub strategy: Option<serde_json::Value>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Request body for creating or fully replacing a cluster group config.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct UpsertClusterGroupConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    pub members: Vec<String>,
    pub max_running_queries: i64,
    pub max_queued_queries: Option<i64>,
    /// `null` = RoundRobin. Set to `{"type":"leastLoaded"}` etc. for other strategies.
    pub strategy: Option<serde_json::Value>,
}

// ---------------------------------------------------------------------------
// Conversion helpers: core config types → Upsert types (for YAML seeding)
// ---------------------------------------------------------------------------

use queryflux_core::config::{ClusterAuth, ClusterConfig, ClusterGroupConfig, EngineConfig};

impl UpsertClusterConfig {
    pub fn from_core(cfg: &ClusterConfig) -> Option<Self> {
        let engine_key = match cfg.engine.as_ref()? {
            EngineConfig::Trino => "trino",
            EngineConfig::DuckDb => "duckDb",
            EngineConfig::StarRocks => "starRocks",
            EngineConfig::ClickHouse => "clickHouse",
        };

        let (auth_type, auth_username, auth_password, auth_token) = match &cfg.auth {
            Some(ClusterAuth::Basic { username, password }) => (
                Some("basic".to_string()),
                Some(username.clone()),
                Some(password.clone()),
                None,
            ),
            Some(ClusterAuth::Bearer { token }) => {
                (Some("bearer".to_string()), None, None, Some(token.clone()))
            }
            None => (None, None, None, None),
        };

        Some(Self {
            engine_key: engine_key.to_string(),
            endpoint: cfg.endpoint.clone(),
            database_path: cfg.database_path.clone(),
            auth_type,
            auth_username,
            auth_password,
            auth_token,
            tls_insecure_skip_verify: cfg.tls.as_ref().map(|t| t.insecure_skip_verify).unwrap_or(false),
            enabled: cfg.enabled,
        })
    }
}

impl UpsertClusterGroupConfig {
    pub fn from_core(cfg: &ClusterGroupConfig) -> Self {
        let strategy = cfg
            .strategy
            .as_ref()
            .and_then(|s| serde_json::to_value(s).ok());

        Self {
            enabled: cfg.enabled,
            members: cfg.members.clone(),
            max_running_queries: cfg.max_running_queries as i64,
            max_queued_queries: cfg.max_queued_queries.map(|v| v as i64),
            strategy,
        }
    }
}

// ---------------------------------------------------------------------------
// Conversion helpers: DB records → core config types (for startup loading)
// ---------------------------------------------------------------------------

impl ClusterConfigRecord {
    pub fn to_core(&self) -> queryflux_core::error::Result<ClusterConfig> {
        use queryflux_core::config::TlsConfig;
        use queryflux_core::error::QueryFluxError;

        let engine = match self.engine_key.as_str() {
            "trino" => EngineConfig::Trino,
            "duckDb" => EngineConfig::DuckDb,
            "starRocks" => EngineConfig::StarRocks,
            "clickHouse" => EngineConfig::ClickHouse,
            other => {
                return Err(QueryFluxError::Engine(format!(
                    "Unknown engine key in DB: '{other}'"
                )))
            }
        };

        let auth = match self.auth_type.as_deref() {
            Some("basic") => Some(ClusterAuth::Basic {
                username: self.auth_username.clone().unwrap_or_default(),
                password: self.auth_password.clone().unwrap_or_default(),
            }),
            Some("bearer") => Some(ClusterAuth::Bearer {
                token: self.auth_token.clone().unwrap_or_default(),
            }),
            _ => None,
        };

        Ok(ClusterConfig {
            engine: Some(engine),
            enabled: self.enabled,
            endpoint: self.endpoint.clone(),
            database_path: self.database_path.clone(),
            tls: if self.tls_insecure_skip_verify {
                Some(TlsConfig { insecure_skip_verify: true })
            } else {
                None
            },
            auth,
        })
    }
}

impl ClusterGroupConfigRecord {
    pub fn to_core(&self) -> ClusterGroupConfig {
        use queryflux_core::config::StrategyConfig;

        let strategy = self
            .strategy
            .as_ref()
            .and_then(|v| serde_json::from_value::<StrategyConfig>(v.clone()).ok());

        ClusterGroupConfig {
            enabled: self.enabled,
            members: self.members.clone(),
            strategy,
            max_running_queries: self.max_running_queries as u64,
            max_queued_queries: self.max_queued_queries.map(|v| v as u64),
        }
    }
}

fn default_true() -> bool {
    true
}
