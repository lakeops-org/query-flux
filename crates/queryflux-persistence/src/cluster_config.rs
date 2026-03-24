//! Persisted cluster and cluster-group configuration records.
//!
//! When Postgres persistence is configured, QueryFlux stores the cluster / group
//! config in `cluster_configs` and `cluster_group_configs` and reads from there
//! instead of the YAML file.  The YAML is only used to seed the tables on the
//! very first run (when both tables are empty).
//!
//! Each cluster row has a stable `id` plus an engine-specific `config JSONB`
//! column. All connection details (endpoint, auth, TLS, region, …) live inside
//! that JSON blob so the schema never needs a migration when a new engine field
//! is added.

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
    /// Stable surrogate key; group `members` stores these ids in Postgres.
    pub id: i64,
    pub name: String,
    /// YAML / registry engine key: `"trino"`, `"duckDb"`, `"starRocks"`, `"clickHouse"`, `"athena"`.
    pub engine_key: String,
    pub enabled: bool,
    /// Per-cluster limit; `NULL` means inherit from the cluster group.
    pub max_running_queries: Option<i64>,
    /// All engine-specific connection details (endpoint, auth, TLS, region, …).
    #[schema(value_type = Object)]
    pub config: serde_json::Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Request body for creating or fully replacing a cluster config.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct UpsertClusterConfig {
    pub engine_key: String,
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Omit or `null` to inherit the group's `maxRunningQueries`.
    #[serde(default)]
    pub max_running_queries: Option<i64>,
    /// Engine-specific connection details. Schema depends on `engineKey`.
    #[schema(value_type = Object)]
    pub config: serde_json::Value,
}

/// Request body for PATCH rename (`/admin/config/clusters/{name}`, `/admin/config/groups/{name}`).
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct RenameConfigRequest {
    pub new_name: String,
}

// ---------------------------------------------------------------------------
// Cluster group config
// ---------------------------------------------------------------------------

/// Full cluster group configuration record as stored in Postgres.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, sqlx::FromRow)]
#[serde(rename_all = "camelCase")]
pub struct ClusterGroupConfigRecord {
    /// Stable surrogate key; used by routing rules and foreign keys.
    pub id: i64,
    pub name: String,
    pub enabled: bool,
    /// Ordered member cluster names (resolved from ids stored in Postgres).
    pub members: Vec<String>,
    pub max_running_queries: i64,
    pub max_queued_queries: Option<i64>,
    /// Serialised `StrategyConfig`. `null` means RoundRobin (the default).
    #[schema(value_type = Option<Object>)]
    pub strategy: Option<serde_json::Value>,
    pub allow_groups: Vec<String>,
    pub allow_users: Vec<String>,
    /// Ordered `user_scripts.id` values run as post-sqlglot translation fixups for this group.
    #[serde(default)]
    pub translation_script_ids: Vec<i64>,
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
    #[serde(default)]
    pub allow_groups: Vec<String>,
    #[serde(default)]
    pub allow_users: Vec<String>,
    /// Ordered translation fixup script ids (`user_scripts.kind = translation_fixup`).
    #[serde(default)]
    pub translation_script_ids: Vec<i64>,
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
            EngineConfig::DuckDbHttp => "duckDbHttp",
            EngineConfig::StarRocks => "starRocks",
            EngineConfig::ClickHouse => "clickHouse",
            EngineConfig::Athena => "athena",
        };

        let mut config = serde_json::Map::new();

        if let Some(v) = &cfg.endpoint {
            config.insert("endpoint".into(), v.clone().into());
        }
        if let Some(v) = &cfg.database_path {
            config.insert("databasePath".into(), v.clone().into());
        }
        if cfg.tls.as_ref().map(|t| t.insecure_skip_verify).unwrap_or(false) {
            config.insert("tlsInsecureSkipVerify".into(), true.into());
        }
        if let Some(v) = &cfg.region {
            config.insert("region".into(), v.clone().into());
        }
        if let Some(v) = &cfg.s3_output_location {
            config.insert("s3OutputLocation".into(), v.clone().into());
        }
        if let Some(v) = &cfg.workgroup {
            config.insert("workgroup".into(), v.clone().into());
        }
        if let Some(v) = &cfg.catalog {
            config.insert("catalog".into(), v.clone().into());
        }

        match &cfg.auth {
            Some(ClusterAuth::Basic { username, password }) => {
                config.insert("authType".into(), "basic".into());
                config.insert("authUsername".into(), username.clone().into());
                config.insert("authPassword".into(), password.clone().into());
            }
            Some(ClusterAuth::Bearer { token }) => {
                config.insert("authType".into(), "bearer".into());
                config.insert("authToken".into(), token.clone().into());
            }
            Some(ClusterAuth::AccessKey { access_key_id, secret_access_key, session_token }) => {
                config.insert("authType".into(), "accessKey".into());
                config.insert("authUsername".into(), access_key_id.clone().into());
                config.insert("authPassword".into(), secret_access_key.clone().into());
                if let Some(st) = session_token {
                    config.insert("authToken".into(), st.clone().into());
                }
            }
            // KeyPair: private key material is not persisted to DB.
            Some(ClusterAuth::KeyPair { username, .. }) => {
                config.insert("authType".into(), "keyPair".into());
                config.insert("authUsername".into(), username.clone().into());
            }
            Some(ClusterAuth::RoleArn { role_arn, external_id }) => {
                config.insert("authType".into(), "roleArn".into());
                config.insert("authUsername".into(), role_arn.clone().into());
                if let Some(eid) = external_id {
                    config.insert("authToken".into(), eid.clone().into());
                }
            }
            None => {}
        }

        Some(Self {
            engine_key: engine_key.to_string(),
            enabled: cfg.enabled,
            max_running_queries: cfg.max_running_queries.map(|v| v as i64),
            config: serde_json::Value::Object(config),
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
            allow_groups: cfg.authorization.allow_groups.clone(),
            allow_users: cfg.authorization.allow_users.clone(),
            translation_script_ids: Vec::new(),
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
            "duckDbHttp" => EngineConfig::DuckDbHttp,
            "starRocks" => EngineConfig::StarRocks,
            "clickHouse" => EngineConfig::ClickHouse,
            "athena" => EngineConfig::Athena,
            other => {
                return Err(QueryFluxError::Engine(format!(
                    "Unknown engine key in DB: '{other}'"
                )))
            }
        };

        // Helpers to extract typed values from the config JSON.
        let s = |key: &str| -> Option<String> {
            self.config.get(key).and_then(|v| v.as_str()).map(String::from)
        };
        let b = |key: &str| -> bool {
            self.config.get(key).and_then(|v| v.as_bool()).unwrap_or(false)
        };

        let auth = match s("authType").as_deref() {
            Some("basic") => Some(ClusterAuth::Basic {
                username: s("authUsername").unwrap_or_default(),
                password: s("authPassword").unwrap_or_default(),
            }),
            Some("bearer") => Some(ClusterAuth::Bearer {
                token: s("authToken").unwrap_or_default(),
            }),
            Some("accessKey") => Some(ClusterAuth::AccessKey {
                access_key_id: s("authUsername").unwrap_or_default(),
                secret_access_key: s("authPassword").unwrap_or_default(),
                session_token: s("authToken"),
            }),
            Some("roleArn") => Some(ClusterAuth::RoleArn {
                role_arn: s("authUsername").unwrap_or_default(),
                external_id: s("authToken"),
            }),
            _ => None,
        };

        Ok(ClusterConfig {
            engine: Some(engine),
            enabled: self.enabled,
            max_running_queries: self.max_running_queries.map(|v| v as u64),
            endpoint: s("endpoint"),
            database_path: s("databasePath"),
            region: s("region"),
            s3_output_location: s("s3OutputLocation"),
            workgroup: s("workgroup"),
            catalog: s("catalog"),
            tls: if b("tlsInsecureSkipVerify") {
                Some(TlsConfig { insecure_skip_verify: true })
            } else {
                None
            },
            auth,
            query_auth: None, // not persisted to DB; loaded from YAML config only
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
            authorization: queryflux_core::config::ClusterGroupAuthorizationConfig {
                allow_groups: self.allow_groups.clone(),
                allow_users: self.allow_users.clone(),
            },
        }
    }
}

fn default_true() -> bool {
    true
}
