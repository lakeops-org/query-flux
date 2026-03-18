use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Root configuration for a QueryFlux deployment.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProxyConfig {
    pub queryflux: QueryFluxConfig,
    pub cluster_groups: HashMap<String, ClusterGroupConfig>,
    pub routers: Vec<RouterConfig>,
    pub routing_fallback: String,
    #[serde(default)]
    pub translation: TranslationConfig,
    #[serde(default)]
    pub catalog_provider: CatalogProviderConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryFluxConfig {
    pub external_address: Option<String>,
    #[serde(default)]
    pub frontends: FrontendsConfig,
    #[serde(default)]
    pub persistence: PersistenceConfig,
    #[serde(default)]
    pub admin_api: AdminApiConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct FrontendsConfig {
    #[serde(default)]
    pub trino_http: FrontendConfig,
    #[serde(default)]
    pub postgres_wire: Option<FrontendConfig>,
    #[serde(default)]
    pub mysql_wire: Option<FrontendConfig>,
    #[serde(default)]
    pub clickhouse_http: Option<FrontendConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FrontendConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    pub port: u16,
}

impl Default for FrontendConfig {
    fn default() -> Self {
        Self { enabled: true, port: 8080 }
    }
}

fn default_true() -> bool { true }

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum PersistenceConfig {
    #[serde(rename = "inMemory")]
    InMemory,
    Redis { url: String },
    Postgres { url: String },
}

impl Default for PersistenceConfig {
    fn default() -> Self {
        PersistenceConfig::InMemory
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AdminApiConfig {
    #[serde(default = "default_admin_port")]
    pub port: u16,
}

fn default_admin_port() -> u16 { 9000 }

impl Default for AdminApiConfig {
    fn default() -> Self {
        Self { port: 9000 }
    }
}

// --- Cluster groups ---

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterGroupConfig {
    pub engine: EngineConfig,
    pub max_running_queries: u64,
    #[serde(default)]
    pub max_queued_queries: Option<u64>,
    pub clusters: Vec<ClusterConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum EngineConfig {
    Trino,
    DuckDb,
    StarRocks,
    ClickHouse,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterConfig {
    pub name: String,
    /// HTTP(S) endpoint for Trino / ClickHouse / StarRocks FE.
    pub endpoint: Option<String>,
    /// Local file path for DuckDB.
    pub database_path: Option<String>,
    #[serde(default)]
    pub tls: Option<TlsConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TlsConfig {
    pub insecure_skip_verify: bool,
}

// --- Routers ---

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum RouterConfig {
    ProtocolBased {
        #[serde(default)]
        trino_http: Option<String>,
        #[serde(default)]
        postgres_wire: Option<String>,
        #[serde(default)]
        mysql_wire: Option<String>,
        #[serde(default)]
        clickhouse_http: Option<String>,
    },
    Header {
        header_name: String,
        header_value_to_group: HashMap<String, String>,
    },
    UserGroup {
        user_to_group: HashMap<String, String>,
    },
    QueryRegex {
        rules: Vec<QueryRegexRule>,
    },
    ClientTags {
        tag_to_group: HashMap<String, String>,
    },
    PythonScript {
        script: String,
        script_file: Option<String>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryRegexRule {
    pub regex: String,
    pub target_group: String,
}

// --- Translation ---

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct TranslationConfig {
    /// If true, fail the query when sqlglot cannot translate a construct.
    /// If false (default), pass through best-effort.
    #[serde(default)]
    pub error_on_unsupported: bool,
    /// Optional Python fixup scripts applied after sqlglot, keyed by "src_dialect_to_tgt_dialect".
    #[serde(default)]
    pub python_scripts: HashMap<String, String>,
}

// --- Catalog provider ---

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum CatalogProviderConfig {
    #[default]
    Null,
    Static {
        schemas: Vec<StaticTableSchema>,
    },
    Trino {
        /// Name of the cluster group to use for metadata queries.
        cluster_group: String,
    },
    HiveMetastore {
        uri: String,
    },
    Glue {
        region: Option<String>,
    },
    Caching {
        ttl_seconds: u64,
        max_entries: usize,
        #[serde(flatten)]
        delegate: Box<CatalogProviderConfig>,
    },
    Fallback {
        primary: Box<CatalogProviderConfig>,
        secondary: Box<CatalogProviderConfig>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StaticTableSchema {
    pub catalog: String,
    pub database: String,
    pub table: String,
    pub columns: Vec<StaticColumnDef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StaticColumnDef {
    pub name: String,
    pub data_type: String,
    #[serde(default = "default_true")]
    pub nullable: bool,
}
