//! Engine registry — types and runtime registry for backend engine descriptors.
//!
//! Core defines only the *types* and the `EngineRegistry` container.
//! The actual descriptor data lives in each engine adapter crate, which calls
//! `EngineRegistry::new(descriptors)` at startup (in `main.rs`).
//!
//! Used for:
//! - Startup validation of `ClusterConfig` (missing endpoint, unsupported auth, …)
//! - Admin API `/admin/engine-registry` so the UI can render forms without hard-coded logic

use serde::Serialize;

use crate::config::{ClusterAuth, ClusterConfig, EngineConfig, QueryAuthConfig, TlsConfig};
use crate::query::EngineType;

// ---------------------------------------------------------------------------
// Core types
// ---------------------------------------------------------------------------

/// How a backend cluster is reached.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum ConnectionType {
    /// REST/HTTP — Trino protocol, ClickHouse HTTP interface, DuckDB HTTP server
    Http,
    /// MySQL wire protocol — StarRocks front-end
    MySqlWire,
    /// In-process embedded library — DuckDB (no network endpoint)
    Embedded,
    /// SDK or cloud-managed — endpoint is implicit (e.g. Athena, BigQuery, Databricks)
    ManagedApi,
}

/// Authentication mechanisms the engine supports.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum AuthType {
    /// HTTP Basic auth (`Authorization: Basic …`)
    Basic,
    /// HTTP Bearer token (`Authorization: Bearer …`)
    Bearer,
    /// RSA key-pair (Snowflake, Databricks).
    KeyPair,
    /// AWS static access key (Athena and other AWS backends).
    AccessKey,
    /// AWS IAM role assumption via STS `AssumeRole` (Athena).
    RoleArn,
}

/// Describes a single configuration field that can appear on a `ClusterConfig`.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfigField {
    /// The YAML / JSON field name (camelCase, matches `ClusterConfig`).
    pub key: &'static str,
    /// Human-readable label for the UI.
    pub label: &'static str,
    /// Short description shown as helper text.
    pub description: &'static str,
    /// Field data type for UI rendering and client-side validation.
    pub field_type: FieldType,
    pub required: bool,
    /// Example value shown as placeholder in forms.
    pub example: Option<&'static str>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum FieldType {
    /// Plain text input
    Text,
    /// URL input (validated as a URL)
    Url,
    /// File system path
    Path,
    /// Password / secret — masked in UI
    Secret,
    /// Boolean toggle
    Boolean,
    /// Unsigned integer
    Number,
}

/// Full descriptor for one supported backend engine.
///
/// Each implemented adapter provides this via its own `descriptor()` method.
/// Core never hard-codes descriptor data.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EngineDescriptor {
    /// Value to use for the `engine` YAML key (e.g. `"trino"`, `"duckDb"`).
    pub engine_key: &'static str,
    /// Human-readable name.
    pub display_name: &'static str,
    /// One-line description of the engine.
    pub description: &'static str,
    /// Brand hex color (no `#`) for UI badges.
    pub hex: &'static str,
    /// How the proxy connects to this engine.
    pub connection_type: ConnectionType,
    /// Default port if the user doesn't supply one (informational).
    pub default_port: Option<u16>,
    /// Example endpoint string shown in docs / forms.
    pub endpoint_example: Option<&'static str>,
    /// Auth mechanisms this engine supports.
    pub supported_auth: Vec<AuthType>,
    /// Ordered list of config fields relevant to this engine.
    pub config_fields: Vec<ConfigField>,
    /// Whether a full adapter is implemented in this build.
    pub implemented: bool,
}

impl EngineDescriptor {
    pub fn requires_endpoint(&self) -> bool {
        matches!(
            self.connection_type,
            ConnectionType::Http | ConnectionType::MySqlWire
        )
    }

    pub fn supports_tls(&self) -> bool {
        self.connection_type == ConnectionType::Http
    }

    pub fn supports_database_path(&self) -> bool {
        self.connection_type == ConnectionType::Embedded
    }
}

// ---------------------------------------------------------------------------
// Registry
// ---------------------------------------------------------------------------

/// Runtime registry of engine descriptors, built at startup from adapter crates.
///
/// Each adapter supplies its own descriptor via `MyAdapter::descriptor()`.
/// `main.rs` collects them and passes the full list to `EngineRegistry::new`.
pub struct EngineRegistry {
    descriptors: Vec<EngineDescriptor>,
}

impl EngineRegistry {
    pub fn new(descriptors: Vec<EngineDescriptor>) -> Self {
        Self { descriptors }
    }

    /// All registered descriptors (for the admin API list endpoint).
    pub fn all(&self) -> &[EngineDescriptor] {
        &self.descriptors
    }

    /// Look up the descriptor for a given engine config variant.
    pub fn descriptor_for(&self, engine: &EngineConfig) -> Option<&EngineDescriptor> {
        let key = engine_key(engine);
        self.descriptors.iter().find(|d| d.engine_key == key)
    }
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

/// Validates a single cluster's configuration against the engine registry.
/// Returns a list of human-readable error messages; empty = valid.
pub fn validate_cluster_config(
    registry: &EngineRegistry,
    cluster_name: &str,
    config: &ClusterConfig,
) -> Vec<String> {
    let Some(engine) = &config.engine else {
        return vec![format!(
            "cluster '{cluster_name}': missing required 'engine' field"
        )];
    };

    let Some(desc) = registry.descriptor_for(engine) else {
        return vec![format!(
            "cluster '{cluster_name}': unknown engine '{}'",
            engine_key(engine)
        )];
    };

    let mut errors: Vec<String> = Vec::new();

    if !desc.implemented {
        errors.push(format!(
            "cluster '{cluster_name}': engine '{}' is defined but not yet implemented",
            desc.display_name
        ));
    }

    if desc.requires_endpoint() && config.endpoint.is_none() {
        errors.push(format!(
            "cluster '{cluster_name}': engine '{}' requires an 'endpoint' field (e.g. {})",
            desc.display_name,
            desc.endpoint_example.unwrap_or("see docs")
        ));
    }

    if !desc.supports_database_path() && config.database_path.is_some() {
        errors.push(format!(
            "cluster '{cluster_name}': 'databasePath' is only applicable to embedded DuckDB, not '{}'",
            desc.display_name
        ));
    }

    if !desc.supports_tls() && config.tls.is_some() {
        errors.push(format!(
            "cluster '{cluster_name}': engine '{}' does not support TLS configuration",
            desc.display_name
        ));
    }

    if let Some(auth) = &config.auth {
        let has_auth_type = match auth {
            ClusterAuth::Basic { .. } => desc.supported_auth.contains(&AuthType::Basic),
            ClusterAuth::Bearer { .. } => desc.supported_auth.contains(&AuthType::Bearer),
            ClusterAuth::KeyPair { .. } => desc.supported_auth.contains(&AuthType::KeyPair),
            ClusterAuth::AccessKey { .. } => desc.supported_auth.contains(&AuthType::AccessKey),
            ClusterAuth::RoleArn { .. } => desc.supported_auth.contains(&AuthType::RoleArn),
        };
        if !has_auth_type {
            let auth_label = match auth {
                ClusterAuth::Basic { .. } => "basic",
                ClusterAuth::Bearer { .. } => "bearer",
                ClusterAuth::KeyPair { .. } => "keyPair",
                ClusterAuth::AccessKey { .. } => "accessKey",
                ClusterAuth::RoleArn { .. } => "roleArn",
            };
            errors.push(format!(
                "cluster '{cluster_name}': engine '{}' does not support '{auth_label}' authentication",
                desc.display_name
            ));
        }
    }

    errors
}

// ---------------------------------------------------------------------------
// Config JSON helpers
// ---------------------------------------------------------------------------

/// Extract a `ClusterAuth` from the flat DB JSON format used by persistence.
///
/// The JSON blob stores auth as flat keys: `authType`, `authUsername`,
/// `authPassword`, `authToken`. This is the canonical format produced by
/// `UpsertClusterConfig::from_core()` and stored in the `config` JSONB column.
///
/// - Missing / empty `authType` → `Ok(None)`.
/// - Known `authType` with missing required fields → `Err` (so callers fail fast instead of
///   building adapters with empty credentials).
pub fn parse_auth_from_config_json(
    json: &serde_json::Value,
) -> Result<Option<ClusterAuth>, String> {
    let s =
        |key: &str| -> Option<String> { json.get(key).and_then(|v| v.as_str()).map(String::from) };
    let require = |key: &str| -> Result<String, String> {
        s(key)
            .filter(|v| !v.is_empty())
            .ok_or_else(|| format!("missing or empty '{key}' for this authType"))
    };
    match s("authType").as_deref() {
        None | Some("") => Ok(None),
        Some("basic") => Ok(Some(ClusterAuth::Basic {
            username: require("authUsername")?,
            password: require("authPassword")?,
        })),
        Some("bearer") => Ok(Some(ClusterAuth::Bearer {
            token: require("authToken")?,
        })),
        Some("keyPair") => Ok(Some(ClusterAuth::KeyPair {
            username: require("authUsername")?,
            private_key_pem: require("authPassword")?,
            private_key_passphrase: s("authToken"),
        })),
        Some("accessKey") => Ok(Some(ClusterAuth::AccessKey {
            access_key_id: require("authUsername")?,
            secret_access_key: require("authPassword")?,
            session_token: s("authToken"),
        })),
        Some("roleArn") => Ok(Some(ClusterAuth::RoleArn {
            role_arn: require("authUsername")?,
            external_id: s("authToken"),
        })),
        Some(other) => Err(format!("unsupported authType: '{other}'")),
    }
}

/// Extract per-query auth (`queryAuth` / Type 2) from the cluster `config` JSONB blob.
///
/// Same JSON shape as YAML `queryAuth` on [`ClusterConfig`] (written on upsert from YAML
/// and preserved in Postgres `cluster_configs.config`).
///
/// Returns [`Ok(None)`] when the field is omitted or null. A present but malformed payload
/// yields [`Err`].
pub fn parse_query_auth_from_config_json(
    json: &serde_json::Value,
) -> Result<Option<QueryAuthConfig>, serde_json::Error> {
    match json.get("queryAuth") {
        None => Ok(None),
        Some(v) if v.is_null() => Ok(None),
        Some(v) => Ok(Some(serde_json::from_value::<QueryAuthConfig>(v.clone())?)),
    }
}

/// Build a [`ClusterConfig`] from a persisted `cluster_configs.config` JSON blob plus parsed auth.
///
/// Field keys match `UpsertClusterConfig::from_core` / Studio camelCase JSON.
pub fn cluster_config_from_persisted_json(
    engine: EngineConfig,
    enabled: bool,
    max_running_queries: Option<u64>,
    config: &serde_json::Value,
    auth: Option<ClusterAuth>,
    query_auth: Option<QueryAuthConfig>,
) -> ClusterConfig {
    let tls = if json_bool(config, "tlsInsecureSkipVerify") {
        Some(TlsConfig {
            insecure_skip_verify: true,
        })
    } else {
        config
            .get("tls")
            .filter(|v| !v.is_null())
            .and_then(|v| serde_json::from_value::<TlsConfig>(v.clone()).ok())
    };
    ClusterConfig {
        engine: Some(engine),
        enabled,
        max_running_queries,
        endpoint: json_str(config, "endpoint"),
        database_path: json_str(config, "databasePath"),
        region: json_str(config, "region"),
        s3_output_location: json_str(config, "s3OutputLocation"),
        workgroup: json_str(config, "workgroup"),
        catalog: json_str(config, "catalog"),
        tls,
        auth,
        query_auth,
    }
}

/// Extract an optional string field from a config JSON blob.
pub fn json_str(json: &serde_json::Value, key: &str) -> Option<String> {
    json.get(key).and_then(|v| v.as_str()).map(String::from)
}

/// Extract a boolean field from a config JSON blob (defaults to `false`).
pub fn json_bool(json: &serde_json::Value, key: &str) -> bool {
    json.get(key).and_then(|v| v.as_bool()).unwrap_or(false)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Maps an `EngineConfig` variant to its canonical string key.
/// Must stay in sync with adapter `descriptor().engine_key` values.
pub fn engine_key(engine: &EngineConfig) -> &'static str {
    match engine {
        EngineConfig::Trino => "trino",
        EngineConfig::DuckDb => "duckDb",
        EngineConfig::DuckDbHttp => "duckDbHttp",
        EngineConfig::StarRocks => "starRocks",
        EngineConfig::ClickHouse => "clickHouse",
        EngineConfig::Athena => "athena",
    }
}

/// Inverse of [`engine_key`]. Used when loading `engine_key` from Postgres / API.
pub fn parse_engine_key(s: &str) -> Result<EngineConfig, String> {
    match s {
        "trino" => Ok(EngineConfig::Trino),
        "duckDb" => Ok(EngineConfig::DuckDb),
        "duckDbHttp" => Ok(EngineConfig::DuckDbHttp),
        "starRocks" => Ok(EngineConfig::StarRocks),
        "clickHouse" => Ok(EngineConfig::ClickHouse),
        "athena" => Ok(EngineConfig::Athena),
        other => Err(format!("Unknown engine key: '{other}'")),
    }
}

impl From<&EngineConfig> for EngineType {
    fn from(cfg: &EngineConfig) -> Self {
        match cfg {
            EngineConfig::Trino => EngineType::Trino,
            EngineConfig::DuckDb => EngineType::DuckDb,
            EngineConfig::DuckDbHttp => EngineType::DuckDbHttp,
            EngineConfig::StarRocks => EngineType::StarRocks,
            EngineConfig::ClickHouse => EngineType::ClickHouse,
            EngineConfig::Athena => EngineType::Athena,
        }
    }
}

#[cfg(test)]
mod query_auth_parse_tests {
    use super::*;
    use crate::config::QueryAuthConfig;

    #[test]
    fn parse_query_auth_impersonate() {
        let blob = serde_json::json!({ "queryAuth": { "type": "impersonate" } });
        let parsed = parse_query_auth_from_config_json(&blob).unwrap().unwrap();
        assert!(matches!(parsed, QueryAuthConfig::Impersonate));
    }

    #[test]
    fn parse_query_auth_omitted_is_none() {
        let blob = serde_json::json!({ "endpoint": "http://t:8080" });
        assert!(parse_query_auth_from_config_json(&blob).unwrap().is_none());
    }

    #[test]
    fn parse_query_auth_invalid_is_err() {
        let blob = serde_json::json!({ "queryAuth": { "type": "notAConfiguredQueryAuth" } });
        assert!(parse_query_auth_from_config_json(&blob).is_err());
    }
}
