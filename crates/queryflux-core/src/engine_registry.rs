//! Static engine registry — describes every query engine QueryFlux can proxy.
//!
//! Used for:
//! - Startup validation of `ClusterConfig` (missing endpoint, unsupported auth, …)
//! - Admin API `/admin/engine-registry` so the UI can render forms without hard-coded logic
//! - Future: config wizard / validation in Studio

use serde::Serialize;

use crate::config::{ClusterAuth, ClusterConfig, EngineConfig};

// ---------------------------------------------------------------------------
// Core types
// ---------------------------------------------------------------------------

/// How a backend cluster is reached.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum ConnectionType {
    /// REST/HTTP — Trino protocol, ClickHouse HTTP interface
    Http,
    /// MySQL wire protocol — StarRocks front-end
    MySqlWire,
    /// In-process embedded library — DuckDB (no network endpoint)
    Embedded,
}

/// Authentication mechanisms the engine supports.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum AuthType {
    /// HTTP Basic auth (`Authorization: Basic …`)
    Basic,
    /// HTTP Bearer token (`Authorization: Bearer …`)
    Bearer,
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
    /// Used by the UI to render a typed form and by validation to emit
    /// helpful error messages.
    pub config_fields: Vec<ConfigField>,
    /// Whether a full adapter is implemented in this build.
    /// `false` = defined in the config model but adapter is a stub / TODO.
    pub implemented: bool,
}

// ---------------------------------------------------------------------------
// Registry
// ---------------------------------------------------------------------------

/// Returns all engine descriptors.
pub fn engine_descriptors() -> Vec<EngineDescriptor> {
    vec![
        // ── Trino ────────────────────────────────────────────────────────────
        EngineDescriptor {
            engine_key: "trino",
            display_name: "Trino",
            description: "Distributed SQL query engine using the Trino REST protocol (async submit/poll).",
            hex: "DD00A1",
            connection_type: ConnectionType::Http,
            default_port: Some(8080),
            endpoint_example: Some("http://trino-host:8080"),
            supported_auth: vec![AuthType::Basic, AuthType::Bearer],
            implemented: true,
            config_fields: vec![
                ConfigField {
                    key: "endpoint",
                    label: "Endpoint",
                    description: "HTTP(S) base URL of the Trino coordinator.",
                    field_type: FieldType::Url,
                    required: true,
                    example: Some("http://trino-coordinator:8080"),
                },
                ConfigField {
                    key: "auth.type",
                    label: "Auth type",
                    description: "Authentication mechanism. Choose 'basic' for username/password or 'bearer' for a JWT/OAuth2 token.",
                    field_type: FieldType::Text,
                    required: false,
                    example: Some("basic"),
                },
                ConfigField {
                    key: "auth.username",
                    label: "Username",
                    description: "Basic auth username (required when auth.type = basic).",
                    field_type: FieldType::Text,
                    required: false,
                    example: Some("admin"),
                },
                ConfigField {
                    key: "auth.password",
                    label: "Password",
                    description: "Basic auth password.",
                    field_type: FieldType::Secret,
                    required: false,
                    example: None,
                },
                ConfigField {
                    key: "auth.token",
                    label: "Bearer token",
                    description: "JWT or OAuth2 bearer token (required when auth.type = bearer).",
                    field_type: FieldType::Secret,
                    required: false,
                    example: None,
                },
                ConfigField {
                    key: "tls.insecureSkipVerify",
                    label: "Skip TLS verification",
                    description: "Disable TLS certificate verification. Use only in development.",
                    field_type: FieldType::Boolean,
                    required: false,
                    example: Some("false"),
                },
            ],
        },

        // ── DuckDB ───────────────────────────────────────────────────────────
        EngineDescriptor {
            engine_key: "duckDb",
            display_name: "DuckDB",
            description: "Embedded in-process OLAP database. No network endpoint required.",
            hex: "FCC021",
            connection_type: ConnectionType::Embedded,
            default_port: None,
            endpoint_example: None,
            supported_auth: vec![],
            implemented: true,
            config_fields: vec![
                ConfigField {
                    key: "databasePath",
                    label: "Database path",
                    description: "Path to the DuckDB database file. Omit (or leave empty) for an in-memory database.",
                    field_type: FieldType::Path,
                    required: false,
                    example: Some("/data/analytics.duckdb"),
                },
            ],
        },

        // ── StarRocks ────────────────────────────────────────────────────────
        EngineDescriptor {
            engine_key: "starRocks",
            display_name: "StarRocks",
            description: "High-performance OLAP database. Connects via the MySQL wire protocol.",
            hex: "EF4444",
            connection_type: ConnectionType::MySqlWire,
            default_port: Some(9030),
            endpoint_example: Some("mysql://starrocks-fe:9030"),
            supported_auth: vec![AuthType::Basic],
            implemented: true,
            config_fields: vec![
                ConfigField {
                    key: "endpoint",
                    label: "Endpoint",
                    description: "MySQL-protocol connection URL for the StarRocks front-end node.",
                    field_type: FieldType::Url,
                    required: true,
                    example: Some("mysql://starrocks-fe:9030"),
                },
                ConfigField {
                    key: "auth.type",
                    label: "Auth type",
                    description: "Must be 'basic' for StarRocks (username + password).",
                    field_type: FieldType::Text,
                    required: false,
                    example: Some("basic"),
                },
                ConfigField {
                    key: "auth.username",
                    label: "Username",
                    description: "MySQL username for the StarRocks connection.",
                    field_type: FieldType::Text,
                    required: false,
                    example: Some("root"),
                },
                ConfigField {
                    key: "auth.password",
                    label: "Password",
                    description: "MySQL password.",
                    field_type: FieldType::Secret,
                    required: false,
                    example: None,
                },
            ],
        },

        // ── ClickHouse ───────────────────────────────────────────────────────
        EngineDescriptor {
            engine_key: "clickHouse",
            display_name: "ClickHouse",
            description: "Real-time OLAP database. Connects via the ClickHouse HTTP interface.",
            hex: "FFCC01",
            connection_type: ConnectionType::Http,
            default_port: Some(8123),
            endpoint_example: Some("http://clickhouse:8123"),
            supported_auth: vec![AuthType::Basic],
            implemented: false,
            config_fields: vec![
                ConfigField {
                    key: "endpoint",
                    label: "Endpoint",
                    description: "HTTP base URL of the ClickHouse server.",
                    field_type: FieldType::Url,
                    required: true,
                    example: Some("http://clickhouse:8123"),
                },
                ConfigField {
                    key: "auth.type",
                    label: "Auth type",
                    description: "Must be 'basic' for ClickHouse (username + password).",
                    field_type: FieldType::Text,
                    required: false,
                    example: Some("basic"),
                },
                ConfigField {
                    key: "auth.username",
                    label: "Username",
                    description: "ClickHouse username.",
                    field_type: FieldType::Text,
                    required: false,
                    example: Some("default"),
                },
                ConfigField {
                    key: "auth.password",
                    label: "Password",
                    description: "ClickHouse password.",
                    field_type: FieldType::Secret,
                    required: false,
                    example: None,
                },
                ConfigField {
                    key: "tls.insecureSkipVerify",
                    label: "Skip TLS verification",
                    description: "Disable TLS certificate verification. Use only in development.",
                    field_type: FieldType::Boolean,
                    required: false,
                    example: Some("false"),
                },
            ],
        },
    ]
}

/// Returns the descriptor for a given engine config variant.
pub fn descriptor_for(engine: &EngineConfig) -> Option<&'static EngineDescriptor> {
    let key = engine_key(engine);
    // Walk the registry each time — the list is tiny (4 entries).
    // We leak once so we can return a &'static reference without a global lazy.
    all_descriptors().iter().find(|d| d.engine_key == key)
}

fn engine_key(engine: &EngineConfig) -> &'static str {
    match engine {
        EngineConfig::Trino => "trino",
        EngineConfig::DuckDb => "duckDb",
        EngineConfig::StarRocks => "starRocks",
        EngineConfig::ClickHouse => "clickHouse",
    }
}

fn all_descriptors() -> &'static Vec<EngineDescriptor> {
    use std::sync::OnceLock;
    static REGISTRY: OnceLock<Vec<EngineDescriptor>> = OnceLock::new();
    REGISTRY.get_or_init(engine_descriptors)
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

/// Validates a single cluster's configuration against the engine registry.
/// Returns a list of human-readable error messages; empty = valid.
pub fn validate_cluster_config(cluster_name: &str, config: &ClusterConfig) -> Vec<String> {
    let Some(engine) = &config.engine else {
        return vec![format!(
            "cluster '{cluster_name}': missing required 'engine' field"
        )];
    };

    let Some(desc) = descriptor_for(engine) else {
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
            "cluster '{cluster_name}': 'databasePath' is only applicable to DuckDB, not '{}'",
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
        };
        if !has_auth_type {
            let auth_label = match auth {
                ClusterAuth::Basic { .. } => "basic",
                ClusterAuth::Bearer { .. } => "bearer",
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
// Convenience helpers on EngineDescriptor
// ---------------------------------------------------------------------------

impl EngineDescriptor {
    pub fn requires_endpoint(&self) -> bool {
        self.connection_type != ConnectionType::Embedded
    }

    pub fn supports_tls(&self) -> bool {
        self.connection_type == ConnectionType::Http
    }

    pub fn supports_database_path(&self) -> bool {
        self.connection_type == ConnectionType::Embedded
    }
}
