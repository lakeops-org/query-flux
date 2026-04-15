use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::tags::QueryTags;

/// Protocol-agnostic session metadata that travels with a query from the frontend.
///
/// Each frontend extracts the common fields (`user`, `database`, `tags`) at connection
/// time and places all remaining protocol-specific key-value data into `extra`.
/// Key conventions for `extra`:
/// - Trino / ClickHouse HTTP frontends: HTTP header names (lowercase) → values
/// - Postgres wire: startup parameter names → values
/// - MySQL wire: session variable names → values
/// - Snowflake HTTP: session variable names → values
///
/// Used by:
/// - Routers: inspect `user`, `database`, `tags`, or raw `extra` entries
/// - CatalogProvider: extract catalog/database hints for schema lookup
/// - Engine adapters: forward relevant auth/session info to the backend
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SessionContext {
    /// Resolved user identity (extracted at connection time by the frontend).
    pub user: Option<String>,
    /// Target database/catalog hint for routing and catalog providers.
    pub database: Option<String>,
    /// Query tags for routing and metrics.
    pub tags: QueryTags,
    /// Protocol-specific key-value data. Frontends own the key conventions.
    pub extra: HashMap<String, String>,
}

impl SessionContext {
    /// Return the pre-extracted query tags for this session.
    pub fn tags(&self) -> &QueryTags {
        &self.tags
    }

    /// Extract the user identity.
    pub fn user(&self) -> Option<&str> {
        self.user.as_deref()
    }

    /// Extract the target database/catalog hint.
    pub fn database(&self) -> Option<&str> {
        self.database.as_deref()
    }

    /// Extract the client source/application name (used for routing and metrics).
    ///
    /// Checks `extra` for well-known keys in precedence order:
    /// `x-trino-source` (Trino HTTP), `application_name` (Postgres/MySQL/Snowflake),
    /// `client_name` (ClickHouse).
    pub fn client_source(&self) -> Option<&str> {
        self.extra
            .get("x-trino-source")
            .or_else(|| self.extra.get("application_name"))
            .or_else(|| self.extra.get("client_name"))
            .map(|s| s.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn session(
        user: Option<&str>,
        database: Option<&str>,
        extra: &[(&str, &str)],
    ) -> SessionContext {
        SessionContext {
            user: user.map(str::to_string),
            database: database.map(str::to_string),
            extra: extra
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            ..Default::default()
        }
    }

    #[test]
    fn default_is_empty() {
        let s = SessionContext::default();
        assert_eq!(s.user(), None);
        assert_eq!(s.database(), None);
        assert!(s.tags().is_empty());
        assert!(s.extra.is_empty());
        assert_eq!(s.client_source(), None);
    }

    #[test]
    fn user_and_database_accessors() {
        let s = session(Some("alice"), Some("tpch"), &[]);
        assert_eq!(s.user(), Some("alice"));
        assert_eq!(s.database(), Some("tpch"));
    }

    #[test]
    fn client_source_trino_header_wins() {
        // x-trino-source takes precedence over application_name and client_name.
        let s = session(
            None,
            None,
            &[
                ("x-trino-source", "my-app"),
                ("application_name", "other"),
                ("client_name", "also-other"),
            ],
        );
        assert_eq!(s.client_source(), Some("my-app"));
    }

    #[test]
    fn client_source_application_name_fallback() {
        let s = session(None, None, &[("application_name", "dbt")]);
        assert_eq!(s.client_source(), Some("dbt"));
    }

    #[test]
    fn client_source_client_name_fallback() {
        let s = session(None, None, &[("client_name", "clickhouse-client")]);
        assert_eq!(s.client_source(), Some("clickhouse-client"));
    }

    #[test]
    fn client_source_none_when_no_known_key() {
        let s = session(None, None, &[("x-custom-header", "value")]);
        assert_eq!(s.client_source(), None);
    }

    #[test]
    fn extra_lookup_is_case_sensitive() {
        // Frontends are responsible for normalising keys (e.g. lowercase headers).
        // SessionContext itself does no case folding.
        let s = session(None, None, &[("X-Trino-Source", "app")]);
        assert_eq!(s.client_source(), None); // uppercase key is not matched
    }
}
