pub mod yaml;

use async_trait::async_trait;
use queryflux_core::{config::ProxyConfig, error::Result};

/// Loads and watches proxy configuration.
///
/// The same proxy binary supports multiple config sources:
/// - `YamlFileConfigProvider`: reads a YAML file, watches for changes
/// - `PostgresConfigProvider` (Phase 2): stores config in DB, hot-reloads via LISTEN/NOTIFY
/// - `EnvOverrideConfigProvider`: wraps any provider, overrides fields from env vars
#[async_trait]
pub trait ConfigProvider: Send + Sync {
    /// Load the current configuration.
    async fn load(&self) -> Result<ProxyConfig>;
}
