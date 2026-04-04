//! Build a [`snowflake_connector_rs::SnowflakeClient`] pointed at fakesnow (same defaults as
//! [`crate::harness::TestHarness`] Snowflake cluster config).

use std::time::Duration;

use anyhow::Context;
use snowflake_connector_rs::{
    SnowflakeAuthMethod, SnowflakeClient, SnowflakeClientConfig, SnowflakeEndpointConfig,
    SnowflakeQueryConfig, SnowflakeSession, SnowflakeSessionConfig,
};
use url::Url;

/// Matches [`crate::harness`] / docker `FAKESNOW_URL` default.
pub fn fakesnow_http_url() -> String {
    std::env::var("FAKESNOW_URL").unwrap_or_else(|_| "http://localhost:18085".to_string())
}

/// Login used by the harness `SnowflakeAdapter::try_from_cluster_config` fakesnow block.
pub fn fakesnow_login() -> (&'static str, &'static str, &'static str) {
    ("fake", "snow", "fakesnow")
}

/// New authenticated session against fakesnow.
pub async fn fakesnow_session() -> anyhow::Result<SnowflakeSession> {
    let base = fakesnow_http_url();
    let url = Url::parse(&base).with_context(|| format!("invalid FAKESNOW_URL: {base}"))?;
    let (user, pass, account) = fakesnow_login();

    let session_cfg = SnowflakeSessionConfig::default();
    let query_cfg = SnowflakeQueryConfig::default()
        .with_async_query_completion_timeout(Duration::from_secs(120));

    let cfg = SnowflakeClientConfig::new(user, account, SnowflakeAuthMethod::Password(pass.into()))
        .with_session(session_cfg)
        .with_query(query_cfg)
        .with_endpoint(SnowflakeEndpointConfig::custom_base_url(url));

    let client = SnowflakeClient::new(cfg).map_err(|e| anyhow::anyhow!("{e}"))?;
    client
        .create_session()
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))
}
