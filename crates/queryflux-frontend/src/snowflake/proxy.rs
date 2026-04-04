use std::collections::HashMap;
use std::time::Duration;

use bytes::Bytes;
use reqwest::{Client, Method, Response};

/// Parameters for [`SnowflakeProxy::forward`].
pub struct SnowflakeForward<'a> {
    pub method: Method,
    pub warehouse_base_url: &'a str,
    pub path: &'a str,
    pub sf_token: Option<&'a str>,
    pub query_string: Option<&'a str>,
    pub body: Option<Bytes>,
    pub passthrough_headers: HashMap<String, String>,
}

/// Shared HTTP client for forwarding Snowflake wire-protocol requests to backend warehouses
/// with service-account token injection.
pub struct SnowflakeProxy {
    client: Client,
}

impl Default for SnowflakeProxy {
    fn default() -> Self {
        Self::new()
    }
}

impl SnowflakeProxy {
    pub fn new() -> Self {
        Self {
            client: Client::builder()
                .timeout(Duration::from_secs(300))
                .connect_timeout(Duration::from_secs(30))
                .build()
                .expect("failed to build SnowflakeProxy reqwest client"),
        }
    }

    /// Forward a request to a Snowflake warehouse.
    ///
    /// When `sf_token` is `Some`, injects `Authorization: Snowflake Token="{sf_token}"`,
    /// replacing any Authorization header in `passthrough_headers`.
    /// When `sf_token` is `None` (e.g. during login), no Authorization header is injected.
    pub async fn forward(&self, req: SnowflakeForward<'_>) -> reqwest::Result<Response> {
        let url = match req.query_string {
            Some(qs) if !qs.is_empty() => {
                format!("{}{}?{qs}", req.warehouse_base_url, req.path)
            }
            _ => format!("{}{}", req.warehouse_base_url, req.path),
        };

        let mut http = self.client.request(req.method, &url);

        // Pass through headers, skipping Authorization (we inject below if token provided).
        for (k, v) in &req.passthrough_headers {
            if k.to_lowercase() != "authorization" {
                http = http.header(k.as_str(), v.as_str());
            }
        }

        if let Some(token) = req.sf_token {
            http = http.header("Authorization", format!("Snowflake Token=\"{token}\""));
            http = http.header("X-Snowflake-Authorization-Token-Type", "TOKEN");
        }

        if let Some(body_bytes) = req.body {
            http = http
                .header("Content-Type", "application/json")
                .body(body_bytes);
        }

        http.send().await
    }
}
