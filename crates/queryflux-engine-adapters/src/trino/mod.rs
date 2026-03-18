pub mod api;

use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use queryflux_core::{
    catalog::TableSchema,
    error::{QueryFluxError, Result},
    query::{
        BackendQueryId, ClusterGroupName, ClusterName, ColumnDef, EngineType, QueryExecution,
        QueryPollResult, QueryStats,
    },
    session::SessionContext,
};
use reqwest::{Client, StatusCode};
use tracing::{debug, warn};

use crate::EngineAdapterTrait;
use api::TrinoResponse;

/// Trino HTTP adapter.
///
/// For Trino→Trino transparent forwarding, `submit_query` and `poll_query` return
/// `QueryExecution::Async { initial_body: Some(...) }` and `QueryPollResult::Raw { ... }`
/// respectively. The Trino HTTP frontend rewrites nextUri and returns raw bytes directly.
pub struct TrinoAdapter {
    pub cluster_name: ClusterName,
    pub group_name: ClusterGroupName,
    pub endpoint: String,
    http_client: Client,
}

impl TrinoAdapter {
    pub fn new(
        cluster_name: ClusterName,
        group_name: ClusterGroupName,
        endpoint: String,
        tls_skip_verify: bool,
    ) -> Self {
        let http_client = Client::builder()
            .timeout(Duration::from_secs(30))
            .danger_accept_invalid_certs(tls_skip_verify)
            .build()
            .expect("Failed to build HTTP client");

        Self { cluster_name, group_name, endpoint, http_client }
    }

    fn trino_url(&self, path: &str) -> String {
        format!("{}{}", self.endpoint.trim_end_matches('/'), path)
    }

    /// Forward X-Trino-* headers from a SessionContext to a reqwest request.
    fn apply_session_headers(
        &self,
        mut builder: reqwest::RequestBuilder,
        session: &SessionContext,
    ) -> reqwest::RequestBuilder {
        if let SessionContext::TrinoHttp { headers } = session {
            for (k, v) in headers {
                if k.to_lowercase().starts_with("x-trino-") || k.to_lowercase() == "authorization" {
                    builder = builder.header(k, v);
                }
            }
        }
        builder
    }
}

#[async_trait]
impl EngineAdapterTrait for TrinoAdapter {
    async fn submit_query(&self, sql: &str, session: &SessionContext) -> Result<QueryExecution> {
        let url = self.trino_url("/v1/statement");
        debug!(cluster = %self.cluster_name, url = %url, "Submitting query to Trino");

        let mut req = self.http_client.post(&url).body(sql.to_string());
        req = self.apply_session_headers(req, session);

        let resp = req.send().await.map_err(|e| {
            QueryFluxError::Engine(format!("Trino submit failed: {e}"))
        })?;

        if resp.status() == StatusCode::UNAUTHORIZED {
            let www_auth = resp
                .headers()
                .get("www-authenticate")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("")
                .to_string();
            return Err(QueryFluxError::Engine(format!(
                "Trino returned 401 Unauthorized: {}",
                www_auth
            )));
        }

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(QueryFluxError::Engine(format!(
                "Trino submit returned {status}: {body}"
            )));
        }

        let body_bytes = resp.bytes().await.map_err(|e| {
            QueryFluxError::Engine(format!("Failed to read Trino response body: {e}"))
        })?;

        let trino_resp: TrinoResponse = serde_json::from_slice(&body_bytes).map_err(|e| {
            QueryFluxError::Engine(format!("Failed to parse Trino response: {e}"))
        })?;

        let backend_query_id = BackendQueryId(trino_resp.id.clone());
        let next_uri = trino_resp.next_uri.clone();

        Ok(QueryExecution::Async {
            backend_query_id,
            next_uri,
            initial_body: Some(body_bytes),
        })
    }

    async fn poll_query(
        &self,
        _backend_id: &BackendQueryId,
        next_uri: Option<&str>,
    ) -> Result<QueryPollResult> {
        let uri = match next_uri {
            Some(u) => u,
            None => return Ok(QueryPollResult::Failed {
                message: "poll_query called with no nextUri".to_string(),
                error_code: None,
            }),
        };

        debug!(uri = %uri, "Polling Trino");

        let resp = self.http_client.get(uri).send().await.map_err(|e| {
            QueryFluxError::Engine(format!("Trino poll GET failed: {e}"))
        })?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(QueryFluxError::Engine(format!(
                "Trino poll returned {status}: {body}"
            )));
        }

        let body_bytes = resp.bytes().await.map_err(|e| {
            QueryFluxError::Engine(format!("Failed to read Trino poll body: {e}"))
        })?;

        let trino_resp: TrinoResponse = serde_json::from_slice(&body_bytes).map_err(|e| {
            QueryFluxError::Engine(format!("Failed to parse Trino poll response: {e}"))
        })?;

        // Check for error in the response body (Trino embeds errors in 200 responses)
        if let Some(err) = &trino_resp.error {
            return Ok(QueryPollResult::Failed {
                message: err.message.clone(),
                error_code: err.error_name.clone(),
            });
        }

        let next_uri = trino_resp.next_uri.clone();
        Ok(QueryPollResult::Raw { body: body_bytes, next_uri })
    }

    async fn cancel_query(&self, _backend_id: &BackendQueryId) -> Result<()> {
        // Trino cancel: DELETE the nextUri. We'd need to look it up from persistence.
        // For now this is handled by the frontend which has the stored next_uri.
        Ok(())
    }

    async fn health_check(&self) -> bool {
        let url = self.trino_url("/v1/info");
        self.http_client.get(&url).send().await
            .map(|r| r.status().is_success())
            .unwrap_or(false)
    }

    fn engine_type(&self) -> EngineType {
        EngineType::Trino
    }

    fn supports_async(&self) -> bool {
        true
    }

    // --- Catalog discovery ---

    async fn list_catalogs(&self) -> Result<Vec<String>> {
        self.run_show_query("SHOW CATALOGS").await
    }

    async fn list_databases(&self, catalog: &str) -> Result<Vec<String>> {
        self.run_show_query(&format!("SHOW SCHEMAS IN {}", catalog)).await
    }

    async fn list_tables(&self, catalog: &str, database: &str) -> Result<Vec<String>> {
        self.run_show_query(&format!("SHOW TABLES IN {}.{}", catalog, database)).await
    }

    async fn describe_table(
        &self,
        catalog: &str,
        database: &str,
        table: &str,
    ) -> Result<Option<TableSchema>> {
        let sql = format!("DESCRIBE {}.{}.{}", catalog, database, table);
        let session = SessionContext::TrinoHttp {
            headers: HashMap::from([
                ("x-trino-user".to_string(), "queryflux-catalog-discovery".to_string()),
            ]),
        };
        let execution = self.submit_query(&sql, &session).await?;
        if let QueryExecution::Async { initial_body: Some(body), .. } = execution {
            let resp: TrinoResponse = serde_json::from_slice(&body).map_err(|e| {
                QueryFluxError::Catalog(format!("DESCRIBE parse failed: {e}"))
            })?;
            let columns = parse_describe_result(&resp, catalog, database, table);
            return Ok(Some(columns));
        }
        Ok(None)
    }
}

impl TrinoAdapter {
    async fn run_show_query(&self, sql: &str) -> Result<Vec<String>> {
        let session = SessionContext::TrinoHttp {
            headers: HashMap::from([
                ("x-trino-user".to_string(), "queryflux-catalog-discovery".to_string()),
            ]),
        };
        let execution = self.submit_query(sql, &session).await?;
        if let QueryExecution::Async { initial_body: Some(body), .. } = execution {
            let resp: TrinoResponse = serde_json::from_slice(&body).map_err(|e| {
                QueryFluxError::Catalog(format!("SHOW query parse failed: {e}"))
            })?;
            if let Some(data) = resp.data {
                if let Some(rows) = data.as_array() {
                    return Ok(rows.iter()
                        .filter_map(|row| row.as_array()?.first()?.as_str().map(String::from))
                        .collect());
                }
            }
        }
        Ok(vec![])
    }
}

fn parse_describe_result(
    resp: &TrinoResponse,
    catalog: &str,
    database: &str,
    table: &str,
) -> TableSchema {
    let columns = resp.data.as_ref()
        .and_then(|d| d.as_array())
        .map(|rows| {
            rows.iter().filter_map(|row| {
                let arr = row.as_array()?;
                Some(queryflux_core::catalog::ColumnDef {
                    name: arr.first()?.as_str()?.to_string(),
                    data_type: arr.get(1)?.as_str()?.to_uppercase(),
                    nullable: arr.get(2).and_then(|v| v.as_str()) != Some("not null"),
                })
            }).collect()
        })
        .unwrap_or_default();

    TableSchema {
        catalog: catalog.to_string(),
        database: database.to_string(),
        table: table.to_string(),
        columns,
    }
}
