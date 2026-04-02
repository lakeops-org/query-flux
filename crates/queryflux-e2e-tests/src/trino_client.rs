/// Minimal Trino HTTP client for E2E tests.
///
/// Sends `POST /v1/statement`, then polls `nextUri` until the query finishes.
/// Returns the complete column list and all data rows.
use std::time::Duration;

use anyhow::{anyhow, Result};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use serde::Deserialize;
use serde_json::Value;

/// A fully-resolved query result (all pages accumulated).
#[derive(Debug)]
pub struct QueryResult {
    pub columns: Vec<ColumnInfo>,
    /// Rows as JSON arrays; each element matches the corresponding column.
    pub rows: Vec<Vec<Value>>,
    pub error: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ColumnInfo {
    pub name: String,
    #[serde(rename = "type")]
    pub col_type: String,
}

// Subset of the Trino response we need for the poll loop.
#[derive(Debug, Deserialize)]
struct TrinoResponse {
    #[serde(rename = "nextUri")]
    next_uri: Option<String>,
    /// Present on Trino pages; kept for JSON shape (poll loop uses `nextUri` only).
    #[serde(default)]
    #[allow(dead_code)]
    stats: Option<TrinoStats>,
    columns: Option<Vec<ColumnInfo>>,
    data: Option<Vec<Vec<Value>>>,
    error: Option<Value>,
}

#[derive(Debug, Deserialize, Default)]
struct TrinoStats {
    #[allow(dead_code)]
    state: String,
}

pub struct TrinoClient {
    http: reqwest::Client,
    base_url: String,
}

impl TrinoClient {
    pub fn new(base_url: &str) -> Self {
        Self {
            http: reqwest::Client::builder()
                // StarRocks + external Iceberg can be much slower than Trino for full-table
                // aggregations; parallel tests also contend on one FE. Keep generous.
                .timeout(Duration::from_secs(300))
                .connect_timeout(Duration::from_secs(30))
                .build()
                .expect("Failed to build reqwest client"),
            base_url: base_url.trim_end_matches('/').to_string(),
        }
    }

    /// Execute `sql` and wait for completion.
    /// `extra_headers` lets callers add routing headers (e.g. `X-Qf-Group`).
    pub async fn execute(&self, sql: &str, extra_headers: &[(&str, &str)]) -> Result<QueryResult> {
        let mut headers = HeaderMap::new();
        headers.insert("X-Trino-User", HeaderValue::from_static("test"));
        for (name, value) in extra_headers {
            headers.insert(
                HeaderName::from_bytes(name.as_bytes())?,
                HeaderValue::from_str(value)?,
            );
        }

        // Submit query
        let resp = self
            .http
            .post(format!("{}/v1/statement", self.base_url))
            .headers(headers.clone())
            .body(sql.to_string())
            .send()
            .await?;

        if !resp.status().is_success() {
            return Err(anyhow!("POST /v1/statement returned {}", resp.status()));
        }

        let mut page: TrinoResponse = resp.json().await?;

        let mut columns: Vec<ColumnInfo> = Vec::new();
        let mut all_rows: Vec<Vec<Value>> = Vec::new();

        // Poll until terminal state
        loop {
            if let Some(cols) = page.columns.take() {
                if columns.is_empty() {
                    columns = cols;
                }
            }
            if let Some(rows) = page.data.take() {
                all_rows.extend(rows);
            }
            if let Some(err) = page.error.take() {
                let msg = err
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown error")
                    .to_string();
                return Ok(QueryResult {
                    columns,
                    rows: all_rows,
                    error: Some(msg),
                });
            }

            let next = page.next_uri.take();

            // Trino may include `stats.state: FINISHED` while `nextUri` is still set (one more poll
            // may be required). Stop only when there is no next page — matches Trino clients and
            // ensures QueryFlux sees a terminal GET so it can record_query.
            if next.is_none() {
                break;
            }

            let next_url = next.unwrap();
            tokio::time::sleep(Duration::from_millis(100)).await;

            let resp = self
                .http
                .get(&next_url)
                .headers(headers.clone())
                .send()
                .await?;

            if !resp.status().is_success() {
                // Non-2xx on a poll means the query failed or was cleaned up.
                let msg = format!("poll returned HTTP {}", resp.status());
                return Ok(QueryResult {
                    columns,
                    rows: all_rows,
                    error: Some(msg),
                });
            }
            page = resp.json().await?;
        }

        Ok(QueryResult {
            columns,
            rows: all_rows,
            error: None,
        })
    }

    /// Shorthand: execute with a single routing header `X-Qf-Group: {group}`.
    pub async fn execute_on(&self, sql: &str, group: &str) -> Result<QueryResult> {
        self.execute(sql, &[("x-qf-group", group)]).await
    }
}
