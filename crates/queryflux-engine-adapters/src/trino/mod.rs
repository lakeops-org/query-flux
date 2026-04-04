pub mod api;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{
    ArrayRef, BooleanBuilder, Float32Builder, Float64Builder, Int16Builder, Int32Builder,
    Int64Builder, Int8Builder, StringBuilder,
};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use queryflux_core::{
    catalog::TableSchema,
    config::{ClusterAuth, ClusterConfig},
    error::{QueryFluxError, Result},
    query::{
        BackendQueryId, ClusterGroupName, ClusterName, EngineType, QueryEngineStats,
        QueryExecution, QueryPollResult,
    },
    session::SessionContext,
    tags::QueryTags,
};
use reqwest::{Client, StatusCode};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tracing::debug;

use crate::EngineAdapterTrait;
use api::TrinoResponse;

use queryflux_core::engine_registry::{
    AuthType, ConfigField, ConnectionType, EngineDescriptor, FieldType,
};

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
    auth: Option<ClusterAuth>,
}

impl TrinoAdapter {
    pub fn new(
        cluster_name: ClusterName,
        group_name: ClusterGroupName,
        endpoint: String,
        tls_skip_verify: bool,
        auth: Option<ClusterAuth>,
    ) -> Self {
        let http_client = Client::builder()
            .timeout(Duration::from_secs(30))
            .danger_accept_invalid_certs(tls_skip_verify)
            .build()
            .expect("Failed to build HTTP client");

        Self {
            cluster_name,
            group_name,
            endpoint,
            http_client,
            auth,
        }
    }

    /// Build from persisted / YAML [`ClusterConfig`] (Trino-specific field usage).
    pub fn try_from_cluster_config(
        cluster_name: ClusterName,
        group_name: ClusterGroupName,
        cfg: &ClusterConfig,
        cluster_name_str: &str,
    ) -> Result<Self> {
        let endpoint = cfg.endpoint.clone().ok_or_else(|| {
            QueryFluxError::Engine(format!("cluster '{cluster_name_str}': missing endpoint"))
        })?;
        let tls_skip = cfg
            .tls
            .as_ref()
            .map(|t| t.insecure_skip_verify)
            .unwrap_or(false);
        Ok(Self::new(
            cluster_name,
            group_name,
            endpoint,
            tls_skip,
            cfg.auth.clone(),
        ))
    }

    /// Build from a DB config JSON blob (bypasses the `ClusterConfig` god struct).
    pub fn try_from_config_json(
        cluster_name: ClusterName,
        group_name: ClusterGroupName,
        json: &serde_json::Value,
        cluster_name_str: &str,
    ) -> Result<Self> {
        use queryflux_core::engine_registry::{json_bool, json_str, parse_auth_from_config_json};

        let endpoint = json_str(json, "endpoint").ok_or_else(|| {
            QueryFluxError::Engine(format!("cluster '{cluster_name_str}': missing endpoint"))
        })?;
        let tls_skip = json_bool(json, "tlsInsecureSkipVerify");
        let auth = parse_auth_from_config_json(json).map_err(|e| {
            QueryFluxError::Engine(format!("cluster '{cluster_name_str}': invalid auth ({e})"))
        })?;
        Ok(Self::new(
            cluster_name,
            group_name,
            endpoint,
            tls_skip,
            auth,
        ))
    }

    /// Apply cluster-level auth credentials to a request builder.
    fn apply_cluster_auth(&self, builder: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.auth {
            Some(ClusterAuth::Basic { username, password }) => {
                builder.basic_auth(username, Some(password))
            }
            Some(ClusterAuth::Bearer { token }) => builder.bearer_auth(token),
            // AccessKey, KeyPair, RoleArn are not used by TrinoAdapter.
            Some(ClusterAuth::AccessKey { .. })
            | Some(ClusterAuth::KeyPair { .. })
            | Some(ClusterAuth::RoleArn { .. }) => builder,
            None => builder,
        }
    }

    fn trino_url(&self, path: &str) -> String {
        format!("{}{}", self.endpoint.trim_end_matches('/'), path)
    }

    /// Forward X-Trino-* headers from a SessionContext to a reqwest request,
    /// then overlay effective query tags.
    ///
    /// Tag forwarding:
    /// - All effective tags are encoded into the `X-Trino-Client-Tags` header
    ///   (key-only tags as bare keys, key/value tags as `key=value` pairs).
    ///
    /// Effective tags always win: they overwrite any client-supplied tag headers so
    /// that group default_tags are always reflected in the backend's query metadata.
    fn apply_session_headers(
        &self,
        mut builder: reqwest::RequestBuilder,
        session: &SessionContext,
        tags: &QueryTags,
    ) -> reqwest::RequestBuilder {
        if let SessionContext::TrinoHttp { headers, .. } = session {
            for (k, v) in headers {
                let k_lower = k.to_lowercase();
                // X-Trino-Client-Tags and X-Trino-Session are rebuilt below so that
                // effective_tags always win. All other X-Trino-* headers pass through.
                if k_lower == "x-trino-client-tags" || k_lower == "x-trino-session" {
                    continue;
                }
                if k_lower.starts_with("x-trino-") || k_lower == "authorization" {
                    builder = builder.header(k, v);
                }
            }
        }

        // Rebuild X-Trino-Session: keep all non-tag properties from the client, then
        // the effective tags own X-Trino-Client-Tags (overwriting whatever the client sent).
        //
        // X-Trino-Session is a comma-separated list of `name=value` pairs where values
        // may be percent-encoded. We filter out `query_tag` and `query_tags` keys so
        // unrelated session properties (join_distribution_type, query_max_run_time, …)
        // are always preserved.
        if let SessionContext::TrinoHttp { headers, .. } = session {
            if let Some(session_props) = headers.get("x-trino-session") {
                let retained: Vec<&str> = session_props
                    .split(',')
                    .map(str::trim)
                    .filter(|prop| {
                        let key = prop.split('=').next().unwrap_or("").trim();
                        key != "query_tag" && key != "query_tags"
                    })
                    .filter(|s| !s.is_empty())
                    .collect();
                if !retained.is_empty() {
                    builder = builder.header("X-Trino-Session", retained.join(","));
                }
            }
        }

        // All effective tags → X-Trino-Client-Tags.
        // Key-only tags: "batch" → "batch"
        // Key-value tags: "team" => Some("eng") → "team:eng"
        if tags.is_empty() {
            // No effective tags — forward the original client-tags header unchanged.
            if let SessionContext::TrinoHttp { headers, .. } = session {
                if let Some(v) = headers.get("x-trino-client-tags") {
                    builder = builder.header("X-Trino-Client-Tags", v);
                }
            }
        } else {
            let client_tags: Vec<String> = tags
                .iter()
                .map(|(k, v)| match v {
                    None => k.clone(),
                    Some(val) => format!("{k}:{val}"),
                })
                .collect();
            builder = builder.header("X-Trino-Client-Tags", client_tags.join(","));
        }
        builder
    }
}

#[async_trait]
impl EngineAdapterTrait for TrinoAdapter {
    async fn submit_query(
        &self,
        sql: &str,
        session: &SessionContext,
        _credentials: &queryflux_auth::QueryCredentials,
        tags: &QueryTags,
    ) -> Result<QueryExecution> {
        let url = self.trino_url("/v1/statement");
        debug!(cluster = %self.cluster_name, url = %url, "Submitting query to Trino");

        let mut req = self.http_client.post(&url).body(sql.to_string());
        req = self.apply_cluster_auth(req);
        req = self.apply_session_headers(req, session, tags);

        let resp = req
            .send()
            .await
            .map_err(|e| QueryFluxError::Engine(format!("Trino submit failed: {e}")))?;

        if resp.status() == StatusCode::UNAUTHORIZED {
            let www_auth = resp
                .headers()
                .get("www-authenticate")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("")
                .to_string();
            return Err(QueryFluxError::Engine(format!(
                "Trino returned 401 Unauthorized: {www_auth}"
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

        let trino_resp: TrinoResponse = serde_json::from_slice(&body_bytes)
            .map_err(|e| QueryFluxError::Engine(format!("Failed to parse Trino response: {e}")))?;

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
            None => {
                return Ok(QueryPollResult::Failed {
                    message: "poll_query called with no nextUri".to_string(),
                    error_code: None,
                })
            }
        };

        debug!(uri = %uri, "Polling Trino");

        let resp = self
            .apply_cluster_auth(self.http_client.get(uri))
            .send()
            .await
            .map_err(|e| QueryFluxError::Engine(format!("Trino poll GET failed: {e}")))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(QueryFluxError::Engine(format!(
                "Trino poll returned {status}: {body}"
            )));
        }

        let body_bytes = resp
            .bytes()
            .await
            .map_err(|e| QueryFluxError::Engine(format!("Failed to read Trino poll body: {e}")))?;

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

        // Trino may return `stats.state: "FINISHED"` while still including `nextUri` for one more
        // poll. The e2e client (and `trino-rust-client`) stop as soon as they see FINISHED, so our
        // proxy must treat that as terminal too — otherwise we never run `record_query` on the last
        // GET the client actually performs.
        let state = trino_resp.stats.state.as_str();
        let mut next_uri = trino_resp.next_uri.clone();
        if next_uri.is_some() && state.eq_ignore_ascii_case("FINISHED") {
            next_uri = None;
        }

        let engine_stats = if next_uri.is_none() {
            let s = &trino_resp.stats;
            Some(QueryEngineStats {
                engine_elapsed_time_ms: Some(s.elapsed_time_millis),
                cpu_time_ms: Some(s.cpu_time_millis),
                processed_rows: Some(s.processed_rows),
                processed_bytes: Some(s.processed_bytes),
                physical_input_bytes: Some(s.physical_input_bytes),
                peak_memory_bytes: Some(s.peak_memory_bytes),
                spilled_bytes: Some(s.spilled_bytes),
                total_splits: Some(s.total_splits),
            })
        } else {
            None
        };
        Ok(QueryPollResult::Raw {
            body: body_bytes,
            next_uri,
            engine_stats,
        })
    }

    async fn cancel_query(&self, _backend_id: &BackendQueryId) -> Result<()> {
        // Trino cancel: DELETE the nextUri. We'd need to look it up from persistence.
        // For now this is handled by the frontend which has the stored next_uri.
        Ok(())
    }

    async fn health_check(&self) -> bool {
        let url = self.trino_url("/v1/info");
        self.apply_cluster_auth(self.http_client.get(&url))
            .send()
            .await
            .map(|r| r.status().is_success())
            .unwrap_or(false)
    }

    fn engine_type(&self) -> EngineType {
        EngineType::Trino
    }

    fn supports_async(&self) -> bool {
        true
    }

    fn base_url(&self) -> &str {
        &self.endpoint
    }

    async fn execute_as_arrow(
        &self,
        sql: &str,
        session: &SessionContext,
        _credentials: &queryflux_auth::QueryCredentials,
        tags: &QueryTags,
    ) -> Result<crate::ArrowStream> {
        // Submit query — get initial body + first next_uri.
        let execution = self
            .submit_query(
                sql,
                session,
                &queryflux_auth::QueryCredentials::ServiceAccount,
                tags,
            )
            .await?;
        let QueryExecution::Async {
            initial_body,
            next_uri: first_next_uri,
            ..
        } = execution;

        let http_client = self.http_client.clone();
        let auth = self.auth.clone();
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<Result<RecordBatch>>();

        tokio::spawn(async move {
            let mut next_uri = first_next_uri;
            let mut schema: Option<Arc<ArrowSchema>> = None;

            // Process initial body (first page from submit).
            if let Some(body) = initial_body {
                match trino_body_to_batch(&body, &mut schema) {
                    Err(e) => {
                        let _ = tx.send(Err(e));
                        return;
                    }
                    Ok(Some(batch)) => {
                        let _ = tx.send(Ok(batch));
                    }
                    Ok(None) => {}
                }
                // Update next_uri from the initial body.
                if let Ok(resp) = serde_json::from_slice::<TrinoResponse>(&body) {
                    next_uri = resp.next_uri;
                }
            }

            // Poll remaining pages.
            while let Some(uri) = next_uri {
                let req = match &auth {
                    Some(ClusterAuth::Basic { username, password }) => {
                        http_client.get(&uri).basic_auth(username, Some(password))
                    }
                    Some(ClusterAuth::Bearer { token }) => http_client.get(&uri).bearer_auth(token),
                    Some(ClusterAuth::AccessKey { .. })
                    | Some(ClusterAuth::KeyPair { .. })
                    | Some(ClusterAuth::RoleArn { .. }) => http_client.get(&uri),
                    None => http_client.get(&uri),
                };
                let body = match req.send().await {
                    Err(e) => {
                        let _ = tx.send(Err(QueryFluxError::Engine(format!(
                            "Trino poll failed: {e}"
                        ))));
                        return;
                    }
                    Ok(resp) => match resp.bytes().await {
                        Err(e) => {
                            let _ = tx.send(Err(QueryFluxError::Engine(format!(
                                "Trino read body failed: {e}"
                            ))));
                            return;
                        }
                        Ok(b) => b,
                    },
                };

                // Parse next_uri before converting to batch.
                let resp: TrinoResponse = match serde_json::from_slice(&body) {
                    Err(e) => {
                        let _ = tx.send(Err(QueryFluxError::Engine(format!(
                            "Trino parse failed: {e}"
                        ))));
                        return;
                    }
                    Ok(r) => r,
                };
                if let Some(err) = &resp.error {
                    let _ = tx.send(Err(QueryFluxError::Engine(err.message.clone())));
                    return;
                }
                next_uri = resp.next_uri.clone();

                match trino_body_to_batch(&body, &mut schema) {
                    Err(e) => {
                        let _ = tx.send(Err(e));
                        return;
                    }
                    Ok(Some(batch)) => {
                        let _ = tx.send(Ok(batch));
                    }
                    Ok(None) => {}
                }
            }
            // tx dropped here → stream ends.
        });

        Ok(Box::pin(UnboundedReceiverStream::new(rx)))
    }

    /// Trino exposes `GET /v1/cluster` with aggregate running/queued counts.
    async fn fetch_running_query_count(&self) -> Option<u64> {
        #[derive(serde::Deserialize)]
        struct ClusterInfo {
            #[serde(rename = "runningQueries")]
            running_queries: u64,
        }
        let url = self.trino_url("/v1/cluster");
        let resp = self
            .apply_cluster_auth(self.http_client.get(&url))
            .send()
            .await
            .ok()?;
        if !resp.status().is_success() {
            return None;
        }
        resp.json::<ClusterInfo>()
            .await
            .ok()
            .map(|c| c.running_queries)
    }

    // --- Catalog discovery ---

    async fn list_catalogs(&self) -> Result<Vec<String>> {
        self.run_show_query("SHOW CATALOGS").await
    }

    async fn list_databases(&self, catalog: &str) -> Result<Vec<String>> {
        self.run_show_query(&format!("SHOW SCHEMAS IN {catalog}"))
            .await
    }

    async fn list_tables(&self, catalog: &str, database: &str) -> Result<Vec<String>> {
        self.run_show_query(&format!("SHOW TABLES IN {catalog}.{database}"))
            .await
    }

    async fn describe_table(
        &self,
        catalog: &str,
        database: &str,
        table: &str,
    ) -> Result<Option<TableSchema>> {
        let sql = format!("DESCRIBE {catalog}.{database}.{table}");
        let session = SessionContext::TrinoHttp {
            headers: HashMap::from([(
                "x-trino-user".to_string(),
                "queryflux-catalog-discovery".to_string(),
            )]),
            tags: QueryTags::new(),
        };
        let execution = self
            .submit_query(
                &sql,
                &session,
                &queryflux_auth::QueryCredentials::ServiceAccount,
                &QueryTags::new(),
            )
            .await?;
        if let QueryExecution::Async {
            initial_body: Some(body),
            ..
        } = execution
        {
            let resp: TrinoResponse = serde_json::from_slice(&body)
                .map_err(|e| QueryFluxError::Catalog(format!("DESCRIBE parse failed: {e}")))?;
            let columns = parse_describe_result(&resp, catalog, database, table);
            return Ok(Some(columns));
        }
        Ok(None)
    }
}

impl TrinoAdapter {
    async fn run_show_query(&self, sql: &str) -> Result<Vec<String>> {
        let session = SessionContext::TrinoHttp {
            headers: HashMap::from([(
                "x-trino-user".to_string(),
                "queryflux-catalog-discovery".to_string(),
            )]),
            tags: QueryTags::new(),
        };
        let execution = self
            .submit_query(
                sql,
                &session,
                &queryflux_auth::QueryCredentials::ServiceAccount,
                &QueryTags::new(),
            )
            .await?;
        if let QueryExecution::Async {
            initial_body: Some(body),
            ..
        } = execution
        {
            let resp: TrinoResponse = serde_json::from_slice(&body)
                .map_err(|e| QueryFluxError::Catalog(format!("SHOW query parse failed: {e}")))?;
            if let Some(data) = resp.data {
                if let Some(rows) = data.as_array() {
                    return Ok(rows
                        .iter()
                        .filter_map(|row| row.as_array()?.first()?.as_str().map(String::from))
                        .collect());
                }
            }
        }
        Ok(vec![])
    }
}

// ---------------------------------------------------------------------------
// Arrow conversion helpers for execute_as_arrow
// ---------------------------------------------------------------------------

/// Parse a raw Trino response body and yield a RecordBatch if the page has data.
/// Builds/reuses the Arrow schema from column metadata on the first data page.
fn trino_body_to_batch(
    body: &[u8],
    schema: &mut Option<Arc<ArrowSchema>>,
) -> Result<Option<RecordBatch>> {
    let resp: TrinoResponse = serde_json::from_slice(body)
        .map_err(|e| QueryFluxError::Engine(format!("Trino parse failed: {e}")))?;

    if let Some(err) = &resp.error {
        return Err(QueryFluxError::Engine(err.message.clone()));
    }

    // Build schema from the first page that has column metadata.
    if schema.is_none() {
        if let Some(cols) = &resp.columns {
            let fields = trino_columns_to_fields(cols)?;
            *schema = Some(Arc::new(ArrowSchema::new(fields)));
        }
    }

    let data = match &resp.data {
        None => return Ok(None),
        Some(d) => d,
    };
    let rows = match data.as_array() {
        None => return Ok(None),
        Some(r) if r.is_empty() => return Ok(None),
        Some(r) => r,
    };

    let schema = match schema.as_ref() {
        None => return Ok(None), // No schema yet (pending page with data — shouldn't happen)
        Some(s) => s.clone(),
    };

    let batch = trino_rows_to_batch(&schema, rows)?;
    Ok(Some(batch))
}

/// Parse Trino column metadata JSON → Arrow Fields.
fn trino_columns_to_fields(cols: &serde_json::Value) -> Result<Vec<Field>> {
    let arr = cols
        .as_array()
        .ok_or_else(|| QueryFluxError::Engine("Trino columns is not an array".into()))?;
    arr.iter()
        .map(|col| {
            let name = col["name"].as_str().unwrap_or("?").to_string();
            let type_str = col["type"].as_str().unwrap_or("varchar");
            Ok(Field::new(name, trino_type_to_arrow(type_str), true))
        })
        .collect()
}

/// Map a Trino type string to an Arrow DataType.
/// Complex types (array, map, row) degrade to Utf8 (JSON string representation).
fn trino_type_to_arrow(type_str: &str) -> DataType {
    let lower = type_str.to_lowercase();
    match lower.as_str() {
        "boolean" => DataType::Boolean,
        "tinyint" => DataType::Int8,
        "smallint" => DataType::Int16,
        "integer" | "int" => DataType::Int32,
        "bigint" => DataType::Int64,
        "real" => DataType::Float32,
        "double" => DataType::Float64,
        "date" => DataType::Utf8, // store as ISO string for simplicity
        "varbinary" => DataType::Utf8,
        _ if lower.starts_with("varchar") => DataType::Utf8,
        _ if lower.starts_with("char(") => DataType::Utf8,
        _ if lower.starts_with("decimal") => DataType::Utf8,
        _ if lower.starts_with("timestamp") => DataType::Utf8,
        _ if lower.starts_with("time") => DataType::Utf8,
        _ if lower.starts_with("interval") => DataType::Utf8,
        _ if lower.starts_with("array") => DataType::Utf8,
        _ if lower.starts_with("map") => DataType::Utf8,
        _ if lower.starts_with("row") => DataType::Utf8,
        "json" | "uuid" | "ipaddress" | "hyperloglog" => DataType::Utf8,
        _ => DataType::Utf8,
    }
}

/// Build a RecordBatch from Trino JSON rows given an Arrow schema.
fn trino_rows_to_batch(
    schema: &Arc<ArrowSchema>,
    rows: &[serde_json::Value],
) -> Result<RecordBatch> {
    let num_cols = schema.fields().len();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(num_cols);

    for (col_idx, field) in schema.fields().iter().enumerate() {
        let col = build_trino_column(field.data_type(), rows, col_idx)?;
        columns.push(col);
    }

    RecordBatch::try_new(schema.clone(), columns)
        .map_err(|e| QueryFluxError::Engine(format!("RecordBatch build failed: {e}")))
}

fn build_trino_column(
    dt: &DataType,
    rows: &[serde_json::Value],
    col_idx: usize,
) -> Result<ArrayRef> {
    match dt {
        DataType::Boolean => {
            let mut b = BooleanBuilder::with_capacity(rows.len());
            for row in rows {
                match row.as_array().and_then(|r| r.get(col_idx)) {
                    None | Some(serde_json::Value::Null) => b.append_null(),
                    Some(v) => b.append_value(v.as_bool().unwrap_or(false)),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Int8 => {
            let mut b = Int8Builder::with_capacity(rows.len());
            for row in rows {
                match row.as_array().and_then(|r| r.get(col_idx)) {
                    None | Some(serde_json::Value::Null) => b.append_null(),
                    Some(v) => b.append_value(v.as_i64().unwrap_or(0) as i8),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Int16 => {
            let mut b = Int16Builder::with_capacity(rows.len());
            for row in rows {
                match row.as_array().and_then(|r| r.get(col_idx)) {
                    None | Some(serde_json::Value::Null) => b.append_null(),
                    Some(v) => b.append_value(v.as_i64().unwrap_or(0) as i16),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Int32 => {
            let mut b = Int32Builder::with_capacity(rows.len());
            for row in rows {
                match row.as_array().and_then(|r| r.get(col_idx)) {
                    None | Some(serde_json::Value::Null) => b.append_null(),
                    Some(v) => b.append_value(v.as_i64().unwrap_or(0) as i32),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Int64 => {
            let mut b = Int64Builder::with_capacity(rows.len());
            for row in rows {
                match row.as_array().and_then(|r| r.get(col_idx)) {
                    None | Some(serde_json::Value::Null) => b.append_null(),
                    Some(v) => b.append_value(v.as_i64().unwrap_or(0)),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Float32 => {
            let mut b = Float32Builder::with_capacity(rows.len());
            for row in rows {
                match row.as_array().and_then(|r| r.get(col_idx)) {
                    None | Some(serde_json::Value::Null) => b.append_null(),
                    Some(v) => b.append_value(v.as_f64().unwrap_or(0.0) as f32),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Float64 => {
            let mut b = Float64Builder::with_capacity(rows.len());
            for row in rows {
                match row.as_array().and_then(|r| r.get(col_idx)) {
                    None | Some(serde_json::Value::Null) => b.append_null(),
                    Some(v) => b.append_value(v.as_f64().unwrap_or(0.0)),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        // Utf8 and all other types: stringify the JSON value.
        _ => {
            let mut b = StringBuilder::with_capacity(rows.len(), rows.len() * 16);
            for row in rows {
                match row.as_array().and_then(|r| r.get(col_idx)) {
                    None | Some(serde_json::Value::Null) => b.append_null(),
                    Some(v) => b.append_value(json_value_to_string(v)),
                }
            }
            Ok(Arc::new(b.finish()))
        }
    }
}

fn json_value_to_string(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Null => "null".to_string(),
        other => other.to_string(), // Array/Object → JSON string
    }
}

fn parse_describe_result(
    resp: &TrinoResponse,
    catalog: &str,
    database: &str,
    table: &str,
) -> TableSchema {
    let columns = resp
        .data
        .as_ref()
        .and_then(|d| d.as_array())
        .map(|rows| {
            rows.iter()
                .filter_map(|row| {
                    let arr = row.as_array()?;
                    Some(queryflux_core::catalog::ColumnDef {
                        name: arr.first()?.as_str()?.to_string(),
                        data_type: arr.get(1)?.as_str()?.to_uppercase(),
                        nullable: arr.get(2).and_then(|v| v.as_str()) != Some("not null"),
                    })
                })
                .collect()
        })
        .unwrap_or_default();

    TableSchema {
        catalog: catalog.to_string(),
        database: database.to_string(),
        table: table.to_string(),
        columns,
    }
}

impl TrinoAdapter {
    pub fn descriptor() -> EngineDescriptor {
        EngineDescriptor {
            engine_key: "trino",
            display_name: "Trino",
            description: "Distributed SQL query engine using the Trino REST protocol (async submit/poll).",
            hex: "DD00A1",
            connection_type: ConnectionType::Http,
            default_port: Some(8080),
            endpoint_example: Some("http://trino-coordinator:8080"),
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
        }
    }
}

pub struct TrinoFactory;

#[async_trait]
impl crate::EngineAdapterFactory for TrinoFactory {
    fn engine_key(&self) -> &'static str {
        "trino"
    }

    fn descriptor(&self) -> EngineDescriptor {
        TrinoAdapter::descriptor()
    }

    async fn build_from_config_json(
        &self,
        cluster_name: ClusterName,
        group: ClusterGroupName,
        json: &serde_json::Value,
    ) -> Result<Arc<dyn crate::EngineAdapterTrait>> {
        let name = cluster_name.0.clone();
        Ok(Arc::new(TrinoAdapter::try_from_config_json(
            cluster_name,
            group,
            json,
            name.as_str(),
        )?))
    }
}
