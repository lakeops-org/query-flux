use std::sync::Arc;
use std::time::Duration;

use arrow::array::{ArrayRef, BooleanBuilder, Float64Builder, Int64Builder, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use queryflux_auth::QueryCredentials;
use queryflux_core::{
    catalog::TableSchema,
    config::{ClusterAuth, ClusterConfig},
    engine_registry::{AuthType, ConfigField, ConnectionType, EngineDescriptor, FieldType},
    error::{QueryFluxError, Result},
    query::{
        BackendQueryId, ClusterGroupName, ClusterName, EngineType, QueryExecution, QueryPollResult,
    },
    session::SessionContext,
    tags::QueryTags,
};
use snowflake_connector_rs::{
    SnowflakeAuthMethod, SnowflakeClient, SnowflakeClientConfig, SnowflakeColumn,
    SnowflakeColumnType, SnowflakeEndpointConfig, SnowflakeQueryConfig, SnowflakeRow,
    SnowflakeSessionConfig,
};
use tokio_stream::wrappers::UnboundedReceiverStream;
use url::Url;

use crate::EngineAdapterTrait;

pub struct SnowflakeAdapter {
    pub cluster_name: ClusterName,
    pub group_name: ClusterGroupName,
    client: SnowflakeClient,
    account: String,
}

impl SnowflakeAdapter {
    /// Build from a DB config JSON blob (bypasses the `ClusterConfig` god struct).
    pub fn try_from_config_json(
        cluster_name: ClusterName,
        group_name: ClusterGroupName,
        json: &serde_json::Value,
        cluster_name_str: &str,
    ) -> Result<Self> {
        use queryflux_core::engine_registry::{json_str, parse_auth_from_config_json};

        let account = json_str(json, "account").ok_or_else(|| {
            QueryFluxError::Engine(format!(
                "cluster '{cluster_name_str}': Snowflake requires 'account' field"
            ))
        })?;

        let auth = parse_auth_from_config_json(json).ok_or_else(|| {
            QueryFluxError::Engine(format!(
                "cluster '{cluster_name_str}': Snowflake requires 'auth' configuration"
            ))
        })?;

        let (username, sf_auth) = map_auth(&auth).map_err(|msg| {
            QueryFluxError::Engine(format!("cluster '{cluster_name_str}': {msg}"))
        })?;

        let client = build_snowflake_client(SnowflakeClientParams {
            cluster_name_str,
            account: account.clone(),
            username,
            sf_auth,
            warehouse: json_str(json, "warehouse"),
            database: json_str(json, "catalog"),
            schema: json_str(json, "schema"),
            role: json_str(json, "role"),
            endpoint: json_str(json, "endpoint"),
        })?;

        Ok(Self {
            cluster_name,
            group_name,
            client,
            account,
        })
    }

    pub fn try_from_cluster_config(
        cluster_name: ClusterName,
        group_name: ClusterGroupName,
        cfg: &ClusterConfig,
        cluster_name_str: &str,
    ) -> Result<Self> {
        let account = cfg.account.clone().ok_or_else(|| {
            QueryFluxError::Engine(format!(
                "cluster '{cluster_name_str}': Snowflake requires 'account' field"
            ))
        })?;

        let auth = cfg.auth.clone().ok_or_else(|| {
            QueryFluxError::Engine(format!(
                "cluster '{cluster_name_str}': Snowflake requires 'auth' configuration"
            ))
        })?;

        let (username, sf_auth) = map_auth(&auth).map_err(|msg| {
            QueryFluxError::Engine(format!("cluster '{cluster_name_str}': {msg}"))
        })?;

        let client = build_snowflake_client(SnowflakeClientParams {
            cluster_name_str,
            account: account.clone(),
            username,
            sf_auth,
            warehouse: cfg.warehouse.clone(),
            database: cfg.catalog.clone(),
            schema: cfg.schema.clone(),
            role: cfg.role.clone(),
            endpoint: cfg.endpoint.clone(),
        })?;

        Ok(Self {
            cluster_name,
            group_name,
            client,
            account,
        })
    }

    async fn run_query(&self, sql: &str) -> Result<Vec<SnowflakeRow>> {
        let session = self.client.create_session().await.map_err(|e| {
            QueryFluxError::Engine(format!("Snowflake session creation failed: {e}"))
        })?;
        session
            .query(sql)
            .await
            .map_err(|e| QueryFluxError::Engine(format!("Snowflake query failed: {e}")))
    }

    async fn run_first_col(&self, sql: &str) -> Result<Vec<String>> {
        let rows = self.run_query(sql).await?;
        Ok(rows
            .iter()
            .filter_map(|row| row.at::<String>(0).ok())
            .collect())
    }
}

struct SnowflakeClientParams<'a> {
    cluster_name_str: &'a str,
    account: String,
    username: String,
    sf_auth: SnowflakeAuthMethod,
    warehouse: Option<String>,
    database: Option<String>,
    schema: Option<String>,
    role: Option<String>,
    endpoint: Option<String>,
}

fn build_snowflake_client(p: SnowflakeClientParams<'_>) -> Result<SnowflakeClient> {
    let mut session = SnowflakeSessionConfig::default();
    if let Some(w) = p.warehouse {
        session = session.with_warehouse(w);
    }
    if let Some(d) = p.database {
        session = session.with_database(d);
    }
    if let Some(s) = p.schema {
        session = session.with_schema(s);
    }
    if let Some(r) = p.role {
        session = session.with_role(r);
    }

    let query = SnowflakeQueryConfig::default()
        .with_async_query_completion_timeout(Duration::from_secs(300));

    let mut cfg = SnowflakeClientConfig::new(p.username, p.account, p.sf_auth)
        .with_session(session)
        .with_query(query);

    if let Some(ep) = p.endpoint {
        let url = Url::parse(&ep).map_err(|e| {
            QueryFluxError::Engine(format!(
                "cluster '{}': invalid endpoint URL: {e}",
                p.cluster_name_str
            ))
        })?;
        cfg = cfg.with_endpoint(SnowflakeEndpointConfig::custom_base_url(url));
    }

    SnowflakeClient::new(cfg).map_err(|e| {
        QueryFluxError::Engine(format!(
            "cluster '{}': failed to create Snowflake client: {e}",
            p.cluster_name_str
        ))
    })
}

fn map_auth(auth: &ClusterAuth) -> std::result::Result<(String, SnowflakeAuthMethod), String> {
    match auth {
        ClusterAuth::Basic { username, password } => Ok((
            username.clone(),
            SnowflakeAuthMethod::Password(password.clone()),
        )),
        ClusterAuth::KeyPair {
            username,
            private_key_pem,
            private_key_passphrase,
        } => {
            let method = if let Some(passphrase) = private_key_passphrase {
                SnowflakeAuthMethod::KeyPair {
                    encrypted_pem: private_key_pem.clone(),
                    password: passphrase.as_bytes().to_vec(),
                }
            } else {
                SnowflakeAuthMethod::KeyPairUnencrypted {
                    pem: private_key_pem.clone(),
                }
            };
            Ok((username.clone(), method))
        }
        ClusterAuth::Bearer { token } => Ok((
            String::new(),
            SnowflakeAuthMethod::Oauth {
                token: token.clone(),
            },
        )),
        other => Err(format!(
            "unsupported auth type for Snowflake: {other:?}. Use basic, keyPair, or bearer."
        )),
    }
}

#[async_trait]
impl EngineAdapterTrait for SnowflakeAdapter {
    async fn submit_query(
        &self,
        _sql: &str,
        _session: &SessionContext,
        _credentials: &QueryCredentials,
        _tags: &QueryTags,
    ) -> Result<QueryExecution> {
        Err(QueryFluxError::Engine(
            "Snowflake requires execute_as_arrow; use the Arrow execution path".to_string(),
        ))
    }

    async fn poll_query(
        &self,
        _backend_id: &BackendQueryId,
        _next_uri: Option<&str>,
    ) -> Result<QueryPollResult> {
        Err(QueryFluxError::Engine(
            "Snowflake does not support async polling through QueryFlux".to_string(),
        ))
    }

    async fn cancel_query(&self, _backend_id: &BackendQueryId) -> Result<()> {
        Ok(())
    }

    async fn health_check(&self) -> bool {
        match self.run_query("SELECT 1").await {
            Ok(_) => true,
            Err(e) => {
                tracing::warn!(
                    cluster = %self.cluster_name,
                    error = %e,
                    "Snowflake health check failed"
                );
                false
            }
        }
    }

    fn engine_type(&self) -> EngineType {
        EngineType::Snowflake
    }

    fn supports_async(&self) -> bool {
        false
    }

    fn base_url(&self) -> &str {
        &self.account
    }

    async fn execute_as_arrow(
        &self,
        sql: &str,
        session: &SessionContext,
        _credentials: &QueryCredentials,
        _tags: &QueryTags,
    ) -> Result<crate::ArrowStream> {
        let sf_session = self.client.create_session().await.map_err(|e| {
            QueryFluxError::Engine(format!("Snowflake session creation failed: {e}"))
        })?;

        // Apply per-query database/schema overrides from the frontend session context.
        if let Some(db) = session.database() {
            let use_sql = format!("USE DATABASE \"{}\"", db.replace('"', "\"\""));
            sf_session.query(use_sql.as_str()).await.map_err(|e| {
                QueryFluxError::Engine(format!("Snowflake USE DATABASE failed: {e}"))
            })?;
        }

        let executor = sf_session
            .execute(sql)
            .await
            .map_err(|e| QueryFluxError::Engine(format!("Snowflake query failed: {e}")))?;

        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<Result<RecordBatch>>();

        tokio::spawn(async move {
            // Metadata is on the first response even when `rowSet` / chunks are empty (e.g. LIMIT 0).
            let col_types = executor.snowflake_columns();
            let fields: Vec<Field> = col_types
                .iter()
                .map(|c| {
                    Field::new(
                        c.name(),
                        snowflake_type_to_arrow(c.column_type()),
                        c.column_type().nullable(),
                    )
                })
                .collect();
            let schema = Arc::new(ArrowSchema::new(fields));
            if schema.fields().is_empty() {
                return;
            }

            let mut emitted_rows = false;
            loop {
                let chunk = match executor.fetch_next_chunk().await {
                    Ok(c) => c,
                    Err(e) => {
                        let _ = tx.send(Err(QueryFluxError::Engine(format!(
                            "Snowflake query failed: {e}"
                        ))));
                        return;
                    }
                };
                let Some(rows) = chunk else { break };

                if rows.is_empty() {
                    continue;
                }
                emitted_rows = true;
                match build_snowflake_record_batch(Arc::clone(&schema), &col_types, &rows) {
                    Ok(batch) => {
                        let _ = tx.send(Ok(batch));
                    }
                    Err(e) => {
                        let _ = tx.send(Err(e));
                        return;
                    }
                }
            }

            if !emitted_rows {
                let _ = tx.send(Ok(RecordBatch::new_empty(schema)));
            }
        });

        Ok(Box::pin(UnboundedReceiverStream::new(rx)))
    }

    async fn list_catalogs(&self) -> Result<Vec<String>> {
        self.run_first_col("SHOW DATABASES").await
    }

    async fn list_databases(&self, catalog: &str) -> Result<Vec<String>> {
        let sql = format!(
            "SHOW SCHEMAS IN DATABASE \"{}\"",
            catalog.replace('"', "\"\"")
        );
        self.run_first_col(&sql).await
    }

    async fn list_tables(&self, catalog: &str, database: &str) -> Result<Vec<String>> {
        let sql = format!(
            "SHOW TABLES IN \"{}\".\"{}\"",
            catalog.replace('"', "\"\""),
            database.replace('"', "\"\"")
        );
        self.run_first_col(&sql).await
    }

    async fn describe_table(
        &self,
        catalog: &str,
        database: &str,
        table: &str,
    ) -> Result<Option<TableSchema>> {
        let qualified = format!(
            "\"{}\".\"{}\".\"{table}\"",
            catalog.replace('"', "\"\""),
            database.replace('"', "\"\""),
        );
        let rows = match self.run_query(&format!("DESCRIBE TABLE {qualified}")).await {
            Ok(r) => r,
            Err(_) => return Ok(None),
        };

        let columns = rows
            .iter()
            .filter_map(|row| {
                let name: String = row.get::<String>("name").ok()?;
                let data_type = row
                    .get::<String>("type")
                    .unwrap_or_else(|_| "VARCHAR".to_string())
                    .to_uppercase();
                let nullable = row
                    .get::<String>("null?")
                    .map(|s| s.to_uppercase() == "Y")
                    .unwrap_or(true);
                Some(queryflux_core::catalog::ColumnDef {
                    name,
                    data_type,
                    nullable,
                })
            })
            .collect();

        Ok(Some(TableSchema {
            catalog: catalog.to_string(),
            database: database.to_string(),
            table: table.to_string(),
            columns,
        }))
    }
}

impl SnowflakeAdapter {
    pub fn descriptor() -> EngineDescriptor {
        EngineDescriptor {
            engine_key: "snowflake",
            display_name: "Snowflake",
            description: "Cloud-native data warehouse. Connects via the Snowflake REST API.",
            hex: "29B5E8",
            connection_type: ConnectionType::Http,
            default_port: Some(443),
            endpoint_example: Some("https://xy12345.us-east-1.snowflakecomputing.com"),
            supported_auth: vec![AuthType::Basic, AuthType::KeyPair, AuthType::Bearer],
            implemented: true,
            config_fields: vec![
                ConfigField {
                    key: "account",
                    label: "Account",
                    description: "Snowflake account identifier (e.g. xy12345.us-east-1).",
                    field_type: FieldType::Text,
                    required: true,
                    example: Some("xy12345.us-east-1"),
                },
                ConfigField {
                    key: "endpoint",
                    label: "Endpoint",
                    description:
                        "Custom base URL override (e.g. PrivateLink). Omit to derive from account.",
                    field_type: FieldType::Url,
                    required: false,
                    example: Some("https://xy12345.us-east-1.privatelink.snowflakecomputing.com"),
                },
                ConfigField {
                    key: "warehouse",
                    label: "Warehouse",
                    description: "Default virtual warehouse for query execution.",
                    field_type: FieldType::Text,
                    required: false,
                    example: Some("COMPUTE_WH"),
                },
                ConfigField {
                    key: "role",
                    label: "Role",
                    description: "Default Snowflake role.",
                    field_type: FieldType::Text,
                    required: false,
                    example: Some("ANALYST"),
                },
                ConfigField {
                    key: "catalog",
                    label: "Database",
                    description: "Default Snowflake database.",
                    field_type: FieldType::Text,
                    required: false,
                    example: Some("MY_DATABASE"),
                },
                ConfigField {
                    key: "schema",
                    label: "Schema",
                    description: "Default Snowflake schema.",
                    field_type: FieldType::Text,
                    required: false,
                    example: Some("PUBLIC"),
                },
            ],
        }
    }
}

pub struct SnowflakeFactory;

#[async_trait]
impl crate::EngineAdapterFactory for SnowflakeFactory {
    fn engine_key(&self) -> &'static str {
        "snowflake"
    }

    fn descriptor(&self) -> EngineDescriptor {
        SnowflakeAdapter::descriptor()
    }

    async fn build_from_config_json(
        &self,
        cluster_name: ClusterName,
        group: ClusterGroupName,
        json: &serde_json::Value,
        cluster_name_str: &str,
    ) -> Result<Arc<dyn crate::EngineAdapterTrait>> {
        Ok(Arc::new(SnowflakeAdapter::try_from_config_json(
            cluster_name,
            group,
            json,
            cluster_name_str,
        )?))
    }
}

// ---------------------------------------------------------------------------
// Type mapping: Snowflake → Arrow
// ---------------------------------------------------------------------------

fn snowflake_type_to_arrow(ct: &SnowflakeColumnType) -> DataType {
    match ct.snowflake_type().to_ascii_lowercase().as_str() {
        "fixed" => {
            let scale = ct.scale().unwrap_or(0);
            if scale == 0 {
                DataType::Int64
            } else {
                DataType::Utf8
            }
        }
        "real" | "float" | "double" => DataType::Float64,
        "boolean" => DataType::Boolean,
        _ => DataType::Utf8,
    }
}

fn build_snowflake_record_batch(
    schema: Arc<ArrowSchema>,
    col_types: &[SnowflakeColumn],
    rows: &[SnowflakeRow],
) -> Result<RecordBatch> {
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(col_types.len());
    for (col_idx, sf_col) in col_types.iter().enumerate() {
        let dt = schema.field(col_idx).data_type();
        let col = build_arrow_column(dt, sf_col.column_type(), rows, col_idx)?;
        columns.push(col);
    }
    RecordBatch::try_new(schema, columns)
        .map_err(|e| QueryFluxError::Engine(format!("Snowflake RecordBatch failed: {e}")))
}

fn build_arrow_column(
    dt: &DataType,
    sf_type: &SnowflakeColumnType,
    rows: &[SnowflakeRow],
    col_idx: usize,
) -> Result<ArrayRef> {
    match dt {
        DataType::Boolean => {
            let mut b = BooleanBuilder::with_capacity(rows.len());
            for row in rows {
                match row.at::<bool>(col_idx) {
                    Ok(v) => b.append_value(v),
                    Err(_) => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Int64 => {
            let mut b = Int64Builder::with_capacity(rows.len());
            for row in rows {
                match row.at::<i64>(col_idx) {
                    Ok(v) => b.append_value(v),
                    Err(_) => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Float64 => {
            let mut b = Float64Builder::with_capacity(rows.len());
            for row in rows {
                match row.at::<f64>(col_idx) {
                    Ok(v) => b.append_value(v),
                    Err(_) => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        _ => {
            let _ = sf_type;
            let mut b = StringBuilder::with_capacity(rows.len(), rows.len() * 32);
            for row in rows {
                match row.at::<String>(col_idx) {
                    Ok(v) => b.append_value(v),
                    Err(_) => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
    }
}
