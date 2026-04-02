use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanBuilder, Float64Builder, Int64Builder, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use futures::stream;
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
    SnowflakeAuthMethod, SnowflakeClient, SnowflakeClientConfig, SnowflakeColumnType, SnowflakeRow,
};
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

        let client_cfg = SnowflakeClientConfig {
            account: account.clone(),
            warehouse: json_str(json, "warehouse"),
            database: json_str(json, "catalog"),
            schema: json_str(json, "schema"),
            role: json_str(json, "role"),
            timeout: Some(std::time::Duration::from_secs(300)),
        };

        let client = SnowflakeClient::new(&username, sf_auth, client_cfg).map_err(|e| {
            QueryFluxError::Engine(format!(
                "cluster '{cluster_name_str}': failed to create Snowflake client: {e}"
            ))
        })?;

        let client = if let Some(endpoint) = json_str(json, "endpoint") {
            let url = Url::parse(&endpoint).map_err(|e| {
                QueryFluxError::Engine(format!(
                    "cluster '{cluster_name_str}': invalid endpoint URL: {e}"
                ))
            })?;
            let host = url.host_str().unwrap_or_default();
            let port = url.port();
            let protocol = Some(url.scheme().to_string());
            client.with_address(host, port, protocol).map_err(|e| {
                QueryFluxError::Engine(format!(
                    "cluster '{cluster_name_str}': failed to configure Snowflake endpoint: {e}"
                ))
            })?
        } else {
            client
        };

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

        let client_cfg = SnowflakeClientConfig {
            account: account.clone(),
            warehouse: cfg.warehouse.clone(),
            database: cfg.catalog.clone(),
            schema: cfg.schema.clone(),
            role: cfg.role.clone(),
            timeout: Some(std::time::Duration::from_secs(300)),
        };

        let client = SnowflakeClient::new(&username, sf_auth, client_cfg).map_err(|e| {
            QueryFluxError::Engine(format!(
                "cluster '{cluster_name_str}': failed to create Snowflake client: {e}"
            ))
        })?;

        let client = if let Some(endpoint) = &cfg.endpoint {
            let url = Url::parse(endpoint).map_err(|e| {
                QueryFluxError::Engine(format!(
                    "cluster '{cluster_name_str}': invalid endpoint URL: {e}"
                ))
            })?;
            let host = url.host_str().unwrap_or_default();
            let port = url.port();
            let protocol = Some(url.scheme().to_string());
            client.with_address(host, port, protocol).map_err(|e| {
                QueryFluxError::Engine(format!(
                    "cluster '{cluster_name_str}': failed to configure Snowflake endpoint: {e}"
                ))
            })?
        } else {
            client
        };

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

        let rows = sf_session
            .query(sql)
            .await
            .map_err(|e| QueryFluxError::Engine(format!("Snowflake query failed: {e}")))?;

        if rows.is_empty() {
            return Ok(Box::pin(stream::empty()));
        }

        let col_types = rows[0].column_types();
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

        let num_cols = schema.fields().len();
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(num_cols);
        for (col_idx, sf_type) in col_types.iter().enumerate() {
            let dt = schema.field(col_idx).data_type();
            let col = build_arrow_column(dt, sf_type.column_type(), &rows, col_idx)?;
            columns.push(col);
        }

        let batch = RecordBatch::try_new(schema, columns)
            .map_err(|e| QueryFluxError::Engine(format!("Snowflake RecordBatch failed: {e}")))?;

        Ok(Box::pin(stream::iter(std::iter::once(Ok(batch)))))
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
