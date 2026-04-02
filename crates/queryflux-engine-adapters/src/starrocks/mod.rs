use std::sync::Arc;
use std::time::Duration;

use arrow::array::{
    ArrayRef, BooleanBuilder, Float32Builder, Float64Builder, Int16Builder, Int32Builder,
    Int64Builder, Int8Builder, StringBuilder,
};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use futures::stream;
use mysql_async::{consts::ColumnType, prelude::Queryable, Conn, Opts, OptsBuilder, Row, Value};
use queryflux_core::{
    catalog::TableSchema,
    config::{ClusterAuth, ClusterConfig},
    error::{QueryFluxError, Result},
    query::{
        BackendQueryId, ClusterGroupName, ClusterName, EngineType, QueryExecution, QueryPollResult,
    },
    session::SessionContext,
    tags::{tags_to_json, QueryTags},
};

use crate::EngineAdapterTrait;
use queryflux_core::engine_registry::{
    AuthType, ConfigField, ConnectionType, EngineDescriptor, FieldType,
};

/// StarRocks adapter — connects over the MySQL wire protocol to a StarRocks FE node.
///
/// StarRocks is synchronous: `submit_query` executes the full query and returns
/// `QueryExecution::Sync` with the complete result. `poll_query` is never called.
///
/// The `endpoint` must be a MySQL connection URL, e.g.:
///   `mysql://root:password@sr-fe-host:9030`
///
/// Alternatively, omit credentials from the URL and supply them via `auth: basic`.
pub struct StarRocksAdapter {
    pub cluster_name: ClusterName,
    pub group_name: ClusterGroupName,
    opts: Opts,
    endpoint: String,
}

impl StarRocksAdapter {
    pub fn new(
        cluster_name: ClusterName,
        group_name: ClusterGroupName,
        endpoint: String,
        auth: Option<ClusterAuth>,
    ) -> Result<Self> {
        let base_opts = Opts::from_url(&endpoint)
            .map_err(|e| QueryFluxError::Engine(format!("StarRocks invalid URL: {e}")))?;

        // Always disable Unix socket preference — StarRocks doesn't support the
        // `@@socket` system variable that mysql_async queries when prefer_socket=true.
        let mut builder = OptsBuilder::from_opts(base_opts).prefer_socket(false);

        // Override credentials from the explicit auth block if provided.
        if let Some(ClusterAuth::Basic { username, password }) = auth {
            builder = builder.user(Some(username)).pass(Some(password));
        }

        let opts = Opts::from(builder);

        Ok(Self {
            cluster_name,
            group_name,
            opts,
            endpoint,
        })
    }

    /// Build from a DB config JSON blob (bypasses the `ClusterConfig` god struct).
    pub fn try_from_config_json(
        cluster_name: ClusterName,
        group_name: ClusterGroupName,
        json: &serde_json::Value,
        cluster_name_str: &str,
    ) -> Result<Self> {
        use queryflux_core::engine_registry::{json_str, parse_auth_from_config_json};

        let endpoint = json_str(json, "endpoint").ok_or_else(|| {
            QueryFluxError::Engine(format!("cluster '{cluster_name_str}': missing endpoint"))
        })?;
        let auth = parse_auth_from_config_json(json);
        Self::new(cluster_name, group_name, endpoint, auth).map_err(|e| {
            QueryFluxError::Engine(format!(
                "cluster '{cluster_name_str}': failed to create StarRocks adapter ({e})"
            ))
        })
    }

    /// Build from persisted / YAML [`ClusterConfig`].
    pub fn try_from_cluster_config(
        cluster_name: ClusterName,
        group_name: ClusterGroupName,
        cfg: &ClusterConfig,
        cluster_name_str: &str,
    ) -> Result<Self> {
        let endpoint = cfg.endpoint.clone().ok_or_else(|| {
            QueryFluxError::Engine(format!("cluster '{cluster_name_str}': missing endpoint"))
        })?;
        Self::new(cluster_name, group_name, endpoint, cfg.auth.clone()).map_err(|e| {
            QueryFluxError::Engine(format!(
                "cluster '{cluster_name_str}': failed to create StarRocks adapter ({e})"
            ))
        })
    }

    /// Execute a DDL/setup statement that returns no rows (CREATE EXTERNAL CATALOG, etc.).
    pub async fn execute_ddl(&self, sql: &str) -> Result<()> {
        let mut conn = self.connect().await?;
        conn.query_drop(sql)
            .await
            .map_err(|e| QueryFluxError::Engine(format!("StarRocks DDL failed: {e}")))
    }

    async fn connect(&self) -> Result<Conn> {
        tokio::time::timeout(Duration::from_secs(10), Conn::new(self.opts.clone()))
            .await
            .map_err(|_| QueryFluxError::Engine("StarRocks connect timed out (10s)".to_string()))?
            .map_err(|e| QueryFluxError::Engine(format!("StarRocks connect failed: {e}")))
    }

    async fn run_query(&self, sql: &str) -> Result<Vec<Row>> {
        let mut conn = self.connect().await?;
        conn.query::<Row, _>(sql)
            .await
            .map_err(|e| QueryFluxError::Engine(format!("StarRocks query failed: {e}")))
    }

    /// Run a query and return the first column of each row as strings.
    async fn run_first_col(&self, sql: &str) -> Result<Vec<String>> {
        let rows = self.run_query(sql).await?;
        Ok(rows
            .into_iter()
            .filter_map(|mut row| {
                row.take::<String, usize>(0)
                    .or_else(|| row.take::<i64, usize>(0).map(|i| i.to_string()))
                    .or_else(|| row.take::<u64, usize>(0).map(|u| u.to_string()))
            })
            .collect())
    }
}

#[async_trait]
impl EngineAdapterTrait for StarRocksAdapter {
    /// Not used — StarRocks queries go through `execute_as_arrow`.
    async fn submit_query(
        &self,
        _sql: &str,
        _session: &SessionContext,
        _credentials: &queryflux_auth::QueryCredentials,
        _tags: &QueryTags,
    ) -> Result<QueryExecution> {
        Err(QueryFluxError::Engine(
            "StarRocks requires execute_as_arrow; use the Arrow execution path".to_string(),
        ))
    }

    async fn poll_query(
        &self,
        _backend_id: &BackendQueryId,
        _next_uri: Option<&str>,
    ) -> Result<QueryPollResult> {
        Err(QueryFluxError::Engine(
            "StarRocks does not support async polling".to_string(),
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
                    "StarRocks health check error"
                );
                false
            }
        }
    }

    fn engine_type(&self) -> EngineType {
        EngineType::StarRocks
    }

    fn supports_async(&self) -> bool {
        false
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
        let mut conn = self.connect().await?;

        if let Some(db) = session.database() {
            let use_sql = format!("USE `{}`", db.replace('`', "``"));
            conn.query_drop(&use_sql)
                .await
                .map_err(|e| QueryFluxError::Engine(format!("StarRocks USE failed: {e}")))?;
        }

        // Set query tag as a session variable so StarRocks surfaces it in audit logs.
        if !tags.is_empty() {
            let tag_json = tags_to_json(tags).to_string();
            // Use the driver's Value escaping so all MySQL string-literal special
            // characters (\0, \n, \r, \\, ', ") are handled correctly.
            let escaped = Value::from(tag_json).as_sql(false);
            let set_sql = format!("SET @query_tag = {escaped}");
            conn.query_drop(&set_sql).await.map_err(|e| {
                QueryFluxError::Engine(format!("StarRocks SET @query_tag failed: {e}"))
            })?;
        }

        let mut rows: Vec<Row> = conn
            .query::<Row, _>(sql)
            .await
            .map_err(|e| QueryFluxError::Engine(format!("StarRocks query failed: {e}")))?;

        if rows.is_empty() {
            return Ok(Box::pin(stream::empty()));
        }

        // Build Arrow schema from first row's column metadata.
        let fields: Vec<Field> = rows[0]
            .columns_ref()
            .iter()
            .map(|c| {
                Field::new(
                    c.name_str().to_string(),
                    mysql_column_type_to_arrow(c.column_type()),
                    true,
                )
            })
            .collect();
        let schema = Arc::new(ArrowSchema::new(fields));

        // Build columns from rows.
        let num_cols = schema.fields().len();
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(num_cols);

        for col_idx in 0..num_cols {
            let dt = schema.field(col_idx).data_type();
            let col = starrocks_build_column(dt, &mut rows, col_idx)?;
            columns.push(col);
        }

        let batch = RecordBatch::try_new(schema, columns)
            .map_err(|e| QueryFluxError::Engine(format!("StarRocks RecordBatch failed: {e}")))?;

        Ok(Box::pin(stream::iter(std::iter::once(Ok(batch)))))
    }

    // --- Catalog discovery ---

    async fn list_catalogs(&self) -> Result<Vec<String>> {
        // SHOW CATALOGS available in StarRocks 3.0+. Fall back gracefully.
        match self.run_first_col("SHOW CATALOGS").await {
            Ok(catalogs) if !catalogs.is_empty() => Ok(catalogs),
            _ => Ok(vec!["default_catalog".to_string()]),
        }
    }

    async fn list_databases(&self, catalog: &str) -> Result<Vec<String>> {
        let sql = if catalog.is_empty() || catalog == "default_catalog" {
            "SHOW DATABASES".to_string()
        } else {
            format!("SHOW DATABASES FROM `{}`", catalog.replace('`', "``"))
        };
        self.run_first_col(&sql).await
    }

    async fn list_tables(&self, _catalog: &str, database: &str) -> Result<Vec<String>> {
        let sql = format!("SHOW TABLES FROM `{}`", database.replace('`', "``"));
        self.run_first_col(&sql).await
    }

    async fn describe_table(
        &self,
        catalog: &str,
        database: &str,
        table: &str,
    ) -> Result<Option<TableSchema>> {
        // DESC [catalog.]db.table — returns Field, Type, Null, Key, Default, Extra
        let qualified = if catalog.is_empty() || catalog == "default_catalog" {
            format!(
                "`{}`.`{}`",
                database.replace('`', "``"),
                table.replace('`', "``")
            )
        } else {
            format!(
                "`{}`.`{}`.`{}`",
                catalog.replace('`', "``"),
                database.replace('`', "``"),
                table.replace('`', "``"),
            )
        };

        let mut rows = match self.run_query(&format!("DESC {qualified}")).await {
            Ok(r) => r,
            Err(_) => return Ok(None),
        };

        let columns = rows
            .iter_mut()
            .filter_map(|row| {
                let name: String = row.take(0)?;
                let data_type = row
                    .take::<String, usize>(1)
                    .unwrap_or_else(|| "VARCHAR".to_string())
                    .to_uppercase();
                let nullable = row
                    .take::<String, usize>(2)
                    .map(|s| s.to_uppercase() != "NO")
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

// ---------------------------------------------------------------------------
// Type mapping helpers
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Arrow conversion helpers for execute_as_arrow
// ---------------------------------------------------------------------------

fn mysql_column_type_to_arrow(ct: ColumnType) -> DataType {
    match ct {
        ColumnType::MYSQL_TYPE_TINY => DataType::Int8,
        ColumnType::MYSQL_TYPE_SHORT | ColumnType::MYSQL_TYPE_YEAR => DataType::Int16,
        ColumnType::MYSQL_TYPE_INT24 | ColumnType::MYSQL_TYPE_LONG => DataType::Int32,
        ColumnType::MYSQL_TYPE_LONGLONG => DataType::Int64,
        ColumnType::MYSQL_TYPE_FLOAT => DataType::Float32,
        ColumnType::MYSQL_TYPE_DOUBLE => DataType::Float64,
        ColumnType::MYSQL_TYPE_BIT => DataType::Boolean,
        _ => DataType::Utf8,
    }
}

fn starrocks_build_column(dt: &DataType, rows: &mut [Row], col_idx: usize) -> Result<ArrayRef> {
    match dt {
        DataType::Boolean => {
            let mut b = BooleanBuilder::with_capacity(rows.len());
            for row in rows.iter_mut() {
                match row.take::<Value, usize>(col_idx).unwrap_or(Value::NULL) {
                    Value::NULL => b.append_null(),
                    Value::Int(i) => b.append_value(i != 0),
                    Value::UInt(u) => b.append_value(u != 0),
                    Value::Bytes(bs) => match String::from_utf8(bs)
                        .ok()
                        .and_then(|s| s.parse::<i64>().ok())
                    {
                        Some(i) => b.append_value(i != 0),
                        None => b.append_null(),
                    },
                    _ => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Int8 => {
            let mut b = Int8Builder::with_capacity(rows.len());
            for row in rows.iter_mut() {
                match row.take::<Value, usize>(col_idx).unwrap_or(Value::NULL) {
                    Value::NULL => b.append_null(),
                    Value::Int(i) => b.append_value(i as i8),
                    Value::UInt(u) => b.append_value(u as i8),
                    Value::Bytes(bs) => match String::from_utf8(bs)
                        .ok()
                        .and_then(|s| s.parse::<i8>().ok())
                    {
                        Some(v) => b.append_value(v),
                        None => b.append_null(),
                    },
                    _ => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Int16 => {
            let mut b = Int16Builder::with_capacity(rows.len());
            for row in rows.iter_mut() {
                match row.take::<Value, usize>(col_idx).unwrap_or(Value::NULL) {
                    Value::NULL => b.append_null(),
                    Value::Int(i) => b.append_value(i as i16),
                    Value::UInt(u) => b.append_value(u as i16),
                    Value::Bytes(bs) => match String::from_utf8(bs)
                        .ok()
                        .and_then(|s| s.parse::<i16>().ok())
                    {
                        Some(v) => b.append_value(v),
                        None => b.append_null(),
                    },
                    _ => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Int32 => {
            let mut b = Int32Builder::with_capacity(rows.len());
            for row in rows.iter_mut() {
                match row.take::<Value, usize>(col_idx).unwrap_or(Value::NULL) {
                    Value::NULL => b.append_null(),
                    Value::Int(i) => b.append_value(i as i32),
                    Value::UInt(u) => b.append_value(u as i32),
                    Value::Bytes(bs) => match String::from_utf8(bs)
                        .ok()
                        .and_then(|s| s.parse::<i32>().ok())
                    {
                        Some(v) => b.append_value(v),
                        None => b.append_null(),
                    },
                    _ => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Int64 => {
            let mut b = Int64Builder::with_capacity(rows.len());
            for row in rows.iter_mut() {
                match row.take::<Value, usize>(col_idx).unwrap_or(Value::NULL) {
                    Value::NULL => b.append_null(),
                    Value::Int(i) => b.append_value(i),
                    Value::UInt(u) => b.append_value(u as i64),
                    Value::Bytes(bs) => match String::from_utf8(bs)
                        .ok()
                        .and_then(|s| s.parse::<i64>().ok())
                    {
                        Some(v) => b.append_value(v),
                        None => b.append_null(),
                    },
                    _ => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Float32 => {
            let mut b = Float32Builder::with_capacity(rows.len());
            for row in rows.iter_mut() {
                match row.take::<Value, usize>(col_idx).unwrap_or(Value::NULL) {
                    Value::NULL => b.append_null(),
                    Value::Float(f) => b.append_value(f),
                    Value::Double(d) => b.append_value(d as f32),
                    Value::Bytes(bs) => match String::from_utf8(bs)
                        .ok()
                        .and_then(|s| s.parse::<f32>().ok())
                    {
                        Some(v) => b.append_value(v),
                        None => b.append_null(),
                    },
                    _ => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Float64 => {
            let mut b = Float64Builder::with_capacity(rows.len());
            for row in rows.iter_mut() {
                match row.take::<Value, usize>(col_idx).unwrap_or(Value::NULL) {
                    Value::NULL => b.append_null(),
                    Value::Float(f) => b.append_value(f as f64),
                    Value::Double(d) => b.append_value(d),
                    Value::Bytes(bs) => match String::from_utf8(bs)
                        .ok()
                        .and_then(|s| s.parse::<f64>().ok())
                    {
                        Some(v) => b.append_value(v),
                        None => b.append_null(),
                    },
                    _ => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        _ => {
            let mut b = StringBuilder::with_capacity(rows.len(), rows.len() * 16);
            for row in rows.iter_mut() {
                match row.take::<Value, usize>(col_idx).unwrap_or(Value::NULL) {
                    Value::NULL => b.append_null(),
                    Value::Bytes(bytes) => match String::from_utf8(bytes) {
                        Ok(s) => b.append_value(s),
                        Err(e) => {
                            b.append_value(format!("<binary:{} bytes>", e.into_bytes().len()))
                        }
                    },
                    Value::Int(i) => b.append_value(i.to_string()),
                    Value::UInt(u) => b.append_value(u.to_string()),
                    Value::Float(f) => b.append_value(f.to_string()),
                    Value::Double(d) => b.append_value(d.to_string()),
                    Value::Date(y, mo, d, h, mi, s, us) => b.append_value(format!(
                        "{y:04}-{mo:02}-{d:02} {h:02}:{mi:02}:{s:02}.{us:06}"
                    )),
                    Value::Time(neg, days, h, mi, s, us) => {
                        let sign = if neg { "-" } else { "" };
                        let total_h = days * 24 + h as u32;
                        b.append_value(format!("{sign}{total_h:02}:{mi:02}:{s:02}.{us:06}"))
                    }
                }
            }
            Ok(Arc::new(b.finish()))
        }
    }
}

impl StarRocksAdapter {
    pub fn descriptor() -> EngineDescriptor {
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
        }
    }
}

pub struct StarRocksFactory;

#[async_trait]
impl crate::EngineAdapterFactory for StarRocksFactory {
    fn engine_key(&self) -> &'static str {
        "starRocks"
    }

    fn descriptor(&self) -> EngineDescriptor {
        StarRocksAdapter::descriptor()
    }

    async fn build_from_config_json(
        &self,
        cluster_name: ClusterName,
        group: ClusterGroupName,
        json: &serde_json::Value,
        cluster_name_str: &str,
    ) -> Result<Arc<dyn crate::EngineAdapterTrait>> {
        Ok(Arc::new(StarRocksAdapter::try_from_config_json(
            cluster_name,
            group,
            json,
            cluster_name_str,
        )?))
    }
}
