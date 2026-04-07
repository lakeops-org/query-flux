use std::sync::Arc;

use adbc_core::options::{AdbcVersion, OptionDatabase};
use adbc_core::{Connection, Driver, Statement, LOAD_FLAG_DEFAULT};
use adbc_driver_manager::{ManagedDatabase, ManagedDriver};
use arrow::array::Array;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use futures::stream;
use queryflux_core::{
    catalog::TableSchema,
    config::ClusterConfig,
    error::{QueryFluxError, Result},
    query::{ClusterGroupName, ClusterName, EngineType},
    session::SessionContext,
    tags::QueryTags,
};
use r2d2_adbc::AdbcConnectionManager;
use tokio::sync::oneshot;

use crate::{AdapterKind, EngineAdapterFactory, SyncAdapter, SyncExecution};
use queryflux_core::engine_registry::{
    AuthType, ConfigField, ConnectionType, EngineDescriptor, FieldType,
};

const DEFAULT_POOL_SIZE: u32 = 4;

const SUPPORTED_DRIVERS: &[&str] = &[
    "trino",
    "duckdb",
    "starrocks",
    "clickhouse",
    "mysql",
    "postgresql",
    "sqlite",
    "flightsql",
    "snowflake",
    "bigquery",
    "databricks",
    "mssql",
    "redshift",
    "exasol",
    "singlestore",
];

/// Maps a driver name to the EngineType used for SQL dialect rewriting.
fn driver_to_engine_type(driver: &str) -> EngineType {
    match driver {
        "trino" => EngineType::Trino,
        "duckdb" => EngineType::DuckDb,
        "starrocks" => EngineType::StarRocks,
        "clickhouse" => EngineType::ClickHouse,
        "mysql" => EngineType::Adbc, // MySql dialect — use Adbc until MySql EngineType exists
        "postgresql" => EngineType::Adbc,
        _ => EngineType::Adbc,
    }
}

fn parse_engine_type_override(value: &str) -> Option<EngineType> {
    match value.trim().to_ascii_lowercase().as_str() {
        "trino" => Some(EngineType::Trino),
        "duckdb" => Some(EngineType::DuckDb),
        "starrocks" => Some(EngineType::StarRocks),
        "clickhouse" => Some(EngineType::ClickHouse),
        "adbc" => Some(EngineType::Adbc),
        _ => None,
    }
}

/// Parsed and validated configuration for an ADBC cluster.
pub struct AdbcConfig {
    pub driver: String,
    pub uri: String,
    pub username: Option<String>,
    pub password: Option<String>,
    pub db_kwargs: Vec<(String, String)>,
    pub flight_sql_engine: Option<EngineType>,
    pub pool_size: u32,
}

impl AdbcConfig {
    pub fn engine_type(&self) -> EngineType {
        if self.driver == "flightsql" {
            if let Some(engine) = &self.flight_sql_engine {
                return engine.clone();
            }
        }
        driver_to_engine_type(&self.driver)
    }
}

impl crate::EngineConfigParseable for AdbcConfig {
    fn from_json(json: &serde_json::Value, cluster_name: &str) -> crate::Result<Self> {
        let driver = json
            .get("driver")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                QueryFluxError::Engine(format!(
                    "cluster '{cluster_name}': missing required field 'driver'"
                ))
            })?
            .to_string();

        let uri = json
            .get("uri")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                QueryFluxError::Engine(format!(
                    "cluster '{cluster_name}': missing required field 'uri'"
                ))
            })?
            .to_string();

        let username = json
            .get("username")
            .and_then(|v| v.as_str())
            .map(str::to_string);
        let password = json
            .get("password")
            .and_then(|v| v.as_str())
            .map(str::to_string);

        let db_kwargs = match json.get("dbKwargs") {
            Some(serde_json::Value::Object(map)) => map
                .iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                .collect(),
            _ => Vec::new(),
        };

        let flight_sql_engine = json
            .get("flightSqlEngine")
            .and_then(|v| v.as_str())
            .and_then(parse_engine_type_override);

        let pool_size = json
            .get("poolSize")
            .and_then(|v| v.as_u64())
            .map(|n| n.min(u32::MAX as u64) as u32)
            .unwrap_or(DEFAULT_POOL_SIZE)
            .max(1);

        Ok(Self {
            driver,
            uri,
            username,
            password,
            db_kwargs,
            flight_sql_engine,
            pool_size,
        })
    }

    fn from_cluster_config(_cfg: &ClusterConfig, cluster_name: &str) -> crate::Result<Self> {
        Err(QueryFluxError::Engine(format!(
            "cluster '{cluster_name}': ADBC clusters must be created via the admin API (no YAML ClusterConfig support)"
        )))
    }
}

type AdbcPool = r2d2::Pool<AdbcConnectionManager<ManagedDatabase>>;

/// ADBC adapter — wraps any ADBC-compatible shared library driver.
///
/// The driver is loaded once at construction via `load_from_name` (manifest-based, searches
/// user/system ADBC driver directories); the shared
/// library remains loaded for the lifetime of the pool via Arc reference counting.
pub struct AdbcAdapter {
    pub cluster_name: ClusterName,
    pub group_name: ClusterGroupName,
    pool: AdbcPool,
    engine_type: EngineType,
}

impl AdbcAdapter {
    pub fn new(
        cluster_name: ClusterName,
        group_name: ClusterGroupName,
        config: AdbcConfig,
    ) -> Result<Self> {
        let engine_type = config.engine_type();

        let mut driver = ManagedDriver::load_from_name(
            &config.driver,
            None,
            AdbcVersion::V110,
            LOAD_FLAG_DEFAULT,
            None,
        )
        .map_err(|e| {
            QueryFluxError::Engine(format!(
                "cluster '{}': failed to load ADBC driver '{}': {e}",
                cluster_name.0, config.driver
            ))
        })?;

        let mut opts: Vec<(OptionDatabase, adbc_core::options::OptionValue)> =
            vec![(OptionDatabase::Uri, config.uri.into())];

        if let Some(username) = config.username {
            opts.push((OptionDatabase::Username, username.into()));
        }
        if let Some(password) = config.password {
            opts.push((OptionDatabase::Password, password.into()));
        }
        for (k, v) in config.db_kwargs {
            opts.push((OptionDatabase::Other(k), v.into()));
        }

        let database = driver.new_database_with_opts(opts).map_err(|e| {
            QueryFluxError::Engine(format!(
                "cluster '{}': failed to create ADBC database: {e}",
                cluster_name.0
            ))
        })?;
        // driver dropped here — ManagedDatabase holds Arc ref to driver internals,
        // so the shared library remains loaded.

        let manager = AdbcConnectionManager::new(database);
        let pool = r2d2::Pool::builder()
            .max_size(config.pool_size)
            .build(manager)
            .map_err(|e| {
                QueryFluxError::Engine(format!(
                    "cluster '{}': failed to create ADBC connection pool: {e}",
                    cluster_name.0
                ))
            })?;

        Ok(Self {
            cluster_name,
            group_name,
            pool,
            engine_type,
        })
    }

    pub fn descriptor() -> EngineDescriptor {
        EngineDescriptor {
            engine_key: "adbc",
            display_name: "ADBC",
            description: "Generic ADBC adapter — connect to any engine via an installed ADBC driver.",
            hex: "6366F1",
            connection_type: ConnectionType::Driver,
            default_port: None,
            endpoint_example: None,
            supported_auth: vec![AuthType::Basic],
            implemented: true,
            config_fields: vec![
                ConfigField {
                    key: "driver",
                    label: "Driver",
                    description: "ADBC driver name (from `dbc install <driver>`) or path to shared library.",
                    field_type: FieldType::Select {
                        options: SUPPORTED_DRIVERS.to_vec(),
                    },
                    required: true,
                    example: Some("trino"),
                },
                ConfigField {
                    key: "uri",
                    label: "URI",
                    description: "Driver-specific connection URI.",
                    field_type: FieldType::Text,
                    required: true,
                    example: Some("http://trino-host:8080"),
                },
                ConfigField {
                    key: "username",
                    label: "Username",
                    description: "Authentication username.",
                    field_type: FieldType::Text,
                    required: false,
                    example: Some("admin"),
                },
                ConfigField {
                    key: "password",
                    label: "Password",
                    description: "Authentication password.",
                    field_type: FieldType::Secret,
                    required: false,
                    example: None,
                },
                ConfigField {
                    key: "dbKwargs",
                    label: "Driver Options",
                    description: "Additional driver-specific key/value options (JSON object).",
                    field_type: FieldType::Text,
                    required: false,
                    example: Some("{}"),
                },
                ConfigField {
                    key: "flightSqlEngine",
                    label: "FlightSQL target engine",
                    description: "When driver is flightsql: backend dialect for SQL translation/transpilation. Flight SQL is transport only.",
                    field_type: FieldType::Text,
                    required: false,
                    example: Some("starrocks"),
                },
                ConfigField {
                    key: "poolSize",
                    label: "Pool Size",
                    description: "Maximum number of pooled connections. Defaults to 4.",
                    field_type: FieldType::Number,
                    required: false,
                    example: Some("4"),
                },
            ],
        }
    }
}

fn collect_batches(
    reader: impl Iterator<Item = std::result::Result<RecordBatch, arrow::error::ArrowError>>,
) -> std::result::Result<Vec<RecordBatch>, QueryFluxError> {
    reader
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| QueryFluxError::Engine(format!("ADBC: failed to read results: {e}")))
}

#[async_trait]
impl SyncAdapter for AdbcAdapter {
    async fn execute_as_arrow(
        &self,
        sql: &str,
        _session: &SessionContext,
        _credentials: &queryflux_auth::QueryCredentials,
        _tags: &QueryTags,
    ) -> Result<SyncExecution> {
        let pool = self.pool.clone();
        let sql = sql.to_string();

        let batches = tokio::task::spawn_blocking(move || {
            let mut conn = pool.get().map_err(|e| {
                QueryFluxError::Engine(format!("ADBC: failed to get connection from pool: {e}"))
            })?;
            let mut stmt = conn.new_statement().map_err(|e| {
                QueryFluxError::Engine(format!("ADBC: failed to create statement: {e}"))
            })?;
            stmt.set_sql_query(&sql).map_err(|e| {
                QueryFluxError::Engine(format!("ADBC: failed to set SQL query: {e}"))
            })?;
            let reader = stmt.execute().map_err(|e| {
                QueryFluxError::Engine(format!("ADBC: query execution failed: {e}"))
            })?;
            collect_batches(reader)
        })
        .await
        .map_err(|e| QueryFluxError::Engine(format!("ADBC: spawn_blocking failed: {e}")))??;

        let (tx, rx) = oneshot::channel();
        let _ = tx.send(None); // ADBC has no standard stats API
        Ok(SyncExecution {
            stream: Box::pin(stream::iter(batches.into_iter().map(Ok))),
            stats: rx,
        })
    }

    fn engine_type(&self) -> EngineType {
        self.engine_type.clone()
    }

    async fn health_check(&self) -> bool {
        let pool = self.pool.clone();
        tokio::task::spawn_blocking(move || {
            let mut conn = pool.get().ok()?;
            let mut stmt = conn.new_statement().ok()?;
            stmt.set_sql_query("SELECT 1").ok()?;
            stmt.execute().ok()?;
            Some(())
        })
        .await
        .ok()
        .flatten()
        .is_some()
    }

    async fn list_catalogs(&self) -> Result<Vec<String>> {
        let pool = self.pool.clone();
        let result = tokio::task::spawn_blocking(move || {
            let mut conn = pool.get().map_err(|e| {
                QueryFluxError::Engine(format!("ADBC: pool error: {e}"))
            })?;
            let mut stmt = conn.new_statement().map_err(|e| {
                QueryFluxError::Engine(format!("ADBC: statement error: {e}"))
            })?;
            stmt.set_sql_query("SELECT catalog_name FROM information_schema.schemata GROUP BY catalog_name ORDER BY catalog_name")
                .map_err(|e| QueryFluxError::Engine(format!("ADBC: {e}")))?;
            let batches = collect_batches(
                stmt.execute().map_err(|e| QueryFluxError::Engine(format!("ADBC: {e}")))?,
            )?;
            let mut catalogs = Vec::new();
            for batch in batches {
                if batch.num_columns() == 0 {
                    continue;
                }
                let col = batch.column(0);
                let arr = col
                    .as_any()
                    .downcast_ref::<arrow::array::StringArray>();
                if let Some(arr) = arr {
                    for i in 0..arr.len() {
                        if !arr.is_null(i) {
                            catalogs.push(arr.value(i).to_string());
                        }
                    }
                }
            }
            Ok(catalogs)
        })
        .await
        .map_err(|e| QueryFluxError::Engine(format!("ADBC: spawn_blocking: {e}")))?;

        result.or_else(|_: QueryFluxError| Ok(Vec::new()))
    }

    async fn list_databases(&self, catalog: &str) -> Result<Vec<String>> {
        let pool = self.pool.clone();
        let catalog = catalog.to_string();
        let result = tokio::task::spawn_blocking(move || {
            let mut conn = pool.get().map_err(|e| QueryFluxError::Engine(format!("ADBC: {e}")))?;
            let mut stmt = conn.new_statement().map_err(|e| QueryFluxError::Engine(format!("ADBC: {e}")))?;
            stmt.set_sql_query(format!(
                "SELECT schema_name FROM information_schema.schemata WHERE catalog_name = '{catalog}' ORDER BY schema_name"
            ))
            .map_err(|e| QueryFluxError::Engine(format!("ADBC: {e}")))?;
            let batches = collect_batches(
                stmt.execute().map_err(|e| QueryFluxError::Engine(format!("ADBC: {e}")))?,
            )?;
            let mut schemas = Vec::new();
            for batch in batches {
                if batch.num_columns() == 0 {
                    continue;
                }
                let arr = batch.column(0).as_any().downcast_ref::<arrow::array::StringArray>();
                if let Some(arr) = arr {
                    for i in 0..arr.len() {
                        if !arr.is_null(i) {
                            schemas.push(arr.value(i).to_string());
                        }
                    }
                }
            }
            Ok(schemas)
        })
        .await
        .map_err(|e| QueryFluxError::Engine(format!("ADBC: spawn_blocking: {e}")))?;

        result.or_else(|_: QueryFluxError| Ok(Vec::new()))
    }

    async fn list_tables(&self, catalog: &str, database: &str) -> Result<Vec<String>> {
        let pool = self.pool.clone();
        let catalog = catalog.to_string();
        let database = database.to_string();
        let result = tokio::task::spawn_blocking(move || {
            let mut conn = pool.get().map_err(|e| QueryFluxError::Engine(format!("ADBC: {e}")))?;
            let mut stmt = conn.new_statement().map_err(|e| QueryFluxError::Engine(format!("ADBC: {e}")))?;
            stmt.set_sql_query(format!(
                "SELECT table_name FROM information_schema.tables WHERE table_catalog = '{catalog}' AND table_schema = '{database}' ORDER BY table_name"
            ))
            .map_err(|e| QueryFluxError::Engine(format!("ADBC: {e}")))?;
            let batches = collect_batches(
                stmt.execute().map_err(|e| QueryFluxError::Engine(format!("ADBC: {e}")))?,
            )?;
            let mut tables = Vec::new();
            for batch in batches {
                if batch.num_columns() == 0 {
                    continue;
                }
                let arr = batch.column(0).as_any().downcast_ref::<arrow::array::StringArray>();
                if let Some(arr) = arr {
                    for i in 0..arr.len() {
                        if !arr.is_null(i) {
                            tables.push(arr.value(i).to_string());
                        }
                    }
                }
            }
            Ok(tables)
        })
        .await
        .map_err(|e| QueryFluxError::Engine(format!("ADBC: spawn_blocking: {e}")))?;

        result.or_else(|_: QueryFluxError| Ok(Vec::new()))
    }

    async fn describe_table(
        &self,
        _catalog: &str,
        _database: &str,
        _table: &str,
    ) -> Result<Option<TableSchema>> {
        // Best-effort: not all ADBC drivers expose information_schema column types uniformly.
        Ok(None)
    }
}

pub struct AdbcFactory;

#[async_trait]
impl EngineAdapterFactory for AdbcFactory {
    fn engine_key(&self) -> &'static str {
        "adbc"
    }

    fn descriptor(&self) -> EngineDescriptor {
        AdbcAdapter::descriptor()
    }

    async fn build_from_config_json(
        &self,
        cluster_name: ClusterName,
        group: ClusterGroupName,
        json: &serde_json::Value,
    ) -> Result<AdapterKind> {
        use crate::EngineConfigParseable;
        let name = cluster_name.0.clone();
        let config = AdbcConfig::from_json(json, &name)?;
        Ok(AdapterKind::Sync(Arc::new(AdbcAdapter::new(
            cluster_name,
            group,
            config,
        )?)))
    }
}
