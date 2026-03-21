/// Test harness that spins up a QueryFlux Trino HTTP server in-process on a
/// random port, with DuckDB always available and Trino/StarRocks/Lakekeeper
/// optional.
///
/// External engines are discovered via environment variables:
///   TRINO_URL       — e.g. http://localhost:18081  (default if services are running)
///   STARROCKS_URL   — e.g. mysql://root@localhost:19030
///   LAKEKEEPER_URL  — e.g. http://localhost:18181  (REST catalog)
///   MINIO_ENDPOINT  — e.g. localhost:19000          (for DuckDB S3 secret)
///
/// If a variable is absent the group is omitted and tests that need it are skipped.
///
/// Iceberg catalog setup per engine:
///   Trino     — the data-loader container (docker-compose) already ran
///               CREATE CATALOG lakekeeper USING iceberg; no harness action needed.
///   StarRocks — CREATE EXTERNAL CATALOG lakekeeper PROPERTIES (...) via MySQL wire.
///   DuckDB    — INSTALL iceberg; LOAD iceberg; CREATE SECRET; ATTACH via execute_batch.
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use axum::Router;
use queryflux_cluster_manager::{
    cluster_state::ClusterState, simple::SimpleClusterGroupManager, strategy::strategy_from_config,
};
use queryflux_core::{
    error::Result as QfResult,
    query::{ClusterGroupName, ClusterName, EngineType},
};
use queryflux_engine_adapters::{
    duckdb::DuckDbAdapter, starrocks::StarRocksAdapter, trino::TrinoAdapter, EngineAdapterTrait,
};
use queryflux_frontend::trino_http::{state::AppState, TrinoHttpFrontend};
use queryflux_metrics::{ClusterSnapshot, MetricsStore, QueryRecord};
use queryflux_persistence::in_memory::InMemoryPersistence;
use queryflux_routing::{chain::RouterChain, implementations::header::HeaderRouter, RouterTrait};
use queryflux_translation::TranslationService;
use tokio::net::TcpListener;

// ---------------------------------------------------------------------------
// No-op metrics store — discards everything, avoids Prometheus registry issues
// ---------------------------------------------------------------------------

struct NoOpMetrics;

#[async_trait]
impl MetricsStore for NoOpMetrics {
    async fn record_query(&self, _r: QueryRecord) -> QfResult<()> {
        Ok(())
    }
    async fn record_cluster_snapshot(&self, _s: ClusterSnapshot) -> QfResult<()> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Group name constants used by tests
// ---------------------------------------------------------------------------

pub const GROUP_DUCKDB: &str = "duckdb";
pub const GROUP_TRINO: &str = "trino";
pub const GROUP_STARROCKS: &str = "starrocks";
/// Available when Lakekeeper is reachable AND the shared Iceberg catalog has
/// been set up in at least one engine. Tests that also need a specific engine
/// should combine: require_group!(GROUP_LAKEKEEPER) + require_group!(GROUP_TRINO).
pub const GROUP_LAKEKEEPER: &str = "lakekeeper";

// ---------------------------------------------------------------------------
// TestHarness
// ---------------------------------------------------------------------------

/// A running QueryFlux Trino HTTP server bound to a random port.
pub struct TestHarness {
    /// Bound port — use `base_url()` to get the full URL.
    pub port: u16,
    /// Which groups are available (depends on env / service discovery).
    pub groups: Vec<String>,
    /// Shutdown signal — dropped when the harness is dropped.
    _shutdown_tx: tokio::sync::oneshot::Sender<()>,
}

impl TestHarness {
    /// Build a harness.  Always adds DuckDB.  Adds Trino/StarRocks/Lakekeeper
    /// when their environment variables are set and connectivity succeeds.
    pub async fn new() -> Result<Self> {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("error")
            .try_init(); // ignore "already initialised" errors

        type GroupEntry = (
            Vec<Arc<ClusterState>>,
            Arc<dyn queryflux_cluster_manager::strategy::ClusterSelectionStrategy>,
        );
        let mut group_states: HashMap<ClusterGroupName, GroupEntry> = HashMap::new();
        let mut adapters: HashMap<String, Arc<dyn EngineAdapterTrait>> = HashMap::new();
        let mut group_members: HashMap<String, Vec<String>> = HashMap::new();
        let mut available_groups: Vec<String> = Vec::new();
        let mut routers: Vec<Box<dyn RouterTrait>> = Vec::new();
        let mut header_map: HashMap<String, ClusterGroupName> = HashMap::new();

        // --- DuckDB (always available — embedded, keep concrete type for Iceberg setup) ---
        let duck_cluster = ClusterName("duckdb-1".to_string());
        let duck_group = ClusterGroupName(GROUP_DUCKDB.to_string());
        let duck_adapter = DuckDbAdapter::new(duck_cluster.clone(), duck_group.clone(), None)
            .expect("Failed to create in-memory DuckDB adapter");
        {
            let state = Arc::new(ClusterState::new(
                duck_cluster.clone(),
                duck_group.clone(),
                EngineType::DuckDb,
                None,
                8,
                true,
            ));
            group_states.insert(
                duck_group.clone(),
                (vec![state], strategy_from_config(None)),
            );
            group_members.insert(GROUP_DUCKDB.to_string(), vec![duck_cluster.0.clone()]);
            available_groups.push(GROUP_DUCKDB.to_string());
            header_map.insert(GROUP_DUCKDB.to_string(), duck_group.clone());
        }

        // --- Trino (optional — needs TRINO_URL or default test port reachable) ---
        let trino_url =
            std::env::var("TRINO_URL").unwrap_or_else(|_| "http://localhost:18081".to_string());
        let trino_available = is_trino_ready(&trino_url).await;
        if trino_available {
            let group = ClusterGroupName(GROUP_TRINO.to_string());
            let cluster = ClusterName("trino-1".to_string());
            let state = Arc::new(ClusterState::new(
                cluster.clone(),
                group.clone(),
                EngineType::Trino,
                Some(trino_url.clone()),
                20,
                true,
            ));
            let adapter = Arc::new(TrinoAdapter::new(
                cluster.clone(),
                group.clone(),
                trino_url,
                false,
                None,
            )) as Arc<dyn EngineAdapterTrait>;

            group_states.insert(group.clone(), (vec![state], strategy_from_config(None)));
            group_members.insert(GROUP_TRINO.to_string(), vec![cluster.0.clone()]);
            adapters.insert(cluster.0.clone(), adapter);
            available_groups.push(GROUP_TRINO.to_string());
            header_map.insert(GROUP_TRINO.to_string(), group);
        }

        // --- StarRocks (optional — keep concrete type for Iceberg setup) ---
        let sr_url = std::env::var("STARROCKS_URL")
            .unwrap_or_else(|_| "mysql://root@localhost:19030".to_string());
        let sr_available = is_starrocks_ready(&sr_url).await;
        let sr_adapter = if sr_available {
            let group = ClusterGroupName(GROUP_STARROCKS.to_string());
            let cluster = ClusterName("starrocks-1".to_string());
            let state = Arc::new(ClusterState::new(
                cluster.clone(),
                group.clone(),
                EngineType::StarRocks,
                Some(sr_url.clone()),
                8,
                true,
            ));
            let adapter = Arc::new(
                StarRocksAdapter::new(cluster.clone(), group.clone(), sr_url, None)
                    .expect("Failed to create StarRocks adapter"),
            );

            group_states.insert(group.clone(), (vec![state], strategy_from_config(None)));
            group_members.insert(GROUP_STARROCKS.to_string(), vec![cluster.0.clone()]);
            available_groups.push(GROUP_STARROCKS.to_string());
            header_map.insert(GROUP_STARROCKS.to_string(), group);
            Some((cluster, adapter))
        } else {
            None
        };

        // --- Lakekeeper (optional — enables Iceberg tests across all engines) ---
        let lakekeeper_url = std::env::var("LAKEKEEPER_URL")
            .unwrap_or_else(|_| "http://localhost:18181".to_string());
        let minio_endpoint =
            std::env::var("MINIO_ENDPOINT").unwrap_or_else(|_| "localhost:19000".to_string());

        if is_lakekeeper_ready(&lakekeeper_url).await {
            let catalog_endpoint = format!("{}/catalog", lakekeeper_url);

            // DuckDB: install iceberg extension, create S3 secret, attach catalog.
            // sts-enabled=false in the warehouse so DuckDB uses static credentials.
            let duck_setup = format!(
                "INSTALL iceberg; \
                 LOAD iceberg; \
                 CREATE OR REPLACE SECRET lakekeeper_minio ( \
                   TYPE S3, KEY_ID 'minio-root-user', SECRET 'minio-root-password', \
                   ENDPOINT '{minio}', USE_SSL false, URL_STYLE 'path', REGION 'local' \
                 ); \
                 ATTACH 'demo' AS lakekeeper ( \
                   TYPE ICEBERG, ENDPOINT '{endpoint}', TOKEN '' \
                 );",
                minio = minio_endpoint,
                endpoint = catalog_endpoint,
            );
            duck_adapter.setup_batch(&duck_setup).await.ok();

            // StarRocks: register external catalog (uses internal Docker addresses).
            if let Some((_, sr)) = &sr_adapter {
                let sr_setup = "CREATE EXTERNAL CATALOG IF NOT EXISTS lakekeeper \
                     PROPERTIES ( \
                       \"type\" = \"iceberg\", \
                       \"iceberg.catalog.type\" = \"rest\", \
                       \"iceberg.catalog.uri\" = \"http://lakekeeper:8181/catalog\", \
                       \"iceberg.catalog.warehouse\" = \"demo\", \
                       \"aws.s3.region\" = \"local\", \
                       \"aws.s3.enable_path_style_access\" = \"true\", \
                       \"aws.s3.endpoint\" = \"http://minio:9000\", \
                       \"aws.s3.access_key\" = \"minio-root-user\", \
                       \"aws.s3.secret_key\" = \"minio-root-password\" \
                     )";
                sr.execute_ddl(sr_setup).await.ok();
            }

            // Trino: the data-loader container already ran CREATE CATALOG lakekeeper.
            // No harness action needed.

            available_groups.push(GROUP_LAKEKEEPER.to_string());
        }

        // Register StarRocks adapter (after Iceberg setup).
        if let Some((cluster, sr)) = sr_adapter {
            adapters.insert(cluster.0.clone(), sr as Arc<dyn EngineAdapterTrait>);
        }

        // Register DuckDB adapter (after Iceberg setup).
        adapters.insert(
            duck_cluster.0.clone(),
            Arc::new(duck_adapter) as Arc<dyn EngineAdapterTrait>,
        );

        // Router: X-Qf-Group header → cluster group
        routers.push(Box::new(HeaderRouter::new(
            "x-qf-group".to_string(),
            header_map,
        )));

        let cluster_manager = Arc::new(SimpleClusterGroupManager::new(group_states));

        // Translation: disabled in tests (sqlglot may not be available)
        let translation = Arc::new(TranslationService::disabled());

        let router_chain = RouterChain::new(
            routers,
            ClusterGroupName(GROUP_DUCKDB.to_string()), // fallback
        );

        // Bind port 0 (OS assigns a free port), then close before starting axum
        // (tiny TOCTOU window, acceptable in tests).
        let tmp = TcpListener::bind("127.0.0.1:0").await?;
        let port = tmp.local_addr()?.port();
        drop(tmp);

        let state = Arc::new(AppState {
            external_address: format!("http://127.0.0.1:{port}"),
            cluster_manager,
            adapters,
            group_members,
            persistence: Arc::new(InMemoryPersistence::new()),
            router_chain,
            translation,
            metrics: Arc::new(NoOpMetrics),
        });

        let router: Router = TrinoHttpFrontend::new(state, port).router();

        let listener = TcpListener::bind(format!("127.0.0.1:{port}")).await?;
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(async {
                    let _ = shutdown_rx.await;
                })
                .await
                .ok();
        });

        // Give the server a moment to be ready.
        tokio::time::sleep(Duration::from_millis(50)).await;

        Ok(Self {
            port,
            groups: available_groups,
            _shutdown_tx: shutdown_tx,
        })
    }

    pub fn base_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }

    /// Returns true if `group` was configured (engine is reachable).
    pub fn has_group(&self, group: &str) -> bool {
        self.groups.iter().any(|g| g == group)
    }
}

// ---------------------------------------------------------------------------
// Connectivity probes
// ---------------------------------------------------------------------------

/// TCP-only probe — check if a host:port is accepting connections.
/// One attempt, short timeout. When `docker compose up --wait` has already
/// guaranteed health, this just confirms the port is reachable from the host.
async fn port_is_open(host: &str, port: u16) -> bool {
    tokio::time::timeout(
        Duration::from_secs(2),
        tokio::net::TcpStream::connect((host, port)),
    )
    .await
    .map(|r| r.is_ok())
    .unwrap_or(false)
}

async fn is_trino_ready(url: &str) -> bool {
    let Ok(parsed) = reqwest::Url::parse(url) else {
        return false;
    };
    let host = parsed.host_str().unwrap_or("localhost");
    let port = parsed.port().unwrap_or(8080);
    port_is_open(host, port).await
}

async fn is_starrocks_ready(url: &str) -> bool {
    let Ok(parsed) = reqwest::Url::parse(url) else {
        return false;
    };
    let host = parsed.host_str().unwrap_or("localhost");
    let port = parsed.port().unwrap_or(9030);
    port_is_open(host, port).await
}

async fn is_lakekeeper_ready(url: &str) -> bool {
    let Ok(parsed) = reqwest::Url::parse(url) else {
        return false;
    };
    let host = parsed.host_str().unwrap_or("localhost");
    let port = parsed.port().unwrap_or(8181);
    port_is_open(host, port).await
}
