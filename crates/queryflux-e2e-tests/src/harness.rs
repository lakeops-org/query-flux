/// Test harness: in-process QueryFlux Trino HTTP server on a random port.
///
/// Backends are optional and discovered via connectivity / env:
///   TRINO_URL         — default http://localhost:18081
///   STARROCKS_URL     — default mysql://root@localhost:19030
///
/// Lakekeeper / Iceberg (optional):
///   LAKEKEEPER_URL, MINIO_ENDPOINT — StarRocks external catalog DDL only.
///
/// At least one of Trino or StarRocks must be reachable or [`TestHarness::new`] fails.
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use axum::Router;
use queryflux_auth::{
    AllowAllAuthorization, AuthProvider, AuthorizationChecker, BackendIdentityResolver,
    NoneAuthProvider,
};
use queryflux_cluster_manager::{
    cluster_state::ClusterState, simple::SimpleClusterGroupManager, strategy::strategy_from_config,
};
use queryflux_core::config::{ClusterAuth, ClusterConfig, EngineConfig};
use queryflux_core::{
    error::Result as QfResult,
    query::{ClusterGroupName, ClusterName, EngineType},
};
use queryflux_engine_adapters::{
    snowflake::SnowflakeAdapter, starrocks::StarRocksAdapter, trino::TrinoAdapter,
    EngineAdapterTrait,
};
use queryflux_frontend::{
    snowflake::{http::session_store::SnowflakeSessionStore, SnowflakeFrontend},
    state::LiveConfig,
    trino_http::{state::AppState, TrinoHttpFrontend},
};
use queryflux_metrics::{ClusterSnapshot, MetricsStore, QueryRecord};
use queryflux_persistence::in_memory::InMemoryPersistence;
use queryflux_routing::{chain::RouterChain, implementations::header::HeaderRouter, RouterTrait};
use queryflux_translation::TranslationService;
use tokio::net::TcpListener;

struct CapturingMetrics {
    records: Arc<Mutex<Vec<QueryRecord>>>,
}

#[async_trait]
impl MetricsStore for CapturingMetrics {
    async fn record_query(&self, r: QueryRecord) -> QfResult<()> {
        self.records.lock().expect("lock records").push(r);
        Ok(())
    }

    async fn record_cluster_snapshot(&self, _s: ClusterSnapshot) -> QfResult<()> {
        Ok(())
    }
}

pub const GROUP_TRINO: &str = "trino";
pub const GROUP_STARROCKS: &str = "starrocks";
pub const GROUP_SNOWFLAKE: &str = "snowflake";
/// Set when Lakekeeper port is reachable (Iceberg tables seeded by e2e tests via Trino).
pub const GROUP_LAKEKEEPER: &str = "lakekeeper";

pub struct TestHarness {
    pub port: u16,
    pub groups: Vec<String>,
    records: Arc<Mutex<Vec<QueryRecord>>>,
    _shutdown_tx: tokio::sync::oneshot::Sender<()>,
}

impl TestHarness {
    pub async fn new() -> Result<Self> {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("error")
            .try_init();

        type GroupEntry = (
            Vec<Arc<ClusterState>>,
            Arc<dyn queryflux_cluster_manager::strategy::ClusterSelectionStrategy>,
        );
        let mut group_states: HashMap<ClusterGroupName, GroupEntry> = HashMap::new();
        let mut adapters: HashMap<String, Arc<dyn EngineAdapterTrait>> = HashMap::new();
        let mut group_members: HashMap<String, Vec<String>> = HashMap::new();
        let mut group_order: Vec<String> = Vec::new();
        let mut available_groups: Vec<String> = Vec::new();
        let mut routers: Vec<Box<dyn RouterTrait>> = Vec::new();
        let mut header_map: HashMap<String, ClusterGroupName> = HashMap::new();

        // --- Trino ---
        let trino_url =
            std::env::var("TRINO_URL").unwrap_or_else(|_| "http://localhost:18081".to_string());
        let trino_available = is_trino_ready(&trino_url).await;
        if trino_available {
            let group = ClusterGroupName(GROUP_TRINO.to_string());
            let cluster = ClusterName("trino-1".to_string());
            let state = Arc::new(ClusterState::new(
                cluster.clone(),
                group.clone(),
                None,
                None,
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
            group_order.push(GROUP_TRINO.to_string());
            adapters.insert(cluster.0.clone(), adapter);
            available_groups.push(GROUP_TRINO.to_string());
            header_map.insert(GROUP_TRINO.to_string(), group);
        }

        // --- StarRocks ---
        let sr_url = std::env::var("STARROCKS_URL")
            .unwrap_or_else(|_| "mysql://root@localhost:9030".to_string());
        let sr_available = is_starrocks_ready(&sr_url).await;
        let sr_adapter = if sr_available {
            let group = ClusterGroupName(GROUP_STARROCKS.to_string());
            let cluster = ClusterName("starrocks-1".to_string());
            let state = Arc::new(ClusterState::new(
                cluster.clone(),
                group.clone(),
                None,
                None,
                EngineType::StarRocks,
                Some(sr_url.clone()),
                8,
                true,
            ));
            let adapter = Arc::new(
                StarRocksAdapter::new(cluster.clone(), group.clone(), sr_url, None)
                    .map_err(|e| anyhow!("StarRocks adapter: {e}"))?,
            );

            group_states.insert(group.clone(), (vec![state], strategy_from_config(None)));
            group_members.insert(GROUP_STARROCKS.to_string(), vec![cluster.0.clone()]);
            group_order.push(GROUP_STARROCKS.to_string());
            available_groups.push(GROUP_STARROCKS.to_string());
            header_map.insert(GROUP_STARROCKS.to_string(), group);
            Some((cluster, adapter))
        } else {
            None
        };

        // --- Lakekeeper + StarRocks Iceberg catalog ---
        let lakekeeper_url = std::env::var("LAKEKEEPER_URL")
            .unwrap_or_else(|_| "http://localhost:18181".to_string());
        if is_lakekeeper_ready(&lakekeeper_url).await {
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
            available_groups.push(GROUP_LAKEKEEPER.to_string());
        }

        if let Some((cluster, sr)) = sr_adapter {
            adapters.insert(cluster.0.clone(), sr as Arc<dyn EngineAdapterTrait>);
        }

        // --- Snowflake (fakesnow) ---
        let fakesnow_url =
            std::env::var("FAKESNOW_URL").unwrap_or_else(|_| "http://localhost:18085".to_string());
        let fakesnow_available = is_fakesnow_ready(&fakesnow_url).await;
        if fakesnow_available {
            let group = ClusterGroupName(GROUP_SNOWFLAKE.to_string());
            let cluster = ClusterName("snowflake-1".to_string());
            let state = Arc::new(ClusterState::new(
                cluster.clone(),
                group.clone(),
                None,
                None,
                EngineType::Snowflake,
                Some(fakesnow_url.clone()),
                8,
                true,
            ));
            let cfg = ClusterConfig {
                engine: Some(EngineConfig::Snowflake),
                enabled: true,
                max_running_queries: None,
                endpoint: Some(fakesnow_url),
                database_path: None,
                region: None,
                s3_output_location: None,
                workgroup: None,
                catalog: None,
                account: Some("fakesnow".to_string()),
                warehouse: None,
                role: None,
                schema: None,
                tls: None,
                auth: Some(ClusterAuth::Basic {
                    username: "fake".to_string(),
                    password: "snow".to_string(),
                }),
                query_auth: None,
            };
            let adapter = Arc::new(
                SnowflakeAdapter::try_from_cluster_config(
                    cluster.clone(),
                    group.clone(),
                    &cfg,
                    "snowflake-1",
                )
                .map_err(|e| anyhow!("Snowflake adapter: {e}"))?,
            ) as Arc<dyn EngineAdapterTrait>;

            group_states.insert(group.clone(), (vec![state], strategy_from_config(None)));
            group_members.insert(GROUP_SNOWFLAKE.to_string(), vec![cluster.0.clone()]);
            group_order.push(GROUP_SNOWFLAKE.to_string());
            adapters.insert(cluster.0.clone(), adapter);
            available_groups.push(GROUP_SNOWFLAKE.to_string());
            header_map.insert(GROUP_SNOWFLAKE.to_string(), group);
        }

        if group_states.is_empty() {
            return Err(anyhow!(
                "No backends reachable. Start docker compose (see docker/test/docker-compose.test.yml): \
                 Trino :18081 and/or StarRocks :19030 and/or fakesnow :18085."
            ));
        }

        let fallback = pick_fallback_group(&group_order);
        // Route compatibility:
        // - `X-Qf-Group` is our internal E2E routing header (legacy tests).
        // - `X-Trino-Client-Tags` is set by real Trino clients like `trino-rust-client`.
        //   We route on it so e2e tests can behave like real-world Trino traffic.
        let header_map_qf = header_map.clone();
        routers.push(Box::new(HeaderRouter::new(
            "x-qf-group".to_string(),
            header_map_qf,
        )));
        routers.push(Box::new(HeaderRouter::new(
            "x-trino-client-tags".to_string(),
            header_map,
        )));

        let cluster_manager = Arc::new(SimpleClusterGroupManager::new(group_states));
        let translation = Arc::new(TranslationService::disabled());
        let router_chain = RouterChain::new(routers, fallback);

        let tmp = TcpListener::bind("127.0.0.1:0").await?;
        let port = tmp.local_addr()?.port();
        drop(tmp);

        let live_config = LiveConfig {
            router_chain,
            cluster_manager,
            adapters,
            health_check_targets: vec![],
            cluster_configs: HashMap::new(),
            group_members,
            group_order,
            group_translation_scripts: HashMap::new(),
            group_default_tags: HashMap::new(),
        };
        let records = Arc::new(Mutex::new(Vec::<QueryRecord>::new()));
        let state = Arc::new(AppState {
            external_address: format!("http://127.0.0.1:{port}"),
            live: Arc::new(tokio::sync::RwLock::new(live_config)),
            persistence: Arc::new(InMemoryPersistence::new()),
            translation,
            metrics: Arc::new(CapturingMetrics {
                records: records.clone(),
            }),
            auth_provider: Arc::new(NoneAuthProvider::new(false)) as Arc<dyn AuthProvider>,
            authorization: Arc::new(AllowAllAuthorization) as Arc<dyn AuthorizationChecker>,
            identity_resolver: Arc::new(BackendIdentityResolver::new()),
            snowflake_sessions: SnowflakeSessionStore::new(Default::default()),
        });

        let trino_fe = TrinoHttpFrontend::new(state.clone(), port);
        let snowflake_fe = SnowflakeFrontend::new(state, port);
        let router: Router = trino_fe.router().merge(snowflake_fe.router());
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

        tokio::time::sleep(Duration::from_millis(50)).await;

        Ok(Self {
            port,
            groups: available_groups,
            records,
            _shutdown_tx: shutdown_tx,
        })
    }

    pub fn base_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }

    pub fn has_group(&self, group: &str) -> bool {
        self.groups.iter().any(|g| g == group)
    }

    pub fn clear_records(&self) {
        self.records.lock().expect("lock records").clear();
    }

    pub async fn wait_for_record<F>(&self, predicate: F) -> Option<QueryRecord>
    where
        F: Fn(&QueryRecord) -> bool,
    {
        for _ in 0..50 {
            if let Some(record) = self
                .records
                .lock()
                .expect("lock records")
                .iter()
                .find(|r| predicate(r))
                .cloned()
            {
                return Some(record);
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        None
    }
}

fn pick_fallback_group(group_order: &[String]) -> ClusterGroupName {
    for preferred in [GROUP_TRINO, GROUP_STARROCKS, GROUP_SNOWFLAKE] {
        if group_order.iter().any(|g| g == preferred) {
            return ClusterGroupName(preferred.to_string());
        }
    }
    ClusterGroupName(group_order[0].clone())
}

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

async fn is_fakesnow_ready(url: &str) -> bool {
    let Ok(parsed) = reqwest::Url::parse(url) else {
        return false;
    };
    let host = parsed.host_str().unwrap_or("localhost");
    let port = parsed.port().unwrap_or(8085);
    port_is_open(host, port).await
}
