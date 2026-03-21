use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Parser;
use queryflux_cluster_manager::{
    cluster_state::ClusterState,
    simple::SimpleClusterGroupManager,
    strategy::strategy_from_config,
};
use queryflux_config::{yaml::YamlFileConfigProvider, ConfigProvider};
use queryflux_core::{
    config::EngineConfig,
    query::{ClusterGroupName, ClusterName, EngineType},
};
use queryflux_persistence::cluster_config::{UpsertClusterConfig, UpsertClusterGroupConfig};
use queryflux_engine_adapters::duckdb::DuckDbAdapter;
use queryflux_engine_adapters::starrocks::StarRocksAdapter;
use queryflux_engine_adapters::trino::TrinoAdapter;
use queryflux_frontend::{
    admin::AdminFrontend,
    flight_sql::FlightSqlFrontend,
    mysql_wire::MysqlWireFrontend,
    postgres_wire::PostgresWireFrontend,
    trino_http::{state::AppState, TrinoHttpFrontend},
    FrontendListenerTrait,
};
use queryflux_metrics::{
    buffered_store::BufferedMetricsStore,
    prometheus_store::PrometheusMetrics,
    MetricsStore, MultiMetricsStore,
};
use queryflux_persistence::{in_memory::InMemoryPersistence, postgres::PostgresStore, AdminStore, ClusterConfigStore};
use queryflux_translation::TranslationService;
use queryflux_routing::{
    chain::RouterChain,
    implementations::{
        client_tags::ClientTagsRouter,
        header::HeaderRouter,
        protocol_based::ProtocolBasedRouter,
        python_script::PythonScriptRouter,
        query_regex::QueryRegexRouter,
    },
    RouterTrait,
};
use tracing::info;

#[derive(Parser)]
#[command(name = "queryflux", about = "Multi-engine SQL query proxy")]
struct Cli {
    #[arg(short, long, default_value = "config.yaml")]
    config: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "queryflux=info,queryflux_frontend=info".into()),
        )
        .init();

    let cli = Cli::parse();

    info!("QueryFlux starting — loading config from: {}", cli.config);
    let mut config = YamlFileConfigProvider::new(&cli.config)
        .load()
        .await
        .context("Failed to load config")?;

    let external_address = config
        .queryflux
        .external_address
        .clone()
        .unwrap_or_else(|| "http://localhost:8080".to_string())
        .trim_end_matches('/')
        .to_string();

    // --- Build persistence + metrics stores (must happen before cluster building) ---
    // When Postgres is configured we seed cluster/group config on first run and read
    // from the DB on subsequent starts, so persistence must be ready before the
    // two-pass cluster/adapter construction below.
    let prometheus = Arc::new(PrometheusMetrics::new().context("Failed to init Prometheus metrics")?);
    let mut pg_store: Option<Arc<PostgresStore>> = None;

    let (persistence, metrics): (Arc<dyn queryflux_persistence::Persistence>, Arc<dyn MetricsStore>) =
        match &config.queryflux.persistence {
            queryflux_core::config::PersistenceConfig::Postgres { url } => {
                let pg = Arc::new(
                    PostgresStore::connect(url)
                        .await
                        .context("Failed to connect to Postgres")?,
                );
                pg.migrate().await.context("Migration failed")?;
                let buffered = Arc::new(BufferedMetricsStore::new(
                    pg.clone() as Arc<dyn MetricsStore>,
                    100,
                    std::time::Duration::from_secs(5),
                ));
                let metrics = Arc::new(MultiMetricsStore::new(vec![
                    prometheus.clone() as Arc<dyn MetricsStore>,
                    buffered as Arc<dyn MetricsStore>,
                ]));
                pg_store = Some(pg.clone());
                (pg as Arc<dyn queryflux_persistence::Persistence>, metrics as Arc<dyn MetricsStore>)
            }
            _ => (
                Arc::new(InMemoryPersistence::new()),
                prometheus.clone() as Arc<dyn MetricsStore>,
            ),
        };

    // --- When Postgres is active, load cluster/group config from DB ---
    // On the very first run (tables empty) we seed from YAML so existing deployments
    // migrate transparently without any manual step.
    if let Some(pg) = &pg_store {
        // Seed cluster configs from YAML if the table is empty.
        if pg.cluster_configs_count().await.context("DB cluster config count")? == 0 {
            info!("Seeding cluster configs from YAML into Postgres (first run)");
            for (name, cfg) in &config.clusters {
                if let Some(upsert) = UpsertClusterConfig::from_core(cfg) {
                    pg.upsert_cluster_config(name, &upsert)
                        .await
                        .with_context(|| format!("Seeding cluster '{name}'"))?;
                }
            }
        }
        // Seed group configs from YAML if the table is empty.
        if pg.group_configs_count().await.context("DB group config count")? == 0 {
            info!("Seeding cluster group configs from YAML into Postgres (first run)");
            for (name, cfg) in &config.cluster_groups {
                pg.upsert_group_config(name, &UpsertClusterGroupConfig::from_core(cfg))
                    .await
                    .with_context(|| format!("Seeding group '{name}'"))?;
            }
        }

        // Override YAML config with DB (source of truth when Postgres is configured).
        info!("Loading cluster and group configs from Postgres");
        config.clusters = pg
            .list_cluster_configs()
            .await
            .context("Load cluster configs from DB")?
            .into_iter()
            .map(|r| {
                let name = r.name.clone();
                r.to_core()
                    .map(|c| (name, c))
                    .map_err(|e| anyhow::anyhow!("{e}"))
            })
            .collect::<anyhow::Result<_>>()?;

        config.cluster_groups = pg
            .list_group_configs()
            .await
            .context("Load group configs from DB")?
            .into_iter()
            .map(|r| (r.name.clone(), r.to_core()))
            .collect();
    }

    // --- Validate cluster configs against the engine registry ---
    {
        use queryflux_core::engine_registry::validate_cluster_config;
        let mut all_errors: Vec<String> = Vec::new();
        for (name, cfg) in &config.clusters {
            all_errors.extend(validate_cluster_config(name, cfg));
        }
        if !all_errors.is_empty() {
            for e in &all_errors {
                tracing::error!("{e}");
            }
            anyhow::bail!("Config validation failed with {} error(s)", all_errors.len());
        }
    }

    // --- Build cluster states and adapters (two-pass) ---
    //
    // Pass 1: iterate `config.clusters`, build one adapter per cluster name.
    // Pass 2: iterate `config.cluster_groups`, resolve members, build ClusterStates.

    type AdapterMap = HashMap<String, Arc<dyn queryflux_engine_adapters::EngineAdapterTrait>>;
    let mut adapters: AdapterMap = HashMap::new();
    // Flat list of (adapter, state) pairs used by the health-check and reconciler loops.
    // States here are the first ClusterState built per cluster (for health-check purposes).
    let mut health_check_pairs: Vec<(Arc<dyn queryflux_engine_adapters::EngineAdapterTrait>, Arc<ClusterState>)> = Vec::new();

    // Pass 1 — one adapter per cluster.
    for (cluster_name_str, cluster_cfg) in &config.clusters {
        if !cluster_cfg.enabled {
            tracing::info!(cluster = %cluster_name_str, "Cluster disabled — skipping");
            continue;
        }
        let cluster_name = ClusterName(cluster_name_str.clone());
        // Use a placeholder group for adapter construction (adapters don't use group_name at runtime).
        let placeholder_group = ClusterGroupName("_".to_string());
        let engine = cluster_cfg.engine.as_ref()
            .context(format!("cluster '{cluster_name_str}' missing required 'engine' field"))?;

        let adapter: Arc<dyn queryflux_engine_adapters::EngineAdapterTrait> = match engine {
            EngineConfig::Trino => {
                let endpoint = cluster_cfg.endpoint.clone()
                    .context(format!("cluster '{cluster_name_str}' missing endpoint"))?;
                let tls_skip = cluster_cfg.tls.as_ref()
                    .map(|t| t.insecure_skip_verify)
                    .unwrap_or(false);
                Arc::new(TrinoAdapter::new(
                    cluster_name.clone(),
                    placeholder_group,
                    endpoint,
                    tls_skip,
                    cluster_cfg.auth.clone(),
                ))
            }
            EngineConfig::DuckDb => {
                Arc::new(
                    DuckDbAdapter::new(cluster_name.clone(), placeholder_group, cluster_cfg.database_path.clone())
                        .context(format!("Failed to open DuckDB for cluster '{cluster_name_str}'"))?,
                )
            }
            EngineConfig::StarRocks => {
                let endpoint = cluster_cfg.endpoint.clone()
                    .context(format!("cluster '{cluster_name_str}' missing endpoint"))?;
                Arc::new(
                    StarRocksAdapter::new(cluster_name.clone(), placeholder_group, endpoint, cluster_cfg.auth.clone())
                        .context(format!("Failed to create StarRocks adapter for cluster '{cluster_name_str}'"))?,
                )
            }
            other => anyhow::bail!("Engine {other:?} not yet implemented"),
        };

        adapters.insert(cluster_name_str.clone(), adapter);
    }

    // Pass 2 — one group entry per cluster_group, resolving member cluster names.
    type GroupMap = HashMap<ClusterGroupName, (Vec<Arc<ClusterState>>, Arc<dyn queryflux_cluster_manager::strategy::ClusterSelectionStrategy>)>;
    let mut group_states: GroupMap = HashMap::new();
    let mut group_members: HashMap<String, Vec<String>> = HashMap::new();

    for (group_name, group_config) in &config.cluster_groups {
        if !group_config.enabled {
            tracing::info!(group = %group_name, "Cluster group disabled — skipping");
            continue;
        }
        let group_key = ClusterGroupName(group_name.clone());
        let mut states: Vec<Arc<ClusterState>> = Vec::new();

        for member_name in &group_config.members {
            let cluster_cfg = config.clusters.get(member_name)
                .context(format!("group '{group_name}' references unknown cluster '{member_name}'"))?;

            let adapter = match adapters.get(member_name) {
                Some(a) => a.clone(),
                None => {
                    // Cluster was disabled in Pass 1 — skip silently.
                    tracing::info!(group = %group_name, cluster = %member_name, "Skipping disabled cluster in group");
                    continue;
                }
            };

            let engine = cluster_cfg.engine.as_ref()
                .context(format!("cluster '{member_name}' missing engine"))?;
            let engine_type = engine_type_from_config(engine);

            let state = Arc::new(ClusterState::new(
                ClusterName(member_name.clone()),
                group_key.clone(),
                engine_type,
                cluster_cfg.endpoint.clone(),
                group_config.max_running_queries,
                cluster_cfg.enabled,
            ));
            states.push(state.clone());

            // Register in health_check_pairs only once per cluster (first group wins).
            if !health_check_pairs.iter().any(|(_, s)| s.cluster_name.0 == *member_name) {
                health_check_pairs.push((adapter, state));
            }
        }

        let strategy = strategy_from_config(group_config.strategy.as_ref());
        group_members.insert(group_name.clone(), group_config.members.clone());
        group_states.insert(group_key, (states, strategy));
    }

    let cluster_manager = Arc::new(SimpleClusterGroupManager::new(group_states));

    // --- Build translation service ---
    let translation = Arc::new(
        TranslationService::new_sqlglot()
            .unwrap_or_else(|e| {
                tracing::warn!("sqlglot unavailable ({e}), translation disabled");
                TranslationService::disabled()
            })
    );

    // --- Build router chain ---
    let fallback = ClusterGroupName(config.routing_fallback.clone());
    let mut routers: Vec<Box<dyn RouterTrait>> = Vec::new();

    for router_cfg in &config.routers {
        use queryflux_core::config::RouterConfig;
        match router_cfg {
            RouterConfig::ProtocolBased { trino_http, postgres_wire, mysql_wire, clickhouse_http } => {
                routers.push(Box::new(ProtocolBasedRouter {
                    trino_http: trino_http.as_ref().map(|s| ClusterGroupName(s.clone())),
                    postgres_wire: postgres_wire.as_ref().map(|s| ClusterGroupName(s.clone())),
                    mysql_wire: mysql_wire.as_ref().map(|s| ClusterGroupName(s.clone())),
                    clickhouse_http: clickhouse_http.as_ref().map(|s| ClusterGroupName(s.clone())),
                }));
            }
            RouterConfig::Header { header_name, header_value_to_group } => {
                let mapping = header_value_to_group
                    .iter()
                    .map(|(k, v)| (k.clone(), ClusterGroupName(v.clone())))
                    .collect();
                routers.push(Box::new(HeaderRouter::new(header_name.clone(), mapping)));
            }
            RouterConfig::QueryRegex { rules } => {
                let pairs = rules
                    .iter()
                    .map(|r| (r.regex.clone(), r.target_group.clone()))
                    .collect();
                routers.push(Box::new(QueryRegexRouter::new(pairs)));
            }
            RouterConfig::ClientTags { tag_to_group } => {
                let mapping = tag_to_group
                    .iter()
                    .map(|(k, v)| (k.clone(), ClusterGroupName(v.clone())))
                    .collect();
                routers.push(Box::new(ClientTagsRouter::new(mapping)));
            }
            RouterConfig::PythonScript { script, script_file } => {
                let router = if let Some(path) = script_file {
                    PythonScriptRouter::from_file(path)
                        .context(format!("Failed to load routing script from {path}"))?
                } else {
                    PythonScriptRouter::new(script.clone())
                };
                routers.push(Box::new(router));
            }
            _ => {
                tracing::warn!("Router type not yet implemented, skipping");
            }
        }
    }

    let router_chain = RouterChain::new(routers, fallback);

    // --- Build app state ---
    let app_state = Arc::new(AppState {
        external_address: external_address.clone(),
        cluster_manager,
        adapters,
        group_members,
        persistence,
        router_chain,
        translation,
        metrics,
    });

    // --- Start admin server (Prometheus /metrics + future /admin/* endpoints) ---
    let admin_port = config.queryflux.admin_api.port;
    let admin_store = pg_store.map(|pg| pg as Arc<dyn AdminStore>);
    let admin = AdminFrontend::new(prometheus, app_state.cluster_manager.clone(), admin_store, admin_port);

    // --- Start Trino HTTP frontend ---
    let trino_port = config.queryflux.frontends.trino_http.port;
    let frontend = TrinoHttpFrontend::new(app_state.clone(), trino_port);

    info!(
        "QueryFlux ready — Trino HTTP on :{trino_port}, admin/metrics on :{admin_port}, external address: {external_address}"
    );

    // Background task: push cluster utilization snapshots to Prometheus every 5s.
    tokio::spawn({
        let state = app_state.clone();
        async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
            loop {
                interval.tick().await;
                if let Ok(snapshots) = state.cluster_manager.all_cluster_states().await {
                    for snap in snapshots {
                        let record = queryflux_metrics::ClusterSnapshot {
                            cluster_name: snap.cluster_name,
                            group_name: snap.group_name,
                            engine_type: snap.engine_type,
                            running_queries: snap.running_queries,
                            queued_queries: snap.queued_queries,
                            max_running_queries: snap.max_running_queries,
                            recorded_at: chrono::Utc::now(),
                        };
                        let _ = state.metrics.record_cluster_snapshot(record).await;
                    }
                }
            }
        }
    });

    // Background task: release capacity for zombie executing queries (client disconnected
    // before polling to completion). Runs every 120s; evicts entries not polled for > 5 min.
    //
    // Uses `last_accessed` from persistence — updated by any proxy instance that handles
    // a poll, throttled to at most one write per 120s. Safe across multiple instances.
    tokio::spawn({
        let state = app_state.clone();
        async move {
            const CLIENT_TIMEOUT_SECS: i64 = 300; // matches Trino's query.client.timeout default
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(120));
            loop {
                interval.tick().await;
                let Ok(all) = state.persistence.list_all().await else { continue };
                let cutoff = chrono::Utc::now() - chrono::Duration::seconds(CLIENT_TIMEOUT_SECS);
                for q in all {
                    if q.last_accessed < cutoff {
                        tracing::warn!(
                            id = %q.backend_query_id,
                            cluster = %q.cluster_name,
                            group = %q.cluster_group,
                            last_accessed = %q.last_accessed,
                            "Evicting zombie executing query — not polled for >5 min"
                        );
                        state.metrics.on_query_finished(&q.cluster_group.0, &q.cluster_name.0);
                        let _ = state.cluster_manager.release_cluster(&q.cluster_group, &q.cluster_name).await;
                        let _ = state.persistence.delete(&q.backend_query_id).await;
                    }
                }
            }
        }
    });

    // Background task: clean up stale queued queries (client disconnected before getting
    // cluster capacity). Mirrors trino-lb's LeftoverQueryDetector. Runs every 120s;
    // deletes queued entries not accessed for > 5 minutes.
    tokio::spawn({
        let state = app_state.clone();
        async move {
            const CLIENT_TIMEOUT_SECS: i64 = 300;
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(120));
            loop {
                interval.tick().await;
                let cutoff = chrono::Utc::now() - chrono::Duration::seconds(CLIENT_TIMEOUT_SECS);
                match state.persistence.delete_queued_not_accessed_since(cutoff).await {
                    Ok(0) => {}
                    Ok(n) => tracing::info!("Cleaned up {n} stale queued queries"),
                    Err(e) => tracing::warn!("Queued query cleanup failed: {e}"),
                }
            }
        }
    });

    let health_check_pairs = Arc::new(health_check_pairs);

    // Background task: health-check each cluster every 30s via its adapter.
    tokio::spawn({
        let pairs = health_check_pairs.clone();
        async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));
            loop {
                interval.tick().await;
                for (adapter, state) in pairs.as_ref() {
                    let healthy = adapter.health_check().await;
                    if !healthy {
                        tracing::warn!(
                            cluster = %state.cluster_name.0,
                            group = %state.group_name.0,
                            "Health check failed — marking cluster unhealthy"
                        );
                    } else if !state.is_healthy() {
                        tracing::info!(
                            cluster = %state.cluster_name.0,
                            group = %state.group_name.0,
                            "Health check recovered — marking cluster healthy"
                        );
                    }
                    state.set_healthy(healthy);
                }
            }
        }
    });

    // Background task: reconcile in-memory running_queries counters with ground truth
    // from each engine (engines that implement fetch_running_query_count). Runs every 30s.
    // Corrects drift caused by proxy crashes, client disconnects, or any other leak.
    tokio::spawn({
        let pairs = health_check_pairs.clone();
        async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));
            loop {
                interval.tick().await;
                for (adapter, state) in pairs.as_ref() {
                    if let Some(actual) = adapter.fetch_running_query_count().await {
                        let tracked = state.running_queries();
                        if actual != tracked {
                            tracing::info!(
                                cluster = %state.cluster_name.0,
                                group = %state.group_name.0,
                                tracked,
                                actual,
                                "Reconciling running_queries counter with engine ground truth"
                            );
                            state.set_running_queries(actual);
                        }
                    }
                }
            }
        }
    });

    // Run all enabled frontends concurrently; any one exiting stops the process.
    let mysql_future = async {
        match &config.queryflux.frontends.mysql_wire {
            Some(cfg) if cfg.enabled => {
                MysqlWireFrontend::new(app_state.clone(), cfg.port)
                    .listen()
                    .await
            }
            _ => std::future::pending::<queryflux_core::error::Result<()>>().await,
        }
    };

    let postgres_future = async {
        match &config.queryflux.frontends.postgres_wire {
            Some(cfg) if cfg.enabled => {
                PostgresWireFrontend::new(app_state.clone(), cfg.port)
                    .listen()
                    .await
            }
            _ => std::future::pending::<queryflux_core::error::Result<()>>().await,
        }
    };

    let flight_sql_future = async {
        match &config.queryflux.frontends.flight_sql {
            Some(cfg) if cfg.enabled => {
                FlightSqlFrontend::new(app_state.clone(), cfg.port)
                    .listen()
                    .await
            }
            _ => std::future::pending::<queryflux_core::error::Result<()>>().await,
        }
    };

    tokio::select! {
        r = frontend.listen()    => r.map_err(|e| anyhow::anyhow!("{e}"))?,
        r = admin.listen()       => r.map_err(|e| anyhow::anyhow!("{e}"))?,
        r = mysql_future         => r.map_err(|e| anyhow::anyhow!("{e}"))?,
        r = postgres_future      => r.map_err(|e| anyhow::anyhow!("{e}"))?,
        r = flight_sql_future    => r.map_err(|e| anyhow::anyhow!("{e}"))?,
    }

    Ok(())
}

fn engine_type_from_config(cfg: &EngineConfig) -> EngineType {
    match cfg {
        EngineConfig::Trino => EngineType::Trino,
        EngineConfig::DuckDb => EngineType::DuckDb,
        EngineConfig::StarRocks => EngineType::StarRocks,
        EngineConfig::ClickHouse => EngineType::ClickHouse,
    }
}
