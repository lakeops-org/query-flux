use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Parser;
use queryflux_cluster_manager::{
    cluster_state::ClusterState,
    simple::SimpleClusterGroupManager,
};
use queryflux_config::{yaml::YamlFileConfigProvider, ConfigProvider};
use queryflux_core::{
    config::{ClusterGroupConfig, EngineConfig},
    query::{ClusterGroupName, ClusterName, EngineType},
};
use queryflux_engine_adapters::trino::TrinoAdapter;
use queryflux_frontend::{trino_http::{state::AppState, TrinoHttpFrontend}, FrontendListenerTrait};
use queryflux_persistence::in_memory::InMemoryPersistence;
use queryflux_routing::{
    chain::RouterChain,
    implementations::{
        header::HeaderRouter,
        protocol_based::ProtocolBasedRouter,
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
    let config = YamlFileConfigProvider::new(&cli.config)
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

    // --- Build cluster states and adapters ---
    let mut group_states: HashMap<ClusterGroupName, Vec<Arc<ClusterState>>> = HashMap::new();
    let mut adapters: HashMap<(String, String), Arc<dyn queryflux_engine_adapters::EngineAdapterTrait>> = HashMap::new();

    for (group_name, group_config) in &config.cluster_groups {
        let group_key = ClusterGroupName(group_name.clone());
        let mut states = Vec::new();

        for cluster_cfg in &group_config.clusters {
            let cluster_name = ClusterName(cluster_cfg.name.clone());
            let engine_type = engine_type_from_config(&group_config.engine);

            let state = Arc::new(ClusterState::new(
                cluster_name.clone(),
                group_key.clone(),
                engine_type.clone(),
                cluster_cfg.endpoint.clone(),
                group_config.max_running_queries,
            ));
            states.push(state);

            // Build adapter for this cluster.
            let adapter: Arc<dyn queryflux_engine_adapters::EngineAdapterTrait> =
                match &group_config.engine {
                    EngineConfig::Trino => {
                        let endpoint = cluster_cfg
                            .endpoint
                            .clone()
                            .context(format!("cluster {} missing endpoint", cluster_cfg.name))?;
                        let tls_skip = cluster_cfg
                            .tls
                            .as_ref()
                            .map(|t| t.insecure_skip_verify)
                            .unwrap_or(false);
                        Arc::new(TrinoAdapter::new(
                            cluster_name.clone(),
                            group_key.clone(),
                            endpoint,
                            tls_skip,
                        ))
                    }
                    other => {
                        anyhow::bail!(
                            "Engine {:?} not yet implemented in Phase 1",
                            other
                        );
                    }
                };

            adapters.insert(
                (group_name.clone(), cluster_cfg.name.clone()),
                adapter,
            );
        }

        group_states.insert(group_key, states);
    }

    let cluster_manager = Arc::new(SimpleClusterGroupManager::new(group_states));
    let persistence = Arc::new(InMemoryPersistence::new());

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
            _ => {
                tracing::warn!("Router type not yet implemented in Phase 1, skipping");
            }
        }
    }

    let router_chain = RouterChain::new(routers, fallback);

    // --- Build app state ---
    let app_state = Arc::new(AppState {
        external_address: external_address.clone(),
        cluster_manager,
        adapters,
        persistence,
        router_chain,
    });

    // --- Start Trino HTTP frontend ---
    let trino_port = config.queryflux.frontends.trino_http.port;
    let frontend = TrinoHttpFrontend::new(app_state, trino_port);

    info!(
        "QueryFlux ready — Trino HTTP on :{trino_port}, external address: {external_address}"
    );

    frontend.listen().await.map_err(|e| anyhow::anyhow!("{e}"))?;

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
