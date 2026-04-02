use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Parser;
use queryflux_auth::{
    AllowAllAuthorization, BackendIdentityResolver, LdapAuthProvider, NoneAuthProvider,
    OidcAuthProvider, OpenFgaAuthorizationClient, SimpleAuthorizationPolicy, StaticAuthProvider,
};
use queryflux_cluster_manager::{
    cluster_state::ClusterState, simple::SimpleClusterGroupManager, strategy::strategy_from_config,
};
use queryflux_config::{yaml::YamlFileConfigProvider, ConfigProvider};
use queryflux_core::query::{ClusterGroupName, ClusterName, EngineType};
use queryflux_frontend::{
    admin::{
        build_frontends_status, AdminFrontend, RoutingConfigDto as AdminRoutingConfigDto,
        SecurityConfigDto as AdminSecurityConfigDto,
    },
    flight_sql::FlightSqlFrontend,
    mysql_wire::MysqlWireFrontend,
    postgres_wire::PostgresWireFrontend,
    snowflake::{http::session_store::SnowflakeSessionStore, SnowflakeFrontend},
    state::LiveConfig,
    trino_http::{state::AppState, TrinoHttpFrontend},
    FrontendListenerTrait,
};
use queryflux_metrics::{
    buffered_store::BufferedMetricsStore, prometheus_store::PrometheusMetrics, MetricsStore,
    MultiMetricsStore,
};
use queryflux_persistence::cluster_config::{UpsertClusterConfig, UpsertClusterGroupConfig};
use queryflux_persistence::{
    in_memory::InMemoryPersistence, postgres::PostgresStore, AdminStore, ClusterConfigStore,
    ProxySettingsStore, RoutingConfigStore,
};
use queryflux_routing::{
    chain::RouterChain,
    implementations::{
        compound::CompoundRouter, header::HeaderRouter, protocol_based::ProtocolBasedRouter,
        python_script::PythonScriptRouter, query_regex::QueryRegexRouter, tags::TagsRouter,
    },
    RouterTrait,
};
use queryflux_translation::TranslationService;
use tracing::info;

mod registered_engines;

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
    let prometheus = Arc::new(
        PrometheusMetrics::new_with_deny_list(config.queryflux.metrics.tags_deny_list.clone())
            .context("Failed to init Prometheus metrics")?,
    );
    let mut pg_store: Option<Arc<PostgresStore>> = None;

    let (persistence, metrics): (
        Arc<dyn queryflux_persistence::Persistence>,
        Arc<dyn MetricsStore>,
    ) = match &config.queryflux.persistence {
        queryflux_core::config::PersistenceConfig::Postgres { conn } => {
            let url = conn
                .connection_url()
                .map_err(|m| anyhow::anyhow!("Invalid postgres persistence config: {m}"))?;
            let pg = Arc::new(
                PostgresStore::connect(&url)
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
            (
                pg as Arc<dyn queryflux_persistence::Persistence>,
                metrics as Arc<dyn MetricsStore>,
            )
        }
        _ => (
            Arc::new(InMemoryPersistence::new()),
            prometheus.clone() as Arc<dyn MetricsStore>,
        ),
    };

    // Filled when Postgres loads cluster/group rows — used for query_history FKs on ClusterState.
    let mut cluster_ids_by_name: HashMap<String, i64> = HashMap::new();
    let mut group_ids_by_name: HashMap<String, i64> = HashMap::new();
    // DB cluster records kept for adapter building via build_adapter_from_record.
    let mut startup_cluster_records: Option<
        Vec<queryflux_persistence::cluster_config::ClusterConfigRecord>,
    > = None;

    // --- When Postgres is active, load cluster/group config from DB ---
    // Merge YAML-defined clusters and groups into Postgres on **every** startup when the
    // file declares them (`clusters` / `clusterGroups` non-empty). This keeps Docker/Compose
    // configs authoritative even if the volume already had older rows (e.g. switched engine).
    // **Studio-first** setups omit those maps (or leave them empty) — then nothing is written
    // here and the DB remains the source of truth for those resources.
    if let Some(pg) = &pg_store {
        if !config.clusters.is_empty() {
            info!("Applying cluster definitions from YAML to Postgres");
            for (name, cfg) in &config.clusters {
                if let Some(upsert) = UpsertClusterConfig::from_core(cfg) {
                    pg.upsert_cluster_config(name, &upsert)
                        .await
                        .with_context(|| format!("Upsert cluster '{name}' from YAML"))?;
                }
            }
        }
        if !config.cluster_groups.is_empty() {
            info!("Applying cluster group definitions from YAML to Postgres");
            for (name, cfg) in &config.cluster_groups {
                pg.upsert_group_config(name, &UpsertClusterGroupConfig::from_core(cfg))
                    .await
                    .with_context(|| format!("Upsert group '{name}' from YAML"))?;
            }
        }

        // Effective config comes from Postgres (YAML above only upserts keys that appear in the file).
        info!("Loading cluster and group configs from Postgres");
        let db_cluster_records = pg
            .list_cluster_configs()
            .await
            .context("Load cluster configs from DB")?;
        cluster_ids_by_name = db_cluster_records
            .iter()
            .map(|r| (r.name.clone(), r.id))
            .collect();
        // Build minimal ClusterConfig values for validation and group resolution.
        // Engine-specific fields are NOT populated — adapters are built from the
        // raw JSONB later via build_adapter_from_record.
        config.clusters = db_cluster_records
            .iter()
            .filter_map(|r| {
                let engine =
                    queryflux_core::engine_registry::parse_engine_key(&r.engine_key).ok()?;
                Some((
                    r.name.clone(),
                    queryflux_core::config::ClusterConfig {
                        engine: Some(engine),
                        enabled: r.enabled,
                        max_running_queries: r.max_running_queries.map(|v| v as u64),
                        endpoint: queryflux_core::engine_registry::json_str(&r.config, "endpoint"),
                        database_path: None,
                        region: None,
                        s3_output_location: None,
                        workgroup: None,
                        catalog: None,
                        account: None,
                        warehouse: None,
                        role: None,
                        schema: None,
                        tls: None,
                        auth: queryflux_core::engine_registry::parse_auth_from_config_json(
                            &r.config,
                        ),
                        query_auth: None,
                    },
                ))
            })
            .collect();
        startup_cluster_records = Some(db_cluster_records);

        let group_records = pg
            .list_group_configs()
            .await
            .context("Load group configs from DB")?;
        group_ids_by_name = group_records
            .iter()
            .map(|r| (r.name.clone(), r.id))
            .collect();
        config.cluster_groups = group_records
            .into_iter()
            .map(|r| (r.name.clone(), r.to_core()))
            .collect();

        // Apply persisted security overrides (`security_settings` / `security_config` key).
        if let Ok(Some(v)) = pg.get_proxy_setting("security_config").await {
            if let Ok(auth_cfg) = serde_json::from_value::<queryflux_core::config::AuthConfig>(
                v.get("authConfig")
                    .cloned()
                    .unwrap_or(serde_json::Value::Null),
            ) {
                config.auth = auth_cfg;
            }
            if let Ok(authz_cfg) =
                serde_json::from_value::<queryflux_core::config::AuthorizationConfig>(
                    v.get("authorizationConfig")
                        .cloned()
                        .unwrap_or(serde_json::Value::Null),
                )
            {
                config.authorization = authz_cfg;
            }
        }
        let mut routing_from_db = false;
        match pg.load_routing_config().await {
            Ok(Some(loaded)) => {
                config.routing_fallback = loaded.routing_fallback;
                let mut routers = Vec::new();
                for v in loaded.routers {
                    match serde_json::from_value::<queryflux_core::config::RouterConfig>(v) {
                        Ok(r) => routers.push(r),
                        Err(e) => {
                            tracing::warn!(error = %e, "Skipping invalid routing_rules row from Postgres")
                        }
                    }
                }
                config.routers = routers;
                routing_from_db = true;
            }
            Ok(None) => {}
            Err(e) => {
                tracing::warn!(error = %e, "load_routing_config failed; keeping YAML routing")
            }
        }
        if !routing_from_db {
            if let Ok(Some(v)) = pg.get_proxy_setting("routing_config").await {
                if let Ok(fallback) = serde_json::from_value::<String>(
                    v.get("routingFallback")
                        .cloned()
                        .unwrap_or(serde_json::Value::Null),
                ) {
                    config.routing_fallback = fallback;
                }
                if let Ok(routers) =
                    serde_json::from_value::<Vec<queryflux_core::config::RouterConfig>>(
                        v.get("routers").cloned().unwrap_or(serde_json::Value::Null),
                    )
                {
                    config.routers = routers;
                }
            }
        }
    }

    // Build the engine registry up front so it can be used for validation and AppState.
    let engine_registry = Arc::new(queryflux_core::engine_registry::EngineRegistry::new(
        registered_engines::all_descriptors(),
    ));

    // --- Validate cluster configs against the engine registry ---
    {
        use queryflux_core::engine_registry::validate_cluster_config;
        let mut all_errors: Vec<String> = Vec::new();
        for (name, cfg) in &config.clusters {
            all_errors.extend(validate_cluster_config(&engine_registry, name, cfg));
        }
        if !all_errors.is_empty() {
            for e in &all_errors {
                tracing::error!("{e}");
            }
            anyhow::bail!(
                "Config validation failed with {} error(s)",
                all_errors.len()
            );
        }
    }

    // --- Build cluster states and adapters (two-pass) ---
    //
    // Pass 1: iterate `config.clusters`, build one adapter per cluster name.
    // Pass 2: iterate `config.cluster_groups`, resolve members, build ClusterStates.

    type AdapterMap = HashMap<String, Arc<dyn queryflux_engine_adapters::EngineAdapterTrait>>;
    let mut adapters: AdapterMap = HashMap::new();

    // Pass 1 — one adapter per cluster.
    // DB path: build from JSONB records directly; YAML path: build from ClusterConfig.
    if let Some(records) = &startup_cluster_records {
        for record in records {
            if !record.enabled {
                tracing::info!(cluster = %record.name, "Cluster disabled — skipping");
                continue;
            }
            let cluster_name = ClusterName(record.name.clone());
            let placeholder_group = ClusterGroupName("_".to_string());
            let adapter = registered_engines::build_adapter_from_record(
                cluster_name,
                placeholder_group,
                &record.engine_key,
                &record.config,
                &record.name,
            )
            .await
            .with_context(|| format!("Failed to build adapter for cluster '{}'", record.name))?;
            adapters.insert(record.name.clone(), adapter);
        }
    } else {
        for (cluster_name_str, cluster_cfg) in &config.clusters {
            if !cluster_cfg.enabled {
                tracing::info!(cluster = %cluster_name_str, "Cluster disabled — skipping");
                continue;
            }
            let cluster_name = ClusterName(cluster_name_str.clone());
            let placeholder_group = ClusterGroupName("_".to_string());
            let adapter = registered_engines::build_adapter(
                cluster_name,
                placeholder_group,
                cluster_cfg,
                cluster_name_str,
            )
            .await
            .with_context(|| format!("Failed to build adapter for cluster '{cluster_name_str}'"))?;
            adapters.insert(cluster_name_str.clone(), adapter);
        }
    }

    // Pass 2 — one group entry per cluster_group, resolving member cluster names.
    type GroupMap = HashMap<
        ClusterGroupName,
        (
            Vec<Arc<ClusterState>>,
            Arc<dyn queryflux_cluster_manager::strategy::ClusterSelectionStrategy>,
        ),
    >;
    let mut group_states: GroupMap = HashMap::new();
    let mut group_members: HashMap<String, Vec<String>> = HashMap::new();
    let mut group_order: Vec<String> = Vec::new();

    for (group_name, group_config) in &config.cluster_groups {
        if !group_config.enabled {
            tracing::info!(group = %group_name, "Cluster group disabled — skipping");
            continue;
        }
        let group_key = ClusterGroupName(group_name.clone());
        let mut states: Vec<Arc<ClusterState>> = Vec::new();
        let mut seen_members: HashSet<&str> = HashSet::new();

        for member_name in &group_config.members {
            if !seen_members.insert(member_name.as_str()) {
                tracing::warn!(
                    group = %group_name,
                    cluster = %member_name,
                    "Duplicate cluster in group members list — ignoring extra entry"
                );
                continue;
            }
            let cluster_cfg = config.clusters.get(member_name).context(format!(
                "group '{group_name}' references unknown cluster '{member_name}'"
            ))?;

            if !adapters.contains_key(member_name.as_str()) {
                // Cluster was disabled in Pass 1 — skip silently.
                tracing::info!(group = %group_name, cluster = %member_name, "Skipping disabled cluster in group");
                continue;
            }

            let engine = cluster_cfg
                .engine
                .as_ref()
                .context(format!("cluster '{member_name}' missing engine"))?;
            let engine_type = EngineType::from(engine);

            let max_q = cluster_cfg
                .max_running_queries
                .unwrap_or(group_config.max_running_queries);
            let cluster_cid = cluster_ids_by_name.get(member_name).copied();
            let group_cid = group_ids_by_name.get(group_name.as_str()).copied();
            let state = Arc::new(ClusterState::new(
                ClusterName(member_name.clone()),
                group_key.clone(),
                cluster_cid,
                group_cid,
                engine_type,
                cluster_cfg.endpoint.clone(),
                max_q,
                cluster_cfg.enabled,
            ));
            states.push(state);
        }

        let strategy = strategy_from_config(group_config.strategy.as_ref());
        group_members.insert(group_name.clone(), group_config.members.clone());
        group_order.push(group_name.clone());
        group_states.insert(group_key, (states, strategy));
    }

    let health_check_targets = health_targets_from_groups(&group_states, &adapters);
    let cluster_manager = Arc::new(SimpleClusterGroupManager::new(group_states));

    // --- Build translation service ---
    let translation = Arc::new(
        TranslationService::new_sqlglot(config.translation.python_scripts.clone()).unwrap_or_else(
            |e| {
                tracing::warn!("sqlglot unavailable ({e}), translation disabled");
                TranslationService::disabled()
            },
        ),
    );

    // --- Build router chain ---
    let fallback = ClusterGroupName(config.routing_fallback.clone());
    let mut routers: Vec<Box<dyn RouterTrait>> = Vec::new();

    for router_cfg in &config.routers {
        use queryflux_core::config::RouterConfig;
        match router_cfg {
            RouterConfig::ProtocolBased {
                trino_http,
                postgres_wire,
                mysql_wire,
                clickhouse_http,
                flight_sql,
                snowflake_http,
                snowflake_sql_api,
            } => {
                routers.push(Box::new(ProtocolBasedRouter {
                    trino_http: trino_http.as_ref().map(|s| ClusterGroupName(s.clone())),
                    postgres_wire: postgres_wire.as_ref().map(|s| ClusterGroupName(s.clone())),
                    mysql_wire: mysql_wire.as_ref().map(|s| ClusterGroupName(s.clone())),
                    clickhouse_http: clickhouse_http
                        .as_ref()
                        .map(|s| ClusterGroupName(s.clone())),
                    flight_sql: flight_sql.as_ref().map(|s| ClusterGroupName(s.clone())),
                    snowflake_http: snowflake_http.as_ref().map(|s| ClusterGroupName(s.clone())),
                    snowflake_sql_api: snowflake_sql_api
                        .as_ref()
                        .map(|s| ClusterGroupName(s.clone())),
                }));
            }
            RouterConfig::Header {
                header_name,
                header_value_to_group,
            } => {
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
            RouterConfig::Tags { rules } => {
                routers.push(Box::new(TagsRouter::new(rules.clone())));
            }
            RouterConfig::PythonScript {
                script,
                script_file,
            } => {
                let router = if let Some(path) = script_file {
                    PythonScriptRouter::from_file(path)
                        .context(format!("Failed to load routing script from {path}"))?
                } else {
                    PythonScriptRouter::new(script.clone())
                };
                routers.push(Box::new(router));
            }
            RouterConfig::Compound {
                combine,
                conditions,
                target_group,
            } => {
                routers.push(Box::new(CompoundRouter::new(
                    *combine,
                    conditions.clone(),
                    target_group.clone(),
                )));
            }
            _ => {
                tracing::warn!("Router type not yet implemented, skipping");
            }
        }
    }

    let router_chain = RouterChain::new(routers, fallback);

    // --- Build auth provider from config ---
    use queryflux_core::config::AuthProviderConfig;
    let auth_required = config.auth.required;
    let auth_provider: Arc<dyn queryflux_auth::AuthProvider> = match &config.auth.provider {
        AuthProviderConfig::None => {
            info!("Auth provider: none (network-trust only)");
            Arc::new(NoneAuthProvider::new(auth_required))
        }
        AuthProviderConfig::Static => {
            let users = config
                .auth
                .static_users
                .as_ref()
                .context("auth.provider = static requires auth.staticUsers to be configured")?
                .users
                .clone();
            info!(user_count = users.len(), "Auth provider: static");
            Arc::new(StaticAuthProvider::new(users, auth_required))
        }
        AuthProviderConfig::Oidc => {
            let oidc_cfg = config
                .auth
                .oidc
                .clone()
                .context("auth.provider = oidc requires auth.oidc to be configured")?;
            info!(issuer = %oidc_cfg.issuer, "Auth provider: OIDC");
            Arc::new(OidcAuthProvider::new(oidc_cfg, auth_required))
        }
        AuthProviderConfig::Ldap => {
            let ldap_cfg = config
                .auth
                .ldap
                .clone()
                .context("auth.provider = ldap requires auth.ldap to be configured")?;
            info!(url = %ldap_cfg.url, "Auth provider: LDAP");
            Arc::new(LdapAuthProvider::new(ldap_cfg, auth_required))
        }
    };
    // --- Build authorization checker from config ---
    use queryflux_core::config::AuthorizationProviderConfig;
    let authorization: Arc<dyn queryflux_auth::AuthorizationChecker> = match &config
        .authorization
        .provider
    {
        AuthorizationProviderConfig::None => {
            // Build per-group allow-lists from cluster group configs.
            // Groups with empty lists are open (allow-all), preserving backward compat.
            let policies = config
                .cluster_groups
                .iter()
                .map(|(name, cfg)| (name.clone(), cfg.authorization.clone()))
                .collect();
            let has_any_policy = config.cluster_groups.values().any(|cfg| {
                !cfg.authorization.allow_groups.is_empty()
                    || !cfg.authorization.allow_users.is_empty()
            });
            if has_any_policy {
                info!("Authorization: simple allow-list policy");
                Arc::new(SimpleAuthorizationPolicy::new(policies))
            } else {
                info!("Authorization: allow-all (no allow-lists configured)");
                Arc::new(AllowAllAuthorization)
            }
        }
        AuthorizationProviderConfig::OpenFga => {
            let openfga_cfg = config.authorization.openfga.clone().context(
                "authorization.provider = openfga requires authorization.openfga to be configured",
            )?;
            info!(url = %openfga_cfg.url, store_id = %openfga_cfg.store_id, "Authorization: OpenFGA");
            Arc::new(OpenFgaAuthorizationClient::new(openfga_cfg))
        }
    };

    // --- Startup validation: impersonate only valid for Trino ---
    for (name, cfg) in &config.clusters {
        if matches!(
            cfg.query_auth,
            Some(queryflux_core::config::QueryAuthConfig::Impersonate)
        ) {
            let engine = cfg
                .engine
                .as_ref()
                .map(|e| format!("{e:?}"))
                .unwrap_or_default();
            if !matches!(
                cfg.engine,
                Some(queryflux_core::config::EngineConfig::Trino)
            ) {
                anyhow::bail!(
                    "cluster '{name}': queryAuth.type = impersonate is only supported for Trino, got {engine}"
                );
            }
        }
    }

    let identity_resolver = Arc::new(BackendIdentityResolver::new());
    let cluster_configs = config.clusters.clone();

    let group_translation_scripts: HashMap<String, Vec<String>> = if let Some(pg) = &pg_store {
        pg.load_group_translation_bodies()
            .await
            .unwrap_or_else(|e| {
                tracing::warn!("Failed to load group translation scripts from Postgres: {e}");
                HashMap::new()
            })
    } else {
        HashMap::new()
    };

    // --- Wrap hot-reloadable fields in LiveConfig ---
    let group_default_tags: HashMap<String, queryflux_core::tags::QueryTags> = config
        .cluster_groups
        .iter()
        .filter(|(_, g)| !g.default_tags.is_empty())
        .map(|(name, g)| (name.clone(), g.default_tags.clone()))
        .collect();
    let live_config = LiveConfig {
        router_chain,
        cluster_manager,
        adapters,
        health_check_targets,
        cluster_configs,
        group_members,
        group_order,
        group_translation_scripts,
        group_default_tags,
    };
    // Seed the reload cache. When Postgres is active, use the raw JSONB config for
    // fingerprinting (same format that build_live_config uses on reload). For YAML-only,
    // serialize the ClusterConfig.
    let initial_config_json: HashMap<String, String> =
        if let Some(records) = &startup_cluster_records {
            records
                .iter()
                .map(|r| {
                    (
                        r.name.clone(),
                        serde_json::to_string(&r.config).unwrap_or_default(),
                    )
                })
                .collect()
        } else {
            live_config
                .cluster_configs
                .iter()
                .map(|(k, v)| (k.clone(), serde_json::to_string(v).unwrap_or_default()))
                .collect()
        };
    let adapter_reload_cache = Arc::new(tokio::sync::Mutex::new(AdapterReloadCache {
        adapters: live_config.adapters.clone(),
        config_json: initial_config_json,
        // Seed with the initial cluster states so the first reload can inherit health status.
        cluster_states: live_config
            .health_check_targets
            .iter()
            .map(|(_, s)| (s.cluster_name.0.clone(), s.clone()))
            .collect(),
    }));
    let live = Arc::new(tokio::sync::RwLock::new(live_config));

    let app_state = Arc::new(AppState {
        external_address: external_address.clone(),
        live: live.clone(),
        persistence,
        translation,
        metrics,
        auth_provider,
        authorization,
        identity_resolver,
        snowflake_sessions: SnowflakeSessionStore::new(),
    });

    // --- Start admin server (Prometheus /metrics + future /admin/* endpoints) ---
    let admin_port = config.queryflux.admin_api.port;
    let admin_store = pg_store.clone().map(|pg| pg as Arc<dyn AdminStore>);
    let security_config = Arc::new(AdminSecurityConfigDto::from_config(
        &config.auth,
        &config.authorization,
        &config.cluster_groups,
    ));
    let routing_config = Arc::new(AdminRoutingConfigDto::from_config(
        &config.routing_fallback,
        &config.routers,
    ));
    let config_reload_notify = Arc::new(tokio::sync::Notify::new());

    let frontends_status = build_frontends_status(
        &config.queryflux.frontends,
        admin_port,
        config.queryflux.external_address.clone(),
    );

    let admin = AdminFrontend::new(
        prometheus,
        live.clone(),
        admin_store,
        admin_port,
        security_config,
        routing_config,
        engine_registry,
        config_reload_notify.clone(),
        frontends_status,
    );

    // --- Start Trino HTTP frontend ---
    let trino_port = config.queryflux.frontends.trino_http.port;
    let frontend = TrinoHttpFrontend::new(app_state.clone(), trino_port);

    info!(
        "QueryFlux ready — Trino HTTP on :{trino_port}, admin/metrics on :{admin_port}, external address: {external_address}"
    );

    if pg_store.is_some() {
        match config.queryflux.periodic_config_reload_interval_secs() {
            None => tracing::info!(
                "Postgres persistence: routing rules and cluster/group config are cached in memory; periodic DB refresh is disabled (configReloadIntervalSecs: 0). Reloads still run after Studio/admin API writes."
            ),
            Some(secs) => tracing::info!(
                secs,
                "Postgres persistence: routing rules and cluster/group config are cached in memory and reloaded from the DB on this interval (seconds), or immediately after Studio/admin writes"
            ),
        }
    }

    // Background task: push cluster utilization snapshots to Prometheus every 5s.
    tokio::spawn({
        let state = app_state.clone();
        async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
            loop {
                interval.tick().await;
                let cluster_manager = state.live.read().await.cluster_manager.clone();
                if let Ok(snapshots) = cluster_manager.all_cluster_states().await {
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
                let Ok(all) = state.persistence.list_all().await else {
                    continue;
                };
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
                        state
                            .metrics
                            .on_query_finished(&q.cluster_group.0, &q.cluster_name.0);
                        let cluster_manager = state.live.read().await.cluster_manager.clone();
                        let _ = cluster_manager
                            .release_cluster(&q.cluster_group, &q.cluster_name)
                            .await;
                        let _ = state.persistence.delete(&q.backend_query_id).await;
                    }
                }
            }
        }
    });

    // Background task: clean up stale queued queries (client disconnected before getting
    // cluster capacity). Runs every 120s;
    // deletes queued entries not accessed for > 5 minutes.
    tokio::spawn({
        let state = app_state.clone();
        async move {
            const CLIENT_TIMEOUT_SECS: i64 = 300;
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(120));
            loop {
                interval.tick().await;
                let cutoff = chrono::Utc::now() - chrono::Duration::seconds(CLIENT_TIMEOUT_SECS);
                match state
                    .persistence
                    .delete_queued_not_accessed_since(cutoff)
                    .await
                {
                    Ok(0) => {}
                    Ok(n) => tracing::info!("Cleaned up {n} stale queued queries"),
                    Err(e) => tracing::warn!("Queued query cleanup failed: {e}"),
                }
            }
        }
    });

    // Background task: enforce query_history_retention_days — runs hourly and deletes
    // query_records rows older than the configured retention window.
    // Only active when Postgres is configured and retention_days is set.
    if let (Some(pg), Some(retention_days)) = (
        pg_store.clone(),
        config.queryflux.query_history_retention_days,
    ) {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(3600));
            interval.tick().await; // skip the first immediate tick at startup
            loop {
                interval.tick().await;
                let cutoff = chrono::Utc::now() - chrono::Duration::days(retention_days as i64);
                match pg.purge_old_query_records(cutoff).await {
                    Ok(0) => {}
                    Ok(n) => {
                        tracing::info!("Purged {n} query records older than {retention_days} days")
                    }
                    Err(e) => tracing::warn!("Query history purge failed: {e}"),
                }
            }
        });
    }

    // Background task: hot-reload routing rules + cluster configs from the DB on a timer **or**
    // immediately when the admin API notifies (PUT/DELETE cluster, group, or routing config).
    // Only active when Postgres persistence is configured.
    // `configReloadIntervalSecs: 0` disables the timer; reloads happen only on admin notify.
    tokio::spawn({
        let live = live.clone();
        let pg = pg_store.clone();
        let cache = adapter_reload_cache.clone();
        let notify = config_reload_notify.clone();
        let periodic_secs = config.queryflux.periodic_config_reload_interval_secs();
        async move {
            async fn do_reload(
                pg: &Arc<PostgresStore>,
                cache: &tokio::sync::Mutex<AdapterReloadCache>,
                live: &Arc<tokio::sync::RwLock<LiveConfig>>,
            ) {
                let mut cache_guard = cache.lock().await;
                match reload_live_config(pg, &mut cache_guard).await {
                    Ok(new_live) => {
                        *live.write().await = new_live;
                        tracing::info!("Live config reloaded from Postgres");
                    }
                    Err(e) => tracing::warn!("Config reload failed: {e}"),
                }
            }

            match periodic_secs {
                None => loop {
                    notify.notified().await;
                    tracing::debug!("Config reload requested via admin API");
                    if let Some(pg) = &pg {
                        do_reload(pg, &cache, &live).await;
                    }
                },
                Some(interval_secs) => {
                    let mut interval =
                        tokio::time::interval(std::time::Duration::from_secs(interval_secs));
                    interval.tick().await; // skip the first immediate tick — startup already loaded
                    loop {
                        tokio::select! {
                            _ = interval.tick() => {}
                            _ = notify.notified() => {
                                tracing::debug!("Config reload requested via admin API");
                            }
                        }
                        if let Some(pg) = &pg {
                            do_reload(pg, &cache, &live).await;
                        }
                    }
                }
            }
        }
    });

    // Background task: health-check each cluster every 30s via its adapter.
    tokio::spawn({
        let state = app_state.clone();
        async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));
            loop {
                interval.tick().await;
                let targets = {
                    let live = state.live.read().await;
                    live.health_check_targets.clone()
                };
                for (adapter, state) in &targets {
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
        let state = app_state.clone();
        async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));
            loop {
                interval.tick().await;
                let targets = {
                    let live = state.live.read().await;
                    live.health_check_targets.clone()
                };
                for (adapter, cstate) in &targets {
                    let tracked = cstate.running_queries();
                    let max = cstate.max_running_queries();
                    // `decrement_running` used to wrap on underflow; or reload can desync counters.
                    if tracked > max {
                        let fix = adapter.fetch_running_query_count().await.unwrap_or(0);
                        tracing::warn!(
                            cluster = %cstate.cluster_name.0,
                            group = %cstate.group_name.0,
                            tracked,
                            max,
                            fix,
                            "running_queries above group capacity; resetting from engine count"
                        );
                        cstate.set_running_queries(fix);
                        continue;
                    }
                    if let Some(actual) = adapter.fetch_running_query_count().await {
                        if actual != tracked {
                            tracing::info!(
                                cluster = %cstate.cluster_name.0,
                                group = %cstate.group_name.0,
                                tracked,
                                actual,
                                "Reconciling running_queries counter with engine ground truth"
                            );
                            cstate.set_running_queries(actual);
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

    let snowflake_future = async {
        match &config.queryflux.frontends.snowflake_http {
            Some(cfg) if cfg.enabled => {
                SnowflakeFrontend::new(app_state.clone(), cfg.port)
                    .listen()
                    .await
            }
            _ => std::future::pending::<queryflux_core::error::Result<()>>().await,
        }
    };

    tokio::select! {
        r = frontend.listen()   => r.map_err(|e| anyhow::anyhow!("{e}"))?,
        r = admin.listen()      => r.map_err(|e| anyhow::anyhow!("{e}"))?,
        r = mysql_future        => r.map_err(|e| anyhow::anyhow!("{e}"))?,
        r = postgres_future     => r.map_err(|e| anyhow::anyhow!("{e}"))?,
        r = flight_sql_future   => r.map_err(|e| anyhow::anyhow!("{e}"))?,
        r = snowflake_future    => r.map_err(|e| anyhow::anyhow!("{e}"))?,
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Hot-reload helpers
// ---------------------------------------------------------------------------

type GroupStatesMap = HashMap<
    ClusterGroupName,
    (
        Vec<Arc<ClusterState>>,
        Arc<dyn queryflux_cluster_manager::strategy::ClusterSelectionStrategy>,
    ),
>;

/// Holds adapter instances between DB reloads. Adapters are recreated when the
/// serialized [`ClusterConfig`] for a cluster changes so pools pick up new endpoints/credentials.
struct AdapterReloadCache {
    adapters: HashMap<String, Arc<dyn queryflux_engine_adapters::EngineAdapterTrait>>,
    config_json: HashMap<String, String>,
    /// Previous-generation cluster states keyed by cluster name.
    /// Preserved across reloads so that health status and running-query counters
    /// are not reset to their initial values every time the config is reloaded.
    cluster_states: HashMap<String, Arc<ClusterState>>,
}

fn health_targets_from_groups(
    group_states: &GroupStatesMap,
    adapters: &HashMap<String, Arc<dyn queryflux_engine_adapters::EngineAdapterTrait>>,
) -> Vec<(
    Arc<dyn queryflux_engine_adapters::EngineAdapterTrait>,
    Arc<ClusterState>,
)> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for (states, _) in group_states.values() {
        for state in states {
            let name = state.cluster_name.0.clone();
            if seen.insert(name.clone()) {
                if let Some(adapter) = adapters.get(&name) {
                    out.push((adapter.clone(), state.clone()));
                }
            }
        }
    }
    out
}

/// Build a `LiveConfig` from DB cluster records, group maps, and router chain components.
///
/// This is the DB load path: adapters are built directly from the JSONB config blob
/// in each `ClusterConfigRecord`, bypassing the `ClusterConfig` god struct.
///
/// `cache` holds adapter instances from the previous generation. Adapters are reused
/// only when the cluster's JSON-serialized config matches the previous reload; otherwise
/// they are rebuilt (e.g. endpoint or password changed).
#[allow(clippy::too_many_arguments)]
async fn build_live_config(
    cluster_records: &[queryflux_persistence::cluster_config::ClusterConfigRecord],
    cluster_groups: &std::collections::HashMap<String, queryflux_core::config::ClusterGroupConfig>,
    cluster_ids_by_name: &HashMap<String, i64>,
    group_ids_by_name: &HashMap<String, i64>,
    routers_cfg: &[queryflux_core::config::RouterConfig],
    routing_fallback: &str,
    group_translation_scripts: HashMap<String, Vec<String>>,
    cache: &mut AdapterReloadCache,
) -> Result<LiveConfig> {
    use queryflux_cluster_manager::{
        cluster_state::ClusterState, simple::SimpleClusterGroupManager,
        strategy::strategy_from_config,
    };
    use queryflux_core::engine_registry::{json_str, parse_engine_key};
    use queryflux_core::tags::QueryTags;

    // Build a lookup map from records for group member resolution.
    let records_by_name: HashMap<
        &str,
        &queryflux_persistence::cluster_config::ClusterConfigRecord,
    > = cluster_records
        .iter()
        .map(|r| (r.name.as_str(), r))
        .collect();

    // Build adapters — reuse when serialized cluster config is unchanged.
    for record in cluster_records {
        let cluster_name_str = &record.name;
        if !record.enabled {
            cache.adapters.remove(cluster_name_str.as_str());
            cache.config_json.remove(cluster_name_str.as_str());
            continue;
        }
        let cfg_json = serde_json::to_string(&record.config).unwrap_or_default();
        let reuse = cache.adapters.contains_key(cluster_name_str.as_str())
            && cache
                .config_json
                .get(cluster_name_str.as_str())
                .map(String::as_str)
                == Some(cfg_json.as_str());
        if reuse {
            continue;
        }
        cache.adapters.remove(cluster_name_str.as_str());
        cache.config_json.remove(cluster_name_str.as_str());

        let cluster_name = ClusterName(cluster_name_str.clone());
        let placeholder_group = ClusterGroupName("_".to_string());
        let adapter = match registered_engines::build_adapter_from_record(
            cluster_name,
            placeholder_group,
            &record.engine_key,
            &record.config,
            cluster_name_str,
        )
        .await
        {
            Ok(a) => a,
            Err(e) => {
                tracing::warn!(cluster = %cluster_name_str, "Reload: adapter build failed: {e:#}");
                continue;
            }
        };
        cache.adapters.insert(cluster_name_str.clone(), adapter);
        cache.config_json.insert(cluster_name_str.clone(), cfg_json);
    }
    cache
        .adapters
        .retain(|name, _| records_by_name.contains_key(name.as_str()));
    cache
        .config_json
        .retain(|name, _| records_by_name.contains_key(name.as_str()));

    // Build group states.
    let mut group_states: GroupStatesMap = HashMap::new();
    let mut group_members: HashMap<String, Vec<String>> = HashMap::new();
    let mut group_order: Vec<String> = Vec::new();

    for (group_name, group_config) in cluster_groups {
        if !group_config.enabled {
            continue;
        }
        let group_key = ClusterGroupName(group_name.clone());
        let mut states: Vec<Arc<ClusterState>> = Vec::new();
        let mut seen_members: HashSet<&str> = HashSet::new();

        for member_name in &group_config.members {
            if !seen_members.insert(member_name.as_str()) {
                tracing::warn!(
                    group = %group_name,
                    cluster = %member_name,
                    "Reload: duplicate cluster in group members — ignoring extra entry"
                );
                continue;
            }
            let record = match records_by_name.get(member_name.as_str()) {
                Some(r) => r,
                None => {
                    tracing::warn!(group = %group_name, cluster = %member_name, "Reload: group references unknown cluster");
                    continue;
                }
            };
            if !cache.adapters.contains_key(member_name.as_str()) {
                tracing::info!(group = %group_name, cluster = %member_name, "Reload: skipping disabled/missing cluster in group");
                continue;
            }
            let engine = match parse_engine_key(&record.engine_key) {
                Ok(e) => e,
                Err(_) => continue,
            };
            let engine_type = EngineType::from(&engine);
            let max_q = record
                .max_running_queries
                .map(|v| v as u64)
                .unwrap_or(group_config.max_running_queries);
            let endpoint = json_str(&record.config, "endpoint");
            let cluster_cid = cluster_ids_by_name.get(member_name.as_str()).copied();
            let group_cid = group_ids_by_name.get(group_name.as_str()).copied();

            // Reuse the previous state when the cluster config is unchanged so that
            // is_healthy and running_queries are not reset across reloads.
            // When config changed or the cluster is new, create a fresh state but
            // still inherit is_healthy from the previous generation so the UI does not
            // flash healthy for 30 s until the next health-check tick.
            let cfg_json = serde_json::to_string(&record.config).unwrap_or_default();
            let config_unchanged = cache
                .config_json
                .get(member_name.as_str())
                .map(String::as_str)
                == Some(cfg_json.as_str());

            let state = if config_unchanged {
                if let Some(prev) = cache.cluster_states.get(member_name.as_str()) {
                    prev.clone()
                } else {
                    Arc::new(ClusterState::new(
                        ClusterName(member_name.clone()),
                        group_key.clone(),
                        cluster_cid,
                        group_cid,
                        engine_type,
                        endpoint,
                        max_q,
                        record.enabled,
                    ))
                }
            } else {
                let s = Arc::new(ClusterState::new(
                    ClusterName(member_name.clone()),
                    group_key.clone(),
                    cluster_cid,
                    group_cid,
                    engine_type,
                    endpoint,
                    max_q,
                    record.enabled,
                ));
                if let Some(prev) = cache.cluster_states.get(member_name.as_str()) {
                    s.set_healthy(prev.is_healthy());
                }
                s
            };
            states.push(state);
        }

        let strategy = strategy_from_config(group_config.strategy.as_ref());
        group_members.insert(group_name.clone(), group_config.members.clone());
        group_order.push(group_name.clone());
        group_states.insert(group_key, (states, strategy));
    }

    let health_check_targets = health_targets_from_groups(&group_states, &cache.adapters);
    cache.cluster_states = health_check_targets
        .iter()
        .map(|(_, s)| (s.cluster_name.0.clone(), s.clone()))
        .collect();
    let cluster_manager = Arc::new(SimpleClusterGroupManager::new(group_states));

    // Build minimal ClusterConfig values for BackendIdentityResolver.
    // query_auth is not persisted to DB, so it's always None for DB-loaded clusters.
    let cluster_configs: HashMap<String, queryflux_core::config::ClusterConfig> = cluster_records
        .iter()
        .filter_map(|r| {
            let engine = parse_engine_key(&r.engine_key).ok()?;
            Some((
                r.name.clone(),
                queryflux_core::config::ClusterConfig {
                    engine: Some(engine),
                    enabled: r.enabled,
                    max_running_queries: r.max_running_queries.map(|v| v as u64),
                    endpoint: json_str(&r.config, "endpoint"),
                    database_path: None,
                    region: None,
                    s3_output_location: None,
                    workgroup: None,
                    catalog: None,
                    account: None,
                    warehouse: None,
                    role: None,
                    schema: None,
                    tls: None,
                    auth: None,
                    query_auth: None,
                },
            ))
        })
        .collect();

    // Build router chain.
    let fallback = ClusterGroupName(routing_fallback.to_string());
    let mut routers: Vec<Box<dyn RouterTrait>> = Vec::new();
    for router_cfg in routers_cfg {
        use queryflux_core::config::RouterConfig;
        match router_cfg {
            RouterConfig::ProtocolBased {
                trino_http,
                postgres_wire,
                mysql_wire,
                clickhouse_http,
                flight_sql,
                snowflake_http,
                snowflake_sql_api,
            } => {
                routers.push(Box::new(
                    queryflux_routing::implementations::protocol_based::ProtocolBasedRouter {
                        trino_http: trino_http.as_ref().map(|s| ClusterGroupName(s.clone())),
                        postgres_wire: postgres_wire.as_ref().map(|s| ClusterGroupName(s.clone())),
                        mysql_wire: mysql_wire.as_ref().map(|s| ClusterGroupName(s.clone())),
                        clickhouse_http: clickhouse_http
                            .as_ref()
                            .map(|s| ClusterGroupName(s.clone())),
                        flight_sql: flight_sql.as_ref().map(|s| ClusterGroupName(s.clone())),
                        snowflake_http: snowflake_http
                            .as_ref()
                            .map(|s| ClusterGroupName(s.clone())),
                        snowflake_sql_api: snowflake_sql_api
                            .as_ref()
                            .map(|s| ClusterGroupName(s.clone())),
                    },
                ));
            }
            RouterConfig::Header {
                header_name,
                header_value_to_group,
            } => {
                let mapping = header_value_to_group
                    .iter()
                    .map(|(k, v)| (k.clone(), ClusterGroupName(v.clone())))
                    .collect();
                routers.push(Box::new(
                    queryflux_routing::implementations::header::HeaderRouter::new(
                        header_name.clone(),
                        mapping,
                    ),
                ));
            }
            RouterConfig::QueryRegex { rules } => {
                let pairs = rules
                    .iter()
                    .map(|r| (r.regex.clone(), r.target_group.clone()))
                    .collect();
                routers.push(Box::new(
                    queryflux_routing::implementations::query_regex::QueryRegexRouter::new(pairs),
                ));
            }
            RouterConfig::Tags { rules } => {
                routers.push(Box::new(
                    queryflux_routing::implementations::tags::TagsRouter::new(rules.clone()),
                ));
            }
            RouterConfig::PythonScript {
                script,
                script_file,
            } => {
                let router = if let Some(path) = script_file {
                    match queryflux_routing::implementations::python_script::PythonScriptRouter::from_file(path) {
                        Ok(r) => r,
                        Err(e) => {
                            tracing::warn!("Reload: failed to load routing script from {path}: {e}");
                            continue;
                        }
                    }
                } else {
                    queryflux_routing::implementations::python_script::PythonScriptRouter::new(
                        script.clone(),
                    )
                };
                routers.push(Box::new(router));
            }
            RouterConfig::Compound {
                combine,
                conditions,
                target_group,
            } => {
                routers.push(Box::new(
                    queryflux_routing::implementations::compound::CompoundRouter::new(
                        *combine,
                        conditions.clone(),
                        target_group.clone(),
                    ),
                ));
            }
            _ => {
                tracing::warn!("Reload: router type not yet implemented, skipping");
            }
        }
    }
    let router_chain = RouterChain::new(routers, fallback);

    let group_default_tags: HashMap<String, QueryTags> = cluster_groups
        .iter()
        .filter(|(_, g)| !g.default_tags.is_empty())
        .map(|(name, g)| (name.clone(), g.default_tags.clone()))
        .collect();

    Ok(LiveConfig {
        router_chain,
        cluster_manager,
        adapters: cache.adapters.clone(),
        health_check_targets,
        cluster_configs,
        group_members,
        group_order,
        group_translation_scripts,
        group_default_tags,
    })
}

/// Load cluster/group configs + routing config from Postgres and build a fresh `LiveConfig`.
/// Existing adapter instances are reused for clusters that haven't changed.
///
/// Cluster records are passed directly to `build_live_config` — no `to_core()` conversion.
async fn reload_live_config(
    pg: &Arc<queryflux_persistence::postgres::PostgresStore>,
    cache: &mut AdapterReloadCache,
) -> Result<LiveConfig> {
    use queryflux_persistence::{ClusterConfigStore, RoutingConfigStore};

    let cluster_records = pg
        .list_cluster_configs()
        .await
        .context("reload: list_cluster_configs")?;
    let cluster_ids_by_name: HashMap<String, i64> = cluster_records
        .iter()
        .map(|r| (r.name.clone(), r.id))
        .collect();

    let group_records = pg
        .list_group_configs()
        .await
        .context("reload: list_group_configs")?;
    let group_ids_by_name: HashMap<String, i64> = group_records
        .iter()
        .map(|r| (r.name.clone(), r.id))
        .collect();
    let cluster_groups: std::collections::HashMap<
        String,
        queryflux_core::config::ClusterGroupConfig,
    > = group_records
        .into_iter()
        .map(|r| (r.name.clone(), r.to_core()))
        .collect();

    // Load routing from DB if present; otherwise fall back to empty defaults.
    let (routing_fallback, routers_cfg) = match pg.load_routing_config().await {
        Ok(Some(loaded)) => {
            let mut routers = Vec::new();
            for v in loaded.routers {
                match serde_json::from_value::<queryflux_core::config::RouterConfig>(v) {
                    Ok(r) => routers.push(r),
                    Err(e) => {
                        tracing::warn!(error = %e, "Reload: skipping invalid routing_rules row")
                    }
                }
            }
            (loaded.routing_fallback, routers)
        }
        Ok(None) => (String::new(), Vec::new()),
        Err(e) => {
            return Err(anyhow::anyhow!("reload: load_routing_config: {e}"));
        }
    };

    let group_translation_scripts = pg
        .load_group_translation_bodies()
        .await
        .unwrap_or_else(|e| {
            tracing::warn!(error = %e, "reload: load_group_translation_bodies failed");
            HashMap::new()
        });

    build_live_config(
        &cluster_records,
        &cluster_groups,
        &cluster_ids_by_name,
        &group_ids_by_name,
        &routers_cfg,
        &routing_fallback,
        group_translation_scripts,
        cache,
    )
    .await
}
