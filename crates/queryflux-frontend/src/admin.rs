use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    http::{Method, StatusCode},
    response::IntoResponse,
    routing::{get, patch},
    Json, Router,
};
use tower_http::cors::{Any, CorsLayer};
use queryflux_cluster_manager::ClusterGroupManager;
use queryflux_core::{
    engine_registry,
    error::Result,
    query::{ClusterGroupName, ClusterName},
};
use queryflux_metrics::prometheus_store::PrometheusMetrics;
use queryflux_persistence::{
    cluster_config::{UpsertClusterConfig, UpsertClusterGroupConfig},
    query_history::{DashboardStats, EngineStatRow, GroupStatRow, QueryFilters, QuerySummary},
    AdminStore,
};
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tracing::info;
use utoipa::{OpenApi, ToSchema};

use crate::FrontendListenerTrait;

// ---------------------------------------------------------------------------
// OpenAPI spec
// ---------------------------------------------------------------------------

/// Live state snapshot of a single cluster returned by /admin/clusters.
#[derive(Debug, Serialize, ToSchema)]
pub struct ClusterStateDto {
    pub group_name: String,
    pub cluster_name: String,
    pub engine_type: String,
    /// The HTTP endpoint of the cluster (e.g. `http://trino-1:8080`). Null for engines without a network endpoint (e.g. DuckDB).
    pub endpoint: Option<String>,
    pub running_queries: u64,
    pub queued_queries: u64,
    pub max_running_queries: u64,
    /// Whether the most recent health check (every 30s) passed.
    pub is_healthy: bool,
    /// Whether this cluster is administratively enabled.
    pub enabled: bool,
}

#[derive(OpenApi)]
#[openapi(
    info(
        title = "QueryFlux Admin API",
        version = "0.1.0",
        description = "Admin REST API for QueryFlux Studio — query history, cluster state, and dashboard stats."
    ),
    paths(
        health_handler,
        clusters_handler,
        update_cluster_handler,
        engine_registry_handler,
        list_queries_handler,
        get_stats_handler,
        list_engines_handler,
        get_engine_stats_handler,
        get_group_stats_handler,
    ),
    components(schemas(
        ClusterStateDto,
        ClusterUpdateRequest,
        QuerySummary,
        DashboardStats,
        EngineStatRow,
        GroupStatRow,
    )),
    tags(
        (name = "admin", description = "Cluster and query management"),
        (name = "metrics", description = "Prometheus metrics endpoint"),
    )
)]
struct ApiDoc;

// ---------------------------------------------------------------------------
// Shared state
// ---------------------------------------------------------------------------

struct AdminState {
    prometheus: Arc<PrometheusMetrics>,
    cluster_manager: Arc<dyn ClusterGroupManager>,
    /// Present when a full-featured persistence backend is configured (e.g. Postgres).
    /// None when running with in-memory persistence.
    admin_store: Option<Arc<dyn AdminStore>>,
}

// ---------------------------------------------------------------------------
// AdminFrontend
// ---------------------------------------------------------------------------

pub struct AdminFrontend {
    prometheus: Arc<PrometheusMetrics>,
    cluster_manager: Arc<dyn ClusterGroupManager>,
    admin_store: Option<Arc<dyn AdminStore>>,
    port: u16,
}

impl AdminFrontend {
    pub fn new(
        prometheus: Arc<PrometheusMetrics>,
        cluster_manager: Arc<dyn ClusterGroupManager>,
        admin_store: Option<Arc<dyn AdminStore>>,
        port: u16,
    ) -> Self {
        Self { prometheus, cluster_manager, admin_store, port }
    }

    fn router(&self) -> Router {
        let state = Arc::new(AdminState {
            prometheus: self.prometheus.clone(),
            cluster_manager: self.cluster_manager.clone(),
            admin_store: self.admin_store.clone(),
        });

        let spec_json = serde_json::to_string(&ApiDoc::openapi())
            .unwrap_or_else(|_| "{}".to_string());

        Router::new()
            .route("/metrics", get(metrics_handler))
            .route("/health", get(health_handler))
            .route("/admin/clusters", get(clusters_handler))
            .route("/admin/queries", get(list_queries_handler))
            .route("/admin/stats", get(get_stats_handler))
            .route("/admin/engines", get(list_engines_handler))
            .route("/admin/engine-stats", get(get_engine_stats_handler))
            .route("/admin/group-stats", get(get_group_stats_handler))
            .route("/admin/clusters/{group}/{cluster}", patch(update_cluster_handler))
            .route("/admin/engine-registry", get(engine_registry_handler))
            // Persisted cluster config CRUD (requires Postgres persistence)
            .route("/admin/config/clusters", get(list_cluster_configs_handler))
            .route(
                "/admin/config/clusters/{name}",
                get(get_cluster_config_handler)
                    .put(upsert_cluster_config_handler)
                    .delete(delete_cluster_config_handler),
            )
            // Persisted cluster group config CRUD
            .route("/admin/config/groups", get(list_group_configs_handler))
            .route(
                "/admin/config/groups/{name}",
                get(get_group_config_handler)
                    .put(upsert_group_config_handler)
                    .delete(delete_group_config_handler),
            )
            .route(
                "/openapi.json",
                get(move || {
                    let spec = spec_json.clone();
                    async move {
                        (
                            StatusCode::OK,
                            [("content-type", "application/json")],
                            spec,
                        )
                    }
                }),
            )
            .route("/docs", get(swagger_ui_handler))
            .with_state(state)
            .layer(
                CorsLayer::new()
                    .allow_origin(Any)
                    .allow_methods([Method::GET, Method::PATCH, Method::OPTIONS])
                    .allow_headers(Any),
            )
    }
}

#[async_trait::async_trait]
impl FrontendListenerTrait for AdminFrontend {
    async fn listen(&self) -> Result<()> {
        let addr = format!("0.0.0.0:{}", self.port);
        info!(
            "Admin server listening on {addr}  — Prometheus: {addr}/metrics  Swagger UI: {addr}/docs"
        );
        let listener = TcpListener::bind(&addr).await.map_err(|e| {
            queryflux_core::error::QueryFluxError::Engine(e.to_string())
        })?;
        axum::serve(listener, self.router()).await.map_err(|e| {
            queryflux_core::error::QueryFluxError::Engine(e.to_string())
        })?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

async fn metrics_handler(State(state): State<Arc<AdminState>>) -> impl IntoResponse {
    let body = state.prometheus.gather_text();
    (
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4; charset=utf-8")],
        body,
    )
}

/// Liveness probe.
#[utoipa::path(
    get,
    path = "/health",
    tag = "admin",
    responses((status = 200, description = "Service is alive", body = str))
)]
async fn health_handler() -> impl IntoResponse {
    (StatusCode::OK, "ok")
}

/// Live state of all cluster groups.
#[utoipa::path(
    get,
    path = "/admin/clusters",
    tag = "admin",
    responses(
        (status = 200, description = "Cluster state snapshots", body = Vec<ClusterStateDto>),
        (status = 500, description = "Internal error", body = str),
    )
)]
async fn clusters_handler(State(state): State<Arc<AdminState>>) -> impl IntoResponse {
    match state.cluster_manager.all_cluster_states().await {
        Ok(snapshots) => {
            let dtos: Vec<ClusterStateDto> = snapshots
                .into_iter()
                .map(|s| ClusterStateDto {
                    group_name: s.group_name.0,
                    cluster_name: s.cluster_name.0,
                    engine_type: format!("{:?}", s.engine_type),
                    endpoint: s.endpoint,
                    running_queries: s.running_queries,
                    queued_queries: s.queued_queries,
                    max_running_queries: s.max_running_queries,
                    is_healthy: s.is_healthy,
                    enabled: s.enabled,
                })
                .collect();
            Json(dtos).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

/// Paginated query history. Requires Postgres persistence.
#[utoipa::path(
    get,
    path = "/admin/queries",
    tag = "admin",
    params(QueryFilters),
    responses(
        (status = 200, description = "Query records (newest first)", body = Vec<QuerySummary>),
        (status = 503, description = "Postgres persistence not configured", body = str),
        (status = 500, description = "Internal error", body = str),
    )
)]
async fn list_queries_handler(
    State(state): State<Arc<AdminState>>,
    Query(filters): Query<QueryFilters>,
) -> impl IntoResponse {
    let Some(pg) = &state.admin_store else {
        return (StatusCode::SERVICE_UNAVAILABLE, "Postgres persistence not configured")
            .into_response();
    };
    match pg.list_queries(&filters).await {
        Ok(rows) => Json::<Vec<QuerySummary>>(rows).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

/// Dashboard stats for the last hour. Requires Postgres persistence.
#[utoipa::path(
    get,
    path = "/admin/stats",
    tag = "admin",
    responses(
        (status = 200, description = "Aggregated last-hour stats", body = DashboardStats),
        (status = 503, description = "Postgres persistence not configured", body = str),
        (status = 500, description = "Internal error", body = str),
    )
)]
async fn get_stats_handler(State(state): State<Arc<AdminState>>) -> impl IntoResponse {
    let Some(pg) = &state.admin_store else {
        return (StatusCode::SERVICE_UNAVAILABLE, "Postgres persistence not configured")
            .into_response();
    };
    match pg.get_dashboard_stats().await {
        Ok(stats) => Json(stats).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

/// Distinct engine types that have recorded queries. Requires Postgres persistence.
#[utoipa::path(
    get,
    path = "/admin/engines",
    tag = "admin",
    responses(
        (status = 200, description = "List of engine type strings", body = Vec<String>),
        (status = 503, description = "Postgres persistence not configured", body = str),
        (status = 500, description = "Internal error", body = str),
    )
)]
async fn list_engines_handler(State(state): State<Arc<AdminState>>) -> impl IntoResponse {
    let Some(pg) = &state.admin_store else {
        return (StatusCode::SERVICE_UNAVAILABLE, "Postgres persistence not configured")
            .into_response();
    };
    match pg.list_engines().await {
        Ok(engines) => Json(engines).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

/// Per-engine aggregated stats. Optional `?hours=N` window (default 24). Requires Postgres persistence.
#[utoipa::path(
    get,
    path = "/admin/engine-stats",
    tag = "admin",
    params(
        ("hours" = Option<i64>, Query, description = "Look-back window in hours (default 24)")
    ),
    responses(
        (status = 200, description = "Per-engine aggregated stats", body = Vec<EngineStatRow>),
        (status = 503, description = "Postgres persistence not configured", body = str),
        (status = 500, description = "Internal error", body = str),
    )
)]
async fn get_engine_stats_handler(
    State(state): State<Arc<AdminState>>,
    Query(params): Query<EngineStatsParams>,
) -> impl IntoResponse {
    let Some(pg) = &state.admin_store else {
        return (StatusCode::SERVICE_UNAVAILABLE, "Postgres persistence not configured")
            .into_response();
    };
    let hours = params.hours.unwrap_or(24).clamp(1, 168);
    match pg.get_engine_stats(hours).await {
        Ok(rows) => Json::<Vec<EngineStatRow>>(rows).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

/// Per-cluster-group aggregated stats. Optional `?hours=N` window (default 24). Requires Postgres persistence.
#[utoipa::path(
    get,
    path = "/admin/group-stats",
    tag = "admin",
    params(
        ("hours" = Option<i64>, Query, description = "Look-back window in hours (default 24)")
    ),
    responses(
        (status = 200, description = "Per-group aggregated stats", body = Vec<GroupStatRow>),
        (status = 503, description = "Postgres persistence not configured", body = str),
        (status = 500, description = "Internal error", body = str),
    )
)]
async fn get_group_stats_handler(
    State(state): State<Arc<AdminState>>,
    Query(params): Query<EngineStatsParams>,
) -> impl IntoResponse {
    let Some(pg) = &state.admin_store else {
        return (StatusCode::SERVICE_UNAVAILABLE, "Postgres persistence not configured")
            .into_response();
    };
    let hours = params.hours.unwrap_or(24).clamp(1, 168);
    match pg.get_group_stats(hours).await {
        Ok(rows) => Json::<Vec<GroupStatRow>>(rows).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

#[derive(Debug, serde::Deserialize)]
struct EngineStatsParams {
    hours: Option<i64>,
}

/// Request body for `PATCH /admin/clusters/:group/:cluster`.
#[derive(Debug, Deserialize, ToSchema)]
pub struct ClusterUpdateRequest {
    /// Set the administrative enabled state. `null` / absent = no change.
    pub enabled: Option<bool>,
    /// Update the maximum concurrent query limit. `null` / absent = no change.
    pub max_running_queries: Option<u64>,
}

/// Update mutable runtime config for a cluster (enable/disable, concurrency limit).
#[utoipa::path(
    patch,
    path = "/admin/clusters/{group}/{cluster}",
    tag = "admin",
    params(
        ("group" = String, Path, description = "Cluster group name"),
        ("cluster" = String, Path, description = "Cluster name"),
    ),
    request_body = ClusterUpdateRequest,
    responses(
        (status = 200, description = "Updated cluster state snapshot", body = ClusterStateDto),
        (status = 404, description = "Cluster not found", body = str),
        (status = 500, description = "Internal error", body = str),
    )
)]
async fn update_cluster_handler(
    State(state): State<Arc<AdminState>>,
    Path((group, cluster)): Path<(String, String)>,
    Json(body): Json<ClusterUpdateRequest>,
) -> impl IntoResponse {
    let group = ClusterGroupName(group);
    let cluster_name = ClusterName(cluster);

    match state.cluster_manager.update_cluster(&group, &cluster_name, body.enabled, body.max_running_queries).await {
        Ok(false) => (StatusCode::NOT_FOUND, "Cluster not found").into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
        Ok(true) => {
            match state.cluster_manager.cluster_state(&group, &cluster_name).await {
                Ok(Some(s)) => Json(ClusterStateDto {
                    group_name: s.group_name.0,
                    cluster_name: s.cluster_name.0,
                    engine_type: format!("{:?}", s.engine_type),
                    endpoint: s.endpoint,
                    running_queries: s.running_queries,
                    queued_queries: s.queued_queries,
                    max_running_queries: s.max_running_queries,
                    is_healthy: s.is_healthy,
                    enabled: s.enabled,
                }).into_response(),
                Ok(None) => (StatusCode::NOT_FOUND, "Cluster not found after update").into_response(),
                Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Persisted cluster config CRUD
// ---------------------------------------------------------------------------

macro_rules! require_pg {
    ($state:expr) => {
        match &$state.admin_store {
            Some(pg) => pg,
            None => {
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    "Postgres persistence not configured",
                )
                    .into_response()
            }
        }
    };
}

async fn list_cluster_configs_handler(
    State(state): State<Arc<AdminState>>,
) -> impl IntoResponse {
    let pg = require_pg!(state);
    match pg.list_cluster_configs().await {
        Ok(rows) => Json(rows).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn get_cluster_config_handler(
    State(state): State<Arc<AdminState>>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    let pg = require_pg!(state);
    match pg.get_cluster_config(&name).await {
        Ok(Some(r)) => Json(r).into_response(),
        Ok(None) => (StatusCode::NOT_FOUND, "Cluster config not found").into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn upsert_cluster_config_handler(
    State(state): State<Arc<AdminState>>,
    Path(name): Path<String>,
    Json(body): Json<UpsertClusterConfig>,
) -> impl IntoResponse {
    let pg = require_pg!(state);
    match pg.upsert_cluster_config(&name, &body).await {
        Ok(r) => Json(r).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn delete_cluster_config_handler(
    State(state): State<Arc<AdminState>>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    let pg = require_pg!(state);
    match pg.delete_cluster_config(&name).await {
        Ok(true) => StatusCode::NO_CONTENT.into_response(),
        Ok(false) => (StatusCode::NOT_FOUND, "Cluster config not found").into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

// ---------------------------------------------------------------------------
// Persisted cluster group config CRUD
// ---------------------------------------------------------------------------

async fn list_group_configs_handler(
    State(state): State<Arc<AdminState>>,
) -> impl IntoResponse {
    let pg = require_pg!(state);
    match pg.list_group_configs().await {
        Ok(rows) => Json(rows).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn get_group_config_handler(
    State(state): State<Arc<AdminState>>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    let pg = require_pg!(state);
    match pg.get_group_config(&name).await {
        Ok(Some(r)) => Json(r).into_response(),
        Ok(None) => (StatusCode::NOT_FOUND, "Group config not found").into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn upsert_group_config_handler(
    State(state): State<Arc<AdminState>>,
    Path(name): Path<String>,
    Json(body): Json<UpsertClusterGroupConfig>,
) -> impl IntoResponse {
    let pg = require_pg!(state);
    match pg.upsert_group_config(&name, &body).await {
        Ok(r) => Json(r).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn delete_group_config_handler(
    State(state): State<Arc<AdminState>>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    let pg = require_pg!(state);
    match pg.delete_group_config(&name).await {
        Ok(true) => StatusCode::NO_CONTENT.into_response(),
        Ok(false) => (StatusCode::NOT_FOUND, "Group config not found").into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

/// Static engine registry — metadata and config schema for every supported engine.
#[utoipa::path(
    get,
    path = "/admin/engine-registry",
    tag = "admin",
    responses(
        (status = 200, description = "List of engine descriptors", body = str),
    )
)]
async fn engine_registry_handler() -> impl IntoResponse {
    Json(engine_registry::engine_descriptors())
}

/// Swagger UI — interactive API explorer (loads spec from /openapi.json via CDN).
async fn swagger_ui_handler() -> impl IntoResponse {
    const HTML: &str = r##"<!DOCTYPE html>
<html>
<head>
  <title>QueryFlux Admin API</title>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <link rel="stylesheet" type="text/css" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css">
</head>
<body>
<div id="swagger-ui"></div>
<script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
<script>
  SwaggerUIBundle({ url: "/openapi.json", dom_id: "#swagger-ui", presets: [SwaggerUIBundle.presets.apis, SwaggerUIBundle.SwaggerUIStandalonePreset], layout: "BaseLayout" });
</script>
</body>
</html>"##;
    (StatusCode::OK, [("content-type", "text/html")], HTML)
}
