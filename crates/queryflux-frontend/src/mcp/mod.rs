pub mod sink;
pub mod tools;

use std::sync::Arc;

use async_trait::async_trait;
use axum::{
    body::Body,
    extract::Request,
    http::{header, StatusCode},
    middleware::{self, Next},
    response::Response,
    Router,
};
use queryflux_auth::Credentials;
use queryflux_core::error::{QueryFluxError, Result};
use rmcp::transport::streamable_http_server::{
    StreamableHttpServerConfig, StreamableHttpService,
    session::local::LocalSessionManager,
};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::{state::AppState, FrontendListenerTrait};
use tools::QueryFluxMcpServer;

// ---------------------------------------------------------------------------
// Task-local bearer token — threaded from the axum auth middleware to tool
// handlers via tokio's task-local storage.
// ---------------------------------------------------------------------------

tokio::task_local! {
    pub(crate) static BEARER_TOKEN: Option<String>;
}

// ---------------------------------------------------------------------------
// Auth middleware
// ---------------------------------------------------------------------------

/// Extracts the `Authorization: Bearer <token>` header and runs it through
/// the auth provider. On failure, returns HTTP 401 before the MCP handler
/// sees the request. On success, threads the raw token into `BEARER_TOKEN`
/// so tool handlers can build a full `AuthContext` later.
async fn auth_middleware(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
    req: Request,
    next: Next,
) -> Response {
    debug!(path = %req.uri().path(), "MCP auth_middleware: incoming request");
    let token = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(|t| t.to_string());

    // Validate credentials (NoneAuthProvider always passes; StaticAuthProvider requires a match).
    let creds = Credentials {
        username: None,
        password: None,
        bearer_token: token.clone(),
    };
    if let Err(e) = state.auth_provider.authenticate(&creds).await {
        warn!(error = %e, "MCP auth_middleware: request unauthorized");
        return Response::builder()
            .status(StatusCode::UNAUTHORIZED)
            .body(Body::from(format!("Unauthorized: {e}")))
            .unwrap_or_default();
    }
    debug!(has_bearer = token.is_some(), "MCP auth_middleware: auth passed");

    // Thread the raw token into the task so tool handlers can rebuild AuthContext.
    BEARER_TOKEN
        .scope(token, async move { next.run(req).await })
        .await
}

// ---------------------------------------------------------------------------
// MCP Frontend
// ---------------------------------------------------------------------------

pub struct McpFrontend {
    state: Arc<AppState>,
    port: u16,
}

impl McpFrontend {
    pub fn new(state: Arc<AppState>, port: u16) -> Self {
        Self { state, port }
    }
}

#[async_trait]
impl FrontendListenerTrait for McpFrontend {
    async fn listen(&self) -> Result<()> {
        let addr = format!("0.0.0.0:{}", self.port)
            .parse::<std::net::SocketAddr>()
            .map_err(|e| QueryFluxError::Other(e.into()))?;

        let ct = CancellationToken::new();

        let server_state = self.state.clone();
        let config = StreamableHttpServerConfig::default()
            .with_cancellation_token(ct.child_token());

        let service: StreamableHttpService<QueryFluxMcpServer, LocalSessionManager> =
            StreamableHttpService::new(
                {
                    let state = server_state.clone();
                    move || Ok(QueryFluxMcpServer::new(state.clone()))
                },
                Default::default(),
                config,
            );
        info!("MCP streamable HTTP service created");

        let mcp_router = Router::new().nest_service("/mcp", service);

        let app = Router::new()
            .merge(mcp_router)
            .route_layer(middleware::from_fn_with_state(
                self.state.clone(),
                auth_middleware,
            ));

        info!("MCP frontend listening on {addr}");

        let listener = TcpListener::bind(addr)
            .await
            .map_err(|e| QueryFluxError::Other(e.into()))?;

        axum::serve(listener, app)
            .with_graceful_shutdown(async move { ct.cancelled().await })
            .await
            .map_err(|e| QueryFluxError::Other(e.into()))
    }
}
