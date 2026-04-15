pub mod http;
pub mod sql_api;
#[cfg(test)]
mod tests;

use std::sync::Arc;

use axum::Router;
use queryflux_core::error::{QueryFluxError, Result};
use tracing::info;

use crate::state::AppState;
use crate::FrontendListenerTrait;

/// Combined Snowflake frontend — wire protocol (Form 1) + SQL REST API v2 (Form 2)
/// on a single port with a single shared `Arc<AppState>`.
pub struct SnowflakeFrontend {
    state: Arc<AppState>,
    port: u16,
}

impl SnowflakeFrontend {
    pub fn new(state: Arc<AppState>, port: u16) -> Self {
        Self { state, port }
    }

    pub fn router(&self) -> Router {
        http::routes()
            .merge(sql_api::routes())
            .with_state(self.state.clone())
    }
}

#[async_trait::async_trait]
impl FrontendListenerTrait for SnowflakeFrontend {
    async fn listen(&self) -> Result<()> {
        let addr: std::net::SocketAddr = format!("0.0.0.0:{}", self.port)
            .parse()
            .map_err(|e: std::net::AddrParseError| QueryFluxError::Other(e.into()))?;

        info!("Snowflake frontend (wire + SQL API) listening on {addr}");

        axum::serve(
            tokio::net::TcpListener::bind(addr)
                .await
                .map_err(|e| QueryFluxError::Other(e.into()))?,
            self.router(),
        )
        .await
        .map_err(|e| QueryFluxError::Other(e.into()))
    }
}
