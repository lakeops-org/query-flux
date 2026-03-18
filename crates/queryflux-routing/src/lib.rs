pub mod chain;
pub mod implementations;

use async_trait::async_trait;
use queryflux_core::{
    error::Result,
    query::{ClusterGroupName, FrontendProtocol},
    session::SessionContext,
};

/// A router inspects an incoming query and optionally returns the target cluster group.
///
/// Routers are evaluated in order (a chain). The first router that returns `Some` wins.
/// If all routers return `None`, the routing fallback group from config is used.
#[async_trait]
pub trait RouterTrait: Send + Sync {
    async fn route(
        &self,
        sql: &str,
        session: &SessionContext,
        frontend_protocol: &FrontendProtocol,
    ) -> Result<Option<ClusterGroupName>>;
}
