pub mod chain;
pub mod implementations;

use async_trait::async_trait;
use queryflux_auth::AuthContext;
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
    /// Short name used in routing traces (e.g. `"Header"`, `"QueryRegex"`).
    fn type_name(&self) -> &'static str;

    /// Route an incoming query to a cluster group.
    ///
    /// `auth_ctx` is `None` during Phase 1 (NoneAuthProvider, no threading yet) and
    /// `Some` once authentication is wired into all frontends (Phase 2+).
    /// Identity-aware routers (e.g. `UserGroup`) should prefer `auth_ctx.user` when
    /// available, falling back to `session.user()` only when `auth_ctx` is `None`.
    async fn route(
        &self,
        sql: &str,
        session: &SessionContext,
        frontend_protocol: &FrontendProtocol,
        auth_ctx: Option<&AuthContext>,
    ) -> Result<Option<ClusterGroupName>>;
}
