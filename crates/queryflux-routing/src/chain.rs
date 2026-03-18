use queryflux_core::{
    error::Result,
    query::{ClusterGroupName, FrontendProtocol},
    session::SessionContext,
};

use crate::RouterTrait;

/// Evaluates a list of routers in order; first non-None result wins.
/// Falls back to the configured default group if all routers return None.
pub struct RouterChain {
    routers: Vec<Box<dyn RouterTrait>>,
    fallback: ClusterGroupName,
}

impl RouterChain {
    pub fn new(routers: Vec<Box<dyn RouterTrait>>, fallback: ClusterGroupName) -> Self {
        Self { routers, fallback }
    }

    pub async fn route(
        &self,
        sql: &str,
        session: &SessionContext,
        frontend_protocol: &FrontendProtocol,
    ) -> Result<ClusterGroupName> {
        for router in &self.routers {
            if let Some(group) = router.route(sql, session, frontend_protocol).await? {
                return Ok(group);
            }
        }
        Ok(self.fallback.clone())
    }
}
