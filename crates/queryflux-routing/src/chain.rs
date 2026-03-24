use queryflux_auth::AuthContext;
use queryflux_core::{
    error::Result,
    query::{ClusterGroupName, FrontendProtocol},
    session::SessionContext,
};
use serde::Serialize;

use crate::RouterTrait;

/// One router's evaluation result recorded in the trace.
#[derive(Debug, Clone, Serialize)]
pub struct RouterDecision {
    pub router_type: &'static str,
    pub matched: bool,
    /// The group selected by this router, if it matched.
    pub result: Option<String>,
}

/// Full record of how a query was routed: every router that was evaluated,
/// which one matched, and whether the fallback was used.
#[derive(Debug, Clone, Serialize)]
pub struct RoutingTrace {
    pub decisions: Vec<RouterDecision>,
    pub final_group: String,
    pub used_fallback: bool,
}

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

    /// Route and return only the target group (no trace overhead).
    pub async fn route(
        &self,
        sql: &str,
        session: &SessionContext,
        frontend_protocol: &FrontendProtocol,
        auth_ctx: Option<&AuthContext>,
    ) -> Result<ClusterGroupName> {
        for router in &self.routers {
            if let Some(group) = router.route(sql, session, frontend_protocol, auth_ctx).await? {
                return Ok(group);
            }
        }
        Ok(self.fallback.clone())
    }

    /// Route and return both the target group and a full routing trace for metrics/UI.
    pub async fn route_with_trace(
        &self,
        sql: &str,
        session: &SessionContext,
        frontend_protocol: &FrontendProtocol,
        auth_ctx: Option<&AuthContext>,
    ) -> Result<(ClusterGroupName, RoutingTrace)> {
        let mut decisions = Vec::with_capacity(self.routers.len());

        for router in &self.routers {
            match router.route(sql, session, frontend_protocol, auth_ctx).await? {
                Some(group) => {
                    decisions.push(RouterDecision {
                        router_type: router.type_name(),
                        matched: true,
                        result: Some(group.0.clone()),
                    });
                    let trace = RoutingTrace {
                        decisions,
                        final_group: group.0.clone(),
                        used_fallback: false,
                    };
                    return Ok((group, trace));
                }
                None => {
                    decisions.push(RouterDecision {
                        router_type: router.type_name(),
                        matched: false,
                        result: None,
                    });
                }
            }
        }

        let trace = RoutingTrace {
            decisions,
            final_group: self.fallback.0.clone(),
            used_fallback: true,
        };
        Ok((self.fallback.clone(), trace))
    }
}
