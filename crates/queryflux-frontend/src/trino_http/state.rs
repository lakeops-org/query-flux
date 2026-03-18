use std::collections::HashMap;
use std::sync::Arc;

use queryflux_cluster_manager::ClusterGroupManager;
use queryflux_engine_adapters::EngineAdapterTrait;
use queryflux_persistence::Persistence;
use queryflux_routing::chain::RouterChain;

/// Shared application state for the Trino HTTP frontend.
/// Passed to every Axum handler via `axum::extract::State`.
pub struct AppState {
    /// The external URL clients use to reach QueryFlux (used for nextUri rewriting).
    pub external_address: String,
    pub cluster_manager: Arc<dyn ClusterGroupManager>,
    /// (group_name, cluster_name) → adapter
    pub adapters: HashMap<(String, String), Arc<dyn EngineAdapterTrait>>,
    pub persistence: Arc<dyn Persistence>,
    pub router_chain: RouterChain,
}

impl AppState {
    pub fn adapter(
        &self,
        group: &str,
        cluster: &str,
    ) -> Option<Arc<dyn EngineAdapterTrait>> {
        self.adapters
            .get(&(group.to_string(), cluster.to_string()))
            .cloned()
    }
}
