use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use queryflux_core::{
    error::{QueryFluxError, Result},
    query::{ClusterGroupName, ClusterName},
};

use crate::{cluster_state::{ClusterState, ClusterStateSnapshot}, ClusterGroupManager};

pub struct SimpleClusterGroupManager {
    groups: HashMap<ClusterGroupName, Vec<Arc<ClusterState>>>,
}

impl SimpleClusterGroupManager {
    pub fn new(groups: HashMap<ClusterGroupName, Vec<Arc<ClusterState>>>) -> Self {
        Self { groups }
    }
}

#[async_trait]
impl ClusterGroupManager for SimpleClusterGroupManager {
    async fn acquire_cluster(&self, group: &ClusterGroupName) -> Result<Option<ClusterName>> {
        let clusters = self.groups.get(group).ok_or_else(|| {
            QueryFluxError::NoClusterGroupAvailable(group.0.clone())
        })?;

        let best = clusters
            .iter()
            .filter(|c| !c.is_at_capacity())
            .min_by_key(|c| c.running_queries());

        if let Some(cluster) = best {
            cluster.increment_running();
            Ok(Some(cluster.cluster_name.clone()))
        } else {
            Ok(None)
        }
    }

    async fn release_cluster(&self, group: &ClusterGroupName, cluster: &ClusterName) -> Result<()> {
        if let Some(clusters) = self.groups.get(group) {
            if let Some(state) = clusters.iter().find(|c| &c.cluster_name == cluster) {
                state.decrement_running();
            }
        }
        Ok(())
    }

    async fn cluster_state(
        &self,
        group: &ClusterGroupName,
        cluster: &ClusterName,
    ) -> Result<Option<ClusterStateSnapshot>> {
        Ok(self.groups.get(group)
            .and_then(|cs| cs.iter().find(|c| &c.cluster_name == cluster))
            .map(|c| c.snapshot()))
    }

    async fn all_cluster_states(&self) -> Result<Vec<ClusterStateSnapshot>> {
        Ok(self.groups.values()
            .flat_map(|cs| cs.iter().map(|c| c.snapshot()))
            .collect())
    }
}
