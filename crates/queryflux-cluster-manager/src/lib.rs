pub mod cluster_state;
pub mod simple;

use async_trait::async_trait;
use queryflux_core::{
    error::Result,
    query::{ClusterGroupName, ClusterName},
};

use cluster_state::ClusterStateSnapshot;

/// Manages all cluster groups: picks the best cluster for a new query,
/// tracks running/queued counts, and exposes live state for the admin API.
#[async_trait]
pub trait ClusterGroupManager: Send + Sync {
    /// Pick the least-loaded healthy cluster in a group.
    /// Returns `None` if the group is at capacity (triggers queueing).
    async fn acquire_cluster(
        &self,
        group: &ClusterGroupName,
    ) -> Result<Option<ClusterName>>;

    /// Signal that a query has finished on a cluster (success, failure, or cancel).
    async fn release_cluster(&self, group: &ClusterGroupName, cluster: &ClusterName) -> Result<()>;

    /// Get a snapshot of live state for a specific cluster.
    async fn cluster_state(
        &self,
        group: &ClusterGroupName,
        cluster: &ClusterName,
    ) -> Result<Option<ClusterStateSnapshot>>;

    /// Get state for all clusters across all groups.
    async fn all_cluster_states(&self) -> Result<Vec<ClusterStateSnapshot>>;
}
