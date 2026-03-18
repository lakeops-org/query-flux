use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use queryflux_core::query::{ClusterGroupName, ClusterName, EngineType};
use serde::{Deserialize, Serialize};

/// Live mutable state for a single cluster instance.
/// Shared across threads via `Arc`; counters are atomic.
#[derive(Debug)]
pub struct ClusterState {
    pub cluster_name: ClusterName,
    pub group_name: ClusterGroupName,
    pub engine_type: EngineType,
    pub endpoint: Option<String>,
    pub max_running_queries: u64,
    running_queries: Arc<AtomicU64>,
    queued_queries: Arc<AtomicU64>,
}

impl ClusterState {
    pub fn new(
        cluster_name: ClusterName,
        group_name: ClusterGroupName,
        engine_type: EngineType,
        endpoint: Option<String>,
        max_running_queries: u64,
    ) -> Self {
        Self {
            cluster_name,
            group_name,
            engine_type,
            endpoint,
            max_running_queries,
            running_queries: Arc::new(AtomicU64::new(0)),
            queued_queries: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn running_queries(&self) -> u64 {
        self.running_queries.load(Ordering::Relaxed)
    }

    pub fn queued_queries(&self) -> u64 {
        self.queued_queries.load(Ordering::Relaxed)
    }

    pub fn increment_running(&self) {
        self.running_queries.fetch_add(1, Ordering::Relaxed);
    }

    pub fn decrement_running(&self) {
        self.running_queries.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn increment_queued(&self) {
        self.queued_queries.fetch_add(1, Ordering::Relaxed);
    }

    pub fn decrement_queued(&self) {
        self.queued_queries.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn is_at_capacity(&self) -> bool {
        self.running_queries() >= self.max_running_queries
    }

    pub fn snapshot(&self) -> ClusterStateSnapshot {
        ClusterStateSnapshot {
            cluster_name: self.cluster_name.clone(),
            group_name: self.group_name.clone(),
            running_queries: self.running_queries(),
            queued_queries: self.queued_queries(),
            max_running_queries: self.max_running_queries,
        }
    }
}

/// A point-in-time read of cluster state, safe to serialize and send over the admin API.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterStateSnapshot {
    pub cluster_name: ClusterName,
    pub group_name: ClusterGroupName,
    pub running_queries: u64,
    pub queued_queries: u64,
    pub max_running_queries: u64,
}
