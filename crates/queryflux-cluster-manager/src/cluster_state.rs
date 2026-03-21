use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
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
    max_running_queries: Arc<AtomicU64>,
    /// Whether this cluster is administratively enabled.
    /// Disabled clusters are excluded from `acquire_cluster`.
    enabled: Arc<AtomicBool>,
    running_queries: Arc<AtomicU64>,
    queued_queries: Arc<AtomicU64>,
    /// Set to `false` by the background health-check loop when the cluster
    /// fails its health check. Starts as `true` (optimistic).
    is_healthy: Arc<AtomicBool>,
}

impl ClusterState {
    pub fn new(
        cluster_name: ClusterName,
        group_name: ClusterGroupName,
        engine_type: EngineType,
        endpoint: Option<String>,
        max_running_queries: u64,
        enabled: bool,
    ) -> Self {
        Self {
            cluster_name,
            group_name,
            engine_type,
            endpoint,
            max_running_queries: Arc::new(AtomicU64::new(max_running_queries)),
            enabled: Arc::new(AtomicBool::new(enabled)),
            running_queries: Arc::new(AtomicU64::new(0)),
            queued_queries: Arc::new(AtomicU64::new(0)),
            is_healthy: Arc::new(AtomicBool::new(true)),
        }
    }

    pub fn max_running_queries(&self) -> u64 {
        self.max_running_queries.load(Ordering::Relaxed)
    }

    pub fn set_max_running_queries(&self, value: u64) {
        self.max_running_queries.store(value, Ordering::Relaxed);
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    pub fn set_enabled(&self, enabled: bool) {
        self.enabled.store(enabled, Ordering::Relaxed);
    }

    pub fn running_queries(&self) -> u64 {
        self.running_queries.load(Ordering::Relaxed)
    }

    pub fn queued_queries(&self) -> u64 {
        self.queued_queries.load(Ordering::Relaxed)
    }

    pub fn is_healthy(&self) -> bool {
        self.is_healthy.load(Ordering::Relaxed)
    }

    /// Called by the background health-check loop.
    pub fn set_healthy(&self, healthy: bool) {
        self.is_healthy.store(healthy, Ordering::Relaxed);
    }

    /// Overwrite the running query counter with a ground-truth value from the engine.
    /// Called by the background reconciler. Clamped to max_running_queries to stay sane.
    pub fn set_running_queries(&self, count: u64) {
        let clamped = count.min(self.max_running_queries());
        self.running_queries.store(clamped, Ordering::Relaxed);
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
        self.running_queries() >= self.max_running_queries()
    }

    pub fn snapshot(&self) -> ClusterStateSnapshot {
        ClusterStateSnapshot {
            cluster_name: self.cluster_name.clone(),
            group_name: self.group_name.clone(),
            engine_type: self.engine_type.clone(),
            endpoint: self.endpoint.clone(),
            running_queries: self.running_queries(),
            queued_queries: self.queued_queries(),
            max_running_queries: self.max_running_queries(),
            is_healthy: self.is_healthy(),
            enabled: self.is_enabled(),
        }
    }
}

/// A point-in-time read of cluster state, safe to serialize and send over the admin API.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterStateSnapshot {
    pub cluster_name: ClusterName,
    pub group_name: ClusterGroupName,
    pub engine_type: EngineType,
    /// The HTTP endpoint of the cluster (e.g. `http://trino-1:8080`).
    pub endpoint: Option<String>,
    pub running_queries: u64,
    pub queued_queries: u64,
    pub max_running_queries: u64,
    /// Whether the most recent health check passed.
    pub is_healthy: bool,
    /// Whether this cluster is administratively enabled.
    pub enabled: bool,
}
