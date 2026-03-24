use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use queryflux_core::{config::EngineConfig, query::EngineType};

/// A point-in-time view of one cluster within a group, passed to strategies.
/// Strategies inspect this to pick a candidate — they never mutate state.
pub struct ClusterCandidate<'a> {
    pub name: &'a str,
    pub engine_type: EngineType,
    pub running_queries: u64,
    pub max_running_queries: u64,
}

/// Pluggable cluster selection algorithm.
///
/// Receives a non-empty slice of healthy, enabled, under-capacity candidates
/// and returns the index of the chosen one. Returning `None` from a non-empty
/// slice is treated as "no selection" — the caller falls back to index 0.
pub trait ClusterSelectionStrategy: Send + Sync {
    fn pick(&self, candidates: &[ClusterCandidate<'_>]) -> Option<usize>;
}

// ---------------------------------------------------------------------------
// Round-robin
// ---------------------------------------------------------------------------

pub struct RoundRobinStrategy {
    counter: AtomicU64,
}

impl RoundRobinStrategy {
    pub fn new() -> Self {
        Self {
            counter: AtomicU64::new(0),
        }
    }
}

impl Default for RoundRobinStrategy {
    fn default() -> Self {
        Self::new()
    }
}

impl ClusterSelectionStrategy for RoundRobinStrategy {
    fn pick(&self, candidates: &[ClusterCandidate<'_>]) -> Option<usize> {
        if candidates.is_empty() {
            return None;
        }
        let idx = self.counter.fetch_add(1, Ordering::Relaxed) as usize % candidates.len();
        Some(idx)
    }
}

// ---------------------------------------------------------------------------
// Least loaded (pick cluster with most remaining capacity)
// ---------------------------------------------------------------------------

pub struct LeastLoadedStrategy;

impl ClusterSelectionStrategy for LeastLoadedStrategy {
    fn pick(&self, candidates: &[ClusterCandidate<'_>]) -> Option<usize> {
        candidates
            .iter()
            .enumerate()
            .min_by_key(|(_, c)| c.running_queries)
            .map(|(i, _)| i)
    }
}

// ---------------------------------------------------------------------------
// Failover (try clusters in member order)
// ---------------------------------------------------------------------------

pub struct FailoverStrategy;

impl ClusterSelectionStrategy for FailoverStrategy {
    fn pick(&self, candidates: &[ClusterCandidate<'_>]) -> Option<usize> {
        // Candidates are already filtered to healthy + under capacity.
        // The first one in the slice is the highest-priority available cluster.
        if candidates.is_empty() {
            None
        } else {
            Some(0)
        }
    }
}

// ---------------------------------------------------------------------------
// Engine affinity (prefer engines in a given order for mixed-engine groups)
// ---------------------------------------------------------------------------

pub struct EngineAffinityStrategy {
    preference: Vec<EngineType>,
}

impl EngineAffinityStrategy {
    pub fn new(preference: Vec<EngineConfig>) -> Self {
        Self {
            preference: preference.iter().map(engine_config_to_type).collect(),
        }
    }
}

impl ClusterSelectionStrategy for EngineAffinityStrategy {
    fn pick(&self, candidates: &[ClusterCandidate<'_>]) -> Option<usize> {
        if candidates.is_empty() {
            return None;
        }
        // Find the highest-priority engine type that has at least one candidate.
        for preferred_engine in &self.preference {
            let engine_candidates: Vec<usize> = candidates
                .iter()
                .enumerate()
                .filter(|(_, c)| &c.engine_type == preferred_engine)
                .map(|(i, _)| i)
                .collect();
            if !engine_candidates.is_empty() {
                // Among candidates of the preferred engine, pick the least loaded.
                return engine_candidates
                    .into_iter()
                    .min_by_key(|&i| candidates[i].running_queries);
            }
        }
        // No preferred engine available — fall back to first candidate.
        Some(0)
    }
}

// ---------------------------------------------------------------------------
// Weighted random
// ---------------------------------------------------------------------------

pub struct WeightedStrategy {
    /// Ordered list of (cluster_name, weight) matching the group's member list order.
    weights: Vec<(String, u32)>,
}

impl WeightedStrategy {
    pub fn new(weights: HashMap<String, u32>) -> Self {
        Self {
            weights: weights.into_iter().collect(),
        }
    }
}

impl ClusterSelectionStrategy for WeightedStrategy {
    fn pick(&self, candidates: &[ClusterCandidate<'_>]) -> Option<usize> {
        if candidates.is_empty() {
            return None;
        }

        // Build (candidate_index, weight) pairs for eligible candidates.
        let weighted: Vec<(usize, u32)> = candidates
            .iter()
            .enumerate()
            .map(|(i, c)| {
                let w = self
                    .weights
                    .iter()
                    .find(|(name, _)| name == c.name)
                    .map(|(_, w)| *w)
                    .unwrap_or(1);
                (i, w)
            })
            .collect();

        let total: u32 = weighted.iter().map(|(_, w)| w).sum();
        if total == 0 {
            return Some(0);
        }

        // Deterministic pseudo-random using sum of running queries as seed.
        // Good enough for load distribution without needing an RNG dependency.
        let seed: u64 = candidates
            .iter()
            .map(|c| c.running_queries)
            .sum::<u64>()
            .wrapping_add(candidates.len() as u64)
            .wrapping_mul(2654435761);
        let roll = (seed % total as u64) as u32;

        let mut acc = 0u32;
        for (idx, weight) in &weighted {
            acc += weight;
            if roll < acc {
                return Some(*idx);
            }
        }
        weighted.last().map(|(i, _)| *i)
    }
}

// ---------------------------------------------------------------------------
// Factory
// ---------------------------------------------------------------------------

pub fn strategy_from_config(
    config: Option<&queryflux_core::config::StrategyConfig>,
) -> Arc<dyn ClusterSelectionStrategy> {
    use queryflux_core::config::StrategyConfig;
    match config {
        None | Some(StrategyConfig::RoundRobin) => Arc::new(RoundRobinStrategy::new()),
        Some(StrategyConfig::LeastLoaded) => Arc::new(LeastLoadedStrategy),
        Some(StrategyConfig::Failover) => Arc::new(FailoverStrategy),
        Some(StrategyConfig::EngineAffinity { preference }) => {
            Arc::new(EngineAffinityStrategy::new(preference.clone()))
        }
        Some(StrategyConfig::Weighted { weights }) => {
            Arc::new(WeightedStrategy::new(weights.clone()))
        }
    }
}

fn engine_config_to_type(cfg: &EngineConfig) -> EngineType {
    match cfg {
        EngineConfig::Trino => EngineType::Trino,
        EngineConfig::DuckDb => EngineType::DuckDb,
        EngineConfig::DuckDbHttp => EngineType::DuckDbHttp,
        EngineConfig::StarRocks => EngineType::StarRocks,
        EngineConfig::ClickHouse => EngineType::ClickHouse,
        EngineConfig::Athena => EngineType::Athena,
    }
}
