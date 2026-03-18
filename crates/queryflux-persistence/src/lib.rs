pub mod in_memory;

use async_trait::async_trait;
use queryflux_core::{
    error::Result,
    query::{ExecutingQuery, ProxyQueryId, QueuedQuery},
};

#[async_trait]
pub trait Persistence: Send + Sync {
    // --- Executing queries (submitted to an engine backend) ---
    async fn upsert(&self, query: ExecutingQuery) -> Result<()>;
    async fn get(&self, id: &ProxyQueryId) -> Result<Option<ExecutingQuery>>;
    async fn delete(&self, id: &ProxyQueryId) -> Result<()>;
    async fn list_all(&self) -> Result<Vec<ExecutingQuery>>;

    // --- Queued queries (waiting for cluster capacity) ---
    async fn upsert_queued(&self, query: QueuedQuery) -> Result<()>;
    async fn get_queued(&self, id: &ProxyQueryId) -> Result<Option<QueuedQuery>>;
    async fn delete_queued(&self, id: &ProxyQueryId) -> Result<()>;
    async fn list_queued(&self) -> Result<Vec<QueuedQuery>>;
}
