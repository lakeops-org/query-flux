use async_trait::async_trait;
use dashmap::DashMap;
use queryflux_core::{
    error::Result,
    query::{ExecutingQuery, ProxyQueryId, QueuedQuery},
};

use crate::Persistence;

#[derive(Default)]
pub struct InMemoryPersistence {
    executing: DashMap<String, ExecutingQuery>,
    queued: DashMap<String, QueuedQuery>,
}

impl InMemoryPersistence {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl Persistence for InMemoryPersistence {
    async fn upsert(&self, query: ExecutingQuery) -> Result<()> {
        self.executing.insert(query.id.0.clone(), query);
        Ok(())
    }
    async fn get(&self, id: &ProxyQueryId) -> Result<Option<ExecutingQuery>> {
        Ok(self.executing.get(&id.0).map(|e| e.value().clone()))
    }
    async fn delete(&self, id: &ProxyQueryId) -> Result<()> {
        self.executing.remove(&id.0);
        Ok(())
    }
    async fn list_all(&self) -> Result<Vec<ExecutingQuery>> {
        Ok(self.executing.iter().map(|e| e.value().clone()).collect())
    }

    async fn upsert_queued(&self, query: QueuedQuery) -> Result<()> {
        self.queued.insert(query.id.0.clone(), query);
        Ok(())
    }
    async fn get_queued(&self, id: &ProxyQueryId) -> Result<Option<QueuedQuery>> {
        Ok(self.queued.get(&id.0).map(|e| e.value().clone()))
    }
    async fn delete_queued(&self, id: &ProxyQueryId) -> Result<()> {
        self.queued.remove(&id.0);
        Ok(())
    }
    async fn list_queued(&self) -> Result<Vec<QueuedQuery>> {
        Ok(self.queued.iter().map(|e| e.value().clone()).collect())
    }
}
