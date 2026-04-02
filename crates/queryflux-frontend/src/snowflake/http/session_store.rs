use std::sync::Arc;
use std::time::Instant;

use dashmap::DashMap;
use queryflux_auth::AuthContext;
use queryflux_core::query::ClusterGroupName;

/// Stores active QueryFlux Snowflake wire-protocol sessions keyed by the qf_token
/// issued to the client at login. Sessions are local to QueryFlux — no backend
/// Snowflake account is needed.
pub struct SnowflakeSessionStore {
    sessions: DashMap<String, SnowflakeSession>,
}

pub struct SnowflakeSession {
    pub qf_token: String,
    pub user: Option<String>,
    pub auth_ctx: AuthContext,
    /// Cluster group resolved at login time (via the router chain).
    pub group: ClusterGroupName,
    /// Database/schema hints from the login request (SESSION_PARAMETERS or query params).
    pub database: Option<String>,
    pub schema: Option<String>,
    pub created_at: Instant,
}

impl SnowflakeSessionStore {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            sessions: DashMap::new(),
        })
    }

    pub fn insert(&self, token: String, session: SnowflakeSession) {
        self.sessions.insert(token, session);
    }

    pub fn get(
        &self,
        token: &str,
    ) -> Option<dashmap::mapref::one::Ref<'_, String, SnowflakeSession>> {
        self.sessions.get(token)
    }

    pub fn remove(&self, token: &str) {
        self.sessions.remove(token);
    }

    pub fn contains(&self, token: &str) -> bool {
        self.sessions.contains_key(token)
    }
}
