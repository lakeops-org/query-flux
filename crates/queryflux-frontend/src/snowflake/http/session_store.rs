//! In-memory Snowflake HTTP wire sessions (login token → routing/auth context).
//!
//! **Not replicated across QueryFlux processes.** With multiple replicas or during rolling
//! upgrades, clients must stick to one instance (load balancer affinity on the Snowflake token /
//! `Authorization` header) or sessions will not resolve. Configure
//! `queryflux.enforceSnowflakeHttpSessionAffinity` + `sessionAffinityAcknowledged` on the
//! Snowflake HTTP frontend once sticky routing is in place.
//!
//! Sessions honor [`SnowflakeHttpSessionPolicy`] (max age + idle timeout) on every authenticated
//! request (`validate_snowflake_session`).

use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use queryflux_auth::AuthContext;
use queryflux_core::config::FrontendConfig;
use queryflux_core::query::ClusterGroupName;

/// Wall-clock and idle limits for Snowflake HTTP wire sessions.
#[derive(Debug, Clone)]
pub struct SnowflakeHttpSessionPolicy {
    /// Maximum lifetime since login. `None` = no limit.
    pub max_session_age: Option<Duration>,
    /// Maximum time since last successful [`SnowflakeSessionStore::validate_snowflake_session`].
    /// `None` = no idle eviction.
    pub idle_timeout: Option<Duration>,
}

impl Default for SnowflakeHttpSessionPolicy {
    fn default() -> Self {
        Self {
            max_session_age: Some(Duration::from_secs(24 * 3600)),
            idle_timeout: Some(Duration::from_secs(4 * 3600)),
        }
    }
}

impl SnowflakeHttpSessionPolicy {
    /// Build policy from `frontends.snowflakeHttp` YAML. Omitted fields use defaults (24h / 4h).
    /// `0` disables that limit (matches “no max age” / “no idle timeout”).
    pub fn from_frontend_config(cfg: &FrontendConfig) -> Self {
        Self {
            max_session_age: match cfg.snowflake_session_max_age_secs {
                None => Some(Duration::from_secs(24 * 3600)),
                Some(0) => None,
                Some(s) => Some(Duration::from_secs(s)),
            },
            idle_timeout: match cfg.snowflake_session_idle_timeout_secs {
                None => Some(Duration::from_secs(4 * 3600)),
                Some(0) => None,
                Some(s) => Some(Duration::from_secs(s)),
            },
        }
    }
}

/// Snapshot returned after a successful [`SnowflakeSessionStore::validate_snowflake_session`].
#[derive(Debug, Clone)]
pub struct SnowflakeSessionSnapshot {
    pub auth_ctx: AuthContext,
    pub group: ClusterGroupName,
    pub user: Option<String>,
    pub database: Option<String>,
    pub schema: Option<String>,
}

/// Successful validation: session fields plus optional `validityInSecondsST` for the token response.
#[derive(Debug, Clone)]
pub struct ValidatedSnowflakeSession {
    pub snapshot: SnowflakeSessionSnapshot,
    /// Remaining seconds until **max session age** or **idle timeout**, whichever is sooner.
    /// `None` when both policy limits are disabled (unbounded session).
    pub validity_in_seconds_st: Option<u64>,
}

/// Stores active QueryFlux Snowflake wire-protocol sessions keyed by the qf_token
/// issued to the client at login. **Process-local** — no backend Snowflake account is needed.
pub struct SnowflakeSessionStore {
    sessions: DashMap<String, SnowflakeSession>,
    policy: SnowflakeHttpSessionPolicy,
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
    /// Last successful [`SnowflakeSessionStore::validate_snowflake_session`] (or login).
    pub last_seen: Instant,
}

impl SnowflakeSessionStore {
    pub fn new(policy: SnowflakeHttpSessionPolicy) -> Arc<Self> {
        Arc::new(Self {
            sessions: DashMap::new(),
            policy,
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

    /// Look up the session, enforce [`SnowflakeHttpSessionPolicy`], bump `last_seen` on success,
    /// and remove the entry when expired or missing.
    pub fn validate_snowflake_session(&self, token: &str) -> Option<ValidatedSnowflakeSession> {
        let mut guard = self.sessions.get_mut(token)?;
        let now = Instant::now();

        if let Some(max) = self.policy.max_session_age {
            if guard.created_at.elapsed() > max {
                drop(guard);
                self.sessions.remove(token);
                return None;
            }
        }
        if let Some(idle) = self.policy.idle_timeout {
            if guard.last_seen.elapsed() > idle {
                drop(guard);
                self.sessions.remove(token);
                return None;
            }
        }

        let age_remaining = self
            .policy
            .max_session_age
            .map(|max| max.saturating_sub(guard.created_at.elapsed()).as_secs());
        let idle_remaining = self
            .policy
            .idle_timeout
            .map(|idle| idle.saturating_sub(guard.last_seen.elapsed()).as_secs());
        let validity_in_seconds_st = match (age_remaining, idle_remaining) {
            (Some(a), Some(i)) => Some(a.min(i)),
            (Some(a), None) => Some(a),
            (None, Some(i)) => Some(i),
            (None, None) => None,
        };

        guard.last_seen = now;
        Some(ValidatedSnowflakeSession {
            validity_in_seconds_st,
            snapshot: SnowflakeSessionSnapshot {
                auth_ctx: guard.auth_ctx.clone(),
                group: guard.group.clone(),
                user: guard.user.clone(),
                database: guard.database.clone(),
                schema: guard.schema.clone(),
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use queryflux_core::query::ClusterGroupName;
    use std::thread;

    fn dummy_session(token: &str, created: Instant, last_seen: Instant) -> SnowflakeSession {
        SnowflakeSession {
            qf_token: token.to_string(),
            user: Some("u".into()),
            auth_ctx: AuthContext {
                user: "u".into(),
                groups: vec![],
                roles: vec![],
                raw_token: None,
            },
            group: ClusterGroupName("g".into()),
            database: None,
            schema: None,
            created_at: created,
            last_seen,
        }
    }

    #[test]
    fn validate_updates_last_seen_and_allows_repeated_checks_within_idle() {
        let store = SnowflakeSessionStore::new(SnowflakeHttpSessionPolicy {
            max_session_age: None,
            idle_timeout: Some(Duration::from_secs(3600)),
        });
        let now = Instant::now();
        store.insert("t1".into(), dummy_session("t1", now, now));
        assert!(store.validate_snowflake_session("t1").is_some());
        assert!(store.validate_snowflake_session("t1").is_some());
    }

    #[test]
    fn idle_timeout_evicts_session() {
        let store = SnowflakeSessionStore::new(SnowflakeHttpSessionPolicy {
            max_session_age: None,
            idle_timeout: Some(Duration::from_millis(40)),
        });
        let now = Instant::now();
        store.insert("t2".into(), dummy_session("t2", now, now));
        assert!(store.validate_snowflake_session("t2").is_some());
        thread::sleep(Duration::from_millis(80));
        assert!(store.validate_snowflake_session("t2").is_none());
        assert!(store.get("t2").is_none());
    }

    #[test]
    fn max_session_age_evicts_even_if_recently_touched() {
        let store = SnowflakeSessionStore::new(SnowflakeHttpSessionPolicy {
            max_session_age: Some(Duration::from_millis(40)),
            idle_timeout: Some(Duration::from_secs(3600)),
        });
        let Some(created) = Instant::now().checked_sub(Duration::from_millis(100)) else {
            return;
        };
        store.insert("t3".into(), dummy_session("t3", created, Instant::now()));
        assert!(store.validate_snowflake_session("t3").is_none());
        assert!(store.get("t3").is_none());
    }

    #[test]
    fn validate_reports_remaining_ttl_min_of_age_and_idle() {
        let store = SnowflakeSessionStore::new(SnowflakeHttpSessionPolicy {
            max_session_age: Some(Duration::from_secs(10_000)),
            idle_timeout: Some(Duration::from_secs(100)),
        });
        let now = Instant::now();
        store.insert("t4".into(), dummy_session("t4", now, now));
        let v = store.validate_snowflake_session("t4").unwrap();
        assert!(v.validity_in_seconds_st.unwrap() <= 100);
    }

    #[test]
    fn validate_omits_validity_when_both_limits_disabled() {
        let store = SnowflakeSessionStore::new(SnowflakeHttpSessionPolicy {
            max_session_age: None,
            idle_timeout: None,
        });
        let now = Instant::now();
        store.insert("t5".into(), dummy_session("t5", now, now));
        let v = store.validate_snowflake_session("t5").unwrap();
        assert!(v.validity_in_seconds_st.is_none());
    }
}
