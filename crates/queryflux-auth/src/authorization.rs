use std::collections::HashMap;

use async_trait::async_trait;
use queryflux_core::config::{ClusterGroupAuthorizationConfig, OpenFgaConfig, OpenFgaCredentials};
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use crate::credentials::AuthContext;

/// Checks whether an authenticated subject may execute queries against a cluster group.
///
/// Implementations:
/// - `AllowAllAuthorization`      — default; permits everything (Phase 1 / no config)
/// - `SimpleAuthorizationPolicy`  — reads `allowGroups`/`allowUsers` from config (Phase 3)
/// - `OpenFgaAuthorizationClient` — Zanzibar-style fine-grained authz (Phase 3)
#[async_trait]
pub trait AuthorizationChecker: Send + Sync {
    /// Returns `true` if `auth_ctx.user` (and/or their groups) may access `group`.
    async fn check(&self, auth_ctx: &AuthContext, group: &str) -> bool;
}

// ---------------------------------------------------------------------------
// AllowAllAuthorization
// ---------------------------------------------------------------------------

/// Permits all requests. Used when no `authorization` block is configured.
///
/// This preserves today's behavior: anyone who can reach the gateway may run queries.
/// Operators opt in to access control by configuring `authorization.provider`.
pub struct AllowAllAuthorization;

#[async_trait]
impl AuthorizationChecker for AllowAllAuthorization {
    async fn check(&self, _auth_ctx: &AuthContext, _group: &str) -> bool {
        true
    }
}

// ---------------------------------------------------------------------------
// SimpleAuthorizationPolicy
// ---------------------------------------------------------------------------

/// Allow-list authorization backed by `allowGroups`/`allowUsers` per cluster group.
///
/// Used when `authorization.provider: none` (no OpenFGA dependency).
///
/// Rules per cluster group:
/// - If both `allowGroups` and `allowUsers` are empty → allow all (open group).
/// - Otherwise: allow if `auth_ctx.user` is in `allowUsers` OR any of `auth_ctx.groups`
///   intersects `allowGroups`.
///
/// Unknown groups (not in the policy map) default to allow-all — this preserves
/// backward compatibility when groups are added to config without an authorization block.
pub struct SimpleAuthorizationPolicy {
    /// cluster_group_name → authorization config
    policies: HashMap<String, ClusterGroupAuthorizationConfig>,
}

impl SimpleAuthorizationPolicy {
    pub fn new(policies: HashMap<String, ClusterGroupAuthorizationConfig>) -> Self {
        Self { policies }
    }
}

#[async_trait]
impl AuthorizationChecker for SimpleAuthorizationPolicy {
    async fn check(&self, auth_ctx: &AuthContext, group: &str) -> bool {
        let Some(policy) = self.policies.get(group) else {
            // Group not in policy map — allow-all (backward compat).
            return true;
        };

        // Both lists empty → open group.
        if policy.allow_groups.is_empty() && policy.allow_users.is_empty() {
            return true;
        }

        // Username match.
        if policy.allow_users.contains(&auth_ctx.user) {
            debug!(user = %auth_ctx.user, group, "SimplePolicy: user allowed by allowUsers");
            return true;
        }

        // Group membership match.
        for g in &auth_ctx.groups {
            if policy.allow_groups.contains(g) {
                debug!(user = %auth_ctx.user, group, matched_group = %g, "SimplePolicy: user allowed by allowGroups");
                return true;
            }
        }

        warn!(user = %auth_ctx.user, group, "SimplePolicy: access denied");
        false
    }
}

// ---------------------------------------------------------------------------
// OpenFgaAuthorizationClient
// ---------------------------------------------------------------------------

/// OpenFGA Zanzibar-style fine-grained authorization.
///
/// Issues a `/stores/{store_id}/check` request for every `check()` call with:
///   user:    `user:<auth_ctx.user>`
///   relation: `reader`
///   object:  `cluster_group:<group>`
///
/// Credentials:
/// - `api_key`: adds `Authorization: Bearer <key>` header
/// - `client_credentials`: exchanges client_id/secret for an OAuth access token,
///   then uses it as Bearer (token cached until expiry - 30s)
///
/// On any HTTP error or unreachable OpenFGA, **denies** access and logs a warning.
/// Operators should ensure OpenFGA is highly available; a sidecar pattern is recommended.
pub struct OpenFgaAuthorizationClient {
    config: OpenFgaConfig,
    http_client: reqwest::Client,
    /// Cached OAuth token for client_credentials flow: (token, expires_at).
    token_cache: tokio::sync::Mutex<Option<(String, std::time::Instant)>>,
}

impl OpenFgaAuthorizationClient {
    pub fn new(config: OpenFgaConfig) -> Self {
        Self {
            config,
            http_client: reqwest::Client::new(),
            token_cache: tokio::sync::Mutex::new(None),
        }
    }

    async fn bearer_token(&self) -> Option<String> {
        match &self.config.credentials {
            None => None,
            Some(OpenFgaCredentials::ApiKey { api_key }) => Some(api_key.clone()),
            Some(OpenFgaCredentials::ClientCredentials {
                client_id,
                client_secret,
                token_endpoint,
            }) => {
                // Check cache first (with 30s buffer before expiry).
                {
                    let guard = self.token_cache.lock().await;
                    if let Some((token, expires_at)) = guard.as_ref() {
                        if expires_at.elapsed().as_secs() < 30 {
                            return Some(token.clone());
                        }
                    }
                }

                // Exchange client credentials for a token.
                let resp = self
                    .http_client
                    .post(token_endpoint)
                    .form(&[
                        ("grant_type", "client_credentials"),
                        ("client_id", client_id),
                        ("client_secret", client_secret),
                    ])
                    .send()
                    .await
                    .ok()?;

                let body: serde_json::Value = resp.json().await.ok()?;
                let token = body.get("access_token")?.as_str()?.to_string();
                let expires_in = body
                    .get("expires_in")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(300);

                let expires_at =
                    std::time::Instant::now() + std::time::Duration::from_secs(expires_in);
                *self.token_cache.lock().await = Some((token.clone(), expires_at));
                Some(token)
            }
        }
    }
}

/// OpenFGA check request body.
#[derive(Serialize)]
struct CheckRequest {
    tuple_key: TupleKey,
}

#[derive(Serialize)]
struct TupleKey {
    user: String,
    relation: String,
    object: String,
}

/// OpenFGA check response body.
#[derive(Deserialize)]
struct CheckResponse {
    allowed: bool,
}

#[async_trait]
impl AuthorizationChecker for OpenFgaAuthorizationClient {
    async fn check(&self, auth_ctx: &AuthContext, group: &str) -> bool {
        let url = format!(
            "{}/stores/{}/check",
            self.config.url.trim_end_matches('/'),
            self.config.store_id,
        );

        let body = CheckRequest {
            tuple_key: TupleKey {
                user: format!("user:{}", auth_ctx.user),
                relation: "reader".to_string(),
                object: format!("cluster_group:{group}"),
            },
        };

        let mut req = self.http_client.post(&url).json(&body);
        if let Some(token) = self.bearer_token().await {
            req = req.bearer_auth(token);
        }

        match req.send().await {
            Ok(resp) if resp.status().is_success() => match resp.json::<CheckResponse>().await {
                Ok(r) => {
                    if !r.allowed {
                        warn!(user = %auth_ctx.user, group, "OpenFGA: access denied");
                    }
                    r.allowed
                }
                Err(e) => {
                    warn!(error = %e, "OpenFGA: failed to parse check response — denying");
                    false
                }
            },
            Ok(resp) => {
                warn!(status = %resp.status(), user = %auth_ctx.user, group, "OpenFGA: check returned error status — denying");
                false
            }
            Err(e) => {
                warn!(error = %e, "OpenFGA: check request failed — denying");
                false
            }
        }
    }
}
