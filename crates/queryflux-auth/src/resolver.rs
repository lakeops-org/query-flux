//! `BackendIdentityResolver` — maps `(AuthContext, ClusterConfig)` → `QueryCredentials`.
//!
//! Called once per query, after cluster selection, before the adapter submits the query.
//!
//! Resolution rules:
//! - `auth_ctx.user == "anonymous"` or no `queryAuth` config → `ServiceAccount`
//! - `queryAuth: serviceAccount`   → `ServiceAccount`
//! - `queryAuth: impersonate`      → `Impersonate { user }`
//! - `queryAuth: tokenExchange`    → RFC 8693 token exchange → `Bearer { token }`
//!   - Falls back to `ServiceAccount` when `raw_token` is absent.
//!   - Exchanged tokens are cached per (user, token_endpoint) until expiry − 30 s.

use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use queryflux_core::config::{ClusterConfig, QueryAuthConfig, TokenExchangeConfig};
use queryflux_core::error::{QueryFluxError, Result};
use tracing::{debug, warn};

use crate::credentials::{AuthContext, QueryCredentials};

/// Drop cached exchanged tokens this long before `expires_at` (same idea as OpenFGA client cache).
const TOKEN_EXCHANGE_CACHE_BUFFER: Duration = Duration::from_secs(30);

// ---------------------------------------------------------------------------
// BackendIdentityResolver
// ---------------------------------------------------------------------------

pub struct BackendIdentityResolver {
    http_client: reqwest::Client,
    /// Cache key: (username, token_endpoint) → (exchanged_token, expires_at).
    token_cache: Arc<DashMap<(String, String), (String, Instant)>>,
}

impl BackendIdentityResolver {
    pub fn new() -> Self {
        Self {
            http_client: reqwest::Client::new(),
            token_cache: Arc::new(DashMap::new()),
        }
    }

    /// Resolve the `QueryCredentials` to use for this query.
    ///
    /// `cluster_cfg` is `None` when the cluster name is not in the config map
    /// (e.g. dynamically registered clusters) — falls back to `ServiceAccount`.
    pub async fn resolve(
        &self,
        auth_ctx: &AuthContext,
        cluster_cfg: Option<&ClusterConfig>,
    ) -> QueryCredentials {
        // Anonymous identity → always service account regardless of cluster config.
        if auth_ctx.user == "anonymous" {
            return QueryCredentials::ServiceAccount;
        }

        match cluster_cfg.and_then(|c| c.query_auth.as_ref()) {
            None | Some(QueryAuthConfig::ServiceAccount) => QueryCredentials::ServiceAccount,

            Some(QueryAuthConfig::Impersonate) => QueryCredentials::Impersonate {
                user: auth_ctx.user.clone(),
            },

            Some(QueryAuthConfig::TokenExchange(cfg)) => {
                match self.exchange_token(auth_ctx, cfg).await {
                    Ok(token) => QueryCredentials::Bearer { token },
                    Err(e) => {
                        warn!(
                            user = %auth_ctx.user,
                            error = %e,
                            "tokenExchange failed — falling back to serviceAccount"
                        );
                        QueryCredentials::ServiceAccount
                    }
                }
            }
        }
    }

    async fn exchange_token(
        &self,
        auth_ctx: &AuthContext,
        cfg: &TokenExchangeConfig,
    ) -> Result<String> {
        let cache_key = (auth_ctx.user.clone(), cfg.token_endpoint.clone());

        // Fast path: return cached token if still more than the buffer before expiry.
        // `expires_at` is in the future — do not call `expires_at.elapsed()` (`now - expires_at` panics).
        if let Some(entry) = self.token_cache.get(&cache_key) {
            let (token, expires_at) = entry.value();
            if Instant::now() + TOKEN_EXCHANGE_CACHE_BUFFER < *expires_at {
                debug!(user = %auth_ctx.user, "tokenExchange: using cached token");
                return Ok(token.clone());
            }
        }

        let raw_token = auth_ctx.raw_token.as_deref().ok_or_else(|| {
            QueryFluxError::Auth(
                "tokenExchange requires a bearer token (use OidcAuthProvider on the frontend)"
                    .into(),
            )
        })?;

        debug!(user = %auth_ctx.user, endpoint = %cfg.token_endpoint, "tokenExchange: exchanging token");

        // RFC 8693 token exchange request.
        let mut params = vec![
            (
                "grant_type",
                "urn:ietf:params:oauth:grant-type:token-exchange",
            ),
            ("subject_token", raw_token),
            (
                "subject_token_type",
                "urn:ietf:params:oauth:token-type:access_token",
            ),
            ("client_id", cfg.client_id.as_str()),
            ("client_secret", cfg.client_secret.as_str()),
        ];
        // Borrow from Option<String> so the lifetime is tied to cfg.
        if let Some(aud) = &cfg.target_audience {
            params.push(("audience", aud.as_str()));
        }
        if let Some(scope) = &cfg.scope {
            params.push(("scope", scope.as_str()));
        }

        let resp = self
            .http_client
            .post(&cfg.token_endpoint)
            .form(&params)
            .send()
            .await
            .map_err(|e| QueryFluxError::Auth(format!("tokenExchange HTTP error: {e}")))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(QueryFluxError::Auth(format!(
                "tokenExchange: server returned {status}: {body}"
            )));
        }

        let body: serde_json::Value = resp.json().await.map_err(|e| {
            QueryFluxError::Auth(format!("tokenExchange: failed to parse response: {e}"))
        })?;

        let token = body
            .get("access_token")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                QueryFluxError::Auth("tokenExchange: response missing 'access_token'".into())
            })?
            .to_string();

        let expires_in = body
            .get("expires_in")
            .and_then(|v| v.as_u64())
            .unwrap_or(300);
        let expires_at = Instant::now() + Duration::from_secs(expires_in);

        self.token_cache
            .insert(cache_key, (token.clone(), expires_at));

        Ok(token)
    }
}

impl Default for BackendIdentityResolver {
    fn default() -> Self {
        Self::new()
    }
}
