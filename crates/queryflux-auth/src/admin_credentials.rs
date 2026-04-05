//! Admin API credential management.
//!
//! # Credential priority (highest → lowest)
//!
//! 1. **Database override** — a bcrypt hash stored under the `"admin_credentials"`
//!    key in `ProxySettingsStore`. Written on the first successful password change
//!    via the web UI. Once present, YAML/env credentials are ignored.
//! 2. **Bootstrap credentials** — plain-text username/password from the YAML config
//!    or `QUERYFLUX_ADMIN_USER` / `QUERYFLUX_ADMIN_PASSWORD` environment variables.
//!    Used until the operator changes the password via the web UI.
//!
//! # Persistence note
//! Password changes require a `ProxySettingsStore` (Postgres). Without Postgres,
//! `change_password` returns an error.

use std::sync::Arc;

use queryflux_core::error::{QueryFluxError, Result};
use queryflux_persistence::ProxySettingsStore;
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

const SETTINGS_KEY: &str = "admin_credentials";
const BCRYPT_COST: u32 = 12;

/// Stored via `ProxySettingsStore` under key `"admin_credentials"` (in Postgres,
/// nested under the `security_settings.config` JSON alongside Studio security config).
#[derive(Debug, Serialize, Deserialize)]
struct StoredCredentials {
    username: String,
    password_hash: String,
}

/// Manages admin API credentials with DB-override semantics.
///
/// Thread-safe; wrap in `Arc` and share across the Axum state.
pub struct AdminCredentialsManager {
    bootstrap_username: String,
    bootstrap_password: String,
    store: Option<Arc<dyn ProxySettingsStore>>,
}

impl AdminCredentialsManager {
    pub fn new(
        bootstrap_username: String,
        bootstrap_password: String,
        store: Option<Arc<dyn ProxySettingsStore>>,
    ) -> Self {
        Self {
            bootstrap_username,
            bootstrap_password,
            store,
        }
    }

    /// Returns `true` when the DB contains an overriding credential record.
    pub async fn has_db_override(&self) -> bool {
        let Some(store) = &self.store else {
            return false;
        };
        match store.get_proxy_setting(SETTINGS_KEY).await {
            Ok(Some(_)) => true,
            Ok(None) => false,
            Err(e) => {
                warn!(error = %e, "has_db_override: could not read admin credentials from store");
                false
            }
        }
    }

    /// Validate `username` + `password` against the active credentials.
    ///
    /// - If a DB record exists: checks bcrypt hash, ignores bootstrap creds.
    /// - Otherwise: plain-text comparison against bootstrap creds.
    pub async fn verify(&self, username: &str, password: &str) -> bool {
        match self.load_db_credentials().await {
            Ok(Some(stored)) => {
                if username != stored.username {
                    return false;
                }
                tokio::task::spawn_blocking({
                    let hash = stored.password_hash.clone();
                    let pw = password.to_string();
                    move || bcrypt::verify(&pw, &hash).unwrap_or(false)
                })
                .await
                .unwrap_or(false)
            }
            Ok(None) => username == self.bootstrap_username && password == self.bootstrap_password,
            Err(e) => {
                warn!(
                    error = %e,
                    "admin credentials could not be loaded from store; denying verify"
                );
                false
            }
        }
    }

    /// Change the admin password.
    ///
    /// Validates `current_password` first, then stores a bcrypt hash of
    /// `new_password` in the DB. After this call `has_db_override()` returns `true`
    /// and the bootstrap credentials are no longer used.
    ///
    /// Returns an error if:
    /// - `current_password` is wrong.
    /// - `new_password` is shorter than 8 characters.
    /// - No persistent store is configured (password cannot be saved).
    pub async fn change_password(&self, current_password: &str, new_password: &str) -> Result<()> {
        let store = self.store.as_ref().ok_or_else(|| {
            QueryFluxError::Auth("password change requires persistent storage".to_string())
        })?;

        let stored_opt = self.load_db_credentials().await?;
        let username = stored_opt
            .as_ref()
            .map(|s| s.username.clone())
            .unwrap_or_else(|| self.bootstrap_username.clone());

        if !self.verify(&username, current_password).await {
            return Err(QueryFluxError::Auth(
                "current password is incorrect".to_string(),
            ));
        }

        if new_password.len() < 8 {
            return Err(QueryFluxError::Auth(
                "new password must be at least 8 characters".to_string(),
            ));
        }

        let hash = tokio::task::spawn_blocking({
            let pw = new_password.to_string();
            move || bcrypt::hash(&pw, BCRYPT_COST)
        })
        .await
        .map_err(|e| QueryFluxError::Auth(format!("bcrypt task panicked: {e}")))?
        .map_err(|e| QueryFluxError::Auth(format!("bcrypt hash failed: {e}")))?;

        let stored = StoredCredentials {
            username: username.clone(),
            password_hash: hash,
        };
        let value = serde_json::to_value(&stored)
            .map_err(|e| QueryFluxError::Auth(format!("serialize credentials: {e}")))?;

        store.set_proxy_setting(SETTINGS_KEY, value).await?;
        info!(username, "Admin password changed and stored in DB");

        Ok(())
    }

    // ---------------------------------------------------------------------------
    // Private helpers
    // ---------------------------------------------------------------------------

    async fn load_db_credentials(&self) -> Result<Option<StoredCredentials>> {
        let Some(store) = self.store.as_ref() else {
            return Ok(None);
        };
        let value = store.get_proxy_setting(SETTINGS_KEY).await?;
        let Some(value) = value else {
            return Ok(None);
        };
        serde_json::from_value(value)
            .map_err(|e| {
                QueryFluxError::Auth(format!(
                    "invalid stored admin credentials at {SETTINGS_KEY}: {e}"
                ))
            })
            .map(Some)
    }
}
