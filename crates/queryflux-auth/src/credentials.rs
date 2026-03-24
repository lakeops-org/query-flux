use serde::{Deserialize, Serialize};

/// Raw credential material extracted from the frontend protocol before any verification.
///
/// Populated by the frontend handler from protocol-specific sources:
/// - `TrinoHttp`:     `Authorization` header → Basic → `username`/`password`, Bearer → `bearer_token`
/// - `PostgresWire`:  startup message `user` field → `username`
/// - `MySqlWire`:     handshake `user` field → `username`
/// - `FlightSQL`:     gRPC metadata `Authorization` Bearer → `bearer_token`
#[derive(Debug, Clone, Default)]
pub struct Credentials {
    pub username: Option<String>,
    pub password: Option<String>,
    /// Raw JWT or opaque token from `Authorization: Bearer <token>`.
    /// Preserved as `raw_token` in `AuthContext` for `tokenExchange` backend mode.
    pub bearer_token: Option<String>,
}

/// Verified identity produced by `AuthProvider::authenticate`.
///
/// This is the canonical subject for all downstream decisions:
/// routing (identity-aware routers), authorization (OpenFGA / allow-lists),
/// audit logs, and backend credential resolution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthContext {
    /// Canonical username. Never empty — `NoneAuthProvider` falls back to `"anonymous"`.
    pub user: String,
    /// Group memberships extracted from the IdP token or LDAP DN.
    #[serde(default)]
    pub groups: Vec<String>,
    /// Roles extracted from the IdP token (e.g. `realm_access.roles` in Keycloak).
    #[serde(default)]
    pub roles: Vec<String>,
    /// The original JWT, kept for `tokenExchange` backend mode.
    /// `None` when using `NoneAuthProvider` or `StaticAuthProvider`.
    pub raw_token: Option<String>,
}

/// Resolved wire credentials for a specific backend query execution.
///
/// Produced by `BackendIdentityResolver` from `(AuthContext, queryAuth config)`.
/// Passed alongside `SessionContext` to adapter methods so adapters know how to
/// authenticate the outgoing request to the backend engine.
///
/// Phase 1: only `ServiceAccount` is produced (no-op — adapters use their own
/// `cluster.auth` config and continue forwarding `SessionContext` headers as today).
/// Phase 4 adds `Impersonate`; Phase 6 adds `Bearer` (token exchange).
#[derive(Debug, Clone)]
pub enum QueryCredentials {
    /// Use the cluster's own service account (Type 1 credentials from `ClusterConfig.auth`).
    ///
    /// The adapter applies `cluster.auth` directly.
    /// For the Trino adapter, `SessionContext::TrinoHttp` headers (including the client's
    /// `Authorization`) are still forwarded unchanged — this is the implicit trino-lb behavior.
    ServiceAccount,

    /// Service account authenticates to the backend; user identity injected via engine header.
    ///
    /// Trino adapter behavior:
    ///   1. Remove client's `Authorization` header from the outgoing request
    ///   2. Apply `cluster.auth` (Type 1) as backend authentication
    ///   3. Set `X-Trino-User: {user}` header
    ///
    /// Only valid for Trino. Startup validation rejects `impersonate` for other engines.
    Impersonate { user: String },

    /// Use a pre-resolved Bearer token (e.g. from OAuth token exchange).
    ///
    /// The adapter sets `Authorization: Bearer <token>` on the outgoing request.
    /// Used by `tokenExchange` mode (Phase 6 — Snowflake, Databricks).
    Bearer { token: String },
}
