//! Snowflake Wire Protocol frontend (Form 1) — Design B: protocol bridge.
//!
//! Exposes `routes()` — a stateless `Router<Arc<AppState>>` that can be merged with
//! other route sets and have state injected at the top level.

use std::sync::Arc;

use axum::{
    routing::{delete, get, post},
    Router,
};

use crate::state::AppState;

use handlers::{query, session, token};

pub mod format;
pub mod handlers;
pub mod session_store;

pub fn routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/session/v1/login-request", post(session::login_request))
        .route("/session", delete(session::logout))
        .route("/session/heartbeat", get(session::heartbeat))
        .route("/session/token-request", post(token::token_request))
        .route("/queries/v1/query-request", post(query::query_request))
        .route(
            "/queries/v1/query-monitoring-request",
            get(query::query_monitoring_request),
        )
        .route("/queries/v1/{query_id}", delete(query::cancel_query))
}
