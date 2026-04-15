//! Snowflake SQL REST API v2 frontend (Form 2) — Design B: protocol bridge.
//!
//! Exposes `routes()` — a stateless `Router<Arc<AppState>>`.

use std::sync::Arc;

use axum::{
    routing::{get, post},
    Router,
};

use crate::state::AppState;

pub mod handlers;

pub fn routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/api/v2/statements", post(handlers::submit_statement))
        .route(
            "/api/v2/statements/{handle}",
            get(handlers::get_statement).delete(handlers::cancel_statement),
        )
}
