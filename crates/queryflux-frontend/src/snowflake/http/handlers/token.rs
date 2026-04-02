use std::sync::Arc;

use crate::state::AppState;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};

use super::common::{extract_snowflake_token, sf_error};

/// POST /session/token-request
///
/// In Design B (protocol bridge), QueryFlux issues its own tokens and manages
/// sessions locally — there are no upstream Snowflake warehouse tokens to renew.
/// This endpoint simply validates the existing session and returns success.
pub async fn token_request(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    let token = match extract_snowflake_token(&headers) {
        Some(t) => t,
        None => {
            return sf_error(
                StatusCode::UNAUTHORIZED,
                390101,
                "Authorization header not found",
            )
        }
    };
    if !state.snowflake_sessions.contains(&token) {
        return sf_error(
            StatusCode::UNAUTHORIZED,
            390104,
            "Session not found or expired",
        );
    }
    (
        StatusCode::OK,
        axum::Json(serde_json::json!({
            "data": {"sessionToken": token, "validityInSecondsST": 3600},
            "success": true,
            "code": null,
            "message": null
        })),
    )
        .into_response()
}
