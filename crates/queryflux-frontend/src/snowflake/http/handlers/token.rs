use std::sync::Arc;

use crate::state::AppState;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use serde_json::json;

use super::common::{extract_snowflake_token, sf_error};

/// POST /session/token-request
///
/// In Design B (protocol bridge), QueryFlux issues its own tokens and manages
/// sessions locally — there are no upstream Snowflake warehouse tokens to renew.
/// This endpoint validates the session and returns `validityInSecondsST` as the remaining
/// seconds until max session age or idle timeout (see `SnowflakeHttpSessionPolicy`), omitting
/// that field when both limits are disabled.
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
    let validated = match state.snowflake_sessions.validate_snowflake_session(&token) {
        Some(v) => v,
        None => {
            return sf_error(
                StatusCode::UNAUTHORIZED,
                390104,
                "Session not found or expired",
            );
        }
    };

    let data = if let Some(secs) = validated.validity_in_seconds_st {
        json!({
            "sessionToken": token,
            "validityInSecondsST": secs,
        })
    } else {
        json!({
            "sessionToken": token,
        })
    };

    (
        StatusCode::OK,
        axum::Json(json!({
            "data": data,
            "success": true,
            "code": null,
            "message": null
        })),
    )
        .into_response()
}
