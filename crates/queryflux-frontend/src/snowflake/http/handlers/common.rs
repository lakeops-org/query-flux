use std::collections::HashMap;
use std::io::Read;

use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use flate2::read::GzDecoder;
use serde_json::json;
use serde_json::Value;

/// Snowflake clients (Python connector, JDBC, etc.) send `Content-Encoding: gzip` and gzip the
/// JSON body for most POSTs. Axum does not decompress automatically — decode before `serde_json`.
pub fn decode_snowflake_request_body(headers: &HeaderMap, body: &Bytes) -> Result<Vec<u8>, String> {
    let gzip = headers
        .get("content-encoding")
        .and_then(|v| v.to_str().ok())
        .map(|s| {
            let s = s.trim();
            s.eq_ignore_ascii_case("gzip") || s.eq_ignore_ascii_case("x-gzip")
        })
        .unwrap_or(false);
    if !gzip {
        return Ok(body.to_vec());
    }
    let mut decoder = GzDecoder::new(std::io::Cursor::new(body.as_ref()));
    let mut out = Vec::new();
    decoder
        .read_to_end(&mut out)
        .map_err(|e| format!("gzip decompress: {e}"))?;
    Ok(out)
}

/// Parse JSON from a request body, after optional gzip decompression.
pub fn parse_snowflake_json_body(headers: &HeaderMap, body: &Bytes) -> Result<Value, String> {
    let decoded = decode_snowflake_request_body(headers, body)?;
    serde_json::from_slice(&decoded).map_err(|e| e.to_string())
}

/// Extract the Snowflake token from the Authorization header.
/// Expected format: `Authorization: Snowflake Token="{token}"`
pub fn extract_snowflake_token(headers: &HeaderMap) -> Option<String> {
    let auth = headers.get("authorization")?.to_str().ok()?;
    // Handle both `Snowflake Token="..."` and `Snowflake Token=...`
    let rest = auth.strip_prefix("Snowflake Token=")?;
    let token = rest.trim_matches('"');
    if token.is_empty() {
        None
    } else {
        Some(token.to_string())
    }
}

/// Build a Snowflake-style JSON error response.
///
/// **Always uses HTTP 200** with `success: false` in the body. The official Snowflake Python
/// connector treats many non-2xx status codes as *retryable* during `login-request` (including
/// 400, 403, and all 5xx — see `is_retryable_http_code` in `snowflake.connector.network`).
/// Returning 502/400 for configuration errors caused errno **251012** ("Login request is retryable")
/// and then **250001** after retries. Real Snowflake often responds with 200 + JSON `success: false`.
pub fn sf_error(_status: StatusCode, code: u64, message: &str) -> Response {
    (
        StatusCode::OK,
        axum::Json(json!({
            "data": null,
            "code": code.to_string(),
            "message": message,
            "success": false
        })),
    )
        .into_response()
}

/// Forward a reqwest response as an Axum response, preserving status and headers.
pub fn proxy_response(
    status: reqwest::StatusCode,
    resp_headers: &reqwest::header::HeaderMap,
    body: Bytes,
) -> Response {
    let axum_status =
        StatusCode::from_u16(status.as_u16()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);

    let mut builder = axum::response::Response::builder().status(axum_status);

    for (name, value) in resp_headers {
        // Skip hop-by-hop headers that don't make sense to forward.
        let name_lower = name.as_str().to_lowercase();
        if matches!(
            name_lower.as_str(),
            "transfer-encoding" | "connection" | "keep-alive" | "te" | "trailer" | "upgrade"
        ) {
            continue;
        }
        if let Ok(val_str) = value.to_str() {
            builder = builder.header(name.as_str(), val_str);
        }
    }

    builder
        .body(axum::body::Body::from(body))
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
}

/// Extract non-auth headers to pass through to the warehouse.
pub fn passthrough_headers(headers: &HeaderMap) -> HashMap<String, String> {
    headers
        .iter()
        .filter_map(|(k, v)| {
            let name = k.as_str().to_lowercase();
            // Never forward Authorization — we inject the service-account token ourselves.
            if name == "authorization" || name == "host" || name == "content-length" {
                return None;
            }
            v.to_str()
                .ok()
                .map(|val| (k.as_str().to_string(), val.to_string()))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;
    use flate2::write::GzEncoder;
    use flate2::Compression;
    use std::io::Write;

    #[test]
    fn decodes_gzip_json_body() {
        let json = br#"{"data":{"LOGIN_NAME":"u"}}"#;
        let mut enc = GzEncoder::new(Vec::new(), Compression::default());
        enc.write_all(json).unwrap();
        let gz = enc.finish().unwrap();
        let bytes = Bytes::from(gz);
        let mut headers = HeaderMap::new();
        headers.insert("content-encoding", HeaderValue::from_static("gzip"));
        let out = decode_snowflake_request_body(&headers, &bytes).unwrap();
        assert_eq!(out.as_slice(), json);
    }

    #[test]
    fn passthrough_plain_json_without_gzip_header() {
        let raw = br#"{"a":1}"#;
        let bytes = Bytes::from_static(raw);
        let headers = HeaderMap::new();
        let out = decode_snowflake_request_body(&headers, &bytes).unwrap();
        assert_eq!(out.as_slice(), raw);
    }
}
