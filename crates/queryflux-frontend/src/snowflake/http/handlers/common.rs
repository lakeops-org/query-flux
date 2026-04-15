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
