//! Minimal Snowflake HTTP client for e2e tests.
//!
//! Covers the two endpoints exercised by query-params tests:
//!   POST /session/v1/login-request   → returns a session token
//!   POST /queries/v1/query-request   → executes SQL with optional parameterBindings
//!
//! Results are decoded from the `rowsetBase64` field (Arrow IPC stream, base64-encoded).

use anyhow::{anyhow, Result};
use arrow::array::Array;
use arrow::ipc::reader::StreamReader;
use base64::Engine as _;
use reqwest::Client;
use serde_json::{json, Value};

pub struct SnowflakeClient {
    client: Client,
    base_url: String,
}

/// Decoded query result from a Snowflake HTTP response.
pub struct SfQueryResult {
    /// `true` when the query succeeded.
    pub success: bool,
    /// Error message when `success` is false.
    pub error: Option<String>,
    /// Total number of rows returned.
    pub total_rows: u64,
    /// All values as strings, row-major: `rows[row][col]`.
    /// `None` represents a SQL NULL.
    pub rows: Vec<Vec<Option<String>>>,
}

impl SnowflakeClient {
    pub fn new(base_url: &str) -> Self {
        Self {
            client: Client::builder()
                .timeout(std::time::Duration::from_secs(30))
                .build()
                .expect("build reqwest client"),
            base_url: base_url.trim_end_matches('/').to_string(),
        }
    }

    /// Authenticate and return a session token. Uses `NoneAuthProvider` (no real credentials).
    pub async fn login(&self) -> Result<String> {
        let body = json!({
            "data": {
                "LOGIN_NAME": "test",
                "PASSWORD": "",
                "CLIENT_APP_ID": "qf-e2e-test",
                "CLIENT_APP_VERSION": "1.0"
            }
        });

        let resp = self
            .client
            .post(format!("{}/session/v1/login-request", self.base_url))
            .json(&body)
            .send()
            .await?
            .json::<Value>()
            .await?;

        let success = resp["success"].as_bool().unwrap_or(false);
        if !success {
            return Err(anyhow!("Snowflake login failed: {resp}"));
        }
        resp["data"]["token"]
            .as_str()
            .map(|s| s.to_string())
            .ok_or_else(|| anyhow!("Login response missing data.token: {resp}"))
    }

    /// Execute SQL with optional Snowflake-style parameter bindings.
    ///
    /// `bindings` format mirrors the Snowflake wire protocol:
    /// ```json
    /// { "1": {"type": "FIXED", "value": "42"} }
    /// ```
    pub async fn query(
        &self,
        token: &str,
        sql: &str,
        bindings: Option<Value>,
    ) -> Result<SfQueryResult> {
        let mut body = json!({"sqlText": sql});
        if let Some(b) = bindings {
            body["parameterBindings"] = b;
        }

        let resp = self
            .client
            .post(format!("{}/queries/v1/query-request", self.base_url))
            .header("Authorization", format!("Snowflake Token=\"{token}\""))
            .json(&body)
            .send()
            .await?
            .json::<Value>()
            .await?;

        let success = resp["success"].as_bool().unwrap_or(false);
        if !success {
            return Ok(SfQueryResult {
                success: false,
                error: resp["message"].as_str().map(|s| s.to_string()),
                total_rows: 0,
                rows: vec![],
            });
        }

        let data = &resp["data"];
        let total_rows = data["total"].as_u64().unwrap_or(0);
        let rows = if let Some(b64) = data["rowsetBase64"].as_str() {
            decode_rowset(b64)?
        } else {
            vec![]
        };

        Ok(SfQueryResult {
            success: true,
            error: None,
            total_rows,
            rows,
        })
    }
}

/// Decode a base64-encoded Arrow IPC stream into row-major string values.
fn decode_rowset(b64: &str) -> Result<Vec<Vec<Option<String>>>> {
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(b64)
        .map_err(|e| anyhow!("base64 decode failed: {e}"))?;

    let cursor = std::io::Cursor::new(bytes);
    let reader =
        StreamReader::try_new(cursor, None).map_err(|e| anyhow!("Arrow IPC read failed: {e}"))?;

    let mut all_rows: Vec<Vec<Option<String>>> = vec![];

    for batch_result in reader {
        let batch = batch_result.map_err(|e| anyhow!("Arrow batch read failed: {e}"))?;
        let num_rows = batch.num_rows();
        let num_cols = batch.num_columns();

        for row_idx in 0..num_rows {
            let mut row = Vec::with_capacity(num_cols);
            for col_idx in 0..num_cols {
                let col = batch.column(col_idx);
                let cell = if col.is_null(row_idx) {
                    None
                } else {
                    Some(array_value_to_string(col.as_ref(), row_idx))
                };
                row.push(cell);
            }
            all_rows.push(row);
        }
    }

    Ok(all_rows)
}

/// Convert a single Arrow array cell to a display string.
fn array_value_to_string(array: &dyn arrow::array::Array, row: usize) -> String {
    use arrow::array::*;

    macro_rules! downcast_to_string {
        ($array:expr, $row:expr, $($t:ty => $arr:ty),+) => {
            $(
                if let Some(a) = $array.as_any().downcast_ref::<$arr>() {
                    return a.value($row).to_string();
                }
            )+
        };
    }

    downcast_to_string!(array, row,
        i8   => Int8Array,
        i16  => Int16Array,
        i32  => Int32Array,
        i64  => Int64Array,
        u8   => UInt8Array,
        u16  => UInt16Array,
        u32  => UInt32Array,
        u64  => UInt64Array,
        f32  => Float32Array,
        f64  => Float64Array,
        bool => BooleanArray
    );

    if let Some(a) = array.as_any().downcast_ref::<StringArray>() {
        return a.value(row).to_string();
    }
    if let Some(a) = array.as_any().downcast_ref::<LargeStringArray>() {
        return a.value(row).to_string();
    }
    if let Some(a) = array.as_any().downcast_ref::<BinaryArray>() {
        return format!("{:?}", a.value(row));
    }

    // Fallback: use the arrow display utility
    arrow::util::display::array_value_to_string(array, row).unwrap_or_else(|_| "<?>".to_string())
}
