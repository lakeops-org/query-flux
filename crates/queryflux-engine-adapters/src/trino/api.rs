use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;

/// Deserialize `null` or a missing optional wrapper as `T::default()` (see `TrinoError.failure_info`).
fn deserialize_null_as_default<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    T: Default + Deserialize<'de>,
    D: Deserializer<'de>,
{
    let opt = Option::<T>::deserialize(deserializer)?;
    Ok(opt.unwrap_or_default())
}

/// Trino's query response JSON structure.
/// We only parse the fields we need; everything else passes through as raw JSON.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TrinoResponse {
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_uri: Option<String>,
    pub info_uri: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partial_cancel_uri: Option<String>,
    pub stats: TrinoStats,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<TrinoError>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub columns: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub update_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub update_count: Option<u64>,
    // `trino-rust-client` expects `warnings` to always be present.
    #[serde(default)]
    pub warnings: Vec<Value>,
}

impl TrinoResponse {
    /// Whether this response represents the final result (no more polling needed).
    pub fn is_final(&self) -> bool {
        self.next_uri.is_none()
    }

    /// Rewrite nextUri to point to QueryFlux instead of the backend Trino cluster.
    pub fn with_next_uri(mut self, next_uri: Option<String>) -> Self {
        self.next_uri = next_uri;
        self
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TrinoStats {
    pub state: String,
    #[serde(default)]
    pub queued: bool,
    #[serde(default)]
    pub scheduled: bool,
    /// Trino's query `nodes` count (required by `trino-rust-client`).
    #[serde(default)]
    pub nodes: u32,
    #[serde(default)]
    pub running_drivers: u32,
    #[serde(default)]
    pub completed_splits: u32,
    #[serde(default)]
    pub total_splits: u32,
    #[serde(default)]
    pub queued_splits: u32,
    #[serde(default)]
    pub running_splits: u32,
    #[serde(default)]
    pub processed_rows: u64,
    #[serde(default)]
    pub processed_bytes: u64,
    #[serde(default)]
    pub queued_time_millis: u64,
    #[serde(default)]
    pub elapsed_time_millis: u64,
    #[serde(default)]
    pub cpu_time_millis: u64,
    #[serde(default)]
    pub wall_time_millis: u64,
    #[serde(default)]
    pub physical_input_bytes: u64,
    #[serde(default)]
    pub peak_memory_bytes: u64,
    #[serde(default)]
    pub spilled_bytes: u64,
    #[serde(default)]
    pub progress_percentage: Option<f32>,
}

/// Subset of Trino's `failureInfo` JSON; shape matches `trino-rust-client::FailureInfo` for clients.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TrinoFailureInfo {
    #[serde(rename = "type")]
    pub ty: String,
    #[serde(default)]
    pub suppressed: Vec<TrinoFailureInfo>,
    #[serde(default)]
    pub stack: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cause: Option<Box<TrinoFailureInfo>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_location: Option<TrinoErrorLocation>,
}

impl Default for TrinoFailureInfo {
    fn default() -> Self {
        Self {
            ty: "io.trino.spi.TrinoException".to_string(),
            suppressed: vec![],
            stack: vec![],
            message: None,
            cause: None,
            error_location: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TrinoErrorLocation {
    pub line_number: u32,
    pub column_number: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TrinoError {
    pub message: String,
    pub error_code: Option<i32>,
    pub error_name: Option<String>,
    pub error_type: Option<String>,
    #[serde(default, deserialize_with = "deserialize_null_as_default")]
    pub failure_info: TrinoFailureInfo,
}

/// Synthetic queued response returned to the client when QueryFlux has no cluster available.
pub fn queued_response(query_id: &str, elapsed_ms: u64, next_uri: String) -> TrinoResponse {
    TrinoResponse {
        id: query_id.to_string(),
        next_uri: Some(next_uri),
        info_uri: format!("http://queryflux/ui/query.html?{query_id}"),
        partial_cancel_uri: None,
        stats: TrinoStats {
            state: "QUEUED".to_string(),
            queued: true,
            scheduled: false,
            nodes: 0,
            running_drivers: 0,
            completed_splits: 0,
            total_splits: 0,
            queued_splits: 0,
            running_splits: 0,
            processed_rows: 0,
            processed_bytes: 0,
            physical_input_bytes: 0,
            peak_memory_bytes: 0,
            spilled_bytes: 0,
            queued_time_millis: elapsed_ms,
            elapsed_time_millis: elapsed_ms,
            cpu_time_millis: 0,
            wall_time_millis: 0,
            progress_percentage: None,
        },
        error: None,
        columns: None,
        data: None,
        update_type: None,
        update_count: None,
        warnings: vec![],
    }
}
