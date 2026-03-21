use arrow::array::{
    Array, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array,
    UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use arrow::util::display::{ArrayFormatter, FormatOptions};
use async_trait::async_trait;
use axum::{
    body::Body,
    http::{Response, StatusCode},
};
use serde_json::{json, Value};

use queryflux_core::{error::Result, query::QueryStats};
use queryflux_engine_adapters::trino::api::{TrinoError, TrinoResponse, TrinoStats};

use crate::dispatch::ResultSink;

/// Buffers Arrow RecordBatches and serializes them as a single-page Trino HTTP response.
///
/// Used for Trino HTTP clients routed to non-Trino backends (DuckDB, StarRocks).
/// V1 buffers all rows — acceptable since those engines return results in memory anyway.
/// The Trino→Trino raw-bytes path is completely separate and unaffected.
pub struct TrinoHttpResultSink {
    query_id: String,
    columns: Vec<Value>,
    rows: Vec<Value>,
    error: Option<String>,
    stats: QueryStats,
}

impl TrinoHttpResultSink {
    pub fn new(query_id: &str) -> Self {
        Self {
            query_id: query_id.to_string(),
            columns: Vec::new(),
            rows: Vec::new(),
            error: None,
            stats: QueryStats::default(),
        }
    }

    /// Consume the sink and produce the final HTTP response.
    pub fn into_response(self) -> Response<Body> {
        let resp = if let Some(msg) = self.error {
            TrinoResponse {
                id: self.query_id,
                next_uri: None,
                info_uri: "http://queryflux/ui/query.html".to_string(),
                partial_cancel_uri: None,
                stats: TrinoStats {
                    state: "FAILED".to_string(),
                    queued: false,
                    scheduled: false,
                    elapsed_time_millis: self.stats.execution_duration_ms,
                    ..Default::default()
                },
                error: Some(TrinoError {
                    message: msg,
                    error_code: Some(0),
                    error_name: Some("QUERY_FAILED".to_string()),
                    error_type: Some("USER_ERROR".to_string()),
                    failure_info: None,
                }),
                columns: None,
                data: None,
                update_type: None,
                update_count: None,
                warnings: vec![],
            }
        } else {
            TrinoResponse {
                id: self.query_id,
                next_uri: None,
                info_uri: "http://queryflux/ui/query.html".to_string(),
                partial_cancel_uri: None,
                stats: TrinoStats {
                    state: "FINISHED".to_string(),
                    queued: false,
                    scheduled: true,
                    completed_splits: 1,
                    total_splits: 1,
                    processed_rows: self.stats.rows_returned,
                    processed_bytes: self.stats.bytes_returned.unwrap_or(0),
                    queued_time_millis: self.stats.queue_duration_ms,
                    elapsed_time_millis: self.stats.execution_duration_ms,
                    cpu_time_millis: self.stats.execution_duration_ms,
                    wall_time_millis: self.stats.execution_duration_ms,
                    progress_percentage: Some(100.0),
                    ..Default::default()
                },
                error: None,
                columns: if self.columns.is_empty() {
                    None
                } else {
                    Some(Value::Array(self.columns))
                },
                data: if self.rows.is_empty() {
                    None
                } else {
                    Some(Value::Array(self.rows))
                },
                update_type: None,
                update_count: None,
                warnings: vec![],
            }
        };

        let json = serde_json::to_vec(&resp).unwrap_or_default();
        Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "application/json")
            .body(Body::from(json))
            .unwrap()
    }
}

#[async_trait]
impl ResultSink for TrinoHttpResultSink {
    async fn on_schema(&mut self, schema: &Schema) -> Result<()> {
        self.columns = schema
            .fields()
            .iter()
            .map(|f| {
                let type_name = arrow_type_to_trino_type(f.data_type());
                json!({
                    "name": f.name(),
                    "type": type_name,
                    "typeSignature": { "rawType": type_name, "arguments": [] }
                })
            })
            .collect();
        Ok(())
    }

    async fn on_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        // Fallback formatter for types that don't have a native JSON primitive
        // (timestamps, dates, decimals, complex types).
        let opts = FormatOptions::default().with_null("NULL");
        let formatters: Vec<ArrayFormatter> = batch
            .columns()
            .iter()
            .map(|col| ArrayFormatter::try_new(col.as_ref(), &opts))
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| queryflux_core::error::QueryFluxError::Engine(e.to_string()))?;

        for row in 0..batch.num_rows() {
            let cells: Vec<Value> = (0..batch.num_columns())
                .map(|col_idx| arrow_cell_to_json(batch, col_idx, row, &formatters[col_idx]))
                .collect();
            self.rows.push(Value::Array(cells));
        }
        Ok(())
    }

    async fn on_complete(&mut self, stats: &QueryStats) -> Result<()> {
        self.stats = stats.clone();
        Ok(())
    }

    async fn on_error(&mut self, message: &str) -> Result<()> {
        self.error = Some(message.to_string());
        Ok(())
    }
}

/// Serialize a single Arrow cell as the correct JSON primitive type.
///
/// Trino's wire protocol expects typed JSON — booleans as `true`/`false`,
/// integers and floats as JSON numbers, strings as JSON strings.  Complex
/// types (array, map, struct) and temporal/decimal types fall back to Arrow's
/// display formatting and are sent as JSON strings, which Trino clients can
/// parse using the column type metadata.
fn arrow_cell_to_json(
    batch: &RecordBatch,
    col_idx: usize,
    row: usize,
    formatter: &ArrayFormatter<'_>,
) -> Value {
    let col = batch.column(col_idx);
    if col.is_null(row) {
        return Value::Null;
    }
    match col.data_type() {
        DataType::Boolean => {
            let v = col
                .as_any()
                .downcast_ref::<BooleanArray>()
                .map(|a| a.value(row));
            v.map(Value::Bool)
                .unwrap_or_else(|| Value::String(formatter.value(row).to_string()))
        }
        DataType::Int8 => {
            json!(col
                .as_any()
                .downcast_ref::<Int8Array>()
                .map(|a| a.value(row) as i64))
        }
        DataType::Int16 => {
            json!(col
                .as_any()
                .downcast_ref::<Int16Array>()
                .map(|a| a.value(row) as i64))
        }
        DataType::Int32 => {
            json!(col
                .as_any()
                .downcast_ref::<Int32Array>()
                .map(|a| a.value(row) as i64))
        }
        DataType::Int64 => {
            json!(col
                .as_any()
                .downcast_ref::<Int64Array>()
                .map(|a| a.value(row)))
        }
        DataType::UInt8 => {
            json!(col
                .as_any()
                .downcast_ref::<UInt8Array>()
                .map(|a| a.value(row) as i64))
        }
        DataType::UInt16 => {
            json!(col
                .as_any()
                .downcast_ref::<UInt16Array>()
                .map(|a| a.value(row) as i64))
        }
        DataType::UInt32 => {
            json!(col
                .as_any()
                .downcast_ref::<UInt32Array>()
                .map(|a| a.value(row) as i64))
        }
        DataType::UInt64 => {
            // UInt64 may exceed i64::MAX; try i64 first, fall back to string.
            if let Some(a) = col.as_any().downcast_ref::<UInt64Array>() {
                let v = a.value(row);
                if v <= i64::MAX as u64 {
                    json!(v as i64)
                } else {
                    Value::String(v.to_string())
                }
            } else {
                Value::String(formatter.value(row).to_string())
            }
        }
        DataType::Float32 => {
            // Widen to f64 so serde_json can represent it without NaN/Inf issues.
            json!(col
                .as_any()
                .downcast_ref::<Float32Array>()
                .map(|a| a.value(row) as f64))
        }
        DataType::Float64 => {
            json!(col
                .as_any()
                .downcast_ref::<Float64Array>()
                .map(|a| a.value(row)))
        }
        // Strings, dates, timestamps, decimals, complex types → display string.
        _ => Value::String(formatter.value(row).to_string()),
    }
}

/// Map Arrow DataType to Trino type name string.
fn arrow_type_to_trino_type(dt: &DataType) -> String {
    match dt {
        DataType::Boolean => "boolean".to_string(),
        DataType::Int8 => "tinyint".to_string(),
        DataType::Int16 => "smallint".to_string(),
        DataType::Int32 => "integer".to_string(),
        DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => "bigint".to_string(),
        DataType::Float16 | DataType::Float32 => "real".to_string(),
        DataType::Float64 => "double".to_string(),
        DataType::Utf8 | DataType::LargeUtf8 => "varchar".to_string(),
        DataType::Binary | DataType::LargeBinary => "varbinary".to_string(),
        DataType::Date32 | DataType::Date64 => "date".to_string(),
        DataType::Timestamp(_, _) => "timestamp(3)".to_string(),
        DataType::Decimal128(p, s) => format!("decimal({p},{s})"),
        DataType::List(f) | DataType::LargeList(f) => {
            format!("array({})", arrow_type_to_trino_type(f.data_type()))
        }
        DataType::Map(f, _) => {
            if let DataType::Struct(fields) = f.data_type() {
                if fields.len() == 2 {
                    let k = arrow_type_to_trino_type(fields[0].data_type());
                    let v = arrow_type_to_trino_type(fields[1].data_type());
                    return format!("map({k},{v})");
                }
            }
            "map(varchar,varchar)".to_string()
        }
        DataType::Struct(fields) => {
            let inner: Vec<_> = fields
                .iter()
                .map(|f| format!("{} {}", f.name(), arrow_type_to_trino_type(f.data_type())))
                .collect();
            format!("row({})", inner.join(","))
        }
        _ => "varchar".to_string(),
    }
}
