/// Serialization helpers: convert engine-agnostic query results into Trino wire format.
///
/// This is the only place in the codebase that knows how to speak Trino JSON to a client.
/// Engine adapters return generic `QueryPollResult::Complete`; this module formats it.
use queryflux_core::query::{ColumnDef, QueryStats, QueryValue};
use queryflux_engine_adapters::trino::api::{TrinoResponse, TrinoStats};
use serde_json::{json, Value};

/// Build a finished Trino HTTP response from a sync engine result (DuckDB, etc.).
/// Returns all columns and data in a single response with no `nextUri`.
pub fn complete_as_trino_response(
    query_id: &str,
    columns: &[ColumnDef],
    data: &[Vec<QueryValue>],
    stats: &QueryStats,
) -> TrinoResponse {
    let trino_columns: Vec<Value> = columns
        .iter()
        .map(|c| {
            let type_name = to_trino_type(&c.data_type);
            json!({
                "name": c.name,
                "type": type_name,
                "typeSignature": { "rawType": type_name, "arguments": [] }
            })
        })
        .collect();

    let trino_data: Vec<Value> = data
        .iter()
        .map(|row| {
            Value::Array(
                row.iter()
                    .map(|v| match v {
                        QueryValue::Null => Value::Null,
                        QueryValue::Bool(b) => json!(b),
                        QueryValue::Int64(n) => json!(n),
                        QueryValue::Float64(f) => json!(f),
                        QueryValue::String(s) => json!(s),
                        QueryValue::Bytes(b) => json!(String::from_utf8_lossy(b).to_string()),
                    })
                    .collect(),
            )
        })
        .collect();

    TrinoResponse {
        id: query_id.to_string(),
        next_uri: None,
        info_uri: format!("http://queryflux/ui/query.html?{query_id}"),
        partial_cancel_uri: None,
        stats: TrinoStats {
            state: "FINISHED".to_string(),
            queued: false,
            scheduled: true,
            running_drivers: 0,
            completed_splits: 1,
            total_splits: 1,
            queued_splits: 0,
            running_splits: 0,
            processed_rows: stats.rows_returned,
            processed_bytes: stats.bytes_returned.unwrap_or(0),
            physical_input_bytes: 0,
            peak_user_memory_bytes: 0,
            spilled_bytes: 0,
            queued_time_millis: stats.queue_duration_ms,
            elapsed_time_millis: stats.execution_duration_ms,
            cpu_time_millis: stats.execution_duration_ms,
            wall_time_millis: stats.execution_duration_ms,
            progress_percentage: Some(100.0),
        },
        error: None,
        columns: if trino_columns.is_empty() { None } else { Some(Value::Array(trino_columns)) },
        data: if trino_data.is_empty() { None } else { Some(Value::Array(trino_data)) },
        update_type: None,
        update_count: None,
        warnings: vec![],
    }
}

/// Map a generic SQL type string (as returned by engine adapters) to a Trino type name.
fn to_trino_type(data_type: &str) -> &str {
    match data_type.to_uppercase().as_str() {
        "INTEGER" | "INT" | "INT4" => "integer",
        "BIGINT" | "INT8" => "bigint",
        "SMALLINT" | "INT2" => "smallint",
        "TINYINT" | "INT1" => "tinyint",
        "DOUBLE" | "FLOAT8" | "DOUBLE PRECISION" => "double",
        "FLOAT" | "FLOAT4" | "REAL" => "real",
        "BOOLEAN" | "BOOL" => "boolean",
        "DATE" => "date",
        "TIMESTAMP" => "timestamp(3)",
        "BLOB" | "BYTEA" => "varbinary",
        "HUGEINT" | "DECIMAL" | "NUMERIC" => "decimal(38,0)",
        _ => "varchar",
    }
}
