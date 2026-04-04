/// Snowflake wire-protocol response formatting.
///
/// Converts Arrow RecordBatches into the Snowflake JSON response format:
///   - `rowtype`: column metadata array (matches Snowflake's JSON schema)
///   - `rowsetBase64`: base64-encoded Arrow IPC stream, with Snowflake field metadata and
///     data transformations that the nanoarrow_arrow_iterator expects.
///
/// Key references (fakesnow + Snowflake connector source):
///   https://github.com/snowflakedb/snowflake-connector-python/blob/main/src/snowflake/connector/nanoarrow_cpp/ArrowIterator/CArrowTableIterator.cpp
///   https://github.com/snowflakedb/snowflake-connector-python/blob/main/src/snowflake/connector/nanoarrow_cpp/ArrowIterator/SnowflakeType.cpp
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, Int32Array, Int64Array, StructArray, TimestampNanosecondArray,
};
use arrow::datatypes::{DataType, Field, Fields, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use arrow_ipc::writer::StreamWriter;
use base64::Engine;
use serde_json::{json, Value};

// ---------------------------------------------------------------------------
// Arrow DataType → Snowflake type string + metadata
// ---------------------------------------------------------------------------

struct SfTypeInfo {
    logical_type: &'static str,
    precision: u32,
    scale: u32,
    char_length: u32,
    byte_length: u32,
}

fn sf_type_info(dt: &DataType) -> SfTypeInfo {
    match dt {
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => SfTypeInfo {
            logical_type: "FIXED",
            precision: 38,
            scale: 0,
            char_length: 0,
            byte_length: 8,
        },
        DataType::Decimal128(p, s) | DataType::Decimal256(p, s) => SfTypeInfo {
            logical_type: "FIXED",
            precision: *p as u32,
            scale: *s as u32,
            char_length: 0,
            byte_length: 16,
        },
        DataType::Float16 | DataType::Float32 | DataType::Float64 => SfTypeInfo {
            logical_type: "REAL",
            precision: 0,
            scale: 0,
            char_length: 0,
            byte_length: 8,
        },
        DataType::Utf8 | DataType::LargeUtf8 => SfTypeInfo {
            logical_type: "TEXT",
            precision: 0,
            scale: 0,
            char_length: 16_777_216,
            byte_length: 16_777_216,
        },
        DataType::Boolean => SfTypeInfo {
            logical_type: "BOOLEAN",
            precision: 0,
            scale: 0,
            char_length: 0,
            byte_length: 1,
        },
        DataType::Date32 | DataType::Date64 => SfTypeInfo {
            logical_type: "DATE",
            precision: 0,
            scale: 0,
            char_length: 0,
            byte_length: 4,
        },
        DataType::Time32(_) | DataType::Time64(_) => SfTypeInfo {
            logical_type: "TIME",
            precision: 0,
            scale: 9,
            char_length: 0,
            byte_length: 8,
        },
        DataType::Timestamp(_, Some(_)) => SfTypeInfo {
            logical_type: "TIMESTAMP_TZ",
            precision: 0,
            scale: 9,
            char_length: 0,
            byte_length: 16,
        },
        DataType::Timestamp(_, None) => SfTypeInfo {
            logical_type: "TIMESTAMP_NTZ",
            precision: 0,
            scale: 9,
            char_length: 0,
            byte_length: 16,
        },
        DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_) => SfTypeInfo {
            logical_type: "BINARY",
            precision: 0,
            scale: 0,
            char_length: 0,
            byte_length: 8_388_608,
        },
        _ => SfTypeInfo {
            logical_type: "VARIANT",
            precision: 0,
            scale: 0,
            char_length: 0,
            byte_length: 0,
        },
    }
}

// ---------------------------------------------------------------------------
// rowtype JSON (for the HTTP response body)
// ---------------------------------------------------------------------------

pub fn schema_to_rowtype(schema: &Schema) -> Value {
    let cols: Vec<Value> = schema
        .fields()
        .iter()
        .map(|f| {
            let info = sf_type_info(f.data_type());
            json!({
                "name": f.name(),
                "database": "",
                "schema": "",
                "table": "",
                "nullable": f.is_nullable(),
                "type": info.logical_type.to_lowercase(),
                "byteLength": if info.byte_length > 0 { Some(info.byte_length) } else { None::<u32> },
                "length": if info.char_length > 0 { Some(info.char_length) } else { None::<u32> },
                "scale": if info.scale > 0 { Some(info.scale) } else { None::<u32> },
                "precision": if info.precision > 0 { Some(info.precision) } else { None::<u32> },
                "collation": null
            })
        })
        .collect();
    json!(cols)
}

// ---------------------------------------------------------------------------
// Arrow schema → Snowflake-annotated schema (adds required field metadata)
// ---------------------------------------------------------------------------

/// The nanoarrow_arrow_iterator reads `metadata.at("logicalType")` (and others) from every
/// Arrow field. Missing metadata causes `unordered_map::at: key not found`.
fn sf_arrow_field(field: &Field) -> Field {
    let info = sf_type_info(field.data_type());
    // Timestamps become structs in Snowflake Arrow format.
    let sf_type = match field.data_type() {
        DataType::Timestamp(_, tz) => {
            let mut fields = vec![
                Field::new("epoch", DataType::Int64, false),
                Field::new("fraction", DataType::Int32, false),
            ];
            if tz.is_some() {
                fields.push(Field::new("timezone", DataType::Int32, false));
            }
            DataType::Struct(Fields::from(fields))
        }
        // Time64 → int64 (nanoseconds)
        DataType::Time64(_) => DataType::Int64,
        DataType::Time32(_) => DataType::Int64,
        // UInt64 → int64 (connector expects signed)
        DataType::UInt64 => DataType::Int64,
        other => other.clone(),
    };

    let metadata = std::collections::HashMap::from([
        ("logicalType".to_string(), info.logical_type.to_string()),
        ("precision".to_string(), info.precision.to_string()),
        ("scale".to_string(), info.scale.to_string()),
        ("charLength".to_string(), info.char_length.to_string()),
    ]);

    Field::new(field.name(), sf_type, field.is_nullable()).with_metadata(metadata)
}

fn sf_arrow_schema(schema: &Schema) -> Schema {
    let fields: Vec<Field> = schema.fields().iter().map(|f| sf_arrow_field(f)).collect();
    Schema::new(fields)
}

// ---------------------------------------------------------------------------
// Data transformation: convert columns to Snowflake Arrow wire format
// ---------------------------------------------------------------------------

/// Cast a column to Snowflake's expected Arrow wire type.
fn to_sf_array(arr: &ArrayRef) -> ArrayRef {
    match arr.data_type() {
        DataType::Timestamp(unit, _tz) => timestamp_to_sf_struct(arr, unit),
        DataType::Time64(unit) => {
            let ns = match unit {
                TimeUnit::Nanosecond => arr.clone(),
                TimeUnit::Microsecond => {
                    let cast =
                        arrow::compute::cast(arr, &DataType::Int64).unwrap_or_else(|_| arr.clone());
                    // µs → ns: multiply by 1000
                    let ns_arr: Int64Array = cast
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .map(|a| Int64Array::from_iter(a.iter().map(|v| v.map(|x| x * 1000))))
                        .unwrap_or_else(|| Int64Array::from(vec![0i64; arr.len()]));
                    Arc::new(ns_arr)
                }
                _ => arrow::compute::cast(arr, &DataType::Int64).unwrap_or_else(|_| arr.clone()),
            };
            ns
        }
        DataType::Time32(_) => {
            // Cast to ns int64
            arrow::compute::cast(arr, &DataType::Int64).unwrap_or_else(|_| arr.clone())
        }
        DataType::UInt64 => {
            arrow::compute::cast(arr, &DataType::Int64).unwrap_or_else(|_| arr.clone())
        }
        _ => arr.clone(),
    }
}

/// Convert a Timestamp array to Snowflake's `{epoch: i64, fraction: i32}` struct.
fn timestamp_to_sf_struct(arr: &ArrayRef, unit: &TimeUnit) -> ArrayRef {
    let len = arr.len();

    // Normalize to nanosecond timestamps for uniform epoch/fraction extraction.
    let ns_arr = match unit {
        TimeUnit::Second => {
            arrow::compute::cast(arr, &DataType::Timestamp(TimeUnit::Nanosecond, None))
        }
        TimeUnit::Millisecond => {
            arrow::compute::cast(arr, &DataType::Timestamp(TimeUnit::Nanosecond, None))
        }
        TimeUnit::Microsecond => {
            arrow::compute::cast(arr, &DataType::Timestamp(TimeUnit::Nanosecond, None))
        }
        TimeUnit::Nanosecond => Ok(arr.clone()),
    };

    let (epochs, fractions): (Vec<Option<i64>>, Vec<Option<i32>>) = match ns_arr {
        Ok(ns) => {
            if let Some(ts) = ns.as_any().downcast_ref::<TimestampNanosecondArray>() {
                (0..len)
                    .map(|i| {
                        if ts.is_null(i) {
                            (None, None)
                        } else {
                            let nanos = ts.value(i);
                            let epoch = nanos / 1_000_000_000;
                            let fraction = (nanos % 1_000_000_000) as i32;
                            (Some(epoch), Some(fraction))
                        }
                    })
                    .unzip()
            } else {
                (vec![None; len], vec![None; len])
            }
        }
        Err(_) => (vec![None; len], vec![None; len]),
    };

    let epoch_arr = Arc::new(Int64Array::from(epochs)) as ArrayRef;
    let fraction_arr = Arc::new(Int32Array::from(fractions)) as ArrayRef;

    let has_tz = matches!(arr.data_type(), DataType::Timestamp(_, Some(_)));

    let struct_arr = if has_tz {
        let timezone_arr = Arc::new(Int32Array::from(vec![1440i32; len])) as ArrayRef;
        StructArray::from(vec![
            (
                Arc::new(Field::new("epoch", DataType::Int64, false)),
                epoch_arr,
            ),
            (
                Arc::new(Field::new("fraction", DataType::Int32, false)),
                fraction_arr,
            ),
            (
                Arc::new(Field::new("timezone", DataType::Int32, false)),
                timezone_arr,
            ),
        ])
    } else {
        StructArray::from(vec![
            (
                Arc::new(Field::new("epoch", DataType::Int64, false)),
                epoch_arr,
            ),
            (
                Arc::new(Field::new("fraction", DataType::Int32, false)),
                fraction_arr,
            ),
        ])
    };

    Arc::new(struct_arr)
}

// ---------------------------------------------------------------------------
// Arrow IPC stream → base64
// ---------------------------------------------------------------------------

pub fn batches_to_arrow_base64(schema: &Arc<Schema>, batches: &[RecordBatch]) -> String {
    let sf_schema = Arc::new(sf_arrow_schema(schema));

    let sf_batches: Vec<RecordBatch> = batches
        .iter()
        .filter_map(|batch| {
            let sf_columns: Vec<ArrayRef> = batch.columns().iter().map(to_sf_array).collect();
            RecordBatch::try_new(sf_schema.clone(), sf_columns).ok()
        })
        .collect();

    let mut buf = Vec::new();
    if let Ok(mut writer) = StreamWriter::try_new(&mut buf, &sf_schema) {
        for (i, batch) in sf_batches.iter().enumerate() {
            if let Err(e) = writer.write(batch) {
                tracing::warn!("Failed to write Arrow batch {i} to IPC stream: {e}");
            }
        }
        if let Err(e) = writer.finish() {
            tracing::warn!("Failed to finish Arrow IPC stream: {e}");
        }
    }
    base64::engine::general_purpose::STANDARD.encode(&buf)
}

// ---------------------------------------------------------------------------
// Full Snowflake query success response
// ---------------------------------------------------------------------------

pub fn sf_query_response(
    schema: &Arc<Schema>,
    batches: &[RecordBatch],
    total_rows: u64,
    query_id: &str,
    database: &str,
    schema_name: &str,
) -> Value {
    let rowtype = schema_to_rowtype(schema);
    let rowset_base64 = batches_to_arrow_base64(schema, batches);

    json!({
        "data": {
            "parameters": [
                {"name": "TIMEZONE", "value": "Etc/UTC"},
                {"name": "CLIENT_RESULT_CHUNK_SIZE", "value": 160},
                {"name": "CLIENT_SESSION_KEEP_ALIVE_HEARTBEAT_FREQUENCY", "value": 3600}
            ],
            "rowtype": rowtype,
            "rowsetBase64": rowset_base64,
            "total": total_rows,
            "returned": total_rows,
            "queryId": query_id,
            "queryResultFormat": "arrow",
            "finalDatabaseName": database,
            "finalSchemaName": schema_name
        },
        "success": true,
        "code": null,
        "message": null
    })
}
