use arrow::array::{Array, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, StringArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array};
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use queryflux_core::{error::Result, query::QueryStats};
use serde_json::{json, Value};

use crate::dispatch::ResultSink;

/// Collects Arrow RecordBatches from `execute_to_sink` and serializes them as
/// JSON rows keyed by column name. Used by all MCP tool handlers that need to
/// return query results as text content.
pub struct JsonResultSink {
    columns: Vec<String>,
    rows: Vec<Value>,
    max_rows: usize,
    pub truncated: bool,
    pub error: Option<String>,
}

impl JsonResultSink {
    pub fn new(max_rows: usize) -> Self {
        Self {
            columns: Vec::new(),
            rows: Vec::new(),
            max_rows,
            truncated: false,
            error: None,
        }
    }

    /// Build the final JSON result payload after `execute_to_sink` returns.
    pub fn into_result(self, elapsed_ms: u64, engine: &str) -> Value {
        let row_count = self.rows.len();
        json!({
            "columns": self.columns,
            "rows": self.rows,
            "row_count": row_count,
            "truncated": self.truncated,
            "elapsed_ms": elapsed_ms,
            "engine": engine,
        })
    }

    pub fn columns(&self) -> &[String] {
        &self.columns
    }

    pub fn rows(&self) -> &[Value] {
        &self.rows
    }
}

#[async_trait]
impl ResultSink for JsonResultSink {
    async fn on_schema(&mut self, schema: &Schema) -> Result<()> {
        self.columns = schema.fields().iter().map(|f| f.name().clone()).collect();
        Ok(())
    }

    async fn on_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        let remaining = self.max_rows.saturating_sub(self.rows.len());
        if remaining == 0 {
            self.truncated = true;
            return Ok(());
        }
        let take = batch.num_rows().min(remaining);
        if batch.num_rows() > remaining {
            self.truncated = true;
        }

        for row_idx in 0..take {
            let mut obj = serde_json::Map::new();
            for (col_idx, field) in batch.schema().fields().iter().enumerate() {
                let col = batch.column(col_idx);
                let val = arrow_value(col.as_ref(), row_idx);
                obj.insert(field.name().clone(), val);
            }
            self.rows.push(Value::Object(obj));
        }
        Ok(())
    }

    async fn on_complete(&mut self, _stats: &QueryStats) -> Result<()> {
        Ok(())
    }

    async fn on_error(&mut self, message: &str) -> Result<()> {
        self.error = Some(message.to_string());
        Ok(())
    }
}

/// Convert a single cell from an Arrow array into a `serde_json::Value`.
/// Falls back to a string representation for complex or unsupported types.
fn arrow_value(array: &dyn Array, idx: usize) -> Value {
    if array.is_null(idx) {
        return Value::Null;
    }
    match array.data_type() {
        DataType::Boolean => {
            let a = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            Value::Bool(a.value(idx))
        }
        DataType::Int8 => {
            let a = array.as_any().downcast_ref::<Int8Array>().unwrap();
            json!(a.value(idx))
        }
        DataType::Int16 => {
            let a = array.as_any().downcast_ref::<Int16Array>().unwrap();
            json!(a.value(idx))
        }
        DataType::Int32 => {
            let a = array.as_any().downcast_ref::<Int32Array>().unwrap();
            json!(a.value(idx))
        }
        DataType::Int64 => {
            let a = array.as_any().downcast_ref::<Int64Array>().unwrap();
            json!(a.value(idx))
        }
        DataType::UInt8 => {
            let a = array.as_any().downcast_ref::<UInt8Array>().unwrap();
            json!(a.value(idx))
        }
        DataType::UInt16 => {
            let a = array.as_any().downcast_ref::<UInt16Array>().unwrap();
            json!(a.value(idx))
        }
        DataType::UInt32 => {
            let a = array.as_any().downcast_ref::<UInt32Array>().unwrap();
            json!(a.value(idx))
        }
        DataType::UInt64 => {
            let a = array.as_any().downcast_ref::<UInt64Array>().unwrap();
            json!(a.value(idx))
        }
        DataType::Float32 => {
            let a = array.as_any().downcast_ref::<Float32Array>().unwrap();
            let v = a.value(idx) as f64;
            Value::Number(serde_json::Number::from_f64(v).unwrap_or(serde_json::Number::from(0)))
        }
        DataType::Float64 => {
            let a = array.as_any().downcast_ref::<Float64Array>().unwrap();
            let v = a.value(idx);
            Value::Number(serde_json::Number::from_f64(v).unwrap_or(serde_json::Number::from(0)))
        }
        DataType::Utf8 | DataType::LargeUtf8 => {
            let a = array.as_any().downcast_ref::<StringArray>().unwrap();
            Value::String(a.value(idx).to_string())
        }
        _ => {
            // For dates, decimals, lists, structs, etc.: use Arrow's display formatting.
            use arrow::util::display::array_value_to_string;
            let s = array_value_to_string(array, idx).unwrap_or_else(|_| "<error>".to_string());
            Value::String(s)
        }
    }
}
