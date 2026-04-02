use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::Array;
use arrow::compute::cast as arrow_cast;
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use queryflux_auth::Credentials;
use queryflux_core::{
    error::Result,
    query::{FrontendProtocol, QueryStats},
    session::SessionContext,
    tags::QueryTags,
};
use serde_json::{json, Value};
use tracing::warn;
use uuid::Uuid;

use crate::dispatch::{execute_to_sink, ResultSink};
use crate::snowflake::http::format::schema_to_rowtype;
use crate::snowflake::http::handlers::common::{parse_snowflake_json_body, sf_error};
use crate::state::AppState;

// ---------------------------------------------------------------------------
// ResultSink that accumulates Arrow batches into SQL API v2 jsonv2 format
// ---------------------------------------------------------------------------

struct SqlApiSink {
    schema: Option<Arc<Schema>>,
    rows: Vec<Vec<Value>>,
    error: Option<String>,
}

impl SqlApiSink {
    fn new() -> Self {
        Self {
            schema: None,
            rows: Vec::new(),
            error: None,
        }
    }
}

#[async_trait]
impl ResultSink for SqlApiSink {
    async fn on_schema(&mut self, schema: &Schema) -> Result<()> {
        self.schema = Some(Arc::new(schema.clone()));
        Ok(())
    }

    async fn on_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        for row_idx in 0..batch.num_rows() {
            let row: Vec<Value> = (0..batch.num_columns())
                .map(|col_idx| array_value_to_json(batch.column(col_idx).as_ref(), row_idx))
                .collect();
            self.rows.push(row);
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

impl SqlApiSink {
    fn into_response(self, handle: &str) -> Response {
        if let Some(err) = self.error {
            return (
                StatusCode::OK,
                axum::Json(json!({
                    "code": "002043",
                    "message": err,
                    "sqlState": "P0001",
                    "statementHandle": handle
                })),
            )
                .into_response();
        }

        let schema = self.schema.unwrap_or_else(|| Arc::new(Schema::empty()));
        let num_rows = self.rows.len() as u64;
        let rowtype = schema_to_rowtype(&schema);

        (
            StatusCode::OK,
            axum::Json(json!({
                "statementHandle": handle,
                "message": "Statement executed successfully.",
                "createdOn": chrono::Utc::now().timestamp_millis(),
                "statementStatusUrl": format!("/api/v2/statements/{handle}"),
                "resultSetMetaData": {
                    "numRows": num_rows,
                    "format": "jsonv2",
                    "rowType": rowtype,
                    "partitionInfo": [{"rowCount": num_rows, "uncompressedSize": 0}]
                },
                "data": self.rows
            })),
        )
            .into_response()
    }
}

/// Serialize a single array value at `row` to a SQL API v2 JSON value (string or null).
fn array_value_to_json(arr: &dyn Array, row: usize) -> Value {
    if arr.is_null(row) {
        return Value::Null;
    }
    // Cast to Utf8 for uniform string serialization.
    let string_arr = arrow_cast(arr, &DataType::Utf8);
    match string_arr {
        Ok(s) => {
            if let Some(str_arr) = s.as_any().downcast_ref::<arrow::array::StringArray>() {
                Value::String(str_arr.value(row).to_string())
            } else {
                Value::Null
            }
        }
        Err(_) => Value::String(format!("{:?}", arr.data_type())),
    }
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

/// POST /api/v2/statements  — submit SQL, execute synchronously, return jsonv2
pub async fn submit_statement(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let body_json: Value = match parse_snowflake_json_body(&headers, &body) {
        Ok(v) => v,
        Err(_) => return sf_error(StatusCode::BAD_REQUEST, 390000, "Invalid JSON body"),
    };
    let sql = body_json["statement"].as_str().unwrap_or("").to_string();

    // Stateless auth: Bearer token in Authorization header.
    let auth_ctx = match authenticate(&state, &headers).await {
        Ok(ctx) => ctx,
        Err(e) => return sf_error(StatusCode::UNAUTHORIZED, 390002, &e.to_string()),
    };

    let session_ctx = SessionContext::MySqlWire {
        user: Some(auth_ctx.user.clone()),
        schema: None,
        session_vars: HashMap::new(),
        tags: QueryTags::default(),
    };
    let group = {
        let live = state.live.read().await;
        live.router_chain
            .route(
                &sql,
                &session_ctx,
                &FrontendProtocol::SnowflakeSqlApi,
                Some(&auth_ctx),
            )
            .await
    };
    let group = match group {
        Ok(g) => g,
        Err(e) => return sf_error(StatusCode::BAD_GATEWAY, 390000, &e.to_string()),
    };

    let handle = Uuid::new_v4().to_string();
    let mut sink = SqlApiSink::new();

    if let Err(e) = execute_to_sink(
        &state,
        sql,
        session_ctx,
        FrontendProtocol::SnowflakeSqlApi,
        group,
        &mut sink,
        &auth_ctx,
    )
    .await
    {
        warn!(handle = %handle, "SQL API execute_to_sink error: {e}");
        sink.error = Some(e.to_string());
    }

    sink.into_response(&handle)
}

/// GET /api/v2/statements/:handle  — stub (sync execution, nothing to poll)
pub async fn get_statement(
    State(_state): State<Arc<AppState>>,
    _headers: HeaderMap,
    axum::extract::Path(handle): axum::extract::Path<String>,
    _raw_query: axum::extract::RawQuery,
) -> Response {
    (
        StatusCode::NOT_FOUND,
        axum::Json(json!({
            "code": "390142",
            "message": format!("Statement handle {handle} not found or already complete."),
            "sqlState": "02000",
            "statementHandle": handle
        })),
    )
        .into_response()
}

/// DELETE /api/v2/statements/:handle  — stub (sync execution, nothing to cancel)
pub async fn cancel_statement(
    State(_state): State<Arc<AppState>>,
    _headers: HeaderMap,
    axum::extract::Path(handle): axum::extract::Path<String>,
) -> Response {
    (
        StatusCode::OK,
        axum::Json(json!({
            "statementHandle": handle,
            "message": "Statement aborted.",
        })),
    )
        .into_response()
}

// ---------------------------------------------------------------------------
// Auth helper
// ---------------------------------------------------------------------------

async fn authenticate(
    state: &Arc<AppState>,
    headers: &HeaderMap,
) -> std::result::Result<queryflux_auth::AuthContext, String> {
    let bearer = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .map(|s| s.to_string());

    state
        .auth_provider
        .authenticate(&Credentials {
            username: None,
            password: None,
            bearer_token: bearer,
        })
        .await
        .map_err(|e| e.to_string())
}
