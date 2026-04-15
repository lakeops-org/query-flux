//! End-to-end tests for QueryParams native parameter binding.
//!
//! These tests use the Snowflake HTTP frontend (which accepts `parameterBindings`)
//! targeting an in-process DuckDB backend (always available — no external dependency).
//!
//! Run with: `cargo test -p queryflux-e2e-tests --test query_params_tests`

use std::sync::OnceLock;

use queryflux_e2e_tests::{
    harness::{TestHarness, GROUP_DUCKDB},
    snowflake_client::SnowflakeClient,
};
use serde_json::json;

// ---------------------------------------------------------------------------
// Shared harness — DuckDB is always available so this never requires docker.
// ---------------------------------------------------------------------------

static HARNESS: OnceLock<TestHarness> = OnceLock::new();

fn harness() -> &'static TestHarness {
    HARNESS.get_or_init(|| {
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
            let h = rt.block_on(TestHarness::new()).expect("TestHarness::new");
            tx.send(h).expect("send harness");
            rt.block_on(std::future::pending::<()>());
        });
        rx.recv().expect("recv harness")
    })
}

fn sf_client() -> SnowflakeClient {
    SnowflakeClient::new(&harness().base_url())
}

/// Helper: login + run a parameterized query, panic on any protocol error.
async fn run(
    sql: &str,
    bindings: Option<serde_json::Value>,
) -> queryflux_e2e_tests::snowflake_client::SfQueryResult {
    let client = sf_client();
    let token = client.login().await.expect("Snowflake login");
    client
        .query(&token, sql, bindings)
        .await
        .expect("Snowflake query")
}

// ---------------------------------------------------------------------------
// Sanity — DuckDB is reachable and the frontend is wired up
// ---------------------------------------------------------------------------

#[tokio::test]
async fn duckdb_group_is_always_available() {
    assert!(
        harness().has_group(GROUP_DUCKDB),
        "DuckDB group should always be available"
    );
}

#[tokio::test]
async fn snowflake_login_succeeds() {
    let token = sf_client().login().await.expect("login");
    assert!(!token.is_empty());
}

#[tokio::test]
async fn no_params_baseline_query_works() {
    let r = run("SELECT 42 AS answer", None).await;
    assert!(r.success, "error: {:?}", r.error);
    assert_eq!(r.total_rows, 1);
    assert_eq!(r.rows[0][0].as_deref(), Some("42"));
}

// ---------------------------------------------------------------------------
// TEXT parameter
// ---------------------------------------------------------------------------

#[tokio::test]
async fn text_param_is_bound_correctly() {
    let r = run(
        "SELECT ? AS greeting",
        Some(json!({"1": {"type": "TEXT", "value": "hello"}})),
    )
    .await;
    assert!(r.success, "error: {:?}", r.error);
    assert_eq!(r.total_rows, 1);
    assert_eq!(r.rows[0][0].as_deref(), Some("hello"));
}

#[tokio::test]
async fn text_param_with_single_quote_is_safe() {
    let r = run(
        "SELECT ? AS name",
        Some(json!({"1": {"type": "TEXT", "value": "o'brien"}})),
    )
    .await;
    assert!(r.success, "error: {:?}", r.error);
    assert_eq!(r.rows[0][0].as_deref(), Some("o'brien"));
}

// ---------------------------------------------------------------------------
// FIXED (integer) parameter
// ---------------------------------------------------------------------------

#[tokio::test]
async fn integer_param_is_bound_correctly() {
    let r = run(
        "SELECT ? AS n",
        Some(json!({"1": {"type": "FIXED", "value": "42"}})),
    )
    .await;
    assert!(r.success, "error: {:?}", r.error);
    assert_eq!(r.total_rows, 1);
    assert_eq!(r.rows[0][0].as_deref(), Some("42"));
}

#[tokio::test]
async fn negative_integer_param_is_bound_correctly() {
    let r = run(
        "SELECT ? AS n",
        Some(json!({"1": {"type": "FIXED", "value": "-7"}})),
    )
    .await;
    assert!(r.success, "error: {:?}", r.error);
    assert_eq!(r.rows[0][0].as_deref(), Some("-7"));
}

#[tokio::test]
async fn integer_param_used_in_arithmetic() {
    let r = run(
        "SELECT ? * 2 AS doubled",
        Some(json!({"1": {"type": "FIXED", "value": "21"}})),
    )
    .await;
    assert!(r.success, "error: {:?}", r.error);
    assert_eq!(r.rows[0][0].as_deref(), Some("42"));
}

// ---------------------------------------------------------------------------
// REAL (float) parameter
// ---------------------------------------------------------------------------

#[tokio::test]
async fn float_param_is_bound_correctly() {
    let r = run(
        "SELECT CAST(? AS DOUBLE) > 3.0 AS result",
        Some(json!({"1": {"type": "REAL", "value": "3.14"}})),
    )
    .await;
    assert!(r.success, "error: {:?}", r.error);
    assert_eq!(r.total_rows, 1);
    // DuckDB returns boolean as true/false or 1/0 depending on format
    let val = r.rows[0][0].as_deref().unwrap_or("");
    assert!(val == "true" || val == "1", "expected truthy, got: {val}");
}

// ---------------------------------------------------------------------------
// BOOLEAN parameter
// ---------------------------------------------------------------------------

#[tokio::test]
async fn boolean_true_param_is_bound_correctly() {
    let r = run(
        "SELECT ? AS flag",
        Some(json!({"1": {"type": "BOOLEAN", "value": "true"}})),
    )
    .await;
    assert!(r.success, "error: {:?}", r.error);
    let val = r.rows[0][0].as_deref().unwrap_or("");
    assert!(val == "true" || val == "1", "expected truthy, got: {val}");
}

#[tokio::test]
async fn boolean_false_param_is_bound_correctly() {
    let r = run(
        "SELECT ? AS flag",
        Some(json!({"1": {"type": "BOOLEAN", "value": "false"}})),
    )
    .await;
    assert!(r.success, "error: {:?}", r.error);
    let val = r.rows[0][0].as_deref().unwrap_or("");
    assert!(val == "false" || val == "0", "expected falsy, got: {val}");
}

// ---------------------------------------------------------------------------
// NULL parameter
// ---------------------------------------------------------------------------

#[tokio::test]
async fn null_text_param_produces_null_row() {
    let r = run(
        "SELECT ? AS val",
        Some(json!({"1": {"type": "TEXT", "value": "NULL"}})),
    )
    .await;
    assert!(r.success, "error: {:?}", r.error);
    assert_eq!(r.total_rows, 1);
    assert!(
        r.rows[0][0].is_none(),
        "expected NULL, got: {:?}",
        r.rows[0][0]
    );
}

// ---------------------------------------------------------------------------
// Multiple parameters
// ---------------------------------------------------------------------------

#[tokio::test]
async fn multiple_params_are_bound_in_order() {
    let r = run(
        "SELECT ? AS a, ? AS b, ? AS c",
        Some(json!({
            "1": {"type": "FIXED",   "value": "1"},
            "2": {"type": "TEXT",    "value": "two"},
            "3": {"type": "BOOLEAN", "value": "true"}
        })),
    )
    .await;
    assert!(r.success, "error: {:?}", r.error);
    assert_eq!(r.total_rows, 1);
    assert_eq!(r.rows[0][0].as_deref(), Some("1"));
    assert_eq!(r.rows[0][1].as_deref(), Some("two"));
    let flag = r.rows[0][2].as_deref().unwrap_or("");
    assert!(
        flag == "true" || flag == "1",
        "expected truthy, got: {flag}"
    );
}

#[tokio::test]
async fn params_applied_in_numeric_key_order_regardless_of_json_order() {
    // JSON object keys have no guaranteed order; binding must use numeric key order.
    let r = run(
        "SELECT ? AS first, ? AS second",
        Some(json!({
            "2": {"type": "TEXT", "value": "second"},
            "1": {"type": "TEXT", "value": "first"}
        })),
    )
    .await;
    assert!(r.success, "error: {:?}", r.error);
    assert_eq!(r.rows[0][0].as_deref(), Some("first"));
    assert_eq!(r.rows[0][1].as_deref(), Some("second"));
}

// ---------------------------------------------------------------------------
// WHERE clause filtering — verifies params affect query results
// ---------------------------------------------------------------------------

#[tokio::test]
async fn integer_param_filters_rows_correctly() {
    let r = run(
        "SELECT n FROM (VALUES (1), (2), (3)) t(n) WHERE n > ?",
        Some(json!({"1": {"type": "FIXED", "value": "1"}})),
    )
    .await;
    assert!(r.success, "error: {:?}", r.error);
    assert_eq!(r.total_rows, 2, "expected rows 2 and 3");
}

#[tokio::test]
async fn text_param_filters_rows_correctly() {
    let r = run(
        "SELECT name FROM (VALUES ('alice'), ('bob'), ('carol')) t(name) WHERE name = ?",
        Some(json!({"1": {"type": "TEXT", "value": "bob"}})),
    )
    .await;
    assert!(r.success, "error: {:?}", r.error);
    assert_eq!(r.total_rows, 1);
    assert_eq!(r.rows[0][0].as_deref(), Some("bob"));
}

// ---------------------------------------------------------------------------
// Error cases
// ---------------------------------------------------------------------------

#[tokio::test]
async fn invalid_sql_returns_error_not_panic() {
    let r = run("NOT VALID SQL !!!!", None).await;
    assert!(!r.success, "expected failure for invalid SQL");
    assert!(r.error.is_some());
}
