/// DuckDB tests — always run (embedded engine, no external services needed).
///
/// Covers basic query correctness and TPC-H SQL patterns via a dedicated
/// in-memory DuckDB instance pre-loaded with `CALL dbgen(sf=0.01)`.
///
/// TPC-H table row counts at SF 0.01:
///   customer  — 1 500
///   orders    — 15 000
///   nation    — 25
///   region    — 5
///   supplier  — 100
///   lineitem  — ~60 175
///
/// Run with: cargo test -p queryflux-e2e-tests --test duckdb_tests
use std::sync::OnceLock;

use queryflux_e2e_tests::{
    harness::{TestHarness, GROUP_DUCKDB, GROUP_DUCKDB_HTTP, GROUP_DUCKDB_TPCH, GROUP_MOTHERDUCK},
    trino_client::TrinoClient,
};

static HARNESS: OnceLock<TestHarness> = OnceLock::new();

fn harness() -> &'static TestHarness {
    HARNESS.get_or_init(|| {
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().expect("runtime");
            let h = rt.block_on(TestHarness::new()).expect("TestHarness::new");
            tx.send(h).expect("send harness");
            rt.block_on(std::future::pending::<()>());
        });
        rx.recv().expect("recv harness")
    })
}

fn client() -> TrinoClient {
    TrinoClient::new(&harness().base_url())
}

macro_rules! require_group {
    ($group:expr) => {
        if !harness().has_group($group) {
            return;
        }
    };
}

/// Extract an i64 from the first column of the first row.
fn first_i64(r: &queryflux_e2e_tests::trino_client::QueryResult) -> i64 {
    let v = &r.rows[0][0];
    match v {
        serde_json::Value::Number(n) => n.as_i64().unwrap_or(0),
        serde_json::Value::String(s) => s.parse().unwrap_or(0),
        _ => 0,
    }
}

// ---------------------------------------------------------------------------
// Routing fallback — no header → DuckDB default
// ---------------------------------------------------------------------------

#[tokio::test]
async fn routing_fallback_uses_duckdb() {
    let r = client()
        .execute("SELECT 99 AS n", &[])
        .await
        .expect("query");
    assert!(r.error.is_none(), "unexpected error: {:?}", r.error);
    assert_eq!(r.rows.len(), 1);
}

// ---------------------------------------------------------------------------
// Basic DuckDB — literal queries
// ---------------------------------------------------------------------------

#[tokio::test]
async fn duckdb_select_literal() {
    let r = client()
        .execute_on("SELECT 1 + 1 AS result", GROUP_DUCKDB)
        .await
        .expect("query");
    assert!(r.error.is_none(), "unexpected error: {:?}", r.error);
    assert_eq!(r.columns.len(), 1);
    assert_eq!(r.columns[0].name, "result");
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], serde_json::json!(2));
}

#[tokio::test]
async fn duckdb_select_multiple_columns() {
    let r = client()
        .execute_on("SELECT 42 AS n, 'hello' AS s, true AS b", GROUP_DUCKDB)
        .await
        .expect("query");
    assert!(r.error.is_none(), "unexpected error: {:?}", r.error);
    assert_eq!(r.columns.len(), 3);
    assert_eq!(r.rows.len(), 1);
}

#[tokio::test]
async fn duckdb_select_multi_row() {
    let r = client()
        .execute_on("SELECT v FROM (VALUES (1), (2), (3)) t(v)", GROUP_DUCKDB)
        .await
        .expect("query");
    assert!(r.error.is_none(), "unexpected error: {:?}", r.error);
    assert_eq!(r.rows.len(), 3);
}

#[tokio::test]
async fn duckdb_empty_result() {
    let r = client()
        .execute_on("SELECT 1 WHERE false", GROUP_DUCKDB)
        .await
        .expect("query");
    assert!(r.error.is_none(), "unexpected error: {:?}", r.error);
    assert_eq!(r.rows.len(), 0);
}

#[tokio::test]
async fn duckdb_syntax_error_returns_error() {
    let r = client()
        .execute_on("THIS IS NOT SQL", GROUP_DUCKDB)
        .await
        .expect("query");
    assert!(r.error.is_some(), "expected error for invalid SQL");
}

// ---------------------------------------------------------------------------
// TPC-H — row counts
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tpch_customer_count() {
    let r = client()
        .execute_on("SELECT COUNT(*) AS n FROM customer", GROUP_DUCKDB_TPCH)
        .await
        .expect("query");
    assert!(r.error.is_none(), "error: {:?}", r.error);
    assert_eq!(first_i64(&r), 1500, "customer should have 1500 rows at SF 0.01");
}

#[tokio::test]
async fn tpch_orders_count() {
    let r = client()
        .execute_on("SELECT COUNT(*) AS n FROM orders", GROUP_DUCKDB_TPCH)
        .await
        .expect("query");
    assert!(r.error.is_none(), "error: {:?}", r.error);
    assert_eq!(first_i64(&r), 15000, "orders should have 15000 rows at SF 0.01");
}

#[tokio::test]
async fn tpch_nation_count() {
    let r = client()
        .execute_on("SELECT COUNT(*) AS n FROM nation", GROUP_DUCKDB_TPCH)
        .await
        .expect("query");
    assert!(r.error.is_none(), "error: {:?}", r.error);
    assert_eq!(first_i64(&r), 25, "nation should have 25 rows");
}

#[tokio::test]
async fn tpch_region_count() {
    let r = client()
        .execute_on("SELECT COUNT(*) AS n FROM region", GROUP_DUCKDB_TPCH)
        .await
        .expect("query");
    assert!(r.error.is_none(), "error: {:?}", r.error);
    assert_eq!(first_i64(&r), 5, "region should have 5 rows");
}

// ---------------------------------------------------------------------------
// TPC-H — aggregations
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tpch_orders_total_price_sum() {
    let r = client()
        .execute_on(
            "SELECT CAST(SUM(o_totalprice) AS DOUBLE) AS total FROM orders",
            GROUP_DUCKDB_TPCH,
        )
        .await
        .expect("query");
    assert!(r.error.is_none(), "error: {:?}", r.error);
    assert_eq!(r.rows.len(), 1);
    // Just verify it's a positive number — exact value depends on random seed
    // DuckDB may return DECIMAL as a string in the Trino protocol response
    let total = match &r.rows[0][0] {
        serde_json::Value::Number(n) => n.as_f64().unwrap_or(0.0),
        serde_json::Value::String(s) => s.parse::<f64>().unwrap_or(0.0),
        _ => 0.0,
    };
    assert!(total > 0.0, "total price sum should be positive: {:?}", r.rows[0][0]);
}

#[tokio::test]
async fn tpch_orders_status_filter() {
    let r = client()
        .execute_on(
            "SELECT COUNT(*) AS n FROM orders WHERE o_orderstatus = 'O'",
            GROUP_DUCKDB_TPCH,
        )
        .await
        .expect("query");
    assert!(r.error.is_none(), "error: {:?}", r.error);
    let n = first_i64(&r);
    assert!(n > 0, "should have open orders");
    assert!(n <= 15000, "can't exceed total orders");
}

// ---------------------------------------------------------------------------
// TPC-H — joins and subqueries
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tpch_customer_nation_join() {
    let r = client()
        .execute_on(
            "SELECT n_name, COUNT(*) AS cnt \
             FROM customer \
             JOIN nation ON c_nationkey = n_nationkey \
             GROUP BY n_name \
             ORDER BY cnt DESC \
             LIMIT 5",
            GROUP_DUCKDB_TPCH,
        )
        .await
        .expect("query");
    assert!(r.error.is_none(), "error: {:?}", r.error);
    assert_eq!(r.rows.len(), 5, "LIMIT 5 should return 5 rows");
    assert_eq!(r.columns[0].name, "n_name");
    assert_eq!(r.columns[1].name, "cnt");
}

#[tokio::test]
async fn tpch_above_average_orders() {
    let r = client()
        .execute_on(
            "SELECT COUNT(*) AS n FROM orders \
             WHERE o_totalprice > (SELECT AVG(o_totalprice) FROM orders)",
            GROUP_DUCKDB_TPCH,
        )
        .await
        .expect("query");
    assert!(r.error.is_none(), "error: {:?}", r.error);
    let n = first_i64(&r);
    assert!(n > 0, "should have orders above average price");
    assert!(n < 15000, "not all orders can be above average");
}

// ---------------------------------------------------------------------------
// DuckDB HTTP server — requires DUCKDB_HTTP_URL (set by make test-e2e)
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires DuckDB HTTP server — run with: make test-e2e"]
async fn duckdb_http_select_literal() {
    require_group!(GROUP_DUCKDB_HTTP);
    let r = client()
        .execute_on("SELECT 1 + 1 AS result", GROUP_DUCKDB_HTTP)
        .await
        .expect("query");
    assert!(r.error.is_none(), "unexpected error: {:?}", r.error);
    assert_eq!(r.columns[0].name, "result");
    assert_eq!(r.rows[0][0], serde_json::json!(2));
}

#[tokio::test]
#[ignore = "requires DuckDB HTTP server — run with: make test-e2e"]
async fn duckdb_http_select_multi_row() {
    require_group!(GROUP_DUCKDB_HTTP);
    let r = client()
        .execute_on("SELECT v FROM (VALUES (1), (2), (3)) t(v)", GROUP_DUCKDB_HTTP)
        .await
        .expect("query");
    assert!(r.error.is_none(), "unexpected error: {:?}", r.error);
    assert_eq!(r.rows.len(), 3);
}

#[tokio::test]
#[ignore = "requires DuckDB HTTP server — run with: make test-e2e"]
async fn duckdb_http_syntax_error_returns_error() {
    require_group!(GROUP_DUCKDB_HTTP);
    let r = client()
        .execute_on("THIS IS NOT SQL", GROUP_DUCKDB_HTTP)
        .await
        .expect("query");
    assert!(r.error.is_some(), "expected error for invalid SQL");
}

#[tokio::test]
#[ignore = "requires DuckDB HTTP server — run with: make test-e2e"]
async fn duckdb_http_iceberg_nation_count() {
    require_group!(GROUP_DUCKDB_HTTP);
    let r = client()
        .execute_on("SELECT COUNT(*) AS n FROM lakekeeper.tpch.nation", GROUP_DUCKDB_HTTP)
        .await
        .expect("query");
    assert!(r.error.is_none(), "error: {:?}", r.error);
    assert_eq!(first_i64(&r), 25, "nation should have 25 rows");
}

// ---------------------------------------------------------------------------
// MotherDuck — requires MOTHERDUCK_TOKEN env var
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires MotherDuck token (MOTHERDUCK_TOKEN env var)"]
async fn motherduck_select_literal() {
    require_group!(GROUP_MOTHERDUCK);
    let r = client()
        .execute_on("SELECT 42 AS n", GROUP_MOTHERDUCK)
        .await
        .expect("query");
    assert!(r.error.is_none(), "unexpected error: {:?}", r.error);
    assert_eq!(first_i64(&r), 42);
}

#[tokio::test]
#[ignore = "requires MotherDuck token (MOTHERDUCK_TOKEN env var)"]
async fn motherduck_list_databases() {
    require_group!(GROUP_MOTHERDUCK);
    let r = client()
        .execute_on("SELECT schema_name FROM information_schema.schemata LIMIT 10", GROUP_MOTHERDUCK)
        .await
        .expect("query");
    assert!(r.error.is_none(), "unexpected error: {:?}", r.error);
    assert!(!r.rows.is_empty(), "should have at least one schema in MotherDuck");
}
