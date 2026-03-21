/// End-to-end tests for QueryFlux.
///
/// The harness (QueryFlux server + readiness probes) is initialized ONCE and
/// shared across all tests via a static OnceLock — no repeated probe overhead.
///
/// Run all tests including engine-gated ones:
///   make test-e2e
use std::sync::OnceLock;

use queryflux_e2e_tests::{
    harness::{TestHarness, GROUP_DUCKDB, GROUP_LAKEKEEPER, GROUP_STARROCKS, GROUP_TRINO},
    trino_client::TrinoClient,
};

// ---------------------------------------------------------------------------
// Shared harness — initialized once for the whole test process.
// ---------------------------------------------------------------------------

static HARNESS: OnceLock<TestHarness> = OnceLock::new();

/// Return the shared harness, probing engines on the first call only.
///
/// A dedicated background thread with its own Tokio runtime owns the axum
/// server so it keeps running across all tests regardless of which test
/// runtime calls first.
fn harness() -> &'static TestHarness {
    HARNESS.get_or_init(|| {
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().expect("runtime");
            let h = rt.block_on(TestHarness::new()).expect("TestHarness::new");
            tx.send(h).expect("send harness");
            // Keep this runtime (and the axum task inside it) alive forever.
            rt.block_on(std::future::pending::<()>());
        });
        rx.recv().expect("recv harness")
    })
}

fn client() -> TrinoClient {
    TrinoClient::new(&harness().base_url())
}

// ---------------------------------------------------------------------------
// Helper macro — skip a test if a required engine group is absent.
// ---------------------------------------------------------------------------
macro_rules! require_group {
    ($group:expr) => {
        if !harness().has_group($group) {
            eprintln!("SKIP: engine group '{}' not available", $group);
            return;
        }
    };
}

// ---------------------------------------------------------------------------
// DuckDB tests — always run (embedded, no external service)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn duckdb_select_literal() {
    let r = client().execute_on("SELECT 1 + 1 AS result", GROUP_DUCKDB).await.expect("query");
    assert!(r.error.is_none(), "unexpected error: {:?}", r.error);
    assert_eq!(r.columns.len(), 1);
    assert_eq!(r.columns[0].name, "result");
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], serde_json::json!(2));
}

#[tokio::test]
async fn duckdb_select_multiple_columns() {
    let r = client().execute_on(
        "SELECT 42 AS n, 'hello' AS s, true AS b",
        GROUP_DUCKDB,
    ).await.expect("query");
    assert!(r.error.is_none(), "unexpected error: {:?}", r.error);
    assert_eq!(r.columns.len(), 3);
    assert_eq!(r.rows.len(), 1);
}

#[tokio::test]
async fn duckdb_select_multi_row() {
    let r = client().execute_on(
        "SELECT v FROM (VALUES (1), (2), (3)) t(v)",
        GROUP_DUCKDB,
    ).await.expect("query");
    assert!(r.error.is_none(), "unexpected error: {:?}", r.error);
    assert_eq!(r.rows.len(), 3);
}

#[tokio::test]
async fn duckdb_empty_result() {
    let r = client().execute_on("SELECT 1 WHERE false", GROUP_DUCKDB).await.expect("query");
    assert!(r.error.is_none(), "unexpected error: {:?}", r.error);
    assert_eq!(r.rows.len(), 0);
}

#[tokio::test]
async fn duckdb_syntax_error_returns_error() {
    let r = client().execute_on("THIS IS NOT SQL", GROUP_DUCKDB).await.expect("query");
    assert!(r.error.is_some(), "expected error for invalid SQL");
}

// ---------------------------------------------------------------------------
// Routing: fallback (no header → DuckDB)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn routing_fallback_uses_duckdb() {
    let r = client().execute("SELECT 99 AS n", &[]).await.expect("query");
    assert!(r.error.is_none(), "unexpected error: {:?}", r.error);
    assert_eq!(r.rows.len(), 1);
}

// ---------------------------------------------------------------------------
// Routing: same SQL routed to two different engines
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires StarRocks — run with: make test-e2e"]
async fn routing_same_sql_duckdb_and_starrocks() {
    require_group!(GROUP_STARROCKS);
    let c = client();
    let sql = "SELECT 1 + 1 AS result";

    let duck = c.execute_on(sql, GROUP_DUCKDB).await.expect("duckdb");
    let sr   = c.execute_on(sql, GROUP_STARROCKS).await.expect("starrocks");

    assert!(duck.error.is_none(), "duckdb error: {:?}", duck.error);
    assert!(sr.error.is_none(),   "starrocks error: {:?}", sr.error);
    assert_eq!(duck.rows.len(), 1);
    assert_eq!(sr.rows.len(),   1);
    assert_eq!(duck.rows[0][0], sr.rows[0][0], "same SQL should return same value");
}

// ---------------------------------------------------------------------------
// Trino tests
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires Trino — run with: make test-e2e"]
async fn trino_select_literal() {
    require_group!(GROUP_TRINO);
    let r = client().execute_on("SELECT 1 + 1 AS result", GROUP_TRINO).await.expect("query");
    assert!(r.error.is_none(), "unexpected error: {:?}", r.error);
    assert_eq!(r.columns.len(), 1);
    assert_eq!(r.columns[0].name, "result");
    assert_eq!(r.rows.len(), 1);
}

#[tokio::test]
#[ignore = "requires Trino — run with: make test-e2e"]
async fn trino_select_multi_row() {
    require_group!(GROUP_TRINO);
    let r = client().execute_on(
        "SELECT v FROM (VALUES (1), (2), (3)) t(v)",
        GROUP_TRINO,
    ).await.expect("query");
    assert!(r.error.is_none(), "unexpected error: {:?}", r.error);
    assert_eq!(r.rows.len(), 3);
}

#[tokio::test]
#[ignore = "requires Trino — run with: make test-e2e"]
async fn trino_syntax_error_returns_error() {
    require_group!(GROUP_TRINO);
    let r = client().execute_on("THIS IS NOT SQL", GROUP_TRINO).await.expect("query");
    assert!(r.error.is_some(), "expected error for invalid SQL");
}

// ---------------------------------------------------------------------------
// StarRocks tests
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires StarRocks — run with: make test-e2e"]
async fn starrocks_select_literal() {
    require_group!(GROUP_STARROCKS);
    let r = client().execute_on("SELECT 1 + 1 AS result", GROUP_STARROCKS).await.expect("query");
    assert!(r.error.is_none(), "unexpected error: {:?}", r.error);
    assert_eq!(r.rows.len(), 1);
}

#[tokio::test]
#[ignore = "requires StarRocks — run with: make test-e2e"]
async fn starrocks_select_multi_row() {
    require_group!(GROUP_STARROCKS);
    let r = client().execute_on(
        "SELECT 1 AS v UNION ALL SELECT 2 UNION ALL SELECT 3",
        GROUP_STARROCKS,
    ).await.expect("query");
    assert!(r.error.is_none(), "unexpected error: {:?}", r.error);
    assert_eq!(r.rows.len(), 3);
}

// ---------------------------------------------------------------------------
// Cross-engine routing: all three engines in one test
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires Trino + StarRocks — run with: make test-e2e"]
async fn routing_all_three_engines() {
    require_group!(GROUP_TRINO);
    require_group!(GROUP_STARROCKS);
    let c = client();
    let sql = "SELECT 7 AS n";

    for group in [GROUP_DUCKDB, GROUP_TRINO, GROUP_STARROCKS] {
        let r = c.execute_on(sql, group).await
            .unwrap_or_else(|e| panic!("query on {group} failed: {e}"));
        assert!(r.error.is_none(), "{group} returned error: {:?}", r.error);
        assert_eq!(r.rows.len(), 1, "{group} should return 1 row");
    }
}

// ---------------------------------------------------------------------------
// Iceberg / Lakekeeper tests — shared TPC-H catalog across all engines.
//
// Data is loaded by the data-loader container (docker-compose.test.yml):
//   lakekeeper.tpch.customer  — 1500 rows  (tpch.tiny = SF 0.01)
//   lakekeeper.tpch.orders    — 15000 rows (tpch.tiny = SF 0.01)
//
// All three engines query the same `lakekeeper.tpch.*` three-part name.
//
// NOTE: DuckDB runs in-process on the host. Lakekeeper embeds its internal
// Docker endpoint (http://minio:9000) in REST catalog responses, so DuckDB
// cannot reach MinIO for data reads. DuckDB Iceberg tests skip gracefully
// when that hostname is unreachable.
// ---------------------------------------------------------------------------

/// Extract an integer from a JSON cell.
/// All engines now return numeric columns as JSON numbers (not strings) via
/// the Arrow→JSON serialization fix in result_sink.rs.  This helper is kept
/// as a single extraction point so test assertions stay readable.
fn json_row_as_i64(r: &queryflux_e2e_tests::trino_client::QueryResult, col: usize) -> i64 {
    let v = &r.rows[0][col];
    match v {
        serde_json::Value::Number(n) => n.as_i64().unwrap_or(0),
        serde_json::Value::String(s) => s.parse().unwrap_or(0), // defensive fallback
        _ => 0,
    }
}

/// Returns true when the error indicates a DNS resolution failure for a
/// Docker-internal hostname (e.g., `minio:9000` unreachable from the host).
fn is_docker_hostname_error(err: &str) -> bool {
    err.contains("Could not resolve hostname") || err.contains("Name or service not known")
}

/// Trino reads the Iceberg customer table; verifies row count matches tpch.tiny.
#[tokio::test]
#[ignore = "requires Trino + Lakekeeper — run with: make test-e2e"]
async fn iceberg_trino_customer_count() {
    require_group!(GROUP_LAKEKEEPER);
    require_group!(GROUP_TRINO);
    let r = client()
        .execute_on("SELECT COUNT(*) AS n FROM lakekeeper.tpch.customer", GROUP_TRINO)
        .await
        .expect("query");
    assert!(r.error.is_none(), "trino error: {:?}", r.error);
    assert_eq!(r.rows.len(), 1);
    // tpch.tiny = SF 0.01 → customer has 1500 rows.
    assert_eq!(json_row_as_i64(&r, 0), 1500, "expected 1500 customers");
}

/// DuckDB reads the Iceberg customer table via ATTACH.
/// Skips gracefully if MinIO's Docker-internal hostname is unreachable from the host.
#[tokio::test]
#[ignore = "requires Lakekeeper — run with: make test-e2e"]
async fn iceberg_duckdb_customer_count() {
    require_group!(GROUP_LAKEKEEPER);
    let r = client()
        .execute_on("SELECT COUNT(*) AS n FROM lakekeeper.tpch.customer", GROUP_DUCKDB)
        .await
        .expect("query");
    if let Some(ref err) = r.error {
        if is_docker_hostname_error(err) {
            eprintln!("SKIP iceberg_duckdb_customer_count: DuckDB can't reach Docker-internal MinIO ({err})");
            return;
        }
        panic!("unexpected duckdb error: {err}");
    }
    assert_eq!(r.rows.len(), 1);
    assert_eq!(json_row_as_i64(&r, 0), 1500, "expected 1500 customers");
}

/// StarRocks reads the Iceberg customer table via CREATE EXTERNAL CATALOG.
#[tokio::test]
#[ignore = "requires StarRocks + Lakekeeper — run with: make test-e2e"]
async fn iceberg_starrocks_customer_count() {
    require_group!(GROUP_LAKEKEEPER);
    require_group!(GROUP_STARROCKS);
    let r = client()
        .execute_on("SELECT COUNT(*) AS n FROM lakekeeper.tpch.customer", GROUP_STARROCKS)
        .await
        .expect("query");
    assert!(r.error.is_none(), "starrocks error: {:?}", r.error);
    assert_eq!(r.rows.len(), 1);
    assert_eq!(json_row_as_i64(&r, 0), 1500, "expected 1500 customers");
}

/// Trino and StarRocks return the same customer count from the shared Iceberg catalog.
/// DuckDB is skipped when MinIO's Docker hostname is not reachable from the host.
#[tokio::test]
#[ignore = "requires Trino + StarRocks + Lakekeeper — run with: make test-e2e"]
async fn iceberg_cross_engine_customer_count_matches() {
    require_group!(GROUP_LAKEKEEPER);
    require_group!(GROUP_TRINO);
    require_group!(GROUP_STARROCKS);

    let c = client();
    let sql = "SELECT COUNT(*) AS n FROM lakekeeper.tpch.customer";

    let trino = c.execute_on(sql, GROUP_TRINO).await.expect("trino");
    let duck  = c.execute_on(sql, GROUP_DUCKDB).await.expect("duckdb");
    let sr    = c.execute_on(sql, GROUP_STARROCKS).await.expect("starrocks");

    assert!(trino.error.is_none(), "trino error: {:?}", trino.error);
    assert!(sr.error.is_none(),    "starrocks error: {:?}", sr.error);

    let trino_n = json_row_as_i64(&trino, 0);
    let sr_n    = json_row_as_i64(&sr, 0);
    assert_eq!(trino_n, sr_n, "trino vs starrocks customer count mismatch");

    // DuckDB Iceberg may fail when MinIO is only reachable inside Docker.
    if let Some(ref err) = duck.error {
        if is_docker_hostname_error(err) {
            eprintln!("NOTE: DuckDB Iceberg skipped (Docker hostname unreachable): {err}");
        } else {
            panic!("unexpected duckdb error: {err}");
        }
    } else {
        assert_eq!(json_row_as_i64(&duck, 0), trino_n, "duckdb vs trino customer count mismatch");
    }
}

/// Trino and StarRocks agree on the orders count from the shared Iceberg catalog.
#[tokio::test]
#[ignore = "requires Trino + StarRocks + Lakekeeper — run with: make test-e2e"]
async fn iceberg_cross_engine_aggregation_matches() {
    require_group!(GROUP_LAKEKEEPER);
    require_group!(GROUP_TRINO);
    require_group!(GROUP_STARROCKS);

    let c = client();
    let sql = "SELECT COUNT(*) AS n FROM lakekeeper.tpch.orders";

    let trino = c.execute_on(sql, GROUP_TRINO).await.expect("trino");
    let duck  = c.execute_on(sql, GROUP_DUCKDB).await.expect("duckdb");
    let sr    = c.execute_on(sql, GROUP_STARROCKS).await.expect("starrocks");

    assert!(trino.error.is_none(), "trino error: {:?}", trino.error);
    assert!(sr.error.is_none(),    "starrocks error: {:?}", sr.error);

    // tpch.tiny = SF 0.01 → orders has 15000 rows.
    let trino_n = json_row_as_i64(&trino, 0);
    let sr_n    = json_row_as_i64(&sr, 0);
    assert_eq!(trino_n, 15000, "trino orders count mismatch");
    assert_eq!(sr_n,    15000, "starrocks orders count mismatch");

    if let Some(ref err) = duck.error {
        if is_docker_hostname_error(err) {
            eprintln!("NOTE: DuckDB Iceberg skipped (Docker hostname unreachable): {err}");
        } else {
            panic!("unexpected duckdb error: {err}");
        }
    } else {
        assert_eq!(json_row_as_i64(&duck, 0), 15000, "duckdb orders count mismatch");
    }
}
