/// Trino tests — require a running Trino instance.
///
/// All tests are marked `#[ignore]` and run with: make test-e2e
/// or: cargo test -p queryflux-e2e-tests --test trino_tests -- --include-ignored
///
/// TPC-H tests use Trino's built-in `tpch` connector (`tpch.tiny.*`).
/// SF tiny = SF 0.01 equivalent — same row counts as DuckDB dbgen(sf=0.01).
use std::sync::OnceLock;

use queryflux_e2e_tests::{
    harness::{TestHarness, GROUP_DUCKDB_TPCH, GROUP_TRINO},
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
            eprintln!("SKIP: engine group '{}' not available", $group);
            return;
        }
    };
}

fn first_i64(r: &queryflux_e2e_tests::trino_client::QueryResult) -> i64 {
    let v = &r.rows[0][0];
    match v {
        serde_json::Value::Number(n) => n.as_i64().unwrap_or(0),
        serde_json::Value::String(s) => s.parse().unwrap_or(0),
        _ => 0,
    }
}

// ---------------------------------------------------------------------------
// Basic Trino
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires Trino — run with: make test-e2e"]
async fn trino_select_literal() {
    require_group!(GROUP_TRINO);
    let r = client()
        .execute_on("SELECT 1 + 1 AS result", GROUP_TRINO)
        .await
        .expect("query");
    assert!(r.error.is_none(), "unexpected error: {:?}", r.error);
    assert_eq!(r.columns.len(), 1);
    assert_eq!(r.columns[0].name, "result");
    assert_eq!(r.rows.len(), 1);
}

#[tokio::test]
#[ignore = "requires Trino — run with: make test-e2e"]
async fn trino_select_multi_row() {
    require_group!(GROUP_TRINO);
    let r = client()
        .execute_on("SELECT v FROM (VALUES (1), (2), (3)) t(v)", GROUP_TRINO)
        .await
        .expect("query");
    assert!(r.error.is_none(), "unexpected error: {:?}", r.error);
    assert_eq!(r.rows.len(), 3);
}

#[tokio::test]
#[ignore = "requires Trino — run with: make test-e2e"]
async fn trino_syntax_error_returns_error() {
    require_group!(GROUP_TRINO);
    let r = client()
        .execute_on("THIS IS NOT SQL", GROUP_TRINO)
        .await
        .expect("query");
    assert!(r.error.is_some(), "expected error for invalid SQL");
}

// ---------------------------------------------------------------------------
// TPC-H via Trino's built-in tpch connector
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires Trino — run with: make test-e2e"]
async fn tpch_trino_customer_count() {
    require_group!(GROUP_TRINO);
    let r = client()
        .execute_on("SELECT COUNT(*) AS n FROM tpch.tiny.customer", GROUP_TRINO)
        .await
        .expect("query");
    assert!(r.error.is_none(), "error: {:?}", r.error);
    assert_eq!(first_i64(&r), 1500, "tpch.tiny.customer should have 1500 rows");
}

#[tokio::test]
#[ignore = "requires Trino — run with: make test-e2e"]
async fn tpch_trino_orders_count() {
    require_group!(GROUP_TRINO);
    let r = client()
        .execute_on("SELECT COUNT(*) AS n FROM tpch.tiny.orders", GROUP_TRINO)
        .await
        .expect("query");
    assert!(r.error.is_none(), "error: {:?}", r.error);
    assert_eq!(first_i64(&r), 15000, "tpch.tiny.orders should have 15000 rows");
}

/// Trino (tpch.tiny connector) and DuckDB (CALL dbgen sf=0.01) should agree
/// on the customer row count — both are TPC-H SF 0.01.
#[tokio::test]
#[ignore = "requires Trino — run with: make test-e2e"]
async fn tpch_trino_duckdb_customer_count_matches() {
    require_group!(GROUP_TRINO);
    let c = client();

    let trino = c
        .execute_on("SELECT COUNT(*) AS n FROM tpch.tiny.customer", GROUP_TRINO)
        .await
        .expect("trino query");
    let duck = c
        .execute_on("SELECT COUNT(*) AS n FROM customer", GROUP_DUCKDB_TPCH)
        .await
        .expect("duckdb query");

    assert!(trino.error.is_none(), "trino error: {:?}", trino.error);
    assert!(duck.error.is_none(), "duckdb error: {:?}", duck.error);
    assert_eq!(
        first_i64(&trino),
        first_i64(&duck),
        "trino tpch.tiny and duckdb dbgen should agree on customer count"
    );
}
