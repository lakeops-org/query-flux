/// Iceberg / Lakekeeper — Trino + StarRocks (shared `lakekeeper.tpch.*`).
///
/// Requires docker-compose stack + `make test-e2e` (or `--include-ignored`).
/// DuckDB is not used in the e2e harness.
use std::sync::OnceLock;

use queryflux_e2e_tests::{
    harness::{TestHarness, GROUP_LAKEKEEPER, GROUP_STARROCKS, GROUP_TRINO},
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
    assert!(
        r.rows.len() == 1 && !r.rows[0].is_empty(),
        "expected one row; rows={}, error={:?}",
        r.rows.len(),
        r.error
    );
    let v = &r.rows[0][0];
    match v {
        serde_json::Value::Number(n) => n.as_i64().unwrap_or(0),
        serde_json::Value::String(s) => s.parse().unwrap_or(0),
        _ => 0,
    }
}

#[tokio::test]
#[ignore = "requires Trino + Lakekeeper — run with: make test-e2e"]
async fn iceberg_trino_customer_count() {
    require_group!(GROUP_LAKEKEEPER);
    require_group!(GROUP_TRINO);
    let r = client()
        .execute_on(
            "SELECT COUNT(*) AS n FROM lakekeeper.tpch.customer",
            GROUP_TRINO,
        )
        .await
        .expect("query");
    assert!(r.error.is_none(), "trino error: {:?}", r.error);
    assert_eq!(first_i64(&r), 1500, "expected 1500 customers");
}

#[tokio::test]
#[ignore = "requires StarRocks + Lakekeeper — run with: make test-e2e"]
async fn iceberg_starrocks_customer_count() {
    require_group!(GROUP_LAKEKEEPER);
    require_group!(GROUP_STARROCKS);
    let r = client()
        .execute_on(
            "SELECT COUNT(*) AS n FROM lakekeeper.tpch.customer",
            GROUP_STARROCKS,
        )
        .await
        .expect("query");
    assert!(r.error.is_none(), "starrocks error: {:?}", r.error);
    assert_eq!(first_i64(&r), 1500, "expected 1500 customers");
}

#[tokio::test]
#[ignore = "requires Trino + StarRocks + Lakekeeper — run with: make test-e2e"]
async fn iceberg_cross_engine_customer_count_matches() {
    require_group!(GROUP_LAKEKEEPER);
    require_group!(GROUP_TRINO);
    require_group!(GROUP_STARROCKS);

    let c = client();
    let sql = "SELECT COUNT(*) AS n FROM lakekeeper.tpch.customer";

    let trino = c.execute_on(sql, GROUP_TRINO).await.expect("trino");
    let sr = c.execute_on(sql, GROUP_STARROCKS).await.expect("starrocks");

    assert!(trino.error.is_none(), "trino error: {:?}", trino.error);
    assert!(sr.error.is_none(), "starrocks error: {:?}", sr.error);

    assert_eq!(first_i64(&trino), first_i64(&sr));
}

#[tokio::test]
#[ignore = "requires Trino + StarRocks + Lakekeeper — run with: make test-e2e"]
async fn iceberg_cross_engine_orders_count_matches() {
    require_group!(GROUP_LAKEKEEPER);
    require_group!(GROUP_TRINO);
    require_group!(GROUP_STARROCKS);

    let c = client();
    let sql = "SELECT COUNT(*) AS n FROM lakekeeper.tpch.orders";

    let trino = c.execute_on(sql, GROUP_TRINO).await.expect("trino");
    let sr = c.execute_on(sql, GROUP_STARROCKS).await.expect("starrocks");

    assert!(trino.error.is_none(), "trino error: {:?}", trino.error);
    assert!(sr.error.is_none(), "starrocks error: {:?}", sr.error);

    assert_eq!(first_i64(&trino), 15000);
    assert_eq!(first_i64(&sr), 15000);
}

#[tokio::test]
#[ignore = "requires Trino + Lakekeeper — run with: make test-e2e"]
async fn tpch_trino_iceberg_nation_count() {
    require_group!(GROUP_LAKEKEEPER);
    require_group!(GROUP_TRINO);
    let r = client()
        .execute_on(
            "SELECT COUNT(*) AS n FROM lakekeeper.tpch.nation",
            GROUP_TRINO,
        )
        .await
        .expect("query");
    assert!(r.error.is_none(), "error: {:?}", r.error);
    assert_eq!(first_i64(&r), 25);
}

#[tokio::test]
#[ignore = "requires Trino + Lakekeeper — run with: make test-e2e"]
async fn tpch_trino_iceberg_orders_aggregation() {
    require_group!(GROUP_LAKEKEEPER);
    require_group!(GROUP_TRINO);
    let r = client()
        .execute_on(
            "SELECT COUNT(*) AS n FROM lakekeeper.tpch.orders WHERE o_orderstatus = 'O'",
            GROUP_TRINO,
        )
        .await
        .expect("query");
    assert!(r.error.is_none(), "error: {:?}", r.error);
    let n = first_i64(&r);
    assert!(n > 0 && n <= 15000);
}

#[tokio::test]
#[ignore = "requires Trino + Lakekeeper — run with: make test-e2e"]
async fn tpch_trino_iceberg_orders_total_price() {
    require_group!(GROUP_LAKEKEEPER);
    require_group!(GROUP_TRINO);
    let r = client()
        .execute_on(
            "SELECT SUM(o_totalprice) AS total FROM lakekeeper.tpch.orders",
            GROUP_TRINO,
        )
        .await
        .expect("query");
    assert!(r.error.is_none(), "error: {:?}", r.error);
    assert_eq!(r.rows.len(), 1);
}

#[tokio::test]
#[ignore = "requires StarRocks + Lakekeeper — run with: make test-e2e"]
async fn tpch_starrocks_iceberg_nation_count() {
    require_group!(GROUP_LAKEKEEPER);
    require_group!(GROUP_STARROCKS);
    let r = client()
        .execute_on(
            "SELECT COUNT(*) AS n FROM lakekeeper.tpch.nation",
            GROUP_STARROCKS,
        )
        .await
        .expect("query");
    assert!(r.error.is_none(), "error: {:?}", r.error);
    assert_eq!(first_i64(&r), 25);
}

/// Full-table scans on StarRocks Iceberg are slow; run both checks in one test so they do not
/// compete with each other when `cargo test` runs tests in parallel.
#[tokio::test]
#[ignore = "requires StarRocks + Lakekeeper — run with: make test-e2e"]
async fn tpch_starrocks_iceberg_orders_aggregation_and_sum() {
    require_group!(GROUP_LAKEKEEPER);
    require_group!(GROUP_STARROCKS);
    let c = client();

    let r = c
        .execute_on(
            "SELECT COUNT(*) AS n FROM lakekeeper.tpch.orders WHERE o_orderstatus = 'O'",
            GROUP_STARROCKS,
        )
        .await
        .expect("count query");
    assert!(r.error.is_none(), "error: {:?}", r.error);
    let n = first_i64(&r);
    assert!(n > 0 && n <= 15000);

    let r = c
        .execute_on(
            "SELECT SUM(o_totalprice) AS total FROM lakekeeper.tpch.orders",
            GROUP_STARROCKS,
        )
        .await
        .expect("sum query");
    assert!(r.error.is_none(), "error: {:?}", r.error);
    assert_eq!(r.rows.len(), 1);
}
