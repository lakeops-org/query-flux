/// Iceberg / Lakekeeper tests — shared TPC-H catalog across all engines.
///
/// All tests are marked `#[ignore]` and require the full docker-compose stack:
///   make test-e2e
///
/// Data is loaded by the data-loader container:
///   lakekeeper.tpch.customer  — 1 500 rows  (tpch.tiny = SF 0.01)
///   lakekeeper.tpch.orders    — 15 000 rows
///   lakekeeper.tpch.nation    — 25 rows
///   lakekeeper.tpch.region    — 5 rows
///
/// All three engines query the same `lakekeeper.tpch.*` three-part name.
///
/// NOTE: DuckDB runs in-process on the host. Lakekeeper embeds its internal
/// Docker endpoint (http://minio:9000) in REST catalog responses, so DuckDB
/// cannot reach MinIO for data reads. DuckDB Iceberg tests skip gracefully
/// when that hostname is unreachable.
use std::sync::OnceLock;

use queryflux_e2e_tests::{
    harness::{TestHarness, GROUP_DUCKDB, GROUP_LAKEKEEPER, GROUP_STARROCKS, GROUP_TRINO},
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

fn is_docker_hostname_error(err: &str) -> bool {
    err.contains("Could not resolve hostname") || err.contains("Name or service not known")
}

// ---------------------------------------------------------------------------
// Existing cross-engine count tests (moved from e2e_tests.rs)
// ---------------------------------------------------------------------------

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
#[ignore = "requires Lakekeeper — run with: make test-e2e"]
async fn iceberg_duckdb_customer_count() {
    require_group!(GROUP_LAKEKEEPER);
    let r = client()
        .execute_on(
            "SELECT COUNT(*) AS n FROM lakekeeper.tpch.customer",
            GROUP_DUCKDB,
        )
        .await
        .expect("query");
    if let Some(ref err) = r.error {
        if is_docker_hostname_error(err) {
            eprintln!("SKIP iceberg_duckdb_customer_count: DuckDB can't reach Docker-internal MinIO ({err})");
            return;
        }
        panic!("unexpected duckdb error: {err}");
    }
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
    let duck = c.execute_on(sql, GROUP_DUCKDB).await.expect("duckdb");
    let sr = c.execute_on(sql, GROUP_STARROCKS).await.expect("starrocks");

    assert!(trino.error.is_none(), "trino error: {:?}", trino.error);
    assert!(sr.error.is_none(), "starrocks error: {:?}", sr.error);

    let trino_n = first_i64(&trino);
    let sr_n = first_i64(&sr);
    assert_eq!(trino_n, sr_n, "trino vs starrocks customer count mismatch");

    if let Some(ref err) = duck.error {
        if is_docker_hostname_error(err) {
            eprintln!("NOTE: DuckDB Iceberg skipped (Docker hostname unreachable): {err}");
        } else {
            panic!("unexpected duckdb error: {err}");
        }
    } else {
        assert_eq!(first_i64(&duck), trino_n, "duckdb vs trino customer count mismatch");
    }
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
    let duck = c.execute_on(sql, GROUP_DUCKDB).await.expect("duckdb");
    let sr = c.execute_on(sql, GROUP_STARROCKS).await.expect("starrocks");

    assert!(trino.error.is_none(), "trino error: {:?}", trino.error);
    assert!(sr.error.is_none(), "starrocks error: {:?}", sr.error);

    assert_eq!(first_i64(&trino), 15000, "trino orders count mismatch");
    assert_eq!(first_i64(&sr), 15000, "starrocks orders count mismatch");

    if let Some(ref err) = duck.error {
        if is_docker_hostname_error(err) {
            eprintln!("NOTE: DuckDB Iceberg skipped (Docker hostname unreachable): {err}");
        } else {
            panic!("unexpected duckdb error: {err}");
        }
    } else {
        assert_eq!(first_i64(&duck), 15000, "duckdb orders count mismatch");
    }
}

// ---------------------------------------------------------------------------
// New TPC-H Iceberg tests — aggregations and joins per engine
// (avoids duplicating the existing customer/orders count tests above)
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires Lakekeeper — run with: make test-e2e"]
async fn tpch_duckdb_iceberg_nation_count() {
    require_group!(GROUP_LAKEKEEPER);
    let r = client()
        .execute_on(
            "SELECT COUNT(*) AS n FROM lakekeeper.tpch.nation",
            GROUP_DUCKDB,
        )
        .await
        .expect("query");
    if let Some(ref err) = r.error {
        if is_docker_hostname_error(err) {
            eprintln!("SKIP: DuckDB can't reach Docker-internal MinIO ({err})");
            return;
        }
        panic!("unexpected duckdb error: {err}");
    }
    assert_eq!(first_i64(&r), 25);
}

#[tokio::test]
#[ignore = "requires Lakekeeper — run with: make test-e2e"]
async fn tpch_duckdb_iceberg_orders_aggregation() {
    require_group!(GROUP_LAKEKEEPER);
    let r = client()
        .execute_on(
            "SELECT COUNT(*) AS n FROM lakekeeper.tpch.orders WHERE o_orderstatus = 'O'",
            GROUP_DUCKDB,
        )
        .await
        .expect("query");
    if let Some(ref err) = r.error {
        if is_docker_hostname_error(err) {
            eprintln!("SKIP: DuckDB can't reach Docker-internal MinIO ({err})");
            return;
        }
        panic!("unexpected duckdb error: {err}");
    }
    let n = first_i64(&r);
    assert!(n > 0 && n <= 15000);
}

#[tokio::test]
#[ignore = "requires Lakekeeper — run with: make test-e2e"]
async fn tpch_duckdb_iceberg_customer_nation_join() {
    require_group!(GROUP_LAKEKEEPER);
    let r = client()
        .execute_on(
            "SELECT n_name, COUNT(*) AS cnt \
             FROM lakekeeper.tpch.customer \
             JOIN lakekeeper.tpch.nation ON c_nationkey = n_nationkey \
             GROUP BY n_name ORDER BY cnt DESC LIMIT 5",
            GROUP_DUCKDB,
        )
        .await
        .expect("query");
    if let Some(ref err) = r.error {
        if is_docker_hostname_error(err) {
            eprintln!("SKIP: DuckDB can't reach Docker-internal MinIO ({err})");
            return;
        }
        panic!("unexpected duckdb error: {err}");
    }
    assert_eq!(r.rows.len(), 5);
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

#[tokio::test]
#[ignore = "requires StarRocks + Lakekeeper — run with: make test-e2e"]
async fn tpch_starrocks_iceberg_orders_aggregation() {
    require_group!(GROUP_LAKEKEEPER);
    require_group!(GROUP_STARROCKS);
    let r = client()
        .execute_on(
            "SELECT COUNT(*) AS n FROM lakekeeper.tpch.orders WHERE o_orderstatus = 'O'",
            GROUP_STARROCKS,
        )
        .await
        .expect("query");
    assert!(r.error.is_none(), "error: {:?}", r.error);
    let n = first_i64(&r);
    assert!(n > 0 && n <= 15000);
}

#[tokio::test]
#[ignore = "requires StarRocks + Lakekeeper — run with: make test-e2e"]
async fn tpch_starrocks_iceberg_orders_total_price() {
    require_group!(GROUP_LAKEKEEPER);
    require_group!(GROUP_STARROCKS);
    let r = client()
        .execute_on(
            "SELECT SUM(o_totalprice) AS total FROM lakekeeper.tpch.orders",
            GROUP_STARROCKS,
        )
        .await
        .expect("query");
    assert!(r.error.is_none(), "error: {:?}", r.error);
    assert_eq!(r.rows.len(), 1);
}
