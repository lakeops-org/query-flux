/// Iceberg / Lakekeeper — Trino + StarRocks (shared `lakekeeper.e2e.*`, seeded in-process).
///
/// Requires docker-compose stack + `make test-e2e` (or `--include-ignored`).
/// DuckDB is not used in the e2e harness.
use std::sync::OnceLock;
use std::time::Duration;

use reqwest::Url;
use trino_rust_client::client::{Client as TrinoHttpClient, ClientBuilder};
use trino_rust_client::types::Row;
use trino_rust_client::Trino;

use queryflux_e2e_tests::harness::{TestHarness, GROUP_LAKEKEEPER, GROUP_STARROCKS, GROUP_TRINO};
use queryflux_e2e_tests::iceberg_seed::ensure_iceberg_e2e_data;

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

/// Per HTTP request timeout for `trino-rust-client` (default is 30s — too short for StarRocks Iceberg scans).
const TRINO_CLIENT_HTTP_TIMEOUT: Duration = Duration::from_secs(600);

fn trino_client_for_group(group: &str) -> TrinoHttpClient {
    // QueryFlux exposes its Trino-compatible HTTP frontend at `harness().base_url()`.
    // The real Trino client sets `X-Trino-Client-Tags`, which we use for QueryFlux routing.
    let base_url = harness().base_url();
    let u = Url::parse(&base_url).expect("base_url parse");
    let host = u.host_str().unwrap_or("127.0.0.1").to_string();
    let port = u.port().unwrap_or(80);

    ClientBuilder::new("test", host)
        .port(port)
        .client_tag(group.to_string())
        .client_request_timeout(TRINO_CLIENT_HTTP_TIMEOUT)
        .build()
        .expect("build trino-rust-client")
}

macro_rules! require_group {
    ($group:expr) => {
        if !harness().has_group($group) {
            eprintln!("SKIP: engine group '{}' not available", $group);
            return;
        }
    };
}

fn first_cell_as_i64(v: &serde_json::Value) -> i64 {
    match v {
        serde_json::Value::Number(n) => n.as_i64().unwrap_or(0),
        serde_json::Value::String(s) => s.parse().unwrap_or(0),
        _ => 0,
    }
}

fn first_cell_as_f64(v: &serde_json::Value) -> f64 {
    match v {
        serde_json::Value::Number(n) => n.as_f64().unwrap_or(0.0),
        serde_json::Value::String(s) => s.parse().unwrap_or(0.0),
        _ => 0.0,
    }
}

async fn execute_on_scalar_i64(client: &TrinoHttpClient, sql: &str, timeout: Duration) -> i64 {
    let rows = tokio::time::timeout(timeout, client.get_all::<Row>(sql.to_string()))
        .await
        .expect("query timed out")
        .expect("query failed");

    assert_eq!(rows.len(), 1, "expected one row; rows={}", rows.len());
    let row0 = &rows.as_slice()[0];
    let cells = row0.value();
    assert!(
        !cells.is_empty(),
        "expected at least one column in scalar query; got {} columns",
        cells.len()
    );
    first_cell_as_i64(&cells[0])
}

async fn execute_on_scalar_f64(client: &TrinoHttpClient, sql: &str, timeout: Duration) -> f64 {
    let rows = tokio::time::timeout(timeout, client.get_all::<Row>(sql.to_string()))
        .await
        .expect("query timed out")
        .expect("query failed");

    assert_eq!(rows.len(), 1, "expected one row; rows={}", rows.len());
    let row0 = &rows.as_slice()[0];
    let cells = row0.value();
    assert!(
        !cells.is_empty(),
        "expected at least one column in scalar query; got {} columns",
        cells.len()
    );
    first_cell_as_f64(&cells[0])
}

#[tokio::test]
#[ignore = "requires Trino + StarRocks + Lakekeeper — run with: make test-e2e"]
async fn iceberg_cross_engine_orders_count_matches() {
    require_group!(GROUP_LAKEKEEPER);
    require_group!(GROUP_TRINO);
    require_group!(GROUP_STARROCKS);
    ensure_iceberg_e2e_data().await.expect("iceberg e2e seed");

    let sql = "SELECT COUNT(*) AS n FROM lakekeeper.e2e.orders";

    let timeout = Duration::from_secs(240);
    let trino_client = trino_client_for_group(GROUP_TRINO);
    let sr_client = trino_client_for_group(GROUP_STARROCKS);

    let (trino_n, sr_n) = tokio::join!(
        execute_on_scalar_i64(&trino_client, sql, timeout),
        execute_on_scalar_i64(&sr_client, sql, timeout),
    );

    assert!(trino_n > 0, "trino orders count");
    assert_eq!(trino_n, sr_n, "Trino vs StarRocks total orders must match");
}

/// Cross-engine compatibility (joins + aggregates). StarRocks Iceberg is slow; keep as one test
/// with stderr progress. Run e2e with `--test-threads=1` (Makefile / CI) so tests do not
/// stampede StarRocks and so libtest does not attribute **mutex wait** from `#[serial]` as runtime.
#[tokio::test]
#[ignore = "requires Trino + StarRocks + Lakekeeper — run with: make test-e2e"]
async fn iceberg_cross_engine_advanced_compatibility_matches() {
    require_group!(GROUP_LAKEKEEPER);
    require_group!(GROUP_TRINO);
    require_group!(GROUP_STARROCKS);
    ensure_iceberg_e2e_data().await.expect("iceberg e2e seed");

    let timeout = Duration::from_secs(240);
    let trino_client = trino_client_for_group(GROUP_TRINO);
    let sr_client = trino_client_for_group(GROUP_STARROCKS);

    let sql_1 = "SELECT COUNT(*) AS n FROM lakekeeper.e2e.orders WHERE o_orderstatus = 'O'";
    let sql_2 =
        "SELECT SUM(o_totalprice) AS total FROM lakekeeper.e2e.orders WHERE o_orderstatus = 'O'";
    let sql_3 = r#"
        SELECT COUNT(*) AS n
        FROM lakekeeper.e2e.customer c
        JOIN lakekeeper.e2e.orders o ON c.c_custkey = o.o_custkey
        WHERE c.c_nationkey = 1 AND o.o_orderstatus = 'O'
    "#;
    let sql_4 = r#"
        SELECT COUNT(*) AS n
        FROM lakekeeper.e2e.orders o
        JOIN lakekeeper.e2e.lineitem l ON o.o_orderkey = l.l_orderkey
        WHERE o.o_orderstatus = 'O' AND l.l_shipmode = 'MAIL'
    "#;
    let sql_5 = r#"
        SELECT SUM(l_quantity) AS total_qty
        FROM lakekeeper.e2e.lineitem
        WHERE l_returnflag = 'R'
    "#;

    eprintln!("[iceberg e2e] launching all Trino + StarRocks queries in parallel");

    let (t1, t2, t3, t4, t5, s1, s2, s3, s4, s5) = tokio::join!(
        execute_on_scalar_i64(&trino_client, sql_1, timeout),
        execute_on_scalar_f64(&trino_client, sql_2, timeout),
        execute_on_scalar_i64(&trino_client, sql_3, timeout),
        execute_on_scalar_i64(&trino_client, sql_4, timeout),
        execute_on_scalar_i64(&trino_client, sql_5, timeout),
        execute_on_scalar_i64(&sr_client, sql_1, timeout),
        execute_on_scalar_f64(&sr_client, sql_2, timeout),
        execute_on_scalar_i64(&sr_client, sql_3, timeout),
        execute_on_scalar_i64(&sr_client, sql_4, timeout),
        execute_on_scalar_i64(&sr_client, sql_5, timeout),
    );

    eprintln!("[iceberg e2e] 1/5 filtered orders count");
    assert_eq!(
        t1, s1,
        "filtered orders count: Trino={} StarRocks={}",
        t1, s1
    );

    eprintln!("[iceberg e2e] 2/5 filtered orders sum");
    let diff = (t2 - s2).abs();
    let tol = 1e-4_f64 * t2.abs().max(s2.abs()).max(1.0);
    assert!(
        diff <= tol,
        "expected close sum totals; trino={}, starrocks={}, diff={}, tol={}",
        t2,
        s2,
        diff,
        tol
    );

    eprintln!("[iceberg e2e] 3/5 customer–orders join count");
    assert_eq!(
        t3, s3,
        "customer-orders join: Trino={} StarRocks={}",
        t3, s3
    );

    eprintln!("[iceberg e2e] 4/5 orders–lineitem join count");
    assert_eq!(
        t4, s4,
        "orders-lineitem join: Trino={} StarRocks={}",
        t4, s4
    );

    eprintln!("[iceberg e2e] 5/5 lineitem return-flag sum");
    assert_eq!(
        t5, s5,
        "lineitem return-flag sum: Trino={} StarRocks={}",
        t5, s5
    );
}

#[tokio::test]
#[ignore = "requires Trino + Lakekeeper — run with: make test-e2e"]
async fn iceberg_e2e_trino_nation_count() {
    require_group!(GROUP_LAKEKEEPER);
    require_group!(GROUP_TRINO);
    ensure_iceberg_e2e_data().await.expect("iceberg e2e seed");
    let timeout = Duration::from_secs(120);
    let trino_client = trino_client_for_group(GROUP_TRINO);
    let sql = "SELECT COUNT(*) AS n FROM lakekeeper.e2e.nation";
    let n = execute_on_scalar_i64(&trino_client, sql, timeout).await;
    assert_eq!(n, 3);
}

#[tokio::test]
#[ignore = "requires Trino + Lakekeeper — run with: make test-e2e"]
async fn iceberg_e2e_trino_orders_open_count() {
    require_group!(GROUP_LAKEKEEPER);
    require_group!(GROUP_TRINO);
    ensure_iceberg_e2e_data().await.expect("iceberg e2e seed");
    let timeout = Duration::from_secs(120);
    let trino_client = trino_client_for_group(GROUP_TRINO);
    let sql = "SELECT COUNT(*) AS n FROM lakekeeper.e2e.orders WHERE o_orderstatus = 'O'";
    let n = execute_on_scalar_i64(&trino_client, sql, timeout).await;
    assert_eq!(n, 3);
}

#[tokio::test]
#[ignore = "requires Trino + Lakekeeper — run with: make test-e2e"]
async fn iceberg_e2e_trino_orders_total_price() {
    require_group!(GROUP_LAKEKEEPER);
    require_group!(GROUP_TRINO);
    ensure_iceberg_e2e_data().await.expect("iceberg e2e seed");
    let timeout = Duration::from_secs(120);
    let trino_client = trino_client_for_group(GROUP_TRINO);
    let sql = "SELECT SUM(o_totalprice) AS total FROM lakekeeper.e2e.orders";
    let total = execute_on_scalar_f64(&trino_client, sql, timeout).await;
    assert!((total - 184.5).abs() < 1e-3, "sum all orders: got {total}");
}

#[tokio::test]
#[ignore = "requires StarRocks + Lakekeeper — run with: make test-e2e"]
async fn iceberg_e2e_starrocks_nation_count() {
    require_group!(GROUP_LAKEKEEPER);
    require_group!(GROUP_STARROCKS);
    ensure_iceberg_e2e_data().await.expect("iceberg e2e seed");
    let timeout = Duration::from_secs(120);
    let sr_client = trino_client_for_group(GROUP_STARROCKS);
    let sql = "SELECT COUNT(*) AS n FROM lakekeeper.e2e.nation";
    let n = execute_on_scalar_i64(&sr_client, sql, timeout).await;
    assert_eq!(n, 3);
}

/// Full-table scans on StarRocks Iceberg are slow; run both checks in one test so they do not
/// compete with each other when `cargo test` runs tests in parallel.
#[tokio::test]
#[ignore = "requires StarRocks + Lakekeeper — run with: make test-e2e"]
async fn iceberg_e2e_starrocks_orders_open_count_and_sum() {
    require_group!(GROUP_LAKEKEEPER);
    require_group!(GROUP_STARROCKS);
    ensure_iceberg_e2e_data().await.expect("iceberg e2e seed");
    let timeout = Duration::from_secs(240);
    let sr_client = trino_client_for_group(GROUP_STARROCKS);

    let sql_count = "SELECT COUNT(*) AS n FROM lakekeeper.e2e.orders WHERE o_orderstatus = 'O'";
    let n = execute_on_scalar_i64(&sr_client, sql_count, timeout).await;
    assert_eq!(n, 3);

    let sql_sum = "SELECT SUM(o_totalprice) AS total FROM lakekeeper.e2e.orders";
    let total = execute_on_scalar_f64(&sr_client, sql_sum, timeout).await;
    assert!((total - 184.5).abs() < 1e-3, "sum all orders: got {total}");
}
