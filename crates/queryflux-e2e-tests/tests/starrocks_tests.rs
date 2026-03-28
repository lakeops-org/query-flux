/// StarRocks tests — require a running StarRocks instance.
///
/// All tests are marked `#[ignore]` and run with: make test-e2e
/// or: cargo test -p queryflux-e2e-tests --test starrocks_tests -- --include-ignored
use std::sync::OnceLock;

use queryflux_e2e_tests::{
    harness::{TestHarness, GROUP_STARROCKS, GROUP_TRINO},
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

// ---------------------------------------------------------------------------
// Basic StarRocks
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires StarRocks — run with: make test-e2e"]
async fn starrocks_select_literal() {
    require_group!(GROUP_STARROCKS);
    let r = client()
        .execute_on("SELECT 1 + 1 AS result", GROUP_STARROCKS)
        .await
        .expect("query");
    assert!(r.error.is_none(), "unexpected error: {:?}", r.error);
    assert_eq!(r.rows.len(), 1);
}

#[tokio::test]
#[ignore = "requires StarRocks — run with: make test-e2e"]
async fn starrocks_select_multi_row() {
    require_group!(GROUP_STARROCKS);
    let r = client()
        .execute_on(
            "SELECT 1 AS v UNION ALL SELECT 2 UNION ALL SELECT 3",
            GROUP_STARROCKS,
        )
        .await
        .expect("query");
    assert!(r.error.is_none(), "unexpected error: {:?}", r.error);
    assert_eq!(r.rows.len(), 3);
}

#[tokio::test]
#[ignore = "requires StarRocks — run with: make test-e2e"]
async fn starrocks_syntax_error_returns_error() {
    require_group!(GROUP_STARROCKS);
    let r = client()
        .execute_on("THIS IS NOT VALID SQL FOR STARROCKS", GROUP_STARROCKS)
        .await
        .expect("query");
    assert!(r.error.is_some(), "expected error for invalid SQL");
}

#[tokio::test]
#[ignore = "requires StarRocks — run with: make test-e2e"]
async fn starrocks_empty_result() {
    require_group!(GROUP_STARROCKS);
    let r = client()
        .execute_on("SELECT 1 AS n WHERE 1 = 0", GROUP_STARROCKS)
        .await
        .expect("query");
    assert!(r.error.is_none(), "unexpected error: {:?}", r.error);
    assert_eq!(r.rows.len(), 0);
}

// ---------------------------------------------------------------------------
// Cross-engine routing
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires Trino + StarRocks — run with: make test-e2e"]
async fn routing_same_sql_trino_and_starrocks() {
    require_group!(GROUP_TRINO);
    require_group!(GROUP_STARROCKS);
    let c = client();
    let sql = "SELECT 1 + 1 AS result";

    let trino = c.execute_on(sql, GROUP_TRINO).await.expect("trino");
    let sr = c.execute_on(sql, GROUP_STARROCKS).await.expect("starrocks");

    assert!(trino.error.is_none(), "trino error: {:?}", trino.error);
    assert!(sr.error.is_none(), "starrocks error: {:?}", sr.error);
    assert_eq!(trino.rows.len(), 1);
    assert_eq!(sr.rows.len(), 1);
    assert_eq!(trino.rows[0][0], sr.rows[0][0]);
}

#[tokio::test]
#[ignore = "requires Trino + StarRocks — run with: make test-e2e"]
async fn routing_literal_trino_and_starrocks() {
    require_group!(GROUP_TRINO);
    require_group!(GROUP_STARROCKS);
    let c = client();
    let sql = "SELECT 7 AS n";

    for group in [GROUP_TRINO, GROUP_STARROCKS] {
        let r = c
            .execute_on(sql, group)
            .await
            .unwrap_or_else(|e| panic!("query on {group} failed: {e}"));
        assert!(r.error.is_none(), "{group} returned error: {:?}", r.error);
        assert_eq!(r.rows.len(), 1, "{group} should return 1 row");
    }
}
