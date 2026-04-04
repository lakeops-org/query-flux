//! `snowflake-connector-rs` against **fakesnow** (same stack as `snowflake_tests.rs` via QueryFlux
//! adapter). Exercises `session::query`, `execute` + `fetch_all`, chunked `fetch_next_chunk`, and
//! `QueryExecutor::snowflake_columns` on an empty rowset.
//!
//! Run with fakesnow up (e.g. docker compose) and:
//!   cargo test -p queryflux-e2e-tests --test snowflake_connector_rs_tests -- --include-ignored

use std::sync::OnceLock;

use queryflux_e2e_tests::{
    harness::{TestHarness, GROUP_SNOWFLAKE},
    snowflake_rs_client::fakesnow_session,
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

macro_rules! require_snowflake {
    () => {
        if !harness().has_group(GROUP_SNOWFLAKE) {
            eprintln!(
                "SKIP: fakesnow not available (set FAKESNOW_URL or start docker/test/docker-compose.test.yml)"
            );
            return;
        }
    };
}

#[tokio::test]
#[ignore = "requires fakesnow — run with: make test-e2e"]
async fn snowflake_rs_session_query_fetchall() {
    require_snowflake!();
    let session = fakesnow_session().await.expect("fakesnow_session");
    let rows = session
        .query("SELECT 1 + 1 AS result")
        .await
        .expect("query");
    assert_eq!(rows.len(), 1, "expected one row");
    let v: i64 = rows[0].get("RESULT").expect("RESULT column");
    assert_eq!(v, 2);
}

#[tokio::test]
#[ignore = "requires fakesnow — run with: make test-e2e"]
async fn snowflake_rs_execute_fetch_all() {
    require_snowflake!();
    let session = fakesnow_session().await.expect("fakesnow_session");
    let sql = "SELECT 'rs-fetch-all' AS label";
    let executor = session.execute(sql).await.expect("execute");
    let rows = executor.fetch_all().await.expect("fetch_all");
    assert_eq!(rows.len(), 1);
    let label: String = rows[0].get("LABEL").expect("LABEL");
    assert_eq!(label, "rs-fetch-all");
}

#[tokio::test]
#[ignore = "requires fakesnow — run with: make test-e2e"]
async fn snowflake_rs_execute_fetch_next_chunk_matches_fetch_all_row_count() {
    require_snowflake!();
    let session = fakesnow_session().await.expect("fakesnow_session");
    let sql = "SELECT 1 AS v UNION ALL SELECT 2 UNION ALL SELECT 3";

    let all = {
        let ex = session.execute(sql).await.expect("execute");
        ex.fetch_all().await.expect("fetch_all")
    };
    assert_eq!(all.len(), 3);

    let ex2 = session.execute(sql).await.expect("execute");
    let mut total = 0usize;
    loop {
        let chunk = ex2.fetch_next_chunk().await.expect("fetch_next_chunk");
        let Some(part) = chunk else { break };
        total += part.len();
    }
    assert_eq!(total, 3, "sum of chunk row counts should match fetch_all");
}

#[tokio::test]
#[ignore = "requires fakesnow — run with: make test-e2e"]
async fn snowflake_rs_snowflake_columns_when_no_rows() {
    require_snowflake!();
    let session = fakesnow_session().await.expect("fakesnow_session");
    let executor = session
        .execute("SELECT 99 AS n WHERE 1 = 0")
        .await
        .expect("execute");

    let cols = executor.snowflake_columns();
    assert_eq!(
        cols.len(),
        1,
        "metadata must be present even when rowset is empty"
    );
    assert_eq!(cols[0].name(), "N");

    let rows = executor.fetch_all().await.expect("fetch_all");
    assert!(rows.is_empty(), "no data rows expected for WHERE 1 = 0");
}
