/// Snowflake tests — require a running fakesnow instance (https://github.com/tekumara/fakesnow).
///
/// All tests are marked `#[ignore]` and run with: make test-e2e
/// or: cargo test -p queryflux-e2e-tests --test snowflake_tests -- --include-ignored
use std::sync::OnceLock;

use queryflux_e2e_tests::{
    harness::{TestHarness, GROUP_SNOWFLAKE},
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

macro_rules! require_snowflake {
    () => {
        if !harness().has_group(GROUP_SNOWFLAKE) {
            eprintln!(
                "SKIP: fakesnow not available (start with docker/test/docker-compose.test.yml)"
            );
            return;
        }
    };
}

// ---------------------------------------------------------------------------
// Basic queries
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires fakesnow — run with: make test-e2e"]
async fn snowflake_select_literal() {
    require_snowflake!();
    let r = client()
        .execute_on("SELECT 1 + 1 AS result", GROUP_SNOWFLAKE)
        .await
        .expect("query");
    assert!(r.error.is_none(), "unexpected error: {:?}", r.error);
    assert_eq!(r.rows.len(), 1);
}

#[tokio::test]
#[ignore = "requires fakesnow — run with: make test-e2e"]
async fn snowflake_select_string_literal() {
    require_snowflake!();
    let r = client()
        .execute_on("SELECT 'hello fakesnow' AS greeting", GROUP_SNOWFLAKE)
        .await
        .expect("query");
    assert!(r.error.is_none(), "unexpected error: {:?}", r.error);
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0].as_str(), Some("hello fakesnow"));
}

#[tokio::test]
#[ignore = "requires fakesnow — run with: make test-e2e"]
async fn snowflake_select_multi_row() {
    require_snowflake!();
    let r = client()
        .execute_on(
            "SELECT 1 AS v UNION ALL SELECT 2 UNION ALL SELECT 3",
            GROUP_SNOWFLAKE,
        )
        .await
        .expect("query");
    assert!(r.error.is_none(), "unexpected error: {:?}", r.error);
    assert_eq!(r.rows.len(), 3);
}

#[tokio::test]
#[ignore = "requires fakesnow — run with: make test-e2e"]
async fn snowflake_empty_result() {
    require_snowflake!();
    let r = client()
        .execute_on("SELECT 1 AS n WHERE 1 = 0", GROUP_SNOWFLAKE)
        .await
        .expect("query");
    assert!(r.error.is_none(), "unexpected error: {:?}", r.error);
    assert_eq!(r.rows.len(), 0);
}

// ---------------------------------------------------------------------------
// DDL + DML (fakesnow supports CREATE TABLE, INSERT, etc.)
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires fakesnow — run with: make test-e2e"]
async fn snowflake_create_and_query_table() {
    require_snowflake!();
    let c = client();

    c.execute_on("CREATE DATABASE IF NOT EXISTS e2e_db", GROUP_SNOWFLAKE)
        .await
        .expect("create database");

    c.execute_on(
        "CREATE SCHEMA IF NOT EXISTS e2e_db.e2e_schema",
        GROUP_SNOWFLAKE,
    )
    .await
    .expect("create schema");

    c.execute_on(
        "CREATE OR REPLACE TABLE e2e_db.e2e_schema.test_tbl (id INTEGER, name VARCHAR)",
        GROUP_SNOWFLAKE,
    )
    .await
    .expect("create table");

    c.execute_on(
        "INSERT INTO e2e_db.e2e_schema.test_tbl VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie')",
        GROUP_SNOWFLAKE,
    )
    .await
    .expect("insert");

    let r = c
        .execute_on(
            "SELECT id, name FROM e2e_db.e2e_schema.test_tbl ORDER BY id",
            GROUP_SNOWFLAKE,
        )
        .await
        .expect("select");
    assert!(r.error.is_none(), "unexpected error: {:?}", r.error);
    assert_eq!(r.rows.len(), 3);
    assert_eq!(r.rows[0][1].as_str(), Some("alice"));
    assert_eq!(r.rows[2][1].as_str(), Some("charlie"));
}

// ---------------------------------------------------------------------------
// Type mapping
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires fakesnow — run with: make test-e2e"]
async fn snowflake_numeric_types() {
    require_snowflake!();
    let r = client()
        .execute_on(
            "SELECT 42 AS int_val, 3.14 AS float_val, TRUE AS bool_val",
            GROUP_SNOWFLAKE,
        )
        .await
        .expect("query");
    assert!(r.error.is_none(), "unexpected error: {:?}", r.error);
    assert_eq!(r.rows.len(), 1);
}

// ---------------------------------------------------------------------------
// Aggregations
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires fakesnow — run with: make test-e2e"]
async fn snowflake_aggregation() {
    require_snowflake!();
    let r = client()
        .execute_on(
            "SELECT COUNT(*) AS cnt, SUM(v) AS total FROM (SELECT 10 AS v UNION ALL SELECT 20 UNION ALL SELECT 30) t",
            GROUP_SNOWFLAKE,
        )
        .await
        .expect("query");
    assert!(r.error.is_none(), "unexpected error: {:?}", r.error);
    assert_eq!(r.rows.len(), 1);
}

// ---------------------------------------------------------------------------
// Metrics capture
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires fakesnow — run with: make test-e2e"]
async fn snowflake_query_recorded_in_metrics() {
    require_snowflake!();
    let h = harness();
    h.clear_records();
    let c = client();
    c.execute_on("SELECT 999 AS metric_test", GROUP_SNOWFLAKE)
        .await
        .expect("query");

    let record = h
        .wait_for_record(|r| r.cluster_group.0 == GROUP_SNOWFLAKE)
        .await;
    assert!(
        record.is_some(),
        "expected a query record for the snowflake group"
    );
}
