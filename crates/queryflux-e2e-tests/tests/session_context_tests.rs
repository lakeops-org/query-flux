/// End-to-end tests for `SessionContext` metadata propagation.
///
/// Each test sends a Trino HTTP request through the in-process harness and
/// asserts that the metadata extracted by the frontend (user, catalog/database)
/// reaches the `QueryRecord` produced by the metrics store.
///
/// These tests use DuckDB as the engine — no external backend required.
/// Run: `cargo test -p queryflux-e2e-tests --test session_context_tests`
use std::sync::OnceLock;

use queryflux_e2e_tests::{harness::TestHarness, trino_client::TrinoClient};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};

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

/// Execute `sql` with an explicit set of headers (no defaults injected).
async fn execute_raw(
    base_url: &str,
    sql: &str,
    headers: &[(&str, &str)],
) -> anyhow::Result<reqwest::Response> {
    let mut hmap = HeaderMap::new();
    for (k, v) in headers {
        hmap.insert(
            HeaderName::from_bytes(k.as_bytes())?,
            HeaderValue::from_str(v)?,
        );
    }
    let resp = reqwest::Client::new()
        .post(format!("{base_url}/v1/statement"))
        .headers(hmap)
        .body(sql.to_string())
        .send()
        .await?;
    Ok(resp)
}

// ---------------------------------------------------------------------------
// User propagation
// ---------------------------------------------------------------------------

#[tokio::test]
async fn session_user_from_x_trino_user_header() {
    harness().clear_records();
    let client = TrinoClient::new(&harness().base_url());
    let r = client
        .execute(
            "SELECT 1",
            &[("x-trino-user", "alice"), ("x-qf-group", "duckdb")],
        )
        .await
        .expect("query");
    assert!(r.error.is_none(), "unexpected error: {:?}", r.error);

    let record = harness()
        .wait_for_record(|r| r.user.as_deref() == Some("alice"))
        .await;
    assert!(record.is_some(), "expected a QueryRecord with user=alice");
}

#[tokio::test]
async fn session_user_is_last_sent_value_when_multiple_requests() {
    harness().clear_records();
    let client = TrinoClient::new(&harness().base_url());
    let sql = "SELECT 99";

    // Two concurrent-ish queries with different users.
    client
        .execute(sql, &[("x-trino-user", "bob"), ("x-qf-group", "duckdb")])
        .await
        .expect("bob query");
    client
        .execute(sql, &[("x-trino-user", "carol"), ("x-qf-group", "duckdb")])
        .await
        .expect("carol query");

    let bob = harness()
        .wait_for_record(|r| r.user.as_deref() == Some("bob"))
        .await;
    let carol = harness()
        .wait_for_record(|r| r.user.as_deref() == Some("carol"))
        .await;

    assert!(bob.is_some(), "expected QueryRecord for bob");
    assert!(carol.is_some(), "expected QueryRecord for carol");
}

// ---------------------------------------------------------------------------
// Catalog / database propagation
// ---------------------------------------------------------------------------

#[tokio::test]
async fn session_catalog_from_x_trino_catalog_header() {
    harness().clear_records();
    let client = TrinoClient::new(&harness().base_url());
    let r = client
        .execute(
            "SELECT 1",
            &[
                ("x-trino-user", "test"),
                ("x-trino-catalog", "my_catalog"),
                ("x-qf-group", "duckdb"),
            ],
        )
        .await
        .expect("query");
    assert!(r.error.is_none(), "unexpected error: {:?}", r.error);

    let record = harness()
        .wait_for_record(|r| r.catalog.as_deref() == Some("my_catalog"))
        .await;
    assert!(
        record.is_some(),
        "expected a QueryRecord with catalog=my_catalog"
    );
}

#[tokio::test]
async fn session_catalog_is_none_when_header_absent() {
    harness().clear_records();
    // Send a recognisable SQL so we can find the right record.
    let unique_sql = "SELECT 777 AS session_no_catalog_marker";
    let client = TrinoClient::new(&harness().base_url());
    let r = client
        .execute(
            unique_sql,
            &[("x-trino-user", "test"), ("x-qf-group", "duckdb")],
        )
        .await
        .expect("query");
    assert!(r.error.is_none(), "unexpected error: {:?}", r.error);

    let record = harness()
        .wait_for_record(|r| {
            r.sql_preview.contains("777") && r.sql_preview.contains("session_no_catalog_marker")
        })
        .await;
    let record = record.expect("record for no-catalog query");
    assert_eq!(
        record.catalog, None,
        "catalog should be None when X-Trino-Catalog is absent"
    );
}

// ---------------------------------------------------------------------------
// Extra headers pass through without breaking the request
// ---------------------------------------------------------------------------

#[tokio::test]
async fn session_unknown_extra_headers_do_not_break_query() {
    // Verifies that arbitrary extra headers stored in `extra` don't cause a panic
    // or routing failure — they're just ignored by components that don't know them.
    let client = TrinoClient::new(&harness().base_url());
    let r = client
        .execute(
            "SELECT 2",
            &[
                ("x-trino-user", "test"),
                ("x-custom-app-header", "some-value"),
                ("x-qf-group", "duckdb"),
            ],
        )
        .await
        .expect("query");
    assert!(r.error.is_none(), "unexpected error: {:?}", r.error);
}

#[tokio::test]
async fn session_missing_user_header_query_still_succeeds() {
    // A request with no X-Trino-User is valid — session.user is None.
    let resp = execute_raw(
        &harness().base_url(),
        "SELECT 3",
        &[("x-qf-group", "duckdb")],
    )
    .await
    .expect("raw request");
    assert!(
        resp.status().is_success(),
        "expected 2xx even without X-Trino-User, got {}",
        resp.status()
    );
}
