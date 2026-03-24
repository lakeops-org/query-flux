/// Header routing and Trino↔StarRocks targeting via `X-Qf-Group`.
///
/// Requires at least one backend (same as [`TestHarness::new`]). Run with
/// `make test-e2e` or after `docker compose -f docker/docker-compose.test.yml up`.
///
/// Run: cargo test -p queryflux-e2e-tests --test routing_tests
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

#[tokio::test]
async fn unknown_x_qf_group_value_uses_fallback() {
    require_group!(GROUP_TRINO);
    let r = client()
        .execute(
            "SELECT 7 AS n",
            &[("x-qf-group", "not-a-configured-group")],
        )
        .await
        .expect("query");
    assert!(r.error.is_none(), "unexpected error: {:?}", r.error);
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], serde_json::json!(7));
}

#[tokio::test]
async fn x_qf_group_header_routes_to_starrocks_when_available() {
    require_group!(GROUP_STARROCKS);
    let r = client()
        .execute("SELECT 8 AS n", &[("X-Qf-Group", GROUP_STARROCKS)])
        .await
        .expect("query");
    assert!(r.error.is_none(), "unexpected error: {:?}", r.error);
    assert_eq!(r.rows[0][0], serde_json::json!(8));
}

/// Trino HTTP ingress with `X-Qf-Group: trino` must hit the Trino cluster (not StarRocks).
#[tokio::test]
async fn x_qf_group_header_routes_to_trino_when_available() {
    require_group!(GROUP_TRINO);
    let r = client()
        .execute("SELECT 9 AS n", &[("X-Qf-Group", GROUP_TRINO)])
        .await
        .expect("query");
    assert!(r.error.is_none(), "unexpected error: {:?}", r.error);
    assert_eq!(r.rows[0][0], serde_json::json!(9));
}

/// Trino-only catalog: header `trino` succeeds; header `starrocks` must not silently use Trino.
#[tokio::test]
async fn tpch_routed_to_trino_only_when_header_is_trino() {
    require_group!(GROUP_TRINO);
    require_group!(GROUP_STARROCKS);
    let sql = "SELECT COUNT(*) AS n FROM tpch.tiny.nation";
    let c = client();

    let on_trino = c.execute_on(sql, GROUP_TRINO).await.expect("trino");
    assert!(on_trino.error.is_none(), "trino: {:?}", on_trino.error);

    let on_sr = c.execute_on(sql, GROUP_STARROCKS).await.expect("starrocks");
    assert!(
        on_sr.error.is_some(),
        "StarRocks must not resolve Trino tpch catalog; got success: {:?}",
        on_sr.rows
    );
}

/// StarRocks FE introspection: must succeed on `starrocks` group and fail on `trino` (no cross-leak).
#[tokio::test]
async fn starrocks_admin_routed_only_to_starrocks() {
    require_group!(GROUP_TRINO);
    require_group!(GROUP_STARROCKS);
    let sql = "SHOW FRONTENDS";
    let c = client();

    let on_sr = c.execute_on(sql, GROUP_STARROCKS).await.expect("starrocks");
    assert!(
        on_sr.error.is_none(),
        "SHOW FRONTENDS should work on StarRocks: {:?}",
        on_sr.error
    );

    let on_trino = c.execute_on(sql, GROUP_TRINO).await.expect("trino");
    assert!(
        on_trino.error.is_some(),
        "Trino must not execute StarRocks SHOW FRONTENDS; got: {:?}",
        on_trino.rows
    );
}

#[tokio::test]
async fn trino_group_can_query_tpch_catalog() {
    require_group!(GROUP_TRINO);
    let r = client()
        .execute_on("SELECT COUNT(*) AS n FROM tpch.tiny.nation", GROUP_TRINO)
        .await
        .expect("query");
    assert!(r.error.is_none(), "{:?}", r.error);
    let v = &r.rows[0][0];
    let n = match v {
        serde_json::Value::Number(x) => x.as_i64().unwrap_or(0),
        serde_json::Value::String(s) => s.parse().unwrap_or(0),
        _ => 0,
    };
    assert_eq!(n, 25);
}
