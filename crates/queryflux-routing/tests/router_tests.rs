/// Unit tests for all router implementations and RouterChain.
///
/// No engine or HTTP server required — pure routing logic.
/// All tests run with: cargo test -p queryflux-routing
use std::collections::HashMap;

use queryflux_auth::AuthContext;
use queryflux_core::{
    config::{CompoundCombineMode, CompoundCondition},
    query::{ClusterGroupName, FrontendProtocol},
    session::SessionContext,
};
use queryflux_routing::{
    chain::RouterChain,
    implementations::{
        client_tags::ClientTagsRouter, compound::CompoundRouter, header::HeaderRouter,
        protocol_based::ProtocolBasedRouter, python_script::PythonScriptRouter,
        query_regex::QueryRegexRouter,
    },
    RouterTrait,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn trino_session(headers: &[(&str, &str)]) -> SessionContext {
    SessionContext::TrinoHttp {
        headers: headers
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
    }
}

fn postgres_session() -> SessionContext {
    SessionContext::PostgresWire {
        database: None,
        user: Some("alice".to_string()),
        session_params: HashMap::new(),
    }
}

fn group(name: &str) -> ClusterGroupName {
    ClusterGroupName(name.to_string())
}

// ---------------------------------------------------------------------------
// HeaderRouter
// ---------------------------------------------------------------------------

#[tokio::test]
async fn header_router_matches() {
    let router = HeaderRouter::new(
        "x-target".to_string(),
        HashMap::from([("analytics".to_string(), group("analytics-group"))]),
    );
    let session = trino_session(&[("x-target", "analytics")]);
    let result = router
        .route("SELECT 1", &session, &FrontendProtocol::TrinoHttp, None)
        .await
        .unwrap();
    assert_eq!(result, Some(group("analytics-group")));
}

#[tokio::test]
async fn header_router_value_not_in_mapping() {
    let router = HeaderRouter::new(
        "x-target".to_string(),
        HashMap::from([("analytics".to_string(), group("analytics-group"))]),
    );
    let session = trino_session(&[("x-target", "unknown-value")]);
    let result = router
        .route("SELECT 1", &session, &FrontendProtocol::TrinoHttp, None)
        .await
        .unwrap();
    assert_eq!(result, None);
}

#[tokio::test]
async fn header_router_absent() {
    let router = HeaderRouter::new(
        "x-target".to_string(),
        HashMap::from([("analytics".to_string(), group("analytics-group"))]),
    );
    let session = trino_session(&[]); // no headers
    let result = router
        .route("SELECT 1", &session, &FrontendProtocol::TrinoHttp, None)
        .await
        .unwrap();
    assert_eq!(result, None);
}

#[tokio::test]
async fn header_router_non_trino_session() {
    let router = HeaderRouter::new(
        "x-target".to_string(),
        HashMap::from([("analytics".to_string(), group("analytics-group"))]),
    );
    // HeaderRouter only applies to TrinoHttp sessions
    let result = router
        .route(
            "SELECT 1",
            &postgres_session(),
            &FrontendProtocol::PostgresWire,
            None,
        )
        .await
        .unwrap();
    assert_eq!(result, None);
}

// ---------------------------------------------------------------------------
// ProtocolBasedRouter
// ---------------------------------------------------------------------------

#[tokio::test]
async fn protocol_router_trino_http() {
    let router = ProtocolBasedRouter {
        trino_http: Some(group("trino-group")),
        postgres_wire: Some(group("pg-group")),
        mysql_wire: None,
        clickhouse_http: None,
    };
    let session = trino_session(&[]);
    let result = router
        .route("SELECT 1", &session, &FrontendProtocol::TrinoHttp, None)
        .await
        .unwrap();
    assert_eq!(result, Some(group("trino-group")));
}

#[tokio::test]
async fn protocol_router_postgres_wire() {
    let router = ProtocolBasedRouter {
        trino_http: Some(group("trino-group")),
        postgres_wire: Some(group("pg-group")),
        mysql_wire: None,
        clickhouse_http: None,
    };
    let result = router
        .route(
            "SELECT 1",
            &postgres_session(),
            &FrontendProtocol::PostgresWire,
            None,
        )
        .await
        .unwrap();
    assert_eq!(result, Some(group("pg-group")));
}

#[tokio::test]
async fn protocol_router_unconfigured() {
    let router = ProtocolBasedRouter {
        trino_http: Some(group("trino-group")),
        postgres_wire: None, // not configured
        mysql_wire: None,
        clickhouse_http: None,
    };
    let result = router
        .route(
            "SELECT 1",
            &postgres_session(),
            &FrontendProtocol::PostgresWire,
            None,
        )
        .await
        .unwrap();
    assert_eq!(result, None);
}

// ---------------------------------------------------------------------------
// ClientTagsRouter
// ---------------------------------------------------------------------------

#[tokio::test]
async fn client_tags_single_match() {
    let router = ClientTagsRouter::new(HashMap::from([(
        "premium".to_string(),
        group("premium-group"),
    )]));
    let session = trino_session(&[("x-trino-client-tags", "premium")]);
    let result = router
        .route("SELECT 1", &session, &FrontendProtocol::TrinoHttp, None)
        .await
        .unwrap();
    assert_eq!(result, Some(group("premium-group")));
}

#[tokio::test]
async fn client_tags_multi_first_match_wins() {
    let router = ClientTagsRouter::new(HashMap::from([
        ("analytics".to_string(), group("analytics-group")),
        ("premium".to_string(), group("premium-group")),
    ]));
    // Tags are evaluated left-to-right; "analytics" appears first in the header
    let session = trino_session(&[("x-trino-client-tags", "analytics,premium")]);
    let result = router
        .route("SELECT 1", &session, &FrontendProtocol::TrinoHttp, None)
        .await
        .unwrap();
    assert_eq!(result, Some(group("analytics-group")));
}

#[tokio::test]
async fn client_tags_no_match() {
    let router = ClientTagsRouter::new(HashMap::from([(
        "premium".to_string(),
        group("premium-group"),
    )]));
    let session = trino_session(&[("x-trino-client-tags", "free,basic")]);
    let result = router
        .route("SELECT 1", &session, &FrontendProtocol::TrinoHttp, None)
        .await
        .unwrap();
    assert_eq!(result, None);
}

#[tokio::test]
async fn client_tags_absent_header() {
    let router = ClientTagsRouter::new(HashMap::from([(
        "premium".to_string(),
        group("premium-group"),
    )]));
    let session = trino_session(&[]); // no client-tags header
    let result = router
        .route("SELECT 1", &session, &FrontendProtocol::TrinoHttp, None)
        .await
        .unwrap();
    assert_eq!(result, None);
}

// ---------------------------------------------------------------------------
// QueryRegexRouter
// ---------------------------------------------------------------------------

#[tokio::test]
async fn query_regex_match() {
    let router = QueryRegexRouter::new(vec![(
        r"(?i)\bSELECT\b.*\bFROM\b.*\borders\b".to_string(),
        "orders-group".to_string(),
    )]);
    let session = trino_session(&[]);
    let result = router
        .route(
            "SELECT id FROM orders WHERE total > 100",
            &session,
            &FrontendProtocol::TrinoHttp,
            None,
        )
        .await
        .unwrap();
    assert_eq!(result, Some(group("orders-group")));
}

#[tokio::test]
async fn query_regex_no_match() {
    let router = QueryRegexRouter::new(vec![(
        r"(?i)\borders\b".to_string(),
        "orders-group".to_string(),
    )]);
    let session = trino_session(&[]);
    let result = router
        .route(
            "SELECT id FROM customers",
            &session,
            &FrontendProtocol::TrinoHttp,
            None,
        )
        .await
        .unwrap();
    assert_eq!(result, None);
}

#[tokio::test]
async fn query_regex_first_match_wins() {
    let router = QueryRegexRouter::new(vec![
        (r"(?i)\borders\b".to_string(), "orders-group".to_string()),
        (
            r"(?i)\bcustomers\b".to_string(),
            "customers-group".to_string(),
        ),
    ]);
    let session = trino_session(&[]);
    // SQL matches both, first rule should win
    let result = router
        .route(
            "SELECT * FROM orders JOIN customers ON orders.cust_id = customers.id",
            &session,
            &FrontendProtocol::TrinoHttp,
            None,
        )
        .await
        .unwrap();
    assert_eq!(result, Some(group("orders-group")));
}

#[tokio::test]
async fn query_regex_invalid_regex_skipped() {
    // Invalid regex is silently skipped at construction; valid rule still works
    let router = QueryRegexRouter::new(vec![
        (r"[invalid regex(".to_string(), "bad-group".to_string()),
        (r"(?i)\borders\b".to_string(), "orders-group".to_string()),
    ]);
    let session = trino_session(&[]);
    let result = router
        .route(
            "SELECT * FROM orders",
            &session,
            &FrontendProtocol::TrinoHttp,
            None,
        )
        .await
        .unwrap();
    assert_eq!(result, Some(group("orders-group")));
}

// ---------------------------------------------------------------------------
// CompoundRouter
// ---------------------------------------------------------------------------

#[tokio::test]
async fn compound_all_both_match() {
    let router = CompoundRouter::new(
        CompoundCombineMode::All,
        vec![
            CompoundCondition::Protocol {
                protocol: "trinoHttp".to_string(),
            },
            CompoundCondition::User {
                username: "alice".to_string(),
            },
        ],
        "premium-group".to_string(),
    );
    let session = trino_session(&[("x-trino-user", "alice")]);
    let result = router
        .route("SELECT 1", &session, &FrontendProtocol::TrinoHttp, None)
        .await
        .unwrap();
    assert_eq!(result, Some(group("premium-group")));
}

#[tokio::test]
async fn compound_all_one_fails() {
    let router = CompoundRouter::new(
        CompoundCombineMode::All,
        vec![
            CompoundCondition::Protocol {
                protocol: "trinoHttp".to_string(),
            },
            CompoundCondition::User {
                username: "alice".to_string(),
            },
        ],
        "premium-group".to_string(),
    );
    // Protocol matches but user is "bob", not "alice"
    let session = trino_session(&[("x-trino-user", "bob")]);
    let result = router
        .route("SELECT 1", &session, &FrontendProtocol::TrinoHttp, None)
        .await
        .unwrap();
    assert_eq!(result, None);
}

#[tokio::test]
async fn compound_any_first_matches() {
    let router = CompoundRouter::new(
        CompoundCombineMode::Any,
        vec![
            CompoundCondition::Protocol {
                protocol: "trinoHttp".to_string(),
            },
            CompoundCondition::User {
                username: "alice".to_string(),
            },
        ],
        "any-group".to_string(),
    );
    // Protocol matches (TrinoHttp), user doesn't matter
    let session = trino_session(&[("x-trino-user", "bob")]);
    let result = router
        .route("SELECT 1", &session, &FrontendProtocol::TrinoHttp, None)
        .await
        .unwrap();
    assert_eq!(result, Some(group("any-group")));
}

#[tokio::test]
async fn compound_any_none_match() {
    let router = CompoundRouter::new(
        CompoundCombineMode::Any,
        vec![
            CompoundCondition::Protocol {
                protocol: "mysqlWire".to_string(),
            },
            CompoundCondition::User {
                username: "alice".to_string(),
            },
        ],
        "any-group".to_string(),
    );
    // Protocol is TrinoHttp (not mysqlWire), user is "bob" (not "alice")
    let session = trino_session(&[("x-trino-user", "bob")]);
    let result = router
        .route("SELECT 1", &session, &FrontendProtocol::TrinoHttp, None)
        .await
        .unwrap();
    assert_eq!(result, None);
}

#[tokio::test]
async fn compound_client_tag_condition() {
    let router = CompoundRouter::new(
        CompoundCombineMode::All,
        vec![CompoundCondition::ClientTag {
            tag: "team-a".to_string(),
        }],
        "team-a-group".to_string(),
    );
    let session = trino_session(&[("x-trino-client-tags", "team-a,priority=high")]);
    let result = router
        .route("SELECT 1", &session, &FrontendProtocol::TrinoHttp, None)
        .await
        .unwrap();
    assert_eq!(result, Some(group("team-a-group")));
}

#[tokio::test]
async fn compound_query_regex_condition() {
    let router = CompoundRouter::new(
        CompoundCombineMode::All,
        vec![CompoundCondition::QueryRegex {
            regex: r"(?i)\blineitem\b".to_string(),
        }],
        "lineitem-group".to_string(),
    );
    let session = trino_session(&[]);
    let result = router
        .route(
            "SELECT * FROM lineitem LIMIT 10",
            &session,
            &FrontendProtocol::TrinoHttp,
            None,
        )
        .await
        .unwrap();
    assert_eq!(result, Some(group("lineitem-group")));
}

// ---------------------------------------------------------------------------
// PythonScriptRouter
// ---------------------------------------------------------------------------

#[tokio::test]
async fn python_script_sees_ctx_protocol() {
    let script = r#"
def route(query, ctx):
    if ctx.get("protocol") == "trinoHttp":
        return "g-trino"
    return None
"#;
    let router = PythonScriptRouter::new(script.to_string());
    let session = trino_session(&[]);
    let out = router
        .route("SELECT 1", &session, &FrontendProtocol::TrinoHttp, None)
        .await
        .unwrap();
    assert_eq!(out, Some(group("g-trino")));
}

#[tokio::test]
async fn python_script_auth_in_ctx() {
    let script = r#"
def route(query, ctx):
    auth = ctx.get("auth") or {}
    if auth.get("user") == "alice" and "admins" in (auth.get("groups") or []):
        return "admin-group"
    return None
"#;
    let router = PythonScriptRouter::new(script.to_string());
    let session = trino_session(&[]);
    let auth = AuthContext {
        user: "alice".to_string(),
        groups: vec!["admins".to_string()],
        roles: vec![],
        raw_token: None,
    };
    let out = router
        .route(
            "SELECT 1",
            &session,
            &FrontendProtocol::TrinoHttp,
            Some(&auth),
        )
        .await
        .unwrap();
    assert_eq!(out, Some(group("admin-group")));
}

// ---------------------------------------------------------------------------
// RouterChain
// ---------------------------------------------------------------------------

#[tokio::test]
async fn chain_first_router_matches() {
    let chain = RouterChain::new(
        vec![
            Box::new(HeaderRouter::new(
                "x-group".to_string(),
                HashMap::from([("analytics".to_string(), group("analytics-group"))]),
            )),
            Box::new(HeaderRouter::new(
                "x-group".to_string(),
                HashMap::from([("other".to_string(), group("other-group"))]),
            )),
        ],
        group("fallback-group"),
    );
    let session = trino_session(&[("x-group", "analytics")]);
    let (result, trace) = chain
        .route_with_trace("SELECT 1", &session, &FrontendProtocol::TrinoHttp, None)
        .await
        .unwrap();
    assert_eq!(result, group("analytics-group"));
    assert!(!trace.used_fallback);
}

#[tokio::test]
async fn chain_fallback_when_no_match() {
    let chain = RouterChain::new(
        vec![Box::new(HeaderRouter::new(
            "x-group".to_string(),
            HashMap::from([("analytics".to_string(), group("analytics-group"))]),
        ))],
        group("fallback-group"),
    );
    let session = trino_session(&[]); // no header → no match
    let (result, trace) = chain
        .route_with_trace("SELECT 1", &session, &FrontendProtocol::TrinoHttp, None)
        .await
        .unwrap();
    assert_eq!(result, group("fallback-group"));
    assert!(trace.used_fallback);
}

#[tokio::test]
async fn chain_trace_records_all_decisions() {
    let chain = RouterChain::new(
        vec![
            Box::new(QueryRegexRouter::new(vec![(
                r"(?i)\borders\b".to_string(),
                "orders-group".to_string(),
            )])),
            Box::new(HeaderRouter::new(
                "x-group".to_string(),
                HashMap::from([("analytics".to_string(), group("analytics-group"))]),
            )),
        ],
        group("fallback-group"),
    );
    // Neither router matches
    let session = trino_session(&[]);
    let (_, trace) = chain
        .route_with_trace("SELECT 1", &session, &FrontendProtocol::TrinoHttp, None)
        .await
        .unwrap();
    // Both routers were evaluated and recorded
    assert_eq!(trace.decisions.len(), 2);
    assert!(!trace.decisions[0].matched);
    assert!(!trace.decisions[1].matched);
    assert!(trace.used_fallback);
}

#[tokio::test]
async fn chain_second_router_matches() {
    let chain = RouterChain::new(
        vec![
            Box::new(HeaderRouter::new(
                "x-first".to_string(),
                HashMap::from([("yes".to_string(), group("first-group"))]),
            )),
            Box::new(HeaderRouter::new(
                "x-second".to_string(),
                HashMap::from([("yes".to_string(), group("second-group"))]),
            )),
        ],
        group("fallback-group"),
    );
    // Only second header present
    let session = trino_session(&[("x-second", "yes")]);
    let (result, trace) = chain
        .route_with_trace("SELECT 1", &session, &FrontendProtocol::TrinoHttp, None)
        .await
        .unwrap();
    assert_eq!(result, group("second-group"));
    assert!(!trace.used_fallback);
    assert_eq!(trace.decisions.len(), 2);
    assert!(!trace.decisions[0].matched); // first router missed
    assert!(trace.decisions[1].matched); // second router hit
}

#[tokio::test]
async fn chain_short_circuits_after_first_match() {
    let chain = RouterChain::new(
        vec![
            Box::new(HeaderRouter::new(
                "x-priority".to_string(),
                HashMap::from([("high".to_string(), group("priority-group"))]),
            )),
            Box::new(HeaderRouter::new(
                "x-other".to_string(),
                HashMap::from([("low".to_string(), group("other-group"))]),
            )),
        ],
        group("fallback-group"),
    );
    let session = trino_session(&[("x-priority", "high"), ("x-other", "low")]);
    let (result, trace) = chain
        .route_with_trace("SELECT 1", &session, &FrontendProtocol::TrinoHttp, None)
        .await
        .unwrap();
    assert_eq!(result, group("priority-group"));
    assert!(!trace.used_fallback);
    assert_eq!(
        trace.decisions.len(),
        1,
        "second router must not run after first match"
    );
    assert!(trace.decisions[0].matched);
}

#[tokio::test]
async fn chain_regex_then_header_first_wins_on_both_match() {
    let chain = RouterChain::new(
        vec![
            Box::new(QueryRegexRouter::new(vec![(
                r"^SELECT".to_string(),
                "regex-group".to_string(),
            )])),
            Box::new(HeaderRouter::new(
                "x-route".to_string(),
                HashMap::from([("batch".to_string(), group("batch-group"))]),
            )),
        ],
        group("fallback-group"),
    );
    let session = trino_session(&[("x-route", "batch")]);
    let (result, trace) = chain
        .route_with_trace("SELECT 1", &session, &FrontendProtocol::TrinoHttp, None)
        .await
        .unwrap();
    assert_eq!(result, group("regex-group"));
    assert_eq!(trace.decisions.len(), 1);
    assert!(trace.decisions[0].matched);
}

#[tokio::test]
async fn chain_regex_miss_then_header_match() {
    let chain = RouterChain::new(
        vec![
            Box::new(QueryRegexRouter::new(vec![(
                r"^INSERT".to_string(),
                "insert-group".to_string(),
            )])),
            Box::new(HeaderRouter::new(
                "x-route".to_string(),
                HashMap::from([("api".to_string(), group("api-group"))]),
            )),
        ],
        group("fallback-group"),
    );
    let session = trino_session(&[("x-route", "api")]);
    let (result, trace) = chain
        .route_with_trace("SELECT 1", &session, &FrontendProtocol::TrinoHttp, None)
        .await
        .unwrap();
    assert_eq!(result, group("api-group"));
    assert_eq!(trace.decisions.len(), 2);
    assert!(!trace.decisions[0].matched);
    assert!(trace.decisions[1].matched);
}
