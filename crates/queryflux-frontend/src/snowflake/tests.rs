//! Unit tests for the Snowflake wire-protocol frontend (Design B — protocol bridge).
//!
//! Architecture: every test spins up an in-process Axum server on a random port using
//! the real `SnowflakeHttpFrontend::router()`. A plain `reqwest` client sends requests;
//! tests assert on JSON bodies without mocking any HTTP path. The backend is a
//! `MockAdapter` defined below — no native engine library is needed.
//!
//! Coverage:
//!   - Session lifecycle: login → heartbeat → token renewal → logout
//!   - Auth: missing token, invalid token after logout
//!   - gzip body decoding (Python connector always sends gzip POSTs)
//!   - `sf_error` always returns HTTP 200 (prevents 251012 retry loop)
//!   - Query execute: success, missing token, stale token, syntax error, gzip body
//!   - Query monitoring stub (empty queries array)
//!   - Cancel no-op (always returns success: true)
//!   - `schema_to_rowtype` mapping for Int64, Utf8, Float64
//!   - `batches_to_arrow_base64` round-trips through Arrow IPC

#[cfg(test)]
mod snowflake_frontend {
    use std::collections::HashMap;
    use std::io::Write;
    use std::sync::Arc;
    use std::time::Duration;

    use arrow::array::{Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use arrow_ipc::reader::StreamReader;
    use async_trait::async_trait;
    use axum::Router;
    use base64::Engine as _;
    use bytes::Bytes;
    use flate2::write::GzEncoder;
    use flate2::Compression;
    use futures::stream;
    use queryflux_auth::{
        AllowAllAuthorization, AuthProvider, AuthorizationChecker, BackendIdentityResolver,
        NoneAuthProvider, QueryCredentials,
    };
    use queryflux_cluster_manager::{
        cluster_state::ClusterState, simple::SimpleClusterGroupManager,
        strategy::strategy_from_config,
    };
    use queryflux_core::{
        catalog::TableSchema as CoreTableSchema,
        error::{QueryFluxError, Result as QfResult},
        query::{
            BackendQueryId, ClusterGroupName, ClusterName, EngineType, QueryExecution,
            QueryPollResult,
        },
        session::SessionContext,
        tags::QueryTags,
    };
    use queryflux_engine_adapters::{ArrowStream, EngineAdapterTrait};
    use queryflux_metrics::{ClusterSnapshot, MetricsStore, QueryRecord};
    use queryflux_persistence::in_memory::InMemoryPersistence;
    use queryflux_routing::{
        chain::RouterChain, implementations::protocol_based::ProtocolBasedRouter,
    };
    use queryflux_translation::TranslationService;
    use serde_json::Value;
    use tokio::net::TcpListener;

    use crate::snowflake::http::{
        format::{batches_to_arrow_base64, schema_to_rowtype},
        session_store::SnowflakeSessionStore,
        SnowflakeHttpFrontend,
    };
    use crate::state::{AppState, LiveConfig};

    // -------------------------------------------------------------------------
    // Noop MetricsStore
    // -------------------------------------------------------------------------

    struct NoopMetrics;

    #[async_trait]
    impl MetricsStore for NoopMetrics {
        async fn record_query(&self, _r: QueryRecord) -> QfResult<()> {
            Ok(())
        }
        async fn record_cluster_snapshot(&self, _s: ClusterSnapshot) -> QfResult<()> {
            Ok(())
        }
    }

    // -------------------------------------------------------------------------
    // MockAdapter — returns one row `{n: 1}` for any query; rejects "SELEKT"
    // -------------------------------------------------------------------------

    struct MockAdapter;

    #[async_trait]
    impl EngineAdapterTrait for MockAdapter {
        async fn submit_query(
            &self,
            _sql: &str,
            _session: &SessionContext,
            _credentials: &QueryCredentials,
            _tags: &QueryTags,
        ) -> QfResult<QueryExecution> {
            Err(QueryFluxError::Engine("submit_query not used".into()))
        }

        async fn poll_query(
            &self,
            _id: &BackendQueryId,
            _next_uri: Option<&str>,
        ) -> QfResult<QueryPollResult> {
            Err(QueryFluxError::Engine("poll_query not used".into()))
        }

        async fn cancel_query(&self, _id: &BackendQueryId) -> QfResult<()> {
            Ok(())
        }

        async fn health_check(&self) -> bool {
            true
        }

        fn engine_type(&self) -> EngineType {
            EngineType::DuckDb
        }

        fn supports_async(&self) -> bool {
            false
        }

        /// Returns one row `{n: 1}` unless SQL contains "SELEKT" (simulated syntax error).
        async fn execute_as_arrow(
            &self,
            sql: &str,
            _session: &SessionContext,
            _credentials: &QueryCredentials,
            _tags: &QueryTags,
        ) -> QfResult<ArrowStream> {
            if sql.to_uppercase().contains("SELEKT") {
                return Err(QueryFluxError::Engine("syntax error near 'SELEKT'".into()));
            }

            let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
            let batch =
                RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1i64]))])
                    .map_err(|e| QueryFluxError::Engine(e.to_string()))?;

            let s = stream::once(async move { Ok(batch) });
            Ok(Box::pin(s))
        }

        async fn list_catalogs(&self) -> QfResult<Vec<String>> {
            Ok(vec![])
        }

        async fn list_databases(&self, _catalog: &str) -> QfResult<Vec<String>> {
            Ok(vec![])
        }

        async fn list_tables(&self, _catalog: &str, _database: &str) -> QfResult<Vec<String>> {
            Ok(vec![])
        }

        async fn describe_table(
            &self,
            _catalog: &str,
            _database: &str,
            _table: &str,
        ) -> QfResult<Option<CoreTableSchema>> {
            Ok(None)
        }
    }

    // -------------------------------------------------------------------------
    // Server bootstrap
    // -------------------------------------------------------------------------

    /// Spins up a `SnowflakeHttpFrontend` on a random port backed by `MockAdapter`.
    /// Returns `(port, shutdown_guard)` — drop the guard to stop the server.
    async fn start_server() -> (u16, tokio::sync::oneshot::Sender<()>) {
        let group = ClusterGroupName("mock".to_string());
        let cluster = ClusterName("mock-1".to_string());

        let adapter = Arc::new(MockAdapter) as Arc<dyn EngineAdapterTrait>;

        let state = Arc::new(ClusterState::new(
            cluster.clone(),
            group.clone(),
            None,
            None,
            EngineType::DuckDb,
            None,
            16,
            true,
        ));

        let mut group_states = HashMap::new();
        group_states.insert(group.clone(), (vec![state], strategy_from_config(None)));

        let mut adapters = HashMap::new();
        adapters.insert(cluster.0.clone(), adapter);

        let mut group_members = HashMap::new();
        group_members.insert("mock".to_string(), vec![cluster.0.clone()]);

        let protocol_router: Box<dyn queryflux_routing::RouterTrait> =
            Box::new(ProtocolBasedRouter {
                trino_http: None,
                postgres_wire: None,
                mysql_wire: None,
                clickhouse_http: None,
                flight_sql: None,
                snowflake_http: Some(group.clone()),
                snowflake_sql_api: None,
            });

        let live_config = LiveConfig {
            router_chain: RouterChain::new(vec![protocol_router], group.clone()),
            cluster_manager: Arc::new(SimpleClusterGroupManager::new(group_states)),
            adapters,
            health_check_targets: vec![],
            cluster_configs: HashMap::new(),
            group_members,
            group_order: vec!["mock".to_string()],
            group_translation_scripts: HashMap::new(),
            group_default_tags: HashMap::new(),
        };

        let app_state = Arc::new(AppState {
            external_address: "http://127.0.0.1".to_string(),
            live: Arc::new(tokio::sync::RwLock::new(live_config)),
            persistence: Arc::new(InMemoryPersistence::new()),
            translation: Arc::new(TranslationService::disabled()),
            metrics: Arc::new(NoopMetrics),
            auth_provider: Arc::new(NoneAuthProvider::new(false)) as Arc<dyn AuthProvider>,
            authorization: Arc::new(AllowAllAuthorization) as Arc<dyn AuthorizationChecker>,
            identity_resolver: Arc::new(BackendIdentityResolver::new()),
            snowflake_sessions: SnowflakeSessionStore::new(Default::default()),
        });

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();

        let router: Router = SnowflakeHttpFrontend::new(app_state, port).router();
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(async {
                    let _ = rx.await;
                })
                .await
                .ok();
        });

        tokio::time::sleep(Duration::from_millis(20)).await;
        (port, tx)
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    fn base_url(port: u16) -> String {
        format!("http://127.0.0.1:{port}")
    }

    fn auth_header(token: &str) -> String {
        format!("Snowflake Token=\"{token}\"")
    }

    fn login_body() -> serde_json::Value {
        serde_json::json!({
            "data": {
                "CLIENT_APP_ID": "test",
                "CLIENT_APP_VERSION": "1.0",
                "LOGIN_NAME": "testuser",
                "PASSWORD": "testpass",
                "AUTHENTICATOR": "SNOWFLAKE"
            }
        })
    }

    async fn do_login(client: &reqwest::Client, base: &str) -> Value {
        client
            .post(format!("{base}/session/v1/login-request"))
            .json(&login_body())
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap()
    }

    // -------------------------------------------------------------------------
    // sf_error HTTP-status invariant
    // -------------------------------------------------------------------------

    /// `sf_error` must always produce HTTP 200 regardless of the `StatusCode`
    /// argument — the Snowflake Python connector retries on 4xx/5xx (errno 251012).
    #[tokio::test]
    async fn sf_error_always_returns_http_200() {
        use crate::snowflake::http::handlers::common::sf_error;
        use axum::http::StatusCode;
        use axum::response::IntoResponse;

        let resp = sf_error(StatusCode::BAD_GATEWAY, 390000, "test error").into_response();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    // -------------------------------------------------------------------------
    // Login
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn login_success_returns_token() {
        let (port, _guard) = start_server().await;
        let client = reqwest::Client::new();
        let body = do_login(&client, &base_url(port)).await;

        assert_eq!(body["success"], true);
        let token = body["data"]["token"].as_str().unwrap();
        assert!(!token.is_empty());
    }

    #[tokio::test]
    async fn login_includes_required_session_parameters() {
        let (port, _guard) = start_server().await;
        let client = reqwest::Client::new();
        let body = do_login(&client, &base_url(port)).await;

        let names: Vec<&str> = body["data"]["parameters"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|p| p["name"].as_str())
            .collect();
        assert!(names.contains(&"AUTOCOMMIT"));
        assert!(names.contains(&"QUERY_RESULT_FORMAT"));
    }

    #[tokio::test]
    async fn login_accepts_gzip_body() {
        let (port, _guard) = start_server().await;
        let client = reqwest::Client::new();

        let json_bytes = serde_json::to_vec(&login_body()).unwrap();
        let mut enc = GzEncoder::new(Vec::new(), Compression::default());
        enc.write_all(&json_bytes).unwrap();
        let gz = enc.finish().unwrap();

        let resp = client
            .post(format!("{}/session/v1/login-request", base_url(port)))
            .header("Content-Type", "application/json")
            .header("Content-Encoding", "gzip")
            .body(Bytes::from(gz))
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), 200);
        let body: Value = resp.json().await.unwrap();
        assert_eq!(body["success"], true);
    }

    /// A malformed body must return HTTP 200 with `success: false` — not HTTP 400.
    #[tokio::test]
    async fn login_malformed_body_returns_200_failure() {
        let (port, _guard) = start_server().await;
        let client = reqwest::Client::new();

        let resp = client
            .post(format!("{}/session/v1/login-request", base_url(port)))
            .header("Content-Type", "application/json")
            .body("not valid json !!")
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), 200, "error must be 200 not 4xx");
        let body: Value = resp.json().await.unwrap();
        assert_eq!(body["success"], false);
    }

    // -------------------------------------------------------------------------
    // Heartbeat
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn heartbeat_valid_session_returns_success() {
        let (port, _guard) = start_server().await;
        let client = reqwest::Client::new();
        let login = do_login(&client, &base_url(port)).await;
        let token = login["data"]["token"].as_str().unwrap();

        let body: Value = client
            .get(format!("{}/session/heartbeat", base_url(port)))
            .header("Authorization", auth_header(token))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();

        assert_eq!(body["success"], true);
    }

    #[tokio::test]
    async fn heartbeat_unknown_token_returns_200_failure() {
        let (port, _guard) = start_server().await;
        let client = reqwest::Client::new();

        let resp = client
            .get(format!("{}/session/heartbeat", base_url(port)))
            .header("Authorization", auth_header("not-a-real-token"))
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), 200, "must be 200 not 401");
        let body: Value = resp.json().await.unwrap();
        assert_eq!(body["success"], false);
    }

    #[tokio::test]
    async fn heartbeat_missing_auth_returns_failure() {
        let (port, _guard) = start_server().await;
        let client = reqwest::Client::new();

        let body: Value = client
            .get(format!("{}/session/heartbeat", base_url(port)))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();

        assert_eq!(body["success"], false);
    }

    // -------------------------------------------------------------------------
    // Logout
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn logout_removes_session() {
        let (port, _guard) = start_server().await;
        let client = reqwest::Client::new();
        let login = do_login(&client, &base_url(port)).await;
        let token = login["data"]["token"].as_str().unwrap();

        let logout: Value = client
            .delete(format!("{}/session", base_url(port)))
            .header("Authorization", auth_header(token))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(logout["success"], true);

        // Heartbeat must fail after logout.
        let hb: Value = client
            .get(format!("{}/session/heartbeat", base_url(port)))
            .header("Authorization", auth_header(token))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(hb["success"], false, "session must be gone after logout");
    }

    // -------------------------------------------------------------------------
    // Token renewal
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn token_renewal_returns_same_token() {
        let (port, _guard) = start_server().await;
        let client = reqwest::Client::new();
        let login = do_login(&client, &base_url(port)).await;
        let token = login["data"]["token"].as_str().unwrap();

        let body: Value = client
            .post(format!("{}/session/token-request", base_url(port)))
            .header("Authorization", auth_header(token))
            .json(&serde_json::json!({}))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();

        assert_eq!(body["success"], true);
        assert_eq!(body["data"]["sessionToken"].as_str().unwrap(), token);
        assert!(body["data"]["validityInSecondsST"].as_u64().unwrap() > 0);
    }

    #[tokio::test]
    async fn token_renewal_with_invalid_token_returns_failure() {
        let (port, _guard) = start_server().await;
        let client = reqwest::Client::new();

        let body: Value = client
            .post(format!("{}/session/token-request", base_url(port)))
            .header("Authorization", auth_header("bogus"))
            .json(&serde_json::json!({}))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();

        assert_eq!(body["success"], false);
    }

    // -------------------------------------------------------------------------
    // Query execute
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn query_select_returns_correct_structure() {
        let (port, _guard) = start_server().await;
        let client = reqwest::Client::new();
        let login = do_login(&client, &base_url(port)).await;
        let token = login["data"]["token"].as_str().unwrap();

        let body: Value = client
            .post(format!("{}/queries/v1/query-request", base_url(port)))
            .header("Authorization", auth_header(token))
            .json(&serde_json::json!({"sqlText": "SELECT 1 AS n", "sequenceId": 1}))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();

        assert_eq!(body["success"], true, "query failed: {body}");
        assert_eq!(body["data"]["total"], 1, "expected one row");

        let rowtype = body["data"]["rowtype"].as_array().unwrap();
        assert_eq!(rowtype.len(), 1);
        assert_eq!(rowtype[0]["name"].as_str().unwrap(), "n");

        let b64 = body["data"]["rowsetBase64"].as_str().unwrap();
        assert!(!b64.is_empty(), "rowsetBase64 must be present");
    }

    #[tokio::test]
    async fn query_accepts_gzip_body() {
        let (port, _guard) = start_server().await;
        let client = reqwest::Client::new();
        let login = do_login(&client, &base_url(port)).await;
        let token = login["data"]["token"].as_str().unwrap();

        let json = serde_json::to_vec(&serde_json::json!({"sqlText": "SELECT 1 AS val"})).unwrap();
        let mut enc = GzEncoder::new(Vec::new(), Compression::default());
        enc.write_all(&json).unwrap();
        let gz = enc.finish().unwrap();

        let body: Value = client
            .post(format!("{}/queries/v1/query-request", base_url(port)))
            .header("Authorization", auth_header(token))
            .header("Content-Encoding", "gzip")
            .body(Bytes::from(gz))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();

        assert_eq!(body["success"], true, "gzip query must succeed: {body}");
        assert_eq!(body["data"]["total"], 1);
    }

    #[tokio::test]
    async fn query_missing_token_returns_failure() {
        let (port, _guard) = start_server().await;
        let client = reqwest::Client::new();

        let body: Value = client
            .post(format!("{}/queries/v1/query-request", base_url(port)))
            .json(&serde_json::json!({"sqlText": "SELECT 1"}))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();

        assert_eq!(body["success"], false);
    }

    #[tokio::test]
    async fn query_stale_token_returns_failure() {
        let (port, _guard) = start_server().await;
        let client = reqwest::Client::new();

        let body: Value = client
            .post(format!("{}/queries/v1/query-request", base_url(port)))
            .header("Authorization", auth_header("expired-or-wrong"))
            .json(&serde_json::json!({"sqlText": "SELECT 1"}))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();

        assert_eq!(body["success"], false);
    }

    /// SQL that triggers a backend error returns `success: false` with an
    /// error message, never a 5xx status.
    #[tokio::test]
    async fn query_backend_error_returns_graceful_failure() {
        let (port, _guard) = start_server().await;
        let client = reqwest::Client::new();
        let login = do_login(&client, &base_url(port)).await;
        let token = login["data"]["token"].as_str().unwrap();

        let body: Value = client
            .post(format!("{}/queries/v1/query-request", base_url(port)))
            .header("Authorization", auth_header(token))
            .json(&serde_json::json!({"sqlText": "SELEKT * FORM bad"}))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();

        assert_eq!(body["success"], false);
        let msg = body["message"].as_str().unwrap_or("");
        assert!(!msg.is_empty(), "error message must be non-empty");
    }

    // -------------------------------------------------------------------------
    // Monitoring + Cancel stubs
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn query_monitoring_stub_returns_empty_queries() {
        let (port, _guard) = start_server().await;
        let client = reqwest::Client::new();
        let login = do_login(&client, &base_url(port)).await;
        let token = login["data"]["token"].as_str().unwrap();

        let body: Value = client
            .get(format!(
                "{}/queries/v1/query-monitoring-request",
                base_url(port)
            ))
            .header("Authorization", auth_header(token))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();

        assert_eq!(body["success"], true);
        assert_eq!(body["data"]["queries"].as_array().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn cancel_is_a_noop_and_returns_success() {
        let (port, _guard) = start_server().await;
        let client = reqwest::Client::new();
        let login = do_login(&client, &base_url(port)).await;
        let token = login["data"]["token"].as_str().unwrap();

        let body: Value = client
            .delete(format!(
                "{}/queries/v1/some-query-id-that-does-not-exist",
                base_url(port)
            ))
            .header("Authorization", auth_header(token))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();

        assert_eq!(body["success"], true);
    }

    // -------------------------------------------------------------------------
    // format.rs: schema_to_rowtype
    // -------------------------------------------------------------------------

    #[test]
    fn schema_to_rowtype_maps_basic_types() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("score", DataType::Float64, true),
        ]);

        let rowtype = schema_to_rowtype(&schema);
        let cols = rowtype.as_array().unwrap();

        assert_eq!(cols.len(), 3);
        assert_eq!(cols[0]["name"], "id");
        assert_eq!(cols[0]["type"], "fixed");
        assert_eq!(cols[0]["nullable"], false);
        assert_eq!(cols[1]["name"], "name");
        assert_eq!(cols[1]["type"], "text");
        assert_eq!(cols[1]["nullable"], true);
        assert_eq!(cols[2]["name"], "score");
        assert_eq!(cols[2]["type"], "real");
    }

    // -------------------------------------------------------------------------
    // format.rs: batches_to_arrow_base64 IPC round-trip
    // -------------------------------------------------------------------------

    #[test]
    fn arrow_ipc_round_trip_preserves_row_count() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Int64, false),
            Field::new("label", DataType::Utf8, true),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap();

        let b64 = batches_to_arrow_base64(&schema, &[batch]);
        assert!(!b64.is_empty());

        let raw = base64::engine::general_purpose::STANDARD
            .decode(&b64)
            .expect("valid base64");
        let reader = StreamReader::try_new(std::io::Cursor::new(raw), None).unwrap();
        let total_rows: usize = reader.flatten().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);
    }

    #[test]
    fn arrow_ipc_empty_batches_produces_valid_stream() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let b64 = batches_to_arrow_base64(&schema, &[]);
        assert!(!b64.is_empty(), "must emit at least the IPC schema message");

        let raw = base64::engine::general_purpose::STANDARD
            .decode(&b64)
            .unwrap();
        let reader = StreamReader::try_new(std::io::Cursor::new(raw), None).unwrap();
        let batches: Vec<RecordBatch> = reader.flatten().collect();
        assert_eq!(batches.len(), 0);
    }

    #[test]
    fn arrow_ipc_float64_survives_round_trip() {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Float64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Float64Array::from(vec![1.5, 2.5, 3.5]))],
        )
        .unwrap();

        let b64 = batches_to_arrow_base64(&schema, &[batch]);
        let raw = base64::engine::general_purpose::STANDARD
            .decode(&b64)
            .unwrap();
        let reader = StreamReader::try_new(std::io::Cursor::new(raw), None).unwrap();
        let back: Vec<RecordBatch> = reader.flatten().collect();
        assert_eq!(back[0].num_rows(), 3);
    }

    // -------------------------------------------------------------------------
    // common.rs: gzip decode (complementary to the existing tests in common.rs)
    // -------------------------------------------------------------------------

    #[test]
    fn plain_body_is_returned_unchanged() {
        use crate::snowflake::http::handlers::common::decode_snowflake_request_body;
        use axum::http::HeaderMap;

        let raw = br#"{"data":{"LOGIN_NAME":"u"}}"#;
        let out =
            decode_snowflake_request_body(&HeaderMap::new(), &Bytes::from_static(raw)).unwrap();
        assert_eq!(out.as_slice(), raw);
    }

    #[test]
    fn gzip_body_is_decompressed_correctly() {
        use crate::snowflake::http::handlers::common::decode_snowflake_request_body;
        use axum::http::{HeaderMap, HeaderValue};

        let json = br#"{"data":{"LOGIN_NAME":"tester"}}"#;
        let mut enc = GzEncoder::new(Vec::new(), Compression::default());
        enc.write_all(json).unwrap();
        let gz = Bytes::from(enc.finish().unwrap());

        let mut headers = HeaderMap::new();
        headers.insert("content-encoding", HeaderValue::from_static("gzip"));
        let out = decode_snowflake_request_body(&headers, &gz).unwrap();
        assert_eq!(out.as_slice(), json);
    }
}
