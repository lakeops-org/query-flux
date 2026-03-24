//! End-to-end overhead benchmark for QueryFlux.
//!
//! Runs two scenarios (mock Trino HTTP backend, mock StarRocks MySQL backend), each
//! comparing direct backend latency vs the same workload through QueryFlux via Trino HTTP.
//! Prints one JSON array in `customSmallerIsBetter` format for
//! `benchmark-action/github-action-benchmark`.
//!
//! Run (after `cargo build --bin queryflux`):
//!   cargo run --bin queryflux-bench

use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use axum::response::Json;
use axum::{routing::get, routing::post, Router};
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Opts, OptsBuilder};
use opensrv_mysql::{
    AsyncMysqlIntermediary, AsyncMysqlShim, Column, ColumnFlags, ColumnType, OkResponse,
    ParamParser, QueryResultWriter, StatementMetaWriter,
};
use serde_json::{json, Value};
use tokio::io::BufWriter;
use tokio::net::TcpListener;
use tokio::time::sleep;

const WARMUP: usize = 50;
const ITERATIONS: usize = 500;

static QUERY_COUNTER: AtomicU64 = AtomicU64::new(0);

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let trino_entries = bench_trino().await?;
    let starrocks_entries = bench_starrocks().await?;

    let combined: Vec<Value> = trino_entries
        .into_iter()
        .chain(starrocks_entries)
        .collect();
    println!("{}", serde_json::to_string_pretty(&json!(combined))?);

    Ok(())
}

async fn bench_trino() -> anyhow::Result<Vec<Value>> {
    let mock_port = free_port();
    let qf_port = free_port();
    let admin_port = free_port();

    let mock_listener = TcpListener::bind(format!("127.0.0.1:{mock_port}")).await?;
    tokio::spawn(async move {
        let app = Router::new()
            .route("/v1/statement", post(mock_trino_statement))
            .route("/v1/info", get(mock_trino_info))
            .route("/v1/cluster", get(mock_trino_cluster));
        axum::serve(mock_listener, app).await.unwrap();
    });
    sleep(Duration::from_millis(100)).await;

    let config_content = format!(
        r#"queryflux:
  externalAddress: "http://127.0.0.1:{qf_port}"
  frontends:
    trinoHttp:
      enabled: true
      port: {qf_port}
  persistence:
    type: inMemory
  adminApi:
    port: {admin_port}

clusters:
  mock-trino-1:
    engine: trino
    endpoint: "http://127.0.0.1:{mock_port}"
    enabled: true

clusterGroups:
  bench-trino-group:
    enabled: true
    maxRunningQueries: 100000
    members: [mock-trino-1]

routers: []
routingFallback: bench-trino-group
"#
    );
    let config_path = std::env::temp_dir().join("queryflux-bench-trino.yaml");
    std::fs::write(&config_path, &config_content)?;

    let queryflux_bin = find_queryflux_bin()?;
    let mut qf_proc = std::process::Command::new(&queryflux_bin)
        .arg("--config")
        .arg(&config_path)
        .env("RUST_LOG", "error")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()?;

    wait_for_health(admin_port).await?;

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;

    eprintln!("[Trino] Warming up ({WARMUP} queries each)...");
    for _ in 0..WARMUP {
        let _ = send_trino_http(&client, mock_port).await;
        let _ = send_trino_http(&client, qf_port).await;
    }

    eprintln!("[Trino] Benchmarking direct ({ITERATIONS} queries)...");
    let mut direct_ms = Vec::with_capacity(ITERATIONS);
    for _ in 0..ITERATIONS {
        let t = Instant::now();
        send_trino_http(&client, mock_port).await?;
        direct_ms.push(t.elapsed().as_secs_f64() * 1000.0);
    }

    eprintln!("[Trino] Benchmarking through QueryFlux ({ITERATIONS} queries)...");
    let mut proxy_ms = Vec::with_capacity(ITERATIONS);
    for _ in 0..ITERATIONS {
        let t = Instant::now();
        send_trino_http(&client, qf_port).await?;
        proxy_ms.push(t.elapsed().as_secs_f64() * 1000.0);
    }

    let _ = qf_proc.kill();
    let _ = std::fs::remove_file(&config_path);

    Ok(build_result_entries("Trino", &direct_ms, &proxy_ms))
}

async fn bench_starrocks() -> anyhow::Result<Vec<Value>> {
    let mysql_port = spawn_mock_mysql_server().await;
    let qf_port = free_port();
    let admin_port = free_port();

    let mysql_url = format!("mysql://127.0.0.1:{mysql_port}");
    let config_content = format!(
        r#"queryflux:
  externalAddress: "http://127.0.0.1:{qf_port}"
  frontends:
    trinoHttp:
      enabled: true
      port: {qf_port}
  persistence:
    type: inMemory
  adminApi:
    port: {admin_port}

clusters:
  mock-sr-1:
    engine: starRocks
    endpoint: "{mysql_url}"
    enabled: true

clusterGroups:
  bench-sr-group:
    enabled: true
    maxRunningQueries: 100000
    members: [mock-sr-1]

routers: []
routingFallback: bench-sr-group
"#
    );
    let config_path = std::env::temp_dir().join("queryflux-bench-starrocks.yaml");
    std::fs::write(&config_path, &config_content)?;

    let queryflux_bin = find_queryflux_bin()?;
    let mut qf_proc = std::process::Command::new(&queryflux_bin)
        .arg("--config")
        .arg(&config_path)
        .env("RUST_LOG", "error")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()?;

    wait_for_health(admin_port).await?;

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;

    let mysql_opts = mysql_opts_mock(&mysql_url)?;

    eprintln!("[StarRocks] Warming up ({WARMUP} queries each)...");
    for _ in 0..WARMUP {
        let _ = send_mysql_select_one(&mysql_opts).await;
        let _ = send_trino_http(&client, qf_port).await;
    }

    eprintln!("[StarRocks] Benchmarking direct MySQL ({ITERATIONS} queries)...");
    let mut direct_ms = Vec::with_capacity(ITERATIONS);
    for _ in 0..ITERATIONS {
        let t = Instant::now();
        send_mysql_select_one(&mysql_opts).await?;
        direct_ms.push(t.elapsed().as_secs_f64() * 1000.0);
    }

    eprintln!("[StarRocks] Benchmarking through QueryFlux ({ITERATIONS} queries)...");
    let mut proxy_ms = Vec::with_capacity(ITERATIONS);
    for _ in 0..ITERATIONS {
        let t = Instant::now();
        send_trino_http(&client, qf_port).await?;
        proxy_ms.push(t.elapsed().as_secs_f64() * 1000.0);
    }

    let _ = qf_proc.kill();
    let _ = std::fs::remove_file(&config_path);

    Ok(build_result_entries("StarRocks", &direct_ms, &proxy_ms))
}

fn mysql_opts_mock(url: &str) -> anyhow::Result<Opts> {
    let base = Opts::from_url(url).map_err(|e| anyhow::anyhow!("invalid mysql url: {e}"))?;
    let opts = OptsBuilder::from_opts(base)
        .prefer_socket(false)
        .into();
    Ok(opts)
}

async fn send_mysql_select_one(opts: &Opts) -> anyhow::Result<()> {
    let mut conn = Conn::new(opts.clone()).await?;
    conn.query_drop("SELECT 1").await?;
    Ok(())
}

/// MySQL wire mock sufficient for `mysql_async` + StarRocks adapter (`SELECT 1`, `USE`, settings query).
async fn spawn_mock_mysql_server() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    tokio::spawn(async move {
        loop {
            let Ok((socket, _)) = listener.accept().await else {
                break;
            };
            let (r, w) = socket.into_split();
            let w = BufWriter::new(w);
            tokio::spawn(async move {
                let _ = AsyncMysqlIntermediary::run_on(MockMysqlBackend, r, w).await;
            });
        }
    });

    sleep(Duration::from_millis(50)).await;
    port
}

struct MockMysqlBackend;

#[async_trait]
impl<W: tokio::io::AsyncWrite + Send + Unpin> AsyncMysqlShim<W> for MockMysqlBackend {
    type Error = io::Error;

    async fn on_prepare<'a>(
        &'a mut self,
        _: &'a str,
        info: StatementMetaWriter<'a, W>,
    ) -> io::Result<()> {
        info.reply(1, &[], &[]).await
    }

    async fn on_execute<'a>(
        &'a mut self,
        _: u32,
        _: ParamParser<'a>,
        results: QueryResultWriter<'a, W>,
    ) -> io::Result<()> {
        results.completed(OkResponse::default()).await
    }

    async fn on_close(&mut self, _: u32) {}

    async fn on_query<'a>(
        &'a mut self,
        sql: &'a str,
        results: QueryResultWriter<'a, W>,
    ) -> io::Result<()> {
        let u = sql.trim().to_ascii_uppercase();

        if u.starts_with("USE ") {
            return results.completed(OkResponse::default()).await;
        }

        // `mysql_async` reads settings with one round-trip: SELECT @@max_allowed_packet,@@wait_timeout
        if u.contains("@@MAX_ALLOWED_PACKET") && u.contains("@@WAIT_TIMEOUT") {
            let cols = [
                Column {
                    table: String::new(),
                    column: "@@max_allowed_packet".to_owned(),
                    coltype: ColumnType::MYSQL_TYPE_LONGLONG,
                    colflags: ColumnFlags::empty(),
                },
                Column {
                    table: String::new(),
                    column: "@@wait_timeout".to_owned(),
                    coltype: ColumnType::MYSQL_TYPE_LONGLONG,
                    colflags: ColumnFlags::empty(),
                },
            ];
            let mut w = results.start(&cols).await?;
            w.write_col(16_777_216i64)?;
            w.write_col(28_800i64)?;
            return w.finish().await;
        }

        if is_select_one_literal(sql) {
            let cols = [Column {
                table: String::new(),
                column: "1".to_owned(),
                coltype: ColumnType::MYSQL_TYPE_LONGLONG,
                colflags: ColumnFlags::empty(),
            }];
            let mut w = results.start(&cols).await?;
            w.write_col(1i64)?;
            return w.finish().await;
        }

        if u.contains("@@") {
            let colname = sql
                .trim()
                .strip_prefix("SELECT")
                .unwrap_or("@@var")
                .trim()
                .split_whitespace()
                .next()
                .unwrap_or("@@var")
                .trim_matches(',')
                .to_string();
            let cols = [Column {
                table: String::new(),
                column: colname,
                coltype: ColumnType::MYSQL_TYPE_LONGLONG,
                colflags: ColumnFlags::empty(),
            }];
            let mut w = results.start(&cols).await?;
            w.write_col(0i64)?;
            return w.finish().await;
        }

        results.completed(OkResponse::default()).await
    }
}

fn is_select_one_literal(sql: &str) -> bool {
    let t = sql.trim().trim_end_matches(';').trim();
    t.eq_ignore_ascii_case("SELECT 1")
}

// ── Mock Trino HTTP ──────────────────────────────────────────────────────────

async fn mock_trino_statement() -> Json<Value> {
    let id = format!("bench_{:08}", QUERY_COUNTER.fetch_add(1, Ordering::Relaxed));
    Json(json!({
        "id": id,
        "infoUri": "http://mock/ui/query.html",
        "stats": {
            "state": "FINISHED",
            "queued": false,
            "scheduled": true,
            "runningDrivers": 0,
            "completedSplits": 1,
            "totalSplits": 1,
            "queuedSplits": 0,
            "runningSplits": 0,
            "processedRows": 1,
            "processedBytes": 8,
            "queuedTimeMillis": 0,
            "elapsedTimeMillis": 1,
            "cpuTimeMillis": 0,
            "wallTimeMillis": 1,
            "physicalInputBytes": 8,
            "peakUserMemoryBytes": 0,
            "spilledBytes": 0
        },
        "columns": [{"name": "col1", "type": "bigint", "typeSignature": {"rawType": "bigint", "arguments": []}}],
        "data": [[1]]
    }))
}

async fn mock_trino_info() -> Json<Value> {
    Json(json!({
        "starting": false,
        "nodeVersion": {"version": "mock-0.1"},
        "coordinator": true,
        "environment": "bench",
        "uptime": "0.00s"
    }))
}

async fn mock_trino_cluster() -> Json<Value> {
    Json(json!({
        "runningQueries": 0,
        "blockedQueries": 0,
        "queuedQueries": 0
    }))
}

async fn send_trino_http(client: &reqwest::Client, port: u16) -> anyhow::Result<()> {
    client
        .post(format!("http://127.0.0.1:{port}/v1/statement"))
        .header("x-trino-user", "bench")
        .body("SELECT 1")
        .send()
        .await?;
    Ok(())
}

fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

fn find_queryflux_bin() -> anyhow::Result<std::path::PathBuf> {
    let current_exe = std::env::current_exe()?;
    let bin_dir = current_exe
        .parent()
        .ok_or_else(|| anyhow::anyhow!("can't determine binary directory"))?;

    let candidate = bin_dir.join("queryflux");
    if candidate.exists() {
        return Ok(candidate);
    }

    if let Some(parent) = bin_dir.parent() {
        let candidate = parent.join("queryflux");
        if candidate.exists() {
            return Ok(candidate);
        }
    }

    anyhow::bail!(
        "queryflux binary not found near {bin_dir:?}. Build it first with: cargo build --bin queryflux"
    )
}

async fn wait_for_health(admin_port: u16) -> anyhow::Result<()> {
    let client = reqwest::Client::new();
    let url = format!("http://127.0.0.1:{admin_port}/health");
    let deadline = Instant::now() + Duration::from_secs(15);

    while Instant::now() < deadline {
        if client
            .get(&url)
            .send()
            .await
            .map(|r| r.status().is_success())
            .unwrap_or(false)
        {
            return Ok(());
        }
        sleep(Duration::from_millis(100)).await;
    }

    anyhow::bail!("QueryFlux did not become healthy within 15s on admin port {admin_port}")
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = ((sorted.len() as f64 * p / 100.0) as usize).min(sorted.len() - 1);
    sorted[idx]
}

fn round2(v: f64) -> f64 {
    (v * 100.0).round() / 100.0
}

fn build_result_entries(engine: &str, direct_ms: &[f64], proxy_ms: &[f64]) -> Vec<Value> {
    let mut d = direct_ms.to_vec();
    let mut p = proxy_ms.to_vec();
    d.sort_by(|a, b| a.partial_cmp(b).unwrap());
    p.sort_by(|a, b| a.partial_cmp(b).unwrap());

    let d_p50 = percentile(&d, 50.0);
    let d_p95 = percentile(&d, 95.0);
    let d_p99 = percentile(&d, 99.0);
    let p_p50 = percentile(&p, 50.0);
    let p_p95 = percentile(&p, 95.0);
    let p_p99 = percentile(&p, 99.0);

    eprintln!("\n── QueryFlux overhead — {engine} ─────────────────────────────────────");
    eprintln!("                  p50       p95       p99");
    eprintln!("  Direct        {d_p50:>6.2} ms  {d_p95:>6.2} ms  {d_p99:>6.2} ms");
    eprintln!("  Via QueryFlux {p_p50:>6.2} ms  {p_p95:>6.2} ms  {p_p99:>6.2} ms");
    eprintln!(
        "  Overhead      {:>6.2} ms  {:>6.2} ms  {:>6.2} ms",
        p_p50 - d_p50,
        p_p95 - d_p95,
        p_p99 - d_p99
    );
    eprintln!("────────────────────────────────────────────────────────────────────────\n");

    let prefix = format!("{engine} — ");
    vec![
        json!({"name": format!("{prefix}Direct p50 (ms)"),         "value": round2(d_p50),         "unit": "ms"}),
        json!({"name": format!("{prefix}Direct p95 (ms)"),         "value": round2(d_p95),         "unit": "ms"}),
        json!({"name": format!("{prefix}Direct p99 (ms)"),         "value": round2(d_p99),         "unit": "ms"}),
        json!({"name": format!("{prefix}Via QueryFlux p50 (ms)"), "value": round2(p_p50),         "unit": "ms"}),
        json!({"name": format!("{prefix}Via QueryFlux p95 (ms)"), "value": round2(p_p95),         "unit": "ms"}),
        json!({"name": format!("{prefix}Via QueryFlux p99 (ms)"), "value": round2(p_p99),         "unit": "ms"}),
        json!({"name": format!("{prefix}Overhead p50 (ms)"),      "value": round2(p_p50 - d_p50), "unit": "ms"}),
        json!({"name": format!("{prefix}Overhead p95 (ms)"),      "value": round2(p_p95 - d_p95), "unit": "ms"}),
        json!({"name": format!("{prefix}Overhead p99 (ms)"),      "value": round2(p_p99 - d_p99), "unit": "ms"}),
    ]
}
