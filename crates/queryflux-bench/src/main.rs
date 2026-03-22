//! End-to-end overhead benchmark for QueryFlux.
//!
//! Spins up a mock Trino backend and a real QueryFlux instance, then fires
//! `ITERATIONS` queries both directly at the mock and through QueryFlux.
//! Reports p50/p95/p99 latencies and the QueryFlux overhead in the
//! `customSmallerIsBetter` JSON format expected by
//! `benchmark-action/github-action-benchmark`.
//!
//! Run (after `cargo build --bin queryflux`):
//!   cargo run --bin queryflux-bench

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use axum::response::Json;
use axum::{routing::get, routing::post, Router};
use serde_json::{json, Value};
use tokio::net::TcpListener;
use tokio::time::sleep;

const WARMUP: usize = 50;
const ITERATIONS: usize = 500;

static QUERY_COUNTER: AtomicU64 = AtomicU64::new(0);

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mock_port = free_port();
    let qf_port = free_port();
    let admin_port = free_port();

    // ── 1. Start mock Trino backend ──────────────────────────────────────────
    let mock_listener = TcpListener::bind(format!("127.0.0.1:{mock_port}")).await?;
    tokio::spawn(async move {
        let app = Router::new()
            .route("/v1/statement", post(mock_statement))
            .route("/v1/info", get(mock_info))
            .route("/v1/cluster", get(mock_cluster));
        axum::serve(mock_listener, app).await.unwrap();
    });
    // Give the mock a moment to bind.
    sleep(Duration::from_millis(100)).await;

    // ── 2. Write a temporary QueryFlux config ────────────────────────────────
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
  mock-1:
    engine: trino
    endpoint: "http://127.0.0.1:{mock_port}"
    enabled: true

clusterGroups:
  bench-group:
    enabled: true
    maxRunningQueries: 100000
    members: [mock-1]

routers: []
routingFallback: bench-group
"#
    );
    let config_path = std::env::temp_dir().join("queryflux-bench.yaml");
    std::fs::write(&config_path, &config_content)?;

    // ── 3. Start QueryFlux as a subprocess ───────────────────────────────────
    let queryflux_bin = find_queryflux_bin()?;
    let mut qf_proc = std::process::Command::new(&queryflux_bin)
        .arg("--config")
        .arg(&config_path)
        // Avoid `set_var` (unsafe in Rust 2024+); child env is isolated and thread-safe.
        .env("RUST_LOG", "error")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()?;

    // ── 4. Wait for QueryFlux to be ready ────────────────────────────────────
    wait_for_health(admin_port).await?;

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;

    // ── 5. Warm up ───────────────────────────────────────────────────────────
    eprintln!("Warming up ({WARMUP} queries each)...");
    for _ in 0..WARMUP {
        let _ = send_query(&client, mock_port).await;
        let _ = send_query(&client, qf_port).await;
    }

    // ── 6. Benchmark direct ──────────────────────────────────────────────────
    eprintln!("Benchmarking direct ({ITERATIONS} queries)...");
    let mut direct_ms = Vec::with_capacity(ITERATIONS);
    for _ in 0..ITERATIONS {
        let t = Instant::now();
        send_query(&client, mock_port).await?;
        direct_ms.push(t.elapsed().as_secs_f64() * 1000.0);
    }

    // ── 7. Benchmark through QueryFlux ───────────────────────────────────────
    eprintln!("Benchmarking through QueryFlux ({ITERATIONS} queries)...");
    let mut proxy_ms = Vec::with_capacity(ITERATIONS);
    for _ in 0..ITERATIONS {
        let t = Instant::now();
        send_query(&client, qf_port).await?;
        proxy_ms.push(t.elapsed().as_secs_f64() * 1000.0);
    }

    // ── 8. Tear down ─────────────────────────────────────────────────────────
    let _ = qf_proc.kill();
    let _ = std::fs::remove_file(&config_path);

    // ── 9. Print results as JSON (benchmark-action format) ───────────────────
    let results = build_results(&direct_ms, &proxy_ms);
    println!("{}", serde_json::to_string_pretty(&results)?);

    Ok(())
}

// ── HTTP handlers for the mock Trino backend ─────────────────────────────────

async fn mock_statement() -> Json<Value> {
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

async fn mock_info() -> Json<Value> {
    Json(json!({
        "starting": false,
        "nodeVersion": {"version": "mock-0.1"},
        "coordinator": true,
        "environment": "bench",
        "uptime": "0.00s"
    }))
}

async fn mock_cluster() -> Json<Value> {
    Json(json!({
        "runningQueries": 0,
        "blockedQueries": 0,
        "queuedQueries": 0
    }))
}

// ── Helpers ──────────────────────────────────────────────────────────────────

async fn send_query(client: &reqwest::Client, port: u16) -> anyhow::Result<()> {
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

    // Standard layout: target/{debug,release}/
    let candidate = bin_dir.join("queryflux");
    if candidate.exists() {
        return Ok(candidate);
    }

    // Cargo test layout: target/{debug,release}/deps/
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

// ── Stats ────────────────────────────────────────────────────────────────────

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

fn build_results(direct_ms: &[f64], proxy_ms: &[f64]) -> Value {
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

    // Print a human-readable summary to stderr so CI logs are easy to read.
    eprintln!("\n── QueryFlux overhead benchmark ────────────────────────────────────────");
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

    json!([
        {"name": "Direct p50 (ms)",         "value": round2(d_p50),          "unit": "ms"},
        {"name": "Direct p95 (ms)",         "value": round2(d_p95),          "unit": "ms"},
        {"name": "Direct p99 (ms)",         "value": round2(d_p99),          "unit": "ms"},
        {"name": "Via QueryFlux p50 (ms)",  "value": round2(p_p50),          "unit": "ms"},
        {"name": "Via QueryFlux p95 (ms)",  "value": round2(p_p95),          "unit": "ms"},
        {"name": "Via QueryFlux p99 (ms)",  "value": round2(p_p99),          "unit": "ms"},
        {"name": "Overhead p50 (ms)",       "value": round2(p_p50 - d_p50),  "unit": "ms"},
        {"name": "Overhead p95 (ms)",       "value": round2(p_p95 - d_p95),  "unit": "ms"},
        {"name": "Overhead p99 (ms)",       "value": round2(p_p99 - d_p99),  "unit": "ms"}
    ])
}
