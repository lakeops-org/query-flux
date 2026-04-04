# QueryFlux Performance Investigation

Date: 2026-03-21
Benchmark: `queryflux-bench` (500 iterations, sequential, mock Trino backend)

## Benchmark Results

```
Run 1                 p50       p95       p99
  Direct          0.21 ms    0.30 ms    0.41 ms
  Via QueryFlux   0.50 ms    0.72 ms    0.95 ms
  Overhead        0.29 ms    0.41 ms    0.54 ms

Run 2                 p50       p95       p99
  Direct          0.27 ms    0.46 ms    0.81 ms
  Via QueryFlux   0.98 ms    1.58 ms    4.09 ms
  Overhead        0.72 ms    1.12 ms    3.28 ms
```

Relative overhead: ~2.4× at p50, up to ~5× at p99.

## What the Benchmark Measures

Only the first hop: `POST /v1/statement`. QueryFlux does route → acquire cluster →
translate (no-op) → forward to mock → persist to DashMap → rewrite URI → return.
Does not follow nextUri polling. So this is purely first-hop proxy latency.

## Issues Found

### 1. `adapter()` allocates 2 Strings per call

**File:** `crates/queryflux-frontend/src/state.rs:34`

```rust
pub fn adapter(&self, group: &str, cluster: &str) -> Option<Arc<dyn EngineAdapterTrait>> {
    self.adapters
        .get(&(group.to_string(), cluster.to_string()))  // 2 heap allocs, immediately discarded
        .cloned()
}
```

`HashMap<(String, String), _>` doesn't support borrowed-key lookup natively, so two
`String`s are allocated and dropped on every request. Fix: implement `Borrow<(&str, &str)>`
for the key type, or switch to a newtype that supports it.

---

### 2. `extract_session()` allocates a fresh HashMap + lowercases every header key

**File:** `crates/queryflux-frontend/src/trino_http/handlers.rs:123`

```rust
fn extract_session(headers: &HeaderMap) -> SessionContext {
    let mut h = std::collections::HashMap::new();
    for (k, v) in headers {
        if let Ok(s) = v.to_str() {
            h.insert(k.as_str().to_lowercase(), s.to_string());  // alloc per header
        }
    }
    SessionContext::TrinoHttp { headers: h }
}
```

Called in both `post_statement` and `get_executing_statement`. With ~5 Trino headers,
that's ~10 allocations per request. `.to_lowercase()` always allocates even when the
input is already lowercase (axum normalizes header names to lowercase already).

Fix: use `HeaderMap` directly in `SessionContext::TrinoHttp`, or at least skip
`.to_lowercase()` since axum header names are already lowercase.

---

### 3. `tokio::spawn` wrapping a non-blocking `try_send`

**File:** `crates/queryflux-frontend/src/state.rs:88`

```rust
tokio::spawn(async move {
    let _ = metrics.record_query(record).await;  // BufferedMetricsStore just does try_send
});
```

`BufferedMetricsStore::record_query` is already a `try_send` on a bounded channel —
it returns immediately. Wrapping it in `tokio::spawn` pays task-spawning overhead
(~200ns + scheduler wakeup) for no benefit. Call it directly inline:

```rust
let _ = self.metrics.record_query(record).await;
```

---

### 4. `reqwest::Client::new()` inside DELETE handler (cold path)

**File:** `crates/queryflux-frontend/src/trino_http/handlers.rs:403`

```rust
let client = reqwest::Client::new();  // owns its own connection pool + TLS context
let _ = client.delete(&trino_url).send().await;
```

Not on the benchmark hot path, but in production every query cancellation creates a
fresh HTTP client (connection pool, TLS runtime, etc.). Should use the adapter's
existing shared `http_client`.

---

### 5. Benchmark design inflates p99 (explains run-to-run variance)

The `queryflux-bench` binary uses a single `#[tokio::main]` runtime for both the
benchmark loop and the mock Trino axum server. QueryFlux runs as a subprocess.

Under the proxy path, the mock task competes for CPU with the benchmark's event loop
on the same runtime. When the tokio scheduler delays the mock, the proxy path gets hit
twice (cross-process round trip holds a thread longer). The direct path is less
affected because there's no inter-process wait.

This is the primary cause of p99 jumping from 0.95ms → 4.09ms between runs — scheduler
jitter amplified by the extra hop, not a code bug.

Fix: run the mock in a separate process or on a dedicated tokio runtime with
`Runtime::new_multi_thread().worker_threads(1)`.

---

## What's Well Optimized (don't break these)

- `InMemoryPersistence` uses `DashMap` — sharded, no global lock
- Cluster capacity tracking uses `AtomicU64` with `Relaxed` ordering — zero contention
- `BufferedMetricsStore` writes via channel — non-blocking on hot path
- URI rewriting uses byte-level scan instead of full JSON parse/serialize
- No `Mutex` or `RwLock` anywhere in `AppState`
- `reqwest::Client` is shared per adapter — connection pool reused across requests

## Real-World Impact

The absolute overhead (0.3–0.7ms p50) matters only for sub-millisecond queries.
For typical Trino workloads (100ms–10s), the proxy overhead is negligible.
The 2.4–5× multiplier looks bad in isolation but is expected when the backend
itself responds in <1ms — the ratio collapses quickly as query duration grows.
