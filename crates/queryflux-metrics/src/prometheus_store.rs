use async_trait::async_trait;
use prometheus::{CounterVec, Encoder, HistogramOpts, HistogramVec, Opts, Registry, TextEncoder};
use queryflux_core::error::Result;

use crate::{ClusterSnapshot, MetricsStore, QueryRecord};

/// Prometheus-backed metrics store.
///
/// Tracks real-time operational metrics exposed at `/metrics` for Prometheus scraping.
/// Use alongside `PostgresMetricsStore` (or `NoopMetricsStore`) for historical storage.
pub struct PrometheusMetrics {
    registry: Registry,
    /// queryflux_queries_total{engine_type, cluster_group, status, protocol}
    queries_total: CounterVec,
    /// queryflux_query_duration_seconds{engine_type, cluster_group}
    query_duration_seconds: HistogramVec,
    /// queryflux_translated_queries_total{src_dialect, tgt_dialect}
    translated_total: CounterVec,
    /// queryflux_running_queries{cluster_group, cluster_name}
    running_queries: prometheus::GaugeVec,
    /// queryflux_queued_queries{cluster_group}
    queued_queries: prometheus::GaugeVec,
}

impl PrometheusMetrics {
    pub fn new() -> std::result::Result<Self, prometheus::Error> {
        let registry = Registry::new();

        let queries_total = CounterVec::new(
            Opts::new("queryflux_queries_total", "Total completed queries"),
            &["engine_type", "cluster_group", "status", "protocol"],
        )?;

        let query_duration_seconds = HistogramVec::new(
            HistogramOpts::new(
                "queryflux_query_duration_seconds",
                "Query execution duration in seconds",
            )
            .buckets(vec![
                0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 15.0, 30.0, 60.0, 120.0, 300.0,
            ]),
            &["engine_type", "cluster_group"],
        )?;

        let translated_total = CounterVec::new(
            Opts::new(
                "queryflux_translated_queries_total",
                "Total queries that required SQL dialect translation",
            ),
            &["src_dialect", "tgt_dialect"],
        )?;

        let running_queries = prometheus::GaugeVec::new(
            Opts::new(
                "queryflux_running_queries",
                "Current number of queries executing on each cluster",
            ),
            &["cluster_group", "cluster_name"],
        )?;

        let queued_queries = prometheus::GaugeVec::new(
            Opts::new(
                "queryflux_queued_queries",
                "Current number of queries queued waiting for cluster capacity",
            ),
            &["cluster_group"],
        )?;

        registry.register(Box::new(queries_total.clone()))?;
        registry.register(Box::new(query_duration_seconds.clone()))?;
        registry.register(Box::new(translated_total.clone()))?;
        registry.register(Box::new(running_queries.clone()))?;
        registry.register(Box::new(queued_queries.clone()))?;

        Ok(Self {
            registry,
            queries_total,
            query_duration_seconds,
            translated_total,
            running_queries,
            queued_queries,
        })
    }

    /// Render all metrics in Prometheus text exposition format.
    /// Returns the text to serve at the `/metrics` endpoint.
    pub fn gather_text(&self) -> String {
        let encoder = TextEncoder::new();
        let metric_families = self.registry.gather();
        let mut buffer = Vec::new();
        encoder
            .encode(&metric_families, &mut buffer)
            .unwrap_or_default();
        String::from_utf8(buffer).unwrap_or_default()
    }
}

impl Default for PrometheusMetrics {
    fn default() -> Self {
        Self::new().expect("Failed to create PrometheusMetrics")
    }
}

#[async_trait]
impl MetricsStore for PrometheusMetrics {
    fn on_query_started(&self, group: &str, cluster: &str) {
        self.running_queries
            .with_label_values(&[group, cluster])
            .inc();
    }

    fn on_query_finished(&self, group: &str, cluster: &str) {
        let g = self.running_queries.with_label_values(&[group, cluster]);
        // Guard against going negative if called without a matching start.
        if g.get() > 0.0 {
            g.dec();
        }
    }

    async fn record_query(&self, record: QueryRecord) -> Result<()> {
        let engine = format!("{:?}", record.engine_type);
        let group = record.cluster_group.0.as_str().to_string();
        let status = format!("{:?}", record.status);
        let protocol = format!("{:?}", record.frontend_protocol);

        self.queries_total
            .with_label_values(&[&engine, &group, &status, &protocol])
            .inc();

        self.query_duration_seconds
            .with_label_values(&[&engine, &group])
            .observe(record.execution_duration_ms as f64 / 1000.0);

        if record.was_translated {
            let src = format!("{:?}", record.source_dialect);
            let tgt = format!("{:?}", record.target_dialect);
            self.translated_total.with_label_values(&[&src, &tgt]).inc();
        }

        Ok(())
    }

    async fn record_cluster_snapshot(&self, snapshot: ClusterSnapshot) -> Result<()> {
        let group = snapshot.group_name.0.as_str().to_string();
        let cluster = snapshot.cluster_name.0.as_str().to_string();

        self.running_queries
            .with_label_values(&[&group, &cluster])
            .set(snapshot.running_queries as f64);

        self.queued_queries
            .with_label_values(&[&group])
            .set(snapshot.queued_queries as f64);

        Ok(())
    }
}
