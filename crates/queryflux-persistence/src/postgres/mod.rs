use async_trait::async_trait;
use sqlx::PgPool;
use queryflux_core::{
    error::{QueryFluxError, Result},
    query::{BackendQueryId, ExecutingQuery, ProxyQueryId, QueuedQuery},
};
use crate::{
    cluster_config::{
        ClusterConfigRecord, ClusterGroupConfigRecord, UpsertClusterConfig,
        UpsertClusterGroupConfig,
    },
    metrics_store::{ClusterSnapshot, MetricsStore, QueryRecord},
    query_history::{DashboardStats, EngineStatRow, GroupStatRow, QueryFilters, QuerySummary},
    ClusterConfigStore, Persistence, QueryHistoryStore,
};

/// Postgres backend — implements both `Persistence` (in-flight query state)
/// and `MetricsStore` (historical query records + cluster snapshots).
///
/// A single shared pool covers all tables. Run `migrate()` once at startup.
pub struct PostgresStore {
    pool: PgPool,
}

impl PostgresStore {
    /// Connect to Postgres and return a ready instance.
    pub async fn connect(database_url: &str) -> Result<Self> {
        let pool = PgPool::connect(database_url)
            .await
            .map_err(|e| QueryFluxError::Persistence(format!("Failed to connect to Postgres: {e}")))?;
        Ok(Self { pool })
    }

    /// Run all migrations (persistence + metrics). Tracks applied migrations in `_sqlx_migrations`.
    pub async fn migrate(&self) -> Result<()> {
        sqlx::migrate!("src/postgres/migrations")
            .run(&self.pool)
            .await
            .map_err(|e| QueryFluxError::Persistence(format!("Migration failed: {e}")))?;
        Ok(())
    }

}

// ---------------------------------------------------------------------------
// QueryHistoryStore
// ---------------------------------------------------------------------------

#[async_trait]
impl QueryHistoryStore for PostgresStore {
    async fn list_queries(&self, filters: &QueryFilters) -> Result<Vec<QuerySummary>> {
        sqlx::query_as::<_, QuerySummary>(
            r#"SELECT id, proxy_query_id, backend_query_id, cluster_group, cluster_name,
                      engine_type, frontend_protocol, username, sql_preview, translated_sql,
                      status, was_translated,
                      source_dialect, target_dialect, queue_duration_ms, execution_duration_ms,
                      rows_returned, error_message, routing_trace, created_at,
                      engine_elapsed_time_ms, cpu_time_ms, processed_rows, processed_bytes,
                      physical_input_bytes, peak_memory_bytes, spilled_bytes, total_splits
               FROM query_records
               WHERE ($1::text IS NULL OR sql_preview ILIKE '%' || $1 || '%')
                 AND ($2::text IS NULL OR status = $2)
                 AND ($3::text IS NULL OR cluster_group = $3)
                 AND ($4::text IS NULL OR engine_type = $4)
               ORDER BY created_at DESC
               LIMIT $5 OFFSET $6"#,
        )
        .bind(&filters.search)
        .bind(&filters.status)
        .bind(&filters.cluster_group)
        .bind(&filters.engine)
        .bind(filters.limit)
        .bind(filters.offset)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| QueryFluxError::Persistence(format!("list_queries: {e}")))
    }

    async fn get_dashboard_stats(&self) -> Result<DashboardStats> {
        let row: (i64, i64, i64, f64) = sqlx::query_as(
            r#"SELECT
                COUNT(*)::bigint,
                COUNT(*) FILTER (WHERE status != 'Success')::bigint,
                COUNT(*) FILTER (WHERE was_translated)::bigint,
                COALESCE(AVG(execution_duration_ms), 0)::float8
               FROM query_records
               WHERE created_at > NOW() - INTERVAL '1 hour'"#,
        )
        .fetch_one(&self.pool)
        .await
        .map_err(|e| QueryFluxError::Persistence(format!("get_dashboard_stats: {e}")))?;

        let (total, failed, translated, avg_ms) = row;
        Ok(DashboardStats {
            queries_last_hour: total,
            error_rate_last_hour: if total > 0 { failed as f64 / total as f64 } else { 0.0 },
            avg_duration_ms_last_hour: avg_ms,
            translation_rate_last_hour: if total > 0 { translated as f64 / total as f64 } else { 0.0 },
        })
    }

    async fn get_engine_stats(&self, hours: i64) -> Result<Vec<EngineStatRow>> {
        sqlx::query_as::<_, EngineStatRow>(
            r#"SELECT
                engine_type,
                COUNT(*)::bigint                                                AS total_queries,
                COUNT(*) FILTER (WHERE status = 'Success')::bigint             AS successful_queries,
                COUNT(*) FILTER (WHERE status = 'Failed')::bigint              AS failed_queries,
                COUNT(*) FILTER (WHERE status = 'Cancelled')::bigint           AS cancelled_queries,
                COALESCE(AVG(execution_duration_ms), 0)::float8                AS avg_execution_ms,
                COALESCE(MIN(execution_duration_ms), 0)::bigint                AS min_execution_ms,
                COALESCE(MAX(execution_duration_ms), 0)::bigint                AS max_execution_ms,
                COALESCE(AVG(queue_duration_ms), 0)::float8                    AS avg_queue_ms,
                COUNT(*) FILTER (WHERE was_translated)::bigint                 AS translated_queries,
                COALESCE(SUM(rows_returned), 0)::bigint                        AS total_rows_returned
               FROM query_records
               WHERE created_at > NOW() - ($1 * INTERVAL '1 hour')
               GROUP BY engine_type
               ORDER BY total_queries DESC"#,
        )
        .bind(hours)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| QueryFluxError::Persistence(format!("get_engine_stats: {e}")))
    }

    async fn get_group_stats(&self, hours: i64) -> Result<Vec<GroupStatRow>> {
        sqlx::query_as::<_, GroupStatRow>(
            r#"SELECT
                cluster_group,
                MAX(engine_type)                                                    AS engine_type,
                COUNT(*)::bigint                                                    AS total_queries,
                COUNT(*) FILTER (WHERE status = 'Success')::bigint                 AS successful_queries,
                COUNT(*) FILTER (WHERE status = 'Failed')::bigint                  AS failed_queries,
                COUNT(*) FILTER (WHERE status = 'Cancelled')::bigint               AS cancelled_queries,
                COALESCE(AVG(execution_duration_ms), 0)::float8                    AS avg_execution_ms,
                COALESCE(MIN(execution_duration_ms), 0)::bigint                    AS min_execution_ms,
                COALESCE(MAX(execution_duration_ms), 0)::bigint                    AS max_execution_ms,
                COALESCE(AVG(queue_duration_ms), 0)::float8                        AS avg_queue_ms,
                COUNT(*) FILTER (WHERE was_translated)::bigint                     AS translated_queries,
                COALESCE(SUM(rows_returned), 0)::bigint                            AS total_rows_returned
               FROM query_records
               WHERE created_at > NOW() - ($1 * INTERVAL '1 hour')
               GROUP BY cluster_group
               ORDER BY total_queries DESC"#,
        )
        .bind(hours)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| QueryFluxError::Persistence(format!("get_group_stats: {e}")))
    }

    async fn list_engines(&self) -> Result<Vec<String>> {
        let rows: Vec<(String,)> =
            sqlx::query_as("SELECT DISTINCT engine_type FROM query_records ORDER BY engine_type")
                .fetch_all(&self.pool)
                .await
                .map_err(|e| QueryFluxError::Persistence(format!("list_engines: {e}")))?;
        Ok(rows.into_iter().map(|(e,)| e).collect())
    }

}

// ---------------------------------------------------------------------------
// ClusterConfigStore
// ---------------------------------------------------------------------------

#[async_trait]
impl ClusterConfigStore for PostgresStore {
    async fn list_cluster_configs(&self) -> Result<Vec<ClusterConfigRecord>> {
        sqlx::query_as::<_, ClusterConfigRecord>(
            "SELECT * FROM cluster_configs ORDER BY name",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| QueryFluxError::Persistence(format!("list_cluster_configs: {e}")))
    }

    async fn get_cluster_config(&self, name: &str) -> Result<Option<ClusterConfigRecord>> {
        sqlx::query_as::<_, ClusterConfigRecord>(
            "SELECT * FROM cluster_configs WHERE name = $1",
        )
        .bind(name)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| QueryFluxError::Persistence(format!("get_cluster_config: {e}")))
    }

    async fn upsert_cluster_config(
        &self,
        name: &str,
        cfg: &UpsertClusterConfig,
    ) -> Result<ClusterConfigRecord> {
        sqlx::query_as::<_, ClusterConfigRecord>(
            r#"INSERT INTO cluster_configs
                   (name, engine_key, endpoint, database_path, auth_type,
                    auth_username, auth_password, auth_token,
                    tls_insecure_skip_verify, enabled)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
               ON CONFLICT (name) DO UPDATE SET
                   engine_key               = EXCLUDED.engine_key,
                   endpoint                 = EXCLUDED.endpoint,
                   database_path            = EXCLUDED.database_path,
                   auth_type                = EXCLUDED.auth_type,
                   auth_username            = EXCLUDED.auth_username,
                   auth_password            = EXCLUDED.auth_password,
                   auth_token               = EXCLUDED.auth_token,
                   tls_insecure_skip_verify = EXCLUDED.tls_insecure_skip_verify,
                   enabled                  = EXCLUDED.enabled,
                   updated_at               = now()
               RETURNING *"#,
        )
        .bind(name)
        .bind(&cfg.engine_key)
        .bind(&cfg.endpoint)
        .bind(&cfg.database_path)
        .bind(&cfg.auth_type)
        .bind(&cfg.auth_username)
        .bind(&cfg.auth_password)
        .bind(&cfg.auth_token)
        .bind(cfg.tls_insecure_skip_verify)
        .bind(cfg.enabled)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| QueryFluxError::Persistence(format!("upsert_cluster_config: {e}")))
    }

    async fn delete_cluster_config(&self, name: &str) -> Result<bool> {
        let r = sqlx::query("DELETE FROM cluster_configs WHERE name = $1")
            .bind(name)
            .execute(&self.pool)
            .await
            .map_err(|e| QueryFluxError::Persistence(format!("delete_cluster_config: {e}")))?;
        Ok(r.rows_affected() > 0)
    }

    async fn cluster_configs_count(&self) -> Result<i64> {
        let (n,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM cluster_configs")
            .fetch_one(&self.pool)
            .await
            .map_err(|e| QueryFluxError::Persistence(format!("cluster_configs_count: {e}")))?;
        Ok(n)
    }

    async fn list_group_configs(&self) -> Result<Vec<ClusterGroupConfigRecord>> {
        sqlx::query_as::<_, ClusterGroupConfigRecord>(
            "SELECT * FROM cluster_group_configs ORDER BY name",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| QueryFluxError::Persistence(format!("list_group_configs: {e}")))
    }

    async fn get_group_config(&self, name: &str) -> Result<Option<ClusterGroupConfigRecord>> {
        sqlx::query_as::<_, ClusterGroupConfigRecord>(
            "SELECT * FROM cluster_group_configs WHERE name = $1",
        )
        .bind(name)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| QueryFluxError::Persistence(format!("get_group_config: {e}")))
    }

    async fn upsert_group_config(
        &self,
        name: &str,
        cfg: &UpsertClusterGroupConfig,
    ) -> Result<ClusterGroupConfigRecord> {
        sqlx::query_as::<_, ClusterGroupConfigRecord>(
            r#"INSERT INTO cluster_group_configs
                   (name, enabled, members, max_running_queries, max_queued_queries, strategy)
               VALUES ($1,$2,$3,$4,$5,$6)
               ON CONFLICT (name) DO UPDATE SET
                   enabled             = EXCLUDED.enabled,
                   members             = EXCLUDED.members,
                   max_running_queries = EXCLUDED.max_running_queries,
                   max_queued_queries  = EXCLUDED.max_queued_queries,
                   strategy            = EXCLUDED.strategy,
                   updated_at          = now()
               RETURNING *"#,
        )
        .bind(name)
        .bind(cfg.enabled)
        .bind(&cfg.members)
        .bind(cfg.max_running_queries)
        .bind(cfg.max_queued_queries)
        .bind(&cfg.strategy)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| QueryFluxError::Persistence(format!("upsert_group_config: {e}")))
    }

    async fn delete_group_config(&self, name: &str) -> Result<bool> {
        let r = sqlx::query("DELETE FROM cluster_group_configs WHERE name = $1")
            .bind(name)
            .execute(&self.pool)
            .await
            .map_err(|e| QueryFluxError::Persistence(format!("delete_group_config: {e}")))?;
        Ok(r.rows_affected() > 0)
    }

    async fn group_configs_count(&self) -> Result<i64> {
        let (n,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM cluster_group_configs")
            .fetch_one(&self.pool)
            .await
            .map_err(|e| QueryFluxError::Persistence(format!("group_configs_count: {e}")))?;
        Ok(n)
    }
}

// ---------------------------------------------------------------------------
// Persistence — in-flight query state (short-lived rows, deleted on completion)
// ---------------------------------------------------------------------------

#[async_trait]
impl Persistence for PostgresStore {
    async fn upsert(&self, query: ExecutingQuery) -> Result<()> {
        // Key by backend_query_id (Trino's ID) — matches the client poll URL.
        let id = query.backend_query_id.0.clone();
        let data = serde_json::to_value(&query)
            .map_err(|e| QueryFluxError::Persistence(format!("Serialize error: {e}")))?;
        sqlx::query(
            "INSERT INTO executing_queries (id, data) VALUES ($1, $2)
             ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data",
        )
        .bind(&id)
        .bind(data)
        .execute(&self.pool)
        .await
        .map_err(|e| QueryFluxError::Persistence(format!("Upsert executing_queries: {e}")))?;
        Ok(())
    }

    async fn get(&self, id: &BackendQueryId) -> Result<Option<ExecutingQuery>> {
        let row: Option<(serde_json::Value,)> =
            sqlx::query_as("SELECT data FROM executing_queries WHERE id = $1")
                .bind(&id.0)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| QueryFluxError::Persistence(format!("Get executing_queries: {e}")))?;
        match row {
            None => Ok(None),
            Some((data,)) => {
                let q = serde_json::from_value(data)
                    .map_err(|e| QueryFluxError::Persistence(format!("Deserialize error: {e}")))?;
                Ok(Some(q))
            }
        }
    }

    async fn delete(&self, id: &BackendQueryId) -> Result<()> {
        sqlx::query("DELETE FROM executing_queries WHERE id = $1")
            .bind(&id.0)
            .execute(&self.pool)
            .await
            .map_err(|e| QueryFluxError::Persistence(format!("Delete executing_queries: {e}")))?;
        Ok(())
    }

    async fn list_all(&self) -> Result<Vec<ExecutingQuery>> {
        let rows: Vec<(serde_json::Value,)> =
            sqlx::query_as("SELECT data FROM executing_queries ORDER BY created_at")
                .fetch_all(&self.pool)
                .await
                .map_err(|e| QueryFluxError::Persistence(format!("List executing_queries: {e}")))?;
        rows.into_iter()
            .map(|(data,)| {
                serde_json::from_value(data)
                    .map_err(|e| QueryFluxError::Persistence(format!("Deserialize error: {e}")))
            })
            .collect()
    }

    async fn upsert_queued(&self, query: QueuedQuery) -> Result<()> {
        let id = query.id.0.clone();
        let data = serde_json::to_value(&query)
            .map_err(|e| QueryFluxError::Persistence(format!("Serialize error: {e}")))?;
        sqlx::query(
            "INSERT INTO queued_queries (id, data) VALUES ($1, $2)
             ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data",
        )
        .bind(&id)
        .bind(data)
        .execute(&self.pool)
        .await
        .map_err(|e| QueryFluxError::Persistence(format!("Upsert queued_queries: {e}")))?;
        Ok(())
    }

    async fn get_queued(&self, id: &ProxyQueryId) -> Result<Option<QueuedQuery>> {
        let row: Option<(serde_json::Value,)> =
            sqlx::query_as("SELECT data FROM queued_queries WHERE id = $1")
                .bind(&id.0)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| QueryFluxError::Persistence(format!("Get queued_queries: {e}")))?;
        match row {
            None => Ok(None),
            Some((data,)) => {
                let q = serde_json::from_value(data)
                    .map_err(|e| QueryFluxError::Persistence(format!("Deserialize error: {e}")))?;
                Ok(Some(q))
            }
        }
    }

    async fn delete_queued(&self, id: &ProxyQueryId) -> Result<()> {
        sqlx::query("DELETE FROM queued_queries WHERE id = $1")
            .bind(&id.0)
            .execute(&self.pool)
            .await
            .map_err(|e| QueryFluxError::Persistence(format!("Delete queued_queries: {e}")))?;
        Ok(())
    }

    async fn list_queued(&self) -> Result<Vec<QueuedQuery>> {
        let rows: Vec<(serde_json::Value,)> =
            sqlx::query_as("SELECT data FROM queued_queries ORDER BY created_at")
                .fetch_all(&self.pool)
                .await
                .map_err(|e| QueryFluxError::Persistence(format!("List queued_queries: {e}")))?;
        rows.into_iter()
            .map(|(data,)| {
                serde_json::from_value(data)
                    .map_err(|e| QueryFluxError::Persistence(format!("Deserialize error: {e}")))
            })
            .collect()
    }

    async fn delete_queued_not_accessed_since(&self, cutoff: chrono::DateTime<chrono::Utc>) -> Result<u64> {
        // last_accessed is stored inside the JSONB data blob.
        let result = sqlx::query(
            "DELETE FROM queued_queries WHERE (data->>'last_accessed')::timestamptz < $1",
        )
        .bind(cutoff)
        .execute(&self.pool)
        .await
        .map_err(|e| QueryFluxError::Persistence(format!("delete_queued_not_accessed_since: {e}")))?;
        Ok(result.rows_affected())
    }
}

// ---------------------------------------------------------------------------
// MetricsStore — historical data for the management UI
// ---------------------------------------------------------------------------

#[async_trait]
impl MetricsStore for PostgresStore {
    async fn record_query(&self, r: QueryRecord) -> Result<()> {
        let (engine_elapsed_ms, cpu_ms, proc_rows, proc_bytes, phys_bytes, peak_mem, spilled, splits) =
            match &r.engine_stats {
                Some(s) => (
                    s.engine_elapsed_time_ms.map(|v| v as i64),
                    s.cpu_time_ms.map(|v| v as i64),
                    s.processed_rows.map(|v| v as i64),
                    s.processed_bytes.map(|v| v as i64),
                    s.physical_input_bytes.map(|v| v as i64),
                    s.peak_memory_bytes.map(|v| v as i64),
                    s.spilled_bytes.map(|v| v as i64),
                    s.total_splits.map(|v| v as i32),
                ),
                None => (None, None, None, None, None, None, None, None),
            };

        sqlx::query(
            r#"INSERT INTO query_records
                (proxy_query_id, backend_query_id, cluster_group, cluster_name, engine_type,
                 frontend_protocol, source_dialect, target_dialect, was_translated, username,
                 catalog, db_name, sql_preview, translated_sql, status, routing_trace,
                 queue_duration_ms, execution_duration_ms, rows_returned, error_message,
                 created_at, engine_elapsed_time_ms, cpu_time_ms, processed_rows, processed_bytes,
                 physical_input_bytes, peak_memory_bytes, spilled_bytes, total_splits)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,
                       $21,$22,$23,$24,$25,$26,$27,$28,$29)"#,
        )
        .bind(&r.proxy_query_id)
        .bind(&r.backend_query_id)
        .bind(&r.cluster_group.0)
        .bind(&r.cluster_name.0)
        .bind(format!("{:?}", r.engine_type))
        .bind(format!("{:?}", r.frontend_protocol))
        .bind(format!("{:?}", r.source_dialect))
        .bind(format!("{:?}", r.target_dialect))
        .bind(r.was_translated)
        .bind(&r.user)
        .bind(&r.catalog)
        .bind(&r.database)
        .bind(&r.sql_preview)
        .bind(&r.translated_sql)
        .bind(format!("{:?}", r.status))
        .bind(&r.routing_trace)
        .bind(r.queue_duration_ms as i64)
        .bind(r.execution_duration_ms as i64)
        .bind(r.rows_returned.map(|v| v as i64))
        .bind(&r.error_message)
        .bind(r.created_at)
        .bind(engine_elapsed_ms)
        .bind(cpu_ms)
        .bind(proc_rows)
        .bind(proc_bytes)
        .bind(phys_bytes)
        .bind(peak_mem)
        .bind(spilled)
        .bind(splits)
        .execute(&self.pool)
        .await
        .map_err(|e| QueryFluxError::Persistence(format!("Insert query_records: {e}")))?;
        Ok(())
    }

    async fn record_cluster_snapshot(&self, s: ClusterSnapshot) -> Result<()> {
        sqlx::query(
            r#"INSERT INTO cluster_snapshots
                (cluster_name, group_name, engine_type, running_queries, queued_queries,
                 max_running_queries, recorded_at)
               VALUES ($1,$2,$3,$4,$5,$6,$7)"#,
        )
        .bind(&s.cluster_name.0)
        .bind(&s.group_name.0)
        .bind(format!("{:?}", s.engine_type))
        .bind(s.running_queries as i32)
        .bind(s.queued_queries as i32)
        .bind(s.max_running_queries as i32)
        .bind(s.recorded_at)
        .execute(&self.pool)
        .await
        .map_err(|e| QueryFluxError::Persistence(format!("Insert cluster_snapshots: {e}")))?;
        Ok(())
    }
}
