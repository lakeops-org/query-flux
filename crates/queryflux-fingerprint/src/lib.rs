//! Query fingerprinting for QueryFlux.
//!
//! Two paths:
//! - [`fast_hash`] — zero-allocation hot-path hash (~200 ns). Used by the router.
//! - [`rich_fingerprint`] — AST-based normalization via polyglot-sql (~10-100 μs).
//!   Returns a [`QueryFingerprint`] with hashes and digest text. Called inside
//!   `tokio::spawn` after query completion, never on the routing hot path.

pub mod fallback;
pub mod fast;
pub mod rich;

pub use fast::fast_hash;
pub use rich::{rich_fingerprint, QueryFingerprint};

/// Map a `SqlDialect` to the dialect name string polyglot-sql expects.
/// Kept here — not in queryflux-core — because polyglot-sql is an implementation
/// detail of this crate only.
pub fn polyglot_dialect(dialect: &queryflux_core::query::SqlDialect) -> &'static str {
    use queryflux_core::query::SqlDialect;
    match dialect {
        SqlDialect::Trino => "trino",
        SqlDialect::Athena => "presto",
        SqlDialect::DuckDb => "duckdb",
        SqlDialect::StarRocks => "starrocks",
        SqlDialect::ClickHouse => "clickhouse",
        SqlDialect::MySql => "mysql",
        SqlDialect::Postgres => "postgresql",
        SqlDialect::Sqlite => "sqlite",
        SqlDialect::Snowflake => "snowflake",
        SqlDialect::BigQuery => "bigquery",
        SqlDialect::Databricks => "databricks",
        SqlDialect::MsSql => "tsql",
        SqlDialect::Redshift => "redshift",
        SqlDialect::Exasol => "exasol",
        SqlDialect::Generic => "generic",
    }
}
