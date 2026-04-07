//! Rich fingerprinting using polyglot-sql for AST-based normalization.
//!
//! Called inside `tokio::spawn` after query completion — never on the routing hot path.
//! Uses polyglot-sql to parse and re-serialize SQL for consistent normalization across
//! dialects, then applies regex-based literal replacement.
//!
//! Falls back to `crate::fallback` if polyglot-sql cannot parse the input.

use tracing::warn;
use xxhash_rust::xxh64::xxh64;

use crate::fallback;

/// The result of rich fingerprinting — everything needed for storage and analytics.
#[derive(Debug, Clone)]
pub struct QueryFingerprint {
    /// Hash of the normalized original SQL (exact match, no literal replacement).
    pub query_hash: u64,
    /// Hash of the parameterized original SQL (literals replaced with `?`).
    pub query_parameterized_hash: u64,
    /// Human-readable parameterized SQL — stored in `query_digest_stats`, not in `query_records`.
    pub digest_text: String,
    /// Hash of the parameterized translated SQL. `None` when no translation occurred.
    pub translated_query_hash: Option<u64>,
    /// Human-readable parameterized translated SQL for `query_digest_stats`. `None` when no translation.
    pub translated_digest_text: Option<String>,
    /// `false` if the query contains non-deterministic functions (NOW, RANDOM, UUID, etc.).
    /// Non-deterministic queries must not be cached.
    pub is_deterministic: bool,
}

/// Compute a rich fingerprint for `original_sql`.
///
/// `translated_sql` is the dialect-translated SQL already stored in `QueryContext.translated_sql`.
/// `src_dialect` / `tgt_dialect` are polyglot-sql dialect name strings (from `SqlDialect::polyglot_name()`).
///
/// This function is synchronous and takes ~10–100 μs. Call it inside `tokio::spawn`.
pub fn rich_fingerprint(
    original_sql: &str,
    translated_sql: Option<&str>,
    src_dialect: &str,
    tgt_dialect: &str,
) -> QueryFingerprint {
    let (query_hash, query_parameterized_hash, digest_text, is_deterministic) =
        fingerprint_one(original_sql, src_dialect);

    let (translated_query_hash, translated_digest_text) = match translated_sql {
        Some(tsql) => {
            let (_, hash, digest, _) = fingerprint_one(tsql, tgt_dialect);
            (Some(hash), Some(digest))
        }
        None => (None, None),
    };

    QueryFingerprint {
        query_hash,
        query_parameterized_hash,
        digest_text,
        translated_query_hash,
        translated_digest_text,
        is_deterministic,
    }
}

/// Normalize and fingerprint a single SQL string using polyglot-sql.
/// Returns `(query_hash, parameterized_hash, digest_text, is_deterministic)`.
fn fingerprint_one(sql: &str, dialect: &str) -> (u64, u64, String, bool) {
    match try_polyglot(sql, dialect) {
        Ok(result) => result,
        Err(e) => {
            warn!(
                dialect,
                "polyglot-sql fingerprint failed, using fallback: {e}"
            );
            let (query_hash, parameterized_hash, digest_text) = fallback::fallback_fingerprint(sql);
            let is_deterministic = !contains_nondeterministic_simple(sql);
            (
                query_hash,
                parameterized_hash,
                digest_text,
                is_deterministic,
            )
        }
    }
}

fn try_polyglot(
    sql: &str,
    dialect: &str,
) -> Result<(u64, u64, String, bool), Box<dyn std::error::Error>> {
    use polyglot_sql::{generate_by_name, parse_by_name};

    // 1. Parse — this normalizes whitespace, comments, and keyword casing.
    let statements = parse_by_name(sql, dialect)?;
    if statements.is_empty() {
        return Err("empty parse result".into());
    }

    // 2. Non-determinism detection via AST walk.
    let is_deterministic = !statements.iter().any(contains_nondeterministic_expr);

    // 3. Re-serialize for query_hash (normalized but with original literal values).
    let normalized_parts: Vec<String> = statements
        .iter()
        .map(|s| generate_by_name(s, dialect))
        .collect::<Result<Vec<_>, _>>()?;
    let normalized = normalized_parts.join("; ").to_lowercase();
    let query_hash = xxh64(normalized.as_bytes(), 0);

    // 4. Apply literal replacement to the normalized SQL → digest_text.
    let digest_text = fallback::parameterize(&normalized);
    let parameterized_hash = xxh64(digest_text.as_bytes(), 0);

    Ok((
        query_hash,
        parameterized_hash,
        digest_text,
        is_deterministic,
    ))
}

/// Walk one polyglot-sql Expression tree (DFS) to detect non-deterministic functions.
fn contains_nondeterministic_expr(expr: &polyglot_sql::expressions::Expression) -> bool {
    use polyglot_sql::expressions::Expression;
    use polyglot_sql::traversal::DfsIter;

    DfsIter::new(expr).any(|e| match e {
        Expression::CurrentDate(_)
        | Expression::CurrentTime(_)
        | Expression::CurrentTimestamp(_)
        | Expression::CurrentTimestampLTZ(_)
        | Expression::CurrentDatetime(_)
        | Expression::Random(_)
        | Expression::Rand(_)
        | Expression::Uuid(_) => true,
        Expression::Function(f) => {
            f.name.eq_ignore_ascii_case("now")
                || f.name.eq_ignore_ascii_case("random")
                || f.name.eq_ignore_ascii_case("uuid")
                || f.name.eq_ignore_ascii_case("sysdate")
                || f.name.eq_ignore_ascii_case("getdate")
        }
        _ => false,
    })
}

/// Simple string-based non-determinism check used in fallback path.
fn contains_nondeterministic_simple(sql: &str) -> bool {
    let lower = sql.to_lowercase();
    [
        "now(",
        "random(",
        "rand(",
        "uuid(",
        "getdate(",
        "sysdate",
        "current_timestamp",
        "current_date",
        "current_time",
    ]
    .iter()
    .any(|p| lower.contains(p))
}
