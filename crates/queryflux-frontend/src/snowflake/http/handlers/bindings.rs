//! Snowflake parameter-binding interpolation.
//!
//! Snowflake connectors (Python, JDBC, Go, etc.) send parameterized queries as a SQL text
//! with positional `?` placeholders plus a `parameterBindings` / `bindings` map:
//!
//! ```json
//! {
//!   "sqlText": "SELECT * FROM t WHERE id = ? AND name = ?",
//!   "parameterBindings": {
//!     "1": {"type": "FIXED",  "value": "42"},
//!     "2": {"type": "TEXT",   "value": "alice"}
//!   }
//! }
//! ```
//!
//! `apply_parameter_bindings` substitutes each `?` (outside string literals) with its
//! typed literal so the resulting SQL can be dispatched to any backend.
//!
//! ## Safety
//! TEXT / VARIANT values are single-quoted with internal `'` escaped as `''`.
//! Numeric types are validated before emission — an invalid number is replaced with
//! `NULL` and a warning is logged rather than panicking or injecting raw bytes.

use std::collections::BTreeMap;

use queryflux_core::params::{QueryParam, QueryParams};
use serde_json::Value;
use tracing::warn;

/// Apply Snowflake parameter bindings to a SQL template.
///
/// Replaces positional `?` placeholders (outside single-quoted string literals) with
/// the typed literal for each binding.  Keys in `bindings` are `"1"`, `"2"`, … (1-based).
///
/// Returns the original SQL unchanged when `bindings` is `None`, `null`, or empty.
pub fn apply_parameter_bindings(sql: &str, bindings: Option<&Value>) -> String {
    let Some(bindings) = bindings else {
        return sql.to_string();
    };
    let Some(obj) = bindings.as_object() else {
        return sql.to_string();
    };
    if obj.is_empty() {
        return sql.to_string();
    }

    // Parse bindings into a BTreeMap<u32, (type, value)> so substitution is in
    // numeric order regardless of JSON key ordering.
    let mut ordered: BTreeMap<u32, (&str, &str)> = BTreeMap::new();
    for (key, entry) in obj {
        let Ok(idx) = key.parse::<u32>() else {
            warn!(
                key,
                "Snowflake binding key is not a positive integer — skipping"
            );
            continue;
        };
        let sf_type = entry.get("type").and_then(|v| v.as_str()).unwrap_or("TEXT");
        let value = entry
            .get("value")
            .and_then(|v| v.as_str())
            .unwrap_or("NULL");
        ordered.insert(idx, (sf_type, value));
    }

    substitute_placeholders(sql, &ordered)
}

/// Walk `sql` char-by-char, replacing each `?` outside a string literal with the
/// next binding literal.  Respects single-quoted strings (with `''` escape) and
/// leaves excess `?` placeholders unchanged when bindings run out.
fn substitute_placeholders(sql: &str, ordered: &BTreeMap<u32, (&str, &str)>) -> String {
    // Build a Vec so we can index by position (1-based).
    let bindings: Vec<_> = ordered.values().collect();

    let mut result = String::with_capacity(sql.len() * 2);
    let mut binding_idx: usize = 0; // 0-based into `bindings`
    let mut in_single_quote = false;
    let mut chars = sql.chars().peekable();

    while let Some(ch) = chars.next() {
        match ch {
            '\'' if in_single_quote => {
                // Could be end of string or escaped quote (`''`).
                result.push('\'');
                if chars.peek() == Some(&'\'') {
                    // Escaped quote inside string — consume and emit the second one.
                    result.push('\'');
                    chars.next();
                } else {
                    in_single_quote = false;
                }
            }
            '\'' => {
                in_single_quote = true;
                result.push('\'');
            }
            '?' if !in_single_quote => {
                if let Some(&&(sf_type, value)) = bindings.get(binding_idx) {
                    result.push_str(&render_literal(sf_type, value));
                    binding_idx += 1;
                } else {
                    // More placeholders than bindings — leave as-is, backend will error.
                    warn!(
                        binding_idx,
                        "SQL has more `?` placeholders than parameter bindings"
                    );
                    result.push('?');
                }
            }
            other => result.push(other),
        }
    }

    result
}

/// Returns `true` if `value` is a safe SQL numeric literal (digits, sign, decimal point,
/// exponent). Accepts integers of any magnitude — avoids f64 precision loss on large values.
fn is_safe_numeric(value: &str) -> bool {
    if value.is_empty() {
        return false;
    }
    let bytes = value.as_bytes();
    let start = if bytes[0] == b'+' || bytes[0] == b'-' {
        1
    } else {
        0
    };
    if start >= bytes.len() {
        return false; // sign only
    }
    let mut has_dot = false;
    let mut has_exp = false;
    let mut after_exp = false;
    for &b in &bytes[start..] {
        match b {
            b'0'..=b'9' => {
                if after_exp {
                    after_exp = false; // consumed sign/first-digit after 'e'
                }
            }
            b'.' if !has_dot && !has_exp => has_dot = true,
            b'e' | b'E' if !has_exp => {
                has_exp = true;
                after_exp = true;
            }
            b'+' | b'-' if after_exp => {
                after_exp = false; // sign right after 'e', allowed once
            }
            _ => return false,
        }
    }
    !after_exp // reject trailing 'e'/'e+'/'e-' with no digits
}

/// Render a Snowflake-typed binding value as a SQL literal.
fn render_literal(sf_type: &str, value: &str) -> String {
    match sf_type.to_uppercase().as_str() {
        // Numeric types — validate before emitting to avoid raw-string injection.
        "FIXED" | "REAL" => {
            if value == "NULL" || value.is_empty() {
                return "NULL".to_string();
            }
            if is_safe_numeric(value) {
                value.to_string()
            } else {
                warn!(
                    value,
                    sf_type, "Invalid numeric binding value — substituting NULL"
                );
                "NULL".to_string()
            }
        }
        // Boolean
        "BOOLEAN" => match value.to_uppercase().as_str() {
            "TRUE" | "1" | "YES" | "ON" => "TRUE".to_string(),
            _ => "FALSE".to_string(),
        },
        // Date — Snowflake connectors send ISO-8601 (YYYY-MM-DD).
        "DATE" => format!("DATE '{}'", escape_single_quotes(value)),
        // Timestamp variants — Snowflake connectors send epoch millis or ISO-8601.
        "TIMESTAMP_NTZ" | "TIMESTAMP_LTZ" | "TIMESTAMP_TZ" | "TIMESTAMP" => {
            format!("TIMESTAMP '{}'", escape_single_quotes(value))
        }
        // Time
        "TIME" => format!("TIME '{}'", escape_single_quotes(value)),
        // NULL literal
        _ if value == "NULL" => "NULL".to_string(),
        // TEXT, VARIANT, ARRAY, OBJECT, BINARY and anything unknown — single-quote.
        _ => format!("'{}'", escape_single_quotes(value)),
    }
}

/// Escape single quotes inside a string value by doubling them (`'` → `''`).
fn escape_single_quotes(s: &str) -> String {
    s.replace('\'', "''")
}

// ---------------------------------------------------------------------------
// Typed parameter extraction
// ---------------------------------------------------------------------------

/// Convert a Snowflake binding map to a typed [`QueryParams`] vector.
///
/// Keys in `bindings` are `"1"`, `"2"`, … (1-based); the returned `Vec` is
/// ordered by numeric key so index 0 corresponds to the first `?` placeholder.
///
/// Returns an empty `Vec` when `bindings` is `None`, `null`, or empty.
pub fn bindings_to_params(bindings: Option<&Value>) -> QueryParams {
    let Some(bindings) = bindings else {
        return vec![];
    };
    let Some(obj) = bindings.as_object() else {
        return vec![];
    };
    if obj.is_empty() {
        return vec![];
    }

    let mut ordered: BTreeMap<u32, (&str, &str)> = BTreeMap::new();
    for (key, entry) in obj {
        let Ok(idx) = key.parse::<u32>() else {
            warn!(
                key,
                "Snowflake binding key is not a positive integer — skipping"
            );
            continue;
        };
        let sf_type = entry.get("type").and_then(|v| v.as_str()).unwrap_or("TEXT");
        let value = entry
            .get("value")
            .and_then(|v| v.as_str())
            .unwrap_or("NULL");
        ordered.insert(idx, (sf_type, value));
    }

    ordered
        .values()
        .map(|&(sf_type, value)| binding_to_param(sf_type, value))
        .collect()
}

/// Convert a single Snowflake binding `(type, value)` pair to a [`QueryParam`].
fn binding_to_param(sf_type: &str, value: &str) -> QueryParam {
    if value == "NULL" || value.is_empty() {
        return QueryParam::Null;
    }
    match sf_type.to_uppercase().as_str() {
        "FIXED" | "REAL" => {
            if is_safe_numeric(value) {
                QueryParam::Numeric(value.to_string())
            } else {
                warn!(
                    value,
                    sf_type, "Invalid numeric binding value — substituting NULL"
                );
                QueryParam::Null
            }
        }
        "BOOLEAN" => {
            let b = matches!(value.to_uppercase().as_str(), "TRUE" | "1" | "YES" | "ON");
            QueryParam::Boolean(b)
        }
        "DATE" => QueryParam::Date(value.to_string()),
        "TIMESTAMP_NTZ" | "TIMESTAMP_LTZ" | "TIMESTAMP_TZ" | "TIMESTAMP" => {
            QueryParam::Timestamp(value.to_string())
        }
        "TIME" => QueryParam::Time(value.to_string()),
        _ => QueryParam::Text(value.to_string()),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn no_bindings_returns_original_sql() {
        let sql = "SELECT * FROM t WHERE id = ?";
        assert_eq!(apply_parameter_bindings(sql, None), sql);
    }

    #[test]
    fn null_bindings_value_returns_original_sql() {
        let sql = "SELECT 1";
        let v = json!(null);
        assert_eq!(apply_parameter_bindings(sql, Some(&v)), sql);
    }

    #[test]
    fn empty_bindings_object_returns_original_sql() {
        let sql = "SELECT 1";
        let v = json!({});
        assert_eq!(apply_parameter_bindings(sql, Some(&v)), sql);
    }

    #[test]
    fn text_binding_is_single_quoted() {
        let sql = "SELECT ?";
        let v = json!({"1": {"type": "TEXT", "value": "alice"}});
        assert_eq!(apply_parameter_bindings(sql, Some(&v)), "SELECT 'alice'");
    }

    #[test]
    fn text_binding_escapes_internal_quotes() {
        let sql = "SELECT ?";
        let v = json!({"1": {"type": "TEXT", "value": "o'brien"}});
        assert_eq!(apply_parameter_bindings(sql, Some(&v)), "SELECT 'o''brien'");
    }

    #[test]
    fn fixed_binding_is_unquoted() {
        let sql = "WHERE id = ?";
        let v = json!({"1": {"type": "FIXED", "value": "42"}});
        assert_eq!(apply_parameter_bindings(sql, Some(&v)), "WHERE id = 42");
    }

    #[test]
    fn real_binding_is_unquoted() {
        let sql = "WHERE score > ?";
        let v = json!({"1": {"type": "REAL", "value": "3.14"}});
        assert_eq!(
            apply_parameter_bindings(sql, Some(&v)),
            "WHERE score > 3.14"
        );
    }

    #[test]
    fn invalid_numeric_substitutes_null() {
        let sql = "WHERE id = ?";
        let v = json!({"1": {"type": "FIXED", "value": "not_a_number"}});
        assert_eq!(apply_parameter_bindings(sql, Some(&v)), "WHERE id = NULL");
    }

    #[test]
    fn boolean_true_variants() {
        for val in &["true", "1", "yes", "YES", "ON"] {
            let sql = "WHERE flag = ?";
            let v = json!({"1": {"type": "BOOLEAN", "value": val}});
            assert_eq!(
                apply_parameter_bindings(sql, Some(&v)),
                "WHERE flag = TRUE",
                "failed for value {val}"
            );
        }
    }

    #[test]
    fn boolean_false_variants() {
        for val in &["false", "0", "no", "off"] {
            let sql = "WHERE flag = ?";
            let v = json!({"1": {"type": "BOOLEAN", "value": val}});
            assert_eq!(
                apply_parameter_bindings(sql, Some(&v)),
                "WHERE flag = FALSE",
                "failed for value {val}"
            );
        }
    }

    #[test]
    fn date_binding_wrapped() {
        let sql = "WHERE dt = ?";
        let v = json!({"1": {"type": "DATE", "value": "2025-01-15"}});
        assert_eq!(
            apply_parameter_bindings(sql, Some(&v)),
            "WHERE dt = DATE '2025-01-15'"
        );
    }

    #[test]
    fn timestamp_binding_wrapped() {
        let sql = "WHERE ts = ?";
        let v = json!({"1": {"type": "TIMESTAMP_NTZ", "value": "2025-01-15 12:00:00"}});
        assert_eq!(
            apply_parameter_bindings(sql, Some(&v)),
            "WHERE ts = TIMESTAMP '2025-01-15 12:00:00'"
        );
    }

    #[test]
    fn multiple_bindings_substituted_in_order() {
        let sql = "SELECT ?, ?, ?";
        let v = json!({
            "1": {"type": "FIXED",   "value": "1"},
            "2": {"type": "TEXT",    "value": "hello"},
            "3": {"type": "BOOLEAN", "value": "true"}
        });
        assert_eq!(
            apply_parameter_bindings(sql, Some(&v)),
            "SELECT 1, 'hello', TRUE"
        );
    }

    #[test]
    fn placeholder_inside_string_literal_is_not_substituted() {
        // The `?` inside the string must NOT be replaced.
        let sql = "WHERE name = '?' AND id = ?";
        let v = json!({"1": {"type": "FIXED", "value": "99"}});
        assert_eq!(
            apply_parameter_bindings(sql, Some(&v)),
            "WHERE name = '?' AND id = 99"
        );
    }

    #[test]
    fn escaped_quote_inside_string_does_not_prematurely_end_string() {
        // `''` inside a string is an escaped quote, not end-of-string.
        let sql = "WHERE x = 'it''s here' AND id = ?";
        let v = json!({"1": {"type": "FIXED", "value": "7"}});
        assert_eq!(
            apply_parameter_bindings(sql, Some(&v)),
            "WHERE x = 'it''s here' AND id = 7"
        );
    }

    #[test]
    fn fewer_bindings_than_placeholders_leaves_excess_unchanged() {
        let sql = "SELECT ?, ?, ?";
        let v = json!({"1": {"type": "FIXED", "value": "1"}});
        // Only first `?` replaced; remaining two left as `?`.
        assert_eq!(apply_parameter_bindings(sql, Some(&v)), "SELECT 1, ?, ?");
    }

    #[test]
    fn null_value_emits_null_literal() {
        let sql = "WHERE x = ?";
        let v = json!({"1": {"type": "TEXT", "value": "NULL"}});
        assert_eq!(apply_parameter_bindings(sql, Some(&v)), "WHERE x = NULL");
    }

    #[test]
    fn bindings_applied_in_numeric_key_order_not_json_order() {
        // JSON object key ordering is not guaranteed — keys parsed as u32 for ordering.
        let sql = "SELECT ?, ?";
        let v = json!({
            "2": {"type": "TEXT",  "value": "second"},
            "1": {"type": "TEXT",  "value": "first"}
        });
        assert_eq!(
            apply_parameter_bindings(sql, Some(&v)),
            "SELECT 'first', 'second'"
        );
    }
}
