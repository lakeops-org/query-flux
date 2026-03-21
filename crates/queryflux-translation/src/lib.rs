pub mod sqlglot;

use std::collections::HashMap;

use async_trait::async_trait;
use queryflux_core::{error::Result, query::SqlDialect};
pub use sqlglot::SqlglotTranslator;

/// Schema context passed to the translator so sqlglot can produce accurate output.
/// Maps table name → { column name → SQL type string }.
#[derive(Debug, Default, Clone)]
pub struct SchemaContext {
    pub catalog: Option<String>,
    pub database: Option<String>,
    /// table_name → { col_name → type_string }
    pub tables: HashMap<String, HashMap<String, String>>,
}

impl SchemaContext {
    pub fn is_empty(&self) -> bool {
        self.tables.is_empty()
    }
}

/// Translates SQL from one dialect to another.
///
/// The primary implementation (`SqlglotTranslator`) uses the sqlglot Python library
/// via PyO3. Additional implementations can provide custom fixups or passthrough.
#[async_trait]
pub trait TranslatorTrait: Send + Sync {
    fn source_dialect(&self) -> &SqlDialect;
    fn target_dialect(&self) -> &SqlDialect;

    /// Translate `sql` from `source_dialect` to `target_dialect`.
    /// `schema_context` is optional — when provided, sqlglot uses schema-aware
    /// optimization for more accurate type handling.
    async fn translate(&self, sql: &str, schema_context: &SchemaContext) -> Result<String>;
}

/// Passthrough translator — returns the SQL unchanged.
/// Used when source and target dialects are the same.
pub struct PassthroughTranslator {
    dialect: SqlDialect,
}

impl PassthroughTranslator {
    pub fn new(dialect: SqlDialect) -> Self {
        Self { dialect }
    }
}

#[async_trait]
impl TranslatorTrait for PassthroughTranslator {
    fn source_dialect(&self) -> &SqlDialect {
        &self.dialect
    }
    fn target_dialect(&self) -> &SqlDialect {
        &self.dialect
    }
    async fn translate(&self, sql: &str, _schema_context: &SchemaContext) -> Result<String> {
        Ok(sql.to_string())
    }
}

/// Central translation service.
///
/// Call `maybe_translate` before submitting SQL to a backend engine.
/// Returns the original SQL unchanged when dialects match (zero overhead).
pub struct TranslationService {
    enabled: bool,
}

impl TranslationService {
    /// Create a service backed by sqlglot. Verifies sqlglot is importable at startup.
    pub fn new_sqlglot() -> Result<Self> {
        SqlglotTranslator::check_available()?;
        Ok(Self { enabled: true })
    }

    /// Create a no-op service (translation disabled).
    pub fn disabled() -> Self {
        Self { enabled: false }
    }

    /// Translate `sql` from `src` to `tgt` if they differ.
    /// Returns the original SQL unchanged when dialects match or translation is disabled.
    pub async fn maybe_translate(
        &self,
        sql: &str,
        src: &SqlDialect,
        tgt: &SqlDialect,
        schema: &SchemaContext,
    ) -> Result<String> {
        if !self.enabled || src.is_compatible_with(tgt) {
            return Ok(sql.to_string());
        }
        let translator = SqlglotTranslator::new(src.clone(), tgt.clone());
        translator.translate(sql, schema).await
    }
}
