use async_trait::async_trait;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use queryflux_core::{
    error::{QueryFluxError, Result},
    query::SqlDialect,
};
use tracing::debug;

use crate::{SchemaContext, TranslatorTrait};

/// SQL translator backed by the sqlglot Python library (via PyO3).
pub struct SqlglotTranslator {
    source: SqlDialect,
    target: SqlDialect,
}

impl SqlglotTranslator {
    pub fn new(source: SqlDialect, target: SqlDialect) -> Self {
        Self { source, target }
    }

    /// Verify that sqlglot is importable. Call once at startup.
    pub fn check_available() -> Result<()> {
        Python::attach(|py| {
            PyModule::import(py, "sqlglot").map_err(|e| {
                QueryFluxError::Translation(format!(
                    "sqlglot not found — run `pip install sqlglot`: {e}"
                ))
            })?;
            Ok(())
        })
    }
}

#[async_trait]
impl TranslatorTrait for SqlglotTranslator {
    fn source_dialect(&self) -> &SqlDialect {
        &self.source
    }

    fn target_dialect(&self) -> &SqlDialect {
        &self.target
    }

    async fn translate(&self, sql: &str, schema_context: &SchemaContext) -> Result<String> {
        let sql = sql.to_string();
        let src = self.source.sqlglot_name().to_string();
        let tgt = self.target.sqlglot_name().to_string();
        let schema_context = schema_context.clone();

        tokio::task::spawn_blocking(move || translate_with_gil(&sql, &src, &tgt, &schema_context))
            .await
            .map_err(|e| QueryFluxError::Translation(format!("spawn_blocking error: {e}")))?
    }
}

fn translate_with_gil(
    sql: &str,
    src: &str,
    tgt: &str,
    schema_context: &SchemaContext,
) -> Result<String> {
    Python::attach(|py| {
        let sqlglot = PyModule::import(py, "sqlglot")
            .map_err(|e| QueryFluxError::Translation(format!("Failed to import sqlglot: {e}")))?;

        if schema_context.is_empty() {
            debug!(src, tgt, "sqlglot dialect-only translation");
            translate_dialect_only(py, &sqlglot, sql, src, tgt)
        } else {
            debug!(src, tgt, "sqlglot schema-aware translation");
            translate_with_schema(py, &sqlglot, sql, src, tgt, schema_context)
        }
    })
}

fn translate_dialect_only(
    py: Python<'_>,
    sqlglot: &Bound<'_, PyModule>,
    sql: &str,
    src: &str,
    tgt: &str,
) -> Result<String> {
    let kwargs = PyDict::new(py);
    kwargs.set_item("read", src).ok();
    kwargs.set_item("write", tgt).ok();

    let result = sqlglot
        .call_method("transpile", (sql,), Some(&kwargs))
        .map_err(|e| QueryFluxError::Translation(format!("sqlglot.transpile failed: {e}")))?;

    let list: Vec<String> = result.extract().map_err(|e| {
        QueryFluxError::Translation(format!("Failed to extract transpile result: {e}"))
    })?;

    Ok(list.into_iter().next().unwrap_or_default())
}

fn translate_with_schema(
    py: Python<'_>,
    sqlglot: &Bound<'_, PyModule>,
    sql: &str,
    src: &str,
    tgt: &str,
    schema_context: &SchemaContext,
) -> Result<String> {
    let schema_dict = PyDict::new(py);
    for (table, cols) in &schema_context.tables {
        let col_dict = PyDict::new(py);
        for (col, ty) in cols {
            col_dict.set_item(col, ty).ok();
        }
        schema_dict.set_item(table, col_dict).ok();
    }

    let parse_kwargs = PyDict::new(py);
    parse_kwargs.set_item("dialect", src).ok();
    let tree = sqlglot
        .call_method("parse_one", (sql,), Some(&parse_kwargs))
        .map_err(|e| QueryFluxError::Translation(format!("sqlglot.parse_one failed: {e}")))?;

    let optimizer = PyModule::import(py, "sqlglot.optimizer").map_err(|e| {
        QueryFluxError::Translation(format!("Failed to import sqlglot.optimizer: {e}"))
    })?;
    let schema_mod = PyModule::import(py, "sqlglot.schema").map_err(|e| {
        QueryFluxError::Translation(format!("Failed to import sqlglot.schema: {e}"))
    })?;

    let mapping_schema_kwargs = PyDict::new(py);
    mapping_schema_kwargs.set_item("schema", schema_dict).ok();
    let schema_obj = schema_mod
        .call_method("MappingSchema", (), Some(&mapping_schema_kwargs))
        .map_err(|e| {
            QueryFluxError::Translation(format!("MappingSchema construction failed: {e}"))
        })?;

    let opt_kwargs = PyDict::new(py);
    opt_kwargs.set_item("schema", schema_obj).ok();
    opt_kwargs.set_item("dialect", src).ok();
    let optimized = optimizer
        .call_method("optimize", (&tree,), Some(&opt_kwargs))
        .unwrap_or_else(|e| {
            tracing::warn!("sqlglot optimizer failed ({e}), falling back to dialect-only");
            tree
        });

    let sql_kwargs = PyDict::new(py);
    sql_kwargs.set_item("dialect", tgt).ok();
    let translated: String = optimized
        .call_method("sql", (), Some(&sql_kwargs))
        .map_err(|e| QueryFluxError::Translation(format!("AST.sql() failed: {e}")))?
        .extract()
        .map_err(|e| QueryFluxError::Translation(format!("Failed to extract sql result: {e}")))?;

    Ok(translated)
}
