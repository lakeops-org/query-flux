# Query translation

This document explains **how** QueryFlux converts SQL between dialects, **when** that happens, and how it fits into the query path.

## Role in the pipeline

Translation runs **after** routing has chosen a **cluster group** and **after** the cluster manager has selected a **concrete cluster** (adapter), but **before** the SQL is submitted or executed on the backend.

Conceptually:

```
Client SQL
  → routers pick cluster group
  → cluster manager picks cluster (adapter)
  → translate(client dialect → engine dialect)   ← this document
  → adapter.submit_query / execute_as_arrow
```

The implementation lives mainly in the `queryflux-translation` crate (`TranslationService`, `SqlglotTranslator`) and is invoked from shared dispatch code in `queryflux-frontend` (`dispatch_query`, `execute_to_sink`).

## Source and target dialects

- **Source dialect** comes from the **frontend protocol**: each `FrontendProtocol` has a `default_dialect()` (e.g. Trino HTTP → Trino, MySQL wire → MySQL). See `queryflux_core::query::FrontendProtocol`.
- **Target dialect** comes from the **engine type** of the chosen adapter: `EngineType::dialect()` (e.g. DuckDB → DuckDB, StarRocks → StarRocks). See `queryflux_core::query::EngineType`.

If source and target are considered **compatible**, translation is skipped entirely (no sqlglot call). Notably, **MySQL and StarRocks** are treated as mutually compatible in `SqlDialect::is_compatible_with`, reflecting similar client SQL expectations.

## TranslationService and sqlglot

`TranslationService` is the façade used by the frontend:

- **`new_sqlglot()`** — Verifies that Python can import `sqlglot` (via PyO3). If that fails at startup, the binary logs a warning and falls back to **`disabled()`**, which passes SQL through unchanged.
- **`maybe_translate(sql, src, tgt, schema)`** — If translation is disabled, or dialects are compatible, returns the original string. Otherwise it constructs a `SqlglotTranslator` and runs translation.

`SqlglotTranslator` runs work on a **blocking thread pool** (`spawn_blocking`) because it holds the Python GIL. Inside Python it either:

1. **Dialect-only** — When `SchemaContext` is empty: `sqlglot.transpile(sql, read=<src>, write=<tgt>)`.
2. **Schema-aware** — When tables/columns are populated: parse with `parse_one`, build a `MappingSchema`, run `sqlglot.optimizer.optimize`, then emit SQL with the target dialect. If optimization fails, it **falls back** to dialect-only behavior (with a warning).

The Rust type `SchemaContext` (`queryflux_translation::SchemaContext`) carries optional catalog/database and a map of **table → column → SQL type string** for sqlglot’s schema-aware path.

### Current default on the hot path

Today, dispatch passes **`SchemaContext::default()`** (empty tables). So in production query paths you get **dialect-only** transpilation. The schema-aware branch is **implemented** in `sqlglot.rs` and is ready for future wiring (e.g. catalog providers or static schema config) to populate `SchemaContext` before `maybe_translate`.

## Passthrough and performance

When the client dialect matches the engine (e.g. Trino client → Trino cluster), `maybe_translate` returns immediately with **no Python work**. That keeps the common “Trino in, Trino out” case cheap.

## Configuration

`translation` in the root config (`queryflux_core::config::TranslationConfig`) includes:

- **`errorOnUnsupported`** — Intended to control strictness when sqlglot cannot translate constructs (see config comments; behavior should align with adapter error handling as the project evolves).
- **`pythonScripts`** — Optional extension point for post-sqlglot fixups keyed by dialect pair strings.

See `config.local.yaml` / your deployment YAML for concrete values.

## Failure modes

- **sqlglot missing** — Startup degrades to a disabled translation service; SQL is sent as-is, which may fail on the backend if dialects differ.
- **Translation errors** — Dispatch releases the acquired cluster slot and returns an error to the client (async Trino path logs and propagates; sync `execute_to_sink` path reports via the result sink).

For how routing picks the group and cluster **before** translation, see [routing-and-clusters.md](routing-and-clusters.md).
