/**
 * sqlglot `write` / `read` dialect names (lowercase module names).
 * Mirrors `sqlglot/dialects/__init__.py` `DIALECTS` → `MODULE_BY_DIALECT`.
 * Update when upgrading sqlglot if new dialects ship.
 */
export const SQLGLOT_WRITE_DIALECTS: readonly string[] = [
  "athena",
  "bigquery",
  "clickhouse",
  "databricks",
  "doris",
  "dremio",
  "drill",
  "druid",
  "duckdb",
  "dune",
  "exasol",
  "fabric",
  "hive",
  "materialize",
  "mysql",
  "oracle",
  "postgres",
  "presto",
  "prql",
  "redshift",
  "risingwave",
  "singlestore",
  "snowflake",
  "solr",
  "spark",
  "spark2",
  "sqlite",
  "starrocks",
  "tableau",
  "teradata",
  "trino",
  "tsql",
];
