/**
 * ADBC driver ids we want to present in the Studio picker.
 *
 * This intentionally includes both:
 * - Apache Arrow ADBC drivers documented under arrow.apache.org/adbc
 * - Community-maintained drivers under the ADBC Driver Foundry org: https://github.com/adbc-drivers
 */
export type AdbcDriverId =
  | "bigquery"
  | "clickhouse"
  | "databricks"
  | "duckdb"
  | "exasol"
  | "flightsql"
  | "jdbc"
  | "mssql"
  | "mysql"
  | "postgresql"
  | "redshift"
  | "singlestore"
  | "snowflake"
  | "sqlite"
  | "trino";

type Spec = {
  driver: AdbcDriverId;
  displayName: string;
  docsUrl: string;
  /** Form fields (flat keys) to hide for this driver variant. */
  hiddenFieldKeys: ReadonlySet<string>;
};

const DRIVER_DOCS_BASE = "https://arrow.apache.org/adbc/main/driver";
const DRIVER_FOUNDRY_BASE = "https://github.com/adbc-drivers";

export const ADBC_DRIVER_SPECS: Record<AdbcDriverId, Spec> = {
  bigquery: {
    driver: "bigquery",
    displayName: "BigQuery",
    docsUrl: `${DRIVER_DOCS_BASE}/bigquery.html`,
    hiddenFieldKeys: new Set(["username", "password"]),
  },
  clickhouse: {
    driver: "clickhouse",
    displayName: "ClickHouse",
    docsUrl: `${DRIVER_FOUNDRY_BASE}/clickhouse`,
    hiddenFieldKeys: new Set([]),
  },
  databricks: {
    driver: "databricks",
    displayName: "Databricks",
    docsUrl: `${DRIVER_FOUNDRY_BASE}/databricks`,
    hiddenFieldKeys: new Set([]),
  },
  duckdb: {
    driver: "duckdb",
    displayName: "DuckDB",
    docsUrl: `${DRIVER_DOCS_BASE}/duckdb.html`,
    hiddenFieldKeys: new Set(["username", "password"]),
  },
  exasol: {
    driver: "exasol",
    displayName: "Exasol",
    docsUrl: `${DRIVER_FOUNDRY_BASE}/exasol`,
    hiddenFieldKeys: new Set([]),
  },
  flightsql: {
    driver: "flightsql",
    displayName: "Flight SQL",
    docsUrl: `${DRIVER_DOCS_BASE}/flight_sql.html`,
    hiddenFieldKeys: new Set([]),
  },
  jdbc: {
    driver: "jdbc",
    displayName: "JDBC",
    docsUrl: `${DRIVER_DOCS_BASE}/jdbc.html`,
    hiddenFieldKeys: new Set([]),
  },
  mssql: {
    driver: "mssql",
    displayName: "SQL Server",
    docsUrl: `${DRIVER_FOUNDRY_BASE}/mssql`,
    hiddenFieldKeys: new Set([]),
  },
  mysql: {
    driver: "mysql",
    displayName: "MySQL",
    docsUrl: `${DRIVER_FOUNDRY_BASE}/mysql`,
    hiddenFieldKeys: new Set([]),
  },
  postgresql: {
    driver: "postgresql",
    displayName: "PostgreSQL",
    docsUrl: `${DRIVER_DOCS_BASE}/postgresql.html`,
    // PostgreSQL ADBC uses userinfo in the URI (see cluster-persist-form.ts).
    hiddenFieldKeys: new Set(["username", "password"]),
  },
  redshift: {
    driver: "redshift",
    displayName: "Redshift",
    docsUrl: `${DRIVER_FOUNDRY_BASE}/redshift`,
    hiddenFieldKeys: new Set([]),
  },
  singlestore: {
    driver: "singlestore",
    displayName: "SingleStore",
    docsUrl: `${DRIVER_FOUNDRY_BASE}/singlestore`,
    hiddenFieldKeys: new Set([]),
  },
  snowflake: {
    driver: "snowflake",
    displayName: "Snowflake",
    docsUrl: `${DRIVER_DOCS_BASE}/snowflake.html`,
    // Snowflake URI format includes user[:password]@account/... in the docs.
    hiddenFieldKeys: new Set(["username", "password"]),
  },
  sqlite: {
    driver: "sqlite",
    displayName: "SQLite",
    docsUrl: `${DRIVER_DOCS_BASE}/sqlite.html`,
    hiddenFieldKeys: new Set(["username", "password"]),
  },
  trino: {
    driver: "trino",
    displayName: "Trino",
    docsUrl: `${DRIVER_FOUNDRY_BASE}/trino`,
    hiddenFieldKeys: new Set([]),
  },
};

export function adbcDocsUrlForDriver(driver: string | undefined): string | null {
  const d = (driver ?? "").trim().toLowerCase() as AdbcDriverId;
  return (ADBC_DRIVER_SPECS as Partial<Record<string, Spec>>)[d]?.docsUrl ?? null;
}

export function hiddenAdbcFieldKeysForDriver(
  driver: string | undefined,
): ReadonlySet<string> | null {
  const d = (driver ?? "").trim().toLowerCase() as AdbcDriverId;
  return (ADBC_DRIVER_SPECS as Partial<Record<string, Spec>>)[d]?.hiddenFieldKeys ?? null;
}

