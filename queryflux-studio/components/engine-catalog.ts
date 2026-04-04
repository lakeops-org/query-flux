/**
 * Master list of query engines QueryFlux can proxy / translate to.
 * Dialects without a backend use static `EngineDef` rows. Registered backends use
 * `{ k: "studio", engineKey }` slots filled from `lib/studio-engines/manifest.ts`.
 */

import type { EngineCategory, EngineDef } from "@/lib/engine-catalog-types";
import { studioModuleToEngineDef } from "@/lib/studio-engines/catalog";
import {
  buildStudioTypeAliases,
  STUDIO_MODULE_BY_KEY,
} from "@/lib/studio-engines/manifest";

export type { EngineCategory, EngineDef } from "@/lib/engine-catalog-types";

type StudioCatalogSlot = { readonly k: "studio"; readonly engineKey: string };

type CatalogSlot = EngineDef | StudioCatalogSlot;

function isStudioCatalogSlot(s: CatalogSlot): s is StudioCatalogSlot {
  return "k" in s && (s as StudioCatalogSlot).k === "studio";
}

function expandCatalog(slots: CatalogSlot[]): EngineDef[] {
  return slots.map((s): EngineDef => {
    if (isStudioCatalogSlot(s)) {
      const m = STUDIO_MODULE_BY_KEY.get(s.engineKey);
      if (!m) {
        throw new Error(
          `engine-catalog: unknown studio engineKey "${s.engineKey}" — add it to studio-engines/manifest.ts`,
        );
      }
      return studioModuleToEngineDef(m);
    }
    return s;
  });
}

const ENGINE_CATALOG_SLOTS = [
  // ── Lakehouse ────────────────────────────────────────────────────────────
  { k: "studio", engineKey: "trino" },
  {
    name: "Presto",
    simpleIconSlug: "siPresto",
    hex: "5890FF",
    category: "Lakehouse",
    description: "Open-source distributed SQL query engine by Meta",
    engineKey: null,
    supported: false,
  },
  {
    name: "Apache Spark",
    simpleIconSlug: "siApachespark",
    hex: "E25A1C",
    category: "Lakehouse",
    description: "Unified analytics engine for large-scale data processing",
    engineKey: null,
    supported: false,
  },
  {
    name: "Databricks",
    simpleIconSlug: "siDatabricks",
    hex: "FF3621",
    category: "Lakehouse",
    description: "Unified data analytics platform built on Apache Spark",
    engineKey: null,
    supported: false,
  },
  {
    name: "Apache Hive",
    simpleIconSlug: "siApachehive",
    hex: "FDEE21",
    category: "Lakehouse",
    description: "Data warehouse software facilitating SQL over Hadoop",
    engineKey: null,
    supported: false,
  },
  {
    name: "Apache Drill",
    simpleIconSlug: null,
    hex: "1EAAF1",
    category: "Lakehouse",
    description: "Schema-free SQL query engine for Hadoop and NoSQL",
    engineKey: null,
    supported: false,
  },
  {
    name: "Dremio",
    simpleIconSlug: null,
    hex: "27BEC8",
    category: "Lakehouse",
    description: "Data lakehouse platform with SQL acceleration",
    engineKey: null,
    supported: false,
  },

  // ── Cloud Warehouse ───────────────────────────────────────────────────────
  { k: "studio", engineKey: "snowflake" },
  {
    name: "BigQuery",
    simpleIconSlug: null,
    hex: "4285F4",
    category: "Cloud Warehouse",
    description: "Serverless, multi-cloud data warehouse by Google",
    engineKey: null,
    supported: false,
  },
  {
    name: "Redshift",
    simpleIconSlug: null,
    hex: "8C4FFF",
    category: "Cloud Warehouse",
    description: "Petabyte-scale data warehouse by Amazon Web Services",
    engineKey: null,
    supported: false,
  },
  { k: "studio", engineKey: "athena" },
  {
    name: "Fabric",
    simpleIconSlug: null,
    hex: "0078D4",
    category: "Cloud Warehouse",
    description: "Microsoft end-to-end analytics SaaS platform",
    engineKey: null,
    supported: false,
  },
  {
    name: "Exasol",
    simpleIconSlug: null,
    hex: "003A70",
    category: "Cloud Warehouse",
    description: "In-memory, column-oriented analytical database",
    engineKey: null,
    supported: false,
  },

  // ── Open Source OLAP ─────────────────────────────────────────────────────
  { k: "studio", engineKey: "clickHouse" },
  { k: "studio", engineKey: "starRocks" },
  {
    name: "Apache Doris",
    simpleIconSlug: null,
    hex: "0043EB",
    category: "Open Source OLAP",
    description: "High-performance real-time analytical database",
    engineKey: null,
    supported: false,
  },
  {
    name: "Apache Druid",
    simpleIconSlug: "siApachedruid",
    hex: "29F1FB",
    category: "Open Source OLAP",
    description: "Real-time analytics database for event-driven data",
    engineKey: null,
    supported: false,
  },
  { k: "studio", engineKey: "duckDb" },
  { k: "studio", engineKey: "duckDbHttp" },
  {
    name: "RisingWave",
    simpleIconSlug: null,
    hex: "1D4ED8",
    category: "Open Source OLAP",
    description:
      "Cloud-native streaming database with PostgreSQL compatibility",
    engineKey: null,
    supported: false,
  },
  {
    name: "Materialize",
    simpleIconSlug: null,
    hex: "6B3FE6",
    category: "Open Source OLAP",
    description: "Operational data warehouse for real-time views",
    engineKey: null,
    supported: false,
  },
  {
    name: "SingleStore",
    simpleIconSlug: "siSinglestore",
    hex: "AA00FF",
    category: "Open Source OLAP",
    description: "Unified database for transactions and analytics",
    engineKey: null,
    supported: false,
  },

  // ── OLTP / General ───────────────────────────────────────────────────────
  {
    name: "PostgreSQL",
    simpleIconSlug: "siPostgresql",
    hex: "4169E1",
    category: "OLTP / General",
    description: "Advanced open-source relational database system",
    engineKey: null,
    supported: false,
  },
  {
    name: "MySQL",
    simpleIconSlug: "siMysql",
    hex: "4479A1",
    category: "OLTP / General",
    description: "World's most popular open-source relational database",
    engineKey: null,
    supported: false,
  },
  {
    name: "MariaDB",
    simpleIconSlug: "siMariadb",
    hex: "003545",
    category: "OLTP / General",
    description: "Community-developed fork of MySQL",
    engineKey: null,
    supported: false,
  },
  {
    name: "Oracle",
    simpleIconSlug: null,
    hex: "F80000",
    category: "OLTP / General",
    description: "Industry-leading enterprise relational database",
    engineKey: null,
    supported: false,
  },
  {
    name: "SQL Server",
    simpleIconSlug: null,
    hex: "CC2927",
    category: "OLTP / General",
    description: "Microsoft's enterprise relational database platform",
    engineKey: null,
    supported: false,
  },
  {
    name: "Teradata",
    simpleIconSlug: "siTeradata",
    hex: "F37440",
    category: "OLTP / General",
    description: "Enterprise data warehouse and analytics platform",
    engineKey: null,
    supported: false,
  },

  // ── Embedded ─────────────────────────────────────────────────────────────
  {
    name: "SQLite",
    simpleIconSlug: "siSqlite",
    hex: "003B57",
    category: "Embedded",
    description: "Self-contained, serverless SQL database engine",
    engineKey: null,
    supported: false,
  },

  // ── Other ─────────────────────────────────────────────────────────────────
  {
    name: "Apache Solr",
    simpleIconSlug: "siApachesolr",
    hex: "D9411E",
    category: "Other",
    description: "Open-source enterprise search platform",
    engineKey: null,
    supported: false,
  },
] as const satisfies readonly CatalogSlot[];

export const ENGINE_CATALOG: EngineDef[] = expandCatalog([...ENGINE_CATALOG_SLOTS]);

const STATIC_ENGINE_TYPE_ALIASES: Record<string, string> = {
  mysql: "MySQL",
  postgres: "PostgreSQL",
  postgresql: "PostgreSQL",
  presto: "Presto",
  snowflake: "Snowflake",
  bigquery: "BigQuery",
  redshift: "Redshift",
  databricks: "Databricks",
  spark: "Apache Spark",
  apachespark: "Apache Spark",
  hive: "Apache Hive",
  apachehive: "Apache Hive",
  druid: "Apache Druid",
  apachedruid: "Apache Druid",
  sqlite: "SQLite",
  oracle: "Oracle",
  teradata: "Teradata",
  singlestore: "SingleStore",
  mariadb: "MariaDB",
  risingwave: "RisingWave",
  materialize: "Materialize",
  doris: "Apache Doris",
  apachedoris: "Apache Doris",
  dremio: "Dremio",
  drill: "Apache Drill",
  apachedrill: "Apache Drill",
};

const ENGINE_TYPE_ALIASES: Record<string, string> = {
  ...STATIC_ENGINE_TYPE_ALIASES,
  ...buildStudioTypeAliases(),
};

export function findEngineByType(engineType: string): EngineDef | undefined {
  const key = engineType.toLowerCase().replace(/\s+/g, "");
  const canonicalName = ENGINE_TYPE_ALIASES[key] ?? engineType;
  return ENGINE_CATALOG.find(
    (e) => e.name.toLowerCase() === canonicalName.toLowerCase(),
  );
}

export const CATEGORY_ORDER: EngineCategory[] = [
  "Lakehouse",
  "Cloud Warehouse",
  "Open Source OLAP",
  "OLTP / General",
  "Embedded",
  "Other",
];
