import type { StudioEngineModule } from "@/lib/studio-engines/types";

export const duckDbStudioEngine: StudioEngineModule = {
  descriptor: {
    engineKey: "duckDb",
    displayName: "DuckDB",
    description:
      "Embedded in-process OLAP database. No network endpoint required.",
    hex: "FCC021",
    connectionType: "embedded",
    defaultPort: null,
    endpointExample: null,
    supportedAuth: [],
    implemented: true,
    configFields: [
      {
        key: "databasePath",
        label: "Database path",
        description:
          "Path to the DuckDB database file. Omit for an in-memory database.",
        fieldType: "path",
        required: false,
        example: "/data/analytics.duckdb",
      },
    ],
  },
  catalog: {
    category: "Open Source OLAP",
    simpleIconSlug: "siDuckdb",
    catalogDescription: "In-process SQL OLAP database management system",
  },
};
