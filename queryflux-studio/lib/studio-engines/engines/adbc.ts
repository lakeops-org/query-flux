import type { StudioEngineModule } from "@/lib/studio-engines/types";

export const adbcStudioEngine: StudioEngineModule = {
  descriptor: {
    engineKey: "adbc",
    displayName: "ADBC",
    description:
      "Generic ADBC adapter. Connect to engines through installed ADBC drivers.",
    hex: "6366F1",
    connectionType: "driver",
    defaultPort: null,
    endpointExample: null,
    supportedAuth: ["basic"],
    implemented: true,
    configFields: [
      {
        key: "driver",
        label: "Driver",
        description:
          "ADBC driver name (for example 'trino', 'postgresql', 'mysql') or shared library path.",
        fieldType: "text",
        required: true,
        example: "trino",
      },
      {
        key: "uri",
        label: "URI",
        description:
          "Driver-specific connection URI. For PostgreSQL ADBC, put user and password in the URI (e.g. postgresql://user:pass@localhost:5433/postgres).",
        fieldType: "text",
        required: true,
        example: "http://trino-host:8080",
      },
      {
        key: "username",
        label: "Username",
        description: "Authentication username (optional).",
        fieldType: "text",
        required: false,
        example: "admin",
      },
      {
        key: "password",
        label: "Password",
        description: "Authentication password (optional).",
        fieldType: "secret",
        required: false,
      },
      {
        key: "dbKwargs",
        label: "Driver options (JSON)",
        description:
          "Driver-specific options as a JSON object string, for example {\"catalog\":\"hive\"}.",
        fieldType: "text",
        required: false,
        example: "{}",
      },
      {
        key: "flightSqlEngine",
        label: "FlightSQL target engine",
        description:
          "Only when driver is flightsql: backend SQL dialect for translation/transpilation. Flight SQL is transport only—it does not tell us which engine speaks on the other end.",
        fieldType: "text",
        required: false,
        example: "starrocks",
      },
      {
        key: "poolSize",
        label: "Pool size",
        description: "Maximum pooled connections. Defaults to 4.",
        fieldType: "number",
        required: false,
        example: "4",
      },
    ],
  },
  catalog: {
    category: "OLTP / General",
    simpleIconSlug: null,
    catalogDescription: "Generic Arrow Database Connectivity (ADBC) adapter",
  },
};
