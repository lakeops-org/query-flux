import type { StudioEngineModule } from "@/lib/studio-engines/types";

export const duckDbHttpStudioEngine: StudioEngineModule = {
  descriptor: {
    engineKey: "duckDbHttp",
    displayName: "DuckDB HTTP Server",
    description:
      "Remote DuckDB with the community httpserver extension (HTTP POST /query).",
    hex: "E8AC00",
    connectionType: "http",
    defaultPort: 4321,
    endpointExample: "http://duckdb-server:4321",
    supportedAuth: ["basic", "bearer"],
    implemented: true,
    configFields: [
      {
        key: "endpoint",
        label: "Endpoint",
        description: "HTTP base URL of the DuckDB httpserver (no trailing path).",
        fieldType: "url",
        required: true,
        example: "http://duckdb-server:4321",
      },
      {
        key: "auth.type",
        label: "Auth type",
        description: "Optional HTTP auth to the DuckDB server.",
        fieldType: "text",
        required: false,
        example: "basic",
      },
      {
        key: "auth.username",
        label: "Username",
        description: "Basic auth username (when auth.type = basic).",
        fieldType: "text",
        required: false,
      },
      {
        key: "auth.password",
        label: "Password",
        description: "Basic auth password.",
        fieldType: "secret",
        required: false,
      },
      {
        key: "auth.token",
        label: "Bearer token",
        description: "Bearer token (when auth.type = bearer).",
        fieldType: "secret",
        required: false,
      },
      {
        key: "tls.insecureSkipVerify",
        label: "Skip TLS verification",
        description:
          "Disable TLS certificate verification. Use only in development.",
        fieldType: "boolean",
        required: false,
        example: "false",
      },
    ],
  },
  catalog: {
    category: "Open Source OLAP",
    simpleIconSlug: "siDuckdb",
    catalogDescription:
      "Remote DuckDB with the community httpserver extension (HTTP POST /query)",
  },
  extraTypeAliases: {
    duckdbhttp: "DuckDB HTTP Server",
    duckdbhttpserver: "DuckDB HTTP Server",
  },
  engineAffinity: { label: "DuckDB (HTTP)" },
};
