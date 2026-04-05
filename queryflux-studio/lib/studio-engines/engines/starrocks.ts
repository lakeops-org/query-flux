import type { FlatClusterForm } from "@/lib/cluster-form/types";
import type { StudioEngineModule } from "@/lib/studio-engines/types";

function validateStarRocksClusterFlat(flat: FlatClusterForm): string[] {
  if (!flat["auth.username"]?.trim() || !flat["auth.password"]) {
    return ["StarRocks: username and password are required."];
  }
  const ps = flat.poolSize?.trim();
  if (ps) {
    const n = Number(ps);
    if (!Number.isInteger(n) || n < 1) {
      return ["StarRocks: connection pool size must be a positive integer."];
    }
  }
  return [];
}

export const starRocksStudioEngine: StudioEngineModule = {
  descriptor: {
    engineKey: "starRocks",
    displayName: "StarRocks",
    description:
      "High-performance OLAP database. Connects via the MySQL wire protocol.",
    hex: "EF4444",
    connectionType: "mysqlWire",
    defaultPort: 9030,
    endpointExample: "mysql://starrocks-fe:9030",
    supportedAuth: ["basic"],
    implemented: true,
    configFields: [
      {
        key: "endpoint",
        label: "Endpoint",
        description:
          "MySQL-protocol connection URL for the StarRocks front-end node.",
        fieldType: "url",
        required: true,
        example: "mysql://starrocks-fe:9030",
      },
      {
        key: "auth.type",
        label: "Auth type",
        description: "Must be 'basic' for StarRocks (username + password).",
        fieldType: "text",
        required: false,
        example: "basic",
      },
      {
        key: "auth.username",
        label: "Username",
        description: "MySQL username for the StarRocks connection.",
        fieldType: "text",
        required: false,
        example: "root",
      },
      {
        key: "auth.password",
        label: "Password",
        description: "MySQL password.",
        fieldType: "secret",
        required: false,
      },
      {
        key: "poolSize",
        label: "Connection pool size",
        description:
          "Max concurrent MySQL connections for QueryFlux (default 8). Separate from max running queries on the engine.",
        fieldType: "number",
        required: false,
        example: "8",
      },
    ],
  },
  catalog: {
    category: "Open Source OLAP",
    simpleIconSlug: null,
    catalogDescription:
      "High-performance analytical database for real-time analytics",
  },
  validateFlat: validateStarRocksClusterFlat,
  customFormId: "starRocks",
};
