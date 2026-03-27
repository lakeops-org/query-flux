import type { FlatClusterForm } from "@/lib/cluster-form/types";
import type { StudioEngineModule } from "@/lib/studio-engines/types";

function validateStarRocksClusterFlat(flat: FlatClusterForm): string[] {
  if (!flat["auth.username"]?.trim() || !flat["auth.password"]) {
    return ["StarRocks: username and password are required."];
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
