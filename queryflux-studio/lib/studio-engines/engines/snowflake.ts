import type { FlatClusterForm } from "@/lib/cluster-form/types";
import type { StudioEngineModule } from "@/lib/studio-engines/types";

function validateSnowflakeClusterFlat(flat: FlatClusterForm): string[] {
  if (!flat.account?.trim()) {
    return ["Snowflake: account identifier is required."];
  }
  const authType = flat["auth.type"] ?? "";
  if (authType === "basic") {
    if (!flat["auth.username"]?.trim()) {
      return ["Snowflake: username is required for password authentication."];
    }
    if (!flat["auth.password"]) {
      return ["Snowflake: password is required for password authentication."];
    }
  }
  if (authType === "keyPair") {
    if (!flat["auth.username"]?.trim()) {
      return ["Snowflake: username is required for key-pair authentication."];
    }
    if (!flat["auth.password"]?.trim()) {
      return ["Snowflake: private key PEM is required for key-pair authentication."];
    }
  }
  if (authType === "bearer") {
    if (!flat["auth.token"]?.trim()) {
      return ["Snowflake: OAuth token is required for OAuth authentication."];
    }
  }
  return [];
}

export const snowflakeStudioEngine: StudioEngineModule = {
  descriptor: {
    engineKey: "snowflake",
    displayName: "Snowflake",
    description:
      "Cloud-native data warehouse. Connects via the Snowflake REST API.",
    hex: "29B5E8",
    connectionType: "http",
    defaultPort: 443,
    endpointExample:
      "https://xy12345.us-east-1.privatelink.snowflakecomputing.com",
    supportedAuth: ["basic", "keyPair", "bearer"],
    implemented: true,
    configFields: [
      {
        key: "account",
        label: "Account",
        description:
          "Snowflake account identifier (e.g. xy12345.us-east-1).",
        fieldType: "text",
        required: true,
        example: "xy12345.us-east-1",
      },
      {
        key: "endpoint",
        label: "Endpoint",
        description:
          "Custom base URL override (e.g. PrivateLink). Omit to derive from account.",
        fieldType: "url",
        required: false,
        example:
          "https://xy12345.us-east-1.privatelink.snowflakecomputing.com",
      },
      {
        key: "warehouse",
        label: "Warehouse",
        description: "Default virtual warehouse for query execution.",
        fieldType: "text",
        required: false,
        example: "COMPUTE_WH",
      },
      {
        key: "role",
        label: "Role",
        description: "Default Snowflake role.",
        fieldType: "text",
        required: false,
        example: "ANALYST",
      },
      {
        key: "catalog",
        label: "Database",
        description: "Default Snowflake database.",
        fieldType: "text",
        required: false,
        example: "MY_DATABASE",
      },
      {
        key: "schema",
        label: "Schema",
        description: "Default Snowflake schema.",
        fieldType: "text",
        required: false,
        example: "PUBLIC",
      },
    ],
  },
  catalog: {
    category: "Cloud Warehouse",
    simpleIconSlug: "siSnowflake",
    catalogDescription: "Cloud-native data warehouse built for the cloud",
  },
  validateFlat: validateSnowflakeClusterFlat,
  customFormId: "snowflake",
};
