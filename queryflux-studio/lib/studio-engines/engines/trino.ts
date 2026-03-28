import type { FlatClusterForm } from "@/lib/cluster-form/types";
import type { StudioEngineModule } from "@/lib/studio-engines/types";

function validateTrinoClusterFlat(flat: FlatClusterForm): string[] {
  const at = flat["auth.type"]?.trim() ?? "";
  if (!at) {
    return ["Trino: choose an authentication method."];
  }
  if (at === "basic") {
    if (!flat["auth.username"]?.trim() || !flat["auth.password"]) {
      return ["Trino: username and password are required for basic authentication."];
    }
  }
  if (at === "bearer") {
    if (!flat["auth.token"]?.trim()) {
      return ["Trino: bearer token is required."];
    }
  }
  return [];
}

export const trinoStudioEngine: StudioEngineModule = {
  descriptor: {
    engineKey: "trino",
    displayName: "Trino",
    description:
      "Distributed SQL query engine using the Trino REST protocol (async submit/poll).",
    hex: "DD00A1",
    connectionType: "http",
    defaultPort: 8080,
    endpointExample: "http://trino-coordinator:8080",
    supportedAuth: ["basic", "bearer"],
    implemented: true,
    configFields: [
      {
        key: "endpoint",
        label: "Endpoint",
        description: "HTTP(S) base URL of the Trino coordinator.",
        fieldType: "url",
        required: true,
        example: "http://trino-coordinator:8080",
      },
      {
        key: "auth.type",
        label: "Auth type",
        description:
          "Authentication mechanism. Choose 'basic' for username/password or 'bearer' for a JWT/OAuth2 token.",
        fieldType: "text",
        required: false,
        example: "basic",
      },
      {
        key: "auth.username",
        label: "Username",
        description: "Basic auth username (required when auth.type = basic).",
        fieldType: "text",
        required: false,
        example: "admin",
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
        description:
          "JWT or OAuth2 bearer token (required when auth.type = bearer).",
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
    category: "Lakehouse",
    simpleIconSlug: "siTrino",
    catalogDescription: "Distributed SQL query engine for big data analytics",
  },
  validateFlat: validateTrinoClusterFlat,
  customFormId: "trino",
};
