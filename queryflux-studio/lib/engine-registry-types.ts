/**
 * Shared types for QueryFlux engine descriptors (Rust config mirror).
 * Kept separate from `engine-registry.ts` so `studio-engines` can import without cycles.
 */

export type FieldType =
  | "text"
  | "url"
  | "path"
  | "secret"
  | "boolean"
  | "number";

export type ConnectionType = "http" | "mysqlWire" | "embedded" | "driver";

export type AuthType =
  | "basic"
  | "bearer"
  | "keyPair"
  | "accessKey"
  | "roleArn";

export interface ConfigField {
  key: string;
  label: string;
  description: string;
  fieldType: FieldType;
  required: boolean;
  example?: string;
}

export interface EngineDescriptor {
  engineKey: string;
  displayName: string;
  description: string;
  hex: string;
  connectionType: ConnectionType;
  defaultPort: number | null;
  endpointExample: string | null;
  supportedAuth: AuthType[];
  configFields: ConfigField[];
  implemented: boolean;
}
