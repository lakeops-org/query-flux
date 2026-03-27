/**
 * Types for the marketing / picker engine grid (separate from `EngineDescriptor`).
 */

export type EngineCategory =
  | "Lakehouse"
  | "Cloud Warehouse"
  | "Open Source OLAP"
  | "OLTP / General"
  | "Embedded"
  | "Other";

export interface EngineDef {
  name: string;
  simpleIconSlug: string | null;
  hex: string;
  category: EngineCategory;
  description: string;
  engineKey: string | null;
  supported: boolean;
}
