"use client";

import type { EngineDescriptor } from "@/lib/engine-registry";
import { ConfigFieldRow } from "./config-field-row";
import type { FlatClusterConfig, PatchClusterConfig } from "./types";

/**
 * Renders all {@link EngineDescriptor.configFields} generically (DuckDB, ClickHouse preview, etc.).
 */
export function GenericEngineClusterConfig({
  descriptor,
  flat,
  onPatch,
}: {
  descriptor: EngineDescriptor;
  flat: FlatClusterConfig;
  onPatch: PatchClusterConfig;
}) {
  return (
    <div className="space-y-4">
      {descriptor.configFields.map((field) => (
        <ConfigFieldRow
          key={field.key}
          field={field}
          value={flat[field.key] ?? ""}
          supportedAuth={descriptor.supportedAuth}
          onChange={(v) => onPatch({ [field.key]: v })}
        />
      ))}
    </div>
  );
}
