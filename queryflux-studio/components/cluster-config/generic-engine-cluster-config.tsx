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
  readOnlyFieldKeys,
  hiddenFieldKeys,
}: {
  descriptor: EngineDescriptor;
  flat: FlatClusterConfig;
  onPatch: PatchClusterConfig;
  /** Field keys that cannot be changed (e.g. driver after ADBC variant pick). */
  readOnlyFieldKeys?: ReadonlySet<string>;
  /** Field keys omitted from the form (e.g. PostgreSQL ADBC auth in URI). */
  hiddenFieldKeys?: ReadonlySet<string>;
}) {
  return (
    <div className="space-y-4">
      {descriptor.configFields
        .filter((field) => !hiddenFieldKeys?.has(field.key))
        .map((field) => (
        <ConfigFieldRow
          key={field.key}
          field={field}
          value={flat[field.key] ?? ""}
          supportedAuth={descriptor.supportedAuth}
          flat={flat}
          readOnly={readOnlyFieldKeys?.has(field.key) ?? false}
          onChange={(v) => onPatch({ [field.key]: v })}
        />
      ))}
    </div>
  );
}
