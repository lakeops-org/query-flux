import { athenaStudioEngine } from "@/lib/studio-engines/engines/athena";
import { clickHouseStudioEngine } from "@/lib/studio-engines/engines/clickhouse";
import { duckDbStudioEngine } from "@/lib/studio-engines/engines/duckdb";
import { duckDbHttpStudioEngine } from "@/lib/studio-engines/engines/duckdb-http";
import { starRocksStudioEngine } from "@/lib/studio-engines/engines/starrocks";
import { trinoStudioEngine } from "@/lib/studio-engines/engines/trino";
import type { StudioEngineModule } from "@/lib/studio-engines/types";

/**
 * Single manifest for Studio-backed engines (registry, catalog slots, type aliases,
 * affinity options, flat validation dispatch).
 *
 * To add a backend:
 *  1. Create `lib/studio-engines/engines/<engine>.ts` exporting `*StudioEngine` (descriptor + catalog + options).
 *  2. Import it below and append to `STUDIO_ENGINE_MODULES`.
 *  3. Insert `{ k: "studio", engineKey: "<same key>" }` in `ENGINE_CATALOG_SLOTS` in
 *     `components/engine-catalog.ts` where the card should appear.
 *  4. If the cluster form is not generic: set `customFormId` on the module and register the component in
 *     `components/cluster-config/studio-engine-forms.tsx`.
 *  5. If persisted `config` uses new JSON keys, extend `lib/cluster-persist-form.ts` (`MANAGED_CONFIG_JSON_KEYS` + mappers).
 */
export const STUDIO_ENGINE_MODULES: StudioEngineModule[] = [
  trinoStudioEngine,
  duckDbStudioEngine,
  duckDbHttpStudioEngine,
  starRocksStudioEngine,
  athenaStudioEngine,
  clickHouseStudioEngine,
];

export const STUDIO_MODULE_BY_KEY = new Map(
  STUDIO_ENGINE_MODULES.map((m) => [m.descriptor.engineKey, m]),
);

export function getStudioEngineModule(
  engineKey: string,
): StudioEngineModule | undefined {
  return STUDIO_MODULE_BY_KEY.get(engineKey);
}

/** Extra `findEngineByType` entries from studio modules (merged with static dialect aliases). */
export function buildStudioTypeAliases(): Record<string, string> {
  const o: Record<string, string> = {};
  for (const m of STUDIO_ENGINE_MODULES) {
    const name = m.descriptor.displayName;
    const k = m.descriptor.engineKey.toLowerCase().replace(/\s+/g, "");
    o[k] = name;
    for (const [alias, canonical] of Object.entries(m.extraTypeAliases ?? {})) {
      o[alias.toLowerCase().replace(/\s+/g, "")] = canonical;
    }
  }
  return o;
}

/** Engine affinity preference keys (group strategy UI). */
export function buildEngineAffinityOptionsFromManifest(): {
  value: string;
  label: string;
}[] {
  const out: { value: string; label: string }[] = [];
  for (const m of STUDIO_ENGINE_MODULES) {
    if (m.engineAffinity === false) continue;
    const label =
      typeof m.engineAffinity === "object" && m.engineAffinity?.label
        ? m.engineAffinity.label
        : m.descriptor.displayName;
    out.push({ value: m.descriptor.engineKey, label });
  }
  return out;
}
