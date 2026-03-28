/**
 * QueryFlux engine registry — TypeScript mirror of crates/queryflux-core/src/engine_registry.rs
 *
 * Source of truth for descriptors is `lib/studio-engines/manifest.ts` (per-engine modules under
 * `lib/studio-engines/engines/`). Keep Rust `engine_registry` in sync with those descriptors.
 */

import type { EngineDescriptor } from "@/lib/engine-registry-types";
import { STUDIO_ENGINE_MODULES } from "@/lib/studio-engines/manifest";

export type {
  AuthType,
  ConfigField,
  ConnectionType,
  EngineDescriptor,
  FieldType,
} from "@/lib/engine-registry-types";

export const ENGINE_REGISTRY: EngineDescriptor[] = STUDIO_ENGINE_MODULES.map(
  (m) => m.descriptor,
);

export function findEngineDescriptor(engineKeyOrType: string): EngineDescriptor | undefined {
  const normalized = engineKeyOrType.toLowerCase();
  return ENGINE_REGISTRY.find(
    (e) =>
      e.engineKey.toLowerCase() === normalized ||
      e.displayName.toLowerCase() === normalized ||
      e.displayName.toLowerCase().replace(/\s+/g, "") === normalized,
  );
}

export function implementedEngines(): EngineDescriptor[] {
  return ENGINE_REGISTRY.filter((e) => e.implemented);
}

export function isClusterOnboardingSelectable(engine: {
  engineKey: string | null;
  supported: boolean;
}): boolean {
  if (!engine.supported || !engine.engineKey) return false;
  const d = findEngineDescriptor(engine.engineKey);
  return !!d?.implemented;
}

export function validateClusterConfig(
  clusterName: string,
  engineKey: string,
  config: Record<string, unknown>,
  options?: { skipImplementedCheck?: boolean },
): string[] {
  const descriptor = findEngineDescriptor(engineKey);
  if (!descriptor) {
    return [`Cluster '${clusterName}': unknown engine '${engineKey}'`];
  }

  const errors: string[] = [];

  if (!options?.skipImplementedCheck && !descriptor.implemented) {
    errors.push(
      `Cluster '${clusterName}': engine '${descriptor.displayName}' is not yet implemented`,
    );
  }

  for (const field of descriptor.configFields) {
    if (!field.required) continue;
    const topKey = field.key.split(".")[0];
    if (!config[topKey]) {
      errors.push(
        `Cluster '${clusterName}': '${field.key}' is required for ${descriptor.displayName}`,
      );
    }
  }

  return errors;
}
