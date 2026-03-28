import type { FlatClusterForm } from "@/lib/cluster-form/types";
import { STUDIO_MODULE_BY_KEY } from "@/lib/studio-engines/manifest";

/** Cross-field validation registered on each studio engine module (`validateFlat`). */
export function validateEngineSpecific(
  engineKey: string,
  flat: FlatClusterForm,
): string[] {
  return STUDIO_MODULE_BY_KEY.get(engineKey)?.validateFlat?.(flat) ?? [];
}
