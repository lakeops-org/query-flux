import type { EngineDef } from "@/lib/engine-catalog-types";
import type { StudioEngineModule } from "@/lib/studio-engines/types";

export function studioModuleToEngineDef(m: StudioEngineModule): EngineDef {
  const d = m.descriptor;
  return {
    name: d.displayName,
    hex: d.hex,
    category: m.catalog.category,
    simpleIconSlug: m.catalog.simpleIconSlug,
    description: m.catalog.catalogDescription,
    engineKey: d.engineKey,
    supported: d.implemented,
  };
}
