import type { EngineCategory } from "@/lib/engine-catalog-types";
import type { EngineDescriptor } from "@/lib/engine-registry-types";
import type { FlatClusterForm } from "@/lib/cluster-form/types";

/**
 * One registration unit for a QueryFlux backend in Studio.
 * Add a file under `engines/`, export a `StudioEngineModule`, append to `manifest.ts`.
 */
export type StudioEngineModule = {
  descriptor: EngineDescriptor;
  /** Engines grid / Add cluster picker (icon, category, blurb). */
  catalog: {
    category: EngineCategory;
    simpleIconSlug: string | null;
    catalogDescription: string;
  };
  /**
   * Extra `findEngineByType` aliases (normalized lowercase, no spaces).
   * The engineKey itself is registered automatically.
   */
  extraTypeAliases?: Record<string, string>;
  /**
   * Shown in group strategy “engine affinity” dropdown. Omit to use `descriptor.displayName`.
   * Set to `false` to exclude (e.g. Athena).
   */
  engineAffinity?: false | { label?: string };
  /** Cross-field checks beyond `validateClusterConfig` (auth modes, etc.). */
  validateFlat?: (flat: FlatClusterForm) => string[];
  /**
   * Key into `STUDIO_CUSTOM_CLUSTER_FORMS` (client). Omit to use the generic field renderer.
   */
  customFormId?: string;
};
