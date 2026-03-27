import type {
  ClusterConfigRecord,
  ClusterGroupConfigRecord,
  UpsertClusterConfig,
} from "@/lib/api-types";

export function readGroupMaxRunningQueries(g: ClusterGroupConfigRecord): number {
  if (typeof g.maxRunningQueries === "number" && Number.isFinite(g.maxRunningQueries)) {
    return g.maxRunningQueries;
  }
  return 0;
}

/** Build a full PUT body from a GET record, with optional overrides. */
export function clusterConfigRecordToUpsert(
  r: ClusterConfigRecord,
  overrides: Partial<Pick<UpsertClusterConfig, "enabled" | "maxRunningQueries">>,
): UpsertClusterConfig {
  return {
    engineKey: r.engineKey,
    enabled: overrides.enabled ?? r.enabled,
    maxRunningQueries:
      overrides.maxRunningQueries !== undefined
        ? overrides.maxRunningQueries
        : (r.maxRunningQueries ?? null),
    config: r.config,
  };
}
