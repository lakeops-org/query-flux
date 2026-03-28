import type {
  ClusterConfigRecord,
  ClusterDisplayRow,
  ClusterGroupConfigRecord,
  ClusterStateDto,
} from "@/lib/api-types";
import { findEngineDescriptor } from "@/lib/engine-registry";

/** Cluster names listed as members of at least one enabled group (same rules as the proxy). */
/** Defensive read — admin API uses camelCase JSON; types may lag. */
function groupMembersList(g: ClusterGroupConfigRecord): string[] {
  const raw = g as unknown as Record<string, unknown>;
  const m = raw.members;
  if (!Array.isArray(m)) return [];
  return m.filter((x): x is string => typeof x === "string" && x.length > 0);
}

export function memberClusterNamesInEnabledGroups(
  groups: ClusterGroupConfigRecord[],
): Set<string> {
  const names = new Set<string>();
  for (const g of groups) {
    const raw = g as unknown as Record<string, unknown>;
    if (raw.enabled === false) continue;
    for (const m of groupMembersList(g)) names.add(m);
  }
  return names;
}

/**
 * `GET /admin/clusters` only returns clusters the proxy has registered inside a cluster group.
 * Rows may exist in `cluster_configs` but not in live state if they are **not** in any enabled
 * group's `members` — that is not fixed by restart; we surface `notInAnyGroup` for that case.
 */
export function mergeLiveAndPersistedClusters(
  live: ClusterStateDto[],
  persisted: ClusterConfigRecord[],
  groups: ClusterGroupConfigRecord[],
): ClusterDisplayRow[] {
  const liveByName = new Set(live.map((c) => c.cluster_name));
  const inEnabledGroup = memberClusterNamesInEnabledGroups(groups);
  const persistedByName = new Map(persisted.map((p) => [p.name, p]));

  const fromLive: ClusterDisplayRow[] = live.map((c) => {
    const p = persistedByName.get(c.cluster_name);
    return {
      ...c,
      persisted_max_running_queries:
        p !== undefined ? (p.maxRunningQueries ?? null) : undefined,
    };
  });

  const fromDb: ClusterDisplayRow[] = [];
  for (const p of persisted) {
    if (liveByName.has(p.name)) continue;
    const d = findEngineDescriptor(p.engineKey);
    const notInAnyGroup = !inEnabledGroup.has(p.name);
    fromDb.push({
      group_name: notInAnyGroup ? "No group" : "Not in live state",
      cluster_name: p.name,
      engine_type: d?.displayName ?? p.engineKey,
      endpoint: typeof p.config?.endpoint === "string" ? p.config.endpoint : null,
      running_queries: 0,
      queued_queries: 0,
      max_running_queries: 0,
      is_healthy: false,
      enabled: p.enabled,
      configPending: true,
      notInAnyGroup,
      persisted_max_running_queries: p.maxRunningQueries ?? null,
    });
  }

  fromDb.sort((a, b) => a.cluster_name.localeCompare(b.cluster_name));
  return [...fromLive, ...fromDb];
}
