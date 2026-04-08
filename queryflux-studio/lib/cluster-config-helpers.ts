import type {
  ClusterConfigRecord,
  ClusterGroupConfigRecord,
  UpsertClusterConfig,
} from "@/lib/api-types";

/**
 * Parse `/admin/config/clusters` JSON whether the payload uses camelCase or snake_case
 * (OpenAPI clients, proxies, or older servers).
 */
export function normalizeClusterConfigRecord(raw: unknown): ClusterConfigRecord {
  const r = raw as Record<string, unknown>;
  const engineKey =
    typeof r.engineKey === "string"
      ? r.engineKey
      : typeof r.engine_key === "string"
        ? r.engine_key
        : "";
  const maybeConfig = r.config;
  const config: Record<string, unknown> =
    maybeConfig && typeof maybeConfig === "object" && !Array.isArray(maybeConfig)
      ? (maybeConfig as Record<string, unknown>)
      : {};
  const maxRunningQueries = (() => {
    const a = r.maxRunningQueries ?? r.max_running_queries;
    if (a === null || a === undefined) return null;
    if (typeof a === "number" && Number.isFinite(a)) return a;
    return null;
  })();
  const strOrIso = (camel: string, snake: string) => {
    const a = r[camel];
    if (typeof a === "string") return a;
    const b = r[snake];
    if (typeof b === "string") return b;
    return new Date().toISOString();
  };
  return {
    id: typeof r.id === "number" ? r.id : Number(r.id) || 0,
    name: typeof r.name === "string" ? r.name : "",
    engineKey,
    enabled: r.enabled !== false,
    maxRunningQueries,
    config,
    createdAt: strOrIso("createdAt", "created_at"),
    updatedAt: strOrIso("updatedAt", "updated_at"),
  };
}

/** Read engine key from a record regardless of camelCase vs snake_case JSON. */
export function readClusterEngineKey(r: ClusterConfigRecord): string {
  const raw = r as unknown as Record<string, unknown>;
  const k = raw.engineKey ?? raw.engine_key;
  return typeof k === "string" ? k : "";
}

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
