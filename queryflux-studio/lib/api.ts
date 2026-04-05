import type {
  ClusterStateDto,
  DashboardStats,
  EngineStatRow,
  FrontendsStatusDto,
  GroupStatRow,
  QueryHistoryRecord,
  QueryListParams,
} from "./api-types";
import { normalizeClusterGroupRecord } from "./group-config-helpers";
import { getAuthHeader } from "./auth";

/** Server: direct admin API. Browser: same-origin proxy (see `app/api/admin-proxy/`). */
function adminApiOrigin(): string {
  if (typeof window !== "undefined") {
    return "/api/admin-proxy";
  }
  return process.env.ADMIN_API_URL ?? "http://localhost:9000";
}

/**
 * Build base headers with Basic auth.
 * - Browser: reads from cookie via `getAuthHeader()`
 * - Server: reads the `qf_auth` cookie via `next/headers`
 */
async function baseHeaders(
  extra?: Record<string, string>
): Promise<Record<string, string>> {
  const headers: Record<string, string> = { ...extra };

  if (typeof window !== "undefined") {
    // Client-side: read from cookie via auth helper.
    const auth = getAuthHeader();
    if (auth) headers["authorization"] = auth;
  } else {
    // Server-side: read cookie via next/headers (available in Server Components).
    try {
      const { cookies } = await import("next/headers");
      const cookieStore = await cookies();
      const raw = cookieStore.get("qf_auth")?.value;
      if (raw) headers["authorization"] = `Basic ${decodeURIComponent(raw)}`;
    } catch {
      // next/headers not available in this context — skip auth header.
    }
  }

  return headers;
}

export class UnauthorizedError extends Error {
  constructor() {
    super("Unauthorized");
    this.name = "UnauthorizedError";
  }
}

function dispatchUnauthorized() {
  if (typeof window !== "undefined") {
    window.dispatchEvent(new Event("qf:unauthorized"));
  }
}

async function apiFetch<T>(path: string): Promise<T> {
  const res = await fetch(`${adminApiOrigin()}${path}`, {
    headers: await baseHeaders(),
    cache: "no-store",
  });
  if (res.status === 401) {
    dispatchUnauthorized();
    throw new UnauthorizedError();
  }
  if (!res.ok) {
    throw new Error(`Admin API ${path} → ${res.status}: ${await res.text()}`);
  }
  return res.json() as Promise<T>;
}

async function apiPatch<T>(path: string, body: unknown): Promise<T> {
  const res = await fetch(`${adminApiOrigin()}${path}`, {
    method: "PATCH",
    headers: await baseHeaders({ "content-type": "application/json" }),
    body: JSON.stringify(body),
    cache: "no-store",
  });
  if (res.status === 401) { dispatchUnauthorized(); throw new UnauthorizedError(); }
  if (!res.ok) {
    throw new Error(`Admin API PATCH ${path} → ${res.status}: ${await res.text()}`);
  }
  return res.json() as Promise<T>;
}

async function apiPut<T>(path: string, body: unknown): Promise<T> {
  const res = await fetch(`${adminApiOrigin()}${path}`, {
    method: "PUT",
    headers: await baseHeaders({ "content-type": "application/json" }),
    body: JSON.stringify(body),
    cache: "no-store",
  });
  if (res.status === 401) { dispatchUnauthorized(); throw new UnauthorizedError(); }
  if (!res.ok) {
    throw new Error(`Admin API PUT ${path} → ${res.status}: ${await res.text()}`);
  }
  return res.json() as Promise<T>;
}

async function apiPost<T>(path: string, body: unknown): Promise<T> {
  const res = await fetch(`${adminApiOrigin()}${path}`, {
    method: "POST",
    headers: await baseHeaders({ "content-type": "application/json" }),
    body: JSON.stringify(body),
    cache: "no-store",
  });
  if (res.status === 401) { dispatchUnauthorized(); throw new UnauthorizedError(); }
  if (!res.ok) {
    throw new Error(`Admin API POST ${path} → ${res.status}: ${await res.text()}`);
  }
  return res.json() as Promise<T>;
}

async function apiPutNoContent(path: string, body: unknown): Promise<void> {
  const res = await fetch(`${adminApiOrigin()}${path}`, {
    method: "PUT",
    headers: await baseHeaders({ "content-type": "application/json" }),
    body: JSON.stringify(body),
    cache: "no-store",
  });
  if (res.status === 401) { dispatchUnauthorized(); throw new UnauthorizedError(); }
  if (!res.ok) {
    throw new Error(`Admin API PUT ${path} → ${res.status}: ${await res.text()}`);
  }
}

async function apiDelete(path: string): Promise<void> {
  const res = await fetch(`${adminApiOrigin()}${path}`, {
    method: "DELETE",
    headers: await baseHeaders(),
    cache: "no-store",
  });
  if (res.status === 401) { dispatchUnauthorized(); throw new UnauthorizedError(); }
  if (!res.ok && res.status !== 204) {
    throw new Error(`Admin API DELETE ${path} → ${res.status}: ${await res.text()}`);
  }
}

export async function getClusters(): Promise<ClusterStateDto[]> {
  return apiFetch<ClusterStateDto[]>("/admin/clusters");
}

export async function getDashboardStats(): Promise<DashboardStats> {
  return apiFetch<DashboardStats>("/admin/stats");
}

export async function getFrontendsStatus(): Promise<FrontendsStatusDto> {
  return apiFetch<FrontendsStatusDto>("/admin/frontends");
}

export async function getQueries(params: QueryListParams = {}): Promise<QueryHistoryRecord[]> {
  const qs = new URLSearchParams();
  if (params.search) qs.set("search", params.search);
  if (params.status) qs.set("status", params.status);
  if (params.cluster_group) qs.set("cluster_group", params.cluster_group);
  if (params.engine) qs.set("engine", params.engine);
  if (params.limit != null) qs.set("limit", String(params.limit));
  if (params.offset != null) qs.set("offset", String(params.offset));
  const query = qs.toString() ? `?${qs}` : "";
  return apiFetch<QueryHistoryRecord[]>(`/admin/queries${query}`);
}

export async function getDistinctEngines(): Promise<string[]> {
  return apiFetch<string[]>("/admin/engines");
}

export async function getEngineStats(hours = 24): Promise<EngineStatRow[]> {
  return apiFetch<EngineStatRow[]>(`/admin/engine-stats?hours=${hours}`);
}

export async function getGroupStats(hours = 24): Promise<GroupStatRow[]> {
  return apiFetch<GroupStatRow[]>(`/admin/group-stats?hours=${hours}`);
}

// ---------------------------------------------------------------------------
// Persisted cluster config CRUD
// ---------------------------------------------------------------------------

export async function listClusterConfigs(): Promise<import("./api-types").ClusterConfigRecord[]> {
  return apiFetch("/admin/config/clusters");
}

export async function getClusterConfig(name: string): Promise<import("./api-types").ClusterConfigRecord> {
  return apiFetch(`/admin/config/clusters/${encodeURIComponent(name)}`);
}

export async function upsertClusterConfig(
  name: string,
  body: import("./api-types").UpsertClusterConfig,
): Promise<import("./api-types").ClusterConfigRecord> {
  return apiPut(`/admin/config/clusters/${encodeURIComponent(name)}`, body);
}

export async function renameClusterConfig(
  currentName: string,
  body: import("./api-types").RenameConfigRequest,
): Promise<import("./api-types").ClusterConfigRecord> {
  return apiPatch<import("./api-types").ClusterConfigRecord>(
    `/admin/config/clusters/${encodeURIComponent(currentName)}`,
    body,
  );
}

export async function deleteClusterConfig(name: string): Promise<void> {
  return apiDelete(`/admin/config/clusters/${encodeURIComponent(name)}`);
}

// ---------------------------------------------------------------------------
// Persisted cluster group config CRUD
// ---------------------------------------------------------------------------

export async function listGroupConfigs(): Promise<import("./api-types").ClusterGroupConfigRecord[]> {
  const raw = await apiFetch<unknown[]>("/admin/config/groups");
  return raw.map((row) => normalizeClusterGroupRecord(row));
}

export async function getGroupConfig(name: string): Promise<import("./api-types").ClusterGroupConfigRecord> {
  const raw = await apiFetch<unknown>(`/admin/config/groups/${encodeURIComponent(name)}`);
  return normalizeClusterGroupRecord(raw);
}

export async function upsertGroupConfig(
  name: string,
  body: import("./api-types").UpsertClusterGroupConfig,
): Promise<import("./api-types").ClusterGroupConfigRecord> {
  const raw = await apiPut<unknown>(
    `/admin/config/groups/${encodeURIComponent(name)}`,
    body,
  );
  return normalizeClusterGroupRecord(raw);
}

export async function renameGroupConfig(
  currentName: string,
  body: import("./api-types").RenameConfigRequest,
): Promise<import("./api-types").ClusterGroupConfigRecord> {
  const raw = await apiPatch<unknown>(
    `/admin/config/groups/${encodeURIComponent(currentName)}`,
    body,
  );
  return normalizeClusterGroupRecord(raw);
}

export async function deleteGroupConfig(name: string): Promise<void> {
  return apiDelete(`/admin/config/groups/${encodeURIComponent(name)}`);
}

// ---------------------------------------------------------------------------
// User script library (`user_scripts` table)
// ---------------------------------------------------------------------------

export async function listUserScripts(kind?: string): Promise<import("./api-types").UserScriptRecord[]> {
  const q = kind ? `?kind=${encodeURIComponent(kind)}` : "";
  return apiFetch<import("./api-types").UserScriptRecord[]>(`/admin/config/scripts${q}`);
}

export async function createUserScript(
  body: import("./api-types").UpsertUserScript,
): Promise<import("./api-types").UserScriptRecord> {
  return apiPost(`/admin/config/scripts`, body);
}

export async function updateUserScript(
  id: number,
  body: import("./api-types").UpsertUserScript,
): Promise<import("./api-types").UserScriptRecord> {
  return apiPut(`/admin/config/scripts/${id}`, body);
}

export async function deleteUserScript(id: number): Promise<void> {
  const res = await fetch(`${adminApiOrigin()}/admin/config/scripts/${id}`, {
    method: "DELETE",
    cache: "no-store",
  });
  if (!res.ok && res.status !== 204) {
    throw new Error(`DELETE script ${id} → ${res.status}: ${await res.text()}`);
  }
}

export async function updateCluster(
  group: string,
  cluster: string,
  update: import("./api-types").ClusterUpdateRequest,
): Promise<import("./api-types").ClusterStateDto> {
  return apiPatch<import("./api-types").ClusterStateDto>(
    `/admin/clusters/${encodeURIComponent(group)}/${encodeURIComponent(cluster)}`,
    update,
  );
}

// ---------------------------------------------------------------------------
// Security and routing config
// ---------------------------------------------------------------------------

export async function getSecurityConfig(): Promise<import("./api-types").SecurityConfigDto> {
  return apiFetch("/admin/config/security");
}

export async function getRoutingConfig(): Promise<import("./api-types").RoutingConfigDto> {
  return apiFetch("/admin/config/routing");
}

export async function putSecurityConfig(body: import("./api-types").UpsertSecurityConfig): Promise<void> {
  return apiPutNoContent("/admin/config/security", body);
}

export async function putRoutingConfig(body: import("./api-types").UpsertRoutingConfig): Promise<void> {
  return apiPutNoContent("/admin/config/routing", body);
}

// ---------------------------------------------------------------------------
// Auth management
// ---------------------------------------------------------------------------

export async function getAuthStatus(): Promise<{ db_override: boolean }> {
  return apiFetch("/admin/auth/status");
}

// Re-export types so pages can import from one place
export type {
  ClusterConfigRecord,
  ClusterGroupConfigRecord,
  ClusterStateDto,
  ClusterUpdateRequest,
  DashboardStats,
  EngineStatRow,
  FrontendsStatusDto,
  GroupStatRow,
  ProtocolFrontendDto,
  GroupAuthzDto,
  LdapConfigDto,
  OidcConfigDto,
  OpenFgaConfigDto,
  QueryHistoryRecord,
  QueryListParams,
  RouterConfigEntry,
  RoutingConfigDto,
  RoutingTrace,
  RoutingDecision,
  SecurityConfigDto,
  UpsertClusterConfig,
  UpsertClusterGroupConfig,
  UpsertRoutingConfig,
  UpsertSecurityConfig,
  UpsertUserScript,
  UserScriptRecord,
} from "./api-types";
