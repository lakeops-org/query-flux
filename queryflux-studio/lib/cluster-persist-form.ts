/**
 * Maps persisted cluster `config` JSON (admin API / Postgres) ↔ flat form state
 * used by {@link EngineClusterConfig}, plus validation helpers shared with add-cluster.
 *
 * Engine-specific flat-form validation lives in `lib/studio-engines/validate-flat.ts` (per-engine modules).
 */

import type { ClusterConfigRecord, UpsertClusterConfig } from "@/lib/api-types";

export { validateEngineSpecific } from "@/lib/studio-engines/validate-flat";

/** Keys we round-trip through Studio forms; other JSON keys are preserved on save. */
export const MANAGED_CONFIG_JSON_KEYS = new Set([
  "endpoint",
  "databasePath",
  "authType",
  "authUsername",
  "authPassword",
  "authToken",
  "tlsInsecureSkipVerify",
  "region",
  "s3OutputLocation",
  "workgroup",
  "catalog",
  "account",
  "warehouse",
  "role",
  "schema",
  "poolSize",
]);

function jsonScalarToString(v: unknown): string {
  if (v === undefined || v === null) return "";
  if (typeof v === "string") return v;
  if (typeof v === "number" || typeof v === "boolean") return String(v);
  return "";
}

/** Positive integer only; rejects floats, NaN, and non-integer `number` values. */
export function parsePositiveIntString(s: string | undefined): number | undefined {
  if (s === undefined) return undefined;
  const t = s.trim();
  if (t === "") return undefined;
  const n = Number(t);
  if (!Number.isInteger(n) || n < 1) return undefined;
  return n;
}

/** DB / API `config` object → flat keys expected by cluster-config components. */
export function persistedClusterConfigToFlat(
  config: Record<string, unknown>,
  descriptor?: { configFields: { key: string }[] },
): Record<string, string> {
  const flat: Record<string, string> = {};

  flat.endpoint = jsonScalarToString(config.endpoint);
  flat.databasePath = jsonScalarToString(config.databasePath);
  flat.region = jsonScalarToString(config.region);
  flat.s3OutputLocation = jsonScalarToString(config.s3OutputLocation);
  flat.workgroup = jsonScalarToString(config.workgroup);
  flat.catalog = jsonScalarToString(config.catalog);
  flat.account = jsonScalarToString(config.account);
  flat.warehouse = jsonScalarToString(config.warehouse);
  flat.role = jsonScalarToString(config.role);
  flat.schema = jsonScalarToString(config.schema);

  const authType = jsonScalarToString(config.authType);
  if (authType) flat["auth.type"] = authType;
  flat["auth.username"] = jsonScalarToString(config.authUsername);
  flat["auth.password"] = jsonScalarToString(config.authPassword);
  flat["auth.token"] = jsonScalarToString(config.authToken);

  flat["tls.insecureSkipVerify"] =
    config.tlsInsecureSkipVerify === true ? "true" : "false";

  flat.poolSize = jsonScalarToString(config.poolSize);

  if (descriptor) {
    for (const f of descriptor.configFields) {
      if (flat[f.key] === undefined) flat[f.key] = "";
    }
  }

  return flat;
}

/** Flat form → persisted `config` JSON (same shape as add-cluster save). */
export function flatToPersistedConfig(flat: Record<string, string>): Record<string, unknown> {
  const cfg: Record<string, unknown> = {};
  if (flat.endpoint) cfg.endpoint = flat.endpoint;
  if (flat.databasePath !== undefined && flat.databasePath !== "") {
    cfg.databasePath = flat.databasePath || null;
  }
  if (flat["auth.type"]) cfg.authType = flat["auth.type"] || null;
  if (flat["auth.username"]) cfg.authUsername = flat["auth.username"];
  if (flat["auth.password"]) cfg.authPassword = flat["auth.password"];
  if (flat["auth.token"]) cfg.authToken = flat["auth.token"];
  if (flat["tls.insecureSkipVerify"] === "true") cfg.tlsInsecureSkipVerify = true;
  else if (flat["tls.insecureSkipVerify"] === "false") cfg.tlsInsecureSkipVerify = false;
  if (flat.region?.trim()) cfg.region = flat.region.trim();
  if (flat.s3OutputLocation?.trim()) cfg.s3OutputLocation = flat.s3OutputLocation.trim();
  if (flat.workgroup?.trim()) cfg.workgroup = flat.workgroup.trim();
  if (flat.catalog?.trim()) cfg.catalog = flat.catalog.trim();
  if (flat.account?.trim()) cfg.account = flat.account.trim();
  if (flat.warehouse?.trim()) cfg.warehouse = flat.warehouse.trim();
  if (flat.role?.trim()) cfg.role = flat.role.trim();
  if (flat.schema?.trim()) cfg.schema = flat.schema.trim();
  const poolN = parsePositiveIntString(flat.poolSize);
  if (poolN !== undefined) cfg.poolSize = poolN;
  return cfg;
}

/**
 * Apply form state onto an existing persisted `config` object (edit path).
 * Starts from `prev` so unknown JSON keys are kept; clears managed keys when the flat field is empty.
 */
export function mergeClusterConfigFromFlat(
  prev: Record<string, unknown>,
  flat: Record<string, string>,
): Record<string, unknown> {
  const out: Record<string, unknown> = { ...prev };

  const setOrDel = (key: string, value: string | undefined, jsonKey: string) => {
    if (value === undefined) return;
    const t = value.trim();
    if (t) out[jsonKey] = t;
    else delete out[jsonKey];
  };

  setOrDel("endpoint", flat.endpoint, "endpoint");
  if (flat.databasePath !== undefined) {
    const t = flat.databasePath.trim();
    if (t) out.databasePath = t;
    else delete out.databasePath;
  }
  if (flat["auth.type"] !== undefined) {
    const t = flat["auth.type"].trim();
    if (t) out.authType = t;
    else delete out.authType;
  }
  setOrDel("auth.username", flat["auth.username"], "authUsername");
  if (flat["auth.password"] !== undefined && flat["auth.password"] !== "") {
    out.authPassword = flat["auth.password"];
  }
  if (flat["auth.token"] !== undefined && flat["auth.token"] !== "") {
    out.authToken = flat["auth.token"];
  }
  if (flat["tls.insecureSkipVerify"] === "true") out.tlsInsecureSkipVerify = true;
  else if (flat["tls.insecureSkipVerify"] === "false") delete out.tlsInsecureSkipVerify;
  else if (flat["tls.insecureSkipVerify"] === "") delete out.tlsInsecureSkipVerify;

  setOrDel("region", flat.region, "region");
  setOrDel("s3OutputLocation", flat.s3OutputLocation, "s3OutputLocation");
  setOrDel("workgroup", flat.workgroup, "workgroup");
  setOrDel("catalog", flat.catalog, "catalog");
  setOrDel("account", flat.account, "account");
  setOrDel("warehouse", flat.warehouse, "warehouse");
  setOrDel("role", flat.role, "role");
  setOrDel("schema", flat.schema, "schema");

  if (flat.poolSize !== undefined) {
    const t = flat.poolSize.trim();
    if (t) {
      const n = parsePositiveIntString(flat.poolSize);
      if (n !== undefined) out.poolSize = n;
      else delete out.poolSize;
    } else delete out.poolSize;
  }

  return out;
}

export function buildClusterUpsertFromForm(
  record: ClusterConfigRecord,
  flat: Record<string, string>,
  opts: { enabled: boolean; maxRunningQueriesInput: string },
): UpsertClusterConfig {
  const prev = record.config as Record<string, unknown>;
  const config = mergeClusterConfigFromFlat(prev, flat);

  const body: UpsertClusterConfig = {
    engineKey: record.engineKey,
    enabled: opts.enabled,
    config,
  };
  const maxTrim = opts.maxRunningQueriesInput.trim();
  body.maxRunningQueries = maxTrim === "" ? null : Number.parseInt(maxTrim, 10);
  return body;
}

/** Shape expected by {@link validateClusterConfig} (nested `auth`, `tls`). */
export function buildValidateShape(flat: Record<string, string>): Record<string, unknown> {
  const o: Record<string, unknown> = {};
  if (flat.endpoint) o.endpoint = flat.endpoint;
  if (flat.databasePath !== undefined && flat.databasePath !== "") {
    o.databasePath = flat.databasePath;
  }
  if (flat.region) o.region = flat.region;
  if (flat.s3OutputLocation) o.s3OutputLocation = flat.s3OutputLocation;
  if (flat.account) o.account = flat.account;
  if (flat.warehouse) o.warehouse = flat.warehouse;
  if (flat.role) o.role = flat.role;
  if (flat.schema) o.schema = flat.schema;
  const poolN = parsePositiveIntString(flat.poolSize);
  if (poolN !== undefined) o.poolSize = poolN;
  const auth: Record<string, string> = {};
  if (flat["auth.type"]) auth.type = flat["auth.type"];
  if (flat["auth.username"]) auth.username = flat["auth.username"];
  if (flat["auth.password"]) auth.password = flat["auth.password"];
  if (flat["auth.token"]) auth.token = flat["auth.token"];
  if (Object.keys(auth).length > 0) o.auth = auth;
  if (flat["tls.insecureSkipVerify"] === "true") {
    o.tls = { insecureSkipVerify: true };
  }
  return o;
}

export function toUpsertBody(
  engineKey: string,
  flat: Record<string, string>,
  runtime: { enabled: boolean; maxRunningQueriesInput: string },
): UpsertClusterConfig {
  const config = flatToPersistedConfig(flat);
  const body: UpsertClusterConfig = { engineKey, enabled: runtime.enabled, config };
  const maxTrim = runtime.maxRunningQueriesInput.trim();
  if (maxTrim !== "") {
    body.maxRunningQueries = Number.parseInt(maxTrim, 10);
  }
  return body;
}
