"use client";

import type { PatchClusterConfig, FlatClusterConfig } from "./types";

const AUTH_BASIC = "basic";
const AUTH_BEARER = "bearer";

/**
 * Trino: HTTP endpoint, optional TLS skip-verify, and auth-specific fields
 * (username/password vs bearer token).
 *
 * @see `trinoStudioEngine` in `@/lib/studio-engines/engines/trino`
 */
export function TrinoClusterConfig({
  flat,
  onPatch,
}: {
  flat: FlatClusterConfig;
  onPatch: PatchClusterConfig;
}) {
  const authType = flat["auth.type"] ?? "";

  function setAuthType(next: string) {
    const patch: Record<string, string> = { "auth.type": next };
    if (next === AUTH_BEARER) {
      patch["auth.username"] = "";
      patch["auth.password"] = "";
    } else if (next === AUTH_BASIC) {
      patch["auth.token"] = "";
    }
    onPatch(patch);
  }

  return (
    <div className="space-y-4">
      <div>
        <label
          htmlFor="trino-endpoint"
          className="block text-[11px] font-semibold text-slate-500 uppercase tracking-wide mb-1.5"
        >
          Endpoint <span className="text-red-500">*</span>
        </label>
        <input
          id="trino-endpoint"
          type="url"
          value={flat.endpoint ?? ""}
          onChange={(e) => onPatch({ endpoint: e.target.value })}
          placeholder="http://trino-coordinator:8080"
          className="w-full text-sm border border-slate-200 rounded-lg px-3 py-2 font-mono focus:outline-none focus:ring-2 focus:ring-indigo-300 focus:border-indigo-400"
          autoComplete="off"
        />
        <p className="text-[10px] text-slate-400 mt-1">
          HTTP(S) base URL of the Trino coordinator (REST API).
        </p>
      </div>

      <div>
        <label
          htmlFor="trino-auth-type"
          className="block text-[11px] font-semibold text-slate-500 uppercase tracking-wide mb-1.5"
        >
          Authentication
        </label>
        <select
          id="trino-auth-type"
          value={authType}
          onChange={(e) => setAuthType(e.target.value)}
          className="w-full text-sm border border-slate-200 rounded-lg px-3 py-2 bg-white focus:outline-none focus:ring-2 focus:ring-indigo-300"
        >
          <option value="">Choose method…</option>
          <option value={AUTH_BASIC}>Username & password</option>
          <option value={AUTH_BEARER}>Bearer token (JWT / OAuth2)</option>
        </select>
        <p className="text-[10px] text-slate-400 mt-1">
          Trino supports LDAP-backed basic auth or a bearer token on the coordinator.
        </p>
      </div>

      {authType === AUTH_BASIC && (
        <div className="space-y-4 rounded-xl border border-slate-100 bg-slate-50/60 p-4">
          <p className="text-[10px] font-semibold text-slate-500 uppercase tracking-widest">
            Basic auth
          </p>
          <div>
            <label
              htmlFor="trino-user"
              className="block text-[11px] font-semibold text-slate-500 uppercase tracking-wide mb-1.5"
            >
              Username
            </label>
            <input
              id="trino-user"
              type="text"
              value={flat["auth.username"] ?? ""}
              onChange={(e) => onPatch({ "auth.username": e.target.value })}
              placeholder="admin"
              className="w-full text-sm border border-slate-200 rounded-lg px-3 py-2 font-mono focus:outline-none focus:ring-2 focus:ring-indigo-300"
              autoComplete="off"
            />
          </div>
          <div>
            <label
              htmlFor="trino-pass"
              className="block text-[11px] font-semibold text-slate-500 uppercase tracking-wide mb-1.5"
            >
              Password
            </label>
            <input
              id="trino-pass"
              type="password"
              value={flat["auth.password"] ?? ""}
              onChange={(e) => onPatch({ "auth.password": e.target.value })}
              className="w-full text-sm border border-slate-200 rounded-lg px-3 py-2 font-mono focus:outline-none focus:ring-2 focus:ring-indigo-300"
              autoComplete="new-password"
            />
          </div>
        </div>
      )}

      {authType === AUTH_BEARER && (
        <div className="space-y-3 rounded-xl border border-slate-100 bg-slate-50/60 p-4">
          <p className="text-[10px] font-semibold text-slate-500 uppercase tracking-widest">
            Bearer token
          </p>
          <div>
            <label
              htmlFor="trino-token"
              className="block text-[11px] font-semibold text-slate-500 uppercase tracking-wide mb-1.5"
            >
              Token
            </label>
            <input
              id="trino-token"
              type="password"
              value={flat["auth.token"] ?? ""}
              onChange={(e) => onPatch({ "auth.token": e.target.value })}
              placeholder="JWT or OAuth2 access token"
              className="w-full text-sm border border-slate-200 rounded-lg px-3 py-2 font-mono focus:outline-none focus:ring-2 focus:ring-indigo-300"
              autoComplete="new-password"
            />
            <p className="text-[10px] text-slate-400 mt-1">
              Sent as <code className="font-mono">Authorization: Bearer …</code>
            </p>
          </div>
        </div>
      )}

      {authType === "" && (
        <p className="text-xs text-slate-500 rounded-lg border border-dashed border-slate-200 bg-slate-50/80 px-3 py-2">
          Select an authentication method to enter credentials.
        </p>
      )}

      <div className="flex items-center justify-between gap-4 pt-1">
        <div>
          <label htmlFor="trino-tls-skip" className="text-sm font-medium text-slate-700">
            Skip TLS certificate verification
          </label>
          <p className="text-[11px] text-slate-400 mt-0.5">
            Dev only — do not use against production coordinators.
          </p>
        </div>
        <input
          id="trino-tls-skip"
          type="checkbox"
          checked={flat["tls.insecureSkipVerify"] === "true"}
          onChange={(e) =>
            onPatch({ "tls.insecureSkipVerify": e.target.checked ? "true" : "false" })
          }
          className="rounded border-slate-300 text-indigo-600 focus:ring-indigo-400"
        />
      </div>
    </div>
  );
}
