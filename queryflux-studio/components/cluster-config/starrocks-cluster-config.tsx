"use client";

import type { PatchClusterConfig, FlatClusterConfig } from "./types";

/**
 * StarRocks: MySQL wire endpoint + username/password (basic auth only).
 *
 * @see `starRocksStudioEngine` in `@/lib/studio-engines/engines/starrocks`
 */
export function StarRocksClusterConfig({
  flat,
  onPatch,
}: {
  flat: FlatClusterConfig;
  onPatch: PatchClusterConfig;
}) {
  return (
    <div className="space-y-4">
      <p className="text-xs text-slate-600 rounded-lg border border-slate-100 bg-indigo-50/40 px-3 py-2">
        Connects via the <strong>MySQL protocol</strong> to the front-end (FE) node — typically port{" "}
        <code className="font-mono text-[11px]">9030</code>. Authentication is username and password.
      </p>

      <div>
        <label
          htmlFor="sr-endpoint"
          className="block text-[11px] font-semibold text-slate-500 uppercase tracking-wide mb-1.5"
        >
          Endpoint <span className="text-red-500">*</span>
        </label>
        <input
          id="sr-endpoint"
          type="url"
          value={flat.endpoint ?? ""}
          onChange={(e) => onPatch({ endpoint: e.target.value })}
          placeholder="mysql://starrocks-fe:9030"
          className="w-full text-sm border border-slate-200 rounded-lg px-3 py-2 font-mono focus:outline-none focus:ring-2 focus:ring-indigo-300 focus:border-indigo-400"
          autoComplete="off"
        />
        <p className="text-[10px] text-slate-400 mt-1">
          MySQL URL for the StarRocks FE (e.g. <code className="font-mono">mysql://host:9030</code>).
        </p>
      </div>

      <div className="space-y-4 rounded-xl border border-slate-100 bg-slate-50/60 p-4">
        <p className="text-[10px] font-semibold text-slate-500 uppercase tracking-widest">
          Credentials
        </p>
        <div>
          <label
            htmlFor="sr-user"
            className="block text-[11px] font-semibold text-slate-500 uppercase tracking-wide mb-1.5"
          >
            Username
          </label>
          <input
            id="sr-user"
            type="text"
            value={flat["auth.username"] ?? ""}
            onChange={(e) => onPatch({ "auth.username": e.target.value })}
            placeholder="root"
            className="w-full text-sm border border-slate-200 rounded-lg px-3 py-2 font-mono focus:outline-none focus:ring-2 focus:ring-indigo-300"
            autoComplete="off"
          />
        </div>
        <div>
          <label
            htmlFor="sr-pass"
            className="block text-[11px] font-semibold text-slate-500 uppercase tracking-wide mb-1.5"
          >
            Password
          </label>
          <input
            id="sr-pass"
            type="password"
            value={flat["auth.password"] ?? ""}
            onChange={(e) => onPatch({ "auth.password": e.target.value })}
            className="w-full text-sm border border-slate-200 rounded-lg px-3 py-2 font-mono focus:outline-none focus:ring-2 focus:ring-indigo-300"
            autoComplete="new-password"
          />
        </div>
        <div>
          <label
            htmlFor="sr-pool"
            className="block text-[11px] font-semibold text-slate-500 uppercase tracking-wide mb-1.5"
          >
            Connection pool size
          </label>
          <input
            id="sr-pool"
            type="number"
            min={1}
            step={1}
            value={flat.poolSize ?? ""}
            onChange={(e) => onPatch({ poolSize: e.target.value })}
            placeholder="8"
            className="w-full text-sm border border-slate-200 rounded-lg px-3 py-2 font-mono focus:outline-none focus:ring-2 focus:ring-indigo-300"
            autoComplete="off"
          />
          <p className="text-[10px] text-slate-400 mt-1">
            Persistent MySQL connections to the FE. Leave empty to use the default (8).
          </p>
        </div>
      </div>
    </div>
  );
}
