"use client";

import type { PatchClusterConfig, FlatClusterConfig } from "./types";

const AUTH_BASIC = "basic";
const AUTH_KEY_PAIR = "keyPair";
const AUTH_BEARER = "bearer";

export function SnowflakeClusterConfig({
  flat,
  onPatch,
}: {
  flat: FlatClusterConfig;
  onPatch: PatchClusterConfig;
}) {
  const authType = flat["auth.type"] ?? AUTH_BASIC;

  function setAuthType(next: string) {
    onPatch({
      "auth.type": next,
      "auth.username": "",
      "auth.password": "",
      "auth.token": "",
    });
  }

  const labelClass =
    "block text-[11px] font-semibold text-slate-500 uppercase tracking-wide mb-1.5";
  const inputClass =
    "w-full text-sm border border-slate-200 rounded-lg px-3 py-2 font-mono focus:outline-none focus:ring-2 focus:ring-indigo-300 focus:border-indigo-400";
  const hintClass = "text-[10px] text-slate-400 mt-1";

  return (
    <div className="space-y-4">
      <p className="text-xs text-slate-600 rounded-lg border border-slate-100 bg-indigo-50/40 px-3 py-2">
        Connects to <strong>Snowflake</strong> via the REST API. Provide your account identifier
        and credentials below.
      </p>

      {/* Account */}
      <div>
        <label htmlFor="sf-account" className={labelClass}>
          Account <span className="text-red-500">*</span>
        </label>
        <input
          id="sf-account"
          type="text"
          value={flat.account ?? ""}
          onChange={(e) => onPatch({ account: e.target.value })}
          placeholder="xy12345.us-east-1"
          className={inputClass}
          autoComplete="off"
        />
        <p className={hintClass}>
          Snowflake account identifier (e.g.{" "}
          <code className="font-mono">xy12345.us-east-1</code>).
        </p>
      </div>

      {/* Endpoint (optional) */}
      <div>
        <label htmlFor="sf-endpoint" className={labelClass}>
          Endpoint
        </label>
        <input
          id="sf-endpoint"
          type="url"
          value={flat.endpoint ?? ""}
          onChange={(e) => onPatch({ endpoint: e.target.value })}
          placeholder="https://xy12345.us-east-1.privatelink.snowflakecomputing.com"
          className={inputClass}
          autoComplete="off"
        />
        <p className={hintClass}>
          Custom base URL override (e.g. PrivateLink). Leave empty to derive from account.
        </p>
      </div>

      {/* Warehouse */}
      <div>
        <label htmlFor="sf-warehouse" className={labelClass}>
          Warehouse
        </label>
        <input
          id="sf-warehouse"
          type="text"
          value={flat.warehouse ?? ""}
          onChange={(e) => onPatch({ warehouse: e.target.value })}
          placeholder="COMPUTE_WH"
          className={inputClass}
          autoComplete="off"
        />
        <p className={hintClass}>Default virtual warehouse for query execution.</p>
      </div>

      {/* Role */}
      <div>
        <label htmlFor="sf-role" className={labelClass}>
          Role
        </label>
        <input
          id="sf-role"
          type="text"
          value={flat.role ?? ""}
          onChange={(e) => onPatch({ role: e.target.value })}
          placeholder="ANALYST"
          className={inputClass}
          autoComplete="off"
        />
        <p className={hintClass}>Default Snowflake role.</p>
      </div>

      {/* Database (catalog) */}
      <div>
        <label htmlFor="sf-database" className={labelClass}>
          Database
        </label>
        <input
          id="sf-database"
          type="text"
          value={flat.catalog ?? ""}
          onChange={(e) => onPatch({ catalog: e.target.value })}
          placeholder="MY_DATABASE"
          className={inputClass}
          autoComplete="off"
        />
        <p className={hintClass}>Default Snowflake database.</p>
      </div>

      {/* Schema */}
      <div>
        <label htmlFor="sf-schema" className={labelClass}>
          Schema
        </label>
        <input
          id="sf-schema"
          type="text"
          value={flat.schema ?? ""}
          onChange={(e) => onPatch({ schema: e.target.value })}
          placeholder="PUBLIC"
          className={inputClass}
          autoComplete="off"
        />
        <p className={hintClass}>Default Snowflake schema.</p>
      </div>

      {/* Auth type selector */}
      <div>
        <label htmlFor="sf-auth-type" className={labelClass}>
          Authentication
        </label>
        <select
          id="sf-auth-type"
          value={authType}
          onChange={(e) => setAuthType(e.target.value)}
          className="w-full text-sm border border-slate-200 rounded-lg px-3 py-2 bg-white focus:outline-none focus:ring-2 focus:ring-indigo-300"
        >
          <option value={AUTH_BASIC}>Password</option>
          <option value={AUTH_KEY_PAIR}>Key Pair (RSA)</option>
          <option value={AUTH_BEARER}>OAuth Token</option>
        </select>
      </div>

      {/* Password auth */}
      {authType === AUTH_BASIC && (
        <div className="space-y-4 rounded-xl border border-slate-100 bg-slate-50/60 p-4">
          <p className="text-[10px] font-semibold text-slate-500 uppercase tracking-widest">
            Password credentials
          </p>
          <div>
            <label htmlFor="sf-user" className={labelClass}>
              Username <span className="text-red-500">*</span>
            </label>
            <input
              id="sf-user"
              type="text"
              value={flat["auth.username"] ?? ""}
              onChange={(e) => onPatch({ "auth.username": e.target.value })}
              placeholder="SVC_QUERYFLUX"
              className={inputClass}
              autoComplete="off"
            />
          </div>
          <div>
            <label htmlFor="sf-pass" className={labelClass}>
              Password <span className="text-red-500">*</span>
            </label>
            <input
              id="sf-pass"
              type="password"
              value={flat["auth.password"] ?? ""}
              onChange={(e) => onPatch({ "auth.password": e.target.value })}
              className={inputClass}
              autoComplete="new-password"
            />
          </div>
        </div>
      )}

      {/* Key Pair auth */}
      {authType === AUTH_KEY_PAIR && (
        <div className="space-y-4 rounded-xl border border-slate-100 bg-slate-50/60 p-4">
          <p className="text-[10px] font-semibold text-slate-500 uppercase tracking-widest">
            RSA Key Pair
          </p>
          <div>
            <label htmlFor="sf-kp-user" className={labelClass}>
              Username <span className="text-red-500">*</span>
            </label>
            <input
              id="sf-kp-user"
              type="text"
              value={flat["auth.username"] ?? ""}
              onChange={(e) => onPatch({ "auth.username": e.target.value })}
              placeholder="SVC_QUERYFLUX"
              className={inputClass}
              autoComplete="off"
            />
          </div>
          <div>
            <label htmlFor="sf-kp-pem" className={labelClass}>
              Private Key (PEM) <span className="text-red-500">*</span>
            </label>
            <textarea
              id="sf-kp-pem"
              value={flat["auth.password"] ?? ""}
              onChange={(e) => onPatch({ "auth.password": e.target.value })}
              placeholder={"-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----"}
              rows={5}
              className={`${inputClass} resize-y`}
            />
            <p className={hintClass}>
              PKCS#8 or PKCS#1 PEM-encoded private key.
            </p>
          </div>
          <div>
            <label htmlFor="sf-kp-passphrase" className={labelClass}>
              Passphrase
            </label>
            <input
              id="sf-kp-passphrase"
              type="password"
              value={flat["auth.token"] ?? ""}
              onChange={(e) => onPatch({ "auth.token": e.target.value })}
              placeholder="optional — leave empty for unencrypted keys"
              className={inputClass}
              autoComplete="new-password"
            />
            <p className={hintClass}>
              Passphrase for encrypted private keys. Leave empty if the key is unencrypted.
            </p>
          </div>
        </div>
      )}

      {/* OAuth token auth */}
      {authType === AUTH_BEARER && (
        <div className="space-y-4 rounded-xl border border-slate-100 bg-slate-50/60 p-4">
          <p className="text-[10px] font-semibold text-slate-500 uppercase tracking-widest">
            OAuth Token
          </p>
          <div>
            <label htmlFor="sf-oauth-token" className={labelClass}>
              Token <span className="text-red-500">*</span>
            </label>
            <input
              id="sf-oauth-token"
              type="password"
              value={flat["auth.token"] ?? ""}
              onChange={(e) => onPatch({ "auth.token": e.target.value })}
              className={inputClass}
              autoComplete="new-password"
            />
            <p className={hintClass}>
              OAuth access token for Snowflake.
            </p>
          </div>
        </div>
      )}
    </div>
  );
}
