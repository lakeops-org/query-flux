"use client";

import type { ConfigField } from "@/lib/engine-registry";
import { DbKwargsField } from "./db-kwargs-field";

const ADBC_DRIVER_OPTIONS = [
  "trino",
  "duckdb",
  "clickhouse",
  "mysql",
  "postgresql",
  "sqlite",
  "flightsql",
  "snowflake",
  "bigquery",
  "databricks",
  "mssql",
  "redshift",
  "exasol",
  "singlestore",
] as const;

export function ConfigFieldRow({
  field,
  value,
  onChange,
  supportedAuth,
  readOnly,
  flat,
}: {
  field: ConfigField;
  value: string;
  onChange: (v: string) => void;
  supportedAuth: string[];
  /** When true, field cannot be edited (e.g. driver locked after picker choice). */
  readOnly?: boolean;
  /** Full flat state for conditional fields. */
  flat?: Record<string, string>;
}) {
  const id = `cluster-field-${field.key.replace(/[^a-zA-Z0-9]/g, "-")}`;
  const isDbKwargsField = field.key === "dbKwargs";
  const dbKwargsTrimmed = (value ?? "").trim();
  const dbKwargsError =
    isDbKwargsField && dbKwargsTrimmed
      ? (() => {
          try {
            const parsed = JSON.parse(dbKwargsTrimmed) as unknown;
            if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
              return "Driver options must be a JSON object, e.g. {\"catalog\":\"hive\"}.";
            }
            return null;
          } catch {
            return "Invalid JSON. Use an object like {\"key\":\"value\"}.";
          }
        })()
      : null;

  if (field.fieldType === "boolean") {
    return (
      <div className="flex items-center justify-between gap-4">
        <div>
          <label htmlFor={id} className="text-sm font-medium text-slate-700">
            {field.label}
          </label>
          <p className="text-[11px] text-slate-400 mt-0.5">{field.description}</p>
        </div>
        <input
          id={id}
          type="checkbox"
          checked={value === "true"}
          onChange={(e) => onChange(e.target.checked ? "true" : "false")}
          className="rounded border-slate-300 text-indigo-600 focus:ring-indigo-400"
        />
      </div>
    );
  }

  if (field.key === "auth.type" && supportedAuth.length > 0) {
    return (
      <div>
        <label
          htmlFor={id}
          className="block text-[11px] font-semibold text-slate-500 uppercase tracking-wide mb-1.5"
        >
          {field.label}
        </label>
        <select
          id={id}
          value={value}
          onChange={(e) => onChange(e.target.value)}
          className="w-full text-sm border border-slate-200 rounded-lg px-3 py-2 bg-white focus:outline-none focus:ring-2 focus:ring-indigo-300"
        >
          <option value="">—</option>
          {supportedAuth.map((a) => (
            <option key={a} value={a}>
              {a === "basic"
                ? "Username & password"
                : a === "bearer"
                  ? "Bearer token"
                  : a === "keyPair"
                    ? "Key pair"
                    : a === "accessKey"
                      ? "AWS access key"
                      : a === "roleArn"
                        ? "IAM role (STS)"
                        : a}
            </option>
          ))}
        </select>
        <p className="text-[10px] text-slate-400 mt-1">{field.description}</p>
      </div>
    );
  }

  if (field.key === "driver") {
    const hasCustomValue = !!value && !ADBC_DRIVER_OPTIONS.includes(value as never);
    return (
      <div>
        <label
          htmlFor={id}
          className="block text-[11px] font-semibold text-slate-500 uppercase tracking-wide mb-1.5"
        >
          {field.label}
          {field.required && <span className="text-red-500 ml-0.5">*</span>}
        </label>
        <select
          id={id}
          value={value}
          disabled={readOnly}
          onChange={(e) => onChange(e.target.value)}
          className={`w-full text-sm border rounded-lg px-3 py-2 focus:outline-none focus:ring-2 focus:ring-indigo-300 ${
            readOnly
              ? "border-slate-200 bg-slate-50 text-slate-700 cursor-not-allowed"
              : "border-slate-200 bg-white"
          }`}
        >
          <option value="">— Select ADBC driver —</option>
          {ADBC_DRIVER_OPTIONS.map((driver) => (
            <option key={driver} value={driver}>
              {driver}
            </option>
          ))}
          {hasCustomValue && <option value={value}>Custom ({value})</option>}
        </select>
        <p className="text-[10px] text-slate-400 mt-1">{field.description}</p>
      </div>
    );
  }

  if (field.key === "flightSqlEngine") {
    if ((flat?.driver ?? "").trim().toLowerCase() !== "flightsql") return null;
    const options = ["starrocks", "trino", "clickhouse", "duckdb"] as const;
    const hasCustomValue = !!value && !options.includes(value as never);
    return (
      <div>
        <label
          htmlFor={id}
          className="block text-[11px] font-semibold text-slate-500 uppercase tracking-wide mb-1.5"
        >
          {field.label}
        </label>
        <select
          id={id}
          value={value}
          disabled={readOnly}
          onChange={(e) => onChange(e.target.value)}
          className={`w-full text-sm border rounded-lg px-3 py-2 focus:outline-none focus:ring-2 focus:ring-indigo-300 ${
            readOnly
              ? "border-slate-200 bg-slate-50 text-slate-700 cursor-not-allowed"
              : "border-slate-200 bg-white"
          }`}
        >
          <option value="">— Select target engine —</option>
          {options.map((opt) => (
            <option key={opt} value={opt}>
              {opt}
            </option>
          ))}
          {hasCustomValue && <option value={value}>Custom ({value})</option>}
        </select>
        <p className="text-[10px] text-slate-400 mt-1">{field.description}</p>
      </div>
    );
  }

  if (isDbKwargsField) {
    const driver = (flat?.driver ?? "").trim().toLowerCase();
    const driverHint =
      driver === "flightsql"
        ? "Tip: for FlightSQL, auth usually belongs in username/password. Use options only for driver-specific settings."
        : "Tip: key/value mode stores string values only. Use Raw JSON for nested objects.";
    return (
      <DbKwargsField
        id={id}
        field={field}
        value={value}
        onChange={onChange}
        readOnly={readOnly}
        driverHint={driverHint}
        dbKwargsError={dbKwargsError}
      />
    );
  }

  const inputType =
    field.fieldType === "secret"
      ? "password"
      : field.fieldType === "url"
        ? "url"
        : "text";

  const textPlaceholder =
    field.key === "uri" &&
    (flat?.driver ?? "").trim().toLowerCase() === "postgresql"
      ? "postgresql://user:pass@localhost:5433/postgres"
      : field.example;

  return (
    <div>
      <label
        htmlFor={id}
        className="block text-[11px] font-semibold text-slate-500 uppercase tracking-wide mb-1.5"
      >
        {field.label}
        {field.required && <span className="text-red-500 ml-0.5">*</span>}
      </label>
      <input
        id={id}
        type={inputType}
        value={value}
        onChange={(e) => onChange(e.target.value)}
        placeholder={textPlaceholder}
        className="w-full text-sm border border-slate-200 rounded-lg px-3 py-2 font-mono focus:outline-none focus:ring-2 focus:ring-indigo-300 focus:border-indigo-400"
        autoComplete={field.fieldType === "secret" ? "new-password" : "off"}
      />
      <p className="text-[10px] text-slate-400 mt-1">{field.description}</p>
    </div>
  );
}
