"use client";

import type { ConfigField } from "@/lib/engine-registry";
import { adbcDocsUrlForDriver } from "@/lib/adbc-driver-spec";
import { SQLGLOT_WRITE_DIALECTS } from "@/lib/sqlglot-dialects";
import { DbKwargsField } from "./db-kwargs-field";

function formatSqlglotDialectLabel(value: string): string {
  if (value === "tsql") return "TSQL";
  if (value === "prql") return "PRQL";
  return value.length
    ? value[0].toUpperCase() + value.slice(1)
    : value;
}

const ADBC_DRIVER_OPTIONS = [
  "trino",
  "duckdb",
  "clickhouse",
  "mysql",
  "flightsql",
  "postgresql",
  "sqlite",
  "snowflake",
  "bigquery",
  "databricks",
  "mssql",
  "redshift",
  "exasol",
  "singlestore",
  "jdbc",
] as const;

const ADBC_URI_EXAMPLES: Partial<Record<(typeof ADBC_DRIVER_OPTIONS)[number], string>> = {
  trino: "http://trino-host:8080",
  duckdb: "file:///tmp/queryflux.duckdb",
  clickhouse: "http://clickhouse-host:8123/default",
  mysql: "mysql://user:pass@mysql-host:3306/db",
  postgresql: "postgresql://user:pass@localhost:5433/postgres",
  sqlite: "file:///tmp/queryflux.sqlite",
  flightsql: "grpc+tls://flightsql-host:32010",
  snowflake: "user:pass@account/database/schema?warehouse=WH&role=ROLE",
  bigquery: "bigquery://project-id",
  databricks: "databricks://token:<token>@<workspace-host>?http_path=<http-path>",
  mssql: "sqlserver://user:pass@mssql-host:1433?database=mydb",
  redshift: "redshift://user:pass@redshift-host:5439/dev",
  exasol: "exa://user:pass@exasol-host:8563/schema",
  singlestore: "mysql://user:pass@singlestore-host:3306/db",
  jdbc: "jdbc:postgresql://db-host:5432/postgres",
};

function isKnownAdbcDriver(
  v: string,
): v is (typeof ADBC_DRIVER_OPTIONS)[number] {
  return ADBC_DRIVER_OPTIONS.includes(v as never);
}

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
    const docsUrl = adbcDocsUrlForDriver(value);
    return (
      <div>
        <div className="flex items-center justify-between gap-2 mb-1.5">
          <label
            htmlFor={id}
            className="block text-[11px] font-semibold text-slate-500 uppercase tracking-wide"
          >
            {field.label}
            {field.required && <span className="text-red-500 ml-0.5">*</span>}
          </label>
          {docsUrl && (
            <a
              href={docsUrl}
              target="_blank"
              rel="noreferrer"
              className="text-[10px] font-medium text-indigo-700 hover:text-indigo-800 underline underline-offset-2"
            >
              Driver docs
            </a>
          )}
        </div>
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

  if (field.key === "flightSqlClusterDialect") {
    if ((flat?.driver ?? "").trim().toLowerCase() !== "flightsql") return null;
    const v = (value ?? "").trim().toLowerCase();
    const hasCustomValue =
      !!v && !SQLGLOT_WRITE_DIALECTS.includes(v);
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
          <option value="">— Select sqlglot dialect —</option>
          {SQLGLOT_WRITE_DIALECTS.map((opt) => (
            <option key={opt} value={opt}>
              {formatSqlglotDialectLabel(opt)}
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
    field.key === "uri"
      ? (() => {
          const raw = (flat?.driver ?? "").trim().toLowerCase();
          if (!isKnownAdbcDriver(raw)) return field.example;
          return ADBC_URI_EXAMPLES[raw] ?? field.example;
        })()
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
