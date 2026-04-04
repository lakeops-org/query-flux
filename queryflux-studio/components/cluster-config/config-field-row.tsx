"use client";

import type { ConfigField } from "@/lib/engine-registry";

export function ConfigFieldRow({
  field,
  value,
  onChange,
  supportedAuth,
}: {
  field: ConfigField;
  value: string;
  onChange: (v: string) => void;
  supportedAuth: string[];
}) {
  const id = `cluster-field-${field.key.replace(/[^a-zA-Z0-9]/g, "-")}`;

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

  const inputType =
    field.fieldType === "secret"
      ? "password"
      : field.fieldType === "url"
        ? "url"
        : "text";

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
        placeholder={field.example}
        className="w-full text-sm border border-slate-200 rounded-lg px-3 py-2 font-mono focus:outline-none focus:ring-2 focus:ring-indigo-300 focus:border-indigo-400"
        autoComplete={field.fieldType === "secret" ? "new-password" : "off"}
      />
      <p className="text-[10px] text-slate-400 mt-1">{field.description}</p>
    </div>
  );
}
