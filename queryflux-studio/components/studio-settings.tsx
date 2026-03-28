import React from "react";
import { CheckCircle2, XCircle } from "lucide-react";

export function SectionHeader({ icon, title }: { icon: React.ReactNode; title: string }) {
  return (
    <div className="flex items-center gap-2.5 px-6 py-4 border-b border-slate-100 bg-gradient-to-r from-slate-50 to-white">
      <span className="text-slate-400">{icon}</span>
      <h2 className="text-sm font-bold text-slate-800">{title}</h2>
    </div>
  );
}

export function Field({
  label,
  children,
}: {
  label: string;
  children: React.ReactNode;
}) {
  return (
    <div>
      <label className="block text-[10px] font-semibold text-slate-400 uppercase tracking-widest mb-1">
        {label}
      </label>
      {children}
    </div>
  );
}

export function TextInput({
  label,
  value,
  onChange,
  placeholder,
  type,
}: {
  label: string;
  value: string;
  onChange: (v: string) => void;
  placeholder?: string;
  type?: string;
}) {
  return (
    <Field label={label}>
      <input
        type={type ?? "text"}
        className="w-full px-2.5 py-1.5 text-xs rounded-lg border border-slate-200 bg-white text-slate-900 font-mono focus:outline-none focus:ring-2 focus:ring-indigo-300"
        placeholder={placeholder}
        value={value}
        onChange={(e) => onChange(e.target.value)}
      />
    </Field>
  );
}

export function SaveBar({
  saving,
  message,
  onSave,
  label,
}: {
  saving: boolean;
  message: { text: string; ok: boolean } | null;
  onSave: () => void;
  label?: string;
}) {
  return (
    <div className="flex items-center gap-3 pt-2">
      <button
        type="button"
        onClick={onSave}
        disabled={saving}
        className="px-4 py-2 rounded-lg bg-indigo-600 text-white text-xs font-semibold hover:bg-indigo-700 disabled:opacity-50 transition-colors"
      >
        {saving ? "Saving…" : (label ?? "Save")}
      </button>
      {message && (
        <span
          className={`flex items-center gap-1.5 text-xs font-medium ${
            message.ok ? "text-emerald-600" : "text-red-600"
          }`}
        >
          {message.ok ? <CheckCircle2 size={13} /> : <XCircle size={13} />}
          {message.text}
        </span>
      )}
    </div>
  );
}
