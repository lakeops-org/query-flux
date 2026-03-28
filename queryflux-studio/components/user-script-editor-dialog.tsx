"use client";

import { useEffect, useState } from "react";
import { createUserScript, updateUserScript } from "@/lib/api";
import type { UpsertUserScript, UserScriptRecord } from "@/lib/api-types";
import { TRANSLATION_FIXUP_TEMPLATE } from "@/lib/script-templates";
import { Loader2 } from "lucide-react";
import CodeMirror from "@uiw/react-codemirror";
import { python } from "@codemirror/lang-python";
import { oneDark } from "@codemirror/theme-one-dark";

/** Persisted as `user_scripts.kind` — translation fixups only (routing uses the Routing page chain). */
export const KIND_TRANSLATION_FIXUP = "translation_fixup";

export interface UserScriptEditorDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  /** `null` = create; otherwise edit this row */
  initialEdit: UserScriptRecord | null;
  onSaved: () => void;
}

export function UserScriptEditorDialog({
  open,
  onOpenChange,
  initialEdit,
  onSaved,
}: UserScriptEditorDialogProps) {
  const [editingId, setEditingId] = useState<number | null>(null);
  const [formName, setFormName] = useState("");
  const [formDescription, setFormDescription] = useState("");
  const [formBody, setFormBody] = useState("");
  const [saving, setSaving] = useState(false);
  const [saveError, setSaveError] = useState<string | null>(null);

  useEffect(() => {
    if (!open) return;
    setSaveError(null);
    if (initialEdit) {
      setEditingId(initialEdit.id);
      setFormName(initialEdit.name);
      setFormDescription(initialEdit.description ?? "");
      setFormBody(initialEdit.body);
    } else {
      setEditingId(null);
      setFormName("");
      setFormDescription("");
      setFormBody(TRANSLATION_FIXUP_TEMPLATE);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps -- reset when opening for a different script (id) or mode
  }, [open, initialEdit?.id]);

  async function handleSave() {
    const name = formName.trim();
    if (!name) {
      setSaveError("Name is required.");
      return;
    }
    const body: UpsertUserScript = {
      name,
      description: formDescription.trim(),
      kind: KIND_TRANSLATION_FIXUP,
      body: formBody,
    };
    setSaving(true);
    setSaveError(null);
    try {
      if (editingId == null) {
        await createUserScript(body);
      } else {
        await updateUserScript(editingId, body);
      }
      onOpenChange(false);
      onSaved();
    } catch (e) {
      setSaveError(e instanceof Error ? e.message : "Save failed");
    } finally {
      setSaving(false);
    }
  }

  if (!open) return null;

  return (
    <div className="fixed inset-0 z-[80] flex items-center justify-center p-4">
      <button
        type="button"
        aria-label="Close"
        className="absolute inset-0 bg-slate-900/45 backdrop-blur-sm"
        onClick={() => !saving && onOpenChange(false)}
      />
      <div className="relative w-full max-w-3xl max-h-[90vh] flex flex-col rounded-2xl bg-white shadow-2xl border border-slate-200">
        <div className="px-5 py-4 border-b border-slate-100 flex items-center justify-between">
          <h2 className="text-base font-bold text-slate-900">
            {editingId == null ? "New translation script" : "Edit translation script"}
          </h2>
          <button
            type="button"
            disabled={saving}
            onClick={() => onOpenChange(false)}
            className="text-slate-400 hover:text-slate-700"
          >
            ×
          </button>
        </div>
        {saveError && (
          <div className="mx-5 mt-3 text-sm text-red-700 bg-red-50 border border-red-100 rounded-lg px-3 py-2">
            {saveError}
          </div>
        )}
        <div className="overflow-y-auto flex-1 px-5 py-4 space-y-4">
          <div>
            <label className="block text-[11px] font-semibold text-slate-500 uppercase mb-1">
              Name
            </label>
            <input
              value={formName}
              onChange={(e) => setFormName(e.target.value)}
              className="w-full text-sm border border-slate-200 rounded-lg px-3 py-2 font-mono"
              disabled={saving}
            />
          </div>
          <div>
            <label className="block text-[11px] font-semibold text-slate-500 uppercase mb-1">
              Description
            </label>
            <input
              value={formDescription}
              onChange={(e) => setFormDescription(e.target.value)}
              className="w-full text-sm border border-slate-200 rounded-lg px-3 py-2"
              disabled={saving}
            />
          </div>
          <div>
            <label className="block text-[11px] font-semibold text-slate-500 uppercase mb-1">
              Python body
            </label>
            <div className="border border-slate-200 rounded-lg overflow-hidden text-xs">
              <CodeMirror
                value={formBody}
                onChange={(val) => setFormBody(val)}
                extensions={[python()]}
                theme={oneDark}
                height="320px"
                editable={!saving}
              />
            </div>
            <div className="flex flex-wrap items-center justify-between gap-2 mt-1">
              <p className="text-[10px] text-slate-400 flex-1 min-w-[12rem]">
                Define <code>def transform(ast: Expression, src: str, dst: str) -&gt; None:</code>. Keep the
                pre-filled imports — they are required at runtime.
              </p>
              <button
                type="button"
                disabled={saving}
                onClick={() => setFormBody(TRANSLATION_FIXUP_TEMPLATE)}
                className="text-[10px] font-semibold uppercase tracking-wide text-indigo-600 hover:text-indigo-800 px-2 py-1 rounded-md hover:bg-indigo-50 shrink-0"
              >
                Insert template
              </button>
            </div>
          </div>
        </div>
        <div className="px-5 py-4 border-t border-slate-100 flex justify-end gap-2">
          <button
            type="button"
            disabled={saving}
            onClick={() => onOpenChange(false)}
            className="text-sm font-medium px-4 py-2 rounded-lg text-slate-600 hover:bg-slate-100"
          >
            Cancel
          </button>
          <button
            type="button"
            disabled={saving}
            onClick={handleSave}
            className="text-sm font-semibold px-4 py-2 rounded-lg bg-indigo-600 text-white hover:bg-indigo-700 inline-flex items-center gap-2"
          >
            {saving && <Loader2 className="animate-spin" size={16} />}
            Save
          </button>
        </div>
      </div>
    </div>
  );
}
