"use client";

import { useState } from "react";
import { ROUTING_SCRIPT_TEMPLATE } from "@/lib/script-templates";
import CodeMirror from "@uiw/react-codemirror";
import { python } from "@codemirror/lang-python";
import { oneDark } from "@codemirror/theme-one-dark";

export interface PythonRoutingScriptDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  /** `null` = append a new Python script step on commit; otherwise replace that row’s script */
  editChainItemId: string | null;
  /** Seed editor when the dialog opens */
  initialScript: string;
  onCommit: (script: string, editChainItemId: string | null) => void;
}

/**
 * Modal editor for a routing-chain Python script step (`def route(query, ctx)`).
 * Same interaction pattern as translation scripts in {@link UserScriptEditorDialog}.
 */
export function PythonRoutingScriptDialog({
  open,
  onOpenChange,
  editChainItemId,
  initialScript,
  onCommit,
}: PythonRoutingScriptDialogProps) {
  const [draft, setDraft] = useState(initialScript);

  if (!open) return null;

  const isEdit = editChainItemId != null;

  return (
    <div className="fixed inset-0 z-[80] flex items-center justify-center p-4">
      <button
        type="button"
        aria-label="Close"
        className="absolute inset-0 bg-slate-900/45 backdrop-blur-sm"
        onClick={() => onOpenChange(false)}
      />
      <div className="relative w-full max-w-3xl max-h-[90vh] flex flex-col rounded-2xl bg-white shadow-2xl border border-slate-200">
        <div className="px-5 py-4 border-b border-slate-100 flex items-center justify-between">
          <h2 className="text-base font-bold text-slate-900">
            {isEdit ? "Edit Python script router" : "New Python script router"}
          </h2>
          <button
            type="button"
            onClick={() => onOpenChange(false)}
            className="text-slate-400 hover:text-slate-700"
          >
            ×
          </button>
        </div>
        <div className="overflow-y-auto flex-1 px-5 py-4 space-y-3">
          <p className="text-[11px] text-slate-500">
            Define{" "}
            <code className="text-slate-700 bg-slate-50 px-1 rounded font-mono text-[10px]">
              def route(query: str, ctx: dict) -&gt; str | None
            </code>
            . Return a cluster <strong>group</strong> name or <code>None</code> to continue the chain. See{" "}
            <code className="text-slate-500">docs/routing-and-clusters.md</code>.
          </p>
          <div>
            <label className="block text-[11px] font-semibold text-slate-500 uppercase mb-1">
              Python body
            </label>
            <div className="border border-slate-200 rounded-lg overflow-hidden text-xs">
              <CodeMirror
                value={draft}
                onChange={(v) => setDraft(v)}
                extensions={[python()]}
                theme={oneDark}
                height="360px"
              />
            </div>
            <div className="flex justify-end mt-1">
              <button
                type="button"
                onClick={() => setDraft(ROUTING_SCRIPT_TEMPLATE)}
                className="text-[10px] font-semibold uppercase tracking-wide text-indigo-600 hover:text-indigo-800 px-2 py-1 rounded-md hover:bg-indigo-50"
              >
                Insert template
              </button>
            </div>
          </div>
        </div>
        <div className="px-5 py-4 border-t border-slate-100 flex justify-end gap-2">
          <button
            type="button"
            onClick={() => onOpenChange(false)}
            className="text-sm font-medium px-4 py-2 rounded-lg text-slate-600 hover:bg-slate-100"
          >
            Cancel
          </button>
          <button
            type="button"
            onClick={() => {
              onCommit(draft, editChainItemId);
              onOpenChange(false);
            }}
            className="text-sm font-semibold px-4 py-2 rounded-lg bg-indigo-600 text-white hover:bg-indigo-700"
          >
            {isEdit ? "Save" : "Add to chain"}
          </button>
        </div>
      </div>
    </div>
  );
}
