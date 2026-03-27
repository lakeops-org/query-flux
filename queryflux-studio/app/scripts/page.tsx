"use client";

import { useCallback, useEffect, useState } from "react";
import Link from "next/link";
import { useRouter } from "next/navigation";
import { deleteUserScript, listUserScripts } from "@/lib/api";
import type { UserScriptRecord } from "@/lib/api-types";
import { UserScriptEditorDialog } from "@/components/user-script-editor-dialog";
import { AlertCircle, Loader2, Pencil, Plus, Trash2 } from "lucide-react";

export default function ScriptsPage() {
  const router = useRouter();
  const [scripts, setScripts] = useState<UserScriptRecord[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [editorOpen, setEditorOpen] = useState(false);
  const [dialogEdit, setDialogEdit] = useState<UserScriptRecord | null>(null);

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const rows = await listUserScripts("translation_fixup");
      setScripts(rows);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to load scripts");
      setScripts([]);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    load();
  }, [load]);

  function openCreate() {
    setDialogEdit(null);
    setEditorOpen(true);
  }

  function openEdit(s: UserScriptRecord) {
    setDialogEdit(s);
    setEditorOpen(true);
  }

  async function handleDelete(id: number) {
    if (!confirm("Delete this script? Groups referencing it will have it removed automatically.")) return;
    setError(null);
    try {
      await deleteUserScript(id);
      await load();
      router.refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : "Delete failed");
    }
  }

  return (
    <div className="p-8 max-w-5xl space-y-6">
      <div className="flex items-start justify-between gap-4">
        <div>
          <h1 className="text-2xl font-bold text-slate-900 tracking-tight">Scripts</h1>
          <p className="text-sm text-slate-500 mt-1 max-w-2xl">
            <strong>Translation fixup</strong> scripts run after sqlglot when the client dialect differs from
            the engine. Each defines <code>def transform(ast, src, dst)</code>. Attach them to cluster groups in{" "}
            <strong>Groups</strong>. <strong>Routing</strong> Python (<code>def route(query, ctx)</code>) is
            configured only on the <Link href="/routing" className="text-indigo-600 hover:text-indigo-700 font-medium">Routing</Link>{" "}
            page as ordered steps in the rule chain.
          </p>
        </div>
        <button
          type="button"
          onClick={openCreate}
          className="inline-flex items-center gap-2 text-sm font-semibold px-4 py-2.5 rounded-xl bg-indigo-600 text-white hover:bg-indigo-700 shadow-sm shrink-0"
        >
          <Plus size={18} />
          New script
        </button>
      </div>

      {error && (
        <div className="flex items-center gap-2 text-sm text-red-700 bg-red-50 border border-red-100 rounded-lg px-4 py-3">
          <AlertCircle size={18} />
          {error}
        </div>
      )}

      {loading ? (
        <div className="flex items-center gap-2 text-slate-500 py-16 justify-center">
          <Loader2 className="animate-spin" size={22} />
          Loading…
        </div>
      ) : scripts.length === 0 ? (
        <div className="bg-white rounded-xl border border-slate-200 px-6 py-12 text-center text-slate-500 text-sm">
          No scripts yet. Requires Postgres persistence on the proxy.
        </div>
      ) : (
        <div className="bg-white rounded-xl border border-slate-200 overflow-hidden shadow-xs">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-slate-100 bg-slate-50/80">
                <th className="text-left px-4 py-3 text-xs font-semibold text-slate-400 uppercase">
                  Name
                </th>
                <th className="text-left px-4 py-3 text-xs font-semibold text-slate-400 uppercase">
                  Description
                </th>
                <th className="text-right px-4 py-3 text-xs font-semibold text-slate-400 uppercase w-28">
                  Actions
                </th>
              </tr>
            </thead>
            <tbody>
              {scripts.map((s) => (
                <tr key={s.id} className="border-b border-slate-50 hover:bg-indigo-50/30">
                  <td className="px-4 py-3 font-mono font-medium text-slate-800">{s.name}</td>
                  <td className="px-4 py-3 text-slate-500 max-w-md truncate">{s.description || "—"}</td>
                  <td className="px-4 py-3 text-right">
                    <button
                      type="button"
                      onClick={() => openEdit(s)}
                      className="p-1.5 rounded-lg text-slate-500 hover:bg-white hover:text-indigo-600 border border-transparent hover:border-slate-200"
                      title="Edit"
                    >
                      <Pencil size={16} />
                    </button>
                    <button
                      type="button"
                      onClick={() => handleDelete(s.id)}
                      className="p-1.5 rounded-lg text-slate-500 hover:bg-red-50 hover:text-red-600 ml-1"
                      title="Delete"
                    >
                      <Trash2 size={16} />
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      <UserScriptEditorDialog
        open={editorOpen}
        onOpenChange={(o) => {
          setEditorOpen(o);
          if (!o) setDialogEdit(null);
        }}
        initialEdit={dialogEdit}
        onSaved={() => {
          void load();
          router.refresh();
        }}
      />
    </div>
  );
}
