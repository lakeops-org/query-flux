"use client";

import { useCallback, useEffect, useState } from "react";
import { useRouter } from "next/navigation";
import { AlertCircle, ChevronDown, ChevronUp, Loader2, X } from "lucide-react";
import { listUserScripts, renameGroupConfig, upsertGroupConfig } from "@/lib/api";
import type {
  ClusterGroupConfigRecord,
  UpsertClusterGroupConfig,
  UserScriptRecord,
} from "@/lib/api-types";
import {
  buildStrategyPayload,
  ENGINE_AFFINITY_OPTIONS,
  parseStrategyRecord,
  STRATEGY_OPTIONS,
  type StrategyKind,
} from "@/lib/cluster-group-strategy";

type Props = {
  open: boolean;
  onClose: () => void;
  mode: "create" | "edit";
  /** Edit: existing row. Create: null */
  initial: ClusterGroupConfigRecord | null;
  clusterNames: string[];
  existingGroupNames: string[];
};

const NAME_RE = /^[a-zA-Z][a-zA-Z0-9_-]{0,62}$/;

export function GroupFormDialog({
  open,
  onClose,
  mode,
  initial,
  clusterNames,
  existingGroupNames,
}: Props) {
  const router = useRouter();
  const [groupName, setGroupName] = useState("");
  const [enabled, setEnabled] = useState(true);
  const [selectedMembers, setSelectedMembers] = useState<Set<string>>(new Set());
  const [maxRunning, setMaxRunning] = useState("10");
  const [maxQueued, setMaxQueued] = useState("");
  const [strategyKind, setStrategyKind] = useState<StrategyKind>("default");
  const [enginePreferenceCsv, setEnginePreferenceCsv] = useState("");
  const [weightedJson, setWeightedJson] = useState("{}");
  const [translationScriptIds, setTranslationScriptIds] = useState<number[]>([]);
  const [scriptLibrary, setScriptLibrary] = useState<UserScriptRecord[]>([]);
  const [addScriptId, setAddScriptId] = useState<string>("");
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const reset = useCallback(() => {
    setGroupName("");
    setEnabled(true);
    setSelectedMembers(new Set());
    setMaxRunning("10");
    setMaxQueued("");
    setStrategyKind("default");
    setEnginePreferenceCsv("");
    setWeightedJson("{}");
    setTranslationScriptIds([]);
    setAddScriptId("");
    setError(null);
  }, []);

  useEffect(() => {
    if (!open) return;
    setError(null);
    if (mode === "create") {
      reset();
    } else if (initial) {
      setGroupName(initial.name);
      setEnabled(initial.enabled);
      setSelectedMembers(new Set(initial.members));
      setMaxRunning(String(initial.maxRunningQueries));
      setMaxQueued(
        initial.maxQueuedQueries != null ? String(initial.maxQueuedQueries) : "",
      );
      setTranslationScriptIds([...initial.translationScriptIds]);
      {
        const p = parseStrategyRecord(initial.strategy);
        setStrategyKind(p.kind);
        setEnginePreferenceCsv(p.enginePreferenceCsv);
        setWeightedJson(p.weightedJson.trim() ? p.weightedJson : "{}");
      }
    }
  }, [open, mode, initial, reset]);

  useEffect(() => {
    if (!open) return;
    let cancelled = false;
    listUserScripts("translation_fixup")
      .then((rows) => {
        if (!cancelled) setScriptLibrary(rows);
      })
      .catch(() => {
        if (!cancelled) setScriptLibrary([]);
      });
    return () => {
      cancelled = true;
    };
  }, [open]);

  function toggleMember(name: string) {
    setSelectedMembers((prev) => {
      const next = new Set(prev);
      if (next.has(name)) next.delete(name);
      else next.add(name);
      return next;
    });
  }

  async function handleSubmit() {
    setError(null);
    const nameTrim = groupName.trim();
    if (mode === "create") {
      if (!NAME_RE.test(nameTrim)) {
        setError(
          "Group name must start with a letter and use only letters, numbers, underscores, and hyphens (max 63 chars).",
        );
        return;
      }
      if (existingGroupNames.includes(nameTrim)) {
        setError("A group with this name already exists.");
        return;
      }
    } else if (initial && nameTrim !== initial.name) {
      if (!NAME_RE.test(nameTrim)) {
        setError(
          "Group name must start with a letter and use only letters, numbers, underscores, and hyphens (max 63 chars).",
        );
        return;
      }
      const taken = existingGroupNames.some((n) => n !== initial.name && n === nameTrim);
      if (taken) {
        setError("A group with this name already exists.");
        return;
      }
    }

    const maxR = parseInt(maxRunning.trim(), 10);
    if (!Number.isFinite(maxR) || maxR < 1) {
      setError("Max running queries must be a positive integer.");
      return;
    }
    let maxQ: number | null = null;
    const mq = maxQueued.trim();
    if (mq !== "") {
      const n = parseInt(mq, 10);
      if (!Number.isFinite(n) || n < 0) {
        setError("Max queued queries must be empty or a non-negative integer.");
        return;
      }
      maxQ = n;
    }

    let strategy: Record<string, unknown> | null;
    try {
      strategy = buildStrategyPayload(
        strategyKind,
        enginePreferenceCsv,
        weightedJson,
      );
    } catch (e) {
      setError(e instanceof Error ? e.message : "Invalid strategy");
      return;
    }

    const sortedClusters = [...clusterNames].sort((a, b) => a.localeCompare(b));
    const members = sortedClusters.filter((n) => selectedMembers.has(n));

    const body: UpsertClusterGroupConfig = {
      enabled,
      members,
      maxRunningQueries: maxR,
      maxQueuedQueries: maxQ,
      strategy,
      // Group allow-lists are driven by routing / security config, not edited here.
      allowGroups: mode === "edit" && initial ? [...initial.allowGroups] : [],
      allowUsers: mode === "edit" && initial ? [...initial.allowUsers] : [],
      translationScriptIds,
    };

    const pathName = mode === "create" ? nameTrim : nameTrim || (initial?.name ?? "").trim();
    if (!pathName) {
      setError("Missing group name.");
      return;
    }

    setSaving(true);
    try {
      if (mode === "edit" && initial && nameTrim !== initial.name) {
        await renameGroupConfig(initial.name, { newName: nameTrim });
      }
      await upsertGroupConfig(pathName, body);
      router.refresh();
      reset();
      onClose();
    } catch (e) {
      setError(e instanceof Error ? e.message : "Save failed");
    } finally {
      setSaving(false);
    }
  }

  useEffect(() => {
    if (!open) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape" && !saving) onClose();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [open, saving, onClose]);

  if (!open) return null;

  const sortedClusters = [...clusterNames].sort((a, b) => a.localeCompare(b));

  return (
    <div className="fixed inset-0 z-[70] flex items-center justify-center p-4">
      <button
        type="button"
        aria-label="Close dialog"
        disabled={saving}
        className="absolute inset-0 bg-slate-900/45 backdrop-blur-sm disabled:cursor-wait"
        onClick={() => {
          if (!saving) onClose();
        }}
      />
      <div className="relative w-full max-w-2xl max-h-[90vh] overflow-hidden flex flex-col rounded-2xl bg-white shadow-2xl border border-slate-200">
        <div className="flex items-center justify-between px-5 py-4 border-b border-slate-100">
          <h2 className="text-base font-bold text-slate-900">
            {mode === "create" ? "New cluster group" : "Edit cluster group"}
          </h2>
          <button
            type="button"
            disabled={saving}
            onClick={() => !saving && onClose()}
            className="p-1.5 rounded-lg text-slate-400 hover:text-slate-700 hover:bg-slate-100"
          >
            <X size={18} />
          </button>
        </div>

        <div className="overflow-y-auto flex-1 px-5 py-4 space-y-4">
          {mode === "create" ? (
            <div>
              <label className="block text-[11px] font-semibold text-slate-500 uppercase tracking-wide mb-1.5">
                Group name <span className="text-red-500">*</span>
              </label>
              <input
                value={groupName}
                onChange={(e) => setGroupName(e.target.value)}
                placeholder="e.g. analytics"
                autoComplete="off"
                className="w-full text-sm border border-slate-200 rounded-lg px-3 py-2 font-mono focus:outline-none focus:ring-2 focus:ring-indigo-300 focus:border-indigo-400"
              />
            </div>
          ) : (
            <div>
              <label className="block text-[11px] font-semibold text-slate-500 uppercase tracking-wide mb-1.5">
                Group name <span className="text-red-500">*</span>
              </label>
              <input
                value={groupName}
                onChange={(e) => setGroupName(e.target.value)}
                placeholder="e.g. analytics"
                autoComplete="off"
                className="w-full text-sm border border-slate-200 rounded-lg px-3 py-2 font-mono focus:outline-none focus:ring-2 focus:ring-indigo-300 focus:border-indigo-400"
              />
              <p className="text-[10px] text-slate-400 mt-1">
                Changing the name updates routing fallback text when it matched the old name. Routing rules use stable ids.
              </p>
            </div>
          )}

          <div className="flex items-center justify-between gap-4">
            <div>
              <p className="text-sm font-medium text-slate-700">Group enabled</p>
              <p className="text-[11px] text-slate-400 mt-0.5">
                Disabled groups are not loaded by the proxy.
              </p>
            </div>
            <button
              type="button"
              role="switch"
              aria-checked={enabled}
              onClick={() => setEnabled((v) => !v)}
              className={`relative inline-flex h-5 w-9 items-center rounded-full transition-colors focus:outline-none focus-visible:ring-2 focus-visible:ring-indigo-400 flex-shrink-0 ${
                enabled ? "bg-emerald-500" : "bg-slate-300"
              }`}
            >
              <span
                className={`inline-block h-3.5 w-3.5 transform rounded-full bg-white shadow transition-transform ${
                  enabled ? "translate-x-4.5" : "translate-x-0.5"
                }`}
              />
            </button>
          </div>

          <div className="grid grid-cols-2 gap-3">
            <div>
              <label className="block text-[11px] font-semibold text-slate-500 uppercase tracking-wide mb-1.5">
                Max running queries <span className="text-red-500">*</span>
              </label>
              <input
                type="number"
                min={1}
                value={maxRunning}
                onChange={(e) => setMaxRunning(e.target.value)}
                className="w-full text-sm border border-slate-200 rounded-lg px-3 py-2 font-mono focus:outline-none focus:ring-2 focus:ring-indigo-300"
              />
            </div>
            <div>
              <label className="block text-[11px] font-semibold text-slate-500 uppercase tracking-wide mb-1.5">
                Max queued queries
              </label>
              <input
                type="number"
                min={0}
                value={maxQueued}
                onChange={(e) => setMaxQueued(e.target.value)}
                placeholder="optional"
                className="w-full text-sm border border-slate-200 rounded-lg px-3 py-2 font-mono focus:outline-none focus:ring-2 focus:ring-indigo-300"
              />
            </div>
          </div>

          <div className="rounded-xl border border-indigo-100 bg-indigo-50/40 p-4 space-y-3">
            <div>
              <p className="text-[11px] font-semibold text-slate-600 uppercase tracking-wide">
                Translation scripts (optional)
              </p>
              <p className="text-[10px] text-slate-500 mt-1 leading-relaxed">
                Ordered post-sqlglot Python fixups for this group (after global YAML{" "}
                <code className="text-[10px] bg-white px-1 rounded border">translation.pythonScripts</code>
                ). Manage snippets in{" "}
                <strong>Scripts</strong> in the sidebar.
              </p>
            </div>
            {translationScriptIds.length > 0 ? (
              <ul className="space-y-1.5">
                {translationScriptIds.map((sid, idx) => {
                  const meta = scriptLibrary.find((s) => s.id === sid);
                  const label = meta?.name ?? `id:${sid}`;
                  return (
                    <li
                      key={`${sid}-${idx}`}
                      className="flex items-center gap-2 text-sm bg-white border border-slate-200 rounded-lg px-2 py-1.5"
                    >
                      <span className="font-mono text-slate-800 flex-1 min-w-0 truncate">{label}</span>
                      <button
                        type="button"
                        disabled={saving || idx === 0}
                        onClick={() => {
                          setTranslationScriptIds((prev) => {
                            const next = [...prev];
                            [next[idx - 1], next[idx]] = [next[idx], next[idx - 1]];
                            return next;
                          });
                        }}
                        className="p-1 rounded text-slate-400 hover:bg-slate-100 disabled:opacity-30"
                        title="Move up"
                      >
                        <ChevronUp size={16} />
                      </button>
                      <button
                        type="button"
                        disabled={saving || idx >= translationScriptIds.length - 1}
                        onClick={() => {
                          setTranslationScriptIds((prev) => {
                            const next = [...prev];
                            [next[idx], next[idx + 1]] = [next[idx + 1], next[idx]];
                            return next;
                          });
                        }}
                        className="p-1 rounded text-slate-400 hover:bg-slate-100 disabled:opacity-30"
                        title="Move down"
                      >
                        <ChevronDown size={16} />
                      </button>
                      <button
                        type="button"
                        disabled={saving}
                        onClick={() =>
                          setTranslationScriptIds((prev) => prev.filter((_, i) => i !== idx))
                        }
                        className="p-1 rounded text-red-400 hover:bg-red-50"
                        title="Remove"
                      >
                        <X size={16} />
                      </button>
                    </li>
                  );
                })}
              </ul>
            ) : (
              <p className="text-xs text-slate-400">No scripts attached.</p>
            )}
            <div className="flex gap-2 items-center">
              <select
                value={addScriptId}
                onChange={(e) => setAddScriptId(e.target.value)}
                className="flex-1 text-sm border border-slate-200 rounded-lg px-3 py-2 bg-white focus:outline-none focus:ring-2 focus:ring-indigo-300"
              >
                <option value="">Add script…</option>
                {scriptLibrary
                  .filter((s) => !translationScriptIds.includes(s.id))
                  .map((s) => (
                    <option key={s.id} value={String(s.id)}>
                      {s.name}
                    </option>
                  ))}
              </select>
              <button
                type="button"
                disabled={saving || !addScriptId}
                onClick={() => {
                  const id = parseInt(addScriptId, 10);
                  if (!Number.isFinite(id)) return;
                  setTranslationScriptIds((prev) => [...prev, id]);
                  setAddScriptId("");
                }}
                className="text-xs font-semibold px-3 py-2 rounded-lg bg-indigo-600 text-white hover:bg-indigo-700 disabled:opacity-40"
              >
                Add
              </button>
            </div>
          </div>

          <div>
            <label className="block text-[11px] font-semibold text-slate-500 uppercase tracking-wide mb-1.5">
              Members (clusters)
            </label>
            <p className="text-[10px] text-slate-400 mb-2">
              Order follows the sorted list below; only existing cluster configs can be added.
            </p>
            <div className="max-h-40 overflow-y-auto rounded-lg border border-slate-200 divide-y divide-slate-100 bg-slate-50/50">
              {sortedClusters.length === 0 ? (
                <p className="text-xs text-slate-400 px-3 py-4 text-center">
                  No clusters in Postgres — add clusters first.
                </p>
              ) : (
                sortedClusters.map((n) => (
                  <label
                    key={n}
                    className="flex items-center gap-3 px-3 py-2 hover:bg-white cursor-pointer"
                  >
                    <input
                      type="checkbox"
                      checked={selectedMembers.has(n)}
                      onChange={() => toggleMember(n)}
                      className="rounded border-slate-300 text-indigo-600 focus:ring-indigo-400"
                    />
                    <span className="text-sm font-mono text-slate-800">{n}</span>
                  </label>
                ))
              )}
            </div>
          </div>

          <div className="rounded-xl border border-slate-200 bg-slate-50/60 p-4 space-y-3">
            <div>
              <label
                htmlFor="group-strategy-kind"
                className="block text-[11px] font-semibold text-slate-500 uppercase tracking-wide mb-1.5"
              >
                Routing strategy
              </label>
              <select
                id="group-strategy-kind"
                value={strategyKind}
                onChange={(e) =>
                  setStrategyKind(e.target.value as StrategyKind)
                }
                className="w-full text-sm border border-slate-200 rounded-lg px-3 py-2 bg-white focus:outline-none focus:ring-2 focus:ring-indigo-300"
              >
                {STRATEGY_OPTIONS.map((o) => (
                  <option key={o.value} value={o.value}>
                    {o.label}
                  </option>
                ))}
              </select>
              <p className="text-[10px] text-slate-500 mt-2 leading-relaxed">
                {STRATEGY_OPTIONS.find((o) => o.value === strategyKind)
                  ?.description ?? ""}
              </p>
            </div>

            {strategyKind === "engineAffinity" ? (
              <div>
                <label className="block text-[11px] font-semibold text-slate-500 uppercase tracking-wide mb-1.5">
                  Engine preference order
                </label>
                <input
                  value={enginePreferenceCsv}
                  onChange={(e) => setEnginePreferenceCsv(e.target.value)}
                  placeholder="e.g. trino, starRocks, duckDb"
                  className="w-full text-sm font-mono border border-slate-200 rounded-lg px-3 py-2 focus:outline-none focus:ring-2 focus:ring-indigo-300"
                />
                <p className="text-[10px] text-slate-400 mt-1.5">
                  Comma-separated, highest priority first. Allowed values:{" "}
                  {ENGINE_AFFINITY_OPTIONS.map((e) => (
                    <code
                      key={e.value}
                      className="text-[10px] bg-white px-1 rounded border border-slate-200 mx-0.5"
                    >
                      {e.value}
                    </code>
                  ))}
                </p>
              </div>
            ) : null}

            {strategyKind === "weighted" ? (
              <div>
                <label className="block text-[11px] font-semibold text-slate-500 uppercase tracking-wide mb-1.5">
                  Weights (JSON object)
                </label>
                <textarea
                  value={weightedJson}
                  onChange={(e) => setWeightedJson(e.target.value)}
                  placeholder={
                    [...selectedMembers].length > 0
                      ? JSON.stringify(
                          Object.fromEntries(
                            [...selectedMembers]
                              .slice(0, 4)
                              .map((n, i) => [n, (i % 3) + 1]),
                          ),
                          null,
                          2,
                        )
                      : '{"trino-prod": 3, "trino-dr": 1}'
                  }
                  rows={5}
                  className="w-full text-xs font-mono border border-slate-200 rounded-lg px-3 py-2 bg-white focus:outline-none focus:ring-2 focus:ring-indigo-300"
                />
                <p className="text-[10px] text-slate-400 mt-1.5">
                  Keys must match cluster <strong>names</strong> in this group. Missing clusters
                  default to weight 1 at runtime.
                </p>
              </div>
            ) : null}
          </div>

          {error && (
            <p className="text-xs text-red-600 flex items-start gap-1.5">
              <AlertCircle size={14} className="flex-shrink-0 mt-0.5" />
              {error}
            </p>
          )}
        </div>

        <div className="flex gap-2 px-5 py-4 border-t border-slate-100 bg-slate-50/80">
          <button
            type="button"
            disabled={saving}
            onClick={() => !saving && onClose()}
            className="flex-1 px-3 py-2.5 rounded-xl text-sm font-semibold text-slate-700 bg-white border border-slate-200 hover:bg-slate-50 disabled:opacity-50"
          >
            Cancel
          </button>
          <button
            type="button"
            disabled={saving}
            onClick={() => void handleSubmit()}
            className="flex-1 flex items-center justify-center gap-2 px-3 py-2.5 rounded-xl text-sm font-semibold text-white bg-indigo-600 hover:bg-indigo-700 disabled:opacity-60"
          >
            {saving ? <Loader2 size={16} className="animate-spin" /> : null}
            {saving ? "Saving…" : "Save group"}
          </button>
        </div>
      </div>
    </div>
  );
}
