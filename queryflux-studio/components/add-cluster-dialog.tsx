"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import { useRouter } from "next/navigation";
import {
  CATEGORY_ORDER,
  ENGINE_CATALOG,
  type EngineDef,
} from "@/components/engine-catalog";
import { EngineIcon } from "@/components/engine-icon";
import { EngineClusterConfig } from "@/components/cluster-config";
import {
  findEngineDescriptor,
  isClusterOnboardingSelectable,
  validateClusterConfig,
} from "@/lib/engine-registry";
import {
  buildValidateShape,
  toUpsertBody,
  validateEngineSpecific,
} from "@/lib/cluster-persist-form";
import { upsertClusterConfig } from "@/lib/api";
import { AlertCircle, ArrowLeft, ChevronRight, Loader2, X } from "lucide-react";

type Props = {
  open: boolean;
  onClose: () => void;
};

type Step = 1 | 2;

export function AddClusterDialog({ open, onClose }: Props) {
  const router = useRouter();
  const [step, setStep] = useState<Step>(1);
  const [selected, setSelected] = useState<EngineDef | null>(null);
  const [clusterName, setClusterName] = useState("");
  const [flat, setFlat] = useState<Record<string, string>>({});
  const [clusterEnabled, setClusterEnabled] = useState(true);
  const [clusterMaxRunning, setClusterMaxRunning] = useState("");
  const [saving, setSaving] = useState(false);
  const [saveError, setSaveError] = useState<string | null>(null);

  const descriptor = useMemo(
    () => (selected?.engineKey ? findEngineDescriptor(selected.engineKey) : undefined),
    [selected],
  );

  const reset = useCallback(() => {
    setStep(1);
    setSelected(null);
    setClusterName("");
    setFlat({});
    setClusterEnabled(true);
    setClusterMaxRunning("");
    setSaving(false);
    setSaveError(null);
  }, []);

  useEffect(() => {
    if (!open) reset();
  }, [open, reset]);

  useEffect(() => {
    if (!open) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [open, onClose]);

  function goNext() {
    if (!selected || !isClusterOnboardingSelectable(selected)) return;
    setSaveError(null);
    const d = findEngineDescriptor(selected.engineKey ?? "");
    const initial: Record<string, string> = {};
    if (d) {
      for (const f of d.configFields) {
        if (f.key === "auth.type" && d.supportedAuth.length === 1) {
          initial[f.key] = d.supportedAuth[0];
        } else {
          initial[f.key] = "";
        }
      }
    }
    setFlat(initial);
    setStep(2);
  }

  function goBack() {
    setStep(1);
    setSaveError(null);
  }

  async function handleSave() {
    if (!selected?.engineKey || !descriptor) return;
    const name = clusterName.trim();
    if (!name) {
      setSaveError("Cluster name is required.");
      return;
    }
    const specific = validateEngineSpecific(selected.engineKey, flat);
    if (specific.length > 0) {
      setSaveError(specific.join(" "));
      return;
    }
    const validatePayload = buildValidateShape(flat);
    const errs = validateClusterConfig(name, selected.engineKey, validatePayload);
    if (errs.length > 0) {
      setSaveError(errs.join(" "));
      return;
    }
    const maxTrim = clusterMaxRunning.trim();
    if (maxTrim !== "") {
      const n = Number.parseInt(maxTrim, 10);
      if (!Number.isFinite(n) || n < 1 || String(n) !== maxTrim) {
        setSaveError("Max concurrent queries must be a positive integer.");
        return;
      }
    }
    setSaving(true);
    setSaveError(null);
    try {
      await upsertClusterConfig(
        name,
        toUpsertBody(selected.engineKey, flat, {
          enabled: clusterEnabled,
          maxRunningQueriesInput: clusterMaxRunning,
        }),
      );
      reset();
      onClose();
      router.refresh();
    } catch (e) {
      setSaveError(e instanceof Error ? e.message : "Failed to save cluster");
    } finally {
      setSaving(false);
    }
  }

  if (!open) return null;

  const canProceedStep1 = !!selected && isClusterOnboardingSelectable(selected);
  const unsupported =
    selected &&
    (!selected.engineKey ||
      !descriptor ||
      !descriptor.implemented);

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4">
      <div
        className="absolute inset-0 bg-slate-900/40 backdrop-blur-sm"
        onClick={onClose}
        aria-hidden
      />

      <div
        role="dialog"
        aria-modal
        aria-labelledby="add-cluster-title"
        className={`relative w-full ${
          step === 1 ? "max-w-6xl" : "max-w-lg"
        } max-h-[min(92vh,900px)] overflow-hidden flex flex-col bg-white rounded-2xl shadow-2xl border border-slate-200`}
      >
        {/* Header */}
        <div className="flex items-start justify-between px-6 py-4 border-b border-slate-100 flex-shrink-0">
          <div className="flex items-start gap-3 min-w-0">
            {step === 2 && (
              <button
                type="button"
                onClick={goBack}
                className="mt-0.5 w-8 h-8 rounded-lg flex items-center justify-center text-slate-500 hover:text-indigo-600 hover:bg-indigo-50 transition-colors flex-shrink-0"
                aria-label="Back"
              >
                <ArrowLeft size={18} />
              </button>
            )}
            <div>
              <p className="text-[10px] font-semibold text-indigo-600 uppercase tracking-widest">
                {step === 1 ? "Step 1 of 2" : "Step 2 of 2"}
              </p>
              <h2 id="add-cluster-title" className="text-base font-bold text-slate-900">
                {step === 1 ? "Choose cluster type" : "Configure cluster"}
              </h2>
              <p className="text-xs text-slate-500 mt-1">
                {step === 1
                  ? "Select an engine. Config is persisted to Postgres and the proxy reloads it automatically."
                  : "Enter a unique cluster name and connection details."}
              </p>
            </div>
          </div>
          <button
            type="button"
            onClick={onClose}
            className="w-8 h-8 rounded-lg flex items-center justify-center text-slate-400 hover:text-slate-600 hover:bg-slate-100 transition-colors flex-shrink-0"
            aria-label="Close"
          >
            <X size={18} />
          </button>
        </div>

        {/* Body */}
        <div className="overflow-y-auto flex-1 min-h-0">
          {step === 1 ? (
            <div className="px-6 py-5 space-y-6">
              <div className="flex flex-col gap-1 sm:flex-row sm:items-center sm:gap-3">
                <h3 className="text-sm font-semibold text-slate-700">Engines</h3>
                <div className="hidden sm:block flex-1 h-px bg-slate-100" />
                <span className="text-xs text-slate-400">
                  {
                    ENGINE_CATALOG.filter((e) => isClusterOnboardingSelectable(e))
                      .length
                  }{" "}
                  supported · {ENGINE_CATALOG.length} total
                </span>
              </div>
              <p className="text-[11px] text-slate-500 -mt-2">
                Only engines marked as supported can be selected. Others show{" "}
                <span className="font-medium text-slate-600">Not supported yet</span>.
              </p>

              {CATEGORY_ORDER.map((category) => {
                const engines = ENGINE_CATALOG.filter((e) => e.category === category);
                if (engines.length === 0) return null;
                return (
                  <div key={category}>
                    <p className="text-[11px] font-semibold text-slate-400 uppercase tracking-widest mb-3">
                      {category}
                    </p>
                    <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-4 gap-3">
                      {engines.map((e) => {
                        const isSel = selected?.name === e.name;
                        const selectable = isClusterOnboardingSelectable(e);
                        return (
                          <button
                            key={e.name}
                            type="button"
                            disabled={!selectable}
                            onClick={() => setSelected(e)}
                            aria-disabled={!selectable}
                            title={
                              selectable
                                ? undefined
                                : "Not supported yet — no QueryFlux adapter for this engine in Studio."
                            }
                            className={`rounded-xl border p-4 flex items-center gap-3 text-left transition-all duration-150 focus:outline-none ${
                              !selectable
                                ? "border-slate-100 bg-slate-50/80 cursor-not-allowed opacity-75 grayscale-[0.35]"
                                : isSel
                                  ? "border-indigo-500 bg-indigo-50/50 shadow-sm ring-1 ring-indigo-200 focus:ring-2 focus:ring-indigo-400 focus:ring-offset-2"
                                  : "border-slate-200 bg-white hover:border-slate-300 hover:shadow-xs focus:ring-2 focus:ring-indigo-400 focus:ring-offset-2"
                            }`}
                          >
                            <EngineIcon engine={e} size={36} />
                            <div className="min-w-0 flex-1">
                              <div className="flex items-start justify-between gap-2">
                                <p
                                  className={`text-sm font-semibold leading-tight truncate ${
                                    selectable ? "text-slate-800" : "text-slate-500"
                                  }`}
                                >
                                  {e.name}
                                </p>
                                {!selectable && (
                                  <span className="flex-shrink-0 text-[9px] font-semibold uppercase tracking-wide text-amber-800 bg-amber-100 border border-amber-200/80 px-1.5 py-0.5 rounded-md">
                                    Not supported yet
                                  </span>
                                )}
                              </div>
                              <p className="text-[11px] text-slate-400 mt-0.5 leading-tight line-clamp-2">
                                {e.description}
                              </p>
                              {selectable && e.engineKey ? (
                                <p className="text-[10px] font-mono text-indigo-600 mt-1.5">
                                  {e.engineKey}
                                </p>
                              ) : selectable && !e.engineKey ? (
                                <p className="text-[10px] text-slate-400 mt-1.5">preview</p>
                              ) : null}
                            </div>
                          </button>
                        );
                      })}
                    </div>
                  </div>
                );
              })}
            </div>
          ) : (
            <div className="px-6 py-5 space-y-5">
              {selected && (
                <div className="flex items-center gap-3 p-3 rounded-xl bg-slate-50 border border-slate-100">
                  <EngineIcon engine={selected} size={40} />
                  <div>
                    <p className="text-sm font-semibold text-slate-800">{selected.name}</p>
                    {selected.engineKey && (
                      <p className="text-[11px] font-mono text-indigo-600">{selected.engineKey}</p>
                    )}
                  </div>
                </div>
              )}

              {unsupported ? (
                <div className="flex gap-2 rounded-xl border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-900">
                  <AlertCircle className="flex-shrink-0 mt-0.5" size={18} />
                  <div>
                    <p className="font-medium">Not available yet</p>
                    <p className="text-xs text-amber-800/90 mt-1">
                      {!selected?.engineKey
                        ? "This engine has no QueryFlux backend key yet."
                        : !descriptor
                          ? "Unknown engine in registry."
                          : "This engine is listed in the catalog but the adapter is not implemented. Use YAML or the API when support lands."}
                    </p>
                  </div>
                </div>
              ) : (
                <>
                  <div>
                    <label className="block text-[11px] font-semibold text-slate-500 uppercase tracking-wide mb-1.5">
                      Cluster name <span className="text-red-500">*</span>
                    </label>
                    <input
                      type="text"
                      value={clusterName}
                      onChange={(e) => setClusterName(e.target.value)}
                      placeholder="e.g. trino-prod"
                      className="w-full text-sm border border-slate-200 rounded-lg px-3 py-2 font-mono focus:outline-none focus:ring-2 focus:ring-indigo-300 focus:border-indigo-400"
                      autoComplete="off"
                    />
                    <p className="text-[10px] text-slate-400 mt-1">
                      Unique key in config; maps to <code className="font-mono">clusters.&lt;name&gt;</code>
                    </p>
                  </div>

                  <div className="rounded-xl border border-slate-100 bg-slate-50/70 p-4 space-y-4">
                    <p className="text-[10px] font-semibold text-slate-400 uppercase tracking-widest">
                      Routing
                    </p>
                    <div className="flex items-center justify-between gap-4">
                      <div>
                        <p className="text-sm font-medium text-slate-700">
                          Cluster enabled
                        </p>
                        <p className="text-[11px] text-slate-400 mt-0.5">
                          When disabled, the proxy skips this cluster when routing.
                        </p>
                      </div>
                      <button
                        type="button"
                        role="switch"
                        aria-checked={clusterEnabled}
                        aria-label="Cluster enabled"
                        onClick={() => setClusterEnabled((v) => !v)}
                        className={`relative inline-flex h-5 w-9 items-center rounded-full transition-colors focus:outline-none focus-visible:ring-2 focus-visible:ring-indigo-400 focus-visible:ring-offset-2 flex-shrink-0 ${
                          clusterEnabled ? "bg-emerald-500" : "bg-slate-300"
                        }`}
                      >
                        <span
                          className={`inline-block h-3.5 w-3.5 transform rounded-full bg-white shadow transition-transform ${
                            clusterEnabled ? "translate-x-4.5" : "translate-x-0.5"
                          }`}
                        />
                      </button>
                    </div>
                    <div>
                      <label
                        htmlFor="add-cluster-max-running"
                        className="block text-[11px] font-semibold text-slate-500 uppercase tracking-wide mb-1.5"
                      >
                        Max concurrent queries
                      </label>
                      <input
                        id="add-cluster-max-running"
                        type="number"
                        min={1}
                        inputMode="numeric"
                        value={clusterMaxRunning}
                        onChange={(e) => setClusterMaxRunning(e.target.value)}
                        placeholder="∞ group default"
                        className="w-full text-sm border border-slate-200 rounded-lg px-3 py-2 font-mono focus:outline-none focus:ring-2 focus:ring-indigo-300 focus:border-indigo-400"
                      />
                      <p className="text-[10px] text-slate-400 mt-1">
                        Leave empty for ∞ — no per-cluster cap; the cluster group&apos;s limit applies.
                      </p>
                    </div>
                  </div>

                  {selected && descriptor && selected.engineKey ? (
                    <div className="space-y-4">
                      <p className="text-[10px] font-semibold text-slate-400 uppercase tracking-widest">
                        Connection
                      </p>
                      <EngineClusterConfig
                        engineKey={selected.engineKey}
                        descriptor={descriptor}
                        flat={flat}
                        onPatch={(patch) =>
                          setFlat((prev) => ({ ...prev, ...patch }))
                        }
                      />
                    </div>
                  ) : null}
                </>
              )}

              {saveError && (
                <p className="text-xs text-red-600 flex items-start gap-1.5">
                  <AlertCircle size={14} className="flex-shrink-0 mt-0.5" />
                  {saveError}
                </p>
              )}
            </div>
          )}
        </div>

        {/* Footer */}
        <div className="flex items-center justify-end gap-2 px-6 py-4 border-t border-slate-100 bg-slate-50/80 flex-shrink-0">
          {step === 1 ? (
            <>
              <button
                type="button"
                onClick={onClose}
                className="px-4 py-2 rounded-lg text-sm font-medium text-slate-600 hover:bg-slate-100 transition-colors"
              >
                Cancel
              </button>
              <button
                type="button"
                disabled={!canProceedStep1}
                onClick={goNext}
                className="inline-flex items-center gap-1.5 px-4 py-2 rounded-lg text-sm font-medium text-white bg-indigo-600 hover:bg-indigo-700 disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
              >
                Next
                <ChevronRight size={16} />
              </button>
            </>
          ) : (
            <>
              <button
                type="button"
                onClick={goBack}
                disabled={saving}
                className="px-4 py-2 rounded-lg text-sm font-medium text-slate-600 hover:bg-slate-100 transition-colors disabled:opacity-50"
              >
                Back
              </button>
              <button
                type="button"
                onClick={onClose}
                disabled={saving}
                className="px-4 py-2 rounded-lg text-sm font-medium text-slate-600 hover:bg-slate-100 transition-colors disabled:opacity-50"
              >
                Cancel
              </button>
              {!unsupported && (
                <button
                  type="button"
                  onClick={handleSave}
                  disabled={saving || !clusterName.trim()}
                  className="inline-flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-medium text-white bg-indigo-600 hover:bg-indigo-700 disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
                >
                  {saving ? <Loader2 size={16} className="animate-spin" /> : null}
                  {saving ? "Saving…" : "Save cluster"}
                </button>
              )}
            </>
          )}
        </div>
      </div>
    </div>
  );
}
