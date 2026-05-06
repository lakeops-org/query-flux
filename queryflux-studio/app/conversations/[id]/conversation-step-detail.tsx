"use client";

import { useState } from "react";
import { ChevronDown, ChevronUp, Shield } from "lucide-react";
import type { QueryHistoryRecord } from "@/lib/api-types";
import { GuardActionsList } from "@/components/guard-actions-list";

export function ConversationStepDetail({ step }: { step: QueryHistoryRecord }) {
  const [expanded, setExpanded] = useState(false);

  const hasGuardActions = step.guard_actions && step.guard_actions.length > 0;
  if (!hasGuardActions && !step.error_message) return null;

  return (
    <div className="border-t border-slate-100">
      <button
        onClick={() => setExpanded((p) => !p)}
        className="w-full px-5 py-2 flex items-center justify-between text-xs text-slate-400 hover:text-slate-600 hover:bg-slate-50 transition-all"
      >
        <span className="flex items-center gap-1.5">
          {hasGuardActions && <Shield size={11} />}
          {hasGuardActions ? `${step.guard_actions!.length} guard action${step.guard_actions!.length !== 1 ? "s" : ""}` : ""}
          {step.error_message && <span className="text-red-500">Error</span>}
        </span>
        {expanded ? <ChevronUp size={12} /> : <ChevronDown size={12} />}
      </button>

      {expanded && (
        <div className="px-5 pb-4 space-y-3">
          {hasGuardActions && (
            <GuardActionsList actions={step.guard_actions!} />
          )}
          {step.error_message && (
            <pre className="bg-red-50 text-red-700 rounded-xl p-3 text-xs font-mono whitespace-pre-wrap border border-red-100">
              {step.error_message}
            </pre>
          )}
        </div>
      )}
    </div>
  );
}
