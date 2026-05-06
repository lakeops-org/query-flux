import { getConversationDetail } from "@/lib/api";
import { StatusBadge, EngineBadge, formatDateTime, formatDuration } from "@/components/ui-helpers";
import { GuardSummaryBadge } from "@/components/guard-actions-list";
import { Bot, ArrowLeft } from "lucide-react";
import Link from "next/link";
import { ConversationStepDetail } from "./conversation-step-detail";

export const revalidate = 0;

export default async function ConversationDetailPage({
  params,
}: {
  params: Promise<{ id: string }>;
}) {
  const { id } = await params;
  const steps = await getConversationDetail(decodeURIComponent(id)).catch(() => []);

  const agentId = steps[0]?.agent_id ?? "—";

  return (
    <div className="box-border flex w-full justify-center px-6 py-8 sm:px-8">
      <div className="w-[min(100%,64rem)] shrink-0 grow-0 space-y-6">
      {/* Header */}
      <div>
        <Link
          href="/conversations"
          className="inline-flex items-center gap-1.5 text-xs text-slate-400 hover:text-slate-600 mb-4 transition-colors"
        >
          <ArrowLeft size={12} />
          Conversations
        </Link>
        <h1 className="text-2xl font-bold text-slate-900 tracking-tight font-mono break-all">{decodeURIComponent(id)}</h1>
        <div className="flex items-center gap-2 mt-1.5">
          <span className="inline-flex items-center gap-1 text-xs text-indigo-700 bg-indigo-50 px-2 py-0.5 rounded-md font-mono">
            <Bot size={10} />
            {agentId}
          </span>
          <span className="text-xs text-slate-400">{steps.length} step{steps.length !== 1 ? "s" : ""}</span>
        </div>
      </div>

      {/* Steps timeline */}
      {steps.length === 0 ? (
        <div className="bg-white rounded-xl border border-slate-200 px-6 py-16 text-center">
          <p className="text-sm text-slate-400">No steps found for this conversation.</p>
        </div>
      ) : (
        <div className="space-y-3">
          {steps.map((step, i) => (
            <div key={step.proxy_query_id} className="bg-white rounded-xl border border-slate-200 shadow-xs overflow-hidden">
              {/* Step header */}
              <div className="px-5 py-3 border-b border-slate-100 bg-gradient-to-r from-slate-50 to-white flex items-center justify-between gap-4">
                <div className="flex items-center gap-3">
                  <span className="w-6 h-6 rounded-full bg-indigo-100 text-indigo-700 text-xs font-bold flex items-center justify-center flex-shrink-0">
                    {step.step_index ?? i + 1}
                  </span>
                  <StatusBadge status={step.status} />
                  <EngineBadge engine={step.engine_type} />
                  <GuardSummaryBadge actions={step.guard_actions} wasBlocked={step.was_guard_blocked} />
                </div>
                <div className="flex items-center gap-3 text-xs text-slate-400">
                  <span>{formatDuration(step.execution_duration_ms)}</span>
                  <span>{formatDateTime(new Date(step.created_at))}</span>
                </div>
              </div>

              {/* SQL preview */}
              <div className="px-5 py-3">
                <pre className="text-xs font-mono text-slate-700 truncate">
                  {step.sql_preview}
                </pre>
              </div>

              <ConversationStepDetail step={step} />
            </div>
          ))}
        </div>
      )}
      </div>
    </div>
  );
}
