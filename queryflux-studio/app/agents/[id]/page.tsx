import { getConversations } from "@/lib/api";
import { ConversationList } from "./conversation-list";
import { ArrowLeft, Bot, MessageSquare, Layers, ShieldX, Clock, Divide } from "lucide-react";
import Link from "next/link";
import { formatDateTime } from "@/components/ui-helpers";

export const revalidate = 0;

export default async function AgentPage({ params }: { params: Promise<{ id: string }> }) {
  const { id } = await params;
  const agentId = decodeURIComponent(id);
  const conversations = await getConversations({ agent_id: agentId, limit: 200 }).catch(() => []);

  const totalSteps = conversations.reduce((acc, c) => acc + c.step_count, 0);
  const blockedConvs = conversations.filter((c) => c.has_blocked).length;
  const lastSeen =
    conversations.length > 0
      ? conversations.reduce(
          (max, c) => (c.last_seen > max ? c.last_seen : max),
          conversations[0].last_seen,
        )
      : null;
  const firstSeen =
    conversations.length > 0
      ? conversations.reduce(
          (min, c) => (c.first_seen < min ? c.first_seen : min),
          conversations[0].first_seen,
        )
      : null;

  const blockedPct =
    conversations.length > 0
      ? Math.round((blockedConvs / conversations.length) * 100)
      : 0;

  const avgStepsPerConv =
    conversations.length > 0 ? totalSteps / conversations.length : 0;
  const avgStepsLabel =
    conversations.length > 0
      ? avgStepsPerConv % 1 === 0
        ? String(avgStepsPerConv)
        : avgStepsPerConv.toFixed(1)
      : "—";

  return (
    <div className="p-6 sm:p-8 space-y-6 max-w-7xl w-full mx-auto">
      {/* Header */}
      <div>
        <Link
          href="/agents"
          className="inline-flex items-center gap-1.5 text-xs text-slate-400 hover:text-slate-600 mb-4 transition-colors"
        >
          <ArrowLeft size={12} />
          Agents
        </Link>
        <div className="flex items-center gap-2.5 flex-wrap">
          <span className="inline-flex items-center gap-1.5 px-2.5 py-0.5 rounded-full bg-indigo-50 text-indigo-700 ring-1 ring-indigo-200 text-xs font-medium">
            <Bot size={11} />
            Agent
          </span>
          <h1 className="text-2xl font-bold text-slate-900 tracking-tight font-mono break-all">
            {agentId}
          </h1>
        </div>
        {firstSeen && lastSeen && (
          <p className="text-xs text-slate-400 mt-1.5">
            Active {formatDateTime(new Date(firstSeen))} → {formatDateTime(new Date(lastSeen))}
          </p>
        )}
      </div>

      {/* Stats — equal-width grid so metrics align with conversation list below */}
      <div className="grid grid-cols-2 lg:grid-cols-5 gap-3">
        <StatItem
          icon={<MessageSquare size={13} className="text-indigo-400" />}
          label="Conversations"
          value={String(conversations.length)}
        />
        <StatItem icon={<Layers size={13} className="text-slate-400" />} label="Total steps" value={String(totalSteps)} />
        <StatItem
          icon={<Divide size={13} className="text-violet-500" />}
          label="Avg steps / conv"
          value={avgStepsLabel}
          sub={conversations.length > 0 ? `${totalSteps} ÷ ${conversations.length}` : undefined}
        />
        <StatItem
          icon={<ShieldX size={13} className={blockedConvs > 0 ? "text-red-400" : "text-slate-300"} />}
          label="Blocked convs"
          value={String(blockedConvs)}
          sub={conversations.length > 0 ? `${blockedPct}% of total` : undefined}
          accent={blockedConvs > 0}
        />
        <StatItem
          icon={<Clock size={13} className="text-slate-400" />}
          label="Last active"
          value={lastSeen ? formatDateTime(new Date(lastSeen)) : "—"}
          className="col-span-2 lg:col-span-1"
          valueClassName="text-base font-mono"
        />
      </div>

      <ConversationList conversations={conversations} />
    </div>
  );
}

function StatItem({
  icon,
  label,
  value,
  sub,
  accent,
  className,
  valueClassName,
}: {
  icon: React.ReactNode;
  label: string;
  value: string;
  sub?: string;
  accent?: boolean;
  className?: string;
  valueClassName?: string;
}) {
  return (
    <div
      className={`flex items-start gap-3 rounded-xl border border-slate-200 bg-white px-4 py-3.5 shadow-xs min-h-[5.25rem] min-w-0 ${className ?? ""}`}
    >
      <div className="shrink-0 mt-0.5">{icon}</div>
      <div className="min-w-0 flex-1">
        <p className="text-[10px] font-semibold text-slate-400 uppercase tracking-wider mb-1 leading-tight">
          {label}
        </p>
        <p
          className={`text-xl font-bold tabular-nums tracking-tight leading-tight truncate ${
            accent ? "text-red-600" : "text-slate-900"
          } ${valueClassName ?? ""}`}
        >
          {value}
        </p>
        {sub && <p className="text-[10px] text-slate-400 mt-1 leading-snug tabular-nums">{sub}</p>}
      </div>
    </div>
  );
}
