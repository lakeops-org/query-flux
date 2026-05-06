"use client";

import { useEffect, useState } from "react";
import { X, Bot, Hash, Clock, MessageSquare } from "lucide-react";
import type { AgentSummary, ConversationSummary } from "@/lib/api-types";
import { getConversations } from "@/lib/api";
import { formatDateTime } from "@/components/ui-helpers";
import Link from "next/link";

export function AgentDetail({
  agent,
  onClose,
}: {
  agent: AgentSummary;
  onClose: () => void;
}) {
  const [conversations, setConversations] = useState<ConversationSummary[] | null>(null);

  useEffect(() => {
    getConversations({ agent_id: agent.agent_id, limit: 50 })
      .then(setConversations)
      .catch(() => setConversations([]));
  }, [agent.agent_id]);

  return (
    <div className="fixed inset-0 z-50 flex">
      <div className="flex-1 bg-slate-900/40 backdrop-blur-sm" onClick={onClose} />

      <div className="w-[560px] bg-white shadow-2xl flex flex-col overflow-hidden border-l border-slate-200">
        {/* Header */}
        <div className="flex items-center justify-between px-6 py-4 border-b border-slate-100 bg-gradient-to-r from-white to-slate-50">
          <div className="flex items-center gap-2.5">
            <span className="inline-flex items-center gap-1.5 px-2.5 py-0.5 rounded-full bg-indigo-50 text-indigo-700 ring-1 ring-indigo-200 text-xs font-medium">
              <Bot size={11} />
              Agent
            </span>
            <span className="font-mono text-sm font-semibold text-slate-800">{agent.agent_id}</span>
          </div>
          <button
            onClick={onClose}
            className="w-7 h-7 rounded-lg flex items-center justify-center text-slate-400 hover:text-slate-600 hover:bg-slate-100 transition-all"
          >
            <X size={16} />
          </button>
        </div>

        {/* Body */}
        <div className="flex-1 overflow-y-auto">
          {/* Stats */}
          <div className="px-6 py-5 grid grid-cols-2 gap-4 border-b border-slate-100">
            <MetaItem icon={<Hash size={13} />} label="Total Queries" value={agent.query_count.toLocaleString()} />
            <MetaItem icon={<Clock size={13} />} label="Last Seen" value={formatDateTime(new Date(agent.last_seen))} />
          </div>

          {/* Conversations */}
          <div className="px-6 py-5">
            <p className="text-[11px] font-semibold uppercase tracking-widest mb-3 text-slate-400 flex items-center gap-1.5">
              <MessageSquare size={11} />
              Conversations
            </p>
            {conversations == null ? (
              <p className="text-xs text-slate-400 italic">Loading…</p>
            ) : conversations.length === 0 ? (
              <p className="text-xs text-slate-400 italic">No conversations recorded</p>
            ) : (
              <div className="space-y-2">
                {conversations.map((c) => (
                  <Link
                    key={c.conversation_id}
                    href={`/conversations/${encodeURIComponent(c.conversation_id)}`}
                    className="block rounded-xl border border-slate-100 p-3 hover:border-indigo-200 hover:bg-indigo-50/40 transition-all"
                  >
                    <div className="flex items-start justify-between gap-4">
                      <div className="min-w-0">
                        <p className="font-mono text-xs font-semibold text-slate-800 truncate">
                          {c.conversation_id}
                        </p>
                        <p className="text-[11px] text-slate-400 mt-0.5">
                          {c.step_count} step{c.step_count !== 1 ? "s" : ""} ·{" "}
                          {formatDateTime(new Date(c.first_seen))}
                        </p>
                      </div>
                      <span className="text-[10px] text-slate-400 font-mono whitespace-nowrap mt-0.5">
                        {formatDateTime(new Date(c.last_seen))}
                      </span>
                    </div>
                  </Link>
                ))}
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}

function MetaItem({ icon, label, value }: { icon: React.ReactNode; label: string; value: string }) {
  return (
    <div>
      <div className="flex items-center gap-1.5 text-slate-400 mb-0.5">
        {icon}
        <p className="text-[11px] font-medium uppercase tracking-wide">{label}</p>
      </div>
      <p className="text-sm text-slate-800 font-mono truncate">{value}</p>
    </div>
  );
}
