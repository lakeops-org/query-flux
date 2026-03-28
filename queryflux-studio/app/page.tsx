import { getClusters, getDashboardStats, getQueries } from "@/lib/api";
import { StatusBadge, EngineBadge, formatDuration, formatTime } from "@/components/ui-helpers";
import { Activity, AlertCircle, AlertTriangle, Clock, Repeat2 } from "lucide-react";

export const revalidate = 10;

export default async function DashboardPage() {
  const [stats, clusters, recent] = await Promise.all([
    getDashboardStats(),
    getClusters(),
    getQueries({ limit: 10 }),
  ]);

  const errorRatePct = (stats.error_rate_last_hour * 100).toFixed(1);
  const translationRatePct = (stats.translation_rate_last_hour * 100).toFixed(1);
  const failedCount = Math.round(stats.error_rate_last_hour * stats.queries_last_hour);
  const translatedCount = Math.round(stats.translation_rate_last_hour * stats.queries_last_hour);

  return (
    <div className="p-8 max-w-7xl space-y-8">
      {/* Header */}
      <div className="flex items-start justify-between">
        <div>
          <h1 className="text-2xl font-bold text-slate-900 tracking-tight">Dashboard</h1>
          <p className="text-sm text-slate-500 mt-1">Last hour · auto-refreshes every 10s</p>
        </div>
        <div className="flex items-center gap-2 text-xs text-slate-500 bg-white border border-slate-200 rounded-lg px-3 py-2 shadow-xs">
          <div className="w-1.5 h-1.5 rounded-full bg-emerald-400 animate-pulse"></div>
          Live
        </div>
      </div>

      {/* Stat cards */}
      <div className="grid grid-cols-4 gap-4">
        <StatCard
          icon={<Activity size={18} />}
          iconBg="bg-indigo-100 text-indigo-600"
          label="Queries"
          value={stats.queries_last_hour.toLocaleString()}
          sub="last hour"
        />
        <StatCard
          icon={<AlertCircle size={18} />}
          iconBg={failedCount > 0 ? "bg-red-100 text-red-600" : "bg-slate-100 text-slate-400"}
          label="Error Rate"
          value={`${errorRatePct}%`}
          sub={`${failedCount} failed`}
          valueColor={failedCount > 0 ? "text-red-600" : undefined}
        />
        <StatCard
          icon={<Clock size={18} />}
          iconBg="bg-amber-100 text-amber-600"
          label="Avg Duration"
          value={formatDuration(Math.round(stats.avg_duration_ms_last_hour))}
          sub="execution time"
        />
        <StatCard
          icon={<Repeat2 size={18} />}
          iconBg="bg-violet-100 text-violet-600"
          label="Translation Rate"
          value={`${translationRatePct}%`}
          sub={`${translatedCount} translated`}
        />
      </div>

      {/* Cluster status */}
      <section>
        <SectionHeader title="Cluster Status" />
        {clusters.length === 0 ? (
          <div className="bg-white rounded-xl border border-slate-200 px-6 py-10 text-center">
            <p className="text-sm text-slate-400">
              No cluster data — ensure the proxy is running with postgres persistence enabled.
            </p>
          </div>
        ) : (
          <div className="grid grid-cols-3 gap-4">
            {clusters.map((c, i) => {
              const utilPct =
                c.max_running_queries > 0
                  ? Math.round((c.running_queries / c.max_running_queries) * 100)
                  : 0;
              const barColor = !c.is_healthy
                ? "bg-slate-300"
                : utilPct > 80 ? "bg-red-400" : utilPct > 50 ? "bg-amber-400" : "bg-emerald-400";
              return (
                <div
                  key={`${c.group_name}-${c.cluster_name}-${i}`}
                  className={`bg-white rounded-xl border p-5 space-y-4 hover:shadow-sm transition-all duration-150 ${
                    c.is_healthy ? "border-slate-200 hover:border-indigo-200" : "border-red-200 bg-red-50/30"
                  }`}
                >
                  <div className="flex items-start justify-between">
                    <div>
                      <div className="flex items-center gap-2">
                        <p className="font-semibold text-sm text-slate-900">{c.cluster_name}</p>
                        {c.is_healthy ? (
                          <span className="flex items-center gap-1 text-[10px] font-semibold text-emerald-600 bg-emerald-50 px-1.5 py-0.5 rounded-md border border-emerald-100">
                            <span className="w-1 h-1 rounded-full bg-emerald-400"></span>
                            healthy
                          </span>
                        ) : (
                          <span className="flex items-center gap-1 text-[10px] font-semibold text-red-600 bg-red-50 px-1.5 py-0.5 rounded-md border border-red-100">
                            <AlertTriangle size={9} />
                            unhealthy
                          </span>
                        )}
                      </div>
                      <p className="text-xs text-slate-400 mt-0.5">{c.group_name}</p>
                    </div>
                    <EngineBadge engine={c.engine_type} />
                  </div>
                  <div className="space-y-1.5">
                    <div className="flex justify-between text-xs">
                      <span className="text-slate-500">{c.running_queries} / {c.max_running_queries} running</span>
                      <span className={`font-semibold ${!c.is_healthy ? "text-slate-400" : utilPct > 80 ? "text-red-500" : utilPct > 50 ? "text-amber-500" : "text-emerald-500"}`}>
                        {utilPct}%
                      </span>
                    </div>
                    <div className="h-2 bg-slate-100 rounded-full overflow-hidden">
                      <div
                        className={`h-full rounded-full transition-all duration-500 ${barColor}`}
                        style={{ width: `${utilPct}%` }}
                      />
                    </div>
                  </div>
                  {c.queued_queries > 0 && (
                    <p className="text-xs text-amber-600 font-medium flex items-center gap-1">
                      <span className="w-1 h-1 rounded-full bg-amber-400 inline-block"></span>
                      {c.queued_queries} queued
                    </p>
                  )}
                </div>
              );
            })}
          </div>
        )}
      </section>

      {/* Recent queries */}
      <section>
        <SectionHeader title="Recent Queries" />
        {recent.length === 0 ? (
          <div className="bg-white rounded-xl border border-slate-200 px-6 py-10 text-center">
            <p className="text-sm text-slate-400">No queries yet.</p>
          </div>
        ) : (
          <div className="bg-white rounded-xl border border-slate-200 overflow-hidden shadow-xs">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-slate-100 bg-gradient-to-r from-slate-50 to-white">
                  {["Time", "SQL", "Engine", "Duration", "Status"].map((h) => (
                    <th key={h} className="text-left px-4 py-3 text-xs font-semibold text-slate-400 uppercase tracking-wider">
                      {h}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {recent.map((q, i) => (
                  <tr
                    key={q.id}
                    className={`hover:bg-indigo-50/50 transition-colors ${i !== recent.length - 1 ? "border-b border-slate-50" : ""}`}
                  >
                    <td className="px-4 py-3 text-xs text-slate-400 whitespace-nowrap font-mono">
                      {formatTime(new Date(q.created_at))}
                    </td>
                    <td className="px-4 py-3 max-w-xs truncate text-slate-700 font-mono text-xs">
                      {q.sql_preview || <span className="text-slate-300 italic">—</span>}
                    </td>
                    <td className="px-4 py-3"><EngineBadge engine={q.engine_type} /></td>
                    <td className="px-4 py-3 text-slate-600 tabular-nums text-xs font-medium">
                      {formatDuration(q.execution_duration_ms)}
                    </td>
                    <td className="px-4 py-3"><StatusBadge status={q.status} /></td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>
    </div>
  );
}

function SectionHeader({ title }: { title: string }) {
  return (
    <div className="flex items-center gap-3 mb-4">
      <h2 className="text-sm font-semibold text-slate-700">{title}</h2>
      <div className="flex-1 h-px bg-slate-100"></div>
    </div>
  );
}

function StatCard({
  icon, iconBg, label, value, sub, valueColor,
}: {
  icon: React.ReactNode;
  iconBg: string;
  label: string;
  value: string;
  sub: string;
  valueColor?: string;
}) {
  return (
    <div className="bg-white rounded-xl border border-slate-200 p-5 flex gap-4 items-start hover:border-indigo-200 hover:shadow-sm transition-all duration-150 shadow-xs">
      <div className={`w-9 h-9 rounded-xl flex items-center justify-center flex-shrink-0 ${iconBg}`}>
        {icon}
      </div>
      <div className="min-w-0">
        <p className="text-xs font-medium text-slate-400 uppercase tracking-wide">{label}</p>
        <p className={`text-2xl font-bold mt-0.5 ${valueColor ?? "text-slate-900"}`}>{value}</p>
        <p className="text-xs text-slate-400 mt-0.5">{sub}</p>
      </div>
    </div>
  );
}
