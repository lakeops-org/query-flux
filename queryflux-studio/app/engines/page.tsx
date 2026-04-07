import { getClusters, getGroupStats, listClusterConfigs, listGroupConfigs } from "@/lib/api";
import type {
  ClusterConfigRecord,
  ClusterGroupConfigRecord,
  ClusterStateDto,
  GroupStatRow,
} from "@/lib/api-types";
import { GroupsConfigPanel } from "@/components/groups-config-panel";
import { formatDuration } from "@/components/ui-helpers";
import { resolveEngineDefForBadge } from "@/lib/engine-badge-def";
import { EngineIcon } from "@/components/engine-icon";
import {
  Activity,
  AlertCircle,
  CheckCircle2,
  ChevronRight,
  Clock,
  Layers,
  Repeat2,
  Rows3,
  Server,
  Timer,
} from "lucide-react";
import Link from "next/link";
import { EnginesExtrasFilters } from "@/components/engines-extras-filters";
import { buildEnginesHref, parseEnginesExtrasFilters } from "@/lib/engines-url";

export const revalidate = 10;

interface Props {
  searchParams: Promise<{ hours?: string; orphan?: string; live?: string }>;
}

export default async function EnginesPage({ searchParams }: Props) {
  const params = await searchParams;
  const hours = Math.min(168, Math.max(1, parseInt(params.hours ?? "24")));
  const extrasFilter = parseEnginesExtrasFilters(params);

  const [groupStats, clusters, groupConfigs, clusterConfigs] = await Promise.all([
    getGroupStats(hours).catch(() => [] as GroupStatRow[]),
    getClusters().catch(() => [] as ClusterStateDto[]),
    listGroupConfigs().catch(() => [] as ClusterGroupConfigRecord[]),
    listClusterConfigs().catch(() => [] as ClusterConfigRecord[]),
  ]);

  const clusterNames = clusterConfigs.map((c) => c.name).sort((a, b) => a.localeCompare(b));

  const clusterConfigByName = new Map(
    clusterConfigs.map((c) => [c.name, c] as const),
  );

  const membersByGroup = new Map<string, string[]>(
    groupConfigs.map((g) => [g.name, g.members]),
  );

  // Cluster rows per group: use persisted `members` order when Postgres config exists,
  // so the UI matches saved group config even if the proxy was not restarted yet.
  const clustersByGroup = new Map<string, ClusterDisplayRow[]>();
  const allGroupNames = new Set<string>();
  for (const c of clusters) {
    allGroupNames.add(c.group_name);
  }
  for (const g of groupConfigs) {
    allGroupNames.add(g.name);
  }
  for (const s of groupStats) {
    allGroupNames.add(s.cluster_group);
  }
  for (const groupName of allGroupNames) {
    clustersByGroup.set(
      groupName,
      clustersForGroupDisplay(groupName, clusters, membersByGroup.get(groupName)),
    );
  }

  const sortedConfigGroups = [...groupConfigs].sort((a, b) => a.name.localeCompare(b.name));
  const configGroupNames = new Set(sortedConfigGroups.map((g) => g.name));
  const statsByGroup = new Map<string, GroupStatRow>();
  for (const s of groupStats) {
    statsByGroup.set(s.cluster_group, s);
  }
  const statsGroupNames = new Set(groupStats.map((s) => s.cluster_group));

  // Live proxy groups not in Postgres config, with no stats row in this window
  const clusterOnlyGroups = [...clustersByGroup.keys()].filter(
    (g) => !statsGroupNames.has(g) && !configGroupNames.has(g),
  );

  // Stats for names that no longer exist in Postgres (e.g. legacy rows without group id)
  const orphanSeen = new Set<string>();
  const orphanStats = groupStats.filter((s) => {
    if (configGroupNames.has(s.cluster_group)) return false;
    if (orphanSeen.has(s.cluster_group)) return false;
    orphanSeen.add(s.cluster_group);
    return true;
  });

  const hasConfiguredGroupCards = sortedConfigGroups.length > 0;
  const rawExtraOrphanCount = orphanStats.length;
  const rawExtraLiveOnlyCount = clusterOnlyGroups.length;
  const rawHasExtraData = rawExtraOrphanCount > 0 || rawExtraLiveOnlyCount > 0;
  const visibleOrphanStats = extrasFilter.includeOrphan ? orphanStats : [];
  const visibleLiveOnlyGroups = extrasFilter.includeLiveOnly ? clusterOnlyGroups : [];
  const visibleHasExtras =
    visibleOrphanStats.length > 0 || visibleLiveOnlyGroups.length > 0;
  const showFullEmpty = !hasConfiguredGroupCards && !rawHasExtraData;

  const timeLabels: Record<number, string> = {
    1: "last hour",
    6: "last 6 hours",
    24: "last 24 hours",
    48: "last 48 hours",
    168: "last 7 days",
  };
  const timeLabel = timeLabels[hours] ?? `last ${hours}h`;

  return (
    <div className="p-8 max-w-7xl space-y-8">
      {/* Header */}
      <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
        <div>
          <h1 className="text-2xl font-bold text-slate-900 tracking-tight">Groups</h1>
          <p className="text-sm text-slate-500 mt-1">
            Per-group query performance and cluster utilization · {timeLabel}
          </p>
        </div>
        <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:gap-6">
          {/* Time window selector */}
          <div className="flex flex-wrap items-center gap-2">
            <span className="text-xs text-slate-400 font-medium">Window</span>
            {[
              { value: "1", label: "1h" },
              { value: "6", label: "6h" },
              { value: "24", label: "24h" },
              { value: "48", label: "48h" },
              { value: "168", label: "7d" },
            ].map(({ value, label }) => (
              <Link
                key={value}
                href={buildEnginesHref(parseInt(value, 10), extrasFilter)}
                scroll={false}
                className={`px-3 py-1.5 rounded-lg text-xs font-medium transition-all border ${
                  String(hours) === value
                    ? "bg-indigo-600 text-white border-indigo-600 shadow-xs"
                    : "bg-white text-slate-600 border-slate-200 hover:border-indigo-300 hover:text-indigo-600"
                }`}
              >
                {label}
              </Link>
            ))}
          </div>
          <EnginesExtrasFilters
            hours={hours}
            extras={extrasFilter}
            orphanCount={rawExtraOrphanCount}
            liveOnlyCount={rawExtraLiveOnlyCount}
          />
        </div>
      </div>

      <GroupsConfigPanel groups={groupConfigs} clusterNames={clusterNames} />

      {showFullEmpty ? (
        <div className="bg-white rounded-xl border border-slate-200 px-6 py-16 text-center">
          <div className="w-10 h-10 rounded-xl bg-slate-100 flex items-center justify-center mx-auto mb-3">
            <Layers size={18} className="text-slate-400" />
          </div>
          <p className="text-sm font-medium text-slate-500">No group data found</p>
          <p className="text-xs text-slate-400 mt-1">
            Ensure the proxy is running with Postgres persistence enabled
          </p>
        </div>
      ) : (
        <div className="space-y-8">
          {hasConfiguredGroupCards && (
            <section className="space-y-5">
              <h2 className="text-xs font-semibold text-slate-500 uppercase tracking-wider">
                Configured in Studio (Postgres)
              </h2>
              <div className="space-y-5">
                {sortedConfigGroups.map((g) => (
                  <GroupCard
                    key={`cfg-${g.name}`}
                    stats={statsByGroup.get(g.name) ?? null}
                    groupName={g.name}
                    clusters={clustersByGroup.get(g.name) ?? []}
                    clusterConfigByName={clusterConfigByName}
                  />
                ))}
              </div>
            </section>
          )}

          {rawHasExtraData && !visibleHasExtras && (
            <div className="rounded-lg border border-slate-200 bg-slate-50 px-4 py-3 text-xs text-slate-600">
              <span className="font-medium text-slate-700">Extra group cards are hidden.</span>{" "}
              Turn on the filters above
              {rawExtraLiveOnlyCount > 0
                ? ` · ${rawExtraLiveOnlyCount} live-only (proxy-only group name${rawExtraLiveOnlyCount === 1 ? "" : "s"})`
                : ""}
              {rawExtraOrphanCount > 0
                ? ` · ${rawExtraOrphanCount} orphan metric row${rawExtraOrphanCount === 1 ? "" : "s"}`
                : ""}
              .
            </div>
          )}

          {visibleHasExtras && (
            <section className="space-y-3 rounded-xl border border-amber-200/80 bg-amber-50/60 p-4 sm:p-5">
              <div>
                <h2 className="text-xs font-semibold text-amber-800 uppercase tracking-wider">
                  Metrics or live state for other group names
                </h2>
                <p className="text-[11px] text-amber-900/80 mt-1 max-w-3xl">
                  These cards are <strong>not</strong> your current Studio group definitions. They
                  appear when query history still references an old group name, or when the
                  running proxy reports clusters under a group that is not saved in Postgres yet.
                  Clean up old data or restart the proxy after config changes if you expect a single
                  group here.
                </p>
              </div>
              <div className="space-y-5 pt-2">
                {visibleOrphanStats.map((s) => (
                  <GroupCard
                    key={`orphan-${s.cluster_group}`}
                    stats={s}
                    clusters={clustersByGroup.get(s.cluster_group) ?? []}
                    clusterConfigByName={clusterConfigByName}
                  />
                ))}
                {visibleLiveOnlyGroups.map((groupName) => (
                  <GroupCard
                    key={`live-${groupName}`}
                    stats={null}
                    groupName={groupName}
                    clusters={clustersByGroup.get(groupName) ?? []}
                    clusterConfigByName={clusterConfigByName}
                  />
                ))}
              </div>
            </section>
          )}
        </div>
      )}
    </div>
  );
}

/** Live snapshot plus optional flag when row comes from Postgres config but runtime has not picked it up yet. */
type ClusterDisplayRow = ClusterStateDto & { configPending?: boolean };

function clustersForGroupDisplay(
  groupName: string,
  allClusters: ClusterStateDto[],
  persistedMembers: string[] | undefined,
): ClusterDisplayRow[] {
  if (!persistedMembers || persistedMembers.length === 0) {
    return allClusters.filter((c) => c.group_name === groupName).map((c) => ({ ...c }));
  }
  return persistedMembers.map((memberName) => {
    const live = allClusters.find(
      (c) => c.group_name === groupName && c.cluster_name === memberName,
    );
    if (live) {
      return { ...live };
    }
    const anywhere = allClusters.find((c) => c.cluster_name === memberName);
    return {
      group_name: groupName,
      cluster_name: memberName,
      engine_type: anywhere?.engine_type ?? "Unknown",
      endpoint: anywhere?.endpoint ?? null,
      running_queries: 0,
      queued_queries: 0,
      max_running_queries: anywhere?.max_running_queries ?? 0,
      is_healthy: true,
      enabled: true,
      configPending: true,
    };
  });
}

function GroupCard({
  stats,
  clusters,
  groupName,
  clusterConfigByName,
}: {
  stats: GroupStatRow | null;
  clusters: ClusterDisplayRow[];
  groupName?: string;
  clusterConfigByName: Map<string, ClusterConfigRecord>;
}) {
  const name = stats?.cluster_group ?? groupName ?? "Unknown";

  const totalRunning = clusters.reduce((s, c) => s + c.running_queries, 0);
  const totalQueued = clusters.reduce((s, c) => s + c.queued_queries, 0);
  const totalCapacity = clusters.reduce((s, c) => s + c.max_running_queries, 0);
  const utilPct = totalCapacity > 0 ? Math.round((totalRunning / totalCapacity) * 100) : 0;

  const healthyClusters = clusters.filter((c) => c.is_healthy).length;
  const unhealthyClusters = clusters.length - healthyClusters;
  const allHealthy = clusters.length === 0 || unhealthyClusters === 0;
  const allDown = clusters.length > 0 && healthyClusters === 0;

  const barColor = allDown
    ? "bg-slate-300"
    : utilPct > 80 ? "bg-red-400" : utilPct > 50 ? "bg-amber-400" : "bg-emerald-400";

  const errorRatePct =
    stats && stats.total_queries > 0
      ? ((stats.failed_queries / stats.total_queries) * 100).toFixed(1)
      : "0.0";
  const translationRatePct =
    stats && stats.total_queries > 0
      ? ((stats.translated_queries / stats.total_queries) * 100).toFixed(1)
      : "0.0";

  return (
    <div className={`bg-white rounded-xl border overflow-hidden shadow-xs transition-all duration-150 ${
      allDown
        ? "border-red-200"
        : !allHealthy
        ? "border-amber-200 hover:border-amber-300"
        : "border-slate-200 hover:border-indigo-200 hover:shadow-sm"
    }`}>
      {/* Group header */}
      <div className={`flex items-center justify-between px-6 py-4 border-b border-slate-100 bg-gradient-to-r ${
        allDown ? "from-red-50 to-white" : !allHealthy ? "from-amber-50 to-white" : "from-slate-50 to-white"
      }`}>
        <div className="flex items-center gap-3">
          <p className="text-sm font-bold text-slate-800">{name}</p>
          {/* Health status */}
          {clusters.length > 0 && (
            allDown ? (
              <span className="flex items-center gap-1.5 text-[11px] font-semibold text-red-600 bg-red-50 px-2 py-1 rounded-lg border border-red-200">
                <AlertCircle size={11} /> All clusters down
              </span>
            ) : !allHealthy ? (
              <span className="flex items-center gap-1.5 text-[11px] font-semibold text-amber-700 bg-amber-50 px-2 py-1 rounded-lg border border-amber-200">
                <AlertCircle size={11} /> {unhealthyClusters} of {clusters.length} unhealthy
              </span>
            ) : (
              <span className="flex items-center gap-1.5 text-[11px] font-semibold text-emerald-600 bg-emerald-50 px-2 py-1 rounded-lg border border-emerald-200">
                <CheckCircle2 size={11} /> All healthy
              </span>
            )
          )}
          <div className="flex items-center gap-4 text-xs text-slate-500">
            <span className="flex items-center gap-1.5">
              <Server size={12} />
              {clusters.length} cluster{clusters.length !== 1 ? "s" : ""}
            </span>
            {totalQueued > 0 && (
              <span className="flex items-center gap-1.5 text-amber-600 font-medium">
                <span className="w-1.5 h-1.5 rounded-full bg-amber-400" />
                {totalQueued} queued
              </span>
            )}
            {!stats && (
              <span className="text-slate-400 italic text-[11px]">no queries in window</span>
            )}
          </div>
        </div>
        <Link
          href={`/queries?cluster_group=${encodeURIComponent(name)}`}
          className="flex items-center gap-1 text-xs text-indigo-600 hover:text-indigo-700 font-medium transition-colors"
        >
          View queries
          <ChevronRight size={13} />
        </Link>
      </div>

      <div className="px-6 py-5 grid grid-cols-[1fr_auto] gap-8">
        <div className="space-y-5">
          {/* Stats row */}
          {stats ? (
            <div className="grid grid-cols-3 gap-3">
              <StatPill
                icon={<Activity size={13} />}
                label="Total queries"
                value={stats.total_queries.toLocaleString()}
                sub={`${stats.successful_queries.toLocaleString()} succeeded`}
                color="text-indigo-600"
                bg="bg-indigo-50"
              />
              <StatPill
                icon={<AlertCircle size={13} />}
                label="Error rate"
                value={`${errorRatePct}%`}
                sub={`${stats.failed_queries.toLocaleString()} failed`}
                color={stats.failed_queries > 0 ? "text-red-600" : "text-slate-500"}
                bg={stats.failed_queries > 0 ? "bg-red-50" : "bg-slate-50"}
              />
              <StatPill
                icon={<Clock size={13} />}
                label="Avg exec time"
                value={formatDuration(Math.round(stats.avg_execution_ms))}
                sub={`max ${formatDuration(stats.max_execution_ms)}`}
                color="text-amber-600"
                bg="bg-amber-50"
              />
              <StatPill
                icon={<Timer size={13} />}
                label="Avg queue time"
                value={formatDuration(Math.round(stats.avg_queue_ms))}
                sub="time waiting"
                color="text-sky-600"
                bg="bg-sky-50"
              />
              <StatPill
                icon={<Repeat2 size={13} />}
                label="Translation rate"
                value={`${translationRatePct}%`}
                sub={`${stats.translated_queries.toLocaleString()} translated`}
                color="text-violet-600"
                bg="bg-violet-50"
              />
              <StatPill
                icon={<Rows3 size={13} />}
                label="Rows returned"
                value={stats.total_rows_returned.toLocaleString()}
                sub="total rows"
                color="text-emerald-600"
                bg="bg-emerald-50"
              />
            </div>
          ) : (
            <p className="text-sm text-slate-400 italic">
              No query history in the selected time window.
            </p>
          )}

          {/* Overall utilization */}
          {clusters.length > 0 && (
            <div className="space-y-1.5">
              <div className="flex items-center justify-between text-xs">
                <span className="text-slate-500 font-medium">Live utilization</span>
                <span className={`font-semibold ${
                  utilPct > 80 ? "text-red-500" : utilPct > 50 ? "text-amber-500" : "text-emerald-500"
                }`}>
                  {totalRunning} / {totalCapacity} running ({utilPct}%)
                </span>
              </div>
              <div className="h-2 bg-slate-100 rounded-full overflow-hidden">
                <div
                  className={`h-full rounded-full transition-all duration-500 ${barColor}`}
                  style={{ width: `${Math.max(utilPct, 0)}%` }}
                />
              </div>
            </div>
          )}
        </div>

        {/* Cluster list — only clusters belonging to THIS group */}
        {clusters.length > 0 && (
          <div className="min-w-[260px] space-y-2.5 border-l border-slate-100 pl-8">
            <p className="text-[11px] font-semibold text-slate-400 uppercase tracking-widest mb-3">
              Clusters
            </p>
            {clusters.map((c, i) => {
              const clusterUtil =
                c.max_running_queries > 0
                  ? Math.round((c.running_queries / c.max_running_queries) * 100)
                  : 0;
              const clusterBarColor =
                !c.is_healthy ? "bg-slate-300"
                : clusterUtil > 80 ? "bg-red-400"
                : clusterUtil > 50 ? "bg-amber-400"
                : "bg-emerald-400";
              const clusterEngineDef = resolveEngineDefForBadge(
                c.engine_type,
                clusterConfigByName.get(c.cluster_name),
              );
              return (
                <div key={`${c.group_name}-${c.cluster_name}-${i}`} className="space-y-1">
                  <div className="flex items-center justify-between text-xs">
                    <div className="flex items-center gap-1.5">
                      {clusterEngineDef ? (
                        <EngineIcon engine={clusterEngineDef} size={18} />
                      ) : (
                        c.is_healthy ? (
                          <CheckCircle2 size={11} className="text-emerald-400 flex-shrink-0" />
                        ) : (
                          <AlertCircle size={11} className="text-red-400 flex-shrink-0" />
                        )
                      )}
                      {c.is_healthy ? (
                        <CheckCircle2 size={10} className="text-emerald-400 flex-shrink-0" />
                      ) : (
                        <AlertCircle size={10} className="text-red-400 flex-shrink-0" />
                      )}
                      <span className={`font-medium ${c.is_healthy ? "text-slate-700" : "text-red-600"}`}>
                        {c.cluster_name}
                      </span>
                      {c.configPending && (
                        <span
                          className="text-[10px] font-medium text-amber-700 bg-amber-50 px-1.5 py-0.5 rounded-md border border-amber-200"
                          title="Saved in Postgres; restart QueryFlux to attach live metrics for this group"
                        >
                          restart to apply
                        </span>
                      )}
                      {!c.is_healthy && (
                        <span className="text-[10px] font-semibold text-red-500 bg-red-50 px-1.5 py-0.5 rounded-md border border-red-100">
                          unhealthy
                        </span>
                      )}
                    </div>
                    <span className="text-slate-500 tabular-nums">
                      {c.running_queries}/{c.max_running_queries}
                    </span>
                  </div>
                  <div className="h-1 bg-slate-100 rounded-full overflow-hidden">
                    <div
                      className={`h-full rounded-full ${clusterBarColor}`}
                      style={{ width: `${clusterUtil}%` }}
                    />
                  </div>
                </div>
              );
            })}
          </div>
        )}
      </div>
    </div>
  );
}

function StatPill({
  icon,
  label,
  value,
  sub,
  color,
  bg,
}: {
  icon: React.ReactNode;
  label: string;
  value: string;
  sub: string;
  color: string;
  bg: string;
}) {
  return (
    <div className={`rounded-xl p-3 ${bg}`}>
      <div className={`flex items-center gap-1.5 mb-1 ${color}`}>
        {icon}
        <span className="text-[10px] font-semibold uppercase tracking-wide">{label}</span>
      </div>
      <p className={`text-lg font-bold ${color}`}>{value}</p>
      <p className={`text-[10px] mt-0.5 opacity-70 ${color}`}>{sub}</p>
    </div>
  );
}
