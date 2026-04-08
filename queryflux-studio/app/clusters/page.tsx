import { getClusters, listClusterConfigs, listGroupConfigs } from "@/lib/api";
import type { ClusterConfigRecord, ClusterDisplayRow } from "@/lib/api-types";
import { mergeLiveAndPersistedClusters } from "@/lib/merge-clusters-display";
import { ClustersGrid } from "./clusters-grid";
import { ClustersHeaderActions } from "./clusters-header-actions";
import { AlertCircle, CheckCircle2 } from "lucide-react";

export const revalidate = 10;

export default async function ClustersPage() {
  const [live, persisted, groups] = await Promise.all([
    getClusters().catch(() => []),
    listClusterConfigs().catch(() => [] as ClusterConfigRecord[]),
    listGroupConfigs().catch(() => []),
  ]);
  const clusters: ClusterDisplayRow[] = mergeLiveAndPersistedClusters(live, persisted, groups);

  const liveRows = clusters.filter((c) => !c.configPending);
  const totalHealthy = liveRows.filter((c) => c.is_healthy).length;
  const totalUnhealthy = liveRows.length - totalHealthy;
  const unassignedCount = clusters.filter((c) => c.configPending && c.notInAnyGroup).length;
  const staleReloadCount = clusters.filter((c) => c.configPending && !c.notInAnyGroup).length;

  return (
    <div className="p-8 max-w-6xl space-y-6">
      {/* Header */}
      <div className="flex items-start justify-between gap-4">
        <div>
          <h1 className="text-2xl font-bold text-slate-900 tracking-tight">Clusters</h1>
          <p className="text-sm text-slate-500 mt-1">
            Live proxy state plus cluster rows from Postgres. Clusters must be <strong>members of a
            cluster group</strong> to load; otherwise they stay idle regardless of restart.
          </p>
        </div>
        <div className="flex items-center gap-3 flex-shrink-0 flex-wrap justify-end">
          <ClustersHeaderActions />
          {liveRows.length > 0 && (
            <>
              <span className="flex items-center gap-1.5 text-xs font-medium text-emerald-600 bg-emerald-50 px-3 py-1.5 rounded-lg border border-emerald-200">
                <CheckCircle2 size={12} />
                {totalHealthy} healthy
              </span>
              {totalUnhealthy > 0 && (
                <span className="flex items-center gap-1.5 text-xs font-medium text-red-600 bg-red-50 px-3 py-1.5 rounded-lg border border-red-200">
                  <AlertCircle size={12} />
                  {totalUnhealthy} unhealthy
                </span>
              )}
            </>
          )}
          {unassignedCount > 0 && (
            <span className="flex items-center gap-1.5 text-xs font-medium text-slate-700 bg-slate-100 px-3 py-1.5 rounded-lg border border-slate-200">
              <AlertCircle size={12} />
              {unassignedCount} not in any group
            </span>
          )}
          {staleReloadCount > 0 && (
            <span className="flex items-center gap-1.5 text-xs font-medium text-amber-800 bg-amber-50 px-3 py-1.5 rounded-lg border border-amber-200">
              <AlertCircle size={12} />
              {staleReloadCount} not in live state
            </span>
          )}
        </div>
      </div>

      <ClustersGrid clusters={clusters} clusterConfigs={persisted} />
    </div>
  );
}
