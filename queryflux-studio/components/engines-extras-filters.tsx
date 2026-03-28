"use client";

import Link from "next/link";
import { buildEnginesHref, type EnginesExtrasFilterState } from "@/lib/engines-url";

type Props = {
  hours: number;
  extras: EnginesExtrasFilterState;
  orphanCount: number;
  liveOnlyCount: number;
};

/**
 * Toggles which “extra” group cards appear (orphan metrics vs live-only proxy groups).
 * Defaults: live-only on, orphan off — URL omits params when at defaults.
 */
export function EnginesExtrasFilters({
  hours,
  extras,
  orphanCount,
  liveOnlyCount,
}: Props) {
  const { includeOrphan, includeLiveOnly } = extras;
  const nextOrphan: EnginesExtrasFilterState = {
    includeOrphan: !includeOrphan,
    includeLiveOnly,
  };
  const nextLive: EnginesExtrasFilterState = {
    includeOrphan,
    includeLiveOnly: !includeLiveOnly,
  };

  return (
    <div className="flex flex-col gap-2 sm:items-end">
      <span className="text-[10px] font-semibold text-slate-400 uppercase tracking-wide">
        Extra group cards
      </span>
      <div className="flex flex-wrap items-center justify-end gap-2">
        <Link
          href={buildEnginesHref(hours, nextLive)}
          scroll={false}
          className={`px-2.5 py-1 rounded-md text-[11px] font-medium border transition-colors ${
            includeLiveOnly
              ? "bg-indigo-50 text-indigo-800 border-indigo-200"
              : "bg-white text-slate-500 border-slate-200 hover:border-slate-300"
          }`}
          title="Groups seen on the running proxy but not in Postgres Studio config"
        >
          Live-only
          {liveOnlyCount > 0 ? ` (${liveOnlyCount})` : ""}
        </Link>
        <Link
          href={buildEnginesHref(hours, nextOrphan)}
          scroll={false}
          className={`px-2.5 py-1 rounded-md text-[11px] font-medium border transition-colors ${
            includeOrphan
              ? "bg-indigo-50 text-indigo-800 border-indigo-200"
              : "bg-white text-slate-500 border-slate-200 hover:border-slate-300"
          }`}
          title="Query stats for group names no longer in Postgres"
        >
          Orphan metrics
          {orphanCount > 0 ? ` (${orphanCount})` : ""}
        </Link>
      </div>
    </div>
  );
}
