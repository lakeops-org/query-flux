/** Query string for `/engines`: time window + optional extra-group filters. */

export type EnginesExtrasFilterState = {
  /** Stale metrics for group names not in Postgres (default: hidden). */
  includeOrphan: boolean;
  /** Proxy-only group names not in Postgres (default: shown). */
  includeLiveOnly: boolean;
};

export function parseEnginesExtrasFilters(params: {
  orphan?: string;
  live?: string;
}): EnginesExtrasFilterState {
  return {
    includeOrphan: params.orphan === "1" || params.orphan === "true",
    includeLiveOnly: params.live !== "0" && params.live !== "false",
  };
}

export function buildEnginesHref(
  hours: number,
  extras: EnginesExtrasFilterState,
): string {
  const p = new URLSearchParams();
  p.set("hours", String(hours));
  if (extras.includeOrphan) p.set("orphan", "1");
  if (!extras.includeLiveOnly) p.set("live", "0");
  const q = p.toString();
  return q ? `/engines?${q}` : "/engines";
}
