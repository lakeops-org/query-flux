import type { ClusterConfigRecord } from "@/lib/api-types";
import { readClusterEngineKey } from "@/lib/cluster-config-helpers";
import {
  ENGINE_CATALOG,
  findEngineByType,
  type EngineDef,
} from "@/components/engine-catalog";

const normalize = (s: string) => s.toLowerCase().replace(/[\s-_]/g, "");

function isLikelySharedLibraryPath(s: string): boolean {
  return s.includes("/") || s.includes("\\");
}

function readConfigString(
  cfg: Record<string, unknown>,
  camel: string,
  snake: string,
): string {
  const a = cfg[camel];
  if (typeof a === "string") return a.trim();
  const b = cfg[snake];
  if (typeof b === "string") return b.trim();
  return "";
}

/**
 * Resolves catalog metadata (logo, label) for a cluster badge.
 * ADBC clusters report a generic engine label from the proxy; use persisted
 * `driver` and optional `flightSqlEngine` to pick the same engine card as Studio.
 */
export function resolveEngineDefForBadge(
  engineTypeLabel: string,
  clusterConfig?: ClusterConfigRecord | null,
): EngineDef | undefined {
  const ek = clusterConfig ? readClusterEngineKey(clusterConfig) : "";
  const labelMatchesAdbc = normalize(engineTypeLabel) === "adbc";
  /** API may omit `engineKey` unless JSON was normalized; live label is often `Adbc`. */
  const treatAsAdbc =
    ek === "adbc" || (labelMatchesAdbc && !!clusterConfig);

  if (clusterConfig && treatAsAdbc) {
    const cfg = (clusterConfig.config ?? {}) as Record<string, unknown>;
    const driverRaw = readConfigString(cfg, "driver", "driver");
    const driver = driverRaw.toLowerCase();
    const flightRaw = readConfigString(
      cfg,
      "flightSqlEngine",
      "flight_sql_engine",
    );
    const flightSqlEngine = flightRaw.toLowerCase();

    if (driver === "flightsql" && flightSqlEngine) {
      const byFlight = findEngineByType(flightSqlEngine);
      if (byFlight) return byFlight;
    }

    if (driver && !isLikelySharedLibraryPath(driverRaw)) {
      const byDriver = findEngineByType(driver);
      if (byDriver) return byDriver;
    }

    return ENGINE_CATALOG.find((e) => e.engineKey === "adbc");
  }

  const byName = ENGINE_CATALOG.find(
    (e) => normalize(e.name) === normalize(engineTypeLabel),
  );
  if (byName) return byName;

  return findEngineByType(engineTypeLabel);
}
