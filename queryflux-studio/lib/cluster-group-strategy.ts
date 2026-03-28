/**
 * Cluster group routing strategies — mirrors `queryflux_core::config::StrategyConfig`
 * (`#[serde(rename_all = "camelCase", tag = "type")]`).
 *
 * Supported `type` values: `roundRobin`, `leastLoaded`, `failover`, `engineAffinity`, `weighted`.
 * `null` / omitted strategy in Postgres = round robin (same as explicit `roundRobin`).
 */

import { buildEngineAffinityOptionsFromManifest } from "@/lib/studio-engines/manifest";

export type StrategyKind =
  | "default"
  | "roundRobin"
  | "leastLoaded"
  | "failover"
  | "engineAffinity"
  | "weighted";

export const STRATEGY_OPTIONS: {
  value: StrategyKind;
  label: string;
  description: string;
}[] = [
  {
    value: "default",
    label: "Round robin (default)",
    description:
      "Rotate through group members in order. Stored as no strategy (null) — same behavior as explicit round robin.",
  },
  {
    value: "roundRobin",
    label: "Round robin (explicit)",
    description: "Same as default, but stored as {\"type\":\"roundRobin\"}.",
  },
  {
    value: "leastLoaded",
    label: "Least loaded",
    description: "Send each query to the member with the most remaining capacity.",
  },
  {
    value: "failover",
    label: "Failover",
    description:
      "Try members in the order listed under Members; use the next when earlier ones are full or unhealthy.",
  },
  {
    value: "engineAffinity",
    label: "Engine affinity",
    description:
      "For mixed-engine groups: prefer engine types in your order; within each type, pick least loaded.",
  },
  {
    value: "weighted",
    label: "Weighted",
    description:
      "Spread load by relative weights per cluster name (JSON object, e.g. {\"trino-a\": 3, \"trino-b\": 1}).",
  },
];

/** JSON / YAML engine keys accepted in `engineAffinity.preference` (camelCase). */
export const ENGINE_AFFINITY_OPTIONS =
  buildEngineAffinityOptionsFromManifest();

const ENGINE_SET = new Set<string>(ENGINE_AFFINITY_OPTIONS.map((e) => e.value));

export function parseStrategyRecord(
  strategy: Record<string, unknown> | null,
): {
  kind: StrategyKind;
  enginePreferenceCsv: string;
  weightedJson: string;
} {
  if (!strategy || typeof strategy !== "object") {
    return { kind: "default", enginePreferenceCsv: "", weightedJson: "{}" };
  }
  const t = strategy.type;
  if (t === "roundRobin") {
    return { kind: "roundRobin", enginePreferenceCsv: "", weightedJson: "{}" };
  }
  if (t === "leastLoaded") {
    return { kind: "leastLoaded", enginePreferenceCsv: "", weightedJson: "{}" };
  }
  if (t === "failover") {
    return { kind: "failover", enginePreferenceCsv: "", weightedJson: "{}" };
  }
  if (t === "engineAffinity") {
    const pref = strategy.preference;
    const csv = Array.isArray(pref)
      ? pref.map((x) => String(x).trim()).filter(Boolean).join(", ")
      : "";
    return { kind: "engineAffinity", enginePreferenceCsv: csv, weightedJson: "{}" };
  }
  if (t === "weighted") {
    const w = strategy.weights;
    if (w && typeof w === "object" && !Array.isArray(w)) {
      return {
        kind: "weighted",
        enginePreferenceCsv: "",
        weightedJson: JSON.stringify(w, null, 2),
      };
    }
    return { kind: "weighted", enginePreferenceCsv: "", weightedJson: "{}" };
  }
  return { kind: "default", enginePreferenceCsv: "", weightedJson: "{}" };
}

function parseEnginePreferenceCsv(csv: string): string[] {
  return csv
    .split(",")
    .map((s) => s.trim())
    .filter((s) => s.length > 0);
}

export function buildStrategyPayload(
  kind: StrategyKind,
  enginePreferenceCsv: string,
  weightedJson: string,
): Record<string, unknown> | null {
  switch (kind) {
    case "default":
      return null;
    case "roundRobin":
      return { type: "roundRobin" };
    case "leastLoaded":
      return { type: "leastLoaded" };
    case "failover":
      return { type: "failover" };
    case "engineAffinity": {
      const raw = parseEnginePreferenceCsv(enginePreferenceCsv);
      const preference: string[] = [];
      for (const p of raw) {
        if (!ENGINE_SET.has(p)) {
          throw new Error(
            `Unknown engine "${p}". Use: ${[...ENGINE_SET].join(", ")}`,
          );
        }
        preference.push(p);
      }
      if (preference.length === 0) {
        throw new Error(
          "Engine affinity requires at least one engine in preference order (comma-separated).",
        );
      }
      return { type: "engineAffinity", preference };
    }
    case "weighted": {
      const trimmed = weightedJson.trim();
      if (trimmed === "" || trimmed === "{}") {
        return { type: "weighted", weights: {} };
      }
      let parsed: unknown;
      try {
        parsed = JSON.parse(trimmed) as unknown;
      } catch {
        throw new Error("Weighted strategy: invalid JSON for weights.");
      }
      if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) {
        throw new Error(
          'Weighted strategy: weights must be a JSON object, e.g. {"cluster-a": 3, "cluster-b": 1}.',
        );
      }
      const weights: Record<string, number> = {};
      for (const [k, v] of Object.entries(parsed as Record<string, unknown>)) {
        const n =
          typeof v === "number" && Number.isFinite(v)
            ? v
            : parseInt(String(v), 10);
        if (!Number.isFinite(n) || n < 0) {
          throw new Error(`Weighted strategy: invalid weight for "${k}" (use non-negative integers).`);
        }
        weights[k] = Math.floor(n);
      }
      return { type: "weighted", weights };
    }
    default:
      return null;
  }
}

/** Short label for tables / summaries. */
export function formatStrategySummary(strategy: Record<string, unknown> | null): string {
  const { kind, enginePreferenceCsv, weightedJson } = parseStrategyRecord(strategy);
  const base = STRATEGY_OPTIONS.find((o) => o.value === kind)?.label ?? "Round robin";
  if (kind === "engineAffinity" && enginePreferenceCsv.trim()) {
    return `${base} (${enginePreferenceCsv.replace(/\s*,\s*/g, " → ")})`;
  }
  if (kind === "weighted" && weightedJson.trim() && weightedJson.trim() !== "{}") {
    return `${base}`;
  }
  return base;
}
