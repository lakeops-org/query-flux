import * as si from "simple-icons";
import type { SimpleIcon } from "simple-icons";
import { Plane, Radio, type LucideIcon } from "lucide-react";

/**
 * Admin `GET /admin/frontends` `protocol.id` → brand mark from
 * [Simple Icons](https://simpleicons.org/) (same data as `EngineIcon`).
 */
const PROTOCOL_SIMPLE_ICONS: Record<string, SimpleIcon> = {
  trino_http: si.siTrino,
  postgres_wire: si.siPostgresql,
  mysql_wire: si.siMysql,
  clickhouse_http: si.siClickhouse,
  snowflake_http: si.siSnowflake,
};

/**
 * Arrow Flight SQL — this `simple-icons` release has no Apache Arrow / Flight mark;
 * keep a neutral Lucide glyph with the same tile chrome as before.
 */
const PROTOCOL_LUCIDE: Record<string, LucideIcon> = {
  flight_sql: Plane,
};

function luminanceIsLight(hex: string): boolean {
  const r = parseInt(hex.slice(0, 2), 16);
  const g = parseInt(hex.slice(2, 4), 16);
  const b = parseInt(hex.slice(4, 6), 16);
  const luminance = (0.299 * r + 0.587 * g + 0.114 * b) / 255;
  return luminance > 0.65;
}

/** 48×48 listener tile: Simple Icons brand tiles match `EngineIcon` background rules. */
export function ProtocolListenerIconTile({
  protocolId,
  enabled,
}: {
  protocolId: string;
  enabled: boolean;
}) {
  const brand = PROTOCOL_SIMPLE_ICONS[protocolId];
  if (brand) {
    const hex = brand.hex;
    const light = luminanceIsLight(hex);
    const backgroundColor = enabled ? (light ? `#${hex}` : `#${hex}18`) : "#f1f5f9";
    const fill = enabled ? (light ? "#1e293b" : `#${hex}`) : "#94a3b8";
    return (
      <div
        className="flex h-12 w-12 flex-shrink-0 items-center justify-center rounded-xl border border-slate-200/70"
        style={{ backgroundColor }}
      >
        <svg
          role="img"
          viewBox="0 0 24 24"
          className="h-6 w-6"
          aria-label={brand.title}
          fill={fill}
        >
          <path d={brand.path} />
        </svg>
      </div>
    );
  }

  const Lucide = PROTOCOL_LUCIDE[protocolId] ?? Radio;
  return (
    <div
      className={`flex h-12 w-12 flex-shrink-0 items-center justify-center rounded-xl border border-slate-200/60 ${
        enabled ? "bg-indigo-100 text-indigo-600" : "bg-slate-100 text-slate-400"
      }`}
    >
      <Lucide className="h-6 w-6" strokeWidth={1.75} />
    </div>
  );
}
