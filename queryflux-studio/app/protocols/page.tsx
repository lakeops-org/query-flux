import { getFrontendsStatus } from "@/lib/api";
import type { ProtocolFrontendDto } from "@/lib/api-types";
import { ProtocolListenerIconTile } from "@/components/protocol-listener-icon";
import { AlertCircle } from "lucide-react";

export const revalidate = 10;

export default async function ProtocolsPage() {
  let status: Awaited<ReturnType<typeof getFrontendsStatus>> | null = null;
  let error: string | null = null;
  try {
    status = await getFrontendsStatus();
  } catch (e) {
    error = e instanceof Error ? e.message : String(e);
  }

  return (
    <div className="p-8 max-w-6xl space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-slate-900 tracking-tight">Protocols</h1>
        <p className="text-sm text-slate-500 mt-1 max-w-2xl">
          Which client protocol frontends this QueryFlux process exposes. Values reflect the config
          loaded at <strong>startup</strong> (they are not hot-reloaded when YAML changes).
        </p>
      </div>

      {error && (
        <div className="flex items-start gap-3 rounded-xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-800">
          <AlertCircle className="w-5 h-5 flex-shrink-0 mt-0.5" />
          <div>
            <p className="font-semibold">Could not load frontends</p>
            <p className="text-red-700/90 mt-1 font-mono text-xs break-all">{error}</p>
          </div>
        </div>
      )}

      {status && (
        <>
          <div className="flex flex-wrap gap-3 text-xs text-slate-600 bg-white border border-slate-200 rounded-xl px-4 py-3 shadow-xs">
            <span>
              <span className="text-slate-400">Admin API</span>{" "}
              <span className="font-mono font-medium text-slate-800">:{status.admin_api_port}</span>
            </span>
            {status.external_address && (
              <span>
                <span className="text-slate-400">External address</span>{" "}
                <span className="font-mono font-medium text-slate-800">{status.external_address}</span>
              </span>
            )}
          </div>

          <div className="grid gap-4 sm:grid-cols-2">
            {status.protocols.map((p) => (
              <ProtocolCard key={p.id} protocol={p} />
            ))}
          </div>
        </>
      )}
    </div>
  );
}

function ProtocolCard({ protocol }: { protocol: ProtocolFrontendDto }) {
  const on = protocol.enabled;
  return (
    <div
      className={`rounded-xl border bg-white p-5 shadow-xs transition-shadow ${
        on ? "border-slate-200" : "border-slate-100 opacity-90"
      }`}
    >
      <div className="flex items-start gap-4">
        <ProtocolListenerIconTile protocolId={protocol.id} enabled={on} />
        <div className="min-w-0 flex-1 space-y-1">
          <div className="flex flex-wrap items-center gap-2">
            <h2 className="font-semibold text-slate-900">{protocol.label}</h2>
            <span
              className={`inline-flex items-center rounded-full px-2 py-0.5 text-[11px] font-semibold uppercase tracking-wide ${
                on
                  ? "bg-emerald-50 text-emerald-700 border border-emerald-200"
                  : "bg-slate-100 text-slate-500 border border-slate-200"
              }`}
            >
              {on ? "On" : "Off"}
            </span>
          </div>
          <p className="text-xs text-slate-500 leading-relaxed">{protocol.short_description}</p>
          <p className="text-xs font-mono text-slate-600 pt-1">
            {protocol.port != null ? (
              <>
                <span className="text-slate-400">port</span> {protocol.port}
              </>
            ) : (
              <span className="text-slate-400">not configured in YAML</span>
            )}
          </p>
        </div>
      </div>
    </div>
  );
}
