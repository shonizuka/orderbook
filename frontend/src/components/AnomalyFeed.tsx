"use client";

import { useCallback, useState } from "react";
import { useSSE } from "@/hooks/useSSE";

const API_URL = process.env.NEXT_PUBLIC_API_URL ?? "http://localhost:8000";
const MAX_ITEMS = 30;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

type AnomalyType =
  | "obi_spike"
  | "spread_widening"
  | "exchange_divergence"
  | "spoofing"
  | "wall"
  | "sweep";

interface AnomalyEvent {
  type: AnomalyType;
  timestamp: number;
  details: Record<string, unknown>;
}

interface MetricsMessage {
  timestamp: number;
  anomalies: AnomalyEvent[];
}

interface DisplayAnomaly extends AnomalyEvent {
  id: string;
}

// ---------------------------------------------------------------------------
// Badge styles per anomaly type
// ---------------------------------------------------------------------------

const BADGE: Record<AnomalyType, { label: string; className: string }> = {
  obi_spike: {
    label: "OBI SPIKE",
    className: "bg-violet-900/60 text-violet-300 border border-violet-700",
  },
  spread_widening: {
    label: "SPREAD",
    className: "bg-yellow-900/60 text-yellow-300 border border-yellow-700",
  },
  exchange_divergence: {
    label: "DIVERGENCE",
    className: "bg-orange-900/60 text-orange-300 border border-orange-700",
  },
  spoofing: {
    label: "SPOOF",
    className: "bg-red-900/60 text-red-300 border border-red-700",
  },
  wall: {
    label: "WALL",
    className: "bg-blue-900/60 text-blue-300 border border-blue-700",
  },
  sweep: {
    label: "SWEEP",
    className: "bg-emerald-900/60 text-emerald-300 border border-emerald-700",
  },
};

// ---------------------------------------------------------------------------
// Detail formatter
// ---------------------------------------------------------------------------

function formatDetails(type: AnomalyType, details: Record<string, unknown>): string {
  switch (type) {
    case "obi_spike":
      return `Δ ${Number(details.delta).toFixed(3)}  (${Number(details.previous_obi).toFixed(3)} → ${Number(details.current_obi).toFixed(3)})`;
    case "spread_widening":
      return `${Number(details.spread_bps).toFixed(1)} bps  bid=${Number(details.best_bid).toFixed(2)}`;
    case "exchange_divergence":
      return `$${Number(details.spread_usd).toFixed(2)} spread across exchanges`;
    case "spoofing":
      return `${details.exchange}  ${details.side}  $${details.price}  ${Number(details.quantity).toFixed(2)} BTC  ${Number(details.elapsed_ms).toFixed(0)}ms`;
    case "wall":
      return `${details.side}  $${details.price}  ${Number(details.quantity).toFixed(2)} BTC  (${details.multiplier}× avg)`;
    case "sweep":
      return `${details.side}  $${Number(details.from_price).toFixed(2)} → $${Number(details.to_price).toFixed(2)}  ${details.levels_consumed} levels`;
    default:
      return JSON.stringify(details);
  }
}

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------

export function AnomalyFeed() {
  const [anomalies, setAnomalies] = useState<DisplayAnomaly[]>([]);

  const handleMessage = useCallback((msg: MetricsMessage) => {
    if (!msg.anomalies?.length) return;

    const incoming: DisplayAnomaly[] = msg.anomalies.map((a) => ({
      ...a,
      id: `${a.type}-${a.timestamp}-${Math.random()}`,
    }));

    setAnomalies((prev) => [...incoming, ...prev].slice(0, MAX_ITEMS));
  }, []);

  useSSE<MetricsMessage>(
    `${API_URL}/api/ob/stream/metrics`,
    handleMessage
  );

  if (anomalies.length === 0) {
    return (
      <div className="text-zinc-500 text-xs py-4 text-center">
        No anomalies detected yet…
      </div>
    );
  }

  return (
    <div className="space-y-1 font-mono text-xs max-h-80 overflow-y-auto pr-1">
      {anomalies.map((a) => {
        const badge = BADGE[a.type] ?? { label: a.type.toUpperCase(), className: "bg-zinc-800 text-zinc-300" };
        return (
          <div
            key={a.id}
            className="flex items-start gap-2 py-1 border-b border-zinc-800/50"
          >
            <span
              className={`shrink-0 rounded px-1.5 py-0.5 text-[10px] font-bold tracking-wide ${badge.className}`}
            >
              {badge.label}
            </span>
            <span className="text-zinc-300 leading-tight">
              {formatDetails(a.type, a.details)}
            </span>
            <span className="ml-auto shrink-0 text-zinc-600">
              {new Date(a.timestamp * 1000).toLocaleTimeString()}
            </span>
          </div>
        );
      })}
    </div>
  );
}
