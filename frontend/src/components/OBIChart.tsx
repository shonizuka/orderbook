"use client";

import { useCallback, useState } from "react";
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  ReferenceLine,
  ResponsiveContainer,
  CartesianGrid,
} from "recharts";
import { useSSE } from "@/hooks/useSSE";

const API_URL = process.env.NEXT_PUBLIC_API_URL ?? "http://localhost:8000";
const WINDOW_SECONDS = 60;
const MAX_POINTS = 600; // 100 ms cadence × 60 s = 600 points max

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface MetricsMessage {
  timestamp: number;
  obi: number;
  bid_volume: number;
  ask_volume: number;
  best_bid: number;
  best_ask: number;
  spread_bps: number;
  anomalies: { type: string; timestamp: number; details: Record<string, unknown> }[];
}

interface DataPoint {
  ts: number;       // seconds
  obi: number;
  bid_vol: number;
  ask_vol: number;
}

// ---------------------------------------------------------------------------
// Custom tooltip
// ---------------------------------------------------------------------------

function CustomTooltip({
  active,
  payload,
}: {
  active?: boolean;
  payload?: { value: number; payload: DataPoint }[];
}) {
  if (!active || !payload?.length) return null;
  const d = payload[0].payload;
  return (
    <div className="bg-zinc-900 border border-zinc-700 rounded px-2 py-1 text-xs">
      <p className="text-zinc-300">
        OBI:{" "}
        <span
          className={
            d.obi > 0 ? "text-emerald-400" : d.obi < 0 ? "text-red-400" : "text-zinc-400"
          }
        >
          {d.obi.toFixed(4)}
        </span>
      </p>
      <p className="text-zinc-500">
        {new Date(d.ts * 1000).toLocaleTimeString()}
      </p>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------

export function OBIChart() {
  const [points, setPoints] = useState<DataPoint[]>([]);
  const [latest, setLatest] = useState<MetricsMessage | null>(null);

  const handleMessage = useCallback((msg: MetricsMessage) => {
    setLatest(msg);
    setPoints((prev) => {
      const cutoff = msg.timestamp - WINDOW_SECONDS;
      const filtered = prev.filter((p) => p.ts >= cutoff);
      const next = [
        ...filtered,
        {
          ts: msg.timestamp,
          obi: msg.obi,
          bid_vol: msg.bid_volume,
          ask_vol: msg.ask_volume,
        },
      ];
      return next.slice(-MAX_POINTS);
    });
  }, []);

  useSSE<MetricsMessage>(
    `${API_URL}/api/ob/stream/metrics`,
    handleMessage
  );

  const currentOBI = latest?.obi ?? 0;
  const obiColor =
    currentOBI > 0.1
      ? "#34d399"
      : currentOBI < -0.1
      ? "#f87171"
      : "#a1a1aa";

  return (
    <div>
      {/* Numeric summary */}
      <div className="flex gap-6 mb-3 text-sm font-mono">
        <div>
          <span className="text-zinc-500">OBI </span>
          <span style={{ color: obiColor }} className="font-bold text-base">
            {currentOBI.toFixed(4)}
          </span>
        </div>
        {latest && (
          <>
            <div>
              <span className="text-zinc-500">Bid vol </span>
              <span className="text-emerald-400">{latest.bid_volume.toFixed(2)}</span>
            </div>
            <div>
              <span className="text-zinc-500">Ask vol </span>
              <span className="text-red-400">{latest.ask_volume.toFixed(2)}</span>
            </div>
            <div>
              <span className="text-zinc-500">Spread </span>
              <span className="text-zinc-300">{latest.spread_bps.toFixed(1)} bps</span>
            </div>
          </>
        )}
      </div>

      {/* Chart */}
      {points.length === 0 ? (
        <div className="flex items-center justify-center h-32 text-zinc-500 text-xs">
          Waiting for data…
        </div>
      ) : (
        <ResponsiveContainer width="100%" height={180}>
          <LineChart data={points} margin={{ top: 4, right: 8, bottom: 4, left: 0 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#27272a" vertical={false} />
            <XAxis
              dataKey="ts"
              type="number"
              domain={["dataMin", "dataMax"]}
              tickFormatter={(v: number) => new Date(v * 1000).toLocaleTimeString()}
              tick={{ fill: "#71717a", fontSize: 10 }}
              tickLine={false}
              axisLine={false}
            />
            <YAxis
              domain={[-1, 1]}
              ticks={[-1, -0.5, 0, 0.5, 1]}
              tick={{ fill: "#71717a", fontSize: 10 }}
              tickLine={false}
              axisLine={false}
              width={32}
            />
            <Tooltip content={<CustomTooltip />} />
            <ReferenceLine y={0} stroke="#52525b" strokeDasharray="4 2" />
            <Line
              type="monotone"
              dataKey="obi"
              stroke={obiColor}
              strokeWidth={1.5}
              dot={false}
              isAnimationActive={false}
            />
          </LineChart>
        </ResponsiveContainer>
      )}
    </div>
  );
}
