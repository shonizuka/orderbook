"use client";

import { useCallback, useRef, useState } from "react";
import { useSSE } from "@/hooks/useSSE";

const API_URL = process.env.NEXT_PUBLIC_API_URL ?? "http://localhost:8000";
const DEPTH = 15;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface AggregatedBook {
  timestamp: number;
  bids: [number, number][]; // [price, qty]
  asks: [number, number][];
  exchanges: string[];
}

interface LevelRow {
  price: number;
  qty: number;
  total: number;
  bar: number;   // 0-100 — relative bar width
  flash: "up" | "down" | "none";
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function buildRows(
  levels: [number, number][],
  depth: number,
  prevMap: Map<number, number>
): { rows: LevelRow[]; maxTotal: number } {
  const sliced = levels.slice(0, depth);
  let cumulative = 0;
  const rows: LevelRow[] = sliced.map(([price, qty]) => {
    cumulative += qty;
    const prevQty = prevMap.get(price);
    const flash: "up" | "down" | "none" =
      prevQty === undefined ? "none" : qty > prevQty ? "up" : qty < prevQty ? "down" : "none";
    return { price, qty, total: cumulative, bar: 0, flash };
  });
  const maxTotal = cumulative;
  rows.forEach((r) => {
    r.bar = maxTotal > 0 ? (r.total / maxTotal) * 100 : 0;
  });
  return { rows, maxTotal };
}

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------

export function OrderBook({ depth = DEPTH }: { depth?: number }) {
  const [book, setBook] = useState<AggregatedBook | null>(null);
  const prevBids = useRef<Map<number, number>>(new Map());
  const prevAsks = useRef<Map<number, number>>(new Map());

  const handleMessage = useCallback((data: AggregatedBook) => {
    setBook((prev) => {
      // Build previous maps for flash detection
      if (prev) {
        prevBids.current = new Map(prev.bids.map(([p, q]) => [p, q]));
        prevAsks.current = new Map(prev.asks.map(([p, q]) => [p, q]));
      }
      return data;
    });
  }, []);

  useSSE<AggregatedBook>(
    `${API_URL}/api/ob/stream?channel=aggregated`,
    handleMessage
  );

  if (!book) {
    return (
      <div className="flex items-center justify-center h-64 text-zinc-400 text-sm">
        Connecting to order book…
      </div>
    );
  }

  const spread =
    book.asks[0] && book.bids[0]
      ? book.asks[0][0] - book.bids[0][0]
      : 0;
  const spreadBps =
    book.bids[0] ? ((spread / book.bids[0][0]) * 10000).toFixed(1) : "–";

  const { rows: bidRows } = buildRows(book.bids, depth, prevBids.current);
  const { rows: askRows } = buildRows(book.asks, depth, prevAsks.current);

  return (
    <div className="font-mono text-xs select-none">
      {/* Header */}
      <div className="grid grid-cols-3 text-zinc-500 uppercase tracking-wider mb-1 px-1">
        <span>Total</span>
        <span className="text-center">Price</span>
        <span className="text-right">Size</span>
      </div>

      {/* Asks — reversed so lowest ask is at bottom (nearest spread) */}
      <div className="flex flex-col-reverse">
        {askRows.map((row) => (
          <LevelRowView key={row.price} row={row} side="ask" />
        ))}
      </div>

      {/* Spread */}
      <div className="text-center text-zinc-400 py-1 border-y border-zinc-800 my-0.5">
        Spread&nbsp;
        <span className="text-zinc-200">${spread.toFixed(2)}</span>
        &nbsp;
        <span className="text-zinc-500">({spreadBps}&nbsp;bps)</span>
      </div>

      {/* Bids */}
      <div className="flex flex-col">
        {bidRows.map((row) => (
          <LevelRowView key={row.price} row={row} side="bid" />
        ))}
      </div>

      {/* Footer */}
      <div className="mt-2 text-zinc-600 text-[10px] px-1">
        {book.exchanges.join(" · ")} ·{" "}
        {new Date(book.timestamp * 1000).toLocaleTimeString()}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Single row
// ---------------------------------------------------------------------------

function LevelRowView({
  row,
  side,
}: {
  row: LevelRow;
  side: "bid" | "ask";
}) {
  const barColor = side === "bid" ? "bg-emerald-900/40" : "bg-red-900/40";
  const priceColor = side === "bid" ? "text-emerald-400" : "text-red-400";
  const flashClass =
    row.flash === "up"
      ? "animate-flash-green"
      : row.flash === "down"
      ? "animate-flash-red"
      : "";

  return (
    <div className={`relative grid grid-cols-3 px-1 py-[1px] ${flashClass}`}>
      {/* Background bar */}
      <div
        className={`absolute inset-y-0 right-0 ${barColor} transition-all duration-100`}
        style={{ width: `${row.bar}%` }}
      />
      {/* Content */}
      <span className="relative text-zinc-400 z-10">
        {row.total.toFixed(4)}
      </span>
      <span className={`relative text-center font-semibold z-10 ${priceColor}`}>
        {row.price.toLocaleString("en-US", {
          minimumFractionDigits: 2,
          maximumFractionDigits: 2,
        })}
      </span>
      <span className="relative text-right z-10 text-zinc-300">
        {row.qty.toFixed(4)}
      </span>
    </div>
  );
}
