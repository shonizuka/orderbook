import { OrderBook } from "@/components/OrderBook";
import { OBIChart } from "@/components/OBIChart";
import { AnomalyFeed } from "@/components/AnomalyFeed";

export default function Home() {
  return (
    <main className="min-h-screen bg-zinc-950 text-zinc-100 p-4 md:p-6">
      {/* Title bar */}
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-lg font-bold tracking-tight">
            BTC-USD Order Book
          </h1>
          <p className="text-zinc-500 text-xs mt-0.5">
            Coinbase · Kraken · Binance · Bitstamp — aggregated, real-time
          </p>
        </div>
        <span className="flex items-center gap-1.5 text-xs text-emerald-400">
          <span className="inline-block w-1.5 h-1.5 rounded-full bg-emerald-400 animate-pulse" />
          Live
        </span>
      </div>

      {/* Main grid */}
      <div className="grid grid-cols-1 xl:grid-cols-[380px_1fr] gap-4">
        {/* Order book column */}
        <section className="bg-zinc-900 rounded-xl border border-zinc-800 p-4">
          <h2 className="text-xs font-semibold text-zinc-400 uppercase tracking-wider mb-3">
            Aggregated Book
          </h2>
          <OrderBook depth={15} />
        </section>

        {/* Right column */}
        <div className="flex flex-col gap-4">
          {/* OBI Chart */}
          <section className="bg-zinc-900 rounded-xl border border-zinc-800 p-4">
            <h2 className="text-xs font-semibold text-zinc-400 uppercase tracking-wider mb-3">
              Order Book Imbalance (OBI) — 60s window
            </h2>
            <OBIChart />
          </section>

          {/* Anomaly Feed */}
          <section className="bg-zinc-900 rounded-xl border border-zinc-800 p-4">
            <h2 className="text-xs font-semibold text-zinc-400 uppercase tracking-wider mb-3">
              Anomaly Feed
            </h2>
            <AnomalyFeed />
          </section>
        </div>
      </div>
    </main>
  );
}
