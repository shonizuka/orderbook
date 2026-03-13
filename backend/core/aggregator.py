"""
Incremental order-book aggregator.

Consumes OrderBookDelta objects from a shared asyncio.Queue, maintains per-exchange
SortedDict books, merges them into a single aggregated view, computes metrics, and
publishes everything to Redis.
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

from sortedcontainers import SortedDict

from backend.core.metrics import MetricsCalculator
from backend.core.normalizer import Exchange, OrderBookDelta, PriceLevel
from backend.core.redis_pub import RedisPublisher

log = logging.getLogger(__name__)

# How many levels to publish in the aggregated book
AGGREGATED_DEPTH = 30


# ---------------------------------------------------------------------------
# Single-exchange book
# ---------------------------------------------------------------------------

class ExchangeBook:
    """
    Maintains one exchange's live order book using SortedDict.

    Bids: sorted descending (highest price first).
    Asks: sorted ascending  (lowest price first).
    """

    def __init__(self, exchange: str) -> None:
        self.exchange = exchange
        # Negated key → descending order for bids
        self._bids: SortedDict = SortedDict(lambda k: -k)
        self._asks: SortedDict = SortedDict()
        self.last_update: float = 0.0

    # ------------------------------------------------------------------

    def apply_delta(self, delta: OrderBookDelta) -> None:
        if delta.is_snapshot:
            self._bids.clear()
            self._asks.clear()

        for pl in delta.bids:
            if pl.quantity == 0.0:
                self._bids.pop(pl.price, None)
            else:
                self._bids[pl.price] = pl.quantity

        for pl in delta.asks:
            if pl.quantity == 0.0:
                self._asks.pop(pl.price, None)
            else:
                self._asks[pl.price] = pl.quantity

        self.last_update = delta.timestamp

    # ------------------------------------------------------------------

    def top_n(self, n: int = AGGREGATED_DEPTH) -> tuple[list[PriceLevel], list[PriceLevel]]:
        bids = [
            PriceLevel(price, qty)
            for price, qty in list(self._bids.items())[:n]
        ]
        asks = [
            PriceLevel(price, qty)
            for price, qty in list(self._asks.items())[:n]
        ]
        return bids, asks

    @property
    def best_bid(self) -> float:
        return self._bids.keys()[0] if self._bids else 0.0

    @property
    def best_ask(self) -> float:
        return self._asks.keys()[0] if self._asks else 0.0


# ---------------------------------------------------------------------------
# Aggregated (merged) book
# ---------------------------------------------------------------------------

class AggregatedBook:
    """
    Merges all exchange books into a single price ladder.
    Quantities at the same price level are summed across exchanges.
    """

    def __init__(self) -> None:
        self.books: dict[str, ExchangeBook] = {}

    def ensure_book(self, exchange: str) -> ExchangeBook:
        if exchange not in self.books:
            self.books[exchange] = ExchangeBook(exchange)
        return self.books[exchange]

    def get_merged(
        self, depth: int = AGGREGATED_DEPTH
    ) -> tuple[list[PriceLevel], list[PriceLevel]]:
        """
        Merge all exchange books.
        Returns top `depth` bids (descending) and asks (ascending).
        """
        bid_map: dict[float, float] = {}
        ask_map: dict[float, float] = {}

        for book in self.books.values():
            b, a = book.top_n(depth * 2)  # pull extra depth before merge
            for pl in b:
                bid_map[pl.price] = bid_map.get(pl.price, 0.0) + pl.quantity
            for pl in a:
                ask_map[pl.price] = ask_map.get(pl.price, 0.0) + pl.quantity

        sorted_bids = sorted(bid_map.items(), key=lambda x: -x[0])[:depth]
        sorted_asks = sorted(ask_map.items(), key=lambda x: x[0])[:depth]

        return (
            [PriceLevel(p, q) for p, q in sorted_bids],
            [PriceLevel(p, q) for p, q in sorted_asks],
        )

    def exchange_best_bids(self) -> dict[str, float]:
        return {ex: book.best_bid for ex, book in self.books.items()}

    @property
    def active_exchanges(self) -> list[str]:
        now = time.time()
        return [
            ex
            for ex, book in self.books.items()
            if now - book.last_update < 30.0  # consider stale after 30 s
        ]


# ---------------------------------------------------------------------------
# Main aggregator
# ---------------------------------------------------------------------------

class Aggregator:
    """
    Consumes OrderBookDelta objects from delta_queue, applies them to the
    per-exchange books, recomputes the merged view + metrics, then publishes.
    """

    def __init__(
        self,
        delta_queue: asyncio.Queue[OrderBookDelta],
        publisher: RedisPublisher,
        metrics: MetricsCalculator,
    ) -> None:
        self._queue = delta_queue
        self._publisher = publisher
        self._metrics = metrics
        self._book = AggregatedBook()
        self._publish_snapshot_counter: dict[str, int] = {}

    # ------------------------------------------------------------------

    async def run(self) -> None:
        """Main event loop — runs until task is cancelled."""
        log.info("Aggregator started")
        while True:
            try:
                delta: OrderBookDelta = await self._queue.get()
                await self._process_delta(delta)
                self._queue.task_done()
            except asyncio.CancelledError:
                log.info("Aggregator cancelled")
                raise
            except Exception as exc:
                log.exception("Aggregator error: %s", exc)
                # Continue processing — do not crash the aggregator

    # ------------------------------------------------------------------

    async def _process_delta(self, delta: OrderBookDelta) -> None:
        # 1. Apply to exchange book
        xbook = self._book.ensure_book(delta.exchange)
        xbook.apply_delta(delta)

        # 2. Merge across all exchanges
        bids, asks = self._book.get_merged()

        # 3. Compute metrics
        result = self._metrics.calculate(
            bids=bids,
            asks=asks,
            exchange_best_bids=self._book.exchange_best_bids(),
            last_delta_exchange=delta.exchange,
            delta_bids=delta.bids,
            delta_asks=delta.asks,
        )

        # 4. Publish (all are fire-and-forget — don't block aggregation loop)
        await asyncio.gather(
            self._publisher.publish_delta(delta),
            self._publisher.publish_aggregated(bids, asks, self._book.active_exchanges),
            self._publisher.publish_metrics(result),
            self._maybe_publish_exchange_snapshot(delta.exchange, xbook),
        )

    async def _maybe_publish_exchange_snapshot(
        self, exchange: str, book: ExchangeBook
    ) -> None:
        """Throttle per-exchange Redis SET to every 10 deltas."""
        n = self._publish_snapshot_counter.get(exchange, 0) + 1
        self._publish_snapshot_counter[exchange] = n
        if n % 10 == 0:
            bids, asks = book.top_n()
            await self._publisher.set_exchange_snapshot(exchange, bids, asks)
