"""
Redis pub/sub publisher.
Publishes normalised deltas, aggregated book snapshots, and metrics.
Also stores per-exchange snapshots as Redis strings (for the REST endpoint).
"""
from __future__ import annotations

import json
import logging
import time
from typing import Any

import redis.asyncio as aioredis

from backend.core.normalizer import Exchange, OrderBookDelta, PriceLevel
from backend.core.metrics import MetricsResult

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Channel / key names
# ---------------------------------------------------------------------------
CHANNEL_DELTA = "orderbook:delta"
CHANNEL_AGGREGATED = "orderbook:aggregated"
CHANNEL_METRICS = "orderbook:metrics"
KEY_SNAPSHOT_PREFIX = "orderbook:snapshot:"
KEY_AGGREGATED_SNAPSHOT = "orderbook:aggregated:latest"
SNAPSHOT_TTL = 120  # seconds


class RedisPublisher:
    def __init__(self, redis_url: str = "redis://localhost:6379") -> None:
        self._url = redis_url
        self._redis: aioredis.Redis | None = None

    # ------------------------------------------------------------------
    # Connection
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        self._redis = aioredis.from_url(
            self._url,
            encoding="utf-8",
            decode_responses=True,
            socket_keepalive=True,
            health_check_interval=30,
        )
        # Verify the connection works
        await self._redis.ping()
        log.info("RedisPublisher connected to %s", self._url)

    async def close(self) -> None:
        if self._redis:
            await self._redis.aclose()

    # ------------------------------------------------------------------
    # Publish helpers
    # ------------------------------------------------------------------

    async def publish_delta(self, delta: OrderBookDelta) -> None:
        payload = {
            "exchange": delta.exchange,
            "timestamp": delta.timestamp,
            "is_snapshot": delta.is_snapshot,
            "bids": [[pl.price, pl.quantity] for pl in delta.bids],
            "asks": [[pl.price, pl.quantity] for pl in delta.asks],
        }
        await self._pub(CHANNEL_DELTA, payload)

    async def publish_aggregated(
        self,
        bids: list[PriceLevel],
        asks: list[PriceLevel],
        exchanges: list[str],
    ) -> None:
        payload = {
            "timestamp": time.time(),
            "bids": [[pl.price, pl.quantity] for pl in bids],
            "asks": [[pl.price, pl.quantity] for pl in asks],
            "exchanges": exchanges,
        }
        serialised = json.dumps(payload)
        assert self._redis is not None
        async with self._redis.pipeline(transaction=False) as pipe:
            pipe.publish(CHANNEL_AGGREGATED, serialised)
            pipe.set(KEY_AGGREGATED_SNAPSHOT, serialised, ex=SNAPSHOT_TTL)
            await pipe.execute()

    async def publish_metrics(self, result: MetricsResult) -> None:
        payload = {
            "timestamp": time.time(),
            "obi": result.obi,
            "bid_volume": result.bid_volume,
            "ask_volume": result.ask_volume,
            "best_bid": result.best_bid,
            "best_ask": result.best_ask,
            "spread_bps": result.spread_bps,
            "anomalies": [
                {"type": a.type, "timestamp": a.timestamp, "details": a.details}
                for a in result.anomalies
            ],
        }
        await self._pub(CHANNEL_METRICS, payload)

    async def set_exchange_snapshot(
        self,
        exchange: str,
        bids: list[PriceLevel],
        asks: list[PriceLevel],
    ) -> None:
        """Store per-exchange book state for the REST /snapshot endpoint."""
        payload = {
            "exchange": exchange,
            "timestamp": time.time(),
            "bids": [[pl.price, pl.quantity] for pl in bids],
            "asks": [[pl.price, pl.quantity] for pl in asks],
        }
        assert self._redis is not None
        await self._redis.set(
            f"{KEY_SNAPSHOT_PREFIX}{exchange}",
            json.dumps(payload),
            ex=SNAPSHOT_TTL,
        )

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _pub(self, channel: str, payload: Any) -> None:
        assert self._redis is not None
        try:
            await self._redis.publish(channel, json.dumps(payload))
        except Exception as exc:
            log.error("Redis publish error on %s: %s", channel, exc)
