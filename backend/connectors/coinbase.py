"""
Coinbase Advanced Trade WebSocket connector.
Stream: wss://advanced-trade-ws.coinbase.com  channel: level2  product: BTC-USD

Message flow:
  connect → subscribe → receive snapshot → receive diffs → …
  On disconnect: exponential backoff reconnect.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any

import websockets
from websockets.exceptions import ConnectionClosed, WebSocketException

from backend.core.normalizer import OrderBookDelta, normalize_coinbase

log = logging.getLogger(__name__)

WS_URL = "wss://advanced-trade-ws.coinbase.com"
PRODUCT_ID = "BTC-USD"
CHANNEL = "level2"


class CoinbaseConnector:
    def __init__(self, delta_queue: asyncio.Queue[OrderBookDelta]) -> None:
        self._queue = delta_queue

    async def connect(self) -> None:
        """Entry point — loops forever with exponential backoff."""
        backoff = 1.0
        while True:
            try:
                async with websockets.connect(
                    WS_URL,
                    ping_interval=20,
                    ping_timeout=20,
                    close_timeout=10,
                    max_size=10 * 1024 * 1024,  # 10 MB — snapshots can be large
                ) as ws:
                    backoff = 1.0
                    log.info("Coinbase WS connected")
                    await self._subscribe(ws)
                    await self._pump(ws)
            except (ConnectionClosed, WebSocketException, OSError) as exc:
                log.warning("Coinbase WS disconnected: %s — retry in %.1fs", exc, backoff)
            except asyncio.CancelledError:
                log.info("CoinbaseConnector cancelled")
                raise
            except Exception as exc:
                log.exception("Coinbase unexpected error: %s", exc)

            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60.0)

    # ------------------------------------------------------------------

    async def _subscribe(self, ws: Any) -> None:
        msg = json.dumps({
            "type": "subscribe",
            "product_ids": [PRODUCT_ID],
            "channel": CHANNEL,
        })
        await ws.send(msg)
        log.debug("Coinbase subscription sent")

    async def _pump(self, ws: Any) -> None:
        async for raw in ws:
            delta = self._parse(raw)
            if delta is not None:
                await self._queue.put(delta)

    def _parse(self, raw: str) -> OrderBookDelta | None:
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            log.warning("Coinbase non-JSON message: %s", raw[:200])
            return None

        channel = msg.get("channel")
        if channel != "l2_data":
            # Subscriptions ack, heartbeats, etc.
            return None

        events = msg.get("events", [])
        return normalize_coinbase(events)
