"""
Kraken WebSocket v1 connector.
Stream: wss://ws.kraken.com  subscription: book  depth: 10  pair: XBT/USD

Message formats:
  Subscription ack  → dict with "event": "subscriptionStatus"
  Snapshot          → [channelID, {"bs": [...], "as": [...]}, "book-10", "XBT/USD"]
  Update            → [channelID, {"b": [...], "a": [...]},  "book-10", "XBT/USD"]
  Heartbeat         → {"event": "heartbeat"}
"""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

import websockets
from websockets.exceptions import ConnectionClosed, WebSocketException

from backend.core.normalizer import OrderBookDelta, normalize_kraken

log = logging.getLogger(__name__)

WS_URL = "wss://ws.kraken.com"
PAIR = "XBT/USD"
DEPTH = 10


class KrakenConnector:
    def __init__(self, delta_queue: asyncio.Queue[OrderBookDelta]) -> None:
        self._queue = delta_queue

    async def connect(self) -> None:
        backoff = 1.0
        while True:
            try:
                async with websockets.connect(
                    WS_URL,
                    ping_interval=20,
                    ping_timeout=20,
                    close_timeout=10,
                ) as ws:
                    backoff = 1.0
                    log.info("Kraken WS connected")
                    await self._subscribe(ws)
                    await self._pump(ws)
            except (ConnectionClosed, WebSocketException, OSError) as exc:
                log.warning("Kraken WS disconnected: %s — retry in %.1fs", exc, backoff)
            except asyncio.CancelledError:
                log.info("KrakenConnector cancelled")
                raise
            except Exception as exc:
                log.exception("Kraken unexpected error: %s", exc)

            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60.0)

    # ------------------------------------------------------------------

    async def _subscribe(self, ws: Any) -> None:
        msg = json.dumps({
            "event": "subscribe",
            "pair": [PAIR],
            "subscription": {"name": "book", "depth": DEPTH},
        })
        await ws.send(msg)
        log.debug("Kraken subscription sent")

    async def _pump(self, ws: Any) -> None:
        async for raw in ws:
            delta = self._parse(raw)
            if delta is not None:
                await self._queue.put(delta)

    def _parse(self, raw: str) -> OrderBookDelta | None:
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            log.warning("Kraken non-JSON: %s", raw[:200])
            return None

        # Dict messages are events (heartbeat, subscriptionStatus, systemStatus)
        if isinstance(msg, dict):
            event = msg.get("event", "")
            if event not in ("heartbeat", "subscriptionStatus", "systemStatus", "pong"):
                log.debug("Kraken event: %s", event)
            return None

        # Array messages are book data: [channelID, data, channelName, pair]
        if not isinstance(msg, list) or len(msg) < 4:
            return None

        channel_name = msg[-2]  # "book-10"
        if not isinstance(channel_name, str) or not channel_name.startswith("book"):
            return None

        # data is at index 1, but updates can split bids+asks across two dicts
        # Kraken sometimes sends [channelID, {bids_part}, {asks_part}, name, pair]
        if len(msg) == 5:
            # Two-part update: merge dicts
            data: dict = {**msg[1], **msg[2]}
        else:
            data = msg[1]

        is_snapshot = "bs" in data or "as" in data
        return normalize_kraken(data, is_snapshot=is_snapshot)
