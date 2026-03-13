"""
Bitstamp WebSocket connector.
Stream: wss://ws.bitstamp.net  channel: order_book_btcusd

Bitstamp sends full top-100 snapshots on every update (not diffs).
The normalizer marks every message is_snapshot=True accordingly.

Heartbeat: Bitstamp requires a periodic "bts:heartbeat" event to keep
the connection alive and to receive server-side heartbeats.
"""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

import websockets
from websockets.exceptions import ConnectionClosed, WebSocketException

from backend.core.normalizer import OrderBookDelta, normalize_bitstamp

log = logging.getLogger(__name__)

WS_URL = "wss://ws.bitstamp.net"
CHANNEL = "order_book_btcusd"
HEARTBEAT_INTERVAL = 10.0  # seconds


class BitstampConnector:
    def __init__(self, delta_queue: asyncio.Queue[OrderBookDelta]) -> None:
        self._queue = delta_queue

    async def connect(self) -> None:
        backoff = 1.0
        while True:
            try:
                async with websockets.connect(
                    WS_URL,
                    ping_interval=None,  # We manage heartbeats manually
                    close_timeout=10,
                    max_size=5 * 1024 * 1024,
                ) as ws:
                    backoff = 1.0
                    log.info("Bitstamp WS connected")
                    await self._subscribe(ws)
                    await self._run_session(ws)
            except (ConnectionClosed, WebSocketException, OSError) as exc:
                log.warning("Bitstamp WS disconnected: %s — retry in %.1fs", exc, backoff)
            except asyncio.CancelledError:
                log.info("BitstampConnector cancelled")
                raise
            except Exception as exc:
                log.exception("Bitstamp unexpected error: %s", exc)

            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60.0)

    # ------------------------------------------------------------------

    async def _subscribe(self, ws: Any) -> None:
        msg = json.dumps({
            "event": "bts:subscribe",
            "data": {"channel": CHANNEL},
        })
        await ws.send(msg)
        log.debug("Bitstamp subscription sent")

    async def _run_session(self, ws: Any) -> None:
        """Pump messages + send periodic heartbeats concurrently."""
        hb_task = asyncio.create_task(self._heartbeat_loop(ws))
        try:
            await self._pump(ws)
        finally:
            hb_task.cancel()
            try:
                await hb_task
            except asyncio.CancelledError:
                pass

    async def _pump(self, ws: Any) -> None:
        async for raw in ws:
            delta = self._parse(raw)
            if delta is not None:
                await self._queue.put(delta)

    async def _heartbeat_loop(self, ws: Any) -> None:
        """Send bts:heartbeat every HEARTBEAT_INTERVAL seconds."""
        hb_msg = json.dumps({"event": "bts:heartbeat"})
        while True:
            await asyncio.sleep(HEARTBEAT_INTERVAL)
            try:
                await ws.send(hb_msg)
            except Exception:
                break

    def _parse(self, raw: str) -> OrderBookDelta | None:
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            log.warning("Bitstamp non-JSON: %s", raw[:200])
            return None

        event = msg.get("event", "")
        channel = msg.get("channel", "")

        # Only process order book data events
        if event != "data" or channel != CHANNEL:
            return None

        data = msg.get("data", {})
        if not data:
            return None

        return normalize_bitstamp(data)
