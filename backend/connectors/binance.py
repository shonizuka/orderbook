"""
Binance WebSocket connector — btcusdt@depth@100ms diff stream.

Binance requires a two-phase synchronisation on connect:
  1. Open WebSocket and buffer all incoming diff messages.
  2. Fetch a REST depth snapshot and record lastUpdateId.
  3. Discard buffered messages with u < lastUpdateId + 1.
  4. First applied message must satisfy: U <= lastUpdateId + 1 <= u.
  5. Thereafter each message must satisfy: U == prev_u + 1.

Reference: https://binance-docs.github.io/apidocs/spot/en/#how-to-manage-a-local-order-book-correctly
"""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

import aiohttp
import websockets
from websockets.exceptions import ConnectionClosed, WebSocketException

from backend.core.normalizer import OrderBookDelta, normalize_binance, normalize_binance_snapshot

log = logging.getLogger(__name__)

WS_URL = "wss://stream.binance.com:9443/ws/btcusdt@depth@100ms"
SNAPSHOT_URL = "https://api.binance.com/api/v3/depth?symbol=BTCUSDT&limit=1000"


class BinanceConnector:
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
                    max_size=10 * 1024 * 1024,
                ) as ws:
                    backoff = 1.0
                    log.info("Binance WS connected")
                    await self._run_session(ws)
            except (ConnectionClosed, WebSocketException, OSError) as exc:
                log.warning("Binance WS disconnected: %s — retry in %.1fs", exc, backoff)
            except asyncio.CancelledError:
                log.info("BinanceConnector cancelled")
                raise
            except Exception as exc:
                log.exception("Binance unexpected error: %s", exc)

            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60.0)

    # ------------------------------------------------------------------

    async def _run_session(self, ws: Any) -> None:
        """
        Binance sync protocol (Phase 1 + 2 + 3).
        Phase 1: buffer messages while fetching REST snapshot.
        Phase 2: apply snapshot + drain buffer.
        Phase 3: stream normally with sequence validation.
        """
        # Phase 1 — buffer incoming messages while snapshot is in flight
        buffer: list[dict] = []
        snapshot_task = asyncio.create_task(self._fetch_snapshot())

        # Collect messages until the snapshot arrives
        while not snapshot_task.done():
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
                msg = json.loads(raw)
                if msg.get("e") == "depthUpdate":
                    buffer.append(msg)
            except asyncio.TimeoutError:
                pass

        snap = await snapshot_task
        last_update_id: int = snap["lastUpdateId"]
        log.info("Binance snapshot received, lastUpdateId=%d, buffered=%d", last_update_id, len(buffer))

        # Phase 2 — emit snapshot, then drain buffer
        snap_delta = normalize_binance_snapshot(snap)
        await self._queue.put(snap_delta)

        prev_u = last_update_id
        for msg in buffer:
            u: int = msg["u"]
            U: int = msg["U"]
            if u < last_update_id + 1:
                continue  # stale, drop
            if U > last_update_id + 1:
                # Gap detected — restart session
                log.warning("Binance buffer gap detected, restarting")
                return
            delta = normalize_binance(msg)
            await self._queue.put(delta)
            prev_u = u

        # Phase 3 — stream with sequence validation
        async for raw in ws:
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                continue

            if msg.get("e") != "depthUpdate":
                continue

            U = msg["U"]
            u = msg["u"]

            if U != prev_u + 1:
                log.warning(
                    "Binance sequence gap: expected U=%d got U=%d — restarting",
                    prev_u + 1, U,
                )
                return  # triggers reconnect + full resync

            delta = normalize_binance(msg)
            await self._queue.put(delta)
            prev_u = u

    async def _fetch_snapshot(self) -> dict:
        """Fetch REST depth snapshot from Binance API."""
        async with aiohttp.ClientSession() as session:
            for attempt in range(5):
                try:
                    async with session.get(SNAPSHOT_URL, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                        resp.raise_for_status()
                        return await resp.json()
                except Exception as exc:
                    log.warning("Binance snapshot fetch attempt %d failed: %s", attempt + 1, exc)
                    await asyncio.sleep(2 ** attempt)
        raise RuntimeError("Failed to fetch Binance snapshot after 5 attempts")
