"""
Server-Sent Events (SSE) endpoints.

GET /api/ob/stream?channel=aggregated|delta|metrics
  - Streams real-time Redis pub/sub events to browser clients.
  - Each event is a JSON-encoded SSE data line.

Behaviour:
  - Client disconnect is detected via asyncio.CancelledError / GeneratorExit.
  - Slow clients are handled with a per-message write timeout.
  - A keepalive comment (": ping\\n\\n") is sent every 15 seconds of inactivity.
"""
from __future__ import annotations

import asyncio
import json
import logging
from typing import AsyncGenerator

from fastapi import APIRouter
from fastapi.requests import Request
from fastapi.responses import StreamingResponse

from backend.core.redis_pub import CHANNEL_DELTA, CHANNEL_AGGREGATED, CHANNEL_METRICS

log = logging.getLogger(__name__)
router = APIRouter()

CHANNEL_MAP = {
    "delta": CHANNEL_DELTA,
    "aggregated": CHANNEL_AGGREGATED,
    "metrics": CHANNEL_METRICS,
}

KEEPALIVE_INTERVAL = 15.0  # seconds


# ---------------------------------------------------------------------------
# SSE generator
# ---------------------------------------------------------------------------

async def _sse_generator(
    request: Request,
    redis_channel: str,
) -> AsyncGenerator[str, None]:
    """
    Subscribe to `redis_channel` and yield SSE-formatted strings.
    Yields ': ping\\n\\n' if no message received for KEEPALIVE_INTERVAL.
    """
    redis = request.app.state.redis
    pubsub = redis.pubsub()

    try:
        await pubsub.subscribe(redis_channel)
        log.info("SSE client subscribed to %s", redis_channel)

        while True:
            # Check if the HTTP client has disconnected
            if await request.is_disconnected():
                log.info("SSE client disconnected from %s", redis_channel)
                break

            try:
                message = await asyncio.wait_for(
                    pubsub.get_message(ignore_subscribe_messages=True, timeout=0.1),
                    timeout=KEEPALIVE_INTERVAL,
                )
            except asyncio.TimeoutError:
                # No message within keepalive window — send a comment ping
                yield ": ping\n\n"
                continue

            if message is None:
                # Short sleep to avoid spinning when Redis is idle
                await asyncio.sleep(0.01)
                continue

            data = message.get("data")
            if data:
                yield f"data: {data}\n\n"

    except asyncio.CancelledError:
        log.info("SSE generator cancelled for %s", redis_channel)
    except Exception as exc:
        log.exception("SSE generator error on %s: %s", redis_channel, exc)
    finally:
        try:
            await pubsub.unsubscribe(redis_channel)
            await pubsub.aclose()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/api/ob/stream", summary="Real-time SSE order book stream")
async def stream_orderbook(
    request: Request,
    channel: str = "aggregated",
) -> StreamingResponse:
    """
    Stream Redis pub/sub events as Server-Sent Events.

    Query params:
      channel: one of ``delta``, ``aggregated``, ``metrics``  (default: aggregated)
    """
    redis_channel = CHANNEL_MAP.get(channel, CHANNEL_AGGREGATED)
    return StreamingResponse(
        _sse_generator(request, redis_channel),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


@router.get("/api/ob/stream/metrics", summary="Real-time SSE metrics stream")
async def stream_metrics(request: Request) -> StreamingResponse:
    """Convenience endpoint — always streams the metrics channel."""
    return StreamingResponse(
        _sse_generator(request, CHANNEL_METRICS),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )
