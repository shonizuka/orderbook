"""
Application entry point.

Starts four exchange WebSocket connectors + the aggregator as concurrent asyncio tasks.
FastAPI lifespan manages startup/shutdown cleanly.
"""
from __future__ import annotations

import asyncio
import logging
import os
from contextlib import asynccontextmanager
from typing import AsyncGenerator

import redis.asyncio as aioredis
import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from backend.connectors.binance import BinanceConnector
from backend.connectors.bitstamp import BitstampConnector
from backend.connectors.coinbase import CoinbaseConnector
from backend.connectors.kraken import KrakenConnector
from backend.core.aggregator import Aggregator
from backend.core.metrics import MetricsCalculator
from backend.core.normalizer import OrderBookDelta
from backend.core.redis_pub import RedisPublisher
from backend.api.rest import router as rest_router
from backend.api.sse import router as sse_router

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config from environment
# ---------------------------------------------------------------------------

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
CORS_ORIGINS = os.getenv("CORS_ORIGINS", "http://localhost:3000").split(",")
DELTA_QUEUE_SIZE = int(os.getenv("DELTA_QUEUE_SIZE", "20000"))


# ---------------------------------------------------------------------------
# Lifespan — startup & graceful shutdown
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    # ---- Startup ----
    log.info("Starting orderbook aggregator…")

    # Shared Redis client (used by REST + SSE endpoints)
    redis_client = aioredis.from_url(
        REDIS_URL,
        encoding="utf-8",
        decode_responses=True,
        socket_keepalive=True,
        health_check_interval=30,
    )
    app.state.redis = redis_client

    # Publisher (separate connection pool for pub/sub writes)
    publisher = RedisPublisher(redis_url=REDIS_URL)
    await publisher.connect()

    # Shared queue
    delta_queue: asyncio.Queue[OrderBookDelta] = asyncio.Queue(maxsize=DELTA_QUEUE_SIZE)

    # Metrics calculator
    metrics_calc = MetricsCalculator()

    # Aggregator
    aggregator = Aggregator(delta_queue, publisher, metrics_calc)

    # Connectors
    connectors = [
        CoinbaseConnector(delta_queue),
        KrakenConnector(delta_queue),
        BinanceConnector(delta_queue),
        BitstampConnector(delta_queue),
    ]

    # Launch all tasks
    tasks: list[asyncio.Task] = []
    tasks.append(asyncio.create_task(aggregator.run(), name="aggregator"))
    for connector in connectors:
        name = connector.__class__.__name__
        tasks.append(asyncio.create_task(connector.connect(), name=name))

    app.state.tasks = tasks
    app.state.publisher = publisher

    log.info("All connectors and aggregator started (%d tasks)", len(tasks))

    yield

    # ---- Shutdown ----
    log.info("Shutting down…")
    for task in tasks:
        task.cancel()

    results = await asyncio.gather(*tasks, return_exceptions=True)
    for task, result in zip(tasks, results):
        if isinstance(result, Exception) and not isinstance(result, asyncio.CancelledError):
            log.error("Task %s raised on shutdown: %s", task.get_name(), result)

    await publisher.close()
    await redis_client.aclose()
    log.info("Shutdown complete")


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = FastAPI(
    title="BTC-USD Order Book Aggregator",
    description="Real-time multi-exchange aggregated order book with OBI metrics",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET"],
    allow_headers=["*"],
)

app.include_router(rest_router)
app.include_router(sse_router)


@app.get("/health")
async def health() -> dict:
    return {"status": "ok"}


# ---------------------------------------------------------------------------
# Dev entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    uvicorn.run(
        "backend.main:app",
        host="0.0.0.0",
        port=8000,
        loop="uvloop",
        log_level="info",
        reload=False,
    )
