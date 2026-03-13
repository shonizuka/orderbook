"""
REST API endpoints.

GET /api/ob/snapshot
  - ?exchange=coinbase|kraken|binance|bitstamp  → single exchange snapshot
  - no param                                    → merged aggregated snapshot
  - ?depth=N                                    → number of levels (default 20, max 100)
"""
from __future__ import annotations

import json
import logging

from fastapi import APIRouter, HTTPException, Query
from fastapi.requests import Request
from pydantic import BaseModel

from backend.core.redis_pub import KEY_AGGREGATED_SNAPSHOT, KEY_SNAPSHOT_PREFIX

log = logging.getLogger(__name__)
router = APIRouter()

VALID_EXCHANGES = {"coinbase", "kraken", "binance", "bitstamp"}


class SnapshotResponse(BaseModel):
    exchange: str
    timestamp: float
    bids: list[list[float]]
    asks: list[list[float]]


class AggregatedSnapshotResponse(BaseModel):
    timestamp: float
    bids: list[list[float]]
    asks: list[list[float]]
    exchanges: list[str]


@router.get(
    "/api/ob/snapshot",
    response_model=SnapshotResponse | AggregatedSnapshotResponse,
    summary="Get order book snapshot",
)
async def get_snapshot(
    request: Request,
    exchange: str | None = Query(default=None, description="Exchange name. Omit for aggregated view."),
    depth: int = Query(default=20, ge=1, le=100, description="Number of levels to return"),
) -> SnapshotResponse | AggregatedSnapshotResponse:
    redis = request.app.state.redis

    if exchange is not None:
        exchange = exchange.lower()
        if exchange not in VALID_EXCHANGES:
            raise HTTPException(
                status_code=400,
                detail=f"Unknown exchange '{exchange}'. Valid: {sorted(VALID_EXCHANGES)}",
            )
        raw = await redis.get(f"{KEY_SNAPSHOT_PREFIX}{exchange}")
        if raw is None:
            raise HTTPException(
                status_code=503,
                detail=f"No data yet for exchange '{exchange}'. Is the connector running?",
            )
        data = json.loads(raw)
        return SnapshotResponse(
            exchange=data["exchange"],
            timestamp=data["timestamp"],
            bids=data["bids"][:depth],
            asks=data["asks"][:depth],
        )

    raw = await redis.get(KEY_AGGREGATED_SNAPSHOT)
    if raw is None:
        raise HTTPException(
            status_code=503,
            detail="No aggregated data yet. Are the connectors running?",
        )
    data = json.loads(raw)
    return AggregatedSnapshotResponse(
        timestamp=data["timestamp"],
        bids=data["bids"][:depth],
        asks=data["asks"][:depth],
        exchanges=data.get("exchanges", []),
    )
