"""
Cross-exchange message format normalization.
All connectors parse raw WS bytes → OrderBookDelta before entering the aggregator.
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Literal


# ---------------------------------------------------------------------------
# Domain types
# ---------------------------------------------------------------------------

Exchange = Literal["coinbase", "kraken", "binance", "bitstamp"]


@dataclass(slots=True)
class PriceLevel:
    price: float
    quantity: float


@dataclass
class OrderBookDelta:
    """Normalised diff/snapshot from one exchange."""

    exchange: Exchange
    timestamp: float          # Unix epoch (seconds, float)
    bids: list[PriceLevel]    # quantity=0.0  → delete this level
    asks: list[PriceLevel]    # quantity=0.0  → delete this level
    is_snapshot: bool = False  # True → replace the full side before applying


# ---------------------------------------------------------------------------
# Coinbase Advanced Trade  (channel "level2")
# ---------------------------------------------------------------------------

def normalize_coinbase(events: list[dict]) -> OrderBookDelta | None:
    """
    Parse the ``events`` array from a Coinbase l2_data channel message.

    Snapshot event:  {"type": "snapshot", "updates": [{side, price_level, new_quantity}]}
    Update event:    {"type": "update",   "updates": [...]}

    ``new_quantity == "0"`` means remove.
    """
    if not events:
        return None

    event = events[0]
    evt_type: str = event.get("type", "")
    if evt_type not in ("snapshot", "update"):
        return None

    is_snapshot = evt_type == "snapshot"
    bids: list[PriceLevel] = []
    asks: list[PriceLevel] = []

    for upd in event.get("updates", []):
        price = float(upd["price_level"])
        qty = float(upd["new_quantity"])
        side = upd["side"]
        if side == "bid":
            bids.append(PriceLevel(price, qty))
        elif side == "offer":
            asks.append(PriceLevel(price, qty))

    return OrderBookDelta(
        exchange="coinbase",
        timestamp=time.time(),
        bids=bids,
        asks=asks,
        is_snapshot=is_snapshot,
    )


# ---------------------------------------------------------------------------
# Kraken  (channel "book")
# ---------------------------------------------------------------------------

def normalize_kraken(data: dict, is_snapshot: bool) -> OrderBookDelta:
    """
    Parse the data dict at index 1 of a Kraken array message.

    Snapshot keys:  ``bs`` (bids), ``as`` (asks)
    Update keys:    ``b``  (bids), ``a``  (asks)

    Each entry: [price_str, qty_str, timestamp_str]
    qty == "0.00000000" means remove.
    """
    bids: list[PriceLevel] = []
    asks: list[PriceLevel] = []

    bid_key = "bs" if is_snapshot else "b"
    ask_key = "as" if is_snapshot else "a"

    for entry in data.get(bid_key, []):
        bids.append(PriceLevel(float(entry[0]), float(entry[1])))

    for entry in data.get(ask_key, []):
        asks.append(PriceLevel(float(entry[0]), float(entry[1])))

    return OrderBookDelta(
        exchange="kraken",
        timestamp=time.time(),
        bids=bids,
        asks=asks,
        is_snapshot=is_snapshot,
    )


# ---------------------------------------------------------------------------
# Binance  (stream btcusdt@depth@100ms)
# ---------------------------------------------------------------------------

def normalize_binance(msg: dict) -> OrderBookDelta:
    """
    Parse a Binance ``depthUpdate`` message.

    ``b`` = bids, ``a`` = asks.
    Quantity ``"0.00000000"`` means remove.
    Timestamp from ``msg["E"]`` (milliseconds).
    """
    bids = [PriceLevel(float(p), float(q)) for p, q in msg.get("b", [])]
    asks = [PriceLevel(float(p), float(q)) for p, q in msg.get("a", [])]

    return OrderBookDelta(
        exchange="binance",
        timestamp=msg["E"] / 1000.0,
        bids=bids,
        asks=asks,
        is_snapshot=False,
    )


def normalize_binance_snapshot(snap: dict) -> OrderBookDelta:
    """
    Parse a Binance REST depth snapshot (used once on connect to seed the book).
    ``snap`` = {"lastUpdateId": int, "bids": [[price, qty]], "asks": [[price, qty]]}
    """
    bids = [PriceLevel(float(p), float(q)) for p, q in snap.get("bids", [])]
    asks = [PriceLevel(float(p), float(q)) for p, q in snap.get("asks", [])]

    return OrderBookDelta(
        exchange="binance",
        timestamp=time.time(),
        bids=bids,
        asks=asks,
        is_snapshot=True,
    )


# ---------------------------------------------------------------------------
# Bitstamp  (channel "order_book_btcusd")
# ---------------------------------------------------------------------------

def normalize_bitstamp(data: dict) -> OrderBookDelta:
    """
    Parse the ``data`` field from a Bitstamp order_book message.
    Bitstamp always sends full snapshots (top-100 levels).
    Timestamp from ``data["microtimestamp"]`` (microseconds).
    """
    ts_us = int(data.get("microtimestamp", 0))
    timestamp = ts_us / 1_000_000.0 if ts_us else time.time()

    bids = [PriceLevel(float(p), float(q)) for p, q in data.get("bids", [])]
    asks = [PriceLevel(float(p), float(q)) for p, q in data.get("asks", [])]

    return OrderBookDelta(
        exchange="bitstamp",
        timestamp=timestamp,
        bids=bids,
        asks=asks,
        is_snapshot=True,
    )
