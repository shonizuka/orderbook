"""
OBI (Order Book Imbalance) calculation + anomaly detection.

Anomaly types implemented:
  - obi_spike:            sudden OBI shift > threshold
  - spread_widening:      bid/ask spread exceeds threshold in bps
  - exchange_divergence:  best-bid spread across exchanges > threshold USD
  - spoofing:             large order (>= SPOOF_MIN_BTC) disappears in < SPOOF_TTL_MS
  - wall:                 single level exceeds WALL_MULTIPLIER × average level size
  - sweep:                best-bid or best-ask moves ≥ SWEEP_LEVELS in one update
"""
from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field
from typing import Optional

from backend.core.normalizer import Exchange, PriceLevel


# ---------------------------------------------------------------------------
# Result types
# ---------------------------------------------------------------------------

@dataclass
class AnomalyEvent:
    type: str        # see module docstring for valid values
    timestamp: float
    details: dict


@dataclass
class MetricsResult:
    obi: float
    bid_volume: float
    ask_volume: float
    best_bid: float
    best_ask: float
    spread_bps: float
    anomalies: list[AnomalyEvent] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Spoofing tracker — tracks large levels and detects fast disappearances
# ---------------------------------------------------------------------------

@dataclass
class _TrackedLevel:
    exchange: str
    price: float
    quantity: float
    seen_at: float  # time.monotonic()


class SpoofingTracker:
    """Track large bid/ask levels per exchange and fire on fast removal."""

    SPOOF_MIN_BTC: float = 5.0       # threshold quantity to track
    SPOOF_TTL_MS: float = 500.0      # disappearance window (milliseconds)
    CLEANUP_INTERVAL: float = 5.0    # seconds between stale-entry purges

    def __init__(self) -> None:
        # (exchange, price) → tracked level
        self._large_bids: dict[tuple[str, float], _TrackedLevel] = {}
        self._large_asks: dict[tuple[str, float], _TrackedLevel] = {}
        self._last_cleanup = time.monotonic()

    def update(
        self,
        exchange: str,
        bids: list[PriceLevel],
        asks: list[PriceLevel],
    ) -> list[AnomalyEvent]:
        """
        Call after each delta is applied. Returns any spoofing events detected.
        """
        now = time.monotonic()
        events: list[AnomalyEvent] = []

        events.extend(self._process_side(exchange, bids, self._large_bids, "bid", now))
        events.extend(self._process_side(exchange, asks, self._large_asks, "ask", now))

        if now - self._last_cleanup > self.CLEANUP_INTERVAL:
            self._purge_stale(now)
            self._last_cleanup = now

        return events

    def _process_side(
        self,
        exchange: str,
        levels: list[PriceLevel],
        store: dict[tuple[str, float], _TrackedLevel],
        side: str,
        now: float,
    ) -> list[AnomalyEvent]:
        events: list[AnomalyEvent] = []
        incoming_prices = {pl.price for pl in levels}

        # Check for removals / size drops of tracked levels
        for pl in levels:
            key = (exchange, pl.price)
            if key in store:
                if pl.quantity == 0.0:
                    # Level removed — check elapsed time
                    tracked = store.pop(key)
                    elapsed_ms = (now - tracked.seen_at) * 1000.0
                    if elapsed_ms < self.SPOOF_TTL_MS:
                        events.append(AnomalyEvent(
                            type="spoofing",
                            timestamp=time.time(),
                            details={
                                "exchange": exchange,
                                "side": side,
                                "price": pl.price,
                                "quantity": tracked.quantity,
                                "elapsed_ms": round(elapsed_ms, 1),
                            },
                        ))
                else:
                    # Update quantity in place
                    store[key].quantity = pl.quantity
            elif pl.quantity >= self.SPOOF_MIN_BTC:
                # New large level — start tracking
                store[(exchange, pl.price)] = _TrackedLevel(
                    exchange=exchange,
                    price=pl.price,
                    quantity=pl.quantity,
                    seen_at=now,
                )

        return events

    def _purge_stale(self, now: float) -> None:
        """Remove tracked levels older than SPOOF_TTL_MS × 10 to avoid memory growth."""
        cutoff = now - self.SPOOF_TTL_MS / 100.0  # convert ms → 10× safety window
        for store in (self._large_bids, self._large_asks):
            stale = [k for k, v in store.items() if now - v.seen_at > cutoff]
            for k in stale:
                del store[k]


# ---------------------------------------------------------------------------
# Main metrics calculator
# ---------------------------------------------------------------------------

class MetricsCalculator:
    """Stateful metrics engine — call :meth:`calculate` after every aggregated update."""

    OBI_DEPTH: int = 30                    # levels for OBI
    OBI_HISTORY_MAXLEN: int = 600          # 60 s @ 100 ms cadence
    OBI_SPIKE_THRESHOLD: float = 0.30

    SPREAD_THRESHOLD_BPS: float = 20.0     # 20 bps

    DIVERGENCE_THRESHOLD_USD: float = 50.0

    WALL_MULTIPLIER: float = 10.0          # N × average level qty
    WALL_MIN_LEVELS: int = 5               # need at least N levels to compute average

    SWEEP_LEVELS: int = 3                  # best price moves ≥ N levels in one update

    def __init__(self) -> None:
        self._prev_obi: float = 0.0
        self._obi_history: deque[float] = deque(maxlen=self.OBI_HISTORY_MAXLEN)
        self._prev_best_bid: float = 0.0
        self._prev_best_ask: float = 0.0
        self._spoofing = SpoofingTracker()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def calculate(
        self,
        bids: list[PriceLevel],
        asks: list[PriceLevel],
        exchange_best_bids: dict[str, float],
        last_delta_exchange: str,
        delta_bids: list[PriceLevel],
        delta_asks: list[PriceLevel],
    ) -> MetricsResult:
        """
        Compute OBI and run all anomaly detectors.

        Parameters
        ----------
        bids / asks:
            Merged aggregated book (full visible depth).
        exchange_best_bids:
            Map of exchange → best bid price (for divergence check).
        last_delta_exchange:
            Which exchange produced the triggering delta.
        delta_bids / delta_asks:
            Raw delta levels from the triggering exchange (for spoofing tracker).
        """
        best_bid = bids[0].price if bids else 0.0
        best_ask = asks[0].price if asks else 0.0

        obi, bid_vol, ask_vol = self._compute_obi(bids, asks)
        self._obi_history.append(obi)

        spread_bps = (
            (best_ask - best_bid) / best_bid * 10_000.0
            if best_bid > 0
            else 0.0
        )

        anomalies: list[AnomalyEvent] = []

        # --- OBI spike ---
        if evt := self._check_obi_spike(obi):
            anomalies.append(evt)

        # --- Spread widening ---
        if evt := self._check_spread(best_bid, best_ask):
            anomalies.append(evt)

        # --- Exchange divergence ---
        if evt := self._check_divergence(exchange_best_bids):
            anomalies.append(evt)

        # --- Wall detection ---
        anomalies.extend(self._check_walls(bids, asks))

        # --- Sweep detection ---
        anomalies.extend(self._check_sweep(best_bid, best_ask, bids, asks))

        # --- Spoofing ---
        anomalies.extend(
            self._spoofing.update(last_delta_exchange, delta_bids, delta_asks)
        )

        # Update state
        self._prev_obi = obi
        self._prev_best_bid = best_bid
        self._prev_best_ask = best_ask

        return MetricsResult(
            obi=round(obi, 6),
            bid_volume=round(bid_vol, 4),
            ask_volume=round(ask_vol, 4),
            best_bid=best_bid,
            best_ask=best_ask,
            spread_bps=round(spread_bps, 4),
            anomalies=anomalies,
        )

    @property
    def obi_history(self) -> list[float]:
        return list(self._obi_history)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _compute_obi(
        self, bids: list[PriceLevel], asks: list[PriceLevel]
    ) -> tuple[float, float, float]:
        bid_vol = sum(pl.quantity for pl in bids[: self.OBI_DEPTH])
        ask_vol = sum(pl.quantity for pl in asks[: self.OBI_DEPTH])
        total = bid_vol + ask_vol
        obi = (bid_vol - ask_vol) / total if total > 0 else 0.0
        return obi, bid_vol, ask_vol

    def _check_obi_spike(self, obi: float) -> Optional[AnomalyEvent]:
        delta = abs(obi - self._prev_obi)
        if delta >= self.OBI_SPIKE_THRESHOLD:
            return AnomalyEvent(
                type="obi_spike",
                timestamp=time.time(),
                details={
                    "previous_obi": round(self._prev_obi, 6),
                    "current_obi": round(obi, 6),
                    "delta": round(delta, 6),
                },
            )
        return None

    def _check_spread(self, best_bid: float, best_ask: float) -> Optional[AnomalyEvent]:
        if best_bid <= 0:
            return None
        spread_bps = (best_ask - best_bid) / best_bid * 10_000.0
        if spread_bps >= self.SPREAD_THRESHOLD_BPS:
            return AnomalyEvent(
                type="spread_widening",
                timestamp=time.time(),
                details={
                    "best_bid": best_bid,
                    "best_ask": best_ask,
                    "spread_bps": round(spread_bps, 4),
                },
            )
        return None

    def _check_divergence(
        self, exchange_best_bids: dict[str, float]
    ) -> Optional[AnomalyEvent]:
        valid = {ex: p for ex, p in exchange_best_bids.items() if p > 0}
        if len(valid) < 2:
            return None
        spread = max(valid.values()) - min(valid.values())
        if spread >= self.DIVERGENCE_THRESHOLD_USD:
            return AnomalyEvent(
                type="exchange_divergence",
                timestamp=time.time(),
                details={
                    "spread_usd": round(spread, 2),
                    "by_exchange": {ex: round(p, 2) for ex, p in valid.items()},
                },
            )
        return None

    def _check_walls(
        self, bids: list[PriceLevel], asks: list[PriceLevel]
    ) -> list[AnomalyEvent]:
        events: list[AnomalyEvent] = []
        for side_label, levels in (("bid", bids), ("ask", asks)):
            if len(levels) < self.WALL_MIN_LEVELS:
                continue
            avg_qty = sum(pl.quantity for pl in levels) / len(levels)
            if avg_qty <= 0:
                continue
            for pl in levels:
                if pl.quantity >= avg_qty * self.WALL_MULTIPLIER:
                    events.append(AnomalyEvent(
                        type="wall",
                        timestamp=time.time(),
                        details={
                            "side": side_label,
                            "price": pl.price,
                            "quantity": round(pl.quantity, 4),
                            "avg_quantity": round(avg_qty, 4),
                            "multiplier": round(pl.quantity / avg_qty, 1),
                        },
                    ))
        return events

    def _check_sweep(
        self,
        best_bid: float,
        best_ask: float,
        bids: list[PriceLevel],
        asks: list[PriceLevel],
    ) -> list[AnomalyEvent]:
        events: list[AnomalyEvent] = []

        if self._prev_best_bid > 0 and bids:
            # Count how many previous bid levels have been consumed
            consumed = sum(
                1 for pl in bids if pl.price < self._prev_best_bid
            )
            if consumed == 0:
                # Check in the opposite direction — bid swept down
                dropped = sum(
                    1
                    for pl in bids
                    if self._prev_best_bid - pl.price > 0
                    and pl.price <= self._prev_best_bid
                )
                # Simple heuristic: best bid moved by ≥ SWEEP_LEVELS price steps
                bid_drop_levels = sum(
                    1
                    for pl in bids
                    if pl.price < self._prev_best_bid
                )
                if bid_drop_levels >= self.SWEEP_LEVELS:
                    events.append(AnomalyEvent(
                        type="sweep",
                        timestamp=time.time(),
                        details={
                            "side": "bid",
                            "from_price": self._prev_best_bid,
                            "to_price": best_bid,
                            "levels_consumed": bid_drop_levels,
                        },
                    ))

        if self._prev_best_ask > 0 and asks:
            ask_rise_levels = sum(
                1 for pl in asks if pl.price > self._prev_best_ask
            )
            if ask_rise_levels >= self.SWEEP_LEVELS:
                events.append(AnomalyEvent(
                    type="sweep",
                    timestamp=time.time(),
                    details={
                        "side": "ask",
                        "from_price": self._prev_best_ask,
                        "to_price": best_ask,
                        "levels_consumed": ask_rise_levels,
                    },
                ))

        return events
