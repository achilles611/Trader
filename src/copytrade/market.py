from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from datetime import datetime
from typing import Any, Protocol

from .hyperliquid import HyperliquidPublicAdapter
from .models import as_utc


@dataclass(frozen=True)
class MarketPrice:
    symbol: str
    price: float
    timestamp: object
    source: str
    quality: str


@dataclass(frozen=True)
class MarketObservation:
    symbol: str
    price: float
    timestamp: datetime
    received_at: datetime
    source: str
    quality: str
    bid: float | None = None
    ask: float | None = None


class LiveMarketCache:
    """In-memory websocket reference cache; no REST work in the fill hot path."""

    def __init__(self) -> None:
        self._latest: dict[str, MarketObservation] = {}

    def update_mid(self, symbol: str, price: float, *, timestamp: object | None = None, received_at: object | None = None) -> MarketObservation:
        received = as_utc(received_at)
        observed = as_utc(timestamp) if timestamp is not None else received
        # An exchange timestamp later than local receipt cannot have been known
        # at receipt; use receipt time as the availability bound.
        if observed > received:
            observed = received
        item = MarketObservation(symbol.upper(), float(price), observed, received, "hyperliquid_allMids", "websocket_midpoint")
        self._latest[item.symbol] = item
        return item

    def latest_available(self, symbol: str, decision_at: object, max_age_ms: int) -> tuple[MarketObservation | None, float | None]:
        item = self._latest.get(symbol.upper())
        decision = as_utc(decision_at)
        if item is None or item.received_at > decision:
            return None, None
        age = max(0.0, (decision - item.received_at).total_seconds() * 1000)
        if age > max_age_ms:
            return None, age
        return item, age

    def symbols(self) -> set[str]:
        return set(self._latest)


@dataclass(frozen=True)
class OrderBook:
    symbol: str
    timestamp: object
    bids: tuple[tuple[float, float], ...]
    asks: tuple[tuple[float, float], ...]
    source: str
    quality: str


class MarketDataProvider(Protocol):
    """Market-data seam for public Hyperliquid data now and indexer L2 data later."""

    def current_price(self, symbol: str) -> MarketPrice: ...
    def historical_price(self, symbol: str, timestamp: object) -> MarketPrice | None: ...
    def current_order_book(self, symbol: str) -> OrderBook: ...


class HyperliquidMarketData:
    """Public mid/candle adapter with explicit historical-quality labeling."""

    def __init__(self, adapter: HyperliquidPublicAdapter) -> None:
        self.adapter = adapter

    def current_price(self, symbol: str) -> MarketPrice:
        payload = self.adapter.info({"type": "allMids"})
        mids = payload.get("mids", payload) if isinstance(payload, dict) else {}
        value = mids.get(symbol) if isinstance(mids, dict) else None
        if value is None:
            raise KeyError(f"No public Hyperliquid mid is available for {symbol}")
        return MarketPrice(symbol=symbol, price=float(value), timestamp=as_utc(None), source="hyperliquid_allMids", quality="public_mid")

    def historical_price(self, symbol: str, timestamp: object) -> MarketPrice | None:
        at = as_utc(timestamp)
        candles = self.adapter.fetch_candle_snapshot(symbol, at - timedelta(minutes=2), at, "1m")
        if not isinstance(candles, list) or not candles:
            return None
        decision_ms = int(at.timestamp() * 1000)
        def close_ms(item: dict) -> int:
            return int(item.get("T") or item.get("endTime") or (int(item.get("t", 0)) + 60_000))
        eligible = [item for item in candles if isinstance(item, dict) and close_ms(item) <= decision_ms]
        if not eligible:
            return None
        closest = max(eligible, key=close_ms)
        return MarketPrice(
            symbol=symbol, price=float(closest["c"]), timestamp=as_utc(close_ms(closest)), source="hyperliquid_candleSnapshot",
            quality="coarse_prior_candle_close_proxy_not_historical_l2",
        )

    def current_order_book(self, symbol: str) -> OrderBook:
        payload = self.adapter.info({"type": "l2Book", "coin": symbol})
        levels = payload.get("levels", []) if isinstance(payload, dict) else []
        bids = tuple((float(item["px"]), float(item["sz"])) for item in (levels[0] if len(levels) > 0 else []))
        asks = tuple((float(item["px"]), float(item["sz"])) for item in (levels[1] if len(levels) > 1 else []))
        return OrderBook(symbol=symbol, timestamp=as_utc(payload.get("time") if isinstance(payload, dict) else None),
                         bids=bids, asks=asks, source="hyperliquid_l2Book", quality="current_public_l2")
