from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import Protocol

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
