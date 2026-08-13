from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import timedelta
from datetime import datetime, timezone
from typing import Any, Callable, Iterable, Mapping, Protocol

from .hyperliquid import HyperliquidPublicAdapter
from .models import as_utc


@dataclass(frozen=True)
class MarketPrice:
    symbol: str
    price: float
    timestamp: object
    source: str
    quality: str
    requested_for_timestamp: object | None = None
    resolution: str | None = None


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
            quality="coarse_prior_candle_close_proxy_not_historical_l2", requested_for_timestamp=at, resolution="1m",
        )

    def current_order_book(self, symbol: str) -> OrderBook:
        payload = self.adapter.info({"type": "l2Book", "coin": symbol})
        levels = payload.get("levels", []) if isinstance(payload, dict) else []
        bids = tuple((float(item["px"]), float(item["sz"])) for item in (levels[0] if len(levels) > 0 else []))
        asks = tuple((float(item["px"]), float(item["sz"])) for item in (levels[1] if len(levels) > 1 else []))
        return OrderBook(symbol=symbol, timestamp=as_utc(payload.get("time") if isinstance(payload, dict) else None),
                         bids=bids, asks=asks, source="hyperliquid_l2Book", quality="current_public_l2")


class CachedHistoricalMarketData:
    """Run-scoped immutable candle evidence cache for research replays.

    It deliberately buckets requests before acquisition and freezes after
    ``prime``.  Baseline, slippage, and latency scenarios can then reuse the
    same public candle proxy without issuing one remote request per event or
    scenario.  An optional durable loader/storer preserves the exact evidence
    selected for an immutable analysis run.
    """

    def __init__(
        self, delegate: MarketDataProvider, *, bucket_seconds: int = 60,
        load: Callable[[str, str], Mapping[str, object] | None] | None = None,
        store: Callable[[Mapping[str, object]], None] | None = None,
    ) -> None:
        if bucket_seconds <= 0:
            raise ValueError("bucket_seconds must be positive.")
        self.delegate = delegate
        self.bucket_seconds = bucket_seconds
        self._load = load
        self._store = store
        self._cache: dict[tuple[str, str], MarketPrice | None] = {}
        self._metadata: dict[tuple[str, str], dict[str, object]] = {}
        self._frozen = False

    def prime(self, requests: Iterable[tuple[str, object]]) -> None:
        """Acquire every unique symbol/time bucket once, then disallow fetches."""
        requested_by_key: dict[tuple[str, str], datetime] = {}
        for symbol, timestamp in requests:
            normalized_symbol = str(symbol).upper()
            requested_at = as_utc(timestamp)
            key = (normalized_symbol, self._bucket_iso(requested_at))
            # The earliest request in a bucket is deterministic and avoids
            # choosing a later candle reference when events arrive unordered.
            if key not in requested_by_key or requested_at < requested_by_key[key]:
                requested_by_key[key] = requested_at
        for (symbol, _), requested_at in sorted(requested_by_key.items()):
            self._get(symbol, requested_at, allow_fetch=True)
        self._frozen = True

    def historical_price(self, symbol: str, timestamp: object) -> MarketPrice | None:
        return self._get(str(symbol).upper(), as_utc(timestamp), allow_fetch=not self._frozen)

    def current_price(self, symbol: str) -> MarketPrice:
        return self.delegate.current_price(symbol)

    def current_order_book(self, symbol: str) -> OrderBook:
        return self.delegate.current_order_book(symbol)

    def evidence_metadata(self) -> list[dict[str, object]]:
        return [self._metadata[key] for key in sorted(self._metadata)]

    def _get(self, symbol: str, requested_at: datetime, *, allow_fetch: bool) -> MarketPrice | None:
        bucket = self._bucket_iso(requested_at)
        key = (symbol, bucket)
        if key in self._cache:
            cached = self._cache[key]
            return replace(cached, requested_for_timestamp=requested_at) if cached is not None else None
        persisted = self._load(symbol, bucket) if self._load else None
        if persisted is not None:
            price = _market_price_from_evidence(persisted, requested_at)
            self._cache[key] = price
            self._metadata[key] = dict(persisted)
            return replace(price, requested_for_timestamp=requested_at) if price is not None else None
        if not allow_fetch:
            return None
        try:
            price = self.delegate.historical_price(symbol, requested_at)
        except Exception:
            # Historical evidence is optional research input.  Preserve a
            # bounded explicit miss rather than retrying remotely per replay.
            price = None
        metadata: dict[str, object] = {
            "symbol": symbol,
            "bucket_timestamp": bucket,
            "price": price.price if price is not None else None,
            "source": price.source if price is not None else "unavailable",
            "quality": price.quality if price is not None else "missing_historical_price",
            "timestamp": as_utc(price.timestamp).isoformat() if price is not None else None,
            "requested_for_timestamp": requested_at.isoformat(),
            "resolution": price.resolution if price is not None and price.resolution else f"{self.bucket_seconds}s",
        }
        self._cache[key] = price
        self._metadata[key] = metadata
        if self._store:
            self._store(metadata)
        return replace(price, requested_for_timestamp=requested_at) if price is not None else None

    def _bucket_iso(self, timestamp: object) -> str:
        at = as_utc(timestamp)
        epoch = int(at.timestamp())
        bucket_epoch = epoch - (epoch % self.bucket_seconds)
        return datetime.fromtimestamp(bucket_epoch, tz=timezone.utc).isoformat()


def _market_price_from_evidence(evidence: Mapping[str, object], requested_at: datetime) -> MarketPrice | None:
    value = evidence.get("price")
    if value in (None, ""):
        return None
    return MarketPrice(
        symbol=str(evidence["symbol"]), price=float(value), timestamp=as_utc(evidence.get("timestamp") or requested_at),
        source=str(evidence.get("source") or "unknown"), quality=str(evidence.get("quality") or "unknown"),
        requested_for_timestamp=requested_at, resolution=str(evidence.get("resolution") or "1m"),
    )
