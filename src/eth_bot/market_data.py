from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from typing import Any

import requests

from .config import GRANULARITY_TO_SECONDS
from .models import Candle, MarketFrame, ProductInfo


class MarketDataError(RuntimeError):
    pass


class TransientMarketDataError(MarketDataError):
    pass


class FatalMarketDataError(MarketDataError):
    pass


class CoinbasePublicClient:
    BASE_URL = "https://api.coinbase.com/api/v3/brokerage/market"

    def __init__(
        self,
        timeout_seconds: int = 20,
        max_retries: int = 3,
        retry_backoff_seconds: float = 2.0,
    ) -> None:
        self.session = requests.Session()
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self.retry_backoff_seconds = retry_backoff_seconds

    def _get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        attempts = self.max_retries + 1
        last_error: Exception | None = None

        for attempt in range(1, attempts + 1):
            try:
                response = self.session.get(
                    f"{self.BASE_URL}{path}",
                    params=params,
                    timeout=self.timeout_seconds,
                )
                response.raise_for_status()
                return response.json()
            except requests.Timeout as exc:
                last_error = exc
                if attempt >= attempts:
                    raise TransientMarketDataError(
                        f"Coinbase market data timeout after {attempts} attempts for {path}"
                    ) from exc
            except requests.ConnectionError as exc:
                last_error = exc
                if attempt >= attempts:
                    raise TransientMarketDataError(
                        f"Coinbase market data connection failure after {attempts} attempts for {path}"
                    ) from exc
            except requests.HTTPError as exc:
                status_code = exc.response.status_code if exc.response is not None else None
                if status_code in {408, 409, 425, 429, 500, 502, 503, 504}:
                    last_error = exc
                    if attempt >= attempts:
                        raise TransientMarketDataError(
                            f"Coinbase market data HTTP {status_code} after {attempts} attempts for {path}"
                        ) from exc
                else:
                    raise FatalMarketDataError(
                        f"Coinbase market data request failed with HTTP {status_code} for {path}"
                    ) from exc
            except requests.RequestException as exc:
                raise FatalMarketDataError(f"Coinbase market data request failed for {path}: {exc}") from exc

            if attempt < attempts and self.retry_backoff_seconds > 0:
                time.sleep(self.retry_backoff_seconds * attempt)

        raise TransientMarketDataError(f"Coinbase market data request failed for {path}: {last_error}")

    def get_product_info(self, product_id: str) -> ProductInfo:
        payload = self._get(f"/products/{product_id}")
        return ProductInfo(
            product_id=payload["product_id"],
            price=float(payload["price"]),
            base_increment=float(payload["base_increment"]),
            quote_increment=float(payload["quote_increment"]),
            base_min_size=float(payload["base_min_size"]),
            quote_min_size=float(payload["quote_min_size"]),
            trading_disabled=bool(payload.get("trading_disabled", False)),
        )

    def get_candles(
        self,
        product_id: str,
        granularity: str,
        limit: int,
        end: datetime | None = None,
    ) -> list[Candle]:
        if granularity not in GRANULARITY_TO_SECONDS:
            raise ValueError(f"Unsupported granularity: {granularity}")

        end = end or datetime.now(timezone.utc)
        seconds_per_candle = GRANULARITY_TO_SECONDS[granularity]
        remaining = limit
        cursor_end = end
        candles: list[Candle] = []

        while remaining > 0:
            batch_size = min(remaining, 350)
            cursor_start = cursor_end - timedelta(seconds=seconds_per_candle * batch_size)
            params = {
                "start": str(int(cursor_start.timestamp())),
                "end": str(int(cursor_end.timestamp())),
                "granularity": granularity,
                "limit": batch_size,
            }
            payload = self._get(f"/products/{product_id}/candles", params=params)
            batch = [
                Candle(
                    start=datetime.fromtimestamp(int(item["start"]), timezone.utc),
                    low=float(item["low"]),
                    high=float(item["high"]),
                    open=float(item["open"]),
                    close=float(item["close"]),
                    volume=float(item["volume"]),
                )
                for item in payload.get("candles", [])
            ]
            batch.sort(key=lambda candle: candle.start)
            if not batch:
                break

            candles = batch + candles
            remaining -= len(batch)
            cursor_end = batch[0].start - timedelta(seconds=seconds_per_candle)

            if len(batch) < batch_size:
                break

        if len(candles) > limit:
            candles = candles[-limit:]
        return candles

    def get_market_frame(
        self,
        *,
        product_id: str,
        granularity: str,
        limit: int,
    ) -> MarketFrame:
        product = self.get_product_info(product_id)
        candles = self.get_candles(
            product_id=product_id,
            granularity=granularity,
            limit=limit,
        )
        if not candles:
            raise FatalMarketDataError("No candles returned from Coinbase public market data.")
        return MarketFrame(
            timestamp=candles[-1].start,
            product=product,
            candles=candles,
            current_price=candles[-1].close,
        )
