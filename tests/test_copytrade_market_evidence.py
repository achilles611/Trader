from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from src.copytrade.analysis import _latency_evidence
from src.copytrade.backtest import CopyTradeBacktester
from src.copytrade.config import CopyTradeConfig
from src.copytrade.market import CachedHistoricalMarketData, MarketPrice, OrderBook
from src.copytrade.models import RawFill


WALLET = "0x1111111111111111111111111111111111111111"
T0 = datetime(2026, 1, 1, 0, 0, 10, tzinfo=timezone.utc)


class CountingHistoricalMarket:
    def __init__(self, *, available: bool = True) -> None:
        self.available = available
        self.calls = 0

    def historical_price(self, symbol: str, timestamp: object) -> MarketPrice | None:
        self.calls += 1
        if not self.available:
            return None
        return MarketPrice(
            symbol=symbol, price=100.0, timestamp=timestamp, source="fixture_candle",
            quality="coarse_prior_candle_close_proxy_not_historical_l2", requested_for_timestamp=timestamp,
            resolution="1m",
        )

    def current_price(self, symbol: str) -> MarketPrice:
        raise AssertionError("historical replay must not request a current price")

    def current_order_book(self, symbol: str) -> OrderBook:
        raise AssertionError("historical replay must not request an order book")


def fills() -> list[RawFill]:
    return [
        RawFill.from_hyperliquid({
            "coin": "BTC", "px": "100", "sz": "1", "side": "B", "startPosition": "0",
            "time": int(T0.timestamp() * 1000), "oid": 1, "tid": 1, "fee": "0",
        }, WALLET),
        RawFill.from_hyperliquid({
            "coin": "BTC", "px": "101", "sz": "1", "side": "A", "startPosition": "1",
            "time": int((T0 + timedelta(seconds=10)).timestamp() * 1000), "oid": 2, "tid": 2, "fee": "0",
        }, WALLET),
    ]


class HistoricalMarketEvidenceTests(unittest.TestCase):
    def test_one_bucketed_observation_is_reused_across_all_latency_scenarios(self) -> None:
        provider = CountingHistoricalMarket()
        market = CachedHistoricalMarketData(provider, bucket_seconds=60)
        # Both events and every configured latency are in the same 1-minute bucket.
        market.prime([("BTC", T0), ("BTC", T0 + timedelta(seconds=10))])
        curve = CopyTradeBacktester(CopyTradeConfig(), market_data=market).latency_decay_curve(fills())
        self.assertEqual(len(curve), len(CopyTradeConfig().backtest.detection_delays_ms))
        self.assertEqual(provider.calls, 1)
        evidence = market.evidence_metadata()
        self.assertEqual(len(evidence), 1)
        self.assertEqual(evidence[0]["quality"], "coarse_prior_candle_close_proxy_not_historical_l2")
        self.assertEqual(evidence[0]["resolution"], "1m")

    def test_unavailable_market_evidence_is_explicit_and_never_fabricates_latency_curve(self) -> None:
        provider = CountingHistoricalMarket(available=False)
        market = CachedHistoricalMarketData(provider, bucket_seconds=60)
        market.prime([("BTC", T0)])
        curve = CopyTradeBacktester(CopyTradeConfig(), market_data=market).latency_decay_curve(fills())
        self.assertEqual(curve, [])
        self.assertEqual(provider.calls, 1)
        self.assertEqual(_latency_evidence(curve)["status"], "unavailable")
        self.assertEqual(_latency_evidence(curve)["reason"], "missing_or_insufficient_historical_price_evidence")


if __name__ == "__main__":
    unittest.main()
