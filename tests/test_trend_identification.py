from __future__ import annotations

import unittest
from dataclasses import replace
from datetime import datetime, timedelta, timezone

from src.eth_bot.config import BotConfig
from src.eth_bot.models import Candle
from src.eth_bot.profiles import default_strategy_profile
from src.eth_bot.strategy import MomentumStrategy


def _build_candles(closes: list[float]) -> list[Candle]:
    start = datetime(2026, 4, 1, tzinfo=timezone.utc)
    candles: list[Candle] = []
    previous_close = closes[0]
    for index, close in enumerate(closes):
        open_price = previous_close
        high = max(open_price, close) * 1.001
        low = min(open_price, close) * 0.999
        candles.append(
            Candle(
                start=start + timedelta(minutes=5 * index),
                low=low,
                high=high,
                open=open_price,
                close=close,
                volume=1000 + index * 10,
            )
        )
        previous_close = close
    return candles


class TrendIdentificationTests(unittest.TestCase):
    def _config(self) -> BotConfig:
        base = BotConfig.from_env()
        return replace(
            base,
            short_rsi_entry_floor=0.0,
            short_rsi_entry_ceiling=100.0,
            rsi_entry_floor=0.0,
            rsi_entry_ceiling=100.0,
            market_state_lookback_candles=12,
            pullback_lookback_candles=12,
            pullback_min_pct=0.001,
        )

    def test_strong_downtrend_continuation_is_not_blocked_near_recent_low(self) -> None:
        config = self._config()
        profile = replace(
            default_strategy_profile(config),
            aggressive_entries=True,
            min_confirmation_signals=2,
            entry_threshold_short=0.45,
            weight_network=0.0,
            rule_weight=1.0,
        )
        strategy = MomentumStrategy(config, profile=profile)
        candles = _build_candles(
            [
                100.0,
                99.7,
                99.3,
                98.9,
                98.6,
                98.2,
                97.8,
                97.3,
                96.9,
                96.5,
                96.9,
                96.1,
                95.7,
                95.2,
                94.8,
                94.4,
                94.0,
                93.6,
                93.2,
                92.9,
                92.5,
                92.1,
                91.8,
                91.4,
                91.1,
                90.8,
                90.5,
                90.1,
                89.8,
                89.4,
            ]
        )

        decision = strategy.evaluate(candles, None)

        self.assertEqual(decision.action, "short")
        self.assertEqual(decision.market_state, "TRENDING")
        self.assertTrue(decision.indicators["trend_down"])
        self.assertTrue(decision.indicators["strong_trend_down"])
        self.assertTrue(decision.indicators["near_recent_low"])
        self.assertNotEqual(decision.reason, "blocked_near_recent_low")

    def test_choppy_sequence_does_not_register_as_directional_trend(self) -> None:
        config = self._config()
        strategy = MomentumStrategy(config, profile=default_strategy_profile(config))
        candles = _build_candles(
            [
                100.0,
                100.2,
                99.9,
                100.1,
                99.8,
                100.0,
                99.7,
                100.1,
                99.9,
                100.0,
                99.8,
                100.2,
                99.9,
                100.1,
                99.8,
                100.0,
                99.9,
                100.1,
                99.8,
                100.0,
                99.7,
                100.1,
                99.9,
                100.0,
                99.8,
                100.1,
                99.9,
                100.0,
                99.8,
                100.0,
            ]
        )

        decision = strategy.evaluate(candles, None)

        self.assertEqual(decision.market_state, "CHOPPY")
        self.assertFalse(decision.indicators["trend_up"])
        self.assertFalse(decision.indicators["trend_down"])
        self.assertEqual(decision.indicators["trend_bias"], "FLAT")


if __name__ == "__main__":
    unittest.main()
