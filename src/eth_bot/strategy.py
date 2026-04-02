from __future__ import annotations

from .config import BotConfig
from .indicators import ema, rsi
from .models import Candle, Position, StrategyDecision


class MomentumStrategy:
    def __init__(self, config: BotConfig) -> None:
        self.config = config

    def evaluate(self, candles: list[Candle], position: Position | None) -> StrategyDecision:
        minimum_candles = max(
            self.config.slow_ema_period + 2,
            self.config.rsi_period + 2,
            self.config.pullback_lookback_candles + 2,
            self.config.market_state_lookback_candles + 2,
        )
        if len(candles) < minimum_candles:
            return StrategyDecision(action="hold", reason="warming_up")

        closes = [candle.close for candle in candles]
        fast_ema_values = ema(closes, self.config.fast_ema_period)
        slow_ema_values = ema(closes, self.config.slow_ema_period)
        rsi_values = rsi(closes, self.config.rsi_period)

        current_close = closes[-1]
        previous_close = closes[-2]
        current_fast = fast_ema_values[-1]
        previous_fast = fast_ema_values[-2]
        current_slow = slow_ema_values[-1]
        previous_slow = slow_ema_values[-2]
        current_rsi = rsi_values[-1]

        if current_rsi is None:
            return StrategyDecision(action="hold", reason="warming_up")

        recent_window = candles[-self.config.pullback_lookback_candles - 1 : -1]
        if not recent_window:
            return StrategyDecision(action="hold", reason="warming_up")

        recent_high = max(candle.high for candle in recent_window)
        recent_low = min(candle.low for candle in recent_window)
        recent_range = max(0.0, recent_high - recent_low)

        market_state, efficiency_ratio, ema_gap_pct = self._market_state(closes, current_fast, current_slow)
        bullish_cross = previous_fast <= previous_slow and current_fast > current_slow
        bearish_cross = previous_fast >= previous_slow and current_fast < current_slow
        trend_up = current_fast > current_slow and current_close > current_slow
        trend_down = current_fast < current_slow and current_close < current_slow
        momentum_resume_up = current_close > previous_close and current_close > current_fast
        momentum_resume_down = current_close < previous_close and current_close < current_fast
        strong_push = momentum_resume_up and current_close > previous_close * 1.001
        long_pullback_pct = (recent_high - current_close) / recent_high if recent_high > 0 else 0.0
        short_pullback_pct = (current_close - recent_low) / recent_low if recent_low > 0 else 0.0
        pullback_detected_long = trend_up and long_pullback_pct >= self.config.pullback_min_pct
        pullback_detected_short = trend_down and short_pullback_pct >= self.config.pullback_min_pct
        near_recent_high = current_close > recent_high * self.config.long_top_guard_pct
        near_recent_low = current_close < recent_low * self.config.short_bottom_guard_pct
        long_rsi_ok = self.config.rsi_entry_floor <= current_rsi <= self.config.rsi_entry_ceiling
        short_rsi_ok = self.config.short_rsi_entry_floor <= current_rsi <= self.config.short_rsi_entry_ceiling
        allow_trend_resume = not self.config.block_entries_in_chop or market_state == "TRENDING"
        cross_required = (market_state == "CHOPPY") or (not self.config.aggressive_entries)

        indicators = {
            "close": current_close,
            "previous_close": previous_close,
            "fast_ema": current_fast,
            "slow_ema": current_slow,
            "rsi": current_rsi,
            "recent_high": recent_high,
            "recent_low": recent_low,
            "recent_range": recent_range,
            "long_pullback_pct": long_pullback_pct * 100,
            "short_pullback_pct": short_pullback_pct * 100,
            "trend_up": trend_up,
            "trend_down": trend_down,
            "momentum_resume_up": momentum_resume_up,
            "momentum_resume_down": momentum_resume_down,
            "strong_push": strong_push,
            "bullish_cross": bullish_cross,
            "bearish_cross": bearish_cross,
            "long_rsi_ok": long_rsi_ok,
            "short_rsi_ok": short_rsi_ok,
            "pullback_detected_long": pullback_detected_long,
            "pullback_detected_short": pullback_detected_short,
            "near_recent_high": near_recent_high,
            "near_recent_low": near_recent_low,
            "market_state": market_state,
            "efficiency_ratio": efficiency_ratio,
            "ema_gap_pct": ema_gap_pct * 100,
            "allow_trend_resume": allow_trend_resume,
            "cross_required": cross_required,
        }

        if position is None:
            long_score = sum(
                [
                    trend_up,
                    pullback_detected_long,
                    momentum_resume_up,
                    bullish_cross,
                    long_rsi_ok,
                ]
            )
            short_score = sum(
                [
                    trend_down,
                    pullback_detected_short,
                    momentum_resume_down,
                    bearish_cross,
                    short_rsi_ok,
                ]
            )
            indicators["long_score"] = long_score
            indicators["short_score"] = short_score
            long_market_state_ok = market_state == "TRENDING" or (
                market_state == "CHOPPY" and long_score >= self.config.chop_high_confidence_signals
            )
            short_market_state_ok = market_state == "TRENDING" or (
                market_state == "CHOPPY" and short_score >= self.config.chop_high_confidence_signals
            )
            indicators["long_entry_market_state_ok"] = long_market_state_ok
            indicators["short_entry_market_state_ok"] = short_market_state_ok
            indicators["entry_market_state_ok"] = long_market_state_ok or short_market_state_ok

            if self.config.block_entries_in_chop and market_state == "CHOPPY" and not indicators["entry_market_state_ok"]:
                return StrategyDecision(
                    action="hold",
                    reason="blocked_choppy_market",
                    market_state=market_state,
                    indicators=indicators,
                )

            long_setup_ready = (
                long_rsi_ok
                and trend_up
                and pullback_detected_long
                and momentum_resume_up
                and long_score >= self.config.min_confirmation_signals
            )
            short_setup_ready = (
                self.config.enable_shorts
                and short_rsi_ok
                and not near_recent_low
                and trend_down
                and pullback_detected_short
                and momentum_resume_down
                and short_score >= self.config.min_confirmation_signals
            )

            if near_recent_high and trend_up and not strong_push:
                return StrategyDecision(
                    action="hold",
                    reason="blocked_near_recent_high_weak",
                    market_state=market_state,
                    indicators=indicators,
                )
            if near_recent_low and trend_down:
                return StrategyDecision(
                    action="hold",
                    reason="blocked_near_recent_low",
                    market_state=market_state,
                    indicators=indicators,
                )
            if long_setup_ready and cross_required and not bullish_cross:
                return StrategyDecision(
                    action="hold",
                    reason="blocked_missing_bullish_cross",
                    market_state=market_state,
                    indicators=indicators,
                )
            if short_setup_ready and cross_required and not bearish_cross:
                return StrategyDecision(
                    action="hold",
                    reason="blocked_missing_bearish_cross",
                    market_state=market_state,
                    indicators=indicators,
                )

            if long_setup_ready and (bullish_cross if cross_required else True):
                if not long_market_state_ok:
                    return StrategyDecision(
                        action="hold",
                        reason="blocked_choppy_market",
                        market_state=market_state,
                        indicators=indicators,
                    )
                reason = "pullback_resume_long" if pullback_detected_long else "trend_resume_long"
                return StrategyDecision(
                    action="buy",
                    reason=reason,
                    stop_loss=current_close * (1 - self.config.stop_loss_pct),
                    take_profit=current_close * (1 + self.config.take_profit_pct),
                    trailing_stop=current_close
                    * (1 - self.config.trailing_stop_pct_for_market_state(market_state)),
                    market_state=market_state,
                    indicators=indicators,
                )

            if short_setup_ready and (bearish_cross if cross_required else True):
                if not short_market_state_ok:
                    return StrategyDecision(
                        action="hold",
                        reason="blocked_choppy_market",
                        market_state=market_state,
                        indicators=indicators,
                    )
                reason = "pullback_resume_short" if pullback_detected_short else "trend_resume_short"
                return StrategyDecision(
                    action="short",
                    reason=reason,
                    stop_loss=current_close * (1 + self.config.stop_loss_pct),
                    take_profit=current_close * (1 - self.config.take_profit_pct),
                    trailing_stop=current_close
                    * (1 + self.config.trailing_stop_pct_for_market_state(market_state)),
                    market_state=market_state,
                    indicators=indicators,
                )
            return StrategyDecision(
                action="hold",
                reason="no_entry",
                market_state=market_state,
                indicators=indicators,
            )

        if position.side == "long":
            if bearish_cross and current_close < current_fast:
                return StrategyDecision(
                    action="sell",
                    reason="trend_reversal",
                    market_state=market_state,
                    indicators=indicators,
                )
            return StrategyDecision(
                action="hold",
                reason="manage_open_position",
                market_state=market_state,
                indicators=indicators,
            )

        if position.side == "short":
            if bullish_cross and current_close > current_fast:
                return StrategyDecision(
                    action="cover",
                    reason="trend_reversal",
                    market_state=market_state,
                    indicators=indicators,
                )
            return StrategyDecision(
                action="hold",
                reason="manage_open_position",
                market_state=market_state,
                indicators=indicators,
            )

        return StrategyDecision(
            action="hold",
            reason="manage_open_position",
            market_state=market_state,
            indicators=indicators,
        )

    def _market_state(
        self,
        closes: list[float],
        current_fast: float,
        current_slow: float,
    ) -> tuple[str, float, float]:
        lookback = min(self.config.market_state_lookback_candles, len(closes) - 1)
        if lookback <= 1:
            return "UNKNOWN", 0.0, 0.0

        recent_closes = closes[-lookback - 1 :]
        net_move = recent_closes[-1] - recent_closes[0]
        gross_move = sum(abs(recent_closes[index] - recent_closes[index - 1]) for index in range(1, len(recent_closes)))
        efficiency_ratio = abs(net_move) / gross_move if gross_move > 0 else 0.0
        ema_gap_pct = abs(current_fast - current_slow) / recent_closes[-1] if recent_closes[-1] > 0 else 0.0
        market_state = (
            "TRENDING"
            if efficiency_ratio >= self.config.market_trend_efficiency_threshold
            and ema_gap_pct >= self.config.market_trend_ema_gap_pct
            else "CHOPPY"
        )
        return market_state, efficiency_ratio, ema_gap_pct
