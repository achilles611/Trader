from __future__ import annotations

from .config import BotConfig, StrategyProfile
from .indicators import ema, rsi
from .models import Candle, NetworkScores, Position, StrategyDecision
from .network import NeuralNetwork


FEATURE_NAMES = [
    "close_norm",
    "previous_close_norm",
    "return_1",
    "return_3",
    "return_5",
    "high_low_spread_pct",
    "volume_norm",
    "fast_ema_ratio",
    "slow_ema_ratio",
    "ema_gap_pct",
    "fast_minus_slow_norm",
    "rsi_norm",
    "trend_up",
    "trend_down",
    "bullish_cross",
    "bearish_cross",
    "momentum_resume_up",
    "momentum_resume_down",
    "pullback_detected_long",
    "pullback_detected_short",
    "near_recent_high",
    "near_recent_low",
    "efficiency_ratio",
    "market_state_trending_flag",
]


def _bool_score(value: bool) -> float:
    return 1.0 if value else 0.0


class MomentumStrategy:
    def __init__(
        self,
        config: BotConfig,
        *,
        profile: StrategyProfile | None = None,
        network: NeuralNetwork | None = None,
        profile_name: str = "default",
    ) -> None:
        self.config = config
        self.profile = profile
        self.network = network
        self.profile_name = profile_name

    def evaluate(self, candles: list[Candle], position: Position | None) -> StrategyDecision:
        profile = self.profile
        if profile is None:
            profile = StrategyProfile(
                entry_threshold_long=0.72,
                entry_threshold_short=0.72,
                weight_trend=1.0,
                weight_pullback=1.0,
                weight_momentum=1.0,
                weight_cross=1.0,
                weight_rsi=1.0,
                weight_near_extreme_penalty=1.0,
                weight_network=0.15,
                rule_weight=0.85,
                block_entries_in_chop=self.config.block_entries_in_chop,
                allow_near_recent_high_long=False,
                allow_near_recent_low_short=False,
                allow_countertrend=False,
                max_hold_seconds=self.config.max_trade_duration_minutes * 60,
                cooldown_after_loss_seconds=self.config.cooldown_after_loss_seconds,
                cooldown_after_win_seconds=self.config.cooldown_after_win_seconds,
                flip_cooldown_seconds=self.config.flip_cooldown_seconds,
                min_confirmation_signals=self.config.min_confirmation_signals,
                aggressive_entries=self.config.aggressive_entries,
            )

        minimum_candles = max(
            self.config.slow_ema_period + 6,
            self.config.rsi_period + 6,
            self.config.pullback_lookback_candles + 6,
            self.config.market_state_lookback_candles + 6,
        )
        if len(candles) < minimum_candles:
            return StrategyDecision(action="hold", reason="warming_up")

        closes = [candle.close for candle in candles]
        fast_ema_values = ema(closes, self.config.fast_ema_period)
        slow_ema_values = ema(closes, self.config.slow_ema_period)
        rsi_values = rsi(closes, self.config.rsi_period)

        current_candle = candles[-1]
        current_close = current_candle.close
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
        cross_required = (market_state == "CHOPPY") or (not profile.aggressive_entries)

        feature_vector = self._feature_vector(
            candles,
            current_close=current_close,
            previous_close=previous_close,
            current_fast=current_fast,
            current_slow=current_slow,
            current_rsi=current_rsi,
            trend_up=trend_up,
            trend_down=trend_down,
            bullish_cross=bullish_cross,
            bearish_cross=bearish_cross,
            momentum_resume_up=momentum_resume_up,
            momentum_resume_down=momentum_resume_down,
            pullback_detected_long=pullback_detected_long,
            pullback_detected_short=pullback_detected_short,
            near_recent_high=near_recent_high,
            near_recent_low=near_recent_low,
            efficiency_ratio=efficiency_ratio,
            ema_gap_pct=ema_gap_pct,
        )
        network_scores = (
            self.network.forward(feature_vector)
            if self.network is not None
            else NetworkScores(prob_win_long=0.5, prob_win_short=0.5, hidden_activations=[], version="rule-only")
        )

        long_confirmation_count = sum(
            [
                trend_up,
                pullback_detected_long,
                momentum_resume_up,
                bullish_cross,
                long_rsi_ok,
            ]
        )
        short_confirmation_count = sum(
            [
                trend_down,
                pullback_detected_short,
                momentum_resume_down,
                bearish_cross,
                short_rsi_ok,
            ]
        )

        long_rule_score = self._normalized_rule_score(
            trend_component=profile.weight_trend * _bool_score(trend_up),
            pullback_component=profile.weight_pullback * _bool_score(pullback_detected_long),
            momentum_component=profile.weight_momentum * _bool_score(momentum_resume_up or strong_push),
            cross_component=profile.weight_cross * _bool_score(bullish_cross),
            rsi_component=profile.weight_rsi * _bool_score(long_rsi_ok),
            penalty_component=profile.weight_near_extreme_penalty
            if near_recent_high and not profile.allow_near_recent_high_long
            else 0.0,
            reversal_bonus=abs(profile.weight_near_extreme_penalty)
            if profile.allow_countertrend and near_recent_low
            else 0.0,
            bias=profile.long_bias,
            profile=profile,
        )
        short_rule_score = self._normalized_rule_score(
            trend_component=profile.weight_trend * _bool_score(trend_down),
            pullback_component=profile.weight_pullback * _bool_score(pullback_detected_short),
            momentum_component=profile.weight_momentum * _bool_score(momentum_resume_down),
            cross_component=profile.weight_cross * _bool_score(bearish_cross),
            rsi_component=profile.weight_rsi * _bool_score(short_rsi_ok),
            penalty_component=profile.weight_near_extreme_penalty
            if near_recent_low and not profile.allow_near_recent_low_short
            else 0.0,
            reversal_bonus=abs(profile.weight_near_extreme_penalty)
            if profile.allow_countertrend and near_recent_high
            else 0.0,
            bias=profile.short_bias,
            profile=profile,
        )

        final_long_score = (
            profile.rule_weight * long_rule_score
            + profile.weight_network * network_scores.prob_win_long
            + profile.exploration_bonus
        )
        final_short_score = (
            profile.rule_weight * short_rule_score
            + profile.weight_network * network_scores.prob_win_short
            + profile.exploration_bonus
        )

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
            "cross_required": cross_required,
            "long_score": long_confirmation_count,
            "short_score": short_confirmation_count,
            "long_rule_score": long_rule_score,
            "short_rule_score": short_rule_score,
            "final_long_score": final_long_score,
            "final_short_score": final_short_score,
            "network_prob_win_long": network_scores.prob_win_long,
            "network_prob_win_short": network_scores.prob_win_short,
            "selected_profile_name": self.profile_name,
            "selected_network_version": network_scores.version,
            "feature_names": FEATURE_NAMES,
            "feature_vector": feature_vector,
        }

        if position is None:
            long_market_state_ok = self._entry_market_state_ok(
                market_state=market_state,
                confirmation_count=long_confirmation_count,
                final_score=final_long_score,
                profile=profile,
            )
            short_market_state_ok = self._entry_market_state_ok(
                market_state=market_state,
                confirmation_count=short_confirmation_count,
                final_score=final_short_score,
                profile=profile,
            )
            indicators["long_entry_market_state_ok"] = long_market_state_ok
            indicators["short_entry_market_state_ok"] = short_market_state_ok
            indicators["entry_market_state_ok"] = long_market_state_ok or short_market_state_ok

            if profile.block_entries_in_chop and market_state == "CHOPPY" and not indicators["entry_market_state_ok"]:
                return StrategyDecision(
                    action="hold",
                    reason="blocked_choppy_market",
                    market_state=market_state,
                    indicators=indicators,
                    network_scores=network_scores,
                )

            if near_recent_high and trend_up and not profile.allow_near_recent_high_long and not strong_push:
                return StrategyDecision(
                    action="hold",
                    reason="blocked_near_recent_high_weak",
                    market_state=market_state,
                    indicators=indicators,
                    network_scores=network_scores,
                )
            if near_recent_low and trend_down and not profile.allow_near_recent_low_short:
                return StrategyDecision(
                    action="hold",
                    reason="blocked_near_recent_low",
                    market_state=market_state,
                    indicators=indicators,
                    network_scores=network_scores,
                )

            countertrend_long_ready = (
                profile.allow_countertrend
                and near_recent_low
                and long_rsi_ok
                and (momentum_resume_up or bullish_cross or profile.aggressive_entries)
            )
            countertrend_short_ready = (
                profile.allow_countertrend
                and near_recent_high
                and short_rsi_ok
                and (momentum_resume_down or bearish_cross or profile.aggressive_entries)
            )

            if profile.prefer_countertrend:
                long_structural_ready = (
                    long_confirmation_count >= profile.min_confirmation_signals
                    and countertrend_long_ready
                )
                short_structural_ready = (
                    self.config.enable_shorts
                    and short_confirmation_count >= profile.min_confirmation_signals
                    and countertrend_short_ready
                )
            else:
                long_structural_ready = (
                    long_confirmation_count >= profile.min_confirmation_signals
                    and (
                        (trend_up and (pullback_detected_long or bullish_cross or profile.aggressive_entries))
                        or countertrend_long_ready
                    )
                    and (momentum_resume_up or bullish_cross or profile.allow_countertrend)
                )
                short_structural_ready = (
                    self.config.enable_shorts
                    and short_confirmation_count >= profile.min_confirmation_signals
                    and (
                        (trend_down and (pullback_detected_short or bearish_cross or profile.aggressive_entries))
                        or countertrend_short_ready
                    )
                    and (momentum_resume_down or bearish_cross or profile.allow_countertrend)
                )

            if long_structural_ready and cross_required and not bullish_cross:
                return StrategyDecision(
                    action="hold",
                    reason="blocked_missing_bullish_cross",
                    market_state=market_state,
                    indicators=indicators,
                    network_scores=network_scores,
                )
            if short_structural_ready and cross_required and not bearish_cross:
                return StrategyDecision(
                    action="hold",
                    reason="blocked_missing_bearish_cross",
                    market_state=market_state,
                    indicators=indicators,
                    network_scores=network_scores,
                )

            if (
                long_structural_ready
                and final_long_score >= profile.entry_threshold_long
                and long_market_state_ok
                and (bullish_cross if cross_required else True)
            ):
                reason = "pullback_resume_long" if pullback_detected_long else "profile_weighted_long"
                return StrategyDecision(
                    action="buy",
                    reason=reason,
                    stop_loss=current_close * (1 - self.config.stop_loss_pct),
                    take_profit=current_close * (1 + self.config.take_profit_pct),
                    trailing_stop=current_close
                    * (1 - self.config.trailing_stop_pct_for_market_state(market_state)),
                    market_state=market_state,
                    indicators=indicators,
                    network_scores=network_scores,
                )

            if (
                short_structural_ready
                and final_short_score >= profile.entry_threshold_short
                and short_market_state_ok
                and (bearish_cross if cross_required else True)
            ):
                reason = "pullback_resume_short" if pullback_detected_short else "profile_weighted_short"
                return StrategyDecision(
                    action="short",
                    reason=reason,
                    stop_loss=current_close * (1 + self.config.stop_loss_pct),
                    take_profit=current_close * (1 - self.config.take_profit_pct),
                    trailing_stop=current_close
                    * (1 + self.config.trailing_stop_pct_for_market_state(market_state)),
                    market_state=market_state,
                    indicators=indicators,
                    network_scores=network_scores,
                )

            return StrategyDecision(
                action="hold",
                reason="no_entry",
                market_state=market_state,
                indicators=indicators,
                network_scores=network_scores,
            )

        if position.side == "long":
            if bearish_cross and current_close < current_fast:
                return StrategyDecision(
                    action="sell",
                    reason="trend_reversal",
                    market_state=market_state,
                    indicators=indicators,
                    network_scores=network_scores,
                )
            return StrategyDecision(
                action="hold",
                reason="manage_open_position",
                market_state=market_state,
                indicators=indicators,
                network_scores=network_scores,
            )

        if position.side == "short":
            if bullish_cross and current_close > current_fast:
                return StrategyDecision(
                    action="cover",
                    reason="trend_reversal",
                    market_state=market_state,
                    indicators=indicators,
                    network_scores=network_scores,
                )
            return StrategyDecision(
                action="hold",
                reason="manage_open_position",
                market_state=market_state,
                indicators=indicators,
                network_scores=network_scores,
            )

        return StrategyDecision(
            action="hold",
            reason="manage_open_position",
            market_state=market_state,
            indicators=indicators,
            network_scores=network_scores,
        )

    def _normalized_rule_score(
        self,
        *,
        trend_component: float,
        pullback_component: float,
        momentum_component: float,
        cross_component: float,
        rsi_component: float,
        penalty_component: float,
        reversal_bonus: float,
        bias: float,
        profile: StrategyProfile,
    ) -> float:
        positive_total = max(
            1e-6,
            profile.weight_trend
            + profile.weight_pullback
            + profile.weight_momentum
            + profile.weight_cross
            + profile.weight_rsi
            + abs(bias)
            + reversal_bonus,
        )
        raw = (
            trend_component
            + pullback_component
            + momentum_component
            + cross_component
            + rsi_component
            + reversal_bonus
            + bias
            - max(0.0, penalty_component)
        )
        return max(0.0, min(1.5, raw / positive_total))

    def _entry_market_state_ok(
        self,
        *,
        market_state: str,
        confirmation_count: int,
        final_score: float,
        profile: StrategyProfile,
    ) -> bool:
        if market_state == "TRENDING":
            return True
        if not profile.block_entries_in_chop:
            return True
        return confirmation_count >= self.config.chop_high_confidence_signals or final_score >= max(
            profile.entry_threshold_long,
            profile.entry_threshold_short,
        ) + 0.1

    def _feature_vector(
        self,
        candles: list[Candle],
        *,
        current_close: float,
        previous_close: float,
        current_fast: float,
        current_slow: float,
        current_rsi: float,
        trend_up: bool,
        trend_down: bool,
        bullish_cross: bool,
        bearish_cross: bool,
        momentum_resume_up: bool,
        momentum_resume_down: bool,
        pullback_detected_long: bool,
        pullback_detected_short: bool,
        near_recent_high: bool,
        near_recent_low: bool,
        efficiency_ratio: float,
        ema_gap_pct: float,
    ) -> list[float]:
        current_volume = candles[-1].volume
        recent_volumes = [candle.volume for candle in candles[-20:]]
        average_volume = sum(recent_volumes) / max(1, len(recent_volumes))
        high_low_spread_pct = (candles[-1].high - candles[-1].low) / current_close if current_close > 0 else 0.0

        def _return(period: int) -> float:
            if len(candles) <= period:
                return 0.0
            prior = candles[-period - 1].close
            if prior <= 0:
                return 0.0
            return (current_close - prior) / prior

        slow_anchor = current_slow if current_slow > 0 else current_close
        return [
            (current_close / slow_anchor) - 1 if slow_anchor else 0.0,
            (previous_close / slow_anchor) - 1 if slow_anchor else 0.0,
            _return(1),
            _return(3),
            _return(5),
            high_low_spread_pct,
            (current_volume / average_volume) - 1 if average_volume > 0 else 0.0,
            current_fast / current_close if current_close > 0 else 0.0,
            current_slow / current_close if current_close > 0 else 0.0,
            ema_gap_pct,
            (current_fast - current_slow) / current_close if current_close > 0 else 0.0,
            current_rsi / 100.0,
            _bool_score(trend_up),
            _bool_score(trend_down),
            _bool_score(bullish_cross),
            _bool_score(bearish_cross),
            _bool_score(momentum_resume_up),
            _bool_score(momentum_resume_down),
            _bool_score(pullback_detected_long),
            _bool_score(pullback_detected_short),
            _bool_score(near_recent_high),
            _bool_score(near_recent_low),
            efficiency_ratio,
            1.0 if self._market_state_label(efficiency_ratio, ema_gap_pct) == "TRENDING" else 0.0,
        ]

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
        return self._market_state_label(efficiency_ratio, ema_gap_pct), efficiency_ratio, ema_gap_pct

    def _market_state_label(self, efficiency_ratio: float, ema_gap_pct: float) -> str:
        return (
            "TRENDING"
            if efficiency_ratio >= self.config.market_trend_efficiency_threshold
            and ema_gap_pct >= self.config.market_trend_ema_gap_pct
            else "CHOPPY"
        )
