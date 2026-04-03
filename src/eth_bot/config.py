from __future__ import annotations

import math
import os
from dataclasses import dataclass, replace
from pathlib import Path

from dotenv import load_dotenv


GRANULARITY_TO_SECONDS = {
    "ONE_MINUTE": 60,
    "FIVE_MINUTE": 300,
    "FIFTEEN_MINUTE": 900,
    "THIRTY_MINUTE": 1800,
    "ONE_HOUR": 3600,
    "TWO_HOUR": 7200,
    "FOUR_HOUR": 14400,
    "SIX_HOUR": 21600,
    "ONE_DAY": 86400,
}


def _get_float(name: str, default: float) -> float:
    value = os.getenv(name)
    return float(value) if value not in (None, "") else default


def _get_int(name: str, default: int) -> int:
    value = os.getenv(name)
    return int(value) if value not in (None, "") else default


def _get_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value in (None, ""):
        return default
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be a boolean value.")


@dataclass(frozen=True)
class BotConfig:
    mode: str
    trading_enabled: bool
    product_id: str
    granularity: str
    lookback_candles: int
    loop_seconds: int
    market_data_timeout_seconds: int
    market_data_max_retries: int
    market_data_retry_backoff_seconds: float
    starting_cash: float
    aggressiveness: float
    fee_rate: float
    slippage_bps: float
    risk_per_trade_pct: float
    max_notional_pct: float
    max_concurrent_trades: int
    max_position_size: float
    min_position_size: float
    min_order_notional: float
    min_cash_reserve: float
    stop_loss_pct: float
    take_profit_pct: float
    trailing_stop_pct: float
    fast_ema_period: int
    slow_ema_period: int
    rsi_period: int
    enable_shorts: bool
    aggressive_entries: bool
    min_confirmation_signals: int
    pullback_lookback_candles: int
    pullback_min_pct: float
    long_top_guard_pct: float
    short_bottom_guard_pct: float
    market_state_lookback_candles: int
    market_trend_efficiency_threshold: float
    market_trend_ema_gap_pct: float
    block_entries_in_chop: bool
    chop_high_confidence_signals: int
    chop_profit_lock_trigger_pct: float
    chop_profit_lock_stop_buffer_pct: float
    chop_stall_minutes: int
    chop_stall_exit_band_pct: float
    trailing_stop_pct_trending: float
    trailing_stop_pct_choppy: float
    rsi_entry_floor: float
    rsi_entry_ceiling: float
    short_rsi_entry_floor: float
    short_rsi_entry_ceiling: float
    cooldown_after_loss_seconds: int
    cooldown_after_win_seconds: int
    flip_cooldown_seconds: int
    max_trades_total: int
    max_trades_per_hour: int
    trade_rate_pause_seconds: int
    max_trade_duration_minutes: int
    max_spread_threshold: float
    min_expected_move_multiple: float
    missed_trend_move_pct: float
    daily_max_loss_pct: float
    max_drawdown_pct: float
    max_consecutive_losses: int
    kill_switch_path: Path
    state_path: Path
    trade_log_path: Path
    signal_log_path: Path
    training_sample_log_path: Path
    baseline_network_path: Path
    coinbase_api_key: str | None
    coinbase_api_secret: str | None

    @property
    def candle_seconds(self) -> int:
        return GRANULARITY_TO_SECONDS[self.granularity]

    @property
    def quote_currency(self) -> str:
        return self.product_id.split("-", 1)[1]

    def trailing_stop_pct_for_market_state(self, market_state: str) -> float:
        if market_state == "TRENDING":
            return self.trailing_stop_pct_trending
        if market_state == "CHOPPY":
            return self.trailing_stop_pct_choppy
        return self.trailing_stop_pct

    @classmethod
    def from_env(cls) -> "BotConfig":
        load_dotenv()
        granularity = os.getenv("BOT_GRANULARITY", "FIVE_MINUTE").upper()
        if granularity not in GRANULARITY_TO_SECONDS:
            raise ValueError(
                f"Unsupported BOT_GRANULARITY={granularity}. "
                f"Choose one of: {', '.join(GRANULARITY_TO_SECONDS)}."
            )

        mode = os.getenv("BOT_MODE", "paper").lower()
        if mode not in {"paper", "live"}:
            raise ValueError("BOT_MODE must be either 'paper' or 'live'.")

        config = cls(
            mode=mode,
            trading_enabled=_get_bool("BOT_TRADING_ENABLED", True),
            product_id=os.getenv("BOT_PRODUCT_ID", "ETH-USD").upper(),
            granularity=granularity,
            lookback_candles=_get_int("BOT_LOOKBACK_CANDLES", 300),
            loop_seconds=_get_int("BOT_LOOP_SECONDS", 60),
            market_data_timeout_seconds=_get_int("BOT_MARKET_DATA_TIMEOUT_SECONDS", 20),
            market_data_max_retries=_get_int("BOT_MARKET_DATA_MAX_RETRIES", 3),
            market_data_retry_backoff_seconds=_get_float("BOT_MARKET_DATA_RETRY_BACKOFF_SECONDS", 2.0),
            starting_cash=_get_float("BOT_STARTING_CASH", 1000.0),
            aggressiveness=_get_float("BOT_AGGRESSIVENESS", 0.65),
            fee_rate=_get_float("BOT_FEE_RATE", 0.006),
            slippage_bps=_get_float("BOT_SLIPPAGE_BPS", 8.0),
            risk_per_trade_pct=_get_float("BOT_RISK_PER_TRADE_PCT", 0.01),
            max_notional_pct=_get_float("BOT_MAX_NOTIONAL_PCT", 0.20),
            max_concurrent_trades=_get_int("BOT_MAX_CONCURRENT_TRADES", 1),
            max_position_size=_get_float("BOT_MAX_POSITION_SIZE", 250.0),
            min_position_size=_get_float("BOT_MIN_POSITION_SIZE", 25.0),
            min_order_notional=_get_float("BOT_MIN_ORDER_NOTIONAL", 25.0),
            min_cash_reserve=_get_float("BOT_MIN_CASH_RESERVE", 50.0),
            stop_loss_pct=_get_float("BOT_STOP_LOSS_PCT", 0.015),
            take_profit_pct=_get_float("BOT_TAKE_PROFIT_PCT", 0.025),
            trailing_stop_pct=_get_float("BOT_TRAILING_STOP_PCT", 0.01),
            fast_ema_period=_get_int("BOT_FAST_EMA_PERIOD", 9),
            slow_ema_period=_get_int("BOT_SLOW_EMA_PERIOD", 21),
            rsi_period=_get_int("BOT_RSI_PERIOD", 14),
            enable_shorts=_get_bool("BOT_ENABLE_SHORTS", True),
            aggressive_entries=_get_bool("BOT_AGGRESSIVE_ENTRIES", False),
            min_confirmation_signals=_get_int("BOT_MIN_CONFIRMATION_SIGNALS", 3),
            pullback_lookback_candles=_get_int("BOT_PULLBACK_LOOKBACK_CANDLES", 20),
            pullback_min_pct=_get_float("BOT_PULLBACK_MIN_PCT", 0.004),
            long_top_guard_pct=_get_float("BOT_LONG_TOP_GUARD_PCT", 0.995),
            short_bottom_guard_pct=_get_float("BOT_SHORT_BOTTOM_GUARD_PCT", 1.005),
            market_state_lookback_candles=_get_int("BOT_MARKET_STATE_LOOKBACK_CANDLES", 20),
            market_trend_efficiency_threshold=_get_float("BOT_MARKET_TREND_EFFICIENCY_THRESHOLD", 0.12),
            market_trend_ema_gap_pct=_get_float("BOT_MARKET_TREND_EMA_GAP_PCT", 0.00002),
            block_entries_in_chop=_get_bool("BOT_BLOCK_ENTRIES_IN_CHOP", True),
            chop_high_confidence_signals=_get_int("BOT_CHOP_HIGH_CONFIDENCE_SIGNALS", 5),
            chop_profit_lock_trigger_pct=_get_float("BOT_CHOP_PROFIT_LOCK_TRIGGER_PCT", 0.004),
            chop_profit_lock_stop_buffer_pct=_get_float("BOT_CHOP_PROFIT_LOCK_STOP_BUFFER_PCT", 0.0005),
            chop_stall_minutes=_get_int("BOT_CHOP_STALL_MINUTES", 20),
            chop_stall_exit_band_pct=_get_float("BOT_CHOP_STALL_EXIT_BAND_PCT", 0.0025),
            trailing_stop_pct_trending=_get_float("BOT_TRAILING_STOP_PCT_TRENDING", 0.01),
            trailing_stop_pct_choppy=_get_float("BOT_TRAILING_STOP_PCT_CHOPPY", 0.006),
            rsi_entry_floor=_get_float("BOT_RSI_ENTRY_FLOOR", 52.0),
            rsi_entry_ceiling=_get_float("BOT_RSI_ENTRY_CEILING", 68.0),
            short_rsi_entry_floor=_get_float("BOT_SHORT_RSI_ENTRY_FLOOR", 32.0),
            short_rsi_entry_ceiling=_get_float("BOT_SHORT_RSI_ENTRY_CEILING", 48.0),
            cooldown_after_loss_seconds=_get_int("BOT_COOLDOWN_AFTER_LOSS_SECONDS", 180),
            cooldown_after_win_seconds=_get_int("BOT_COOLDOWN_AFTER_WIN_SECONDS", 45),
            flip_cooldown_seconds=_get_int("BOT_FLIP_COOLDOWN_SECONDS", 180),
            max_trades_total=_get_int("BOT_MAX_TRADES_TOTAL", 40),
            max_trades_per_hour=_get_int("BOT_MAX_TRADES_PER_HOUR", 8),
            trade_rate_pause_seconds=_get_int("BOT_TRADE_RATE_PAUSE_SECONDS", 3600),
            max_trade_duration_minutes=_get_int("BOT_MAX_TRADE_DURATION_MINUTES", 60),
            max_spread_threshold=_get_float("BOT_MAX_SPREAD_THRESHOLD", 0.01),
            min_expected_move_multiple=_get_float("BOT_MIN_EXPECTED_MOVE_MULTIPLE", 2.0),
            missed_trend_move_pct=_get_float("BOT_MISSED_TREND_MOVE_PCT", 0.003),
            daily_max_loss_pct=_get_float("BOT_DAILY_MAX_LOSS_PCT", 0.03),
            max_drawdown_pct=_get_float("BOT_MAX_DRAWDOWN_PCT", 0.025),
            max_consecutive_losses=_get_int("BOT_MAX_CONSECUTIVE_LOSSES", 4),
            kill_switch_path=Path(os.getenv("BOT_KILL_SWITCH_PATH", "state/kill_switch.txt")),
            state_path=Path(os.getenv("BOT_STATE_PATH", "state/bot_state.json")),
            trade_log_path=Path(os.getenv("BOT_TRADE_LOG_PATH", "logs/trades.jsonl")),
            signal_log_path=Path(os.getenv("BOT_SIGNAL_LOG_PATH", "logs/signals.jsonl")),
            training_sample_log_path=Path(
                os.getenv("BOT_TRAINING_SAMPLE_LOG_PATH", "logs/training/trade_samples.jsonl")
            ),
            baseline_network_path=Path(os.getenv("BOT_BASELINE_NETWORK_PATH", "models/baseline/network.json")),
            coinbase_api_key=os.getenv("COINBASE_API_KEY") or None,
            coinbase_api_secret=os.getenv("COINBASE_API_SECRET") or None,
        )
        config.validate()
        return config

    def validate(self) -> None:
        if self.fast_ema_period >= self.slow_ema_period:
            raise ValueError("BOT_FAST_EMA_PERIOD must be smaller than BOT_SLOW_EMA_PERIOD.")
        minimum_lookback = max(
            self.slow_ema_period + 5,
            self.pullback_lookback_candles + 2,
            self.market_state_lookback_candles + 2,
        )
        if self.lookback_candles < minimum_lookback:
            raise ValueError("BOT_LOOKBACK_CANDLES is too small for the configured indicators.")
        if self.market_data_timeout_seconds < 1:
            raise ValueError("BOT_MARKET_DATA_TIMEOUT_SECONDS must be at least 1.")
        if self.market_data_max_retries < 0:
            raise ValueError("BOT_MARKET_DATA_MAX_RETRIES must be >= 0.")
        if self.market_data_retry_backoff_seconds < 0:
            raise ValueError("BOT_MARKET_DATA_RETRY_BACKOFF_SECONDS must be >= 0.")
        if not 0 < self.stop_loss_pct < 1:
            raise ValueError("BOT_STOP_LOSS_PCT must be between 0 and 1.")
        if not 0 < self.take_profit_pct < 1:
            raise ValueError("BOT_TAKE_PROFIT_PCT must be between 0 and 1.")
        if not 0 < self.trailing_stop_pct < 1:
            raise ValueError("BOT_TRAILING_STOP_PCT must be between 0 and 1.")
        if not 0 < self.risk_per_trade_pct < 1:
            raise ValueError("BOT_RISK_PER_TRADE_PCT must be between 0 and 1.")
        if not 0 < self.aggressiveness <= 1:
            raise ValueError("BOT_AGGRESSIVENESS must be between 0 and 1.")
        if not 0 < self.max_notional_pct <= 1:
            raise ValueError("BOT_MAX_NOTIONAL_PCT must be between 0 and 1.")
        if not 0 <= self.daily_max_loss_pct < 1:
            raise ValueError("BOT_DAILY_MAX_LOSS_PCT must be between 0 and 1.")
        if not 0 <= self.max_drawdown_pct < 1:
            raise ValueError("BOT_MAX_DRAWDOWN_PCT must be between 0 and 1.")
        if self.max_concurrent_trades < 1:
            raise ValueError("BOT_MAX_CONCURRENT_TRADES must be at least 1.")
        if self.min_position_size < 0:
            raise ValueError("BOT_MIN_POSITION_SIZE must be >= 0.")
        if self.max_position_size < self.min_position_size:
            raise ValueError("BOT_MAX_POSITION_SIZE must be >= BOT_MIN_POSITION_SIZE.")
        if self.cooldown_after_loss_seconds < 0 or self.cooldown_after_win_seconds < 0:
            raise ValueError("Cooldown settings must be >= 0.")
        if self.flip_cooldown_seconds < 0:
            raise ValueError("BOT_FLIP_COOLDOWN_SECONDS must be >= 0.")
        if self.max_trades_total < 1 or self.max_trades_per_hour < 1:
            raise ValueError("Trade limits must be at least 1.")
        if self.trade_rate_pause_seconds < 0:
            raise ValueError("BOT_TRADE_RATE_PAUSE_SECONDS must be >= 0.")
        if self.max_trade_duration_minutes < 1:
            raise ValueError("BOT_MAX_TRADE_DURATION_MINUTES must be at least 1.")
        if self.max_spread_threshold < 0:
            raise ValueError("BOT_MAX_SPREAD_THRESHOLD must be >= 0.")
        if self.min_expected_move_multiple < 0:
            raise ValueError("BOT_MIN_EXPECTED_MOVE_MULTIPLE must be >= 0.")
        if not 1 <= self.chop_high_confidence_signals <= 5:
            raise ValueError("BOT_CHOP_HIGH_CONFIDENCE_SIGNALS must be between 1 and 5.")
        if not 0 <= self.missed_trend_move_pct < 1:
            raise ValueError("BOT_MISSED_TREND_MOVE_PCT must be between 0 and 1.")
        if self.max_consecutive_losses < 1:
            raise ValueError("BOT_MAX_CONSECUTIVE_LOSSES must be at least 1.")
        if self.min_confirmation_signals < 1:
            raise ValueError("BOT_MIN_CONFIRMATION_SIGNALS must be at least 1.")
        if self.pullback_lookback_candles < 3:
            raise ValueError("BOT_PULLBACK_LOOKBACK_CANDLES must be at least 3.")
        if not 0 <= self.pullback_min_pct < 1:
            raise ValueError("BOT_PULLBACK_MIN_PCT must be between 0 and 1.")
        if not 0 < self.long_top_guard_pct <= 1:
            raise ValueError("BOT_LONG_TOP_GUARD_PCT must be between 0 and 1.")
        if self.short_bottom_guard_pct < 1:
            raise ValueError("BOT_SHORT_BOTTOM_GUARD_PCT must be at least 1.")
        if self.market_state_lookback_candles < 3:
            raise ValueError("BOT_MARKET_STATE_LOOKBACK_CANDLES must be at least 3.")
        if not 0 <= self.market_trend_efficiency_threshold <= 1:
            raise ValueError("BOT_MARKET_TREND_EFFICIENCY_THRESHOLD must be between 0 and 1.")
        if self.market_trend_ema_gap_pct < 0:
            raise ValueError("BOT_MARKET_TREND_EMA_GAP_PCT must be >= 0.")
        if not 0 <= self.chop_profit_lock_trigger_pct < 1:
            raise ValueError("BOT_CHOP_PROFIT_LOCK_TRIGGER_PCT must be between 0 and 1.")
        if not 0 <= self.chop_profit_lock_stop_buffer_pct < 1:
            raise ValueError("BOT_CHOP_PROFIT_LOCK_STOP_BUFFER_PCT must be between 0 and 1.")
        if self.chop_stall_minutes < 1:
            raise ValueError("BOT_CHOP_STALL_MINUTES must be at least 1.")
        if not 0 <= self.chop_stall_exit_band_pct < 1:
            raise ValueError("BOT_CHOP_STALL_EXIT_BAND_PCT must be between 0 and 1.")
        if not 0 < self.trailing_stop_pct_trending < 1:
            raise ValueError("BOT_TRAILING_STOP_PCT_TRENDING must be between 0 and 1.")
        if not 0 < self.trailing_stop_pct_choppy < 1:
            raise ValueError("BOT_TRAILING_STOP_PCT_CHOPPY must be between 0 and 1.")
        if not 0 <= self.short_rsi_entry_floor <= 100:
            raise ValueError("BOT_SHORT_RSI_ENTRY_FLOOR must be between 0 and 100.")
        if not 0 <= self.short_rsi_entry_ceiling <= 100:
            raise ValueError("BOT_SHORT_RSI_ENTRY_CEILING must be between 0 and 100.")
        if self.short_rsi_entry_floor > self.short_rsi_entry_ceiling:
            raise ValueError("BOT_SHORT_RSI_ENTRY_FLOOR must be <= BOT_SHORT_RSI_ENTRY_CEILING.")
        if not 0 <= self.rsi_entry_floor <= 100:
            raise ValueError("BOT_RSI_ENTRY_FLOOR must be between 0 and 100.")
        if not 0 <= self.rsi_entry_ceiling <= 100:
            raise ValueError("BOT_RSI_ENTRY_CEILING must be between 0 and 100.")
        if self.rsi_entry_floor > self.rsi_entry_ceiling:
            raise ValueError("BOT_RSI_ENTRY_FLOOR must be <= BOT_RSI_ENTRY_CEILING.")
        if self.mode == "live" and (not self.coinbase_api_key or not self.coinbase_api_secret):
            raise ValueError("Live mode requires COINBASE_API_KEY and COINBASE_API_SECRET.")


@dataclass(frozen=True)
class InstancePaths:
    state_path: Path
    trade_log_path: Path
    signal_log_path: Path
    report_path: Path
    network_snapshot_path: Path
    network_viz_path: Path
    network_json_path: Path
    activations_path: Path


@dataclass(frozen=True)
class StrategyProfile:
    entry_threshold_long: float
    entry_threshold_short: float
    weight_trend: float
    weight_pullback: float
    weight_momentum: float
    weight_cross: float
    weight_rsi: float
    weight_near_extreme_penalty: float
    weight_network: float
    rule_weight: float = 1.0
    exploration_bonus: float = 0.0
    block_entries_in_chop: bool = True
    allow_near_recent_high_long: bool = False
    allow_near_recent_low_short: bool = False
    allow_countertrend: bool = False
    max_hold_seconds: int = 3600
    cooldown_after_loss_seconds: int = 180
    cooldown_after_win_seconds: int = 45
    flip_cooldown_seconds: int = 180
    min_confirmation_signals: int = 3
    aggressive_entries: bool = False
    long_bias: float = 0.0
    short_bias: float = 0.0

    def validate(self) -> None:
        if self.entry_threshold_long < 0 or self.entry_threshold_short < 0:
            raise ValueError("Entry thresholds must be >= 0.")
        if self.max_hold_seconds < 1:
            raise ValueError("max_hold_seconds must be at least 1.")
        if self.min_confirmation_signals < 1:
            raise ValueError("min_confirmation_signals must be at least 1.")


@dataclass(frozen=True)
class NetworkConfig:
    layer_sizes: tuple[int, ...] = (24, 32, 24, 16, 8, 2)
    learning_rate: float = 0.01
    seed: int = 7
    mutation_scale: float = 0.05
    version: str = "baseline-v1"

    def validate(self) -> None:
        if len(self.layer_sizes) < 2:
            raise ValueError("layer_sizes must include at least an input and output layer.")
        if self.layer_sizes[0] != 24:
            raise ValueError("The baseline network expects 24 input features.")
        if self.layer_sizes[-1] != 2:
            raise ValueError("The baseline network expects two output logits.")
        if self.learning_rate <= 0:
            raise ValueError("learning_rate must be > 0.")
        if self.mutation_scale < 0:
            raise ValueError("mutation_scale must be >= 0.")


@dataclass(frozen=True)
class BotInstanceConfig:
    instance_id: str
    family: str
    generation: int
    profile_name: str
    base_config: BotConfig
    strategy_profile: StrategyProfile
    network_config: NetworkConfig
    storage_paths: InstancePaths

    @property
    def logger_name(self) -> str:
        return f"eth_bot.{self.instance_id}"


def build_instance_paths(root_dir: Path, instance_id: str, generation: int = 0) -> InstancePaths:
    generation_segment = f"generation_{generation:03d}" if generation > 0 else "singleton"
    return InstancePaths(
        state_path=root_dir / "state" / "instances" / instance_id / "state.json",
        trade_log_path=root_dir / "logs" / "instances" / instance_id / "trades.jsonl",
        signal_log_path=root_dir / "logs" / "instances" / instance_id / "signals.jsonl",
        report_path=root_dir / "reports" / generation_segment / "instances" / instance_id / "session_report.json",
        network_snapshot_path=root_dir / "models" / "instances" / instance_id / "network.json",
        network_viz_path=root_dir / "viz" / "instances" / instance_id / "network.svg",
        network_json_path=root_dir / "viz" / "instances" / instance_id / "network.json",
        activations_path=root_dir / "viz" / "instances" / instance_id / "activations_latest.json",
    )


def apply_instance_overrides(
    base_config: BotConfig,
    profile: StrategyProfile,
    paths: InstancePaths,
) -> BotConfig:
    profile.validate()
    return replace(
        base_config,
        aggressive_entries=profile.aggressive_entries,
        block_entries_in_chop=profile.block_entries_in_chop,
        min_confirmation_signals=profile.min_confirmation_signals,
        cooldown_after_loss_seconds=profile.cooldown_after_loss_seconds,
        cooldown_after_win_seconds=profile.cooldown_after_win_seconds,
        flip_cooldown_seconds=profile.flip_cooldown_seconds,
        max_trade_duration_minutes=max(1, math.ceil(profile.max_hold_seconds / 60)),
        state_path=paths.state_path,
        trade_log_path=paths.trade_log_path,
        signal_log_path=paths.signal_log_path,
    )

