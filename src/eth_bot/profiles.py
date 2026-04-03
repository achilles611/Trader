from __future__ import annotations

from dataclasses import replace
from pathlib import Path

from .config import (
    BotConfig,
    BotInstanceConfig,
    NetworkConfig,
    StrategyProfile,
    apply_instance_overrides,
    build_instance_paths,
)


SWARM_INSTANCE_IDS = ("zerk1", "zerk2", "tr1", "tr2", "tr3", "tr4", "tr5", "tr6", "tr7", "tr8")


def default_strategy_profile(base_config: BotConfig) -> StrategyProfile:
    return StrategyProfile(
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
        exploration_bonus=0.0,
        block_entries_in_chop=base_config.block_entries_in_chop,
        allow_near_recent_high_long=False,
        allow_near_recent_low_short=False,
        allow_countertrend=False,
        max_hold_seconds=base_config.max_trade_duration_minutes * 60,
        cooldown_after_loss_seconds=base_config.cooldown_after_loss_seconds,
        cooldown_after_win_seconds=base_config.cooldown_after_win_seconds,
        flip_cooldown_seconds=base_config.flip_cooldown_seconds,
        min_confirmation_signals=base_config.min_confirmation_signals,
        aggressive_entries=base_config.aggressive_entries,
        long_bias=0.0,
        short_bias=0.0,
    )


def build_singleton_instance_config(
    base_config: BotConfig,
    *,
    root_dir: Path | None = None,
    generation: int = 0,
) -> BotInstanceConfig:
    root = root_dir or Path(".")
    profile = default_strategy_profile(base_config)
    paths = build_instance_paths(root, "singleton", generation)
    paths = replace(
        paths,
        state_path=base_config.state_path,
        trade_log_path=base_config.trade_log_path,
        signal_log_path=base_config.signal_log_path,
    )
    effective_config = apply_instance_overrides(base_config, profile, paths)
    effective_config.validate()
    return BotInstanceConfig(
        instance_id="singleton",
        family="single",
        generation=generation,
        profile_name="default",
        base_config=effective_config,
        strategy_profile=profile,
        network_config=NetworkConfig(version=f"baseline-v1-g{generation:03d}"),
        storage_paths=paths,
    )


def build_swarm_instance_configs(
    base_config: BotConfig,
    *,
    generation: int,
    root_dir: Path | None = None,
) -> list[BotInstanceConfig]:
    root = root_dir or Path(".")
    baseline_profile = default_strategy_profile(base_config)
    shared_network_version = f"baseline-v1-g{generation:03d}"

    definitions: list[tuple[str, str, str, StrategyProfile, dict[str, object]]] = [
        (
            "zerk1",
            "zerk",
            "hyperactive_momentum_raider",
            replace(
                baseline_profile,
                entry_threshold_long=0.42,
                entry_threshold_short=0.42,
                weight_trend=0.8,
                weight_pullback=0.4,
                weight_momentum=1.8,
                weight_cross=0.25,
                weight_rsi=0.35,
                weight_near_extreme_penalty=0.05,
                weight_network=0.65,
                rule_weight=0.55,
                exploration_bonus=0.12,
                block_entries_in_chop=False,
                allow_near_recent_high_long=True,
                allow_near_recent_low_short=True,
                allow_countertrend=False,
                max_hold_seconds=60,
                cooldown_after_loss_seconds=0,
                cooldown_after_win_seconds=0,
                flip_cooldown_seconds=0,
                min_confirmation_signals=1,
                aggressive_entries=True,
            ),
            {
                "max_trades_total": 300,
                "max_trades_per_hour": 300,
                "pullback_min_pct": 0.0015,
                "fast_ema_period": 5,
                "slow_ema_period": 13,
            },
        ),
        (
            "zerk2",
            "zerk",
            "chaotic_contrarian_scalper",
            replace(
                baseline_profile,
                entry_threshold_long=0.45,
                entry_threshold_short=0.45,
                weight_trend=0.2,
                weight_pullback=0.25,
                weight_momentum=0.95,
                weight_cross=0.75,
                weight_rsi=2.0,
                weight_near_extreme_penalty=-1.15,
                weight_network=0.55,
                rule_weight=0.65,
                exploration_bonus=0.18,
                block_entries_in_chop=False,
                allow_near_recent_high_long=True,
                allow_near_recent_low_short=True,
                allow_countertrend=True,
                prefer_countertrend=True,
                max_hold_seconds=60,
                cooldown_after_loss_seconds=0,
                cooldown_after_win_seconds=0,
                flip_cooldown_seconds=0,
                min_confirmation_signals=1,
                aggressive_entries=True,
            ),
            {
                "max_trades_total": 300,
                "max_trades_per_hour": 300,
                "pullback_min_pct": 0.0015,
                "rsi_entry_floor": 45.0,
                "rsi_entry_ceiling": 75.0,
                "short_rsi_entry_floor": 25.0,
                "short_rsi_entry_ceiling": 55.0,
            },
        ),
        (
            "tr1",
            "tr",
            "baseline_conservative",
            replace(baseline_profile),
            {},
        ),
        (
            "tr2",
            "tr",
            "faster_trend_follower",
            replace(
                baseline_profile,
                entry_threshold_long=0.64,
                entry_threshold_short=0.64,
                weight_momentum=1.35,
                weight_rsi=0.65,
                weight_network=0.2,
                rule_weight=0.8,
                block_entries_in_chop=True,
                min_confirmation_signals=2,
                aggressive_entries=True,
            ),
            {
                "fast_ema_period": 6,
                "slow_ema_period": 15,
                "pullback_min_pct": 0.0025,
            },
        ),
        (
            "tr3",
            "tr",
            "pullback_purist",
            replace(
                baseline_profile,
                entry_threshold_long=0.74,
                entry_threshold_short=0.74,
                weight_pullback=1.7,
                weight_momentum=0.8,
                weight_cross=0.5,
                weight_network=0.18,
                rule_weight=0.82,
            ),
            {
                "pullback_min_pct": 0.006,
            },
        ),
        (
            "tr4",
            "tr",
            "breakout_confirmer",
            replace(
                baseline_profile,
                entry_threshold_long=0.76,
                entry_threshold_short=0.76,
                weight_trend=1.2,
                weight_pullback=0.75,
                weight_momentum=1.45,
                weight_cross=1.55,
                weight_rsi=0.55,
                weight_network=0.18,
                rule_weight=0.82,
            ),
            {
                "pullback_min_pct": 0.002,
            },
        ),
        (
            "tr5",
            "tr",
            "chop_survivor",
            replace(
                baseline_profile,
                entry_threshold_long=0.82,
                entry_threshold_short=0.82,
                weight_network=0.12,
                rule_weight=0.88,
                block_entries_in_chop=True,
                min_confirmation_signals=4,
                aggressive_entries=False,
            ),
            {
                "chop_high_confidence_signals": 5,
                "max_trades_per_hour": 4,
            },
        ),
        (
            "tr6",
            "tr",
            "rsi_sensitive",
            replace(
                baseline_profile,
                entry_threshold_long=0.71,
                entry_threshold_short=0.71,
                weight_rsi=1.55,
                weight_network=0.22,
                rule_weight=0.78,
            ),
            {
                "rsi_entry_floor": 50.0,
                "rsi_entry_ceiling": 64.0,
                "short_rsi_entry_floor": 36.0,
                "short_rsi_entry_ceiling": 50.0,
            },
        ),
        (
            "tr7",
            "tr",
            "short_biased_hunter",
            replace(
                baseline_profile,
                entry_threshold_long=0.73,
                entry_threshold_short=0.68,
                weight_momentum=1.15,
                weight_cross=1.2,
                weight_network=0.2,
                rule_weight=0.8,
                short_bias=0.08,
            ),
            {
                "take_profit_pct": 0.02,
                "stop_loss_pct": 0.012,
            },
        ),
        (
            "tr8",
            "tr",
            "hybrid_network_experimental",
            replace(
                baseline_profile,
                entry_threshold_long=0.61,
                entry_threshold_short=0.61,
                weight_network=0.52,
                rule_weight=0.48,
                block_entries_in_chop=False,
                aggressive_entries=True,
            ),
            {},
        ),
    ]

    instances: list[BotInstanceConfig] = []
    for index, (instance_id, family, profile_name, profile, overrides) in enumerate(definitions, start=1):
        paths = build_instance_paths(root, instance_id, generation)
        effective_base = replace(base_config, **overrides)
        effective_config = apply_instance_overrides(effective_base, profile, paths)
        effective_config.validate()
        network_config = NetworkConfig(
            seed=7 + generation * 100 + index,
            version=shared_network_version,
        )
        network_config.validate()
        instances.append(
            BotInstanceConfig(
                instance_id=instance_id,
                family=family,
                generation=generation,
                profile_name=profile_name,
                base_config=effective_config,
                strategy_profile=profile,
                network_config=network_config,
                storage_paths=paths,
            )
        )
    return instances
