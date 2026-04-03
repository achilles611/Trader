from __future__ import annotations

import json
import random
from dataclasses import asdict, replace
from pathlib import Path
from typing import Any

from .config import BotInstanceConfig, StrategyProfile


def _safe_divide(numerator: float, denominator: float) -> float:
    if denominator == 0:
        return 0.0
    return numerator / denominator


def compute_tr_fitness(report: dict[str, Any]) -> float:
    win_rate = float(report.get("win_rate", 0.0)) / 100.0
    normalized_pnl = float(report.get("return_pct", 0.0)) / 100.0
    trades = report.get("trades", [])
    expectancy = _safe_divide(sum(float(trade.get("pnl_fee_aware", trade.get("pnl", 0.0))) for trade in trades), len(trades))
    expectancy = max(-1.0, min(1.0, expectancy / 10.0))
    max_drawdown = float(report.get("max_drawdown_pct", 0.0)) / 100.0
    total_trades = int(report.get("total_trades", 0))
    overtrading_penalty = max(0.0, (total_trades - 25) / 100.0)
    stability_score = max(0.0, 1.0 - max_drawdown)
    return (
        0.40 * win_rate
        + 0.25 * normalized_pnl
        + 0.15 * expectancy
        - 0.10 * max_drawdown
        - 0.05 * overtrading_penalty
        + 0.05 * stability_score
    )


def compute_zerk_fitness(report: dict[str, Any]) -> float:
    trades = report.get("trades", [])
    positive_trade_count = sum(1 for trade in trades if float(trade.get("pnl_fee_aware", trade.get("pnl", 0.0))) > 0)
    count_of_winning_patterns_found = float(report.get("wins", 0))
    regimes = report.get("market_state_histogram", {})
    regime_diversity = min(1.0, len([value for value in regimes.values() if value]) / 2.0)
    raw_win_rate = float(report.get("win_rate", 0.0)) / 100.0
    return (
        0.35 * count_of_winning_patterns_found
        + 0.25 * positive_trade_count
        + 0.20 * regime_diversity
        + 0.20 * raw_win_rate
    )


def compute_instance_fitness(instance: BotInstanceConfig, report: dict[str, Any]) -> float:
    if instance.family == "zerk":
        return compute_zerk_fitness(report)
    return compute_tr_fitness(report)


def mutate_profile(profile: StrategyProfile, *, rng: random.Random, aggressive: bool) -> StrategyProfile:
    pct = 0.18 if aggressive else 0.08

    def adjust(value: float) -> float:
        return value * (1 + rng.uniform(-pct, pct))

    return replace(
        profile,
        entry_threshold_long=max(0.1, adjust(profile.entry_threshold_long)),
        entry_threshold_short=max(0.1, adjust(profile.entry_threshold_short)),
        weight_trend=max(0.0, adjust(profile.weight_trend)),
        weight_pullback=max(0.0, adjust(profile.weight_pullback)),
        weight_momentum=max(0.0, adjust(profile.weight_momentum)),
        weight_cross=max(0.0, adjust(profile.weight_cross)),
        weight_rsi=max(0.0, adjust(profile.weight_rsi)),
        weight_near_extreme_penalty=adjust(profile.weight_near_extreme_penalty),
        weight_network=max(0.0, adjust(profile.weight_network)),
        rule_weight=max(0.0, adjust(profile.rule_weight)),
        exploration_bonus=max(0.0, adjust(profile.exploration_bonus)),
        long_bias=adjust(profile.long_bias),
        short_bias=adjust(profile.short_bias),
    )


def propose_next_generation(
    instances: list[BotInstanceConfig],
    reports: dict[str, dict[str, Any]],
    *,
    to_generation: int,
) -> dict[str, Any]:
    scored = []
    for instance in instances:
        report = reports.get(instance.instance_id, {})
        scored.append(
            {
                "instance_id": instance.instance_id,
                "family": instance.family,
                "profile_name": instance.profile_name,
                "fitness": compute_instance_fitness(instance, report),
                "report": report,
                "instance": instance,
            }
        )

    tr_ranked = sorted((item for item in scored if item["family"] == "tr"), key=lambda item: item["fitness"], reverse=True)
    zerk_ranked = sorted((item for item in scored if item["family"] == "zerk"), key=lambda item: item["fitness"], reverse=True)
    rng = random.Random(1000 + to_generation)

    proposals: list[dict[str, Any]] = []
    elites = tr_ranked[:2]
    for elite in elites:
        proposals.append(
            {
                "instance_id": elite["instance_id"],
                "mode": "elite_keep",
                "profile": asdict(elite["instance"].strategy_profile),
                "fitness": elite["fitness"],
            }
        )

    for candidate in tr_ranked[2:6]:
        proposals.append(
            {
                "instance_id": candidate["instance_id"],
                "mode": "mutated_child",
                "profile": asdict(mutate_profile(candidate["instance"].strategy_profile, rng=rng, aggressive=False)),
                "fitness": candidate["fitness"],
            }
        )

    for candidate in tr_ranked[6:]:
        proposals.append(
            {
                "instance_id": candidate["instance_id"],
                "mode": "exploratory_outlier",
                "profile": asdict(mutate_profile(candidate["instance"].strategy_profile, rng=rng, aggressive=True)),
                "fitness": candidate["fitness"],
            }
        )

    for candidate in zerk_ranked:
        proposals.append(
            {
                "instance_id": candidate["instance_id"],
                "mode": "zerk_mutation",
                "profile": asdict(mutate_profile(candidate["instance"].strategy_profile, rng=rng, aggressive=True)),
                "fitness": candidate["fitness"],
            }
        )

    return {
        "to_generation": to_generation,
        "ranked_fitness": [
            {
                "instance_id": item["instance_id"],
                "family": item["family"],
                "profile_name": item["profile_name"],
                "fitness": item["fitness"],
            }
            for item in sorted(scored, key=lambda item: item["fitness"], reverse=True)
        ],
        "proposals": proposals,
    }


def load_generation_reports(report_paths: list[Path]) -> dict[str, dict[str, Any]]:
    reports: dict[str, dict[str, Any]] = {}
    for path in report_paths:
        if not path.exists():
            continue
        payload = json.loads(path.read_text(encoding="utf-8"))
        instance_id = str(payload.get("instance_id") or path.parent.name)
        reports[instance_id] = payload
    return reports
