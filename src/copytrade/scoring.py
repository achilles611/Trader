from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Iterable, Mapping

from .config import CandidateConfig, ConfidenceConfig
from .models import CandidateScore, TraderMetrics, utc_now


@dataclass(frozen=True)
class FollowerMetrics:
    net_pnl: float = 0.0
    expectancy: float = 0.0
    profit_factor: float = 0.0
    max_drawdown: float = 0.0
    target_edge_retained: float = 0.0
    missed_trade_rate: float = 0.0
    latency_curve: tuple[dict[str, float], ...] = ()
    latency_status: str = "unavailable"
    return_fraction: float | None = None
    copyability_score: float | None = None
    slippage_robustness: float | None = None
    walk_forward_score: float | None = None
    walk_forward_status: str = "unavailable"
    walk_forward_window_count: int = 0
    friction_robustness: float | None = None
    regime_robustness: float | None = None


def score_candidate(
    metrics: TraderMetrics, config: CandidateConfig, follower: FollowerMetrics | None = None,
    *, source_quality: float = 1.0, diversification: float = 0.5, confidence_score: float = 0.0,
) -> CandidateScore:
    follower = follower or FollowerMetrics()
    # Portfolio diversification is a selection-stage concern.  Keep this
    # legacy argument harmless for callers while never blending it into the
    # stored single-wallet suitability score.
    del diversification
    reasons: list[str] = []
    if metrics.history_days < config.history_days_min:
        reasons.append("insufficient_history")
    if metrics.closed_campaign_count < config.closed_campaigns_min:
        reasons.append("insufficient_closed_campaigns")
    if metrics.max_drawdown > config.max_drawdown_hard:
        reasons.append("max_drawdown_hard_limit")
    if follower.max_drawdown > config.max_follower_drawdown_hard:
        reasons.append("follower_drawdown_hard_limit")
    if config.require_positive_expectancy and metrics.expectancy <= 0:
        reasons.append("non_positive_expectancy")
    if config.require_positive_follower_expectancy and follower.expectancy <= 0:
        reasons.append("non_positive_follower_expectancy")
    if metrics.activity_recency_days is not None and metrics.activity_recency_days > config.activity_max_age_days:
        reasons.append("inactive")
    if int(metrics.raw.get("truncated_campaign_count", 0)):
        reasons.append("truncated_campaign_history")
    if int(metrics.raw.get("reconciliation_mismatch_count", 0)):
        reasons.append("pnl_reconciliation_mismatch")
    coverage_state = str(metrics.raw.get("coverage_state") or ("PROVEN_COMPLETE" if metrics.raw.get("coverage_complete") else "UNPROVEN"))
    if coverage_state == "KNOWN_INCOMPLETE":
        reasons.append("known_incomplete_historical_coverage")
    elif coverage_state == "UNPROVEN":
        reasons.append("coverage_unproven")
        source_quality *= 0.90
    elif coverage_state != "PROVEN_COMPLETE":
        reasons.append("coverage_state_unknown")

    latency_survival = _latency_survival(follower)
    latency_available = latency_survival is not None
    if not latency_available:
        reasons.append("latency_unavailable")
    if follower.copyability_score is None:
        reasons.append("copyability_unavailable")
    elif follower.copyability_score < config.minimum_copyability_hard:
        reasons.append("copyability_hard_limit")
    if follower.walk_forward_score is None or follower.walk_forward_status != "available":
        reasons.append("walk_forward_unavailable")
    components = {
        "risk_adjusted_expectancy": _clamp01(0.5 + metrics.expectancy / max(abs(metrics.average_loser), 1.0) / 2),
        "drawdown_tail": _clamp01((1 - metrics.max_drawdown / max(config.max_drawdown_hard, 1e-12)) * (0.5 if metrics.fifth_percentile < 0 else 1.0)),
        "follower_drawdown": _clamp01(1 - follower.max_drawdown / max(config.max_follower_drawdown_hard, 1e-12)),
        "consistency": _clamp01((metrics.shrunk_win_rate + min(metrics.profit_factor, 2.0) / 2) / 2),
        "history_quality": _clamp01(min(metrics.history_days / max(config.history_days_preferred, 1), metrics.closed_campaign_count / max(config.closed_campaigns_min, 1))),
        "position_size_stability": _clamp01(1 - metrics.entry_size_variance / max(metrics.median_entry_size_fraction ** 2, 0.01)),
        "source_quality": _clamp01(source_quality),
    }
    if follower.return_fraction is not None:
        components["follower_performance"] = _clamp01(0.5 + follower.return_fraction / 0.20)
    if follower.copyability_score is not None:
        friction = follower.slippage_robustness if follower.slippage_robustness is not None else 1.0
        components["copyability"] = _clamp01((follower.copyability_score + friction) / 2)
    if follower.walk_forward_score is not None and follower.walk_forward_status == "available":
        components["walk_forward"] = _clamp01(follower.walk_forward_score)
    if follower.friction_robustness is not None:
        components["friction_robustness"] = _clamp01(follower.friction_robustness)
    if follower.regime_robustness is not None:
        components["regime_robustness"] = _clamp01(follower.regime_robustness)
    active_weights = {name: weight for name, weight in config.score_weights.items() if name in components}
    if latency_available:
        components["latency_survivability"] = latency_survival
        active_weights["latency_survivability"] = config.score_weights.get("latency_survivability", 0.0)
    # An unavailable price-sensitive replay contributes no evidence in either
    # direction.  Reweight the evidence that actually exists to the 100-point
    # scale instead of rewarding a flat target-price proxy.
    scale = sum(config.score_weights.values()) / max(sum(active_weights.values()), 1e-12)
    weighted = {name: components[name] * weight * scale for name, weight in active_weights.items()}
    penalties: dict[str, float] = {}
    if metrics.martingale_indicator:
        penalties["martingale"] = config.penalty_weights.get("martingale", 0.0)
    if metrics.adverse_averaging_indicator:
        penalties["adverse_averaging"] = config.penalty_weights.get("adverse_averaging", 0.0)
    if follower.max_drawdown > config.max_follower_drawdown_preferred:
        excess = (follower.max_drawdown - config.max_follower_drawdown_preferred) / max(
            config.max_follower_drawdown_hard - config.max_follower_drawdown_preferred, 1e-12,
        )
        penalties["follower_drawdown"] = config.penalty_weights.get("follower_drawdown", 0.0) * _clamp01(excess)
    liquidation_frequency = float(metrics.raw.get("liquidation_frequency", 0.0) or 0.0)
    if liquidation_frequency > 0:
        penalties["liquidation"] = config.penalty_weights.get("liquidation", 0.0) * _clamp01(
            liquidation_frequency / max(config.liquidation_frequency_hard, 1e-12)
        )
    if liquidation_frequency >= config.liquidation_frequency_hard:
        reasons.append("liquidation_frequency_hard_limit")
    if metrics.pnl_concentration_best > config.pnl_concentration_preferred:
        excess = (metrics.pnl_concentration_best - config.pnl_concentration_preferred) / max(
            1 - config.pnl_concentration_preferred, 1e-12,
        )
        penalties["jackpot_concentration"] = config.penalty_weights.get("jackpot_concentration", config.penalty_weights.get("concentration", 0.0)) * _clamp01(excess)
        reasons.append("jackpot_concentration")
    if config.pnl_concentration_hard < 1.0 and metrics.pnl_concentration_best >= config.pnl_concentration_hard:
        reasons.append("pnl_concentration_hard_limit")
    if metrics.closed_campaign_count < config.closed_campaigns_min:
        penalties["small_sample"] = config.penalty_weights.get("small_sample", 0.0)
    if metrics.activity_recency_days is not None and metrics.activity_recency_days > config.activity_max_age_days:
        penalties["inactivity"] = config.penalty_weights.get("inactivity", 0.0)
    if latency_survival is not None and latency_survival < 0.4:
        penalties["latency_decay"] = config.penalty_weights.get("latency_decay", 0.0) * (1 - latency_survival)
    if follower.friction_robustness is not None and follower.friction_robustness < 0.4:
        penalties["friction_sensitivity"] = config.penalty_weights.get("friction_sensitivity", 0.0) * (1 - follower.friction_robustness)
    if follower.regime_robustness is not None and follower.regime_robustness < 0.4:
        penalties["regime_dependency"] = config.penalty_weights.get("regime_dependency", 0.0) * (1 - follower.regime_robustness)
    if metrics.fifth_percentile < -abs(metrics.average_winner) * 3:
        penalties["negative_skew"] = config.penalty_weights.get("negative_skew", 0.0)
    total = max(0.0, min(100.0, sum(weighted.values()) - sum(penalties.values())))
    soft_reasons = {
        "latency_unavailable", "copyability_unavailable", "coverage_unproven", "walk_forward_unavailable",
        "jackpot_concentration",
    }
    hard_gates = tuple(sorted(
        reason for reason in reasons
        if reason not in soft_reasons or (reason == "coverage_unproven" and config.require_proven_history)
    ))
    return CandidateScore(
        target_wallet=metrics.target_wallet, calculated_at=utc_now(), total_score=total,
        component_scores=weighted, penalties=penalties,
        eligible=not hard_gates, reasons=tuple(sorted(set(reasons))), hard_gates=hard_gates,
        confidence_score=max(0.0, min(100.0, confidence_score)),
        source_quality=source_quality,
    )


def suitability_confidence(
    metrics: TraderMetrics, config: ConfidenceConfig, *, coverage_state: str,
    walk_forward_windows: int, represented_regimes: int,
) -> dict[str, object]:
    """Deterministic evidence strength, deliberately independent of return quality."""
    active_days = int(metrics.raw.get("active_days", 0) or 0)
    factors = {
        "closed_campaign_sample": _clamp01(metrics.closed_campaign_count / max(config.closed_campaigns_reference, 1)),
        "active_days": _clamp01(active_days / max(config.active_days_reference, 1)),
        "history_span": _clamp01(metrics.history_days / max(config.history_days_reference, 1)),
        "walk_forward_windows": _clamp01(walk_forward_windows / max(config.walk_forward_windows_reference, 1)),
        "regime_representation": _clamp01(represented_regimes / max(config.regimes_reference, 1)),
        "coverage": 1.0 if coverage_state == "PROVEN_COMPLETE" else (0.65 if coverage_state == "UNPROVEN" else 0.0),
        "source_quality": _clamp01(float(metrics.raw.get("source_quality", 1.0) or 0.0)),
    }
    return {"score": 100.0 * sum(factors.values()) / len(factors), "factors": factors,
            "coverage_state": coverage_state}


def select_diverse_targets(
    scores: Iterable[CandidateScore], return_series: Mapping[str, Mapping[str, float] | list[float]], *, target_count: int = 7,
) -> list[CandidateScore]:
    return [score for score, _ in select_diverse_targets_with_metadata(scores, return_series, target_count=target_count)]


def select_diverse_targets_with_metadata(
    scores: Iterable[CandidateScore], return_series: Mapping[str, Mapping[str, float] | list[float]], *,
    target_count: int = 7, exposures: Mapping[str, Mapping[str, object]] | None = None,
) -> list[tuple[CandidateScore, dict[str, object]]]:
    """Greedy quality selection with explicit uncertainty and exposure overlap."""
    candidates = sorted((score for score in scores if score.eligible), key=lambda score: (-score.total_score, score.target_wallet))
    selected: list[CandidateScore] = []
    details_by_wallet: dict[str, dict[str, object]] = {}
    exposures = exposures or {}
    while candidates and len(selected) < target_count:
        def value(score: CandidateScore) -> tuple[float, str]:
            detail = _diversification_detail(score.target_wallet, selected, return_series, exposures)
            details_by_wallet[score.target_wallet] = detail
            return (score.total_score - float(detail["penalty"]), score.target_wallet)
        best = max(candidates, key=value)
        selected.append(best)
        candidates.remove(best)
    return [(score, details_by_wallet.get(score.target_wallet, _diversification_detail(score.target_wallet, [], return_series, exposures))) for score in selected]


def pairwise_correlation_details(
    left: Mapping[str, float] | list[float], right: Mapping[str, float] | list[float], *, minimum_buckets: int = 7,
) -> tuple[float, int]:
    """Correlate UTC buckets, treating no activity inside the union as zero."""
    if isinstance(left, list):
        left = {str(index): value for index, value in enumerate(left)}
    if isinstance(right, list):
        right = {str(index): value for index, value in enumerate(right)}
    keys = sorted(set(left) | set(right))
    size = len(keys)
    if size < minimum_buckets:
        return 0.0, size
    x = [float(left.get(key, 0.0)) for key in keys]
    y = [float(right.get(key, 0.0)) for key in keys]
    mx = sum(x) / size
    my = sum(y) / size
    numerator = sum((a - mx) * (b - my) for a, b in zip(x, y))
    denominator_x = math.sqrt(sum((a - mx) ** 2 for a in x))
    denominator_y = math.sqrt(sum((b - my) ** 2 for b in y))
    if denominator_x == 0 or denominator_y == 0:
        return 0.0, size
    return max(-1.0, min(1.0, numerator / (denominator_x * denominator_y))), size


def pairwise_correlation_status(
    left: Mapping[str, float] | list[float], right: Mapping[str, float] | list[float], *, minimum_buckets: int = 7,
) -> dict[str, object]:
    correlation, bucket_count = pairwise_correlation_details(left, right, minimum_buckets=minimum_buckets)
    return {
        "status": "available" if bucket_count >= minimum_buckets else "insufficient_history",
        "correlation": correlation if bucket_count >= minimum_buckets else None,
        "bucket_count": bucket_count,
    }


def pairwise_correlation(left: Mapping[str, float] | list[float], right: Mapping[str, float] | list[float]) -> float:
    correlation, _ = pairwise_correlation_details(left, right)
    return correlation


def _legacy_pairwise_correlation(left: list[float], right: list[float]) -> float:
    """Retained only for old serialized data migrations; do not use for ranking."""
    size = min(len(left), len(right))
    if size < 2:
        return 0.0
    x = left[-size:]
    y = right[-size:]
    mx = sum(x) / size
    my = sum(y) / size
    numerator = sum((a - mx) * (b - my) for a, b in zip(x, y))
    denominator_x = math.sqrt(sum((a - mx) ** 2 for a in x))
    denominator_y = math.sqrt(sum((b - my) ** 2 for b in y))
    if denominator_x == 0 or denominator_y == 0:
        return 0.0
    return max(-1.0, min(1.0, numerator / (denominator_x * denominator_y)))


def _latency_survival(follower: FollowerMetrics) -> float | None:
    if follower.latency_status == "unavailable" or not follower.latency_curve:
        return None
    returns = [item.get("return_fraction", 0.0) for item in follower.latency_curve]
    if not returns:
        return 0.0
    positive = sum(value > 0 for value in returns) / len(returns)
    initial = abs(returns[0])
    final_ratio = max(0.0, returns[-1]) / initial if initial > 1e-12 else positive
    return _clamp01((positive + min(1.0, final_ratio)) / 2)


def _clamp01(value: float) -> float:
    return max(0.0, min(1.0, value))


def _diversification_detail(
    wallet: str, selected: Iterable[CandidateScore],
    return_series: Mapping[str, Mapping[str, float] | list[float]], exposures: Mapping[str, Mapping[str, object]],
) -> dict[str, object]:
    cohort = list(selected)
    correlations: list[float] = []
    buckets: list[int] = []
    insufficient = 0
    symbol_overlaps: list[float] = []
    directional_overlaps: list[float] = []
    profile = exposures.get(wallet, {})
    for item in cohort:
        detail = pairwise_correlation_status(return_series.get(wallet, {}), return_series.get(item.target_wallet, {}))
        buckets.append(int(detail["bucket_count"]))
        if detail["status"] == "available":
            correlations.append(abs(float(detail["correlation"])))
        else:
            insufficient += 1
        other = exposures.get(item.target_wallet, {})
        symbol_overlaps.append(_set_overlap(profile.get("symbols", ()), other.get("symbols", ())))
        directional_overlaps.append(_set_overlap(profile.get("directions", ()), other.get("directions", ())))
    average_correlation = sum(correlations) / len(correlations) if correlations else None
    maximum_correlation = max(correlations) if correlations else None
    average_symbols = sum(symbol_overlaps) / len(symbol_overlaps) if symbol_overlaps else 0.0
    average_directions = sum(directional_overlaps) / len(directional_overlaps) if directional_overlaps else 0.0
    # Missing return history is not assumed to be zero correlation.  It earns
    # neither a diversification bonus nor a free pass against better-known peers.
    penalty = 25 * (average_correlation or 0.0) + 8 * average_symbols + 3 * average_directions + 2.5 * insufficient
    return {
        "correlation_status": "available" if not insufficient and cohort else ("insufficient_history" if insufficient else "not_applicable"),
        "correlation_bucket_count": min(buckets) if buckets else 0,
        "average_correlation": average_correlation, "max_correlation": maximum_correlation,
        "symbol_overlap": average_symbols, "directional_overlap": average_directions,
        "insufficient_correlation_pairs": insufficient, "penalty": penalty,
    }


def _set_overlap(left: object, right: object) -> float:
    left_set, right_set = set(left or ()), set(right or ())
    if not left_set and not right_set:
        return 0.0
    return len(left_set & right_set) / max(len(left_set | right_set), 1)
