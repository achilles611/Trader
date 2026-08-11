from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Iterable, Mapping

from .config import CandidateConfig
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


def score_candidate(
    metrics: TraderMetrics, config: CandidateConfig, follower: FollowerMetrics | None = None,
    *, source_quality: float = 1.0, diversification: float = 0.5,
) -> CandidateScore:
    follower = follower or FollowerMetrics()
    reasons: list[str] = []
    if metrics.history_days < config.history_days_min:
        reasons.append("insufficient_history")
    if metrics.closed_campaign_count < config.closed_campaigns_min:
        reasons.append("insufficient_closed_campaigns")
    if metrics.max_drawdown > config.max_drawdown_hard:
        reasons.append("max_drawdown_hard_limit")
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
    components = {
        "risk_adjusted_expectancy": _clamp01(0.5 + metrics.expectancy / max(abs(metrics.average_loser), 1.0) / 2),
        "drawdown_tail": _clamp01((1 - metrics.max_drawdown / max(config.max_drawdown_hard, 1e-12)) * (0.5 if metrics.fifth_percentile < 0 else 1.0)),
        "consistency": _clamp01((metrics.shrunk_win_rate + min(metrics.profit_factor, 2.0) / 2) / 2),
        "history_quality": _clamp01(min(metrics.history_days / max(config.history_days_preferred, 1), metrics.closed_campaign_count / max(config.closed_campaigns_min, 1))),
        "position_size_stability": _clamp01(1 - metrics.entry_size_variance / max(metrics.median_entry_size_fraction ** 2, 0.01)),
        "diversification": _clamp01(diversification),
        "source_quality": _clamp01(source_quality),
    }
    if follower.return_fraction is not None:
        components["follower_performance"] = _clamp01(0.5 + follower.return_fraction / 0.20)
    if follower.copyability_score is not None:
        friction = follower.slippage_robustness if follower.slippage_robustness is not None else 1.0
        components["copyability"] = _clamp01((follower.copyability_score + friction) / 2)
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
    if metrics.pnl_concentration_best > 0.5:
        penalties["concentration"] = config.penalty_weights.get("concentration", 0.0) * metrics.pnl_concentration_best
    if metrics.closed_campaign_count < config.closed_campaigns_min:
        penalties["small_sample"] = config.penalty_weights.get("small_sample", 0.0)
    if metrics.activity_recency_days is not None and metrics.activity_recency_days > config.activity_max_age_days:
        penalties["inactivity"] = config.penalty_weights.get("inactivity", 0.0)
    if latency_survival is not None and latency_survival < 0.4:
        penalties["latency_decay"] = config.penalty_weights.get("latency_decay", 0.0) * (1 - latency_survival)
    if metrics.fifth_percentile < -abs(metrics.average_winner) * 3:
        penalties["negative_skew"] = config.penalty_weights.get("negative_skew", 0.0)
    total = max(0.0, min(100.0, sum(weighted.values()) - sum(penalties.values())))
    return CandidateScore(
        target_wallet=metrics.target_wallet, calculated_at=utc_now(), total_score=total,
        component_scores=weighted, penalties=penalties,
        eligible=not [reason for reason in reasons if reason not in {
            "latency_unavailable", "copyability_unavailable", "coverage_unproven",
        } or (reason == "coverage_unproven" and config.require_proven_history)], reasons=tuple(reasons),
        source_quality=source_quality,
    )


def select_diverse_targets(
    scores: Iterable[CandidateScore], return_series: Mapping[str, Mapping[str, float] | list[float]], *, target_count: int = 7,
) -> list[CandidateScore]:
    """Greedy selection balances candidate quality with independently varying returns."""
    candidates = sorted((score for score in scores if score.eligible), key=lambda score: (-score.total_score, score.target_wallet))
    selected: list[CandidateScore] = []
    while candidates and len(selected) < target_count:
        def value(score: CandidateScore) -> tuple[float, str]:
            correlations = [abs(pairwise_correlation(return_series.get(score.target_wallet, {}), return_series.get(item.target_wallet, {}))) for item in selected]
            penalty = sum(correlations) / len(correlations) if correlations else 0.0
            return (score.total_score - 25 * penalty, score.target_wallet)
        best = max(candidates, key=value)
        selected.append(best)
        candidates.remove(best)
    return selected


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
