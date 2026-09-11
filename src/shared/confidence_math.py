"""Pure confidence mathematics with the legacy Lane II clipping contract.

This module deliberately knows nothing about accounts, runtimes, evidence
meaning, or execution permission.  Callers remain responsible for validating
the semantic meaning and provenance of every supplied log-odds effect.
"""

from __future__ import annotations

import math
from typing import Iterable


PROBABILITY_EPSILON = 1e-9
LOG_ODDS_CLIP = 30.0


def unit_interval(value: float, name: str) -> float:
    """Return a finite value in ``[0, 1]`` or fail explicitly."""
    # Compare before conversion to retain the legacy copytrade behavior for
    # unsupported input types while adding an explicit finite check.
    if not 0 <= value <= 1:
        raise ValueError(f"{name} must be in [0, 1].")
    converted = float(value)
    if not math.isfinite(converted):
        raise ValueError(f"{name} must be in [0, 1].")
    return converted


def logit_probability(
    probability: float,
    *,
    name: str = "probability",
    epsilon: float = PROBABILITY_EPSILON,
) -> float:
    """Return the legacy finite logit used by ``copytrade.confidence``."""
    value = unit_interval(probability, name)
    if not math.isfinite(epsilon) or not 0 < epsilon < 0.5:
        raise ValueError("epsilon must be finite and in (0, 0.5).")
    return math.log(max(value, epsilon) / max(1 - value, epsilon))


def sigmoid_clipped(log_odds: float, *, clip: float = LOG_ODDS_CLIP) -> float:
    """Apply a sigmoid after symmetric finite clipping."""
    value = float(log_odds)
    if not math.isfinite(value):
        raise ValueError("log_odds must be finite.")
    if not math.isfinite(clip) or clip <= 0:
        raise ValueError("clip must be finite and positive.")
    return 1 / (1 + math.exp(-max(-clip, min(clip, value))))


def combine_log_odds(
    baseline: float,
    effects: Iterable[float],
    *,
    baseline_name: str = "baseline trade confidence",
) -> float:
    """Recalculate a score from a baseline and caller-supplied effects."""
    log_odds = logit_probability(baseline, name=baseline_name)
    for effect in effects:
        converted = float(effect)
        if not math.isfinite(converted):
            raise ValueError("log-odds effect must be finite.")
        log_odds += converted
    return sigmoid_clipped(log_odds)


def shrink_toward_neutral(score: float, reliability: float) -> float:
    """Shrink a raw score toward 0.5 by an independent reliability value."""
    return 0.5 + (unit_interval(score, "trade_confidence") - 0.5) * unit_interval(
        reliability, "model_confidence"
    )


def attainable_effective_range(
    reliability: float,
    *,
    log_odds_clip: float = LOG_ODDS_CLIP,
) -> tuple[float, float]:
    """Return exact attainable effective bounds under finite logit clipping."""
    reliability_value = unit_interval(reliability, "reliability")
    low = 0.5 + reliability_value * (sigmoid_clipped(-log_odds_clip, clip=log_odds_clip) - 0.5)
    high = 0.5 + reliability_value * (sigmoid_clipped(log_odds_clip, clip=log_odds_clip) - 0.5)
    return low, high
