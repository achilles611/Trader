from __future__ import annotations

from .config import SizingConfig


def is_equity_observation_usable(
    config: SizingConfig, equity: float | None, source: str, age_seconds: float | None,
) -> bool:
    """Single causal-quality rule for all historical sizing consumers."""
    return (
        equity is not None
        and equity > 0
        and source in config.accepted_equity_sources
        and (age_seconds is None or 0 <= age_seconds <= config.max_equity_age_seconds)
    )
