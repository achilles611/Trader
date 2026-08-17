"""Cost-adjusted alpha survival and empirical half-life measurements."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Sequence


EPSILON = 1e-12


def alpha_survival(edge_at_age: float, edge_at_arrival: float, *, epsilon: float = EPSILON) -> float:
    return max(edge_at_age, 0.0) / max(edge_at_arrival, epsilon)


@dataclass(frozen=True)
class AlphaDecayPoint:
    age_seconds: float
    expected_net_edge: float

    @property
    def survival(self) -> float:
        return 0.0  # populated relative to a curve's arrival edge


def alpha_half_life(curve: Sequence[AlphaDecayPoint]) -> float | None:
    """First measured age at or below half actionable arrival edge; no linear assumption."""
    if not curve or curve[0].expected_net_edge <= 0:
        return None
    baseline = curve[0].expected_net_edge
    for point in sorted(curve, key=lambda item: item.age_seconds):
        if alpha_survival(point.expected_net_edge, baseline) <= 0.5:
            return point.age_seconds
    return None


def survival_curve(edges: Iterable[tuple[float, float]]) -> list[dict[str, float]]:
    points = sorted((AlphaDecayPoint(float(age), float(edge)) for age, edge in edges), key=lambda item: item.age_seconds)
    baseline = points[0].expected_net_edge if points else 0.0
    return [{"age_seconds": point.age_seconds, "expected_net_edge": point.expected_net_edge, "alpha_survival": alpha_survival(point.expected_net_edge, baseline)} for point in points]
