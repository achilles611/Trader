"""Bounded, interpretable search over declared scientific feature families."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from statistics import fmean
from typing import Any, Mapping, Sequence

from .science_repository import canonical_hash


@dataclass(frozen=True)
class SearchFamily:
    family_id: str
    version: int
    feature_ids: tuple[str, ...]
    horizons_seconds: tuple[int, ...]
    maximum_interaction_order: int
    minimum_sample: int
    maximum_candidates: int
    minimum_effect_size: float
    allowed_regimes: tuple[str, ...] = ("unknown", "calm", "volatile")

    def __post_init__(self) -> None:
        if not self.family_id or not self.feature_ids or not self.horizons_seconds:
            raise ValueError("Search families require an ID, features, and horizons.")
        if not 1 <= self.maximum_interaction_order <= 3 or self.minimum_sample <= 0 or self.maximum_candidates <= 0:
            raise ValueError("Search-family complexity and budgets are invalid.")
        if any(not 0 < horizon <= 600 for horizon in self.horizons_seconds):
            raise ValueError("Search-family horizons must be in (0, 600].")

    def payload(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class DiscoveryCandidate:
    discovery_id: str
    family_id: str
    family_version: int
    feature_id: str
    feature_version: int
    condition: str
    threshold: float
    horizon_seconds: int
    sample_count: int
    estimated_net_expectancy: float
    effect_size: float
    data_fingerprint: str
    observation_ids: tuple[str, ...]

    def payload(self) -> dict[str, Any]:
        return asdict(self)


class BoundedPatternDiscovery:
    """Generates only declared single-feature threshold propositions.

    This deliberately refuses wallet-identity conditions and high-order search.
    Interactions can be added as another versioned family after independent
    evidence and an explicit implementation review.
    """

    def discover(self, family: SearchFamily, records: Sequence[Mapping[str, Any]]) -> list[DiscoveryCandidate]:
        candidates: list[DiscoveryCandidate] = []
        for feature_id in family.feature_ids:
            for horizon in family.horizons_seconds:
                rows = [row for row in records if row.get("horizon_seconds") == horizon and row.get("features", {}).get(feature_id) is not None and row.get("net_outcome") is not None]
                if len(rows) < family.minimum_sample:
                    continue
                values = [float(row["features"][feature_id]) for row in rows]
                # Zero is an auditable and interpretable action/direction threshold;
                # for continuous features the family can choose a median threshold.
                threshold = 0.0 if min(values) < 0 < max(values) else fmean(values)
                for condition, selected in (("above", [row for row in rows if float(row["features"][feature_id]) > threshold]),
                                            ("below", [row for row in rows if float(row["features"][feature_id]) <= threshold])):
                    if len(selected) < family.minimum_sample:
                        continue
                    outcomes = [float(row["net_outcome"]) for row in selected]
                    expectancy = fmean(outcomes)
                    baseline = fmean(float(row["net_outcome"]) for row in rows)
                    effect = expectancy - baseline
                    if abs(effect) < family.minimum_effect_size:
                        continue
                    fingerprint = canonical_hash({"family": family.payload(), "feature": feature_id, "condition": condition,
                                                  "threshold": threshold, "horizon": horizon,
                                                  "observations": [str(row["observation_id"]) for row in selected]})
                    candidates.append(DiscoveryCandidate(
                        discovery_id=f"discovery-{fingerprint[:24]}", family_id=family.family_id, family_version=family.version,
                        feature_id=feature_id, feature_version=1, condition=condition, threshold=threshold,
                        horizon_seconds=horizon, sample_count=len(selected), estimated_net_expectancy=expectancy,
                        effect_size=effect, data_fingerprint=fingerprint,
                        observation_ids=tuple(str(row["observation_id"]) for row in selected),
                    ))
        # Absolute effect is merely a proposal priority, never a promotion rule.
        return sorted(candidates, key=lambda item: (-abs(item.effect_size), item.discovery_id))[:family.maximum_candidates]
