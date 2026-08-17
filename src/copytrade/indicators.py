"""Indicator promotion: validated scientific relationships with provenance."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from enum import StrEnum
from typing import Any, Mapping

from .science_repository import ScientificRepository


class IndicatorState(StrEnum):
    EXPERIMENTAL = "EXPERIMENTAL"
    VALIDATED = "VALIDATED"
    ACTIVE = "ACTIVE"
    DEGRADED = "DEGRADED"
    RETIRED = "RETIRED"


@dataclass(frozen=True)
class IndicatorProvenance:
    originating_hypothesis_id: str
    originating_hypothesis_version: int
    historical_experiment_id: str
    forward_experiment_ids: tuple[str, ...]
    feature_versions: tuple[tuple[str, int], ...]
    model_assumptions: Mapping[str, Any]
    data_fingerprints: Mapping[str, str]
    code_sha: str
    valid_regimes: tuple[str, ...]
    degraded_regimes: tuple[str, ...]
    invalid_regimes: tuple[str, ...]
    unknown_regimes: tuple[str, ...]
    expected_horizon_seconds: float
    alpha_decay_curve: tuple[Mapping[str, float], ...]
    cost_robustness: Mapping[str, Any]
    evidence_state: Mapping[str, Any]

    def __post_init__(self) -> None:
        if not self.originating_hypothesis_id or not self.historical_experiment_id or not self.feature_versions:
            raise ValueError("Indicator provenance requires hypothesis, historical experiment, and versioned features.")
        if self.expected_horizon_seconds <= 0 or not self.code_sha or not self.data_fingerprints:
            raise ValueError("Indicator provenance requires horizon, code SHA, and data fingerprints.")

    def payload(self) -> dict[str, Any]:
        result = asdict(self)
        result["feature_versions"] = [{"feature_id": name, "version": version} for name, version in self.feature_versions]
        return result


class IndicatorRegistry:
    def __init__(self, repository: ScientificRepository) -> None:
        self.repository = repository

    def register(self, indicator_id: str, version: int, provenance: IndicatorProvenance, *, state: IndicatorState, created_at: str, predecessor_id: str | None = None) -> dict[str, Any]:
        if state in {IndicatorState.VALIDATED, IndicatorState.ACTIVE} and not provenance.forward_experiment_ids:
            raise ValueError("Validated or active indicators require forward-shadow evidence.")
        return self.repository.register_indicator(indicator_id, version, state=state.value, provenance=provenance.payload(), created_at=created_at, predecessor_id=predecessor_id)

    def set_state(self, indicator_id: str, version: int, state: IndicatorState) -> None:
        self.repository.set_indicator_state(indicator_id, version, state.value)
