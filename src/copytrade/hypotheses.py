"""Immutable registered propositions and retained failed ideas."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import StrEnum
from typing import Any, Mapping

from .science_repository import ScientificRepository, canonical_hash


class HypothesisState(StrEnum):
    PROPOSED = "PROPOSED"
    REGISTERED = "REGISTERED"
    HISTORICAL_TESTING = "HISTORICAL_TESTING"
    REJECTED_HISTORICAL = "REJECTED_HISTORICAL"
    FORWARD_SHADOW = "FORWARD_SHADOW"
    PROMOTED = "PROMOTED"
    DEGRADED = "DEGRADED"
    RETIRED = "RETIRED"


_TRANSITIONS = {
    HypothesisState.PROPOSED: {HypothesisState.REGISTERED, HypothesisState.RETIRED},
    HypothesisState.REGISTERED: {HypothesisState.HISTORICAL_TESTING, HypothesisState.RETIRED},
    HypothesisState.HISTORICAL_TESTING: {HypothesisState.REJECTED_HISTORICAL, HypothesisState.FORWARD_SHADOW, HypothesisState.DEGRADED},
    HypothesisState.FORWARD_SHADOW: {HypothesisState.PROMOTED, HypothesisState.DEGRADED, HypothesisState.RETIRED},
    HypothesisState.PROMOTED: {HypothesisState.DEGRADED, HypothesisState.RETIRED},
    HypothesisState.DEGRADED: {HypothesisState.FORWARD_SHADOW, HypothesisState.RETIRED},
    HypothesisState.REJECTED_HISTORICAL: {HypothesisState.RETIRED},
    HypothesisState.RETIRED: set(),
}


@dataclass(frozen=True)
class HypothesisDefinition:
    hypothesis_id: str
    version: int
    title: str
    scientific_statement: str
    null_hypothesis: str
    alternative_hypothesis: str
    feature_versions: tuple[tuple[str, int], ...]
    thresholds: Mapping[str, Any]
    symbol_scope: tuple[str, ...]
    regime_scope: tuple[str, ...]
    prediction_horizon_seconds: float
    entry_definition: str
    outcome_definition: str
    cost_model: Mapping[str, Any]
    detection_latency_ms: int
    fee_model: Mapping[str, Any]
    slippage_model: Mapping[str, Any]
    minimum_sample: int
    discovery_range: Mapping[str, str]
    validation_range: Mapping[str, str]
    purge_embargo_seconds: float
    success_criteria: Mapping[str, Any]
    failure_criteria: Mapping[str, Any]
    multiple_testing_family: str
    registered_at: str
    code_sha: str
    data_fingerprints: Mapping[str, str]
    predecessor_id: str | None = None
    tags: tuple[str, ...] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        if self.version <= 0 or not self.hypothesis_id or not self.title:
            raise ValueError("Hypothesis identity and title are required.")
        if not self.feature_versions or self.prediction_horizon_seconds <= 0 or self.minimum_sample <= 0:
            raise ValueError("Hypotheses require features, a positive horizon, and a minimum sample.")
        if not self.multiple_testing_family or not self.code_sha or not self.data_fingerprints:
            raise ValueError("Hypotheses require FDR family, code SHA, and data fingerprints.")

    def payload(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["feature_versions"] = [{"feature_id": name, "version": version} for name, version in self.feature_versions]
        return payload

    @property
    def config_hash(self) -> str:
        return canonical_hash(self.payload())


class HypothesisRegistry:
    def __init__(self, repository: ScientificRepository) -> None:
        self.repository = repository

    def register(self, definition: HypothesisDefinition, *, state: HypothesisState = HypothesisState.REGISTERED) -> dict[str, Any]:
        result = self.repository.register_hypothesis(
            definition.hypothesis_id, definition.version, definition.payload(), state=state.value,
            registered_at=definition.registered_at, predecessor_id=definition.predecessor_id,
        )
        result["similar_rejections"] = self.similar_rejections(definition)
        return result

    def transition(self, definition: HypothesisDefinition, *, from_state: HypothesisState, to_state: HypothesisState, reason: str, event_id: str, created_at: str, evidence: Mapping[str, Any] | None = None) -> dict[str, Any]:
        if to_state not in _TRANSITIONS[from_state]:
            raise ValueError(f"Invalid hypothesis transition: {from_state} -> {to_state}")
        return self.repository.transition_hypothesis(definition.hypothesis_id, definition.version, state=to_state.value, reason=reason, event_id=event_id, created_at=created_at, evidence=evidence)

    def reject(self, definition: HypothesisDefinition, *, experiment_id: str, reason: str, result: Mapping[str, Any], recorded_at: str, event_id: str) -> None:
        self.transition(definition, from_state=HypothesisState.HISTORICAL_TESTING, to_state=HypothesisState.REJECTED_HISTORICAL, reason=reason, event_id=event_id, created_at=recorded_at, evidence=result)
        self.repository.add_graveyard_entry(hypothesis_id=definition.hypothesis_id, version=definition.version, experiment_id=experiment_id, reason=reason, payload={"hypothesis": definition.payload(), "result": dict(result), "config_hash": definition.config_hash, "code_sha": definition.code_sha}, recorded_at=recorded_at)

    def similar_rejections(self, definition: HypothesisDefinition, *, minimum_similarity: float = 0.60) -> list[dict[str, Any]]:
        candidate = self._terms(definition.payload())
        matches = []
        for entry in self.repository.list_graveyard():
            prior = entry["payload"].get("hypothesis", {})
            previous = self._terms(prior)
            union = candidate | previous
            similarity = len(candidate & previous) / len(union) if union else 0.0
            if similarity >= minimum_similarity:
                matches.append({"hypothesis_id": entry["hypothesis_id"], "version": entry["version"], "reason": entry["reason"], "similarity": round(similarity, 4)})
        return sorted(matches, key=lambda item: item["similarity"], reverse=True)

    @staticmethod
    def _terms(payload: Mapping[str, Any]) -> set[str]:
        parts = [str(payload.get(name, "")) for name in ("title", "scientific_statement", "entry_definition", "outcome_definition")]
        parts.extend(str(item.get("feature_id", "")) for item in payload.get("feature_versions", []) if isinstance(item, Mapping))
        return {token.lower() for part in parts for token in part.replace("_", " ").split() if len(token) > 2}
