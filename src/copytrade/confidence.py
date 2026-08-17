"""Separate model confidence, trade confidence, and uncertainty shrinkage."""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass
from typing import Any, Mapping


def _unit(value: float, name: str) -> float:
    if not 0 <= value <= 1:
        raise ValueError(f"{name} must be in [0, 1].")
    return float(value)


def effective_confidence(trade_confidence: float, model_confidence: float) -> float:
    return 0.5 + (_unit(trade_confidence, "trade_confidence") - 0.5) * _unit(model_confidence, "model_confidence")


@dataclass(frozen=True)
class ModelEvidence:
    effective_sample_strength: float
    validation_strength: float
    walk_forward_stability: float
    forward_shadow_strength: float | None
    regime_coverage: float
    provenance_quality: float
    calibration_quality: float
    fdr_quality: float
    temporal_stability: float
    experimental_ceiling: float = 0.70
    forward_minimum_observations: int = 100
    forward_observations: int = 0

    def confidence(self) -> float:
        values = [
            self.effective_sample_strength, self.validation_strength, self.walk_forward_stability,
            self.regime_coverage, self.provenance_quality, self.calibration_quality, self.fdr_quality, self.temporal_stability,
        ]
        mature_forward = self.forward_shadow_strength is not None and self.forward_observations >= self.forward_minimum_observations
        if mature_forward:
            values.append(self.forward_shadow_strength)
        values = [_unit(value, "model evidence") for value in values]
        aggregate = math.prod(max(value, 1e-9) for value in values) ** (1 / len(values))
        return min(aggregate, _unit(self.experimental_ceiling, "experimental_ceiling")) if not mature_forward else aggregate


@dataclass(frozen=True)
class ConfidenceSnapshot:
    model_confidence: float
    trade_confidence: float
    effective_confidence: float
    explanation: tuple[str, ...]

    def payload(self) -> dict[str, Any]:
        return asdict(self)


class ConfidenceEngine:
    """Recalculates probabilities from supplied validated evidence, never a clock tax."""

    def model_confidence(self, evidence: ModelEvidence) -> float:
        return evidence.confidence()

    def trade_confidence(self, baseline: float, *, evidence_updates: Mapping[str, float]) -> tuple[float, tuple[str, ...]]:
        """Apply calibrated log-odds updates; age appears only through a learned input update."""
        probability = _unit(baseline, "baseline trade confidence")
        log_odds = math.log(max(probability, 1e-9) / max(1 - probability, 1e-9))
        explanations = []
        for name, update in evidence_updates.items():
            if not math.isfinite(update):
                raise ValueError(f"Evidence update '{name}' is not finite.")
            log_odds += float(update)
            if update:
                explanations.append(f"{name}:{update:+.3f} calibrated log-odds")
        recalculated = 1 / (1 + math.exp(-max(-30.0, min(30.0, log_odds))))
        return recalculated, tuple(explanations or ("No new validated evidence.",))

    def snapshot(self, model_evidence: ModelEvidence, baseline_trade_confidence: float, *, evidence_updates: Mapping[str, float]) -> ConfidenceSnapshot:
        model = self.model_confidence(model_evidence)
        trade, explanation = self.trade_confidence(baseline_trade_confidence, evidence_updates=evidence_updates)
        return ConfidenceSnapshot(model, trade, effective_confidence(trade, model), explanation)
