"""Temporal experiments, block-aware statistics, and forward shadow records."""

from __future__ import annotations

import math
import random
from dataclasses import dataclass
from datetime import datetime, timezone
from statistics import fmean
from typing import Any, Iterable, Mapping, Sequence

from .science_repository import ScientificRepository, canonical_hash


def _as_time(value: str | datetime) -> datetime:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def benjamini_hochberg(p_values: Sequence[float]) -> list[float]:
    """Return q-values in original order using the monotone BH correction."""
    if any(not 0 <= value <= 1 for value in p_values):
        raise ValueError("p-values must be in [0, 1].")
    count = len(p_values)
    ranked = sorted(enumerate(p_values), key=lambda item: item[1])
    adjusted = [0.0] * count
    minimum = 1.0
    for rank in range(count, 0, -1):
        index, p_value = ranked[rank - 1]
        minimum = min(minimum, p_value * count / rank)
        adjusted[index] = min(1.0, minimum)
    return adjusted


def block_sign_permutation_pvalue(values: Sequence[float], *, block_size: int = 8, iterations: int = 1_000, seed: int = 7) -> float:
    """Deterministic block sign resampling avoids IID row randomization."""
    if not values:
        return 1.0
    if block_size <= 0 or iterations <= 0:
        raise ValueError("Block size and iterations must be positive.")
    observed = fmean(values)
    blocks = [list(values[start:start + block_size]) for start in range(0, len(values), block_size)]
    rng = random.Random(seed)
    extreme = 0
    for _ in range(iterations):
        sample = [sign * item for block in blocks for sign in (1 if rng.random() >= 0.5 else -1,) for item in block]
        if abs(fmean(sample)) >= abs(observed):
            extreme += 1
    return (extreme + 1) / (iterations + 1)


@dataclass(frozen=True)
class TemporalSplit:
    discovery_start: str
    train_end: str
    validation_start: str
    validation_end: str
    prediction_horizon_seconds: float
    embargo_seconds: float

    def __post_init__(self) -> None:
        if self.prediction_horizon_seconds <= 0 or self.embargo_seconds < 0:
            raise ValueError("Prediction horizon must be positive and embargo nonnegative.")
        if _as_time(self.validation_start) < _as_time(self.train_end):
            raise ValueError("Validation must begin after training ends.")
        separation = (_as_time(self.validation_start) - _as_time(self.train_end)).total_seconds()
        if separation < self.prediction_horizon_seconds + self.embargo_seconds:
            raise ValueError("Temporal split lacks required prediction-horizon purge/embargo separation.")

    def partition(self, observations: Iterable[Mapping[str, Any]], *, timestamp_field: str = "timestamp") -> tuple[list[Mapping[str, Any]], list[Mapping[str, Any]]]:
        train, validation = [], []
        train_end, validation_start, validation_end = _as_time(self.train_end), _as_time(self.validation_start), _as_time(self.validation_end)
        for item in observations:
            observed = _as_time(str(item[timestamp_field]))
            if observed <= train_end:
                train.append(item)
            elif validation_start <= observed <= validation_end:
                validation.append(item)
        train_ids = {str(item.get("id", item.get(timestamp_field))) for item in train}
        validation_ids = {str(item.get("id", item.get(timestamp_field))) for item in validation}
        if train_ids & validation_ids:
            raise ValueError("Temporal split leaked an observation into both train and validation.")
        return train, validation


@dataclass(frozen=True)
class CostModel:
    fee: float = 0.0
    spread: float = 0.0
    slippage: float = 0.0
    market_impact: float = 0.0
    latency: float = 0.0

    @property
    def total(self) -> float:
        return self.fee + self.spread + self.slippage + self.market_impact + self.latency


def net_outcome(record: Mapping[str, Any]) -> tuple[float, CostModel]:
    costs = CostModel(
        fee=float(record.get("fee", 0.0)), spread=float(record.get("spread_cost", 0.0)),
        slippage=float(record.get("slippage_cost", 0.0)), market_impact=float(record.get("market_impact_cost", 0.0)),
        latency=float(record.get("latency_cost", 0.0)),
    )
    return float(record.get("gross_return", record.get("outcome_return", 0.0))) - costs.total, costs


class HistoricalExperimentEngine:
    def __init__(self, repository: ScientificRepository, *, seed: int = 7, resamples: int = 1_000) -> None:
        self.repository, self.seed, self.resamples = repository, seed, resamples

    def evaluate(self, *, experiment_id: str, records: Sequence[Mapping[str, Any]], minimum_sample: int, block_size: int = 8, q_value: float | None = None) -> dict[str, Any]:
        net_values, gross_values, costs = [], [], []
        for record in records:
            net, cost = net_outcome(record)
            net_values.append(net); gross_values.append(float(record.get("gross_return", record.get("outcome_return", 0.0)))); costs.append(cost.total)
        sample_count = len(net_values)
        p_value = block_sign_permutation_pvalue(net_values, block_size=block_size, iterations=self.resamples, seed=self.seed) if sample_count else 1.0
        result = {
            "sample_count": sample_count,
            "gross_expectancy": fmean(gross_values) if gross_values else 0.0,
            "expected_costs": fmean(costs) if costs else 0.0,
            "net_expectancy": fmean(net_values) if net_values else 0.0,
            "effect_size": fmean(net_values) if net_values else 0.0,
            "uncertainty_standard_error": self._standard_error(net_values),
            "p_value": p_value,
            "q_value": q_value,
            "minimum_sample": minimum_sample,
            "economic_pass": sample_count >= minimum_sample and fmean(net_values) > 0 if net_values else False,
            "statistical_method": {"name": "deterministic_block_sign_permutation", "block_size": block_size, "seed": self.seed, "resamples": self.resamples},
        }
        return result

    def persist_result(self, experiment_id: str, result: Mapping[str, Any], *, recorded_at: str) -> dict[str, Any]:
        return self.repository.record_experiment_result(experiment_id, result, recorded_at=recorded_at)

    def evaluate_family(self, experiments: Sequence[tuple[str, Sequence[Mapping[str, Any]], int]], *, block_size: int = 8) -> dict[str, dict[str, Any]]:
        """Evaluate one predeclared family and attach BH q-values to every result."""
        evaluated = {
            experiment_id: self.evaluate(experiment_id=experiment_id, records=records, minimum_sample=minimum_sample, block_size=block_size)
            for experiment_id, records, minimum_sample in experiments
        }
        q_values = benjamini_hochberg([result["p_value"] for result in evaluated.values()])
        for result, q_value in zip(evaluated.values(), q_values):
            result["q_value"] = q_value
            result["fdr_family_size"] = len(evaluated)
        return evaluated

    @staticmethod
    def fingerprint(records: Sequence[Mapping[str, Any]], configuration: Mapping[str, Any]) -> str:
        return canonical_hash({"records": list(records), "configuration": dict(configuration)})

    @staticmethod
    def _standard_error(values: Sequence[float]) -> float:
        if len(values) < 2:
            return 0.0
        average = fmean(values)
        return math.sqrt(sum((value - average) ** 2 for value in values) / (len(values) - 1) / len(values))


class ForwardShadowEngine:
    """Predictions are stored before outcomes and never rewritten afterwards."""

    def __init__(self, repository: ScientificRepository) -> None:
        self.repository = repository

    def predict(self, prediction_id: str, *, experiment_id: str, predicted_at: str, market: str, horizon_seconds: float, features: Mapping[str, Any], predicted_direction: str, predicted_net_edge: float, trade_confidence: float, model_confidence: float, expected_costs: float) -> dict[str, Any]:
        if predicted_direction not in {"long", "short", "flat"}:
            raise ValueError("Predicted direction must be long, short, or flat.")
        payload = {"features": dict(features), "predicted_direction": predicted_direction, "predicted_net_edge": predicted_net_edge, "trade_confidence": trade_confidence, "model_confidence": model_confidence, "expected_costs": expected_costs}
        return self.repository.create_forward_prediction(prediction_id, experiment_id=experiment_id, predicted_at=predicted_at, horizon_seconds=horizon_seconds, market=market, payload=payload)

    def resolve(self, prediction_id: str, *, realized_at: str, realized_net_outcome: float, outcome_metadata: Mapping[str, Any] | None = None) -> dict[str, Any]:
        return self.repository.record_forward_outcome(prediction_id, realized_at=realized_at, payload={"realized_net_outcome": realized_net_outcome, "outcome_metadata": dict(outcome_metadata or {})})
