"""Deterministic forward-evidence degradation checks."""

from __future__ import annotations

from dataclasses import dataclass
from statistics import fmean
from typing import Mapping, Sequence


@dataclass(frozen=True)
class DriftAssessment:
    state: str
    reason: str
    sample_count: int
    net_expectancy: float | None
    calibration_error: float | None
    cost_inflation: float | None


def assess_forward_drift(records: Sequence[Mapping[str, object]], *, minimum_observations: int, net_expectancy_floor: float) -> DriftAssessment:
    if len(records) < minimum_observations:
        return DriftAssessment("INSUFFICIENT_EVIDENCE", "forward sample below degradation minimum", len(records), None, None, None)
    outcomes = [float(record["net_outcome"]) for record in records]
    expectancy = fmean(outcomes)
    probabilities = [float(record.get("trade_confidence", 0.5)) for record in records]
    actual = [1.0 if outcome > 0 else 0.0 for outcome in outcomes]
    calibration = fmean((probability - realized) ** 2 for probability, realized in zip(probabilities, actual))
    cost_pairs = [(float(record["actual_cost"]), float(record["expected_cost"])) for record in records
                  if record.get("actual_cost") is not None and float(record.get("expected_cost", 0.0)) > 0]
    actual_costs = [actual_cost for actual_cost, _ in cost_pairs]
    expected_costs = [expected_cost for _, expected_cost in cost_pairs]
    inflation = (fmean(actual_costs) / fmean(expected_costs)) if actual_costs and expected_costs else None
    if expectancy <= net_expectancy_floor:
        return DriftAssessment("DEGRADED", f"forward net expectancy {expectancy:.8f} <= configured floor {net_expectancy_floor:.8f}", len(records), expectancy, calibration, inflation)
    if inflation is not None and inflation > 1.25:
        return DriftAssessment("DEGRADED", f"realized cost inflation {inflation:.3f} exceeds 1.250", len(records), expectancy, calibration, inflation)
    if calibration > 0.25:
        return DriftAssessment("DEGRADED", f"forward calibration Brier score {calibration:.6f} exceeds 0.250000", len(records), expectancy, calibration, inflation)
    return DriftAssessment("HEALTHY", "forward expectancy, costs, and calibration remain within configured bounds", len(records), expectancy, calibration, inflation)
