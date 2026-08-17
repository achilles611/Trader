"""The intentionally narrow, deterministic E.1 experiment runner."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable, Mapping

from .ledger import CorpusProvenanceError, ExperimentConflictError, PhaseELedger
from .types import CANONICALIZATION_VERSION, ExperimentConclusion, ExperimentResult, ExperimentStatus, RejectionReason, canonical_hash


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class NullExperimentRunner:
    """Prove E.1 lifecycle mechanics with a deterministic zero-effect test.

    It has no candidate generator, model, prediction, signal, or execution
    route.  Its only supported test intentionally retains a negative result.
    """

    STATISTIC_NAME = "DETERMINISTIC_NULL_EFFECT"
    CODE_VERSION = "phase-e1-null-runner-v1"
    CONFIG_VERSION = "phase-e1-null-config-v1"

    def __init__(self, ledger: PhaseELedger, *, clock: Callable[[], str] = utc_now) -> None:
        self.ledger = ledger
        self.clock = clock

    def run(self, experiment_id: str, *, before_evaluate: Callable[[], None] | None = None) -> dict[str, Any]:
        existing = self.ledger.get(experiment_id)
        if existing["result"] is not None:
            return existing
        self._validate_runner_contract(existing)
        self.ledger.verify_current_provenance(experiment_id)
        started = self.ledger.start(experiment_id, started_at=self.clock())
        try:
            if before_evaluate is not None:
                before_evaluate()
            result = self.evaluate(started)
            # A second provenance check prevents an evaluator from committing
            # a result after a source-contract change.
            self.ledger.verify_current_provenance(experiment_id)
            return self.ledger.record_result(experiment_id, result, recorded_at=self.clock())
        except Exception as exc:
            # KeyboardInterrupt/SystemExit deliberately remain RUNNING.  The
            # next process must append an explicit recovery event instead of
            # pretending that an interrupted run succeeded or failed.
            try:
                self.ledger.fail(
                    experiment_id,
                    reason=RejectionReason.EXECUTION_FAILURE.value,
                    failed_at=self.clock(),
                    payload={"error_class": type(exc).__name__, "message": str(exc)[:300]},
                )
            except ExperimentConflictError:
                pass
            raise

    def reproduce(self, experiment_id: str) -> dict[str, Any]:
        """Recompute a persisted result without mutating any ledger record."""
        experiment = self.ledger.get(experiment_id)
        if experiment["status"] not in {ExperimentStatus.COMPLETED.value, ExperimentStatus.REJECTED.value} or experiment["result"] is None:
            raise ExperimentConflictError("Only completed/rejected experiments with a persisted result can be reproduced.")
        self._validate_runner_contract(experiment)
        self.ledger.verify_current_provenance(experiment_id)
        recomputed = self.evaluate(experiment).payload()
        persisted = experiment["result"]
        return {
            "experiment_id": experiment_id,
            "reproducible": canonical_hash(recomputed) == canonical_hash(persisted),
            "recomputed_result": recomputed,
            "persisted_result": persisted,
            "specification_hash": experiment["specification_hash"],
            "corpus_provenance_hash": experiment["corpus_provenance_hash"],
            "trading_authority": False,
        }

    def evaluate(self, experiment: Mapping[str, Any]) -> ExperimentResult:
        definition = experiment["definition"]
        if definition.get("statistical_test", {}).get("name") != self.STATISTIC_NAME:
            raise ValueError("E.1 runner only supports the explicitly declared deterministic null statistic.")
        corpus = experiment["corpus_provenance"]
        sample_count = int(corpus["verified_observation_count"])
        minimum = int(definition["minimum_sample_size"])
        if sample_count < minimum:
            reason = RejectionReason.INSUFFICIENT_SAMPLE
        else:
            reason = RejectionReason.NULL_HYPOTHESIS_NOT_REJECTED
        return ExperimentResult(
            sample_count=sample_count,
            effect_size=0.0,
            p_value=1.0,
            confidence_interval_low=0.0,
            confidence_interval_high=0.0,
            statistic={
                "name": self.STATISTIC_NAME,
                "method_version": self.CODE_VERSION,
                "canonicalization_version": CANONICALIZATION_VERSION,
                "corpus_provenance_hash": experiment["corpus_provenance_hash"],
                "specification_hash": experiment["specification_hash"],
                "minimum_sample_size": minimum,
            },
            conclusion=ExperimentConclusion.REJECTED,
            rejection_reason=reason,
        )

    def _validate_runner_contract(self, experiment: Mapping[str, Any]) -> None:
        definition = experiment["definition"]
        if (definition.get("code_version") != self.CODE_VERSION
                or definition.get("config_version") != self.CONFIG_VERSION):
            raise CorpusProvenanceError("Experiment code/config identity is stale or unavailable; E.1 refuses to evaluate it.")
