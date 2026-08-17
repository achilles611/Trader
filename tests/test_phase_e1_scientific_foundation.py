from __future__ import annotations

import math
import json
import sqlite3
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from pathlib import Path

from src.copytrade.science_repository import ScientificRepository
from src.phase_e import (
    CorpusProvenanceError,
    ExperimentConflictError,
    ExperimentResult,
    ExperimentStatus,
    FeatureReference,
    HypothesisDefinition,
    NullExperimentRunner,
    OutcomeHorizon,
    PartitionIdentity,
    PhaseELedger,
    RejectionReason,
    StatisticSpec,
)
from src.phase_e.types import ExperimentConclusion


NOW = "2026-08-17T02:00:00Z"
CORPUS = "corpus-e1-test"


def partition(*, horizon: int = 5, validation_start: str = "2026-08-17T00:10:10Z", test_start: str = "2026-08-17T00:20:20Z") -> PartitionIdentity:
    return PartitionIdentity(
        partition_id="e1-temporal-split-v1",
        train_start="2026-08-17T00:00:00Z",
        train_end="2026-08-17T00:10:00Z",
        validation_start=validation_start,
        validation_end="2026-08-17T00:20:00Z",
        test_start=test_start,
        test_end="2026-08-17T00:30:00Z",
        purge_seconds=0,
        embargo_seconds=0,
        random_seed=17,
        horizon=OutcomeHorizon(horizon),
    )


def hypothesis(*, version: int = 1, required_features: tuple[FeatureReference, ...] = (FeatureReference("wallet_action", 1),), code_version: str = "phase-e1-null-runner-v1", config_version: str = "phase-e1-null-config-v1", success_threshold: dict[str, object] | None = None) -> HypothesisDefinition:
    return HypothesisDefinition(
        hypothesis_id="H-E1-NULL-EVIDENCE",
        version=version,
        title="D.7 null-evidence lifecycle control",
        proposition="The bound corpus supports a zero-effect null control at five seconds.",
        null_hypothesis="The predefined effect is exactly zero.",
        alternative_hypothesis="The predefined effect differs from zero.",
        population_definition="Every normalized observation in the bound D.7 corpus snapshot.",
        required_features=required_features,
        feature_transforms={"wallet_action@1": "identity"},
        entry_definition="No entry; this E.1 control never opens a position.",
        outcome_definition="Deterministic null effect for lifecycle verification.",
        outcome_horizon=OutcomeHorizon(5),
        comparator_definition="A fixed zero-effect comparator declared before evaluation.",
        inclusion_rules=("D.7 corpus coverage is PROVEN_COMPLETE",),
        exclusion_rules=("Do not infer unavailable market fields",),
        minimum_sample_size=10,
        statistical_test=StatisticSpec("DETERMINISTIC_NULL_EFFECT", {"two_sided": True}),
        minimum_effect_size=0.001,
        success_threshold=success_threshold or {"minimum_effect_size": 0.001, "maximum_p_value": 0.05},
        failure_threshold={"zero_effect_rejects": True},
        multiple_testing_family="e1-null-controls",
        partition=partition(),
        code_version=code_version,
        config_version=config_version,
        created_at=NOW,
    )


class PhaseE1ScientificFoundationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / "hot" / "science.sqlite3"
        self.phase_d = ScientificRepository(self.path)
        self.phase_d.initialize()
        self._record_corpus()
        self.ledger = PhaseELedger(self.path)
        self.runner = NullExperimentRunner(self.ledger, clock=lambda: "2026-08-17T03:00:00Z")

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _record_corpus(self, *, fingerprint: str = CORPUS, payload_observation_fingerprint: str = "observations-e1", coverage_state: str = "PROVEN_COMPLETE") -> None:
        coverage = {
            "coverage_id": f"coverage-{fingerprint}",
            "interval_start": "2026-08-17T00:00:00Z",
            "interval_end": "2026-08-17T01:00:00Z",
            "source_name": f"hyperliquid_hypercore_node_fills_by_block:{fingerprint}",
            "state": coverage_state,
            "coverage_fraction": 1.0,
            "expected_hours": 1,
            "verified_hours": 1,
            "missing_hours": 0,
            "malformed_hours": 0,
            "parsed_hours": 1,
            "observation_count": 25,
            "duplicate_count": 0,
            "timestamp_anomalies": 0,
            "wallet_attribution_quality": "official_per_fill",
            "market_evidence_availability": "trade_print_only",
            "computed_at": NOW,
            "details": {"test_fixture": True},
        }
        self.phase_d.record_coverage(coverage)
        self.phase_d.record_corpus_snapshot({
            "corpus_fingerprint": fingerprint,
            "interval_start": coverage["interval_start"],
            "interval_end": coverage["interval_end"],
            "coverage_id": coverage["coverage_id"],
            "observation_fingerprint": "observations-e1",
            "feature_versions": [{"feature_id": "wallet_action", "version": 1}],
            "symbols": ["BTC"],
            "code_sha": "d7-data-ignition-v1",
            "config_sha": "d7-config-sha",
            "created_at": NOW,
            "coverage": {"coverage_id": coverage["coverage_id"], "state": coverage_state},
            "source_hours": [coverage["interval_start"]],
            # This allows one fixture to prove ledger corruption detection.
            "fixture_payload_observation_fingerprint": payload_observation_fingerprint,
        })

    def test_predeclared_contract_is_deterministic_reproducible_and_rejected(self) -> None:
        registered = self.ledger.register(hypothesis(), corpus_fingerprint=CORPUS)
        duplicate = self.ledger.register(hypothesis(), corpus_fingerprint=CORPUS)
        self.assertEqual(registered["experiment_id"], duplicate["experiment_id"])
        self.assertEqual(len(self.ledger.events(registered["experiment_id"])), 1)
        finished = self.runner.run(registered["experiment_id"])
        self.assertEqual(finished["status"], ExperimentStatus.REJECTED.value)
        self.assertEqual(finished["result"]["rejection_reason"], RejectionReason.NULL_HYPOTHESIS_NOT_REJECTED.value)
        reproduced = self.runner.reproduce(registered["experiment_id"])
        self.assertTrue(reproduced["reproducible"])
        self.assertFalse(reproduced["trading_authority"])
        self.assertEqual(self.ledger.get(registered["experiment_id"])["corpus_provenance"]["corpus_fingerprint"], CORPUS)

    def test_hypothesis_and_result_cannot_be_rewritten_or_deleted(self) -> None:
        registered = self.ledger.register(hypothesis(), corpus_fingerprint=CORPUS)
        with self.assertRaises(ExperimentConflictError):
            self.ledger.register(hypothesis(success_threshold={"minimum_effect_size": 0.5}), corpus_fingerprint=CORPUS)
        self.runner.run(registered["experiment_id"])
        with closing(sqlite3.connect(self.path)) as connection:
            with self.assertRaises(sqlite3.DatabaseError):
                connection.execute("UPDATE phase_e_hypotheses SET definition_json='{}'")
            with self.assertRaises(sqlite3.DatabaseError):
                connection.execute("UPDATE phase_e_experiment_results SET result_json='{}'")
            with self.assertRaises(sqlite3.DatabaseError):
                connection.execute("DELETE FROM phase_e_experiment_events")
        self.assertEqual(len(self.ledger.events(registered["experiment_id"])), 3)

    def test_missing_or_corrupt_d_provenance_fails_closed_before_registration(self) -> None:
        with self.assertRaises(CorpusProvenanceError):
            self.ledger.register(hypothesis(), corpus_fingerprint="missing-corpus")
        self._record_corpus(fingerprint="corpus-corrupt", payload_observation_fingerprint="not-the-column")
        # Make the snapshot internally inconsistent in a way a normal D.7
        # writer cannot silently repair.
        with closing(sqlite3.connect(self.path)) as connection:
            payload = json.loads(connection.execute("SELECT payload_json FROM science_corpus_snapshots WHERE corpus_fingerprint='corpus-corrupt'").fetchone()[0])
            payload["observation_fingerprint"] = "not-the-column"
            connection.execute("DROP TRIGGER science_corpus_snapshots_immutable")
            connection.execute("UPDATE science_corpus_snapshots SET payload_json=? WHERE corpus_fingerprint='corpus-corrupt'", (json.dumps(payload),))
            connection.commit()
        with self.assertRaises(CorpusProvenanceError):
            self.ledger.register(hypothesis(version=2), corpus_fingerprint="corpus-corrupt")
        self.assertEqual(self.ledger.list(), [])

    def test_partition_leakage_invalid_horizon_and_nonfinite_statistics_are_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "purge/embargo"):
            partition(validation_start="2026-08-17T00:10:01Z")
        with self.assertRaisesRegex(ValueError, "short-horizon"):
            OutcomeHorizon(7)
        with self.assertRaisesRegex(ValueError, "NaN"):
            StatisticSpec("x", {"threshold": math.nan})
        with self.assertRaisesRegex(ValueError, "Infinity"):
            ExperimentResult(1, math.inf, 0.5, 0, 1, {}, ExperimentConclusion.REJECTED, RejectionReason.MALFORMED_STATISTIC)

    def test_required_features_and_code_configuration_identity_fail_closed(self) -> None:
        missing = hypothesis(version=2, required_features=(FeatureReference("not-in-d7", 1),))
        with self.assertRaises(CorpusProvenanceError):
            self.ledger.register(missing, corpus_fingerprint=CORPUS)
        stale = hypothesis(version=3, code_version="future-code")
        registered = self.ledger.register(stale, corpus_fingerprint=CORPUS)
        with self.assertRaises(CorpusProvenanceError):
            self.runner.run(registered["experiment_id"])
        self.assertEqual(self.ledger.get(registered["experiment_id"])["status"], ExperimentStatus.REGISTERED.value)

    def test_failure_restart_recovery_and_concurrent_duplicate_execution_are_auditable(self) -> None:
        registered = self.ledger.register(hypothesis(), corpus_fingerprint=CORPUS)
        with self.assertRaisesRegex(RuntimeError, "deliberate"):
            self.runner.run(registered["experiment_id"], before_evaluate=lambda: (_ for _ in ()).throw(RuntimeError("deliberate failure")))
        failed = self.ledger.get(registered["experiment_id"])
        self.assertEqual(failed["status"], ExperimentStatus.FAILED.value)
        self.assertIsNone(failed["result"])

        recovered_registration = self.ledger.register(hypothesis(version=2), corpus_fingerprint=CORPUS)
        self.ledger.start(recovered_registration["experiment_id"], started_at=NOW)
        restarted = PhaseELedger(self.path).recover_interrupted(recovered_registration["experiment_id"], recovered_at="2026-08-17T03:00:00Z")
        self.assertEqual(restarted["status"], ExperimentStatus.RECOVERABLE.value)
        self.assertEqual(self.runner.run(recovered_registration["experiment_id"])["status"], ExperimentStatus.REJECTED.value)

        concurrent = self.ledger.register(hypothesis(version=3), corpus_fingerprint=CORPUS)
        barrier = threading.Barrier(2)

        def start_once() -> str:
            barrier.wait()
            try:
                self.ledger.start(concurrent["experiment_id"], started_at="2026-08-17T04:00:00Z")
                return "started"
            except ExperimentConflictError:
                return "conflict"

        with ThreadPoolExecutor(max_workers=2) as pool:
            outcomes = list(pool.map(lambda _: start_once(), range(2)))
        self.assertEqual(sorted(outcomes), ["conflict", "started"])

    def test_unknown_status_and_promotion_attempt_fail_closed_without_signal(self) -> None:
        registered = self.ledger.register(hypothesis(), corpus_fingerprint=CORPUS)
        with closing(sqlite3.connect(self.path)) as connection:
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute("UPDATE phase_e_experiments SET status='UNKNOWN' WHERE experiment_id=?", (registered["experiment_id"],))
        finished = self.runner.run(registered["experiment_id"])
        denied = self.ledger.request_promotion(finished["experiment_id"], requested_at="2026-08-17T04:00:00Z")
        self.assertFalse(denied["qualified_signal"])
        self.assertFalse(denied["trading_authority"])
        self.assertTrue(any("denied_e1_no_signal_authority" in item["reason"] for item in self.ledger.promotion_history(finished["experiment_id"])))


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
