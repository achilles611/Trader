from __future__ import annotations

import ast
import math
import json
import multiprocessing
import os
import sqlite3
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from dataclasses import replace
from pathlib import Path

from src.copytrade.science_repository import ScientificRepository, canonical_hash as phase_d_canonical_hash
from src.phase_e import (
    CorpusProvenanceError,
    ExperimentConflictError,
    ExperimentResult,
    ExperimentStatus,
    FeatureReference,
    HypothesisDefinition,
    LedgerIntegrityError,
    NullExperimentRunner,
    OutcomeHorizon,
    PartitionIdentity,
    PhaseELedger,
    RejectionReason,
    StatisticSpec,
)
from src.phase_e.types import ExperimentConclusion, canonical_hash, storage_json


NOW = "2026-08-17T02:00:00Z"
CORPUS = "corpus-e1-test"


def _process_register(database_path: str, gate: multiprocessing.synchronize.Event, queue: multiprocessing.queues.Queue) -> None:
    gate.wait()
    try:
        result = PhaseELedger(database_path).register(hypothesis(version=60), corpus_fingerprint=CORPUS)
        queue.put(("registered", result["experiment_id"]))
    except Exception as exc:  # pragma: no cover - reported to the parent process
        queue.put((type(exc).__name__, str(exc)))


def _process_start(database_path: str, experiment_id: str, gate: multiprocessing.synchronize.Event, queue: multiprocessing.queues.Queue) -> None:
    gate.wait()
    try:
        PhaseELedger(database_path).start(experiment_id, started_at="2026-08-17T05:00:00Z")
        queue.put("started")
    except ExperimentConflictError:
        queue.put("conflict")
    except Exception as exc:  # pragma: no cover - reported to the parent process
        queue.put(type(exc).__name__)


def _process_record_result(database_path: str, experiment_id: str, gate: multiprocessing.synchronize.Event, queue: multiprocessing.queues.Queue) -> None:
    gate.wait()
    try:
        ledger = PhaseELedger(database_path)
        result = NullExperimentRunner(ledger).evaluate(ledger.get(experiment_id))
        ledger.record_result(experiment_id, result, recorded_at="2026-08-17T06:00:00Z")
        queue.put("recorded")
    except ExperimentConflictError:
        queue.put("conflict")
    except Exception as exc:  # pragma: no cover - reported to the parent process
        queue.put(type(exc).__name__)


def _process_reproduce(database_path: str, experiment_id: str, queue: multiprocessing.queues.Queue) -> None:
    try:
        result = NullExperimentRunner(PhaseELedger(database_path)).reproduce(experiment_id)
        queue.put(result["reproducible"])
    except Exception as exc:  # pragma: no cover - reported to the parent process
        queue.put(type(exc).__name__)


def _process_running_transition(
    database_path: str, experiment_id: str, action: str,
    gate: multiprocessing.synchronize.Event, queue: multiprocessing.queues.Queue,
) -> None:
    gate.wait()
    ledger = PhaseELedger(database_path)
    try:
        if action == "fail":
            ledger.fail(experiment_id, reason="process_failure", failed_at="2026-08-17T06:00:00Z")
        else:
            ledger.recover_interrupted(experiment_id, recovered_at="2026-08-17T06:00:00Z")
        queue.put(action)
    except ExperimentConflictError:
        queue.put("conflict")
    except Exception as exc:  # pragma: no cover - reported to the parent process
        queue.put(type(exc).__name__)


def _process_promotion_request(
    database_path: str, experiment_id: str,
    gate: multiprocessing.synchronize.Event, queue: multiprocessing.queues.Queue,
) -> None:
    gate.wait()
    try:
        result = PhaseELedger(database_path).request_promotion(
            experiment_id, requested_at="2026-08-17T07:00:00Z", reason="same_concurrent_request",
        )
        queue.put(("denied", result["trading_authority"]))
    except Exception as exc:  # pragma: no cover - reported to the parent process
        queue.put((type(exc).__name__, str(exc)))


def _die_after_projection_update(database_path: str, experiment_id: str) -> None:
    connection = sqlite3.connect(database_path, isolation_level=None)
    connection.execute("BEGIN IMMEDIATE")
    connection.execute(
        "UPDATE phase_e_experiments SET status='RUNNING', execution_attempts=1, started_at=? WHERE experiment_id=?",
        ("2026-08-17T05:00:00Z", experiment_id),
    )
    os._exit(17)


def _die_after_result_insert(database_path: str, experiment_id: str, result_json: str, result_hash: str) -> None:
    connection = sqlite3.connect(database_path, isolation_level=None)
    connection.execute("BEGIN IMMEDIATE")
    connection.execute(
        "INSERT INTO phase_e_experiment_results VALUES (?, ?, ?, ?)",
        (experiment_id, result_json, result_hash, "2026-08-17T06:00:00Z"),
    )
    os._exit(18)


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
        feature_transforms={f"{item.feature_id}@{item.version}": "identity" for item in required_features},
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

    def _record_corpus(self, *, fingerprint: str = CORPUS, payload_observation_fingerprint: str = "observations-e1", coverage_state: str = "PROVEN_COMPLETE",
                       timestamp_anomalies: int = 0, first_event_at: str = "2026-08-17T00:00:01Z") -> None:
        self.phase_d.register_feature(
            "wallet_action", 1, {"source": "normalized_fill", "transform": "identity"},
            created_at=NOW, code_sha="d7-data-ignition-v1",
        )
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
            "timestamp_anomalies": timestamp_anomalies,
            "first_event_at": first_event_at,
            "last_event_at": "2026-08-17T00:59:59Z",
            "wallet_attribution_quality": "official_per_fill",
            "market_evidence_availability": "trade_print_only",
            "computed_at": NOW,
            "details": {"test_fixture": True},
        }
        recorded_coverage = self.phase_d.record_coverage(coverage)
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
            "coverage": recorded_coverage,
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

    def test_documented_pre_interval_timestamp_anomaly_is_excluded_not_treated_as_corpus_corruption(self) -> None:
        self._record_corpus(
            fingerprint="corpus-d7-boundary-anomaly", timestamp_anomalies=1,
            first_event_at="2026-08-16T23:59:59.892000Z",
        )
        provenance = self.ledger.resolve_phase_d_corpus("corpus-d7-boundary-anomaly")
        self.assertEqual(provenance.coverage_state, "PROVEN_COMPLETE")
        self.assertEqual(provenance.interval_start, "2026-08-17T00:00:00Z")

        self._record_corpus(fingerprint="corpus-d7-unexplained-anomaly", timestamp_anomalies=1)
        with self.assertRaises(CorpusProvenanceError):
            self.ledger.resolve_phase_d_corpus("corpus-d7-unexplained-anomaly")

    def test_coverage_recomputation_timestamp_does_not_rewrite_frozen_snapshot_evidence(self) -> None:
        before = self.ledger.resolve_phase_d_corpus(CORPUS)
        with closing(sqlite3.connect(self.path)) as connection:
            connection.execute("UPDATE science_data_coverage SET computed_at=? WHERE coverage_id=?", ("2026-08-17T03:00:00Z", "coverage-" + CORPUS))
            connection.commit()
        after = self.ledger.resolve_phase_d_corpus(CORPUS)
        self.assertEqual(canonical_hash(before.payload()), canonical_hash(after.payload()))

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

    def test_fractional_timestamp_ordering_uses_instants_not_text(self) -> None:
        later_fraction = self.ledger.register(
            hypothesis(version=4), corpus_fingerprint=CORPUS,
            registered_at="2026-08-17T03:00:00Z",
        )
        running = self.ledger.start(
            later_fraction["experiment_id"], started_at="2026-08-17T03:00:00.100000Z",
        )
        self.assertEqual(running["status"], ExperimentStatus.RUNNING.value)

        earlier_whole = self.ledger.register(
            hypothesis(version=5), corpus_fingerprint=CORPUS,
            registered_at="2026-08-17T02:59:59Z",
        )
        self.ledger.start(earlier_whole["experiment_id"], started_at="2026-08-17T03:00:00.900000Z")
        result = self.runner.evaluate(self.ledger.get(earlier_whole["experiment_id"]))
        with self.assertRaisesRegex(ExperimentConflictError, "cannot precede"):
            self.ledger.record_result(
                earlier_whole["experiment_id"], result, recorded_at="2026-08-17T03:00:00Z",
            )

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

    def test_valid_sql_projection_forgeries_are_detected_on_every_read(self) -> None:
        registered = self.ledger.register(hypothesis(version=10), corpus_fingerprint=CORPUS)
        with closing(sqlite3.connect(self.path)) as connection:
            connection.execute(
                """UPDATE phase_e_experiments
                   SET status='COMPLETED', promotion_state='HISTORICAL_SURVIVOR',
                       execution_attempts=1, started_at=?, terminal_at=?
                   WHERE experiment_id=?""",
                ("2026-08-17T03:00:00Z", "2026-08-17T03:01:00Z", registered["experiment_id"]),
            )
            connection.commit()
        with self.assertRaisesRegex(LedgerIntegrityError, "STARTED|result|evidence"):
            self.ledger.get(registered["experiment_id"])

        rejected = self.ledger.register(hypothesis(version=11), corpus_fingerprint=CORPUS)
        self.runner.run(rejected["experiment_id"])
        with closing(sqlite3.connect(self.path)) as connection:
            connection.execute(
                "UPDATE phase_e_experiments SET status='COMPLETED', promotion_state='HISTORICAL_SURVIVOR' WHERE experiment_id=?",
                (rejected["experiment_id"],),
            )
            connection.commit()
        with self.assertRaises(LedgerIntegrityError):
            self.ledger.events(rejected["experiment_id"])

        promotion = self.ledger.register(hypothesis(version=12), corpus_fingerprint=CORPUS)
        with closing(sqlite3.connect(self.path)) as connection:
            connection.execute(
                "UPDATE phase_e_experiments SET promotion_state='HISTORICAL_SURVIVOR' WHERE experiment_id=?",
                (promotion["experiment_id"],),
            )
            connection.commit()
        with self.assertRaisesRegex(LedgerIntegrityError, "Promotion projection"):
            self.ledger.promotion_history(promotion["experiment_id"])

    def test_missing_extra_duplicate_and_reordered_lifecycle_evidence_fail_closed(self) -> None:
        missing = self.ledger.register(hypothesis(version=20), corpus_fingerprint=CORPUS)
        extra = self.ledger.register(hypothesis(version=21), corpus_fingerprint=CORPUS)
        duplicate = self.ledger.register(hypothesis(version=22), corpus_fingerprint=CORPUS)
        reordered = self.ledger.register(hypothesis(version=23), corpus_fingerprint=CORPUS)
        self.ledger.start(duplicate["experiment_id"], started_at="2026-08-17T03:00:00Z")
        self.ledger.start(reordered["experiment_id"], started_at="2026-08-17T03:00:00Z")

        def insert_event(connection: sqlite3.Connection, experiment_id: str, *, event_at: str) -> None:
            payload = {"attempt": 1}
            payload_hash = canonical_hash(payload)
            event_id = canonical_hash({
                "experiment_id": experiment_id, "event_type": "STARTED", "attempt": 1,
                "from_status": "REGISTERED", "to_status": "RUNNING", "reason": "evaluation_started",
                "event_at": event_at, "payload_hash": payload_hash,
            })
            connection.execute(
                "INSERT INTO phase_e_experiment_events VALUES (?, ?, 'STARTED', 'REGISTERED', 'RUNNING', 'evaluation_started', ?, ?, ?)",
                (event_id, experiment_id, event_at, storage_json(payload), payload_hash),
            )

        with closing(sqlite3.connect(self.path)) as connection:
            connection.execute("DROP TRIGGER phase_e_events_append_only_delete")
            connection.execute(
                "DELETE FROM phase_e_experiment_events WHERE experiment_id=? AND event_type='REGISTERED'",
                (missing["experiment_id"],),
            )
            insert_event(connection, extra["experiment_id"], event_at="2026-08-17T03:00:00Z")
            insert_event(connection, duplicate["experiment_id"], event_at="2026-08-17T03:01:00Z")
            connection.execute("DROP TRIGGER phase_e_events_append_only_update")
            started = connection.execute(
                "SELECT payload_hash FROM phase_e_experiment_events WHERE experiment_id=? AND event_type='STARTED'",
                (reordered["experiment_id"],),
            ).fetchone()
            assert started is not None
            earlier = "2026-08-17T01:00:00Z"
            changed_id = canonical_hash({
                "experiment_id": reordered["experiment_id"], "event_type": "STARTED", "attempt": 1,
                "from_status": "REGISTERED", "to_status": "RUNNING", "reason": "evaluation_started",
                "event_at": earlier, "payload_hash": started[0],
            })
            connection.execute(
                "UPDATE phase_e_experiment_events SET event_id=?, event_at=? WHERE experiment_id=? AND event_type='STARTED'",
                (changed_id, earlier, reordered["experiment_id"]),
            )
            connection.commit()

        for experiment_id in (missing["experiment_id"], extra["experiment_id"], duplicate["experiment_id"], reordered["experiment_id"]):
            with self.subTest(experiment_id=experiment_id), self.assertRaises(LedgerIntegrityError):
                self.ledger.get(experiment_id)

    def test_result_state_attempt_and_recovery_mismatches_fail_closed(self) -> None:
        result_only = self.ledger.register(hypothesis(version=30), corpus_fingerprint=CORPUS)
        bad_recovery = self.ledger.register(hypothesis(version=31), corpus_fingerprint=CORPUS)
        result = ExperimentResult(
            25, 0.0, 1.0, 0.0, 0.0, {"name": "forged"},
            ExperimentConclusion.REJECTED, RejectionReason.NULL_HYPOTHESIS_NOT_REJECTED,
        ).payload()
        with closing(sqlite3.connect(self.path)) as connection:
            connection.execute(
                "INSERT INTO phase_e_experiment_results VALUES (?, ?, ?, ?)",
                (result_only["experiment_id"], storage_json(result), canonical_hash(result), "2026-08-17T03:00:00Z"),
            )
            recovery_payload = {"attempt": 1, "partial_result_discarded": False, "trading_authority": False}
            recovery_hash = canonical_hash(recovery_payload)
            recovery_id = canonical_hash({
                "experiment_id": bad_recovery["experiment_id"], "event_type": "RECOVERY_REQUIRED", "attempt": 1,
                "from_status": "RUNNING", "to_status": "RECOVERABLE", "reason": "restart_detected_before_result",
                "event_at": "2026-08-17T03:00:00Z", "payload_hash": recovery_hash,
            })
            connection.execute(
                """INSERT INTO phase_e_experiment_events
                   VALUES (?, ?, 'RECOVERY_REQUIRED', 'RUNNING', 'RECOVERABLE',
                           'restart_detected_before_result', ?, ?, ?)""",
                (recovery_id, bad_recovery["experiment_id"], "2026-08-17T03:00:00Z", storage_json(recovery_payload), recovery_hash),
            )
            connection.commit()
        with self.assertRaisesRegex(LedgerIntegrityError, "result contract"):
            self.ledger.get(result_only["experiment_id"])
        with self.assertRaisesRegex(LedgerIntegrityError, "Unexpected|attempt"):
            self.ledger.get(bad_recovery["experiment_id"])

        survivor = self.ledger.register(hypothesis(version=32), corpus_fingerprint=CORPUS)
        self.ledger.start(survivor["experiment_id"], started_at="2026-08-17T03:00:00Z")
        fabricated = ExperimentResult(
            25, 100.0, 0.0, 99.0, 101.0, {"name": "fabricated_alpha"},
            ExperimentConclusion.SURVIVED,
        )
        with self.assertRaisesRegex(LedgerIntegrityError, "result contract"):
            self.ledger.record_result(survivor["experiment_id"], fabricated, recorded_at="2026-08-17T03:01:00Z")
        unchanged = self.ledger.get(survivor["experiment_id"])
        self.assertEqual(unchanged["status"], ExperimentStatus.RUNNING.value)
        self.assertIsNone(unchanged["result"])

    def test_creation_time_is_metadata_and_scientific_identity_is_type_safe(self) -> None:
        original = hypothesis(version=40)
        first = self.ledger.register(original, corpus_fingerprint=CORPUS, registered_at="2026-08-17T02:00:00Z")
        later_metadata = replace(original, created_at="2026-08-18T02:00:00+00:00")
        duplicate = self.ledger.register(later_metadata, corpus_fingerprint=CORPUS, registered_at="2026-08-19T02:00:00Z")
        self.assertEqual(first["experiment_id"], duplicate["experiment_id"])
        self.assertEqual(duplicate["registered_at"], "2026-08-17T02:00:00Z")
        self.assertEqual(len(self.ledger.events(first["experiment_id"])), 1)

        changed = replace(original, proposition=original.proposition + " Materially changed.")
        self.assertNotEqual(original.definition_hash, changed.definition_hash)
        with self.assertRaises(ExperimentConflictError):
            self.ledger.register(changed, corpus_fingerprint=CORPUS)
        next_version = self.ledger.register(replace(changed, version=41), corpus_fingerprint=CORPUS)
        self.assertNotEqual(first["experiment_id"], next_version["experiment_id"])

        self.assertNotEqual(canonical_hash(1), canonical_hash(1.0))
        self.assertNotEqual(canonical_hash(1), canonical_hash("1"))
        self.assertNotEqual(canonical_hash(["float64", "0x1.0000000000000p+0"]), canonical_hash(1.0))
        self.assertEqual(canonical_hash(-0.0), canonical_hash(0.0))
        self.assertEqual(canonical_hash("e\u0301"), canonical_hash("é"))
        with self.assertRaisesRegex(ValueError, "non-string"):
            canonical_hash({1: "coercion forbidden", "1": "distinct"})
        with self.assertRaisesRegex(ValueError, "Unicode normalization"):
            canonical_hash({"e\u0301": 1, "é": 2})
        with self.assertRaisesRegex(ValueError, "finite number"):
            replace(original, minimum_effect_size="0.001")  # type: ignore[arg-type]

    def test_noncanonical_or_duplicate_persisted_json_fails_closed(self) -> None:
        whitespace = self.ledger.register(hypothesis(version=42), corpus_fingerprint=CORPUS)
        duplicate_key = self.ledger.register(hypothesis(version=43), corpus_fingerprint=CORPUS)
        changed_event = self.ledger.register(hypothesis(version=44), corpus_fingerprint=CORPUS)
        self.ledger.start(changed_event["experiment_id"], started_at="2026-08-17T03:00:00Z")
        self.ledger.fail(
            changed_event["experiment_id"], reason="original_failure",
            failed_at="2026-08-17T03:01:00Z",
        )
        with closing(sqlite3.connect(self.path)) as connection:
            connection.execute("DROP TRIGGER phase_e_hypotheses_immutable")
            definition = connection.execute(
                "SELECT definition_json FROM phase_e_hypotheses WHERE hypothesis_id=? AND version=42",
                (whitespace["hypothesis_id"],),
            ).fetchone()
            assert definition is not None
            spaced = json.dumps(json.loads(definition[0]), sort_keys=True, indent=2)
            connection.execute(
                "UPDATE phase_e_hypotheses SET definition_json=? WHERE hypothesis_id=? AND version=42",
                (spaced, whitespace["hypothesis_id"]),
            )
            registration = connection.execute(
                "SELECT event_id, payload_json FROM phase_e_experiment_events WHERE experiment_id=? AND event_type='REGISTERED'",
                (duplicate_key["experiment_id"],),
            ).fetchone()
            assert registration is not None
            connection.execute("DROP TRIGGER phase_e_events_append_only_update")
            malformed = registration[1][:-1] + ',"attempt":0}'
            connection.execute(
                "UPDATE phase_e_experiment_events SET payload_json=? WHERE event_id=?",
                (malformed, registration[0]),
            )
            connection.execute(
                "UPDATE phase_e_experiment_events SET reason='rewritten_failure' WHERE experiment_id=? AND event_type='FAILED'",
                (changed_event["experiment_id"],),
            )
            connection.commit()
        with self.assertRaisesRegex(LedgerIntegrityError, "canonical storage JSON"):
            self.ledger.get(whitespace["experiment_id"])
        with self.assertRaisesRegex(LedgerIntegrityError, "canonical storage JSON"):
            self.ledger.get(duplicate_key["experiment_id"])
        with self.assertRaisesRegex(LedgerIntegrityError, "identity"):
            self.ledger.get(changed_event["experiment_id"])

    def test_partition_contract_encodes_feature_windows_boundaries_and_sampling_version(self) -> None:
        exact = PartitionIdentity(
            partition_id="lookback-exact-v1",
            train_start="2026-08-17T00:00:00Z", train_end="2026-08-17T00:10:00Z",
            validation_start="2026-08-17T00:10:10Z", validation_end="2026-08-17T00:20:00Z",
            test_start="2026-08-17T00:20:10Z", test_end="2026-08-17T00:30:00Z",
            purge_seconds=0, embargo_seconds=0, random_seed=17, horizon=OutcomeHorizon(5),
            feature_lookback_seconds=5, sampling_algorithm="NONE_V1",
        )
        lookback_feature = FeatureReference("wallet_action", 1, lookback_seconds=5)
        definition = replace(
            hypothesis(version=50), required_features=(lookback_feature,), partition=exact,
            feature_transforms={"wallet_action@1": "identity"},
        )
        self.assertEqual(definition.partition.payload()["interval_semantics"], "START_INCLUSIVE_END_EXCLUSIVE")
        self.assertEqual(definition.partition.payload()["outcome_boundary_policy"], "END_EXCLUSIVE_OUTCOME_CONTAINED")
        with self.assertRaisesRegex(ValueError, "purge/embargo"):
            replace(exact, validation_start="2026-08-17T00:10:09Z")
        with self.assertRaisesRegex(ValueError, "forward information"):
            FeatureReference("future", 1, lookforward_seconds=1)
        with self.assertRaisesRegex(ValueError, "feature lookback"):
            replace(definition, partition=replace(exact, feature_lookback_seconds=0))
        with self.assertRaisesRegex(ValueError, "sampling algorithm"):
            replace(exact, sampling_algorithm="")

        offset = replace(
            partition(),
            train_start="2026-08-16T18:00:00-06:00", train_end="2026-08-16T18:10:00-06:00",
            validation_start="2026-08-16T18:10:10-06:00", validation_end="2026-08-16T18:20:00-06:00",
            test_start="2026-08-16T18:20:20-06:00", test_end="2026-08-16T18:30:00-06:00",
        )
        self.assertEqual(partition().payload(), offset.payload())

    def test_d_provenance_binds_full_coverage_and_feature_definition_without_writing_d(self) -> None:
        d_tables = ("science_features", "science_data_coverage", "science_corpus_snapshots")

        def snapshot_d() -> dict[str, list[tuple[object, ...]]]:
            with closing(sqlite3.connect(self.path)) as connection:
                return {name: connection.execute(f"SELECT * FROM {name} ORDER BY 1").fetchall() for name in d_tables}

        before = snapshot_d()
        registered = self.ledger.register(hypothesis(version=70), corpus_fingerprint=CORPUS)
        self.runner.run(registered["experiment_id"])
        self.assertEqual(before, snapshot_d())

        with closing(sqlite3.connect(self.path)) as connection:
            connection.execute("UPDATE science_data_coverage SET verified_hours=0 WHERE coverage_id=?", (f"coverage-{CORPUS}",))
            connection.commit()
        with self.assertRaises(CorpusProvenanceError):
            self.ledger.verify_current_provenance(registered["experiment_id"])

    def test_malformed_numeric_and_post_registration_feature_drift_fail_closed(self) -> None:
        with closing(sqlite3.connect(self.path)) as connection:
            connection.execute("UPDATE science_data_coverage SET coverage_fraction='NaN' WHERE coverage_id=?", (f"coverage-{CORPUS}",))
            connection.commit()
        with self.assertRaisesRegex(CorpusProvenanceError, "finite number"):
            self.ledger.register(hypothesis(version=71), corpus_fingerprint=CORPUS)

        # Restore the fixture, register against it, then simulate a privileged
        # writer defeating D's trigger. E's frozen copy remains readable but
        # current-provenance verification must identify the drift.
        with closing(sqlite3.connect(self.path)) as connection:
            connection.execute("UPDATE science_data_coverage SET coverage_fraction=1.0 WHERE coverage_id=?", (f"coverage-{CORPUS}",))
            connection.commit()
        registered = self.ledger.register(hypothesis(version=72), corpus_fingerprint=CORPUS)
        changed_definition = {"source": "normalized_fill", "transform": "future_information"}
        with closing(sqlite3.connect(self.path)) as connection:
            connection.execute("DROP TRIGGER science_features_immutable")
            connection.execute(
                "UPDATE science_features SET definition_json=?, definition_hash=? WHERE feature_id='wallet_action' AND version=1",
                (json.dumps(changed_definition, sort_keys=True, separators=(",", ":")), phase_d_canonical_hash(changed_definition)),
            )
            connection.commit()
        self.assertEqual(self.ledger.get(registered["experiment_id"])["status"], ExperimentStatus.REGISTERED.value)
        with self.assertRaises(CorpusProvenanceError):
            self.ledger.verify_current_provenance(registered["experiment_id"])

    def test_registration_and_result_commit_close_d_provenance_race_windows(self) -> None:
        class MutatingFirstReadLedger(PhaseELedger):
            reads = 0

            def resolve_phase_d_corpus(self, corpus_fingerprint: str):  # type: ignore[no-untyped-def]
                provenance = super().resolve_phase_d_corpus(corpus_fingerprint)
                self.reads += 1
                if self.reads == 1:
                    with closing(sqlite3.connect(self.path)) as connection:
                        connection.execute(
                            "UPDATE science_data_coverage SET verified_hours=0 WHERE coverage_id=?",
                            (f"coverage-{CORPUS}",),
                        )
                        connection.commit()
                return provenance

        racing = MutatingFirstReadLedger(self.path)
        with self.assertRaises(CorpusProvenanceError):
            racing.register(hypothesis(version=73), corpus_fingerprint=CORPUS)
        self.assertEqual(self.ledger.list(), [])

        with closing(sqlite3.connect(self.path)) as connection:
            connection.execute(
                "UPDATE science_data_coverage SET verified_hours=1 WHERE coverage_id=?",
                (f"coverage-{CORPUS}",),
            )
            connection.commit()
        registered = self.ledger.register(hypothesis(version=74), corpus_fingerprint=CORPUS)
        running = self.ledger.start(registered["experiment_id"], started_at="2026-08-17T03:00:00Z")
        result = self.runner.evaluate(running)
        with closing(sqlite3.connect(self.path)) as connection:
            connection.execute(
                "UPDATE science_data_coverage SET observation_count=24 WHERE coverage_id=?",
                (f"coverage-{CORPUS}",),
            )
            connection.commit()
        with self.assertRaises(CorpusProvenanceError):
            self.ledger.record_result(registered["experiment_id"], result, recorded_at="2026-08-17T03:01:00Z")
        unchanged = self.ledger.get(registered["experiment_id"])
        self.assertEqual(unchanged["status"], ExperimentStatus.RUNNING.value)
        self.assertIsNone(unchanged["result"])

    def test_runner_identity_cannot_be_overridden_and_software_evolution_fails_closed(self) -> None:
        with self.assertRaises(TypeError):
            NullExperimentRunner(self.ledger, code_version="claim-an-old-version")  # type: ignore[call-arg]
        registered = self.ledger.register(hypothesis(version=80), corpus_fingerprint=CORPUS)
        self.runner.run(registered["experiment_id"])

        class FutureRunner(NullExperimentRunner):
            CODE_VERSION = "phase-e1-null-runner-v2"

        with self.assertRaisesRegex(CorpusProvenanceError, "stale or unavailable"):
            FutureRunner(PhaseELedger(self.path)).reproduce(registered["experiment_id"])

    def test_two_process_registration_start_result_and_fresh_process_reproduction(self) -> None:
        context = multiprocessing.get_context("spawn")

        gate = context.Event()
        queue = context.Queue()
        registration_processes = [context.Process(target=_process_register, args=(str(self.path), gate, queue)) for _ in range(2)]
        for process in registration_processes:
            process.start()
        gate.set()
        registration_outcomes = [queue.get(timeout=20) for _ in registration_processes]
        for process in registration_processes:
            process.join(20)
            self.assertEqual(process.exitcode, 0)
        self.assertEqual([item[0] for item in registration_outcomes], ["registered", "registered"])
        self.assertEqual(len({item[1] for item in registration_outcomes}), 1)
        self.assertEqual(len(self.ledger.events(registration_outcomes[0][1])), 1)

        concurrent = self.ledger.register(hypothesis(version=61), corpus_fingerprint=CORPUS)
        gate = context.Event()
        queue = context.Queue()
        start_processes = [context.Process(target=_process_start, args=(str(self.path), concurrent["experiment_id"], gate, queue)) for _ in range(2)]
        for process in start_processes:
            process.start()
        gate.set()
        start_outcomes = sorted(queue.get(timeout=20) for _ in start_processes)
        for process in start_processes:
            process.join(20)
            self.assertEqual(process.exitcode, 0)
        self.assertEqual(start_outcomes, ["conflict", "started"])

        result_race = self.ledger.register(hypothesis(version=62), corpus_fingerprint=CORPUS)
        self.ledger.start(result_race["experiment_id"], started_at="2026-08-17T05:00:00Z")
        gate = context.Event()
        queue = context.Queue()
        result_processes = [context.Process(target=_process_record_result, args=(str(self.path), result_race["experiment_id"], gate, queue)) for _ in range(2)]
        for process in result_processes:
            process.start()
        gate.set()
        result_outcomes = sorted(queue.get(timeout=20) for _ in result_processes)
        for process in result_processes:
            process.join(20)
            self.assertEqual(process.exitcode, 0)
        self.assertEqual(result_outcomes, ["conflict", "recorded"])

        transition_race = self.ledger.register(hypothesis(version=64), corpus_fingerprint=CORPUS)
        self.ledger.start(transition_race["experiment_id"], started_at="2026-08-17T05:00:00Z")
        gate = context.Event()
        queue = context.Queue()
        transition_processes = [
            context.Process(
                target=_process_running_transition,
                args=(str(self.path), transition_race["experiment_id"], action, gate, queue),
            )
            for action in ("fail", "recover")
        ]
        for process in transition_processes:
            process.start()
        gate.set()
        transition_outcomes = sorted(queue.get(timeout=20) for _ in transition_processes)
        for process in transition_processes:
            process.join(20)
            self.assertEqual(process.exitcode, 0)
        self.assertEqual(transition_outcomes.count("conflict"), 1)
        self.assertEqual(len(set(transition_outcomes) & {"fail", "recover"}), 1)

        promotion_race = self.ledger.register(hypothesis(version=65), corpus_fingerprint=CORPUS)
        gate = context.Event()
        queue = context.Queue()
        promotion_processes = [
            context.Process(
                target=_process_promotion_request,
                args=(str(self.path), promotion_race["experiment_id"], gate, queue),
            )
            for _ in range(2)
        ]
        for process in promotion_processes:
            process.start()
        gate.set()
        promotion_outcomes = [queue.get(timeout=20) for _ in promotion_processes]
        for process in promotion_processes:
            process.join(20)
            self.assertEqual(process.exitcode, 0)
        self.assertEqual(promotion_outcomes, [("denied", False), ("denied", False)])
        history = self.ledger.promotion_history(promotion_race["experiment_id"])
        self.assertEqual([item["payload"]["event_type"] for item in history].count("DENIED_REQUEST"), 1)

        reproducible = self.ledger.register(hypothesis(version=63), corpus_fingerprint=CORPUS)
        self.runner.run(reproducible["experiment_id"])
        queue = context.Queue()
        fresh = context.Process(target=_process_reproduce, args=(str(self.path), reproducible["experiment_id"], queue))
        fresh.start()
        self.assertIs(queue.get(timeout=20), True)
        fresh.join(20)
        self.assertEqual(fresh.exitcode, 0)

    def test_process_death_rolls_back_projection_and_result_checkpoints(self) -> None:
        context = multiprocessing.get_context("spawn")
        projection = self.ledger.register(hypothesis(version=90), corpus_fingerprint=CORPUS)
        died = context.Process(target=_die_after_projection_update, args=(str(self.path), projection["experiment_id"]))
        died.start()
        died.join(20)
        self.assertEqual(died.exitcode, 17)
        after_death = self.ledger.get(projection["experiment_id"])
        self.assertEqual(after_death["status"], ExperimentStatus.REGISTERED.value)
        self.assertEqual(after_death["execution_attempts"], 0)
        self.assertEqual(len(self.ledger.events(projection["experiment_id"])), 1)

        partial_result = self.ledger.register(hypothesis(version=91), corpus_fingerprint=CORPUS)
        running = self.ledger.start(partial_result["experiment_id"], started_at="2026-08-17T05:00:00Z")
        result = self.runner.evaluate(running).payload()
        died = context.Process(
            target=_die_after_result_insert,
            args=(str(self.path), partial_result["experiment_id"], storage_json(result), canonical_hash(result)),
        )
        died.start()
        died.join(20)
        self.assertEqual(died.exitcode, 18)
        after_death = self.ledger.get(partial_result["experiment_id"])
        self.assertEqual(after_death["status"], ExperimentStatus.RUNNING.value)
        self.assertIsNone(after_death["result"])
        self.assertEqual([item["event_type"] for item in self.ledger.events(partial_result["experiment_id"])], ["REGISTERED", "STARTED"])

    def test_phase_e_has_no_import_or_call_path_into_trading_authority(self) -> None:
        source_root = Path(__file__).resolve().parents[1] / "src"
        authority_imports: list[str] = []
        for path in source_root.rglob("*.py"):
            relative = path.relative_to(source_root).as_posix()
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            if not relative.startswith("phase_e/"):
                for node in ast.walk(tree):
                    if isinstance(node, ast.ImportFrom) and node.module and "phase_e" in node.module:
                        authority_imports.append(f"{relative}:{node.lineno}:{node.module}")
                    elif isinstance(node, ast.Import):
                        for alias in node.names:
                            if "phase_e" in alias.name:
                                authority_imports.append(f"{relative}:{node.lineno}:{alias.name}")
        self.assertEqual(authority_imports, [])

        phase_e_source = "\n".join(path.read_text(encoding="utf-8") for path in (source_root / "phase_e").glob("*.py"))
        for forbidden in ("CopySignal(", "place_order(", "submit_order(", "PaperExecutionEngine(", "allocation_fraction="):
            with self.subTest(forbidden=forbidden):
                self.assertNotIn(forbidden, phase_e_source)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
