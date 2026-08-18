from __future__ import annotations

import inspect
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import threading
import unittest
from contextlib import closing
from dataclasses import replace
from importlib import import_module
from pathlib import Path

from src.phase_e.evaluation import (
    EvaluationConflictError,
    EvaluationIntegrityError,
    EvaluationSettings,
    HypothesisEvaluationStatus,
    PhaseEEvaluator,
    _Observation,
)
from src.phase_e.generation import (
    PhaseEHypothesisGenerator,
    StatisticalTestPlan,
    wallet_action_sign_family,
)
from src.phase_e.types import canonical_hash, storage_json


_e2_fixture = import_module("tests.test_phase_e2_materialization_sampling")
NOW = "2026-08-17T02:30:00Z"


class PhaseE4ScientificEvaluationTests(unittest.TestCase):
    """Adversarial closure tests over one copied, immutable E.2/E.3 base."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.base_fixture = _e2_fixture.PhaseE2MaterializationTests("run")
        cls.base_fixture.setUp()
        cls.spec = cls.base_fixture._spec(target=36)
        cls.base_fixture.materializer.build(cls.spec, registered_at=_e2_fixture.NOW)
        cls.family = wallet_action_sign_family(minimum_training_support=2)
        cls.e3 = PhaseEHypothesisGenerator(cls.base_fixture.path).run(
            cls.spec.materialization_id, cls.family, registered_at=_e2_fixture.NOW,
        )

    @classmethod
    def tearDownClass(cls) -> None:
        cls.base_fixture.tearDown()

    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / "science.sqlite3"
        source = sqlite3.connect(self.base_fixture.path)
        target = sqlite3.connect(self.path)
        try:
            source.backup(target)
        finally:
            target.close()
            source.close()
        self.evaluator = PhaseEEvaluator(self.path)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _protocol(self, **settings: object) -> dict[str, object]:
        contract = EvaluationSettings(**settings)
        return self.evaluator.preregister(self.e3["generation_run_id"], settings=contract, registered_at=NOW)

    def test_protocol_preregisters_exact_e3_family_without_authority_or_outcome_values(self) -> None:
        protocol = self._protocol()
        body = protocol["protocol"]
        self.assertEqual(protocol["family_size"], self.e3["registered_hypothesis_count"])
        self.assertEqual(body["family"]["hypothesis_universe_fingerprint"], self.e3["hypothesis_universe_fingerprint"])
        self.assertEqual(body["evidence_snapshot"]["materialization_id"], self.spec.materialization_id)
        self.assertEqual(body["partitions"]["evaluation_partition"], "validation")
        self.assertEqual(body["partitions"]["maximum_test_queries"], 0)
        self.assertFalse(any(body["authority"].values()))
        self.assertNotIn("net_outcome", json.dumps(body["family"]))
        self.assertEqual(self.evaluator.eligible_families()[0]["protocol_registered"], True)

    def test_single_wallet_and_overlapping_events_fail_closed_but_retain_full_holm_denominator(self) -> None:
        protocol = self._protocol(minimum_independent_components=2)
        run = self.evaluator.evaluate(protocol["protocol_id"], evaluated_at=NOW)
        self.assertEqual(run["status"], "COMPLETE")
        self.assertEqual(run["correction_family_size"], 2)
        self.assertEqual(len(run["results"]), 2)
        for result in run["results"]:
            self.assertEqual(result["evaluation_status"], "INSUFFICIENT_SUPPORT")
            self.assertEqual(result["adjusted_p_value"], 1.0)
            self.assertEqual(result["statistic"]["independent_component_count"], 1)
            self.assertEqual(result["statistical_decision"], "NOT_EVALUATED")
            self.assertEqual(result["practical_relevance_decision"], "NOT_EVALUATED")
            self.assertEqual(result["downstream_eligibility"], "NOT_ELIGIBLE")
        self.assertEqual(sorted(item["correction_rank"] for item in run["results"]), [1, 2])

    def test_synthetic_independent_components_exercise_weighted_bootstrap_holm_and_separate_effect_gate(self) -> None:
        protocol_entry = self._protocol(minimum_independent_components=8)
        protocol = protocol_entry["protocol"]
        observations = []
        for index in range(16):
            positive = index < 8
            observation_id = f"synthetic-{index:02}"
            observations.append(_Observation(
                observation_id=observation_id, ordinal=index,
                anchor_at=f"2026-08-17T00:03:{index * 2:02}Z", wallet=f"wallet-{index:02}",
                symbol=f"SYMBOL-{index:02}", source_event_id=f"event-{index:02}", stratum_id="validation:all",
                sampling_weight=1.0, feature_values={"wallet_action@1": 1.0 if positive else -1.0},
                feature_sources=(observation_id,), outcome_state="RESOLVED", outcome_reason=None,
                net_outcome=0.02 if positive else -0.02, outcome_source_id=f"outcome-{index:02}",
                resolved_at=f"2026-08-17T00:03:{index * 2 + 5:02}Z",
            ))
        with self.evaluator._connection() as connection:
            members = connection.execute(
                "SELECT * FROM phase_e_evaluation_protocol_members WHERE protocol_id=? ORDER BY ordinal",
                (protocol_entry["protocol_id"],),
            ).fetchall()
            proposals = connection.execute(
                "SELECT * FROM phase_e_hypothesis_proposals WHERE generation_run_id=? ORDER BY ordinal",
                (self.e3["generation_run_id"],),
            ).fetchall()
            results = self.evaluator._compute_results(
                protocol_entry["protocol_id"], protocol, members, proposals, observations,
            )
        self.assertTrue(all(item["evaluation_status"] == "EVALUATED" for item in results))
        self.assertTrue(all(item["raw_p_value"] is not None for item in results))
        self.assertTrue(all(item["adjusted_p_value"] <= 0.05 for item in results))
        self.assertEqual({item["statistical_decision"] for item in results}, {"STATISTICALLY_SUPPORTED"})
        self.assertEqual({item["practical_relevance_decision"] for item in results}, {"PRACTICALLY_RELEVANT"})
        self.assertAlmostEqual(abs(results[0]["effect_estimate"]), 0.04)
        tiny = [
            replace(item, net_outcome=0.0001 if item.feature_values["wallet_action@1"] > 0 else -0.0001)
            for item in observations
        ]
        tiny_results = self.evaluator._compute_results(
            protocol_entry["protocol_id"], protocol, members, proposals, tiny,
        )
        self.assertEqual({item["statistical_decision"] for item in tiny_results}, {"STATISTICALLY_SUPPORTED"})
        self.assertEqual({item["practical_relevance_decision"] for item in tiny_results}, {"BELOW_PRACTICAL_THRESHOLD"})
        self.assertEqual({item["scientific_decision"] for item in tiny_results}, {"NOT_SCIENTIFICALLY_SUPPORTED"})

    def test_unevaluable_members_use_p_one_and_cannot_disappear_from_correction(self) -> None:
        protocol = self._protocol()["protocol"]
        drafts = [
            {"raw_p_value": 0.01, "ordinal": 0, "proposal_id": "a", "evaluation_status": "EVALUATED",
             "effect_estimate": 0.1, "reason_codes": []},
            {"raw_p_value": None, "ordinal": 1, "proposal_id": "b", "evaluation_status": "INVALID_EVIDENCE",
             "effect_estimate": None, "reason_codes": ["INVALID"]},
        ]
        adjusted = self.evaluator._apply_holm(protocol, drafts)
        self.assertEqual(len(adjusted), 2)
        self.assertEqual(adjusted[0]["adjusted_p_value"], 0.02)
        self.assertEqual(adjusted[1]["adjusted_p_value"], 1.0)
        self.assertEqual(adjusted[1]["correction_rank"], 2)

    def test_missing_unresolved_invalid_and_negative_outcomes_are_distinct(self) -> None:
        def item(state: str, value: float | None = None) -> _Observation:
            return _Observation("id-" + state, 0, "2026-08-17T00:03:01Z", "wallet", "BTC", "event",
                                "validation:all", 1.0, {}, (), state, state, value, None, None)
        counts = self.evaluator._resolution_counts([
            item("RESOLVED", 0.1), item("RESOLVED", -0.1), item("UNRESOLVED"),
            item("MATURE_MISSING"), item("INVALID"),
        ])
        self.assertEqual(counts, {"resolved": 2, "positive": 1, "negative": 1,
                                  "unresolved": 1, "mature_missing": 1, "invalid": 1})

    def test_family_cannot_be_added_removed_duplicated_or_replaced_after_freeze(self) -> None:
        protocol = self._protocol()
        with closing(sqlite3.connect(self.path)) as connection:
            member = connection.execute(
                "SELECT * FROM phase_e_evaluation_protocol_members WHERE protocol_id=? ORDER BY ordinal LIMIT 1",
                (protocol["protocol_id"],),
            ).fetchone()
            with self.assertRaisesRegex(sqlite3.DatabaseError, "freeze only during protocol registration"):
                connection.execute(
                    """INSERT INTO phase_e_evaluation_protocol_members VALUES (?, 99, 'forged', 'forged',
                              'forged', 'forged', 1, 'forged', 'forged', 'forged')""",
                    (protocol["protocol_id"],),
                )
            with self.assertRaisesRegex(sqlite3.DatabaseError, "cannot be deleted"):
                connection.execute(
                    "DELETE FROM phase_e_evaluation_protocol_members WHERE protocol_id=? AND proposal_id=?",
                    (protocol["protocol_id"], member[2]),
                )
            with self.assertRaisesRegex(sqlite3.DatabaseError, "cannot be deleted"):
                connection.execute(
                    "DELETE FROM phase_e_hypothesis_proposals WHERE generation_run_id=?",
                    (self.e3["generation_run_id"],),
                )

    def test_consistently_rehashed_forged_family_membership_is_detected(self) -> None:
        protocol = self._protocol()
        connection = sqlite3.connect(self.path)
        connection.row_factory = sqlite3.Row
        try:
            connection.execute("DROP TRIGGER phase_e_evaluation_members_no_update")
            member = connection.execute(
                "SELECT * FROM phase_e_evaluation_protocol_members WHERE protocol_id=? ORDER BY ordinal LIMIT 1",
                (protocol["protocol_id"],),
            ).fetchone()
            identity = {key: member[key] for key in (
                "protocol_id", "ordinal", "proposal_id", "predicate_hash", "experiment_id", "hypothesis_id",
                "hypothesis_version", "definition_hash", "e3_mapping_hash",
            )}
            identity["experiment_id"] = "forged-experiment"
            connection.execute(
                "UPDATE phase_e_evaluation_protocol_members SET experiment_id=?, member_hash=? WHERE protocol_id=? AND proposal_id=?",
                (identity["experiment_id"], canonical_hash(identity), protocol["protocol_id"], member["proposal_id"]),
            )
            connection.commit()
        finally:
            connection.close()
        with self.assertRaises(EvaluationIntegrityError):
            self.evaluator.get_protocol(protocol["protocol_id"])

    def test_caller_cannot_swap_method_alpha_or_lifecycle_after_results_are_visible(self) -> None:
        protocol = self._protocol(minimum_independent_components=2)
        run = self.evaluator.evaluate(protocol["protocol_id"], evaluated_at=NOW)
        with self.assertRaises(EvaluationConflictError):
            self.evaluator.preregister(
                self.e3["generation_run_id"], settings=EvaluationSettings(minimum_independent_components=9),
            )
        with closing(sqlite3.connect(self.path)) as connection:
            with self.assertRaisesRegex(sqlite3.DatabaseError, "scientific inputs are immutable"):
                connection.execute(
                    "UPDATE phase_e_evaluation_protocols SET protocol_json='{}' WHERE protocol_id=?",
                    (protocol["protocol_id"],),
                )
            with self.assertRaisesRegex(sqlite3.DatabaseError, "finalize only once"):
                connection.execute(
                    "UPDATE phase_e_evaluation_runs SET status='EVALUATING' WHERE evaluation_run_id=?",
                    (run["evaluation_run_id"],),
                )

    def test_forged_adjusted_p_value_fails_manifest_reconciliation_and_replay(self) -> None:
        protocol = self._protocol(minimum_independent_components=2)
        run = self.evaluator.evaluate(protocol["protocol_id"], evaluated_at=NOW)
        connection = sqlite3.connect(self.path)
        connection.row_factory = sqlite3.Row
        try:
            connection.execute("DROP TRIGGER phase_e_hypothesis_evaluations_no_update")
            row = connection.execute(
                "SELECT * FROM phase_e_hypothesis_evaluations WHERE evaluation_run_id=? ORDER BY ordinal LIMIT 1",
                (run["evaluation_run_id"],),
            ).fetchone()
            payload = json.loads(row["result_json"])
            payload["adjusted_p_value"] = 0.0
            connection.execute(
                """UPDATE phase_e_hypothesis_evaluations
                   SET adjusted_p_value=0.0, result_json=?, result_hash=?
                   WHERE evaluation_run_id=? AND proposal_id=?""",
                (storage_json(payload), canonical_hash(payload), run["evaluation_run_id"], row["proposal_id"]),
            )
            connection.commit()
        finally:
            connection.close()
        with self.assertRaises(EvaluationIntegrityError):
            self.evaluator.get_run(run["evaluation_run_id"])

    def test_upstream_evidence_and_train_thresholds_are_immutable_during_evaluation(self) -> None:
        protocol = self._protocol(minimum_independent_components=2)
        before = self.e3["proposals"]
        run = self.evaluator.evaluate(protocol["protocol_id"], evaluated_at=NOW)
        after = PhaseEHypothesisGenerator(self.path).get(self.e3["generation_run_id"])["proposals"]
        self.assertEqual(before, after)
        with closing(sqlite3.connect(self.path)) as connection:
            with self.assertRaisesRegex(sqlite3.DatabaseError, "immutable"):
                connection.execute(
                    "UPDATE phase_e_materialization_outcomes SET missing=1 WHERE materialization_id=?",
                    (self.spec.materialization_id,),
                )
            with self.assertRaisesRegex(sqlite3.DatabaseError, "immutable"):
                connection.execute(
                    "UPDATE phase_e_hypothesis_proposals SET threshold_provenance_json='{}' WHERE generation_run_id=?",
                    (self.e3["generation_run_id"],),
                )
        self.assertTrue(self.evaluator.verify(run["evaluation_run_id"])["verified"])

    def test_process_death_rolls_back_half_persistence_and_retry_converges(self) -> None:
        protocol = self._protocol(minimum_independent_components=2)
        code = (
            "import os; from src.phase_e.evaluation import PhaseEEvaluator; "
            f"PhaseEEvaluator({str(self.path)!r}, fault_hook=lambda stage: os._exit(31)).evaluate({protocol['protocol_id']!r})"
        )
        child = subprocess.run([sys.executable, "-c", code], cwd=Path(__file__).parents[1], check=False)
        self.assertEqual(child.returncode, 31)
        with closing(sqlite3.connect(self.path)) as connection:
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM phase_e_evaluation_runs").fetchone()[0], 0)
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM phase_e_hypothesis_evaluations").fetchone()[0], 0)
        completed = PhaseEEvaluator(self.path).evaluate(protocol["protocol_id"], evaluated_at=NOW)
        self.assertEqual(completed["correction_family_size"], 2)

    def test_concurrent_workers_and_repeated_runs_converge_to_identical_decisions(self) -> None:
        protocol = self._protocol(minimum_independent_components=2)
        results: list[dict[str, object]] = []
        failures: list[Exception] = []

        def evaluate() -> None:
            try:
                results.append(PhaseEEvaluator(self.path).evaluate(protocol["protocol_id"], evaluated_at=NOW))
            except Exception as exc:  # pragma: no cover - asserted below
                failures.append(exc)

        first, second = threading.Thread(target=evaluate), threading.Thread(target=evaluate)
        first.start(); second.start(); first.join(); second.join()
        self.assertEqual(failures, [])
        self.assertEqual({item["evaluation_run_id"] for item in results}, {results[0]["evaluation_run_id"]})
        again = PhaseEEvaluator(self.path).evaluate(protocol["protocol_id"], evaluated_at="2026-08-17T09:00:00Z")
        self.assertEqual(again["results"], results[0]["results"])
        self.assertTrue(self.evaluator.reproduce(again["evaluation_run_id"])["reproducible"])

    def test_incompatible_statistical_schema_fails_closed_before_evaluation(self) -> None:
        plan = replace(self.family.statistical_test_plan, test_id="OUTCOME_SELECTED_UNSUPPORTED_TEST")
        incompatible = replace(
            self.family, family_id="E4_INCOMPATIBLE_CONTROL", title="Incompatible E.4 control",
            description="Threat fixture only.", statistical_test_plan=plan,
        )
        run = PhaseEHypothesisGenerator(self.path).run(self.spec.materialization_id, incompatible)
        with self.assertRaisesRegex(EvaluationIntegrityError, "incompatible"):
            self.evaluator.preregister(run["generation_run_id"])

    def test_numeric_extremes_and_tiny_support_do_not_create_exciting_results(self) -> None:
        with self.assertRaises(ValueError):
            EvaluationSettings(maximum_absolute_outcome=float("nan"))
        with self.assertRaises(ValueError):
            EvaluationSettings(maximum_sampling_weight=float("inf"))
        protocol = self._protocol(minimum_independent_components=2)
        run = self.evaluator.evaluate(protocol["protocol_id"], evaluated_at=NOW)
        self.assertTrue(all(item["scientific_decision"] == "INCONCLUSIVE" for item in run["results"]))
        self.assertTrue(all("MINIMUM_SUPPORT_OR_INDEPENDENCE_NOT_MET" in item["reason_codes"] for item in run["results"]))

    def test_wallet_identity_holdout_and_legacy_paths_cannot_leak_into_results(self) -> None:
        protocol = self._protocol(minimum_independent_components=2)
        run = self.evaluator.evaluate(protocol["protocol_id"], evaluated_at=NOW)
        encoded = json.dumps(run, sort_keys=True)
        self.assertNotIn("0xabc", encoded)
        self.assertNotIn("wallet-", encoded)
        self.assertEqual(run["test_partition_queries"], 0)
        self.assertEqual(run["manifest"]["test_partition_queries"], 0)
        source = inspect.getsource(PhaseEEvaluator._load_validation_evidence)
        self.assertIn("partition_name='validation'", source)
        self.assertNotIn("partition_name='test'", source)
        module = (Path(__file__).parents[1] / "src" / "phase_e" / "evaluation.py").read_text(encoding="utf-8")
        self.assertNotIn("copytrade.pattern_discovery", module)
        self.assertNotIn("HistoricalExperimentEngine", module)
        for token in ("CopySignal", "paper_trade", "allocate_capital", "assign_leverage"):
            self.assertNotIn(token, module)

    def test_operator_cli_exposes_eligible_protocol_result_and_replay_controls(self) -> None:
        completed = subprocess.run(
            [sys.executable, "main.py", "hypothesis-evaluation", "--database", str(self.path), "eligible"],
            cwd=Path(__file__).parents[1], check=True, capture_output=True, text=True,
        )
        payload = json.loads(completed.stdout)
        self.assertEqual(payload["items"][0]["generation_run_id"], self.e3["generation_run_id"])
        self.assertFalse(payload["trading_authority"])


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
