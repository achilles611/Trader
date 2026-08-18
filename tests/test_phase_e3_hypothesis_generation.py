from __future__ import annotations

import json
import sqlite3
import subprocess
import sys
import threading
import unittest
from pathlib import Path
from importlib import import_module

from src.phase_e.generation import (
    PREDICATE_COMPLEMENT_WITHIN_PARTITION_V1,
    GenerationConflictError,
    GenerationIntegrityError,
    HypothesisFamilySpec,
    OutcomeAccessError,
    PhaseEHypothesisGenerator,
    PredicateOperator,
    StatisticalTestPlan,
    ThresholdPolicy,
    wallet_action_sign_family,
)
from src.phase_e.types import FeatureReference, OutcomeHorizon, canonical_hash

_e2_fixture = import_module("tests.test_phase_e2_materialization_sampling")


def _fixed_family(*, thresholds: tuple[float, ...] = (0.0,), maximum: int = 2) -> HypothesisFamilySpec:
    return HypothesisFamilySpec(
        family_id="WALLET_ACTION_FIXED_CONTROL", version=1,
        title="Wallet action fixed control", description="Predictor-only fixed threshold regression family.",
        allowed_features=(FeatureReference("wallet_action", 1),), allowed_operators=(PredicateOperator.GT,),
        threshold_policy=ThresholdPolicy.FIXED_THRESHOLD_V1,
        fixed_thresholds={"wallet_action@1": thresholds}, quantiles=(), permitted_interaction_order=1,
        comparator_policy=PREDICATE_COMPLEMENT_WITHIN_PARTITION_V1, compatible_horizons=(OutcomeHorizon(5),),
        statistical_test_plan=StatisticalTestPlan(
            test_id="E4_WEIGHTED_NET_OUTCOME_DISTRIBUTION_DIFFERENCE_V1", direction="TWO_SIDED",
            effect_metric="DECLARED_PHASE_E2_NET_OUTCOME_V2", comparator_policy=PREDICATE_COMPLEMENT_WITHIN_PARTITION_V1,
            sampling_weights_required=True, resampling_seed=17, resample_count=20,
            significance_threshold=0.05, minimum_effect_size=0.0, minimum_sample_size=2,
        ),
        minimum_training_support=2, minimum_feature_coverage=1.0, maximum_candidates=maximum,
        maximum_candidates_per_feature=maximum, global_budget_behavior="CANONICAL_ORDER_TRUNCATE_V1",
        missing_feature_policy="SUPPRESS_WITH_DURABLE_REASON_V1",
        duplicate_semantics="CANONICAL_PREDICATE_AND_E1_DEFINITION_V1",
        multiple_testing_family_rule="RUN_MATERIALIZATION_TEST_PLAN_V1", generation_seed=0,
    )


class PhaseE3HypothesisGenerationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.fixture = _e2_fixture.PhaseE2MaterializationTests("run")
        self.fixture.setUp()
        self.path = self.fixture.path
        self.spec = self.fixture._spec(target=36)
        self.fixture.materializer.build(self.spec, registered_at=_e2_fixture.NOW)
        self.generator = PhaseEHypothesisGenerator(self.path)

    def tearDown(self) -> None:
        self.fixture.tearDown()

    def test_control_family_generates_and_registers_a_frozen_outcome_blind_universe(self) -> None:
        result = self.generator.run(
            self.spec.materialization_id, wallet_action_sign_family(minimum_training_support=2), registered_at=_e2_fixture.NOW,
        )
        self.assertEqual(result["status"], "COMPLETE")
        self.assertEqual(result["unique_candidate_count"], 2)
        self.assertEqual(result["registered_hypothesis_count"], 2)
        self.assertEqual(result["outcome_reads_attempted"], 0)
        self.assertFalse(result["outcome_reads_permitted"])
        self.assertFalse(result["trading_authority"])
        self.assertEqual({item["predicate"]["operator"] for item in result["proposals"]}, {"GT", "LT"})
        verification = self.generator.verify(result["generation_run_id"])
        self.assertTrue(verification["verified"])
        self.assertEqual(verification["outcome_reads_attempted"], 0)

    def test_predictor_reader_fails_closed_for_outcomes_results_and_legacy_tables(self) -> None:
        for relation in ("phase_e_materialization_outcomes", "science_outcome_labels", "phase_e_experiment_results"):
            with self.subTest(relation=relation), self.assertRaises(OutcomeAccessError):
                with self.generator._predictor_connection() as connection:
                    connection.execute(f"SELECT * FROM {relation}").fetchall()

    def test_forbidden_internal_read_is_durably_reported_against_its_run(self) -> None:
        family = wallet_action_sign_family(minimum_training_support=2)
        self.generator.register_family(family, registered_at=_e2_fixture.NOW)
        snapshot = self.generator._verified_snapshot(self.spec.materialization_id, family)
        self.generator._register_run(snapshot.run_spec, _e2_fixture.NOW)
        with self.assertRaises(OutcomeAccessError):
            with self.generator._predictor_connection(snapshot.run_spec.run_id) as connection:
                connection.execute("SELECT * FROM phase_e_materialization_outcomes").fetchall()
        connection = sqlite3.connect(self.path)
        try:
            count = connection.execute(
                "SELECT COUNT(*) FROM phase_e_generation_access_violations WHERE generation_run_id=?",
                (snapshot.run_spec.run_id,),
            ).fetchone()[0]
        finally:
            connection.close()
        self.assertEqual(count, 1)

    def test_repeated_thresholds_collapse_to_one_semantic_hypothesis_and_are_audited(self) -> None:
        result = self.generator.run(self.spec.materialization_id, _fixed_family(thresholds=(0.0, 0.0)), registered_at=_e2_fixture.NOW)
        self.assertEqual(result["unique_candidate_count"], 1)
        self.assertEqual(result["suppressed_counts"]["SEMANTIC_DUPLICATE"], 1)
        self.assertEqual(result["registered_hypothesis_count"], 1)

    def test_candidate_budget_is_deterministic_and_outcome_blind(self) -> None:
        family = _fixed_family(thresholds=(-2.0, -1.0, 0.0), maximum=1)
        planned = self.generator.plan(self.spec.materialization_id, family)
        completed = self.generator.run(self.spec.materialization_id, family, registered_at=_e2_fixture.NOW)
        self.assertEqual(planned["estimated_unique_proposals"], 1)
        self.assertEqual(completed["unique_candidate_count"], 1)
        self.assertEqual(completed["suppressed_counts"]["PER_FEATURE_BUDGET"], 1)

    def test_family_version_cannot_mutate_after_registration(self) -> None:
        family = _fixed_family()
        self.generator.register_family(family, registered_at=_e2_fixture.NOW)
        changed = _fixed_family(thresholds=(1.0,))
        with self.assertRaises(GenerationConflictError):
            self.generator.register_family(changed, registered_at=_e2_fixture.NOW)

    def test_restart_reproduces_the_identical_universe(self) -> None:
        family = wallet_action_sign_family(minimum_training_support=2)
        first = self.generator.run(self.spec.materialization_id, family, registered_at=_e2_fixture.NOW)
        second = PhaseEHypothesisGenerator(self.path).run(self.spec.materialization_id, family, registered_at="2026-08-17T09:00:00Z")
        self.assertEqual(first["generation_run_id"], second["generation_run_id"])
        self.assertEqual(first["hypothesis_universe_fingerprint"], second["hypothesis_universe_fingerprint"])
        self.assertEqual(first["proposals"], second["proposals"])

    def test_concurrent_generators_converge_on_one_complete_universe(self) -> None:
        family = wallet_action_sign_family(minimum_training_support=2)
        results: list[dict[str, object]] = []
        failures: list[Exception] = []

        def run() -> None:
            try:
                results.append(PhaseEHypothesisGenerator(self.path).run(
                    self.spec.materialization_id, family, registered_at=_e2_fixture.NOW,
                ))
            except Exception as exc:  # pragma: no cover - asserted below
                failures.append(exc)

        first, second = threading.Thread(target=run), threading.Thread(target=run)
        first.start(); second.start(); first.join(); second.join()
        self.assertEqual(failures, [])
        self.assertEqual({item["generation_run_id"] for item in results}, {results[0]["generation_run_id"]})
        self.assertTrue(self.generator.verify(str(results[0]["generation_run_id"]))["verified"])

    def test_process_death_after_candidate_freeze_resumes_the_same_universe(self) -> None:
        family = wallet_action_sign_family(minimum_training_support=2)
        self.generator.register_family(family, registered_at=_e2_fixture.NOW)
        snapshot = self.generator._verified_snapshot(self.spec.materialization_id, family)
        self.generator._register_run(snapshot.run_spec, _e2_fixture.NOW)
        code = (
            "import os; from src.phase_e.generation import PhaseEHypothesisGenerator, HypothesisFamilySpec; "
            f"g=PhaseEHypothesisGenerator({str(self.path)!r}); "
            f"f=HypothesisFamilySpec.from_payload(g.get_family({family.family_id!r}, {family.version})['family']); "
            f"s=g._verified_snapshot({self.spec.materialization_id!r}, f); g._generate_if_needed(s, f); os._exit(23)"
        )
        child = subprocess.run([sys.executable, "-c", code], cwd=Path(__file__).parents[1], check=False)
        self.assertEqual(child.returncode, 23)
        frozen = self.generator.get(snapshot.run_spec.run_id)
        self.assertEqual(frozen["status"], "CANDIDATES_FROZEN")
        completed = PhaseEHypothesisGenerator(self.path).run(self.spec.materialization_id, family, registered_at=_e2_fixture.NOW)
        self.assertEqual(completed["status"], "COMPLETE")
        self.assertEqual(completed["hypothesis_universe_fingerprint"], frozen["hypothesis_universe_fingerprint"])

    def test_forged_complete_projection_fails_closed(self) -> None:
        family = wallet_action_sign_family(minimum_training_support=2)
        self.generator.register_family(family, registered_at=_e2_fixture.NOW)
        snapshot = self.generator._verified_snapshot(self.spec.materialization_id, family)
        self.generator._register_run(snapshot.run_spec, _e2_fixture.NOW)
        connection = sqlite3.connect(self.path)
        try:
            connection.execute("UPDATE phase_e_generation_runs SET status='COMPLETE' WHERE generation_run_id=?", (snapshot.run_spec.run_id,))
            connection.commit()
        finally:
            connection.close()
        with self.assertRaises(GenerationIntegrityError):
            self.generator.get(snapshot.run_spec.run_id)

    def test_consistently_rehashed_candidate_support_forgery_fails_predictor_replay(self) -> None:
        result = self.generator.run(
            self.spec.materialization_id, wallet_action_sign_family(minimum_training_support=2), registered_at=_e2_fixture.NOW,
        )
        run_id = result["generation_run_id"]
        connection = sqlite3.connect(self.path)
        connection.row_factory = sqlite3.Row
        try:
            connection.execute("DROP TRIGGER phase_e_hypothesis_proposals_no_update")
            run = connection.execute("SELECT * FROM phase_e_generation_runs WHERE generation_run_id=?", (run_id,)).fetchone()
            proposal = connection.execute(
                "SELECT * FROM phase_e_hypothesis_proposals WHERE generation_run_id=? AND ordinal=0", (run_id,),
            ).fetchone()
            assert run is not None and proposal is not None
            run_spec = json.loads(run["generation_specification_json"])
            predicate = json.loads(proposal["predicate_json"])
            features = json.loads(proposal["required_features_json"])
            threshold = json.loads(proposal["threshold_provenance_json"])
            provenance = json.loads(proposal["generation_provenance_json"])
            definition = json.loads(proposal["proposed_definition_json"])
            forged_support = int(proposal["training_support_count"]) + 1
            proposal_identity = {
                "proposal_id": proposal["proposal_id"], "generation_run_id": run_id,
                "family_id": run_spec["family_id"], "family_version": run_spec["family_version"],
                "source_materialization_id": run_spec["materialization_id"], "predicate": predicate,
                "predicate_hash": proposal["predicate_hash"], "required_features": features,
                "threshold_provenance": threshold, "training_support_count": forged_support,
                "training_missing_count": proposal["training_missing_count"],
                "training_population_count": proposal["training_population_count"],
                "comparator_policy": proposal["comparator_policy"],
                "outcome_horizon": {"seconds": proposal["outcome_horizon_seconds"]},
                "proposed_hypothesis_id": proposal["proposed_hypothesis_id"],
                "proposed_hypothesis_version": proposal["proposed_hypothesis_version"],
                "proposed_definition_hash": proposal["proposed_definition_hash"],
                "multiple_testing_family_id": proposal["multiple_testing_family_id"],
                "generation_provenance": provenance,
            }
            artifact = canonical_hash({"ordinal": 0, "proposal": proposal_identity, "definition": definition})
            connection.execute(
                "UPDATE phase_e_hypothesis_proposals SET training_support_count=?, artifact_hash=? WHERE generation_run_id=? AND proposal_id=?",
                (forged_support, artifact, run_id, proposal["proposal_id"]),
            )
            connection.commit()
        finally:
            connection.close()
        self.assertEqual(self.generator.get(run_id)["status"], "COMPLETE")
        with self.assertRaisesRegex(GenerationIntegrityError, "deterministic predictor-only replay"):
            self.generator.verify(run_id)

    def test_wallet_and_symbol_identity_features_are_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "wallet or symbol identities"):
            _fixed_family().__class__(
                **{**_fixed_family().__dict__, "allowed_features": (FeatureReference("wallet_id", 1),)}
            )
        with self.assertRaisesRegex(ValueError, "wallet or symbol identities"):
            _fixed_family().__class__(
                **{**_fixed_family().__dict__, "allowed_features": (FeatureReference("symbol", 1),)}
            )

    def test_static_legacy_and_trading_authority_isolation(self) -> None:
        source = (Path(__file__).parents[1] / "src" / "phase_e" / "generation.py").read_text(encoding="utf-8")
        self.assertNotIn("copytrade.pattern_discovery", source)
        for token in ("CopySignal", "enter_decision", "paper_trade", "allocate_capital", "assign_leverage"):
            self.assertNotIn(token, source)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
