from __future__ import annotations

import copy
import json
import math
import random
import sqlite3
import tempfile
import unittest
from contextlib import closing
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

from src.phase_e.prospective import (
    E5_OBSERVATION_SCHEMA,
    E5_PROTOCOL_SCHEMA,
    E5_STATISTICAL_METHOD,
    AdmissibilityReport,
    DesignObservation,
    EvidenceClassification,
    EvidenceState,
    ExperimentState,
    InferenceRefused,
    OutcomeAccessForbidden,
    OutcomeRecord,
    PhaseE5Registry,
    ProtocolIntegrityError,
    classify_evidence,
    compute_protocol_hash,
    dependence_diagnostics,
    evaluate_admissibility,
    holm_adjust,
    load_frozen_protocol,
    scheduled_blocks,
    scientific_replay_hash,
    validate_protocol_document,
    wallet_cohort,
    synthetic_wild_cluster_bootstrap_t,
)
from src.phase_e.types import canonical_hash


ROOT = Path(__file__).parents[1]
FROZEN_PROTOCOL = ROOT / "docs" / "commissioning" / "phase-e5-prospective-experiment" / "e5-protocol-v1.json"
FINAL_AS_OF = "2027-12-25T00:00:00Z"


def _utc_text(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def make_protocol() -> dict[str, object]:
    document: dict[str, object] = {
        "schema": E5_PROTOCOL_SCHEMA,
        "protocol_version": 1,
        "identity": {
            "protocol_id": None,
            "protocol_hash": None,
            "hash_algorithm": "PHASE_E_TYPE_TAGGED_SHA256_V1",
            "created_at": "2026-08-18T16:00:00Z",
            "frozen_at": "2026-08-18T17:00:00Z",
            "code_commit": "synthetic-method-validation",
            "schema_version": E5_PROTOCOL_SCHEMA,
            "configuration_hashes": {
                "sampling": "bound-in-protocol-hash",
                "admissibility": "bound-in-protocol-hash",
                "inference": "bound-in-protocol-hash",
            },
        },
        "lifecycle": {
            "initial_state": "FROZEN_NOT_STARTED",
            "first_admission_irreversibly_locks_protocol": True,
            "semantic_change_requires_successor": True,
            "successor_naming": "phase-e5-prospective-protocol-v2-or-later",
        },
        "scientific_history": {
            "phase_d": "FROZEN",
            "phase_e1": "FROZEN",
            "phase_e2": "FROZEN",
            "phase_e3": "FROZEN",
            "phase_e4_v1": "PERMANENTLY_INCONCLUSIVE",
            "phase_e4_1_review_commit": "ee990643ea096e32f97c6177b4b1a165241d05e7",
        },
        "experimental_unit": {
            "primary_unit": "ONE_PRECOMMITTED_SEPARATED_30_MINUTE_MARKET_SESSION_BLOCK_V1",
            "claim": "approximately independent market-session replicate after design exclusions",
            "within_block_dependence": "UNRESTRICTED_RETAINED_BY_ONE_CLUSTER_MULTIPLIER",
            "connected_components": "DIAGNOSTIC_AND_GATE_ONLY_NOT_RESAMPLING_UNITS",
            "cross_block_dependence": "FAIL_CLOSED",
        },
        "sampling": {
            "schedule": {
                "algorithm": "HASHED_SLOT_WITHIN_FIXED_8DAY_EPOCH_V1",
                "acquisition_start": "2026-09-01T00:00:00Z",
                "hard_stop": "2027-12-25T00:00:00Z",
                "block_count": 60,
                "epoch_seconds": 691200,
                "slot_seconds": 1800,
                "slots_per_epoch_prefix": 47,
                "sample_duration_seconds": 1800,
                "minimum_separation_seconds": 604800,
                "schedule_seed": 550017,
                "replacement_blocks": 0,
            },
            "eligible_hours": "ALL_UTC_HOURS_AS_SELECTED_BY_FROZEN_HASHED_SCHEDULE",
            "symbol_eligibility": {
                "rule": "PREANCHOR_CONTINUOUS_TRADE_LIQUIDITY_V1",
                "source": "AUTHORITATIVE_PHASE_D_MARKET_TRADE_TIMESTAMPS_ONLY",
                "lookback_24h_minimum_prints": 172800,
                "lookback_30m_minimum_prints": 3600,
                "lookback_30m_maximum_interprint_gap_seconds": 2,
                "source_discontinuity_allowed": False,
                "outcome_or_postanchor_fields": "FORBIDDEN",
            },
            "wallet_cohort_count": 4,
            "wallet_cohort_salt": "BEELZEBUB_E5_V1_WALLET_COHORT_FIXED_BEFORE_ACQUISITION",
            "wallet_rule": "HASH_COHORT_MATCH_AND_FIRST_ADMITTED_BLOCK_ONLY_V1",
            "observation_rule": "FIRST_ELIGIBLE_WALLET_EVENT_PER_BLOCK_WITH_60S_WALLET_COOLDOWN_V1",
            "source_partition": "NEW_E5_PROSPECTIVE_ONLY",
            "selection_inputs": "PREDICTOR_AND_ACQUISITION_METADATA_AVAILABLE_AT_ANCHOR_ONLY",
            "post_start_adaptation": "FORBIDDEN_EXCEPT_RULES_EXPLICITLY_ENCODED_HERE",
        },
        "outcome": {
            "definition": "PROSPECTIVE_SIGNED_NET_RETURN_FROM_ANCHOR_TO_FIRST_QUALIFYING_TRADE_PRINT_V1",
            "horizon_seconds": 5,
            "maximum_resolution_lag_seconds": 10,
            "ingestion_grace_seconds": 120,
            "economic_window": "ANCHOR_THROUGH_ACTUAL_QUALIFYING_PRINT_AT_ANCHOR_PLUS_5_TO_10_SECONDS",
            "late_evidence": "STALE_NOT_REOPENED_PROTOCOL_INTEGRITY_REVIEW_REQUIRED",
            "maturity": "ANCHOR_PLUS_10_SECONDS_PLUS_120_SECOND_INGESTION_GRACE",
        },
        "hypothesis_family": {
            "family_id": "WALLET_ACTION_SIGN_PROSPECTIVE_V1",
            "family_version": 1,
            "family_membership": "EXACT_TWO_MEMBERS_FROZEN_NO_REMOVAL",
            "members": [
                {"ordinal": 0, "hypothesis_id": "wallet-action-gt-zero", "predicate": "wallet_action@1 GT 0"},
                {"ordinal": 1, "hypothesis_id": "wallet-action-lt-zero", "predicate": "wallet_action@1 LT 0"},
            ],
            "comparator": "PREDICATE_COMPLEMENT_WITHIN_EACH_PRIMARY_BLOCK",
            "outcome_blind_origin": "FROZEN_E3_WALLET_ACTION_SIGN_CONTROL_FAMILY",
        },
        "estimand": {
            "name": "DESIGN_WEIGHTED_COMMON_WITHIN_BLOCK_PREDICATE_MINUS_COMPLEMENT_MEAN_V1",
            "formula": "sum_g q_g*(mean_w(Y|P,g)-mean_w(Y|notP,g))/sum_g q_g; q_g=W1g*W0g/(W1g+W0g)",
            "interpretation": "observational within-session association, not randomized treatment effect",
            "practical_effect_floor": 0.001,
        },
        "admissibility": {
            "ordering": ["protocol_integrity", "maturity", "missingness", "dependence", "concentration", "effective_support", "inference"],
            "minimum_support": {
                "observations": 600,
                "observations_per_arm": 240,
                "blocks": 48,
                "mixed_blocks": 40,
                "effective_blocks": 40.0,
                "effective_blocks_per_arm": 40.0,
                "effective_contrast_blocks": 40.0,
                "effective_symbols": 12.0,
            },
            "maximum_shares": {
                "block": 0.05,
                "predicate_block": 0.05,
                "complement_block": 0.05,
                "component": 0.05,
                "wallet": 0.01,
                "symbol": 0.10,
                "endpoint_family": 0.025,
                "local_time_window": 0.025,
                "contrast_information": 0.05,
            },
            "maximum_sampling_weight_ratio": 5.0,
            "local_time_window_seconds": 30,
            "cross_block_wallet_transaction_endpoint_campaign_or_exposure_edges": 0,
            "failure_semantics": "NO_EFFECT_ESTIMATE_P_VALUE_CI_OR_BOOTSTRAP_HAS_AUTHORITY",
        },
        "missingness": {
            "overall_minimum_resolution_rate": 1.0,
            "per_block_minimum_resolution_rate": 1.0,
            "per_symbol_minimum_resolution_rate": 1.0,
            "per_liquidity_stratum_minimum_resolution_rate": 1.0,
            "per_graph_density_stratum_minimum_resolution_rate": 1.0,
            "per_time_stratum_minimum_resolution_rate": 1.0,
            "maximum_unresolved_concentration": 0.0,
            "correction": "NONE_COMPLETE_RESOLUTION_REQUIRED_V1",
            "ipw_authorized": False,
            "sensitivity_bound_authorized": False,
            "mature_missing_action": "FAMILY_WIDE_MISSINGNESS_GATE_FAILED",
        },
        "inference": {
            "method": E5_STATISTICAL_METHOD,
            "model": "DESIGN_WEIGHTED_BLOCK_FIXED_EFFECTS_WITH_COMMON_PREDICATE_SLOPE_V1",
            "null": "WITHIN_BLOCK_PREDICATE_MINUS_COMPLEMENT_ESTIMAND_EQUALS_ZERO",
            "alternative": "ESTIMAND_DIFFERS_FROM_ZERO",
            "direction": "TWO_SIDED",
            "cluster_unit": "FROZEN_PRIMARY_MARKET_SESSION_BLOCK",
            "bootstrap_weights": "WEBB_SIX_POINT_MEAN_ZERO_UNIT_VARIANCE_V1",
            "restriction": "BLOCK_FIXED_EFFECTS_ONLY_UNDER_NULL",
            "studentization": "DELETE_ONE_PRIMARY_BLOCK_JACKKNIFE_EACH_DRAW_V1",
            "replications": 9999,
            "base_seed": 550017,
            "seed_derivation": "SHA256_PROTOCOL_HASH_HYPOTHESIS_ID_BASE_SEED_V1",
            "canonical_block_order": "PRIMARY_BLOCK_HASH_ASCENDING_V1",
            "minimum_valid_replication_fraction": 0.99,
            "p_value": "PLUS_ONE_ABSOLUTE_BOOTSTRAP_T_EXCEEDANCE_V1",
            "confidence_interval": "EQUAL_TAILED_BOOTSTRAP_T_TYPE7_QUANTILES_V1",
            "confidence_level": 0.95,
            "singleton_clusters": "ALLOWED_ONLY_IF_ALL_GLOBAL_SUPPORT_AND_LEVERAGE_GATES_PASS",
            "degenerate_variance": "INFERENCE_REFUSED",
            "unequal_clusters": "ALLOWED_ONLY_WITHIN_FROZEN_CONCENTRATION_ENVELOPE",
        },
        "multiplicity": {
            "method": "HOLM_BONFERRONI_FWER_V1",
            "family_alpha": 0.05,
            "denominator": 2,
            "unevaluable_member_p_value": 1.0,
            "family_change_after_freeze": "FORBIDDEN",
            "decision": "ADJUSTED_P_AT_MOST_ALPHA_AND_ABS_EFFECT_AT_LEAST_PRACTICAL_FLOOR",
        },
        "stopping": {
            "rule": "FIXED_60_SCHEDULED_8DAY_EPOCH_BLOCKS_AND_FIXED_HARD_STOP_V1",
            "block_target": 60,
            "hard_stop": "2027-12-25T00:00:00Z",
            "replacement_or_extension": "FORBIDDEN",
            "effect_or_p_value_monitoring": "FORBIDDEN",
            "terminal_insufficient_support": "INCONCLUSIVE_NO_EXTENSION",
        },
        "protected_data": {
            "selection_repository": "PREDICTOR_SIDE_CAPABILITY_ONLY_V1",
            "outcome_repository": "SEPARATE_CAPABILITY_SEALED_UNTIL_ELIGIBLE_FOR_INFERENCE_V1",
            "maximum_reserved_test_queries": 0,
            "prefreeze_evaluation_outcome_reads": 0,
            "ui_outcome_exposure_before_inference": False,
            "logging_outcome_exposure_before_inference": False,
            "debug_outcome_exposure_before_inference": False,
        },
        "replay": {
            "canonicalization": "PHASE_E_TYPE_TAGGED_SHA256_V1",
            "required_hashes": [
                "protocol", "family", "experimental_unit", "block_schedule", "observations",
                "admissibility", "dependence_diagnostics", "bootstrap_distribution", "ordered_results",
            ],
            "row_order_invariance": True,
            "rng_replay": "IDENTICAL_PROTOCOL_HYPOTHESIS_BASE_SEED_AND_CANONICAL_BLOCK_ORDER",
        },
        "historical_compatibility": {
            "e4_rows_eligible": False,
            "e4_materializations_eligible": False,
            "synthetic_fixture_namespace": "SYNTHETIC_E5_ONLY_NEVER_PRODUCTION",
        },
        "authority": {"prediction": False, "signal": False, "execution": False, "trading": False},
    }
    digest = compute_protocol_hash(document)
    document["identity"]["protocol_hash"] = digest  # type: ignore[index]
    document["identity"]["protocol_id"] = "e5p-" + digest[:32]  # type: ignore[index]
    return validate_protocol_document(document)


def make_observations(protocol: dict[str, object], *, blocks: int = 60, rows_per_block: int = 10) -> list[DesignObservation]:
    output: list[DesignObservation] = []
    schedule = scheduled_blocks(protocol)
    protocol_hash = protocol["identity"]["protocol_hash"]  # type: ignore[index]
    cohort_salt = protocol["sampling"]["wallet_cohort_salt"]  # type: ignore[index]
    cohort_count = protocol["sampling"]["wallet_cohort_count"]  # type: ignore[index]
    for block in schedule[:blocks]:
        start = datetime.fromisoformat(block.sample_start.replace("Z", "+00:00"))
        for offset in range(rows_per_block):
            anchor = start + timedelta(seconds=offset * 12)
            ordinal = block.ordinal * rows_per_block + offset
            nonce = 0
            wallet = f"wallet-{ordinal:05}-{nonce}"
            while int(canonical_hash({
                "algorithm": "SALTED_WALLET_COHORT_SHA256_V1",
                "salt": cohort_salt, "wallet_id": wallet,
            })[:16], 16) % cohort_count != block.cohort:
                nonce += 1
                wallet = f"wallet-{ordinal:05}-{nonce}"
            output.append(DesignObservation(
                observation_id=f"future-{ordinal:05}", source_schema=E5_OBSERVATION_SCHEMA,
                protocol_hash=str(protocol_hash), block_id=block.block_id,
                anchor_at=_utc_text(anchor), exposure_end_at=_utc_text(anchor + timedelta(seconds=10)),
                wallet_id=wallet, symbol=f"SYM-{ordinal % 12:02}",
                source_event_id=f"event-{ordinal:05}", sampling_weight=1.0,
                predicate=offset % 2 == 0, liquidity_stratum=f"liq-{ordinal % 4}",
                graph_density_stratum=f"density-{ordinal % 4}", time_stratum=f"utc-{anchor.hour:02}",
                eligibility_snapshot_hash=f"eligibility-{ordinal:05}", symbol_liquidity_eligible=True,
                transaction_id=f"tx-{ordinal:05}", endpoint_family_id=f"endpoint-{ordinal:05}",
                campaign_id=f"campaign-{ordinal:05}",
            ))
    return output


def observed_classifications(observations: list[DesignObservation]) -> list[EvidenceClassification]:
    return [
        EvidenceClassification(item.observation_id, EvidenceState.ADMISSIBLE_OBSERVED, "2027-12-25T00:00:00Z")
        for item in observations
    ]


class PhaseE5ProspectiveExperimentTests(unittest.TestCase):
    def setUp(self) -> None:
        self.protocol = make_protocol()
        self.temp = tempfile.TemporaryDirectory()
        self.database = Path(self.temp.name) / "e5-control.sqlite3"

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_protocol_identity_schedule_and_wallet_assignment_are_deterministic(self) -> None:
        self.assertEqual(validate_protocol_document(self.protocol), self.protocol)
        first, second = scheduled_blocks(self.protocol), scheduled_blocks(copy.deepcopy(self.protocol))
        self.assertEqual(first, second)
        self.assertEqual(len(first), 60)
        self.assertGreaterEqual(
            min((datetime.fromisoformat(right.sample_start.replace("Z", "+00:00")) -
                 datetime.fromisoformat(left.exposure_end.replace("Z", "+00:00"))).total_seconds()
                for left, right in zip(first, first[1:])),
            604800,
        )
        self.assertEqual(wallet_cohort(self.protocol, "wallet-x"), wallet_cohort(self.protocol, "wallet-x"))

    def test_checked_in_frozen_protocol_matches_implementation_contract(self) -> None:
        if not FROZEN_PROTOCOL.exists():
            self.skipTest("Frozen artifact is written at the final freeze commit.")
        frozen = load_frozen_protocol(FROZEN_PROTOCOL)
        self.assertEqual(frozen["identity"]["protocol_hash"], compute_protocol_hash(frozen))
        self.assertEqual(frozen["inference"]["replications"], 9999)
        self.assertEqual(frozen["stopping"]["replacement_or_extension"], "FORBIDDEN")

    def test_protocol_cannot_mutate_after_freeze_and_semantic_change_rehashes(self) -> None:
        registry = PhaseE5Registry(self.database)
        entry = registry.freeze(self.protocol)
        self.assertEqual(entry["state"], "FROZEN_NOT_STARTED")
        changed = copy.deepcopy(self.protocol)
        changed["admissibility"]["minimum_support"]["blocks"] = 49  # type: ignore[index]
        self.assertNotEqual(compute_protocol_hash(changed), entry["protocol_hash"])
        with closing(sqlite3.connect(self.database)) as connection:
            with self.assertRaisesRegex(sqlite3.DatabaseError, "immutable"):
                connection.execute(
                    "UPDATE phase_e5_protocols SET protocol_json='{}' WHERE protocol_id=?",
                    (entry["protocol_id"],),
                )

    def test_outcome_capability_is_not_invoked_before_all_gates_pass(self) -> None:
        registry = PhaseE5Registry(self.database)
        entry = registry.freeze(self.protocol)
        calls = 0

        def reader() -> list[OutcomeRecord]:
            nonlocal calls
            calls += 1
            return []

        with self.assertRaises(OutcomeAccessForbidden):
            registry.read_outcomes(entry["protocol_id"], reader=reader, requested_at="2026-08-18T18:00:00Z")
        self.assertEqual(calls, 0)
        self.assertEqual(
            registry.outcome_access_counts(entry["protocol_id"]),
            {"evaluation_outcome_reads": 0, "blocked_attempts": 1},
        )
        self.assertEqual(registry.reserved_test_query_count(), 0)

    def test_historical_e4_rows_and_protocol_mismatch_fail_integrity_gate(self) -> None:
        observations = make_observations(self.protocol)
        observations[0] = replace(observations[0], source_schema="phase-e4-validation-observation-v1")
        report = evaluate_admissibility(
            self.protocol, observations, observed_classifications(observations), as_of=FINAL_AS_OF,
        )
        self.assertEqual(report.state, ExperimentState.PROTOCOL_INTEGRITY_FAILED)
        self.assertIn("NON_PROSPECTIVE_OR_HISTORICAL_OBSERVATION", report.reasons)

    def test_balanced_independent_design_is_eligible_without_reading_outcomes(self) -> None:
        observations = make_observations(self.protocol)
        early = evaluate_admissibility(
            self.protocol, observations, observed_classifications(observations), as_of="2027-01-01T00:00:00Z",
        )
        self.assertEqual(early.state, ExperimentState.PROTOCOL_INTEGRITY_FAILED)
        early_classes = [replace(item, classified_at="2027-01-01T00:00:00Z") for item in observed_classifications(observations)]
        early = evaluate_admissibility(self.protocol, observations, early_classes, as_of="2027-01-01T00:00:00Z")
        self.assertEqual(early.state, ExperimentState.COLLECTING)
        report = evaluate_admissibility(
            self.protocol, observations, observed_classifications(observations), as_of=FINAL_AS_OF,
        )
        self.assertEqual(report.state, ExperimentState.ELIGIBLE_FOR_INFERENCE)
        self.assertGreaterEqual(report.metrics["effective_block_count"], 40)
        self.assertLessEqual(report.metrics["maximum_block_share"], 0.05)

    def test_concentration_and_insufficient_effective_support_fail_closed(self) -> None:
        observations = make_observations(self.protocol)
        dominant = [replace(item, sampling_weight=5.0 if item.block_id == observations[0].block_id else 1.0)
                    for item in observations]
        concentrated = evaluate_admissibility(
            self.protocol, dominant, observed_classifications(dominant), as_of=FINAL_AS_OF,
        )
        self.assertEqual(concentrated.state, ExperimentState.CONCENTRATION_GATE_FAILED)
        self.assertIsNone(getattr(concentrated, "p_value", None))

        few = make_observations(self.protocol, blocks=25, rows_per_block=10)
        insufficient = evaluate_admissibility(
            self.protocol, few, observed_classifications(few), as_of=FINAL_AS_OF,
        )
        self.assertEqual(insufficient.state, ExperimentState.INSUFFICIENT_SUPPORT)
        self.assertIn("MINIMUM_BLOCKS_NOT_MET", insufficient.reasons)

    def test_transitive_graph_bridge_and_cross_block_relation_are_detected(self) -> None:
        base = make_observations(self.protocol, blocks=2, rows_per_block=4)
        first_block = base[:4]
        bridged = [
            replace(first_block[0], wallet_id="w-a", symbol="S", anchor_at="2026-09-01T00:00:00Z", exposure_end_at="2026-09-01T00:00:10Z"),
            replace(first_block[1], wallet_id="w-a", symbol="T", anchor_at="2026-09-01T00:00:20Z", exposure_end_at="2026-09-01T00:00:30Z"),
            replace(first_block[2], wallet_id="w-b", symbol="T", anchor_at="2026-09-01T00:00:25Z", exposure_end_at="2026-09-01T00:00:35Z"),
            replace(first_block[3], wallet_id="w-b", symbol="U", anchor_at="2026-09-01T00:00:40Z", exposure_end_at="2026-09-01T00:00:50Z"),
        ]
        diagnostics = dependence_diagnostics(bridged)
        self.assertEqual(len(set(diagnostics.component_by_observation.values())), 1)
        self.assertGreaterEqual(diagnostics.relation_counts["SAME_WALLET"], 2)
        self.assertGreaterEqual(diagnostics.relation_counts["OVERLAPPING_REALIZED_EXPOSURE"], 1)

        cross = [bridged[0], replace(base[4], wallet_id="w-a")]
        cross_diagnostics = dependence_diagnostics(cross)
        self.assertEqual(len(cross_diagnostics.cross_block_edges), 1)

    def test_realized_resolution_window_does_not_split_linked_exposure(self) -> None:
        observations = make_observations(self.protocol, blocks=1, rows_per_block=2)
        linked = [
            replace(observations[0], symbol="S", anchor_at="2026-09-01T00:00:00Z", exposure_end_at="2026-09-01T00:00:10Z"),
            replace(observations[1], symbol="S", anchor_at="2026-09-01T00:00:06Z", exposure_end_at="2026-09-01T00:00:16Z"),
        ]
        diagnostics = dependence_diagnostics(linked)
        self.assertEqual(len(set(diagnostics.component_by_observation.values())), 1)

    def test_immaturity_missingness_structured_missingness_and_late_evidence_are_distinct(self) -> None:
        observation = make_observations(self.protocol, blocks=1, rows_per_block=1)[0]
        immature = classify_evidence(
            self.protocol, observation, as_of=observation.anchor_at,
            resolution_event_at=None, ingested_at=None,
        )
        self.assertEqual(immature.state, EvidenceState.IMMATURE)
        late = classify_evidence(
            self.protocol, observation,
            as_of=_utc_text(datetime.fromisoformat(observation.anchor_at.replace("Z", "+00:00")) + timedelta(seconds=200)),
            resolution_event_at=_utc_text(datetime.fromisoformat(observation.anchor_at.replace("Z", "+00:00")) + timedelta(seconds=7)),
            ingested_at=_utc_text(datetime.fromisoformat(observation.anchor_at.replace("Z", "+00:00")) + timedelta(seconds=131)),
        )
        self.assertEqual(late.state, EvidenceState.LATE)

        observations = make_observations(self.protocol)
        classes = observed_classifications(observations)
        for index in range(0, len(classes), 12):
            classes[index] = replace(classes[index], state=EvidenceState.MATURE_MISSING, reason="low-liquidity synthetic")
        report = evaluate_admissibility(self.protocol, observations, classes, as_of=FINAL_AS_OF)
        self.assertEqual(report.state, ExperimentState.MISSINGNESS_GATE_FAILED)
        self.assertEqual(report.metrics["resolution_rate"], 11 / 12)

    def test_multiplicity_family_and_stopping_rule_are_frozen(self) -> None:
        family = [item["hypothesis_id"] for item in self.protocol["hypothesis_family"]["members"]]  # type: ignore[index]
        adjusted = holm_adjust({family[0]: 0.02}, family, alpha=0.05)
        self.assertEqual(adjusted[family[1]]["raw_p_value"], 1.0)
        self.assertEqual(len(adjusted), 2)
        changed_family = copy.deepcopy(self.protocol)
        changed_family["hypothesis_family"]["members"].pop()  # type: ignore[index]
        changed_stop = copy.deepcopy(self.protocol)
        changed_stop["stopping"]["block_target"] = 61  # type: ignore[index]
        self.assertNotEqual(compute_protocol_hash(changed_family), compute_protocol_hash(self.protocol))
        self.assertNotEqual(compute_protocol_hash(changed_stop), compute_protocol_hash(self.protocol))

    def test_wild_bootstrap_replay_is_row_order_invariant_and_detects_effect(self) -> None:
        observations = make_observations(self.protocol, blocks=48, rows_per_block=4)
        outcomes = []
        rng = random.Random(41)
        for item in observations:
            value = (0.8 if item.predicate else -0.8) + rng.gauss(0.0, 0.5)
            outcomes.append(OutcomeRecord(item.observation_id, value))
        first = synthetic_wild_cluster_bootstrap_t(
            self.protocol, observations, outcomes, hypothesis_id="wallet-action-gt-zero",
            fixture_namespace="SYNTHETIC_E5_ONLY_NEVER_PRODUCTION", replications=999,
        )
        second = synthetic_wild_cluster_bootstrap_t(
            self.protocol, list(reversed(observations)), list(reversed(outcomes)),
            hypothesis_id="wallet-action-gt-zero", fixture_namespace="SYNTHETIC_E5_ONLY_NEVER_PRODUCTION",
            replications=999,
        )
        self.assertEqual(first, second)
        self.assertLess(first.raw_p_value, 0.05)
        self.assertGreater(first.confidence_interval[0], 0.0)

    def test_synthetic_null_has_approximately_valid_type_i_error(self) -> None:
        observations = make_observations(self.protocol, blocks=48, rows_per_block=4)
        rejections = 0
        for trial in range(24):
            rng = random.Random(9000 + trial)
            outcomes = [OutcomeRecord(item.observation_id, rng.gauss(0.0, 1.0)) for item in observations]
            result = synthetic_wild_cluster_bootstrap_t(
                self.protocol, observations, outcomes, hypothesis_id=f"null-{trial}",
                fixture_namespace="SYNTHETIC_E5_ONLY_NEVER_PRODUCTION", replications=199,
            )
            rejections += result.raw_p_value <= 0.05
        self.assertLessEqual(rejections, 4)

    def test_degenerate_variance_refuses_inference(self) -> None:
        observations = make_observations(self.protocol, blocks=48, rows_per_block=4)
        outcomes = [OutcomeRecord(item.observation_id, 0.0) for item in observations]
        with self.assertRaisesRegex(InferenceRefused, "degenerate"):
            synthetic_wild_cluster_bootstrap_t(
                self.protocol, observations, outcomes, hypothesis_id="degenerate",
                fixture_namespace="SYNTHETIC_E5_ONLY_NEVER_PRODUCTION", replications=199,
            )

    def test_identical_replay_produces_identical_scientific_hash(self) -> None:
        observations = make_observations(self.protocol)
        classes = observed_classifications(observations)
        report = evaluate_admissibility(self.protocol, observations, classes, as_of=FINAL_AS_OF)
        first = scientific_replay_hash(self.protocol, observations, classes, report)
        second = scientific_replay_hash(self.protocol, list(reversed(observations)), list(reversed(classes)), report)
        self.assertEqual(first, second)

    def test_e5_has_no_trading_execution_signal_or_prediction_authority(self) -> None:
        self.assertFalse(any(self.protocol["authority"].values()))  # type: ignore[union-attr]
        registry = PhaseE5Registry(self.database)
        self.assertFalse(registry.TRADING_AUTHORITY)
        self.assertFalse(registry.EXECUTION_AUTHORITY)
        self.assertFalse(registry.SIGNAL_AUTHORITY)
        self.assertFalse(registry.PREDICTION_AUTHORITY)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
