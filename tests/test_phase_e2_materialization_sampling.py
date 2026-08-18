from __future__ import annotations

import json
import sqlite3
import subprocess
import sys
import tempfile
import threading
import unittest
from dataclasses import replace
from pathlib import Path

from src.copytrade.science_repository import ScientificRepository, canonical_hash as d_hash
from src.phase_e import (
    ALL_ELIGIBLE_V1,
    CorpusProvenanceError,
    DETERMINISTIC_HASH_V1,
    TIME_STRATIFIED_HASH_V2,
    EligibilitySpec,
    FeatureReference,
    MaterializationIntegrityError,
    MaterializationSpec,
    OutcomeHorizon,
    OutcomeResolutionSpec,
    PartitionIdentity,
    PhaseEMaterializer,
    StratificationSpec,
)
from src.phase_e.types import canonical_hash, storage_json


NOW = "2026-08-17T02:00:00Z"
CORPUS = "corpus-e2-full-retention"
SOURCE = "HISTORICAL_OFFICIAL_ARCHIVE"


def _at(second: int) -> str:
    return f"2026-08-17T00:{second // 60:02}:{second % 60:02}Z"


def _partition() -> PartitionIdentity:
    return PartitionIdentity(
        partition_id="e2-boundary-v1",
        train_start="2026-08-17T00:00:00Z", train_end="2026-08-17T00:02:00Z",
        validation_start="2026-08-17T00:03:00Z", validation_end="2026-08-17T00:05:00Z",
        test_start="2026-08-17T00:06:00Z", test_end="2026-08-17T00:08:00Z",
        purge_seconds=0, embargo_seconds=0, random_seed=19, horizon=OutcomeHorizon(5),
        sampling_algorithm="DETERMINISTIC_HASH_V1",
    )


class PhaseE2MaterializationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / "hot" / "science.sqlite3"
        self.repository = ScientificRepository(self.path)
        self.repository.initialize()
        self._record_d_corpus()
        self.materializer = PhaseEMaterializer(self.path)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _record_d_corpus(self) -> None:
        self.repository.register_feature("wallet_action", 1, {"source": "normalized_fill", "transform": "identity"}, created_at=NOW, code_sha="d7-data-ignition-v1")
        coverage = {
            "coverage_id": "coverage-e2", "interval_start": "2026-08-17T00:00:00Z", "interval_end": "2026-08-17T00:10:00Z",
            "source_name": "hyperliquid_hypercore_node_fills_by_block", "state": "PROVEN_COMPLETE", "coverage_fraction": 1.0,
            "expected_hours": 1, "verified_hours": 1, "missing_hours": 0, "malformed_hours": 0, "parsed_hours": 1,
            "observation_count": 117, "duplicate_count": 0, "timestamp_anomalies": 0,
            "first_event_at": "2026-08-17T00:00:01Z", "last_event_at": "2026-08-17T00:07:59Z",
            "wallet_attribution_quality": "official_per_fill", "market_evidence_availability": "trade_print_only",
            "computed_at": NOW, "details": {"fixture": "e2"},
        }
        recorded = self.repository.record_coverage(coverage)
        # D's snapshot intentionally records only a commissioning projection.
        # E.2 must fingerprint all 117 retained rows below instead.
        self.repository.record_corpus_snapshot({
            "corpus_fingerprint": CORPUS, "interval_start": coverage["interval_start"], "interval_end": coverage["interval_end"],
            "coverage_id": coverage["coverage_id"], "observation_fingerprint": "d7-bounded-256-style-projection",
            "feature_versions": [{"feature_id": "wallet_action", "version": 1}], "symbols": ["BTC"],
            "code_sha": "d7-data-ignition-v1", "config_sha": "d7-config", "created_at": NOW,
            "coverage": recorded, "source_hours": [coverage["interval_start"]],
        })
        records: list[dict[str, object]] = []
        # Each split has 12 early, outcome-contained anchors and one anchor
        # exactly five seconds before its end, which must be excluded.
        for base, boundary in ((1, 120), (181, 300), (361, 480)):
            for index, second in enumerate([*range(base, base + 12), boundary - 5]):
                fill_payload = {"origin": SOURCE, "price": 100.0 + second / 10,
                                "side": "buy" if index % 2 else "sell", "notional": 20.0,
                                "estimated_cost": 0.001}
                market_payload = {"origin": SOURCE, "price": 100.0 + second / 10}
                for kind, payload, suffix in (("WALLET_FILL", fill_payload, "fill"), ("MARKET_PRICE", market_payload, "market")):
                    fingerprint = d_hash(payload)
                    records.append({
                        "observation_id": f"obs-{suffix}-{second:04}", "kind": kind, "source": SOURCE,
                        "source_event_id": f"{suffix}-{second}", "wallet": "0xabc" if kind == "WALLET_FILL" else None,
                        "symbol": "BTC", "event_at": _at(second), "received_at": _at(second), "normalized_at": _at(second),
                        "network": "mainnet-public", "raw_fingerprint": fingerprint, "schema_version": 1,
                        "code_sha": "d7-data-ignition-v1", "config_hash": "d7-config", "quality_flags": {"historical": True},
                        "payload": payload, "persisted_at": NOW,
                    })
                # The causal label has a post-horizon market price to attach.
                later = second + 5
                later_payload = {"origin": SOURCE, "price": 100.0 + later / 10}
                fingerprint = d_hash(later_payload)
                records.append({
                    "observation_id": f"obs-market-end-{second:04}", "kind": "MARKET_PRICE", "source": SOURCE,
                    "source_event_id": f"market-end-{second}", "wallet": None, "symbol": "BTC", "event_at": _at(later),
                    "received_at": _at(later), "normalized_at": _at(later), "network": "mainnet-public",
                    "raw_fingerprint": fingerprint, "schema_version": 1, "code_sha": "d7-data-ignition-v1", "config_hash": "d7-config",
                    "quality_flags": {"historical": True}, "payload": later_payload, "persisted_at": NOW,
                })
        self.repository.record_observations_batch(records)

    def _spec(self, *, seed: int = 7, target: int = 20, algorithm: str = DETERMINISTIC_HASH_V1) -> MaterializationSpec:
        eligibility = EligibilitySpec(source=SOURCE, kinds=("WALLET_FILL",))
        universe = self.materializer.bind_source_universe(corpus_fingerprint=CORPUS, eligibility=eligibility)
        return MaterializationSpec(
            source_universe=universe, partition=_partition(), eligibility=eligibility,
            required_features=(FeatureReference("wallet_action", 1),), outcome_horizon=OutcomeHorizon(5),
            sampling_algorithm=algorithm, sampling_seed=seed, target_count=target,
            tier="PILOT", purpose="E2 infrastructure commissioning",
            stratification=(StratificationSpec("UTC_TIME_BUCKET", 60) if algorithm == TIME_STRATIFIED_HASH_V2 else StratificationSpec()),
        )

    def _record_extra(self, *, observation_id: str, kind: str, symbol: str, at: str,
                      payload: dict[str, object]) -> None:
        body = {"origin": SOURCE, **payload}
        if kind == "WALLET_FILL":
            body.setdefault("estimated_cost", 0.001)
        fingerprint = d_hash(body)
        self.repository.record_observation(
            observation_id, kind=kind, source=SOURCE, source_event_id=f"extra-{observation_id}",
            wallet="0xdef" if kind == "WALLET_FILL" else None, symbol=symbol,
            event_at=at, received_at=at, normalized_at=at, network="mainnet-public",
            raw_fingerprint=fingerprint, schema_version=1, code_sha="d7-data-ignition-v1",
            config_hash="d7-config", quality_flags={"historical": True}, payload=body, persisted_at=NOW,
        )

    def test_full_retention_is_bound_not_the_d7_bounded_projection(self) -> None:
        spec = self._spec()
        self.assertEqual(spec.source_universe.observation_count, 117)
        self.assertNotEqual(spec.source_universe.observation_fingerprint, "d7-bounded-256-style-projection")
        plan = self.materializer.plan(spec)
        self.assertEqual(plan["eligible_count"], 36)
        self.assertEqual(plan["excluded_counts"]["OUTCOME_WINDOW_CROSSES_PARTITION_END"], 3)
        self.assertEqual(plan["excluded_counts"]["UNSUPPORTED_KIND"], 78)

    def test_deterministic_membership_freezes_before_outcome_attachment(self) -> None:
        spec = self._spec(target=20)
        finished = self.materializer.build(spec, registered_at=NOW)
        self.assertEqual(finished["status"], "COMPLETE")
        self.assertEqual(finished["selected_count"], 20)
        self.assertFalse(finished["trading_authority"])
        membership = self.materializer.membership(spec.materialization_id)
        self.assertEqual(len(membership), 20)
        self.assertEqual([item["ordinal"] for item in membership], list(range(20)))
        verification = self.materializer.verify(spec.materialization_id)
        self.assertTrue(verification["verified"])
        again = self.materializer.build(spec, registered_at="2026-08-17T09:00:00Z")
        self.assertEqual(again["membership_fingerprint"], finished["membership_fingerprint"])
        self.assertEqual(again["completed_artifact_fingerprint"], finished["completed_artifact_fingerprint"])

    def test_identity_changes_with_sampling_input_and_time_stratification_is_causal(self) -> None:
        first = self._spec(seed=1)
        second = self._spec(seed=2)
        changed_target = self._spec(seed=1, target=19)
        changed_resolution = replace(first, outcome_resolution=OutcomeResolutionSpec(maximum_lag_seconds=1))
        self.assertNotEqual(first.materialization_id, second.materialization_id)
        self.assertNotEqual(first.materialization_id, changed_target.materialization_id)
        self.assertNotEqual(first.materialization_id, changed_resolution.materialization_id)
        stratified = self._spec(algorithm=TIME_STRATIFIED_HASH_V2, target=18)
        completed = self.materializer.build(stratified)
        self.assertEqual(completed["selected_count"], 18)
        self.assertTrue(all("utc:" in item["stratum"] for item in self.materializer.membership(stratified.materialization_id)))

    def test_all_eligible_mode_streams_and_freezes_every_eligible_anchor(self) -> None:
        base = self._spec(target=1)
        full = MaterializationSpec(
            source_universe=base.source_universe, partition=base.partition, eligibility=base.eligibility,
            required_features=base.required_features, outcome_horizon=base.outcome_horizon,
            sampling_algorithm=ALL_ELIGIBLE_V1, sampling_seed=base.sampling_seed, target_count=None,
            tier="FULL", purpose="full-population infrastructure check",
        )
        completed = self.materializer.build(full)
        self.assertEqual(completed["selected_count"], 36)
        self.assertEqual(len(self.materializer.membership(full.materialization_id)), 36)

    def test_boundary_and_missing_data_do_not_silently_change_membership(self) -> None:
        spec = self._spec(target=36)
        finished = self.materializer.build(spec)
        members = self.materializer.membership(spec.materialization_id)
        self.assertNotIn("obs-fill-0115", {item["observation_id"] for item in members})
        self.assertNotIn("obs-fill-0295", {item["observation_id"] for item in members})
        self.assertNotIn("obs-fill-0475", {item["observation_id"] for item in members})
        self.assertEqual(finished["selected_count"], 36)
        connection = sqlite3.connect(self.path)
        try:
            outcomes = connection.execute("SELECT COUNT(*) FROM phase_e_materialization_outcomes WHERE materialization_id=?", (spec.materialization_id,)).fetchone()[0]
            self.assertEqual(outcomes, 36)
        finally:
            connection.close()

    def test_projection_and_membership_forgery_fail_closed(self) -> None:
        status_spec = self._spec(target=10)
        registered = self.materializer.register(status_spec)
        connection = sqlite3.connect(self.path)
        try:
            connection.execute("UPDATE phase_e_materializations SET status='COMPLETE' WHERE materialization_id=?", (registered["materialization_id"],))
            connection.commit()
        finally:
            connection.close()
        with self.assertRaises(MaterializationIntegrityError):
            self.materializer.get(registered["materialization_id"])

        spec = self._spec(seed=71, target=10)
        self.materializer.build(spec)
        connection = sqlite3.connect(self.path)
        try:
            with self.assertRaisesRegex(sqlite3.IntegrityError, "only while selecting"):
                connection.execute(
                    """INSERT INTO phase_e_materialization_membership VALUES (?, 999, 'forged', '2026-08-17T00:00:01Z', 'train', 'train:all', 'forged')""",
                    (spec.materialization_id,),
                )
        finally:
            connection.close()

    def test_concurrent_builds_cannot_diverge(self) -> None:
        spec = self._spec(seed=83, target=18)
        failures: list[Exception] = []

        def build() -> None:
            try:
                self.materializer.build(spec)
            except Exception as exc:  # pragma: no cover - reported below
                failures.append(exc)

        first = threading.Thread(target=build)
        second = threading.Thread(target=build)
        first.start(); second.start(); first.join(); second.join()
        self.assertEqual(failures, [])
        complete = self.materializer.get(spec.materialization_id)
        self.assertEqual(complete["status"], "COMPLETE")
        self.assertEqual(len(self.materializer.membership(spec.materialization_id)), 18)

    def test_restart_after_membership_freeze_resumes_same_immutable_membership(self) -> None:
        spec = self._spec(seed=91, target=16)
        self.materializer.register(spec)
        self.materializer._select_and_freeze(spec)
        frozen = self.materializer.get(spec.materialization_id)
        self.assertEqual(frozen["status"], "MEMBERSHIP_FROZEN")
        fingerprint = frozen["membership_fingerprint"]
        restarted = PhaseEMaterializer(self.path)
        completed = restarted.build(spec)
        self.assertEqual(completed["status"], "COMPLETE")
        self.assertEqual(completed["membership_fingerprint"], fingerprint)

    def test_fractional_second_boundaries_and_horizons_use_instant_not_text_order(self) -> None:
        self._record_extra(observation_id="obs-fractional-fill", kind="WALLET_FILL", symbol="FRACTIONAL",
                           at="2026-08-17T00:00:00.500000Z", payload={"price": 100.0, "side": "buy"})
        self._record_extra(observation_id="obs-fractional-before", kind="MARKET_PRICE", symbol="FRACTIONAL",
                           at="2026-08-17T00:00:05Z", payload={"price": 101.0})
        self._record_extra(observation_id="obs-fractional-after", kind="MARKET_PRICE", symbol="FRACTIONAL",
                           at="2026-08-17T00:00:05.750000Z", payload={"price": 102.0})
        base = self._spec(target=1)
        full = MaterializationSpec(
            source_universe=base.source_universe, partition=base.partition, eligibility=base.eligibility,
            required_features=base.required_features, outcome_horizon=base.outcome_horizon,
            sampling_algorithm=ALL_ELIGIBLE_V1, sampling_seed=base.sampling_seed, target_count=None,
            tier="FULL", purpose="fractional instant regression",
        )
        completed = self.materializer.build(full)
        self.assertEqual(completed["selected_count"], 37)
        self.assertIn("obs-fractional-fill", {item["observation_id"] for item in self.materializer.membership(full.materialization_id)})
        connection = sqlite3.connect(self.path)
        try:
            row = connection.execute(
                "SELECT resolved_at, payload_json FROM phase_e_materialization_outcomes WHERE materialization_id=? AND observation_id=?",
                (full.materialization_id, "obs-fractional-fill"),
            ).fetchone()
            self.assertEqual(row[0], "2026-08-17T00:00:05.750000Z")
            self.assertIn('"resolution_lag_seconds":0.25', row[1])
        finally:
            connection.close()

    def test_sparse_outcome_beyond_tolerance_is_missing_without_replacement(self) -> None:
        self._record_extra(observation_id="obs-sparse-fill", kind="WALLET_FILL", symbol="SPARSE",
                           at="2026-08-17T00:00:30.500000Z", payload={"price": 100.0, "side": "buy"})
        self._record_extra(observation_id="obs-sparse-before", kind="MARKET_PRICE", symbol="SPARSE",
                           at="2026-08-17T00:00:35Z", payload={"price": 101.0})
        self._record_extra(observation_id="obs-sparse-late", kind="MARKET_PRICE", symbol="SPARSE",
                           at="2026-08-17T00:00:41Z", payload={"price": 102.0})
        base = self._spec(target=1)
        full = MaterializationSpec(
            source_universe=base.source_universe, partition=base.partition, eligibility=base.eligibility,
            required_features=base.required_features, outcome_horizon=base.outcome_horizon,
            sampling_algorithm=ALL_ELIGIBLE_V1, sampling_seed=base.sampling_seed, target_count=None,
            tier="FULL", purpose="sparse outcome tolerance regression",
        )
        completed = self.materializer.build(full)
        self.assertEqual(completed["selected_count"], 37)
        connection = sqlite3.connect(self.path)
        try:
            row = connection.execute(
                "SELECT missing, missing_reason FROM phase_e_materialization_outcomes WHERE materialization_id=? AND observation_id=?",
                (full.materialization_id, "obs-sparse-fill"),
            ).fetchone()
            self.assertEqual(row, (1, "OUTCOME_MARKET_EVIDENCE_NOT_WITHIN_TOLERANCE"))
        finally:
            connection.close()

    def test_full_row_source_fingerprint_rejects_semantic_timestamp_drift(self) -> None:
        spec = self._spec(target=10)
        connection = sqlite3.connect(self.path)
        try:
            connection.execute("DROP TRIGGER science_observations_immutable")
            connection.execute("UPDATE science_observations SET event_at=? WHERE observation_id=?",
                               ("2026-08-17T00:00:02Z", "obs-fill-0001"))
            connection.commit()
        finally:
            connection.close()
        with self.assertRaises((CorpusProvenanceError, MaterializationIntegrityError)):
            self.materializer.plan(spec)

    def test_d_change_between_selection_batches_cannot_cross_freeze_barrier(self) -> None:
        spec = self._spec(seed=109, target=18)
        self.materializer.register(spec)
        connection = sqlite3.connect(self.path)
        try:
            connection.execute("DROP TRIGGER science_observations_immutable")
            original_code_sha = connection.execute(
                "SELECT code_sha FROM science_observations WHERE observation_id='obs-fill-0001'",
            ).fetchone()[0]
            connection.commit()
        finally:
            connection.close()
        original_insert = self.materializer._insert_membership

        def insert_then_change(materialization_id: str, candidates: object) -> None:
            original_insert(materialization_id, candidates)  # type: ignore[arg-type]
            changed = sqlite3.connect(self.path)
            try:
                changed.execute("UPDATE science_observations SET code_sha='raced-d-evidence' WHERE observation_id='obs-fill-0001'")
                changed.commit()
            finally:
                changed.close()

        self.materializer._insert_membership = insert_then_change  # type: ignore[method-assign]
        try:
            with self.assertRaises(CorpusProvenanceError):
                self.materializer._select_and_freeze(spec)
        finally:
            self.materializer._insert_membership = original_insert  # type: ignore[method-assign]
        incomplete = self.materializer.get(spec.materialization_id)
        self.assertEqual(incomplete["status"], "SELECTING")
        self.assertIsNone(incomplete["membership_fingerprint"])
        connection = sqlite3.connect(self.path)
        try:
            connection.execute("UPDATE science_observations SET code_sha=? WHERE observation_id='obs-fill-0001'",
                               (original_code_sha,))
            connection.commit()
        finally:
            connection.close()
        completed = self.materializer.build(spec)
        self.assertEqual(completed["status"], "COMPLETE")

    def test_unicode_and_alternate_offset_canonicalization_fail_closed(self) -> None:
        spec = self._spec(target=10)
        self.materializer.build(spec)
        connection = sqlite3.connect(self.path)
        try:
            connection.execute("DROP TRIGGER phase_e_materialization_membership_append_only_update")
            connection.execute(
                "UPDATE phase_e_materialization_membership SET normalized_at=replace(normalized_at, 'Z', '+00:00') WHERE materialization_id=? AND ordinal=0",
                (spec.materialization_id,),
            )
            connection.commit()
        finally:
            connection.close()
        with self.assertRaisesRegex(MaterializationIntegrityError, "canonical UTC"):
            self.materializer.get(spec.materialization_id)

        # A decomposed Unicode symbol would normalize to the same E identity
        # hash as NFC while changing SQLite equality during outcome lookup.
        fresh = self._spec(seed=127, target=10)
        connection = sqlite3.connect(self.path)
        try:
            connection.execute("DROP TRIGGER science_observations_immutable")
            connection.execute("UPDATE science_observations SET symbol=? WHERE observation_id=?",
                               ("e\u0301", "obs-fill-0002"))
            connection.commit()
        finally:
            connection.close()
        with self.assertRaisesRegex(MaterializationIntegrityError, "NFC-normalized"):
            self.materializer.plan(fresh)

    def test_feature_lookback_cannot_cross_partition_start(self) -> None:
        eligibility = EligibilitySpec(source=SOURCE, kinds=("WALLET_FILL",))
        universe = self.materializer.bind_source_universe(corpus_fingerprint=CORPUS, eligibility=eligibility)
        partition = PartitionIdentity(
            partition_id="lookback-safe-v1",
            train_start="2026-08-17T00:00:00Z", train_end="2026-08-17T00:02:00Z",
            validation_start="2026-08-17T00:03:00Z", validation_end="2026-08-17T00:05:00Z",
            test_start="2026-08-17T00:06:00Z", test_end="2026-08-17T00:08:00Z",
            purge_seconds=0, embargo_seconds=0, random_seed=19, horizon=OutcomeHorizon(5),
            feature_lookback_seconds=5, sampling_algorithm=DETERMINISTIC_HASH_V1,
        )
        spec = MaterializationSpec(
            source_universe=universe, partition=partition, eligibility=eligibility,
            required_features=(FeatureReference("wallet_action", 1, lookback_seconds=5),),
            outcome_horizon=OutcomeHorizon(5), sampling_algorithm=DETERMINISTIC_HASH_V1,
            sampling_seed=7, target_count=24, tier="PILOT", purpose="lookback boundary regression",
        )
        plan = self.materializer.plan(spec)
        self.assertEqual(plan["eligible_count"], 24)
        self.assertEqual(plan["excluded_counts"]["FEATURE_WINDOW_CROSSES_PARTITION_START"], 12)

    def test_persisted_supported_feature_must_match_deterministic_replay(self) -> None:
        observation = self.repository.observation_by_id("obs-fill-0001")
        assert observation is not None
        fingerprint = d_hash({
            "feature": "wallet_action", "observation": observation["raw_fingerprint"],
            "sources": [(observation["observation_id"], observation["raw_fingerprint"])],
        })
        self.repository.record_feature_value(
            "fv-false-wallet-action", feature_id="wallet_action", feature_version=1,
            observation_id=observation["observation_id"], value=1.0, missing=False,
            source_observation_ids=(observation["observation_id"],), data_fingerprint=fingerprint,
            materialized_at=NOW,
        )
        base = self._spec(target=1)
        full = MaterializationSpec(
            source_universe=base.source_universe, partition=base.partition, eligibility=base.eligibility,
            required_features=base.required_features, outcome_horizon=base.outcome_horizon,
            sampling_algorithm=ALL_ELIGIBLE_V1, sampling_seed=base.sampling_seed, target_count=None,
            tier="FULL", purpose="supported feature replay regression",
        )
        with self.assertRaisesRegex(MaterializationIntegrityError, "conflicts with deterministic causal replay"):
            self.materializer.build(full)

    def test_sampling_design_preserves_population_and_inclusion_probabilities(self) -> None:
        spec = self._spec(seed=211, target=2, algorithm=TIME_STRATIFIED_HASH_V2)
        completed = self.materializer.build(spec)
        design = completed["sampling_design"]
        self.assertEqual(design["eligible_count"], 36)
        self.assertEqual(design["selected_count"], 2)
        self.assertEqual(len(design["strata"]), 3)
        self.assertEqual(sum(item["target_count"] for item in design["strata"]), 2)
        self.assertEqual(sum(item["selected_count"] for item in design["strata"]), 2)
        self.assertTrue(all("inclusion_probability" in item for item in design["strata"]))
        targeted = {item["stratum_id"] for item in design["strata"] if item["target_count"]}
        expected = {key for key, value in self.materializer._time_stratum_targets(
            sorted(item["stratum_id"] for item in design["strata"]), 2, spec,
        ).items() if value}
        self.assertEqual(targeted, expected)

    def test_consistently_rehashed_false_sampling_weights_fail_exact_replay(self) -> None:
        spec = self._spec(seed=307, target=18, algorithm=TIME_STRATIFIED_HASH_V2)
        self.materializer.build(spec)
        connection = sqlite3.connect(self.path)
        connection.row_factory = sqlite3.Row
        try:
            connection.execute("DROP TRIGGER phase_e_materialization_sampling_design_append_only_update")
            connection.execute("DROP TRIGGER phase_e_materialization_events_append_only_update")
            projection = connection.execute(
                "SELECT * FROM phase_e_materializations WHERE materialization_id=?", (spec.materialization_id,),
            ).fetchone()
            sampling = connection.execute(
                "SELECT * FROM phase_e_materialization_sampling_design WHERE materialization_id=?",
                (spec.materialization_id,),
            ).fetchone()
            design = json.loads(sampling["design_json"])
            design["strata"][0]["sampling_weight"] = {"numerator": 999, "denominator": 1}
            design_hash = canonical_hash(design)
            sampling_artifact = canonical_hash({
                "materialization_id": spec.materialization_id, "design_hash": design_hash, "design": design,
            })
            connection.execute(
                "UPDATE phase_e_materialization_sampling_design SET design_json=?, design_hash=?, artifact_hash=? WHERE materialization_id=?",
                (storage_json(design), design_hash, sampling_artifact, spec.materialization_id),
            )
            completed = canonical_hash({
                "membership": projection["membership_fingerprint"], "sampling_design": sampling_artifact,
                "features": projection["feature_artifact_fingerprint"],
                "outcomes": projection["outcome_artifact_fingerprint"],
                "specification_hash": projection["specification_hash"],
            })
            connection.execute(
                "UPDATE phase_e_materializations SET sampling_design_fingerprint=?, completed_artifact_fingerprint=? WHERE materialization_id=?",
                (sampling_artifact, completed, spec.materialization_id),
            )
            for event_type in ("MEMBERSHIP_FROZEN", "COMPLETE"):
                event = connection.execute(
                    "SELECT * FROM phase_e_materialization_events WHERE materialization_id=? AND event_type=?",
                    (spec.materialization_id, event_type),
                ).fetchone()
                payload = json.loads(event["payload_json"])
                payload["sampling_design_fingerprint"] = sampling_artifact
                if event_type == "COMPLETE":
                    payload["completed_artifact_fingerprint"] = completed
                payload_hash = canonical_hash(payload)
                event_id = canonical_hash({
                    "materialization_id": spec.materialization_id, "event_type": event_type,
                    "from_status": event["from_status"], "to_status": event["to_status"],
                    "reason": event["reason"], "event_at": event["event_at"], "payload_hash": payload_hash,
                })
                connection.execute(
                    "UPDATE phase_e_materialization_events SET event_id=?, payload_json=?, payload_hash=? WHERE event_id=?",
                    (event_id, storage_json(payload), payload_hash, event["event_id"]),
                )
            connection.commit()
        finally:
            connection.close()
        # Local hashes are deliberately made self-consistent. Exact D-backed
        # replay, not hash recomputation alone, must reject the false weight.
        self.assertEqual(self.materializer.get(spec.materialization_id)["status"], "COMPLETE")
        with self.assertRaisesRegex(MaterializationIntegrityError, "conflicts with deterministic selection"):
            self.materializer.verify(spec.materialization_id)

    def test_validly_rehashed_false_event_semantics_fail_closed(self) -> None:
        spec = self._spec(seed=313, target=10)
        self.materializer.build(spec)
        connection = sqlite3.connect(self.path)
        connection.row_factory = sqlite3.Row
        try:
            connection.execute("DROP TRIGGER phase_e_materialization_events_append_only_update")
            event = connection.execute(
                "SELECT * FROM phase_e_materialization_events WHERE materialization_id=? AND event_type='SELECTING'",
                (spec.materialization_id,),
            ).fetchone()
            forged_reason = "valid_hash_but_false_reason"
            forged_id = canonical_hash({
                "materialization_id": spec.materialization_id, "event_type": event["event_type"],
                "from_status": event["from_status"], "to_status": event["to_status"],
                "reason": forged_reason, "event_at": event["event_at"], "payload_hash": event["payload_hash"],
            })
            connection.execute("UPDATE phase_e_materialization_events SET event_id=?, reason=? WHERE event_id=?",
                               (forged_id, forged_reason, event["event_id"]))
            connection.commit()
        finally:
            connection.close()
        with self.assertRaisesRegex(MaterializationIntegrityError, "false transition semantics"):
            self.materializer.get(spec.materialization_id)

    def test_real_process_death_after_freeze_recovers_same_membership(self) -> None:
        spec = self._spec(seed=401, target=18)
        self.materializer.register(spec)
        code = (
            "import os; from src.phase_e import PhaseEMaterializer; "
            f"m=PhaseEMaterializer({str(self.path)!r}); "
            f"c=m._connection(); conn=c.__enter__(); row=m._require(conn,{spec.materialization_id!r}); s=m._spec_from_row(row); c.__exit__(None,None,None); "
            "m._select_and_freeze(s); os._exit(23)"
        )
        child = subprocess.run([sys.executable, "-c", code], cwd=Path(__file__).parents[1], check=False)
        self.assertEqual(child.returncode, 23)
        frozen = self.materializer.get(spec.materialization_id)
        self.assertEqual(frozen["status"], "MEMBERSHIP_FROZEN")
        fingerprint = frozen["membership_fingerprint"]
        completed = PhaseEMaterializer(self.path).build(spec)
        self.assertEqual(completed["membership_fingerprint"], fingerprint)

    def test_two_process_builders_produce_one_identical_complete_artifact(self) -> None:
        spec = self._spec(seed=419, target=18)
        self.materializer.register(spec)
        code = (
            "from src.phase_e import PhaseEMaterializer; "
            f"m=PhaseEMaterializer({str(self.path)!r}); "
            f"c=m._connection(); conn=c.__enter__(); row=m._require(conn,{spec.materialization_id!r}); s=m._spec_from_row(row); c.__exit__(None,None,None); "
            "m.build(s)"
        )
        first = subprocess.Popen([sys.executable, "-c", code], cwd=Path(__file__).parents[1])
        second = subprocess.Popen([sys.executable, "-c", code], cwd=Path(__file__).parents[1])
        self.assertEqual(first.wait(timeout=30), 0)
        self.assertEqual(second.wait(timeout=30), 0)
        complete = self.materializer.verify(spec.materialization_id)
        self.assertTrue(complete["verified"])

    def test_two_process_full_population_builders_cannot_diverge(self) -> None:
        base = self._spec(seed=423, target=1)
        spec = MaterializationSpec(
            source_universe=base.source_universe, partition=base.partition, eligibility=base.eligibility,
            required_features=base.required_features, outcome_horizon=base.outcome_horizon,
            sampling_algorithm=ALL_ELIGIBLE_V1, sampling_seed=base.sampling_seed, target_count=None,
            tier="FULL", purpose="process-level full-population concurrency regression",
        )
        self.materializer.register(spec)
        code = (
            "from src.phase_e import PhaseEMaterializer; "
            f"m=PhaseEMaterializer({str(self.path)!r}); "
            f"c=m._connection(); conn=c.__enter__(); row=m._require(conn,{spec.materialization_id!r}); s=m._spec_from_row(row); c.__exit__(None,None,None); "
            "m.build(s)"
        )
        first = subprocess.Popen([sys.executable, "-c", code], cwd=Path(__file__).parents[1])
        second = subprocess.Popen([sys.executable, "-c", code], cwd=Path(__file__).parents[1])
        self.assertEqual(first.wait(timeout=30), 0)
        self.assertEqual(second.wait(timeout=30), 0)
        complete = self.materializer.verify(spec.materialization_id)
        self.assertTrue(complete["verified"])
        self.assertEqual(self.materializer.get(spec.materialization_id)["selected_count"], 36)

    def test_process_death_between_projection_update_and_event_rolls_back_atomically(self) -> None:
        spec = self._spec(seed=431, target=18)
        self.materializer.register(spec)
        code = (
            "import os; from src.phase_e import PhaseEMaterializer; "
            f"m=PhaseEMaterializer({str(self.path)!r}); "
            f"c=m._connection(); conn=c.__enter__(); row=m._require(conn,{spec.materialization_id!r}); s=m._spec_from_row(row); c.__exit__(None,None,None); "
            "m._append_event=lambda *a,**k: os._exit(29); m._select_and_freeze(s)"
        )
        child = subprocess.run([sys.executable, "-c", code], cwd=Path(__file__).parents[1], check=False)
        self.assertEqual(child.returncode, 29)
        rolled_back = self.materializer.get(spec.materialization_id)
        self.assertEqual(rolled_back["status"], "REGISTERED")
        self.assertEqual(rolled_back["persisted_membership_count"], 0)
        self.assertEqual(self.materializer.build(spec)["status"], "COMPLETE")

    def test_process_death_after_feature_batch_resumes_exact_artifacts(self) -> None:
        spec = self._spec(seed=433, target=18)
        self.materializer.register(spec)
        code = (
            "import os; from src.phase_e import PhaseEMaterializer; "
            f"m=PhaseEMaterializer({str(self.path)!r}); "
            f"c=m._connection(); conn=c.__enter__(); row=m._require(conn,{spec.materialization_id!r}); s=m._spec_from_row(row); c.__exit__(None,None,None); "
            "m._select_and_freeze(s); m._BATCH_SIZE=5; original=m._insert_feature_batch; "
            "m._insert_feature_batch=lambda *a,**k: (original(*a,**k),os._exit(31)); m._materialize_features(s)"
        )
        child = subprocess.run([sys.executable, "-c", code], cwd=Path(__file__).parents[1], check=False)
        self.assertEqual(child.returncode, 31)
        partial = self.materializer.get(spec.materialization_id)
        self.assertEqual(partial["status"], "MATERIALIZING_FEATURES")
        completed = self.materializer.build(spec)
        self.assertEqual(completed["status"], "COMPLETE")
        self.assertTrue(self.materializer.verify(spec.materialization_id)["verified"])

    def test_process_death_after_outcome_batch_resumes_same_members(self) -> None:
        spec = self._spec(seed=439, target=18)
        self.materializer.register(spec)
        code = (
            "import os; from src.phase_e import PhaseEMaterializer; "
            f"m=PhaseEMaterializer({str(self.path)!r}); "
            f"c=m._connection(); conn=c.__enter__(); row=m._require(conn,{spec.materialization_id!r}); s=m._spec_from_row(row); c.__exit__(None,None,None); "
            "m._select_and_freeze(s); m._materialize_features(s); m._BATCH_SIZE=5; original=m._insert_outcome_batch; "
            "m._insert_outcome_batch=lambda *a,**k: (original(*a,**k),os._exit(37)); m._attach_outcomes(s)"
        )
        child = subprocess.run([sys.executable, "-c", code], cwd=Path(__file__).parents[1], check=False)
        self.assertEqual(child.returncode, 37)
        partial = self.materializer.get(spec.materialization_id)
        self.assertEqual(partial["status"], "ATTACHING_OUTCOMES")
        frozen_fingerprint = partial["membership_fingerprint"]
        completed = self.materializer.build(spec)
        self.assertEqual(completed["membership_fingerprint"], frozen_fingerprint)
        self.assertTrue(self.materializer.verify(spec.materialization_id)["verified"])

    def test_free_space_refusal_is_restartable_without_partial_membership(self) -> None:
        spec = self._spec(seed=443, target=18)
        guarded = PhaseEMaterializer(self.path, minimum_free_bytes=2**63)
        with self.assertRaisesRegex(OSError, "free space"):
            guarded.build(spec)
        registered = self.materializer.get(spec.materialization_id)
        self.assertEqual(registered["status"], "REGISTERED")
        self.assertEqual(registered["persisted_membership_count"], 0)
        self.assertEqual(self.materializer.build(spec)["status"], "COMPLETE")


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
