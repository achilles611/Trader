from __future__ import annotations

import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path

from src.copytrade.science_repository import ScientificRepository, canonical_hash as d_hash
from src.phase_e import (
    ALL_ELIGIBLE_V1,
    DETERMINISTIC_HASH_V1,
    TIME_STRATIFIED_HASH_V1,
    EligibilitySpec,
    FeatureReference,
    MaterializationIntegrityError,
    MaterializationSpec,
    OutcomeHorizon,
    PartitionIdentity,
    PhaseEMaterializer,
    StratificationSpec,
)


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
                fill_payload = {"price": 100.0 + second / 10, "side": "buy" if index % 2 else "sell", "notional": 20.0}
                market_payload = {"price": 100.0 + second / 10}
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
                later_payload = {"price": 100.0 + later / 10}
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
            stratification=(StratificationSpec("UTC_TIME_BUCKET", 60) if algorithm == TIME_STRATIFIED_HASH_V1 else StratificationSpec()),
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
        self.assertNotEqual(first.materialization_id, second.materialization_id)
        self.assertNotEqual(first.materialization_id, changed_target.materialization_id)
        stratified = self._spec(algorithm=TIME_STRATIFIED_HASH_V1, target=18)
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
            connection.execute(
                """INSERT INTO phase_e_materialization_membership VALUES (?, 999, 'forged', '2026-08-17T00:00:01Z', 'train', 'train:all', 'forged')""",
                (spec.materialization_id,),
            )
            connection.commit()
        finally:
            connection.close()
        with self.assertRaises(MaterializationIntegrityError):
            self.materializer.get(spec.materialization_id)

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


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
