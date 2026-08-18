from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

from src.phase_e.acquisition import (
    E6_EXPECTED_PROTOCOL_HASH,
    AcquisitionCandidate,
    AcquisitionProtocolError,
    AcquisitionStateError,
    BlockState,
    PhaseE6Acquisition,
    ResolutionMetadata,
    ResolutionRefused,
)
from src.phase_e.prospective import E5_OBSERVATION_SCHEMA, DesignObservation, load_frozen_protocol, scheduled_blocks, wallet_cohort


ROOT = Path(__file__).parents[1]
FROZEN_PROTOCOL = ROOT / "docs" / "commissioning" / "phase-e5-prospective-experiment" / "e5-protocol-v1.json"


def utc(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


class PhaseE6ProspectiveAcquisitionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.protocol = load_frozen_protocol(FROZEN_PROTOCOL)
        self.blocks = scheduled_blocks(self.protocol)
        self.database = Path(self.temp.name) / "e6-acquisition.sqlite3"
        self.engine = PhaseE6Acquisition(self.database, FROZEN_PROTOCOL)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def matching_wallet(self, block_ordinal: int, suffix: str) -> str:
        block = self.blocks[block_ordinal]
        nonce = 0
        while True:
            wallet = f"e6-wallet-{block_ordinal}-{suffix}-{nonce}"
            if wallet_cohort(self.protocol, wallet) == block.cohort:
                return wallet
            nonce += 1

    def candidate(
        self, block_ordinal: int = 0, *, suffix: str = "a", wallet: str | None = None,
        transaction_id: str | None = None, source_namespace: str = "NEW_E5_PROSPECTIVE_ONLY",
    ) -> AcquisitionCandidate:
        block = self.blocks[block_ordinal]
        anchor = utc(block.sample_start) + timedelta(seconds=10 + (len(suffix) % 10))
        observation = DesignObservation(
            observation_id=f"e6-observation-{block_ordinal}-{suffix}", source_schema=E5_OBSERVATION_SCHEMA,
            protocol_hash=E6_EXPECTED_PROTOCOL_HASH, block_id=block.block_id, anchor_at=iso(anchor),
            exposure_end_at=iso(anchor + timedelta(seconds=10)), wallet_id=wallet or self.matching_wallet(block_ordinal, suffix),
            symbol="E6-SYM", source_event_id=f"e6-event-{block_ordinal}-{suffix}", sampling_weight=1.0,
            predicate=suffix.endswith("p"), liquidity_stratum="liquidity", graph_density_stratum="density",
            time_stratum="utc", eligibility_snapshot_hash=f"eligibility-{block_ordinal}-{suffix}",
            symbol_liquidity_eligible=True, transaction_id=transaction_id,
            endpoint_family_id=f"endpoint-{block_ordinal}-{suffix}", campaign_id=f"campaign-{block_ordinal}-{suffix}",
        )
        return AcquisitionCandidate(observation, source_namespace, iso(anchor + timedelta(seconds=1)))

    def open(self, ordinal: int = 0) -> None:
        block = self.blocks[ordinal]
        self.engine.open_block(block.block_id, at=iso(utc(block.sample_start) + timedelta(seconds=1)))

    def seal(self, ordinal: int = 0) -> None:
        block = self.blocks[ordinal]
        self.engine.seal_block(block.block_id, at=iso(utc(block.sample_end) + timedelta(seconds=1)))

    def test_exact_protocol_verification_materializes_only_the_fixed_schedule(self) -> None:
        status = self.engine.status()
        self.assertEqual(status["protocol_hash"], E6_EXPECTED_PROTOCOL_HASH)
        self.assertEqual(status["block_count"], 60)
        self.assertEqual(status["observation_count"], 0)
        self.assertEqual(status["reserved_test_queries"], 0)
        self.assertEqual(status["hard_stop"], "2027-12-25T00:00:00Z")
        self.assertEqual(status["trades_placed"], 0)
        self.assertEqual(status["outcome_access"]["scientific_evaluation_reads"], 0)
        self.assertFalse(any(status["authority"].values()))
        with closing(sqlite3.connect(self.database)) as connection:
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM phase_e6_blocks").fetchone()[0], 60)
            with self.assertRaisesRegex(sqlite3.DatabaseError, "cannot be deleted"):
                connection.execute("DELETE FROM phase_e6_blocks WHERE ordinal=0")
            with self.assertRaisesRegex(sqlite3.DatabaseError, "schedule is immutable"):
                connection.execute("UPDATE phase_e6_blocks SET scheduled_start='2026-01-01T00:00:00Z' WHERE ordinal=0")
            columns = {row[1] for row in connection.execute("PRAGMA table_info(phase_e6_observations)")}
            self.assertFalse(any("outcome" in column.lower() or "return" in column.lower() for column in columns))

    def test_wrong_or_altered_protocol_refuses_acquisition_before_any_database_is_created(self) -> None:
        bad_protocol = Path(self.temp.name) / "altered-protocol.json"
        payload = json.loads(FROZEN_PROTOCOL.read_text(encoding="utf-8"))
        payload["sampling"]["schedule"]["block_count"] = 61
        bad_protocol.write_text(json.dumps(payload), encoding="utf-8")
        engine = PhaseE6Acquisition(Path(self.temp.name) / "bad.sqlite3", bad_protocol)
        with self.assertRaises(AcquisitionProtocolError):
            engine.status()
        self.assertFalse((Path(self.temp.name) / "bad.sqlite3").exists())

    def test_protocol_artifact_is_reverified_after_initialization(self) -> None:
        copied_protocol = Path(self.temp.name) / "e5-protocol.json"
        copied_protocol.write_text(FROZEN_PROTOCOL.read_text(encoding="utf-8"), encoding="utf-8")
        engine = PhaseE6Acquisition(Path(self.temp.name) / "recheck.sqlite3", copied_protocol)
        engine.status()
        altered = json.loads(copied_protocol.read_text(encoding="utf-8"))
        altered["authority"]["trading"] = True
        copied_protocol.write_text(json.dumps(altered), encoding="utf-8")
        with self.assertRaises(AcquisitionProtocolError):
            engine.status()

    def test_clock_semantics_miss_instead_of_rescheduling_and_no_block_61_exists(self) -> None:
        block = self.blocks[0]
        with self.assertRaisesRegex(AcquisitionStateError, "recorded as missed"):
            self.engine.open_block(block.block_id, at=block.sample_end)
        with self.assertRaises(AcquisitionStateError):
            self.engine.open_block(block.block_id, at=block.sample_start)
        self.assertEqual(self.engine.status()["block_states"], {BlockState.SCHEDULED.value: 59, BlockState.ACQUISITION_FAILED.value: 1})
        with closing(sqlite3.connect(self.database)) as connection:
            self.assertEqual(connection.execute("SELECT MAX(ordinal) FROM phase_e6_blocks").fetchone()[0], 59)

    def test_restart_and_repeated_event_are_idempotent_and_sealing_is_immutable(self) -> None:
        block = self.blocks[0]
        self.open()
        candidate = self.candidate()
        first = self.engine.admit_candidate(candidate)
        restarted = PhaseE6Acquisition(self.database, FROZEN_PROTOCOL)
        second = restarted.admit_candidate(candidate)
        self.assertEqual(first, second)
        self.assertEqual(restarted.status()["observation_count"], 1)
        self.seal()
        self.assertEqual(restarted.seal_block(block.block_id, at=iso(utc(block.sample_end) + timedelta(seconds=2)))["state"], BlockState.AWAITING_RESOLUTION.value)
        late = self.candidate(suffix="late")
        response = restarted.admit_candidate(late)
        self.assertEqual(response["decision"], "REJECTED")
        self.assertEqual(response["reason"], "BLOCK_MEMBERSHIP_SEALED")
        self.assertEqual(restarted.status()["observation_count"], 1)

    def test_concurrent_wallet_admission_has_one_authoritative_membership_decision(self) -> None:
        self.open()
        wallet = self.matching_wallet(0, "shared")
        first = self.candidate(suffix="race-a", wallet=wallet)
        second = self.candidate(suffix="race-b", wallet=wallet)
        with ThreadPoolExecutor(max_workers=2) as pool:
            outcomes = list(pool.map(self.engine.admit_candidate, (first, second)))
        self.assertEqual(sum(item["decision"] == "ADMITTED" for item in outcomes), 1)
        self.assertEqual(sum(item["decision"] == "REJECTED" for item in outcomes), 1)
        self.assertEqual(self.engine.status()["observation_count"], 1)

    def test_cross_block_relations_are_refused_but_within_block_relations_are_not_over_rejected(self) -> None:
        self.open()
        same_block_first = self.engine.admit_candidate(self.candidate(suffix="within-a", transaction_id="within-tx"))
        same_block_second = self.engine.admit_candidate(self.candidate(suffix="within-b", transaction_id="within-tx"))
        self.assertEqual((same_block_first["decision"], same_block_second["decision"]), ("ADMITTED", "ADMITTED"))

        self.open(1)
        first = self.candidate(1, suffix="linked-a", transaction_id="cross-block-tx")
        self.assertEqual(self.engine.admit_candidate(first)["decision"], "ADMITTED")
        self.open(2)
        second = self.candidate(2, suffix="linked-b", transaction_id="cross-block-tx")
        rejected = self.engine.admit_candidate(second)
        self.assertEqual(rejected["decision"], "REJECTED")
        self.assertEqual(rejected["reason"], "CROSS_BLOCK_TRANSACTION_RELATION")

    def test_historical_and_synthetic_candidates_cannot_enter_production_membership(self) -> None:
        self.open()
        historical = self.candidate(suffix="historical")
        historical = AcquisitionCandidate(replace(historical.observation, source_schema="phase-e4-observation-v1"), historical.source_namespace, historical.received_at)
        synthetic = self.candidate(suffix="synthetic", source_namespace="SYNTHETIC_E5_ONLY_NEVER_PRODUCTION")
        self.assertEqual(self.engine.admit_candidate(historical)["reason"], "NON_PROSPECTIVE_OR_HISTORICAL_OBSERVATION")
        self.assertEqual(self.engine.admit_candidate(synthetic)["reason"], "SYNTHETIC_OR_NONPROSPECTIVE_SOURCE")
        self.assertEqual(self.engine.status()["observation_count"], 0)

    def test_wrong_wallet_cohort_is_rejected_without_changing_the_frozen_cohort(self) -> None:
        self.open()
        nonce = 0
        while wallet_cohort(self.protocol, f"wrong-cohort-{nonce}") == self.blocks[0].cohort:
            nonce += 1
        result = self.engine.admit_candidate(self.candidate(suffix="wrong-cohort", wallet=f"wrong-cohort-{nonce}"))
        self.assertEqual(result["decision"], "REJECTED")
        self.assertEqual(result["reason"], "WALLET_COHORT_DOES_NOT_MATCH_BLOCK")

    def test_maturity_tracks_only_metadata_and_late_resolution_cannot_create_membership(self) -> None:
        block = self.blocks[0]
        self.open()
        candidate = self.candidate(suffix="resolve")
        self.engine.admit_candidate(candidate)
        anchor = utc(candidate.observation.anchor_at)
        self.engine.record_resolution_metadata(
            ResolutionMetadata(candidate.observation.observation_id, iso(anchor + timedelta(seconds=5)), iso(anchor + timedelta(seconds=6))),
            recorded_at=iso(anchor + timedelta(seconds=7)),
        )
        self.seal()
        finalized = self.engine.finalize_maturity(block.block_id, as_of=iso(utc(block.sample_end) + timedelta(seconds=1)))
        self.assertEqual(finalized["state"], BlockState.FINALIZED.value)
        self.assertEqual(finalized["maturity"], {"ADMISSIBLE_OBSERVED": 1})
        late = self.engine.record_resolution_metadata(
            ResolutionMetadata(candidate.observation.observation_id, iso(anchor + timedelta(seconds=5)), iso(anchor + timedelta(seconds=140))),
            recorded_at=iso(anchor + timedelta(seconds=141)),
        )
        self.assertTrue(late["late"])
        self.assertEqual(self.engine.finalize_maturity(block.block_id, as_of=iso(utc(block.sample_end) + timedelta(seconds=2)))["maturity"], {"ADMISSIBLE_OBSERVED": 1})
        with self.assertRaises(ResolutionRefused):
            self.engine.record_resolution_metadata(
                ResolutionMetadata("not-a-member", None, None), recorded_at=block.sample_end,
            )
        self.assertFalse(hasattr(self.engine, "infer"))
        self.assertEqual(self.engine.status()["outcome_access"]["scientific_evaluation_reads"], 0)

    def test_recovery_does_not_reopen_expired_membership_and_integrity_replay_is_deterministic(self) -> None:
        self.open()
        self.engine.admit_candidate(self.candidate(suffix="recover"))
        block = self.blocks[0]
        first = self.engine.recover(at=iso(utc(block.sample_end) + timedelta(seconds=1)))
        second = self.engine.recover(at=iso(utc(block.sample_end) + timedelta(seconds=1)))
        self.assertEqual(first, {BlockState.AWAITING_RESOLUTION.value: 1})
        self.assertEqual(second, {})
        replay = self.engine.replay_hash()
        self.assertEqual(replay, PhaseE6Acquisition(self.database, FROZEN_PROTOCOL).replay_hash())
        audit = self.engine.integrity_audit()
        self.assertTrue(audit["ok"])
        self.assertEqual(audit["sqlite_quick_check"], "ok")
        self.assertEqual(audit["scientific_evaluation_reads"], 0)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
