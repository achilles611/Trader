from __future__ import annotations

from dataclasses import replace
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from src.l3f_provider.ninjatrader_observation import (
    L3F3_SCHEMA,
    NINJATRADER_ATTESTATION_PROTOCOL,
    NinjaTraderObservation,
    NinjaTraderProducerAttestation,
    expected_observer_source_fingerprint,
)
from src.lane_iii.contracts import canonical_hash
from src.lsc.codec import LSCContext, context_digest, encode_context, encode_observation
from src.lsc.commitment_log import (
    DecisionProvenanceReference,
    LSCCommitmentError,
    LSCCommitmentIntegrityError,
    ShadowCommitmentLog,
    ShadowIdentityContext,
    canonical_storage_identity,
)
from src.lsc.segment_store import SegmentStore


SESSION = "fedcba9876543210fedcba9876543210"
SOURCE_HASH = expected_observer_source_fingerprint()
BUILD_HASH = "2" * 64
PROTOCOL = NINJATRADER_ATTESTATION_PROTOCOL
BUILD_TIME = "2026-09-22T23:59:59.1234567Z"
NATIVE_CONTRACT = "MNQ DEC26"
CANONICAL_CONTRACT = "MNQZ6"


def context() -> LSCContext:
    return LSCContext(
        observation_schema=L3F3_SCHEMA,
        source="NINJATRADER",
        provider="LUCID_CQG",
        environment="PROP_SIM",
        producer_protocol_version=PROTOCOL,
        producer_source_fingerprint=SOURCE_HASH,
        producer_build_fingerprint=BUILD_HASH,
        producer_build_timestamp=BUILD_TIME,
        native_contract_id=NATIVE_CONTRACT,
        canonical_contract_id=CANONICAL_CONTRACT,
        tick_size="0.25",
        session_id=SESSION,
        generation=9,
        timestamp_origin="2026-09-23T00:00:00.0000000Z",
        legacy_mode=False,
    )


def observation(sequence: int) -> NinjaTraderObservation:
    payload = {
        "contract_id": NATIVE_CONTRACT,
        "bid": "20000.00",
        "ask": "20000.25",
        "bid_size": sequence,
        "ask_size": sequence + 1,
        "publication_policy": "EVENT_DRIVEN_EVERY_BID_ASK_CALLBACK_V1",
        "source_event_sequence": sequence + 1_000,
        "source_generation": 9,
    }
    producer = NinjaTraderProducerAttestation(
        protocol_version=PROTOCOL,
        source_fingerprint=SOURCE_HASH,
        build_fingerprint=BUILD_HASH,
        build_timestamp=BUILD_TIME,
        source_stream="MARKET_DATA_BID_ASK",
        publication_policy="EVENT_DRIVEN_EVERY_BID_ASK_CALLBACK_V1",
    )
    return NinjaTraderObservation(
        observation_id=f"nt-{SESSION}-{sequence}",
        session_id=SESSION,
        observation_type="QUOTE",
        ninja_receipt_time=f"2026-09-23T00:00:00.{sequence:07d}Z",
        local_monotonic_sequence=sequence,
        payload=payload,
        provider_timestamp=f"2026-09-23T00:00:00.{sequence:07d}Z",
        provider_sequence=sequence,
        schema=L3F3_SCHEMA,
        payload_hash=canonical_hash(payload),
        producer=producer,
        signature="a" * 64,
        signature_verified=True,
    )


def identity(store: SegmentStore, **changes: object) -> ShadowIdentityContext:
    frozen = encode_context(context())
    values: dict[str, object] = {
        "journal_id": "lsc-shadow-journal-test",
        "profile_id": "beelzebub_pong_discernment_v1",
        "context_digest": context_digest(frozen),
        "observation_schema": L3F3_SCHEMA,
        "source": "NINJATRADER",
        "provider": "LUCID_CQG",
        "environment": "PROP_SIM",
        "producer_protocol_version": PROTOCOL,
        "producer_source_fingerprint": SOURCE_HASH,
        "producer_build_fingerprint": BUILD_HASH,
        "producer_build_timestamp": BUILD_TIME,
        "native_contract_id": NATIVE_CONTRACT,
        "canonical_contract_id": CANONICAL_CONTRACT,
        "tick_size": "0.25",
        "session_id": SESSION,
        "generation": 9,
        "legacy_mode": False,
        "format_identity": "LSC-V1-LSCR-SEG01",
        "storage_identity": canonical_storage_identity(store),
    }
    values.update(changes)
    return ShadowIdentityContext(**values)  # type: ignore[arg-type]


def make_segments(root: Path, counts: tuple[int, ...] = (2, 1)) -> tuple[SegmentStore, tuple[object, ...]]:
    store = SegmentStore(root)
    encoded_context = encode_context(context())
    result: list[object] = []
    sequence = 1
    for count in counts:
        builder = store.new_segment(encoded_context)
        for _ in range(count):
            builder.append(encode_observation(observation(sequence), encoded_context))
            sequence += 1
        result.append(builder.seal())
    return store, tuple(result)


class CommitmentLogTests(unittest.TestCase):
    def test_exact_prefix_commitment_uncommitted_detection_and_provenance_resolution(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            store, segments = make_segments(Path(folder))
            first, second = segments
            log = ShadowCommitmentLog(
                store,
                identity(store),
                wall_clock=lambda: "2026-09-22T12:00:00.000000Z",
            )
            initial = log.recover()
            self.assertEqual(initial.committed_segment_ids, ())
            self.assertEqual(initial.uncommitted_segment_ids, (first.segment_id, second.segment_id))
            with self.assertRaisesRegex(LSCCommitmentError, "SKIP_OR_FORK"):
                log.commit_segment(second.segment_id)
            with self.assertRaisesRegex(LSCCommitmentError, "WITHOUT_VERIFIED_SEGMENT"):
                log.commit_segment("f" * 64)

            first_commitment = log.commit_segment(first.segment_id)
            self.assertEqual(first_commitment.segment_ordinal, 0)
            self.assertEqual((first_commitment.event_index_start, first_commitment.event_index_end), (0, 1))
            self.assertEqual((first_commitment.local_sequence_start, first_commitment.local_sequence_end), (1, 2))
            after_first = log.recover()
            self.assertEqual(after_first.committed_segment_ids, (first.segment_id,))
            self.assertEqual(after_first.uncommitted_segment_ids, (second.segment_id,))
            with self.assertRaisesRegex(LSCCommitmentError, "NOT_COMMITTED"):
                log.reference_for(second.segment_id, 0, 3)

            reference = log.reference_for(first.segment_id, 1, 2)
            self.assertEqual(
                set(reference.payload()),
                {"segment_id", "event_index", "local_sequence", "observation_id", "payload_hash", "event_digest"},
            )
            resolved = log.resolve_reference(reference)
            self.assertEqual(resolved.observation_id, reference.observation_id)
            self.assertEqual(resolved.payload_hash, reference.payload_hash)
            self.assertEqual(resolved.event_digest, reference.event_digest)
            with self.assertRaisesRegex(LSCCommitmentIntegrityError, "REFERENCE_MISMATCH"):
                log.resolve_reference(replace(reference, observation_id="different-observation"))
            with self.assertRaisesRegex(LSCCommitmentError, "OUTSIDE_COMMITMENT"):
                log.reference_for(first.segment_id, 2, 2)

            second_commitment = log.commit_segment(second.segment_id)
            self.assertEqual(second_commitment.previous_journal_hash, first_commitment.journal_hash)
            self.assertEqual(second_commitment.previous_segment_commitment, first.segment_commitment)
            self.assertEqual(log.recover().uncommitted_segment_ids, ())
            with self.assertRaisesRegex(LSCCommitmentError, "DUPLICATE"):
                log.commit_segment(first.segment_id)

    def test_cross_identity_duplicate_fork_and_previous_link_are_refused(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            store, segments = make_segments(Path(folder), (1,))
            segment = segments[0]
            wrong_build = ShadowCommitmentLog(
                store,
                identity(store, producer_build_fingerprint="9" * 64),
            )
            with self.assertRaisesRegex(LSCCommitmentIntegrityError, "SEGMENT_CROSS_IDENTITY"):
                wrong_build.commit_segment(segment.segment_id)
            log = ShadowCommitmentLog(store, identity(store))
            record = log.commit_segment(segment.segment_id)
            alien = ShadowCommitmentLog(store, identity(store, journal_id="different-journal"), root=log.root)
            with self.assertRaisesRegex(LSCCommitmentIntegrityError, "CROSS_IDENTITY"):
                alien.verify()

            original = record.path.read_bytes()
            fork = record.path.with_name(f"{1:020d}-{'a' * 64}.json")
            fork.write_bytes(original)
            with self.assertRaisesRegex(LSCCommitmentIntegrityError, "DUPLICATE_FORK_OR_SEQUENCE|HASH"):
                log.verify()
            fork.unlink()

            value = json.loads(original)
            value["previous_journal_hash"] = "1" * 64
            record.path.write_bytes(json.dumps(value, sort_keys=True, separators=(",", ":")).encode())
            with self.assertRaisesRegex(LSCCommitmentIntegrityError, "PREVIOUS_LINK"):
                log.verify()

    def test_storage_identity_uses_resolved_destination_and_refuses_cross_root(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            base = Path(folder)
            actual = base / "actual"
            alias_component = actual / "path-alias"
            alias_component.mkdir(parents=True)
            direct_store = SegmentStore(actual)
            alias_store = SegmentStore(alias_component / "..")
            self.assertEqual(
                canonical_storage_identity(alias_store),
                canonical_storage_identity(direct_store),
            )
            aliased_log = ShadowCommitmentLog(alias_store, identity(direct_store))
            self.assertEqual(aliased_log.identity.storage_identity, canonical_storage_identity(alias_store))

            other_store = SegmentStore(base / "other")
            refused_root = direct_store.root / "refused-shadow-commitments"
            with self.assertRaisesRegex(LSCCommitmentError, "STORAGE_IDENTITY_DESTINATION_MISMATCH"):
                ShadowCommitmentLog(direct_store, identity(other_store), root=refused_root)
            self.assertFalse(refused_root.exists())

            custom_root = base / "custom-shadow-commitments"
            with self.assertRaisesRegex(LSCCommitmentError, "STORAGE_IDENTITY_DESTINATION_MISMATCH"):
                ShadowCommitmentLog(direct_store, identity(direct_store), root=custom_root)
            self.assertFalse(custom_root.exists())
            custom_identity = identity(
                direct_store,
                storage_identity=canonical_storage_identity(direct_store, custom_root),
            )
            custom_log = ShadowCommitmentLog(direct_store, custom_identity, root=custom_root)
            self.assertTrue(custom_log.root.is_dir())

    def test_storage_destination_substitution_blocks_verify_and_recovery_before_mutation(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            base = Path(folder)
            store, segments = make_segments(base / "original", (1,))
            log = ShadowCommitmentLog(store, identity(store))
            log.commit_segment(segments[0].segment_id)
            pending = log.root / "incomplete.tmp"
            pending.write_bytes(b"not a commitment")
            replacement = SegmentStore(base / "replacement")
            store.root = replacement.root
            store.segment_root = replacement.segment_root

            with self.assertRaisesRegex(LSCCommitmentIntegrityError, "STORAGE_IDENTITY_DESTINATION_MISMATCH"):
                log.verify()
            with self.assertRaisesRegex(LSCCommitmentIntegrityError, "STORAGE_IDENTITY_DESTINATION_MISMATCH"):
                log.recover()
            self.assertTrue(pending.is_file())
            self.assertEqual(list(log.quarantine_root.iterdir()), [])

            store.root = base / "original"
            store.segment_root = store.root / "segments"
            substituted_journal = base / "substituted-journal"
            substituted_journal.mkdir()
            log.root = substituted_journal
            with self.assertRaisesRegex(LSCCommitmentIntegrityError, "STORAGE_IDENTITY_DESTINATION_MISMATCH"):
                log.verify()

    def test_commitment_without_segment_is_refused_on_subsequent_verification(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            store, segments = make_segments(Path(folder), (1,))
            segment = segments[0]
            log = ShadowCommitmentLog(store, identity(store))
            log.commit_segment(segment.segment_id)
            segment.path.unlink()
            with self.assertRaisesRegex(LSCCommitmentIntegrityError, "WITHOUT_VERIFIED_SEGMENT"):
                log.verify()

    def test_commitment_crash_boundaries_distinguish_uncommitted_from_durable(self) -> None:
        stages = (
            "after_temp_create", "after_commitment_write", "before_file_fsync", "after_file_fsync",
            "before_publish", "after_publish", "after_directory_sync",
        )
        for stage in stages:
            with self.subTest(stage=stage), tempfile.TemporaryDirectory() as folder:
                store, segments = make_segments(Path(folder), (1,))
                segment = segments[0]
                log = ShadowCommitmentLog(store, identity(store))

                def crash(current: str) -> None:
                    if current == stage:
                        raise RuntimeError(f"injected:{stage}")

                with self.assertRaisesRegex(RuntimeError, "injected"):
                    log.commit_segment(segment.segment_id, fault_hook=crash)
                report = log.recover()
                if stage in {"after_publish", "after_directory_sync"}:
                    self.assertEqual(report.committed_segment_ids, (segment.segment_id,))
                    self.assertEqual(report.uncommitted_segment_ids, ())
                    self.assertEqual(report.quarantined, ())
                else:
                    self.assertEqual(report.committed_segment_ids, ())
                    self.assertEqual(report.uncommitted_segment_ids, (segment.segment_id,))
                    self.assertEqual(len(report.quarantined), 1)
                    self.assertTrue(report.quarantined[0].quarantine_path.is_file())

    def test_reference_refuses_wrong_sequence_hashes_and_uncommitted_segment(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            store, segments = make_segments(Path(folder))
            first, second = segments
            log = ShadowCommitmentLog(store, identity(store))
            log.commit_segment(first.segment_id)
            reference = log.reference_for(first.segment_id, 0, 1)
            cases = (
                replace(reference, local_sequence=2),
                replace(reference, payload_hash="0" * 64),
                replace(reference, event_digest="0" * 64),
            )
            for candidate in cases:
                with self.subTest(candidate=candidate), self.assertRaises((LSCCommitmentError, LSCCommitmentIntegrityError)):
                    log.resolve_reference(candidate)
            uncommitted = DecisionProvenanceReference(
                segment_id=second.segment_id,
                event_index=0,
                local_sequence=3,
                observation_id=f"nt-{SESSION}-3",
                payload_hash="0" * 64,
                event_digest="0" * 64,
            )
            with self.assertRaisesRegex(LSCCommitmentError, "NOT_COMMITTED"):
                log.resolve_reference(uncommitted)

    def test_legacy_epoch_and_sidecars_are_byte_for_byte_untouched(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            legacy_root = root / "legacy-epoch"
            legacy_root.mkdir()
            legacy_files = {
                legacy_root / "lane_iii_paper.sqlite3": b"immutable legacy epoch image",
                legacy_root / "lane_iii_paper.sqlite3-wal": b"immutable legacy wal",
                legacy_root / "lane_iii_paper.sqlite3-shm": b"immutable legacy shm",
            }
            for path, data in legacy_files.items():
                path.write_bytes(data)
            before = {
                path: (path.read_bytes(), path.stat().st_size, path.stat().st_mtime_ns, hashlib.sha256(path.read_bytes()).hexdigest())
                for path in legacy_files
            }

            store, segments = make_segments(root / "lsc", (2,))
            log = ShadowCommitmentLog(store, identity(store))
            log.commit_segment(segments[0].segment_id)
            reference = log.reference_for(segments[0].segment_id, 1, 2)
            log.resolve_reference(reference)
            log.recover()

            after = {
                path: (path.read_bytes(), path.stat().st_size, path.stat().st_mtime_ns, hashlib.sha256(path.read_bytes()).hexdigest())
                for path in legacy_files
            }
            self.assertEqual(after, before)

    def test_identity_context_is_bounded_and_control_character_free(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            store = SegmentStore(Path(folder))
            with self.assertRaises(ValueError):
                identity(store, journal_id="")
            with self.assertRaises(ValueError):
                identity(store, journal_id=" shadow ")
            with self.assertRaises(ValueError):
                identity(store, journal_id="shadow\nother")
            with self.assertRaises(ValueError):
                identity(store, journal_id="x" * 513)
            with self.assertRaisesRegex(ValueError, "canonical LSC storage identity"):
                identity(store, storage_identity="caller-label")
            with self.assertRaisesRegex(LSCCommitmentError, "ROOT_MUST_BE_DISTINCT"):
                ShadowCommitmentLog(store, identity(store), root=store.segment_root)
            self.assertFalse((store.segment_root / "quarantine").exists())


if __name__ == "__main__":
    unittest.main()
