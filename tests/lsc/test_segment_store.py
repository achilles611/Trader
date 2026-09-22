from __future__ import annotations

from dataclasses import dataclass, replace
import hashlib
import json
import os
from pathlib import Path
import shutil
import tempfile
import unittest
import zlib

from src.lsc.segment_store import (
    GENESIS_COMMITMENT,
    LSCStoreError,
    LSCStoreIntegrityError,
    LSCStoreLimitError,
    SegmentLimits,
    SegmentStore,
    VerifiedRecordMetadata,
    _atomic_write,
    _bounded_decompress,
)


def canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")


def fake_context_digest(value: bytes) -> str:
    if not value.startswith(b"CTX:"):
        raise ValueError("bad context")
    return hashlib.sha256(b"fake-context\0" + value).hexdigest()


@dataclass(frozen=True)
class FakeRecord:
    record_bytes: bytes
    observation_type: str
    observation_id: str
    local_sequence: int
    payload_hash: str
    wire_hash: str
    event_digest: str
    context_digest: str


def fake_record(context: bytes, sequence: int, kind: str = "QUOTE", *, size: int = 0) -> FakeRecord:
    context_hash = fake_context_digest(context)
    payload_hash = hashlib.sha256(f"payload-{sequence}".encode()).hexdigest()
    wire_hash = hashlib.sha256(f"wire-{sequence}".encode()).hexdigest()
    core = canonical({
        "context_digest": context_hash,
        "filler": "x" * size,
        "local_sequence": sequence,
        "observation_id": f"observation-{sequence}",
        "observation_type": kind,
        "payload_hash": payload_hash,
        "wire_hash": wire_hash,
    })
    event_digest = hashlib.sha256(b"fake-event\0" + core).hexdigest()
    return FakeRecord(
        record_bytes=core + bytes.fromhex(event_digest),
        observation_type=kind,
        observation_id=f"observation-{sequence}",
        local_sequence=sequence,
        payload_hash=payload_hash,
        wire_hash=wire_hash,
        event_digest=event_digest,
        context_digest=context_hash,
    )


def verify_fake_record(record_bytes: bytes, context: bytes) -> VerifiedRecordMetadata:
    if len(record_bytes) < 33:
        raise ValueError("short record")
    core, supplied = record_bytes[:-32], record_bytes[-32:].hex()
    if hashlib.sha256(b"fake-event\0" + core).hexdigest() != supplied:
        raise ValueError("event digest")
    value = json.loads(core)
    if value["context_digest"] != fake_context_digest(context):
        raise ValueError("context")
    return VerifiedRecordMetadata(
        observation_id=value["observation_id"],
        observation_type=value["observation_type"],
        local_sequence=value["local_sequence"],
        payload_hash=value["payload_hash"],
        wire_hash=value["wire_hash"],
        event_digest=supplied,
        context_digest=value["context_digest"],
    )


class Clock:
    def __init__(self) -> None:
        self.value = 0.0

    def __call__(self) -> float:
        return self.value


class SegmentStoreTests(unittest.TestCase):
    def store(
        self,
        root: Path,
        *,
        limits: SegmentLimits | None = None,
        clock: Clock | None = None,
    ) -> SegmentStore:
        return SegmentStore(
            root,
            limits=limits,
            context_digest_verifier=fake_context_digest,
            record_verifier=verify_fake_record,
            monotonic=clock or Clock(),
            wall_clock=lambda: "2026-09-22T00:00:00.000000Z",
        )

    def test_seal_lookup_and_segment_chain_are_exact(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            store = self.store(Path(folder))
            context = b"CTX:alpha"
            first_builder = store.new_segment(context)
            records = [fake_record(context, index, kind) for index, kind in enumerate(("QUOTE", "TRADE", "DEPTH"), 1)]
            for record in records:
                first_builder.append(record)
            first = first_builder.seal()
            self.assertEqual(first.previous_commitment, GENESIS_COMMITMENT)
            self.assertEqual(first.record_count, 3)
            self.assertIn("write_through", first.durability_method.lower()) if os.name == "nt" else self.assertIn("directory_fsync", first.durability_method)
            event = store.lookup(first.segment_id, 1, 2)
            self.assertEqual(event.record_bytes, records[1].record_bytes)
            self.assertEqual(event.observation_id, records[1].observation_id)
            with self.assertRaisesRegex(LSCStoreIntegrityError, "LOCAL_SEQUENCE"):
                store.lookup(first.segment_id, 1, 999)

            second_builder = store.new_segment(context, previous_commitment=first.segment_commitment)
            second_builder.append(fake_record(context, 4))
            second = second_builder.seal()
            self.assertEqual(second.previous_commitment, first.segment_commitment)
            self.assertEqual([item.segment_id for item in store.verify_chain()], [first.segment_id, second.segment_id])

    def test_passive_context_identity_and_bounds_fail_closed(self) -> None:
        limits = SegmentLimits(
            max_records=2,
            max_record_bytes=700,
            max_context_bytes=32,
            max_raw_body_bytes=1_200,
            max_compressed_body_bytes=2_000,
            max_segment_age_seconds=1.0,
        )
        with tempfile.TemporaryDirectory() as folder:
            clock = Clock()
            store = self.store(Path(folder), limits=limits, clock=clock)
            with self.assertRaisesRegex(LSCStoreLimitError, "CONTEXT"):
                store.new_segment(b"CTX:" + b"x" * 40)
            builder = store.new_segment(b"CTX:a")
            with self.assertRaisesRegex(LSCStoreError, "NON_PASSIVE"):
                builder.append(fake_record(b"CTX:a", 1, "ORDER"))
            with self.assertRaisesRegex(LSCStoreError, "CONTEXT_MISMATCH"):
                builder.append(fake_record(b"CTX:b", 1))
            too_large = fake_record(b"CTX:a", 1, size=1_000)
            with self.assertRaisesRegex(LSCStoreLimitError, "ENCODED_RECORD"):
                builder.append(too_large)
            first = fake_record(b"CTX:a", 1)
            builder.append(first)
            with self.assertRaisesRegex(LSCStoreError, "NON_MONOTONIC|DUPLICATE_OBSERVATION"):
                builder.append(first)
            duplicate_sequence = replace(fake_record(b"CTX:a", 3), local_sequence=2)
            with self.assertRaisesRegex(LSCStoreIntegrityError, "METADATA_MISMATCH"):
                builder.append(duplicate_sequence)
            builder.append(fake_record(b"CTX:a", 2))
            with self.assertRaisesRegex(LSCStoreLimitError, "RECORD_LIMIT"):
                builder.append(fake_record(b"CTX:a", 3))

            aging = store.new_segment(b"CTX:a")
            clock.value = 2.0
            with self.assertRaisesRegex(LSCStoreLimitError, "AGE_LIMIT"):
                aging.append(fake_record(b"CTX:a", 10))

    def test_default_codec_boundary_reverifies_context_record_and_lookup(self) -> None:
        from src.l3f_provider.ninjatrader_observation import NinjaTraderObservation
        from src.lsc.codec import LSCContext, encode_context, encode_observation

        session = "0123456789abcdef0123456789abcdef"
        context = LSCContext(
            source="NINJATRADER",
            provider="LUCID_CQG",
            environment="PROP_SIM",
            source_identity="source-" + "1" * 64,
            build_identity="build-" + "2" * 64,
            contract_identity="MNQZ6-CME-2026-12",
            contract_id="MNQ DEC26-MNQZ6",
            tick_size="0.25",
            session_id=session,
            generation=7,
            timestamp_origin="2026-09-23T00:00:00.0000000Z",
        )
        observation = NinjaTraderObservation(
            observation_id=f"nt-{session}-1",
            session_id=session,
            observation_type="QUOTE",
            ninja_receipt_time="2026-09-23T00:00:00.0000001Z",
            local_monotonic_sequence=1,
            payload={
                "contract_id": "MNQ DEC26-MNQZ6",
                "bid": "20000.00",
                "ask": "20000.25",
                "bid_size": 2,
                "ask_size": 3,
            },
            provider_timestamp="2026-09-23T00:00:00.0000000Z",
            provider_sequence=1,
        )
        encoded_context = encode_context(context)
        encoded_record = encode_observation(observation, encoded_context)
        with tempfile.TemporaryDirectory() as folder:
            store = SegmentStore(Path(folder), monotonic=Clock())
            builder = store.new_segment(encoded_context)
            builder.append(encoded_record)
            segment = builder.seal()
            event = store.lookup(segment.segment_id, 0, 1)
            self.assertEqual(event.record_bytes, encoded_record.record_bytes)
            self.assertEqual(event.event_digest, encoded_record.event_digest)

    def test_record_metadata_and_bytes_are_verified_before_acceptance(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            store = self.store(Path(folder))
            context = b"CTX:verified"
            original = fake_record(context, 1)
            builder = store.new_segment(context)
            with self.assertRaisesRegex(LSCStoreIntegrityError, "METADATA_MISMATCH"):
                builder.append(replace(original, wire_hash="0" * 64))
            corrupted = bytearray(original.record_bytes)
            corrupted[0] ^= 1
            with self.assertRaisesRegex(LSCStoreIntegrityError, "VERIFICATION_FAILED"):
                builder.append(replace(original, record_bytes=bytes(corrupted)))

    def test_segment_fault_boundaries_quarantine_before_publish_and_recover_after(self) -> None:
        stages = (
            "after_temp_create", "after_prefix_write", "after_header_write", "after_context_write",
            "after_body_write", "after_index_write", "after_footer_write", "before_file_fsync",
            "after_file_fsync", "before_publish", "after_publish", "after_directory_sync",
        )
        for stage in stages:
            with self.subTest(stage=stage), tempfile.TemporaryDirectory() as folder:
                store = self.store(Path(folder))
                builder = store.new_segment(b"CTX:crash")
                builder.append(fake_record(b"CTX:crash", 1))

                def crash(current: str) -> None:
                    if current == stage:
                        raise RuntimeError(f"injected:{stage}")

                with self.assertRaisesRegex(RuntimeError, "injected"):
                    builder.seal(fault_hook=crash)
                report = store.recover()
                if stage in {"after_publish", "after_directory_sync"}:
                    self.assertEqual(len(report.sealed_segment_ids), 1)
                    self.assertEqual(report.quarantined, ())
                else:
                    self.assertEqual(report.sealed_segment_ids, ())
                    self.assertEqual(len(report.quarantined), 1)
                    self.assertTrue(report.quarantined[0].quarantine_path.is_file())

    def test_complete_temp_never_masquerades_as_sealed(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            store = self.store(Path(folder))
            builder = store.new_segment(b"CTX:temp")
            builder.append(fake_record(b"CTX:temp", 1))
            sealed = builder.seal()
            temporary = store.segment_root / ".complete-but-temporary.tmp"
            shutil.copyfile(sealed.path, temporary)
            report = store.recover()
            self.assertEqual(report.sealed_segment_ids, (sealed.segment_id,))
            self.assertEqual(len(report.quarantined), 1)
            self.assertNotEqual(report.quarantined[0].sha256, None)
            self.assertFalse(temporary.exists())

    def test_mutation_unknown_version_substitution_and_chain_gap_are_refused(self) -> None:
        def one(root: Path, context: bytes, sequence: int = 1) -> tuple[SegmentStore, Path]:
            store = self.store(root)
            builder = store.new_segment(context)
            builder.append(fake_record(context, sequence))
            return store, builder.seal().path

        with tempfile.TemporaryDirectory() as folder:
            store, path = one(Path(folder), b"CTX:mutate")
            data = bytearray(path.read_bytes()); data[-1] ^= 1; path.write_bytes(data)
            with self.assertRaises(LSCStoreIntegrityError):
                store.verify_chain()
        with tempfile.TemporaryDirectory() as folder:
            store, path = one(Path(folder), b"CTX:version")
            data = bytearray(path.read_bytes()); data[8:10] = (999).to_bytes(2, "little"); path.write_bytes(data)
            with self.assertRaisesRegex(LSCStoreIntegrityError, "VERSION"):
                store.verify_chain()
        with tempfile.TemporaryDirectory() as folder:
            store = self.store(Path(folder)); context = b"CTX:gap"
            first_builder = store.new_segment(context); first_builder.append(fake_record(context, 1)); first = first_builder.seal()
            second_builder = store.new_segment(context); second_builder.append(fake_record(context, 2)); second = second_builder.seal()
            gap = second.path.with_name(second.path.name.replace("00000000000000000001", "00000000000000000002"))
            second.path.rename(gap)
            with self.assertRaisesRegex(LSCStoreIntegrityError, "ORDINAL|CHAIN"):
                store.verify_chain()
            self.assertTrue(first.path.is_file())

    def test_decompression_is_bounded_and_refuses_trailing_or_truncated_streams(self) -> None:
        raw = b"z" * 1_000
        compressed = zlib.compress(raw)
        self.assertEqual(_bounded_decompress(compressed, 1_000), raw)
        with self.assertRaisesRegex(LSCStoreLimitError, "LIMIT"):
            _bounded_decompress(compressed, 999)
        with self.assertRaisesRegex(LSCStoreIntegrityError, "TRAILING_OR_TRUNCATED"):
            _bounded_decompress(compressed + b"junk", 2_000)
        with self.assertRaises(LSCStoreIntegrityError):
            _bounded_decompress(compressed[:-2], 2_000)

    def test_atomic_publication_never_overwrites_existing_destination(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            destination = Path(folder) / "existing"
            destination.write_bytes(b"original")
            with self.assertRaises(FileExistsError):
                _atomic_write(
                    destination,
                    (("after_payload", b"replacement"),),
                    temp_label="collision",
                )
            self.assertEqual(destination.read_bytes(), b"original")
            self.assertEqual(len(list(Path(folder).glob("*.tmp"))), 1)


if __name__ == "__main__":
    unittest.main()
