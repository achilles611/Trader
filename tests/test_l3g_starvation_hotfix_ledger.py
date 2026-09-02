from __future__ import annotations

from contextlib import closing
import json
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory
import threading
import unittest

from src.lane_iii.contracts import canonical_hash
from src.l3g_paper.ledger import (
    COMMISSIONING_NO_AUTHORITY_EFFECT,
    COMMISSIONING_READINESS_RECORD_SEMANTICS,
    COMMISSIONING_READINESS_RECORD_SEMANTICS_VERSION,
    COMMISSIONING_WARMUP_POLICY_HASH,
    COMMISSIONING_WARMUP_REQUIRED_FAMILIES,
    PaperLedger,
)
from src.l3g_paper.sessions import UNSPECIFIED_OFF_SESSION_CONTEXT


NOW = "2026-08-28T17:30:00Z"


def observation(number: int, label: str) -> dict[str, object]:
    return {
        **UNSPECIFIED_OFF_SESSION_CONTEXT.payload(),
        "observation_id": f"nt-starvation-{label}",
        "observation_type": "QUOTE",
        "observed_at": NOW,
        "ninja_receipt_time": NOW,
        "provider_timestamp": None,
        "exchange_timestamp": NOW,
        "local_monotonic_sequence": number,
        "source_payload_hash": canonical_hash({"number": number, "label": label}),
    }


def readiness_attestation(kind: str) -> dict[str, object]:
    common = {
        **UNSPECIFIED_OFF_SESSION_CONTEXT.payload(),
        "authority_effect": COMMISSIONING_NO_AUTHORITY_EFFECT,
        "record_semantics": COMMISSIONING_READINESS_RECORD_SEMANTICS,
        "record_semantics_version": COMMISSIONING_READINESS_RECORD_SEMANTICS_VERSION,
        "policy_hash": COMMISSIONING_WARMUP_POLICY_HASH,
        "required_families": list(COMMISSIONING_WARMUP_REQUIRED_FAMILIES),
    }
    if kind == "COMMISSIONING_SESSION_WARMUP_RESET":
        return {
            **common,
            "commissioning_warmup_state": "NOT_WARMED",
            "reset_at": NOW,
            "reason": "LOCAL_SEQUENCE_GAP",
            "seen_families": ["STRUCTURAL_CONTEXT"],
            "warmed_at": NOW,
        }
    if kind == "COMMISSIONING_SESSION_WARMED":
        return {
            **common,
            "commissioning_warmup_state": "WARMED",
            "warmed_at": NOW,
            "reason": "ALL_REQUIRED_FAMILIES_GENUINELY_OBSERVED",
            "evidence_provenance": {
                family: {
                    "evidence_id": f"l3g-pe-{index:032x}",
                    "observed_at": NOW,
                    "source_observation_ids": [f"nt-attestation-{index}"],
                    "source_local_sequences": [index],
                }
                for index, family in enumerate(
                    COMMISSIONING_WARMUP_REQUIRED_FAMILIES, start=1,
                )
            },
        }
    raise ValueError(kind)


class LedgerBarrierStarvationHotfixTests(unittest.TestCase):
    timeout_seconds = 5.0

    def wait_for_queued_barrier(self, ledger: PaperLedger) -> None:
        with ledger._deferred_condition:
            self.assertTrue(
                ledger._deferred_condition.wait_for(
                    lambda: ledger._deferred_barrier_count == 1,
                    timeout=self.timeout_seconds,
                ),
                "Commissioning barrier was not admitted behind the active prefix.",
            )

    @staticmethod
    def identities(path: Path) -> list[str]:
        with closing(sqlite3.connect(path)) as connection:
            return [
                str(row[0])
                for row in connection.execute(
                    "SELECT identity FROM lane_iii_paper_audit ORDER BY ledger_sequence"
                ).fetchall()
            ]

    def test_deferred_readiness_attestation_whitelist_is_exact_and_fail_closed(self) -> None:
        with TemporaryDirectory() as directory:
            ledger = PaperLedger(Path(directory) / "paper.sqlite3")
            reset = readiness_attestation("COMMISSIONING_SESSION_WARMUP_RESET")
            warmed = readiness_attestation("COMMISSIONING_SESSION_WARMED")
            try:
                ledger.append(
                    "SESSION_AUTHORITY", {"reason": "attestation whitelist anchor"},
                    identity="verified-anchor", occurred_at=NOW,
                )
                anchor = int(ledger.health_status()["highest_sequence"])

                with self.assertRaises(ValueError):
                    ledger.append_deferred(
                        "COMMISSIONING_SESSION_WARMUP_RESET", reset,
                        identity="generic-incident-deferral-must-fail", occurred_at=NOW,
                    )
                with self.assertRaises(ValueError):
                    ledger.append_commissioning_attestation_deferred(
                        "INCIDENT_SAFETY_EVENT", reset,
                        identity="arbitrary-incident-must-fail", occurred_at=NOW,
                    )
                malformed = dict(reset)
                malformed.pop("authority_effect")
                with self.assertRaises(ValueError):
                    ledger.append_commissioning_attestation_deferred(
                        "COMMISSIONING_SESSION_WARMUP_RESET", malformed,
                        identity="malformed-attestation-must-fail", occurred_at=NOW,
                    )

                ledger.append_commissioning_attestation_deferred(
                    "COMMISSIONING_SESSION_WARMUP_RESET", reset,
                    identity="exact-reset", occurred_at=NOW,
                )
                ledger.append_commissioning_attestation_deferred(
                    "COMMISSIONING_SESSION_WARMED", warmed,
                    identity="exact-warmed", occurred_at=NOW,
                )
                snapshot = ledger.commissioning_tail_snapshot(
                    anchor, last_full_verified_sequence=anchor,
                )

                self.assertEqual(snapshot["arm_snapshot_tip"], anchor + 2)
                self.assertEqual(snapshot["deferred_barrier_ledger_sequence"], anchor + 2)
                self.assertEqual(snapshot["last_authority_observation_kind"], "COMMISSIONING_SESSION_WARMED")
                self.assertEqual(
                    snapshot["tail_record_categories"], ["AUTHORITY_OBSERVATION"],
                )
                self.assertEqual(
                    self.identities(ledger.path),
                    ["verified-anchor", "exact-reset", "exact-warmed"],
                )
                self.assertEqual(ledger.verify_chain(), (True, None))
            finally:
                ledger.close()

    def test_deferred_readiness_attestation_is_a_deep_immutable_snapshot(self) -> None:
        with TemporaryDirectory() as directory:
            path = Path(directory) / "paper.sqlite3"
            ledger = PaperLedger(path, persist_high_frequency_records=True)
            prefix_release = threading.Event()
            prefix_started = threading.Event()
            original_append_prepared = ledger._append_prepared

            try:
                ledger.append(
                    "SESSION_AUTHORITY", {"reason": "immutable attestation anchor"},
                    identity="verified-anchor", occurred_at=NOW,
                )
                anchor = int(ledger.health_status()["highest_sequence"])

                def gated_append(records: tuple[dict[str, object], ...]) -> list[str]:
                    if any(str(record["identity"]) == "blocked-prefix" for record in records):
                        prefix_started.set()
                        if not prefix_release.wait(self.timeout_seconds):
                            raise AssertionError("Timed out releasing the immutable-snapshot prefix.")
                    return original_append_prepared(records)

                ledger._append_prepared = gated_append  # type: ignore[method-assign]
                ledger.append_deferred(
                    "OBSERVATION_ENVELOPE", observation(1, "blocked-prefix"),
                    identity="blocked-prefix", occurred_at=NOW,
                )
                self.assertTrue(prefix_started.wait(self.timeout_seconds))

                warmed = readiness_attestation("COMMISSIONING_SESSION_WARMED")
                expected_payload_hash = canonical_hash(warmed)
                ledger.append_commissioning_attestation_deferred(
                    "COMMISSIONING_SESSION_WARMED", warmed, occurred_at=NOW,
                )

                # Retain and mutate every nested container after admission but
                # before the blocked writer can serialize the attestation.
                required_families = warmed["required_families"]
                self.assertIsInstance(required_families, list)
                required_families.append("CALLER_MUTATION")
                evidence_provenance = warmed["evidence_provenance"]
                self.assertIsInstance(evidence_provenance, dict)
                first_family = COMMISSIONING_WARMUP_REQUIRED_FAMILIES[0]
                first_provenance = evidence_provenance[first_family]
                self.assertIsInstance(first_provenance, dict)
                source_ids = first_provenance["source_observation_ids"]
                self.assertIsInstance(source_ids, list)
                source_ids.append("nt-post-admission-mutation")
                first_provenance["secret"] = "must-never-enter-the-ledger"

                prefix_release.set()
                ledger.flush_deferred()

                with closing(sqlite3.connect(path)) as connection:
                    identity, serialized = connection.execute(
                        "SELECT identity, payload_json FROM lane_iii_paper_audit "
                        "WHERE kind = 'COMMISSIONING_SESSION_WARMED'",
                    ).fetchone()
                stored = json.loads(str(serialized))
                stored_payload = stored["payload"]
                self.assertEqual(canonical_hash(stored_payload), expected_payload_hash)
                self.assertNotIn("CALLER_MUTATION", stored_payload["required_families"])
                self.assertNotIn("nt-post-admission-mutation", json.dumps(stored_payload))
                self.assertNotIn("must-never-enter-the-ledger", json.dumps(stored_payload))

                common = {
                    key: value for key, value in stored.items()
                    if key not in {"identity", "previous_record_hash", "record_hash"}
                }
                self.assertEqual(identity, "l3g-ledger-" + canonical_hash(common))
                snapshot = ledger.commissioning_tail_snapshot(
                    anchor, last_full_verified_sequence=anchor,
                )
                self.assertEqual(snapshot["last_authority_observation_kind"], "COMMISSIONING_SESSION_WARMED")
                self.assertEqual(
                    snapshot["tail_record_categories"],
                    ["PASSIVE_DATA", "AUTHORITY_OBSERVATION"],
                )
                self.assertEqual(ledger.verify_chain(), (True, None))
            finally:
                prefix_release.set()
                ledger._append_prepared = original_append_prepared  # type: ignore[method-assign]
                ledger.close()

    def test_prefix_barrier_completes_without_draining_blocked_suffix(self) -> None:
        with TemporaryDirectory() as directory:
            path = Path(directory) / "paper.sqlite3"
            ledger = PaperLedger(path, persist_high_frequency_records=True)
            prefix_release = threading.Event()
            suffix_release = threading.Event()
            prefix_started = threading.Event()
            suffix_started = threading.Event()
            snapshot_done = threading.Event()
            snapshot: dict[str, object] = {}
            snapshot_errors: list[BaseException] = []
            snapshot_thread: threading.Thread | None = None
            original_append_prepared = ledger._append_prepared

            try:
                ledger.append(
                    "SESSION_AUTHORITY",
                    {"reason": "verified starvation-hotfix anchor"},
                    identity="verified-anchor",
                    occurred_at=NOW,
                )
                anchor = int(ledger.health_status()["highest_sequence"])

                def gated_append(records: tuple[dict[str, object], ...]) -> list[str]:
                    record_identities = {str(record["identity"]) for record in records}
                    if "prefix-1" in record_identities:
                        prefix_started.set()
                        if not prefix_release.wait(self.timeout_seconds):
                            raise AssertionError("Timed out releasing the deferred prefix batch.")
                    if {"suffix-1", "suffix-2"} & record_identities:
                        suffix_started.set()
                        if not suffix_release.wait(self.timeout_seconds):
                            raise AssertionError("Timed out releasing the deferred suffix batch.")
                    return original_append_prepared(records)

                ledger._append_prepared = gated_append  # type: ignore[method-assign]
                ledger.append_deferred(
                    "OBSERVATION_ENVELOPE", observation(1, "prefix-1"),
                    identity="prefix-1", occurred_at=NOW,
                )
                self.assertTrue(prefix_started.wait(self.timeout_seconds))

                # The active identity remains admitted until its batch commits;
                # an exact duplicate must not create another queue entry or row.
                ledger.append_deferred(
                    "OBSERVATION_ENVELOPE", observation(1, "prefix-1"),
                    identity="prefix-1", occurred_at=NOW,
                )
                ledger.append_deferred(
                    "OBSERVATION_ENVELOPE", observation(2, "prefix-2"),
                    identity="prefix-2", occurred_at=NOW,
                )

                def capture_snapshot() -> None:
                    try:
                        snapshot.update(ledger.commissioning_tail_snapshot(
                            anchor, last_full_verified_sequence=anchor,
                        ))
                    except BaseException as error:
                        snapshot_errors.append(error)
                    finally:
                        snapshot_done.set()

                snapshot_thread = threading.Thread(
                    target=capture_snapshot,
                    name="LedgerBarrierSnapshotTest",
                )
                snapshot_thread.start()
                self.wait_for_queued_barrier(ledger)

                # Both records are admitted after the fence while the prefix
                # writer is still blocked. They must not enter its captured tip.
                ledger.append_deferred(
                    "OBSERVATION_ENVELOPE", observation(3, "suffix-1"),
                    identity="suffix-1", occurred_at=NOW,
                )
                ledger.append_deferred(
                    "OBSERVATION_ENVELOPE", observation(4, "suffix-2"),
                    identity="suffix-2", occurred_at=NOW,
                )

                prefix_release.set()
                self.assertTrue(suffix_started.wait(self.timeout_seconds))
                self.assertTrue(
                    snapshot_done.wait(self.timeout_seconds),
                    "Snapshot waited for a post-barrier suffix batch.",
                )
                self.assertEqual(snapshot_errors, [])
                self.assertEqual(snapshot["arm_snapshot_tip"], anchor + 2)
                self.assertEqual(snapshot["deferred_barrier_ledger_sequence"], anchor + 2)
                self.assertEqual(snapshot["unverified_tail_rows"], 2)
                self.assertEqual(
                    self.identities(path),
                    ["verified-anchor", "prefix-1", "prefix-2"],
                )

                suffix_release.set()
                snapshot_thread.join(self.timeout_seconds)
                self.assertFalse(snapshot_thread.is_alive())
                ledger.flush_deferred()

                expected = [
                    "verified-anchor", "prefix-1", "prefix-2", "suffix-1", "suffix-2",
                ]
                persisted = self.identities(path)
                self.assertEqual(persisted, expected)
                self.assertEqual(len(persisted), len(set(persisted)))
                self.assertEqual(ledger.verify_chain(), (True, None))

                health = ledger.health_status()
                self.assertEqual(health["deferred_queue_depth"], 0)
                self.assertFalse(health["deferred_writer_active"])
                self.assertGreaterEqual(int(health["deferred_queue_high_water"]), 3)
                self.assertIsNone(health["deferred_writer_error"])
                self.assertEqual(
                    health["last_deferred_barrier_token"], snapshot["deferred_barrier_token"],
                )
                self.assertEqual(
                    health["last_deferred_barrier_ledger_sequence"], snapshot["arm_snapshot_tip"],
                )
                self.assertGreaterEqual(float(health["last_deferred_barrier_wait_seconds"]), 0.0)

                later = ledger.commissioning_tail_snapshot(
                    anchor, last_full_verified_sequence=anchor,
                )
                self.assertEqual(later["arm_snapshot_tip"], anchor + 4)
                self.assertGreater(
                    int(later["deferred_barrier_token"]), int(snapshot["deferred_barrier_token"]),
                )
                self.assertEqual(self.identities(path), expected)
            finally:
                prefix_release.set()
                suffix_release.set()
                if snapshot_thread is not None:
                    snapshot_thread.join(self.timeout_seconds)
                ledger._append_prepared = original_append_prepared  # type: ignore[method-assign]
                ledger.close()

    def test_synchronous_operational_append_cannot_leapfrog_barrier_suffix(self) -> None:
        with TemporaryDirectory() as directory:
            path = Path(directory) / "paper.sqlite3"
            ledger = PaperLedger(path, persist_high_frequency_records=True)
            prefix_release = threading.Event()
            suffix_release = threading.Event()
            prefix_started = threading.Event()
            suffix_started = threading.Event()
            snapshot_done = threading.Event()
            operational_started = threading.Event()
            operational_done = threading.Event()
            snapshot: dict[str, object] = {}
            thread_errors: list[BaseException] = []
            snapshot_thread: threading.Thread | None = None
            operational_thread: threading.Thread | None = None
            original_append_prepared = ledger._append_prepared

            try:
                ledger.append(
                    "SESSION_AUTHORITY", {"reason": "verified operational-order anchor"},
                    identity="verified-anchor", occurred_at=NOW,
                )
                anchor = int(ledger.health_status()["highest_sequence"])

                def gated_append(records: tuple[dict[str, object], ...]) -> list[str]:
                    record_identities = {str(record["identity"]) for record in records}
                    if "prefix" in record_identities:
                        prefix_started.set()
                        if not prefix_release.wait(self.timeout_seconds):
                            raise AssertionError("Timed out releasing the operational-order prefix.")
                    if "suffix" in record_identities:
                        suffix_started.set()
                        if not suffix_release.wait(self.timeout_seconds):
                            raise AssertionError("Timed out releasing the operational-order suffix.")
                    return original_append_prepared(records)

                ledger._append_prepared = gated_append  # type: ignore[method-assign]
                ledger.append_deferred(
                    "OBSERVATION_ENVELOPE", observation(1, "prefix"),
                    identity="prefix", occurred_at=NOW,
                )
                self.assertTrue(prefix_started.wait(self.timeout_seconds))

                def capture_snapshot() -> None:
                    try:
                        snapshot.update(ledger.commissioning_tail_snapshot(
                            anchor, last_full_verified_sequence=anchor,
                        ))
                    except BaseException as error:
                        thread_errors.append(error)
                    finally:
                        snapshot_done.set()

                snapshot_thread = threading.Thread(
                    target=capture_snapshot,
                    name="LedgerOperationalOrderingSnapshotTest",
                )
                snapshot_thread.start()
                self.wait_for_queued_barrier(ledger)
                ledger.append_deferred(
                    "OBSERVATION_ENVELOPE", observation(2, "suffix"),
                    identity="suffix", occurred_at=NOW,
                )

                def append_operational_record() -> None:
                    operational_started.set()
                    try:
                        ledger.append(
                            "COMMAND", {"command_id": "operational"},
                            identity="operational", occurred_at=NOW,
                        )
                    except BaseException as error:
                        thread_errors.append(error)
                    finally:
                        operational_done.set()

                operational_thread = threading.Thread(
                    target=append_operational_record,
                    name="LedgerOperationalAppendTest",
                )
                operational_thread.start()
                self.assertTrue(operational_started.wait(self.timeout_seconds))

                prefix_release.set()
                self.assertTrue(suffix_started.wait(self.timeout_seconds))
                self.assertTrue(snapshot_done.wait(self.timeout_seconds))
                self.assertFalse(
                    operational_done.is_set(),
                    "Synchronous operational append crossed an unpersisted deferred suffix.",
                )
                self.assertEqual(snapshot["arm_snapshot_tip"], anchor + 1)
                self.assertEqual(
                    self.identities(path), ["verified-anchor", "prefix"],
                )

                suffix_release.set()
                snapshot_thread.join(self.timeout_seconds)
                operational_thread.join(self.timeout_seconds)
                self.assertFalse(snapshot_thread.is_alive())
                self.assertFalse(operational_thread.is_alive())
                self.assertEqual(thread_errors, [])
                self.assertEqual(
                    self.identities(path),
                    ["verified-anchor", "prefix", "suffix", "operational"],
                )
                self.assertEqual(ledger.verify_chain(), (True, None))
            finally:
                prefix_release.set()
                suffix_release.set()
                if snapshot_thread is not None:
                    snapshot_thread.join(self.timeout_seconds)
                if operational_thread is not None:
                    operational_thread.join(self.timeout_seconds)
                ledger._append_prepared = original_append_prepared  # type: ignore[method-assign]
                ledger.close()


if __name__ == "__main__":
    unittest.main()
