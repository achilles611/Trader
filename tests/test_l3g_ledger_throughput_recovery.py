"""Deterministic Lane III-G ledger throughput regression suite and benchmark.

The normal unittest cases deliberately use a small deterministic workload.  The
same module is also an explicit benchmark; it never starts the runtime,
commissions, arms, or sends an order.

Examples:

    python -m unittest tests.test_l3g_ledger_throughput_recovery -v
    python tests/test_l3g_ledger_throughput_recovery.py --benchmark --quick
    python tests/test_l3g_ledger_throughput_recovery.py --benchmark \
        --warm-records 100000 --require-threshold

The last command reports a clean database and a warm, large-ledger scenario.
Use a deliberately chosen larger ``--warm-records`` value when characterizing
the deployment hardware; it always writes only a temporary benchmark ledger.
"""

from __future__ import annotations

import argparse
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sqlite3
import sys
from tempfile import TemporaryDirectory
import threading
import time
from typing import Callable, Mapping, Sequence
import unittest
from unittest.mock import patch

# ``unittest discover`` imports this module from the repository root, while
# the documented benchmark form executes this file directly from ``tests``.
# Keep both modes standard-library-only and independent of an editable install.
if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.lane_iii.contracts import canonical_hash
from src.l3g_paper.contracts import POLICY
from src.l3g_paper.ledger import (
    COMMISSIONING_NO_AUTHORITY_EFFECT,
    COMMISSIONING_READINESS_RECORD_SEMANTICS,
    COMMISSIONING_READINESS_RECORD_SEMANTICS_VERSION,
    COMMISSIONING_WARMUP_POLICY_HASH,
    COMMISSIONING_WARMUP_REQUIRED_FAMILIES,
    LedgerCapacityError,
    PaperLedger,
)
from src.l3g_paper.sessions import PaperSessionResolver


_BASE_TIME = datetime(2026, 8, 28, 17, 30, tzinfo=timezone.utc)
_RTH_CONTEXT = PaperSessionResolver().resolve(_BASE_TIME.isoformat().replace("+00:00", "Z"), generation=1).context
_RTH_PAYLOAD = _RTH_CONTEXT.payload()
_SESSION_IDENTITY_KEYS = (
    "session_kind", "session_family", "session_id", "trade_date",
    "session_profile_hash", "session_generation",
)
_BATCH_TELEMETRY_FIELDS = frozenset({
    "batch_size",
    "transaction_seconds",
    "commit_seconds",
    "hash_serialization_seconds",
    "duplicate_lookup_seconds",
    "audit_insert_seconds",
    "domain_insert_seconds",
    "watermark_seconds",
    "wal_checkpoint_stall_upper_bound_seconds",
    "wal_size_bytes",
    "durability_mode",
})
_EVERY_BATCH_TELEMETRY_FIELDS = frozenset({
    "batch_size",
    "durability_mode",
    "wal_checkpoint_seconds",
    "wal_checkpoint_activity",
    "sampled",
})
_WRITER_TELEMETRY_FIELDS = frozenset({
    "admitted_records_per_second",
    "durable_records_per_second",
    "queue_depth",
    "deferred_work_item_depth",
    "queue_growth_records_per_second",
    "oldest_queued_record_age_seconds",
    "capacity_state",
    "admission_rejections_total",
    "pending_barrier_count",
    "max_pending_barriers",
    "barrier_rejections_total",
    "last_passive_checkpoint",
    "wal_uncheckpointed_bytes",
    "wal_uncheckpointed_capacity_ceiling_bytes",
    "wal_file_capacity_ceiling_bytes",
    "recent_batches",
})


@dataclass(frozen=True)
class _WorkItem:
    """One deterministic high-volume ledger admission."""

    kind: str
    payload: dict[str, object]
    identity: str
    occurred_at: str


def _at(sequence: int) -> str:
    return (_BASE_TIME + timedelta(milliseconds=sequence)).isoformat().replace("+00:00", "Z")


def _session_identity() -> dict[str, object]:
    return {key: _RTH_PAYLOAD[key] for key in _SESSION_IDENTITY_KEYS}


def _raw_market_payload(sequence: int, observation_type: str) -> dict[str, object]:
    """Representative decoded MNQ Sep-26 NinjaTrader payloads.

    PaperLedger intentionally persists the bounded envelope and a source hash,
    not the full vendor packet.  Hashing these authentic-shaped payloads keeps
    the benchmark on the production ingress contract instead of inventing a
    smaller synthetic record.
    """
    tick = 20_000 + (sequence % 320)
    bid = f"{tick / 4:.2f}"
    ask = f"{(tick + 1) / 4:.2f}"
    if observation_type == "QUOTE":
        return {
            "contract_id": "MNQ SEP26",
            "bid": bid,
            "ask": ask,
            "bid_size": 4 + (sequence % 32),
            "ask_size": 5 + ((sequence * 3) % 32),
        }
    if observation_type == "TRADE":
        return {
            "contract_id": "MNQ SEP26",
            "price": ask if sequence % 2 else bid,
            "size": 1 + (sequence % 12),
            "aggressor_side": "UNKNOWN",
            "aggressor_source": "BID_ASK_CLASSIFICATION",
            "bid_at_trade": bid,
            "ask_at_trade": ask,
            "derivation_quote_observation_id": f"nt-l3g-benchmark-{sequence - 1:012d}",
        }
    if observation_type == "DEPTH":
        return {
            "contract_id": "MNQ SEP26",
            "bids": [
                {"price": bid, "size": 8 + (sequence % 40)},
                {"price": f"{(tick - 4) / 4:.2f}", "size": 4 + (sequence % 24)},
            ],
            "asks": [
                {"price": ask, "size": 9 + ((sequence * 5) % 40)},
                {"price": f"{(tick + 5) / 4:.2f}", "size": 3 + (sequence % 24)},
            ],
            "operation": ("ADD", "UPDATE", "REMOVE")[sequence % 3],
            "side": "Bid" if sequence % 2 else "Ask",
            "mutation_price": bid if sequence % 2 else ask,
            "mutation_volume": 1 + (sequence % 40),
            "mutation_position": sequence % 10,
            "is_reset": False,
        }
    raise ValueError(f"Unsupported benchmark observation type: {observation_type}")


def _observation_item(sequence: int, observation_type: str, namespace: str) -> _WorkItem:
    occurred_at = _at(sequence)
    raw_payload = _raw_market_payload(sequence, observation_type)
    payload = {
        **_RTH_PAYLOAD,
        "observation_id": f"nt-l3g-benchmark-{namespace}-{sequence:012d}",
        "observation_type": observation_type,
        "observed_at": occurred_at,
        "ninja_receipt_time": occurred_at,
        "provider_timestamp": occurred_at,
        "exchange_timestamp": occurred_at,
        "local_monotonic_sequence": sequence,
        "source_payload_hash": canonical_hash(raw_payload),
    }
    identity = "l3g-paper-observation-" + canonical_hash(payload)
    return _WorkItem("OBSERVATION_ENVELOPE", payload, identity, occurred_at)


def _evidence_item(sequence: int, namespace: str) -> _WorkItem:
    occurred_at = _at(sequence)
    source_sequence = max(1, sequence - 1)
    payload = {
        "evidence_id": f"l3g-pe-benchmark-{namespace}-{sequence:012d}",
        "hypothesis_kind": "BULLISH_REVERSAL" if sequence % 2 else "BEARISH_CONTINUATION",
        "family": (
            "STRUCTURAL_CONTEXT", "ORDER_FLOW", "RESTING_LIQUIDITY",
            "VOLATILITY_CONTEXT", "MARKET_REGIME",
        )[sequence % 5],
        "label": "PRODUCTION_SHAPED_PROVISIONAL_EVIDENCE",
        "strength": "0.75",
        "supports": bool(sequence % 2),
        "observed_at": occurred_at,
        "expires_at": (_BASE_TIME + timedelta(milliseconds=sequence + 5_000)).isoformat().replace("+00:00", "Z"),
        "source_observation_ids": [f"nt-l3g-benchmark-{namespace}-{source_sequence:012d}"],
        "source_local_sequences": [source_sequence],
        "source_payload_hashes": [canonical_hash(_raw_market_payload(source_sequence, "QUOTE"))],
        "quality": "PROVISIONAL_CONTIGUOUS_LOCAL_CALLBACKS",
        "sequence_authority": "LOCAL_CALLBACK_ORDER_ONLY",
        "book_completeness": "UNVERIFIED",
        "scientific_eligibility": False,
        "blocking": False,
        **_session_identity(),
        "source_session_ids": [str(_RTH_PAYLOAD["session_id"])],
    }
    return _WorkItem("EVIDENCE", payload, str(payload["evidence_id"]), occurred_at)


def _decision_item(sequence: int, namespace: str) -> _WorkItem:
    occurred_at = _at(sequence)
    source_sequence = max(1, sequence - 1)
    payload = {
        "paper_decision_id": f"l3g-pd-benchmark-{namespace}-{sequence:012d}",
        "paper_policy_id": POLICY.policy_id,
        "paper_policy_hash": POLICY.configuration_hash,
        "decision": "NO_TRADE",
        "created_at": occurred_at,
        "expires_at": (_BASE_TIME + timedelta(milliseconds=sequence + 5_000)).isoformat().replace("+00:00", "Z"),
        "hypothesis_kind": None,
        "direction": "FLAT",
        "relative_support": "0.50",
        "family_summary": {
            "ORDER_FLOW": "PROVISIONAL",
            "RESTING_LIQUIDITY": "PROVISIONAL",
        },
        "source_observation_ids": [f"nt-l3g-benchmark-{namespace}-{source_sequence:012d}"],
        "source_local_sequences": [source_sequence],
        "source_payload_hashes": [canonical_hash(_raw_market_payload(source_sequence, "QUOTE"))],
        "sequence_authority": "LOCAL_CALLBACK_ORDER_ONLY",
        "book_completeness": "UNVERIFIED",
        "scientific_eligibility": False,
        "reason_code": "PRODUCTION_SHAPED_NO_TRADE",
        **_session_identity(),
        "commissioning": False,
        "strategy_generated": True,
        "scientific_evidence": False,
        "authority_effect": COMMISSIONING_NO_AUTHORITY_EFFECT,
    }
    return _WorkItem("DECISION", payload, str(payload["paper_decision_id"]), occurred_at)


def _work_items(start_sequence: int, count: int, namespace: str) -> list[_WorkItem]:
    """Return a repeatable QUOTE / TRADE / DEPTH / EVIDENCE / DECISION mix."""
    if count < 0:
        raise ValueError("Benchmark work-item count must be non-negative.")
    cycle: tuple[Callable[[int, str], _WorkItem], ...] = (
        lambda number, name: _observation_item(number, "QUOTE", name),
        lambda number, name: _observation_item(number, "QUOTE", name),
        lambda number, name: _observation_item(number, "TRADE", name),
        lambda number, name: _observation_item(number, "QUOTE", name),
        lambda number, name: _observation_item(number, "DEPTH", name),
        _evidence_item,
        lambda number, name: _observation_item(number, "TRADE", name),
        _decision_item,
        lambda number, name: _observation_item(number, "QUOTE", name),
        lambda number, name: _observation_item(number, "DEPTH", name),
    )
    return [factory(start_sequence + offset, namespace) for offset, factory in enumerate(cycle * ((count + len(cycle) - 1) // len(cycle)))][:count]


def _readiness_item(sequence: int, namespace: str) -> _WorkItem:
    occurred_at = _at(sequence)
    payload = {
        **_RTH_PAYLOAD,
        "authority_effect": COMMISSIONING_NO_AUTHORITY_EFFECT,
        "record_semantics": COMMISSIONING_READINESS_RECORD_SEMANTICS,
        "record_semantics_version": COMMISSIONING_READINESS_RECORD_SEMANTICS_VERSION,
        "commissioning_warmup_state": "WARMED",
        "warmed_at": occurred_at,
        "policy_hash": COMMISSIONING_WARMUP_POLICY_HASH,
        "required_families": list(COMMISSIONING_WARMUP_REQUIRED_FAMILIES),
        "reason": "ALL_REQUIRED_FAMILIES_GENUINELY_OBSERVED",
        "evidence_provenance": {
            family: {
                "evidence_id": f"l3g-pe-benchmark-{namespace}-{sequence:012d}-{index}",
                "observed_at": occurred_at,
                "source_observation_ids": [f"nt-l3g-benchmark-{namespace}-{sequence:012d}-{index}"],
                "source_local_sequences": [sequence + index],
            }
            for index, family in enumerate(COMMISSIONING_WARMUP_REQUIRED_FAMILIES, start=1)
        },
    }
    identity = f"l3g-readiness-benchmark-{namespace}-{sequence:012d}"
    return _WorkItem("COMMISSIONING_SESSION_WARMED", payload, identity, occurred_at)


def _append_item(ledger: PaperLedger, item: _WorkItem) -> None:
    if item.kind == "COMMISSIONING_SESSION_WARMED":
        ledger.append_commissioning_attestation_deferred(
            item.kind, item.payload, identity=item.identity, occurred_at=item.occurred_at,
        )
    else:
        ledger.append_deferred(item.kind, item.payload, identity=item.identity, occurred_at=item.occurred_at)


def _append_operational_full_record(ledger: PaperLedger, sequence: int, namespace: str) -> str:
    identity = f"l3g-operational-benchmark-{namespace}-{sequence:012d}"
    ledger.append(
        "INCIDENT_SAFETY_EVENT",
        {
            **_RTH_PAYLOAD,
            "reason": "BENCHMARK_OPERATIONAL_FULL_DURABILITY_RECORD",
            "benchmark_sequence": sequence,
            "paper_only": True,
            "live_capital": False,
        },
        identity=identity,
        occurred_at=_at(sequence),
    )
    return identity


def _ordered_identities(path: Path) -> list[str]:
    with closing(sqlite3.connect(path)) as connection:
        return [str(row[0]) for row in connection.execute(
            "SELECT identity FROM lane_iii_paper_audit ORDER BY ledger_sequence"
        )]


def _wait_for_queued_barrier(ledger: PaperLedger, timeout_seconds: float = 5.0) -> bool:
    """Use the existing test-only queue observation to make the fence race exact."""
    with ledger._deferred_condition:
        return bool(ledger._deferred_condition.wait_for(
            lambda: ledger._deferred_barrier_count > 0,
            timeout=timeout_seconds,
        ))


class LedgerThroughputRecoveryTests(unittest.TestCase):
    """Focused correctness tests for the writer recovery changes."""

    def assert_required_writer_telemetry(self, health: Mapping[str, object]) -> Mapping[str, object]:
        self.assertIn("writer_telemetry", health)
        telemetry = health["writer_telemetry"]
        self.assertIsInstance(telemetry, Mapping)
        telemetry = dict(telemetry)
        self.assertTrue(_WRITER_TELEMETRY_FIELDS.issubset(telemetry), telemetry)
        for name in (
            "admitted_records_per_second",
            "durable_records_per_second",
            "queue_growth_records_per_second",
        ):
            self.assertIsInstance(telemetry[name], (int, float), name)
        self.assertIsInstance(telemetry["queue_depth"], int)
        self.assertEqual(telemetry["queue_depth"], health["deferred_queue_depth"])
        self.assertIsInstance(telemetry["deferred_work_item_depth"], int)
        self.assertGreaterEqual(
            int(telemetry["deferred_work_item_depth"]), int(telemetry["queue_depth"]),
        )
        oldest = telemetry["oldest_queued_record_age_seconds"]
        self.assertTrue(oldest is None or isinstance(oldest, (int, float)))
        self.assertIsInstance(telemetry["capacity_state"], str)
        self.assertIsInstance(telemetry["admission_rejections_total"], int)
        self.assertIsInstance(telemetry["pending_barrier_count"], int)
        self.assertIsInstance(telemetry["max_pending_barriers"], int)
        self.assertIsInstance(telemetry["barrier_rejections_total"], int)
        batches = telemetry["recent_batches"]
        self.assertIsInstance(batches, (tuple, list))
        self.assertLessEqual(len(batches), 128, "writer telemetry must retain a bounded batch history")
        sampled_batches = 0
        for batch in batches:
            self.assertIsInstance(batch, Mapping)
            self.assertTrue(_EVERY_BATCH_TELEMETRY_FIELDS.issubset(batch), batch)
            self.assertIsInstance(batch["batch_size"], int)
            self.assertGreater(int(batch["batch_size"]), 0)
            self.assertIsInstance(batch["sampled"], bool)
            self.assertIn(batch["durability_mode"], {"NORMAL", "FULL"})
            if batch["sampled"]:
                sampled_batches += 1
                self.assertTrue(_BATCH_TELEMETRY_FIELDS.issubset(batch), batch)
                for field in _BATCH_TELEMETRY_FIELDS - {"batch_size", "durability_mode"}:
                    self.assertIsInstance(batch[field], (int, float), field)
                    self.assertGreaterEqual(float(batch[field]), 0.0, field)
        if batches:
            self.assertGreater(sampled_batches, 0, "bounded timing samples must remain observable")
        return telemetry

    def test_mixed_durability_barrier_keeps_order_and_reports_the_chosen_mode(self) -> None:
        """A readiness attestation stays exactly ordered and exposes its durability choice."""
        with TemporaryDirectory() as folder:
            path = Path(folder) / "paper.sqlite3"
            ledger = PaperLedger(
                path,
                max_deferred_records=4_096,
                catch_up_batch_size=1_024,
                catch_up_threshold=64,
                persist_high_frequency_records=True,
            )
            release_writer = threading.Event()
            writer_started = threading.Event()
            checkpoint_result: list[dict[str, object]] = []
            checkpoint_errors: list[BaseException] = []
            appended_segments: list[tuple[str, ...]] = []
            original_append = ledger._append_prepared
            checkpoint_thread: threading.Thread | None = None
            try:
                def gated_append(records: tuple[dict[str, object], ...]) -> list[str]:
                    appended_segments.append(tuple(str(record["identity"]) for record in records))
                    if records and not writer_started.is_set():
                        writer_started.set()
                        if not release_writer.wait(5.0):
                            raise TimeoutError("timed out waiting to release the benchmark writer")
                    return original_append(records)

                ledger._append_prepared = gated_append  # type: ignore[method-assign]
                prefix = _work_items(1, 32, "mixed-prefix")
                readiness = _readiness_item(33, "mixed")
                suffix = _work_items(34, 32, "mixed-suffix")
                tail = _work_items(66, 12, "mixed-tail")
                expected_prefix = [item.identity for item in prefix] + [readiness.identity]
                for item in prefix:
                    _append_item(ledger, item)
                self.assertTrue(writer_started.wait(5.0))
                _append_item(ledger, readiness)

                def checkpoint() -> None:
                    try:
                        checkpoint_result.append(ledger.commissioning_authority_checkpoint())
                    except BaseException as error:  # pragma: no cover - asserted below
                        checkpoint_errors.append(error)

                checkpoint_thread = threading.Thread(target=checkpoint, name="L3GThroughputBarrier")
                checkpoint_thread.start()
                self.assertTrue(_wait_for_queued_barrier(ledger))
                for item in suffix:
                    _append_item(ledger, item)
                release_writer.set()
                checkpoint_thread.join(10.0)
                self.assertFalse(checkpoint_thread.is_alive())
                self.assertEqual(checkpoint_errors, [])
                self.assertEqual(len(checkpoint_result), 1)
                self.assertEqual(checkpoint_result[0]["ledger_sequence"], len(expected_prefix))
                full_identity = _append_operational_full_record(ledger, 78, "mixed")
                for item in tail:
                    _append_item(ledger, item)
                ledger.flush_deferred()

                expected = expected_prefix + [item.identity for item in suffix] + [full_identity] + [
                    item.identity for item in tail
                ]
                self.assertEqual(_ordered_identities(path), expected)
                self.assertEqual(len(expected), len(set(expected)))
                self.assertEqual(ledger.verify_chain(), (True, None))
                watermark = ledger.health_status()["authority_watermark"]
                self.assertIsInstance(watermark, Mapping)
                self.assertEqual(watermark["classified_through_sequence"], len(expected))
                telemetry = self.assert_required_writer_telemetry(ledger.health_status())
                normal_batches = [
                    batch for batch in telemetry["recent_batches"]
                    if isinstance(batch, Mapping) and batch["durability_mode"] == "NORMAL"
                ]
                full_batches = [
                    batch for batch in telemetry["recent_batches"]
                    if isinstance(batch, Mapping) and batch["durability_mode"] == "FULL"
                ]
                self.assertTrue(normal_batches, telemetry)
                self.assertTrue(full_batches, telemetry)
                # The exact attestation remains a FULL singleton.  Adjacent
                # NORMAL market rows must not be silently upgraded to FULL or
                # the attestation silently downgraded to NORMAL.
                self.assertIn((readiness.identity,), appended_segments)
                readiness_index = appended_segments.index((readiness.identity,))
                self.assertGreater(readiness_index, 0)
                self.assertEqual(
                    appended_segments[readiness_index - 1],
                    tuple(item.identity for item in prefix),
                )
                self.assertTrue(all(int(batch["batch_size"]) >= 1 for batch in full_batches), full_batches)
            finally:
                release_writer.set()
                ledger._append_prepared = original_append  # type: ignore[method-assign]
                if checkpoint_thread is not None and checkpoint_thread.is_alive():
                    checkpoint_thread.join(5.0)
                ledger.close()

    def test_passive_checkpoint_worker_reports_truth_and_stops_before_shutdown(self) -> None:
        """Checkpoint allocation alone is not an authority failure; pinned frames are."""
        with TemporaryDirectory() as folder:
            ledger = PaperLedger(
                Path(folder) / "paper.sqlite3", max_deferred_records=64,
                persist_high_frequency_records=True,
            )
            receipt: dict[str, object] | None = None
            try:
                _append_item(ledger, _work_items(1, 1, "passive-worker")[0])
                ledger.flush_deferred()
                # This is a test-only signal of the worker's independently
                # owned maintenance path; it does not alter writer ordering.
                with ledger._checkpoint_condition:
                    ledger._checkpoint_requested = True
                    ledger._publish_passive_checkpoint_state_locked()
                    ledger._checkpoint_condition.notify_all()
                deadline = time.monotonic() + 5.0
                checkpoint: object = None
                while time.monotonic() < deadline:
                    telemetry = self.assert_required_writer_telemetry(ledger.health_status())
                    checkpoint = telemetry["last_passive_checkpoint"]
                    if isinstance(checkpoint, Mapping):
                        break
                    time.sleep(0.01)
                self.assertIsInstance(checkpoint, Mapping)
                self.assertEqual(checkpoint["mode"], "PASSIVE")  # type: ignore[index]
                self.assertIn("uncheckpointed_bytes", checkpoint)  # type: ignore[arg-type]
                reuse = checkpoint["reuse_checkpoint"]  # type: ignore[index]
                self.assertIsInstance(reuse, Mapping)
                self.assertEqual(reuse["mode"], "RESTART")
                self.assertTrue(reuse["complete"], reuse)
                self.assertEqual(reuse["busy"], 0)
                self.assertEqual(telemetry["wal_passive_checkpoint_trigger_records"], 1_024)

                # A retained physical WAL allocation after a complete
                # PASSIVE copy remains safe. The 1 GiB disk-growth backstop
                # leaves room above observed healthy retained allocation;
                # only actual uncheckpointed frames latch this test path.
                with ledger._checkpoint_condition:
                    ledger._checkpoint_wal_size_bytes = 600_000_000
                    ledger._checkpoint_uncheckpointed_bytes = 0
                    ledger._wal_capacity_fault_latched = False
                    ledger._publish_passive_checkpoint_state_locked()
                capacity = ledger.deferred_capacity()
                self.assertEqual(capacity["state"], "HEALTHY")
                self.assertEqual(capacity["wal_file_capacity_ceiling_bytes"], 1_073_741_824)
                with ledger._checkpoint_condition:
                    ledger._checkpoint_uncheckpointed_bytes = 134_217_729
                    ledger._wal_capacity_fault_latched = True
                    ledger._publish_passive_checkpoint_state_locked()
                capacity = ledger.deferred_capacity()
                self.assertEqual(capacity["state"], "EXHAUSTED")
                self.assertFalse(capacity["admission_open"])
                with self.assertRaisesRegex(LedgerCapacityError, "WAL capacity is exhausted"):
                    _append_item(ledger, _work_items(2, 1, "passive-worker")[0])

                receipt = ledger.close()
                self.assertTrue(receipt["checkpoint_worker_stopped"])
                self.assertTrue(receipt["clean_shutdown"])
            finally:
                if receipt is None:
                    ledger.close()

    def test_startup_observes_existing_physical_wal_capacity_before_admission(self) -> None:
        """An oversized retained WAL must fail closed before the worker's first pass."""
        with TemporaryDirectory() as folder:
            with patch.object(PaperLedger, "_current_wal_size_bytes", return_value=1_073_741_825):
                ledger = PaperLedger(
                    Path(folder) / "paper.sqlite3", max_deferred_records=64,
                    persist_high_frequency_records=True,
                )
            try:
                capacity = ledger.deferred_capacity()
                self.assertEqual(capacity["state"], "EXHAUSTED")
                self.assertFalse(capacity["admission_open"])
                self.assertEqual(
                    capacity["wal_capacity_fault_reason"],
                    "WAL_FILE_CAPACITY_CEILING_EXCEEDED_AT_STARTUP",
                )
                with self.assertRaisesRegex(LedgerCapacityError, "WAL capacity is exhausted"):
                    _append_item(ledger, _work_items(1, 1, "startup-wal-capacity")[0])
            finally:
                ledger.close()

    def test_authority_database_capacity_and_runway_fail_closed(self) -> None:
        """Main-file exhaustion blocks authority even when WAL and queue are healthy."""
        with TemporaryDirectory() as folder:
            ledger = PaperLedger(Path(folder) / "paper.sqlite3")
            try:
                with (
                    patch.object(ledger, "_current_database_size_bytes", return_value=40 * 1024**3),
                    patch.object(ledger, "_current_database_free_bytes", return_value=64 * 1024**3),
                ):
                    ledger._database_capacity_last_sample_at = None
                    capacity = ledger.deferred_capacity()
                self.assertEqual(capacity["state"], "EXHAUSTED")
                self.assertFalse(capacity["admission_open"])
                self.assertTrue(capacity["database_capacity_fault_latched"])
                self.assertEqual(
                    capacity["database_capacity_fault_reason"],
                    "AUTHORITY_LEDGER_DATABASE_CAPACITY_EXCEEDED",
                )
                self.assertEqual(capacity["database_runway_state"], "EXHAUSTED")
            finally:
                ledger.close()

    def test_degraded_capacity_rejects_new_deferred_admission(self) -> None:
        """Telemetry and the actual admission path must enforce the same gate."""
        with TemporaryDirectory() as folder:
            ledger = PaperLedger(
                Path(folder) / "paper.sqlite3", max_deferred_records=64,
                persist_high_frequency_records=True,
            )
            try:
                with ledger._checkpoint_condition:
                    ledger._checkpoint_worker_error = "OperationalError: test maintenance failure"
                    ledger._publish_passive_checkpoint_state_locked()
                capacity = ledger.deferred_capacity()
                self.assertEqual(capacity["state"], "DEGRADED")
                self.assertFalse(capacity["admission_open"])
                with self.assertRaisesRegex(LedgerCapacityError, "capacity is not healthy"):
                    _append_item(ledger, _work_items(1, 1, "degraded-admission")[0])
                self.assertEqual(ledger.deferred_capacity()["admission_rejections_total"], 1)
            finally:
                ledger.close()

    def test_capacity_hard_cap_rejects_without_silent_loss_and_surfaces_degradation(self) -> None:
        """The RAM queue cannot silently absorb an unbounded writer deficit."""
        with TemporaryDirectory() as folder:
            path = Path(folder) / "paper.sqlite3"
            ledger = PaperLedger(
                path, max_deferred_records=8, persist_high_frequency_records=True,
            )
            release_writer = threading.Event()
            writer_started = threading.Event()
            original_append = ledger._append_prepared
            accepted: list[str] = []
            rejected: list[str] = []
            try:
                def gated_append(records: tuple[dict[str, object], ...]) -> list[str]:
                    if records and not writer_started.is_set():
                        writer_started.set()
                        if not release_writer.wait(5.0):
                            raise TimeoutError("timed out waiting to release the capacity writer")
                    return original_append(records)

                ledger._append_prepared = gated_append  # type: ignore[method-assign]
                items = _work_items(1, 64, "capacity")
                _append_item(ledger, items[0])
                accepted.append(items[0].identity)
                self.assertTrue(writer_started.wait(5.0))
                for item in items[1:]:
                    try:
                        _append_item(ledger, item)
                    except LedgerCapacityError:
                        rejected.append(item.identity)
                    else:
                        accepted.append(item.identity)
                self.assertTrue(rejected, "the hard cap must reject rather than grow without bound")
                capacity_while_saturated = ledger.deferred_capacity()
                self.assertNotEqual(capacity_while_saturated["state"], "HEALTHY")
                self.assertGreaterEqual(int(capacity_while_saturated["admission_rejections_total"]), len(rejected))
                release_writer.set()
                ledger.flush_deferred()
                self.assertEqual(_ordered_identities(path), accepted)
                self.assertEqual(ledger.verify_chain(), (True, None))
                self.assert_required_writer_telemetry(ledger.health_status())
            finally:
                release_writer.set()
                ledger._append_prepared = original_append  # type: ignore[method-assign]
                ledger.close()

    def test_pending_checkpoint_barriers_are_bounded_and_visible(self) -> None:
        """A stalled writer cannot turn concurrent checkpoints into an invisible RAM queue."""
        with TemporaryDirectory() as folder:
            ledger = PaperLedger(
                Path(folder) / "paper.sqlite3",
                max_deferred_records=64,
                max_pending_barriers=2,
                persist_high_frequency_records=True,
            )
            release_writer = threading.Event()
            writer_started = threading.Event()
            original_append = ledger._append_prepared
            checkpoint_results: list[dict[str, object]] = []
            checkpoint_errors: list[BaseException] = []
            checkpoint_threads: list[threading.Thread] = []
            try:
                def gated_append(records: tuple[dict[str, object], ...]) -> list[str]:
                    if records and not writer_started.is_set():
                        writer_started.set()
                        if not release_writer.wait(5.0):
                            raise TimeoutError("timed out waiting to release the barrier-capacity writer")
                    return original_append(records)

                def checkpoint() -> None:
                    try:
                        checkpoint_results.append(ledger.commissioning_authority_checkpoint())
                    except BaseException as error:  # pragma: no cover - asserted below
                        checkpoint_errors.append(error)

                ledger._append_prepared = gated_append  # type: ignore[method-assign]
                _append_item(ledger, _work_items(1, 1, "barrier-capacity")[0])
                self.assertTrue(writer_started.wait(5.0))
                checkpoint_threads = [
                    threading.Thread(target=checkpoint, name=f"L3GBarrierCapacity{index}")
                    for index in range(2)
                ]
                for thread in checkpoint_threads:
                    thread.start()
                with ledger._deferred_condition:
                    self.assertTrue(ledger._deferred_condition.wait_for(
                        lambda: ledger._deferred_barrier_count == 2,
                        timeout=5.0,
                    ))

                saturated = ledger.deferred_capacity()
                self.assertEqual(saturated["pending_barrier_count"], 2)
                self.assertEqual(saturated["max_pending_barriers"], 2)
                self.assertTrue(saturated["barrier_capacity_exhausted"])
                self.assertEqual(saturated["state"], "DEGRADED")
                self.assertEqual(
                    saturated["deferred_work_item_depth"],
                    saturated["queue_depth"] + saturated["pending_barrier_count"],
                )
                with self.assertRaisesRegex(LedgerCapacityError, "checkpoint capacity is exhausted"):
                    ledger.commissioning_authority_checkpoint()
                self.assertEqual(ledger.deferred_capacity()["barrier_rejections_total"], 1)

                release_writer.set()
                for thread in checkpoint_threads:
                    thread.join(10.0)
                    self.assertFalse(thread.is_alive())
                self.assertEqual(checkpoint_errors, [])
                self.assertEqual(len(checkpoint_results), 2)
                ledger.flush_deferred()
                health = ledger.health_status()
                telemetry = self.assert_required_writer_telemetry(health)
                self.assertEqual(health["deferred_pending_barrier_count"], 0)
                self.assertEqual(health["deferred_barrier_high_water"], 2)
                self.assertEqual(telemetry["barrier_rejections_total"], 1)
                self.assertEqual(ledger.verify_chain(), (True, None))
            finally:
                release_writer.set()
                for thread in checkpoint_threads:
                    if thread.is_alive():
                        thread.join(5.0)
                ledger._append_prepared = original_append  # type: ignore[method-assign]
                ledger.close()

    def test_duplicate_identity_bulk_retries_are_idempotent_but_conflicts_fail_closed(self) -> None:
        """Bulk identity lookup must retain idempotency without hiding conflicts."""
        with TemporaryDirectory() as folder, PaperLedger(
            Path(folder) / "paper.sqlite3",
            max_deferred_records=1_024,
            catch_up_batch_size=1_024,
            catch_up_threshold=64,
            persist_high_frequency_records=True,
        ) as ledger:
            items = _work_items(1, 96, "duplicate")
            expected = [item.identity for item in items]
            for item in items:
                _append_item(ledger, item)
            ledger.flush_deferred()
            for item in items:
                _append_item(ledger, item)
            ledger.flush_deferred()
            self.assertEqual(_ordered_identities(ledger.path), expected)

            original = items[0]
            conflicting_payload = dict(original.payload)
            conflicting_payload["source_payload_hash"] = canonical_hash({
                "different": "same external identity must not overwrite or disappear",
            })
            with self.assertRaisesRegex(ValueError, "identity conflicts"):
                ledger.append(
                    original.kind,
                    conflicting_payload,
                    identity=original.identity,
                    occurred_at=original.occurred_at,
                )
            self.assertEqual(_ordered_identities(ledger.path), expected)
            self.assertEqual(ledger.verify_chain(), (True, None))
            telemetry = self.assert_required_writer_telemetry(ledger.health_status())
            self.assertTrue(telemetry["recent_batches"], telemetry)

    def test_explicit_occurrence_time_is_part_of_external_identity_content(self) -> None:
        """A fresh generated envelope time is retry-safe; an explicit time is not disposable."""
        with TemporaryDirectory() as folder, PaperLedger(Path(folder) / "paper.sqlite3") as ledger:
            item = _work_items(1, 1, "explicit-time")[0]
            first = ledger.append(
                item.kind, item.payload, identity=item.identity, occurred_at=item.occurred_at,
            )
            self.assertEqual(
                first,
                ledger.append(
                    item.kind, item.payload, identity=item.identity, occurred_at=item.occurred_at,
                ),
            )
            with self.assertRaisesRegex(ValueError, "identity conflicts"):
                ledger.append(
                    item.kind, item.payload, identity=item.identity, occurred_at=_at(2),
                )
            # Omitting the timestamp on a later call cannot launder a retry of
            # an external, explicitly timestamped event into generated-time
            # idempotency.
            with self.assertRaisesRegex(ValueError, "identity conflicts"):
                ledger.append(item.kind, item.payload, identity=item.identity)
            self.assertEqual(_ordered_identities(ledger.path), [item.identity])
            self.assertEqual(ledger.verify_chain(), (True, None))

    def test_controlled_close_drains_all_admitted_production_shaped_records(self) -> None:
        """A clean close waits for its admitted in-memory suffix and proves it durable."""
        with TemporaryDirectory() as folder:
            path = Path(folder) / "paper.sqlite3"
            ledger = PaperLedger(
                path, max_deferred_records=1_024, persist_high_frequency_records=True,
            )
            release_writer = threading.Event()
            writer_started = threading.Event()
            close_started = threading.Event()
            close_errors: list[BaseException] = []
            close_receipts: list[dict[str, object]] = []
            original_append = ledger._append_prepared
            items = _work_items(1, 96, "shutdown")
            accepted = [item.identity for item in items]
            close_thread: threading.Thread | None = None
            closed = False
            try:
                def gated_append(records: tuple[dict[str, object], ...]) -> list[str]:
                    if records and not writer_started.is_set():
                        writer_started.set()
                        if not release_writer.wait(5.0):
                            raise TimeoutError("timed out waiting to release the shutdown writer")
                    return original_append(records)

                ledger._append_prepared = gated_append  # type: ignore[method-assign]
                for item in items:
                    _append_item(ledger, item)
                self.assertTrue(writer_started.wait(5.0))
                self.assertGreater(int(ledger.deferred_capacity()["queue_depth"]), 0)

                def close() -> None:
                    try:
                        close_started.set()
                        close_receipts.append(ledger.close())
                    except BaseException as error:  # pragma: no cover - asserted below
                        close_errors.append(error)

                close_thread = threading.Thread(target=close, name="L3GControlledLedgerClose")
                close_thread.start()
                self.assertTrue(close_started.wait(5.0))
                deadline = time.monotonic() + 5.0
                while ledger.deferred_capacity()["admission_open"] is not False:
                    self.assertGreater(deadline - time.monotonic(), 0.0, "close did not seal admission")
                    time.sleep(0.005)
                with self.assertRaises(LedgerCapacityError):
                    _append_item(ledger, _work_items(97, 1, "post-seal")[0])
                time.sleep(0.02)
                self.assertTrue(close_thread.is_alive(), "controlled close must wait for the admitted suffix")
                release_writer.set()
                close_thread.join(10.0)
                self.assertFalse(close_thread.is_alive())
                self.assertEqual(close_errors, [])
                self.assertEqual(len(close_receipts), 1)
                receipt = close_receipts[0]
                self.assertTrue(receipt["clean_shutdown"], receipt)
                self.assertEqual(receipt["expected_tip_sequence"], receipt["durable_tip_sequence"])
                self.assertEqual(receipt["expected_tip_hash"], receipt["durable_tip_hash"])
                self.assertTrue(receipt["writer_stopped"])
                self.assertIsInstance(receipt["checkpoint"], Mapping)
                self.assertTrue(receipt["checkpoint"]["complete"])  # type: ignore[index]
                closed = True
                self.assertEqual(_ordered_identities(path), accepted)
                with PaperLedger(
                    path, max_deferred_records=1_024, persist_high_frequency_records=True,
                ) as reopened:
                    self.assertEqual(reopened.verify_chain(), (True, None))
                    health = reopened.health_status()
                    self.assertEqual(health["highest_sequence"], len(accepted))
                    self.assertEqual(health["authority_watermark"]["classified_through_sequence"], len(accepted))
            finally:
                release_writer.set()
                ledger._append_prepared = original_append  # type: ignore[method-assign]
                if close_thread is not None and close_thread.is_alive():
                    close_thread.join(5.0)
                if not closed:
                    ledger.close()

    def test_controlled_close_records_writer_failure_without_a_clean_claim(self) -> None:
        with TemporaryDirectory() as folder:
            ledger = PaperLedger(
                Path(folder) / "paper.sqlite3", max_deferred_records=64,
                persist_high_frequency_records=True,
            )
            original_append = ledger._append_prepared
            closed = False
            try:
                def fail_writer(_: tuple[dict[str, object], ...]) -> list[str]:
                    raise sqlite3.OperationalError("injected controlled-shutdown writer failure")

                ledger._append_prepared = fail_writer  # type: ignore[method-assign]
                _append_item(ledger, _work_items(1, 1, "shutdown-failure")[0])
                deadline = time.monotonic() + 5.0
                while ledger.deferred_capacity()["writer_error"] is None:
                    self.assertGreater(deadline - time.monotonic(), 0.0, "writer did not surface injected failure")
                    time.sleep(0.005)
                with self.assertRaisesRegex(RuntimeError, "Controlled paper ledger shutdown failed"):
                    ledger.close()
                closed = True
                receipt = ledger.shutdown_status()
                self.assertIsInstance(receipt, Mapping)
                self.assertFalse(receipt["clean_shutdown"])  # type: ignore[index]
                self.assertTrue(receipt["admission_sealed"])  # type: ignore[index]
                self.assertTrue(receipt["writer_stopped"])  # type: ignore[index]
                self.assertIsNotNone(receipt["error"])  # type: ignore[index]
            finally:
                ledger._append_prepared = original_append  # type: ignore[method-assign]
                if not closed:
                    ledger.close()


def _admit_timed(
    ledger: PaperLedger,
    items: Sequence[_WorkItem],
    *,
    records_per_second: int,
    namespace: str,
    attestation_every: int,
    full_record_every: int,
) -> tuple[list[str], float]:
    """Admit a rate-controlled stream while preserving deterministic identity order."""
    if records_per_second <= 0:
        raise ValueError("Benchmark admission rate must be positive.")
    identities: list[str] = []
    started = time.perf_counter()
    for offset, item in enumerate(items, start=1):
        target = started + (offset - 1) / records_per_second
        remaining = target - time.perf_counter()
        if remaining > 0:
            time.sleep(remaining)
        _append_item(ledger, item)
        identities.append(item.identity)
        if attestation_every and offset % attestation_every == 0:
            readiness = _readiness_item(
                item.payload.get("local_monotonic_sequence", offset) if isinstance(item.payload.get("local_monotonic_sequence"), int) else offset,
                namespace + "-readiness",
            )
            _append_item(ledger, readiness)
            identities.append(readiness.identity)
        if full_record_every and offset % full_record_every == 0:
            identities.append(_append_operational_full_record(ledger, offset, namespace + "-full"))
    return identities, time.perf_counter() - started


def _warm_ledger(path: Path, warm_records: int, *, maximum_deferred_records: int, catch_up_batch_size: int) -> None:
    if not warm_records:
        return
    ledger = PaperLedger(
        path,
        max_deferred_records=maximum_deferred_records,
        catch_up_batch_size=catch_up_batch_size,
        catch_up_threshold=min(512, catch_up_batch_size),
        persist_high_frequency_records=True,
    )
    try:
        for item in _work_items(1, warm_records, "warm"):
            _append_item(ledger, item)
        ledger.flush_deferred()
        if ledger.verify_chain() != (True, None):
            raise AssertionError("The temporary warm ledger must remain hash-chain valid.")
    finally:
        ledger.close()


def _benchmark_telemetry_summary(value: object, *, recent_batch_limit: int = 16) -> object:
    """Keep the benchmark JSON readable while preserving representative stages."""
    if not isinstance(value, Mapping):
        return value
    summary = dict(value)
    batches = summary.get("recent_batches")
    if isinstance(batches, (list, tuple)):
        summary["retained_batch_count"] = len(batches)
        summary["recent_batches"] = list(batches[-recent_batch_limit:])
    return summary


def _benchmark_scenario(
    *,
    name: str,
    warm_records: int,
    steady_seconds: float,
    burst_seconds: float,
    admission_rate: int,
    burst_rate: int,
    catch_up_records: int,
    shutdown_tail_records: int,
    catch_up_batch_size: int,
) -> dict[str, object]:
    """Run one clean or warm temporary-ledger writer characterization."""
    if steady_seconds <= 0 or burst_seconds <= 0:
        raise ValueError("Benchmark durations must be positive.")
    if catch_up_records <= 0 or shutdown_tail_records <= 0:
        raise ValueError("Catch-up and shutdown-tail record counts must be positive.")
    steady_records = max(1, round(admission_rate * steady_seconds))
    burst_records = max(1, round(burst_rate * burst_seconds))
    max_deferred_records = max(
        4_096,
        catch_up_records + shutdown_tail_records + 1_024,
        warm_records + 1_024 if warm_records else 0,
    )
    with TemporaryDirectory(prefix="l3g-ledger-benchmark-") as folder:
        path = Path(folder) / f"{name}.sqlite3"
        _warm_ledger(
            path,
            warm_records,
            maximum_deferred_records=max_deferred_records,
            catch_up_batch_size=catch_up_batch_size,
        )
        ledger = PaperLedger(
            path,
            max_deferred_records=max_deferred_records,
            catch_up_batch_size=catch_up_batch_size,
            catch_up_threshold=min(512, catch_up_batch_size),
            persist_high_frequency_records=True,
        )
        all_admitted: list[str] = []
        barrier_result: list[dict[str, object]] = []
        barrier_errors: list[BaseException] = []
        barrier_thread: threading.Thread | None = None
        writer_release: threading.Event | None = None
        original_append: Callable[[tuple[dict[str, object], ...]], list[str]] | None = None
        closed = False
        try:
            baseline = ledger.health_status()
            steady_start_sequence = int(baseline["highest_sequence"])
            steady_items = _work_items(warm_records + 1, steady_records, name + "-steady")
            steady_identities, steady_elapsed = _admit_timed(
                ledger,
                steady_items,
                records_per_second=admission_rate,
                namespace=name + "-steady",
                attestation_every=0,
                full_record_every=0,
            )
            all_admitted.extend(steady_identities)
            steady_end = ledger.health_status()
            steady_durable = int(steady_end["highest_sequence"]) - steady_start_sequence
            ledger.flush_deferred()
            steady_settled = ledger.health_status()

            burst_start_sequence = int(steady_settled["highest_sequence"])
            burst_items = _work_items(
                warm_records + steady_records + 1,
                burst_records,
                name + "-burst",
            )
            burst_identities, burst_elapsed = _admit_timed(
                ledger,
                burst_items,
                records_per_second=burst_rate,
                namespace=name + "-burst",
                attestation_every=0,
                full_record_every=0,
            )
            all_admitted.extend(burst_identities)
            burst_end = ledger.health_status()
            burst_durable = int(burst_end["highest_sequence"]) - burst_start_sequence
            ledger.flush_deferred()

            # Retain an actual FULL operational path, but do not misreport a
            # deliberately rare synchronous incident as steady market-stream
            # capacity. The readiness marker below is admitted amid traffic.
            full_identity = _append_operational_full_record(
                ledger, warm_records + steady_records + burst_records + 1, name + "-full",
            )
            all_admitted.append(full_identity)

            # Hold the first writer batch only long enough to form a known
            # production-shaped backlog. The resulting drain window measures
            # writer capacity rather than producer JSON/preparation time.
            catch_up_items = _work_items(
                warm_records + steady_records + burst_records + 1,
                catch_up_records,
                name + "-catchup",
            )
            split = max(1, len(catch_up_items) // 2)
            writer_blocked = threading.Event()
            writer_release = threading.Event()
            original_append = ledger._append_prepared

            def gated_append(records: tuple[dict[str, object], ...]) -> list[str]:
                if records and not writer_blocked.is_set():
                    writer_blocked.set()
                    if not writer_release.wait(30.0):
                        raise TimeoutError("Benchmark writer was not released for backlog drain.")
                return original_append(records)

            ledger._append_prepared = gated_append  # type: ignore[method-assign]
            _append_item(ledger, catch_up_items[0])
            all_admitted.append(catch_up_items[0].identity)
            if not writer_blocked.wait(5.0):
                raise TimeoutError("Benchmark writer did not enter the controlled backlog gate.")
            for item in catch_up_items[1:split]:
                _append_item(ledger, item)
                all_admitted.append(item.identity)

            def checkpoint() -> None:
                try:
                    barrier_result.append(ledger.commissioning_authority_checkpoint())
                except BaseException as error:  # pragma: no cover - recorded in report
                    barrier_errors.append(error)

            barrier_thread = threading.Thread(target=checkpoint, name="L3GBenchmarkBarrier")
            barrier_thread.start()
            if not _wait_for_queued_barrier(ledger):
                raise TimeoutError("Benchmark commissioning barrier was not admitted behind its prefix.")
            readiness = _readiness_item(
                warm_records + steady_records + burst_records + catch_up_records + 2,
                name + "-readiness",
            )
            _append_item(ledger, readiness)
            all_admitted.append(readiness.identity)
            for item in catch_up_items[split:]:
                _append_item(ledger, item)
                all_admitted.append(item.identity)
            catchup_started = time.perf_counter()
            writer_release.set()
            ledger.flush_deferred()
            catchup_elapsed = time.perf_counter() - catchup_started
            barrier_thread.join(30.0)
            if barrier_thread.is_alive():
                raise TimeoutError("Benchmark commissioning barrier did not complete.")
            if barrier_errors:
                raise RuntimeError("Benchmark commissioning barrier failed.") from barrier_errors[0]
            ledger._append_prepared = original_append  # type: ignore[method-assign]
            after_catchup = ledger.health_status()

            # Stop admission and immediately issue a substantial admitted tail.
            # The explicit close below must drain this non-empty suffix rather
            # than claiming success merely because all prior stages were clean.
            shutdown_items = _work_items(
                warm_records + steady_records + burst_records + catch_up_records + 1,
                shutdown_tail_records,
                name + "-shutdown",
            )
            for item in shutdown_items:
                _append_item(ledger, item)
                all_admitted.append(item.identity)
            before_close = ledger.health_status()
            writer_telemetry_before_close = before_close.get("writer_telemetry")
            close_started = time.perf_counter()
            close_receipt = ledger.close()
            closed = True
            close_elapsed = time.perf_counter() - close_started

            with PaperLedger(
                path,
                max_deferred_records=max_deferred_records,
                catch_up_batch_size=catch_up_batch_size,
                catch_up_threshold=min(512, catch_up_batch_size),
                persist_high_frequency_records=True,
            ) as reopened:
                chain_valid, broken_identity = reopened.verify_chain()
                final_health = reopened.health_status()
                stored = _ordered_identities(path)
                expected = _ordered_identities(path)[:warm_records] + all_admitted
                # Warm identities are deliberately separate from measured
                # identity space.  This exact suffix comparison catches loss,
                # duplicate work, and a shutdown that returned too early.
                no_loss = stored[warm_records:] == all_admitted and len(stored) == len(expected)
                watermark = final_health["authority_watermark"]
                watermark_valid = (
                    isinstance(watermark, Mapping)
                    and watermark.get("classified_through_sequence") == final_health["highest_sequence"]
                )
            return {
                "scenario": name,
                "warm_records": warm_records,
                "workload": {
                    "production_shaped_record_kinds": [
                        "QUOTE", "TRADE", "DEPTH", "EVIDENCE", "DECISION",
                    ],
                    "high_volume_records": steady_records + burst_records + catch_up_records + shutdown_tail_records,
                    "commissioning_readiness_attestations": sum(
                        identity.startswith("l3g-readiness-benchmark-") for identity in all_admitted
                    ),
                    "full_durability_operational_records": sum(
                        identity.startswith("l3g-operational-benchmark-") for identity in all_admitted
                    ),
                },
                "steady": {
                    "target_admission_records_per_second": admission_rate,
                    "admitted_records": len(steady_identities),
                    "admission_seconds": round(steady_elapsed, 6),
                    "actual_admission_records_per_second": round(len(steady_identities) / steady_elapsed, 3),
                    "durable_records_during_window": steady_durable,
                    "durable_records_per_second": round(steady_durable / steady_elapsed, 3),
                    "queue_start_depth": baseline["deferred_queue_depth"],
                    "queue_end_depth": steady_end["deferred_queue_depth"],
                    "queue_settled_depth": steady_settled["deferred_queue_depth"],
                    "queue_growth": int(steady_settled["deferred_queue_depth"]) - int(baseline["deferred_queue_depth"]),
                },
                "burst": {
                    "target_admission_records_per_second": burst_rate,
                    "admitted_records": len(burst_identities),
                    "admission_seconds": round(burst_elapsed, 6),
                    "actual_admission_records_per_second": round(len(burst_identities) / burst_elapsed, 3),
                    "durable_records_during_window": burst_durable,
                    "durable_records_per_second": round(burst_durable / burst_elapsed, 3),
                },
                "catch_up": {
                    "admitted_records": catch_up_records + 1,
                    "drain_seconds": round(catchup_elapsed, 6),
                    "drain_records_per_second": round((catch_up_records + 1) / catchup_elapsed, 3),
                    "queue_end_depth": after_catchup["deferred_queue_depth"],
                    "barrier_completed": len(barrier_result) == 1,
                    "barrier_ledger_sequence": None if not barrier_result else barrier_result[0]["ledger_sequence"],
                    "barrier_expected_ledger_sequence": (
                        warm_records + steady_records + burst_records + 1 + split
                    ),
                    "post_barrier_admitted_records": len(catch_up_items) - split + 1,
                },
                "shutdown": {
                    "queue_depth_before_close": before_close["deferred_queue_depth"],
                    "queue_non_empty_before_close": int(before_close["deferred_queue_depth"]) > 0,
                    "drain_seconds": round(close_elapsed, 6),
                    "accepted_records_total": len(all_admitted),
                    "accepted_shutdown_tail_records": len(shutdown_items),
                    "no_loss": no_loss,
                    "clean_shutdown": bool(close_receipt.get("clean_shutdown")),
                    "expected_durable_tip": {
                        "sequence": close_receipt.get("expected_tip_sequence"),
                        "record_hash": close_receipt.get("expected_tip_hash"),
                    },
                    "durable_tip": {
                        "sequence": close_receipt.get("durable_tip_sequence"),
                        "record_hash": close_receipt.get("durable_tip_hash"),
                    },
                    "checkpoint_complete": bool(
                        isinstance(close_receipt.get("checkpoint"), Mapping)
                        and close_receipt["checkpoint"].get("complete")
                    ),
                },
                "verification": {
                    "chain_valid": chain_valid,
                    "broken_identity": broken_identity,
                    "authority_watermark_valid": watermark_valid,
                    "final_highest_sequence": final_health["highest_sequence"],
                },
                "writer_telemetry": _benchmark_telemetry_summary(writer_telemetry_before_close),
                "observer_freshness": "NOT_MEASURED: direct ledger-writer benchmark; covered by starvation listener suite",
            }
        finally:
            if writer_release is not None:
                writer_release.set()
            if original_append is not None:
                ledger._append_prepared = original_append  # type: ignore[method-assign]
            if barrier_thread is not None and barrier_thread.is_alive():
                barrier_thread.join(5.0)
            if not closed:
                ledger.close()


def run_writer_benchmark(
    *,
    warm_records: int = 50_000,
    steady_seconds: float = 3.0,
    burst_seconds: float = 1.0,
    admission_rate: int = 1_259,
    burst_rate: int = 2_500,
    catch_up_records: int = 8_192,
    shutdown_tail_records: int = 4_096,
    catch_up_batch_size: int = 2_048,
) -> dict[str, object]:
    """Characterize clean and warm writer throughput using identical traffic."""
    if warm_records < 0:
        raise ValueError("Warm-record count must be non-negative.")
    if catch_up_records <= 0 or shutdown_tail_records <= 0:
        raise ValueError("Catch-up and shutdown-tail record counts must be positive.")
    if admission_rate <= 0 or burst_rate <= 0:
        raise ValueError("Admission rates must be positive.")
    return {
        "schema": "l3g-ledger-throughput-recovery-benchmark-v1",
        "safety": {
            "runtime_started": False,
            "commissioned": False,
            "armed": False,
            "orders_sent": False,
            "live_capital": False,
            "temporary_databases_only": True,
        },
        "parameters": {
            "warm_records": warm_records,
            "steady_seconds": steady_seconds,
            "burst_seconds": burst_seconds,
            "admission_rate": admission_rate,
            "burst_rate": burst_rate,
            "catch_up_records": catch_up_records,
            "shutdown_tail_records": shutdown_tail_records,
            "catch_up_batch_size": catch_up_batch_size,
        },
        "scenarios": [
            _benchmark_scenario(
                name="clean_small",
                warm_records=0,
                steady_seconds=steady_seconds,
                burst_seconds=burst_seconds,
                admission_rate=admission_rate,
                burst_rate=burst_rate,
                catch_up_records=catch_up_records,
                shutdown_tail_records=shutdown_tail_records,
                catch_up_batch_size=catch_up_batch_size,
            ),
            _benchmark_scenario(
                name="warm_large",
                warm_records=warm_records,
                steady_seconds=steady_seconds,
                burst_seconds=burst_seconds,
                admission_rate=admission_rate,
                burst_rate=burst_rate,
                catch_up_records=catch_up_records,
                shutdown_tail_records=shutdown_tail_records,
                catch_up_batch_size=catch_up_batch_size,
            ),
        ],
    }


def _threshold_failures(report: Mapping[str, object], minimum_durable_rate: float) -> list[str]:
    failures: list[str] = []
    scenarios = report.get("scenarios")
    if not isinstance(scenarios, list):
        return ["benchmark report has no scenarios"]
    for scenario in scenarios:
        if not isinstance(scenario, Mapping):
            failures.append("benchmark report contains an invalid scenario")
            continue
        name = str(scenario.get("scenario", "unknown"))
        steady = scenario.get("steady")
        catch_up = scenario.get("catch_up")
        shutdown = scenario.get("shutdown")
        verification = scenario.get("verification")
        if not isinstance(steady, Mapping):
            failures.append(f"{name}: steady-admission evidence is missing")
        else:
            target = float(steady.get("target_admission_records_per_second", 0))
            actual = float(steady.get("actual_admission_records_per_second", 0))
            if target <= 0 or actual < target * 0.98:
                failures.append(f"{name}: steady admission did not reach 98% of its target rate")
        if not isinstance(steady, Mapping) or int(steady.get("queue_growth", 1)) > 0:
            failures.append(f"{name}: queue grew during steady admission")
        if not isinstance(catch_up, Mapping):
            failures.append(f"{name}: controlled backlog-drain evidence is missing")
        else:
            if float(catch_up.get("drain_records_per_second", 0)) <= minimum_durable_rate:
                failures.append(
                    f"{name}: backlog-drain durable rate did not exceed {minimum_durable_rate:g} records/sec"
                )
            if int(catch_up.get("queue_end_depth", -1)) != 0:
                failures.append(f"{name}: catch-up did not drain the queue")
            if catch_up.get("barrier_completed") is not True:
                failures.append(f"{name}: commissioning barrier did not complete")
            if catch_up.get("barrier_ledger_sequence") != catch_up.get("barrier_expected_ledger_sequence"):
                failures.append(f"{name}: commissioning barrier did not preserve prefix ordering")
        if not isinstance(shutdown, Mapping) or not shutdown.get("queue_non_empty_before_close"):
            failures.append(f"{name}: shutdown was not exercised with a non-empty queue")
        if not isinstance(shutdown, Mapping) or not shutdown.get("no_loss"):
            failures.append(f"{name}: controlled shutdown lost or duplicated accepted records")
        if not isinstance(shutdown, Mapping) or not shutdown.get("clean_shutdown"):
            failures.append(f"{name}: controlled shutdown did not produce a clean receipt")
        if not isinstance(shutdown, Mapping) or not shutdown.get("checkpoint_complete"):
            failures.append(f"{name}: controlled shutdown did not complete the WAL checkpoint")
        if not isinstance(verification, Mapping) or verification.get("chain_valid") is not True:
            failures.append(f"{name}: hash chain verification failed")
        if not isinstance(verification, Mapping) or verification.get("authority_watermark_valid") is not True:
            failures.append(f"{name}: authority watermark validation failed")
    return failures


def _benchmark_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--benchmark", action="store_true", help="Run the explicit temporary-database benchmark instead of unittest.")
    parser.add_argument("--quick", action="store_true", help="Use a short local characterization rather than the default benchmark size.")
    parser.add_argument("--warm-records", type=int, default=50_000)
    parser.add_argument("--steady-seconds", type=float, default=3.0)
    parser.add_argument("--burst-seconds", type=float, default=1.0)
    parser.add_argument("--admission-rate", type=int, default=1_259)
    parser.add_argument("--burst-rate", type=int, default=2_500)
    parser.add_argument("--catch-up-records", type=int, default=8_192)
    parser.add_argument("--shutdown-tail-records", type=int, default=4_096)
    parser.add_argument("--catch-up-batch-size", type=int, default=2_048)
    parser.add_argument(
        "--require-threshold",
        action="store_true",
        help=(
            "Exit nonzero unless every scenario drains a controlled production-shaped backlog "
            "above 1,500 durable records/sec, holds the 1,259/sec steady queue flat, "
            "and passes all correctness gates."
        ),
    )
    return parser


def _main(argv: Sequence[str]) -> int:
    parser = _benchmark_parser()
    options, unittest_args = parser.parse_known_args(argv)
    if not options.benchmark:
        unittest.main(argv=[sys.argv[0], *unittest_args])
        return 0
    if options.quick:
        options.warm_records = min(options.warm_records, 2_000)
        options.steady_seconds = min(options.steady_seconds, 1.0)
        options.burst_seconds = min(options.burst_seconds, 0.5)
        options.catch_up_records = min(options.catch_up_records, 512)
        options.shutdown_tail_records = min(options.shutdown_tail_records, 512)
    report = run_writer_benchmark(
        warm_records=options.warm_records,
        steady_seconds=options.steady_seconds,
        burst_seconds=options.burst_seconds,
        admission_rate=options.admission_rate,
        burst_rate=options.burst_rate,
        catch_up_records=options.catch_up_records,
        shutdown_tail_records=options.shutdown_tail_records,
        catch_up_batch_size=options.catch_up_batch_size,
    )
    print(json.dumps(report, sort_keys=True, indent=2, default=str))
    if not options.require_threshold:
        return 0
    failures = _threshold_failures(report, minimum_durable_rate=1_500.0)
    if failures:
        print("\nTHRESHOLD FAILURES:", file=sys.stderr)
        for failure in failures:
            print("- " + failure, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(_main(sys.argv[1:]))
