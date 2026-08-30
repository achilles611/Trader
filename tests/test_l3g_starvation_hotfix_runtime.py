from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import threading
import unittest
from unittest.mock import patch

from src.lane_iii.contracts import canonical_hash
from src.l3f_provider.tradovate_observation import StreamHealth
from src.l3g_paper.contracts import PaperEntryOwner, PaperRuntimeState
from src.l3g_paper.ledger import PaperLedger
from src.l3g_paper.ninjatrader_transport import (
    ADDON_PROTOCOL_VERSION,
    EXECUTION_SCHEMA,
    PaperExecutionTransport,
    expected_addon_source_fingerprint,
    sign_payload,
)
from src.l3g_paper.risk import PaperRiskSnapshot
from src.l3g_paper.runtime import LaneIIIPaperRuntime
from src.l3g_paper.sessions import PaperSessionResolver

from tests.l3g_helpers import ObservationFactory, warmed_bullish_policy


NOW = "2026-08-26T14:00:00Z"
STALE_REASON = "COMMISSIONING_READINESS_SNAPSHOT_STALE"


class LaneIIIStarvationHotfixRuntimeTests(unittest.TestCase):
    def ready_runtime(
        self, directory: str,
    ) -> tuple[PaperLedger, LaneIIIPaperRuntime]:
        context = PaperSessionResolver().resolve(NOW, generation=1).context
        ledger = PaperLedger(Path(directory) / "paper.sqlite3")
        runtime = LaneIIIPaperRuntime(ledger)
        transport = PaperExecutionTransport(ledger, port=48341)
        runtime.bind_transport(transport)
        transport._addon_protocol_version = ADDON_PROTOCOL_VERSION
        transport._addon_source_fingerprint = expected_addon_source_fingerprint()
        runtime._execution_session_id = lambda: "l3g-es-starvation-hotfix-test"  # type: ignore[method-assign]
        runtime._state = PaperRuntimeState.READY_DISARMED
        runtime._session_context = context
        runtime._commissioning_warmup_context = context
        runtime._commissioning_warmup_warmed_at = NOW
        runtime._snapshot = PaperRiskSnapshot(
            NOW,
            position_snapshot_complete=True,
            order_snapshot_complete=True,
            reconciliation_current=True,
            local_bridge_healthy=True,
            market_price_connected=True,
            execution_bridge_healthy=True,
            evidence_warmed=True,
            commissioning_session_warmed=True,
            depth_reset_recovery=False,
            quote_observed_at=NOW,
            classified_trade_observed_at=NOW,
            depth_mutation_observed_at=NOW,
            session_kind=context.session_kind,
            session_id=context.session_id,
            trade_date=context.trade_date,
            session_profile_hash=context.session_profile_hash,
            session_generation=context.session_generation,
        )
        runtime._last_quote = (Decimal("100"), Decimal("100.25"), NOW)
        return ledger, runtime

    @staticmethod
    def close(runtime: LaneIIIPaperRuntime, ledger: PaperLedger) -> None:
        runtime.stop()
        ledger.close()

    @staticmethod
    def accepted_preflight(
        commissioning_id: str, runtime_snapshot: object,
    ) -> dict[str, object]:
        return {
            "ledger_trust_state": "TEST_VERIFIED_ANCHOR",
            "commissioning_id": commissioning_id,
        }

    def assert_no_commissioning_authority(
        self, runtime: LaneIIIPaperRuntime, ledger: PaperLedger,
    ) -> None:
        status = runtime.status()
        self.assertEqual(status["entry_owner"], PaperEntryOwner.NONE.value)
        self.assertFalse(status["commissioning_lifecycle"]["active"])
        kinds = {record["kind"] for record in ledger.recent(100)}
        self.assertTrue({
            "COMMISSIONING_PREFLIGHT_ACCEPTED",
            "COMMISSIONING_OWNERSHIP_RESERVED",
            "COMMISSIONING_ENTRY_AUTHORIZED",
            "COMMISSIONING_ENTRY_CONSUMED",
            "COMMISSIONING_ENTRY_SUBMITTED",
            "COMMAND",
        }.isdisjoint(kinds), kinds)

    def test_warmup_attestations_never_globally_drain_the_observer_callback(self) -> None:
        with TemporaryDirectory() as directory:
            ledger, runtime = self.ready_runtime(directory)
            provenance = {
                family: {
                    "evidence_id": f"l3g-pe-{index:032x}",
                    "observed_at": NOW,
                    "source_observation_ids": [f"nt-warmup-{index}"],
                    "source_local_sequences": [index],
                }
                for index, family in enumerate(
                    ("STRUCTURAL_CONTEXT", "ORDER_FLOW", "RESTING_LIQUIDITY"),
                    start=1,
                )
            }
            runtime._commissioning_warmup_seen = {
                "STRUCTURAL_CONTEXT": provenance["STRUCTURAL_CONTEXT"],
            }

            try:
                # The former synchronous append() path calls flush_deferred().
                # Refusing that call makes this a deterministic regression for
                # the production gap -> reset -> global drain feedback loop.
                with patch.object(
                    ledger,
                    "flush_deferred",
                    side_effect=AssertionError("observer callback attempted a global ledger drain"),
                ):
                    with runtime._lock:
                        runtime._reset_commissioning_warmup("LOCAL_SEQUENCE_GAP")
                        runtime._commissioning_warmup_seen = dict(provenance)
                        runtime._commissioning_warmup_warmed_at = None
                        runtime._snapshot = replace(
                            runtime._snapshot, commissioning_session_warmed=False,
                        )
                        runtime._observe_commissioning_warmup(NOW)

                self.assertTrue(runtime._snapshot.commissioning_session_warmed)
                ledger.flush_deferred()
                with ledger._lock:
                    attestations = ledger._connection.execute(
                        "SELECT kind FROM lane_iii_paper_audit "
                        "WHERE kind IN (?, ?) ORDER BY ledger_sequence",
                        (
                            "COMMISSIONING_SESSION_WARMUP_RESET",
                            "COMMISSIONING_SESSION_WARMED",
                        ),
                    ).fetchall()
                self.assertEqual(
                    [str(record["kind"]) for record in attestations],
                    ["COMMISSIONING_SESSION_WARMUP_RESET", "COMMISSIONING_SESSION_WARMED"],
                )
                self.assertEqual(ledger.verify_chain(), (True, None))
            finally:
                self.close(runtime, ledger)

    def test_status_never_holds_the_runtime_lock_while_ledger_health_waits(self) -> None:
        with TemporaryDirectory() as directory:
            ledger, runtime = self.ready_runtime(directory)
            health_started = threading.Event()
            health_release = threading.Event()
            status_done = threading.Event()
            ingest_done = threading.Event()
            errors: list[BaseException] = []
            status_result: dict[str, object] = {}
            original_health_status = ledger.health_status
            factory = ObservationFactory(
                start=datetime.fromisoformat(NOW.replace("Z", "+00:00")),
            )

            def blocked_health_status() -> dict[str, object]:
                health_started.set()
                if not health_release.wait(5):
                    raise AssertionError("Timed out releasing blocked ledger telemetry.")
                return original_health_status()

            def read_status() -> None:
                try:
                    status_result.update(runtime.status())
                except BaseException as error:
                    errors.append(error)
                finally:
                    status_done.set()

            def ingest_quote() -> None:
                try:
                    runtime.ingest(factory.quote(100))
                except BaseException as error:
                    errors.append(error)
                finally:
                    ingest_done.set()

            status_thread = threading.Thread(target=read_status, name="BlockedLedgerStatus")
            ingest_thread = threading.Thread(target=ingest_quote, name="IngestDuringLedgerStatus")
            try:
                with patch.object(ledger, "health_status", side_effect=blocked_health_status):
                    status_thread.start()
                    self.assertTrue(health_started.wait(2))

                    # This acquisition and the real callback both failed under
                    # the former status -> runtime lock -> ledger lock order.
                    self.assertTrue(runtime._lock.acquire(timeout=1))
                    runtime._lock.release()
                    ingest_thread.start()
                    self.assertTrue(
                        ingest_done.wait(1),
                        "live ingest waited behind informational ledger telemetry",
                    )

                    health_release.set()
                    status_thread.join(3)
                    ingest_thread.join(3)
                    self.assertFalse(status_thread.is_alive())
                    self.assertFalse(ingest_thread.is_alive())
                    self.assertTrue(status_done.is_set())
                    self.assertEqual(errors, [])
                    self.assertIn("ledger", status_result)
                    self.assertGreaterEqual(
                        int(status_result["ledger"]["highest_sequence"]), 1,
                    )
            finally:
                health_release.set()
                if status_thread.ident is not None:
                    status_thread.join(2)
                if ingest_thread.ident is not None:
                    ingest_thread.join(2)
                self.close(runtime, ledger)

    def test_sequence_gap_reset_returns_while_the_deferred_writer_is_blocked(self) -> None:
        with TemporaryDirectory() as directory:
            ledger = PaperLedger(Path(directory) / "paper.sqlite3")
            runtime = LaneIIIPaperRuntime(ledger)
            factory = ObservationFactory(
                start=datetime.fromisoformat(NOW.replace("Z", "+00:00")),
            )
            writer_started = threading.Event()
            release_writer = threading.Event()
            ingest_done = threading.Event()
            ingest_errors: list[BaseException] = []
            ingest_thread: threading.Thread | None = None
            original_append_prepared = ledger._append_prepared

            try:
                runtime.on_observation_transport_state(StreamHealth.HEALTHY)
                runtime.ingest(factory.make(
                    "CONNECTION", {"scope": "MARKET_DATA", "price_status": "Connected"},
                ))
                for price in (100, 99, 100):
                    quote = factory.quote(price)
                    runtime.ingest(quote)
                    runtime.ingest(factory.trade(quote, price))
                for operation, volume in (
                    ("ADD", 10), ("UPDATE", 5), ("UPDATE", 10),
                    ("UPDATE", 5), ("UPDATE", 11),
                ):
                    runtime.ingest(factory.depth(operation, volume))
                self.assertTrue(runtime.status()["commissioning_session_warmed"])
                ledger.flush_deferred()
                baseline = int(ledger.health_status()["highest_sequence"])

                def gated_append(records: tuple[dict[str, object], ...]) -> list[str]:
                    if any(str(record["identity"]) == "blocked-writer-prefix" for record in records):
                        writer_started.set()
                        if not release_writer.wait(10):
                            raise AssertionError("Timed out releasing the blocked deferred writer.")
                    return original_append_prepared(records)

                ledger._append_prepared = gated_append  # type: ignore[method-assign]
                context = runtime._session_context
                blocker = {
                    **context.payload(),
                    "observation_id": "nt-blocked-writer-prefix",
                    "observation_type": "QUOTE",
                    "observed_at": NOW,
                    "ninja_receipt_time": NOW,
                    "provider_timestamp": None,
                    "exchange_timestamp": None,
                    "local_monotonic_sequence": 0,
                    "source_payload_hash": canonical_hash({"blocker": True}),
                }
                ledger.append_deferred(
                    "OBSERVATION_ENVELOPE", blocker,
                    identity="blocked-writer-prefix", occurred_at=NOW,
                )
                self.assertTrue(writer_started.wait(2))

                # Manufacture one authentic continuity gap.  The old RESET
                # append() waited for the blocked writer/global drain here.
                factory.sequence += 1
                gap = factory.quote(100)

                def ingest_gap() -> None:
                    try:
                        runtime.ingest(gap)
                    except BaseException as exc:  # pragma: no cover - surfaced below
                        ingest_errors.append(exc)
                    finally:
                        ingest_done.set()

                ingest_thread = threading.Thread(target=ingest_gap, name="GapWhileWriterBlocked")
                ingest_thread.start()
                self.assertTrue(
                    ingest_done.wait(1),
                    "sequence-gap callback waited for a global deferred drain",
                )
                self.assertEqual(ingest_errors, [])
                with runtime._lock:
                    self.assertFalse(runtime._snapshot.commissioning_session_warmed)

                release_writer.set()
                ingest_thread.join(3)
                self.assertFalse(ingest_thread.is_alive())
                snapshot = ledger.commissioning_tail_snapshot(
                    baseline, last_full_verified_sequence=baseline,
                )
                with ledger._lock:
                    rows = ledger._connection.execute(
                        "SELECT kind, identity FROM lane_iii_paper_audit "
                        "WHERE ledger_sequence > ? ORDER BY ledger_sequence",
                        (baseline,),
                    ).fetchall()
                kinds = [str(row["kind"]) for row in rows]
                self.assertEqual(kinds[:4], [
                    "OBSERVATION_ENVELOPE",
                    "OBSERVATION_ENVELOPE",
                    "COMMISSIONING_SESSION_WARMUP_RESET",
                    "DECISION",
                ])
                self.assertEqual(sum(kind == "COMMISSIONING_SESSION_WARMUP_RESET" for kind in kinds), 1)
                self.assertEqual(snapshot["last_authority_observation_kind"], "COMMISSIONING_SESSION_WARMUP_RESET")
                self.assertEqual(ledger.verify_chain(), (True, None))
            finally:
                release_writer.set()
                if ingest_thread is not None:
                    ingest_thread.join(2)
                ledger._append_prepared = original_append_prepared  # type: ignore[method-assign]
                self.close(runtime, ledger)

    def test_rehearsal_callback_does_not_hold_runtime_lock(self) -> None:
        with TemporaryDirectory() as directory:
            ledger, runtime = self.ready_runtime(directory)
            callback_entered = threading.Event()
            release_callback = threading.Event()
            result: dict[str, object] = {}
            errors: list[BaseException] = []
            before_sequence = ledger.health_status()["highest_sequence"]

            def preflight(
                commissioning_id: str, runtime_snapshot: object,
            ) -> dict[str, object]:
                callback_entered.set()
                release_callback.wait(5)
                return self.accepted_preflight(commissioning_id, runtime_snapshot)

            def rehearse() -> None:
                try:
                    result.update(runtime.commissioning_rehearsal(preflight))
                except BaseException as exc:  # pragma: no cover - surfaced below
                    errors.append(exc)

            worker = threading.Thread(target=rehearse)
            try:
                with patch("src.l3g_paper.runtime._now", return_value=NOW):
                    worker.start()
                    self.assertTrue(callback_entered.wait(2))
                    acquired = runtime._lock.acquire(timeout=0.5)
                    self.assertTrue(acquired, "ledger preflight retained the runtime lock")
                    if acquired:
                        runtime._lock.release()
                    release_callback.set()
                    worker.join(3)

                self.assertFalse(worker.is_alive())
                self.assertEqual(errors, [])
                self.assertEqual(result["result"], "READY", result)
                self.assertFalse(result["runtime_snapshot"]["stale"])  # type: ignore[index]
                self.assertEqual(ledger.health_status()["highest_sequence"], before_sequence)
                self.assert_no_commissioning_authority(runtime, ledger)
            finally:
                release_callback.set()
                worker.join(1)
                self.close(runtime, ledger)

    def test_authority_mutation_during_arm_preflight_fails_stale(self) -> None:
        def reconciliation(runtime: LaneIIIPaperRuntime) -> None:
            runtime._snapshot = replace(runtime._snapshot, reconciliation_current=False)

        def session(runtime: LaneIIIPaperRuntime) -> None:
            next_context = PaperSessionResolver().resolve(NOW, generation=2).context
            runtime._session_generation = 2
            runtime._set_session_context(next_context, reason="TEST_PREFLIGHT_SESSION_MUTATION")

        def state(runtime: LaneIIIPaperRuntime) -> None:
            runtime._transition(PaperRuntimeState.RECONCILING, "TEST_PREFLIGHT_STATE_MUTATION")

        def warmup(runtime: LaneIIIPaperRuntime) -> None:
            runtime._reset_commissioning_warmup("TEST_PREFLIGHT_WARMUP_MUTATION")

        mutations = {
            "reconciliation": reconciliation,
            "session": session,
            "state": state,
            "warmup": warmup,
        }
        for name, mutate in mutations.items():
            with self.subTest(mutation=name), TemporaryDirectory() as directory:
                ledger, runtime = self.ready_runtime(directory)
                callback_entered = threading.Event()
                release_callback = threading.Event()
                result: dict[str, object] = {}
                errors: list[BaseException] = []

                def preflight(
                    commissioning_id: str, runtime_snapshot: object,
                ) -> dict[str, object]:
                    callback_entered.set()
                    release_callback.wait(5)
                    return self.accepted_preflight(commissioning_id, runtime_snapshot)

                def arm() -> None:
                    try:
                        result.update(runtime.commissioning_arm(preflight))
                    except BaseException as exc:  # pragma: no cover - surfaced below
                        errors.append(exc)

                worker = threading.Thread(target=arm)
                try:
                    with patch("src.l3g_paper.runtime._now", return_value=NOW):
                        worker.start()
                        self.assertTrue(callback_entered.wait(2))
                        with runtime._lock:
                            mutate(runtime)
                        release_callback.set()
                        worker.join(3)

                    self.assertFalse(worker.is_alive())
                    self.assertEqual(errors, [])
                    self.assertFalse(result["armed"], result)
                    self.assertIn(STALE_REASON, result["reason_codes"])
                    self.assertGreaterEqual(
                        runtime.status()["commissioning_stale_snapshot_refusal_count"], 1,
                    )
                    self.assert_no_commissioning_authority(runtime, ledger)
                finally:
                    release_callback.set()
                    worker.join(1)
                    self.close(runtime, ledger)

    def test_freshness_only_timestamp_advancement_does_not_stale(self) -> None:
        with TemporaryDirectory() as directory:
            ledger, runtime = self.ready_runtime(directory)
            prior = "2026-08-26T13:59:59Z"
            runtime._snapshot = replace(
                runtime._snapshot,
                quote_observed_at=prior,
                classified_trade_observed_at=prior,
                depth_mutation_observed_at=prior,
            )
            callback_entered = threading.Event()
            release_callback = threading.Event()
            result: dict[str, object] = {}
            errors: list[BaseException] = []

            def preflight(
                commissioning_id: str, runtime_snapshot: object,
            ) -> dict[str, object]:
                callback_entered.set()
                release_callback.wait(5)
                return self.accepted_preflight(commissioning_id, runtime_snapshot)

            def rehearse() -> None:
                try:
                    result.update(runtime.commissioning_rehearsal(preflight))
                except BaseException as exc:  # pragma: no cover - surfaced below
                    errors.append(exc)

            worker = threading.Thread(target=rehearse)
            try:
                with patch("src.l3g_paper.runtime._now", return_value=NOW):
                    worker.start()
                    self.assertTrue(callback_entered.wait(2))
                    with runtime._lock:
                        runtime._snapshot = replace(
                            runtime._snapshot,
                            observed_at=NOW,
                            quote_observed_at=NOW,
                            classified_trade_observed_at=NOW,
                            depth_mutation_observed_at=NOW,
                        )
                    release_callback.set()
                    worker.join(3)

                self.assertFalse(worker.is_alive())
                self.assertEqual(errors, [])
                self.assertEqual(result["result"], "READY", result)
                snapshot = result["runtime_snapshot"]
                self.assertFalse(snapshot["stale"])  # type: ignore[index]
                self.assertEqual(snapshot["token"], snapshot["current_token"])  # type: ignore[index]
                self.assertEqual(result["market_freshness"]["quote"]["age_seconds"], 0)  # type: ignore[index]
                self.assertEqual(
                    runtime.status()["commissioning_stale_snapshot_refusal_count"], 0,
                )
                self.assert_no_commissioning_authority(runtime, ledger)
            finally:
                release_callback.set()
                worker.join(1)
                self.close(runtime, ledger)

    def test_arm_reservation_remains_atomic_against_strategy_entry(self) -> None:
        with TemporaryDirectory() as directory:
            ledger, runtime = self.ready_runtime(directory)
            reservation_append_entered = threading.Event()
            release_reservation_append = threading.Event()
            strategy_started = threading.Event()
            original_append = ledger.append
            arm_result: dict[str, object] = {}
            strategy_result: list[bool] = []
            errors: list[BaseException] = []
            commands: list[object] = []

            def append(kind: str, payload: object, **kwargs: object) -> str:
                if kind == "COMMISSIONING_OWNERSHIP_RESERVED":
                    reservation_append_entered.set()
                    release_reservation_append.wait(5)
                return original_append(kind, payload, **kwargs)  # type: ignore[arg-type]

            ledger.append = append  # type: ignore[method-assign]
            runtime._persist_and_send = lambda command, grant: commands.append(command)  # type: ignore[method-assign]

            def arm() -> None:
                try:
                    arm_result.update(runtime.commissioning_arm(self.accepted_preflight))
                except BaseException as exc:  # pragma: no cover - surfaced below
                    errors.append(exc)

            def strategy() -> None:
                strategy_started.set()
                strategy_result.append(
                    runtime._request_entry(warmed_bullish_policy()[2]),  # type: ignore[arg-type]
                )

            arm_thread = threading.Thread(target=arm)
            strategy_thread = threading.Thread(target=strategy)
            try:
                with patch("src.l3g_paper.runtime._now", return_value=NOW):
                    arm_thread.start()
                    self.assertTrue(reservation_append_entered.wait(2))
                    strategy_thread.start()
                    self.assertTrue(strategy_started.wait(1))
                    strategy_thread.join(0.2)
                    self.assertTrue(
                        strategy_thread.is_alive(),
                        "strategy crossed the locked commissioning reservation boundary",
                    )
                    release_reservation_append.set()
                    arm_thread.join(3)
                    strategy_thread.join(3)

                self.assertFalse(arm_thread.is_alive())
                self.assertFalse(strategy_thread.is_alive())
                self.assertEqual(errors, [])
                self.assertTrue(arm_result["armed"], arm_result)
                self.assertEqual(strategy_result, [False])
                self.assertEqual(commands, [])
                self.assertEqual(runtime.status()["entry_owner"], PaperEntryOwner.COMMISSIONING.value)
            finally:
                release_reservation_append.set()
                arm_thread.join(1)
                strategy_thread.join(1)
                self.close(runtime, ledger)

    def test_final_arm_fence_orders_external_receipt_after_durable_reservation(self) -> None:
        with TemporaryDirectory() as directory:
            ledger, runtime = self.ready_runtime(directory)
            ledger.append(
                "SESSION_AUTHORITY",
                {"reason": "verified test anchor"},
                identity="l3g-starvation-final-fence-anchor",
            )
            anchor = int(ledger.health_status()["highest_sequence"])
            trigger_receipt = threading.Event()
            receipt_started = threading.Event()
            receipt_finished = threading.Event()
            receipt_errors: list[BaseException] = []
            callback_calls = 0
            receipt_finished_inside_final_callback: list[bool] = []

            def append_external_receipt() -> None:
                try:
                    if not trigger_receipt.wait(5):
                        raise TimeoutError("final ARM callback did not trigger the external receipt")
                    receipt_started.set()
                    ledger.append(
                        "COMMAND_RECEIPT_EXTERNAL",
                        {"reason": "authority receipt racing final ARM fence"},
                        identity="l3g-starvation-receipt-after-final-fence",
                    )
                    receipt_finished.set()
                except BaseException as exc:  # pragma: no cover - surfaced below
                    receipt_errors.append(exc)
                    receipt_finished.set()

            def preflight(
                commissioning_id: str, runtime_snapshot: object,
            ) -> dict[str, object]:
                nonlocal callback_calls
                callback_calls += 1
                evidence = ledger.commissioning_tail_snapshot(
                    anchor, last_full_verified_sequence=anchor,
                )
                if callback_calls == 2:
                    trigger_receipt.set()
                    if not receipt_started.wait(2):
                        raise TimeoutError("external receipt writer did not start")
                    receipt_finished_inside_final_callback.append(receipt_finished.wait(0.5))
                return evidence

            receipt_thread = threading.Thread(target=append_external_receipt)
            receipt_thread.start()
            try:
                with patch("src.l3g_paper.runtime._now", return_value=NOW):
                    result = runtime.commissioning_arm(preflight)
                receipt_thread.join(3)

                self.assertFalse(receipt_thread.is_alive())
                self.assertEqual(receipt_errors, [])
                self.assertTrue(result["armed"], result)
                self.assertEqual(callback_calls, 2)
                self.assertEqual(
                    receipt_finished_inside_final_callback, [False],
                    "external authority receipt crossed the final ledger admission fence",
                )
                with ledger._lock:  # test-only exact ordering proof after both writers finish
                    rows = ledger._connection.execute(
                        "SELECT kind, ledger_sequence FROM lane_iii_paper_audit "
                        "WHERE kind IN ('COMMAND_RECEIPT_EXTERNAL', "
                        "'COMMISSIONING_OWNERSHIP_RESERVED', 'SESSION_TRANSITION')"
                    ).fetchall()
                sequences = {str(row["kind"]): int(row["ledger_sequence"]) for row in rows}
                self.assertGreater(
                    sequences["COMMAND_RECEIPT_EXTERNAL"],
                    sequences["COMMISSIONING_OWNERSHIP_RESERVED"],
                )
                self.assertGreater(
                    sequences["COMMAND_RECEIPT_EXTERNAL"],
                    sequences["SESSION_TRANSITION"],
                )
            finally:
                trigger_receipt.set()
                receipt_thread.join(1)
                self.close(runtime, ledger)

    def test_atomic_start_fence_orders_external_receipt_after_command_submission(self) -> None:
        with TemporaryDirectory() as directory:
            ledger, runtime = self.ready_runtime(directory)
            trigger_receipt = threading.Event()
            receipt_started = threading.Event()
            receipt_finished = threading.Event()
            receipt_errors: list[BaseException] = []
            receipt_finished_before_entry: list[bool] = []
            commands: list[object] = []
            original_commission_entry = runtime.commission_entry
            runtime._persist_and_send = lambda command, grant: commands.append(command)  # type: ignore[method-assign]

            def append_external_receipt() -> None:
                try:
                    if not trigger_receipt.wait(5):
                        raise TimeoutError("atomic start did not trigger the external receipt")
                    receipt_started.set()
                    ledger.append(
                        "POSITION_SNAPSHOT_EVENT",
                        {"reason": "position receipt racing atomic command admission", "position_quantity": 1},
                        identity="l3g-starvation-receipt-racing-command",
                    )
                    receipt_finished.set()
                except BaseException as exc:  # pragma: no cover - surfaced below
                    receipt_errors.append(exc)
                    receipt_finished.set()

            def commission_entry(commissioning_id: str, commissioning_token: str) -> dict[str, object]:
                trigger_receipt.set()
                if not receipt_started.wait(2):
                    raise TimeoutError("external receipt writer did not start")
                receipt_finished_before_entry.append(receipt_finished.wait(0.5))
                return original_commission_entry(commissioning_id, commissioning_token)

            runtime.commission_entry = commission_entry  # type: ignore[method-assign]
            receipt_thread = threading.Thread(target=append_external_receipt)
            receipt_thread.start()
            try:
                with patch("src.l3g_paper.runtime._now", return_value=NOW):
                    result = runtime.commissioning_start(
                        "starvation-command-race-0001", self.accepted_preflight,
                    )
                receipt_thread.join(3)

                self.assertFalse(receipt_thread.is_alive())
                self.assertEqual(receipt_errors, [])
                self.assertTrue(result["submitted"], result)
                self.assertEqual(len(commands), 1)
                self.assertEqual(receipt_finished_before_entry, [False])
                with ledger._lock:  # test-only exact persisted order
                    rows = ledger._connection.execute(
                        "SELECT kind, ledger_sequence FROM lane_iii_paper_audit "
                        "WHERE kind IN ('POSITION_SNAPSHOT_EVENT', "
                        "'COMMISSIONING_ENTRY_SUBMITTED')"
                    ).fetchall()
                sequences = {str(row["kind"]): int(row["ledger_sequence"]) for row in rows}
                self.assertGreater(
                    sequences["POSITION_SNAPSHOT_EVENT"],
                    sequences["COMMISSIONING_ENTRY_SUBMITTED"],
                )
            finally:
                trigger_receipt.set()
                receipt_thread.join(1)
                self.close(runtime, ledger)

    def test_two_step_entry_refuses_authority_receipt_persisted_after_arm(self) -> None:
        with TemporaryDirectory() as directory:
            ledger, runtime = self.ready_runtime(directory)
            commands: list[object] = []
            runtime._persist_and_send = lambda command, grant: commands.append(command)  # type: ignore[method-assign]
            try:
                with patch("src.l3g_paper.runtime._now", return_value=NOW):
                    armed = runtime.commissioning_arm(self.accepted_preflight)
                    ledger.append(
                        "POSITION_SNAPSHOT_EVENT",
                        {"reason": "authority receipt admitted between ARM and entry", "position_quantity": 1},
                        identity="l3g-starvation-receipt-between-arm-entry",
                    )
                    result = runtime.commission_entry(
                        str(armed["commissioning_id"]), str(armed["commissioning_token"]),
                    )

                self.assertTrue(armed["armed"], armed)
                self.assertFalse(result["submitted"], result)
                self.assertEqual(
                    result["reason_codes"],
                    ("COMMISSIONING_LEDGER_AUTHORITY_CHANGED_AFTER_ARM",),
                )
                self.assertEqual(commands, [])
                self.assertEqual(runtime.status()["entry_owner"], PaperEntryOwner.NONE.value)
            finally:
                self.close(runtime, ledger)

    def test_transport_ingress_linearizes_before_two_step_entry(self) -> None:
        with TemporaryDirectory() as directory:
            ledger, runtime = self.ready_runtime(directory)
            transport = runtime._transport
            self.assertIsInstance(transport, PaperExecutionTransport)
            assert isinstance(transport, PaperExecutionTransport)
            key = bytes(range(32))
            execution_session_id = "l3g-es-starvation-ingress-test"
            with transport._lock:
                transport._key = key
                transport._state = "AUTHENTICATED"
                transport._authenticated = True
                transport._reconciled = True
                transport._execution_session_id = execution_session_id
                transport._on_message = runtime.on_execution_message
            commands: list[object] = []
            runtime._persist_and_send = lambda command, grant: commands.append(command)  # type: ignore[method-assign]
            receipt_append_entered = threading.Event()
            release_receipt_append = threading.Event()
            original_append = ledger.append

            def append(kind: str, payload: object, **kwargs: object) -> str:
                if kind == "POSITION_SNAPSHOT_EVENT":
                    receipt_append_entered.set()
                    if not release_receipt_append.wait(5):
                        raise TimeoutError("transport receipt append was not released")
                return original_append(kind, payload, **kwargs)  # type: ignore[arg-type]

            ledger.append = append  # type: ignore[method-assign]
            payload: dict[str, object] = {
                "schema": EXECUTION_SCHEMA,
                "message_type": "POSITION_EVENT",
                "execution_session_id": execution_session_id,
                "receipt_id": "l3g-starvation-ingress-position",
                "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                "quantity": 1,
            }
            payload["signature"] = sign_payload(key, payload)
            frame = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
            receipt_errors: list[BaseException] = []
            entry_errors: list[BaseException] = []
            entry_result: list[dict[str, object]] = []

            def receive() -> None:
                try:
                    transport._receive_frame(frame)
                except BaseException as exc:  # pragma: no cover - surfaced below
                    receipt_errors.append(exc)

            with patch("src.l3g_paper.runtime._now", return_value=NOW):
                armed = runtime.commissioning_arm(self.accepted_preflight)

            def enter() -> None:
                try:
                    with patch("src.l3g_paper.runtime._now", return_value=NOW):
                        entry_result.append(runtime.commission_entry(
                            str(armed["commissioning_id"]), str(armed["commissioning_token"]),
                        ))
                except BaseException as exc:  # pragma: no cover - surfaced below
                    entry_errors.append(exc)

            receipt_thread = threading.Thread(target=receive)
            entry_thread = threading.Thread(target=enter)
            try:
                receipt_thread.start()
                self.assertTrue(receipt_append_entered.wait(2))
                entry_thread.start()
                entry_thread.join(0.2)
                self.assertTrue(
                    entry_thread.is_alive(),
                    "commissioning entry crossed accepted transport ingress before receipt persistence",
                )
                release_receipt_append.set()
                receipt_thread.join(3)
                entry_thread.join(3)

                self.assertFalse(receipt_thread.is_alive())
                self.assertFalse(entry_thread.is_alive())
                self.assertEqual(receipt_errors, [])
                self.assertEqual(entry_errors, [])
                self.assertEqual(len(entry_result), 1)
                self.assertFalse(entry_result[0]["submitted"], entry_result[0])
                self.assertEqual(
                    entry_result[0]["reason_codes"],
                    ("COMMISSIONING_LEDGER_AUTHORITY_CHANGED_AFTER_ARM",),
                )
                self.assertEqual(commands, [])
                self.assertEqual(runtime.status()["current_position"], "LONG")
            finally:
                release_receipt_append.set()
                receipt_thread.join(1)
                entry_thread.join(1)
                self.close(runtime, ledger)

    def test_two_step_entry_refuses_execution_session_rollover_after_arm(self) -> None:
        with TemporaryDirectory() as directory:
            ledger, runtime = self.ready_runtime(directory)
            transport = runtime._transport
            self.assertIsInstance(transport, PaperExecutionTransport)
            assert isinstance(transport, PaperExecutionTransport)
            transport._send_signed = lambda payload: None  # type: ignore[method-assign]
            commands: list[object] = []
            runtime._persist_and_send = lambda command, grant: commands.append(command)  # type: ignore[method-assign]
            try:
                with patch("src.l3g_paper.runtime._now", return_value=NOW):
                    armed = runtime.commissioning_arm(self.accepted_preflight)
                transport._handle_hello({
                    "schema": EXECUTION_SCHEMA,
                    "message_type": "HELLO",
                    "bridge_instance_id": "l3g-starvation-rollover-bridge",
                    "ninjatrader_session_id": "l3g-starvation-rollover-session",
                    "addon_protocol_version": ADDON_PROTOCOL_VERSION,
                    "addon_source_fingerprint": expected_addon_source_fingerprint(),
                    "addon_build_fingerprint": "a" * 64,
                    "addon_build_timestamp": "2026-08-28T00:00:00Z",
                    "account_name": "Sim101",
                    "account_class": "LOCAL_SIMULATION",
                    "instrument": "MNQ SEP26",
                    "capability": "PAPER_ONLY",
                    "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                    "nonce": "l3g-starvation-rollover-nonce",
                    "signature": "unused-by-direct-handler",
                })
                with patch("src.l3g_paper.runtime._now", return_value=NOW):
                    result = runtime.commission_entry(
                        str(armed["commissioning_id"]), str(armed["commissioning_token"]),
                    )

                self.assertTrue(armed["armed"], armed)
                self.assertFalse(result["submitted"], result)
                self.assertEqual(
                    result["reason_codes"],
                    ("COMMISSIONING_LEDGER_AUTHORITY_CHANGED_AFTER_ARM",),
                )
                self.assertEqual(commands, [])
            finally:
                self.close(runtime, ledger)

    def test_concurrent_starts_keep_single_submit_after_unlocked_preflight(self) -> None:
        with TemporaryDirectory() as directory:
            ledger, runtime = self.ready_runtime(directory)
            first_preflights = threading.Barrier(2, timeout=3)
            thread_state = threading.local()
            results: list[dict[str, object]] = []
            errors: list[BaseException] = []
            commands: list[object] = []
            runtime._persist_and_send = lambda command, grant: commands.append(command)  # type: ignore[method-assign]

            def preflight(
                commissioning_id: str, runtime_snapshot: object,
            ) -> dict[str, object]:
                if not getattr(thread_state, "entered", False):
                    thread_state.entered = True
                    first_preflights.wait()
                return self.accepted_preflight(commissioning_id, runtime_snapshot)

            def start() -> None:
                try:
                    results.append(runtime.commissioning_start("starvation-race-0001", preflight))
                except BaseException as exc:  # pragma: no cover - surfaced below
                    errors.append(exc)

            first = threading.Thread(target=start)
            second = threading.Thread(target=start)
            try:
                with patch("src.l3g_paper.runtime._now", return_value=NOW):
                    first.start()
                    second.start()
                    first.join(6)
                    second.join(6)

                self.assertFalse(first.is_alive())
                self.assertFalse(second.is_alive())
                self.assertEqual(errors, [])
                self.assertEqual(len(results), 2)
                self.assertEqual(len(commands), 1)
                self.assertEqual(
                    sum(bool(result.get("idempotent_replay")) for result in results), 1,
                )
                self.assertTrue(all(result["submitted"] for result in results), results)
                self.assertEqual(
                    {str(result["commissioning_id"]) for result in results},
                    {str(results[0]["commissioning_id"])},
                )
            finally:
                first.join(1)
                second.join(1)
                self.close(runtime, ledger)


if __name__ == "__main__":
    unittest.main()
