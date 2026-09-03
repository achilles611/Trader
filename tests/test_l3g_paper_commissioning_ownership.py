from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
import threading
import unittest
from unittest.mock import patch

from src.l3g_paper.contracts import (
    PaperDecisionKind, PaperDirection, PaperEntryOwner, PaperRuntimeState, PaperSessionArmGrant,
)
from src.l3g_paper.ledger import PaperLedger
from src.l3g_paper.ninjatrader_transport import ADDON_PROTOCOL_VERSION, PaperExecutionTransport, expected_addon_source_fingerprint
from src.l3g_paper.risk import PaperRiskSnapshot
from src.l3g_paper.runtime import LaneIIIPaperRuntime
from src.l3g_paper.sessions import PaperSessionResolver, context_from_identity
from tests.l3g_helpers import ObservationFactory, warmed_bullish_policy


class CommissioningOwnershipTests(unittest.TestCase):
    now = "2026-08-26T14:00:00Z"

    def ready_runtime(self, directory: str, now: str | None = None) -> tuple[PaperLedger, LaneIIIPaperRuntime, object]:
        observed_at = now or self.now
        context = PaperSessionResolver().resolve(observed_at, generation=1).context
        ledger = PaperLedger(Path(directory) / "paper.sqlite3")
        runtime = LaneIIIPaperRuntime(ledger)
        transport = PaperExecutionTransport(ledger, port=48311)
        runtime.bind_transport(transport)
        transport._addon_protocol_version = ADDON_PROTOCOL_VERSION
        transport._addon_source_fingerprint = expected_addon_source_fingerprint()
        runtime._execution_session_id = lambda: "l3g-es-ownership-test"  # type: ignore[method-assign]
        runtime._state = PaperRuntimeState.READY_DISARMED
        runtime._session_context = context
        runtime._session_generation = context.session_generation
        runtime._commissioning_warmup_context = context
        runtime._commissioning_warmup_warmed_at = observed_at
        runtime._snapshot = PaperRiskSnapshot(
            observed_at, position_snapshot_complete=True, order_snapshot_complete=True,
            reconciliation_current=True, local_bridge_healthy=True, market_price_connected=True,
            execution_bridge_healthy=True, evidence_warmed=True,
            commissioning_session_warmed=True, depth_reset_recovery=False,
            quote_observed_at=observed_at, classified_trade_observed_at=observed_at,
            depth_mutation_observed_at=observed_at, session_kind=context.session_kind,
            session_id=context.session_id, trade_date=context.trade_date,
            session_profile_hash=context.session_profile_hash, session_generation=context.session_generation,
        )
        runtime._last_quote = (Decimal("100"), Decimal("100.25"), observed_at)
        source_candidate = warmed_bullish_policy()[2]
        runtime._last_qualifying_entry_decision = replace(
            source_candidate,
            created_at=observed_at,
            expires_at=(datetime.fromisoformat(observed_at.replace("Z", "+00:00")) + timedelta(seconds=5)).isoformat().replace("+00:00", "Z"),
            session_kind=context.session_kind,
            session_id=context.session_id,
            trade_date=context.trade_date,
            session_profile_hash=context.session_profile_hash,
            session_generation=context.session_generation,
        )
        return ledger, runtime, context

    @staticmethod
    def close(runtime: LaneIIIPaperRuntime, ledger: PaperLedger) -> None:
        runtime.stop()
        ledger.close()

    @staticmethod
    def strategy_decision() -> object:
        return warmed_bullish_policy()[2]

    def commissioning_arm(self, runtime: LaneIIIPaperRuntime, now: str | None = None) -> dict[str, object]:
        with patch("src.l3g_paper.runtime._now", return_value=now or self.now):
            result = runtime.commissioning_arm(
                lambda commissioning_id, runtime_snapshot: {
                    "ledger_trust_state": "TEST_VERIFIED_ANCHOR",
                    "commissioning_id": commissioning_id,
                }
            )
        self.assertTrue(result["armed"], result)
        return result

    def test_normal_strategy_entry_is_unchanged_without_a_commissioning_reservation(self) -> None:
        policy, _, decision = warmed_bullish_policy()
        now = decision.created_at  # type: ignore[attr-defined]
        context = context_from_identity(
            decision.session_kind, decision.session_id, decision.trade_date,  # type: ignore[attr-defined]
            decision.session_profile_hash, decision.session_generation,  # type: ignore[attr-defined]
        )
        with TemporaryDirectory() as directory:
            ledger, runtime, _ = self.ready_runtime(directory, now)
            runtime.policy = policy
            runtime._session_context = context
            runtime._snapshot = replace(
                runtime._snapshot, session_kind=context.session_kind, session_id=context.session_id,
                trade_date=context.trade_date, session_profile_hash=context.session_profile_hash,
                session_generation=context.session_generation,
            )
            runtime._state = PaperRuntimeState.ARMED_FLAT
            runtime._armed_session = PaperSessionArmGrant(
                context.session_kind, context.session_id, context.trade_date, context.session_profile_hash,
                context.session_generation, now, context.boundary_at("entry_cutoff").isoformat().replace("+00:00", "Z"),
            )
            commands: list[object] = []
            runtime._persist_and_send = lambda command, grant: commands.append(command)  # type: ignore[method-assign]
            with patch("src.l3g_paper.runtime._now", return_value=now):
                self.assertTrue(runtime._request_entry(decision))  # type: ignore[arg-type]
            self.assertEqual(len(commands), 1)
            self.assertEqual(runtime.status()["entry_owner"], PaperEntryOwner.STRATEGY.value)
            self.close(runtime, ledger)

    def test_strategy_is_suppressed_immediately_after_atomic_commissioning_arm(self) -> None:
        with TemporaryDirectory() as directory:
            ledger, runtime, _ = self.ready_runtime(directory)
            result = self.commissioning_arm(runtime)
            commands: list[object] = []
            runtime._persist_and_send = lambda command, grant: commands.append(command)  # type: ignore[method-assign]
            self.assertFalse(runtime._request_entry(self.strategy_decision()))  # type: ignore[arg-type]
            self.assertEqual(commands, [])
            self.assertEqual(runtime.status()["entry_owner"], PaperEntryOwner.COMMISSIONING.value)
            suppressed = [item for item in ledger.recent_kinds(("COMMISSIONING_STRATEGY_ENTRY_SUPPRESSED",))]
            self.assertEqual(len(suppressed), 1)
            self.assertEqual(suppressed[0]["payload"]["reason"], "COMMISSIONING_ENTRY_RESERVED")
            self.assertEqual(suppressed[0]["payload"]["commissioning_id"], result["commissioning_id"])
            self.close(runtime, ledger)

    def test_strategy_racing_commissioning_arm_cannot_cross_the_reserved_admission_boundary(self) -> None:
        with TemporaryDirectory() as directory:
            ledger, runtime, _ = self.ready_runtime(directory)
            reserved = threading.Event()
            contender_started = threading.Event()
            release_reservation = threading.Event()
            original_append = ledger.append

            def append(kind: str, payload: object, **kwargs: object) -> str:
                if kind == "COMMISSIONING_OWNERSHIP_RESERVED":
                    reserved.set()
                    self.assertTrue(contender_started.wait(2))
                    release_reservation.wait(2)
                return original_append(kind, payload, **kwargs)  # type: ignore[arg-type]

            ledger.append = append  # type: ignore[method-assign]
            commands: list[object] = []
            runtime._persist_and_send = lambda command, grant: commands.append(command)  # type: ignore[method-assign]
            arm_result: dict[str, object] = {}
            strategy_result: list[bool] = []

            def arm() -> None:
                with patch("src.l3g_paper.runtime._now", return_value=self.now):
                    arm_result.update(runtime.commissioning_arm(
                        lambda commissioning_id, runtime_snapshot: {
                            "ledger_trust_state": "TEST_VERIFIED_ANCHOR",
                            "commissioning_id": commissioning_id,
                        }
                    ))

            def strategy() -> None:
                self.assertTrue(reserved.wait(2))
                contender_started.set()
                strategy_result.append(runtime._request_entry(self.strategy_decision()))  # type: ignore[arg-type]

            arm_thread = threading.Thread(target=arm)
            strategy_thread = threading.Thread(target=strategy)
            arm_thread.start(); strategy_thread.start()
            self.assertTrue(contender_started.wait(2))
            release_reservation.set()
            arm_thread.join(2); strategy_thread.join(2)
            self.assertFalse(arm_thread.is_alive())
            self.assertFalse(strategy_thread.is_alive())
            self.assertTrue(arm_result["armed"])
            self.assertEqual(strategy_result, [False])
            self.assertEqual(commands, [])
            self.close(runtime, ledger)

    def test_passive_writer_can_append_across_arm_snapshot_without_crossing_atomic_ownership_boundary(self) -> None:
        with TemporaryDirectory() as directory:
            ledger, runtime, _ = self.ready_runtime(directory)
            anchor = int(ledger.health_status()["highest_sequence"])
            append_before_snapshot = threading.Event()
            before_snapshot_done = threading.Event()
            append_after_snapshot = threading.Event()
            after_snapshot_done = threading.Event()

            def passive_payload(number: int) -> dict[str, object]:
                return {
                    "observation_id": f"nt-concurrent-{number}",
                    "observation_type": "QUOTE",
                    "local_monotonic_sequence": number,
                    "source_payload_hash": f"hash-{number}",
                }

            def writer() -> None:
                self.assertTrue(append_before_snapshot.wait(2))
                ledger.append("OBSERVATION_ENVELOPE", passive_payload(1))
                before_snapshot_done.set()
                self.assertTrue(append_after_snapshot.wait(2))
                ledger.append("OBSERVATION_ENVELOPE", passive_payload(2))
                after_snapshot_done.set()

            captured: dict[str, object] = {}

            def ledger_preflight(commissioning_id: str, runtime_snapshot: object) -> dict[str, object]:
                append_before_snapshot.set()
                self.assertTrue(before_snapshot_done.wait(2))
                captured.update(ledger.commissioning_tail_snapshot(anchor, last_full_verified_sequence=anchor))
                append_after_snapshot.set()
                self.assertTrue(after_snapshot_done.wait(2))
                return {
                    "ledger_trust_state": "VERIFIED_ANCHOR_WITH_PASSIVE_LIVE_TAIL",
                    "verified_through_sequence": anchor,
                    "arm_snapshot_tip": captured["arm_snapshot_tip"],
                    "commissioning_id": commissioning_id,
                }

            commands: list[object] = []
            runtime._persist_and_send = lambda command, grant: commands.append(command)  # type: ignore[method-assign]
            writer_thread = threading.Thread(target=writer)
            writer_thread.start()
            with patch("src.l3g_paper.runtime._now", return_value=self.now):
                arm_result = runtime.commissioning_arm(ledger_preflight)
            writer_thread.join(2)
            self.assertFalse(writer_thread.is_alive())
            self.assertTrue(arm_result["armed"], arm_result)
            self.assertLess(anchor, int(captured["arm_snapshot_tip"]))
            self.assertEqual(captured["last_authority_mutation_sequence"], 0)
            self.assertEqual(runtime.status()["entry_owner"], PaperEntryOwner.COMMISSIONING.value)
            self.assertFalse(runtime._request_entry(self.strategy_decision()))  # type: ignore[arg-type]
            self.assertEqual(commands, [])
            reservation = ledger.recent_kinds(("COMMISSIONING_OWNERSHIP_RESERVED",), limit=1)[0]
            self.assertEqual(
                reservation["payload"]["ledger_preflight"]["arm_snapshot_tip"],
                captured["arm_snapshot_tip"],
            )
            self.close(runtime, ledger)

    def test_commissioning_entry_racing_strategy_allows_only_the_commissioning_command(self) -> None:
        with TemporaryDirectory() as directory:
            ledger, runtime, _ = self.ready_runtime(directory)
            lifecycle = self.commissioning_arm(runtime)
            commands: list[object] = []
            runtime._persist_and_send = lambda command, grant: commands.append(command)  # type: ignore[method-assign]
            start = threading.Barrier(2)
            entry_result: list[dict[str, object]] = []
            strategy_result: list[bool] = []

            def entry() -> None:
                start.wait()
                with patch("src.l3g_paper.runtime._now", return_value=self.now):
                    entry_result.append(runtime.commission_entry(str(lifecycle["commissioning_id"]), str(lifecycle["commissioning_token"])))

            def strategy() -> None:
                start.wait()
                strategy_result.append(runtime._request_entry(self.strategy_decision()))  # type: ignore[arg-type]

            entry_thread = threading.Thread(target=entry)
            strategy_thread = threading.Thread(target=strategy)
            entry_thread.start(); strategy_thread.start(); entry_thread.join(2); strategy_thread.join(2)
            self.assertEqual(len(commands), 1)
            self.assertTrue(commands[0].commissioning)  # type: ignore[attr-defined]
            self.assertTrue(entry_result[0]["submitted"])
            self.assertEqual(strategy_result, [False])
            self.close(runtime, ledger)

    def test_many_strategy_attempts_and_duplicate_commissioning_entry_remain_single_use(self) -> None:
        with TemporaryDirectory() as directory:
            ledger, runtime, _ = self.ready_runtime(directory)
            lifecycle = self.commissioning_arm(runtime)
            commands: list[object] = []
            runtime._persist_and_send = lambda command, grant: commands.append(command)  # type: ignore[method-assign]
            decisions = [self.strategy_decision() for _ in range(24)]
            threads = [threading.Thread(target=runtime._request_entry, args=(decision,)) for decision in decisions]
            for thread in threads: thread.start()
            for thread in threads: thread.join(2)
            self.assertEqual(commands, [])
            duplicate_start = threading.Barrier(2)
            entries: list[dict[str, object]] = []

            def entry() -> None:
                duplicate_start.wait()
                with patch("src.l3g_paper.runtime._now", return_value=self.now):
                    entries.append(runtime.commission_entry(str(lifecycle["commissioning_id"]), str(lifecycle["commissioning_token"])))

            first = threading.Thread(target=entry); second = threading.Thread(target=entry)
            first.start(); second.start(); first.join(2); second.join(2)
            self.assertEqual(len(commands), 1)
            self.assertEqual(sum(result["submitted"] is True for result in entries), 1)
            self.assertEqual(sum(result.get("reason_codes") == ("COMMISSIONING_ENTRY_ALREADY_CONSUMED",) for result in entries), 1)
            self.close(runtime, ledger)

    def test_atomic_start_is_idempotent_for_duplicate_and_timeout_retry_requests(self) -> None:
        with TemporaryDirectory() as directory:
            ledger, runtime, _ = self.ready_runtime(directory)
            commands: list[object] = []
            runtime._persist_and_send = lambda command, grant: commands.append(command)  # type: ignore[method-assign]
            ledger_preflight = lambda commissioning_id, snapshot: {
                "ledger_trust_state": "TEST_VERIFIED_ANCHOR",
                "commissioning_id": commissioning_id,
            }
            request_id = "http-timeout-retry-0001"
            with patch("src.l3g_paper.runtime._now", return_value=self.now):
                accepted = runtime.commissioning_start(request_id, ledger_preflight)
                retry = runtime.commissioning_start(request_id, ledger_preflight)
            self.assertTrue(accepted["submitted"])
            self.assertFalse(accepted.get("idempotent_replay", False))
            self.assertTrue(retry["submitted"])
            self.assertTrue(retry["idempotent_replay"])
            self.assertEqual(accepted["commissioning_id"], retry["commissioning_id"])
            self.assertEqual(accepted["decision_id"], retry["decision_id"])
            self.assertEqual(len(commands), 1)
            kinds = {record["kind"] for record in ledger.recent(50)}
            self.assertTrue({
                "COMMISSIONING_PREFLIGHT_ACCEPTED", "COMMISSIONING_OWNERSHIP_RESERVED",
                "COMMISSIONING_ENTRY_AUTHORIZED", "COMMISSIONING_ENTRY_SUBMITTED",
            }.issubset(kinds))
            self.close(runtime, ledger)

    def test_atomic_start_waits_without_a_qualified_signal_and_later_consumes_one(self) -> None:
        with TemporaryDirectory() as directory:
            ledger, runtime, context = self.ready_runtime(directory)
            candidate = runtime._last_qualifying_entry_decision
            runtime._last_qualifying_entry_decision = None
            commands: list[object] = []
            runtime._persist_and_send = lambda command, grant: commands.append(command)  # type: ignore[method-assign]
            with patch("src.l3g_paper.runtime._now", return_value=self.now):
                waiting = runtime.commissioning_start(
                    "wait-for-confluence-0001",
                    lambda commissioning_id, snapshot: {"ledger_trust_state": "TEST_VERIFIED_ANCHOR"},
                )
            self.assertTrue(waiting["armed"])
            self.assertFalse(waiting["submitted"])
            self.assertEqual(waiting["reason_codes"], ("COMMISSIONING_WAITING_FOR_HIGH_CONFLUENCE",))
            self.assertEqual(runtime.state, PaperRuntimeState.ARMED_FLAT)
            self.assertEqual(commands, [])
            self.assertIsNotNone(candidate)
            ownership = runtime._commissioning_ownership
            self.assertIsNotNone(ownership)
            with patch("src.l3g_paper.runtime._now", return_value=self.now):
                submitted = runtime.commission_entry(
                    str(ownership.commissioning_id),  # type: ignore[union-attr]
                    str(ownership.commissioning_token),  # type: ignore[union-attr]
                    candidate=candidate,
                )
            self.assertTrue(submitted["submitted"])
            self.assertEqual(len(commands), 1)
            self.assertTrue(commands[0].commissioning)  # type: ignore[attr-defined]
            self.assertEqual(commands[0].action.value, "ENTER_LONG")  # type: ignore[attr-defined]
            authorized = ledger.recent_kinds(("COMMISSIONING_ENTRY_AUTHORIZED",), limit=1)[0]
            self.assertEqual(authorized["payload"]["qualification"]["required_support"], "0.675")
            self.assertEqual(authorized["payload"]["qualification"]["required_family_count"], 3)
            self.assertEqual(authorized["payload"]["session_kind"], context.session_kind.value)
            self.close(runtime, ledger)

    def test_waiting_atomic_commissioning_auto_consumes_the_next_policy_candidate(self) -> None:
        with TemporaryDirectory() as directory:
            ledger, runtime, _ = self.ready_runtime(directory)
            candidate = runtime._last_qualifying_entry_decision
            self.assertIsNotNone(candidate)
            runtime._last_qualifying_entry_decision = None
            commands: list[object] = []
            runtime._persist_and_send = lambda command, grant: commands.append(command)  # type: ignore[method-assign]
            with patch("src.l3g_paper.runtime._now", return_value=self.now):
                waiting = runtime.commissioning_start(
                    "auto-consume-confluence-0001",
                    lambda commissioning_id, snapshot: {"ledger_trust_state": "TEST_VERIFIED_ANCHOR"},
                )
            self.assertFalse(waiting["submitted"])
            self.assertEqual(runtime.status()["commissioning_lifecycle"]["phase"], "WAITING_FOR_HIGH_CONFLUENCE")

            factory = ObservationFactory(
                start=datetime.fromisoformat(self.now.replace("Z", "+00:00")) - timedelta(milliseconds=100),
            )
            observation = factory.quote(100)
            with (
                patch("src.l3g_paper.runtime._now", return_value=self.now),
                patch.object(runtime.policy, "ingest_runtime", return_value=candidate),
                patch.object(runtime.policy, "runtime_gate_state", return_value=(10, True, False)),
                patch.object(runtime.policy, "active_evidence", return_value=()),
            ):
                runtime.ingest(observation)

            self.assertEqual(len(commands), 1)
            self.assertEqual(commands[0].action.value, "ENTER_LONG")  # type: ignore[attr-defined]
            self.assertTrue(commands[0].commissioning)  # type: ignore[attr-defined]
            status = runtime.status()
            self.assertEqual(status["commissioning_lifecycle"]["phase"], "ENTRY_CONSUMED")
            self.assertFalse(status["commissioning_lifecycle"]["waiting_for_high_confluence"])
            self.close(runtime, ledger)

    def test_commissioned_position_ignores_strategy_retention_exit_until_a_safety_exit(self) -> None:
        with TemporaryDirectory() as directory:
            ledger, runtime, _ = self.ready_runtime(directory)
            candidate = runtime._last_qualifying_entry_decision
            self.assertIsNotNone(candidate)
            commands: list[object] = []
            runtime._persist_and_send = lambda command, grant: commands.append(command)  # type: ignore[method-assign]
            with patch("src.l3g_paper.runtime._now", return_value=self.now):
                started = runtime.commissioning_start(
                    "longer-hold-commissioning-0001",
                    lambda commissioning_id, snapshot: {"ledger_trust_state": "TEST_VERIFIED_ANCHOR"},
                )
            self.assertTrue(started["submitted"])
            runtime._state = PaperRuntimeState.LONG
            runtime._position = PaperDirection.LONG
            runtime._position_quantity = 1
            runtime._snapshot = replace(
                runtime._snapshot,
                current_position=PaperDirection.LONG,
                current_position_quantity=1,
                position_opened_at=self.now,
                protective_stop_state="WORKING",
            )
            exit_decision = replace(
                candidate,  # type: ignore[arg-type]
                decision=PaperDecisionKind.EXIT,
                direction=PaperDirection.FLAT,
                reason_code="RETENTION_FAILED",
            )
            factory = ObservationFactory(
                start=datetime.fromisoformat(self.now.replace("Z", "+00:00")) - timedelta(milliseconds=100),
            )
            with (
                patch("src.l3g_paper.runtime._now", return_value=self.now),
                patch.object(runtime.policy, "ingest_runtime", return_value=exit_decision),
                patch.object(runtime.policy, "runtime_gate_state", return_value=(10, True, False)),
                patch.object(runtime.policy, "active_evidence", return_value=()),
            ):
                runtime.ingest(factory.quote(100))

            self.assertEqual(len(commands), 1)
            self.assertEqual(runtime.state, PaperRuntimeState.LONG)
            status = runtime.status()
            self.assertEqual(status["entry_owner"], PaperEntryOwner.COMMISSIONING.value)
            self.assertEqual(status["last_paper_decision"]["decision"], "EXIT")
            self.assertEqual(status["last_paper_decision"]["reason_code"], "RETENTION_FAILED")
            self.close(runtime, ledger)

    def test_weak_or_stale_candidate_never_consumes_commissioning_authority(self) -> None:
        with TemporaryDirectory() as directory:
            ledger, runtime, _ = self.ready_runtime(directory)
            lifecycle = self.commissioning_arm(runtime)
            candidate = runtime._last_qualifying_entry_decision
            self.assertIsNotNone(candidate)
            commands: list[object] = []
            runtime._persist_and_send = lambda command, grant: commands.append(command)  # type: ignore[method-assign]
            weak = replace(candidate, relative_support=Decimal("0.65"))  # type: ignore[arg-type]
            stale = replace(candidate, created_at="2026-08-26T13:59:50Z", expires_at="2026-08-26T13:59:55Z")  # type: ignore[arg-type]
            with patch("src.l3g_paper.runtime._now", return_value=self.now):
                weak_result = runtime.commission_entry(
                    str(lifecycle["commissioning_id"]), str(lifecycle["commissioning_token"]), candidate=weak,
                )
                stale_result = runtime.commission_entry(
                    str(lifecycle["commissioning_id"]), str(lifecycle["commissioning_token"]), candidate=stale,
                )
            self.assertEqual(weak_result["reason_codes"], ("COMMISSIONING_WAITING_FOR_HIGH_CONFLUENCE",))
            self.assertEqual(stale_result["reason_codes"], ("COMMISSIONING_WAITING_FOR_HIGH_CONFLUENCE",))
            self.assertEqual(commands, [])
            self.assertFalse(runtime._commissioning_ownership.entry_consumed)  # type: ignore[union-attr]
            self.close(runtime, ledger)

    def test_concurrent_duplicate_atomic_starts_emit_at_most_one_entry_command(self) -> None:
        with TemporaryDirectory() as directory:
            ledger, runtime, _ = self.ready_runtime(directory)
            commands: list[object] = []
            runtime._persist_and_send = lambda command, grant: commands.append(command)  # type: ignore[method-assign]
            barrier = threading.Barrier(2)
            results: list[dict[str, object]] = []

            def start() -> None:
                barrier.wait()
                with patch("src.l3g_paper.runtime._now", return_value=self.now):
                    results.append(runtime.commissioning_start(
                        "concurrent-request-0001",
                        lambda commissioning_id, snapshot: {"ledger_trust_state": "TEST_VERIFIED_ANCHOR"},
                    ))

            first = threading.Thread(target=start); second = threading.Thread(target=start)
            first.start(); second.start(); first.join(2); second.join(2)
            self.assertFalse(first.is_alive()); self.assertFalse(second.is_alive())
            self.assertEqual(len(commands), 1)
            self.assertEqual(sum(bool(result.get("idempotent_replay")) for result in results), 1)
            self.assertEqual({str(result["commissioning_id"]) for result in results}, {str(results[0]["commissioning_id"])})
            self.close(runtime, ledger)

    def test_synchronous_fill_callback_during_atomic_submit_is_reentrant_and_owned(self) -> None:
        with TemporaryDirectory() as directory:
            ledger, runtime, _ = self.ready_runtime(directory)
            commands: list[object] = []

            def submit(command: object, grant: object) -> None:
                commands.append(command)
                runtime.on_execution_message({
                    "message_type": "EXECUTION_EVENT", "order_role": "ENTRY",
                    "account_name": "Sim101", "instrument": "MNQ SEP26",
                    "price": "100.25", "quantity": 1, "direction": "LONG",
                    "command_id": command.command_id, "decision_id": command.decision_id,  # type: ignore[attr-defined]
                    "native_order_id": "sync-entry-order", "native_execution_id": "sync-entry-fill",
                    "timestamp": self.now,
                })

            runtime._persist_and_send = submit  # type: ignore[method-assign]
            with patch("src.l3g_paper.runtime._now", return_value=self.now):
                result = runtime.commissioning_start(
                    "synchronous-callback-0001",
                    lambda commissioning_id, snapshot: {"ledger_trust_state": "TEST_VERIFIED_ANCHOR"},
                )
            self.assertTrue(result["submitted"])
            self.assertEqual(len(commands), 1)
            self.assertEqual(runtime.state, PaperRuntimeState.LONG)
            self.assertEqual(runtime.status()["entry_owner"], PaperEntryOwner.COMMISSIONING.value)
            self.assertEqual(runtime.status()["last_execution"]["native_order_id"], "sync-entry-order")
            self.close(runtime, ledger)

    def test_commissioning_authorization_short_delay_succeeds_and_expiry_disarms(self) -> None:
        def clock(delay_seconds: int):
            calls = 0
            base = datetime.fromisoformat(self.now.replace("Z", "+00:00"))

            def current() -> str:
                nonlocal calls
                calls += 1
                # The authorization boundary is the third runtime clock read:
                # creation, risk evaluation, then transport admission. Ledger
                # record payloads no longer fabricate an extra wall-clock
                # value solely for retry identity.
                delay = delay_seconds if calls >= 3 else 0
                return (base + timedelta(seconds=delay)).isoformat().replace("+00:00", "Z")

            return current

        with TemporaryDirectory() as directory:
            ledger, runtime, _ = self.ready_runtime(directory)
            lifecycle = self.commissioning_arm(runtime)
            commands: list[object] = []
            runtime._persist_and_send = lambda command, grant: commands.append(command)  # type: ignore[method-assign]
            with patch("src.l3g_paper.runtime._now", side_effect=clock(1)):
                result = runtime.commission_entry(
                    str(lifecycle["commissioning_id"]), str(lifecycle["commissioning_token"]),
                )
            self.assertTrue(result["submitted"])
            self.assertEqual(len(commands), 1)
            self.close(runtime, ledger)

        with TemporaryDirectory() as directory:
            ledger, runtime, _ = self.ready_runtime(directory)
            lifecycle = self.commissioning_arm(runtime)
            commands = []
            runtime._persist_and_send = lambda command, grant: commands.append(command)  # type: ignore[method-assign]
            with patch("src.l3g_paper.runtime._now", side_effect=clock(6)):
                result = runtime.commission_entry(
                    str(lifecycle["commissioning_id"]), str(lifecycle["commissioning_token"]),
                )
            self.assertFalse(result["submitted"])
            self.assertEqual(result["reason_codes"], ("COMMISSIONING_ENTRY_AUTHORIZATION_EXPIRED",))
            self.assertEqual(commands, [])
            self.assertEqual(runtime.state, PaperRuntimeState.READY_DISARMED)
            self.assertEqual(runtime.status()["entry_owner"], PaperEntryOwner.NONE.value)
            ownership = ledger.commissioning_ownership(str(lifecycle["commissioning_id"]))
            self.assertIsNotNone(ownership)
            self.assertFalse(ownership[1])  # type: ignore[index]
            self.assertTrue(ownership[2])  # type: ignore[index]
            self.close(runtime, ledger)

    def test_transport_delay_holds_single_atomic_authority_boundary_against_strategy(self) -> None:
        with TemporaryDirectory() as directory:
            ledger, runtime, _ = self.ready_runtime(directory)
            transport_entered = threading.Event()
            release_transport = threading.Event()
            commands: list[object] = []
            start_result: list[dict[str, object]] = []
            strategy_result: list[bool] = []

            def submit(command: object, grant: object) -> None:
                commands.append(command)
                transport_entered.set()
                self.assertTrue(release_transport.wait(2))

            runtime._persist_and_send = submit  # type: ignore[method-assign]

            def start() -> None:
                with patch("src.l3g_paper.runtime._now", return_value=self.now):
                    start_result.append(runtime.commissioning_start(
                        "transport-delay-0001",
                        lambda commissioning_id, snapshot: {"ledger_trust_state": "TEST_VERIFIED_ANCHOR"},
                    ))

            def strategy() -> None:
                self.assertTrue(transport_entered.wait(2))
                strategy_result.append(runtime._request_entry(self.strategy_decision()))  # type: ignore[arg-type]

            start_thread = threading.Thread(target=start); strategy_thread = threading.Thread(target=strategy)
            start_thread.start(); strategy_thread.start()
            self.assertTrue(transport_entered.wait(2))
            self.assertTrue(strategy_thread.is_alive())
            release_transport.set()
            start_thread.join(2); strategy_thread.join(2)
            self.assertTrue(start_result[0]["submitted"])
            self.assertEqual(strategy_result, [False])
            self.assertEqual(len(commands), 1)
            self.assertTrue(commands[0].commissioning)  # type: ignore[attr-defined]
            self.close(runtime, ledger)

    def test_natural_alpha_expiry_does_not_clear_commissioning_authority(self) -> None:
        with TemporaryDirectory() as directory:
            ledger, runtime, _ = self.ready_runtime(directory)
            lifecycle = self.commissioning_arm(runtime)
            runtime._snapshot = replace(runtime._snapshot, evidence_warmed=False)
            commands: list[object] = []
            runtime._persist_and_send = lambda command, grant: commands.append(command)  # type: ignore[method-assign]
            with patch("src.l3g_paper.runtime._now", return_value=self.now):
                result = runtime.commission_entry(str(lifecycle["commissioning_id"]), str(lifecycle["commissioning_token"]))
            self.assertTrue(result["submitted"])
            self.assertEqual(len(commands), 1)
            self.assertTrue(commands[0].commissioning)  # type: ignore[attr-defined]
            self.assertEqual(runtime.state, PaperRuntimeState.ENTRY_PENDING)
            self.close(runtime, ledger)

    def test_commissioning_warmup_reset_denies_entry_disarms_and_releases(self) -> None:
        with TemporaryDirectory() as directory:
            ledger, runtime, _ = self.ready_runtime(directory)
            lifecycle = self.commissioning_arm(runtime)
            runtime._snapshot = replace(runtime._snapshot, commissioning_session_warmed=False)
            with patch("src.l3g_paper.runtime._now", return_value=self.now):
                failed = runtime.commission_entry(str(lifecycle["commissioning_id"]), str(lifecycle["commissioning_token"]))
            self.assertFalse(failed["submitted"])
            self.assertIn("COMMISSIONING_SESSION_NOT_WARMED", failed["reason_codes"])
            self.assertEqual(runtime.state, PaperRuntimeState.READY_DISARMED)
            self.assertEqual(runtime.status()["entry_owner"], PaperEntryOwner.NONE.value)
            kinds = {item["kind"] for item in ledger.recent_kinds(("COMMISSIONING_OWNERSHIP_RELEASED", "INCIDENT_COMMISSIONING_ENTRY_REJECTED"))}
            self.assertEqual(kinds, {"COMMISSIONING_OWNERSHIP_RELEASED", "INCIDENT_COMMISSIONING_ENTRY_REJECTED"})
            self.close(runtime, ledger)

    def test_emergency_flatten_remains_available_while_commissioning_owns_entry_admission(self) -> None:
        with TemporaryDirectory() as directory:
            ledger, runtime, _ = self.ready_runtime(directory)
            lifecycle = self.commissioning_arm(runtime)
            commands: list[object] = []
            runtime._persist_and_send = lambda command, grant: commands.append(command)  # type: ignore[method-assign]
            with patch("src.l3g_paper.runtime._now", return_value=self.now):
                self.assertTrue(runtime.commission_entry(str(lifecycle["commissioning_id"]), str(lifecycle["commissioning_token"]))["submitted"])
                runtime._state = PaperRuntimeState.LONG
                runtime._position = PaperDirection.LONG
                runtime._position_quantity = 1
                runtime._snapshot = replace(runtime._snapshot, current_position=PaperDirection.LONG, current_position_quantity=1)
                flatten = runtime.flatten_and_disarm()
            self.assertTrue(flatten["initiated"])
            self.assertEqual(runtime.state, PaperRuntimeState.EXIT_PENDING)
            self.assertEqual(commands[-1].action.value, "EMERGENCY_FLATTEN")  # type: ignore[attr-defined]
            self.assertEqual(runtime.status()["entry_owner"], PaperEntryOwner.COMMISSIONING.value)
            self.close(runtime, ledger)

    def test_restart_recovery_never_restores_normal_entry_authority(self) -> None:
        with TemporaryDirectory() as directory:
            ledger, runtime, _ = self.ready_runtime(directory)
            self.commissioning_arm(runtime)
            runtime.stop(); ledger.close()
            recovered_ledger = PaperLedger(Path(directory) / "paper.sqlite3")
            recovered = LaneIIIPaperRuntime(recovered_ledger)
            self.assertEqual(recovered.status()["entry_owner"], PaperEntryOwner.COMMISSIONING.value)
            recovered._state = PaperRuntimeState.RECONCILING
            recovered.on_execution_message({
                "message_type": "RECONCILIATION", "receipt_id": "recovered-flat", "account_name": "Sim101",
                "account_class": "LOCAL_SIMULATION", "instrument": "MNQ SEP26", "position_quantity": 0,
                "working_order_count": 0, "working_entry_count": 0, "position_snapshot_complete": True,
                "order_snapshot_complete": True, "foreign_activity": False, "timestamp": self.now,
            })
            self.assertEqual(recovered.state, PaperRuntimeState.READY_DISARMED)
            self.assertEqual(recovered.status()["entry_owner"], PaperEntryOwner.NONE.value)
            kinds = {item["kind"] for item in recovered_ledger.recent_kinds(("COMMISSIONING_OWNERSHIP_RECOVERED", "COMMISSIONING_OWNERSHIP_RELEASED"))}
            self.assertEqual(kinds, {"COMMISSIONING_OWNERSHIP_RECOVERED", "COMMISSIONING_OWNERSHIP_RELEASED"})
            self.close(recovered, recovered_ledger)

    def test_restart_after_a_consumed_entry_locks_out_instead_of_restoring_strategy_authority(self) -> None:
        with TemporaryDirectory() as directory:
            ledger, runtime, _ = self.ready_runtime(directory)
            lifecycle = self.commissioning_arm(runtime)
            runtime._persist_and_send = lambda command, grant: None  # type: ignore[method-assign]
            with patch("src.l3g_paper.runtime._now", return_value=self.now):
                self.assertTrue(runtime.commission_entry(str(lifecycle["commissioning_id"]), str(lifecycle["commissioning_token"]))["submitted"])
            runtime.stop(); ledger.close()
            recovered_ledger = PaperLedger(Path(directory) / "paper.sqlite3")
            recovered = LaneIIIPaperRuntime(recovered_ledger)
            recovered._state = PaperRuntimeState.RECONCILING
            recovered.on_execution_message({
                "message_type": "RECONCILIATION", "receipt_id": "recovered-consumed", "account_name": "Sim101",
                "account_class": "LOCAL_SIMULATION", "instrument": "MNQ SEP26", "position_quantity": 0,
                "working_order_count": 0, "working_entry_count": 0, "position_snapshot_complete": True,
                "order_snapshot_complete": True, "foreign_activity": False, "timestamp": self.now,
            })
            self.assertEqual(recovered.state, PaperRuntimeState.LOCKED_OUT)
            self.assertEqual(recovered.status()["entry_owner"], PaperEntryOwner.COMMISSIONING.value)
            self.assertEqual(recovered.status()["lockout_or_fault_reason"], "COMMISSIONING_OWNERSHIP_RECOVERY_AMBIGUOUS")
            self.close(recovered, recovered_ledger)


if __name__ == "__main__":
    unittest.main()
