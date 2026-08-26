from __future__ import annotations

from dataclasses import replace
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
import threading
import unittest
from unittest.mock import patch

from src.l3g_paper.contracts import PaperDirection, PaperEntryOwner, PaperRuntimeState, PaperSessionArmGrant
from src.l3g_paper.ledger import PaperLedger
from src.l3g_paper.ninjatrader_transport import ADDON_PROTOCOL_VERSION, PaperExecutionTransport, expected_addon_source_fingerprint
from src.l3g_paper.risk import PaperRiskSnapshot
from src.l3g_paper.runtime import LaneIIIPaperRuntime
from src.l3g_paper.sessions import PaperSessionResolver, context_from_identity
from .l3g_helpers import warmed_bullish_policy


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
        runtime._snapshot = PaperRiskSnapshot(
            observed_at, position_snapshot_complete=True, order_snapshot_complete=True,
            reconciliation_current=True, local_bridge_healthy=True, market_price_connected=True,
            execution_bridge_healthy=True, evidence_warmed=True, depth_reset_recovery=False,
            quote_observed_at=observed_at, classified_trade_observed_at=observed_at,
            depth_mutation_observed_at=observed_at, session_kind=context.session_kind,
            session_id=context.session_id, trade_date=context.trade_date,
            session_profile_hash=context.session_profile_hash, session_generation=context.session_generation,
        )
        runtime._last_quote = (Decimal("100"), Decimal("100.25"), observed_at)
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
            result = runtime.commissioning_arm()
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
                    arm_result.update(runtime.commissioning_arm())

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

    def test_pre_command_commissioning_failure_disarms_and_releases_but_exit_and_flatten_remain_available(self) -> None:
        with TemporaryDirectory() as directory:
            ledger, runtime, _ = self.ready_runtime(directory)
            lifecycle = self.commissioning_arm(runtime)
            runtime._snapshot = replace(runtime._snapshot, evidence_warmed=False)
            with patch("src.l3g_paper.runtime._now", return_value=self.now):
                failed = runtime.commission_entry(str(lifecycle["commissioning_id"]), str(lifecycle["commissioning_token"]))
            self.assertFalse(failed["submitted"])
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
