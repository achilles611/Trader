from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from decimal import Decimal
import unittest
from unittest.mock import patch

from src.l3f_provider.tradovate_observation import StreamHealth
from src.l3g_paper.ledger import PaperLedger
from src.l3g_paper.ninjatrader_transport import ADDON_PROTOCOL_VERSION, PaperExecutionTransport, expected_addon_source_fingerprint
from src.l3g_paper.runtime import LaneIIIPaperRuntime, ObservationFanout, _CommissioningOwnership
from src.l3g_paper.contracts import PaperDirection, PaperEntryOwner, PaperRuntimeState, PaperSessionArmGrant
from src.l3g_paper.risk import PaperRiskSnapshot
from src.l3g_paper.sessions import PaperSessionResolver
from .l3g_helpers import ObservationFactory, warmed_bullish_policy


class PaperRuntimeTests(unittest.TestCase):
    @staticmethod
    def reserve_commissioning(runtime: LaneIIIPaperRuntime, context: object, now: str) -> tuple[str, str]:
        ownership = _CommissioningOwnership("l3g-commissioning-test", "l3g-commissioning-token-test", context, now)  # type: ignore[arg-type]
        runtime._commissioning_ownership = ownership
        runtime._entry_owner = PaperEntryOwner.COMMISSIONING
        return ownership.commissioning_id, ownership.commissioning_token

    @staticmethod
    def reconcile_flat(runtime: LaneIIIPaperRuntime) -> None:
        runtime.on_execution_bridge_state("AUTHENTICATED")
        runtime.on_execution_message({
            "message_type": "RECONCILIATION", "receipt_id": "reconcile-flat", "account_name": "Sim101",
            "account_class": "LOCAL_SIMULATION", "instrument": "MNQ SEP26", "position_quantity": 0,
            "working_order_count": 0, "working_entry_count": 0, "position_snapshot_complete": True,
            "order_snapshot_complete": True, "foreign_activity": False, "timestamp": "2026-08-24T14:00:00Z",
        })

    def test_restart_reconciliation_returns_ready_disarmed_and_never_auto_arms(self) -> None:
        with TemporaryDirectory() as directory:
            ledger = PaperLedger(Path(directory) / "paper.sqlite3")
            runtime = LaneIIIPaperRuntime(ledger); transport = PaperExecutionTransport(ledger, port=48159)
            runtime.bind_transport(transport); runtime.start(); runtime.on_execution_bridge_state("AUTHENTICATED")
            runtime.on_execution_message({
                "message_type": "RECONCILIATION", "receipt_id": "r", "account_name": "Sim101",
                "account_class": "LOCAL_SIMULATION", "instrument": "MNQ SEP26", "position_quantity": 0,
                "working_order_count": 0, "working_entry_count": 0, "position_snapshot_complete": True,
                "order_snapshot_complete": True, "foreign_activity": False, "timestamp": "2026-08-24T14:00:00Z",
            })
            self.assertEqual(runtime.state.value, "READY_DISARMED")
            self.assertFalse(runtime.arm()["armed"])
            runtime.stop(); ledger.close()

    def test_addon_provenance_denies_arm_but_not_observation_or_exit_safety(self) -> None:
        with TemporaryDirectory() as directory:
            ledger = PaperLedger(Path(directory) / "paper.sqlite3")
            transport = PaperExecutionTransport(ledger, port=48171)
            runtime = LaneIIIPaperRuntime(ledger); runtime.bind_transport(transport)
            runtime._state = PaperRuntimeState.READY_DISARMED
            transport._addon_protocol_version = ADDON_PROTOCOL_VERSION
            transport._addon_source_fingerprint = "stale"
            self.assertEqual(runtime.arm()["reason_codes"], ("ADDON_BUILD_MISMATCH",))
            transport._addon_source_fingerprint = expected_addon_source_fingerprint()
            # The provenance gate is now clear; an off-session fence, not the
            # AddOn, is the reason this fixture cannot arm.
            self.assertEqual(runtime.arm()["reason_codes"], ("NO_CURRENT_EVENT_SESSION",))
            ledger.close()

    def test_backward_provider_timestamp_does_not_close_the_current_paper_session(self) -> None:
        with TemporaryDirectory() as directory:
            ledger = PaperLedger(Path(directory) / "paper.sqlite3")
            runtime = LaneIIIPaperRuntime(ledger)
            factory = ObservationFactory(start=datetime(2026, 8, 25, 19, 0, tzinfo=timezone.utc))
            first = factory.quote(100); runtime.ingest(first)
            first_status = runtime.status()
            crossing = factory.depth("ADD", 10)
            earlier_provider_time = (datetime.fromisoformat(first.provider_timestamp.replace("Z", "+00:00")) - timedelta(milliseconds=50)).isoformat().replace("+00:00", "Z")
            crossing = type(crossing)(
                crossing.observation_id, crossing.session_id, crossing.observation_type,
                crossing.ninja_receipt_time, crossing.local_monotonic_sequence,
                crossing.payload, provider_timestamp=earlier_provider_time,
            )
            runtime.ingest(crossing)
            second_status = runtime.status()
            self.assertEqual(second_status["current_session_id"], first_status["current_session_id"])
            self.assertEqual(second_status["session_generation"], first_status["session_generation"])
            runtime.stop(); ledger.close()

    def test_late_local_callback_is_refused_without_resetting_ny_after_evidence_domain(self) -> None:
        with TemporaryDirectory() as directory:
            ledger = PaperLedger(Path(directory) / "paper.sqlite3")
            runtime = LaneIIIPaperRuntime(ledger)
            factory = ObservationFactory(start=datetime(2026, 8, 25, 20, 10, tzinfo=timezone.utc))
            runtime.ingest(factory.quote(100))
            admitted = runtime.status()
            late = factory.depth("ADD", 10)
            late = type(late)(
                late.observation_id, late.session_id, late.observation_type,
                (datetime.fromisoformat(late.ninja_receipt_time.replace("Z", "+00:00")) - timedelta(seconds=5)).isoformat().replace("+00:00", "Z"),
                late.local_monotonic_sequence, late.payload, provider_timestamp=late.provider_timestamp,
            )
            runtime.ingest(late)
            refused = runtime.status()
            self.assertEqual(admitted["current_session"], "NY_AFTER")
            self.assertEqual(refused["current_session_id"], admitted["current_session_id"])
            self.assertEqual(refused["session_generation"], admitted["session_generation"])
            self.assertTrue(any(item["kind"] == "INCIDENT_STALE_CALLBACK_REFUSED" for item in ledger.recent(20, domain="INCIDENT")))
            runtime.stop(); ledger.close()

    def test_fanout_isolates_sink_failures_and_preserves_both_deliveries(self) -> None:
        calls: list[str] = []; failures: list[tuple[str, str, str]] = []
        def broken(_: object) -> None: calls.append("shadow"); raise RuntimeError()
        def paper(_: object) -> None: calls.append("paper")
        fanout = ObservationFanout(
            shadow_observation=broken, shadow_transport=broken, shadow_rejection=broken, shadow_duplicate=lambda: None,
            paper_observation=paper, paper_transport=paper, paper_rejection=paper, paper_duplicate=lambda: None,
            record_failure=lambda *args: failures.append(args),
        )
        fanout.on_transport_state(StreamHealth.HEALTHY)
        self.assertEqual(calls, ["shadow", "paper"])
        self.assertEqual(failures[0][0], "SHADOW")

    def test_controlled_execution_events_cover_entry_protection_exit_and_flat(self) -> None:
        with TemporaryDirectory() as directory:
            ledger = PaperLedger(Path(directory) / "paper.sqlite3")
            runtime = LaneIIIPaperRuntime(ledger); transport = PaperExecutionTransport(ledger, port=48158)
            runtime.bind_transport(transport); runtime.start(); self.reconcile_flat(runtime)
            runtime._transition(PaperRuntimeState.ARMED_FLAT, "TEST_ARM")
            decision = warmed_bullish_policy()[2]
            runtime._pending_intent = runtime.risk.make_intent(
                decision, reference_bid=Decimal("100"), reference_ask=Decimal("100.25"), reference_last=Decimal("100.25"),
            )
            runtime._transition(PaperRuntimeState.ENTRY_PENDING, "TEST_ENTRY_SENT")
            runtime.on_execution_message({
                "message_type": "EXECUTION_EVENT", "order_role": "ENTRY", "price": "100.25",
                "quantity": 1, "direction": "LONG", "timestamp": "2026-08-24T14:00:01Z",
            })
            self.assertEqual(runtime.state, PaperRuntimeState.LONG)
            runtime.on_execution_message({"message_type": "ORDER_EVENT", "order_role": "PROTECTIVE", "order_state": "WORKING"})
            self.assertEqual(runtime.status()["protective_stop_state"], "WORKING")
            runtime._transition(PaperRuntimeState.EXIT_PENDING, "TEST_EXIT_SENT")
            runtime.on_execution_message({"message_type": "EXECUTION_EVENT", "order_role": "EXIT", "price": "101", "quantity": 1})
            reconciliation_commands: list[object] = []
            runtime._execution_session_id = lambda: "l3g-es-test"  # type: ignore[method-assign]
            runtime._persist_and_send = lambda command, grant: reconciliation_commands.append((command, grant))  # type: ignore[method-assign]
            runtime.on_execution_message({"message_type": "POSITION_EVENT", "quantity": 0, "timestamp": "2026-08-24T14:00:02Z"})
            self.assertEqual(runtime.state, PaperRuntimeState.RECONCILING)
            self.assertEqual(reconciliation_commands[0][0].action.value, "RECONCILE")
            runtime.on_execution_message({
                "message_type": "RECONCILIATION", "receipt_id": "post-exit-flat", "account_name": "Sim101",
                "account_class": "LOCAL_SIMULATION", "instrument": "MNQ SEP26", "position_quantity": 0,
                "working_order_count": 0, "working_entry_count": 0, "position_snapshot_complete": True,
                "order_snapshot_complete": True, "foreign_activity": False, "timestamp": "2026-08-24T14:00:03Z",
            })
            status = runtime.status()
            self.assertEqual(runtime.state, PaperRuntimeState.ARMED_FLAT)
            self.assertEqual(status["session_entries"], 1)
            self.assertEqual(status["daily_realized_pnl"], "1.50")
            runtime.stop(); ledger.close()

    def test_commissioning_entry_is_fixed_and_retains_all_non_strategy_markers(self) -> None:
        now = "2026-08-24T22:10:00Z"
        context = PaperSessionResolver().resolve(now, generation=1).context
        with TemporaryDirectory() as directory:
            ledger = PaperLedger(Path(directory) / "paper.sqlite3")
            runtime = LaneIIIPaperRuntime(ledger)
            runtime._state = PaperRuntimeState.ARMED_FLAT
            runtime._session_context = context
            runtime._armed_session = PaperSessionArmGrant(
                context.session_kind, context.session_id, context.trade_date, context.session_profile_hash,
                context.session_generation, now, "2026-08-25T05:30:00Z",
            )
            runtime._snapshot = PaperRiskSnapshot(
                now, position_snapshot_complete=True, order_snapshot_complete=True,
                reconciliation_current=True, local_bridge_healthy=True,
                market_price_connected=True, execution_bridge_healthy=True, evidence_warmed=True,
                depth_reset_recovery=False, quote_observed_at=now, classified_trade_observed_at=now,
                depth_mutation_observed_at=now, session_kind=context.session_kind,
                session_id=context.session_id, trade_date=context.trade_date,
                session_profile_hash=context.session_profile_hash, session_generation=context.session_generation,
            )
            submitted: list[object] = []
            runtime._persist_and_send = lambda command, grant: submitted.append((command, grant))  # type: ignore[method-assign]
            runtime._execution_session_id = lambda: "l3g-es-test"  # type: ignore[method-assign]
            commissioning_id, commissioning_token = self.reserve_commissioning(runtime, context, now)
            with patch("src.l3g_paper.runtime._now", return_value=now):
                result = runtime.commission_entry(commissioning_id, commissioning_token)
            self.assertTrue(result["submitted"])
            self.assertEqual(runtime.state, PaperRuntimeState.ENTRY_PENDING)
            command, grant = submitted[0]
            self.assertEqual(command.account_name, "Sim101")
            self.assertEqual(command.instrument, "MNQ SEP26")
            self.assertEqual(command.quantity, 1)
            for record in ledger.recent(10):
                if record["kind"] in {"DECISION", "INTENT", "RISK_GRANT", "COMMAND"}:
                    self.assertTrue(record["payload"]["commissioning"])
                    self.assertFalse(record["payload"]["strategy_generated"])
                    self.assertFalse(record["payload"]["scientific_evidence"])
            runtime.stop(); ledger.close()

    def test_exit_submission_cannot_reenter_before_exit_pending_is_recorded(self) -> None:
        now = "2026-08-24T22:10:00Z"
        context = PaperSessionResolver().resolve(now, generation=1).context
        with TemporaryDirectory() as directory:
            ledger = PaperLedger(Path(directory) / "paper.sqlite3")
            runtime = LaneIIIPaperRuntime(ledger)
            runtime._state = PaperRuntimeState.SHORT
            runtime._position = PaperDirection.SHORT
            runtime._position_quantity = -1
            runtime._session_context = context
            runtime._snapshot = PaperRiskSnapshot(
                now, current_position=PaperDirection.SHORT, current_position_quantity=1,
                position_snapshot_complete=True, order_snapshot_complete=True,
                reconciliation_current=True, execution_bridge_healthy=True,
                session_kind=context.session_kind, session_id=context.session_id,
                trade_date=context.trade_date, session_profile_hash=context.session_profile_hash,
                session_generation=context.session_generation,
            )
            runtime._execution_session_id = lambda: "l3g-es-test"  # type: ignore[method-assign]
            submitted: list[object] = []

            def send(command: object, grant: object) -> None:
                submitted.append((command, grant))
                runtime._request_exit("PROTECTIVE_STOP_REJECTED", emergency=True)

            runtime._persist_and_send = send  # type: ignore[method-assign]
            with patch("src.l3g_paper.runtime._now", return_value=now):
                runtime._request_exit("OPPOSING_HYPOTHESIS")
            self.assertEqual(len(submitted), 1)
            self.assertEqual(runtime.state, PaperRuntimeState.EXIT_PENDING)
            runtime.stop(); ledger.close()

    def test_expected_protective_cancellation_during_owned_exit_does_not_lock_authority(self) -> None:
        with TemporaryDirectory() as directory:
            ledger = PaperLedger(Path(directory) / "paper.sqlite3")
            runtime = LaneIIIPaperRuntime(ledger)
            runtime._state = PaperRuntimeState.EXIT_PENDING
            runtime._position = PaperDirection.LONG
            runtime._position_quantity = 1
            runtime.on_execution_message({"message_type": "ORDER_EVENT", "order_role": "PROTECTIVE", "order_state": "CANCELLED"})
            self.assertIsNone(runtime.status()["lockout_or_fault_reason"])
            runtime.stop(); ledger.close()

    def test_commissioning_exit_requires_clean_reconciliation_and_persists_fill_pnl(self) -> None:
        now = "2026-08-24T22:10:00Z"
        context = PaperSessionResolver().resolve(now, generation=1).context
        with TemporaryDirectory() as directory:
            ledger = PaperLedger(Path(directory) / "paper.sqlite3")
            runtime = LaneIIIPaperRuntime(ledger)
            runtime._state = PaperRuntimeState.ARMED_FLAT
            runtime._session_context = context
            runtime._armed_session = PaperSessionArmGrant(
                context.session_kind, context.session_id, context.trade_date, context.session_profile_hash,
                context.session_generation, now, "2026-08-25T05:30:00Z",
            )
            runtime._snapshot = PaperRiskSnapshot(
                now, position_snapshot_complete=True, order_snapshot_complete=True,
                reconciliation_current=True, local_bridge_healthy=True,
                market_price_connected=True, execution_bridge_healthy=True, evidence_warmed=True,
                depth_reset_recovery=False, quote_observed_at=now, classified_trade_observed_at=now,
                depth_mutation_observed_at=now, session_kind=context.session_kind,
                session_id=context.session_id, trade_date=context.trade_date,
                session_profile_hash=context.session_profile_hash, session_generation=context.session_generation,
            )
            submitted: list[object] = []
            runtime._execution_session_id = lambda: "l3g-es-test"  # type: ignore[method-assign]
            runtime._persist_and_send = lambda command, grant: submitted.append((command, grant))  # type: ignore[method-assign]
            runtime._last_quote = (Decimal("100"), Decimal("100.25"), now)
            commissioning_id, commissioning_token = self.reserve_commissioning(runtime, context, now)
            with patch("src.l3g_paper.runtime._now", return_value=now):
                self.assertTrue(runtime.commission_entry(commissioning_id, commissioning_token)["submitted"])
                self.assertEqual(runtime.status()["entry_owner"], PaperEntryOwner.COMMISSIONING.value)
                runtime.on_execution_message({
                    "message_type": "EXECUTION_EVENT", "order_role": "ENTRY", "price": "100.25", "quantity": 1,
                    "direction": "LONG", "command_id": submitted[0][0].command_id, "decision_id": submitted[0][0].decision_id,
                    "native_order_id": "entry-order", "native_execution_id": "entry-execution", "timestamp": now,
                })
                runtime.on_execution_message({"message_type": "ORDER_EVENT", "order_role": "PROTECTIVE", "order_state": "WORKING"})
                self.assertTrue(runtime.commission_exit()["submitted"])
                exit_command = submitted[1][0]
                self.assertTrue(exit_command.commissioning)
                self.assertFalse(exit_command.strategy_generated)
                self.assertEqual(exit_command.action.value, "EXIT")
                runtime.on_execution_message({"message_type": "ORDER_EVENT", "order_role": "PROTECTIVE", "order_state": "CANCELLED"})
                runtime.on_execution_message({
                    "message_type": "EXECUTION_EVENT", "order_role": "EXIT", "price": "101", "quantity": 1,
                    "command_id": exit_command.command_id, "native_order_id": "exit-order",
                    "native_execution_id": "exit-execution", "timestamp": now,
                })
                runtime.on_execution_message({"message_type": "POSITION_EVENT", "quantity": 0, "timestamp": now})
                self.assertEqual(runtime.state, PaperRuntimeState.RECONCILING)
                self.assertEqual(submitted[2][0].action.value, "RECONCILE")
                runtime.on_execution_message({
                    "message_type": "RECONCILIATION", "receipt_id": "commission-flat", "account_name": "Sim101",
                    "account_class": "LOCAL_SIMULATION", "instrument": "MNQ SEP26", "position_quantity": 0,
                    "working_order_count": 0, "working_entry_count": 0, "position_snapshot_complete": True,
                    "order_snapshot_complete": True, "foreign_activity": False, "timestamp": now,
                })
            closure = next(record["payload"] for record in ledger.recent(10) if record["kind"] == "COMMISSIONING_CLOSURE")
            self.assertEqual(runtime.state, PaperRuntimeState.READY_DISARMED)
            self.assertEqual(closure["entry_order_id"], "entry-order")
            self.assertEqual(closure["exit_order_id"], "exit-order")
            self.assertEqual(closure["realized_pnl"], "1.50")
            self.assertEqual(closure["final_working_order_count"], 0)
            self.assertEqual(runtime.status()["entry_owner"], PaperEntryOwner.NONE.value)
            runtime.stop(); ledger.close()

    def test_ambiguous_restarts_and_unexpected_fills_lock_out(self) -> None:
        with TemporaryDirectory() as directory:
            ledger = PaperLedger(Path(directory) / "paper.sqlite3")
            runtime = LaneIIIPaperRuntime(ledger); transport = PaperExecutionTransport(ledger, port=48157)
            runtime.bind_transport(transport); runtime.start(); runtime.on_execution_bridge_state("AUTHENTICATED")
            runtime.on_execution_message({
                "message_type": "RECONCILIATION", "receipt_id": "ambiguous", "account_name": "Sim101",
                "account_class": "LOCAL_SIMULATION", "instrument": "MNQ SEP26", "position_quantity": 1,
                "working_order_count": 1, "working_entry_count": 0, "position_snapshot_complete": True,
                "order_snapshot_complete": True, "foreign_activity": True, "timestamp": "2026-08-24T14:00:00Z",
            })
            self.assertEqual(runtime.state, PaperRuntimeState.LOCKED_OUT)
            runtime.stop(); ledger.close()

        with TemporaryDirectory() as directory:
            ledger = PaperLedger(Path(directory) / "paper.sqlite3")
            runtime = LaneIIIPaperRuntime(ledger); transport = PaperExecutionTransport(ledger, port=48156)
            runtime.bind_transport(transport); runtime.start(); self.reconcile_flat(runtime)
            runtime._transition(PaperRuntimeState.ARMED_FLAT, "TEST_ARM")
            runtime._transition(PaperRuntimeState.ENTRY_PENDING, "TEST_ENTRY_SENT")
            runtime.on_execution_message({"message_type": "EXECUTION_EVENT", "order_role": "ENTRY", "price": "100", "quantity": 1, "direction": "LONG"})
            self.assertEqual(runtime.state, PaperRuntimeState.LOCKED_OUT)
            runtime.stop(); ledger.close()


if __name__ == "__main__":
    unittest.main()
