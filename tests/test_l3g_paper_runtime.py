from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
from decimal import Decimal
import unittest

from src.l3f_provider.tradovate_observation import StreamHealth
from src.l3g_paper.ledger import PaperLedger
from src.l3g_paper.ninjatrader_transport import PaperExecutionTransport
from src.l3g_paper.runtime import LaneIIIPaperRuntime, ObservationFanout
from src.l3g_paper.contracts import PaperRuntimeState
from .l3g_helpers import warmed_bullish_policy


class PaperRuntimeTests(unittest.TestCase):
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
            runtime.on_execution_message({"message_type": "POSITION_EVENT", "quantity": 0, "timestamp": "2026-08-24T14:00:02Z"})
            status = runtime.status()
            self.assertEqual(runtime.state, PaperRuntimeState.ARMED_FLAT)
            self.assertEqual(status["session_entries"], 1)
            self.assertEqual(status["daily_realized_pnl"], "1.50")
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
