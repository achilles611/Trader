from __future__ import annotations

from datetime import datetime
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from src.l3g_paper.contracts import (
    ExecutionAction,
    PaperDirection,
    PaperRuntimeState,
)
from src.l3g_paper.slim_status import derive_slim_paper_status
from tests import test_l3g_perpetual_runtime as perpetual_fixture


NOW = perpetual_fixture.NOW


class PerpetualPositionReconciliationTests(unittest.TestCase):
    """Focused V2 coverage for the filled-position proof boundary."""

    @staticmethod
    def _actions(capture: object) -> list[ExecutionAction]:
        return [command.action for command in capture.commands]  # type: ignore[attr-defined]

    def _start_and_fill(
        self,
        runtime: object,
        capture: object,
        direction: PaperDirection,
        *,
        suffix: str,
    ) -> None:
        signal = perpetual_fixture.PerpetualRuntimeTests._decision(
            runtime,  # type: ignore[arg-type]
            direction,
            created_at="2026-09-01T20:51:00Z",
            candle_close_utc="2026-09-01T20:50:00Z",
            suffix=suffix,
        )
        perpetual_fixture.PerpetualRuntimeTests._commit_signal(runtime, signal)  # type: ignore[arg-type]
        perpetual_fixture.PerpetualRuntimeTests._ready(runtime)  # type: ignore[arg-type]
        result = runtime.operational_paper_start(  # type: ignore[attr-defined]
            "position-proof-" + suffix,
        )
        self.assertTrue(result["started"])
        self.assertEqual(runtime.state, PaperRuntimeState.ENTRY_PENDING)  # type: ignore[attr-defined]
        self.assertEqual(
            self._actions(capture),
            [
                ExecutionAction.ENTER_LONG
                if direction is PaperDirection.LONG
                else ExecutionAction.ENTER_SHORT
            ],
        )

        runtime.on_execution_message({  # type: ignore[attr-defined]
            "message_type": "EXECUTION_EVENT",
            "order_role": "ENTRY",
            "direction": direction.value,
            "price": "100.25",
            "quantity": 1,
            "native_execution_id": suffix + "-entry-execution",
            "native_order_id": suffix + "-entry-order",
            "account_name": "Sim101",
            "instrument": "MNQ SEP26",
            "timestamp": NOW,
        })
        self.assertEqual(
            runtime.state,  # type: ignore[attr-defined]
            PaperRuntimeState.LONG
            if direction is PaperDirection.LONG
            else PaperRuntimeState.SHORT,
        )
        self.assertNotIn(ExecutionAction.RECONCILE, self._actions(capture))

    @staticmethod
    def _protective_working(suffix: str) -> dict[str, object]:
        return {
            "message_type": "ORDER_EVENT",
            "order_role": "PROTECTIVE",
            "order_state": "WORKING",
            "account_name": "Sim101",
            "instrument": "MNQ SEP26",
            "quantity": 1,
            "native_order_id": suffix + "-protective-order",
            "timestamp": NOW,
        }

    @staticmethod
    def _positioned_reconciliation(
        suffix: str,
        direction: PaperDirection,
        *,
        position_quantity: int | None = None,
        working_order_count: int = 1,
        foreign_activity: bool = False,
        protective_stop_state: str = "WORKING",
    ) -> dict[str, object]:
        signed_quantity = (
            position_quantity
            if position_quantity is not None
            else 1 if direction is PaperDirection.LONG else -1
        )
        return {
            "message_type": "RECONCILIATION",
            "receipt_id": suffix + "-positioned-reconciliation",
            "account_name": "Sim101",
            "account_class": "LOCAL_SIMULATION",
            "instrument": "MNQ SEP26",
            "position_quantity": signed_quantity,
            "working_order_count": working_order_count,
            "working_entry_count": 0,
            "position_snapshot_complete": True,
            "order_snapshot_complete": True,
            "foreign_activity": foreign_activity,
            "protective_stop_state": protective_stop_state,
            "timestamp": NOW,
        }

    def test_entry_and_working_protective_request_one_reconciliation_then_prove_position(self) -> None:
        for direction in (PaperDirection.LONG, PaperDirection.SHORT):
            with self.subTest(direction=direction.value), TemporaryDirectory() as directory, patch(
                "src.l3g_paper.runtime._now", return_value=NOW,
            ):
                ledger, runtime, capture = perpetual_fixture.PerpetualRuntimeTests._runtime(directory)
                try:
                    suffix = "positioned-" + direction.value.lower()
                    self._start_and_fill(runtime, capture, direction, suffix=suffix)
                    protective = self._protective_working(suffix)

                    runtime.on_execution_message(protective)
                    self.assertEqual(
                        self._actions(capture).count(ExecutionAction.RECONCILE),
                        1,
                    )
                    runtime.on_execution_message(protective)
                    self.assertEqual(
                        self._actions(capture).count(ExecutionAction.RECONCILE),
                        1,
                    )

                    # This callback represents a complete aggregate which has
                    # already passed the transport's schema, HMAC, and session
                    # checks and whose wire receipt is durable.
                    runtime.on_execution_message(
                        self._positioned_reconciliation(suffix, direction),
                    )
                    status = runtime.status()
                    requirement = status["position_requirement"]
                    self.assertEqual(status["state"], direction.value)
                    self.assertEqual(status["current_position"], direction.value)
                    self.assertEqual(status["current_quantity"], 1)
                    self.assertEqual(status["broker_snapshot_position"], direction.value)
                    self.assertEqual(status["broker_snapshot_position_quantity"], 1)
                    self.assertEqual(status["working_owned_orders"], 1)
                    self.assertEqual(status["working_entry_orders"], 0)
                    self.assertFalse(status["foreign_activity"])
                    self.assertEqual(status["protective_stop_state"], "WORKING")
                    self.assertTrue(status["reconciliation_current"])
                    self.assertEqual(requirement["state"], "POSITIONED")  # type: ignore[index]
                    self.assertEqual(requirement["blocking_reasons"], [])  # type: ignore[index]
                    self.assertIsNone(requirement["primary_blocker"])  # type: ignore[index]
                    self.assertTrue(
                        requirement["source_signal"]["ledger_verified"],  # type: ignore[index]
                    )
                    positioned_rows = ledger.recent_kinds(
                        ("RISK_EVENT_POSITIONED_RECONCILIATION",),
                    )
                    self.assertEqual(len(positioned_rows), 1)

                    # A repeated native WORKING state after the proof is not a
                    # new fill and must not start another reconciliation loop.
                    runtime.on_execution_message(protective)
                    runtime._maintain_perpetual_position_locked(
                        "POST_ENTRY_POSITION_PROOF_REPLAY",
                    )
                    self.assertEqual(
                        self._actions(capture).count(ExecutionAction.RECONCILE),
                        1,
                    )
                    self.assertNotIn(
                        ExecutionAction.EMERGENCY_FLATTEN,
                        self._actions(capture),
                    )
                finally:
                    ledger.close()

    def test_protective_working_before_entry_fill_is_buffered_then_reconciled(self) -> None:
        with TemporaryDirectory() as directory, patch(
            "src.l3g_paper.runtime._now", return_value=NOW,
        ):
            ledger, runtime, capture = perpetual_fixture.PerpetualRuntimeTests._runtime(directory)
            try:
                self._start_and_fill_signal_only(runtime, PaperDirection.LONG)
                suffix = "protective-before-entry"
                runtime.on_execution_message(self._protective_working(suffix))
                self.assertEqual(
                    self._actions(capture), [ExecutionAction.ENTER_LONG],
                )

                runtime.on_execution_message({
                    "message_type": "EXECUTION_EVENT",
                    "order_role": "ENTRY",
                    "direction": "LONG",
                    "price": "100.25",
                    "quantity": 1,
                    "native_execution_id": suffix + "-entry-execution",
                    "native_order_id": suffix + "-entry-order",
                    "account_name": "Sim101",
                    "instrument": "MNQ SEP26",
                    "timestamp": NOW,
                })

                self.assertEqual(runtime.state, PaperRuntimeState.LONG)
                self.assertEqual(
                    self._actions(capture).count(ExecutionAction.RECONCILE), 1,
                )
                requirement = runtime.status()["position_requirement"]
                self.assertEqual(  # type: ignore[index]
                    requirement["primary_blocker"],
                    "POSITIONED_RECONCILIATION_PENDING",
                )
                runtime.on_execution_message(
                    self._positioned_reconciliation(
                        suffix, PaperDirection.LONG,
                    ),
                )
                requirement = runtime.status()["position_requirement"]
                self.assertEqual(requirement["blocking_reasons"], [])  # type: ignore[index]
            finally:
                ledger.close()

    def test_post_entry_reconciliation_rejection_emergency_flattens_once(self) -> None:
        with TemporaryDirectory() as directory, patch(
            "src.l3g_paper.runtime._now", return_value=NOW,
        ):
            ledger, runtime, capture = perpetual_fixture.PerpetualRuntimeTests._runtime(directory)
            try:
                suffix = "reconcile-rejected"
                self._start_and_fill(
                    runtime, capture, PaperDirection.LONG, suffix=suffix,
                )
                runtime.on_execution_message(self._protective_working(suffix))
                reconcile = next(
                    command for command in capture.commands
                    if command.action is ExecutionAction.RECONCILE
                )

                runtime.on_execution_message({
                    "message_type": "COMMAND_REJECTED",
                    "command_id": reconcile.command_id,
                    "reason_code": "NINJATRADER_RECONCILIATION_UNAVAILABLE",
                    "timestamp": NOW,
                })

                expected = (
                    "POST_ENTRY_RECONCILIATION_COMMAND_REJECTED:"
                    "NINJATRADER_RECONCILIATION_UNAVAILABLE"
                )
                status = runtime.status()
                self.assertEqual(runtime.state, PaperRuntimeState.EXIT_PENDING)
                self.assertEqual(status["lockout_or_fault_reason"], expected)
                self.assertTrue(status["entries_paused"])
                self.assertEqual(
                    self._actions(capture).count(ExecutionAction.EMERGENCY_FLATTEN),
                    1,
                )
            finally:
                ledger.close()

    def test_new_opposite_checkpoint_while_entry_pending_reverses_after_position_proof(self) -> None:
        with TemporaryDirectory() as directory, patch(
            "src.l3g_paper.runtime._now", return_value=NOW,
        ):
            ledger, runtime, capture = perpetual_fixture.PerpetualRuntimeTests._runtime(directory)
            try:
                suffix = "pending-opposite"
                self._start_and_fill_signal_only(runtime, PaperDirection.LONG)
                opposite = perpetual_fixture.PerpetualRuntimeTests._decision(
                    runtime,
                    PaperDirection.SHORT,
                    created_at="2026-09-01T20:56:00Z",
                    candle_close_utc="2026-09-01T20:55:00Z",
                    shape="PENDING",
                    suffix=suffix,
                )
                perpetual_fixture.PerpetualRuntimeTests._commit_signal(runtime, opposite)

                runtime.on_execution_message({
                    "message_type": "EXECUTION_EVENT",
                    "order_role": "ENTRY",
                    "direction": "LONG",
                    "price": "100.25",
                    "quantity": 1,
                    "native_execution_id": suffix + "-entry-execution",
                    "native_order_id": suffix + "-entry-order",
                    "account_name": "Sim101",
                    "instrument": "MNQ SEP26",
                    "timestamp": NOW,
                })
                runtime.on_execution_message(self._protective_working(suffix))
                runtime.on_execution_message(
                    self._positioned_reconciliation(
                        suffix, PaperDirection.LONG,
                    ),
                )

                actions = self._actions(capture)
                self.assertEqual(runtime.state, PaperRuntimeState.EXIT_PENDING)
                self.assertEqual(actions.count(ExecutionAction.ENTER_LONG), 1)
                self.assertEqual(actions.count(ExecutionAction.RECONCILE), 1)
                self.assertEqual(actions.count(ExecutionAction.EXIT), 1)
                self.assertNotIn(ExecutionAction.ENTER_SHORT, actions)
                self.assertEqual(
                    runtime.status()["position_requirement"]["desired_position"],  # type: ignore[index]
                    "SHORT",
                )
            finally:
                ledger.close()

    def test_bad_positioned_reconciliation_locks_entries_and_starts_one_safety_settlement(self) -> None:
        cases = {
            "opposite position": (
                {"position_quantity": -1},
                "POST_ENTRY_RECONCILIATION_POSITION_MISMATCH",
            ),
            "foreign activity": (
                {"foreign_activity": True},
                "POST_ENTRY_RECONCILIATION_FOREIGN_ACTIVITY",
            ),
            "missing protective": (
                {
                    "working_order_count": 0,
                    "protective_stop_state": "NONE",
                },
                "POST_ENTRY_RECONCILIATION_PROTECTIVE_ORDER_COUNT_NOT_ONE",
            ),
        }
        for name, (replacements, expected_reason) in cases.items():
            with self.subTest(case=name), TemporaryDirectory() as directory, patch(
                "src.l3g_paper.runtime._now", return_value=NOW,
            ):
                ledger, runtime, capture = perpetual_fixture.PerpetualRuntimeTests._runtime(directory)
                try:
                    suffix = "unsafe-" + name.replace(" ", "-")
                    self._start_and_fill(
                        runtime,
                        capture,
                        PaperDirection.LONG,
                        suffix=suffix,
                    )
                    runtime.on_execution_message(self._protective_working(suffix))
                    self.assertEqual(
                        self._actions(capture).count(ExecutionAction.RECONCILE),
                        1,
                    )

                    message = self._positioned_reconciliation(
                        suffix,
                        PaperDirection.LONG,
                    )
                    message.update(replacements)
                    runtime.on_execution_message(message)

                    status = runtime.status()
                    self.assertEqual(runtime.state, PaperRuntimeState.EXIT_PENDING)
                    self.assertTrue(status["entries_paused"])
                    self.assertEqual(status["lockout_or_fault_reason"], expected_reason)
                    self.assertTrue(runtime._retain_safety_lockout_after_flat)
                    self.assertEqual(
                        self._actions(capture).count(ExecutionAction.EMERGENCY_FLATTEN),
                        1,
                    )
                    self.assertEqual(
                        self._actions(capture).count(ExecutionAction.RECONCILE),
                        1,
                    )
                finally:
                    ledger.close()

    def test_entry_command_rejection_replaces_pending_with_exact_flat_blocker(self) -> None:
        with TemporaryDirectory() as directory, patch(
            "src.l3g_paper.runtime._now", return_value=NOW,
        ):
            ledger, runtime, capture = perpetual_fixture.PerpetualRuntimeTests._runtime(directory)
            try:
                self._start_and_fill_signal_only(runtime, PaperDirection.LONG)
                self.assertEqual(runtime.state, PaperRuntimeState.ENTRY_PENDING)

                runtime.on_execution_message({
                    "message_type": "COMMAND_REJECTED",
                    "command_id": capture.commands[0].command_id,
                    "reason_code": "MARKET_ENTRY_REJECTED_BY_EXCHANGE",
                    "timestamp": NOW,
                })

                exact_reason = (
                    "EXECUTION_COMMAND_REJECTED:MARKET_ENTRY_REJECTED_BY_EXCHANGE"
                )
                paper = runtime.status()
                requirement = paper["position_requirement"]
                self.assertEqual(runtime.state, PaperRuntimeState.LOCKED_OUT)
                self.assertEqual(paper["lockout_or_fault_reason"], exact_reason)
                self.assertEqual(requirement["primary_blocker"], exact_reason)  # type: ignore[index]
                self.assertNotIn("ENTRY_PENDING", requirement["blocking_reasons"])  # type: ignore[index]

                slim = derive_slim_paper_status(
                    paper,
                    {
                        "status": "PASS",
                        "chain_valid": True,
                        "checkpoint_valid": True,
                        "full_scan_required": False,
                        "quick_check": "ok",
                        "completed_at": NOW,
                    },
                    {
                        "market_observer_active": True,
                        "market_observer_state": "ACTIVE",
                    },
                    {"result": "BLOCKED", "blocking_reasons": [exact_reason]},
                    now=datetime.fromisoformat(NOW.replace("Z", "+00:00")),
                )
                self.assertEqual(slim["light"], "RED")
                self.assertEqual(slim["label"], "FLAT — BLOCKED: " + exact_reason)
                self.assertNotIn("ENTRY_PENDING", slim["label"])
                self.assertEqual(
                    self._actions(capture),
                    [ExecutionAction.ENTER_LONG],
                )
            finally:
                ledger.close()

    def test_native_entry_rejection_preserves_exact_ninjatrader_diagnostic(self) -> None:
        with TemporaryDirectory() as directory, patch(
            "src.l3g_paper.runtime._now", return_value=NOW,
        ):
            ledger, runtime, capture = perpetual_fixture.PerpetualRuntimeTests._runtime(directory)
            try:
                self._start_and_fill_signal_only(runtime, PaperDirection.LONG)
                runtime.on_execution_message({
                    "message_type": "ORDER_EVENT",
                    "order_role": "ENTRY",
                    "order_state": "REJECTED",
                    "native_order_id": "native-rejected-entry",
                    "native_error_code": "ORDERREJECTED",
                    "native_error_comment": "Exchange is closed for this instrument",
                    "timestamp": NOW,
                })

                exact_reason = (
                    "MARKET_ENTRY_ORDER_REJECTED:"
                    "NATIVE_ERROR=ORDERREJECTED:"
                    "NATIVE_COMMENT=Exchange is closed for this instrument"
                )
                paper = runtime.status()
                self.assertEqual(runtime.state, PaperRuntimeState.LOCKED_OUT)
                self.assertEqual(paper["lockout_or_fault_reason"], exact_reason)
                self.assertEqual(
                    paper["position_requirement"]["primary_blocker"],  # type: ignore[index]
                    exact_reason,
                )
                self.assertEqual(
                    self._actions(capture),
                    [ExecutionAction.ENTER_LONG],
                )
            finally:
                ledger.close()

    def test_market_data_disconnect_projects_the_exact_flat_blocker(self) -> None:
        with TemporaryDirectory() as directory, patch(
            "src.l3g_paper.runtime._now", return_value=NOW,
        ):
            ledger, runtime, capture = perpetual_fixture.PerpetualRuntimeTests._runtime(directory)
            try:
                perpetual_fixture.PerpetualRuntimeTests._ready(runtime)
                result = runtime.operational_paper_start(
                    "flat-market-data-disconnect",
                )
                self.assertTrue(result["started"])
                self.assertEqual(runtime.state, PaperRuntimeState.PAPER_RUNNING)
                runtime.ingest(
                    perpetual_fixture.PerpetualRuntimeTests._observation(
                        1,
                        "CONNECTION",
                        NOW,
                        {"scope": "MARKET_DATA", "price_status": "Disconnected"},
                    ),
                )

                status = runtime.status()
                self.assertFalse(status["continuity"]["market_price_connected"])  # type: ignore[index]
                self.assertEqual(
                    status["position_requirement"]["primary_blocker"],  # type: ignore[index]
                    "MARKET_DATA_DISCONNECTED",
                )
                self.assertEqual(capture.commands, [])
            finally:
                ledger.close()

    def test_synchronous_entry_send_failure_retains_exact_connection_reason(self) -> None:
        class _DisconnectedAdapter:
            def submit(self, _command: object, _grant: object) -> None:
                raise ConnectionError("NinjaTrader Sim101 socket is disconnected")

        with TemporaryDirectory() as directory, patch(
            "src.l3g_paper.runtime._now", return_value=NOW,
        ):
            ledger, runtime, _capture = perpetual_fixture.PerpetualRuntimeTests._runtime(directory)
            try:
                signal = perpetual_fixture.PerpetualRuntimeTests._decision(
                    runtime,
                    PaperDirection.LONG,
                    created_at="2026-09-01T20:51:00Z",
                    candle_close_utc="2026-09-01T20:50:00Z",
                    suffix="send-disconnected",
                )
                perpetual_fixture.PerpetualRuntimeTests._commit_signal(runtime, signal)
                perpetual_fixture.PerpetualRuntimeTests._ready(runtime)
                runtime._adapter = _DisconnectedAdapter()

                result = runtime.operational_paper_start(
                    "perpetual-send-disconnected",
                )

                self.assertTrue(result["started"])
                exact = (
                    "DURABLE_COMMAND_SEND_FAILED:ConnectionError:"
                    "NinjaTrader Sim101 socket is disconnected"
                )
                status = runtime.status()
                self.assertEqual(status["lockout_or_fault_reason"], exact)
                self.assertEqual(
                    status["position_requirement"]["primary_blocker"],  # type: ignore[index]
                    exact,
                )
            finally:
                ledger.close()

    @staticmethod
    def _start_and_fill_signal_only(
        runtime: object,
        direction: PaperDirection,
    ) -> None:
        signal = perpetual_fixture.PerpetualRuntimeTests._decision(
            runtime,  # type: ignore[arg-type]
            direction,
            created_at="2026-09-01T20:51:00Z",
            candle_close_utc="2026-09-01T20:50:00Z",
            suffix="rejected-entry",
        )
        perpetual_fixture.PerpetualRuntimeTests._commit_signal(runtime, signal)  # type: ignore[arg-type]
        perpetual_fixture.PerpetualRuntimeTests._ready(runtime)  # type: ignore[arg-type]
        runtime.operational_paper_start("rejected-entry-command")  # type: ignore[attr-defined]


if __name__ == "__main__":
    unittest.main()
