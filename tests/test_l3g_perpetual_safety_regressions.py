from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from src.l3g_paper.contracts import (
    FIVE_MINUTE_PERPETUAL_PROFILE,
    ExecutionAction,
    PaperDirection,
    PaperEntryOwner,
    PaperExecutionIntent,
    PaperRuntimeState,
)
from src.l3g_paper.ledger import PaperLedger
from src.l3g_paper.ninjatrader_transport import (
    ADDON_PROTOCOL_VERSION,
    PaperExecutionTransport,
    expected_addon_source_fingerprint,
)
from src.l3g_paper.risk import PaperRiskSnapshot
from src.l3g_paper.runtime import LaneIIIPaperRuntime
from src.l3g_paper.sessions import (
    PaperCalendarState,
    PaperSessionCalendar,
    PaperSessionResolver,
    perpetual_exchange_blocker,
)


NOW = "2026-09-08T14:00:00Z"
HOLIDAY_NOW = "2026-09-07T14:00:00Z"


class _CommandCapture:
    def __init__(self) -> None:
        self.commands: list[object] = []

    def submit(self, command: object, _grant: object) -> None:
        self.commands.append(command)


class PerpetualSafetyRegressionTests(unittest.TestCase):
    @staticmethod
    def _context():
        result = PaperSessionResolver().resolve(NOW, generation=7)
        if result.context.calendar_state is not PaperCalendarState.NORMAL:
            raise AssertionError("The safety fixture must use an ordinary exchange-open date.")
        return result.context

    @staticmethod
    def _healthy_snapshot(context, *, direction: PaperDirection = PaperDirection.FLAT) -> PaperRiskSnapshot:
        quantity = 0 if direction is PaperDirection.FLAT else 1
        return PaperRiskSnapshot(
            NOW,
            account_name="Sim101",
            account_class="LOCAL_SIMULATION",
            instrument="MNQ SEP26",
            current_position=direction,
            current_position_quantity=quantity,
            position_snapshot_complete=True,
            order_snapshot_complete=True,
            reconciliation_current=True,
            local_bridge_healthy=True,
            market_price_connected=True,
            execution_bridge_healthy=True,
            evidence_warmed=True,
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

    @staticmethod
    def _runtime(
        directory: str,
        *,
        risk_continuity: dict[str, object] | None = None,
    ) -> tuple[PaperLedger, LaneIIIPaperRuntime, _CommandCapture]:
        profile = FIVE_MINUTE_PERPETUAL_PROFILE
        ledger = PaperLedger(
            Path(directory) / "paper.sqlite3",
            policy=profile.policy,
            risk=profile.risk,
        )
        runtime = LaneIIIPaperRuntime(ledger, risk_continuity=risk_continuity)
        transport = PaperExecutionTransport(
            ledger,
            port=48176,
            policy=profile.policy,
            risk=profile.risk,
        )
        runtime.bind_transport(transport)
        with transport._lock:
            transport._state = "AUTHENTICATED"
            transport._authenticated = True
            transport._reconciled = True
            transport._client = object()  # type: ignore[assignment]
            transport._execution_session_id = "l3g-es-perpetual-safety-test"
            transport._addon_protocol_version = ADDON_PROTOCOL_VERSION
            transport._addon_source_fingerprint = expected_addon_source_fingerprint()
        capture = _CommandCapture()
        runtime._adapter = capture  # type: ignore[assignment]
        return ledger, runtime, capture

    @staticmethod
    def _reconciliation(quantity: int, receipt_id: str) -> dict[str, object]:
        return {
            "message_type": "RECONCILIATION",
            "receipt_id": receipt_id,
            "account_name": "Sim101",
            "account_class": "LOCAL_SIMULATION",
            "instrument": "MNQ SEP26",
            "position_quantity": quantity,
            "working_order_count": 0,
            "working_entry_count": 0,
            "position_snapshot_complete": True,
            "order_snapshot_complete": True,
            "foreign_activity": False,
            "protective_stop_state": "NONE",
            "timestamp": NOW,
        }

    def test_exact_v2_ignores_static_closed_calendar_but_keeps_true_exchange_closures(self) -> None:
        # The static session calendar is deliberately conservative and is not
        # an exchange-hours oracle. Perpetual V2 ignores this CLOSED override
        # while retaining the independently calculated exchange fences.
        resolver = PaperSessionResolver(PaperSessionCalendar({"2026-09-07": PaperCalendarState.CLOSED}))
        resolution = resolver.resolve(HOLIDAY_NOW, generation=7)
        self.assertEqual(resolution.context.calendar_state, PaperCalendarState.CLOSED)
        self.assertEqual(resolution.reason_code, "SESSION_CLOSED")
        self.assertFalse(FIVE_MINUTE_PERPETUAL_PROFILE.risk.enforce_holiday_fence)
        self.assertIsNone(perpetual_exchange_blocker(HOLIDAY_NOW, resolution.context))

        self.assertEqual(
            perpetual_exchange_blocker("2026-09-08T21:30:00Z"),
            "EXCHANGE_DAILY_MAINTENANCE",
        )
        self.assertEqual(
            perpetual_exchange_blocker("2026-09-08T20:20:00Z"),
            "EXCHANGE_INTRADAY_HALT",
        )
        self.assertEqual(
            perpetual_exchange_blocker("2026-09-12T14:00:00Z"),
            "EXCHANGE_WEEKEND_CLOSED",
        )

    def test_continuity_locked_nonflat_reconciliation_flattens_once_and_stays_latched(self) -> None:
        context = self._context()
        with TemporaryDirectory() as directory, patch("src.l3g_paper.runtime._now", return_value=NOW):
            ledger, runtime, capture = self._runtime(
                directory,
                risk_continuity={"schema": "corrupt"},
            )
            try:
                runtime._session_context = context
                ledger.set_session_context(context)
                runtime._snapshot = self._healthy_snapshot(context)
                runtime._state = PaperRuntimeState.RECONCILING

                runtime.on_execution_message(self._reconciliation(1, "locked-nonflat"))
                self.assertEqual(runtime.state, PaperRuntimeState.LOCKED_OUT)
                self.assertEqual(runtime.status()["current_position"], "LONG")
                self.assertEqual(
                    runtime.status()["risk_continuity_fault"],
                    "RISK_CONTINUITY_SNAPSHOT_INVALID",
                )

                runtime.flatten_and_disarm()
                runtime.flatten_and_disarm()
                actions = [command.action for command in capture.commands]  # type: ignore[attr-defined]
                self.assertEqual(actions.count(ExecutionAction.EMERGENCY_FLATTEN), 1)
                self.assertFalse(
                    any(action in {ExecutionAction.ENTER_LONG, ExecutionAction.ENTER_SHORT} for action in actions)
                )
                self.assertEqual(runtime.state, PaperRuntimeState.EXIT_PENDING)
                exit_command_id = runtime._pending_exit_command_id
                self.assertIsNotNone(exit_command_id)

                # A signed flat position event alone cannot settle an exit.
                # Settlement requires the matching execution and terminal
                # order callback before the fresh reconciliation handshake.
                runtime.on_execution_message({
                    "message_type": "POSITION_EVENT",
                    "quantity": 0,
                    "timestamp": NOW,
                })
                self.assertEqual(runtime.state, PaperRuntimeState.EXIT_PENDING)
                runtime.on_execution_message({
                    "message_type": "EXECUTION_EVENT",
                    "order_role": "EXIT",
                    "direction": "FLAT",
                    "price": "100",
                    "quantity": 1,
                    "native_execution_id": "locked-exit-execution",
                    "native_order_id": "locked-exit-order",
                    "command_id": exit_command_id,
                    "account_name": "Sim101",
                    "instrument": "MNQ SEP26",
                    "timestamp": NOW,
                })
                self.assertEqual(runtime.state, PaperRuntimeState.EXIT_PENDING)
                runtime.on_execution_message({
                    "message_type": "ORDER_EVENT",
                    "order_role": "EXIT",
                    "order_state": "FILLED",
                    "native_order_id": "locked-exit-order",
                    "command_id": exit_command_id,
                })
                self.assertEqual(runtime.state, PaperRuntimeState.RECONCILING)
                runtime.on_execution_message(self._reconciliation(0, "locked-flat"))

                status = runtime.status()
                risk = runtime.risk.status()
                actions = [command.action for command in capture.commands]  # type: ignore[attr-defined]
                self.assertEqual(actions.count(ExecutionAction.EMERGENCY_FLATTEN), 1)
                self.assertFalse(
                    any(action in {ExecutionAction.ENTER_LONG, ExecutionAction.ENTER_SHORT} for action in actions)
                )
                self.assertEqual(status["current_position"], "FLAT")
                self.assertEqual(status["current_quantity"], 0)
                self.assertTrue(status["entries_paused"])
                self.assertTrue(risk["locked_out"])
                self.assertEqual(
                    status["risk_continuity_fault"],
                    "RISK_CONTINUITY_SNAPSHOT_INVALID",
                )
            finally:
                ledger.close()

    def test_malformed_intent_context_entry_fill_retains_physical_truth_and_flattens_once(self) -> None:
        context = self._context()
        with TemporaryDirectory() as directory, patch("src.l3g_paper.runtime._now", return_value=NOW):
            ledger, runtime, capture = self._runtime(directory)
            try:
                runtime._session_context = context
                ledger.set_session_context(context)
                runtime._snapshot = self._healthy_snapshot(context)
                runtime._state = PaperRuntimeState.ENTRY_PENDING
                runtime._entry_owner = PaperEntryOwner.STRATEGY
                expires = (
                    datetime.fromisoformat(NOW.replace("Z", "+00:00")) + timedelta(seconds=30)
                ).astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
                intent = PaperExecutionIntent(
                    "l3g-pi-malformed-context-fill",
                    "l3g-pd-malformed-context-fill",
                    PaperDirection.LONG,
                    1,
                    "MNQ SEP26",
                    NOW,
                    expires,
                    FIVE_MINUTE_PERPETUAL_PROFILE.policy.configuration_hash,
                    Decimal("99.75"),
                    Decimal("100"),
                    Decimal("100"),
                    context.session_kind,
                    context.session_id,
                    context.trade_date,
                    context.session_profile_hash,
                    context.session_generation,
                )
                # Model a post-construction corruption in the pending local
                # authority. The inbound fill itself has already crossed the
                # authenticated/signed transport boundary and is physical
                # truth even though its local attribution cannot be trusted.
                object.__setattr__(intent, "session_id", "MALFORMED_SESSION_ID")
                runtime._pending_intent = intent

                fill = {
                    "message_type": "EXECUTION_EVENT",
                    "receipt_id": "malformed-context-entry-receipt",
                    "order_role": "ENTRY",
                    "direction": "LONG",
                    "price": "100",
                    "quantity": 1,
                    "native_execution_id": "malformed-context-entry-fill",
                    "native_order_id": "malformed-context-entry-order",
                    "account_name": "Sim101",
                    "instrument": "MNQ SEP26",
                    "timestamp": NOW,
                }
                ledger.append(
                    "EXECUTION",
                    fill,
                    identity="raw-malformed-context-entry-fill",
                    occurred_at=NOW,
                    execution_session_id="l3g-es-perpetual-safety-test",
                )
                runtime.on_execution_message(fill)
                runtime.on_execution_message(fill)

                status = runtime.status()
                actions = [command.action for command in capture.commands]  # type: ignore[attr-defined]
                self.assertEqual(status["current_position"], "LONG")
                self.assertEqual(status["current_quantity"], 1)
                self.assertEqual(status["broker_snapshot_position"], "LONG")
                self.assertEqual(status["broker_snapshot_position_quantity"], 1)
                self.assertEqual(runtime._entry_fill_price, Decimal("100"))
                self.assertEqual(runtime._entry_fill_quantity, 1)
                self.assertEqual(
                    runtime._entry_execution["native_execution_id"],  # type: ignore[index]
                    "malformed-context-entry-fill",
                )
                self.assertTrue(status["entries_paused"])
                self.assertTrue(runtime.risk.status()["locked_out"])
                self.assertEqual(actions.count(ExecutionAction.EMERGENCY_FLATTEN), 1)
                self.assertFalse(
                    any(action in {ExecutionAction.ENTER_LONG, ExecutionAction.ENTER_SHORT} for action in actions)
                )
            finally:
                ledger.close()


if __name__ == "__main__":
    unittest.main()
