from __future__ import annotations

from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from src.l3g_paper.contracts import (
    FIVE_MINUTE_PERPETUAL_PROFILE,
    PaperDirection,
    PaperRuntimeState,
)
from src.l3g_paper.ledger import PaperLedger
from src.l3g_paper.risk import PaperRiskSnapshot
from src.l3g_paper.runtime import LaneIIIPaperRuntime
from src.l3g_paper.sessions import PaperSessionResolver


NOW = "2026-09-08T14:00:00Z"


class PerpetualExitAccountingFailClosedTests(unittest.TestCase):
    @staticmethod
    def _runtime(directory: str) -> tuple[PaperLedger, LaneIIIPaperRuntime]:
        profile = FIVE_MINUTE_PERPETUAL_PROFILE
        ledger = PaperLedger(
            Path(directory) / "paper.sqlite3",
            policy=profile.policy,
            risk=profile.risk,
        )
        runtime = LaneIIIPaperRuntime(ledger)
        context = PaperSessionResolver().resolve(NOW, generation=8).context
        runtime._session_context = context
        ledger.set_session_context(context)
        runtime._state = PaperRuntimeState.EXIT_PENDING
        runtime._position = PaperDirection.LONG
        runtime._position_quantity = 1
        runtime._entry_fill_price = Decimal("100")
        runtime._entry_fill_quantity = 1
        runtime._entry_direction = PaperDirection.LONG
        runtime._entry_session_context = context
        runtime._entry_execution = {
            "native_execution_id": "entry-fill",
            "native_order_id": "entry-order",
            "price": "100",
            "quantity": 1,
            "timestamp": NOW,
        }
        runtime._snapshot = PaperRiskSnapshot(
            NOW,
            account_name="Sim101",
            account_class="LOCAL_SIMULATION",
            instrument="MNQ SEP26",
            canonical_contract="MNQU6",
            current_position=PaperDirection.LONG,
            current_position_quantity=1,
            position_snapshot_complete=True,
            order_snapshot_complete=True,
            reconciliation_current=True,
            execution_bridge_healthy=True,
            session_kind=context.session_kind,
            session_id=context.session_id,
            trade_date=context.trade_date,
            session_profile_hash=context.session_profile_hash,
            session_generation=context.session_generation,
        )
        runtime._execution_session_id = (  # type: ignore[method-assign]
            lambda: "l3g-es-perpetual-exit-accounting-test"
        )
        return ledger, runtime

    @staticmethod
    def _exit_fill(role: str, execution_id: str) -> dict[str, object]:
        return {
            "message_type": "EXECUTION_EVENT",
            "order_role": role,
            "price": "101",
            "quantity": 1,
            "account_name": "Sim101",
            "instrument": "MNQ SEP26",
            "native_execution_id": execution_id,
            "native_order_id": "exit-order",
            "timestamp": NOW,
        }

    def _assert_accounting_failure_is_latched(
        self,
        runtime: LaneIIIPaperRuntime,
        execution_id: str,
    ) -> None:
        reason = "RISK_CONTINUITY_EXIT_ACCOUNTING_FAILED:RuntimeError"
        self.assertEqual(runtime.state, PaperRuntimeState.EXIT_PENDING)
        self.assertEqual(runtime._fault_reason, reason)
        self.assertEqual(runtime._risk_continuity_fault, reason)
        self.assertTrue(runtime._entries_paused)
        self.assertTrue(runtime._retain_safety_lockout_after_flat)
        self.assertTrue(runtime.risk.status()["locked_out"])
        self.assertEqual(runtime._exit_execution["native_execution_id"], execution_id)  # type: ignore[index]
        self.assertEqual(runtime._lifecycle_realized_pnl, Decimal("2"))

    def test_exit_accounting_ledger_write_failures_retain_lockout(self) -> None:
        for failed_kind in ("RISK_EVENT_EXIT_ACCOUNTED", "EXECUTION_REALIZED_PNL"):
            with self.subTest(failed_kind=failed_kind), TemporaryDirectory() as directory:
                ledger, runtime = self._runtime(directory)
                original_append = ledger.append

                def fail_selected_kind(kind: str, *args: object, **kwargs: object):
                    if kind == failed_kind:
                        raise RuntimeError("injected accounting ledger failure")
                    return original_append(kind, *args, **kwargs)

                execution_id = "exit-fill-" + failed_kind.lower()
                try:
                    with patch.object(ledger, "append", side_effect=fail_selected_kind):
                        runtime.on_execution_message(self._exit_fill("EXIT", execution_id))
                    self._assert_accounting_failure_is_latched(runtime, execution_id)
                finally:
                    ledger.close()

    def test_protective_fill_risk_accounting_failure_retains_lockout(self) -> None:
        with TemporaryDirectory() as directory:
            ledger, runtime = self._runtime(directory)
            execution_id = "protective-fill-accounting-failure"
            try:
                with patch.object(
                    runtime,
                    "_account_recovered_exit",
                    side_effect=RuntimeError("injected risk accounting failure"),
                ):
                    runtime.on_execution_message(
                        self._exit_fill("PROTECTIVE", execution_id)
                    )
                self._assert_accounting_failure_is_latched(runtime, execution_id)
            finally:
                ledger.close()


if __name__ == "__main__":
    unittest.main()
