from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from src.l3h_live.event_store import LiveEventStore
from src.l3h_live.lifecycle import ExecutionLifecycle, OrderLifecycleState, ProtectionLifecycle, ProtectionState


class L3HLifecycleTests(unittest.TestCase):
    def test_unknown_cannot_be_resubmitted_and_requires_reconciliation(self) -> None:
        with TemporaryDirectory() as directory:
            lifecycle = ExecutionLifecycle(LiveEventStore(Path(directory) / "l3h.sqlite3"), "BZ-L3H-LIFECYCLE")
            lifecycle.transition(OrderLifecycleState.INTENT_CREATED, evidence={"intent_id": "l3h-intent-1"})
            lifecycle.transition(OrderLifecycleState.ADMITTED, evidence={"risk": "pass"})
            lifecycle.transition(OrderLifecycleState.SUBMITTING, evidence={"seal": "durable"})
            lifecycle.transition(OrderLifecycleState.UNKNOWN, evidence={"reason": "ACK_LOST"})
            with self.assertRaisesRegex(ValueError, "INVALID_ORDER_TRANSITION"):
                lifecycle.transition(OrderLifecycleState.SUBMITTING, evidence={})
            lifecycle.transition(OrderLifecycleState.RECONCILED, evidence={"native_order_id": "NATIVE-1"})
            self.assertEqual(lifecycle.current_state(), OrderLifecycleState.RECONCILED)

    def test_protection_failure_immediately_records_emergency_exit(self) -> None:
        with TemporaryDirectory() as directory:
            protection = ProtectionLifecycle(LiveEventStore(Path(directory) / "l3h.sqlite3"), "BZ-L3H-PROTECT")
            self.assertEqual(protection.fail_closed("STOP_REJECTED"), ProtectionState.EMERGENCY_EXIT)
            with self.assertRaisesRegex(ValueError, "TERMINALLY_UNRESOLVED"):
                protection.transition(ProtectionState.PROTECTED, evidence={})


if __name__ == "__main__":
    unittest.main()
