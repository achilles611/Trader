from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from src.l3h_live.contracts import AccountClass, CAPABILITY_SCHEMA, LiveCapability
from src.l3h_live.event_store import LiveEventStore
from src.l3h_live.reconciliation import BrokerSnapshot, ExecutionSupervisor


HASH = "b" * 64


def capability() -> LiveCapability:
    now = datetime.now(timezone.utc)
    return LiveCapability(
        schema=CAPABILITY_SCHEMA, capability_id="l3h-cap-reconciliation", created_at=now.isoformat().replace("+00:00", "Z"),
        expires_at=(now + timedelta(minutes=5)).isoformat().replace("+00:00", "Z"), account_alias="UnitLive",
        account_binding_hash=HASH, account_class=AccountClass.BROKERAGE_LIVE, connection_identity_hash=HASH,
        native_instrument="MNQ SEP26", canonical_contract="MNQU6", exchange="CME", tick_size=Decimal("0.25"),
        tick_value_dollars=Decimal("0.50"), point_value_dollars=Decimal("2.00"), maximum_quantity=1, live_capital=True,
        policy_hash=HASH, risk_hash=HASH, prop_rule_hash=HASH, strategy_artifact_hash=HASH, source_fingerprint=HASH,
        ninjatrader_build_fingerprint=HASH, allowed_session_profiles=("NY_RTH",), commissioning_epoch="l3h-unit-002",
        activation_nonce_family="l3h-activation-reconciliation",
    )


def snapshot(*, foreign: int = 0) -> BrokerSnapshot:
    return BrokerSnapshot(
        observed_at=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"), account_alias="UnitLive",
        account_class=AccountClass.BROKERAGE_LIVE, account_binding_hash=HASH, native_instrument="MNQ SEP26",
        position="FLAT", quantity=0, owned_working_orders=0, foreign_or_unknown_orders=foreign,
        position_snapshot_complete=True, order_snapshot_complete=True, connection_healthy=True,
    )


class L3HReconciliationTests(unittest.TestCase):
    def test_startup_requires_native_clean_flat_and_foreign_activity_quarantines(self) -> None:
        with TemporaryDirectory() as directory:
            supervisor = ExecutionSupervisor(capability(), LiveEventStore(Path(directory) / "l3h.sqlite3"))
            self.assertEqual(supervisor.reconcile_startup(snapshot()).state, "FLAT")
            self.assertTrue(supervisor.ready_disarmed)
            self.assertEqual(supervisor.reconcile_startup(snapshot(foreign=1)).reason, "FOREIGN_OR_UNKNOWN_ACTIVITY")
            self.assertFalse(supervisor.ready_disarmed)
            self.assertEqual(supervisor.quarantined_reason, "FOREIGN_OR_UNKNOWN_ACTIVITY")


if __name__ == "__main__":
    unittest.main()
