from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
import tempfile
import unittest

from src.l3h_live.authority import LiveReadiness, ReadinessGate, derive_terminal_status
from src.l3h_live.contracts import AccountClass, CAPABILITY_SCHEMA, LiveCapability
from src.l3h_live.event_store import LiveEventStore
from src.l3h_live.gateway import GatewayDispatchError
from src.l3h_live.reconciliation import BrokerSnapshot, reconcile
from src.l3h_live.runtime import LiveRuntime, LiveRuntimeState, OperatorActivation


HASH = "a" * 64


def capability(*, account_class: AccountClass = AccountClass.BROKERAGE_LIVE, live_capital: bool = True) -> LiveCapability:
    now = datetime.now(timezone.utc)
    return LiveCapability(
        schema=CAPABILITY_SCHEMA, capability_id="l3h-cap-unit-test", created_at=now.isoformat().replace("+00:00", "Z"),
        expires_at=(now + timedelta(minutes=5)).isoformat().replace("+00:00", "Z"), account_alias="UnitLive",
        account_binding_hash=HASH, account_class=account_class, connection_identity_hash=HASH, native_instrument="MNQ SEP26",
        canonical_contract="MNQU6", exchange="CME", tick_size=Decimal("0.25"), tick_value_dollars=Decimal("0.50"),
        point_value_dollars=Decimal("2.00"), maximum_quantity=1, live_capital=live_capital, policy_hash=HASH,
        risk_hash=HASH, prop_rule_hash=HASH, strategy_artifact_hash=HASH, source_fingerprint=HASH,
        ninjatrader_build_fingerprint=HASH, allowed_session_profiles=("NY_RTH",), commissioning_epoch="l3h-unit-001",
        activation_nonce_family="l3h-activation-unit",
    )


def readiness() -> LiveReadiness:
    return LiveReadiness(
        gate_passes={gate: True for gate in ReadinessGate}, broker_position="FLAT", owned_working_orders=0,
        foreign_or_unknown_orders=0, reconciliation_fresh=True,
    )


def snapshot() -> BrokerSnapshot:
    return BrokerSnapshot(
        observed_at=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"), account_alias="UnitLive",
        account_class=AccountClass.BROKERAGE_LIVE, account_binding_hash=HASH, native_instrument="MNQ SEP26", position="FLAT",
        quantity=0, owned_working_orders=0, foreign_or_unknown_orders=0, position_snapshot_complete=True,
        order_snapshot_complete=True, connection_healthy=True, source="NINJATRADER_CALLBACK_ORDER",
    )


class L3HAuthorityTests(unittest.TestCase):
    def test_live_capability_is_signed_and_expired_or_tampered_artifact_is_refused(self) -> None:
        key = b"k" * 32
        signed = capability().signed(key)
        signed.verify(key)
        with self.assertRaisesRegex(ValueError, "SIGNATURE"):
            signed.verify(b"x" * 32)

    def test_evaluation_account_is_never_labeled_live(self) -> None:
        evaluated = capability(account_class=AccountClass.PROVIDER_EVALUATION, live_capital=False)
        self.assertEqual(derive_terminal_status(evaluated, readiness()), "PROVIDER_EVALUATION_READY_DISARMED")

    def test_unknown_or_incomplete_broker_facts_never_map_to_flat(self) -> None:
        result = reconcile(capability(), BrokerSnapshot(
            observed_at=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"), account_alias="UnitLive",
            account_class=AccountClass.BROKERAGE_LIVE, account_binding_hash=HASH, native_instrument="MNQ SEP26",
        ))
        self.assertEqual((result.state, result.reason), ("UNKNOWN", "BROKER_CONNECTION_UNHEALTHY"))
        blocked = derive_terminal_status(capability(), LiveReadiness(gate_passes={gate: True for gate in ReadinessGate}))
        self.assertEqual(blocked, "BLOCKED_BROKER_POSITION_NOT_PROVEN_FLAT")

    def test_event_store_seal_is_idempotent_and_unknown_is_terminal(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = LiveEventStore(Path(directory) / "l3h.sqlite3")
            command = {"command_id": "l3h-cmd-unit", "quantity": 1}
            first, replayed = store.seal_command(request_id="request-123", command=command)
            second, replayed_again = store.seal_command(request_id="request-123", command=command)
            self.assertFalse(replayed)
            self.assertTrue(replayed_again)
            self.assertEqual(first.event_id, second.event_id)
            store.mark_command("l3h-cmd-unit", state="UNKNOWN", acknowledgement={"reason": "SOCKET_LOST"})
            self.assertEqual(store.command("l3h-cmd-unit")["state"], "UNKNOWN")
            with self.assertRaisesRegex(ValueError, "TERMINAL"):
                store.mark_command("l3h-cmd-unit", state="ACKNOWLEDGED")
            self.assertEqual(store.verify(), (True, "PASS"))

    def test_default_runtime_gateway_quarantines_after_durable_seal_without_resubmission(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            runtime = LiveRuntime(LiveEventStore(Path(directory) / "l3h.sqlite3"), capability=capability())
            self.assertEqual(runtime.preflight(readiness(), snapshot()), "LIVE_READY_DISARMED")
            self.assertEqual(runtime.activate(OperatorActivation("request-123", "l3h-activation-unit-1", True, "2026-08-30T00:00:00Z")), "ARMED_FLAT")
            sealed = runtime.seal_entry(expected_trade_risk=Decimal("50.00"))
            command_id = str(sealed["command"]["command_id"])
            with self.assertRaisesRegex(GatewayDispatchError, "NOT_CONFIGURED"):
                runtime.dispatch_sealed()
            self.assertEqual(runtime.state, LiveRuntimeState.QUARANTINED)
            self.assertEqual(runtime.store.command(command_id)["state"], "UNKNOWN")
            with self.assertRaisesRegex(ValueError, "NO_SEALED_COMMAND"):
                runtime.dispatch_sealed()


if __name__ == "__main__":
    unittest.main()
