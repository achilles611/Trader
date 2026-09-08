from __future__ import annotations

import asyncio
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from fastapi import HTTPException
from starlette.requests import Request

from src.copytrade.config import CopyTradeConfig
from src.copytrade.control_center import create_control_center_app
from src.lane_iii.contracts import canonical_hash
from src.l3g_paper.contracts import PaperDirection, PaperRuntimeState, resolve_paper_profile
from src.l3g_paper.ledger import PaperLedger
from src.l3g_paper.ninjatrader_transport import (
    ADDON_PROTOCOL_VERSION,
    PaperExecutionTransport,
    expected_addon_source_fingerprint,
)
from src.l3g_paper.reconciliation_recovery import (
    INCIDENT,
    LOCKOUTS,
    OPERATOR_STATEMENT,
    ORDER_IDENTITIES,
    ReconciliationRecoveryService,
)
from src.l3g_paper.risk import PaperRiskSnapshot
from src.l3g_paper.runtime import LaneIIIPaperRuntime


class ReconciliationRecoveryTests(unittest.TestCase):
    @staticmethod
    def valid_request(authority_hash: str = "a" * 64) -> dict[str, object]:
        return {
            "request_id": "incident-8631-recovery-test",
            "operator": "Joseph",
            "incident_sequence": 8631,
            "incident_hash": INCIDENT["record_hash"],
            "lockout_sequences": [8632, 8636],
            "order_identities": list(ORDER_IDENTITIES),
            "account": "Sim101",
            "environment": "LOCAL_SIMULATION",
            "instrument": "MNQ SEP26",
            "operator_statement": OPERATOR_STATEMENT,
            "expected_lockout_reason": "RECONCILIATION_BLOCKED",
            "authority_state_hash": authority_hash,
        }

    @staticmethod
    def runtime_fixture(directory: str) -> tuple[PaperLedger, LaneIIIPaperRuntime, PaperExecutionTransport]:
        profile = resolve_paper_profile("BEELZEBUB_FIVE_MINUTE_BIAS_V1")
        ledger = PaperLedger(
            Path(directory) / "paper.sqlite3", policy=profile.policy, risk=profile.risk,
        )
        runtime = LaneIIIPaperRuntime(ledger)
        transport = PaperExecutionTransport(
            ledger, port=49136, policy=profile.policy, risk=profile.risk,
        )
        runtime.bind_transport(transport)
        with transport._lock:
            transport._state = "AUTHENTICATED"
            transport._authenticated = True
            transport._reconciled = True
            transport._client = object()  # type: ignore[assignment]
            transport._execution_session_id = "l3g-es-recovery-test"
            transport._addon_protocol_version = ADDON_PROTOCOL_VERSION
            transport._addon_source_fingerprint = expected_addon_source_fingerprint()
        runtime._state = PaperRuntimeState.READY_DISARMED
        runtime._snapshot = PaperRiskSnapshot(
            "2026-09-07T23:30:00Z",
            account_name="Sim101", account_class="LOCAL_SIMULATION", instrument="MNQ SEP26",
            current_position=PaperDirection.FLAT, current_position_quantity=0,
            working_owned_orders=0, working_entry_orders=0,
            position_snapshot_complete=True, order_snapshot_complete=True,
            foreign_activity=False, protective_stop_state="NONE",
            reconciliation_current=True, execution_bridge_healthy=True,
        )
        runtime.risk.restore_lockout(True, "RECONCILIATION_BLOCKED", None)
        runtime._entries_paused = True
        return ledger, runtime, transport

    @staticmethod
    def append_incident_chain(ledger: PaperLedger) -> tuple[dict[str, object], tuple[dict[str, object], ...]]:
        ledger.append(
            "INCIDENT_SAFETY_EVENT", {"reason_code": "FOREIGN_ORDER_ACTIVITY"},
            identity="test-incident",
        )
        incident_record = ledger.record_by_identity("test-incident")
        assert incident_record is not None
        first_payload = {
            "locked_out": True,
            "lockout_reason": "NINJATRADER_SAFETY_EVENT:FOREIGN_ORDER_ACTIVITY",
            "lockout_trade_date": None,
            "effective_trade_date": None,
            "effect": "ENTRY_AUTHORITY_LOCKED",
        }
        ledger.append("RISK_EVENT_AUTHORITY_LOCKOUT", first_payload, identity="test-lockout-1")
        ledger.append("OBSERVATION", {"filler": True}, identity="test-filler")
        second_payload = {
            "locked_out": True,
            "lockout_reason": "RECONCILIATION_BLOCKED",
            "lockout_trade_date": None,
            "effective_trade_date": None,
            "effect": "ENTRY_AUTHORITY_LOCKED",
        }
        ledger.append("RISK_EVENT_AUTHORITY_LOCKOUT", second_payload, identity="test-lockout-2")
        first = ledger.record_by_identity("test-lockout-1")
        second = ledger.record_by_identity("test-lockout-2")
        assert first is not None and second is not None
        incident = {
            "ledger_sequence": incident_record["ledger_sequence"],
            "record_hash": incident_record["record_hash"],
            "identity": "test-incident",
            "reason": "FOREIGN_ORDER_ACTIVITY",
        }
        lockouts = (
            {
                "ledger_sequence": first["ledger_sequence"],
                "record_hash": first["record_hash"],
                "lockout_reason": first_payload["lockout_reason"],
            },
            {
                "ledger_sequence": second["ledger_sequence"],
                "record_hash": second["record_hash"],
                "lockout_reason": second_payload["lockout_reason"],
            },
        )
        return incident, lockouts

    @staticmethod
    def safe_probe() -> dict[str, object]:
        result = {
            "account_name": "Sim101", "account_class": "LOCAL_SIMULATION",
            "instrument": "MNQ SEP26", "position_quantity": 0,
            "working_order_count": 0, "working_entry_count": 0,
            "position_snapshot_complete": True, "order_snapshot_complete": True,
            "foreign_activity": False, "protective_stop_state": "NONE",
        }
        return {
            "observations": [{"probe_result": dict(result)}, {"probe_result": dict(result)}],
            "commands_sent": 0, "proof_hash": "b" * 64,
        }

    def test_route_rejects_missing_authentication(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            defaults = CopyTradeConfig()
            config = replace(
                defaults,
                storage=replace(defaults.storage, cold_root=root / "cold"),
                artifacts=replace(defaults.artifacts, database_path=root / "hot" / "copy.sqlite3"),
            )
            app = create_control_center_app(config)
            endpoint = next(
                route.endpoint for route in app.routes
                if route.path == "/api/lane-iii/paper/reconciliation-recovery"
                and "POST" in route.methods
            )
            request = Request({
                "type": "http", "method": "POST", "path": "/", "headers": [],
                "scheme": "http", "server": ("testserver", 80), "client": ("127.0.0.1", 1),
                "query_string": b"",
            })
            with self.assertRaises(HTTPException) as raised:
                asyncio.run(endpoint(request, self.valid_request()))
            self.assertEqual(raised.exception.status_code, 403)

    def test_wrong_incident_hash_and_order_identity_are_rejected(self) -> None:
        for key, value in (
            ("incident_hash", "0" * 64),
            ("order_identities", ["not-the-recorded-order"]),
        ):
            body = self.valid_request()
            body[key] = value
            with self.assertRaisesRegex(ValueError, "does not match incident 8631"):
                ReconciliationRecoveryService._validated_request(body)

    def test_live_account_input_is_rejected(self) -> None:
        body = self.valid_request()
        body["account"] = "LucidFlex25k"
        with self.assertRaisesRegex(ValueError, "does not match incident 8631"):
            ReconciliationRecoveryService._validated_request(body)

    def test_stale_authority_state_is_rejected(self) -> None:
        with TemporaryDirectory() as directory:
            ledger, runtime, _ = self.runtime_fixture(directory)
            try:
                with self.assertRaisesRegex(RuntimeError, "AUTHORITY_STATE_STALE"):
                    runtime.begin_reconciliation_recovery_lease("lease-test", "0" * 64)
            finally:
                ledger.close()

    def test_incomplete_nonflat_working_and_other_lockout_snapshots_fail_closed(self) -> None:
        authority = {
            "entry_profile_version": "BEELZEBUB_FIVE_MINUTE_BIAS_V1",
            "runtime_state": "READY_DISARMED", "entry_owner": "NONE",
            "operational_owner_active": False, "runtime_position": "FLAT", "runtime_quantity": 0,
            "account": "Sim101", "environment": "LOCAL_SIMULATION", "instrument": "MNQ SEP26",
            "broker_position": "FLAT", "broker_quantity": 0, "working_orders": 0,
            "working_entry_orders": 0, "position_snapshot_complete": True,
            "order_snapshot_complete": True, "foreign_activity": False,
            "protective_stop_state": "NONE", "reconciliation_current": True,
            "unresolved_command": False, "unresolved_native_order": False,
            "unresolved_execution": False, "risk_locked_out": True,
            "risk_lockout_reason": "RECONCILIATION_BLOCKED", "risk_lockout_trade_date": None,
            "risk_continuity_fault": None, "retained_safety_lockout": False,
            "fault_reason": None, "transport_authenticated": True,
            "transport_reconciled": True, "transport_addon_provenance_valid": True,
            "paper_only": True, "live_capital": "DENIED", "maximum_quantity": 1,
        }
        mutations = (
            ("position_snapshot_complete", False),
            ("broker_quantity", 1),
            ("runtime_quantity", 1),
            ("working_orders", 1),
            ("working_entry_orders", 1),
            ("risk_lockout_reason", "ANOTHER_ACTIVE_LOCKOUT"),
            ("environment", "LIVE_BROKERAGE"),
            ("unresolved_execution", True),
        )
        self.assertTrue(LaneIIIPaperRuntime._reconciliation_recovery_safe_snapshot(authority))
        for key, value in mutations:
            candidate = {**authority, key: value}
            self.assertFalse(
                LaneIIIPaperRuntime._reconciliation_recovery_safe_snapshot(candidate), key,
            )

    def test_new_order_activity_between_ack_and_clear_is_a_race_failure(self) -> None:
        runtime = object.__new__(LaneIIIPaperRuntime)

        class Ledger:
            @staticmethod
            def authority_records_after(_: int) -> list[dict[str, object]]:
                return [{
                    "kind": "ORDER_EVENT", "record": {"payload": {"order_state": "WORKING"}},
                }]

        runtime.ledger = Ledger()  # type: ignore[assignment]
        with self.assertRaisesRegex(RuntimeError, "CONFLICTING_AUTHORITY"):
            runtime._assert_reconciliation_recovery_suffix_locked(10)

    def test_persists_acknowledgement_clear_zero_commands_and_replays_after_restart(self) -> None:
        with TemporaryDirectory() as directory:
            ledger, runtime, transport = self.runtime_fixture(directory)
            try:
                incident, lockouts = self.append_incident_chain(ledger)
                authority = runtime.reconciliation_recovery_authority()
                lease = runtime.begin_reconciliation_recovery_lease(
                    "lease-test", str(authority["authority_state_hash"]),
                )
                request_id = "incident-8631-recovery-test"
                request_hash = canonical_hash(self.valid_request(str(authority["authority_state_hash"])))
                ack_identity = "test-ack"
                ledger.append(
                    "RISK_EVENT_OPERATOR_INCIDENT_ACKNOWLEDGEMENT",
                    {"request_id": request_id, "request_hash": request_hash},
                    identity=ack_identity,
                )
                ack_record = ledger.record_by_identity(ack_identity)
                assert ack_record is not None
                acknowledgement = {
                    "identity": ack_identity,
                    "ledger_sequence": ack_record["ledger_sequence"],
                    "record_hash": ack_record["record_hash"],
                }
                proof = self.safe_probe()
                result = runtime.complete_reconciliation_recovery(
                    lease_id="lease-test", request_id=request_id, request_hash=request_hash,
                    incident=incident, lockouts=lockouts, acknowledgement=acknowledgement,
                    proof=proof, evidence_digest=canonical_hash(proof),
                    commands_before=int(lease["transport_commands_sent"]),
                )
                self.assertEqual(result["status"], "RECOVERED")
                self.assertEqual(result["transport_command_delta"], 0)
                self.assertEqual(transport.status().commands_sent, 0)
                self.assertFalse(runtime.risk.status()["locked_out"])
                kinds = [record["kind"] for record in ledger.recent(20)]
                self.assertEqual(kinds.count("RISK_EVENT_OPERATOR_INCIDENT_ACKNOWLEDGEMENT"), 1)
                self.assertEqual(kinds.count("RISK_EVENT_RECONCILIATION_LOCKOUT_CLEARED"), 1)

                replay = ReconciliationRecoveryService._replay(ledger, request_id, request_hash)
                assert replay is not None
                self.assertTrue(replay["idempotent_replay"])
                self.assertEqual(
                    [record["kind"] for record in ledger.recent(20)].count(
                        "RISK_EVENT_RECONCILIATION_LOCKOUT_CLEARED"
                    ),
                    1,
                )
                recovered = LaneIIIPaperRuntime(ledger)
                self.assertFalse(recovered.risk.status()["locked_out"])
            finally:
                ledger.close()

    def test_replay_conflict_is_rejected(self) -> None:
        with TemporaryDirectory() as directory:
            ledger, runtime, _ = self.runtime_fixture(directory)
            try:
                request_id = "incident-8631-recovery-test"
                identity = "l3g-reconciliation-lockout-clear-" + canonical_hash(
                    {"request_id": request_id}
                )
                ledger.append(
                    "RISK_EVENT_RECONCILIATION_LOCKOUT_CLEARED",
                    {"request_hash": "a" * 64}, identity=identity,
                )
                with self.assertRaisesRegex(RuntimeError, "REQUEST_REPLAY_CONFLICT"):
                    ReconciliationRecoveryService._replay(ledger, request_id, "b" * 64)
            finally:
                ledger.close()


if __name__ == "__main__":
    unittest.main()
