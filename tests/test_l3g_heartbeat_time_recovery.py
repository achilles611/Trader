from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
import socket
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from src.copytrade.control_center import (
    HeartbeatTimeRecoveryBoundaryError,
    _recover_heartbeat_time_at_stable_ledger_tip,
)
from src.l3g_paper.contracts import PaperRuntimeState
from src.l3g_paper.ledger import PaperLedger
from src.l3g_paper.ninjatrader_transport import (
    ADDON_PROTOCOL_VERSION,
    PaperExecutionTransport,
    expected_addon_source_fingerprint,
)
from src.l3g_paper.runtime import LaneIIIPaperRuntime


def wire(moment: datetime) -> str:
    return moment.isoformat().replace("+00:00", "Z")


def free_port() -> int:
    candidate = socket.socket()
    candidate.bind(("127.0.0.1", 0))
    port = int(candidate.getsockname()[1])
    candidate.close()
    return port


class HeartbeatTimeRecoveryTests(unittest.TestCase):
    def _runtime(
        self, directory: str,
    ) -> tuple[PaperLedger, LaneIIIPaperRuntime, PaperExecutionTransport, Path]:
        path = Path(directory) / "paper.sqlite3"
        ledger = PaperLedger(path)
        runtime = LaneIIIPaperRuntime(ledger)
        transport = PaperExecutionTransport(ledger, port=free_port())
        runtime.bind_transport(transport)

        old_session = "l3g-es-heartbeat-time-incident"
        with transport._lock:
            transport._execution_session_id = old_session
        runtime._entries_paused = True
        runtime._fault_reason = "EXECUTION_COMMAND_REJECTED:HEARTBEAT_TIME"
        runtime.risk.lock_out("EXECUTION_COMMAND_REJECTED:HEARTBEAT_TIME")

        now = datetime.now(timezone.utc)
        session_id = "l3g-es-heartbeat-time-recovery"
        source = expected_addon_source_fingerprint()
        with transport._lock:
            transport._state = "AUTHENTICATED"
            transport._authenticated = True
            transport._reconciled = True
            transport._execution_session_id = session_id
            transport._addon_protocol_version = ADDON_PROTOCOL_VERSION
            transport._addon_source_fingerprint = source
            transport._addon_build_fingerprint = "a" * 64
            transport._addon_build_timestamp = wire(now - timedelta(seconds=10))
            transport._authenticated_at = wire(now - timedelta(seconds=8))
            transport._last_heartbeat_sent_at = wire(now)
            transport._heartbeats_sent_current_session = 7
            transport._last_heartbeat_send_error = None
            transport._command_rejections_at_session_start = 0
            transport._command_rejections = 0
            transport._heartbeat_send_lock_timeouts_at_session_start = 0
            transport._heartbeat_send_lock_timeouts = 0
            transport._commands_sent_at_session_start = 0
            transport._commands_sent = 0

        reconciliation = {
            "schema": "lane-iii-phase-g-paper-execution-v1",
            "message_type": "RECONCILIATION",
            "execution_session_id": session_id,
            "receipt_id": "l3g-heartbeat-time-recovery-flat",
            "timestamp": wire(now),
            "account_name": "Sim101",
            "account_class": "LOCAL_SIMULATION",
            "instrument": "MNQ SEP26",
            "position_quantity": 0,
            "working_order_count": 0,
            "working_entry_count": 0,
            "position_snapshot_complete": True,
            "order_snapshot_complete": True,
            "foreign_activity": False,
            "protective_stop_state": "NONE",
        }
        ledger.append(
            "POSITION_SNAPSHOT_RECONCILIATION",
            reconciliation,
            identity=reconciliation["receipt_id"],
            execution_session_id=session_id,
        )
        runtime._snapshot = replace(
            runtime._snapshot,
            observed_at=wire(now),
            position_snapshot_complete=True,
            order_snapshot_complete=True,
            reconciliation_current=True,
            execution_bridge_healthy=True,
            protective_stop_state="NONE",
        )
        runtime._state = PaperRuntimeState.READY_DISARMED
        return ledger, runtime, transport, path

    @staticmethod
    def _verified_readiness(ledger: PaperLedger) -> dict[str, object]:
        tip = int(ledger.health_status()["highest_sequence"])
        record = ledger.record_by_sequence(tip)
        assert record is not None
        return {
            "result": "BLOCKED",
            "blocking_reasons": ["EXECUTION_COMMAND_REJECTED:HEARTBEAT_TIME"],
            "ledger": {
                "ledger_trust_state": "VERIFIED_TO_ARM_SNAPSHOT_TIP",
                "verified_through_sequence": tip,
                "arm_snapshot_tip": tip,
                "arm_snapshot_tip_hash": record["record_hash"],
                "unverified_tail_rows": 0,
            },
        }

    def test_reason_specific_recovery_preserves_counters_and_survives_restart(self) -> None:
        with TemporaryDirectory() as directory:
            ledger, runtime, _transport, path = self._runtime(directory)
            before = runtime.risk_continuity_snapshot()
            readiness = self._verified_readiness(ledger)
            runtime.operational_paper_readiness = (  # type: ignore[method-assign]
                lambda _preflight=None: readiness
            )
            try:
                with patch(
                    "src.l3g_paper.ledger.deferred_capacity_allows_authority",
                    return_value=True,
                ):
                    result = runtime.recover_heartbeat_time_lockout(
                        "heartbeat-time-recovery-test",
                    )
                self.assertTrue(result["recovered"], result)
                self.assertFalse(result["armed"])
                self.assertFalse(result["operational_paper_started"])
                self.assertFalse(runtime.risk.status()["locked_out"])
                self.assertEqual(runtime.state, PaperRuntimeState.READY_DISARMED)
                after = runtime.risk_continuity_snapshot()
                self.assertEqual(after["trade_dates"], before["trade_dates"])
                self.assertEqual(after["profile_trade_dates"], before["profile_trade_dates"])
                clear = ledger.recent_kind_records(
                    ("RISK_EVENT_AUTHORITY_LOCKOUT_CLEARED",), 1,
                )[0]
                payload = clear["record"]["payload"]  # type: ignore[index]
                self.assertEqual(
                    payload["effect"],  # type: ignore[index]
                    "ONLY_PROVEN_RETIRED_HEARTBEAT_TIME_LOCKOUT_CLEARED",
                )
            finally:
                ledger.close()

            reopened = PaperLedger(path)
            try:
                recovered_runtime = LaneIIIPaperRuntime(reopened)
                self.assertFalse(recovered_runtime.risk.status()["locked_out"])
                self.assertEqual(
                    recovered_runtime.risk_continuity_snapshot()["trade_dates"],
                    before["trade_dates"],
                )
            finally:
                reopened.close()

    def test_new_rejection_or_wire_lock_timeout_keeps_lockout(self) -> None:
        for fault in ("rejection", "lock_timeout"):
            with self.subTest(fault=fault), TemporaryDirectory() as directory:
                ledger, runtime, transport, _path = self._runtime(directory)
                readiness = self._verified_readiness(ledger)
                runtime.operational_paper_readiness = (  # type: ignore[method-assign]
                    lambda _preflight=None: readiness
                )
                with transport._lock:
                    if fault == "rejection":
                        transport._command_rejections += 1
                    else:
                        transport._heartbeat_send_lock_timeouts += 1
                try:
                    with patch(
                        "src.l3g_paper.ledger.deferred_capacity_allows_authority",
                        return_value=True,
                    ):
                        result = runtime.recover_heartbeat_time_lockout(
                            "heartbeat-time-must-stay-locked",
                        )
                    self.assertFalse(result["recovered"])
                    self.assertTrue(result["locked_out"])
                    expected = (
                        "HEARTBEAT_TIME_RECOVERY_NEW_REJECTION"
                        if fault == "rejection"
                        else "HEARTBEAT_TIME_RECOVERY_LOCK_TIMEOUT"
                    )
                    self.assertIn(expected, result["reason_codes"])
                    self.assertEqual(
                        ledger.recent_kind_records(
                            ("RISK_EVENT_AUTHORITY_LOCKOUT_CLEARED",), 1,
                        ),
                        [],
                    )
                finally:
                    ledger.close()


class HeartbeatTimeRecoveryBoundaryTests(unittest.IsolatedAsyncioTestCase):
    class Verifier:
        def __init__(self, terminal: dict[str, object] | None = None) -> None:
            self.events: list[str] = []
            self.running = False
            self.polls = 0
            self.terminal = terminal or {
                "status": "PASS",
                "verification_id": "lv-heartbeat-fixed-tip",
                "verification_mode": "full",
                "verified_through_sequence": 42,
            }

        def status(self) -> dict[str, object]:
            self.events.append("status")
            if not self.running:
                return {"status": "PASS"}
            self.polls += 1
            if self.polls == 1:
                return {
                    "status": "IN_PROGRESS",
                    "verification_id": "lv-heartbeat-fixed-tip",
                    "requested_mode": "full",
                }
            return dict(self.terminal)

        def start(self, mode: str) -> dict[str, object]:
            self.events.append("start:" + mode)
            self.running = True
            return {
                "status": "IN_PROGRESS",
                "verification_id": "lv-heartbeat-fixed-tip",
                "requested_mode": mode,
            }

        def cancel(self) -> dict[str, object]:
            self.events.append("cancel")
            return {"status": "IN_PROGRESS", "cancellation_requested": True}

    class Paper:
        def __init__(self, events: list[str]) -> None:
            self.events = events

        def heartbeat_time_recovery_authority(self) -> dict[str, object]:
            return {"eligible_without_ledger_proof": True}

        def recover_heartbeat_time_lockout(
            self, request_id: str, preflight=None,
        ) -> dict[str, object]:
            self.events.append("recover")
            evidence = preflight("heartbeat-operation", {"tip": 42})
            return {
                "request_id": request_id,
                "recovered": evidence.get("accepted") is True,
                "armed": False,
                "operational_paper_started": False,
            }

    async def test_full_verification_and_recovery_share_one_paused_tip(self) -> None:
        events: list[str] = []
        verifier = self.Verifier()

        def pause() -> dict[str, object]:
            events.append("pause-and-drain")
            return {"paused": True, "drained": True}

        def preflight(operation_id: str, snapshot: object) -> dict[str, object]:
            self.assertEqual(operation_id, "heartbeat-operation")
            self.assertEqual(snapshot, {"tip": 42})
            events.append("preflight")
            return {"accepted": True}

        def resume() -> dict[str, object]:
            events.append("resume-and-replay")
            return {"paused": False, "replayed_observations": 3}

        result = await _recover_heartbeat_time_at_stable_ledger_tip(
            paper=self.Paper(events),  # type: ignore[arg-type]
            ledger_verifier=verifier,  # type: ignore[arg-type]
            begin_observation_pause=pause,
            end_observation_pause=resume,
            ledger_preflight=preflight,
            request_id="heartbeat-boundary-pass",
            timeout_seconds=1,
            poll_seconds=0.001,
        )

        self.assertTrue(result["recovered"])
        self.assertFalse(result["armed"])
        self.assertEqual(
            events,
            ["pause-and-drain", "recover", "preflight", "resume-and-replay"],
        )
        self.assertEqual(
            verifier.events,
            ["status", "start:full", "status", "status"],
        )
        self.assertEqual(result["ledger_boundary"]["replayed_observations"], 3)

    async def test_failed_full_verification_replays_without_recovery(self) -> None:
        events: list[str] = []
        verifier = self.Verifier({
            "status": "FAIL",
            "verification_id": "lv-heartbeat-fixed-tip",
            "verification_mode": "full",
            "errors": [{"code": "HASH_CHAIN_MISMATCH"}],
        })

        with self.assertRaises(HeartbeatTimeRecoveryBoundaryError) as refused:
            await _recover_heartbeat_time_at_stable_ledger_tip(
                paper=self.Paper(events),  # type: ignore[arg-type]
                ledger_verifier=verifier,  # type: ignore[arg-type]
                begin_observation_pause=lambda: (
                    events.append("pause") or {"paused": True, "drained": True}
                ),
                end_observation_pause=lambda: (
                    events.append("resume") or {"paused": False}
                ),
                ledger_preflight=lambda *_: {"accepted": True},
                request_id="heartbeat-boundary-fail",
                timeout_seconds=1,
                poll_seconds=0.001,
            )

        self.assertEqual(refused.exception.status_code, 409)
        self.assertIn("HASH_CHAIN_MISMATCH", str(refused.exception.detail))
        self.assertEqual(events, ["pause", "resume"])

    async def test_writer_drain_failure_replays_and_never_verifies(self) -> None:
        events: list[str] = []
        verifier = self.Verifier()

        with self.assertRaises(HeartbeatTimeRecoveryBoundaryError) as refused:
            await _recover_heartbeat_time_at_stable_ledger_tip(
                paper=self.Paper(events),  # type: ignore[arg-type]
                ledger_verifier=verifier,  # type: ignore[arg-type]
                begin_observation_pause=lambda: {
                    "paused": True, "drained": False,
                },
                end_observation_pause=lambda: (
                    events.append("resume") or {"paused": False}
                ),
                ledger_preflight=lambda *_: {"accepted": True},
                request_id="heartbeat-boundary-drain-fail",
            )

        self.assertEqual(
            refused.exception.detail,
            "HEARTBEAT_TIME_RECOVERY_LEDGER_WRITER_DRAIN_FAILED",
        )
        self.assertEqual(verifier.events, ["status"])
        self.assertEqual(events, ["resume"])


if __name__ == "__main__":
    unittest.main()
