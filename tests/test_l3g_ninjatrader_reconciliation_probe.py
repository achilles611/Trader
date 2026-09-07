from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import socket
import threading
import time
import unittest
from unittest.mock import patch

from src.l3g_paper.contracts import ACCOUNT_BINDING, canonical_json
from src.l3g_paper.ninjatrader_reconciliation_probe import (
    PROBE_SCHEMA,
    NinjaTraderReconciliationProbeError,
    probe_flat_sim101_reconciliation,
    validate_flat_sim101_reconciliation_proof,
)
from src.l3g_paper.ninjatrader_transport import (
    ADDON_PROTOCOL_VERSION,
    EXECUTION_SCHEMA,
    expected_addon_source_fingerprint,
    sign_payload,
    verify_signature,
)


KEY = bytes(range(32))
BUILD_FINGERPRINT = "a" * 64
BUILD_TIMESTAMP = "2026-09-07T10:53:09.1234567Z"
POLICY_HASH = "b" * 64
RISK_HASH = "c" * 64


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _free_port() -> int:
    candidate = socket.socket()
    candidate.bind(("127.0.0.1", 0))
    port = candidate.getsockname()[1]
    candidate.close()
    return port


def _sign(payload: dict[str, object]) -> dict[str, object]:
    payload["signature"] = sign_payload(KEY, payload)
    return payload


def _hello(index: int) -> dict[str, object]:
    return _sign({
        "schema": EXECUTION_SCHEMA,
        "message_type": "HELLO",
        "bridge_instance_id": "bridge-probe",
        "ninjatrader_session_id": "ninjatrader-probe",
        "addon_protocol_version": ADDON_PROTOCOL_VERSION,
        "addon_source_fingerprint": expected_addon_source_fingerprint(),
        "addon_build_fingerprint": BUILD_FINGERPRINT,
        "addon_build_timestamp": BUILD_TIMESTAMP,
        "account_name": "Sim101",
        "account_class": "LOCAL_SIMULATION",
        "instrument": "MNQ SEP26",
        "capability": "PAPER_ONLY",
        "timestamp": _utc_now(),
        "nonce": f"hello-nonce-{index}",
    })


def _probe_result(
    probe_session_id: str, server_nonce: str, index: int,
) -> dict[str, object]:
    sample_hash = hashlib.sha256(f"flat-sample-{index}".encode()).hexdigest()
    return _sign({
        "schema": EXECUTION_SCHEMA,
        "message_type": "RECONCILIATION_PROBE_RESULT",
        "probe_session_id": probe_session_id,
        "server_nonce": server_nonce,
        "observation_generation": (index - 1) * 2,
        "first_sample_hash": sample_hash,
        "second_sample_hash": sample_hash,
        "snapshot_stable": True,
        "timestamp": _utc_now(),
        "receipt_id": f"probe-result-receipt-{index}",
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
    })


def _send(connection: socket.socket, payload: dict[str, object]) -> None:
    connection.sendall(canonical_json(payload) + b"\n")


def _connect(port: int) -> socket.socket:
    deadline = time.monotonic() + 2.0
    while True:
        try:
            connection = socket.create_connection(("127.0.0.1", port), timeout=0.2)
            connection.settimeout(2.0)
            return connection
        except OSError:
            if time.monotonic() >= deadline:
                raise
            time.sleep(0.005)


def _read(connection: socket.socket) -> dict[str, object]:
    buffer = bytearray()
    while b"\n" not in buffer:
        chunk = connection.recv(4096)
        if not chunk:
            raise AssertionError("probe closed before sending its status-only grant")
        buffer.extend(chunk)
    frame, _, _ = buffer.partition(b"\n")
    value = json.loads(frame)
    assert isinstance(value, dict)
    return value


class ProbeThread:
    def __init__(self, port: int, *, timeout_seconds: float = 2.0) -> None:
        self.port = port
        self.timeout_seconds = timeout_seconds
        self.proof: dict[str, object] | None = None
        self.error: BaseException | None = None
        self.thread = threading.Thread(target=self._run, daemon=True)

    def _run(self) -> None:
        try:
            self.proof = probe_flat_sim101_reconciliation(
                paper_policy_hash=POLICY_HASH,
                risk_profile_hash=RISK_HASH,
                timeout_seconds=self.timeout_seconds,
                _test_endpoint=("127.0.0.1", self.port),
                _test_secret=KEY,
            )
        except BaseException as error:  # surfaced by each assertion below
            self.error = error

    def start(self) -> "ProbeThread":
        self.thread.start()
        return self

    def finish(self) -> None:
        self.thread.join(3.0)
        if self.thread.is_alive():
            raise AssertionError("probe thread exceeded its bounded test deadline")


class NinjaTraderReconciliationProbeTests(unittest.TestCase):
    def _complete_session(
        self,
        port: int,
        index: int,
        *,
        hello: dict[str, object] | None = None,
        reconciliation_change=None,
    ) -> dict[str, object]:
        with _connect(port) as connection:
            _send(connection, hello or _hello(index))
            grant = _read(connection)
            self.assertEqual(grant["message_type"], "RECONCILIATION_PROBE_GRANT")
            self.assertTrue(verify_signature(KEY, grant))
            self.assertNotIn("execution_session_id", grant)
            self.assertNotIn("heartbeat_watchdog_seconds", grant)
            self.assertNotIn("command_ttl_seconds", grant)
            reconciliation = _probe_result(
                str(grant["probe_session_id"]), str(grant["server_nonce"]), index,
            )
            if reconciliation_change is not None:
                reconciliation_change(reconciliation)
            _send(connection, reconciliation)
            return grant

    def test_two_fresh_signed_sessions_return_hash_bound_flat_no_command_proof(self) -> None:
        port = _free_port()
        probe = ProbeThread(port).start()
        grants = [self._complete_session(port, index) for index in (1, 2)]
        probe.finish()

        self.assertIsNone(probe.error)
        assert probe.proof is not None
        proof = probe.proof
        self.assertEqual(proof["schema"], PROBE_SCHEMA)
        self.assertEqual(proof["status"], "PASS")
        self.assertEqual(proof["session_count"], 2)
        self.assertEqual(proof["commands_sent"], 0)
        self.assertEqual(proof["heartbeats_sent"], 0)
        self.assertIs(proof["paper_ledger_used"], False)
        self.assertEqual(
            [grant["message_type"] for grant in grants],
            ["RECONCILIATION_PROBE_GRANT"] * 2,
        )
        self.assertNotEqual(grants[0]["probe_session_id"], grants[1]["probe_session_id"])
        self.assertEqual(proof["session_grants_sent"], 0)
        unsigned = dict(proof)
        proof_signature = unsigned.pop("proof_signature")
        supplied_hash = unsigned.pop("proof_hash")
        self.assertEqual(supplied_hash, hashlib.sha256(canonical_json(unsigned)).hexdigest())
        signed_proof = dict(proof)
        signed_proof.pop("proof_signature")
        signed_proof["signature"] = proof_signature
        self.assertTrue(verify_signature(KEY, signed_proof))
        observations = proof["observations"]
        assert isinstance(observations, list)
        self.assertEqual(len(observations), 2)
        for observation in observations:
            self.assertTrue(verify_signature(KEY, observation["hello"]))
            self.assertTrue(verify_signature(KEY, observation["probe_grant"]))
            self.assertTrue(verify_signature(KEY, observation["probe_result"]))
            self.assertEqual(observation["probe_result"]["position_quantity"], 0)
            self.assertEqual(observation["probe_result"]["working_order_count"], 0)
            self.assertTrue(observation["probe_result"]["snapshot_stable"])

        with patch(
            "src.l3g_paper.ninjatrader_reconciliation_probe.LocalPaperSecretProvider.load_key",
            return_value=KEY,
        ):
            validated = validate_flat_sim101_reconciliation_proof(
                proof,
                expected_policy_hash=POLICY_HASH,
                expected_risk_hash=RISK_HASH,
                expected_at=str(proof["completed_at"]),
                _test_endpoint=("127.0.0.1", port),
                _test_secret=KEY,
            )
        self.assertEqual(validated, proof)

    def _assert_rejected(
        self,
        expected_code: str,
        *,
        stage: str,
        change,
        resign: bool = True,
    ) -> None:
        port = _free_port()
        probe = ProbeThread(port).start()
        with _connect(port) as connection:
            hello = _hello(1)
            if stage == "hello":
                change(hello)
                if resign:
                    _sign(hello)
                _send(connection, hello)
            else:
                _send(connection, hello)
                grant = _read(connection)
                reconciliation = _probe_result(
                    str(grant["probe_session_id"]), str(grant["server_nonce"]), 1,
                )
                change(reconciliation)
                if resign:
                    _sign(reconciliation)
                _send(connection, reconciliation)
        probe.finish()
        self.assertIsInstance(probe.error, NinjaTraderReconciliationProbeError)
        assert isinstance(probe.error, NinjaTraderReconciliationProbeError)
        self.assertEqual(probe.error.code, expected_code)
        self.assertIsNone(probe.proof)

    def test_hello_shape_signature_time_identity_and_provenance_fail_closed(self) -> None:
        stale = (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat().replace("+00:00", "Z")
        cases = (
            ("HELLO_SHAPE_INVALID", lambda value: value.__setitem__("extra", True), True),
            ("HELLO_SIGNATURE_INVALID", lambda value: value.__setitem__("signature", "0" * 64), False),
            ("HELLO_TIMESTAMP_NOT_FRESH", lambda value: value.__setitem__("timestamp", stale), True),
            ("HELLO_IDENTITY_MISMATCH", lambda value: value.__setitem__("account_name", "Lucid25kflex01"), True),
            ("HELLO_PROTOCOL_MISMATCH", lambda value: value.__setitem__("addon_protocol_version", "wrong"), True),
            ("HELLO_SOURCE_FINGERPRINT_MISMATCH", lambda value: value.__setitem__("addon_source_fingerprint", "e" * 64), True),
            ("HELLO_BUILD_PROVENANCE_MISMATCH", lambda value: value.__setitem__("addon_build_fingerprint", "wrong"), True),
            ("HELLO_BUILD_PROVENANCE_MISMATCH", lambda value: value.__setitem__("addon_build_timestamp", "wrong"), True),
        )
        for code, change, resign in cases:
            with self.subTest(code=code, change=change):
                self._assert_rejected(code, stage="hello", change=change, resign=resign)

    def test_reconciliation_shape_signature_time_session_and_safe_state_fail_closed(self) -> None:
        stale = (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat().replace("+00:00", "Z")
        cases = (
            ("PROBE_RESULT_SHAPE_INVALID", lambda value: value.__setitem__("extra", True), True),
            ("PROBE_RESULT_SIGNATURE_INVALID", lambda value: value.__setitem__("signature", "0" * 64), False),
            ("PROBE_RESULT_TIMESTAMP_NOT_FRESH", lambda value: value.__setitem__("timestamp", stale), True),
            ("PROBE_RESULT_CORRELATION_MISMATCH", lambda value: value.__setitem__("probe_session_id", "wrong"), True),
            ("PROBE_RESULT_IDENTITY_MISMATCH", lambda value: value.__setitem__("instrument", "ES SEP26"), True),
            ("PROBE_RESULT_NOT_FLAT", lambda value: value.__setitem__("position_quantity", -1), True),
            ("PROBE_RESULT_ORDERS_PRESENT", lambda value: value.__setitem__("working_order_count", 1), True),
            ("PROBE_RESULT_FOREIGN_ACTIVITY", lambda value: value.__setitem__("foreign_activity", True), True),
            ("PROBE_RESULT_PROTECTIVE_STATE_INVALID", lambda value: value.__setitem__("protective_stop_state", "WORKING"), True),
            ("PROBE_RESULT_SNAPSHOT_INCOMPLETE", lambda value: value.__setitem__("order_snapshot_complete", False), True),
            ("PROBE_RESULT_SNAPSHOT_UNSTABLE", lambda value: value.__setitem__("snapshot_stable", False), True),
            ("UNEXPECTED_FRAME", lambda value: value.__setitem__("message_type", "ORDER_EVENT"), True),
        )
        for code, change, resign in cases:
            with self.subTest(code=code, change=change):
                self._assert_rejected(code, stage="reconciliation", change=change, resign=resign)

    def test_cross_session_identity_nonce_and_receipt_replays_fail_closed(self) -> None:
        def run(second_hello_change=None, second_reconciliation_change=None) -> str:
            port = _free_port()
            probe = ProbeThread(port).start()
            self._complete_session(port, 1)
            second = _hello(2)
            if second_hello_change is not None:
                second_hello_change(second)
                _sign(second)
                with _connect(port) as connection:
                    _send(connection, second)
            else:
                self._complete_session(
                    port,
                    2,
                    hello=second,
                    reconciliation_change=second_reconciliation_change,
                )
            probe.finish()
            assert isinstance(probe.error, NinjaTraderReconciliationProbeError)
            return probe.error.code

        self.assertEqual(
            run(second_hello_change=lambda value: value.__setitem__("bridge_instance_id", "other")),
            "HELLO_SESSION_IDENTITY_CHANGED",
        )
        self.assertEqual(
            run(second_hello_change=lambda value: value.__setitem__("nonce", "hello-nonce-1")),
            "HELLO_NONCE_REPLAY",
        )
        self.assertEqual(
            run(second_hello_change=lambda value: value.__setitem__(
                "addon_build_fingerprint", "f" * 64,
            )),
            "HELLO_BUILD_PROVENANCE_CHANGED",
        )
        self.assertEqual(
            run(second_reconciliation_change=lambda value: (
                value.__setitem__("receipt_id", "probe-result-receipt-1"),
                _sign(value),
            )),
            "PROBE_RESULT_RECEIPT_REPLAY",
        )

    def test_missing_second_session_times_out_and_listener_is_released(self) -> None:
        port = _free_port()
        probe = ProbeThread(port, timeout_seconds=0.75).start()
        self._complete_session(port, 1)
        probe.finish()
        self.assertIsInstance(probe.error, NinjaTraderReconciliationProbeError)
        assert isinstance(probe.error, NinjaTraderReconciliationProbeError)
        self.assertEqual(probe.error.code, "PROBE_TIMEOUT")
        replacement = socket.socket()
        try:
            replacement.bind(("127.0.0.1", port))
        finally:
            replacement.close()

    def test_occupied_port_and_invalid_secret_fail_without_fallback(self) -> None:
        port = _free_port()
        owner = socket.socket()
        owner.bind(("127.0.0.1", port))
        owner.listen(1)
        try:
            with self.assertRaises(NinjaTraderReconciliationProbeError) as occupied:
                probe_flat_sim101_reconciliation(
                    paper_policy_hash=POLICY_HASH,
                    risk_profile_hash=RISK_HASH,
                    timeout_seconds=0.1,
                    _test_endpoint=("127.0.0.1", port),
                    _test_secret=KEY,
                )
            self.assertEqual(occupied.exception.code, "PROBE_LISTENER_FAILED")
        finally:
            owner.close()
        with self.assertRaises(NinjaTraderReconciliationProbeError) as secret:
            probe_flat_sim101_reconciliation(
                paper_policy_hash=POLICY_HASH,
                risk_profile_hash=RISK_HASH,
                timeout_seconds=0.1,
                _test_endpoint=("127.0.0.1", port),
                _test_secret=b"short",
            )
        self.assertEqual(secret.exception.code, "PROBE_SECRET_INVALID")

    def test_proof_validator_rejects_hash_and_authority_tampering(self) -> None:
        port = _free_port()
        probe = ProbeThread(port).start()
        self._complete_session(port, 1)
        self._complete_session(port, 2)
        probe.finish()
        assert probe.proof is not None
        tampered = dict(probe.proof)
        tampered["commands_sent"] = 1
        with self.assertRaises(NinjaTraderReconciliationProbeError) as error:
            validate_flat_sim101_reconciliation_proof(
                tampered,
                expected_policy_hash=POLICY_HASH,
                expected_risk_hash=RISK_HASH,
                expected_at=str(probe.proof["completed_at"]),
                _test_endpoint=("127.0.0.1", port),
                _test_secret=KEY,
            )
        self.assertEqual(error.exception.code, "PROOF_HASH_MISMATCH")

    def test_source_has_no_command_send_or_paper_ledger_construction(self) -> None:
        source = (
            Path(__file__).parents[1]
            / "src"
            / "l3g_paper"
            / "ninjatrader_reconciliation_probe.py"
        ).read_text(encoding="utf-8")
        self.assertNotIn('"message_type": "COMMAND"', source)
        self.assertNotIn("PaperLedger(", source)
        self.assertNotIn("sqlite3", source)


if __name__ == "__main__":
    unittest.main()
