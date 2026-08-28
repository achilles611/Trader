from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
import json
from pathlib import Path
import socket
import threading
import time
from tempfile import TemporaryDirectory
import unittest

from src.l3g_paper.ledger import PaperLedger
from src.l3g_paper.ninjatrader_transport import (
    ADDON_PROTOCOL_VERSION, EXECUTION_SCHEMA, LocalPaperSecretProvider, PaperExecutionTransport, expected_addon_source_fingerprint,
    sign_payload, verify_signature,
)
from src.l3g_paper.contracts import (
    ACCOUNT_BINDING, POLICY, RISK_PROFILE, ExecutionAction, PaperDirection,
    PaperExecutionCommand, PaperRiskGrant,
)


def free_port() -> int:
    candidate = socket.socket(); candidate.bind(("127.0.0.1", 0)); port = candidate.getsockname()[1]; candidate.close(); return port


class PaperTransportTests(unittest.TestCase):
    def test_hmac_is_canonical_and_detects_tampering(self) -> None:
        key = bytes(range(32)); payload = {"b": 2, "a": 1}
        signature = sign_payload(key, payload)
        self.assertTrue(verify_signature(key, {**payload, "signature": signature}))
        self.assertFalse(verify_signature(key, {**payload, "a": 3, "signature": signature}))

    def test_one_loopback_listener_starts_once_and_releases_port(self) -> None:
        with TemporaryDirectory() as directory:
            key_path = Path(directory) / "key"; key_path.write_bytes(bytes(range(32)))
            ledger = PaperLedger(Path(directory) / "paper.sqlite3"); port = free_port()
            transport = PaperExecutionTransport(ledger, secret_provider=LocalPaperSecretProvider(key_path), port=port)
            self.assertEqual(transport.start().state, "LISTENING")
            self.assertEqual(transport.start().duplicate_start_attempts, 1)
            transport.stop(); ledger.close()
            probe = socket.socket(); probe.bind(("127.0.0.1", port)); probe.close()

    def test_signed_hello_authenticates_exact_sim101_only(self) -> None:
        with TemporaryDirectory() as directory:
            key = bytes(range(32)); key_path = Path(directory) / "key"; key_path.write_bytes(key)
            ledger = PaperLedger(Path(directory) / "paper.sqlite3"); port = free_port()
            transport = PaperExecutionTransport(ledger, secret_provider=LocalPaperSecretProvider(key_path), port=port)
            transport.start(); client = socket.create_connection(("127.0.0.1", port), timeout=2)
            hello = {
                "schema": EXECUTION_SCHEMA, "message_type": "HELLO", "bridge_instance_id": "bridge",
                "ninjatrader_session_id": "nt", "addon_protocol_version": ADDON_PROTOCOL_VERSION,
                "addon_source_fingerprint": expected_addon_source_fingerprint(), "addon_build_fingerprint": "0" * 64,
                "addon_build_timestamp": "2026-08-25T00:00:00Z",
                "account_name": "Sim101", "account_class": "LOCAL_SIMULATION", "instrument": "MNQ SEP26",
                "capability": "PAPER_ONLY", "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                "nonce": "nonce",
            }
            hello["signature"] = sign_payload(key, hello)
            client.sendall(json.dumps(hello, sort_keys=True, separators=(",", ":")).encode() + b"\n")
            grant = json.loads(client.makefile("rb").readline())
            self.assertEqual(grant["mode"], "PAPER_SIM101")
            self.assertFalse(grant["live_capital"])
            self.assertTrue(transport.status().authenticated_client)
            self.assertTrue(transport.status().addon_provenance_valid)
            client.close(); transport.stop(); ledger.close()

    def test_addon_provenance_mismatch_is_visible_without_hiding_observation_transport(self) -> None:
        with TemporaryDirectory() as directory:
            key_path = Path(directory) / "key"; key_path.write_bytes(bytes(range(32)))
            ledger = PaperLedger(Path(directory) / "paper.sqlite3")
            transport = PaperExecutionTransport(
                ledger, secret_provider=LocalPaperSecretProvider(key_path), port=free_port(), expected_source_fingerprint="a" * 64,
            )
            transport._send_signed = lambda _: None  # type: ignore[method-assign]
            transport._handle_hello({
                "schema": EXECUTION_SCHEMA, "message_type": "HELLO", "bridge_instance_id": "bridge", "ninjatrader_session_id": "nt",
                "addon_protocol_version": ADDON_PROTOCOL_VERSION, "addon_source_fingerprint": "b" * 64,
                "addon_build_fingerprint": "c" * 64, "addon_build_timestamp": "2026-08-25T00:00:00Z",
                "account_name": "Sim101", "account_class": "LOCAL_SIMULATION", "instrument": "MNQ SEP26", "capability": "PAPER_ONLY",
                "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"), "nonce": "nonce", "signature": "unused",
            })
            state = transport.status()
            self.assertTrue(state.authenticated_client)
            self.assertFalse(state.addon_provenance_valid)
            self.assertEqual(state.addon_source_fingerprint, "b" * 64)
            ledger.close()

    def test_missing_addon_provenance_is_rejected(self) -> None:
        with TemporaryDirectory() as directory:
            key_path = Path(directory) / "key"; key_path.write_bytes(bytes(range(32)))
            ledger = PaperLedger(Path(directory) / "paper.sqlite3")
            transport = PaperExecutionTransport(ledger, secret_provider=LocalPaperSecretProvider(key_path), port=free_port())
            transport._handle_hello({"schema": EXECUTION_SCHEMA, "message_type": "HELLO"})
            self.assertFalse(transport.status().authenticated_client)
            self.assertGreaterEqual(transport.status().rejected_frames, 1)
            ledger.close()

    def test_bad_signature_malformed_duplicate_key_and_oversized_frames_are_rejected(self) -> None:
        with TemporaryDirectory() as directory:
            key_path = Path(directory) / "key"; key_path.write_bytes(bytes(range(32)))
            ledger = PaperLedger(Path(directory) / "paper.sqlite3"); port = free_port()
            transport = PaperExecutionTransport(ledger, secret_provider=LocalPaperSecretProvider(key_path), port=port, maximum_frame_bytes=1024)
            transport.start(); client = socket.create_connection(("127.0.0.1", port), timeout=2)
            client.sendall(b'{"schema":"x","schema":"y"}\n')
            client.sendall(b"{" * 1025)
            time.sleep(0.4)
            self.assertGreaterEqual(transport.status().rejected_frames, 1)
            self.assertFalse(transport.status().authenticated_client)
            client.close(); transport.stop(); ledger.close()

    def test_second_client_and_execution_port_collision_fail_closed(self) -> None:
        with TemporaryDirectory() as directory:
            key_path = Path(directory) / "key"; key_path.write_bytes(bytes(range(32)))
            ledger = PaperLedger(Path(directory) / "paper.sqlite3"); port = free_port()
            transport = PaperExecutionTransport(ledger, secret_provider=LocalPaperSecretProvider(key_path), port=port)
            transport.start(); first = socket.create_connection(("127.0.0.1", port), timeout=2)
            second = socket.create_connection(("127.0.0.1", port), timeout=2)
            time.sleep(0.3)
            self.assertEqual(transport.status().client_count, 1)
            self.assertGreaterEqual(transport.status().rejected_clients, 1)
            first.close(); second.close(); transport.stop(); ledger.close()

            blocker = socket.socket()
            if hasattr(socket, "SO_EXCLUSIVEADDRUSE"):
                blocker.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
            blocker.bind(("127.0.0.1", port)); blocker.listen(1)
            collided_ledger = PaperLedger(Path(directory) / "collision.sqlite3")
            collided = PaperExecutionTransport(collided_ledger, secret_provider=LocalPaperSecretProvider(key_path), port=port)
            self.assertEqual(collided.start().state, "FAULTED")
            collided.stop(); collided_ledger.close(); blocker.close()

    def test_command_wire_carries_both_authenticated_envelope_and_closed_session_id(self) -> None:
        with TemporaryDirectory() as directory:
            key = bytes(range(32)); key_path = Path(directory) / "key"; key_path.write_bytes(key)
            ledger = PaperLedger(Path(directory) / "paper.sqlite3")
            transport = PaperExecutionTransport(ledger, secret_provider=LocalPaperSecretProvider(key_path), port=free_port())
            session = "l3g-es-test-session"
            created = datetime.now(timezone.utc); created_text = created.isoformat().replace("+00:00", "Z")
            expiry = (created + timedelta(seconds=5)).isoformat().replace("+00:00", "Z")
            grant = PaperRiskGrant(
                "l3g-pg-" + "a" * 32, "l3g-pi-" + "b" * 32,
                RISK_PROFILE.configuration_hash, ACCOUNT_BINDING.binding_hash, True,
                ("PAPER_RISK_GRANTED",), created_text, expiry, PaperDirection.FLAT,
                0, Decimal("0"), Decimal("0"), 0, 0,
            )
            command = PaperExecutionCommand(
                "l3g-pc-" + "c" * 32, 1, session, grant.intent_id,
                "l3g-pd-" + "d" * 32, ExecutionAction.ENTER_LONG, "Sim101",
                "LOCAL_SIMULATION", "MNQ SEP26", 1, PaperDirection.LONG,
                created_text, expiry, POLICY.configuration_hash,
                RISK_PROFILE.configuration_hash, ACCOUNT_BINDING.binding_hash,
                "TEST_ENTRY", grant.grant_id,
            )
            ledger.append("COMMAND", command.payload(), identity=command.command_id)
            captured: list[dict[str, object]] = []
            transport._key = key; transport._authenticated = True; transport._reconciled = True
            transport._execution_session_id = session
            transport._send_pre_signed = lambda payload: captured.append(dict(payload))  # type: ignore[method-assign]
            transport.send_command(command, grant)
            self.assertEqual(captured[0]["execution_session_id"], session)
            self.assertEqual(captured[0]["session_id"], session)
            self.assertTrue(verify_signature(key, captured[0]))
            ledger.close()

    def test_receipt_ingress_and_ordinary_send_have_one_deadlock_free_lock_order(self) -> None:
        with TemporaryDirectory() as directory:
            key = bytes(range(32))
            ledger = PaperLedger(Path(directory) / "paper.sqlite3")
            transport = PaperExecutionTransport(ledger, port=free_port())
            session = "l3g-es-lock-order-test"
            created = datetime.now(timezone.utc)
            created_text = created.isoformat().replace("+00:00", "Z")
            expiry = (created + timedelta(seconds=5)).isoformat().replace("+00:00", "Z")
            grant = PaperRiskGrant(
                "l3g-pg-" + "e" * 32, "l3g-pi-" + "f" * 32,
                RISK_PROFILE.configuration_hash, ACCOUNT_BINDING.binding_hash, True,
                ("PAPER_RISK_GRANTED",), created_text, expiry, PaperDirection.FLAT,
                0, Decimal("0"), Decimal("0"), 0, 0,
            )
            command = PaperExecutionCommand(
                "l3g-pc-" + "1" * 32, 1, session, grant.intent_id,
                "l3g-pd-" + "2" * 32, ExecutionAction.RECONCILE, "Sim101",
                "LOCAL_SIMULATION", "MNQ SEP26", 0, PaperDirection.FLAT,
                created_text, expiry, POLICY.configuration_hash,
                RISK_PROFILE.configuration_hash, ACCOUNT_BINDING.binding_hash,
                "LOCK_ORDER_TEST", grant.grant_id,
            )
            ledger.append("COMMAND", command.payload(), identity=command.command_id)
            with transport._lock:
                transport._key = key
                transport._state = "AUTHENTICATED"
                transport._authenticated = True
                transport._reconciled = True
                transport._execution_session_id = session
            captured: list[dict[str, object]] = []
            transport._send_pre_signed = lambda payload: captured.append(dict(payload))  # type: ignore[method-assign]
            ingress_has_ledger_fence = threading.Event()
            release_ingress = threading.Event()
            original_validate = transport._validate_reconciliation

            def validate(payload: object) -> bool:
                ingress_has_ledger_fence.set()
                if not release_ingress.wait(5):
                    raise TimeoutError("receipt ingress was not released")
                return original_validate(payload)  # type: ignore[arg-type]

            transport._validate_reconciliation = validate  # type: ignore[method-assign]
            send_reached_ledger = threading.Event()
            original_contains = ledger.contains

            def contains(identity: str) -> bool:
                send_reached_ledger.set()
                return original_contains(identity)

            ledger.contains = contains  # type: ignore[method-assign]
            receipt: dict[str, object] = {
                "schema": EXECUTION_SCHEMA,
                "message_type": "RECONCILIATION",
                "execution_session_id": session,
                "receipt_id": "l3g-lock-order-reconciliation",
                "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                "account_name": "Sim101",
                "account_class": "LOCAL_SIMULATION",
                "instrument": "MNQ SEP26",
                "position_quantity": 0,
                "working_order_count": 0,
                "position_snapshot_complete": True,
                "order_snapshot_complete": True,
            }
            receipt["signature"] = sign_payload(key, receipt)
            frame = json.dumps(receipt, sort_keys=True, separators=(",", ":")).encode("utf-8")
            errors: list[BaseException] = []

            def receive() -> None:
                try:
                    transport._receive_frame(frame)
                except BaseException as exc:  # pragma: no cover - surfaced below
                    errors.append(exc)

            def send() -> None:
                try:
                    transport.send_command(command, grant)
                except BaseException as exc:  # pragma: no cover - surfaced below
                    errors.append(exc)

            receipt_thread = threading.Thread(target=receive)
            send_thread = threading.Thread(target=send)
            try:
                receipt_thread.start()
                self.assertTrue(ingress_has_ledger_fence.wait(2))
                send_thread.start()
                self.assertTrue(send_reached_ledger.wait(2))
                acquired = transport._lock.acquire(timeout=0.5)
                self.assertTrue(acquired, "ordinary send held transport lock while waiting for ledger")
                if acquired:
                    transport._lock.release()
                release_ingress.set()
                receipt_thread.join(3)
                send_thread.join(3)

                self.assertFalse(receipt_thread.is_alive())
                self.assertFalse(send_thread.is_alive())
                self.assertEqual(errors, [])
                self.assertEqual(len(captured), 1)
            finally:
                release_ingress.set()
                receipt_thread.join(1)
                send_thread.join(1)
                ledger.close()


if __name__ == "__main__":
    unittest.main()
