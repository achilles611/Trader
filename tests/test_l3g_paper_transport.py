from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
import json
from pathlib import Path
import socket
import threading
import time
from typing import Callable, Mapping
import uuid
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from src.l3g_paper.ledger import PaperLedger
from src.l3g_paper.ninjatrader_transport import (
    ADDON_PROTOCOL_VERSION, EXECUTION_SCHEMA, LocalPaperSecretProvider, PaperExecutionTransport, expected_addon_source_fingerprint,
    sign_payload, verify_signature,
)
from src.l3g_paper.contracts import (
    ACCOUNT_BINDING, POLICY, RISK_PROFILE, ExecutionAction, PaperDirection,
    PaperExecutionCommand, PaperRiskGrant,
)
from src.l3g_paper.runtime import LaneIIIPaperRuntime


def free_port() -> int:
    candidate = socket.socket(); candidate.bind(("127.0.0.1", 0)); port = candidate.getsockname()[1]; candidate.close(); return port


class PaperTransportTests(unittest.TestCase):
    @staticmethod
    def _authenticated_ingress_transport(
        directory: str, key: bytes, callback: Callable[[Mapping[str, object]], None],
    ) -> tuple[PaperLedger, PaperExecutionTransport, str]:
        ledger = PaperLedger(Path(directory) / ("paper-" + uuid.uuid4().hex + ".sqlite3"))
        transport = PaperExecutionTransport(ledger, port=free_port(), on_message=callback)
        session_id = "l3g-es-transport-safety-fallback"
        with transport._lock:
            transport._key = key
            transport._client = object()  # type: ignore[assignment]
            transport._state = "AUTHENTICATED"
            transport._authenticated = True
            transport._reconciled = True
            transport._execution_session_id = session_id
        return ledger, transport, session_id

    @staticmethod
    def _signed_inbound_receipt(
        key: bytes, session_id: str, message_type: str, receipt_id: str, **fields: object,
    ) -> bytes:
        payload: dict[str, object] = {
            "schema": EXECUTION_SCHEMA,
            "message_type": message_type,
            "execution_session_id": session_id,
            "receipt_id": receipt_id,
            "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            **fields,
        }
        payload["signature"] = sign_payload(key, payload)
        return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")

    def test_hmac_is_canonical_and_detects_tampering(self) -> None:
        key = bytes(range(32)); payload = {"b": 2, "a": 1}
        signature = sign_payload(key, payload)
        self.assertTrue(verify_signature(key, {**payload, "signature": signature}))
        self.assertFalse(verify_signature(key, {**payload, "a": 3, "signature": signature}))

    def test_reconciliation_receipt_and_runtime_projection_have_distinct_identities(self) -> None:
        """A valid signed flat snapshot must not become a callback incident."""
        with TemporaryDirectory() as directory:
            key = bytes(range(32))
            ledger = PaperLedger(Path(directory) / "paper.sqlite3")
            runtime = LaneIIIPaperRuntime(ledger)
            transport = PaperExecutionTransport(
                ledger, port=free_port(), on_message=runtime.on_execution_message,
            )
            runtime.bind_transport(transport)
            runtime.start()
            session_id = "l3g-es-reconciliation-identity"
            receipt_id = "l3g-reconciliation-identity-0001"
            with transport._lock:
                transport._key = key
                transport._client = object()  # type: ignore[assignment]
                transport._state = "AUTHENTICATED"
                transport._authenticated = True
                transport._reconciled = True
                transport._execution_session_id = session_id
            runtime.on_execution_bridge_state("AUTHENTICATED")
            frame = self._signed_inbound_receipt(
                key,
                session_id,
                "RECONCILIATION",
                receipt_id,
                account_name="Sim101",
                account_class="LOCAL_SIMULATION",
                instrument="MNQ SEP26",
                position_quantity=0,
                working_order_count=0,
                working_entry_count=0,
                position_snapshot_complete=True,
                order_snapshot_complete=True,
            )
            try:
                transport._receive_frame(frame)
                self.assertEqual(runtime.status()["state"], "READY_DISARMED")
                self.assertTrue(ledger.contains(receipt_id))
                self.assertTrue(ledger.contains("l3g-position-snapshot-reconciliation-" + receipt_id))
                with ledger._lock:
                    incident_count = ledger._connection.execute(
                        "SELECT COUNT(*) FROM lane_iii_paper_audit WHERE kind = ?",
                        ("INCIDENT_CALLBACK_FAILURE",),
                    ).fetchone()[0]
                self.assertEqual(incident_count, 0)
            finally:
                runtime.stop()
                ledger.close()

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
                "working_entry_count": 0,
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

    def test_verified_safety_receipts_reach_runtime_once_when_durable_audit_fails(self) -> None:
        key = bytes(range(32))
        message_fields = {
            "SAFETY_EVENT": {"reason_code": "HEARTBEAT_TIMEOUT"},
            "POSITION_EVENT": {"quantity": 0},
            "RECONCILIATION": {
                "account_name": "Sim101", "account_class": "LOCAL_SIMULATION", "instrument": "MNQ SEP26",
                "position_quantity": 0, "working_order_count": 0, "working_entry_count": 0,
                "position_snapshot_complete": True, "order_snapshot_complete": True,
            },
        }
        with TemporaryDirectory() as directory:
            for message_type, fields in message_fields.items():
                with self.subTest(message_type=message_type):
                    delivered: list[dict[str, object]] = []
                    ledger, transport, session_id = self._authenticated_ingress_transport(
                        directory, key, lambda payload: delivered.append(dict(payload)),
                    )
                    frame = self._signed_inbound_receipt(
                        key, session_id, message_type, "l3g-safety-fallback-" + message_type.lower(), **fields,
                    )
                    try:
                        with patch.object(ledger, "append", side_effect=RuntimeError("ledger unavailable")) as append:
                            transport._receive_frame(frame)
                            transport._receive_frame(frame)
                        self.assertEqual(append.call_count, 1, "fallback must not retry the failed durable receipt")
                        self.assertEqual(len(delivered), 1, "receipt-id dedupe must remain in force")
                        self.assertEqual(delivered[0]["message_type"], message_type)
                        self.assertTrue(delivered[0].get("_l3g_durable_receipt_unavailable"))
                        status = transport.status()
                        self.assertTrue(status.authenticated_client)
                        self.assertEqual(status.client_count, 1)
                        self.assertEqual(status.duplicate_receipts, 1)
                    finally:
                        ledger.close()

    def test_safety_audit_failure_does_not_disconnect_a_real_signed_client(self) -> None:
        with TemporaryDirectory() as directory:
            key = bytes(range(32)); key_path = Path(directory) / "key"; key_path.write_bytes(key)
            delivered: list[dict[str, object]] = []
            ledger = PaperLedger(Path(directory) / "paper.sqlite3"); port = free_port()
            transport = PaperExecutionTransport(
                ledger, secret_provider=LocalPaperSecretProvider(key_path), port=port,
                on_message=lambda payload: delivered.append(dict(payload)),
            )
            client: socket.socket | None = None
            original_append = ledger.append
            try:
                self.assertEqual(transport.start().state, "LISTENING")
                client = socket.create_connection(("127.0.0.1", port), timeout=2)
                hello = {
                    "schema": EXECUTION_SCHEMA, "message_type": "HELLO", "bridge_instance_id": "bridge",
                    "ninjatrader_session_id": "nt", "addon_protocol_version": ADDON_PROTOCOL_VERSION,
                    "addon_source_fingerprint": expected_addon_source_fingerprint(), "addon_build_fingerprint": "0" * 64,
                    "addon_build_timestamp": "2026-08-25T00:00:00Z",
                    "account_name": "Sim101", "account_class": "LOCAL_SIMULATION", "instrument": "MNQ SEP26",
                    "capability": "PAPER_ONLY", "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                    "nonce": "safety-fallback",
                }
                hello["signature"] = sign_payload(key, hello)
                client.sendall(json.dumps(hello, sort_keys=True, separators=(",", ":")).encode("utf-8") + b"\n")
                grant = json.loads(client.makefile("rb").readline())

                def append(kind: str, payload: object, **kwargs: object) -> object:
                    if kind == "INCIDENT_SAFETY_EVENT":
                        raise RuntimeError("ledger unavailable")
                    return original_append(kind, payload, **kwargs)  # type: ignore[arg-type]

                ledger.append = append  # type: ignore[method-assign]
                safety = {
                    "schema": EXECUTION_SCHEMA, "message_type": "SAFETY_EVENT",
                    "execution_session_id": grant["execution_session_id"], "receipt_id": "l3g-live-safety-fallback",
                    "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                    "reason_code": "HEARTBEAT_TIMEOUT",
                }
                safety["signature"] = sign_payload(key, safety)
                client.sendall(json.dumps(safety, sort_keys=True, separators=(",", ":")).encode("utf-8") + b"\n")
                deadline = time.monotonic() + 2.0
                while not delivered and time.monotonic() < deadline:
                    time.sleep(0.01)
                self.assertEqual(len(delivered), 1)
                self.assertTrue(delivered[0].get("_l3g_durable_receipt_unavailable"))
                self.assertTrue(transport.status().authenticated_client)
                self.assertEqual(transport.status().client_count, 1)
            finally:
                ledger.append = original_append  # type: ignore[method-assign]
                if client is not None:
                    client.close()
                transport.stop()
                ledger.close()

    def test_fallback_safety_callback_failure_is_swallowed_without_another_ledger_append(self) -> None:
        key = bytes(range(32))
        delivered: list[dict[str, object]] = []

        def fail_after_receiving(payload: object) -> None:
            delivered.append(dict(payload))  # type: ignore[arg-type]
            raise RuntimeError("runtime unavailable")

        with TemporaryDirectory() as directory:
            ledger, transport, session_id = self._authenticated_ingress_transport(directory, key, fail_after_receiving)
            frame = self._signed_inbound_receipt(
                key, session_id, "SAFETY_EVENT", "l3g-safety-callback-failure", reason_code="HEARTBEAT_TIMEOUT",
            )
            try:
                with patch.object(ledger, "append", side_effect=RuntimeError("ledger unavailable")) as append:
                    transport._receive_frame(frame)
                self.assertEqual(append.call_count, 1)
                self.assertEqual(len(delivered), 1)
                self.assertTrue(delivered[0].get("_l3g_durable_receipt_unavailable"))
                self.assertTrue(transport.status().authenticated_client)
            finally:
                ledger.close()

    def test_verified_safety_receipt_falls_back_when_authority_fence_is_unavailable(self) -> None:
        key = bytes(range(32))
        delivered: list[dict[str, object]] = []
        with TemporaryDirectory() as directory:
            ledger, transport, session_id = self._authenticated_ingress_transport(
                directory, key, lambda payload: delivered.append(dict(payload)),
            )
            frame = self._signed_inbound_receipt(
                key, session_id, "RECONCILIATION", "l3g-safety-fence-failure",
                account_name="Sim101", account_class="LOCAL_SIMULATION", instrument="MNQ SEP26",
                position_quantity=0, working_order_count=0, working_entry_count=0,
                position_snapshot_complete=True, order_snapshot_complete=True,
            )
            try:
                with (
                    patch.object(ledger, "commissioning_authority_fence", side_effect=RuntimeError("fence unavailable")),
                    patch.object(ledger, "append", wraps=ledger.append) as append,
                ):
                    transport._receive_frame(frame)
                    transport._receive_frame(frame)
                self.assertEqual(append.call_count, 0)
                self.assertEqual(len(delivered), 1)
                self.assertTrue(delivered[0].get("_l3g_durable_receipt_unavailable"))
                self.assertTrue(transport.status().authenticated_client)
                self.assertEqual(transport.status().duplicate_receipts, 1)
            finally:
                ledger.close()

    def test_non_safety_receipt_remains_strict_when_durable_audit_fails(self) -> None:
        key = bytes(range(32))
        delivered: list[dict[str, object]] = []
        with TemporaryDirectory() as directory:
            ledger, transport, session_id = self._authenticated_ingress_transport(
                directory, key, lambda payload: delivered.append(dict(payload)),
            )
            frame = self._signed_inbound_receipt(key, session_id, "ORDER_EVENT", "l3g-order-no-fallback", order_id="order-1")
            try:
                with patch.object(ledger, "append", side_effect=RuntimeError("ledger unavailable")) as append:
                    with self.assertRaisesRegex(RuntimeError, "ledger unavailable"):
                        transport._receive_frame(frame)
                self.assertEqual(append.call_count, 1)
                self.assertEqual(delivered, [])
            finally:
                ledger.close()

    def test_non_safety_receipt_never_bypasses_an_unavailable_authority_fence(self) -> None:
        key = bytes(range(32))
        delivered: list[dict[str, object]] = []
        with TemporaryDirectory() as directory:
            ledger, transport, session_id = self._authenticated_ingress_transport(
                directory, key, lambda payload: delivered.append(dict(payload)),
            )
            frame = self._signed_inbound_receipt(key, session_id, "ORDER_EVENT", "l3g-order-fence-strict", order_id="order-1")
            try:
                with patch.object(ledger, "commissioning_authority_fence", side_effect=RuntimeError("fence unavailable")):
                    with self.assertRaisesRegex(RuntimeError, "fence unavailable"):
                        transport._receive_frame(frame)
                self.assertEqual(delivered, [])
            finally:
                ledger.close()

    def test_normal_callback_cannot_receive_the_internal_fallback_marker(self) -> None:
        key = bytes(range(32))
        delivered: list[dict[str, object]] = []
        with TemporaryDirectory() as directory:
            ledger, transport, session_id = self._authenticated_ingress_transport(
                directory, key, lambda payload: delivered.append(dict(payload)),
            )
            frame = self._signed_inbound_receipt(
                key, session_id, "SAFETY_EVENT", "l3g-normal-safety-marker",
                reason_code="HEARTBEAT_TIMEOUT", _l3g_durable_receipt_unavailable=True,
            )
            try:
                transport._receive_frame(frame)
                self.assertEqual(len(delivered), 1)
                self.assertNotIn("_l3g_durable_receipt_unavailable", delivered[0])
            finally:
                ledger.close()

    def test_reconciliation_rejects_negative_or_inconsistent_order_counts(self) -> None:
        key = bytes(range(32))
        invalid_counts = (
            {"working_order_count": -1, "working_entry_count": 0},
            {"working_order_count": 0, "working_entry_count": -1},
            {"working_order_count": 1, "working_entry_count": 2},
        )
        with TemporaryDirectory() as directory:
            delivered: list[dict[str, object]] = []
            ledger, transport, session_id = self._authenticated_ingress_transport(
                directory, key, lambda payload: delivered.append(dict(payload)),
            )
            try:
                for index, counts in enumerate(invalid_counts):
                    with self.subTest(counts=counts):
                        frame = self._signed_inbound_receipt(
                            key, session_id, "RECONCILIATION", "l3g-invalid-count-" + str(index),
                            account_name="Sim101", account_class="LOCAL_SIMULATION", instrument="MNQ SEP26",
                            position_quantity=0, position_snapshot_complete=True, order_snapshot_complete=True,
                            **counts,
                        )
                        rejected_before = transport.status().rejected_frames
                        transport._receive_frame(frame)
                        self.assertEqual(delivered, [])
                        self.assertFalse(transport.status().reconciled)
                        self.assertEqual(transport.status().rejected_frames, rejected_before + 1)
            finally:
                ledger.close()


if __name__ == "__main__":
    unittest.main()
