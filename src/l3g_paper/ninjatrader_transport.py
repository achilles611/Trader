"""Signed loopback execution transport for the Sim101-only NinjaScript AddOn."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import hmac
import json
from pathlib import Path
import re
import select
import socket
import threading
import uuid
from typing import Callable, Mapping

from src.lane_iii.contracts import normalized_utc

from .contracts import (
    ACCOUNT_BINDING,
    AUTHORITY,
    CAPABILITY,
    POLICY,
    RISK_PROFILE,
    ExecutionAction,
    ExecutionCapabilityManifest,
    ExecutionVenueAdapter,
    PaperExecutionCommand,
    PaperRiskGrant,
    canonical_json,
)
from .ledger import PaperLedger
from .sessions import PaperSessionKind, PaperSessionResolver, UNSPECIFIED_OFF_SESSION_CONTEXT, context_from_identity


EXECUTION_HOST = "127.0.0.1"
EXECUTION_PORT = 48136
EXECUTION_SCHEMA = "lane-iii-phase-g-paper-execution-v1"
ADDON_PROTOCOL_VERSION = "l3g-paper-addon-provenance-v1"
EXPECTED_ADDON_SOURCE_FINGERPRINT = "b91b91b651d312768e2bbfbf4206de9d133303e31597ef364691e4f5c7728bf9"
MAXIMUM_FRAME_BYTES = 65536
HELLO_MAXIMUM_AGE_SECONDS = 10
FUTURE_TOLERANCE_SECONDS = 1
HEARTBEAT_INTERVAL_SECONDS = 1
HEARTBEAT_WATCHDOG_SECONDS = 5
COMMAND_ACKNOWLEDGEMENT_TIMEOUT_SECONDS = 3
# This marker is transport-local metadata, never a wire-protocol field.  It
# lets the runtime prioritize an authenticated safety callback when its normal
# durable receipt cannot be written during a ledger outage.
_DURABLE_RECEIPT_UNAVAILABLE_MARKER = "_l3g_durable_receipt_unavailable"
_FALLBACK_SAFETY_MESSAGE_TYPES = frozenset({"SAFETY_EVENT", "POSITION_EVENT", "RECONCILIATION"})


def expected_addon_source_fingerprint() -> str:
    """Fingerprint the checked-in AddOn while excluding its embedded value.

    The AddOn carries the resulting constant into the compiled DLL.  A stale
    DLL therefore reports the old source value even when its file timestamp is
    misleading, and arm preflight can fail closed without compiling anything.
    """
    source = Path(__file__).resolve().parents[2] / "ninjatrader" / "NinjaScript" / "AddOns" / "BeelzebubPaperExecutionAddOn.cs"
    try:
        text = source.read_text(encoding="utf-8").replace("\r\n", "\n")
    except OSError:
        # Packaged BeezConsole releases need the checked-in expectation even
        # though NinjaScript source is not bundled beside the executable.
        return EXPECTED_ADDON_SOURCE_FINGERPRINT
    normalized = re.sub(
        r'(private const string AddonSourceFingerprint = ")[0-9a-f]{64}(";)',
        r"\1<SOURCE_FINGERPRINT>\2", text,
    )
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _time(value: str) -> datetime:
    return datetime.fromisoformat(normalized_utc(value, "Execution protocol time").replace("Z", "+00:00"))


def sign_payload(key: bytes, payload: Mapping[str, object]) -> str:
    if not isinstance(key, bytes) or len(key) < 32:
        raise ValueError("The local paper HMAC key must contain at least 32 bytes.")
    unsigned = {str(name): value for name, value in payload.items() if name != "signature"}
    return hmac.new(key, canonical_json(unsigned), hashlib.sha256).hexdigest()


def verify_signature(key: bytes, payload: Mapping[str, object]) -> bool:
    supplied = payload.get("signature")
    return isinstance(supplied, str) and len(supplied) == 64 and hmac.compare_digest(supplied, sign_payload(key, payload))


class LocalPaperSecretProvider:
    """Reads the commissioning key without exposing it to status or audit."""

    def __init__(self, path: str | Path | None = None) -> None:
        self.path = Path(path) if path is not None else Path.home() / "Documents" / "NinjaTrader 8" / "l3g.paper.local.key"

    def load_key(self) -> bytes:
        try:
            key = self.path.read_bytes()
        except OSError as exc:
            raise RuntimeError("Lane III-G local signing key is missing or unreadable.") from exc
        if len(key) < 32:
            raise RuntimeError("Lane III-G local signing key is too short.")
        return key


@dataclass(frozen=True)
class ExecutionTransportStatus:
    state: str
    host: str
    port: int
    listener_ready: bool
    authenticated_client: bool
    client_count: int
    execution_session_id: str | None
    reconciled: bool
    error: str | None
    start_attempts: int
    duplicate_start_attempts: int
    rejected_clients: int
    received_frames: int
    rejected_frames: int
    commands_sent: int
    acknowledgements: int
    command_rejections: int
    duplicate_receipts: int
    addon_protocol_version: str | None
    addon_source_fingerprint: str | None
    addon_build_fingerprint: str | None
    addon_build_timestamp: str | None
    expected_addon_source_fingerprint: str
    addon_provenance_valid: bool

    def as_dict(self) -> dict[str, object]:
        return {
            **self.__dict__,
            "mode": "PAPER_SIM101",
            "account": "Sim101",
            "account_class": "LOCAL_SIMULATION",
            "instrument": "MNQ SEP26",
            "maximum_quantity": 1,
            "scientific_eligibility": False,
            "paper_only": True,
            "live_capital": "DENIED",
            "addon_provenance": {
                "protocol_version": self.addon_protocol_version,
                "source_fingerprint": self.addon_source_fingerprint,
                "build_fingerprint": self.addon_build_fingerprint,
                "build_timestamp": self.addon_build_timestamp,
                "expected_source_fingerprint": self.expected_addon_source_fingerprint,
                "status": "MATCH" if self.addon_provenance_valid else "COMPILE REQUIRED",
            },
        }


class PaperExecutionTransport:
    """One lifespan-owned listener and at most one authenticated client."""

    _START_TIMEOUT_SECONDS = 5.0
    _STOP_TIMEOUT_SECONDS = 5.0

    def __init__(
        self,
        ledger: PaperLedger,
        *,
        secret_provider: LocalPaperSecretProvider | None = None,
        host: str = EXECUTION_HOST,
        port: int = EXECUTION_PORT,
        maximum_frame_bytes: int = MAXIMUM_FRAME_BYTES,
        on_message: Callable[[Mapping[str, object]], None] | None = None,
        on_bridge_state: Callable[[str], None] | None = None,
        expected_source_fingerprint: str | None = None,
    ) -> None:
        if type(ledger) is not PaperLedger:
            raise ValueError("Execution transport requires the durable paper ledger.")
        if host != EXECUTION_HOST:
            raise ValueError("Execution transport is loopback-only.")
        if type(port) is not int or not 1024 <= port <= 65535:
            raise ValueError("Execution transport port is invalid.")
        if type(maximum_frame_bytes) is not int or not 1024 <= maximum_frame_bytes <= 1048576:
            raise ValueError("Maximum execution frame size is invalid.")
        self.ledger = ledger
        self.secret_provider = secret_provider or LocalPaperSecretProvider()
        self.host = host
        self.port = port
        self.maximum_frame_bytes = maximum_frame_bytes
        self._on_message = on_message
        self._on_bridge_state = on_bridge_state
        self._lock = threading.RLock()
        self._write_lock = threading.Lock()
        self._stop = threading.Event()
        self._ready = threading.Event()
        self._finished = threading.Event()
        self._thread: threading.Thread | None = None
        self._listener: socket.socket | None = None
        self._client: socket.socket | None = None
        self._key: bytes | None = None
        self._state = "NEW"
        self._error: str | None = None
        self._execution_session_id: str | None = None
        self._authenticated = False
        self._reconciled = False
        self._ninjatrader_session_id: str | None = None
        self._last_sent_sequence = 0
        self._seen_receipts: set[str] = set()
        self._pending_acknowledgements: dict[str, datetime] = {}
        self._start_attempts = 0
        self._duplicate_start_attempts = 0
        self._rejected_clients = 0
        self._received_frames = 0
        self._rejected_frames = 0
        self._commands_sent = 0
        self._acknowledgements = 0
        self._command_rejections = 0
        self._duplicate_receipts = 0
        self._expected_addon_source_fingerprint = expected_source_fingerprint or expected_addon_source_fingerprint()
        self._addon_protocol_version: str | None = None
        self._addon_source_fingerprint: str | None = None
        self._addon_build_fingerprint: str | None = None
        self._addon_build_timestamp: str | None = None

    def status(self) -> ExecutionTransportStatus:
        with self._lock:
            return ExecutionTransportStatus(
                self._state, self.host, self.port, self._state in {"LISTENING", "CONNECTED", "AUTHENTICATED"},
                self._authenticated, 1 if self._client is not None else 0, self._execution_session_id,
                self._reconciled, self._error, self._start_attempts, self._duplicate_start_attempts,
                self._rejected_clients, self._received_frames, self._rejected_frames,
                self._commands_sent, self._acknowledgements, self._command_rejections, self._duplicate_receipts,
                self._addon_protocol_version, self._addon_source_fingerprint, self._addon_build_fingerprint,
                self._addon_build_timestamp, self._expected_addon_source_fingerprint,
                self._addon_protocol_version == ADDON_PROTOCOL_VERSION
                and self._addon_source_fingerprint == self._expected_addon_source_fingerprint,
            )

    def start(self, *, timeout_seconds: float = _START_TIMEOUT_SECONDS) -> ExecutionTransportStatus:
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                self._duplicate_start_attempts += 1
                return self.status()
            self._start_attempts += 1
            self._state = "STARTING"
            self._error = None
            self._stop = threading.Event()
            self._ready = threading.Event()
            self._finished = threading.Event()
            try:
                self._key = self.secret_provider.load_key()
            except RuntimeError as exc:
                self._state = "DISABLED"
                self._error = str(exc)
                return self.status()
            self._thread = threading.Thread(target=self._run, name="L3GPaperExecutionTransport", daemon=True)
            self._thread.start()
        if not self._ready.wait(timeout_seconds):
            self.stop()
            with self._lock:
                self._state = "FAULTED"
                self._error = "Execution transport startup timed out."
        return self.status()

    def _notify_bridge(self, state: str) -> None:
        callback = self._on_bridge_state
        if callback is not None:
            try:
                callback(state)
            except Exception as exc:
                self.ledger.append("INCIDENT_CALLBACK_FAILURE", {"sink": "execution_bridge_state", "error_type": type(exc).__name__})

    def _run(self) -> None:
        listener: socket.socket | None = None
        client: socket.socket | None = None
        buffer = bytearray()
        try:
            listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            if hasattr(socket, "SO_EXCLUSIVEADDRUSE"):
                listener.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
            else:
                listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind((self.host, self.port))
            listener.listen(2)
            listener.setblocking(False)
            with self._lock:
                self._listener = listener
                self._state = "LISTENING"
            self._ready.set()
            self._notify_bridge("LISTENING")
            while not self._stop.is_set():
                self._check_acknowledgement_timeouts()
                readable = [listener]
                if client is not None:
                    readable.append(client)
                ready, _, _ = select.select(readable, [], [], 0.25)
                for source in ready:
                    if source is listener:
                        candidate, remote = listener.accept()
                        if remote[0] != EXECUTION_HOST or client is not None:
                            candidate.close()
                            with self._lock:
                                self._rejected_clients += 1
                            self.ledger.append("INCIDENT_EXECUTION_CLIENT_REJECTED", {"reason": "NON_LOOPBACK" if remote[0] != EXECUTION_HOST else "SECOND_CLIENT"})
                            continue
                        candidate.setblocking(False)
                        client = candidate
                        buffer = bytearray()
                        with self._lock:
                            self._client = client
                            self._state = "CONNECTED"
                            self._authenticated = False
                            self._reconciled = False
                            self._execution_session_id = None
                            self._addon_protocol_version = None
                            self._addon_source_fingerprint = None
                            self._addon_build_fingerprint = None
                            self._addon_build_timestamp = None
                        self._notify_bridge("CONNECTED")
                        continue
                    try:
                        chunk = source.recv(min(4096, self.maximum_frame_bytes + 1 - len(buffer)))
                    except (BlockingIOError, InterruptedError):
                        continue
                    except OSError:
                        chunk = b""
                    if not chunk:
                        self._close_client(client)
                        client = None
                        buffer = bytearray()
                        self._notify_bridge("DISCONNECTED")
                        continue
                    buffer.extend(chunk)
                    while b"\n" in buffer:
                        frame, _, remainder = buffer.partition(b"\n")
                        buffer = bytearray(remainder)
                        self._receive_frame(bytes(frame))
                    if len(buffer) > self.maximum_frame_bytes:
                        with self._lock:
                            self._rejected_frames += 1
                        self.ledger.append("INCIDENT_PROTOCOL_REJECTION", {"reason": "OVERSIZED_FRAME"})
                        self._close_client(client)
                        client = None
                        buffer = bytearray()
                        self._notify_bridge("DISCONNECTED")
        except OSError as exc:
            with self._lock:
                self._state = "FAULTED"
                self._error = f"Execution listener failed at {self.host}:{self.port}: {exc}"
            self.ledger.append("INCIDENT_EXECUTION_TRANSPORT_FAULT", {"error_type": type(exc).__name__, "port": self.port})
            self._ready.set()
        finally:
            self._close_client(client)
            if listener is not None:
                try:
                    listener.close()
                except OSError:
                    pass
            with self._lock:
                self._client = None
                self._listener = None
                self._authenticated = False
                self._reconciled = False
                if self._state != "FAULTED":
                    self._state = "STOPPED"
                self._key = None
            self._finished.set()

    def _close_client(self, client: socket.socket | None) -> None:
        if client is not None:
            try:
                client.close()
            except OSError:
                pass
        with self._lock:
            if self._client is client:
                self._client = None
            self._authenticated = False
            self._reconciled = False
            self._execution_session_id = None
            self._addon_protocol_version = None
            self._addon_source_fingerprint = None
            self._addon_build_fingerprint = None
            self._addon_build_timestamp = None
            self._pending_acknowledgements.clear()
            if self._state not in {"STOPPING", "STOPPED", "FAULTED"}:
                self._state = "LISTENING"

    def _decode(self, frame: bytes) -> Mapping[str, object] | None:
        if not frame or len(frame) > self.maximum_frame_bytes:
            return None
        try:
            text = frame.decode("utf-8")
            def unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
                result: dict[str, object] = {}
                for name, item in pairs:
                    if name in result:
                        raise ValueError("duplicate protocol key")
                    result[name] = item
                return result
            value = json.loads(text, object_pairs_hook=unique_object)
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
            return None
        return value if isinstance(value, dict) else None

    def _claim_receipt_id(self, receipt_id: str) -> bool:
        """Claim a receipt exactly once, including a ledger-fence outage."""
        if not receipt_id:
            return True
        with self._lock:
            if receipt_id in self._seen_receipts:
                self._duplicate_receipts += 1
                return False
            self._seen_receipts.add(receipt_id)
            return True

    def _deliver_inbound_callback(
        self,
        payload: Mapping[str, object],
        session_id: str | None,
        *,
        durable_receipt_unavailable: bool,
    ) -> None:
        callback = self._on_message
        if callback is None:
            return
        # Never allow a wire payload to impersonate the local fallback marker.
        callback_payload = dict(payload)
        callback_payload.pop(_DURABLE_RECEIPT_UNAVAILABLE_MARKER, None)
        if durable_receipt_unavailable:
            callback_payload[_DURABLE_RECEIPT_UNAVAILABLE_MARKER] = True
        try:
            callback(callback_payload)
        except Exception as exc:
            if not durable_receipt_unavailable:
                self.ledger.append("INCIDENT_CALLBACK_FAILURE", {"sink": "execution_message", "error_type": type(exc).__name__}, execution_session_id=session_id)

    def _receive_frame(self, frame: bytes) -> None:
        with self._lock:
            self._received_frames += 1
        payload = self._decode(frame)
        if payload is None or payload.get("schema") != EXECUTION_SCHEMA:
            self._reject_frame("MALFORMED_JSON_OR_SCHEMA")
            return
        key = self._key
        if key is None or not verify_signature(key, payload):
            self._reject_frame("INVALID_OR_MISSING_SIGNATURE")
            return
        message_type = payload.get("message_type")
        message_type_text = str(message_type)
        if message_type == "HELLO":
            self._handle_hello(payload)
            return
        with self._lock:
            authenticated = self._authenticated
            session_id = self._execution_session_id
        if not authenticated or payload.get("execution_session_id") != session_id:
            self._reject_frame("WRONG_EXECUTION_SESSION")
            return
        timestamp = payload.get("timestamp")
        try:
            moment = _time(str(timestamp))
        except (TypeError, ValueError):
            self._reject_frame("INVALID_TIMESTAMP")
            return
        now = datetime.now(timezone.utc)
        if moment > now + timedelta(seconds=FUTURE_TOLERANCE_SECONDS) or now - moment > timedelta(seconds=30):
            self._reject_frame("STALE_OR_FUTURE_TIMESTAMP")
            return
        receipt_id = str(payload.get("receipt_id", ""))
        # Keep the pure shape check available if the authority fence itself is
        # unavailable. The normal path repeats it under that fence before
        # mutating reconciliation state.
        fallback_reconciliation_valid = (
            message_type != "RECONCILIATION" or PaperExecutionTransport._validate_reconciliation(payload)
        )
        fallback_safety_message = message_type_text in _FALLBACK_SAFETY_MESSAGE_TYPES
        receipt_claimed = False
        durable_receipt_unavailable = False
        identity = receipt_id or "l3g-receipt-" + hashlib.sha256(canonical_json(payload)).hexdigest()
        kind = {
            "ORDER_EVENT": "ORDER_EVENT",
            "EXECUTION_EVENT": "EXECUTION",
            "POSITION_EVENT": "POSITION_SNAPSHOT_EVENT",
            "SAFETY_EVENT": "INCIDENT_SAFETY_EVENT",
        }.get(message_type_text, "COMMAND_RECEIPT_" + message_type_text)
        # Linearize accepted broker input against commissioning authority before
        # mutating receipt/reconciliation state. Runtime authority holds the
        # same ledger fence from its final proof through command admission.
        # Release it before the runtime callback to preserve runtime->ledger
        # lock order and avoid a ledger->runtime deadlock.
        try:
            with self.ledger.commissioning_authority_fence():
                if not self._claim_receipt_id(receipt_id):
                    return
                receipt_claimed = bool(receipt_id)
                if message_type == "RECONCILIATION":
                    reconciliation_valid = self._validate_reconciliation(payload)
                    with self._lock:
                        self._reconciled = reconciliation_valid
                    if not reconciliation_valid:
                        self._reject_frame("RECONCILIATION_MISMATCH")
                        return
                if message_type == "COMMAND_ACK":
                    with self._lock:
                        self._acknowledgements += 1
                        self._pending_acknowledgements.pop(str(payload.get("command_id", "")), None)
                elif message_type == "COMMAND_REJECTED":
                    with self._lock:
                        self._command_rejections += 1
                        self._pending_acknowledgements.pop(str(payload.get("command_id", "")), None)
                try:
                    self.ledger.append(kind, dict(payload), identity=identity, execution_session_id=session_id)
                except Exception:
                    # A verified flat/reconciliation or safety signal is the
                    # last fail-safe input when ledger persistence is
                    # unavailable.  It must reach the runtime without closing
                    # the signed client. Other receipt classes remain strict.
                    if not fallback_safety_message:
                        raise
                    durable_receipt_unavailable = True
        except Exception:
            # The authority fence is normally the linearization point. If the
            # ledger cannot even establish it, the verified safety channel is
            # still more important than tearing down its signed connection.
            # Invalid reconciliations and every non-safety receipt remain
            # strict and are never delivered through this path.
            if not fallback_safety_message or not fallback_reconciliation_valid:
                raise
            if receipt_id:
                if not receipt_claimed and not self._claim_receipt_id(receipt_id):
                    return
            durable_receipt_unavailable = True
        self._deliver_inbound_callback(
            payload, session_id, durable_receipt_unavailable=durable_receipt_unavailable,
        )

    def _handle_hello(self, payload: Mapping[str, object]) -> None:
        required = {
            "schema", "message_type", "bridge_instance_id", "ninjatrader_session_id", "addon_protocol_version",
            "addon_source_fingerprint", "addon_build_fingerprint", "addon_build_timestamp",
            "account_name", "account_class", "instrument", "capability", "timestamp", "nonce", "signature",
        }
        if set(payload) != required:
            self._reject_frame("HELLO_SHAPE")
            return
        try:
            timestamp = _time(str(payload["timestamp"]))
        except ValueError:
            self._reject_frame("HELLO_TIMESTAMP")
            return
        now = datetime.now(timezone.utc)
        if timestamp > now + timedelta(seconds=FUTURE_TOLERANCE_SECONDS) or now - timestamp > timedelta(seconds=HELLO_MAXIMUM_AGE_SECONDS):
            self._reject_frame("HELLO_STALE_OR_FUTURE")
            return
        if (
            payload.get("account_name"), payload.get("account_class"), payload.get("instrument"), payload.get("capability")
        ) != ("Sim101", "LOCAL_SIMULATION", "MNQ SEP26", "PAPER_ONLY"):
            self._reject_frame("HELLO_CAPABILITY_MISMATCH")
            return
        if not all(isinstance(payload.get(key), str) and str(payload[key]).strip() for key in (
            "bridge_instance_id", "ninjatrader_session_id", "addon_protocol_version", "addon_source_fingerprint",
            "addon_build_fingerprint", "addon_build_timestamp", "nonce",
        )):
            self._reject_frame("HELLO_IDENTITY_MISSING")
            return
        session_id = "l3g-es-" + uuid.uuid4().hex
        with self.ledger.commissioning_authority_fence():
            with self._lock:
                self._execution_session_id = session_id
                self._ninjatrader_session_id = str(payload["ninjatrader_session_id"])
                self._authenticated = True
                self._reconciled = False
                self._state = "AUTHENTICATED"
                self._last_sent_sequence = 0
                self._addon_protocol_version = str(payload["addon_protocol_version"])
                self._addon_source_fingerprint = str(payload["addon_source_fingerprint"])
                self._addon_build_fingerprint = str(payload["addon_build_fingerprint"])
                self._addon_build_timestamp = str(payload["addon_build_timestamp"])
            self.ledger.append(
                "SESSION_HANDSHAKE",
                {key: value for key, value in payload.items() if key != "signature"},
                identity=session_id,
                execution_session_id=session_id,
            )
        response: dict[str, object] = {
            "schema": EXECUTION_SCHEMA,
            "message_type": "SESSION_GRANT",
            "execution_session_id": session_id,
            "server_nonce": uuid.uuid4().hex,
            "paper_policy_hash": POLICY.configuration_hash,
            "risk_profile_hash": RISK_PROFILE.configuration_hash,
            "account_binding_hash": ACCOUNT_BINDING.binding_hash,
            "heartbeat_interval_seconds": HEARTBEAT_INTERVAL_SECONDS,
            "heartbeat_watchdog_seconds": HEARTBEAT_WATCHDOG_SECONDS,
            "command_ttl_seconds": POLICY.decision_ttl_seconds,
            "mode": "PAPER_SIM101",
            "live_capital": False,
            "timestamp": _now(),
        }
        self._send_signed(response)
        self._notify_bridge("AUTHENTICATED")

    @staticmethod
    def _validate_reconciliation(payload: Mapping[str, object]) -> bool:
        working_order_count = payload.get("working_order_count")
        working_entry_count = payload.get("working_entry_count")
        return (
            payload.get("account_name") == "Sim101"
            and payload.get("account_class") == "LOCAL_SIMULATION"
            and payload.get("instrument") == "MNQ SEP26"
            and type(payload.get("position_quantity")) is int
            and type(working_order_count) is int
            and type(working_entry_count) is int
            and working_order_count >= 0
            and working_entry_count >= 0
            and working_entry_count <= working_order_count
            and payload.get("position_snapshot_complete") is True
            and payload.get("order_snapshot_complete") is True
        )

    def _reject_frame(self, reason: str) -> None:
        with self._lock:
            self._rejected_frames += 1
            session_id = self._execution_session_id
        self.ledger.append("INCIDENT_PROTOCOL_REJECTION", {"reason": reason}, execution_session_id=session_id)

    def _check_acknowledgement_timeouts(self) -> None:
        now = datetime.now(timezone.utc)
        with self._lock:
            expired = [
                command_id for command_id, deadline in self._pending_acknowledgements.items()
                if now > deadline
            ]
            for command_id in expired:
                self._pending_acknowledgements.pop(command_id, None)
                self._command_rejections += 1
            session_id = self._execution_session_id
        for command_id in expired:
            message = {
                "schema": EXECUTION_SCHEMA,
                "message_type": "COMMAND_REJECTED",
                "execution_session_id": session_id,
                "timestamp": _now(),
                "command_id": command_id,
                "reason_code": "ACKNOWLEDGEMENT_TIMEOUT",
            }
            self.ledger.append(
                "INCIDENT_COMMAND_ACKNOWLEDGEMENT_TIMEOUT",
                message,
                identity="l3g-ack-timeout-" + command_id,
                execution_session_id=session_id,
            )
            callback = self._on_message
            if callback is not None:
                try:
                    callback(message)
                except Exception as exc:
                    self.ledger.append("INCIDENT_CALLBACK_FAILURE", {"sink": "execution_message", "error_type": type(exc).__name__}, execution_session_id=session_id)

    def _send_signed(self, payload: Mapping[str, object]) -> None:
        key = self._key
        if key is None:
            raise RuntimeError("Execution signing key is unavailable.")
        signed = dict(payload)
        signed["signature"] = sign_payload(key, signed)
        frame = canonical_json(signed) + b"\n"
        with self._write_lock:
            with self._lock:
                client = self._client
            if client is None:
                raise RuntimeError("The NinjaTrader paper execution bridge is disconnected.")
            try:
                client.sendall(frame)
            except OSError as exc:
                raise RuntimeError("The NinjaTrader paper execution bridge send failed.") from exc

    def send_command(self, command: PaperExecutionCommand, grant: PaperRiskGrant) -> PaperExecutionCommand:
        if type(command) is not PaperExecutionCommand or type(grant) is not PaperRiskGrant:
            raise ValueError("Execution transport requires exact command and grant contracts.")
        # Ledger records are append-only, so proving durability before taking
        # the transport lock cannot become false. Keeping the one ledger access
        # outside that lock standardizes ledger->transport ordering with broker
        # receipt ingress and prevents a send/receipt lock inversion.
        if not self.ledger.contains(command.command_id):
            raise RuntimeError("A command must be durably recorded before socket send.")
        with self._lock:
            session_id = self._execution_session_id
            if not self._authenticated or session_id is None:
                raise RuntimeError("The execution bridge is not authenticated.")
            if not self._reconciled and command.action not in {ExecutionAction.RECONCILE, ExecutionAction.HEARTBEAT}:
                raise RuntimeError("Reconciliation is required before order mutation.")
            if command.execution_session_id != session_id:
                raise ValueError("Command execution session mismatch.")
            if command.command_sequence != self._last_sent_sequence + 1:
                raise ValueError("Command sequence must be exactly monotonic.")
            if not grant.valid_at(_now()) or grant.grant_id != command.risk_grant_id or grant.intent_id != command.intent_id:
                raise ValueError("A positive, current, matching paper risk grant is required.")
            if command.policy_hash != POLICY.configuration_hash or command.risk_profile_hash != RISK_PROFILE.configuration_hash or command.account_binding_hash != ACCOUNT_BINDING.binding_hash:
                raise ValueError("Command authority hashes do not match the compiled paper authority.")
            legacy = command.session_kind is PaperSessionKind.OFF_SESSION and command.session_id != UNSPECIFIED_OFF_SESSION_CONTEXT.session_id
            if not legacy and (
                (grant.session_kind, grant.session_id, grant.trade_date, grant.session_profile_hash, grant.session_generation)
                != (command.session_kind, command.session_id, command.trade_date, command.session_profile_hash, command.session_generation)
            ):
                raise ValueError("Command and risk grant session identity mismatch.")
            if command.action in {ExecutionAction.ENTER_LONG, ExecutionAction.ENTER_SHORT}:
                if legacy:
                    # Retained only for pre-regime in-process protocol fixtures;
                    # the compiled AddOn independently rejects this shape.
                    pass
                else:
                    context = context_from_identity(
                        command.session_kind, command.session_id, command.trade_date,
                        command.session_profile_hash, command.session_generation,
                    )
                    current = PaperSessionResolver().resolve(_now(), generation=command.session_generation)
                    if (
                        not current.entry_authorized or context.session_kind is PaperSessionKind.OFF_SESSION
                        or current.context.session_kind is not context.session_kind
                        or current.context.session_id != context.session_id
                        or current.context.trade_date != context.trade_date
                        or current.context.session_profile_hash != context.session_profile_hash
                    ):
                        raise ValueError("Entry command is outside its exact paper session window.")
        signed = command.with_signature(sign_payload(self._key or b"", command.unsigned_payload()))
        wire = {
            "schema": EXECUTION_SCHEMA,
            "message_type": "COMMAND",
            "execution_session_id": session_id,
            **signed.payload(),
        }
        # Sign the full protocol frame, not only the dataclass body.
        wire["signature"] = sign_payload(self._key or b"", wire)
        with self._lock:
            self._pending_acknowledgements[command.command_id] = datetime.now(timezone.utc) + timedelta(seconds=COMMAND_ACKNOWLEDGEMENT_TIMEOUT_SECONDS)
        try:
            self._send_pre_signed(wire)
        except Exception:
            with self._lock:
                self._pending_acknowledgements.pop(command.command_id, None)
            raise
        with self._lock:
            self._last_sent_sequence = command.command_sequence
            self._commands_sent += 1
        return signed

    def _send_pre_signed(self, payload: Mapping[str, object]) -> None:
        frame = canonical_json(payload) + b"\n"
        with self._write_lock:
            with self._lock:
                client = self._client
            if client is None:
                raise RuntimeError("The NinjaTrader paper execution bridge is disconnected.")
            client.sendall(frame)

    def send_heartbeat(self, *, armed: bool) -> None:
        with self._lock:
            if not self._authenticated or self._execution_session_id is None:
                return
            session_id = self._execution_session_id
        self._send_signed({
            "schema": EXECUTION_SCHEMA,
            "message_type": "HEARTBEAT",
            "execution_session_id": session_id,
            "armed": bool(armed),
            "paper_policy_hash": POLICY.configuration_hash,
            "risk_profile_hash": RISK_PROFILE.configuration_hash,
            "account_binding_hash": ACCOUNT_BINDING.binding_hash,
            "timestamp": _now(),
        })

    def stop(self, *, timeout_seconds: float = _STOP_TIMEOUT_SECONDS) -> ExecutionTransportStatus:
        with self._lock:
            thread = self._thread
            if thread is None or not thread.is_alive():
                if self._state not in {"FAULTED", "DISABLED"}:
                    self._state = "STOPPED"
                return self.status()
            self._state = "STOPPING"
            self._stop.set()
            listener = self._listener
            client = self._client
        self._close_client(client)
        if listener is not None:
            try:
                listener.close()
            except OSError:
                pass
        self._finished.wait(timeout_seconds)
        thread.join(timeout=max(0.0, timeout_seconds))
        with self._lock:
            if thread.is_alive():
                self._state = "FAULTED"
                self._error = "Execution transport shutdown timed out."
            self._thread = None
        return self.status()


class NinjaTraderSim101PaperAdapter(ExecutionVenueAdapter):
    """The sole concrete execution venue registration in this patch."""

    def __init__(self, transport: PaperExecutionTransport) -> None:
        if type(transport) is not PaperExecutionTransport:
            raise ValueError("The Sim101 adapter requires the exact signed transport.")
        self.transport = transport

    @property
    def capability(self) -> ExecutionCapabilityManifest:
        return CAPABILITY

    def submit(self, command: PaperExecutionCommand, grant: PaperRiskGrant) -> None:
        self.transport.send_command(command, grant)


def registered_execution_adapters(transport: PaperExecutionTransport) -> tuple[NinjaTraderSim101PaperAdapter, ...]:
    """Closed registration; no parser, factory, or configuration switch exists."""
    return (NinjaTraderSim101PaperAdapter(transport),)
