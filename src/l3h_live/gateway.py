"""Authenticated, loopback-only protocol for the isolated L3H AddOn.

The paper transport is intentionally not imported here.  This boundary uses a
different port, schema, key, message namespace, and client-order prefix.  A
socket acknowledgement proves receipt only; broker reconciliation remains the
only authority for an order outcome.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import hmac
import json
import socket
import threading
from typing import Any, Mapping
from uuid import uuid4

from .contracts import canonical_hash, canonical_json, parse_utc, utc_now
from .live_authorization import LIVE_ACCOUNT_CLASS, verify_native_admission_envelope


WIRE_SCHEMA = "lane-iii-phase-h-live-execution-v1"
PROTOCOL_VERSION = "l3h-live-addon-protocol-v1"
LOOPBACK_PORT = 48137
MAXIMUM_FRAME_BYTES = 65_536
MAXIMUM_FRESHNESS_SECONDS = 10


class GatewayDispatchError(RuntimeError):
    """A dispatch boundary failure whose outcome must be reconciled."""


class GatewayProtocolError(GatewayDispatchError):
    """A signed-frame or protocol invariant failed before side effects."""


def _signature_material(frame: Mapping[str, object]) -> bytes:
    """A simple cross-language HMAC representation.

    The nested payload is canonical-hashed separately, so JSON property order
    is never accepted as an authority boundary.
    """

    fields = (
        str(frame.get("schema", "")), str(frame.get("protocol_version", "")),
        str(frame.get("message_type", "")), str(frame.get("request_id", "")),
        str(frame.get("nonce", "")), str(frame.get("timestamp", "")),
        str(frame.get("payload_hash", "")),
    )
    return "|".join(fields).encode("ascii")


def sign_frame(frame: Mapping[str, object], key: bytes) -> dict[str, object]:
    """Return a schema-exact HMAC frame without mutating caller data."""

    if not isinstance(key, bytes) or len(key) < 32:
        raise ValueError("L3H gateway key must contain at least 256 bits.")
    payload = frame.get("payload")
    if not isinstance(payload, Mapping):
        raise ValueError("L3H gateway payload must be an object.")
    result = dict(frame)
    result["schema"] = WIRE_SCHEMA
    result["protocol_version"] = PROTOCOL_VERSION
    result["payload"] = dict(payload)
    result["payload_hash"] = canonical_hash(result["payload"])
    result["signature"] = hmac.new(key, _signature_material(result), hashlib.sha256).hexdigest()
    return result


@dataclass(frozen=True)
class VerifiedFrame:
    message_type: str
    request_id: str
    nonce: str
    timestamp: str
    payload: Mapping[str, object]


class ReplayGuard:
    """Bounded in-memory nonce/request guard; restart never inherits trust."""

    def __init__(self, *, maximum_entries: int = 8_192) -> None:
        self.maximum_entries = maximum_entries
        self._values: dict[str, float] = {}
        self._lock = threading.Lock()

    def accept(self, nonce: str, request_id: str) -> None:
        marker = nonce + "\x00" + request_id
        with self._lock:
            if marker in self._values:
                raise GatewayProtocolError("DENY_REPLAY")
            if len(self._values) >= self.maximum_entries:
                oldest = min(self._values, key=self._values.__getitem__)
                del self._values[oldest]
            self._values[marker] = datetime.now(timezone.utc).timestamp()


def verify_frame(
    frame: Mapping[str, object], key: bytes, *, replay_guard: ReplayGuard | None = None,
    now: str | None = None, maximum_freshness_seconds: int = MAXIMUM_FRESHNESS_SECONDS,
) -> VerifiedFrame:
    """Fail closed before a frame can reach a native command queue."""

    required = {"schema", "protocol_version", "message_type", "request_id", "nonce", "timestamp", "payload", "payload_hash", "signature"}
    if set(frame) != required:
        raise GatewayProtocolError("DENY_PROTOCOL_FIELDS")
    if frame.get("schema") != WIRE_SCHEMA or frame.get("protocol_version") != PROTOCOL_VERSION:
        raise GatewayProtocolError("DENY_PROTOCOL_VERSION")
    message_type = frame.get("message_type")
    request_id = frame.get("request_id")
    nonce = frame.get("nonce")
    timestamp = frame.get("timestamp")
    payload = frame.get("payload")
    signature = frame.get("signature")
    if not all(isinstance(value, str) and value for value in (message_type, request_id, nonce, timestamp, signature)) or not isinstance(payload, Mapping):
        raise GatewayProtocolError("DENY_MALFORMED_FRAME")
    if len(request_id) > 128 or len(nonce) > 128 or len(message_type) > 64:
        raise GatewayProtocolError("DENY_FRAME_LIMIT")
    if frame.get("payload_hash") != canonical_hash(payload):
        raise GatewayProtocolError("DENY_PAYLOAD_HASH")
    expected = hmac.new(key, _signature_material(frame), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected, signature):
        raise GatewayProtocolError("DENY_BAD_SIGNATURE")
    observed = parse_utc(now or utc_now(), "Gateway verification time")
    sent = parse_utc(timestamp, "Gateway frame timestamp")
    if abs((observed - sent).total_seconds()) > maximum_freshness_seconds:
        raise GatewayProtocolError("DENY_STALE_TIMESTAMP")
    verified = VerifiedFrame(message_type, request_id, nonce, timestamp, dict(payload))
    if replay_guard is not None:
        replay_guard.accept(nonce, request_id)
    return verified


class LiveGateway:
    """Protocol shape shared by the safe default and loopback implementation."""

    def dispatch(self, command: Mapping[str, object]) -> Mapping[str, object]:  # pragma: no cover - interface
        raise NotImplementedError


class NoDispatchLiveGateway(LiveGateway):
    """Safe production default until the isolated signed AddOn handshake exists."""

    def dispatch(self, command: Mapping[str, object]) -> Mapping[str, object]:
        del command
        raise GatewayDispatchError("LIVE_GATEWAY_NOT_CONFIGURED")


class AuthenticatedLoopbackGateway(LiveGateway):
    """An explicit one-AddOn listener for the dedicated L3H port.

    Constructing this object cannot listen, arm a runtime, write a capability,
    or contact NinjaTrader.  The native AddOn initiates the connection and
    must prove independent provenance before any command is accepted.
    """

    def __init__(
        self, key: bytes, *, expected_addon_fingerprint: str, expected_capability_hash: str,
        port: int = LOOPBACK_PORT, timeout_seconds: float = 3.0,
        authorization_session_id: str | None = None,
    ) -> None:
        if not isinstance(key, bytes) or len(key) < 32:
            raise ValueError("L3H gateway key must contain at least 256 bits.")
        if not all(isinstance(value, str) and len(value) == 64 for value in (expected_addon_fingerprint, expected_capability_hash)):
            raise ValueError("L3H gateway requires exact AddOn and capability fingerprints.")
        self._key = key
        self.expected_addon_fingerprint = expected_addon_fingerprint
        self.expected_capability_hash = expected_capability_hash
        self.port = port
        self.timeout_seconds = timeout_seconds
        self.authorization_session_id = authorization_session_id or "l3h3-auth-session-" + uuid4().hex
        if not self.authorization_session_id.startswith("l3h3-auth-session-"):
            raise ValueError("L3H gateway authorization session is invalid.")
        self.gateway_session_id = "l3h3-gateway-session-" + uuid4().hex
        self._listener: socket.socket | None = None
        self._connection: socket.socket | None = None
        self._thread: threading.Thread | None = None
        self._stopping = threading.Event()
        self._connected = threading.Event()
        self._replay = ReplayGuard()
        self._write_lock = threading.Lock()
        self._response_lock = threading.Condition()
        self._responses: dict[str, Mapping[str, object]] = {}
        self._command_responses: dict[str, Mapping[str, object]] = {}
        self._reconciliation_lock = threading.Condition()
        self._reconciliations: list[Mapping[str, object]] = []
        self._session_id: str | None = None
        self._last_error: str | None = None
        self._live_send_count = 0

    @property
    def status(self) -> Mapping[str, object]:
        return {
            "state": "AUTHENTICATED" if self._connected.is_set() else "DISCONNECTED",
            "port": self.port, "loopback_only": True, "protocol_version": PROTOCOL_VERSION,
            "authenticated_addon": self._connected.is_set(), "session_id": self._session_id,
            "authorization_session_id_hash": canonical_hash(self.authorization_session_id),
            "gateway_session_id_hash": canonical_hash(self.gateway_session_id),
            "live_send_count": self._live_send_count, "last_error": self._last_error,
        }

    @property
    def addon_session_id(self) -> str | None:
        return self._session_id

    @property
    def live_send_count(self) -> int:
        return self._live_send_count

    def start(self) -> None:
        if self._listener is not None:
            return
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 0)
        listener.bind(("127.0.0.1", self.port))
        listener.listen(1)
        listener.settimeout(0.5)
        self._listener = listener
        self._thread = threading.Thread(target=self._serve, name="L3HAuthenticatedLoopback", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stopping.set()
        for item in (self._connection, self._listener):
            if item is not None:
                try:
                    item.close()
                except OSError:
                    pass
        if self._thread is not None and self._thread is not threading.current_thread():
            self._thread.join(timeout=3)
        self._connection = None
        self._listener = None
        self._connected.clear()
        self._session_id = None

    def dispatch(self, command: Mapping[str, object]) -> Mapping[str, object]:
        command_id = command.get("command_id")
        request_id = command.get("request_id")
        if not isinstance(command_id, str) or not command_id.startswith("l3h-cmd-") or not isinstance(request_id, str):
            raise GatewayDispatchError("COMMAND_IDENTITY_INVALID")
        live_entry = self._validate_entry_authority(command)
        with self._response_lock:
            prior = self._command_responses.get(command_id)
            if prior is not None:
                return prior
        if not self._connected.is_set() or self._connection is None:
            raise GatewayDispatchError("GATEWAY_ADDON_NOT_AUTHENTICATED")
        frame = sign_frame({
            "message_type": "COMMAND", "request_id": request_id, "nonce": "l3h-gw-" + uuid4().hex,
            "timestamp": utc_now(), "payload": dict(command),
        }, self._key)
        if live_entry:
            self._live_send_count += 1
        self._send(frame)
        with self._response_lock:
            if not self._response_lock.wait_for(lambda: request_id in self._responses, timeout=self.timeout_seconds):
                raise GatewayDispatchError("DISPATCH_ACKNOWLEDGEMENT_UNKNOWN")
            result = self._responses.pop(request_id)
            if result.get("command_id") != command_id:
                raise GatewayDispatchError("ACK_COMMAND_ID_MISMATCH")
            if result.get("outcome") not in {"ACK", "NACK"}:
                raise GatewayDispatchError("ACK_OUTCOME_INVALID")
            self._command_responses[command_id] = result
            return result

    def heartbeat(self) -> None:
        """Keep an already-authenticated native session disarmed-or-alive.

        A heartbeat neither arms the AddOn nor carries an execution command.
        It exists so a mechanical test can prove the native watchdog without
        reaching into socket internals.
        """

        if not self._connected.is_set():
            raise GatewayDispatchError("GATEWAY_ADDON_NOT_AUTHENTICATED")
        self._send(sign_frame({
            "message_type": "HEARTBEAT", "request_id": "l3h-heartbeat-" + uuid4().hex,
            "nonce": "l3h-gw-" + uuid4().hex, "timestamp": utc_now(), "payload": {},
        }, self._key))

    def reconciliations(self) -> tuple[Mapping[str, object], ...]:
        """Return received native truth reports without creating broker state."""

        with self._reconciliation_lock:
            return tuple(self._reconciliations)

    def wait_for_reconciliation(self, *, after: int = 0, timeout_seconds: float = 3.0) -> Mapping[str, object] | None:
        """Wait for a new native reconciliation report, or return ``None``."""

        with self._reconciliation_lock:
            if not self._reconciliation_lock.wait_for(lambda: len(self._reconciliations) > after, timeout=timeout_seconds):
                return None
            return self._reconciliations[-1]

    def _serve(self) -> None:
        assert self._listener is not None
        while not self._stopping.is_set():
            try:
                connection, address = self._listener.accept()
            except TimeoutError:
                continue
            except OSError:
                return
            if address[0] not in {"127.0.0.1", "::1"}:
                connection.close()
                continue
            self._serve_connection(connection)

    def _serve_connection(self, connection: socket.socket) -> None:
        connection.settimeout(0.5)
        buffer = b""
        try:
            while not self._stopping.is_set():
                try:
                    part = connection.recv(4096)
                except TimeoutError:
                    continue
                if not part:
                    return
                buffer += part
                if len(buffer) > MAXIMUM_FRAME_BYTES:
                    self._last_error = "OVERSIZED_FRAME"
                    return
                while b"\n" in buffer:
                    raw, buffer = buffer.split(b"\n", 1)
                    if raw:
                        self._ingest(raw, connection)
        except (OSError, UnicodeError, json.JSONDecodeError, GatewayProtocolError) as exc:
            self._last_error = str(exc) or type(exc).__name__
        finally:
            if self._connection is connection:
                self._connection = None
                self._connected.clear()
                self._session_id = None
            try:
                connection.close()
            except OSError:
                pass

    def _ingest(self, raw: bytes, connection: socket.socket) -> None:
        if len(raw) > MAXIMUM_FRAME_BYTES:
            raise GatewayProtocolError("DENY_OVERSIZED_FRAME")
        decoded: Any = json.loads(raw.decode("utf-8"))
        if not isinstance(decoded, Mapping):
            raise GatewayProtocolError("DENY_MALFORMED_FRAME")
        frame = verify_frame(decoded, self._key, replay_guard=self._replay)
        if frame.message_type == "ADDON_HELLO":
            self._accept_hello(frame, connection)
            return
        if not self._connected.is_set() or connection is not self._connection:
            raise GatewayProtocolError("DENY_NOT_AUTHENTICATED")
        if frame.message_type not in {"ACK", "NACK", "HEARTBEAT", "RECONCILIATION", "KILL_RECEIPT"}:
            raise GatewayProtocolError("DENY_UNSUPPORTED_MESSAGE")
        if frame.message_type in {"ACK", "NACK"}:
            with self._response_lock:
                self._responses[frame.request_id] = dict(frame.payload)
                self._response_lock.notify_all()
        elif frame.message_type == "RECONCILIATION":
            with self._reconciliation_lock:
                self._reconciliations.append(dict(frame.payload))
                if len(self._reconciliations) > 2_048:
                    del self._reconciliations[: len(self._reconciliations) - 2_048]
                self._reconciliation_lock.notify_all()

    def _accept_hello(self, frame: VerifiedFrame, connection: socket.socket) -> None:
        payload = frame.payload
        if payload.get("addon_fingerprint") != self.expected_addon_fingerprint:
            raise GatewayProtocolError("DENY_ADDON_PROVENANCE")
        if payload.get("capability_hash") != self.expected_capability_hash:
            raise GatewayProtocolError("DENY_CAPABILITY_BINDING")
        session_id = payload.get("addon_session_id")
        if not isinstance(session_id, str) or len(session_id) < 16:
            raise GatewayProtocolError("DENY_ADDON_SESSION")
        if self._connection is not None and self._connection is not connection:
            raise GatewayProtocolError("DENY_CONCURRENT_ADDON")
        self._connection = connection
        self._session_id = session_id
        self._connected.set()
        self._send(sign_frame({
            "message_type": "GATEWAY_HELLO", "request_id": frame.request_id, "nonce": "l3h-gw-" + uuid4().hex,
            "timestamp": utc_now(), "payload": {
                "addon_session_id": session_id, "protocol_version": PROTOCOL_VERSION,
                "authorization_session_id": self.authorization_session_id,
                "gateway_session_id": self.gateway_session_id,
            },
        }, self._key))

    def _validate_entry_authority(self, command: Mapping[str, object]) -> bool:
        """A bare signed gateway frame is never enough for a live entry."""

        action = command.get("action")
        if action not in {"ENTER_LONG", "ENTER_SHORT"}:
            return False
        if command.get("account_class") == "LOCAL_SIMULATION" and command.get("live_capital") is False:
            return False
        if command.get("account_class") != LIVE_ACCOUNT_CLASS or command.get("live_capital") is not True:
            raise GatewayDispatchError("LIVE_AUTHORIZATION_REQUIRED")
        envelope = command.get("live_authorization")
        if not isinstance(envelope, Mapping) or self._session_id is None:
            raise GatewayDispatchError("LIVE_AUTHORIZATION_REQUIRED")
        try:
            verify_native_admission_envelope(
                envelope, self._key, authorization_session_id=self.authorization_session_id,
                addon_session_id=self._session_id, gateway_session_id=self.gateway_session_id, command=command,
            )
        except ValueError as error:
            raise GatewayDispatchError(str(error)) from error
        return True

    def _send(self, frame: Mapping[str, object]) -> None:
        encoded = canonical_json(frame) + b"\n"
        if len(encoded) > MAXIMUM_FRAME_BYTES:
            raise GatewayDispatchError("GATEWAY_FRAME_TOO_LARGE")
        with self._write_lock:
            if self._connection is None:
                raise GatewayDispatchError("GATEWAY_ADDON_NOT_AUTHENTICATED")
            try:
                self._connection.sendall(encoded)
            except OSError as exc:
                self._connected.clear()
                raise GatewayDispatchError("GATEWAY_TRANSPORT_LOST") from exc
