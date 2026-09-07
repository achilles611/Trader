"""Bounded, status-only proof of native Sim101 reconciliation.

This is deliberately not :class:`PaperExecutionTransport`: it owns no
``PaperLedger`` and has no command or authenticated execution-session API. It
accepts two fresh AddOn connections and emits only the distinct
``RECONCILIATION_PROBE_GRANT`` frame, whose native handler is observational and
does not establish watchdog, command, entry, exit, or protective authority.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import hashlib
import ipaddress
import json
import math
import re
import socket
import time
from typing import Mapping, Sequence
from uuid import uuid4

from .contracts import ACCOUNT_BINDING, canonical_json
from .ninjatrader_transport import (
    ADDON_PROTOCOL_VERSION,
    EXECUTION_HOST,
    EXECUTION_PORT,
    EXECUTION_SCHEMA,
    FUTURE_TOLERANCE_SECONDS,
    HELLO_MAXIMUM_AGE_SECONDS,
    LocalPaperSecretProvider,
    MAXIMUM_FRAME_BYTES,
    expected_addon_source_fingerprint,
    sign_payload,
    verify_signature,
)


PROBE_SCHEMA = "lane-iii-ninjatrader-reconciliation-probe-v2"
RECONCILIATION_MAXIMUM_AGE_SECONDS = 30
_CANONICAL_ENDPOINT = ("127.0.0.1", 48136)
_HASH = re.compile(r"^[0-9a-f]{64}$")
_HELLO_FIELDS = frozenset({
    "schema", "message_type", "bridge_instance_id",
    "ninjatrader_session_id", "addon_protocol_version",
    "addon_source_fingerprint", "addon_build_fingerprint",
    "addon_build_timestamp", "account_name", "account_class",
    "instrument", "capability", "timestamp", "nonce", "signature",
})
_PROBE_GRANT_FIELDS = frozenset({
    "schema", "message_type", "probe_session_id", "server_nonce",
    "account_binding_hash", "mode", "live_capital", "timestamp",
    "signature",
})
_PROBE_RESULT_FIELDS = frozenset({
    "schema", "message_type", "probe_session_id", "server_nonce",
    "observation_generation", "first_sample_hash", "second_sample_hash",
    "snapshot_stable", "timestamp", "receipt_id", "account_name",
    "account_class", "instrument", "position_quantity",
    "working_order_count", "working_entry_count",
    "position_snapshot_complete", "order_snapshot_complete",
    "foreign_activity", "protective_stop_state", "signature",
})
_OBSERVATION_FIELDS = frozenset({
    "session_index", "hello", "hello_hash", "probe_grant",
    "probe_grant_hash", "probe_result", "probe_result_hash",
})
_PROOF_FIELDS = frozenset({
    "schema", "status", "authority", "host", "port", "started_at",
    "completed_at", "observed_provenance", "requested_context",
    "session_count", "observations", "probe_grants_sent",
    "session_grants_sent", "commands_sent", "heartbeats_sent",
    "paper_ledger_used", "proof_hash", "proof_signature",
})


class NinjaTraderReconciliationProbeError(RuntimeError):
    """One fail-closed probe refusal with a stable machine-readable code."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


def _refuse(code: str) -> None:
    raise NinjaTraderReconciliationProbeError(code)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _required_hash(value: object, code: str) -> str:
    if not isinstance(value, str) or _HASH.fullmatch(value) is None:
        _refuse(code)
    return value


def _required_text(value: object, code: str) -> str:
    if not isinstance(value, str) or not value.strip():
        _refuse(code)
    return value


def _required_build_timestamp(value: object, code: str) -> str:
    value = _required_text(value, code)
    if not value.endswith("Z"):
        _refuse(code)
    try:
        moment = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError:
        _refuse(code)
    if moment.tzinfo is None:
        _refuse(code)
    return value


def _fresh_timestamp(
    value: object,
    maximum_age_seconds: int,
    prefix: str,
    *,
    reference_time: datetime | None = None,
) -> datetime:
    if not isinstance(value, str):
        _refuse(prefix + "_TIMESTAMP_INVALID")
    try:
        moment = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        _refuse(prefix + "_TIMESTAMP_INVALID")
    if moment.tzinfo is None:
        _refuse(prefix + "_TIMESTAMP_INVALID")
    moment = moment.astimezone(timezone.utc)
    now = reference_time or datetime.now(timezone.utc)
    if (
        moment > now + timedelta(seconds=FUTURE_TOLERANCE_SECONDS)
        or now - moment > timedelta(seconds=maximum_age_seconds)
    ):
        _refuse(prefix + "_TIMESTAMP_NOT_FRESH")
    return moment


def _decode_frame(frame: bytes) -> dict[str, object]:
    if not frame or len(frame) > MAXIMUM_FRAME_BYTES:
        _refuse("FRAME_SIZE_INVALID")

    def unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for name, value in pairs:
            if name in result:
                _refuse("FRAME_DUPLICATE_KEY")
            result[name] = value
        return result

    try:
        value = json.loads(frame.decode("utf-8"), object_pairs_hook=unique_object)
    except (UnicodeDecodeError, json.JSONDecodeError):
        _refuse("FRAME_MALFORMED")
    if not isinstance(value, dict):
        _refuse("FRAME_MALFORMED")
    return value


class _FrameReader:
    def __init__(self, connection: socket.socket, deadline: float) -> None:
        self.connection = connection
        self.deadline = deadline
        self.buffer = bytearray()

    def read(self) -> dict[str, object]:
        while b"\n" not in self.buffer:
            remaining = self.deadline - time.monotonic()
            if remaining <= 0:
                _refuse("PROBE_TIMEOUT")
            self.connection.settimeout(remaining)
            try:
                chunk = self.connection.recv(
                    min(4096, MAXIMUM_FRAME_BYTES + 1 - len(self.buffer)),
                )
            except socket.timeout:
                _refuse("PROBE_TIMEOUT")
            except OSError:
                _refuse("PROBE_RECEIVE_FAILED")
            if not chunk:
                _refuse("FRAME_EOF")
            self.buffer.extend(chunk)
            if len(self.buffer) > MAXIMUM_FRAME_BYTES and b"\n" not in self.buffer:
                _refuse("FRAME_SIZE_INVALID")
        frame, _, remainder = self.buffer.partition(b"\n")
        self.buffer = bytearray(remainder)
        return _decode_frame(bytes(frame))

    def reject_buffered_extra_frame(self) -> None:
        if self.buffer:
            _refuse("UNEXPECTED_FRAME")


def _validate_hello(
    payload: Mapping[str, object],
    *,
    key: bytes,
    expected_source_fingerprint: str,
    expected_build_fingerprint: str | None = None,
    expected_build_timestamp: str | None = None,
    reference_time: datetime | None = None,
) -> tuple[datetime, str, str]:
    if set(payload) != _HELLO_FIELDS:
        _refuse("HELLO_SHAPE_INVALID")
    if payload.get("schema") != EXECUTION_SCHEMA or payload.get("message_type") != "HELLO":
        _refuse("HELLO_SHAPE_INVALID")
    if not verify_signature(key, payload):
        _refuse("HELLO_SIGNATURE_INVALID")
    moment = _fresh_timestamp(
        payload.get("timestamp"), HELLO_MAXIMUM_AGE_SECONDS, "HELLO",
        reference_time=reference_time,
    )
    if (
        payload.get("account_name"), payload.get("account_class"),
        payload.get("instrument"), payload.get("capability"),
    ) != ("Sim101", "LOCAL_SIMULATION", "MNQ SEP26", "PAPER_ONLY"):
        _refuse("HELLO_IDENTITY_MISMATCH")
    for name in ("bridge_instance_id", "ninjatrader_session_id", "nonce"):
        _required_text(payload.get(name), "HELLO_IDENTITY_INVALID")
    if payload.get("addon_protocol_version") != ADDON_PROTOCOL_VERSION:
        _refuse("HELLO_PROTOCOL_MISMATCH")
    if payload.get("addon_source_fingerprint") != expected_source_fingerprint:
        _refuse("HELLO_SOURCE_FINGERPRINT_MISMATCH")
    build_fingerprint = _required_hash(
        payload.get("addon_build_fingerprint"),
        "HELLO_BUILD_PROVENANCE_MISMATCH",
    )
    build_timestamp = _required_build_timestamp(
        payload.get("addon_build_timestamp"),
        "HELLO_BUILD_PROVENANCE_MISMATCH",
    )
    if (
        expected_build_fingerprint is not None
        and build_fingerprint != expected_build_fingerprint
    ) or (
        expected_build_timestamp is not None
        and build_timestamp != expected_build_timestamp
    ):
        _refuse("HELLO_BUILD_PROVENANCE_MISMATCH")
    return moment, build_fingerprint, build_timestamp


def _validate_probe_grant(
    payload: Mapping[str, object],
    *,
    key: bytes,
    reference_time: datetime | None = None,
) -> tuple[str, str, datetime]:
    if set(payload) != _PROBE_GRANT_FIELDS:
        _refuse("PROBE_GRANT_SHAPE_INVALID")
    if (
        payload.get("schema") != EXECUTION_SCHEMA
        or payload.get("message_type") != "RECONCILIATION_PROBE_GRANT"
    ):
        _refuse("PROBE_GRANT_SHAPE_INVALID")
    if not verify_signature(key, payload):
        _refuse("PROBE_GRANT_SIGNATURE_INVALID")
    probe_session_id = _required_text(
        payload.get("probe_session_id"), "PROBE_GRANT_IDENTITY_INVALID",
    )
    server_nonce = _required_text(
        payload.get("server_nonce"), "PROBE_GRANT_IDENTITY_INVALID",
    )
    if (
        payload.get("account_binding_hash") != ACCOUNT_BINDING.binding_hash
        or payload.get("mode") != "PAPER_SIM101"
        or payload.get("live_capital") is not False
    ):
        _refuse("PROBE_GRANT_AUTHORITY_INVALID")
    moment = _fresh_timestamp(
        payload.get("timestamp"), HELLO_MAXIMUM_AGE_SECONDS, "PROBE_GRANT",
        reference_time=reference_time,
    )
    return probe_session_id, server_nonce, moment


def _validate_probe_result(
    payload: Mapping[str, object],
    *,
    key: bytes,
    probe_session_id: str,
    server_nonce: str,
    reference_time: datetime | None = None,
) -> tuple[int, datetime]:
    if set(payload) != _PROBE_RESULT_FIELDS:
        _refuse("PROBE_RESULT_SHAPE_INVALID")
    if (
        payload.get("schema") != EXECUTION_SCHEMA
        or payload.get("message_type") != "RECONCILIATION_PROBE_RESULT"
    ):
        _refuse("UNEXPECTED_FRAME")
    if not verify_signature(key, payload):
        _refuse("PROBE_RESULT_SIGNATURE_INVALID")
    moment = _fresh_timestamp(
        payload.get("timestamp"), RECONCILIATION_MAXIMUM_AGE_SECONDS,
        "PROBE_RESULT", reference_time=reference_time,
    )
    if (
        payload.get("probe_session_id") != probe_session_id
        or payload.get("server_nonce") != server_nonce
    ):
        _refuse("PROBE_RESULT_CORRELATION_MISMATCH")
    _required_text(payload.get("receipt_id"), "PROBE_RESULT_RECEIPT_INVALID")
    generation = payload.get("observation_generation")
    if type(generation) is not int or generation < 0 or generation % 2 != 0:
        _refuse("PROBE_RESULT_GENERATION_INVALID")
    first_hash = _required_hash(
        payload.get("first_sample_hash"), "PROBE_RESULT_SAMPLE_INVALID",
    )
    second_hash = _required_hash(
        payload.get("second_sample_hash"), "PROBE_RESULT_SAMPLE_INVALID",
    )
    if payload.get("snapshot_stable") is not True or first_hash != second_hash:
        _refuse("PROBE_RESULT_SNAPSHOT_UNSTABLE")
    if (
        payload.get("account_name"), payload.get("account_class"),
        payload.get("instrument"),
    ) != ("Sim101", "LOCAL_SIMULATION", "MNQ SEP26"):
        _refuse("PROBE_RESULT_IDENTITY_MISMATCH")
    if type(payload.get("position_quantity")) is not int or payload.get("position_quantity") != 0:
        _refuse("PROBE_RESULT_NOT_FLAT")
    if (
        type(payload.get("working_order_count")) is not int
        or type(payload.get("working_entry_count")) is not int
        or payload.get("working_order_count") != 0
        or payload.get("working_entry_count") != 0
    ):
        _refuse("PROBE_RESULT_ORDERS_PRESENT")
    if (
        payload.get("position_snapshot_complete") is not True
        or payload.get("order_snapshot_complete") is not True
    ):
        _refuse("PROBE_RESULT_SNAPSHOT_INCOMPLETE")
    if payload.get("foreign_activity") is not False:
        _refuse("PROBE_RESULT_FOREIGN_ACTIVITY")
    if payload.get("protective_stop_state") != "NONE":
        _refuse("PROBE_RESULT_PROTECTIVE_STATE_INVALID")
    return generation, moment


def _send_signed(
    connection: socket.socket,
    payload: Mapping[str, object],
    key: bytes,
    deadline: float,
) -> dict[str, object]:
    signed = dict(payload)
    signed["signature"] = sign_payload(key, signed)
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        _refuse("PROBE_TIMEOUT")
    connection.settimeout(remaining)
    try:
        connection.sendall(canonical_json(signed) + b"\n")
    except socket.timeout:
        _refuse("PROBE_TIMEOUT")
    except OSError:
        _refuse("PROBE_SEND_FAILED")
    return signed


def _frame_hash(payload: Mapping[str, object]) -> str:
    return hashlib.sha256(canonical_json(payload)).hexdigest()


def _endpoint(test_endpoint: tuple[str, int] | None) -> tuple[str, int]:
    if test_endpoint is None:
        endpoint = (EXECUTION_HOST, EXECUTION_PORT)
        if endpoint != _CANONICAL_ENDPOINT:
            _refuse("PROBE_ENDPOINT_CONFIGURATION_INVALID")
    else:
        endpoint = test_endpoint
    host, port = endpoint
    try:
        if not isinstance(host, str) or not ipaddress.ip_address(host).is_loopback:
            _refuse("PROBE_HOST_NOT_LOOPBACK")
    except ValueError:
        _refuse("PROBE_HOST_NOT_LOOPBACK")
    if type(port) is not int or not 1 <= port <= 65535:
        _refuse("PROBE_PORT_INVALID")
    return host, port


def _secret(test_secret: bytes | None) -> bytes:
    if test_secret is None:
        try:
            return LocalPaperSecretProvider().load_key()
        except RuntimeError:
            _refuse("PROBE_SECRET_UNAVAILABLE")
    if type(test_secret) is not bytes or len(test_secret) < 32:
        _refuse("PROBE_SECRET_INVALID")
    return test_secret


def probe_flat_sim101_reconciliation(
    *,
    paper_policy_hash: str,
    risk_profile_hash: str,
    timeout_seconds: float = 15.0,
    _test_endpoint: tuple[str, int] | None = None,
    _test_secret: bytes | None = None,
) -> dict[str, object]:
    """Prove two stable native flat observations without runtime authority."""

    if (
        isinstance(timeout_seconds, bool)
        or not isinstance(timeout_seconds, (int, float))
        or not math.isfinite(float(timeout_seconds))
        or timeout_seconds <= 0
    ):
        _refuse("PROBE_TIMEOUT_INVALID")
    host, port = _endpoint(_test_endpoint)
    key = _secret(_test_secret)
    paper_policy_hash = _required_hash(paper_policy_hash, "PAPER_POLICY_HASH_INVALID")
    risk_profile_hash = _required_hash(risk_profile_hash, "RISK_PROFILE_HASH_INVALID")
    source_fingerprint = _required_hash(
        expected_addon_source_fingerprint(), "EXPECTED_SOURCE_FINGERPRINT_INVALID",
    )

    started_at = _utc_now()
    deadline = time.monotonic() + float(timeout_seconds)
    listener: socket.socket | None = None
    observations: list[dict[str, object]] = []
    first_hello_identity: tuple[object, object] | None = None
    seen_hello_nonces: set[str] = set()
    seen_receipts: set[str] = set()
    previous_generation = 0
    observed_build: tuple[str, str] | None = None
    try:
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if hasattr(socket, "SO_EXCLUSIVEADDRUSE"):
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
        else:  # pragma: no cover - production host is Windows
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind((host, port))
        listener.listen(1)
        for session_index in (1, 2):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                _refuse("PROBE_TIMEOUT")
            listener.settimeout(remaining)
            try:
                connection, remote = listener.accept()
            except socket.timeout:
                _refuse("PROBE_TIMEOUT")
            except OSError:
                _refuse("PROBE_ACCEPT_FAILED")
            with connection:
                try:
                    if not ipaddress.ip_address(remote[0]).is_loopback:
                        _refuse("PROBE_CLIENT_NOT_LOOPBACK")
                except ValueError:
                    _refuse("PROBE_CLIENT_NOT_LOOPBACK")
                reader = _FrameReader(connection, deadline)
                hello = reader.read()
                hello_time, build_fingerprint, build_timestamp = _validate_hello(
                    hello, key=key,
                    expected_source_fingerprint=source_fingerprint,
                )
                candidate_build = (build_fingerprint, build_timestamp)
                if observed_build is None:
                    observed_build = candidate_build
                elif candidate_build != observed_build:
                    _refuse("HELLO_BUILD_PROVENANCE_CHANGED")
                identity = (
                    hello.get("bridge_instance_id"),
                    hello.get("ninjatrader_session_id"),
                )
                nonce = str(hello["nonce"])
                if first_hello_identity is None:
                    first_hello_identity = identity
                elif identity != first_hello_identity:
                    _refuse("HELLO_SESSION_IDENTITY_CHANGED")
                if nonce in seen_hello_nonces:
                    _refuse("HELLO_NONCE_REPLAY")
                seen_hello_nonces.add(nonce)

                probe_session_id = "l3g-reconciliation-probe-" + uuid4().hex
                server_nonce = uuid4().hex
                grant = _send_signed(connection, {
                    "schema": EXECUTION_SCHEMA,
                    "message_type": "RECONCILIATION_PROBE_GRANT",
                    "probe_session_id": probe_session_id,
                    "server_nonce": server_nonce,
                    "account_binding_hash": ACCOUNT_BINDING.binding_hash,
                    "mode": "PAPER_SIM101",
                    "live_capital": False,
                    "timestamp": _utc_now(),
                }, key, deadline)
                _, _, grant_time = _validate_probe_grant(grant, key=key)
                result = reader.read()
                generation, result_time = _validate_probe_result(
                    result, key=key, probe_session_id=probe_session_id,
                    server_nonce=server_nonce,
                )
                if not hello_time <= grant_time <= result_time:
                    _refuse("PROBE_FRAME_TIME_ORDER_INVALID")
                if generation < previous_generation:
                    _refuse("PROBE_RESULT_GENERATION_NOT_MONOTONIC")
                previous_generation = generation
                receipt = str(result["receipt_id"])
                if receipt in seen_receipts:
                    _refuse("PROBE_RESULT_RECEIPT_REPLAY")
                seen_receipts.add(receipt)
                reader.reject_buffered_extra_frame()
                observations.append({
                    "session_index": session_index,
                    "hello": dict(hello),
                    "hello_hash": _frame_hash(hello),
                    "probe_grant": grant,
                    "probe_grant_hash": _frame_hash(grant),
                    "probe_result": dict(result),
                    "probe_result_hash": _frame_hash(result),
                })
    except NinjaTraderReconciliationProbeError:
        raise
    except OSError:
        _refuse("PROBE_LISTENER_FAILED")
    finally:
        if listener is not None:
            try:
                listener.close()
            except OSError:
                pass

    if observed_build is None:  # pragma: no cover - two-loop completion proves it
        _refuse("PROBE_BUILD_PROVENANCE_UNAVAILABLE")
    proof: dict[str, object] = {
        "schema": PROBE_SCHEMA,
        "status": "PASS",
        "authority": "OBSERVATION_ONLY_NO_RUNTIME_AUTHORITY",
        "host": host,
        "port": port,
        "started_at": started_at,
        "completed_at": _utc_now(),
        "observed_provenance": {
            "addon_protocol_version": ADDON_PROTOCOL_VERSION,
            "addon_source_fingerprint": source_fingerprint,
            "addon_build_fingerprint": observed_build[0],
            "addon_build_timestamp": observed_build[1],
        },
        "requested_context": {
            "paper_policy_hash": paper_policy_hash,
            "risk_profile_hash": risk_profile_hash,
            "account_binding_hash": ACCOUNT_BINDING.binding_hash,
            "mode": "PAPER_SIM101",
            "live_capital": False,
        },
        "session_count": 2,
        "observations": observations,
        "probe_grants_sent": 2,
        "session_grants_sent": 0,
        "commands_sent": 0,
        "heartbeats_sent": 0,
        "paper_ledger_used": False,
    }
    proof["proof_hash"] = _frame_hash(proof)
    proof["proof_signature"] = sign_payload(key, proof)
    return proof


def _expected_moment(value: object) -> datetime:
    if value is None:
        return datetime.now(timezone.utc)
    if isinstance(value, datetime):
        if value.tzinfo is None:
            _refuse("PROOF_EXPECTED_AT_INVALID")
        return value.astimezone(timezone.utc)
    if isinstance(value, str):
        try:
            moment = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            _refuse("PROOF_EXPECTED_AT_INVALID")
        if moment.tzinfo is None:
            _refuse("PROOF_EXPECTED_AT_INVALID")
        return moment.astimezone(timezone.utc)
    _refuse("PROOF_EXPECTED_AT_INVALID")


def validate_flat_sim101_reconciliation_proof(
    proof: Mapping[str, object],
    *,
    expected_policy_hash: str,
    expected_risk_hash: str,
    expected_at: str | datetime | None = None,
    _test_endpoint: tuple[str, int] | None = None,
    _test_secret: bytes | None = None,
) -> dict[str, object]:
    """Cryptographically and semantically revalidate one status-only proof."""

    if not isinstance(proof, Mapping) or set(proof) != _PROOF_FIELDS:
        _refuse("PROOF_SHAPE_INVALID")
    candidate = dict(proof)
    supplied_signature = candidate.pop("proof_signature", None)
    supplied_hash = candidate.pop("proof_hash", None)
    if not isinstance(supplied_hash, str) or _HASH.fullmatch(supplied_hash) is None:
        _refuse("PROOF_HASH_INVALID")
    if hashlib.sha256(canonical_json(candidate)).hexdigest() != supplied_hash:
        _refuse("PROOF_HASH_MISMATCH")
    if (
        proof.get("schema") != PROBE_SCHEMA
        or proof.get("status") != "PASS"
        or proof.get("authority") != "OBSERVATION_ONLY_NO_RUNTIME_AUTHORITY"
        or proof.get("session_count") != 2
        or proof.get("probe_grants_sent") != 2
        or proof.get("session_grants_sent") != 0
        or proof.get("commands_sent") != 0
        or proof.get("heartbeats_sent") != 0
        or proof.get("paper_ledger_used") is not False
    ):
        _refuse("PROOF_AUTHORITY_INVALID")

    host, port = _endpoint(_test_endpoint)
    if proof.get("host") != host or proof.get("port") != port:
        _refuse("PROOF_ENDPOINT_MISMATCH")
    expected_policy_hash = _required_hash(
        expected_policy_hash, "EXPECTED_POLICY_HASH_INVALID",
    )
    expected_risk_hash = _required_hash(expected_risk_hash, "EXPECTED_RISK_HASH_INVALID")
    reference_time = _expected_moment(expected_at)
    completed = _fresh_timestamp(
        proof.get("completed_at"), RECONCILIATION_MAXIMUM_AGE_SECONDS,
        "PROOF_COMPLETED", reference_time=reference_time,
    )
    started = _fresh_timestamp(
        proof.get("started_at"), RECONCILIATION_MAXIMUM_AGE_SECONDS,
        "PROOF_STARTED", reference_time=reference_time,
    )
    if started > completed:
        _refuse("PROOF_TIME_INVALID")

    source_fingerprint = expected_addon_source_fingerprint()
    provenance = proof.get("observed_provenance")
    if not isinstance(provenance, Mapping):
        _refuse("PROOF_PROVENANCE_MISMATCH")
    observed_build_fingerprint = _required_hash(
        provenance.get("addon_build_fingerprint"),
        "PROOF_PROVENANCE_MISMATCH",
    )
    observed_build_timestamp = _required_build_timestamp(
        provenance.get("addon_build_timestamp"),
        "PROOF_PROVENANCE_MISMATCH",
    )
    if dict(provenance) != {
        "addon_protocol_version": ADDON_PROTOCOL_VERSION,
        "addon_source_fingerprint": source_fingerprint,
        "addon_build_fingerprint": observed_build_fingerprint,
        "addon_build_timestamp": observed_build_timestamp,
    }:
        _refuse("PROOF_PROVENANCE_MISMATCH")
    context = proof.get("requested_context")
    if not isinstance(context, Mapping) or dict(context) != {
        "paper_policy_hash": expected_policy_hash,
        "risk_profile_hash": expected_risk_hash,
        "account_binding_hash": ACCOUNT_BINDING.binding_hash,
        "mode": "PAPER_SIM101",
        "live_capital": False,
    }:
        _refuse("PROOF_CONTEXT_MISMATCH")
    key = _secret(_test_secret)
    signed_proof = dict(proof)
    signed_proof.pop("proof_signature", None)
    signed_proof["signature"] = supplied_signature
    if not verify_signature(key, signed_proof):
        _refuse("PROOF_SIGNATURE_INVALID")

    observations = proof.get("observations")
    if not isinstance(observations, list) or len(observations) != 2:
        _refuse("PROOF_OBSERVATIONS_INVALID")
    first_identity: tuple[object, object] | None = None
    hello_nonces: set[str] = set()
    probe_ids: set[str] = set()
    grant_nonces: set[str] = set()
    receipt_ids: set[str] = set()
    previous_generation = 0
    for expected_index, value in enumerate(observations, 1):
        if not isinstance(value, Mapping) or set(value) != _OBSERVATION_FIELDS:
            _refuse("PROOF_OBSERVATION_SHAPE_INVALID")
        if value.get("session_index") != expected_index:
            _refuse("PROOF_OBSERVATION_ORDER_INVALID")
        hello = value.get("hello")
        grant = value.get("probe_grant")
        result = value.get("probe_result")
        if not all(isinstance(item, Mapping) for item in (hello, grant, result)):
            _refuse("PROOF_OBSERVATION_SHAPE_INVALID")
        assert isinstance(hello, Mapping)
        assert isinstance(grant, Mapping)
        assert isinstance(result, Mapping)
        if (
            value.get("hello_hash") != _frame_hash(hello)
            or value.get("probe_grant_hash") != _frame_hash(grant)
            or value.get("probe_result_hash") != _frame_hash(result)
        ):
            _refuse("PROOF_OBSERVATION_HASH_MISMATCH")
        hello_time, _, _ = _validate_hello(
            hello, key=key, expected_source_fingerprint=source_fingerprint,
            expected_build_fingerprint=observed_build_fingerprint,
            expected_build_timestamp=observed_build_timestamp,
            reference_time=reference_time,
        )
        identity = (hello.get("bridge_instance_id"), hello.get("ninjatrader_session_id"))
        if first_identity is None:
            first_identity = identity
        elif identity != first_identity:
            _refuse("PROOF_ADDON_IDENTITY_CHANGED")
        hello_nonce = str(hello["nonce"])
        if hello_nonce in hello_nonces:
            _refuse("PROOF_HELLO_NONCE_REPLAY")
        hello_nonces.add(hello_nonce)
        probe_id, grant_nonce, grant_time = _validate_probe_grant(
            grant, key=key, reference_time=reference_time,
        )
        if probe_id in probe_ids or grant_nonce in grant_nonces:
            _refuse("PROOF_GRANT_REPLAY")
        probe_ids.add(probe_id)
        grant_nonces.add(grant_nonce)
        generation, result_time = _validate_probe_result(
            result, key=key, probe_session_id=probe_id,
            server_nonce=grant_nonce, reference_time=reference_time,
        )
        if not hello_time <= grant_time <= result_time:
            _refuse("PROOF_FRAME_TIME_ORDER_INVALID")
        if generation < previous_generation:
            _refuse("PROOF_GENERATION_NOT_MONOTONIC")
        previous_generation = generation
        receipt_id = str(result["receipt_id"])
        if receipt_id in receipt_ids:
            _refuse("PROOF_RESULT_RECEIPT_REPLAY")
        receipt_ids.add(receipt_id)
    return dict(proof)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Obtain two signed, stable, status-only Sim101 reconciliations.",
    )
    parser.add_argument("--paper-policy-hash", required=True)
    parser.add_argument("--risk-profile-hash", required=True)
    parser.add_argument("--timeout-seconds", type=float, default=15.0)
    arguments = parser.parse_args(argv)
    try:
        proof = probe_flat_sim101_reconciliation(
            paper_policy_hash=arguments.paper_policy_hash,
            risk_profile_hash=arguments.risk_profile_hash,
            timeout_seconds=arguments.timeout_seconds,
        )
    except NinjaTraderReconciliationProbeError as error:
        print(json.dumps({"schema": PROBE_SCHEMA, "status": "FAIL", "error": error.code}))
        return 2
    print(json.dumps(proof, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":  # pragma: no cover - exercised through main().
    raise SystemExit(main())
