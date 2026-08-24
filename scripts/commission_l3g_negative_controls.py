"""Run disarmed protocol attacks against the installed Sim101-only AddOn.

This commissioning harness temporarily owns the execution port, authenticates
the exact installed AddOn, and sends only commands that must be rejected plus
one non-mutating RECONCILE command for duplicate/idempotence verification.  It
never sends a valid entry, exit, cancel, or flatten command.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import socket
import sys
import time
from typing import Mapping

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.l3g_paper.contracts import ACCOUNT_BINDING, POLICY, RISK_PROFILE, canonical_json
from src.l3g_paper.ninjatrader_transport import EXECUTION_SCHEMA, sign_payload, verify_signature


HOST = "127.0.0.1"
PORT = 48136
KEY_PATH = Path.home() / "Documents" / "NinjaTrader 8" / "l3g.paper.local.key"


def now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def signed(key: bytes, payload: Mapping[str, object]) -> dict[str, object]:
    result = dict(payload)
    result["signature"] = sign_payload(key, result)
    return result


def send(stream: socket.SocketIO, key: bytes, payload: Mapping[str, object], *, corrupt: bool = False) -> None:
    message = signed(key, payload)
    if corrupt:
        message["signature"] = "0" * 64
    stream.write(canonical_json(message) + b"\n")
    stream.flush()


def receive(stream: socket.SocketIO, key: bytes, *, timeout_at: float) -> dict[str, object]:
    while time.monotonic() < timeout_at:
        line = stream.readline()
        if not line:
            raise RuntimeError("Installed paper AddOn disconnected during negative controls.")
        value = json.loads(line)
        if not isinstance(value, dict) or not verify_signature(key, value):
            raise RuntimeError("Installed paper AddOn returned an invalid signed frame.")
        return value
    raise TimeoutError("Timed out waiting for installed paper AddOn response.")


def wait_for(stream: socket.SocketIO, key: bytes, message_type: str, *, command_id: str | None = None) -> dict[str, object]:
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        value = receive(stream, key, timeout_at=deadline)
        if value.get("message_type") != message_type:
            continue
        if command_id is not None and value.get("command_id") != command_id:
            continue
        return value
    raise TimeoutError(f"No {message_type} response for {command_id or 'session'}.")


def command(session_id: str, command_id: str, **changes: object) -> dict[str, object]:
    created = now()
    payload: dict[str, object] = {
        "schema": EXECUTION_SCHEMA,
        "message_type": "COMMAND",
        "execution_session_id": session_id,
        "command_id": command_id,
        "command_sequence": 1,
        "session_id": session_id,
        "intent_id": "l3g-pi-negative-control",
        "decision_id": "l3g-pd-negative-control",
        "action": "ENTER_LONG",
        "account_name": "Sim101",
        "account_class": "LOCAL_SIMULATION",
        "instrument": "MNQ SEP26",
        "quantity": 1,
        "expected_position": "LONG",
        "created_at": created,
        "expires_at": (datetime.now(timezone.utc) + timedelta(seconds=5)).isoformat().replace("+00:00", "Z"),
        "policy_hash": POLICY.configuration_hash,
        "risk_profile_hash": RISK_PROFILE.configuration_hash,
        "account_binding_hash": ACCOUNT_BINDING.binding_hash,
        "reason_code": "DISARMED_NEGATIVE_CONTROL",
        "risk_grant_id": "l3g-pg-negative-control",
    }
    payload.update(changes)
    return payload


def main() -> int:
    key = KEY_PATH.read_bytes()
    if len(key) < 32:
        raise RuntimeError("Local paper signing key is unavailable or invalid.")
    listener = socket.socket()
    if hasattr(socket, "SO_EXCLUSIVEADDRUSE"):
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
    else:
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind((HOST, PORT))
    listener.listen(1)
    listener.settimeout(10)
    connection, remote = listener.accept()
    if remote[0] != HOST:
        raise RuntimeError("Non-loopback AddOn peer refused.")
    connection.settimeout(5)
    stream = connection.makefile("rwb", buffering=0)
    try:
        hello = receive(stream, key, timeout_at=time.monotonic() + 5)
        exact_hello = (
            hello.get("message_type") == "HELLO"
            and hello.get("account_name") == "Sim101"
            and hello.get("account_class") == "LOCAL_SIMULATION"
            and hello.get("instrument") == "MNQ SEP26"
            and hello.get("capability") == "PAPER_ONLY"
        )
        if not exact_hello:
            raise RuntimeError("Installed AddOn HELLO did not retain exact paper capability.")
        session_id = "l3g-es-negative-controls"
        send(stream, key, {
            "schema": EXECUTION_SCHEMA,
            "message_type": "SESSION_GRANT",
            "execution_session_id": session_id,
            "server_nonce": "negative-controls",
            "paper_policy_hash": POLICY.configuration_hash,
            "risk_profile_hash": RISK_PROFILE.configuration_hash,
            "account_binding_hash": ACCOUNT_BINDING.binding_hash,
            "heartbeat_interval_seconds": 1,
            "heartbeat_watchdog_seconds": 5,
            "command_ttl_seconds": 5,
            "mode": "PAPER_SIM101",
            "live_capital": False,
            "timestamp": now(),
        })
        before = wait_for(stream, key, "RECONCILIATION")
        if before.get("position_quantity") != 0 or before.get("working_order_count") != 0 or before.get("foreign_activity") is not False:
            raise RuntimeError("Negative controls require exact flat, order-free Sim101 reconciliation.")

        attacks = (
            ("lucid_account", {"account_name": "Lucid25kflex01"}, "ACCOUNT_MISMATCH", False),
            ("quantity_two", {"quantity": 2}, "QUANTITY_REFUSED", False),
            ("wrong_instrument", {"instrument": "NQ SEP26"}, "INSTRUMENT_MISMATCH", False),
            ("expired", {"expires_at": (datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat().replace("+00:00", "Z")}, "COMMAND_EXPIRED", False),
            ("wrong_policy_hash", {"policy_hash": "0" * 64}, "AUTHORITY_HASH_MISMATCH", False),
            ("bad_signature", {}, "INVALID_SIGNATURE_OR_SCHEMA", True),
        )
        results: dict[str, str] = {}
        for name, changes, expected, corrupt in attacks:
            command_id = "l3g-pc-negative-" + name
            payload = command(session_id, command_id, **changes)
            send(stream, key, payload, corrupt=corrupt)
            rejection = wait_for(stream, key, "COMMAND_REJECTED")
            reason = str(rejection.get("reason_code"))
            if reason != expected:
                raise RuntimeError(f"{name} returned {reason}, expected {expected}.")
            results[name] = reason

        reconcile_id = "l3g-pc-negative-reconcile"
        reconcile = command(
            session_id, reconcile_id, action="RECONCILE", quantity=0,
            expected_position="FLAT", reason_code="NEGATIVE_CONTROL_RECONCILE",
        )
        send(stream, key, reconcile)
        after = wait_for(stream, key, "RECONCILIATION")
        acknowledgement = wait_for(stream, key, "COMMAND_ACK", command_id=reconcile_id)
        send(stream, key, reconcile)
        duplicate = wait_for(stream, key, "COMMAND_ACK", command_id=reconcile_id)
        if acknowledgement.get("reason_code") != "ACCEPTED" or duplicate.get("reason_code") != "DUPLICATE_IDEMPOTENT" or duplicate.get("duplicate") is not True:
            raise RuntimeError("Duplicate-command idempotence was not confirmed.")
        if after.get("position_quantity") != 0 or after.get("working_order_count") != 0 or after.get("foreign_activity") is not False:
            raise RuntimeError("Sim101 changed during disarmed negative controls.")
        print(json.dumps({
            "state": "PASSED",
            "exact_addon_hello": True,
            "attacks": results,
            "duplicate_command": "DUPLICATE_IDEMPOTENT",
            "sim101_position": 0,
            "sim101_working_orders": 0,
            "live_capital_touched": False,
        }, sort_keys=True))
        return 0
    finally:
        stream.close()
        connection.close()
        listener.close()


if __name__ == "__main__":
    raise SystemExit(main())
