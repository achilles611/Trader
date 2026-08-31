"""Installed-runtime Sim101 mechanical commissioning harness.

It accepts only a locally signed capability whose account class is
``LOCAL_SIMULATION`` and whose ``live_capital`` flag is false.  The harness
never derives live authority, and records compact non-secret results under the
ACL-restricted L3H authority root.
"""

from __future__ import annotations

import argparse
from dataclasses import asdict
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import threading
import time
from typing import Any, Mapping
from uuid import uuid4


REPOSITORY = Path(__file__).resolve().parents[1]
import sys
if str(REPOSITORY) not in sys.path:
    sys.path.insert(0, str(REPOSITORY))

from src.l3h_live.contracts import AccountClass, canonical_json, load_verified_capability
from src.l3h_live.gateway import AuthenticatedLoopbackGateway, GatewayDispatchError, sign_frame, utc_now


def _authority_root(value: str | None) -> Path:
    return Path(value).expanduser().resolve() if value else Path(os.environ["LOCALAPPDATA"]) / "Beelzebub" / "authority" / "l3h"


def _latest_capability(root: Path) -> Path:
    candidates = [path for path in (root / "capabilities").glob("l3h-cap-sim101-*.json") if not path.name.endswith(".attestation.json")]
    if not candidates:
        raise ValueError("No local Sim101 mechanical capability is available.")
    return max(candidates, key=lambda path: path.stat().st_mtime)


def _load(root: Path):
    capability = load_verified_capability(_latest_capability(root), (root / "keys" / "l3h.capability.hmac.key").read_bytes())
    if capability.account_class is not AccountClass.LOCAL_SIMULATION or capability.live_capital:
        raise ValueError("Harness refuses non-Sim101 or live-capital capability material.")
    return capability


def _command(capability, action: str, *, command_id: str | None = None, client_order_id: str | None = None, **overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "command_id": command_id or "l3h-cmd-sim101-" + uuid4().hex,
        "request_id": "l3h-request-sim101-" + uuid4().hex,
        "client_order_id": client_order_id or "BZ-L3H-" + uuid4().hex[:20].upper(),
        "action": action,
        "capability_hash": capability.capability_hash,
        "capability_generation": capability.capability_id,
        "commissioning_epoch": capability.commissioning_epoch,
        "account_binding_hash": capability.account_binding_hash,
        "native_instrument": capability.native_instrument,
        "canonical_contract": capability.canonical_contract,
        "quantity": 1,
        "account_class": AccountClass.LOCAL_SIMULATION.value,
        "live_capital": False,
        "session_valid": True,
        "daily_loss_clear": True,
    }
    values.update(overrides)
    return values


def _wait_authenticated(gateway: AuthenticatedLoopbackGateway, timeout: float = 20.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if gateway.status["authenticated_addon"] is True:
            return
        time.sleep(0.1)
    raise GatewayDispatchError("RUNTIME_HELLO_TIMEOUT")


def _heartbeat_loop(gateway: AuthenticatedLoopbackGateway, stop: threading.Event) -> None:
    while not stop.wait(1.0):
        try:
            gateway.heartbeat()
        except GatewayDispatchError:
            return


def _raw(gateway: AuthenticatedLoopbackGateway, frame: Mapping[str, object]) -> Mapping[str, object]:
    request_id = str(frame["request_id"])
    with gateway._response_lock:  # Deliberately private: only adversarial test frames use this path.
        gateway._responses.pop(request_id, None)
    gateway._send(frame)
    with gateway._response_lock:
        if not gateway._response_lock.wait_for(lambda: request_id in gateway._responses, timeout=gateway.timeout_seconds):
            raise GatewayDispatchError("NEGATIVE_FRAME_ACK_TIMEOUT")
        return dict(gateway._responses.pop(request_id))


def _expect(result: Mapping[str, object], outcome: str, reason: str) -> dict[str, str]:
    actual_outcome, actual_reason = str(result.get("outcome")), str(result.get("reason"))
    if (actual_outcome, actual_reason) != (outcome, reason):
        raise RuntimeError(f"Expected {outcome}/{reason}, received {actual_outcome}/{actual_reason}.")
    return {"outcome": actual_outcome, "reason": actual_reason}


def _flat(reconciliation: Mapping[str, object]) -> bool:
    return (
        reconciliation.get("account") == "Sim101" and reconciliation.get("contract") == "MNQ SEP26"
        and reconciliation.get("position") == "FLAT" and reconciliation.get("quantity") == 0 and reconciliation.get("owned_working_orders") == 0
        and reconciliation.get("foreign_or_unknown_orders") == 0 and reconciliation.get("armed") is False
    )


def _record(root: Path, stage: str, result: Mapping[str, object]) -> None:
    path = root / "events" / "l3h-sim101-mechanical-results.json"
    prior: dict[str, Any] = {}
    if path.exists():
        prior = json.loads(path.read_text(encoding="utf-8"))
    stages = dict(prior.get("stages", {}))
    stages[stage] = {"at": utc_now(), **dict(result)}
    payload = {"schema": "l3h-sim101-mechanical-results-v1", "live_capital": "DENIED", "stages": stages}
    temporary = path.with_suffix(".tmp")
    temporary.write_bytes(canonical_json(payload))
    os.replace(temporary, path)


def _gateway(root: Path, capability) -> AuthenticatedLoopbackGateway:
    gateway = AuthenticatedLoopbackGateway(
        (root / "keys" / "l3h.execution.local.key").read_bytes(),
        expected_addon_fingerprint=capability.source_fingerprint,
        expected_capability_hash=capability.capability_hash,
    )
    gateway.start()
    _wait_authenticated(gateway)
    return gateway


def _probe(root: Path, capability, gateway: AuthenticatedLoopbackGateway) -> dict[str, object]:
    prior = len(gateway.reconciliations())
    _expect(gateway.dispatch(_command(capability, "RECONCILE")), "ACK", "RECONCILIATION_SENT")
    reconciliation = gateway.wait_for_reconciliation(after=prior, timeout_seconds=5)
    if reconciliation is None or not _flat(reconciliation):
        raise RuntimeError("Native Sim101 flat reconciliation is incomplete or non-flat.")
    return {"runtime_hello": "PASS", "gateway_auth": "PASS", "account_class": capability.account_class.value,
            "account_binding": "PASS", "reconciliation": dict(reconciliation), "live_armed": False}


def _negative(root: Path, capability, gateway: AuthenticatedLoopbackGateway) -> dict[str, object]:
    gateway.heartbeat()
    rejected = _expect(gateway.dispatch(_command(capability, "ARM", daily_loss_clear=False)), "NACK", "DENY_DAILY_LOSS")
    wrong_contract = _expect(gateway.dispatch(_command(capability, "ARM", native_instrument="NQ SEP26")), "NACK", "DENY_WRONG_CONTRACT")
    qty_two = _expect(gateway.dispatch(_command(capability, "ARM", quantity=2)), "NACK", "DENY_QTY")
    bad = sign_frame({"message_type": "COMMAND", "request_id": "l3h-negative-signature-" + uuid4().hex,
                      "nonce": "l3h-negative-signature-" + uuid4().hex, "timestamp": utc_now(),
                      "payload": _command(capability, "RECONCILE")}, gateway._key)
    bad["signature"] = "0" * 64
    bad_signature = _expect(_raw(gateway, bad), "NACK", "DENY_BAD_SIGNATURE")
    replay_command = _command(capability, "RECONCILE")
    replay = sign_frame({"message_type": "COMMAND", "request_id": "l3h-negative-replay-" + uuid4().hex,
                         "nonce": "l3h-negative-replay-" + uuid4().hex, "timestamp": utc_now(), "payload": replay_command}, gateway._key)
    _expect(_raw(gateway, replay), "ACK", "RECONCILIATION_SENT")
    replay_result = _expect(_raw(gateway, replay), "NACK", "DENY_REPLAY")
    duplicate_id, duplicate_order = "l3h-cmd-duplicate-" + uuid4().hex, "BZ-L3H-DUP-" + uuid4().hex[:20].upper()
    first = sign_frame({"message_type": "COMMAND", "request_id": "l3h-negative-duplicate-a-" + uuid4().hex,
                        "nonce": "l3h-negative-duplicate-a-" + uuid4().hex, "timestamp": utc_now(),
                        "payload": _command(capability, "RECONCILE", command_id=duplicate_id, client_order_id=duplicate_order)}, gateway._key)
    second = sign_frame({"message_type": "COMMAND", "request_id": "l3h-negative-duplicate-b-" + uuid4().hex,
                         "nonce": "l3h-negative-duplicate-b-" + uuid4().hex, "timestamp": utc_now(),
                         "payload": _command(capability, "RECONCILE", command_id=duplicate_id, client_order_id=duplicate_order)}, gateway._key)
    _expect(_raw(gateway, first), "ACK", "RECONCILIATION_SENT")
    duplicate = _expect(_raw(gateway, second), "ACK", "DUPLICATE_COMMAND_NOOP")
    return {"sim101_reject": rejected, "wrong_contract": wrong_contract, "qty_2_reject": qty_two,
            "bad_signature": bad_signature, "replay": replay_result, "duplicate": duplicate, "live_armed": False}


def _entry(capability, gateway: AuthenticatedLoopbackGateway, side: str) -> dict[str, object]:
    gateway.heartbeat()
    _expect(gateway.dispatch(_command(capability, "ARM")), "ACK", "ARMED_FLAT")
    acknowledgement = _expect(gateway.dispatch(_command(capability, "ENTER_" + side)), "ACK", "BROKER_SUBMIT_REQUESTED")
    deadline = time.monotonic() + 10
    observed: Mapping[str, object] | None = None
    while time.monotonic() < deadline:
        reports = gateway.reconciliations()
        if reports:
            candidate = reports[-1]
            if candidate.get("quantity") in (1, -1) and candidate.get("protection_available") is True:
                observed = candidate
                break
        time.sleep(0.1)
    if observed is None:
        raise RuntimeError("Native entry/protective-stop proof was not observed before timeout.")
    if observed.get("position") != side:
        raise RuntimeError("Native position side does not match the submitted Sim101 command.")
    return {"entry_ack": acknowledgement, "position": dict(observed), "protection": "PASS"}


def _kill(capability, gateway: AuthenticatedLoopbackGateway) -> dict[str, object]:
    prior = len(gateway.reconciliations())
    acknowledgement = _expect(gateway.dispatch(_command(capability, "KILL_FLATTEN_DISARM")), "ACK", "KILL_LATCHED")
    deadline = time.monotonic() + 10
    observed: Mapping[str, object] | None = None
    while time.monotonic() < deadline:
        reports = gateway.reconciliations()
        if len(reports) > prior and _flat(reports[-1]):
            observed = reports[-1]
            break
        time.sleep(0.1)
    if observed is None:
        raise RuntimeError("Native kill did not prove a flat, disarmed Sim101 reconciliation.")
    return {"kill": acknowledgement, "reconciliation": dict(observed), "live_armed": False}


def _unknown_transport(root: Path, capability, gateway: AuthenticatedLoopbackGateway) -> dict[str, object]:
    """Prove a lost armed transport is quarantined and flattened natively."""

    gateway.heartbeat()
    _expect(gateway.dispatch(_command(capability, "ARM")), "ACK", "ARMED_FLAT")
    gateway.stop()
    time.sleep(2)
    replacement = _gateway(root, capability)
    try:
        reconciliation = replacement.wait_for_reconciliation(after=0, timeout_seconds=5)
        if reconciliation is None or not _flat(reconciliation):
            raise RuntimeError("Transport-loss quarantine did not return a flat native reconciliation.")
        if reconciliation.get("unknown_state") is not True or reconciliation.get("kill_latch") is not True:
            raise RuntimeError("Native transport loss did not remain explicitly quarantined and latched.")
        return {"unknown_state": "PASS", "reconciliation": dict(reconciliation), "live_armed": False}
    finally:
        replacement.stop()


def _await_native_menu_kill(capability, gateway: AuthenticatedLoopbackGateway, side: str) -> dict[str, object]:
    entry = _entry(capability, gateway, side)
    prior = len(gateway.reconciliations())
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        reports = gateway.reconciliations()
        if len(reports) > prior and _flat(reports[-1]) and reports[-1].get("kill_latch") is True:
            return {"entry": entry, "native_menu_kill": "PASS", "reconciliation": dict(reports[-1]), "live_armed": False}
        time.sleep(0.1)
    raise RuntimeError("Timed out waiting for the native NinjaTrader kill-menu proof; transport-loss safeguard will now flatten/disarm.")


def _await_script_kill(capability, gateway: AuthenticatedLoopbackGateway) -> dict[str, object]:
    entry = _entry(capability, gateway, "LONG")
    prior = len(gateway.reconciliations())
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        reports = gateway.reconciliations()
        if len(reports) > prior and _flat(reports[-1]) and reports[-1].get("kill_latch") is True:
            return {"entry": entry, "script_kill": "PASS", "reconciliation": dict(reports[-1]), "live_armed": False}
        time.sleep(0.1)
    raise RuntimeError("Timed out waiting for the out-of-band script kill proof; transport-loss safeguard will now flatten/disarm.")


def _reconnect(root: Path, capability, gateway: AuthenticatedLoopbackGateway) -> dict[str, object]:
    gateway.stop()
    time.sleep(2)
    replacement = _gateway(root, capability)
    try:
        reconciliation = replacement.wait_for_reconciliation(after=0, timeout_seconds=5)
        if reconciliation is None or not _flat(reconciliation) or reconciliation.get("kill_latch") is not False:
            raise RuntimeError("Reconnect did not restore a fresh, disarmed Sim101 reconciliation.")
        return {"reconnect": "PASS", "reconciliation": dict(reconciliation), "live_armed": False}
    finally:
        replacement.stop()


def _await_foreign_activity(capability, gateway: AuthenticatedLoopbackGateway) -> dict[str, object]:
    """Wait for an independently submitted Sim101 order to be quarantined."""

    gateway.heartbeat()
    _expect(gateway.dispatch(_command(capability, "ARM")), "ACK", "ARMED_FLAT")
    prior = len(gateway.reconciliations())
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        reports = gateway.reconciliations()
        if len(reports) > prior:
            current = reports[-1]
            if (current.get("foreign_or_unknown_orders") == 1 and current.get("kill_latch") is True
                    and current.get("armed") is False and current.get("position") == "FLAT"
                    and current.get("quantity") == 0 and current.get("owned_working_orders") == 0):
                return {"foreign_activity": "PASS", "reconciliation": dict(current), "live_armed": False}
        time.sleep(0.1)
    raise RuntimeError("Timed out waiting for foreign Sim101 activity to quarantine and flatten natively.")


def main() -> int:
    parser = argparse.ArgumentParser(description="Run one safe installed-Sim101 L3H commissioning stage.")
    parser.add_argument("stage", choices=("probe", "restart-proof", "negative", "long-kill-command", "unknown-transport", "reconnect", "short-await-menu-kill", "long-await-script-kill", "foreign-await"))
    parser.add_argument("--authority-root")
    args = parser.parse_args()
    root = _authority_root(args.authority_root)
    capability = _load(root)
    gateway = _gateway(root, capability)
    heartbeat_stop = threading.Event()
    heartbeat = threading.Thread(target=_heartbeat_loop, args=(gateway, heartbeat_stop), daemon=True)
    heartbeat.start()
    try:
        if args.stage == "probe":
            result = _probe(root, capability, gateway)
        elif args.stage == "restart-proof":
            result = {**_probe(root, capability, gateway), "restart": "PASS"}
        elif args.stage == "negative":
            result = _negative(root, capability, gateway)
        elif args.stage == "long-kill-command":
            result = {"long": _entry(capability, gateway, "LONG"), "command_kill": _kill(capability, gateway)}
        elif args.stage == "unknown-transport":
            result = _unknown_transport(root, capability, gateway)
        elif args.stage == "reconnect":
            result = _reconnect(root, capability, gateway)
        elif args.stage == "long-await-script-kill":
            result = _await_script_kill(capability, gateway)
        elif args.stage == "foreign-await":
            result = _await_foreign_activity(capability, gateway)
        else:
            result = _await_native_menu_kill(capability, gateway, "SHORT")
        _record(root, args.stage, result)
        print(json.dumps({"stage": args.stage, "result": result, "live_capital": "DENIED", "live_armed": False}, sort_keys=True))
        return 0
    finally:
        heartbeat_stop.set()
        heartbeat.join(timeout=2)
        gateway.stop()


if __name__ == "__main__":
    raise SystemExit(main())
