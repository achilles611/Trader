"""Sanitized L3H mechanical readiness projection for BeezConsole."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Mapping

from .contracts import LiveCapability
from .gateway import PROTOCOL_VERSION


_REQUIRED_COMPONENTS = (
    "ACCOUNT", "ACCOUNT_CLASS", "CONTRACT", "SESSION", "MARKET_DATA", "ACCOUNT_TRUTH",
    "POSITION_TRUTH", "ORDER_TRUTH", "EXECUTION_GATEWAY", "NT_RISK_GUARD", "PROTECTION",
    "RECONCILIATION", "LEDGER", "CAPABILITY", "KILL_PATHS", "DISK", "STRATEGY", "LIVE_AUTHORITY",
)


def fail_closed_status(
    capability: LiveCapability | None = None, *, blockers: Mapping[str, str] | None = None,
    mechanical_status_path: Path | None = None,
) -> dict[str, object]:
    """Build an honest, non-authoritative status payload.

    This is safe to call from a web GET handler: it does not open sockets,
    read local keys, create a capability, or create a live runtime.
    """

    commissioned = _mechanically_commissioned(mechanical_status_path)
    if commissioned is not None:
        account_class, reconciliation = commissioned
        values = {component: {"state": "GREEN", "reason": "SIM101_MECHANICAL_PROOF"} for component in _REQUIRED_COMPONENTS}
        values["ACCOUNT_CLASS"] = {"state": "GREEN", "reason": account_class}
        values["LIVE_AUTHORITY"] = {"state": "RED", "reason": "DISARMED_SIM101_ONLY"}
        return {
            "schema": "lane-iii-phase-h-live-status-v2", "mode": "L3H_LIVE_CAPITAL", "state": "MECHANICALLY_COMMISSIONED",
            "terminal_status": "L3H_MECHANICALLY_COMMISSIONED", "account_alias": "Sim101", "account_class": account_class,
            "live_capital": "DENIED", "contract": "MNQ SEP26", "maximum_quantity": 1, "canary_limit": 1,
            "gateway_protocol": PROTOCOL_VERSION, "components": values, "native_reconciliation": reconciliation,
            "one_control_start": {
                "label": "START LIVE — 1 MNQ CANARY", "enabled": False,
                "reason": "L3H_3_CAPITAL_BEARING_AUTHORIZATION_REQUIRED",
            },
            "emergency_control": {
                "label": "FLATTEN MNQ & DISARM", "enabled": False,
                "reason": "SIM101_MECHANICAL_GATEWAY_IS_DISARMED",
            },
            "authority": "DISARMED_SIM101_ONLY",
        }

    reason = "LOCAL_SIGNED_CAPABILITY_REQUIRED" if capability is None else "INSTALLED_L3H_COMMISSIONING_REQUIRED"
    values: dict[str, dict[str, str]] = {
        component: {"state": "RED", "reason": reason} for component in _REQUIRED_COMPONENTS
    }
    values["CONTRACT"] = {"state": "YELLOW", "reason": "MNQ SEP26_SOURCE_BOUND_NOT_INSTALLED"}
    values["LEDGER"] = {"state": "YELLOW", "reason": "SEPARATE_L3H_EVENT_LEDGER_IMPLEMENTED"}
    values["LIVE_AUTHORITY"] = {"state": "RED", "reason": "DISARMED_FAIL_CLOSED"}
    if capability is not None:
        values["CAPABILITY"] = {"state": "YELLOW", "reason": "CAPABILITY_PRESENT_REQUIRES_RUNTIME_VERIFICATION"}
        values["ACCOUNT"] = {"state": "YELLOW", "reason": "CAPABILITY_BOUND_NOT_NATIVE_RECONCILED"}
        values["ACCOUNT_CLASS"] = {"state": "YELLOW", "reason": capability.account_class.value}
    for name, value in (blockers or {}).items():
        if name in values:
            values[name] = {"state": "RED", "reason": value}
    return {
        "schema": "lane-iii-phase-h-live-status-v2", "mode": "L3H_LIVE_CAPITAL", "state": "BLOCKED",
        "terminal_status": "BLOCKED_CAPABILITY_MISSING" if capability is None else "BLOCKED_SIM101_COMMISSIONING",
        "account_alias": None if capability is None else capability.account_alias,
        "account_class": "UNKNOWN" if capability is None else capability.account_class.value,
        "live_capital": "DENIED" if capability is None or not capability.live_capital else "DISARMED_PENDING_PROOF",
        "contract": "MNQ SEP26", "maximum_quantity": 1, "canary_limit": 1,
        "gateway_protocol": PROTOCOL_VERSION, "components": values,
        "one_control_start": {
            "label": "START LIVE — 1 MNQ CANARY", "enabled": False,
            "reason": "SIM101_MATRIX_AND_INSTALLED_ADDON_PROVENANCE_REQUIRED",
        },
        "emergency_control": {
            "label": "FLATTEN MNQ & DISARM", "enabled": False,
            "reason": "NATIVE_KILL_PATH_NOT_INSTALLED_OR_VERIFIED",
        },
        "authority": "DISARMED_FAIL_CLOSED",
    }


def _mechanically_commissioned(path: Path | None) -> tuple[str, Mapping[str, object]] | None:
    """Accept only a complete, current local Sim101 mechanical proof bundle.

    This reads no key or capability material.  Any missing, malformed, stale,
    non-loopback, armed, non-flat, or incomplete evidence remains fail-closed.
    """

    if path is None:
        return None
    try:
        gateway = json.loads(path.read_text(encoding="utf-8"))
        results = json.loads(path.with_name("l3h-sim101-mechanical-results.json").read_text(encoding="utf-8"))
        stages = results["stages"]
        reconciliation = gateway["reconciliation"]
        negative = stages["negative"]
        long_run = stages["long-kill-command"]
        required = (
            gateway["schema"] == "l3h-gateway-status-v1",
            gateway["account_class"] == "LOCAL_SIMULATION",
            gateway["live_capital"] == "DENIED",
            gateway["live_armed"] is False,
            gateway["gateway"]["state"] == "AUTHENTICATED",
            gateway["gateway"]["loopback_only"] is True,
            gateway["gateway"]["port"] == 48137,
            reconciliation["account"] == "Sim101", reconciliation["contract"] == "MNQ SEP26",
            reconciliation["position"] == "FLAT", reconciliation["quantity"] == 0,
            reconciliation["owned_working_orders"] == 0, reconciliation["foreign_or_unknown_orders"] == 0,
            stages["probe"]["runtime_hello"] == "PASS", stages["probe"]["gateway_auth"] == "PASS",
            stages["restart-proof"]["restart"] == "PASS", stages["reconnect"]["reconnect"] == "PASS",
            negative["bad_signature"]["reason"] == "DENY_BAD_SIGNATURE",
            negative["replay"]["reason"] == "DENY_REPLAY", negative["duplicate"]["reason"] == "DUPLICATE_COMMAND_NOOP",
            negative["wrong_contract"]["reason"] == "DENY_WRONG_CONTRACT", negative["qty_2_reject"]["reason"] == "DENY_QTY",
            long_run["long"]["protection"] == "PASS", long_run["command_kill"]["reconciliation"]["owned_working_orders"] == 0,
            stages["short-await-menu-kill"]["native_menu_kill"] == "PASS",
            stages["long-await-script-kill"]["script_kill"] == "PASS",
            stages["unknown-transport"]["unknown_state"] == "PASS",
            stages["foreign-await"]["foreign_activity"] == "PASS",
        )
        if not all(required):
            return None
        return str(gateway["account_class"]), dict(reconciliation)
    except (KeyError, OSError, TypeError, ValueError, json.JSONDecodeError):
        return None
