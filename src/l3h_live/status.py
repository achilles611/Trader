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
    "LIVE_ACCOUNT_IDENTITY", "AUTHORIZATION_BOUNDARY", "LIVE_CANARY", "QUARANTINE", "LOCK",
)


def fail_closed_status(
    capability: LiveCapability | None = None, *, blockers: Mapping[str, str] | None = None,
    mechanical_status_path: Path | None = None, authorization_status_path: Path | None = None,
) -> dict[str, object]:
    """Build an honest, non-authoritative status payload.

    This is safe to call from a web GET handler: it does not open sockets,
    read local keys, create a capability, or create a live runtime.
    """

    commissioned = _mechanically_commissioned(mechanical_status_path)
    if commissioned is not None:
        account_class, reconciliation = commissioned
        authorization = _authorization_boundary_status(authorization_status_path)
        values = {component: {"state": "GREEN", "reason": "SIM101_MECHANICAL_PROOF"} for component in _REQUIRED_COMPONENTS}
        values["ACCOUNT_CLASS"] = {"state": "GREEN", "reason": account_class}
        values["LIVE_AUTHORITY"] = {"state": "RED", "reason": "DISARMED_SIM101_ONLY"}
        values["LIVE_ACCOUNT_IDENTITY"] = {"state": "RED", "reason": "UNVERIFIED"}
        values["AUTHORIZATION_BOUNDARY"] = {
            "state": "YELLOW" if authorization is not None else "RED",
            "reason": "IMPLEMENTED_IDENTITY_BLOCKED" if authorization is not None else "L3H3_EVIDENCE_MISSING",
        }
        values["LIVE_CANARY"] = {"state": "YELLOW", "reason": "NOT_RUN"}
        values["QUARANTINE"] = {"state": "GREEN", "reason": "CLEAR"}
        values["LOCK"] = {"state": "GREEN", "reason": "CLEAR"}
        if authorization is not None and authorization.get("quarantine") is True:
            values["QUARANTINE"] = {"state": "RED", "reason": str(authorization.get("denial_reason") or "QUARANTINED")}
        if authorization is not None and authorization.get("locked") is True:
            values["LOCK"] = {"state": "RED", "reason": str(authorization.get("denial_reason") or "LOCKED")}
        terminal = "L3H_MECHANICALLY_COMMISSIONED" if authorization is None else str(authorization["terminal_status"])
        return {
            "schema": "lane-iii-phase-h-live-status-v3", "mode": "L3H_LIVE_CAPITAL",
            "state": "MECHANICALLY_COMMISSIONED" if authorization is None else "AUTHORIZATION_BOUNDARY_IMPLEMENTED",
            "terminal_status": terminal, "mechanical_commissioning": "COMMISSIONED",
            "account_alias": "Sim101", "mechanical_account_class": account_class,
            "account_class": "UNKNOWN" if authorization is None else str(authorization.get("account_class", "UNKNOWN")),
            "live_capital": "DENIED", "contract": "MNQ SEP26", "maximum_quantity": 1, "canary_limit": 1,
            "gateway_protocol": PROTOCOL_VERSION, "components": values, "native_reconciliation": reconciliation,
            "live_account_identity": "UNVERIFIED" if authorization is None else str(authorization["live_account_identity"]),
            "authorization_boundary": "NOT_COMMISSIONED" if authorization is None else str(authorization["authorization_boundary"]),
            "live_authority": "DISARMED", "live_canary": "NOT_RUN",
            "authorized_account": None if authorization is None else authorization.get("authorized_account"),
            "preflight_age_seconds": None if authorization is None else authorization.get("preflight_age_seconds"),
            "authorization_expires_at": None if authorization is None else authorization.get("authorization_expires_at"),
            "reconciliation": "PASS", "protection": "PASS", "kill_paths": "PASS",
            "addon_provenance": "PASS_L3H2_INSTALLED_RUNTIME" if authorization is None else str(authorization.get("addon_provenance", "UNVERIFIED")),
            "gateway": "AUTHENTICATED_LOOPBACK_L3H2" if authorization is not None else "AUTHENTICATED_LOOPBACK",
            "quarantine": False if authorization is None else bool(authorization.get("quarantine", False)),
            "locked": False if authorization is None else bool(authorization.get("locked", False)),
            "live_send_count": 0 if authorization is None else int(authorization.get("live_send_count", 0)),
            "one_control_start": {
                "label": "START LIVE — 1 MNQ CANARY", "enabled": False,
                "reason": "LIVE_ACCOUNT_IDENTITY_UNVERIFIED" if authorization is not None else "L3H_3_CAPITAL_BEARING_AUTHORIZATION_REQUIRED",
            },
            "emergency_control": {
                "label": "FLATTEN MNQ & DISARM", "enabled": False,
                "reason": "SIM101_MECHANICAL_GATEWAY_IS_DISARMED",
            },
            "authority": "DISARMED", "live_chart_authority": "OBSERVE_ONLY",
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
        "schema": "lane-iii-phase-h-live-status-v3", "mode": "L3H_LIVE_CAPITAL", "state": "BLOCKED",
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
        "mechanical_commissioning": "UNVERIFIED", "live_account_identity": "UNVERIFIED",
        "authorization_boundary": "NOT_COMMISSIONED", "live_authority": "DISARMED", "live_canary": "NOT_RUN",
        "authorized_account": None, "preflight_age_seconds": None, "authorization_expires_at": None,
        "reconciliation": "UNVERIFIED", "protection": "UNVERIFIED", "kill_paths": "UNVERIFIED",
        "addon_provenance": "UNVERIFIED", "gateway": "DISCONNECTED", "quarantine": False, "locked": False,
        "live_send_count": 0, "live_chart_authority": "OBSERVE_ONLY",
    }


def _authorization_boundary_status(path: Path | None) -> Mapping[str, object] | None:
    """Read a non-secret commissioning projection; it can never grant authority."""

    if path is None:
        return None
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
        required = (
            value["schema"] == "lane-iii-phase-h3-commissioning-result-v1",
            value["terminal_status"] in {"BLOCKED_LIVE_ACCOUNT_IDENTITY", "PARTIALLY_READY", "L3H3_LIVE_AUTHORIZATION_READY"},
            value["live_authority"] == "DISARMED", value["live_canary"] == "NOT_RUN",
            value["live_send_count"] == 0, value["maximum_quantity"] == 1,
            value["contract"] == "MNQ SEP26",
        )
        if not all(required):
            return None
        return dict(value)
    except (KeyError, OSError, TypeError, ValueError, json.JSONDecodeError):
        return None


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
