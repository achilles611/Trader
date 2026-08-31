"""Sanitized L3H mechanical readiness projection for BeezConsole."""

from __future__ import annotations

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
) -> dict[str, object]:
    """Build an honest, non-authoritative status payload.

    This is safe to call from a web GET handler: it does not open sockets,
    read local keys, create a capability, or create a live runtime.
    """

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
