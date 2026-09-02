"""Compact, fail-closed Lane III-G presentation state for BeezConsole Slim Mode.

This module is deliberately a presentation adapter over runtime, verifier, and
observer facts.  It does not grant authority, start verification, or issue a
paper command.  The browser receives its light and start eligibility from this
one backend projection rather than reconstructing safety gates client-side.
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Mapping


SLIM_STATUS_SCHEMA = "lane-iii-phase-g-slim-status-v1"
_ACCEPTED_LEDGER_STATES = frozenset({
    "VERIFIED_TO_CURRENT_TIP",
    "VERIFIED_TO_ARM_SNAPSHOT_TIP",
    "VERIFIED_ANCHOR_WITH_PASSIVE_LIVE_TAIL",
    "VERIFIED_ANCHOR_WITH_ACCEPTED_LIVE_TAIL",
})
_TRANSIENT_READINESS_REASONS = frozenset({
    "COMMISSIONING_LEDGER_VERIFICATION_IN_PROGRESS",
    "COMMISSIONING_SESSION_NOT_WARMED",
    "PAPER_EVIDENCE_NOT_WARMED",
})
_TRANSIENT_RUNTIME_STATES = frozenset({
    "STARTING",
    "RECONCILING",
    "ENTRY_PENDING",
    "EXIT_PENDING",
    "STOPPING",
})

_BLOCKER_MESSAGES = {
    "MARKET_OBSERVER_NOT_ACTIVE": "Attach and warm the MNQ market observer.",
    "COMMISSIONING_LEDGER_VERIFICATION_REQUIRED": "Run a current ledger verification.",
    "OPERATIONAL_FULL_LEDGER_VERIFICATION_REQUIRED": "Run a successful Full ledger verification before paper start.",
    "COMMISSIONING_LEDGER_VERIFICATION_IN_PROGRESS": "Ledger verification is still running.",
    "COMMISSIONING_LEDGER_VERIFICATION_FAILED": "Ledger verification failed; review Full Console diagnostics.",
    "COMMISSIONING_LEDGER_ANCHOR_STALE": "Ledger verification is stale; run it again.",
    "COMMISSIONING_LEDGER_CAPACITY_INADEQUATE": "Ledger writer capacity is not healthy.",
    "ADDON_BUILD_MISMATCH": "Installed paper AddOn provenance is not current.",
    "COMMISSIONING_SESSION_NOT_WARMED": "Warming current-session observation evidence.",
    "PAPER_EVIDENCE_NOT_WARMED": "Warming current-session observation evidence.",
    "STATE_NOT_READY_DISARMED": "Paper runtime is not safely disarmed.",
    "COMMISSIONING_READINESS_SNAPSHOT_STALE": "Runtime changed during the last readiness check.",
    "NO_CURRENT_EVENT_SESSION": "No eligible paper session is currently active.",
    "RECONCILIATION_INCOMPLETE": "Waiting for a current Sim101 reconciliation.",
    "MARKET_BRIDGE_UNHEALTHY": "Market-data bridge is not healthy.",
    "EXECUTION_BRIDGE_UNHEALTHY": "Paper execution bridge is not healthy.",
    "QUOTE_STALE": "Market quote data is stale.",
    "CLASSIFIED_TRADE_STALE": "Classified trade data is stale.",
    "DEPTH_MUTATION_STALE": "Market depth data is stale.",
    "FOREIGN_ACTIVITY_LOCKOUT": "Paper runtime is locked by foreign activity.",
    "PROTECTIVE_STOP_REJECTED": "Protective-stop health is not confirmed.",
    "PENDING_ORDER_LOCKOUT": "Owned working orders must be reconciled first.",
    "COMMISSIONING_OWNERSHIP_ACTIVE": "A paper operation is already in progress.",
    "PAPER_SESSION_PNL_UNAVAILABLE": "Current paper-session P&L is unavailable.",
    "OPERATIONAL_LEDGER_INTEGRITY_FAILED": "Online ledger integrity is not confirmed; stop paper trading.",
}


def _timestamp(now: datetime | None = None) -> str:
    value = now or datetime.now(timezone.utc)
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _message(reason: str | None, fallback: str) -> str:
    if reason is None:
        return fallback
    return _BLOCKER_MESSAGES.get(reason, fallback)


def _as_mapping(value: object) -> Mapping[str, object]:
    return value if isinstance(value, Mapping) else {}


def _verification_is_current(
    verification: Mapping[str, object], *, now: datetime, freshness_seconds: int,
) -> bool:
    if (
        verification.get("status") != "PASS"
        or verification.get("chain_valid") is not True
        or verification.get("checkpoint_valid") is not True
        or verification.get("full_scan_required") is True
        or verification.get("quick_check") not in {"ok", "inherited_from_full"}
    ):
        return False
    completed = verification.get("completed_at")
    if not isinstance(completed, str):
        return False
    try:
        at = datetime.fromisoformat(completed.replace("Z", "+00:00"))
    except ValueError:
        return False
    if at.tzinfo is None:
        return False
    age = (now.astimezone(timezone.utc) - at.astimezone(timezone.utc)).total_seconds()
    return 0 <= age <= freshness_seconds


def _active_blockers(
    runtime: Mapping[str, object],
    verification: Mapping[str, object],
    observer: Mapping[str, object],
    *,
    now: datetime,
    verification_freshness_seconds: int,
) -> tuple[str, ...]:
    """Return exact backend facts that make an active paper operation unsafe.

    This is intentionally stricter than a visual "positioned" check.  It only
    certifies the compact active-green presentation when the runtime exposes a
    complete, healthy Sim101 position and protection state.
    """
    blockers: list[str] = []
    expected = {
        "mode": "PAPER_SIM101",
        "paper_account": "Sim101",
        "account_class": "LOCAL_SIMULATION",
        "market_instrument": "MNQ SEP26",
        "maximum_quantity": 1,
        "live_capital": "DENIED",
    }
    for field, value in expected.items():
        if runtime.get(field) != value:
            blockers.append("ACTIVE_RUNTIME_IDENTITY_UNHEALTHY")
            break

    state = runtime.get("state")
    position = runtime.get("current_position")
    positioned = state in {"LONG", "SHORT"}
    if positioned:
        if runtime.get("paper_execution") != "POSITIONED" or position != state or runtime.get("current_quantity") != 1:
            blockers.append("ACTIVE_POSITION_UNHEALTHY")
        if runtime.get("current_position_quantity") != 1 or runtime.get("broker_snapshot_position") != state:
            blockers.append("ACTIVE_POSITION_UNHEALTHY")
        if runtime.get("broker_snapshot_position_quantity") != 1:
            blockers.append("ACTIVE_POSITION_UNHEALTHY")
        if runtime.get("protective_stop_state") != "WORKING":
            blockers.append("PROTECTIVE_STOP_REJECTED")
    else:
        if (
            state != "PAPER_RUNNING"
            or runtime.get("paper_execution") != "RUNNING"
            or position != "FLAT"
            or runtime.get("current_quantity") != 0
            or runtime.get("current_position_quantity") != 0
            or runtime.get("broker_snapshot_position") != "FLAT"
            or runtime.get("broker_snapshot_position_quantity") != 0
            or runtime.get("protective_stop_state") not in {"NONE", "CANCELLED"}
        ):
            blockers.append("ACTIVE_POSITION_UNHEALTHY")
    if runtime.get("working_entry_orders") != 0:
        blockers.append("ACTIVE_WORKING_ENTRY_ORDER")
    if runtime.get("lockout_or_fault_reason") not in (None, ""):
        blockers.append("ACTIVE_LOCKOUT_OR_FAULT")
    if any(runtime.get(name) is not expected for name, expected in {
        "position_snapshot_complete": True,
        "order_snapshot_complete": True,
        "reconciliation_current": True,
        "unresolved_command": False,
        "unresolved_native_order": False,
        "unresolved_execution": False,
    }.items()):
        blockers.append("RECONCILIATION_INCOMPLETE")

    continuity = _as_mapping(runtime.get("continuity"))
    if any(continuity.get(name) is not True for name in (
        "healthy", "local_bridge_healthy", "market_price_connected",
    )):
        blockers.append("MARKET_BRIDGE_UNHEALTHY")
    freshness = _as_mapping(runtime.get("market_freshness"))
    if any(_as_mapping(freshness.get(name)).get("fresh") is not True for name in (
        "quote", "classified_trade", "depth_mutation",
    )):
        blockers.append("QUOTE_STALE")

    transport = _as_mapping(runtime.get("transport"))
    if (
        transport.get("state") != "AUTHENTICATED"
        or transport.get("authenticated_client") is not True
        or transport.get("reconciled") is not True
        or _as_mapping(transport.get("addon_provenance")).get("status") != "MATCH"
    ):
        blockers.append("EXECUTION_BRIDGE_UNHEALTHY")
    if observer.get("market_observer_active") is not True or observer.get("market_observer_state") != "ACTIVE":
        blockers.append("MARKET_OBSERVER_NOT_ACTIVE")

    ledger = _as_mapping(runtime.get("ledger"))
    operational = _as_mapping(runtime.get("operational_paper_session"))
    if operational.get("active") is True:
        online = _as_mapping(ledger.get("operational_ledger"))
        if online.get("online_append_integrity") is not True:
            blockers.append("OPERATIONAL_LEDGER_INTEGRITY_FAILED")
        if verification.get("status") == "FAIL":
            blockers.append("COMMISSIONING_LEDGER_VERIFICATION_FAILED")
    else:
        if ledger.get("commissioning_ledger_state") not in _ACCEPTED_LEDGER_STATES or ledger.get("writer_capacity_healthy") is not True:
            blockers.append("COMMISSIONING_LEDGER_CAPACITY_INADEQUATE")
        if not _verification_is_current(
            verification, now=now, freshness_seconds=verification_freshness_seconds,
        ):
            blockers.append("COMMISSIONING_LEDGER_VERIFICATION_REQUIRED")
    return tuple(dict.fromkeys(blockers))


def _verification_message(
    verification: Mapping[str, object], *, now: datetime, freshness_seconds: int,
) -> str:
    status = str(verification.get("status") or "UNVERIFIED")
    if status == "IN_PROGRESS":
        return "Verifying…"
    if _verification_is_current(verification, now=now, freshness_seconds=freshness_seconds):
        return "Verified"
    if status == "FAIL":
        errors = verification.get("errors")
        if isinstance(errors, list) and errors:
            first = _as_mapping(errors[0]).get("message")
            if isinstance(first, str) and first.strip():
                return "Verification failed: " + first.strip()[:160]
        return "Verification failed"
    if status == "PASS":
        return "Verification stale"
    return "Verification required"


def paper_session_pnl(runtime: Mapping[str, object]) -> dict[str, object]:
    """Expose only reconciled current-session P&L; never synthesize zero."""
    values = _as_mapping(runtime.get("paper_session_pnl"))
    if runtime.get("reconciliation_current") is not True:
        return {"state": "STALE", "total": None, "realized": None, "unrealized": None}
    try:
        realized = Decimal(str(values["realized"]))
        unrealized = Decimal(str(values["unrealized"]))
    except (KeyError, TypeError, ValueError, InvalidOperation):
        return {"state": "MISSING", "total": None, "realized": None, "unrealized": None}
    if not realized.is_finite() or not unrealized.is_finite():
        return {"state": "MISSING", "total": None, "realized": None, "unrealized": None}
    return {
        "state": "CURRENT",
        "total": str(realized + unrealized),
        "realized": str(realized),
        "unrealized": str(unrealized),
    }


def unavailable_slim_status(reason: str = "PAPER_RUNTIME_UNAVAILABLE") -> dict[str, object]:
    return {
        "schema": SLIM_STATUS_SCHEMA,
        "generated_at": _timestamp(),
        "light": "RED",
        "label": "NOT READY",
        "message": "Paper runtime status is unavailable.",
        "primary_blocker": reason,
        "can_start": False,
        "paper_active": False,
        "session": None,
        "ledger_verification": {"state": "UNAVAILABLE", "completed_at": None, "message": "Verification status is unavailable."},
        "pnl": {"state": "MISSING", "total": None, "realized": None, "unrealized": None},
    }


def derive_slim_paper_status(
    runtime: Mapping[str, object] | None,
    verification: Mapping[str, object] | None,
    observer: Mapping[str, object] | None,
    readiness: Mapping[str, object] | None,
    *,
    now: datetime | None = None,
    verification_freshness_seconds: int = 15 * 60,
) -> dict[str, object]:
    """Produce the sole Slim Mode readiness/display decision.

    A green pre-start result is delegated to the existing canonical
    commissioning rehearsal.  A green active result is granted only by the
    exact backend health facts above.  Unknown or incomplete inputs are red.
    """
    if runtime is None:
        return unavailable_slim_status()
    session = {
        "session_kind": runtime.get("current_session", "OFF_SESSION"),
        "session_family": runtime.get("current_session_family", "OFF_SESSION"),
        "session_id": runtime.get("current_session_id"),
        "trade_date": runtime.get("trade_date"),
        "timezone": runtime.get("session_timezone", "America/New_York"),
        "entry_window": runtime.get("entry_window"),
        "session_generation": runtime.get("session_generation"),
    }

    def finalize(payload: dict[str, object]) -> dict[str, object]:
        payload["session"] = session
        return payload
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    verification_value = verification or {}
    observer_value = observer or {}
    readiness_value = readiness or {}
    verification_status = str(verification_value.get("status") or "UNVERIFIED")
    verification_message = _verification_message(
        verification_value, now=current, freshness_seconds=verification_freshness_seconds,
    )
    verification_payload = {
        "state": verification_status,
        "completed_at": verification_value.get("completed_at") if verification_status == "PASS" else None,
        "message": verification_message,
    }
    pnl = paper_session_pnl(runtime)

    state = str(runtime.get("state") or "UNKNOWN")
    operational = _as_mapping(runtime.get("operational_paper_session"))
    active_state = (
        operational.get("active") is True
        or state in {"PAPER_RUNNING", "ENTRY_PENDING", "LONG", "SHORT", "EXIT_PENDING", "STOPPING"}
    )
    if active_state:
        stopping = operational.get("stopping") is True or state == "STOPPING"
        if stopping:
            return finalize({
                "schema": SLIM_STATUS_SCHEMA,
                "generated_at": _timestamp(current),
                "light": "YELLOW",
                "label": "STOPPING…",
                "message": "Cancelling owned work and waiting for Sim101 flat reconciliation.",
                "primary_blocker": None,
                "can_start": False,
                "paper_active": True,
                "ledger_verification": verification_payload,
                "pnl": pnl,
            })
        if state in {"ENTRY_PENDING", "EXIT_PENDING", "STARTING"}:
            return finalize({
                "schema": SLIM_STATUS_SCHEMA,
                "generated_at": _timestamp(current),
                "light": "YELLOW",
                "label": "PAPER TRADING ACTIVE",
                "message": "Waiting for the current Sim101 order lifecycle to settle.",
                "primary_blocker": None,
                "can_start": False,
                "paper_active": True,
                "ledger_verification": verification_payload,
                "pnl": pnl,
            })
        blockers = _active_blockers(
            runtime, verification_value, observer_value, now=current,
            verification_freshness_seconds=verification_freshness_seconds,
        )
        if pnl["state"] != "CURRENT":
            blockers = tuple(dict.fromkeys((*blockers, "PAPER_SESSION_PNL_UNAVAILABLE")))
        if not blockers:
            return finalize({
                "schema": SLIM_STATUS_SCHEMA,
                "generated_at": _timestamp(current),
                "light": "GREEN",
                "label": "PAPER TRADING ACTIVE",
                "message": "Sim101 paper operation is healthy and protected.",
                "primary_blocker": None,
                "can_start": False,
                "paper_active": True,
                "ledger_verification": verification_payload,
                "pnl": pnl,
            })
        return finalize({
            "schema": SLIM_STATUS_SCHEMA,
            "generated_at": _timestamp(current),
            "light": "RED",
            "label": "PAPER OPERATION NEEDS ATTENTION",
            "message": _message(blockers[0], "Active paper health is not fully confirmed."),
            "primary_blocker": blockers[0],
            "can_start": False,
            "paper_active": True,
            "ledger_verification": verification_payload,
            "pnl": pnl,
        })

    reasons = tuple(str(value) for value in readiness_value.get("blocking_reasons", []) if isinstance(value, str))
    # A compact operator surface must not show a green start state while its
    # required current-session P&L observation is missing or stale.  The
    # canonical runtime normally provides a zero-valued, reconciled session
    # baseline, so this only fails closed on unavailable evidence.
    if readiness_value.get("result") == "READY" and pnl["state"] != "CURRENT":
        return finalize({
            "schema": SLIM_STATUS_SCHEMA,
            "generated_at": _timestamp(current),
            "light": "RED",
            "label": "NOT READY",
            "message": "Current paper-session P&L is unavailable.",
            "primary_blocker": "PAPER_SESSION_PNL_UNAVAILABLE",
            "can_start": False,
            "paper_active": False,
            "ledger_verification": verification_payload,
            "pnl": pnl,
        })
    if readiness_value.get("result") == "READY":
        return finalize({
            "schema": SLIM_STATUS_SCHEMA,
            "generated_at": _timestamp(current),
            "light": "GREEN",
            "label": "READY TO START PAPER TRADING",
            "message": "All canonical paper-start gates are currently satisfied.",
            "primary_blocker": None,
            "can_start": True,
            "paper_active": False,
            "ledger_verification": verification_payload,
            "pnl": pnl,
        })

    waiting_for_bridge = state == "WAITING_FOR_EXECUTION_BRIDGE" and _as_mapping(runtime.get("transport")).get("state") in {
        "NEW", "LISTENING", "CONNECTED", "AUTHENTICATED",
    }
    state_transition_reasons = set(_TRANSIENT_READINESS_REASONS)
    if state in _TRANSIENT_RUNTIME_STATES or waiting_for_bridge:
        # The rehearsal is intentionally a pre-start proof, so a known state
        # transition reports READY_DISARMED as a blocker.  That is transient
        # only while no independent unsafe fact is present.
        state_transition_reasons.add("STATE_NOT_READY_DISARMED")
    transient = (
        (state in _TRANSIENT_RUNTIME_STATES or waiting_for_bridge)
        and (not reasons or set(reasons).issubset(state_transition_reasons))
    ) or (reasons and set(reasons).issubset(_TRANSIENT_READINESS_REASONS))
    if transient:
        message = (
            "Ledger verification is running."
            if verification_status == "IN_PROGRESS"
            else _message(reasons[0] if reasons else None, "Preparing canonical paper runtime state.")
        )
        return finalize({
            "schema": SLIM_STATUS_SCHEMA,
            "generated_at": _timestamp(current),
            "light": "YELLOW",
            "label": "PREPARING",
            "message": message,
            "primary_blocker": reasons[0] if reasons else None,
            "can_start": False,
            "paper_active": False,
            "ledger_verification": verification_payload,
            "pnl": pnl,
        })

    reason = reasons[0] if reasons else "PAPER_RUNTIME_NOT_READY"
    return finalize({
        "schema": SLIM_STATUS_SCHEMA,
        "generated_at": _timestamp(current),
        "light": "RED",
        "label": "NOT READY",
        "message": _message(reason, "Canonical paper readiness is not confirmed."),
        "primary_blocker": reason,
        "can_start": False,
        "paper_active": False,
        "ledger_verification": verification_payload,
        "pnl": pnl,
    })
