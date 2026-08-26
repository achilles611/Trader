"""Commissioning-only trust gate for a verified anchor plus passive live tail."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Mapping

from .ledger import COMMISSIONING_TAIL_POLICY_VERSION


class CommissioningLedgerGateError(RuntimeError):
    def __init__(self, code: str, message: str, *, launch_auto: bool = False) -> None:
        super().__init__(message)
        self.code = code
        self.launch_auto = launch_auto


def _gate_error(code: str, message: str, *, launch_auto: bool = False) -> None:
    raise CommissioningLedgerGateError(code, message, launch_auto=launch_auto)


def _runtime_reconciled(runtime: Mapping[str, object]) -> bool:
    transport = runtime.get("transport")
    return (
        runtime.get("account") == "Sim101"
        and runtime.get("account_class") == "LOCAL_SIMULATION"
        and runtime.get("instrument") == "MNQ SEP26"
        and runtime.get("current_position") == "FLAT"
        and runtime.get("current_position_quantity") == 0
        and runtime.get("broker_snapshot_position") == "FLAT"
        and runtime.get("broker_snapshot_position_quantity") == 0
        and runtime.get("working_owned_orders") == 0
        and runtime.get("working_entry_orders") == 0
        and runtime.get("position_snapshot_complete") is True
        and runtime.get("order_snapshot_complete") is True
        and runtime.get("reconciliation_current") is True
        and runtime.get("unresolved_command") is False
        and runtime.get("unresolved_native_order") is False
        and runtime.get("unresolved_execution") is False
        and runtime.get("entry_owner") == "NONE"
        and runtime.get("commissioning_ownership_active") is False
        and runtime.get("live_capital") == "DENIED"
        and runtime.get("runtime_state") == "READY_DISARMED"
        and isinstance(transport, Mapping)
        and transport.get("reconciled") is True
        and transport.get("account") == "Sim101"
        and transport.get("account_class") == "LOCAL_SIMULATION"
        and transport.get("instrument") == "MNQ SEP26"
        and transport.get("live_capital") == "DENIED"
    )


def evaluate_commissioning_ledger_gate(
    verification: Mapping[str, object],
    tail: Mapping[str, object],
    runtime: Mapping[str, object],
    *,
    checkpoint_matches_report: bool,
    freshness_seconds: int,
    now: datetime | None = None,
) -> dict[str, object]:
    """Return immutable ARM audit evidence or fail with a stable blocker code."""
    status = str(verification.get("status") or "UNVERIFIED")
    if status == "IN_PROGRESS":
        _gate_error("COMMISSIONING_LEDGER_VERIFICATION_IN_PROGRESS", "Local ledger verification is in progress.")
    if status == "FAIL":
        _gate_error("COMMISSIONING_LEDGER_VERIFICATION_FAILED", "Latest local ledger verification failed.")
    if status != "PASS":
        _gate_error(
            "COMMISSIONING_LEDGER_VERIFICATION_REQUIRED",
            "A trusted local ledger verification is required.",
            launch_auto=True,
        )
    if (
        verification.get("chain_valid") is not True
        or verification.get("checkpoint_valid") is not True
        or verification.get("full_scan_required") is True
        or not checkpoint_matches_report
    ):
        _gate_error("COMMISSIONING_LEDGER_ANCHOR_UNTRUSTED", "Latest local ledger PASS is not a trusted checkpoint.")
    if verification.get("quick_check") not in {"ok", "inherited_from_full"}:
        _gate_error("COMMISSIONING_LEDGER_FULL_PROVENANCE_INVALID", "Full quick-check provenance is invalid.")

    completed = verification.get("completed_at")
    try:
        completed_at = datetime.fromisoformat(str(completed).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        _gate_error("COMMISSIONING_LEDGER_ANCHOR_TIME_INVALID", "Latest verification has no valid completion time.")
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    age = (current.astimezone(timezone.utc) - completed_at.astimezone(timezone.utc)).total_seconds()
    if age < 0 or age > freshness_seconds:
        _gate_error(
            "COMMISSIONING_LEDGER_ANCHOR_STALE",
            "Latest trusted verification is outside the commissioning freshness bound.",
            launch_auto=True,
        )

    verified = verification.get("verified_through_sequence")
    verified_hash = verification.get("tip_hash")
    full_sequence = verification.get("last_full_verified_sequence")
    full_hash = verification.get("last_full_verified_hash")
    full_id = verification.get("last_full_verification_id")
    full_at = verification.get("last_full_quick_check_at")
    verification_id = verification.get("verification_id")
    if (
        type(verified) is not int
        or verified < 0
        or not isinstance(verified_hash, str)
        or type(full_sequence) is not int
        or full_sequence < 0
        or full_sequence > verified
        or not isinstance(full_hash, str)
        or not isinstance(full_id, str)
        or not isinstance(full_at, str)
        or not isinstance(verification_id, str)
    ):
        _gate_error("COMMISSIONING_LEDGER_FULL_PROVENANCE_INVALID", "Verification ancestry is incomplete.")
    if (
        tail.get("policy_version") != COMMISSIONING_TAIL_POLICY_VERSION
        or tail.get("ledger_identity") != verification.get("ledger_identity")
        or tail.get("ledger_epoch") != verification.get("ledger_epoch")
        or tail.get("ledger_schema_version") != verification.get("ledger_schema_version")
        or tail.get("verified_through_sequence") != verified
        or tail.get("verified_anchor_record_hash") != verified_hash
        or tail.get("last_full_verified_sequence") != full_sequence
        or tail.get("last_full_anchor_record_hash") != full_hash
    ):
        _gate_error("COMMISSIONING_LEDGER_IDENTITY_MISMATCH", "Ledger/checkpoint identity or ancestry changed after verification.")

    tip = tail.get("arm_snapshot_tip")
    classified = tail.get("classified_through_sequence")
    last_authority = tail.get("last_authority_mutation_sequence")
    tail_rows = tail.get("unverified_tail_rows")
    tail_kinds = tail.get("tail_record_kinds")
    if (
        type(tip) is not int
        or type(classified) is not int
        or type(last_authority) is not int
        or type(tail_rows) is not int
        or not isinstance(tail_kinds, list)
        or not all(isinstance(kind, str) for kind in tail_kinds)
        or verified > tip
        or classified != tip
        or tail_rows != tip - verified
    ):
        _gate_error("COMMISSIONING_LEDGER_TAIL_UNCLASSIFIED", "Live ledger tail classification is incomplete.")
    if last_authority > verified:
        _gate_error(
            "COMMISSIONING_LEDGER_TAIL_UNTRUSTED",
            "The unverified live tail contains an authority-changing or unknown record.",
            launch_auto=True,
        )
    if tail_rows > 0 and not tail_kinds:
        _gate_error("COMMISSIONING_LEDGER_TAIL_UNCLASSIFIED", "Live ledger tail kinds are unavailable.")
    if not _runtime_reconciled(runtime):
        _gate_error("COMMISSIONING_RUNTIME_NOT_RECONCILED", "Current Sim101 broker/runtime authority is not cleanly reconciled.")

    trust_state = "VERIFIED_TO_ARM_SNAPSHOT_TIP" if tail_rows == 0 else "VERIFIED_ANCHOR_WITH_PASSIVE_LIVE_TAIL"
    return {
        "ledger_trust_state": trust_state,
        "verification_id": verification_id,
        "verified_through_sequence": verified,
        "verified_tip_hash": verified_hash,
        "verification_completed_at": completed,
        "verification_age_seconds": round(age, 6),
        "full_verification_provenance": {
            "verification_id": full_id,
            "verified_through_sequence": full_sequence,
            "verified_tip_hash": full_hash,
            "quick_check_completed_at": full_at,
            "quick_check": verification.get("quick_check"),
        },
        "arm_snapshot_tip": tip,
        "arm_snapshot_tip_hash": tail.get("arm_snapshot_tip_hash"),
        "unverified_tail_rows": tail_rows,
        "tail_start_sequence": tail.get("tail_start_sequence"),
        "tail_end_sequence": tail.get("tail_end_sequence"),
        "tail_record_kinds": list(tail_kinds),
        "tail_authority_classification": "EMPTY" if tail_rows == 0 else "PASSIVE_ONLY",
        "last_authority_mutation_sequence": last_authority,
        "last_authority_mutation_kind": tail.get("last_authority_mutation_kind"),
        "last_authority_mutation_domain": tail.get("last_authority_mutation_domain"),
        "commissioning_id": runtime.get("commissioning_id"),
        "session_identity": {
            key: runtime.get(key)
            for key in (
                "session_kind", "session_family", "session_id", "trade_date",
                "session_profile_hash", "session_generation",
            )
        },
        "account": runtime.get("account"),
        "account_class": runtime.get("account_class"),
        "instrument": runtime.get("instrument"),
        "policy_version": COMMISSIONING_TAIL_POLICY_VERSION,
        "broker_runtime_reconciliation": {
            key: runtime.get(key)
            for key in (
                "current_position", "current_position_quantity", "working_owned_orders", "working_entry_orders",
                "broker_snapshot_position", "broker_snapshot_position_quantity",
                "position_snapshot_complete", "order_snapshot_complete", "reconciliation_current",
                "unresolved_command", "unresolved_native_order", "unresolved_execution", "entry_owner",
                "commissioning_ownership_active", "live_capital",
            )
        },
    }
