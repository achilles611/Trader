"""Sanitized Lane III-G health projection."""

from __future__ import annotations

from typing import Mapping


def ledger_health_projection(runtime: Mapping[str, object], verification: Mapping[str, object]) -> dict[str, object]:
    """Join inexpensive writer state with the verifier's durable authority."""
    value = dict(runtime)
    current = int(value.get("highest_sequence") or 0)
    verified_raw = verification.get("verified_through_sequence")
    verified = verified_raw if type(verified_raw) is int and verified_raw >= 0 else None
    status = str(verification.get("status") or "UNVERIFIED")
    verification_pass = status == "PASS" and verification.get("chain_valid") is True
    tail = None if verified is None else max(0, current - verified)
    watermark = value.get("authority_watermark")
    authority = watermark if isinstance(watermark, Mapping) else {}
    authority_sequence = authority.get("last_authority_mutation_sequence")
    observation_sequence = authority.get("last_authority_observation_sequence")
    unknown_sequence = authority.get("last_unknown_sequence")
    classified_sequence = authority.get("classified_through_sequence")
    classified_hash = authority.get("classified_through_hash")
    blocking_ready = type(authority_sequence) is int and type(unknown_sequence) is int
    blocking_sequence = max(authority_sequence, unknown_sequence) if blocking_ready else None
    blocking_prefix = (
        "last_unknown"
        if blocking_ready and unknown_sequence >= authority_sequence
        else "last_authority_mutation"
    )
    blocking_classification = (
        None if blocking_sequence in (None, 0)
        else "UNKNOWN" if blocking_prefix == "last_unknown"
        else "AUTHORITY_MUTATION"
    )
    if (
        verification_pass and verified is not None and tail == 0
        and verification.get("tip_hash") == value.get("final_record_hash")
        and classified_sequence == current
        and classified_hash == value.get("final_record_hash")
    ):
        commissioning_state = "VERIFIED_TO_CURRENT_TIP"
    elif (
        verification_pass
        and verified is not None
        and type(authority_sequence) is int
        and type(observation_sequence) is int
        and type(unknown_sequence) is int
        and authority_sequence <= verified
        and unknown_sequence <= verified
        and classified_sequence == current
        and classified_hash == value.get("final_record_hash")
    ):
        commissioning_state = (
            "VERIFIED_ANCHOR_WITH_ACCEPTED_LIVE_TAIL"
            if observation_sequence > verified else "VERIFIED_ANCHOR_WITH_PASSIVE_LIVE_TAIL"
        )
    elif (
        verification_pass and verified is not None and blocking_sequence is not None
        and blocking_sequence > verified
    ):
        commissioning_state = (
            "UNVERIFIED_UNKNOWN_TAIL"
            if blocking_classification == "UNKNOWN" else "UNVERIFIED_AUTHORITY_MUTATION_TAIL"
        )
    else:
        commissioning_state = "UNTRUSTED"
    value.update({
        "main_database_bytes": value.get("file_size"),
        "total_footprint_bytes": sum(int(value.get(name) or 0) for name in ("file_size", "wal_size")),
        "verification_status": status,
        "verification_mode": verification.get("verification_mode"),
        "verified_through_sequence": verified,
        "verified_tip_hash": verification.get("tip_hash"),
        "unverified_tail_rows": tail,
        "commissioning_ledger_state": commissioning_state,
        "last_authority_mutation_sequence": authority_sequence,
        "last_authority_mutation_kind": authority.get("last_authority_mutation_kind"),
        "last_authority_mutation_domain": authority.get("last_authority_mutation_domain"),
        "last_authority_mutation_hash": authority.get("last_authority_mutation_hash"),
        "last_authority_observation_sequence": observation_sequence,
        "last_authority_observation_kind": authority.get("last_authority_observation_kind"),
        "last_authority_observation_domain": authority.get("last_authority_observation_domain"),
        "last_authority_observation_hash": authority.get("last_authority_observation_hash"),
        "last_unknown_sequence": unknown_sequence,
        "last_unknown_kind": authority.get("last_unknown_kind"),
        "last_unknown_domain": authority.get("last_unknown_domain"),
        "last_unknown_hash": authority.get("last_unknown_hash"),
        "last_blocking_sequence": blocking_sequence,
        "last_blocking_kind": (
            None if blocking_sequence in (None, 0) else authority.get(f"{blocking_prefix}_kind")
        ),
        "last_blocking_domain": (
            None if blocking_sequence in (None, 0) else authority.get(f"{blocking_prefix}_domain")
        ),
        "last_blocking_hash": (
            None if blocking_sequence in (None, 0) else authority.get(f"{blocking_prefix}_hash")
        ),
        "last_blocking_classification": blocking_classification,
        "tail_classified_through_sequence": classified_sequence,
        "tail_classified_through_hash": classified_hash,
        "last_full_quick_check_at": verification.get("last_full_quick_check_at"),
        "last_full_verification_id": verification.get("last_full_verification_id"),
        "last_full_verified_sequence": verification.get("last_full_verified_sequence"),
        "last_incremental_at": verification.get("completed_at") if verification.get("verification_mode") == "incremental" and verification_pass else None,
        "quick_check_state": (
            "PASS" if verification_pass and verification.get("quick_check") in {"ok", "inherited_from_full"}
            else value.get("quick_check_state", "UNKNOWN")
        ),
        "hash_chain_state": (
            f"VERIFIED THROUGH #{verified}" + (f"; TAIL PENDING: {tail} ROWS" if tail else "")
            if verification_pass and verified is not None else "PENDING" if status == "IN_PROGRESS" else "UNKNOWN"
        ),
        "epoch_warning": value.get("epoch_id") == "UNSPECIFIED",
    })
    return value


def sanitized_paper_health(status: Mapping[str, object]) -> dict[str, object]:
    """Return an API-safe authority/status view with no key or credential fields."""
    return {
        "mode": "PAPER_SIM101",
        "scientific_eligibility": False,
        "paper_execution": status.get("paper_execution", "DISARMED"),
        "state": status.get("state", "UNSTARTED"),
        "account": "Sim101",
        "account_class": "LOCAL_SIMULATION",
        "instrument": "MNQ SEP26",
        "maximum_quantity": 1,
        "live_capital": "DENIED",
        "current_position": status.get("current_position", "FLAT"),
        "working_owned_orders": status.get("working_owned_orders", 0),
        "protective_stop_state": status.get("protective_stop_state", "NONE"),
        "current_session": status.get("current_session", "OFF_SESSION"),
        "current_session_id": status.get("current_session_id"),
        "trade_date": status.get("trade_date"),
        "session_armed_state": status.get("session_armed_state", "DISARMED"),
        "combined_trade_date_pnl": status.get("combined_trade_date_pnl", "0"),
        "lockout_or_fault_reason": status.get("lockout_or_fault_reason"),
        "transport": status.get("transport"),
        "ledger": status.get("ledger"),
    }
