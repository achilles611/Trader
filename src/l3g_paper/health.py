"""Sanitized Lane III-G health projection."""

from __future__ import annotations

from typing import Mapping


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
    }
