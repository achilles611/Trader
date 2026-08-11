"""Read-only Phase B.2 to Phase C normalization.

Phase B persists deliberately structured research evidence.  The control
center consumes it through this module so its UI contract never depends on
ad-hoc JSON paths or on legacy compatibility scores.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Mapping


def _mapping(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _sequence(value: object) -> list[dict[str, Any]]:
    return [dict(item) for item in value] if isinstance(value, list) and all(isinstance(item, Mapping) for item in value) else []


def _score_value(score: object, key: str, default: Any = None) -> Any:
    if isinstance(score, Mapping):
        return score.get(key, default)
    return getattr(score, key, default)


def _history_days(window: dict[str, Any]) -> float | None:
    try:
        return max(0.0, (datetime.fromisoformat(str(window["required_end"]).replace("Z", "+00:00")) -
                         datetime.fromisoformat(str(window["required_start"]).replace("Z", "+00:00"))).total_seconds() / 86_400)
    except (KeyError, TypeError, ValueError):
        return None


def phase_b_candidate_view(
    summary: object, score: object | None, *, current_config_fingerprint: str | None = None,
    legacy_score: object | None = None,
) -> dict[str, Any]:
    """Normalize a frozen Phase B.2 persisted summary into stable Phase C fields.

    ``score`` must be the authoritative B.2 score belonging to the persisted
    analysis run.  A later legacy score is carried separately and is never used
    for eligibility, display rank, sorting, or state transitions.
    """
    root = _mapping(summary)
    target = _mapping(root.get("target_metrics"))
    activity = _mapping(target.get("activity"))
    profitability = _mapping(target.get("profitability"))
    risk = _mapping(target.get("risk"))
    stability = _mapping(target.get("stability"))
    concentration = _mapping(target.get("concentration"))
    sizing = _mapping(target.get("sizing"))
    follower = _mapping(root.get("follower"))
    copyability = _mapping(root.get("copyability"))
    coverage = _mapping(root.get("coverage"))
    window = _mapping(root.get("analysis_window"))
    diversification = _mapping(root.get("diversification_input"))
    latency = _mapping(root.get("latency"))
    walk_forward_windows = _sequence(root.get("walk_forward"))
    walk_forward_evaluation = _mapping(root.get("walk_forward_evaluation"))
    slippage_scenarios = _sequence(root.get("slippage_scenarios"))

    components = _score_value(score, "component_scores", {}) if score else {}
    penalties = _score_value(score, "penalties", {}) if score else {}
    reasons = _score_value(score, "reasons", ()) if score else ()
    score_fingerprint = _score_value(score, "config_fingerprint") if score else None
    canonical_score = {
        "total": _score_value(score, "total_score") if score else None,
        "eligible": bool(_score_value(score, "eligible", False)) if score else False,
        "components": _mapping(components), "penalties": _mapping(penalties),
        "reasons": [str(value) for value in reasons] if isinstance(reasons, (list, tuple)) else [],
        "provenance": _score_value(score, "provenance") if score else None,
        "analysis_run_id": _score_value(score, "analysis_run_id") if score else None,
        "config_fingerprint": score_fingerprint,
        "current": bool(score and (current_config_fingerprint is None or score_fingerprint == current_config_fingerprint)),
    }
    legacy = None
    if legacy_score is not None:
        legacy = {
            "total": _score_value(legacy_score, "total_score"),
            "eligible": bool(_score_value(legacy_score, "eligible", False)),
            "label": "legacy compatibility score — excluded from Phase C decisions",
        }

    return {
        "score": canonical_score,
        "legacy_compatibility_score": legacy,
        "history_days": _history_days(window),
        "campaigns": activity.get("campaigns"),
        "last_active_recency_days": activity.get("recent_activity_days"),
        "target": {
            "activity": activity, "profitability": profitability, "risk": risk,
            "stability": stability, "concentration": concentration, "sizing": sizing,
            "net_pnl": profitability.get("net_pnl"), "gross_pnl": profitability.get("gross_pnl"),
            "fees": profitability.get("fees"), "win_rate": profitability.get("win_rate"),
            "profit_factor": profitability.get("profit_factor"), "expectancy": profitability.get("expectancy"),
            "max_drawdown": risk.get("max_drawdown_fraction"), "liquidation_frequency": risk.get("liquidation_frequency"),
            "concentration": concentration.get("top_five_campaign_pnl_fraction"),
        },
        "follower": {
            **follower,
            "max_drawdown": follower.get("max_drawdown"),
            "slippage_robustness": follower.get("slippage_robustness_score"),
        },
        "copyability": copyability,
        "coverage": coverage,
        "analysis_window": window,
        "diversification": diversification,
        "slippage_scenarios": slippage_scenarios,
        "latency": latency or {"status": "unavailable", "message": "Historical latency evidence unavailable"},
        "walk_forward": {
            **walk_forward_evaluation,
            "windows": walk_forward_windows,
            "status": walk_forward_evaluation.get("status", "unavailable"),
            "score": walk_forward_evaluation.get("score"),
        },
    }
