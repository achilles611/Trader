"""Allowlisted local/public status projections with explicit evidence age.

No filesystem writes occur here. The coordinator atomically publishes this
projection and its manifest. Fresh report generation never refreshes source age.
"""
from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import math
import re

from .hypotheses import document_hash

STATES = {"queued", "leased", "running", "completed", "failed", "cancelled", "admission-held", "unknown"}
NUMERIC_METRICS = ("realized_net", "modeled_fees", "unresolved_basis", "unresolved_positions", "entries",
                   "cash_exits", "settlements", "gross_net", "turnover", "max_drawdown", "exposure_seconds",
                   "fill_rate", "rejection_rate", "latency_p95_seconds", "participation_rate", "mark_coverage")
RESOURCE_METRICS = ("cpu_percent", "available_ram_bytes", "owned_rss_bytes", "free_disk_bytes",
                    "disk_reserve_bytes", "ingest_bytes_per_second", "analysis_backlog_bytes",
                    "process_count", "optional_analysis_paused")
COMPONENTS = ("collector", "worker", "study", "cohort", "supervisor", "report", "accounting", "analysis")


def _number(value):
    return value if type(value) in {int, float} and math.isfinite(value) else None


def _text(value, limit=180):
    value = str(value) if value is not None else "UNKNOWN"
    # Reasons and labels may be user-controlled. Do not leak paths or credential
    # values via an exception string, even though their parent key is allowlisted.
    if (re.search(r"[A-Za-z]:[\\/]|\\\\|/Users/|/home/|0x[0-9a-fA-F]{40}", value)
            or re.search(r"sk-proj-|Bearer\s|PRIVATE KEY|api[_ -]?key|credential\s*[:=]|password\s*[:=]", value, re.I)):
        return "LOCAL_DETAIL_WITHHELD"
    return value.replace("\r", " ").replace("\n", " ").replace("|", "/").replace("<", "&lt;").replace(">", "&gt;")[:limit]


def _timestamp(value):
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            return None
        return parsed.astimezone(timezone.utc)
    except (TypeError, ValueError):
        return None


def _iso(value):
    parsed = _timestamp(value)
    return parsed.isoformat() if parsed else None


def _identity(value):
    if isinstance(value, str):
        return value if re.fullmatch(r"[0-9a-f]{64}", value) else None
    if isinstance(value, dict):
        return {key: val for key, val in value.items()
                if re.fullmatch(r"[A-Za-z0-9_.-]{1,64}", key)
                and isinstance(val, str) and re.fullmatch(r"[0-9a-f]{64}", val)}
    return None


def _state(value):
    state = str(value or "unknown").lower().replace("_", "-")
    return state if state in STATES else "unknown"


def _age(value, now, threshold):
    at = _timestamp(value)
    seconds = (now - at).total_seconds() if at else None
    return {"as_of_utc": at.isoformat() if at else None,
            "age_seconds": max(0, seconds) if seconds is not None else None,
            "future_clock": seconds is not None and seconds < -1,
            "stale": seconds is None or seconds < -1 or seconds > threshold}


def build_status(state, *, now_utc=None, stale_after_seconds=120):
    if not _number(stale_after_seconds) or stale_after_seconds <= 0:
        raise ValueError("POSITIVE_STALE_THRESHOLD_REQUIRED")
    now = _timestamp(now_utc) if now_utc is not None else datetime.now(timezone.utc)
    if now is None:
        raise ValueError("TIMEZONED_REPORT_CLOCK_REQUIRED")
    evidence_time = state.get("as_of_utc", state.get("as_of"))
    evidence_age = _age(evidence_time, now, stale_after_seconds)
    original = state.get("execution", {})
    if not isinstance(original, dict):
        original = {"state": original}
    reported_execution = original.get("state", state.get("state", "unknown"))
    execution = {"state": _state(reported_execution), "reported_state": _text(reported_execution),
                 "terminal_reason": _text(original.get("terminal_reason", original.get("reason"))),
                 "window_ended": original.get("window_ended") is True or str(reported_execution).upper() == "WINDOW_ENDED"}
    components = {}
    for name in COMPONENTS:
        value = state.get("components", {}).get(name, state.get(name))
        if not isinstance(value, dict):
            continue
        age = _age(value.get("as_of_utc", value.get("utc", value.get("heartbeat_utc"))), now, stale_after_seconds)
        components[name] = {"reported_state": _text(value.get("state")), "state": _state(value.get("state")),
                            "reason": _text(value.get("reason")), **age,
                            "currently_running_verified": value.get("currently_running_verified") is True and not age["stale"]}
    source_components = [components[k] for k in ("study", "worker", "collector", "cohort") if k in components]
    if source_components:
        known_ages = [row for row in [evidence_age, *source_components] if row["age_seconds"] is not None]
        oldest = max(known_ages, key=lambda row: row["age_seconds"]) if known_ages else evidence_age
        evidence_age = {"as_of_utc": oldest["as_of_utc"], "age_seconds": oldest["age_seconds"],
                        "stale": evidence_age["stale"] or any(row["stale"] for row in source_components),
                        "future_clock": evidence_age["future_clock"] or any(row["future_clock"] for row in source_components)}
    if any(components.get(k, {}).get("state") == "failed" for k in ("study", "worker", "collector", "cohort")):
        execution["state"] = "failed"
    cov = state.get("coverage", {})
    planned = _number(cov.get("planned_seconds"))
    observed = _number(cov.get("observed_seconds"))
    gaps = _number(cov.get("gap_seconds", cov.get("gaps_seconds")))
    full = (execution["state"] == "completed" and planned is not None and planned > 0
            and observed is not None and observed >= planned and gaps == 0 and cov.get("requirements_met") is True)
    coverage = {"planned_seconds": planned, "observed_seconds": observed, "gap_seconds": gaps,
                "designation": "FULL" if full else "PARTIAL_OR_UNKNOWN",
                "contract_coverage": _number(cov.get("contract_coverage")),
                "source_coverage": _number(cov.get("source_coverage")),
                "continuity": _text(cov.get("continuity", "UNVERIFIED"))}
    raw_closure = state.get("closure", {})
    closure = {key: _text(raw_closure.get(key, "UNKNOWN")) for key in ("seal", "integrity", "accounting", "replay", "analysis")}
    closure["complete_evidence"] = (str(raw_closure.get("seal", "")).upper() in {"SEALED", "PASS"}
        and str(raw_closure.get("integrity", "")).upper() == "PASS"
        and str(raw_closure.get("accounting", "")).upper() == "RECONCILED"
        and str(raw_closure.get("replay", "")).upper() == "PASS"
        and str(raw_closure.get("analysis", "")).upper() == "PUBLISHED")
    queue = state.get("queue", {})
    counts = queue.get("counts", queue.get("states", original.get("counts", {})))
    queue_view = {"counts": {key: int(value) for key, value in counts.items()
                             if key in STATES and type(value) is int and value >= 0},
                  "oldest_age_seconds": _number(queue.get("oldest_age_seconds")),
                  "estimated_output_bytes": _number(queue.get("estimated_output_bytes")),
                  "bounded": queue.get("bounded") is True}
    resources = {key: value if type(value) is bool else _number(value)
                 for key, value in state.get("resources", {}).items() if key in RESOURCE_METRICS}
    clock = state.get("clock", {})
    clock_view = {"state": _text(clock.get("state", "UNKNOWN")),
                  "confidence": _text(clock.get("confidence", "UNKNOWN")),
                  "correction_seconds": _number(clock.get("correction_seconds")),
                  "dispersion_seconds": _number(clock.get("dispersion_seconds")),
                  "accepted_bound_seconds": _number(clock.get("accepted_bound_seconds")),
                  "windows_time_repaired": False}
    accounts = []
    if state.get("paper_only") is True:
        for row in state.get("paper_accounts", [])[:128]:
            accounts.append({"account_id": _text(row.get("account_id", row.get("id"))),
                             **{key: _number(row.get(key)) for key in NUMERIC_METRICS}})
    public_links = []
    for value in state.get("evidence_links", []):
        if isinstance(value, str) and re.fullmatch(r"https://github\.com/achilles611/Trader/(?:blob|commit|pull)/[A-Za-z0-9_./%#?=&-]+", value):
            public_links.append(value)
    result = {"schema": "LANE_II_CONTINUOUS_PUBLIC_STATUS_V1", "generated_at_utc": now.isoformat(),
              "evidence_as_of_utc": evidence_age["as_of_utc"], "evidence_age_seconds": evidence_age["age_seconds"],
              "stale": evidence_age["stale"], "future_clock": evidence_age["future_clock"],
              "stale_after_seconds": stale_after_seconds,
              "source": _identity(state.get("source", state.get("source_sha256"))),
              "config": _identity(state.get("config", state.get("config_sha256"))),
              "current_experiment": _text(state.get("current_experiment", "NONE")),
              "execution": execution, "coverage": coverage, "closure": closure, "components": components,
              "queue": queue_view, "resources": resources, "clock": clock_view,
              "paper_accounts": accounts, "paper_accounts_displayed": len(accounts),
              "paper_account_count": len(state.get("paper_accounts", [])) if state.get("paper_only") is True else None,
              "paper_alternatives_nonadditive": True,
              "blockers": [_text(item) for item in state.get("blockers", [])[:64]],
              "next_action": _text(state.get("next_action", "RECONCILE_CURRENT_EVIDENCE")),
              "experiment_decision": _text(state.get("experiment_decision", "INSUFFICIENT_EVIDENCE")),
              "cohort_decision": _text(state.get("cohort_decision", "RETAIN_FROZEN_ROSTER")),
              "evidence_links": public_links[:16], "funded_authority": False,
              "runtime_model_calls_observed": _number(state.get("runtime_model_calls_observed")),
              "model_enrichment_enabled": False}
    result["status_sha256"] = document_hash(result)
    return result


def render_latest(status):
    if status.get("schema") != "LANE_II_CONTINUOUS_PUBLIC_STATUS_V1":
        raise ValueError("SANITIZED_STATUS_REQUIRED")
    lines = ["# Lane II continuous research status", "",
        f"Evidence as of: **{status['evidence_as_of_utc'] or 'UNKNOWN'}**. "
        f"Age: **{status['evidence_age_seconds'] if status['evidence_age_seconds'] is not None else 'UNKNOWN'} seconds**. "
        f"Freshness: **{'STALE OR UNVERIFIED' if status['stale'] else 'CURRENT WITHIN DECLARED THRESHOLD'}**.", "",
        f"Report generated: {status['generated_at_utc']}. Generation time does not refresh source evidence.", "",
        f"Experiment: `{status['current_experiment']}`. Execution: **{status['execution']['state'].upper()}**. "
        f"Reported terminal reason: `{status['execution']['terminal_reason']}`.", "",
        "| Dimension | Result |", "| --- | --- |",
        f"| Coverage | {status['coverage']['observed_seconds']} observed / {status['coverage']['planned_seconds']} planned seconds; {status['coverage']['designation']}; gaps {status['coverage']['gap_seconds']} seconds |",
        f"| Evidence closure | Seal {status['closure']['seal']}; integrity {status['closure']['integrity']}; accounting {status['closure']['accounting']}; replay {status['closure']['replay']}; analysis {status['closure']['analysis']} |",
        f"| Queue | {status['queue']['counts']}; oldest age {status['queue']['oldest_age_seconds']} seconds |",
        f"| Clock | {status['clock']['state']}; dispersion {status['clock']['dispersion_seconds']} seconds |",
        f"| Research decision | {status['experiment_decision']} |",
        f"| Cohort decision | {status['cohort_decision']} |", ""]
    if status["execution"]["window_ended"]:
        lines += ["The scheduled window ended. This event is not a study-completion or accounting-success claim.", ""]
    if status["components"]:
        lines += ["| Component | Recorded state | Evidence age seconds | Stale |", "| --- | --- | ---: | --- |"]
        for key, row in status["components"].items():
            lines.append(f"| {key} | {row['reported_state']} | {row['age_seconds']} | {row['stale']} |")
        lines.append("")
    if status["paper_accounts"]:
        lines += ["Paper accounts below are independent alternatives. Their results are not a combined portfolio return.", "",
                  "| Alternative | Realized net | Modeled fees | Open basis | Unresolved positions |",
                  "| --- | ---: | ---: | ---: | ---: |"]
        for row in status["paper_accounts"]:
            lines.append(f"| {row['account_id']} | {row['realized_net']} | {row['modeled_fees']} | {row['unresolved_basis']} | {row['unresolved_positions']} |")
        lines += ["", f"Displayed {status['paper_accounts_displayed']} of {status['paper_account_count']} paper alternatives. Unknown values remain unknown.", ""]
    lines += ["Blockers: " + ("; ".join(status["blockers"]) if status["blockers"] else "None recorded; readiness is separately assessed."), "",
              f"Next action: {status['next_action']}.", "",
              f"Source identities: `{status['source']}`. Configuration identities: `{status['config']}`.", "",
              f"Observed runtime model calls: {status['runtime_model_calls_observed']}. Model enrichment is disabled. Funded authority is false.", ""]
    for link in status["evidence_links"]:
        lines.append(f"- [Published evidence]({link})")
    return "\n".join(lines).rstrip() + "\n"


def daily_digest(status):
    if status.get("schema") != "LANE_II_CONTINUOUS_PUBLIC_STATUS_V1":
        raise ValueError("SANITIZED_STATUS_REQUIRED")
    return {"schema": "LANE_II_DAILY_RESEARCH_DIGEST_V1", "source_status_sha256": status["status_sha256"],
            "evidence_as_of_utc": status["evidence_as_of_utc"], "stale": status["stale"],
            "execution": status["execution"], "coverage": status["coverage"], "closure": status["closure"],
            "decision": status["experiment_decision"], "blockers": status["blockers"],
            "next_action": status["next_action"], "automatic_strategy_change": False, "model_api_calls": 0}


def weekly_review(registry, wallet_registry, *, as_of_utc):
    if _iso(as_of_utc) is None:
        raise ValueError("TIMEZONED_REVIEW_CLOCK_REQUIRED")
    dispositions = Counter(row.get("status", "unknown") for row in registry.get("trials", {}).values())
    members = wallet_registry.get("members", [])
    now = _timestamp(as_of_utc)
    eligible = 0
    for row in members:
        assessment = row.get("eligibility", {})
        evaluated, expires = (_timestamp(assessment.get(key)) for key in ("evaluated_at_utc", "valid_until_utc"))
        if (row.get("copying_eligible") is True and assessment.get("paper_copying_eligible") is True
                and evaluated is not None and expires is not None and evaluated <= now <= expires):
            eligible += 1
    observations = Counter(row.get("observation_state", "SOURCE_UNOBSERVED") for row in members)
    pending = sorted(row["trial_id"] for row in registry.get("trials", {}).values() if row.get("status") == "registered")
    return {"schema": "LANE_II_WEEKLY_FROZEN_REVIEW_V1", "as_of_utc": _iso(as_of_utc),
            "hypothesis_registry_sha256": document_hash(registry), "wallet_registry_sha256": document_hash(wallet_registry),
            "trial_dispositions": dict(sorted(dispositions.items())), "pending_trial_ids": pending,
            "wallet_count": len(members), "wallet_lineage": dict(Counter(row.get("cohort", "UNKNOWN") for row in members)),
            "wallet_observation_states": dict(observations),
            "eligible_paper_wallets": eligible,
            "negative_results_retained": dispositions["rejected"] + dispositions["insufficient_evidence"],
            "automatic_admissions": [], "automatic_demotions": [], "automatic_weight_changes": [],
            "review_decision": "REVIEW_FROZEN_EVIDENCE_BEFORE_NEXT_CONFIRMATION_REGISTRATION",
            "capital_review_requires_separate_evidence_gate": True, "model_api_calls": 0}


def closeout_summary(analysis):
    """Allowlisted public projection; source paths, wallet IDs and cash stay local."""
    if analysis.get("schema") != "LANE_II_DESCRIPTIVE_CLOSEOUT_V1":
        raise ValueError("DESCRIPTIVE_CLOSEOUT_REQUIRED")
    components = {}
    for name, component in analysis.get("components", {}).items():
        if name not in {"study", "cohort"}:
            continue
        execution, coverage, closure = (component.get(key, {}) for key in ("execution", "coverage", "closure"))
        components[name] = {
            "execution": {key: _text(execution.get(key)) for key in ("state", "reason", "source")},
            "coverage": {"planned_seconds": _number(coverage.get("planned_seconds")),
                         "observed_seconds": _number(coverage.get("observed_seconds")),
                         "designation": _text(coverage.get("designation"))},
            "closure": {"terminal_receipt_verified": closure.get("terminal_receipt_verified") is True,
                        **{key: _text(closure.get(key)) for key in ("integrity", "accounting", "replay", "analysis")}}}
    accounts = []
    for row in analysis.get("accounts", [])[:512]:
        accounts.append({"account_id": _text(row.get("account_id")), "component": _text(row.get("component")),
            "account_identity_sha256": _identity(row.get("account_identity_sha256")),
            **{key: _number(row.get(key)) for key in ("gross_net", "realized_net", "modeled_fees", "unresolved_basis",
                "unresolved_positions", "entries", "cash_exits", "settlements", "unrealized_pnl", "mark_coverage")},
            "gross_basis": _text(row.get("gross_basis")), "accounting_verified": row.get("accounting_verified") is True,
            "missing_fields": [_text(value) for value in row.get("missing_fields", [])[:16]]})
    return {"schema": "LANE_II_DESCRIPTIVE_CLOSEOUT_V1", "evidence_as_of_utc": _iso(analysis.get("evidence_as_of_utc")),
        "source_sha256": _identity(analysis.get("source_sha256")), "config_sha256": _identity(analysis.get("config_sha256")),
        "input_bindings": [{"role": _text(row.get("role")), "sha256": _identity(row.get("sha256")), "bytes": _number(row.get("bytes"))}
                           for row in analysis.get("input_bindings", [])[:3]],
        "components": components, "accounts": accounts,
        "pending_research": {key: {"status": "PENDING", "reason": _text(value.get("reason"))}
                             for key, value in analysis.get("pending_research", {}).items()
                             if key in {"cost_and_latency_stress", "drawdown_and_exposure_time", "concentration_and_regimes",
                                        "matched_control_comparison", "sample_power_and_duration", "untouched_confirmation", "causal_replay"}},
        "decision": analysis.get("decision") if analysis.get("decision") in {"rejected", "insufficient_evidence"} else "insufficient_evidence",
        "decision_scope": "EVIDENCE_PACKAGE_ACCEPTANCE_NOT_HYPOTHESIS_TEST",
        "rejected_reasons": [_text(value) for value in analysis.get("rejected_reasons", [])[:64]],
        "holds": [_text(value) for value in analysis.get("holds", [])[:64]],
        "independent_accounts_nonadditive": True, "full_duration_qualified": False, "strategy_edge_qualified": False,
        "capital_review_eligible": False, "funded_authority": False, "replay_eligibility_inferred": False,
        "analysis_network_requests": 0, "analysis_model_api_calls": 0, "analysis_real_orders": 0}


def render_closeout(analysis):
    if analysis.get("schema") != "LANE_II_DESCRIPTIVE_CLOSEOUT_V1":
        raise ValueError("DESCRIPTIVE_CLOSEOUT_REQUIRED")
    report = closeout_summary(analysis)
    show = lambda value: "unknown" if value is None else str(value)
    lines = ["# Lane II descriptive closeout", "",
        f"Source evidence as of: {show(report['evidence_as_of_utc'])}. Decision: **{report['decision']}**.", "",
        "This report describes retained paper accounting. Independent accounts are alternatives and their returns are not added together.", "",
        "| Component | Original execution | Terminal reason | Observed / planned seconds | Coverage |",
        "| --- | --- | --- | ---: | --- |"]
    for name, component in report["components"].items():
        execution, coverage = component["execution"], component["coverage"]
        lines.append(f"| {name} | {execution['state']} | {execution['reason']} | {show(coverage['observed_seconds'])} / {show(coverage['planned_seconds'])} | {coverage['designation']} |")
    lines += ["", "| Alternative | Gross realized result | Realized net | All modeled fees | Open basis | Open positions | Accounting verified |",
              "| --- | ---: | ---: | ---: | ---: | ---: | --- |"]
    for row in report["accounts"]:
        lines.append(f"| {row['component']} / {row['account_id']} | {show(row['gross_net'])} | {show(row['realized_net'])} | {show(row['modeled_fees'])} | {show(row['unresolved_basis'])} | {show(row['unresolved_positions'])} | {row['accounting_verified']} |")
    lines += ["", "Gross is net plus all modeled fees only where no position or basis remains open. With open positions, allocation of entry fees to realized results is missing and gross stays unknown. Open basis is unresolved exposure, not a marked profit or loss.", "",
              "| Component | Terminal verified | Retained integrity | Accounting | Replay | Analysis |",
              "| --- | --- | --- | --- | --- | --- |"]
    for name, component in report["components"].items():
        closure = component["closure"]
        lines.append(f"| {name} | {closure['terminal_receipt_verified']} | {closure['integrity']} | {closure['accounting']} | {closure['replay']} | {closure['analysis']} |")
    lines += ["", "Research evidence still pending:", ""]
    lines.extend(f"- {key}: {value['reason']}." for key, value in report["pending_research"].items())
    lines += ["", "Evidence-package rejections: " + ("; ".join(report["rejected_reasons"]) or "None recorded.")]
    lines += ["", "Holds: " + ("; ".join(report["holds"]) or "None recorded."), "",
              "Publication does not establish full-window completion, causal replay, a strategy edge or capital eligibility. Funded authority remains false.", ""]
    return "\n".join(lines)
