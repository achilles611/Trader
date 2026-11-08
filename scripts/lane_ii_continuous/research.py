"""Local replay planning and conservative, preregistered research assessments.

Nothing here launches a process, accesses a venue, places an order, or calls a
model provider. Existing replay adapters remain the financial-state oracles.
"""
from __future__ import annotations

from datetime import datetime
import hashlib
import json
import math
import os
from pathlib import Path
import random
import re

from .hypotheses import SUPPORTED_CONTRASTS, document_hash, validate_registration


def _finite(value):
    return type(value) in {int, float} and math.isfinite(value)


def _stamp(value):
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.tzinfo is None:
        raise ValueError("TIMEZONE_REQUIRED")
    return result.timestamp()


def _quantile(values, p):
    values = sorted(values)
    location = (len(values) - 1) * p
    lower = math.floor(location)
    upper = math.ceil(location)
    return values[lower] + (values[upper] - values[lower]) * (location - lower)


def analyze_blocks(blocks, sample_plan, *, registration_sha256, resamples=None):
    """Frozen independent-block percentile bootstrap, never fill-level inference.

    The registration must justify the block definition and power calculation.
    This helper cannot establish independence or complete source coverage itself.
    Missing/overlapping blocks are rejected, rather than silently dropped.
    """
    if resamples is None:
        resamples = sample_plan.get("resamples")
    if resamples != sample_plan.get("resamples"):
        raise ValueError("REGISTERED_RESAMPLING_COUNT_REQUIRED")
    if (not 2 <= len(blocks) <= 10000 or type(resamples) is not int or not 199 <= resamples <= 10000
            or len(blocks) * resamples > 2_000_000):
        raise ValueError("BOUNDED_BLOCK_SAMPLE_AND_RESAMPLING_REQUIRED")
    if (sample_plan.get("resampling_method") != "INDEPENDENT_BLOCK_BOOTSTRAP"
            or sample_plan.get("look_policy") != "FIXED_END_ONLY"
            or sample_plan.get("multiplicity") != "BONFERRONI"
            or type(sample_plan.get("family_comparisons")) is not int
            or sample_plan["family_comparisons"] < 2
            or sample_plan.get("tested_contrasts") != SUPPORTED_CONTRASTS
            or not isinstance(sample_plan.get("family_parameter_sha256"), list)
            or sample_plan["family_comparisons"] != len(sample_plan["family_parameter_sha256"]) * len(SUPPORTED_CONTRASTS)
            or not _finite(sample_plan.get("alpha")) or not 0 < sample_plan["alpha"] <= .1
            or type(sample_plan.get("seed")) is not int):
        raise ValueError("FROZEN_SUPPORTED_INFERENCE_PLAN_REQUIRED")
    if resamples * sample_plan["alpha"] / sample_plan["family_comparisons"] < 1:
        raise ValueError("INSUFFICIENT_BOOTSTRAP_TAIL_RESOLUTION")
    ids, markets = set(), set()
    for row in blocks:
        if (not isinstance(row.get("block_id"), str) or not row["block_id"] or row["block_id"] in ids
                or not isinstance(row.get("market_ids"), list) or not row["market_ids"]
                or len(set(row["market_ids"])) != len(row["market_ids"])
                or markets.intersection(row["market_ids"])
                or any(not _finite(row.get(field)) for field in ("net", "baseline_net", "stress_net"))):
            raise ValueError("UNIQUE_COMPLETE_NONOVERLAPPING_BLOCKS_REQUIRED")
        ids.add(row["block_id"])
        markets.update(row["market_ids"])
    net = [row["net"] for row in blocks]
    differences = [row["net"] - row["baseline_net"] for row in blocks]
    rng = random.Random(sample_plan["seed"])
    n = len(blocks)
    net_boot, difference_boot = [], []
    for _ in range(resamples):
        indices = [rng.randrange(n) for _ in range(n)]
        net_boot.append(math.fsum(net[i] for i in indices) / n)
        difference_boot.append(math.fsum(differences[i] for i in indices) / n)
    tail = sample_plan["alpha"] / sample_plan["family_comparisons"]
    total, delta = math.fsum(net), math.fsum(differences)
    result = {"schema": "LANE_II_BLOCK_ANALYSIS_V1", "registration_sha256": registration_sha256,
        "sample_plan_sha256": document_hash(sample_plan), "blocks_sha256": document_hash(blocks),
        "independent_blocks_claimed": n, "independence_externally_verified": False,
        "resamples": resamples, "seed": sample_plan["seed"], "method": sample_plan["resampling_method"],
        "multiplicity": sample_plan["multiplicity"], "family_comparisons": sample_plan["family_comparisons"],
        "corrected_one_sided_tail_probability": tail,
        "realized_net": total, "matched_baseline_net": math.fsum(row["baseline_net"] for row in blocks),
        "mean_net_per_block": total / n, "mean_matched_difference_per_block": delta / n,
        "net_lower_bound_per_block": _quantile(net_boot, tail),
        "matched_difference_lower_bound_per_block": _quantile(difference_boot, tail),
        "stress_net": math.fsum(row["stress_net"] for row in blocks),
        "leave_best_block_out_net": total - max(net),
        "leave_best_block_out_matched_difference": delta - max(differences),
        "trial_count_is_not_independent_sample_size": True, "accounts_nonadditive": True,
        "calculation_is_not_source_or_independence_verification": True}
    result["analysis_sha256"] = document_hash(result)
    return result


def assess_capital_candidate(registration, evidence):
    """Prepare a review verdict only. Live authority is always false."""
    reasons = []
    try:
        spec = validate_registration(registration)
    except (ValueError, KeyError, TypeError) as exc:
        return {"schema": "LANE_II_CAPITAL_REVIEW_GATE_V1", "research_status": "INSUFFICIENT_EVIDENCE",
                "capital_review_eligible": False, "reasons": ["INVALID_REGISTRATION:" + str(exc)],
                "operational_readiness": "UNKNOWN", "funded_authority": False}
    if spec["stage"] != "confirmation":
        reasons.append("UNTOUCHED_CONFIRMATION_REQUIRED")
    plan = spec.get("sample_plan", {})
    if evidence.get("registration_sha256") != document_hash(spec):
        reasons.append("REGISTRATION_BINDING_MISMATCH")
    for key in ("complete_evidence", "accounting_reconciled", "full_planned_coverage", "untouched_confirmation",
                "causal_inputs_verified", "contract_semantics_verified", "block_independence_verified",
                "family_trial_inventory_complete", "stress_at_proposed_size_verified", "concentration_review_passed",
                "registered_regime_coverage_met", "source_and_config_unchanged", "registered_power_plan_verified"):
        if evidence.get(key) is not True:
            reasons.append(key.upper())
    if evidence.get("historical_positive_selection") is not False:
        reasons.append("HISTORICAL_SELECTION_IS_DISCOVERY")
    if evidence.get("execution_state") != "completed":
        reasons.append("STUDY_NOT_COMPLETED")
    if evidence.get("unresolved_basis") != 0 or evidence.get("unresolved_positions") != 0:
        reasons.append("UNRESOLVED_ACCOUNTING")
    if evidence.get("allowed_look") != "REGISTERED_FINAL_LOOK" or evidence.get("look_number") != 1:
        reasons.append("UNREGISTERED_OR_REPEATED_LOOK")
    if evidence.get("family_parameter_sha256") != plan.get("family_parameter_sha256"):
        reasons.append("FROZEN_FAMILY_INVENTORY_BINDING_REQUIRED")
    if evidence.get("power_result_sha256") != plan.get("power_calculation", {}).get("result_sha256"):
        reasons.append("VERIFIED_POWER_RESULT_BINDING_REQUIRED")
    try:
        observed_start = _stamp(evidence["observed_start_utc"])
        observed_end = _stamp(evidence["observed_end_utc"])
        planned_start = _stamp(spec["confirmation_start_utc"])
        planned_end = _stamp(spec["confirmation_end_utc"])
        if not (observed_start == planned_start < planned_end == observed_end):
            reasons.append("CONFIRMATION_WINDOW_NOT_MET")
    except (KeyError, ValueError, TypeError):
        reasons.append("CONFIRMATION_CLOCKS_MISSING")
    analysis = evidence.get("block_analysis", {})
    claimed_hash = analysis.get("analysis_sha256")
    if (analysis.get("schema") != "LANE_II_BLOCK_ANALYSIS_V1"
            or claimed_hash != document_hash({k: v for k, v in analysis.items() if k != "analysis_sha256"})
            or analysis.get("registration_sha256") != document_hash(spec)
            or analysis.get("sample_plan_sha256") != document_hash(plan)):
        reasons.append("FROZEN_BLOCK_ANALYSIS_BINDING_REQUIRED")
    if (analysis.get("resamples") != plan.get("resamples")
            or analysis.get("seed") != plan.get("seed")
            or analysis.get("method") != plan.get("resampling_method")
            or analysis.get("multiplicity") != plan.get("multiplicity")
            or analysis.get("family_comparisons") != plan.get("family_comparisons")):
        reasons.append("FROZEN_INFERENCE_PARAMETERS_REQUIRED")
    n = analysis.get("independent_blocks_claimed")
    required = plan.get("minimum_independent_blocks")
    if type(n) is not int or type(required) is not int or n < required:
        reasons.append("REGISTERED_INDEPENDENT_SUPPORT_NOT_MET")
    for key in ("realized_net", "mean_net_per_block", "net_lower_bound_per_block", "stress_net",
                "leave_best_block_out_net", "leave_best_block_out_matched_difference"):
        if not _finite(analysis.get(key)) or analysis[key] <= 0:
            reasons.append("NONPOSITIVE_OR_UNKNOWN_" + key.upper())
    minimum = plan.get("minimum_effect")
    lower = analysis.get("matched_difference_lower_bound_per_block")
    if not _finite(minimum) or not _finite(lower) or lower <= minimum:
        reasons.append("MATERIAL_MATCHED_EFFECT_UNCONFIRMED")
    operational = "PASS" if evidence.get("operational_readiness_pass") is True else "UNQUALIFIED"
    research_pass = not reasons
    return {"schema": "LANE_II_CAPITAL_REVIEW_GATE_V1", "registration_sha256": document_hash(spec),
            "research_status": "PASS_FOR_REVIEW" if research_pass else "INSUFFICIENT_EVIDENCE",
            "capital_review_eligible": research_pass and operational == "PASS", "reasons": reasons,
            "operational_readiness": operational, "funded_authority": False,
            "next_action": "PREPARE_OPERATOR_CAPITAL_PACKET" if research_pass and operational == "PASS"
                           else "RESOLVE_RESEARCH_OR_OPERATIONAL_EVIDENCE_GAPS"}


def _small_json(path):
    if path.stat().st_size > 4 * 1024 * 1024:
        raise ValueError("OVERSIZED_REPLAY_METADATA")
    return json.loads(path.read_bytes())


def build_replay_commands(repo, python_executable, *, candidate, study, output, cohort_capture=None, cohort_db=None):
    """Plan the existing strict replay adapters; never loosen their prerequisites."""
    repo, candidate, study, output = map(lambda p: Path(p).resolve(), (repo, candidate, study, output))
    reasons = []
    if output.exists() or output.is_relative_to(study.parent) or output.is_relative_to(candidate.parent):
        reasons.append("NEW_EXTERNAL_REPLAY_OUTPUT_REQUIRED")
    required = [candidate / "SOURCE_MANIFEST.json", study / "PAPER_RUN_RESULT.json", study / "exit.json",
                study / "operator.json", study / "capture/receipt.json", study / "capture/COLLECTION_CLOSED.json"]
    for path in required:
        if not path.is_file():
            reasons.append("MISSING:" + path.name)
    if not reasons:
        receipt = _small_json(study / "capture/receipt.json")
        audit = _small_json(study / "PAPER_RUN_RESULT.json")
        operator = _small_json(study / "operator.json")
        if (receipt.get("state"), receipt.get("reason")) not in {("STOPPED", "USER_STOP"), ("COMPLETE", "DEADLINE")}:
            reasons.append("EXISTING_REPLAY_ADAPTER_REFUSES_FAILED_OR_UNSEALED_CAPTURE")
        if (audit.get("capture_audit", {}).get("status") != "PASS_RETAINED_NEXT12_CAPTURE_INTEGRITY_ONLY"
                or audit.get("capture_audit", {}).get("problems") != []):
            reasons.append("EXISTING_CAPTURE_AUDIT_REQUIRED")
        command = operator.get("command")
        if (not isinstance(command, list) or not command or not isinstance(command[0], str)
                or Path(command[0]).resolve() != Path(python_executable).resolve()):
            reasons.append("RECORDED_INTERPRETER_REQUIRED")
    if (cohort_capture is None) != (cohort_db is None):
        reasons.append("COHORT_CAPTURE_AND_VERIFIED_RESTORE_REQUIRED_TOGETHER")
    if cohort_capture is not None:
        capture = Path(cohort_capture).resolve()
        if not (capture / "receipt.json").is_file() or not (capture / "COLLECTION_CLOSED.json").is_file() or not Path(cohort_db).is_file():
            reasons.append("SEALED_COHORT_AND_RESTORE_REQUIRED")
    commands = []
    if not reasons:
        commands.append({"type": "study_replay", "command": [str(python_executable), "-I", "-B",
            str(repo / "scripts/overnight_paper/replay_study.py"), "--candidate", str(candidate),
            "--study", str(study), "--output", str(output / "study")], "network_allowed": False,
            "resource_class": "heavy", "requires_parent_admission": True})
        if cohort_capture is not None:
            commands.append({"type": "cohort_replay", "command": [str(python_executable), "-I", "-B",
                str(repo / "scripts/overnight_paper/replay_cohort.py"), "--capture", str(Path(cohort_capture).resolve()),
                "--db", str(Path(cohort_db).resolve()), "--output", str(output / "cohort.json")],
                "network_allowed": False, "resource_class": "heavy", "requires_parent_admission": True})
    return {"schema": "LANE_II_REPLAY_PLAN_V1", "state": "BLOCKED" if reasons else "READY_FOR_ADMISSION",
            "reasons": reasons, "commands": commands, "adapter_rechecks_all_evidence": True,
            "concurrent_heavy_jobs": 1, "launch_performed": False, "model_api_calls": 0}


def assess_replay_results(study_result, cohort_result=None):
    count = study_result.get("replayed_original_profile_count")
    financial = study_result.get("parity", {})
    decisions = study_result.get("recorded_decision_parity", {})
    study_pass = (type(count) is int and count > 0
        and study_result.get("source_profiles_with_financial_parity") == count
        and study_result.get("source_profiles_with_recorded_decision_parity") == count
        and len(financial) == len(decisions) == count and set(financial) == set(decisions)
        and all(row.get("pass") is True for row in financial.values())
        and all(row.get("pass") is True for row in decisions.values()))
    cohort_pass = None if cohort_result is None else (
        cohort_result.get("frozen_policy_reconciliation", {}).get("pass") is True
        and cohort_result.get("terminal_receipt_reconciliation", {}).get("pass") is True
        and cohort_result.get("network_requests") == 0 and cohort_result.get("real_orders") == 0)
    return {"schema": "LANE_II_REPLAY_ASSESSMENT_V1", "study_parity": "PASS" if study_pass else "FAIL",
            "cohort_parity": "NOT_REQUESTED" if cohort_pass is None else ("PASS" if cohort_pass else "FAIL"),
            "all_requested_parity_passed": study_pass and cohort_pass is not False,
            "source_execution_state": study_result.get("source_state", "UNKNOWN"),
            "source_profile_count": study_result.get("source_profile_count"), "replayed_profile_count": count,
            "planned_window_completion_inferred": False, "profitability_qualified": False}


def _bound_document(binding):
    if (not isinstance(binding, dict) or not isinstance(binding.get("path"), str)
            or not isinstance(binding.get("sha256"), str) or not re.fullmatch(r"[0-9a-f]{64}", binding["sha256"])):
        raise ValueError("BOUND_ARTIFACT_PATH_AND_SHA256_REQUIRED")
    path = Path(binding["path"]).resolve()
    if path.stat().st_size > 4 * 1024 * 1024:
        raise ValueError("OVERSIZED_CLOSEOUT_METADATA")
    with path.open("rb") as stream:
        raw = stream.read(4 * 1024 * 1024 + 1)
    if len(raw) > 4 * 1024 * 1024 or hashlib.sha256(raw).hexdigest() != binding["sha256"]:
        raise ValueError("CLOSEOUT_ARTIFACT_HASH_MISMATCH")
    document = json.loads(raw, parse_constant=lambda _: (_ for _ in ()).throw(ValueError("NONFINITE_CLOSEOUT_METADATA")))
    if not isinstance(document, dict):
        raise ValueError("CLOSEOUT_DOCUMENT_REQUIRED")
    return document, {"path": str(path), "sha256": binding["sha256"], "bytes": len(raw)}


def _includes_binding(document, binding):
    if not binding:
        return False
    for item in document.get("input_bindings", []):
        if (isinstance(item, dict) and item.get("sha256") == binding["sha256"]
                and isinstance(item.get("path"), str) and Path(item["path"]).resolve() == Path(binding["path"])):
            return True
    return False


def _closeout_account(row, component):
    identity = row.get("id") if component == "study" else {"wallet": row.get("wallet"), "policy": row.get("policy")}
    public_id = identity if isinstance(identity, str) and re.fullmatch(r"[A-Za-z0-9_.-]{1,64}", identity) else component + "-" + document_hash(identity)[:20]
    result = {"account_id": public_id, "account_identity_sha256": document_hash(identity),
              "component": component, "realized_net": row.get("realized_pnl"),
              "modeled_fees": row.get("modeled_fees"), "unresolved_basis": row.get("unresolved_basis"),
              "unresolved_positions": row.get("unresolved_positions"),
              "entries": row.get("entries"), "cash_exits": row.get("cash_exits"), "settlements": row.get("settlements")}
    missing = []
    for key in ("realized_net", "modeled_fees", "unresolved_basis"):
        if not _finite(result[key]) or (key != "realized_net" and result[key] < 0):
            result[key] = None
            missing.append(key.upper())
    for key in ("unresolved_positions", "entries", "cash_exits", "settlements"):
        if type(result[key]) is not int or result[key] < 0:
            result[key] = None
            missing.append(key.upper())
    closed = result["unresolved_positions"] == 0 and result["unresolved_basis"] == 0
    result["gross_net"] = (result["realized_net"] + result["modeled_fees"]
                           if closed and result["realized_net"] is not None and result["modeled_fees"] is not None else None)
    result["gross_basis"] = "REALIZED_NET_PLUS_ALL_FEES_NO_OPEN_POSITIONS" if result["gross_net"] is not None else "REALIZED_FEE_ATTRIBUTION_MISSING"
    result["unrealized_pnl"] = None
    result["mark_coverage"] = None
    result["missing_fields"] = missing
    return result


def analyze_closeout(evidence, output_dir):
    """Publish a bounded descriptive closeout from hash-bound local artifacts.

    ``artifacts`` accepts terminal_receipt (raw worker receipt), raw_audit
    (LANE_II_READONLY_CAPTURE_AUDIT_V1), and accounting (retained CLOSEOUT.json).
    Missing artifacts remain explicit holds. Present artifacts must match their
    exact hashes; at most three 4 MiB documents and 512 alternatives are read.
    No ledger/tape scan, replay, process, account, or network action occurs.
    """
    from .reports import closeout_summary, render_closeout

    supplied = evidence.get("artifacts", {})
    if not isinstance(supplied, dict) or set(supplied) - {"terminal_receipt", "raw_audit", "accounting"}:
        raise ValueError("EXACT_CLOSEOUT_ARTIFACT_ROLES_REQUIRED")
    documents, bindings, holds, rejected = {}, {}, [], []
    for role in ("terminal_receipt", "raw_audit", "accounting"):
        if role not in supplied or supplied[role] is None:
            holds.append("MISSING_" + role.upper())
            continue
        documents[role], bindings[role] = _bound_document(supplied[role])
    output = Path(output_dir).resolve()
    receipt, audit, accounting = (documents.get(key, {}) for key in ("terminal_receipt", "raw_audit", "accounting"))
    protected = []
    if "terminal_receipt" in bindings:
        protected.append(Path(bindings["terminal_receipt"]["path"]).parent)
    for value in (audit.get("capture_dir"), accounting.get("source_root")):
        if isinstance(value, str):
            protected.append(Path(value).resolve())
    if any(output.is_relative_to(path) or path.is_relative_to(output) for path in protected):
        raise ValueError("ANALYSIS_OUTPUT_MUST_BE_OUTSIDE_CAPTURE")
    terminal_valid = receipt.get("state") in {"COMPLETE", "STOPPED", "FAILED", "CANCELLED"}
    if receipt and not terminal_valid:
        rejected.append("GENUINE_TERMINAL_RECEIPT_REQUIRED")
    if receipt and any(type(receipt.get(key)) is not int or receipt[key] != 0
                       for key in ("real_orders", "model_api_calls", "authenticated_account_calls")):
        holds.append("RECEIPT_PAPER_BOUNDARY_NOT_PROVEN")
    receipt_binding = bindings.get("terminal_receipt")
    receipt_in_accounting = _includes_binding(accounting, receipt_binding)
    receipt_in_audit = _includes_binding(audit, receipt_binding)
    if receipt and not receipt_in_accounting:
        holds.append("ACCOUNTING_NOT_BOUND_TO_TERMINAL_RECEIPT")
    if receipt and not receipt_in_audit:
        holds.append("RAW_AUDIT_NOT_BOUND_TO_TERMINAL_RECEIPT")
    audit_pass = (audit.get("schema") == "LANE_II_READONLY_CAPTURE_AUDIT_V1"
                  and audit.get("status") == "PASS_RETAINED_CAPTURE_INTEGRITY_ONLY"
                  and audit.get("problem_counts") == {} and receipt_in_audit
                  and audit.get("execution", {}).get("state") == receipt.get("state"))
    if audit and not audit_pass:
        rejected.append("RAW_AUDIT_FAILED_OR_BINDING_MISMATCH")
    if accounting and accounting.get("schema") != "LANE_II_RETAINED_CLOSEOUT_V1":
        rejected.append("SUPPORTED_ACCOUNTING_ARTIFACT_REQUIRED")
    raw_components = accounting.get("components", {})
    if not isinstance(raw_components, dict) or set(raw_components) - {"study", "cohort"}:
        raise ValueError("BOUNDED_KNOWN_CLOSEOUT_COMPONENTS_REQUIRED")
    components, accounts = {}, []
    for name in ("study", "cohort"):
        component = raw_components.get(name)
        if component is None:
            continue
        account_document = component.get("accounting", {})
        rows = account_document.get("accounts", [])
        if not isinstance(rows, list) or len(accounts) + len(rows) > 512:
            raise ValueError("CLOSEOUT_ACCOUNT_BOUND_EXCEEDED")
        expected_status = "PASS_RETAINED_PROFILE_ACCOUNTING" if name == "study" else "PASS_RETAINED_COHORT_ACCOUNTING"
        accounting_pass = account_document.get("status") == expected_status and bool(rows)
        execution = dict(component.get("execution", {}))
        if name == "study" and receipt:
            if str(execution.get("state", "")).upper() != receipt.get("state"):
                rejected.append("STUDY_EXECUTION_BINDING_MISMATCH")
            execution = {"state": str(receipt.get("state", "UNKNOWN")).lower(), "reason": receipt.get("reason"),
                         "source": "ORIGINAL_GENUINE_RECEIPT", "actual_end_utc": receipt.get("actual_end_utc")}
            original_rows = receipt.get("profiles", [])
            originals = {row.get("id"): row for row in original_rows}
            if (not receipt_in_accounting or len(originals) != len(original_rows)
                    or {row.get("id") for row in rows} != set(originals) or len(rows) != len(originals)):
                accounting_pass = False
            else:
                for row in rows:
                    original = originals[row["id"]]
                    for key in ("realized_pnl", "modeled_fees", "unresolved_basis", "unresolved_positions", "entries", "cash_exits", "settlements"):
                        if not _finite(row.get(key)) or not _finite(original.get(key)) or abs(row[key] - original[key]) > 1e-5:
                            accounting_pass = False
        if not accounting_pass:
            rejected.append(name.upper() + "_ACCOUNTING_NOT_VERIFIED")
        coverage = component.get("coverage", {})
        planned, observed = coverage.get("planned_seconds"), coverage.get("observed_seconds")
        if name == "study" and receipt:
            try:
                start, cutoff, end = (_stamp(receipt[key]) for key in ("paper_start_utc", "cutoff_utc", "actual_end_utc"))
                if not start < cutoff or end < start:
                    raise ValueError("INVALID_CAPTURE_CLOCKS")
                planned, observed = cutoff - start, max(0, min(end, cutoff) - start)
            except (KeyError, TypeError, ValueError, AttributeError):
                holds.append("STUDY_COVERAGE_CLOCKS_UNVERIFIED")
        components[name] = {"execution": execution, "coverage": {"planned_seconds": planned, "observed_seconds": observed,
            "designation": "DURATION_REACHED_CONTINUITY_UNVERIFIED" if _finite(planned) and _finite(observed)
                           and planned > 0 and observed >= planned and execution.get("state") == "complete" else "PARTIAL_OR_UNKNOWN"},
            "closure": {"terminal_receipt_verified": name == "study" and terminal_valid and receipt_in_accounting,
                        "integrity": "PASS_RETAINED_CAPTURE_ONLY" if name == "study" and audit_pass else "UNVERIFIED",
                        "accounting": "RECONCILED" if accounting_pass else "UNVERIFIED", "replay": "NOT_RUN", "analysis": "PUBLISHED"}}
        if name == "cohort":
            holds.append("COHORT_GENUINE_TERMINAL_RAW_AUDIT_AND_REPLAY_NOT_SUPPLIED")
        for row in rows:
            account = _closeout_account(row, name)
            account["accounting_verified"] = accounting_pass and not account["missing_fields"]
            if account["missing_fields"]:
                holds.append(name.upper() + "_DESCRIPTIVE_ACCOUNT_FIELDS_MISSING")
            if account["unresolved_positions"] != 0 or account["unresolved_basis"] != 0:
                holds.append(name.upper() + "_OPEN_EXPOSURE_UNRESOLVED")
            accounts.append(account)
    if not components:
        holds.append("NO_BOUND_COMPONENT_ACCOUNTING")
    pending = {key: {"status": "PENDING", "reason": reason} for key, reason in {
        "cost_and_latency_stress": "EXECUTABLE_DEPTH_DELAY_AND_FEE_STRESS_NOT_SUPPLIED",
        "drawdown_and_exposure_time": "ORDERED_MARK_AND_EXPOSURE_SERIES_NOT_SUPPLIED",
        "concentration_and_regimes": "INDEPENDENT_MARKET_DAY_CAMPAIGN_BLOCKS_NOT_SUPPLIED",
        "matched_control_comparison": "MATCHED_CONTRACT_AND_OPPORTUNITY_KEYS_NOT_SUPPLIED",
        "sample_power_and_duration": "STRUCTURED_VERIFIED_DISCOVERY_POWER_RESULT_NOT_SUPPLIED",
        "untouched_confirmation": "PREREGISTERED_UNUSED_CONFIRMATION_NOT_SUPPLIED",
        "causal_replay": "SEALED_ADAPTER_PREREQUISITES_AND_FULL_PARITY_NOT_ESTABLISHED"}.items()}
    holds.extend(row["reason"] for row in pending.values())
    report = closeout_summary({"schema": "LANE_II_DESCRIPTIVE_CLOSEOUT_V1",
        "evidence_as_of_utc": receipt.get("actual_end_utc"), "source_sha256": evidence.get("source_sha256"),
        "config_sha256": evidence.get("config_sha256"),
        "input_bindings": [{"role": role, "sha256": binding["sha256"], "bytes": binding["bytes"]} for role, binding in bindings.items()],
        "components": components, "accounts": accounts, "pending_research": pending,
        "decision": "rejected" if rejected else "insufficient_evidence",
        "decision_scope": "EVIDENCE_PACKAGE_ACCEPTANCE_NOT_HYPOTHESIS_TEST",
        "rejected_reasons": sorted(set(rejected)), "holds": sorted(set(holds)),
        "independent_accounts_nonadditive": True, "full_duration_qualified": False,
        "strategy_edge_qualified": False, "capital_review_eligible": False, "funded_authority": False,
        "replay_eligibility_inferred": False, "network_requests": 0, "model_api_calls": 0, "real_orders": 0})
    report["analysis_sha256"] = document_hash(report)
    payloads = {"ANALYSIS.json": (json.dumps(report, indent=2, sort_keys=True, allow_nan=False) + "\n").encode(),
                "LATEST.md": render_closeout(report).encode()}
    for binding in bindings.values():
        _bound_document(binding)
    artifacts = [{"path": str(output / name), "bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()}
                 for name, raw in payloads.items()]
    manifest = {"schema": "LANE_II_CLOSEOUT_ANALYSIS_ARTIFACTS_V1", "analysis_sha256": report["analysis_sha256"], "artifacts": artifacts}
    manifest_raw = (json.dumps(manifest, indent=2, sort_keys=True) + "\n").encode()
    if output.exists():
        if (not (output / "ARTIFACT_MANIFEST.json").is_file()
                or (output / "ARTIFACT_MANIFEST.json").stat().st_size != len(manifest_raw)
                or (output / "ARTIFACT_MANIFEST.json").read_bytes() != manifest_raw):
            raise ValueError("EXISTING_ANALYSIS_OUTPUT_NOT_IDENTICAL")
        for name, raw in payloads.items():
            if (output / name).stat().st_size != len(raw) or (output / name).read_bytes() != raw:
                raise ValueError("EXISTING_ANALYSIS_ARTIFACT_CHANGED")
    else:
        output.mkdir(parents=True, exist_ok=False)
        for name, raw in {**payloads, "ARTIFACT_MANIFEST.json": manifest_raw}.items():
            temp = output / (name + ".tmp")
            with temp.open("xb") as stream:
                stream.write(raw)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temp, output / name)
    return {"schema": "LANE_II_CLOSEOUT_ANALYSIS_DELIVERY_V1", "result": report, "artifacts": artifacts,
            "manifest": {"path": str(output / "ARTIFACT_MANIFEST.json"), "bytes": len(manifest_raw),
                         "sha256": hashlib.sha256(manifest_raw).hexdigest()}}
