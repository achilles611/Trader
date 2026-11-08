"""Pure, bounded research registrations and source-bound slim roster preparation.

The coordinator is the sole registry writer. These functions return a new state;
the caller must publish it with its fenced job receipt before acknowledgment.
No registration or selected historical result activates a strategy.
"""
from __future__ import annotations

import ast
from copy import deepcopy
from datetime import datetime
import hashlib
import itertools
import json
from pathlib import Path
import re

SCHEMA = "LANE_II_HYPOTHESIS_REGISTRY_V1"
SHA = re.compile(r"[0-9a-f]{64}\Z")
IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}\Z")
DISPOSITIONS = {"insufficient_evidence", "rejected", "further_research", "research_pass"}
SUPPORTED_CONTRASTS = ["NET_VS_CASH", "NET_VS_MATCHED_BASELINE"]


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")


def document_hash(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def _require(condition, reason):
    if not condition:
        raise ValueError(reason)


def _time(value):
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    _require(parsed.tzinfo is not None, "TIMEZONE_REQUIRED")
    return parsed.timestamp()


def _hash(value):
    return isinstance(value, str) and bool(SHA.fullmatch(value))


def registered_parameter_hashes(specification):
    """The complete frozen Cartesian inventory, independent of trial outcomes."""
    grid = specification["parameter_grid"]
    names = sorted(grid)
    size = 1
    for name in names:
        size *= len(grid[name])
    _require(1 <= size <= 1024, "BOUNDED_PARAMETER_INVENTORY_REQUIRED")
    return sorted(document_hash(dict(zip(names, values)))
                  for values in itertools.product(*(grid[name] for name in names)))


def validate_registration(specification):
    """Require an immutable bounded design; confirmation needs a frozen sample plan."""
    spec = deepcopy(specification)
    for key in ("hypothesis_id", "family_id"):
        _require(isinstance(spec.get(key), str) and IDENTIFIER.fullmatch(spec[key]), "INVALID_" + key.upper())
    _require(type(spec.get("version")) is int and spec["version"] > 0, "POSITIVE_VERSION_REQUIRED")
    _require(spec.get("stage") in {"discovery", "validation", "confirmation"}, "INVALID_RESEARCH_STAGE")
    _require(isinstance(spec.get("question"), str) and 1 <= len(spec["question"]) <= 2000, "QUESTION_REQUIRED")
    _time(spec["registered_at_utc"])
    for key in ("candidate_sha256", "universe_sha256", "membership_sha256", "config_sha256"):
        _require(_hash(spec.get(key)), key.upper() + "_REQUIRED")
    controls = spec.get("control_sha256")
    _require(isinstance(controls, dict) and controls and all(_hash(v) for v in controls.values()), "FROZEN_CONTROLS_REQUIRED")
    _require(type(spec.get("trial_budget")) is int and 1 <= spec["trial_budget"] <= 1024, "BOUNDED_TRIAL_BUDGET_REQUIRED")
    grid = spec.get("parameter_grid")
    _require(isinstance(grid, dict) and len(grid) <= 12, "BOUNDED_PARAMETER_GRID_REQUIRED")
    size = 1
    for key, values in grid.items():
        _require(IDENTIFIER.fullmatch(key) and isinstance(values, list) and 1 <= len(values) <= 64, "INVALID_PARAMETER_GRID")
        _require(all(type(v) in {str, int, float, bool} or v is None for v in values), "SCALAR_GRID_VALUES_REQUIRED")
        encoded = [canonical(v) for v in values]
        _require(len(set(encoded)) == len(values), "DUPLICATE_GRID_VALUE")
        size *= len(values)
    _require(size <= spec["trial_budget"], "GRID_EXCEEDS_REGISTERED_TRIAL_BUDGET")
    if spec["stage"] == "confirmation":
        for key in ("entry_exit_size_rules", "cost_model", "contract_semantics", "primary_metric", "coverage_requirements", "rejection_rule"):
            _require(bool(spec.get(key)), "CONFIRMATION_" + key.upper() + "_REQUIRED")
        start = _time(spec["confirmation_start_utc"])
        end = _time(spec["confirmation_end_utc"])
        _require(_time(spec["registered_at_utc"]) < start < end, "REGISTRATION_MUST_PRECEDE_UNTOUCHED_WINDOW")
        plan = spec.get("sample_plan", {})
        _require(type(plan.get("minimum_independent_blocks")) is int and plan["minimum_independent_blocks"] >= 2, "INDEPENDENT_BLOCK_SAMPLE_PLAN_REQUIRED")
        _require(plan.get("unit") in {"market", "day", "campaign"}, "DEPENDENCE_UNIT_REQUIRED")
        _require(_hash(plan.get("discovery_variance_sha256")), "REGISTERED_POWER_SOURCE_REQUIRED")
        _require(type(plan.get("minimum_effect")) in {float, int} and plan["minimum_effect"] > 0, "ECONOMIC_MINIMUM_EFFECT_REQUIRED")
        _require(plan.get("look_policy") == "FIXED_END_ONLY", "SEQUENTIAL_PROMOTION_NOT_IMPLEMENTED")
        _require(plan.get("tested_contrasts") == SUPPORTED_CONTRASTS,
                 "FROZEN_SUPPORTED_CONTRASTS_REQUIRED")
        _require(plan.get("family_parameter_sha256") == registered_parameter_hashes(spec),
                 "COMPLETE_FROZEN_PARAMETER_INVENTORY_REQUIRED")
        _require(plan.get("multiplicity") == "BONFERRONI"
                 and type(plan.get("family_comparisons")) is int
                 and plan["family_comparisons"] == size * len(SUPPORTED_CONTRASTS),
                 "PREREGISTERED_FAMILY_CORRECTION_REQUIRED")
        _require(type(plan.get("alpha")) in {float, int} and 0 < plan["alpha"] <= .1, "INVALID_REGISTERED_ALPHA")
        _require(plan.get("resampling_method") == "INDEPENDENT_BLOCK_BOOTSTRAP" and type(plan.get("seed")) is int, "FROZEN_BLOCK_RESAMPLING_REQUIRED")
        _require(type(plan.get("resamples")) is int and 199 <= plan["resamples"] <= 10000,
                 "FROZEN_RESAMPLING_COUNT_REQUIRED")
        _require(plan["resamples"] * plan["alpha"] / plan["family_comparisons"] >= 1,
                 "INSUFFICIENT_BOOTSTRAP_TAIL_RESOLUTION")
        power = plan.get("power_calculation")
        _require(isinstance(power, dict) and power.get("schema") == "LANE_II_REGISTERED_POWER_V1"
                 and power.get("method") == "INDEPENDENT_BLOCK_BOOTSTRAP_POWER"
                 and power.get("status") == "PASS" and _hash(power.get("result_sha256"))
                 and power.get("source_variance_sha256") == plan["discovery_variance_sha256"],
                 "STRUCTURED_SOURCE_BOUND_POWER_RESULT_REQUIRED")
        _require(type(power.get("target_power")) in {int, float} and .8 <= power["target_power"] < 1
                 and type(power.get("estimated_power")) in {int, float}
                 and power["target_power"] <= power["estimated_power"] <= 1,
                 "REGISTERED_POWER_TARGET_NOT_MET")
        _require(all(power.get(key) == plan[key] for key in ("minimum_effect", "alpha", "family_comparisons"))
                 and power.get("inference_resamples") == plan["resamples"],
                 "POWER_RESULT_INFERENCE_PLAN_MISMATCH")
        _require(type(power.get("required_independent_blocks")) is int
                 and 2 <= power["required_independent_blocks"] <= plan["minimum_independent_blocks"]
                 and type(power.get("required_window_seconds")) in {int, float}
                 and 0 < power["required_window_seconds"] <= end - start,
                 "POWER_DERIVED_SUPPORT_AND_DURATION_NOT_MET")
    canonical(spec)
    return spec


def new_registry(max_hypotheses=128):
    _require(type(max_hypotheses) is int and 1 <= max_hypotheses <= 1024, "INVALID_REGISTRY_BOUND")
    return {"schema": SCHEMA, "max_hypotheses": max_hypotheses, "revision": 0,
            "hypotheses": {}, "trials": {}, "history": [], "production_self_editing": False}


def register_hypothesis(registry, specification):
    _require(registry.get("schema") == SCHEMA, "REGISTRY_SCHEMA_REQUIRED")
    spec = validate_registration(specification)
    key = f"{spec['hypothesis_id']}@{spec['version']}"
    digest = document_hash(spec)
    old = registry["hypotheses"].get(key)
    if old is not None:
        _require(old["sha256"] == digest, "FROZEN_HYPOTHESIS_CANNOT_CHANGE")
        return deepcopy(registry)
    _require(len(registry["hypotheses"]) < registry["max_hypotheses"], "HYPOTHESIS_REGISTRY_FULL")
    # Rewording or renaming the same numerical design is not a novel trial family.
    design = {k: v for k, v in spec.items() if k not in {"hypothesis_id", "family_id", "version", "question", "registered_at_utc"}}
    novelty = document_hash(design)
    _require(all(row["design_sha256"] != novelty for row in registry["hypotheses"].values()), "DUPLICATE_RESEARCH_DESIGN")
    result = deepcopy(registry)
    result["hypotheses"][key] = {"specification": spec, "sha256": digest, "design_sha256": novelty}
    result["revision"] += 1
    result["history"].append({"event": "REGISTERED", "key": key, "sha256": digest, "at": spec["registered_at_utc"]})
    return result


def register_variant(registry, hypothesis_id, version, parameters, *, created_at_utc):
    key = f"{hypothesis_id}@{version}"
    _require(registry.get("schema") == SCHEMA and key in registry["hypotheses"], "REGISTERED_HYPOTHESIS_REQUIRED")
    registration = registry["hypotheses"][key]
    spec = registration["specification"]
    _require(_time(created_at_utc) >= _time(spec["registered_at_utc"]), "TRIAL_PRECEDES_REGISTRATION")
    grid = spec["parameter_grid"]
    _require(set(parameters) == set(grid), "EXACT_REGISTERED_PARAMETERS_REQUIRED")
    for name, value in parameters.items():
        _require(canonical(value) in {canonical(v) for v in grid[name]}, "UNREGISTERED_PARAMETER_VALUE:" + name)
    trial_id = document_hash({"registration": registration["sha256"], "parameters": parameters})
    if trial_id in registry["trials"]:
        return deepcopy(registry)
    count = sum(row["hypothesis_key"] == key for row in registry["trials"].values())
    _require(count < spec["trial_budget"], "TRIAL_BUDGET_EXHAUSTED")
    result = deepcopy(registry)
    result["trials"][trial_id] = {"trial_id": trial_id, "hypothesis_key": key,
        "parameters": deepcopy(parameters), "created_at_utc": created_at_utc,
        "status": "registered", "evidence_sha256": None}
    result["revision"] += 1
    result["history"].append({"event": "TRIAL_REGISTERED", "trial_id": trial_id, "at": created_at_utc})
    return result


def record_result(registry, trial_id, disposition, evidence_sha256, *, as_of_utc):
    _require(disposition in DISPOSITIONS and _hash(evidence_sha256), "BOUND_RESEARCH_RESULT_REQUIRED")
    _require(trial_id in registry["trials"], "UNKNOWN_TRIAL")
    row = registry["trials"][trial_id]
    _require(_time(as_of_utc) >= _time(row["created_at_utc"]), "RESULT_PRECEDES_TRIAL")
    if row["status"] != "registered":
        _require(row["status"] == disposition and row["evidence_sha256"] == evidence_sha256, "RESULT_IS_IMMUTABLE")
        return deepcopy(registry)
    result = deepcopy(registry)
    result["trials"][trial_id].update(status=disposition, evidence_sha256=evidence_sha256, as_of_utc=as_of_utc)
    result["history"].append({"event": "RESULT", "trial_id": trial_id, "disposition": disposition,
                              "evidence_sha256": evidence_sha256, "at": as_of_utc})
    result["revision"] += 1
    return result


def _source_model(source):
    """Evaluate only the bound pure model constructor, never import its runtime."""
    tree = ast.parse(source)
    weights = next(node for node in tree.body if isinstance(node, ast.Assign)
                   and any(isinstance(t, ast.Name) and t.id == "WEIGHTS" for t in node.targets))
    function = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == "_model")
    prohibited = (ast.Import, ast.ImportFrom, ast.With, ast.AsyncWith, ast.Try, ast.While,
                  ast.For, ast.AsyncFor, ast.ClassDef, ast.Global, ast.Nonlocal, ast.Delete,
                  ast.Lambda, ast.Yield, ast.YieldFrom, ast.Await)
    for node in ast.walk(function):
        _require(not isinstance(node, prohibited), "PURE_MODEL_CONSTRUCTOR_REQUIRED")
        if isinstance(node, ast.Call):
            permitted = (isinstance(node.func, ast.Name) and node.func.id == "dict") or (
                isinstance(node.func, ast.Attribute) and isinstance(node.func.value, ast.Name)
                and node.func.value.id == "model" and node.func.attr == "update")
            _require(permitted, "UNSUPPORTED_MODEL_CONSTRUCTOR_CALL")
    namespace = {"__builtins__": {"dict": dict}, "WEIGHTS": ast.literal_eval(weights.value)}
    module = ast.Module(body=[function], type_ignores=[])
    exec(compile(ast.fix_missing_locations(module), "<bound-model-constructor>", "exec"), namespace)
    return namespace["_model"](300, "P2")


def freeze_minimal_roster(candidate_dir, expected_manifest_sha256):
    candidate = Path(candidate_dir).resolve()
    manifest_bytes = (candidate / "SOURCE_MANIFEST.json").read_bytes()
    _require(_hash(expected_manifest_sha256) and hashlib.sha256(manifest_bytes).hexdigest() == expected_manifest_sha256,
             "PARENT_MANIFEST_CHANGED")
    manifest = json.loads(manifest_bytes)
    bindings, source_bytes = {}, {}
    for name in ("PROFILE_SPEC.json", "optimized_controls.py", "slim_roster.py", "quote_freshness.py"):
        path = (candidate / name).resolve()
        _require(path.is_relative_to(candidate), "SOURCE_ESCAPES_CANDIDATE")
        raw = path.read_bytes()
        digest = hashlib.sha256(raw).hexdigest()
        expected = manifest["files"][name]
        _require(digest == (expected.get("sha256") if isinstance(expected, dict) else expected), "PARENT_SOURCE_CHANGED:" + name)
        bindings[name] = {"sha256": digest, "bytes": len(raw)}
        source_bytes[name] = raw
    p13 = json.loads(source_bytes["PROFILE_SPEC.json"])
    control_model = _source_model(source_bytes["optimized_controls.py"])
    _require(p13["id"] == "5m-P13" and p13["control_id"] == "5m-P2" and p13["horizon_seconds"] == 300
             and p13["allowed_entry_outcomes"] == ["Up", "Down"] and p13["direction_bias"] == "NONE"
             and p13["model"]["budget"] == 5 and control_model["budget"] == 6
             and control_model["requires_current_forecast"] is False, "QUALIFIED_BIDIRECTIONAL_SLIM_LINEAGE_REQUIRED")
    labels = {"budget", "name", "parameter_status", "strategy_version", "display_model_id"}
    _require({k: v for k, v in p13["model"].items() if k not in labels} ==
             {k: v for k, v in control_model.items() if k not in labels}, "P13_HAS_UNREGISTERED_ECONOMIC_DIFFERENCE")
    p2 = {"id": "5m-P2", "horizon_seconds": 300, "model": control_model,
          "kind": "OPTIMIZED_CONTROL", "strategy_version": control_model["strategy_version"],
          "comparison_arm": "MATCHED_BASELINE", "control_id": "cash", "participation": "SELECTIVE",
          "entry_parameters": {}, "direction_bias": "NONE", "allowed_entry_outcomes": ["Up", "Down"],
          "lineage": {"source_module": "optimized_controls.py", "source_manifest_sha256": expected_manifest_sha256,
                      "parent_profile_id": "5m-P13", "economic_difference": "PER_ENTRY_BUDGET_6_VS_5"}}
    result = {"schema": "LANE_II_MINIMAL_ROSTER_V1", "parent_manifest_sha256": expected_manifest_sha256,
              "source_bindings": bindings, "profiles": [p13, p2],
              "cash_control": {"id": "cash", "kind": "ABSTAIN", "virtual_initial_cash": 3000, "entries": 0, "fees": 0},
              "counterfactual_accounts_nonadditive": True, "strategy_edge_qualified": False}
    result["roster_sha256"] = document_hash(result)
    return result
