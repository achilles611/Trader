"""Strict JSON scenario loading and deterministic counterfactual branches."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Mapping

from .contracts import (
    BackendType,
    CounterfactualAssertion,
    CounterfactualMutation,
    CounterfactualScenario,
    SCENARIO_SCHEMA_VERSION,
    ScenarioValidationError,
)


def scenario_from_dict(payload: object) -> CounterfactualScenario:
    if not isinstance(payload, Mapping):
        raise ScenarioValidationError("Scenario document must be a JSON object.")
    allowed = {
        "schema_version", "scenario_id", "scenario_version", "deterministic_seed", "backend",
        "target_domain", "target_chain_id", "fixed_fork_block", "initial_state_fingerprint",
        "mutations", "assertions", "timeout_seconds", "provenance", "parent_scenario_hash", "mutation_delta",
    }
    unknown = set(payload) - allowed
    if unknown:
        raise ScenarioValidationError(f"Scenario contains unrecognized fields: {sorted(unknown)[0]}.")
    required = {
        "schema_version", "scenario_id", "scenario_version", "deterministic_seed", "backend",
        "target_domain", "initial_state_fingerprint", "mutations", "assertions", "timeout_seconds",
    }
    missing = required - set(payload)
    if missing:
        raise ScenarioValidationError(f"Scenario is missing required field: {sorted(missing)[0]}.")
    try:
        backend = BackendType(payload["backend"])
    except (TypeError, ValueError) as exc:
        raise ScenarioValidationError("Scenario backend is not supported.") from exc
    return CounterfactualScenario(
        schema_version=payload["schema_version"], scenario_id=payload["scenario_id"],
        scenario_version=payload["scenario_version"], deterministic_seed=payload["deterministic_seed"],
        backend=backend, target_domain=payload["target_domain"], target_chain_id=payload.get("target_chain_id"),
        fixed_fork_block=payload.get("fixed_fork_block"), initial_state_fingerprint=payload["initial_state_fingerprint"],
        mutations=_mutations(payload["mutations"]), assertions=_assertions(payload["assertions"]),
        timeout_seconds=payload["timeout_seconds"], provenance=payload.get("provenance", "COUNTERFACTUAL_ONLY"),
        parent_scenario_hash=payload.get("parent_scenario_hash"), mutation_delta=_mutations(payload.get("mutation_delta", [])),
    )


def _mutations(value: object) -> tuple[CounterfactualMutation, ...]:
    if not isinstance(value, list):
        raise ScenarioValidationError("mutations must be a JSON array.")
    values: list[CounterfactualMutation] = []
    for item in value:
        if not isinstance(item, Mapping) or set(item) - {"verb", "parameters"} or "verb" not in item:
            raise ScenarioValidationError("Every mutation needs only verb and parameters fields.")
        values.append(CounterfactualMutation(item["verb"], item.get("parameters", {})))
    return tuple(values)


def _assertions(value: object) -> tuple[CounterfactualAssertion, ...]:
    if not isinstance(value, list):
        raise ScenarioValidationError("assertions must be a JSON array.")
    values: list[CounterfactualAssertion] = []
    for item in value:
        if not isinstance(item, Mapping) or set(item) - {"verb", "parameters"} or "verb" not in item:
            raise ScenarioValidationError("Every assertion needs only verb and parameters fields.")
        values.append(CounterfactualAssertion(item["verb"], item.get("parameters", {})))
    return tuple(values)


def load_scenario(path: str | Path) -> CounterfactualScenario:
    document = Path(path)
    if document.suffix.lower() != ".json":
        raise ScenarioValidationError("Only JSON scenario documents are permitted.")
    try:
        raw = json.loads(document.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ScenarioValidationError("Scenario document could not be parsed as JSON.") from exc
    return scenario_from_dict(raw)


def branch_scenario(
    parent: CounterfactualScenario,
    *,
    scenario_id: str,
    mutation_delta: tuple[CounterfactualMutation, ...],
) -> CounterfactualScenario:
    """Create an immutable child universe from a parent scenario manifest."""
    if type(parent) is not CounterfactualScenario or type(mutation_delta) is not tuple:
        raise ScenarioValidationError("Branching requires an exact scenario and immutable mutation delta.")
    return CounterfactualScenario(
        schema_version=SCENARIO_SCHEMA_VERSION, scenario_id=scenario_id,
        scenario_version=parent.scenario_version, deterministic_seed=parent.deterministic_seed,
        backend=parent.backend, target_domain=parent.target_domain, target_chain_id=parent.target_chain_id,
        fixed_fork_block=parent.fixed_fork_block, initial_state_fingerprint=parent.initial_state_fingerprint,
        mutations=parent.mutations + mutation_delta, assertions=parent.assertions,
        timeout_seconds=parent.timeout_seconds, parent_scenario_hash=parent.scenario_hash,
        mutation_delta=mutation_delta,
    )
