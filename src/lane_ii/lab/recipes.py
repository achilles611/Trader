"""Small deterministic recipe registry; recipes are data, never plugins."""

from __future__ import annotations

from collections.abc import Mapping

from .contracts import BackendType, CounterfactualMutation, ScenarioValidationError


RECIPE_NAMES = frozenset({
    "FUNDED_ACTOR", "CONTRACT_CODE_OVERRIDE", "STORAGE_OVERRIDE", "TIME_JUMP",
    "BLOCK_ADVANCE", "HOSTILE_EXTERNAL_POSITION", "HOSTILE_EXTERNAL_ORDER", "PARTIAL_FILL_RACE",
    "RATE_LIMIT_EVENT", "METADATA_DRIFT",
})


def recipe(name: str, parameters: Mapping[str, object], *, backend: BackendType) -> tuple[CounterfactualMutation, ...]:
    """Return an allowlisted immutable mutation collection for one recipe."""
    if name not in RECIPE_NAMES or not isinstance(parameters, Mapping):
        raise ScenarioValidationError("Recipe is not allowlisted or its parameters are invalid.")
    if name == "FUNDED_ACTOR" and backend is BackendType.ANVIL:
        return (CounterfactualMutation("set_native_balance", dict(parameters)),)
    if name == "CONTRACT_CODE_OVERRIDE" and backend is BackendType.ANVIL:
        return (CounterfactualMutation("set_contract_code", dict(parameters)),)
    if name == "STORAGE_OVERRIDE" and backend is BackendType.ANVIL:
        return (CounterfactualMutation("set_storage_slot", dict(parameters)),)
    if name == "TIME_JUMP":
        return (CounterfactualMutation("advance_timestamp" if backend is BackendType.ANVIL else "advance_time", dict(parameters)),)
    if name == "BLOCK_ADVANCE" and backend is BackendType.ANVIL:
        return (CounterfactualMutation("mine_blocks", dict(parameters)),)
    if name == "HOSTILE_EXTERNAL_POSITION" and backend is BackendType.MODEL:
        return (CounterfactualMutation("inject_external_position", dict(parameters)),)
    if name == "HOSTILE_EXTERNAL_ORDER" and backend is BackendType.MODEL:
        return (CounterfactualMutation("inject_open_order", dict(parameters)),)
    if name == "PARTIAL_FILL_RACE" and backend is BackendType.MODEL:
        return (CounterfactualMutation("inject_partial_fill", dict(parameters)), CounterfactualMutation("inject_cancel_fill_race", dict(parameters)))
    if name == "RATE_LIMIT_EVENT" and backend is BackendType.MODEL:
        return (CounterfactualMutation("inject_rate_limit", dict(parameters)),)
    if name == "METADATA_DRIFT" and backend is BackendType.MODEL:
        return (CounterfactualMutation("set_metadata", dict(parameters)),)
    raise ScenarioValidationError("Recipe is incompatible with this backend.")
