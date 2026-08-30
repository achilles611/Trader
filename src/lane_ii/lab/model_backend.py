"""Deterministic venue-neutral hostile execution-condition model.

This is intentionally a laboratory model, not a Phase D adapter.  It never
opens sockets or emits orders; its only output is counterfactual state.
"""

from __future__ import annotations

import copy
import math
from collections.abc import Mapping
from typing import Any

from .contracts import CounterfactualAssertion, CounterfactualMutation, ScenarioValidationError, canonical_hash


class ModelExperimentError(RuntimeError):
    """The model could not apply an allowlisted laboratory condition."""


def _number(value: object, field_name: str, *, non_negative: bool = False) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(float(value)):
        raise ScenarioValidationError(f"{field_name} must be a finite number.")
    result = float(value)
    if non_negative and result < 0:
        raise ScenarioValidationError(f"{field_name} must be non-negative.")
    return result


def _text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > 96:
        raise ScenarioValidationError(f"{field_name} must be bounded non-empty text.")
    return value


class VenueModelBackend:
    """One in-memory universe, designed to be discarded after one run."""

    toolchain_version = "venue-model-v1"

    def __init__(self) -> None:
        self._state: dict[str, Any] = self._initial_state()
        self._closed = False

    @staticmethod
    def _initial_state() -> dict[str, Any]:
        return {
            "balances": {}, "positions": {}, "external_positions": {}, "open_orders": [],
            "marks": {}, "metadata": {}, "fills": {}, "faults": {}, "clock": 0,
            "impersonated": [], "submission_state": "KNOWN", "safety_state": "SAFE",
        }

    @classmethod
    def expected_initial_fingerprint(cls) -> str:
        return canonical_hash(cls._initial_state())

    def start(self) -> None:
        if self._closed:
            raise ModelExperimentError("Discarded venue-model process cannot be reused.")

    def snapshot(self) -> dict[str, Any]:
        return copy.deepcopy(self._state)

    def revert(self, snapshot: object) -> bool:
        if not isinstance(snapshot, dict):
            return False
        self._state = copy.deepcopy(snapshot)
        return True

    def state(self) -> dict[str, Any]:
        return copy.deepcopy(self._state)

    def fingerprint(self) -> str:
        return canonical_hash(self._state)

    def close(self) -> None:
        self._closed = True

    def kill(self) -> None:
        self._closed = True

    def apply(self, mutation: CounterfactualMutation) -> None:
        if type(mutation) is not CounterfactualMutation:
            raise ModelExperimentError("Model requires an exact counterfactual mutation.")
        operation = getattr(self, f"_apply_{mutation.verb}", None)
        if operation is None:
            raise ModelExperimentError("Unknown model mutation.")
        operation(mutation.parameters)
        self._refresh_safety()

    def _refresh_safety(self) -> None:
        faults = self._state["faults"]
        unsafe = bool(self._state["external_positions"] or self._state["open_orders"])
        unsafe = unsafe or self._state["submission_state"] == "UNKNOWN"
        unsafe = unsafe or any(bool(faults.get(item)) for item in (
            "stale_positions", "stale_orders", "account_mismatch", "transport_unavailable", "rate_limit",
            "metadata_drift",
        ))
        self._state["safety_state"] = "RECONCILIATION_REQUIRED" if unsafe else "SAFE"

    def entry_is_safe(self) -> bool:
        """A non-authoritative model answer that preserves 'unknown means reconcile'."""
        return self._state["safety_state"] == "SAFE"

    def _balance(self, parameters: Mapping[str, object]) -> tuple[str, str, float]:
        return (
            _text(parameters.get("actor"), "actor"), _text(parameters.get("asset"), "asset"),
            _number(parameters.get("amount"), "amount", non_negative=True),
        )

    def _apply_set_balance(self, parameters: Mapping[str, object]) -> None:
        actor, asset, amount = self._balance(parameters)
        self._state["balances"].setdefault(actor, {})[asset] = amount

    def _apply_set_position(self, parameters: Mapping[str, object]) -> None:
        self._state["positions"][_text(parameters.get("symbol"), "symbol")] = _number(parameters.get("quantity"), "quantity")

    def _apply_inject_external_position(self, parameters: Mapping[str, object]) -> None:
        symbol = _text(parameters.get("symbol"), "symbol")
        self._state["external_positions"][symbol] = _number(parameters.get("quantity"), "quantity")

    def _apply_inject_open_order(self, parameters: Mapping[str, object]) -> None:
        symbol = _text(parameters.get("symbol"), "symbol")
        quantity = _number(parameters.get("quantity"), "quantity", non_negative=True)
        order_id = parameters.get("order_id", f"foreign-{len(self._state['open_orders']) + 1}")
        self._state["open_orders"].append({"order_id": _text(order_id, "order_id"), "symbol": symbol, "quantity": quantity, "foreign": True})

    def _apply_clear_open_orders(self, parameters: Mapping[str, object]) -> None:
        if parameters:
            symbol = _text(parameters.get("symbol"), "symbol")
            self._state["open_orders"] = [item for item in self._state["open_orders"] if item["symbol"] != symbol]
        else:
            self._state["open_orders"] = []

    def _apply_set_mark_price(self, parameters: Mapping[str, object]) -> None:
        self._state["marks"][_text(parameters.get("symbol"), "symbol")] = _number(parameters.get("price"), "price", non_negative=True)

    def _apply_set_metadata(self, parameters: Mapping[str, object]) -> None:
        symbol = _text(parameters.get("symbol"), "symbol")
        metadata = parameters.get("metadata")
        if not isinstance(metadata, Mapping):
            raise ScenarioValidationError("metadata must be a JSON object.")
        self._state["metadata"][symbol] = dict(metadata)
        self._state["faults"]["metadata_drift"] = True

    def _apply_change_minimum_notional(self, parameters: Mapping[str, object]) -> None:
        symbol = _text(parameters.get("symbol"), "symbol")
        self._state["metadata"].setdefault(symbol, {})["minimum_notional"] = _number(parameters.get("minimum_notional"), "minimum_notional", non_negative=True)
        self._state["faults"]["metadata_drift"] = True

    def _apply_change_precision(self, parameters: Mapping[str, object]) -> None:
        symbol = _text(parameters.get("symbol"), "symbol")
        decimals = parameters.get("quantity_decimals")
        if isinstance(decimals, bool) or not isinstance(decimals, int) or not 0 <= decimals <= 18:
            raise ScenarioValidationError("quantity_decimals must be an integer from 0 through 18.")
        self._state["metadata"].setdefault(symbol, {})["quantity_decimals"] = decimals
        self._state["faults"]["metadata_drift"] = True

    def _add_fill(self, parameters: Mapping[str, object], *, out_of_order: bool = False) -> None:
        fill_id = _text(parameters.get("fill_id"), "fill_id")
        if fill_id in self._state["fills"]:
            return  # deterministic venue-fill identity deduplication
        fill = {
            "fill_id": fill_id, "symbol": _text(parameters.get("symbol"), "symbol"),
            "quantity": _number(parameters.get("quantity"), "quantity", non_negative=True),
            "price": _number(parameters.get("price"), "price", non_negative=True),
            "timestamp": _number(parameters.get("timestamp", self._state["clock"]), "timestamp", non_negative=True),
            "out_of_order": out_of_order,
        }
        self._state["fills"][fill_id] = fill

    def _apply_inject_partial_fill(self, parameters: Mapping[str, object]) -> None:
        self._add_fill(parameters)

    def _apply_inject_duplicate_fill(self, parameters: Mapping[str, object]) -> None:
        self._add_fill(parameters)
        self._state["faults"]["duplicate_fill_seen"] = True

    def _apply_inject_out_of_order_fill(self, parameters: Mapping[str, object]) -> None:
        self._add_fill(parameters, out_of_order=True)

    def _apply_inject_cancel_fill_race(self, parameters: Mapping[str, object]) -> None:
        self._state["faults"]["cancel_fill_race"] = True
        if parameters.get("fill_id") is not None:
            self._add_fill(parameters)

    def _apply_inject_submission_timeout(self, parameters: Mapping[str, object]) -> None:
        self._state["submission_state"] = "UNKNOWN"
        self._state["faults"]["submission_timeout"] = True

    def _apply_inject_accepted_timeout(self, parameters: Mapping[str, object]) -> None:
        self._state["submission_state"] = "UNKNOWN"
        self._state["faults"]["accepted_timeout"] = True

    def _apply_inject_malformed_response(self, parameters: Mapping[str, object]) -> None:
        self._state["faults"]["malformed_response"] = True

    def _apply_inject_rate_limit(self, parameters: Mapping[str, object]) -> None:
        self._state["faults"]["rate_limit"] = True

    def _apply_inject_stale_positions(self, parameters: Mapping[str, object]) -> None:
        self._state["faults"]["stale_positions"] = True

    def _apply_inject_stale_orders(self, parameters: Mapping[str, object]) -> None:
        self._state["faults"]["stale_orders"] = True

    def _apply_inject_account_mismatch(self, parameters: Mapping[str, object]) -> None:
        self._state["faults"]["account_mismatch"] = True

    def _apply_inject_transport_unavailable(self, parameters: Mapping[str, object]) -> None:
        self._state["faults"]["transport_unavailable"] = True

    def _apply_advance_time(self, parameters: Mapping[str, object]) -> None:
        seconds = _number(parameters.get("seconds"), "seconds", non_negative=True)
        self._state["clock"] += seconds

    def assert_state(self, assertion: CounterfactualAssertion) -> None:
        if type(assertion) is not CounterfactualAssertion:
            raise ModelExperimentError("Assertions must be exact laboratory assertions.")
        values = assertion.parameters
        if assertion.verb == "state_path_equals":
            path = _text(values.get("path"), "path")
            actual: object = self._state
            for part in path.split("."):
                if not isinstance(actual, dict) or part not in actual:
                    raise ModelExperimentError("Assertion state path is absent.")
                actual = actual[part]
            if actual != values.get("expected"):
                raise ModelExperimentError("State assertion failed.")
            return
        if assertion.verb == "balance_equals":
            actor, asset, amount = self._balance(values)
            if self._state["balances"].get(actor, {}).get(asset) != amount:
                raise ModelExperimentError("Balance assertion failed.")
            return
        if assertion.verb == "position_equals":
            symbol = _text(values.get("symbol"), "symbol")
            if self._state["positions"].get(symbol, 0.0) != _number(values.get("quantity"), "quantity"):
                raise ModelExperimentError("Position assertion failed.")
            return
        if assertion.verb == "open_order_count_equals":
            if len(self._state["open_orders"]) != values.get("count"):
                raise ModelExperimentError("Open-order count assertion failed.")
            return
        if assertion.verb == "fill_count_equals":
            if len(self._state["fills"]) != values.get("count"):
                raise ModelExperimentError("Fill count assertion failed.")
            return
        if assertion.verb == "safety_state_equals":
            if self._state["safety_state"] != values.get("state"):
                raise ModelExperimentError("Safety-state assertion failed.")
            return
        raise ModelExperimentError("Assertion is not supported by the venue model.")
