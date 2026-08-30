"""Fresh-universe counterfactual coordinator with mandatory finally cleanup."""

from __future__ import annotations

import time
import threading
from collections.abc import Callable
from typing import Any

from .anvil_backend import AnvilBackend, AnvilUnavailable
from .contracts import (
    BackendType,
    CounterfactualEvidence,
    CounterfactualRunIdentity,
    CounterfactualRunResult,
    CounterfactualScenario,
    CounterfactualStateDiff,
    RunStatus,
    ScenarioValidationError,
    canonical_hash,
)
from .model_backend import VenueModelBackend


class CounterfactualRunFailure(RuntimeError):
    """A lab run failed but remained non-authoritative and bounded."""


def _state_diffs(before: object, after: object, *, path: str = "state", limit: int = 64) -> tuple[CounterfactualStateDiff, ...]:
    values: list[CounterfactualStateDiff] = []

    def visit(left: object, right: object, current: str) -> None:
        if len(values) >= limit:
            return
        if isinstance(left, dict) and isinstance(right, dict):
            for key in sorted(set(left) | set(right)):
                visit(left.get(key), right.get(key), f"{current}.{key}")
            return
        if left != right:
            values.append(CounterfactualStateDiff(current, left, right))

    visit(before, after, path)
    return tuple(values)


class CounterfactualCoordinator:
    """Runs one scenario against one disposable mutable universe.

    The constructor deliberately receives backend factories, never an existing
    backend instance.  This makes a shared mutable EVM universe impossible at
    the API boundary.
    """

    def __init__(
        self,
        *,
        model_factory: Callable[[], VenueModelBackend] = VenueModelBackend,
        anvil_factory: Callable[[CounterfactualScenario], AnvilBackend] | None = None,
    ) -> None:
        self._model_factory = model_factory
        self._anvil_factory = anvil_factory or self._default_anvil_factory
        self._issued_backends: list[VenueModelBackend | AnvilBackend] = []
        self._backend_lock = threading.Lock()

    @staticmethod
    def _default_anvil_factory(scenario: CounterfactualScenario) -> AnvilBackend:
        assert scenario.target_chain_id is not None
        return AnvilBackend(chain_id=scenario.target_chain_id, fixed_fork_block=scenario.fixed_fork_block)

    def _backend_for(self, scenario: CounterfactualScenario) -> VenueModelBackend | AnvilBackend:
        backend = self._model_factory() if scenario.backend is BackendType.MODEL else self._anvil_factory(scenario)
        if type(backend) not in {VenueModelBackend, AnvilBackend} and not isinstance(backend, (VenueModelBackend, AnvilBackend)):
            raise CounterfactualRunFailure("Counterfactual backend factory returned an unrecognized backend.")
        with self._backend_lock:
            if any(backend is issued for issued in self._issued_backends):
                raise CounterfactualRunFailure("A mutable counterfactual universe may not be reused.")
            self._issued_backends.append(backend)
        return backend

    @staticmethod
    def _scenario_input_fingerprint(backend: VenueModelBackend | AnvilBackend) -> str:
        return backend.fingerprint() if isinstance(backend, VenueModelBackend) else backend.local_fingerprint()

    @staticmethod
    def _observable_state(backend: VenueModelBackend | AnvilBackend) -> object:
        if isinstance(backend, VenueModelBackend):
            return backend.state()
        return {
            "chain_id": backend._chain_id(),  # private backend operation; no raw RPC exposed to scenarios
            "block_number": backend._block_number(),
            "full_state_fingerprint": backend.fingerprint(),
        }

    def run(self, scenario: CounterfactualScenario, *, force_experiment_failure: bool = False) -> CounterfactualRunResult:
        if type(scenario) is not CounterfactualScenario:
            raise ScenarioValidationError("Counterfactual coordinator accepts only exact scenario contracts.")
        identity = CounterfactualRunIdentity.create(scenario)
        backend = self._backend_for(scenario)
        mutation_names: list[str] = []
        assertion_names: list[str] = []
        before_state: object = {}
        after_state: object = {}
        initial_full_fingerprint = "0" * 64
        failure_kind: str | None = None
        cleanup_status = "NOT_STARTED"
        restoration_verified = False
        status = RunStatus.FAILED
        started = time.monotonic()
        snapshot: object | None = None
        try:
            backend.start()
            supplied_initial = self._scenario_input_fingerprint(backend)
            if supplied_initial != scenario.initial_state_fingerprint:
                raise ScenarioValidationError("Scenario initial-state fingerprint does not match this isolated universe.")
            before_state = self._observable_state(backend)
            initial_full_fingerprint = canonical_hash(before_state)
            snapshot = backend.snapshot()
            for mutation in scenario.mutations:
                if time.monotonic() - started > scenario.timeout_seconds:
                    raise CounterfactualRunFailure("Scenario hard timeout reached.")
                backend.apply(mutation)
                mutation_names.append(mutation.verb)
            if force_experiment_failure:
                raise CounterfactualRunFailure("Deliberate isolated experiment failure.")
            for assertion in scenario.assertions:
                backend.assert_state(assertion)
                assertion_names.append(assertion.verb)
            after_state = self._observable_state(backend)
            status = RunStatus.SUCCEEDED
        except Exception as exc:  # error payloads can carry provider/secret data; record type only
            failure_kind = type(exc).__name__
            try:
                after_state = self._observable_state(backend)
            except Exception:
                after_state = {"observation": "UNAVAILABLE"}
            status = RunStatus.BLOCKED if isinstance(exc, AnvilUnavailable) else RunStatus.FAILED
        finally:
            try:
                if snapshot is None or not backend.revert(snapshot):
                    raise CounterfactualRunFailure("Snapshot restoration refused.")
                restored_state = self._observable_state(backend)
                restoration_verified = canonical_hash(restored_state) == initial_full_fingerprint
                if not restoration_verified:
                    raise CounterfactualRunFailure("Restored state fingerprint differs from initial state.")
                cleanup_status = "REVERTED_AND_VERIFIED"
            except Exception as cleanup_error:
                failure_kind = failure_kind or type(cleanup_error).__name__
                cleanup_status = "REVERT_FAILED_PROCESS_QUARANTINED"
                restoration_verified = False
                status = RunStatus.QUARANTINED
                backend.kill()
            finally:
                backend.close()
        diagnostics: dict[str, object] = {
            "scenario_id": scenario.scenario_id,
            "parent_scenario_hash": scenario.parent_scenario_hash,
            "mutation_delta_count": len(scenario.mutation_delta),
        }
        if failure_kind is not None:
            diagnostics["failure_kind"] = failure_kind
        evidence = CounterfactualEvidence(
            scenario_hash=scenario.scenario_hash, backend=scenario.backend,
            toolchain_version=backend.toolchain_version, initial_fingerprint=initial_full_fingerprint,
            mutations_applied=tuple(mutation_names), assertions=tuple(assertion_names),
            state_diff=_state_diffs(before_state, after_state), cleanup_status=cleanup_status,
            restoration_verified=restoration_verified, run_status=status,
            fork_chain_id=scenario.target_chain_id, fork_block=scenario.fixed_fork_block,
            bounded_diagnostics=diagnostics,
        )
        outcome = "COUNTERFACTUAL_ASSERTIONS_PASSED" if status is RunStatus.SUCCEEDED else "COUNTERFACTUAL_RUN_NOT_AUTHORITATIVE"
        return CounterfactualRunResult(identity, evidence, outcome)
