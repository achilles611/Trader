"""Fresh-universe counterfactual coordinator with mandatory finally cleanup."""

from __future__ import annotations

import time
import threading
from collections.abc import Callable
from typing import Any

from .anvil_backend import AnvilBackend, AnvilUnavailable
from .anvil_state import (
    ANVIL_EXECUTION_STATE_SCHEMA,
    PINNED_WINDOWS_ARCHIVE_SHA256,
    MutationWitnessRecord,
    RawDumpCapture,
    RestorationObservationError,
    SemanticObservation,
    StructuralDifference,
    UnsupportedAnvilDumpSchema,
    classify_structural_differences,
    differences_payload,
    restoration_verdict,
    witness_specs,
)
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
    canonical_json,
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
        anvil = backend if isinstance(backend, AnvilBackend) else None
        specs = witness_specs(scenario.mutations) if anvil is not None else ()
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
        before_raw: RawDumpCapture | None = None
        after_raw: RawDumpCapture | None = None
        before_semantic: SemanticObservation | None = None
        after_semantic: SemanticObservation | None = None
        witness_before: list[object] = []
        witness_mutated: list[object] = []
        witness_records: tuple[MutationWitnessRecord, ...] = ()
        structural_differences: tuple[StructuralDifference, ...] = ()
        revert_succeeded = False
        restoration_reason = "NOT_APPLICABLE"
        restoration_observation_error: str | None = None
        provider_artifacts: dict[str, str] = {}
        try:
            backend.start()
            supplied_initial = self._scenario_input_fingerprint(backend)
            if supplied_initial != scenario.initial_state_fingerprint:
                raise ScenarioValidationError("Scenario initial-state fingerprint does not match this isolated universe.")
            if anvil is not None:
                before_raw = anvil.capture_raw_dump()
                before_semantic = anvil.capture_semantic_observation(before_raw)
                witness_before = [anvil.read_mutation_witness(spec) for spec in specs]
                before_state = {
                    "semantic_state_sha256": before_semantic.semantic_state_sha256,
                    "canonical_head": before_semantic.canonical_head,
                    "txpool": before_semantic.txpool,
                    "raw_provider_dump_sha256": before_raw.sha256,
                }
                initial_full_fingerprint = before_semantic.semantic_state_sha256
            else:
                before_state = self._observable_state(backend)
                initial_full_fingerprint = canonical_hash(before_state)
            snapshot = backend.snapshot()
            for index, mutation in enumerate(scenario.mutations):
                if time.monotonic() - started > scenario.timeout_seconds:
                    raise CounterfactualRunFailure("Scenario hard timeout reached.")
                backend.apply(mutation)
                mutation_names.append(mutation.verb)
                if anvil is not None:
                    mutated = anvil.read_mutation_witness(specs[index])
                    witness_mutated.append(mutated)
                    if mutated == witness_before[index]:
                        raise CounterfactualRunFailure("Declared Anvil mutation did not change its independent witness.")
            if force_experiment_failure:
                raise CounterfactualRunFailure("Deliberate isolated experiment failure.")
            for assertion in scenario.assertions:
                backend.assert_state(assertion)
                assertion_names.append(assertion.verb)
            if anvil is not None:
                after_state = {
                    "mutation_witnesses": [
                        {"spec": specs[index].payload(), "before": witness_before[index], "mutated": witness_mutated[index]}
                        for index in range(len(witness_mutated))
                    ]
                }
            else:
                after_state = self._observable_state(backend)
            status = RunStatus.SUCCEEDED
        except Exception as exc:  # error payloads can carry provider/secret data; record type only
            failure_kind = type(exc).__name__
            if anvil is None:
                try:
                    after_state = self._observable_state(backend)
                except Exception:
                    after_state = {"observation": "UNAVAILABLE"}
            else:
                after_state = {
                    "mutation_witnesses": [
                        {"spec": specs[index].payload(), "before": witness_before[index], "mutated": witness_mutated[index]}
                        for index in range(len(witness_mutated))
                    ]
                }
            status = RunStatus.BLOCKED if isinstance(exc, AnvilUnavailable) else RunStatus.FAILED
        finally:
            if anvil is None:
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
            else:
                try:
                    try:
                        revert_succeeded = snapshot is not None and anvil.revert(snapshot)
                    except Exception as cleanup_error:
                        failure_kind = failure_kind or type(cleanup_error).__name__
                        revert_succeeded = False
                    try:
                        after_raw = anvil.capture_raw_dump()
                    except UnsupportedAnvilDumpSchema:
                        restoration_observation_error = "UNSUPPORTED_ANVIL_DUMP_SCHEMA"
                    except Exception:
                        restoration_observation_error = "RESTORATION_OBSERVATION_FAILED"
                    if after_raw is not None:
                        try:
                            after_semantic = anvil.capture_semantic_observation(after_raw)
                        except UnsupportedAnvilDumpSchema:
                            restoration_observation_error = "UNSUPPORTED_ANVIL_DUMP_SCHEMA"
                        except RestorationObservationError:
                            restoration_observation_error = "RESTORATION_OBSERVATION_FAILED"
                        except Exception:
                            restoration_observation_error = "RESTORATION_OBSERVATION_FAILED"
                    restored_values: list[object] = []
                    for spec in specs:
                        try:
                            restored_values.append(anvil.read_mutation_witness(spec))
                        except Exception:
                            restored_values.append({"observation": "UNAVAILABLE"})
                            restoration_observation_error = restoration_observation_error or "RESTORATION_OBSERVATION_FAILED"
                    witness_records = tuple(
                        MutationWitnessRecord(
                            spec=specs[index],
                            before=witness_before[index] if index < len(witness_before) else {"observation": "UNAVAILABLE"},
                            mutated=witness_mutated[index] if index < len(witness_mutated) else {"observation": "UNAVAILABLE"},
                            restored=restored_values[index],
                        )
                        for index in range(len(specs))
                    )
                    if before_raw is not None and after_raw is not None:
                        structural_differences = classify_structural_differences(
                            before_raw, after_raw,
                            before_semantic=before_semantic,
                            after_semantic=after_semantic,
                        )
                    restoration_verified, restoration_reason = restoration_verdict(
                        revert_succeeded=revert_succeeded,
                        before_semantic=before_semantic,
                        after_semantic=after_semantic,
                        witnesses=witness_records,
                        differences=structural_differences,
                        observation_error=restoration_observation_error,
                    )
                    if restoration_verified:
                        cleanup_status = "REVERTED_AND_VERIFIED"
                    else:
                        cleanup_status = "REVERT_FAILED_PROCESS_QUARANTINED"
                        status = RunStatus.QUARANTINED
                        anvil.kill()
                except Exception as cleanup_error:
                    failure_kind = failure_kind or type(cleanup_error).__name__
                    restoration_reason = "RESTORATION_OBSERVATION_FAILED_PROCESS_QUARANTINED"
                    cleanup_status = "REVERT_FAILED_PROCESS_QUARANTINED"
                    restoration_verified = False
                    status = RunStatus.QUARANTINED
                    anvil.kill()
                finally:
                    anvil.close()
        diagnostics: dict[str, object] = {
            "scenario_id": scenario.scenario_id,
            "parent_scenario_hash": scenario.parent_scenario_hash,
            "mutation_delta_count": len(scenario.mutation_delta),
        }
        if failure_kind is not None:
            diagnostics["failure_kind"] = failure_kind
        restoration_evidence: dict[str, object] = {}
        if anvil is not None:
            difference_document = differences_payload(structural_differences)
            witness_document = {
                "schema": "anvil-mutation-witness-manifest-v1",
                "records": [item.payload() for item in witness_records],
            }
            witness_document["manifest_sha256"] = canonical_hash(witness_document)
            raw_equal = before_raw is not None and after_raw is not None and before_raw.raw == after_raw.raw
            restoration_evidence = {
                "semantic_fingerprint_schema": ANVIL_EXECUTION_STATE_SCHEMA,
                "raw_provider_dump_before_sha256": before_raw.sha256 if before_raw is not None else "0" * 64,
                "raw_provider_dump_after_sha256": after_raw.sha256 if after_raw is not None else "0" * 64,
                "raw_provider_dump_equal": raw_equal,
                "semantic_state_before_sha256": before_semantic.semantic_state_sha256 if before_semantic is not None else "0" * 64,
                "semantic_state_after_sha256": after_semantic.semantic_state_sha256 if after_semantic is not None else "0" * 64,
                "semantic_state_equal": before_semantic is not None and after_semantic is not None and before_semantic.semantic_state_sha256 == after_semantic.semantic_state_sha256,
                "canonical_head_before": before_semantic.canonical_head if before_semantic is not None else {},
                "canonical_head_after": after_semantic.canonical_head if after_semantic is not None else {},
                "canonical_head_equal": before_semantic is not None and after_semantic is not None and before_semantic.canonical_head == after_semantic.canonical_head,
                "txpool_before": before_semantic.txpool if before_semantic is not None else {},
                "txpool_after": after_semantic.txpool if after_semantic is not None else {},
                "txpool_restored_and_empty": before_semantic is not None and after_semantic is not None and before_semantic.txpool == after_semantic.txpool == {"pending": 0, "queued": 0},
                "mutation_witness_manifest_sha256": witness_document["manifest_sha256"],
                "mutation_witness_count": len(witness_records),
                "mutation_witness_restoration": all(item.restored_exactly for item in witness_records),
                "structural_diff_sha256": difference_document["structural_diff_sha256"],
                "classified_difference_count": difference_document["classified_difference_count"],
                "unknown_difference_count": difference_document["unknown_difference_count"],
                "evm_revert_result": revert_succeeded,
                "restoration_result": restoration_verified,
                "restoration_reason_code": restoration_reason,
                "provider_environment_before": before_semantic.provider_environment if before_semantic is not None else {},
                "provider_environment_after": after_semantic.provider_environment if after_semantic is not None else {},
                "binary_identity": anvil.binary_identity,
                "binary_archive_sha256": PINNED_WINDOWS_ARCHIVE_SHA256,
                "process_shutdown_verified": anvil.shutdown_verified,
                "port_release_verified": anvil.port_release_verified,
                "forked": False,
                "remote_network_contacts": 0,
            }
            if before_raw is not None:
                provider_artifacts["raw_dump_before.txt"] = before_raw.raw
            if after_raw is not None:
                provider_artifacts["raw_dump_after.txt"] = after_raw.raw
            provider_artifacts["raw_dump_structural_diff.json"] = canonical_json(difference_document) + "\n"
            provider_artifacts["mutation_witness_manifest.json"] = canonical_json(witness_document) + "\n"
            if before_semantic is not None:
                provider_artifacts["semantic_state_before.json"] = canonical_json(before_semantic.projection) + "\n"
            if after_semantic is not None:
                provider_artifacts["semantic_state_after.json"] = canonical_json(after_semantic.projection) + "\n"
        evidence = CounterfactualEvidence(
            scenario_hash=scenario.scenario_hash, backend=scenario.backend,
            toolchain_version=backend.toolchain_version, initial_fingerprint=initial_full_fingerprint,
            mutations_applied=tuple(mutation_names), assertions=tuple(assertion_names),
            state_diff=_state_diffs(before_state, after_state), cleanup_status=cleanup_status,
            restoration_verified=restoration_verified, run_status=status,
            restoration_evidence=restoration_evidence,
            fork_chain_id=scenario.target_chain_id, fork_block=scenario.fixed_fork_block,
            bounded_diagnostics=diagnostics,
        )
        outcome = "COUNTERFACTUAL_ASSERTIONS_PASSED" if status is RunStatus.SUCCEEDED else "COUNTERFACTUAL_RUN_NOT_AUTHORITATIVE"
        return CounterfactualRunResult(identity, evidence, outcome, provider_artifacts)
