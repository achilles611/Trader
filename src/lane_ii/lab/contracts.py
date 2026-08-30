"""Immutable, non-authoritative contracts for Phase f4.

The scenario format is intentionally data-only.  No function, import path,
RPC method, URL, shell command, or opaque object can cross this boundary.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import StrEnum
from types import MappingProxyType
from typing import Any, Mapping, Sequence


COUNTERFACTUAL_ONLY = "COUNTERFACTUAL_ONLY"
SCENARIO_SCHEMA_VERSION = "counterfactual-scenario-v1"
EVIDENCE_SCHEMA_VERSION = "counterfactual-evidence-v1"
RUN_SCHEMA_VERSION = "counterfactual-run-v1"
_SHA256 = re.compile(r"[0-9a-f]{64}")
_IDENTIFIER = re.compile(r"[a-z][a-z0-9_-]{0,95}")
_ADDRESS = re.compile(r"0x[0-9a-fA-F]{40}")
_HEX = re.compile(r"0x(?:[0-9a-fA-F]{2})*")
_PRIVATE_KEY_TEXT = re.compile(r"0x[0-9a-fA-F]{64}")
_SECRET_KEY = re.compile(r"(?:secret|private|mnemonic|seed|password|auth(?:orization)?|api[_-]?key|token)", re.I)
_FORBIDDEN_KEY = re.compile(r"(?:callable|callback|module|command|rpc|url|method)", re.I)


class ScenarioValidationError(ValueError):
    """A data-only counterfactual scenario did not meet its schema."""


class LaboratoryAuthorityRefused(RuntimeError):
    """An attempt crossed from the laboratory into an authority domain."""


class BackendType(StrEnum):
    MODEL = "VENUE_MODEL"
    ANVIL = "EVM_ANVIL"


class RunStatus(StrEnum):
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    QUARANTINED = "QUARANTINED"
    BLOCKED = "BLOCKED"


MODEL_MUTATION_VERBS = frozenset({
    "set_balance", "set_position", "inject_external_position", "inject_open_order",
    "clear_open_orders", "set_mark_price", "set_metadata", "change_minimum_notional",
    "change_precision", "inject_partial_fill", "inject_duplicate_fill",
    "inject_out_of_order_fill", "inject_cancel_fill_race", "inject_submission_timeout",
    "inject_accepted_timeout", "inject_malformed_response", "inject_rate_limit",
    "inject_stale_positions", "inject_stale_orders", "inject_account_mismatch",
    "inject_transport_unavailable", "advance_time",
})
ANVIL_MUTATION_VERBS = frozenset({
    "set_native_balance", "set_nonce", "set_contract_code", "set_storage_slot",
    "advance_timestamp", "mine_block", "mine_blocks",
})
ASSERTION_VERBS = frozenset({
    "state_path_equals", "balance_equals", "position_equals", "open_order_count_equals",
    "fill_count_equals", "safety_state_equals", "chain_id_equals", "block_number_at_least",
    "code_equals", "storage_equals", "native_balance_equals",
})


def canonical_json(payload: object) -> str:
    """Return the one canonical representation used for all hashes."""
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)


def canonical_hash(payload: object) -> str:
    return hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()


def _required_identifier(value: object, field_name: str) -> str:
    if not isinstance(value, str) or _IDENTIFIER.fullmatch(value) is None:
        raise ScenarioValidationError(f"{field_name} must be a lowercase identifier.")
    return value


def _sha256(value: object, field_name: str) -> str:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise ScenarioValidationError(f"{field_name} must be a lowercase SHA-256 digest.")
    return value


def _bounded_text(value: object, field_name: str, *, maximum: int = 256) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > maximum:
        raise ScenarioValidationError(f"{field_name} must be non-empty bounded text.")
    if "://" in value:
        raise ScenarioValidationError(f"{field_name} may not contain a URL.")
    return value


def _freeze_json(value: object, field_name: str = "value", *, depth: int = 0) -> object:
    """Accept only small JSON values and make mappings/arrays immutable."""
    if depth > 12:
        raise ScenarioValidationError(f"{field_name} is nested too deeply.")
    if value is None or type(value) in {bool, int, str}:
        if isinstance(value, str) and len(value) > 4096:
            raise ScenarioValidationError(f"{field_name} text exceeds the bounded evidence limit.")
        if isinstance(value, str) and "://" in value:
            raise ScenarioValidationError(f"{field_name} may not contain a URL.")
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ScenarioValidationError(f"{field_name} must be finite JSON data.")
        return value
    if isinstance(value, Mapping):
        frozen: dict[str, object] = {}
        if len(value) > 128:
            raise ScenarioValidationError(f"{field_name} mapping has too many values.")
        for key, item in value.items():
            if not isinstance(key, str) or not key or len(key) > 96:
                raise ScenarioValidationError(f"{field_name} keys must be bounded text.")
            if _SECRET_KEY.search(key):
                raise ScenarioValidationError(f"{field_name} cannot accept secrets or wallet credentials.")
            if _FORBIDDEN_KEY.search(key):
                raise ScenarioValidationError(f"{field_name} cannot accept executable routing fields.")
            if key.lower() in {"actor", "wallet", "account", "credential"} and isinstance(item, str) and _PRIVATE_KEY_TEXT.fullmatch(item):
                raise ScenarioValidationError(f"{field_name} cannot accept wallet secret material.")
            frozen[key] = _freeze_json(item, f"{field_name}.{key}", depth=depth + 1)
        return MappingProxyType(dict(sorted(frozen.items())))
    if isinstance(value, (tuple, list)):
        if len(value) > 256:
            raise ScenarioValidationError(f"{field_name} sequence has too many values.")
        return tuple(_freeze_json(item, field_name, depth=depth + 1) for item in value)
    raise ScenarioValidationError(f"{field_name} must be serialized JSON data, not {type(value).__name__}.")


def thaw_json(value: object) -> object:
    if isinstance(value, Mapping):
        return {str(key): thaw_json(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [thaw_json(item) for item in value]
    return value


def validate_address(value: object, field_name: str = "address") -> str:
    if not isinstance(value, str) or _ADDRESS.fullmatch(value) is None:
        raise ScenarioValidationError(f"{field_name} must be a 20-byte Ethereum address.")
    return value.lower()


def validate_hex(value: object, field_name: str, *, exact_bytes: int | None = None, allow_empty: bool = True) -> str:
    if not isinstance(value, str) or _HEX.fullmatch(value) is None:
        raise ScenarioValidationError(f"{field_name} must be even-length hexadecimal bytes.")
    if not allow_empty and value == "0x":
        raise ScenarioValidationError(f"{field_name} may not be empty.")
    if exact_bytes is not None and len(value) != 2 + (exact_bytes * 2):
        raise ScenarioValidationError(f"{field_name} must be exactly {exact_bytes} bytes.")
    return value.lower()


def validate_uint(value: object, field_name: str, *, maximum: int = (2**256) - 1) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= maximum:
        raise ScenarioValidationError(f"{field_name} must be an unsigned bounded integer.")
    return value


def _expected_backend_verbs(backend: BackendType) -> frozenset[str]:
    return MODEL_MUTATION_VERBS if backend is BackendType.MODEL else ANVIL_MUTATION_VERBS


@dataclass(frozen=True)
class CounterfactualMutation:
    verb: str
    parameters: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _required_identifier(self.verb, "mutation verb")
        if not isinstance(self.parameters, Mapping):
            raise ScenarioValidationError("mutation parameters must be a JSON object.")
        object.__setattr__(self, "parameters", _freeze_json(self.parameters, "mutation parameters"))

    def payload(self) -> dict[str, object]:
        return {"verb": self.verb, "parameters": thaw_json(self.parameters)}


@dataclass(frozen=True)
class CounterfactualAssertion:
    verb: str
    parameters: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _required_identifier(self.verb, "assertion verb")
        if self.verb not in ASSERTION_VERBS:
            raise ScenarioValidationError(f"Unknown assertion verb: {self.verb}.")
        if not isinstance(self.parameters, Mapping):
            raise ScenarioValidationError("assertion parameters must be a JSON object.")
        object.__setattr__(self, "parameters", _freeze_json(self.parameters, "assertion parameters"))

    def payload(self) -> dict[str, object]:
        return {"verb": self.verb, "parameters": thaw_json(self.parameters)}


@dataclass(frozen=True)
class CounterfactualScenario:
    schema_version: str
    scenario_id: str
    scenario_version: str
    deterministic_seed: int
    backend: BackendType
    target_domain: str
    target_chain_id: int | None
    fixed_fork_block: int | None
    initial_state_fingerprint: str
    mutations: tuple[CounterfactualMutation, ...]
    assertions: tuple[CounterfactualAssertion, ...]
    timeout_seconds: int
    provenance: str = COUNTERFACTUAL_ONLY
    parent_scenario_hash: str | None = None
    mutation_delta: tuple[CounterfactualMutation, ...] = ()

    def __post_init__(self) -> None:
        if self.schema_version != SCENARIO_SCHEMA_VERSION:
            raise ScenarioValidationError("Unsupported counterfactual scenario schema.")
        _required_identifier(self.scenario_id, "scenario_id")
        _required_identifier(self.scenario_version, "scenario_version")
        validate_uint(self.deterministic_seed, "deterministic_seed", maximum=(2**63) - 1)
        if type(self.backend) is not BackendType:
            raise ScenarioValidationError("backend must be an explicit BackendType.")
        _bounded_text(self.target_domain, "target_domain", maximum=96)
        if self.backend is BackendType.ANVIL:
            if self.target_chain_id is None:
                raise ScenarioValidationError("EVM Anvil scenarios require an explicit target_chain_id.")
            validate_uint(self.target_chain_id, "target_chain_id", maximum=(2**63) - 1)
        elif self.target_chain_id is not None:
            raise ScenarioValidationError("Venue-model scenarios may not claim an EVM chain identity.")
        if self.fixed_fork_block is not None:
            validate_uint(self.fixed_fork_block, "fixed_fork_block", maximum=(2**63) - 1)
            if self.fixed_fork_block == 0:
                raise ScenarioValidationError("fixed_fork_block must be positive when supplied.")
        _sha256(self.initial_state_fingerprint, "initial_state_fingerprint")
        if type(self.mutations) is not tuple or not self.mutations:
            raise ScenarioValidationError("scenario mutations must be a non-empty immutable tuple.")
        if any(type(item) is not CounterfactualMutation for item in self.mutations):
            raise ScenarioValidationError("scenario mutations must be exact CounterfactualMutation values.")
        allowed = _expected_backend_verbs(self.backend)
        unknown = [item.verb for item in self.mutations if item.verb not in allowed]
        if unknown:
            raise ScenarioValidationError(f"Unknown {self.backend.value} mutation verb: {unknown[0]}.")
        if type(self.assertions) is not tuple or any(type(item) is not CounterfactualAssertion for item in self.assertions):
            raise ScenarioValidationError("scenario assertions must be an immutable assertion tuple.")
        if isinstance(self.timeout_seconds, bool) or not isinstance(self.timeout_seconds, int) or not 1 <= self.timeout_seconds <= 300:
            raise ScenarioValidationError("timeout_seconds must be an integer from 1 through 300.")
        if self.provenance != COUNTERFACTUAL_ONLY:
            raise LaboratoryAuthorityRefused("Laboratory scenarios must remain COUNTERFACTUAL_ONLY.")
        if self.parent_scenario_hash is not None:
            _sha256(self.parent_scenario_hash, "parent_scenario_hash")
        if type(self.mutation_delta) is not tuple or any(type(item) is not CounterfactualMutation for item in self.mutation_delta):
            raise ScenarioValidationError("mutation_delta must be an immutable mutation tuple.")
        if self.mutation_delta and self.parent_scenario_hash is None:
            raise ScenarioValidationError("mutation_delta requires a parent_scenario_hash.")

    def payload(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "scenario_id": self.scenario_id,
            "scenario_version": self.scenario_version,
            "deterministic_seed": self.deterministic_seed,
            "backend": self.backend.value,
            "target_domain": self.target_domain,
            "target_chain_id": self.target_chain_id,
            "fixed_fork_block": self.fixed_fork_block,
            "initial_state_fingerprint": self.initial_state_fingerprint,
            "mutations": [item.payload() for item in self.mutations],
            "assertions": [item.payload() for item in self.assertions],
            "timeout_seconds": self.timeout_seconds,
            "provenance": self.provenance,
            "parent_scenario_hash": self.parent_scenario_hash,
            "mutation_delta": [item.payload() for item in self.mutation_delta],
        }

    @property
    def scenario_hash(self) -> str:
        return canonical_hash(self.payload())


@dataclass(frozen=True)
class CounterfactualRunIdentity:
    run_id: str
    scenario_hash: str
    backend: BackendType
    started_at: str
    provenance: str = COUNTERFACTUAL_ONLY

    def __post_init__(self) -> None:
        try:
            uuid.UUID(self.run_id)
        except (ValueError, TypeError) as exc:
            raise ScenarioValidationError("run_id must be a UUID.") from exc
        _sha256(self.scenario_hash, "scenario_hash")
        if type(self.backend) is not BackendType or self.provenance != COUNTERFACTUAL_ONLY:
            raise LaboratoryAuthorityRefused("Run identity is not an exact counterfactual identity.")
        try:
            parsed = datetime.fromisoformat(self.started_at.replace("Z", "+00:00"))
        except (AttributeError, ValueError) as exc:
            raise ScenarioValidationError("started_at must be ISO-8601 UTC text.") from exc
        if parsed.tzinfo is None:
            raise ScenarioValidationError("started_at requires an explicit offset.")

    @classmethod
    def create(cls, scenario: CounterfactualScenario, *, now: datetime | None = None) -> "CounterfactualRunIdentity":
        if type(scenario) is not CounterfactualScenario:
            raise ScenarioValidationError("run identity requires an exact counterfactual scenario.")
        instant = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
        return cls(str(uuid.uuid4()), scenario.scenario_hash, scenario.backend, instant.isoformat().replace("+00:00", "Z"))

    def payload(self) -> dict[str, object]:
        return {
            "schema": RUN_SCHEMA_VERSION, "run_id": self.run_id, "scenario_hash": self.scenario_hash,
            "backend": self.backend.value, "started_at": self.started_at, "provenance": self.provenance,
        }


@dataclass(frozen=True)
class CounterfactualStateDiff:
    path: str
    before: object
    after: object

    def __post_init__(self) -> None:
        _bounded_text(self.path, "state diff path", maximum=256)
        object.__setattr__(self, "before", _freeze_json(self.before, "state diff before"))
        object.__setattr__(self, "after", _freeze_json(self.after, "state diff after"))

    def payload(self) -> dict[str, object]:
        return {"path": self.path, "before": thaw_json(self.before), "after": thaw_json(self.after)}


@dataclass(frozen=True)
class CounterfactualEvidence:
    scenario_hash: str
    backend: BackendType
    toolchain_version: str
    initial_fingerprint: str
    mutations_applied: tuple[str, ...]
    assertions: tuple[str, ...]
    state_diff: tuple[CounterfactualStateDiff, ...]
    cleanup_status: str
    restoration_verified: bool
    run_status: RunStatus
    restoration_evidence: Mapping[str, object] = field(default_factory=dict)
    fork_chain_id: int | None = None
    fork_block: int | None = None
    bounded_diagnostics: Mapping[str, object] = field(default_factory=dict)
    provenance: str = COUNTERFACTUAL_ONLY

    def __post_init__(self) -> None:
        _sha256(self.scenario_hash, "evidence scenario_hash")
        if type(self.backend) is not BackendType or type(self.run_status) is not RunStatus:
            raise ScenarioValidationError("evidence backend and status must be explicit enums.")
        _bounded_text(self.toolchain_version, "toolchain_version")
        _sha256(self.initial_fingerprint, "evidence initial_fingerprint")
        if type(self.mutations_applied) is not tuple or any(not isinstance(item, str) for item in self.mutations_applied):
            raise ScenarioValidationError("mutations_applied must be immutable text.")
        if type(self.assertions) is not tuple or any(not isinstance(item, str) for item in self.assertions):
            raise ScenarioValidationError("assertions must be immutable text.")
        if type(self.state_diff) is not tuple or any(type(item) is not CounterfactualStateDiff for item in self.state_diff):
            raise ScenarioValidationError("state_diff must be immutable bounded diff values.")
        _bounded_text(self.cleanup_status, "cleanup_status")
        if type(self.restoration_verified) is not bool or self.provenance != COUNTERFACTUAL_ONLY:
            raise LaboratoryAuthorityRefused("Evidence must be exact COUNTERFACTUAL_ONLY data.")
        if self.fork_chain_id is not None:
            validate_uint(self.fork_chain_id, "fork_chain_id", maximum=(2**63) - 1)
        if self.fork_block is not None:
            validate_uint(self.fork_block, "fork_block", maximum=(2**63) - 1)
        if not isinstance(self.bounded_diagnostics, Mapping):
            raise ScenarioValidationError("bounded_diagnostics must be a JSON object.")
        object.__setattr__(self, "bounded_diagnostics", _freeze_json(self.bounded_diagnostics, "bounded_diagnostics"))
        if not isinstance(self.restoration_evidence, Mapping):
            raise ScenarioValidationError("restoration_evidence must be a JSON object.")
        object.__setattr__(self, "restoration_evidence", _freeze_json(self.restoration_evidence, "restoration_evidence"))

    def payload(self) -> dict[str, object]:
        return {
            "schema": EVIDENCE_SCHEMA_VERSION, "scenario_hash": self.scenario_hash, "backend": self.backend.value,
            "toolchain_version": self.toolchain_version, "fork_chain_id": self.fork_chain_id,
            "fork_block": self.fork_block, "initial_fingerprint": self.initial_fingerprint,
            "mutations_applied": list(self.mutations_applied), "assertions": list(self.assertions),
            "state_diff": [item.payload() for item in self.state_diff], "cleanup_status": self.cleanup_status,
            "restoration_verified": self.restoration_verified, "run_status": self.run_status.value,
            "restoration_evidence": thaw_json(self.restoration_evidence),
            "bounded_diagnostics": thaw_json(self.bounded_diagnostics), "provenance": self.provenance,
        }

    @property
    def evidence_hash(self) -> str:
        return canonical_hash(self.payload())


@dataclass(frozen=True)
class CounterfactualRunResult:
    identity: CounterfactualRunIdentity
    evidence: CounterfactualEvidence
    outcome: str
    provider_artifacts: Mapping[str, str] = field(default_factory=dict, repr=False, compare=False)

    def __post_init__(self) -> None:
        if type(self.identity) is not CounterfactualRunIdentity or type(self.evidence) is not CounterfactualEvidence:
            raise ScenarioValidationError("Counterfactual results require exact immutable contracts.")
        if self.identity.scenario_hash != self.evidence.scenario_hash or self.identity.backend is not self.evidence.backend:
            raise ScenarioValidationError("Result identity/evidence mismatch.")
        _bounded_text(self.outcome, "outcome")
        allowed_artifacts = {
            "raw_dump_before.txt", "raw_dump_after.txt", "raw_dump_structural_diff.json",
            "mutation_witness_manifest.json", "semantic_state_before.json", "semantic_state_after.json",
        }
        if not isinstance(self.provider_artifacts, Mapping) or set(self.provider_artifacts) - allowed_artifacts:
            raise ScenarioValidationError("Provider artifacts contain an unrecognized file.")
        artifacts: dict[str, str] = {}
        for name, content in self.provider_artifacts.items():
            if not isinstance(content, str) or not content or len(content.encode("utf-8")) > 64 * 1024 * 1024:
                raise ScenarioValidationError("Provider artifact content is malformed or unbounded.")
            artifacts[name] = content
        object.__setattr__(self, "provider_artifacts", MappingProxyType(artifacts))

    def payload(self) -> dict[str, object]:
        return {"identity": self.identity.payload(), "evidence": self.evidence.payload(), "outcome": self.outcome}


def forbid_authoritative_conversion(value: object) -> None:
    """A single explicit runtime guard for future test harnesses.

    The current f4 phase deliberately implements no Trader harness.  Keeping
    this public guard makes accidental bridge adaptation fail closed.
    """
    if type(value) in {CounterfactualEvidence, CounterfactualRunResult, CounterfactualScenario}:
        raise LaboratoryAuthorityRefused("COUNTERFACTUAL_ONLY material cannot become an authoritative request.")
