"""Fail-closed constitutional boundary for Phase F.0 Lane II.

F.0 creates no strategy, signal, execution adapter, venue transport, or
capital authority.  The contracts below deliberately carry only provenance
hashes rather than scientific artifacts or raw outcome-bearing payloads.
They are therefore safe to import from future Trader code without importing
the Phase E outcome capability or the Phase D execution engine.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import StrEnum
from typing import Iterable, Sequence


F0_BOUNDARY_SCHEMA = "phase-f0-lane-ii-authority-boundary-v1"
F0_BOUNDARY_VERSION = "phase-f0-lane-ii-boundary-v1"


class BoundaryError(RuntimeError):
    """Base error for a denied constitutional-boundary operation."""


class AuthorityRefused(BoundaryError):
    """An authority is absent, ambiguous, or owned by another principal."""


class InputProvenanceRefused(AuthorityRefused):
    """An input is not a permitted, provenance-bearing operational input."""


class StrategyProvenanceRefused(AuthorityRefused):
    """A strategy does not have a separate immutable operational identity."""


class TradeIntentRefused(AuthorityRefused):
    """A Lane II trade-intent request was not separately commissioned."""


class ExecutionAuthorityRefused(AuthorityRefused):
    """A Lane II request attempted to act as Phase D execution authority."""


class LaneIdentity(StrEnum):
    """The two identities whose capabilities must never be substituted."""

    SCIENTIFIC_LANE = "SCIENTIFIC_LANE"
    TRADER_LANE = "TRADER_LANE"


class AuthorityOwner(StrEnum):
    """Named owners for authority records, including Phase D sovereignty."""

    SCIENTIFIC_LANE = LaneIdentity.SCIENTIFIC_LANE.value
    TRADER_LANE = LaneIdentity.TRADER_LANE.value
    PHASE_D_EXECUTION_SOVEREIGN = "PHASE_D_EXECUTION_SOVEREIGN"


class AuthorityCapability(StrEnum):
    SCIENTIFIC_EVALUATION = "SCIENTIFIC_EVALUATION"
    PREDICTION = "PREDICTION"
    SIGNAL = "SIGNAL"
    EXECUTION = "EXECUTION"
    TRADING = "TRADING"
    LIVE_CAPITAL = "LIVE_CAPITAL"


class AuthorityState(StrEnum):
    """A capability state is explicit; object existence is never authority."""

    DENIED = "DENIED"
    PROTOCOL_GATED = "PROTOCOL_GATED"
    PHASE_D_SOVEREIGN = "PHASE_D_SOVEREIGN"
    GRANTED = "GRANTED"


class OperationalInputSource(StrEnum):
    """The only source classes that a future F.1 Trader may present to F.0."""

    LIVE_PUBLIC_MARKET_DATA = "LIVE_PUBLIC_MARKET_DATA"
    LIVE_PUBLIC_WALLET_ACTIVITY = "LIVE_PUBLIC_WALLET_ACTIVITY"
    PHASE_ABC_OPERATIONAL_OBSERVATION = "PHASE_ABC_OPERATIONAL_OBSERVATION"
    PHASE_D_MARKET_TIMESTAMP = "PHASE_D_MARKET_TIMESTAMP"
    OPERATIONAL_INDICATOR = "OPERATIONAL_INDICATOR"
    CONFIGURATION_OR_RISK_POLICY = "CONFIGURATION_OR_RISK_POLICY"
    CURRENT_ACCOUNT_OR_EXECUTION_STATE = "CURRENT_ACCOUNT_OR_EXECUTION_STATE"
    INDEPENDENTLY_APPROVED_STRATEGY_ARTIFACT = "INDEPENDENTLY_APPROVED_STRATEGY_ARTIFACT"


class TradeDirection(StrEnum):
    LONG = "LONG"
    SHORT = "SHORT"


def _canonical_hash(payload: object) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _required_text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} is required.")
    return value


def _normalized_utc(value: object, field: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field} must be ISO-8601 text with an explicit offset.")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{field} must be ISO-8601 text with an explicit offset.") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"{field} must have an explicit UTC offset.")
    return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _sha256(value: object, field: str) -> str:
    if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise ValueError(f"{field} must be a lowercase SHA-256 digest.")
    return value


@dataclass(frozen=True)
class AuthorityRecord:
    """One explicit capability decision, bound to an owner and review basis."""

    owner: AuthorityOwner
    capability: AuthorityCapability
    state: AuthorityState
    basis: str

    def __post_init__(self) -> None:
        if type(self.owner) is not AuthorityOwner:
            raise ValueError("Authority owner must be an explicit AuthorityOwner.")
        if type(self.capability) is not AuthorityCapability:
            raise ValueError("Authority capability must be an explicit AuthorityCapability.")
        if type(self.state) is not AuthorityState:
            raise ValueError("Authority state must be an explicit AuthorityState.")
        _required_text(self.basis, "Authority basis")

    @property
    def granted(self) -> bool:
        return self.state is AuthorityState.GRANTED

    def payload(self) -> dict[str, str]:
        return {
            "owner": self.owner.value,
            "capability": self.capability.value,
            "state": self.state.value,
            "basis": self.basis,
        }


@dataclass(frozen=True)
class AuthorityManifest:
    """Immutable F.0 authority snapshot used in every deterministic decision."""

    schema: str
    version: str
    records: tuple[AuthorityRecord, ...]

    def __post_init__(self) -> None:
        if self.schema != F0_BOUNDARY_SCHEMA or self.version != F0_BOUNDARY_VERSION:
            raise ValueError("Unsupported Lane II authority manifest.")
        if not isinstance(self.records, tuple) or not self.records:
            raise ValueError("Authority manifest records must be a non-empty tuple.")
        keys = [(record.owner, record.capability) for record in self.records]
        if len(keys) != len(set(keys)):
            raise ValueError("Authority manifest may not contain duplicate owner/capability decisions.")

    def record_for(self, owner: AuthorityOwner, capability: AuthorityCapability) -> AuthorityRecord:
        if type(owner) is not AuthorityOwner or type(capability) is not AuthorityCapability:
            raise AuthorityRefused("Authority lookup requires explicit owner and capability identities.")
        for record in self.records:
            if record.owner is owner and record.capability is capability:
                return record
        raise AuthorityRefused(f"No explicit authority record exists for {owner.value}/{capability.value}.")

    def payload(self) -> dict[str, object]:
        return {
            "schema": self.schema,
            "version": self.version,
            "records": [
                record.payload()
                for record in sorted(self.records, key=lambda item: (item.owner.value, item.capability.value))
            ],
        }

    @property
    def manifest_hash(self) -> str:
        return _canonical_hash(self.payload())


def _f0_records() -> tuple[AuthorityRecord, ...]:
    records: list[AuthorityRecord] = [
        AuthorityRecord(
            AuthorityOwner.SCIENTIFIC_LANE,
            AuthorityCapability.SCIENTIFIC_EVALUATION,
            AuthorityState.PROTOCOL_GATED,
            "E5_FROZEN_PROTOCOL_GATES_ONLY",
        ),
        AuthorityRecord(
            AuthorityOwner.PHASE_D_EXECUTION_SOVEREIGN,
            AuthorityCapability.EXECUTION,
            AuthorityState.PHASE_D_SOVEREIGN,
            "PHASE_D_INDEPENDENT_RISK_SAFETY_AND_EXECUTION",
        ),
    ]
    for capability in AuthorityCapability:
        if capability is not AuthorityCapability.SCIENTIFIC_EVALUATION:
            records.append(AuthorityRecord(
                AuthorityOwner.SCIENTIFIC_LANE, capability, AuthorityState.DENIED,
                "SCIENTIFIC_LANE_HAS_NO_OPERATIONAL_AUTHORITY",
            ))
        records.append(AuthorityRecord(
            AuthorityOwner.TRADER_LANE, capability, AuthorityState.DENIED,
            "F0_NO_LANE_II_AUTHORITY_HAS_BEEN_COMMISSIONED",
        ))
    return tuple(records)


F0_MANIFEST = AuthorityManifest(F0_BOUNDARY_SCHEMA, F0_BOUNDARY_VERSION, _f0_records())
F0_AUTHORITY_MANIFEST_HASH = F0_MANIFEST.manifest_hash


def _require_f0_manifest(manifest: object) -> AuthorityManifest:
    if type(manifest) is not AuthorityManifest:
        raise AuthorityRefused("Lane II authority manifest is missing or ambiguous.")
    if manifest.manifest_hash != F0_AUTHORITY_MANIFEST_HASH:
        raise AuthorityRefused("Lane II authority manifest is not the frozen F.0 manifest.")
    return manifest


@dataclass(frozen=True)
class OperationalInput:
    """An allowed input reference; raw content is deliberately out of contract."""

    input_id: str
    source: OperationalInputSource
    observed_at: str
    payload_hash: str
    source_system: str

    def __post_init__(self) -> None:
        _required_text(self.input_id, "Input identity")
        if type(self.source) is not OperationalInputSource:
            raise ValueError("Operational input source must be an approved source class.")
        _normalized_utc(self.observed_at, "Input observation time")
        _sha256(self.payload_hash, "Input payload hash")
        _required_text(self.source_system, "Input source system")

    def payload(self) -> dict[str, str]:
        return {
            "input_id": self.input_id,
            "source": self.source.value,
            "observed_at": _normalized_utc(self.observed_at, "Input observation time"),
            "payload_hash": self.payload_hash,
            "source_system": self.source_system,
        }

    @property
    def provenance_hash(self) -> str:
        return _canonical_hash(self.payload())


@dataclass(frozen=True)
class OperationalStrategyArtifact:
    """Immutable operational identity distinct from every scientific hypothesis."""

    strategy_id: str
    strategy_version: str
    strategy_artifact_hash: str
    allowed_input_sources: tuple[OperationalInputSource, ...]

    def __post_init__(self) -> None:
        strategy_id = _required_text(self.strategy_id, "Strategy identity")
        if not strategy_id.startswith("trader-"):
            raise StrategyProvenanceRefused("Operational strategy identity must begin with 'trader-'.")
        if strategy_id.lower().startswith(("e5", "e6", "hypothesis", "scientific", "wallet-action")):
            raise StrategyProvenanceRefused("Scientific identities cannot be substituted for operational strategy identities.")
        _required_text(self.strategy_version, "Strategy version")
        _sha256(self.strategy_artifact_hash, "Strategy artifact hash")
        if not isinstance(self.allowed_input_sources, tuple) or not self.allowed_input_sources:
            raise StrategyProvenanceRefused("Strategy input provenance must be a non-empty immutable tuple.")
        if any(type(source) is not OperationalInputSource for source in self.allowed_input_sources):
            raise StrategyProvenanceRefused("Strategy input provenance includes an unknown source class.")
        if len(set(self.allowed_input_sources)) != len(self.allowed_input_sources):
            raise StrategyProvenanceRefused("Strategy input provenance may not contain duplicate source classes.")

    def payload(self) -> dict[str, object]:
        return {
            "strategy_id": self.strategy_id,
            "strategy_version": self.strategy_version,
            "strategy_artifact_hash": self.strategy_artifact_hash,
            "allowed_input_sources": [source.value for source in self.allowed_input_sources],
        }

    @property
    def strategy_identity(self) -> str:
        return "trader-strategy-" + _canonical_hash(self.payload())[:32]


@dataclass(frozen=True)
class StrategyRegistration:
    """A future commissioning record; F.0 permits only an explicit denial."""

    strategy_identity: str
    strategy_version: str
    signal_authority: AuthorityRecord

    def __post_init__(self) -> None:
        _required_text(self.strategy_identity, "Registered strategy identity")
        _required_text(self.strategy_version, "Registered strategy version")
        if (
            self.signal_authority.owner is not AuthorityOwner.TRADER_LANE
            or self.signal_authority.capability is not AuthorityCapability.SIGNAL
        ):
            raise ValueError("Strategy registration must carry a Lane II signal-authority decision.")
        if self.signal_authority.granted:
            raise ValueError("F.0 cannot register signal authority; a successor commissioning is required.")


@dataclass(frozen=True)
class StrategyAuthorityRegistry:
    """An immutable registry prevents strategy existence from implying authority."""

    registrations: tuple[StrategyRegistration, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.registrations, tuple):
            raise ValueError("Strategy registrations must be immutable.")
        keys = [(item.strategy_identity, item.strategy_version) for item in self.registrations]
        if len(keys) != len(set(keys)):
            raise ValueError("Strategy registrations may not duplicate an identity/version pair.")

    def find(self, strategy: OperationalStrategyArtifact) -> StrategyRegistration | None:
        if type(strategy) is not OperationalStrategyArtifact:
            return None
        for registration in self.registrations:
            if (
                registration.strategy_identity == strategy.strategy_identity
                and registration.strategy_version == strategy.strategy_version
            ):
                return registration
        return None


def f0_strategy_registry() -> StrategyAuthorityRegistry:
    """F.0 starts with no commissioned Lane II strategy versions."""
    return StrategyAuthorityRegistry()


@dataclass(frozen=True)
class AuthorityEvaluation:
    """Immutable, replayable fail-closed decision made before creating an intent."""

    allowed: bool
    reason_code: str
    manifest_hash: str
    strategy_identity: str | None
    strategy_version: str | None
    input_provenance_hashes: tuple[str, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.allowed, bool):
            raise ValueError("Authority decision must be boolean.")
        _required_text(self.reason_code, "Authority decision reason")
        _sha256(self.manifest_hash, "Authority manifest hash")
        if self.strategy_identity is not None:
            _required_text(self.strategy_identity, "Authority decision strategy identity")
        if self.strategy_version is not None:
            _required_text(self.strategy_version, "Authority decision strategy version")
        if not isinstance(self.input_provenance_hashes, tuple):
            raise ValueError("Authority input provenance must be immutable.")
        for item in self.input_provenance_hashes:
            _sha256(item, "Authority input provenance hash")

    def payload(self) -> dict[str, object]:
        return {
            "schema": "phase-f0-authority-evaluation-v1",
            "allowed": self.allowed,
            "reason_code": self.reason_code,
            "manifest_hash": self.manifest_hash,
            "strategy_identity": self.strategy_identity,
            "strategy_version": self.strategy_version,
            "input_provenance_hashes": list(self.input_provenance_hashes),
        }

    @property
    def decision_hash(self) -> str:
        return _canonical_hash(self.payload())


def _denied(
    reason_code: str,
    *,
    manifest_hash: str,
    strategy: OperationalStrategyArtifact | None = None,
    inputs: Iterable[OperationalInput] = (),
) -> AuthorityEvaluation:
    return AuthorityEvaluation(
        allowed=False,
        reason_code=reason_code,
        manifest_hash=manifest_hash,
        strategy_identity=strategy.strategy_identity if strategy is not None else None,
        strategy_version=strategy.strategy_version if strategy is not None else None,
        input_provenance_hashes=tuple(item.provenance_hash for item in inputs),
    )


def evaluate_lane_ii_authority(
    strategy: object,
    inputs: Sequence[object],
    *,
    registry: object | None = None,
    manifest: object = F0_MANIFEST,
) -> AuthorityEvaluation:
    """Assess a future intent request without reading protected scientific data.

    Inputs are checked by exact type before their fields are read.  Passing an
    E.5/E.6 repository, result object, callback, mapping, or any other
    scientific capability therefore yields a deterministic refusal without
    invoking it.
    """
    try:
        checked_manifest = _require_f0_manifest(manifest)
    except AuthorityRefused:
        return _denied("AUTHORITY_MANIFEST_AMBIGUOUS", manifest_hash=F0_AUTHORITY_MANIFEST_HASH)

    if type(strategy) is not OperationalStrategyArtifact:
        return _denied("STRATEGY_PROVENANCE_MISSING", manifest_hash=checked_manifest.manifest_hash)
    checked_strategy = strategy
    if type(inputs) not in {tuple, list}:
        return _denied("INPUT_PROVENANCE_MISSING", manifest_hash=checked_manifest.manifest_hash, strategy=checked_strategy)
    checked_inputs: list[OperationalInput] = []
    for item in inputs:
        if type(item) is not OperationalInput:
            return _denied(
                "PROTECTED_OR_UNKNOWN_INPUT_CAPABILITY",
                manifest_hash=checked_manifest.manifest_hash,
                strategy=checked_strategy,
                inputs=checked_inputs,
            )
        if item.source not in checked_strategy.allowed_input_sources:
            return _denied(
                "INPUT_SOURCE_NOT_DECLARED_BY_STRATEGY",
                manifest_hash=checked_manifest.manifest_hash,
                strategy=checked_strategy,
                inputs=checked_inputs,
            )
        checked_inputs.append(item)
    if not checked_inputs:
        return _denied("INPUT_PROVENANCE_MISSING", manifest_hash=checked_manifest.manifest_hash, strategy=checked_strategy)
    if type(registry) is not StrategyAuthorityRegistry:
        return _denied(
            "STRATEGY_REGISTRY_AMBIGUOUS",
            manifest_hash=checked_manifest.manifest_hash,
            strategy=checked_strategy,
            inputs=checked_inputs,
        )
    registration = registry.find(checked_strategy)
    if registration is None:
        return _denied(
            "UNKNOWN_STRATEGY_VERSION",
            manifest_hash=checked_manifest.manifest_hash,
            strategy=checked_strategy,
            inputs=checked_inputs,
        )
    trader_signal = checked_manifest.record_for(AuthorityOwner.TRADER_LANE, AuthorityCapability.SIGNAL)
    if not trader_signal.granted or not registration.signal_authority.granted:
        return _denied(
            "SIGNAL_AUTHORITY_NOT_COMMISSIONED",
            manifest_hash=checked_manifest.manifest_hash,
            strategy=checked_strategy,
            inputs=checked_inputs,
        )
    # This branch is deliberately unreachable in F.0.  It is retained so a
    # successor commissioning has one explicit point to replace after review.
    return AuthorityEvaluation(
        allowed=True,
        reason_code="SIGNAL_AUTHORITY_COMMISSIONED",
        manifest_hash=checked_manifest.manifest_hash,
        strategy_identity=checked_strategy.strategy_identity,
        strategy_version=checked_strategy.strategy_version,
        input_provenance_hashes=tuple(item.provenance_hash for item in checked_inputs),
    )


@dataclass(frozen=True)
class TradeIntentRequest:
    """Bounded request contract; it has no execution or live-capital power."""

    strategy_id: str
    strategy_version: str
    strategy_identity: str
    symbol: str
    direction: TradeDirection
    requested_notional_ceiling: float
    created_at: str
    expires_at: str
    authority_decision_hash: str
    input_provenance_hashes: tuple[str, ...]
    exit_policy_ref: str
    risk_policy_ref: str

    def __post_init__(self) -> None:
        strategy_id = _required_text(self.strategy_id, "Trade intent strategy identity")
        if not strategy_id.startswith("trader-"):
            raise StrategyProvenanceRefused("Trade intent must name a separate trader strategy identity.")
        _required_text(self.strategy_version, "Trade intent strategy version")
        strategy_identity = _required_text(self.strategy_identity, "Trade intent strategy immutable identity")
        if not strategy_identity.startswith("trader-strategy-"):
            raise StrategyProvenanceRefused("Trade intent cannot substitute a scientific identity for strategy provenance.")
        _required_text(self.symbol, "Trade intent symbol")
        if type(self.direction) is not TradeDirection:
            raise ValueError("Trade intent direction must be explicit.")
        if isinstance(self.requested_notional_ceiling, bool) or not isinstance(self.requested_notional_ceiling, (int, float)):
            raise ValueError("Trade intent requested notional ceiling must be numeric.")
        if not math.isfinite(float(self.requested_notional_ceiling)) or self.requested_notional_ceiling <= 0:
            raise ValueError("Trade intent requested notional ceiling must be positive and finite.")
        if _normalized_utc(self.expires_at, "Trade intent expiry") <= _normalized_utc(self.created_at, "Trade intent creation time"):
            raise ValueError("Trade intent expiry must be later than its creation time.")
        _sha256(self.authority_decision_hash, "Trade intent authority decision hash")
        if not isinstance(self.input_provenance_hashes, tuple) or not self.input_provenance_hashes:
            raise ValueError("Trade intent must carry immutable input provenance.")
        for item in self.input_provenance_hashes:
            _sha256(item, "Trade intent input provenance hash")
        _required_text(self.exit_policy_ref, "Trade intent exit-policy reference")
        _required_text(self.risk_policy_ref, "Trade intent risk-policy reference")

    @property
    def execution_authority(self) -> bool:
        return False

    @property
    def live_capital_authority(self) -> bool:
        return False

    def payload(self) -> dict[str, object]:
        return {
            "schema": "phase-f0-trade-intent-request-v1",
            "strategy_id": self.strategy_id,
            "strategy_version": self.strategy_version,
            "strategy_identity": self.strategy_identity,
            "symbol": self.symbol,
            "direction": self.direction.value,
            "requested_notional_ceiling": float(self.requested_notional_ceiling),
            "created_at": _normalized_utc(self.created_at, "Trade intent creation time"),
            "expires_at": _normalized_utc(self.expires_at, "Trade intent expiry"),
            "authority_decision_hash": self.authority_decision_hash,
            "input_provenance_hashes": list(self.input_provenance_hashes),
            "exit_policy_ref": self.exit_policy_ref,
            "risk_policy_ref": self.risk_policy_ref,
            "execution_authority": False,
            "live_capital_authority": False,
        }

    @property
    def intent_id(self) -> str:
        return "trader-intent-" + _canonical_hash(self.payload())[:32]


def create_trade_intent(
    strategy: object,
    inputs: Sequence[object],
    *,
    symbol: str,
    direction: TradeDirection,
    requested_notional_ceiling: float,
    created_at: str,
    expires_at: str,
    exit_policy_ref: str,
    risk_policy_ref: str,
    registry: object | None = None,
    manifest: object = F0_MANIFEST,
) -> TradeIntentRequest:
    """Create an intent only after separately commissioned signal authority.

    Under the frozen F.0 manifest this always raises.  The callable exists to
    make the future F.1 commissioning seam explicit and testable rather than
    allowing a strategy object to be mistaken for trading authority.
    """
    evaluation = evaluate_lane_ii_authority(strategy, inputs, registry=registry, manifest=manifest)
    if not evaluation.allowed:
        raise TradeIntentRefused(evaluation.reason_code)
    assert type(strategy) is OperationalStrategyArtifact  # guarded above; narrows the immutable contract.
    return TradeIntentRequest(
        strategy_id=strategy.strategy_id,
        strategy_version=strategy.strategy_version,
        strategy_identity=strategy.strategy_identity,
        symbol=symbol,
        direction=direction,
        requested_notional_ceiling=requested_notional_ceiling,
        created_at=created_at,
        expires_at=expires_at,
        authority_decision_hash=evaluation.decision_hash,
        input_provenance_hashes=evaluation.input_provenance_hashes,
        exit_policy_ref=exit_policy_ref,
        risk_policy_ref=risk_policy_ref,
    )


def request_phase_d_execution(intent: object) -> None:
    """Refuse direct execution: only a separately commissioned Phase D bridge may act."""
    if type(intent) is not TradeIntentRequest:
        raise ExecutionAuthorityRefused("Only a bounded Lane II trade-intent request may be handed to Phase D.")
    raise ExecutionAuthorityRefused(
        "A Lane II trade-intent request grants no execution authority; Phase D remains the sole execution sovereign."
    )
