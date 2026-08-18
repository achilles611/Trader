"""Phase F.1 commissioning for the one Lane II Trader V0 strategy.

This module is deliberately an additive successor to :mod:`boundary`.  It
does not alter the frozen F.0 manifest, import Phase E, or contact Phase D.
Trader V0 may only issue deterministic simulation/shadow *requests*; the
``TradeIntentRequest`` it produces has no execution or live-capital power.
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import StrEnum
from typing import Iterable, Sequence

from .boundary import (
    F0_AUTHORITY_MANIFEST_HASH,
    F0_MANIFEST,
    AuthorityCapability,
    AuthorityOwner,
    AuthorityRecord,
    AuthorityState,
    OperationalInput,
    OperationalInputSource,
    OperationalStrategyArtifact,
    TradeDirection,
    TradeIntentRefused,
    TradeIntentRequest,
)


F1_SCHEMA = "phase-f1-trader-v0-commissioning-v1"
TRADER_V0_AUTHORITY_BASIS = "TRADER_V0_OPERATIONAL_SIMULATION_SHADOW_COMMISSIONING"
TRADER_V0_STRATEGY_ID = "trader-v0"
TRADER_V0_STRATEGY_VERSION = "1"


class F1AuthorityRefused(RuntimeError):
    """The successor commissioning is absent, altered, or out of scope."""


def _canonical_hash(payload: object) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _normalized_utc(value: object) -> str:
    if not isinstance(value, str):
        raise ValueError("Timestamp must be ISO-8601 text with an explicit offset.")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("Timestamp must be ISO-8601 text with an explicit offset.") from exc
    if parsed.tzinfo is None:
        raise ValueError("Timestamp must include an explicit UTC offset.")
    return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _as_utc(value: object) -> datetime:
    return datetime.fromisoformat(_normalized_utc(value).replace("Z", "+00:00"))


def _finite_number(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    number = float(value)
    return number if math.isfinite(number) else None


def _safe_value(value: object) -> object:
    """Make a deterministic fingerprint without invoking hostile objects."""
    if value is None or type(value) in {bool, int, str}:
        return value
    if type(value) is float:
        return value if math.isfinite(value) else {"invalid_float": repr(value)}
    if type(value) is TradeDirection:
        return value.value
    return {"unsupported_type": f"{type(value).__module__}.{type(value).__qualname__}"}


# This payload is the complete frozen semantics of the only F.1 strategy.  The
# hash deliberately excludes only its self-referential ``strategy_artifact_hash``
# field, which is then embedded in the externally auditable artifact document.
TRADER_V0_ARTIFACT_PAYLOAD: dict[str, object] = {
    "schema": "phase-f1-trader-v0-strategy-artifact-v1",
    "strategy_id": TRADER_V0_STRATEGY_ID,
    "strategy_version": TRADER_V0_STRATEGY_VERSION,
    "authority_basis": TRADER_V0_AUTHORITY_BASIS,
    "scope": "SIMULATION_SHADOW_TRADE_INTENT_REQUEST_ONLY",
    "allowed_input_sources": [
        OperationalInputSource.CONFIGURATION_OR_RISK_POLICY.value,
        OperationalInputSource.CURRENT_ACCOUNT_OR_EXECUTION_STATE.value,
        OperationalInputSource.LIVE_PUBLIC_MARKET_DATA.value,
        OperationalInputSource.LIVE_PUBLIC_WALLET_ACTIVITY.value,
        OperationalInputSource.OPERATIONAL_INDICATOR.value,
    ],
    "decision_policy": {
        "entry_effective_confidence": 0.60,
        "entry_signal_ttl_seconds": 10.0,
        "market_evidence_ttl_seconds": 10.0,
        "require_positive_alpha_survival": True,
        "require_positive_net_edge_after_friction": True,
        "source_wallet_action_is_evidence_only": True,
    },
    "exit_policy": {
        "exit_effective_confidence": 0.52,
        "maximum_position_age_seconds": 600.0,
        "exit_on_hard_risk": True,
        "exit_on_input_integrity_failure": True,
        "exit_on_non_positive_net_edge": True,
        "exit_on_regime_invalidation": True,
    },
    "risk_policy": {
        "maximum_requested_notional_ceiling": 1000.0,
        "source_wallet_leverage_is_sizing_authority": False,
    },
}
_TRADER_V0_DECISION_POLICY = TRADER_V0_ARTIFACT_PAYLOAD["decision_policy"]
_TRADER_V0_EXIT_POLICY = TRADER_V0_ARTIFACT_PAYLOAD["exit_policy"]
_TRADER_V0_RISK_POLICY = TRADER_V0_ARTIFACT_PAYLOAD["risk_policy"]
assert isinstance(_TRADER_V0_DECISION_POLICY, dict)
assert isinstance(_TRADER_V0_EXIT_POLICY, dict)
assert isinstance(_TRADER_V0_RISK_POLICY, dict)
TRADER_V0_ENTRY_EFFECTIVE_CONFIDENCE = float(_TRADER_V0_DECISION_POLICY["entry_effective_confidence"])
TRADER_V0_EXIT_EFFECTIVE_CONFIDENCE = float(_TRADER_V0_EXIT_POLICY["exit_effective_confidence"])
TRADER_V0_ENTRY_SIGNAL_TTL_SECONDS = float(_TRADER_V0_DECISION_POLICY["entry_signal_ttl_seconds"])
TRADER_V0_MARKET_EVIDENCE_TTL_SECONDS = float(_TRADER_V0_DECISION_POLICY["market_evidence_ttl_seconds"])
TRADER_V0_MAXIMUM_POSITION_AGE_SECONDS = float(_TRADER_V0_EXIT_POLICY["maximum_position_age_seconds"])
TRADER_V0_MAXIMUM_REQUESTED_NOTIONAL_CEILING = float(_TRADER_V0_RISK_POLICY["maximum_requested_notional_ceiling"])
TRADER_V0_ARTIFACT_HASH = _canonical_hash(TRADER_V0_ARTIFACT_PAYLOAD)
TRADER_V0_ALLOWED_INPUT_SOURCES = tuple(
    OperationalInputSource(value) for value in TRADER_V0_ARTIFACT_PAYLOAD["allowed_input_sources"]  # type: ignore[arg-type]
)
TRADER_V0_STRATEGY = OperationalStrategyArtifact(
    strategy_id=TRADER_V0_STRATEGY_ID,
    strategy_version=TRADER_V0_STRATEGY_VERSION,
    strategy_artifact_hash=TRADER_V0_ARTIFACT_HASH,
    allowed_input_sources=TRADER_V0_ALLOWED_INPUT_SOURCES,
)
TRADER_V0_EXIT_POLICY_REF = "trader-v0-exit-policy-" + _canonical_hash(TRADER_V0_ARTIFACT_PAYLOAD["exit_policy"])[:32]
TRADER_V0_RISK_POLICY_REF = "trader-v0-risk-policy-" + _canonical_hash(TRADER_V0_ARTIFACT_PAYLOAD["risk_policy"])[:32]


@dataclass(frozen=True)
class F1AuthorityManifest:
    """Separate successor authority that anchors, but never changes, F.0."""

    schema: str
    f0_manifest_hash: str
    strategy_identity: str
    strategy_artifact_hash: str
    records: tuple[AuthorityRecord, ...]

    def __post_init__(self) -> None:
        if self.schema != F1_SCHEMA:
            raise ValueError("Unsupported F.1 authority manifest schema.")
        if self.f0_manifest_hash != F0_AUTHORITY_MANIFEST_HASH:
            raise ValueError("F.1 must anchor the frozen F.0 manifest hash.")
        if self.strategy_identity != TRADER_V0_STRATEGY.strategy_identity:
            raise ValueError("F.1 manifest must identify the exact Trader V0 artifact.")
        if self.strategy_artifact_hash != TRADER_V0_ARTIFACT_HASH:
            raise ValueError("F.1 manifest must carry the exact Trader V0 artifact hash.")
        if type(self.records) is not tuple or len(self.records) != len(AuthorityCapability):
            raise ValueError("F.1 must state every Trader Lane authority decision exactly once.")
        keys = [(record.owner, record.capability) for record in self.records]
        if len(keys) != len(set(keys)) or any(record.owner is not AuthorityOwner.TRADER_LANE for record in self.records):
            raise ValueError("F.1 authority records must be unique Trader Lane decisions.")

    def record_for(self, capability: AuthorityCapability) -> AuthorityRecord:
        if type(capability) is not AuthorityCapability:
            raise F1AuthorityRefused("F.1 capability lookup requires an explicit capability identity.")
        for record in self.records:
            if record.capability is capability:
                return record
        raise F1AuthorityRefused("F.1 authority record is missing.")

    def payload(self) -> dict[str, object]:
        return {
            "schema": self.schema,
            "f0_manifest_hash": self.f0_manifest_hash,
            "strategy_identity": self.strategy_identity,
            "strategy_artifact_hash": self.strategy_artifact_hash,
            "records": [
                record.payload() for record in sorted(self.records, key=lambda item: item.capability.value)
            ],
        }

    @property
    def manifest_hash(self) -> str:
        return _canonical_hash(self.payload())


def _f1_records() -> tuple[AuthorityRecord, ...]:
    records: list[AuthorityRecord] = []
    for capability in AuthorityCapability:
        state = AuthorityState.GRANTED if capability is AuthorityCapability.SIGNAL else AuthorityState.DENIED
        basis = (
            TRADER_V0_AUTHORITY_BASIS
            if capability is AuthorityCapability.SIGNAL
            else "F1_TRADER_V0_HAS_NO_" + capability.value + "_AUTHORITY"
        )
        records.append(AuthorityRecord(AuthorityOwner.TRADER_LANE, capability, state, basis))
    return tuple(records)


F1_MANIFEST = F1AuthorityManifest(
    schema=F1_SCHEMA,
    f0_manifest_hash=F0_AUTHORITY_MANIFEST_HASH,
    strategy_identity=TRADER_V0_STRATEGY.strategy_identity,
    strategy_artifact_hash=TRADER_V0_ARTIFACT_HASH,
    records=_f1_records(),
)
F1_AUTHORITY_MANIFEST_HASH = F1_MANIFEST.manifest_hash


@dataclass(frozen=True)
class F1StrategyRegistration:
    """The sole explicit authority registration permitted by F.1."""

    strategy_identity: str
    strategy_version: str
    strategy_artifact_hash: str
    signal_authority: AuthorityRecord

    def __post_init__(self) -> None:
        if (
            self.strategy_identity != TRADER_V0_STRATEGY.strategy_identity
            or self.strategy_version != TRADER_V0_STRATEGY_VERSION
            or self.strategy_artifact_hash != TRADER_V0_ARTIFACT_HASH
        ):
            raise ValueError("F.1 can register only the exact frozen Trader V0 version.")
        if (
            self.signal_authority.owner is not AuthorityOwner.TRADER_LANE
            or self.signal_authority.capability is not AuthorityCapability.SIGNAL
            or not self.signal_authority.granted
            or self.signal_authority.basis != TRADER_V0_AUTHORITY_BASIS
        ):
            raise ValueError("F.1 registration requires its narrow Trader V0 signal grant.")


@dataclass(frozen=True)
class F1StrategyAuthorityRegistry:
    registrations: tuple[F1StrategyRegistration, ...]

    def __post_init__(self) -> None:
        if type(self.registrations) is not tuple or len(self.registrations) != 1:
            raise ValueError("F.1 permits exactly one immutable strategy registration.")

    def find(self, strategy: OperationalStrategyArtifact) -> F1StrategyRegistration | None:
        for registration in self.registrations:
            if (
                registration.strategy_identity == strategy.strategy_identity
                and registration.strategy_version == strategy.strategy_version
                and registration.strategy_artifact_hash == strategy.strategy_artifact_hash
            ):
                return registration
        return None


F1_STRATEGY_REGISTRY = F1StrategyAuthorityRegistry((
    F1StrategyRegistration(
        strategy_identity=TRADER_V0_STRATEGY.strategy_identity,
        strategy_version=TRADER_V0_STRATEGY_VERSION,
        strategy_artifact_hash=TRADER_V0_ARTIFACT_HASH,
        signal_authority=F1_MANIFEST.record_for(AuthorityCapability.SIGNAL),
    ),
))


@dataclass(frozen=True)
class F1AuthorityEvaluation:
    """A deterministic authority decision bound to F.1 and F.0 hashes."""

    allowed: bool
    reason_code: str
    f0_manifest_hash: str
    f1_manifest_hash: str
    strategy_identity: str | None
    strategy_version: str | None
    input_provenance_hashes: tuple[str, ...]

    def payload(self) -> dict[str, object]:
        return {
            "schema": "phase-f1-authority-evaluation-v1",
            "allowed": self.allowed,
            "reason_code": self.reason_code,
            "f0_manifest_hash": self.f0_manifest_hash,
            "f1_manifest_hash": self.f1_manifest_hash,
            "strategy_identity": self.strategy_identity,
            "strategy_version": self.strategy_version,
            "input_provenance_hashes": list(self.input_provenance_hashes),
        }

    @property
    def decision_hash(self) -> str:
        return _canonical_hash(self.payload())


def _f1_denied(
    reason_code: str,
    *,
    strategy: OperationalStrategyArtifact | None = None,
    inputs: Iterable[OperationalInput] = (),
) -> F1AuthorityEvaluation:
    hashes = tuple(sorted(item.provenance_hash for item in inputs))
    return F1AuthorityEvaluation(
        allowed=False,
        reason_code=reason_code,
        f0_manifest_hash=F0_AUTHORITY_MANIFEST_HASH,
        f1_manifest_hash=F1_AUTHORITY_MANIFEST_HASH,
        strategy_identity=strategy.strategy_identity if strategy is not None else None,
        strategy_version=strategy.strategy_version if strategy is not None else None,
        input_provenance_hashes=hashes,
    )


def _valid_f1_manifest(manifest: object) -> bool:
    return (
        type(manifest) is F1AuthorityManifest
        and manifest.manifest_hash == F1_AUTHORITY_MANIFEST_HASH
        and manifest.f0_manifest_hash == F0_AUTHORITY_MANIFEST_HASH
        and F0_MANIFEST.manifest_hash == F0_AUTHORITY_MANIFEST_HASH
    )


def evaluate_f1_authority(
    strategy: object,
    inputs: Sequence[object],
    *,
    registry: object = F1_STRATEGY_REGISTRY,
    manifest: object = F1_MANIFEST,
) -> F1AuthorityEvaluation:
    """Grant only the registered artifact's bounded signal request authority.

    Exact type checks occur before fields are read.  Therefore a callback,
    mapping, Phase E object, foreign enum, or subclass cannot be used as an
    operational input capability.
    """
    if not _valid_f1_manifest(manifest):
        return _f1_denied("F1_AUTHORITY_MANIFEST_AMBIGUOUS")
    if type(strategy) is not OperationalStrategyArtifact:
        return _f1_denied("STRATEGY_PROVENANCE_MISSING")
    checked_strategy = strategy
    if (
        checked_strategy.strategy_id != TRADER_V0_STRATEGY_ID
        or checked_strategy.strategy_version != TRADER_V0_STRATEGY_VERSION
        or checked_strategy.strategy_artifact_hash != TRADER_V0_ARTIFACT_HASH
        or checked_strategy.strategy_identity != TRADER_V0_STRATEGY.strategy_identity
        or checked_strategy.allowed_input_sources != TRADER_V0_ALLOWED_INPUT_SOURCES
    ):
        return _f1_denied("STRATEGY_IDENTITY_MISMATCH", strategy=checked_strategy)
    if type(inputs) not in {tuple, list}:
        return _f1_denied("INPUT_PROVENANCE_MISSING", strategy=checked_strategy)
    checked_inputs: list[OperationalInput] = []
    for item in inputs:
        if type(item) is not OperationalInput:
            return _f1_denied(
                "PROTECTED_OR_UNKNOWN_INPUT_CAPABILITY", strategy=checked_strategy, inputs=checked_inputs,
            )
        if item.source not in TRADER_V0_ALLOWED_INPUT_SOURCES:
            return _f1_denied("INPUT_SOURCE_NOT_DECLARED_BY_STRATEGY", strategy=checked_strategy, inputs=checked_inputs)
        checked_inputs.append(item)
    if not checked_inputs:
        return _f1_denied("INPUT_PROVENANCE_MISSING", strategy=checked_strategy)
    hashes = [item.provenance_hash for item in checked_inputs]
    if len(hashes) != len(set(hashes)):
        return _f1_denied("DUPLICATE_INPUT_PROVENANCE", strategy=checked_strategy, inputs=checked_inputs)
    if type(registry) is not F1StrategyAuthorityRegistry:
        return _f1_denied("STRATEGY_REGISTRY_AMBIGUOUS", strategy=checked_strategy, inputs=checked_inputs)
    registration = registry.find(checked_strategy)
    if registration is None:
        return _f1_denied("UNKNOWN_STRATEGY_VERSION", strategy=checked_strategy, inputs=checked_inputs)
    if not manifest.record_for(AuthorityCapability.SIGNAL).granted or not registration.signal_authority.granted:
        return _f1_denied("SIGNAL_AUTHORITY_NOT_COMMISSIONED", strategy=checked_strategy, inputs=checked_inputs)
    return F1AuthorityEvaluation(
        allowed=True,
        reason_code="F1_TRADER_V0_SIGNAL_AUTHORITY_GRANTED",
        f0_manifest_hash=F0_AUTHORITY_MANIFEST_HASH,
        f1_manifest_hash=F1_AUTHORITY_MANIFEST_HASH,
        strategy_identity=checked_strategy.strategy_identity,
        strategy_version=checked_strategy.strategy_version,
        input_provenance_hashes=tuple(sorted(hashes)),
    )


class TraderV0Action(StrEnum):
    LONG = "LONG"
    SHORT = "SHORT"
    SKIP = "SKIP"
    EXIT = "EXIT"


@dataclass(frozen=True)
class TraderV0DecisionInput:
    """Explicit operational observations supplied to the fixed Trader V0 gate."""

    operational_inputs: tuple[object, ...]
    now: object
    symbol: object
    direction: object
    source_action_at: object
    market_observed_at: object
    indicator_ids: tuple[object, ...]
    effective_confidence: object
    expected_gross_edge: object
    estimated_fees: object
    estimated_spread: object
    estimated_slippage: object
    estimated_market_impact: object
    estimated_latency_cost: object
    alpha_survival: object
    requested_notional_ceiling: object
    market_regime: object
    position_open: object = False
    position_age_seconds: object = 0.0
    hard_risk_exit: object = False
    regime_invalidated: object = False
    source_wallet_leverage: object | None = None


_REASON_ORDER = {
    "INPUT_PROVENANCE_REFUSED": 10,
    "MISSING_REQUIRED_OPERATIONAL_INPUT": 20,
    "AMBIGUOUS_OPERATIONAL_INPUT": 21,
    "INPUT_INTEGRITY_FAILURE": 30,
    "INVALID_DIRECTION": 40,
    "STALE_SIGNAL": 50,
    "MARKET_EVIDENCE_STALE": 51,
    "MISSING_REQUIRED_INDICATOR": 60,
    "LOW_EFFECTIVE_CONFIDENCE": 70,
    "NON_POSITIVE_NET_EDGE": 80,
    "ALPHA_SURVIVAL_NON_POSITIVE": 90,
    "REGIME_INVALIDATED": 100,
    "HARD_RISK_EXIT": 110,
    "MAX_POSITION_AGE": 120,
    "CONFIDENCE_DECAY_EXIT": 130,
    "INPUT_INTEGRITY_EXIT": 140,
    "POSITION_REMAINS_WITHIN_HYSTERESIS": 150,
    "ENTRY_AUTHORIZED": 200,
}


def _ordered_reasons(reasons: Iterable[str]) -> tuple[str, ...]:
    return tuple(sorted(set(reasons), key=lambda value: (_REASON_ORDER.get(value, 999), value)))


def _source_set(inputs: tuple[object, ...]) -> tuple[set[OperationalInputSource], bool]:
    if any(type(item) is not OperationalInput for item in inputs):
        return set(), False
    sources = [item.source for item in inputs if type(item) is OperationalInput]
    return set(sources), len(sources) == len(set(sources))


def _input_fingerprint(item: TraderV0DecisionInput, provenance_hashes: tuple[str, ...]) -> str:
    """Hash every material gate input without treating wallet leverage as sizing."""
    return _canonical_hash({
        "operational_input_provenance_hashes": list(provenance_hashes),
        "now": _safe_value(item.now),
        "symbol": _safe_value(item.symbol),
        "direction": _safe_value(item.direction),
        "source_action_at": _safe_value(item.source_action_at),
        "market_observed_at": _safe_value(item.market_observed_at),
        "indicator_ids": sorted(
            (_safe_value(value) for value in item.indicator_ids),
            key=lambda value: json.dumps(value, sort_keys=True, separators=(",", ":")),
        ) if type(item.indicator_ids) is tuple else _safe_value(item.indicator_ids),
        "effective_confidence": _safe_value(item.effective_confidence),
        "expected_gross_edge": _safe_value(item.expected_gross_edge),
        "estimated_fees": _safe_value(item.estimated_fees),
        "estimated_spread": _safe_value(item.estimated_spread),
        "estimated_slippage": _safe_value(item.estimated_slippage),
        "estimated_market_impact": _safe_value(item.estimated_market_impact),
        "estimated_latency_cost": _safe_value(item.estimated_latency_cost),
        "alpha_survival": _safe_value(item.alpha_survival),
        "requested_notional_ceiling": _safe_value(item.requested_notional_ceiling),
        "market_regime": _safe_value(item.market_regime),
        "position_open": _safe_value(item.position_open),
        "position_age_seconds": _safe_value(item.position_age_seconds),
        "hard_risk_exit": _safe_value(item.hard_risk_exit),
        "regime_invalidated": _safe_value(item.regime_invalidated),
        "source_wallet_leverage_sizing_authority": False,
    })


@dataclass(frozen=True)
class TraderV0Decision:
    action: TraderV0Action
    reason_codes: tuple[str, ...]
    strategy: OperationalStrategyArtifact
    authority: F1AuthorityEvaluation
    decision_input: TraderV0DecisionInput
    symbol: str | None
    input_provenance_hashes: tuple[str, ...]
    created_at: str | None
    expires_at: str | None
    requested_notional_ceiling: float
    input_fingerprint: str

    def __post_init__(self) -> None:
        if type(self.action) is not TraderV0Action or type(self.reason_codes) is not tuple:
            raise ValueError("Trader V0 decisions must be immutable and explicit.")
        if type(self.strategy) is not OperationalStrategyArtifact or type(self.authority) is not F1AuthorityEvaluation:
            raise ValueError("Trader V0 decision provenance is required.")
        if type(self.decision_input) is not TraderV0DecisionInput or type(self.input_provenance_hashes) is not tuple:
            raise ValueError("Trader V0 decision input provenance must be immutable.")
        if not math.isfinite(self.requested_notional_ceiling) or self.requested_notional_ceiling < 0:
            raise ValueError("Trader V0 requested ceiling must be finite and non-negative.")

    def payload(self) -> dict[str, object]:
        return {
            "schema": "phase-f1-trader-v0-decision-v1",
            "action": self.action.value,
            "reason_codes": list(self.reason_codes),
            "strategy_identity": self.strategy.strategy_identity,
            "strategy_version": self.strategy.strategy_version,
            "strategy_artifact_hash": self.strategy.strategy_artifact_hash,
            "symbol": self.symbol,
            "authority_decision_hash": self.authority.decision_hash,
            "input_provenance_hashes": list(self.input_provenance_hashes),
            "created_at": self.created_at,
            "expires_at": self.expires_at,
            "requested_notional_ceiling": self.requested_notional_ceiling,
            "input_fingerprint": self.input_fingerprint,
            "exit_policy_ref": TRADER_V0_EXIT_POLICY_REF,
            "risk_policy_ref": TRADER_V0_RISK_POLICY_REF,
        }

    @property
    def decision_hash(self) -> str:
        return _canonical_hash(self.payload())


class TraderV0:
    """Frozen indicator-confirmed wallet-action strategy for shadow requests."""

    required_entry_sources = frozenset({
        OperationalInputSource.LIVE_PUBLIC_MARKET_DATA,
        OperationalInputSource.LIVE_PUBLIC_WALLET_ACTIVITY,
        OperationalInputSource.OPERATIONAL_INDICATOR,
        OperationalInputSource.CONFIGURATION_OR_RISK_POLICY,
    })

    def decide(self, item: TraderV0DecisionInput) -> TraderV0Decision:
        if type(item) is not TraderV0DecisionInput:
            raise TypeError("Trader V0 requires the explicit immutable decision-input contract.")
        authority = evaluate_f1_authority(TRADER_V0_STRATEGY, item.operational_inputs)
        provenance_hashes = authority.input_provenance_hashes
        fingerprint = _input_fingerprint(item, provenance_hashes)
        created_at, expires_at, timestamp_valid = self._timestamps(item.now)
        values, integrity_valid = self._values(item)
        if type(item.operational_inputs) is tuple:
            sources, sources_unambiguous = _source_set(item.operational_inputs)
        else:
            # Do not invoke or iterate a foreign container after authority has
            # already refused it as non-immutable provenance.
            sources, sources_unambiguous = set(), False
        required_sources = (
            {OperationalInputSource.CURRENT_ACCOUNT_OR_EXECUTION_STATE}
            if item.position_open is True else self.required_entry_sources
        )
        source_requirements_met = required_sources.issubset(sources) and sources_unambiguous
        base_reasons: list[str] = []
        if not authority.allowed:
            base_reasons.append("INPUT_PROVENANCE_REFUSED")
        if type(item.operational_inputs) is not tuple:
            base_reasons.append("INPUT_INTEGRITY_FAILURE")
        if not source_requirements_met:
            base_reasons.append("AMBIGUOUS_OPERATIONAL_INPUT" if not sources_unambiguous else "MISSING_REQUIRED_OPERATIONAL_INPUT")
        if not timestamp_valid or not integrity_valid:
            base_reasons.append("INPUT_INTEGRITY_FAILURE")

        if item.position_open is True:
            return self._position_decision(
                item, authority, provenance_hashes, fingerprint, created_at, expires_at, values, base_reasons,
            )
        return self._entry_decision(
            item, authority, provenance_hashes, fingerprint, created_at, expires_at, values, base_reasons,
        )

    @staticmethod
    def _timestamps(now: object) -> tuple[str | None, str | None, bool]:
        try:
            parsed = _as_utc(now)
        except ValueError:
            return None, None, False
        created_at = parsed.isoformat().replace("+00:00", "Z")
        expires_at = (parsed + timedelta(seconds=TRADER_V0_ENTRY_SIGNAL_TTL_SECONDS)).isoformat().replace("+00:00", "Z")
        return created_at, expires_at, True

    @staticmethod
    def _values(item: TraderV0DecisionInput) -> tuple[dict[str, float], bool]:
        names = (
            "effective_confidence", "expected_gross_edge", "estimated_fees", "estimated_spread",
            "estimated_slippage", "estimated_market_impact", "estimated_latency_cost", "alpha_survival",
            "requested_notional_ceiling", "position_age_seconds",
        )
        values = {name: _finite_number(getattr(item, name)) for name in names}
        valid = all(value is not None for value in values.values())
        if valid:
            valid = (
                0.0 <= values["effective_confidence"] <= 1.0  # type: ignore[operator]
                and values["alpha_survival"] >= 0.0  # type: ignore[operator]
                and values["requested_notional_ceiling"] > 0.0  # type: ignore[operator]
                and values["position_age_seconds"] >= 0.0  # type: ignore[operator]
                and type(item.position_open) is bool
                and type(item.hard_risk_exit) is bool
                and type(item.regime_invalidated) is bool
                and isinstance(item.market_regime, str) and bool(item.market_regime.strip())
            )
        return {name: value if value is not None else 0.0 for name, value in values.items()}, bool(valid)

    def _entry_decision(
        self,
        item: TraderV0DecisionInput,
        authority: F1AuthorityEvaluation,
        provenance_hashes: tuple[str, ...],
        fingerprint: str,
        created_at: str | None,
        expires_at: str | None,
        values: dict[str, float],
        reasons: list[str],
    ) -> TraderV0Decision:
        if not isinstance(item.symbol, str) or not item.symbol.strip():
            reasons.append("INPUT_INTEGRITY_FAILURE")
        if type(item.direction) is not TradeDirection:
            reasons.append("INVALID_DIRECTION")
        if type(item.indicator_ids) is not tuple or not item.indicator_ids or any(
            not isinstance(value, str) or not value.strip() for value in item.indicator_ids
        ):
            reasons.append("MISSING_REQUIRED_INDICATOR")
        elif len(set(item.indicator_ids)) != len(item.indicator_ids):
            reasons.append("MISSING_REQUIRED_INDICATOR")
        try:
            now = _as_utc(item.now)
            source_age = (now - _as_utc(item.source_action_at)).total_seconds()
            market_age = (now - _as_utc(item.market_observed_at)).total_seconds()
            if not 0.0 <= source_age <= TRADER_V0_ENTRY_SIGNAL_TTL_SECONDS:
                reasons.append("STALE_SIGNAL")
            if not 0.0 <= market_age <= TRADER_V0_MARKET_EVIDENCE_TTL_SECONDS:
                reasons.append("MARKET_EVIDENCE_STALE")
        except ValueError:
            reasons.append("INPUT_INTEGRITY_FAILURE")
        if values["effective_confidence"] < TRADER_V0_ENTRY_EFFECTIVE_CONFIDENCE:
            reasons.append("LOW_EFFECTIVE_CONFIDENCE")
        net_edge = self._net_edge(values)
        if net_edge <= 0.0:
            reasons.append("NON_POSITIVE_NET_EDGE")
        if values["alpha_survival"] <= 0.0:
            reasons.append("ALPHA_SURVIVAL_NON_POSITIVE")
        if item.regime_invalidated is True:
            reasons.append("REGIME_INVALIDATED")
        ordered = _ordered_reasons(reasons)
        if ordered:
            return self._decision(
                item, TraderV0Action.SKIP, ordered, authority, provenance_hashes, created_at, expires_at, 0.0, fingerprint,
            )
        direction = item.direction
        assert type(direction) is TradeDirection
        requested = min(values["requested_notional_ceiling"], TRADER_V0_MAXIMUM_REQUESTED_NOTIONAL_CEILING)
        return self._decision(
            item, TraderV0Action.LONG if direction is TradeDirection.LONG else TraderV0Action.SHORT,
            ("ENTRY_AUTHORIZED",), authority, provenance_hashes, created_at, expires_at, requested, fingerprint,
        )

    def _position_decision(
        self,
        item: TraderV0DecisionInput,
        authority: F1AuthorityEvaluation,
        provenance_hashes: tuple[str, ...],
        fingerprint: str,
        created_at: str | None,
        expires_at: str | None,
        values: dict[str, float],
        base_reasons: list[str],
    ) -> TraderV0Decision:
        reasons: list[str] = []
        if base_reasons:
            reasons.append("INPUT_INTEGRITY_EXIT")
        if item.hard_risk_exit is True:
            reasons.append("HARD_RISK_EXIT")
        if values["position_age_seconds"] >= TRADER_V0_MAXIMUM_POSITION_AGE_SECONDS:
            reasons.append("MAX_POSITION_AGE")
        if self._net_edge(values) <= 0.0:
            reasons.append("NON_POSITIVE_NET_EDGE")
        if values["effective_confidence"] < TRADER_V0_EXIT_EFFECTIVE_CONFIDENCE:
            reasons.append("CONFIDENCE_DECAY_EXIT")
        if item.regime_invalidated is True:
            reasons.append("REGIME_INVALIDATED")
        if reasons:
            return self._decision(
                item, TraderV0Action.EXIT, _ordered_reasons(reasons), authority, provenance_hashes,
                created_at, expires_at, 0.0, fingerprint,
            )
        return self._decision(
            item, TraderV0Action.SKIP, ("POSITION_REMAINS_WITHIN_HYSTERESIS",), authority, provenance_hashes,
            created_at, expires_at, 0.0, fingerprint,
        )

    @staticmethod
    def _net_edge(values: dict[str, float]) -> float:
        return (
            values["expected_gross_edge"] - values["estimated_fees"] - values["estimated_spread"]
            - values["estimated_slippage"] - values["estimated_market_impact"] - values["estimated_latency_cost"]
        )

    @staticmethod
    def _decision(
        item: TraderV0DecisionInput,
        action: TraderV0Action,
        reason_codes: tuple[str, ...],
        authority: F1AuthorityEvaluation,
        provenance_hashes: tuple[str, ...],
        created_at: str | None,
        expires_at: str | None,
        requested_notional_ceiling: float,
        fingerprint: str,
    ) -> TraderV0Decision:
        return TraderV0Decision(
            action=action,
            reason_codes=reason_codes,
            strategy=TRADER_V0_STRATEGY,
            authority=authority,
            decision_input=item,
            symbol=item.symbol.strip() if isinstance(item.symbol, str) and item.symbol.strip() else None,
            input_provenance_hashes=provenance_hashes,
            created_at=created_at,
            expires_at=expires_at,
            requested_notional_ceiling=requested_notional_ceiling,
            input_fingerprint=fingerprint,
        )


def create_f1_trade_intent(decision: object) -> TradeIntentRequest:
    """Issue a bounded F.0 request from an authorized LONG or SHORT decision.

    This is intentionally not an execution bridge.  It neither imports nor
    calls Phase D, and it returns F.0's request type with both authorities
    permanently false.
    """
    if type(decision) is not TraderV0Decision:
        raise TradeIntentRefused("F1_DECISION_PROVENANCE_MISSING")
    if decision.action not in {TraderV0Action.LONG, TraderV0Action.SHORT}:
        raise TradeIntentRefused("F1_DECISION_IS_NOT_AN_ENTRY")
    if not decision.authority.allowed:
        raise TradeIntentRefused("F1_SIGNAL_AUTHORITY_NOT_GRANTED")
    fresh = TraderV0().decide(decision.decision_input)
    if fresh.decision_hash != decision.decision_hash:
        raise TradeIntentRefused("F1_DECISION_REPLAY_MISMATCH")
    if (
        decision.strategy != TRADER_V0_STRATEGY
        or decision.authority.f1_manifest_hash != F1_AUTHORITY_MANIFEST_HASH
        or decision.authority.f0_manifest_hash != F0_AUTHORITY_MANIFEST_HASH
        or decision.created_at is None
        or decision.expires_at is None
        or decision.requested_notional_ceiling <= 0.0
        or decision.requested_notional_ceiling > TRADER_V0_MAXIMUM_REQUESTED_NOTIONAL_CEILING
        or decision.symbol is None
    ):
        raise TradeIntentRefused("F1_DECISION_IDENTITY_OR_BOUNDS_MISMATCH")
    direction = TradeDirection.LONG if decision.action is TraderV0Action.LONG else TradeDirection.SHORT
    return TradeIntentRequest(
        strategy_id=TRADER_V0_STRATEGY_ID,
        strategy_version=TRADER_V0_STRATEGY_VERSION,
        strategy_identity=TRADER_V0_STRATEGY.strategy_identity,
        symbol=decision.symbol,
        direction=direction,
        requested_notional_ceiling=decision.requested_notional_ceiling,
        created_at=decision.created_at,
        expires_at=decision.expires_at,
        authority_decision_hash=decision.authority.decision_hash,
        input_provenance_hashes=decision.input_provenance_hashes,
        exit_policy_ref=TRADER_V0_EXIT_POLICY_REF,
        risk_policy_ref=TRADER_V0_RISK_POLICY_REF,
    )
