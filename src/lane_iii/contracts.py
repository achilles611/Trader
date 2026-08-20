"""Lane III Phase A constitutional contracts.

This module is deliberately a *contract-only* surface.  It has no broker,
network, credentials, order transport, Phase E, Phase D, or Lane II imports.
It models MNQ observations as evidence, evidence as family-scoped support or
contradiction of a deterministic market hypothesis, and a future bounded
execution request.  None of these objects can submit an order.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from typing import Iterable


L3A_SCHEMA = "lane-iii-phase-a-constitutional-foundation-v1"
L3A_VERSION = "lane-iii-phase-a-v1"


class LaneIIIRefused(RuntimeError):
    """A Lane III contract or authority boundary was violated."""


class InstrumentRefused(LaneIIIRefused):
    """Lane III received an instrument outside the deliberately narrow MNQ scope."""


class IntentRefused(LaneIIIRefused):
    """An execution intent does not fit the narrow future execution contract."""


class StrategyProvenanceRefused(LaneIIIRefused):
    """A strategy identity is not an immutable Lane III artifact identity."""


class LaneIIIInstrument(StrEnum):
    """Strategy instruments admitted by L3-A.  This is intentionally not generic."""

    MNQ = "MNQ"


class EvidenceFamily(StrEnum):
    STRUCTURAL_CONTEXT = "STRUCTURAL_CONTEXT"
    ORDER_FLOW = "ORDER_FLOW"
    RESTING_LIQUIDITY = "RESTING_LIQUIDITY"
    DERIVATIVES_CONTEXT = "DERIVATIVES_CONTEXT"
    TIMING_SESSION_CONTEXT = "TIMING_SESSION_CONTEXT"


class EvidenceRelation(StrEnum):
    SUPPORTS = "SUPPORTS"
    CONTRADICTS = "CONTRADICTS"
    INCONCLUSIVE = "INCONCLUSIVE"


class TemporalBehavior(StrEnum):
    """Observed behaviour; displayed liquidity is never treated as ground truth."""

    SNAPSHOT = "SNAPSHOT"
    PERSISTENT = "PERSISTENT"
    PULLED = "PULLED"
    REPLENISHED = "REPLENISHED"
    EXECUTED = "EXECUTED"
    UNKNOWN = "UNKNOWN"


class HypothesisKind(StrEnum):
    REVERSAL = "REVERSAL"
    CONTINUATION = "CONTINUATION"
    UNRESOLVED = "UNRESOLVED"


class HypothesisDirection(StrEnum):
    BULLISH = "BULLISH"
    BEARISH = "BEARISH"
    NEUTRAL = "NEUTRAL"


class HypothesisValidity(StrEnum):
    ACTIVE = "ACTIVE"
    INVALIDATED = "INVALIDATED"
    EXPIRED = "EXPIRED"


class ConfidenceLifecycle(StrEnum):
    UNRESOLVED = "UNRESOLVED"
    BUILDING = "BUILDING"
    ALIGNED = "ALIGNED"
    DECAYING = "DECAYING"
    CONTRADICTED = "CONTRADICTED"
    EXPIRED = "EXPIRED"


class IntentPurpose(StrEnum):
    ENTRY = "ENTRY"
    THESIS_EXIT = "THESIS_EXIT"
    PROTECTIVE_EXIT = "PROTECTIVE_EXIT"


class TargetExposure(StrEnum):
    LONG = "LONG"
    SHORT = "SHORT"
    FLAT = "FLAT"


class StopProtectionKind(StrEnum):
    PRICE_STOP = "PRICE_STOP"


class ProfitTakingKind(StrEnum):
    NONE = "NONE"
    LIMIT_TARGET = "LIMIT_TARGET"


class PositionMode(StrEnum):
    ONE_POSITION = "ONE_POSITION"


class KnowledgeState(StrEnum):
    FRESH = "FRESH"
    STALE = "STALE"
    UNKNOWN = "UNKNOWN"


class PositionKnowledge(StrEnum):
    FLAT = "FLAT"
    LONG = "LONG"
    SHORT = "SHORT"
    UNKNOWN = "UNKNOWN"


class WorkingOrderKnowledge(StrEnum):
    CLEAR = "CLEAR"
    PRESENT = "PRESENT"
    UNKNOWN = "UNKNOWN"


class FuturesSessionPhase(StrEnum):
    """Named context only; it has no direct strategy or execution authority."""

    OVERNIGHT = "OVERNIGHT"
    NY_CASH_OPEN = "NY_CASH_OPEN"
    REGULAR = "REGULAR"
    LUNCH = "LUNCH"
    SETTLEMENT = "SETTLEMENT"
    ECONOMIC_EVENT_WINDOW = "ECONOMIC_EVENT_WINDOW"
    CLOSED = "CLOSED"


class OperatorCommand(StrEnum):
    ARM = "ARM"
    DISARM = "DISARM"
    PAUSE_NEW_ENTRIES = "PAUSE_NEW_ENTRIES"
    RESUME_NEW_ENTRIES = "RESUME_NEW_ENTRIES"
    FLATTEN = "FLATTEN"
    VERIFY_POSITION = "VERIFY_POSITION"
    VERIFY_BROKER_STATE = "VERIFY_BROKER_STATE"
    INSPECT_ACTIVE_HYPOTHESIS = "INSPECT_ACTIVE_HYPOTHESIS"
    INSPECT_CONFIDENCE = "INSPECT_CONFIDENCE"
    INSPECT_EVIDENCE = "INSPECT_EVIDENCE"
    INSPECT_UNRESOLVED_ORDERS = "INSPECT_UNRESOLVED_ORDERS"


class AuthorityCapability(StrEnum):
    OBSERVE_MNQ_MARKET_DATA = "OBSERVE_MNQ_MARKET_DATA"
    OBSERVE_DOM_RESTING_LIQUIDITY = "OBSERVE_DOM_RESTING_LIQUIDITY"
    OBSERVE_OPTIONS_CONTEXT = "OBSERVE_OPTIONS_CONTEXT"
    CONSTRUCT_MARKET_HYPOTHESES = "CONSTRUCT_MARKET_HYPOTHESES"
    COMPUTE_CONFIDENCE = "COMPUTE_CONFIDENCE"
    GENERATE_TRADE_SIGNALS = "GENERATE_TRADE_SIGNALS"
    REQUEST_FUTURES_EXECUTION = "REQUEST_FUTURES_EXECUTION"
    EXECUTE_FUTURES_ORDERS = "EXECUTE_FUTURES_ORDERS"
    ACCESS_BROKER_ACCOUNT = "ACCESS_BROKER_ACCOUNT"
    ACCESS_PROP_ACCOUNT = "ACCESS_PROP_ACCOUNT"
    CHANGE_HARD_RISK_LIMITS = "CHANGE_HARD_RISK_LIMITS"
    OVERRIDE_FLATTEN = "OVERRIDE_FLATTEN"
    CONTROL_FOLLOWER_ACCOUNTS = "CONTROL_FOLLOWER_ACCOUNTS"
    SCIENTIFIC_AUTHORITY = "SCIENTIFIC_AUTHORITY"
    MODIFY_ACTIVE_STRATEGY_FROM_PNL = "MODIFY_ACTIVE_STRATEGY_FROM_PNL"
    MODIFY_PHASE_E = "MODIFY_PHASE_E"
    MODIFY_LANE_II = "MODIFY_LANE_II"
    LIVE_CAPITAL_AUTHORITY = "LIVE_CAPITAL_AUTHORITY"


class AuthorityStatus(StrEnum):
    ARCHITECTURE_ONLY = "ARCHITECTURE_ONLY"
    CONTRACT_ONLY = "CONTRACT_ONLY"
    SEMANTICS_ONLY = "SEMANTICS_ONLY"
    DENIED = "DENIED"


def canonical_hash(payload: object) -> str:
    """Return the canonical SHA-256 used by all replayable L3-A contracts."""
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def normalized_utc(value: object, field: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field} must be ISO-8601 text with an explicit UTC offset.")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{field} must be ISO-8601 text with an explicit UTC offset.") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"{field} must have an explicit UTC offset.")
    return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def utc_after(left: object, right: object) -> bool:
    return normalized_utc(left, "time") > normalized_utc(right, "time")


def _required_text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} is required.")
    return value


def _sha256(value: object, field: str) -> str:
    if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise ValueError(f"{field} must be a lowercase SHA-256 digest.")
    return value


def _decimal(value: object, field: str, *, positive: bool = False, nonnegative: bool = False) -> Decimal:
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{field} must be a finite decimal.") from exc
    if not number.is_finite() or (positive and number <= 0) or (nonnegative and number < 0):
        qualifier = "positive" if positive else "non-negative" if nonnegative else "finite"
        raise ValueError(f"{field} must be {qualifier}.")
    return number


@dataclass(frozen=True)
class FuturesExecutionInstrument:
    """A concrete CME MNQ expiry, distinct from the strategy root instrument."""

    strategy_instrument: LaneIIIInstrument
    contract_symbol: str
    exchange: str = "CME"

    def __post_init__(self) -> None:
        if self.strategy_instrument is not LaneIIIInstrument.MNQ:
            raise InstrumentRefused("L3-A admits only the MNQ strategy instrument.")
        if self.exchange != "CME":
            raise InstrumentRefused("L3-A concrete execution instrument must be CME MNQ.")
        if not isinstance(self.contract_symbol, str) or re.fullmatch(r"MNQ[FGHJKMNQUVXZ][0-9]{1,2}", self.contract_symbol) is None:
            raise InstrumentRefused("Execution instrument must be a specific MNQ CME expiry such as MNQZ6.")

    def payload(self) -> dict[str, str]:
        return {
            "strategy_instrument": self.strategy_instrument.value,
            "contract_symbol": self.contract_symbol,
            "exchange": self.exchange,
        }


@dataclass(frozen=True)
class MarketLocation:
    """A deterministic MNQ location reference, never a prose trading narrative."""

    strategy_instrument: LaneIIIInstrument
    reference_price: Decimal
    location_hash: str

    def __post_init__(self) -> None:
        if self.strategy_instrument is not LaneIIIInstrument.MNQ:
            raise InstrumentRefused("Market hypotheses may describe MNQ only during L3-A.")
        _decimal(self.reference_price, "Market-location reference price", positive=True)
        _sha256(self.location_hash, "Market-location hash")

    def payload(self) -> dict[str, str]:
        return {
            "strategy_instrument": self.strategy_instrument.value,
            "reference_price": str(self.reference_price),
            "location_hash": self.location_hash,
        }


@dataclass(frozen=True)
class EvidenceObservation:
    """One provenance-bearing observation.  It cannot create an order or signal."""

    evidence_id: str
    family: EvidenceFamily
    strategy_instrument: LaneIIIInstrument
    observed_at: str
    expires_at: str
    source_payload_hash: str
    temporal_behavior: TemporalBehavior = TemporalBehavior.UNKNOWN

    def __post_init__(self) -> None:
        _required_text(self.evidence_id, "Evidence identity")
        if type(self.family) is not EvidenceFamily:
            raise ValueError("Evidence family must be explicit.")
        if self.strategy_instrument is not LaneIIIInstrument.MNQ:
            raise InstrumentRefused("L3-A evidence may observe MNQ only.")
        if type(self.temporal_behavior) is not TemporalBehavior:
            raise ValueError("Evidence temporal behaviour must be explicit.")
        observed_at = normalized_utc(self.observed_at, "Evidence observation time")
        expires_at = normalized_utc(self.expires_at, "Evidence expiry time")
        if expires_at <= observed_at:
            raise ValueError("Evidence expiry must be after its observation time.")
        _sha256(self.source_payload_hash, "Evidence source payload hash")

    def reference(self) -> "EvidenceReference":
        return EvidenceReference(
            evidence_id=self.evidence_id,
            family=self.family,
            observed_at=self.observed_at,
            expires_at=self.expires_at,
            source_payload_hash=self.source_payload_hash,
        )


@dataclass(frozen=True)
class EvidenceReference:
    """The minimum evidence retained inside hypotheses and confidence state."""

    evidence_id: str
    family: EvidenceFamily
    observed_at: str
    expires_at: str
    source_payload_hash: str

    def __post_init__(self) -> None:
        _required_text(self.evidence_id, "Evidence reference identity")
        if type(self.family) is not EvidenceFamily:
            raise ValueError("Evidence reference family must be explicit.")
        observed_at = normalized_utc(self.observed_at, "Evidence reference observation time")
        expires_at = normalized_utc(self.expires_at, "Evidence reference expiry time")
        if expires_at <= observed_at:
            raise ValueError("Evidence reference expiry must be after observation time.")
        _sha256(self.source_payload_hash, "Evidence reference source payload hash")

    def payload(self) -> dict[str, str]:
        return {
            "evidence_id": self.evidence_id,
            "family": self.family.value,
            "observed_at": normalized_utc(self.observed_at, "Evidence reference observation time"),
            "expires_at": normalized_utc(self.expires_at, "Evidence reference expiry time"),
            "source_payload_hash": self.source_payload_hash,
        }


@dataclass(frozen=True)
class FamilyAssessment:
    """One family contributes once before global confidence is considered.

    Multiple order-flow observations remain inside one assessment rather than
    masquerading as independent global confirmations.
    """

    family: EvidenceFamily
    relation: EvidenceRelation
    evidence: tuple[EvidenceReference, ...]

    def __post_init__(self) -> None:
        if type(self.family) is not EvidenceFamily or type(self.relation) is not EvidenceRelation:
            raise ValueError("Family assessment requires explicit family and relation.")
        if not isinstance(self.evidence, tuple) or not self.evidence:
            raise ValueError("Family assessment evidence must be a non-empty immutable tuple.")
        if any(type(item) is not EvidenceReference or item.family is not self.family for item in self.evidence):
            raise ValueError("Family assessment may contain only references from its declared family.")
        ids = [item.evidence_id for item in self.evidence]
        if len(ids) != len(set(ids)):
            raise ValueError("Family assessment may not repeat an evidence observation.")

    def payload(self) -> dict[str, object]:
        return {
            "family": self.family.value,
            "relation": self.relation.value,
            "evidence": [item.payload() for item in sorted(self.evidence, key=lambda item: item.evidence_id)],
        }


@dataclass(frozen=True)
class ConfidenceState:
    """Stateful, non-numeric confidence semantics for one active hypothesis.

    There is deliberately no score, weight, threshold, or entry boolean in
    this L3-A type.  Contradiction and staleness collapse the state instead of
    being hidden behind an aggregate indicator count.
    """

    hypothesis_id: str
    evaluated_at: str
    expires_at: str
    lifecycle: ConfidenceLifecycle
    family_assessments: tuple[FamilyAssessment, ...]

    def __post_init__(self) -> None:
        _required_text(self.hypothesis_id, "Confidence hypothesis identity")
        evaluated_at = normalized_utc(self.evaluated_at, "Confidence evaluation time")
        expires_at = normalized_utc(self.expires_at, "Confidence expiry time")
        if expires_at <= evaluated_at:
            raise ValueError("Confidence expiry must be after its evaluation time.")
        if type(self.lifecycle) is not ConfidenceLifecycle:
            raise ValueError("Confidence lifecycle must be explicit.")
        if not isinstance(self.family_assessments, tuple):
            raise ValueError("Confidence assessments must be immutable.")
        if any(type(item) is not FamilyAssessment for item in self.family_assessments):
            raise ValueError("Confidence includes an invalid family assessment.")
        families = [item.family for item in self.family_assessments]
        if len(families) != len(set(families)):
            raise ValueError("Confidence may aggregate each evidence family only once.")
        references = [reference.evidence_id for item in self.family_assessments for reference in item.evidence]
        if len(references) != len(set(references)):
            raise ValueError("An evidence observation may not appear in multiple family assessments.")
        any_contradiction = any(item.relation is EvidenceRelation.CONTRADICTS for item in self.family_assessments)
        any_support = any(item.relation is EvidenceRelation.SUPPORTS for item in self.family_assessments)
        if any_contradiction and self.lifecycle is not ConfidenceLifecycle.CONTRADICTED:
            raise ValueError("Contradictory evidence must collapse confidence to CONTRADICTED.")
        if self.lifecycle is ConfidenceLifecycle.CONTRADICTED and not any_contradiction:
            raise ValueError("CONTRADICTED confidence requires a contradictory evidence-family assessment.")
        if self.lifecycle in {ConfidenceLifecycle.BUILDING, ConfidenceLifecycle.ALIGNED} and not any_support:
            raise ValueError("BUILDING or ALIGNED confidence requires at least one supporting evidence family.")
        any_stale = any(
            normalized_utc(reference.expires_at, "Evidence reference expiry time") <= evaluated_at
            for item in self.family_assessments for reference in item.evidence
        )
        if any_stale and self.lifecycle not in {ConfidenceLifecycle.DECAYING, ConfidenceLifecycle.EXPIRED}:
            raise ValueError("Stale evidence requires DECAYING or EXPIRED confidence.")

    @property
    def supporting_families(self) -> tuple[EvidenceFamily, ...]:
        return tuple(item.family for item in self.family_assessments if item.relation is EvidenceRelation.SUPPORTS)

    @property
    def contradicting_families(self) -> tuple[EvidenceFamily, ...]:
        return tuple(item.family for item in self.family_assessments if item.relation is EvidenceRelation.CONTRADICTS)

    def payload(self) -> dict[str, object]:
        return {
            "hypothesis_id": self.hypothesis_id,
            "evaluated_at": normalized_utc(self.evaluated_at, "Confidence evaluation time"),
            "expires_at": normalized_utc(self.expires_at, "Confidence expiry time"),
            "lifecycle": self.lifecycle.value,
            "family_assessments": [
                item.payload() for item in sorted(self.family_assessments, key=lambda item: item.family.value)
            ],
        }

    @property
    def snapshot_hash(self) -> str:
        return canonical_hash(self.payload())


@dataclass(frozen=True)
class MarketHypothesis:
    """A deterministic market thesis that consumes evidence but has no order authority."""

    hypothesis_id: str
    kind: HypothesisKind
    direction: HypothesisDirection
    created_at: str
    expires_at: str
    location: MarketLocation
    strategy_identity: str
    strategy_artifact_hash: str
    supporting_evidence: tuple[EvidenceReference, ...]
    contradictory_evidence: tuple[EvidenceReference, ...]
    validity: HypothesisValidity = HypothesisValidity.ACTIVE

    def __post_init__(self) -> None:
        if type(self.kind) is not HypothesisKind or type(self.direction) is not HypothesisDirection:
            raise ValueError("Hypothesis kind and direction must be explicit.")
        if self.kind is HypothesisKind.UNRESOLVED and self.direction is not HypothesisDirection.NEUTRAL:
            raise ValueError("An unresolved hypothesis must be directionally neutral.")
        if self.kind is not HypothesisKind.UNRESOLVED and self.direction is HypothesisDirection.NEUTRAL:
            raise ValueError("A directional hypothesis must be bullish or bearish.")
        created_at = normalized_utc(self.created_at, "Hypothesis creation time")
        expires_at = normalized_utc(self.expires_at, "Hypothesis expiry time")
        if expires_at <= created_at:
            raise ValueError("Hypothesis expiry must be after creation time.")
        if type(self.location) is not MarketLocation:
            raise ValueError("Hypothesis requires an explicit deterministic market location.")
        _lane_iii_strategy_identity(self.strategy_identity)
        _sha256(self.strategy_artifact_hash, "Hypothesis strategy artifact hash")
        if not isinstance(self.supporting_evidence, tuple) or not isinstance(self.contradictory_evidence, tuple):
            raise ValueError("Hypothesis evidence must be immutable tuples.")
        if any(type(item) is not EvidenceReference for item in self.supporting_evidence + self.contradictory_evidence):
            raise ValueError("Hypothesis evidence must be explicit references.")
        support_ids = {item.evidence_id for item in self.supporting_evidence}
        contradiction_ids = {item.evidence_id for item in self.contradictory_evidence}
        if support_ids & contradiction_ids:
            raise ValueError("One evidence observation cannot both support and contradict a hypothesis.")
        if type(self.validity) is not HypothesisValidity:
            raise ValueError("Hypothesis validity must be explicit.")
        expected = "l3h-" + canonical_hash(self.payload_without_identity())[:32]
        if self.hypothesis_id != expected:
            raise ValueError("Hypothesis identity does not match its deterministic contract payload.")

    @classmethod
    def create(
        cls,
        *,
        kind: HypothesisKind,
        direction: HypothesisDirection,
        created_at: str,
        expires_at: str,
        location: MarketLocation,
        strategy_identity: str,
        strategy_artifact_hash: str,
        supporting_evidence: tuple[EvidenceReference, ...] = (),
        contradictory_evidence: tuple[EvidenceReference, ...] = (),
        validity: HypothesisValidity = HypothesisValidity.ACTIVE,
    ) -> "MarketHypothesis":
        payload = cls._payload_without_identity_static(
            kind, direction, created_at, expires_at, location, strategy_identity, strategy_artifact_hash,
            supporting_evidence, contradictory_evidence, validity,
        )
        return cls(
            hypothesis_id="l3h-" + canonical_hash(payload)[:32], kind=kind, direction=direction,
            created_at=created_at, expires_at=expires_at, location=location,
            strategy_identity=strategy_identity, strategy_artifact_hash=strategy_artifact_hash,
            supporting_evidence=supporting_evidence, contradictory_evidence=contradictory_evidence, validity=validity,
        )

    @staticmethod
    def _payload_without_identity_static(
        kind: HypothesisKind, direction: HypothesisDirection, created_at: str, expires_at: str,
        location: MarketLocation, strategy_identity: str, strategy_artifact_hash: str,
        supporting_evidence: tuple[EvidenceReference, ...], contradictory_evidence: tuple[EvidenceReference, ...],
        validity: HypothesisValidity,
    ) -> dict[str, object]:
        return {
            "kind": kind.value,
            "direction": direction.value,
            "created_at": normalized_utc(created_at, "Hypothesis creation time"),
            "expires_at": normalized_utc(expires_at, "Hypothesis expiry time"),
            "location": location.payload(),
            "strategy_identity": strategy_identity,
            "strategy_artifact_hash": strategy_artifact_hash,
            "supporting_evidence": [item.payload() for item in sorted(supporting_evidence, key=lambda item: item.evidence_id)],
            "contradictory_evidence": [item.payload() for item in sorted(contradictory_evidence, key=lambda item: item.evidence_id)],
            "validity": validity.value,
        }

    def payload_without_identity(self) -> dict[str, object]:
        return self._payload_without_identity_static(
            self.kind, self.direction, self.created_at, self.expires_at, self.location, self.strategy_identity,
            self.strategy_artifact_hash, self.supporting_evidence, self.contradictory_evidence, self.validity,
        )

    def payload(self) -> dict[str, object]:
        return {"hypothesis_id": self.hypothesis_id, **self.payload_without_identity()}


@dataclass(frozen=True)
class LaneIIIStrategyArtifact:
    """An immutable future strategy artifact identity, not an admitted trading strategy."""

    strategy_id: str
    strategy_version: str
    strategy_artifact_hash: str
    strategy_instrument: LaneIIIInstrument

    def __post_init__(self) -> None:
        if not isinstance(self.strategy_id, str) or re.fullmatch(r"l3-[a-z0-9][a-z0-9-]{1,62}", self.strategy_id) is None:
            raise StrategyProvenanceRefused("Lane III strategy IDs must be constrained l3-* identifiers.")
        _required_text(self.strategy_version, "Lane III strategy version")
        _sha256(self.strategy_artifact_hash, "Lane III strategy artifact hash")
        if self.strategy_instrument is not LaneIIIInstrument.MNQ:
            raise InstrumentRefused("Lane III strategy artifacts may admit MNQ only in L3-A.")

    def payload(self) -> dict[str, str]:
        return {
            "strategy_id": self.strategy_id,
            "strategy_version": self.strategy_version,
            "strategy_artifact_hash": self.strategy_artifact_hash,
            "strategy_instrument": self.strategy_instrument.value,
        }

    @property
    def strategy_identity(self) -> str:
        return "l3-strategy-" + canonical_hash(self.payload())[:32]


def _lane_iii_strategy_identity(value: object) -> str:
    if not isinstance(value, str) or re.fullmatch(r"l3-strategy-[0-9a-f]{32}", value) is None:
        raise StrategyProvenanceRefused("Lane III strategy identity is missing or malformed.")
    return value


@dataclass(frozen=True)
class ProtectiveStopSemantics:
    """A required protective-stop request; risk remains sovereign outside the strategy."""

    kind: StopProtectionKind
    trigger_price: Decimal

    def __post_init__(self) -> None:
        if self.kind is not StopProtectionKind.PRICE_STOP:
            raise IntentRefused("L3-A supports only explicit price-stop semantics in a future intent.")
        _decimal(self.trigger_price, "Protective-stop trigger price", positive=True)

    def payload(self) -> dict[str, str]:
        return {"kind": self.kind.value, "trigger_price": str(self.trigger_price)}


@dataclass(frozen=True)
class ProfitTakingSemantics:
    """Optional, bounded profit-taking semantics; no order operation is exposed."""

    kind: ProfitTakingKind
    target_price: Decimal | None = None

    def __post_init__(self) -> None:
        if type(self.kind) is not ProfitTakingKind:
            raise IntentRefused("Profit-taking kind must be explicit.")
        if self.kind is ProfitTakingKind.NONE and self.target_price is not None:
            raise IntentRefused("No-profit-taking semantics cannot carry a target price.")
        if self.kind is ProfitTakingKind.LIMIT_TARGET:
            if self.target_price is None:
                raise IntentRefused("A limit target requires a target price.")
            _decimal(self.target_price, "Profit target price", positive=True)

    def payload(self) -> dict[str, str | None]:
        return {"kind": self.kind.value, "target_price": None if self.target_price is None else str(self.target_price)}


@dataclass(frozen=True)
class ExecutionIntent:
    """The narrowest future request from an L3 strategy to a futures sovereign.

    It intentionally has no broker, account, follower, amend, cancel,
    withdrawal, credential, or arbitrary command field.  It requests desired
    exposure only; an external futures service must independently admit it.
    """

    intent_id: str
    strategy_identity: str
    strategy_artifact_hash: str
    hypothesis_id: str
    instrument: FuturesExecutionInstrument
    purpose: IntentPurpose
    target_exposure: TargetExposure
    quantity: int
    created_at: str
    expires_at: str
    protective_stop: ProtectiveStopSemantics | None
    profit_taking: ProfitTakingSemantics
    confidence_snapshot_hash: str
    evidence_state_hash: str

    def __post_init__(self) -> None:
        _lane_iii_strategy_identity(self.strategy_identity)
        _sha256(self.strategy_artifact_hash, "Intent strategy artifact hash")
        _required_text(self.hypothesis_id, "Intent hypothesis identity")
        if type(self.instrument) is not FuturesExecutionInstrument:
            raise IntentRefused("Execution intent requires a concrete MNQ futures expiry.")
        if type(self.purpose) is not IntentPurpose or type(self.target_exposure) is not TargetExposure:
            raise IntentRefused("Intent purpose and target exposure must be explicit.")
        if type(self.quantity) is not int or isinstance(self.quantity, bool) or self.quantity <= 0:
            raise IntentRefused("Intent quantity must be a positive whole MNQ contract count.")
        created_at = normalized_utc(self.created_at, "Intent creation time")
        expires_at = normalized_utc(self.expires_at, "Intent expiry time")
        if expires_at <= created_at:
            raise IntentRefused("Intent expiry must be after creation time.")
        if self.purpose is IntentPurpose.ENTRY:
            if self.target_exposure is TargetExposure.FLAT or type(self.protective_stop) is not ProtectiveStopSemantics:
                raise IntentRefused("An entry must request long/short exposure and an explicit protective stop.")
        elif self.target_exposure is not TargetExposure.FLAT:
            raise IntentRefused("A strategy exit must request flat target exposure, not a reversal.")
        if type(self.profit_taking) is not ProfitTakingSemantics:
            raise IntentRefused("Intent profit-taking semantics must be explicit.")
        _sha256(self.confidence_snapshot_hash, "Intent confidence snapshot hash")
        _sha256(self.evidence_state_hash, "Intent evidence-state hash")
        expected = "l3i-" + canonical_hash(self.payload_without_identity())[:32]
        if self.intent_id != expected:
            raise IntentRefused("Intent identity does not match its deterministic contract payload.")

    @classmethod
    def create(
        cls,
        *,
        strategy_identity: str,
        strategy_artifact_hash: str,
        hypothesis_id: str,
        instrument: FuturesExecutionInstrument,
        purpose: IntentPurpose,
        target_exposure: TargetExposure,
        quantity: int,
        created_at: str,
        expires_at: str,
        protective_stop: ProtectiveStopSemantics | None,
        profit_taking: ProfitTakingSemantics,
        confidence_snapshot_hash: str,
        evidence_state_hash: str,
    ) -> "ExecutionIntent":
        payload = cls._payload_without_identity_static(
            strategy_identity, strategy_artifact_hash, hypothesis_id, instrument, purpose, target_exposure, quantity,
            created_at, expires_at, protective_stop, profit_taking, confidence_snapshot_hash, evidence_state_hash,
        )
        return cls(
            intent_id="l3i-" + canonical_hash(payload)[:32], strategy_identity=strategy_identity,
            strategy_artifact_hash=strategy_artifact_hash, hypothesis_id=hypothesis_id, instrument=instrument,
            purpose=purpose, target_exposure=target_exposure, quantity=quantity, created_at=created_at,
            expires_at=expires_at, protective_stop=protective_stop, profit_taking=profit_taking,
            confidence_snapshot_hash=confidence_snapshot_hash, evidence_state_hash=evidence_state_hash,
        )

    @staticmethod
    def _payload_without_identity_static(
        strategy_identity: str, strategy_artifact_hash: str, hypothesis_id: str, instrument: FuturesExecutionInstrument,
        purpose: IntentPurpose, target_exposure: TargetExposure, quantity: int, created_at: str, expires_at: str,
        protective_stop: ProtectiveStopSemantics | None, profit_taking: ProfitTakingSemantics,
        confidence_snapshot_hash: str, evidence_state_hash: str,
    ) -> dict[str, object]:
        return {
            "strategy_identity": strategy_identity,
            "strategy_artifact_hash": strategy_artifact_hash,
            "hypothesis_id": hypothesis_id,
            "instrument": instrument.payload(),
            "purpose": purpose.value,
            "target_exposure": target_exposure.value,
            "quantity": quantity,
            "created_at": normalized_utc(created_at, "Intent creation time"),
            "expires_at": normalized_utc(expires_at, "Intent expiry time"),
            "protective_stop": None if protective_stop is None else protective_stop.payload(),
            "profit_taking": profit_taking.payload(),
            "confidence_snapshot_hash": confidence_snapshot_hash,
            "evidence_state_hash": evidence_state_hash,
        }

    def payload_without_identity(self) -> dict[str, object]:
        return self._payload_without_identity_static(
            self.strategy_identity, self.strategy_artifact_hash, self.hypothesis_id, self.instrument, self.purpose,
            self.target_exposure, self.quantity, self.created_at, self.expires_at, self.protective_stop,
            self.profit_taking, self.confidence_snapshot_hash, self.evidence_state_hash,
        )

    def payload(self) -> dict[str, object]:
        return {"intent_id": self.intent_id, **self.payload_without_identity()}

    @property
    def intent_hash(self) -> str:
        return canonical_hash(self.payload())

    @property
    def execution_authority(self) -> bool:
        return False

    @property
    def live_capital_authority(self) -> bool:
        return False


@dataclass(frozen=True)
class FuturesRiskConstitution:
    """Hard, external limits.  They are absent from strategy artifacts and intents."""

    maximum_mnq_exposure: int
    maximum_individual_order_quantity: int
    maximum_daily_loss: Decimal
    maximum_session_loss: Decimal
    admitted_execution_instrument: FuturesExecutionInstrument
    position_mode: PositionMode = PositionMode.ONE_POSITION

    def __post_init__(self) -> None:
        for value, field in (
            (self.maximum_mnq_exposure, "Maximum MNQ exposure"),
            (self.maximum_individual_order_quantity, "Maximum individual order quantity"),
        ):
            if type(value) is not int or isinstance(value, bool) or value <= 0:
                raise ValueError(f"{field} must be a positive whole contract count.")
        if self.maximum_individual_order_quantity > self.maximum_mnq_exposure:
            raise ValueError("Maximum individual order quantity may not exceed maximum MNQ exposure.")
        object.__setattr__(self, "maximum_daily_loss", _decimal(self.maximum_daily_loss, "Maximum daily loss", positive=True))
        object.__setattr__(self, "maximum_session_loss", _decimal(self.maximum_session_loss, "Maximum session loss", positive=True))
        if type(self.admitted_execution_instrument) is not FuturesExecutionInstrument:
            raise InstrumentRefused("Hard risk configuration must explicitly admit one concrete MNQ expiry.")
        if self.position_mode is not PositionMode.ONE_POSITION:
            raise ValueError("L3-A supports only explicit one-position semantics.")


@dataclass(frozen=True)
class SessionRiskState:
    session_id: str
    daily_loss: Decimal
    session_loss: Decimal

    def __post_init__(self) -> None:
        _required_text(self.session_id, "Futures session identity")
        object.__setattr__(self, "daily_loss", _decimal(self.daily_loss, "Daily loss", nonnegative=True))
        object.__setattr__(self, "session_loss", _decimal(self.session_loss, "Session loss", nonnegative=True))


@dataclass(frozen=True)
class FuturesTimeContext:
    """One canonical context for UTC, exchange, session, and display time.

    The display zone is validated only for rendering; it may never be used as
    a substitute for exchange timestamp or trading-session identity.
    """

    observed_at_utc: str
    exchange_timestamp_utc: str
    trading_session_id: str
    session_phase: FuturesSessionPhase
    display_timezone: str
    economic_event_context_hash: str
    expiration_context_hash: str

    def __post_init__(self) -> None:
        normalized_utc(self.observed_at_utc, "Observed UTC time")
        normalized_utc(self.exchange_timestamp_utc, "Exchange timestamp")
        _required_text(self.trading_session_id, "Trading session identity")
        if type(self.session_phase) is not FuturesSessionPhase:
            raise ValueError("Futures session phase must be explicit.")
        if not isinstance(self.display_timezone, str) or re.fullmatch(
            r"(?:UTC|(?:Africa|America|Antarctica|Asia|Atlantic|Australia|Europe|Indian|Pacific|Etc)/[A-Za-z0-9_+\-]+)",
            self.display_timezone,
        ) is None:
            raise ValueError("Display timezone must be a well-formed IANA timezone.")
        _sha256(self.economic_event_context_hash, "Economic-event context hash")
        _sha256(self.expiration_context_hash, "Expiration context hash")

    def payload(self) -> dict[str, str]:
        return {
            "observed_at_utc": normalized_utc(self.observed_at_utc, "Observed UTC time"),
            "exchange_timestamp_utc": normalized_utc(self.exchange_timestamp_utc, "Exchange timestamp"),
            "trading_session_id": self.trading_session_id,
            "session_phase": self.session_phase.value,
            "display_timezone": self.display_timezone,
            "economic_event_context_hash": self.economic_event_context_hash,
            "expiration_context_hash": self.expiration_context_hash,
        }


@dataclass(frozen=True)
class SafetySnapshot:
    """Authoritative state required before a future exposure request is even eligible."""

    observed_at: str
    market_data: KnowledgeState
    broker_state: KnowledgeState
    position_state: PositionKnowledge
    position_quantity: int | None
    working_orders: WorkingOrderKnowledge
    session_risk: SessionRiskState

    def __post_init__(self) -> None:
        normalized_utc(self.observed_at, "Safety snapshot time")
        if type(self.market_data) is not KnowledgeState or type(self.broker_state) is not KnowledgeState:
            raise ValueError("Market and broker state must be explicit knowledge states.")
        if type(self.position_state) is not PositionKnowledge or type(self.working_orders) is not WorkingOrderKnowledge:
            raise ValueError("Position and working-order state must be explicit.")
        if type(self.session_risk) is not SessionRiskState:
            raise ValueError("Safety snapshot requires a session-risk state.")
        if self.position_state is PositionKnowledge.UNKNOWN:
            if self.position_quantity is not None:
                raise ValueError("Unknown position state must not claim a position quantity.")
        else:
            if type(self.position_quantity) is not int or isinstance(self.position_quantity, bool):
                raise ValueError("Known position state requires an exact whole-contract quantity.")
            if self.position_state is PositionKnowledge.FLAT and self.position_quantity != 0:
                raise ValueError("FLAT is exact zero only; UNKNOWN is never flat.")
            if self.position_state is PositionKnowledge.LONG and self.position_quantity <= 0:
                raise ValueError("LONG position state requires a positive quantity.")
            if self.position_state is PositionKnowledge.SHORT and self.position_quantity >= 0:
                raise ValueError("SHORT position state requires a negative quantity.")


@dataclass(frozen=True)
class AuthorityRecord:
    capability: AuthorityCapability
    status: AuthorityStatus
    basis: str

    def __post_init__(self) -> None:
        if type(self.capability) is not AuthorityCapability or type(self.status) is not AuthorityStatus:
            raise ValueError("Authority record requires explicit capability and status.")
        _required_text(self.basis, "Authority basis")

    def payload(self) -> dict[str, str]:
        return {"capability": self.capability.value, "status": self.status.value, "basis": self.basis}


@dataclass(frozen=True)
class AuthorityManifest:
    schema: str
    version: str
    records: tuple[AuthorityRecord, ...]

    def __post_init__(self) -> None:
        if self.schema != L3A_SCHEMA or self.version != L3A_VERSION:
            raise ValueError("Unsupported Lane III authority manifest.")
        if not isinstance(self.records, tuple) or not self.records:
            raise ValueError("Authority manifest must be an immutable non-empty tuple.")
        if any(type(item) is not AuthorityRecord for item in self.records):
            raise ValueError("Authority manifest contains an invalid record.")
        capabilities = [item.capability for item in self.records]
        if set(capabilities) != set(AuthorityCapability) or len(capabilities) != len(set(capabilities)):
            raise ValueError("Authority manifest must make one explicit decision for every L3-A capability.")

    def payload(self) -> dict[str, object]:
        return {
            "schema": self.schema,
            "version": self.version,
            "records": [item.payload() for item in sorted(self.records, key=lambda item: item.capability.value)],
        }

    @property
    def manifest_hash(self) -> str:
        return canonical_hash(self.payload())

    def record_for(self, capability: AuthorityCapability) -> AuthorityRecord:
        if type(capability) is not AuthorityCapability:
            raise LaneIIIRefused("Authority lookups require an explicit Lane III capability.")
        return next(item for item in self.records if item.capability is capability)


def _authority_records() -> tuple[AuthorityRecord, ...]:
    architecture = {
        AuthorityCapability.OBSERVE_MNQ_MARKET_DATA,
        AuthorityCapability.OBSERVE_DOM_RESTING_LIQUIDITY,
        AuthorityCapability.OBSERVE_OPTIONS_CONTEXT,
    }
    contract = {
        AuthorityCapability.CONSTRUCT_MARKET_HYPOTHESES,
        AuthorityCapability.REQUEST_FUTURES_EXECUTION,
    }
    semantic = {AuthorityCapability.COMPUTE_CONFIDENCE}
    records: list[AuthorityRecord] = []
    for capability in AuthorityCapability:
        status = (
            AuthorityStatus.ARCHITECTURE_ONLY if capability in architecture
            else AuthorityStatus.CONTRACT_ONLY if capability in contract
            else AuthorityStatus.SEMANTICS_ONLY if capability in semantic
            else AuthorityStatus.DENIED
        )
        records.append(AuthorityRecord(capability, status, "L3_A_CONSTITUTIONAL_BOUNDARY"))
    return tuple(records)


L3A_MANIFEST = AuthorityManifest(L3A_SCHEMA, L3A_VERSION, _authority_records())
L3A_AUTHORITY_MANIFEST_HASH = L3A_MANIFEST.manifest_hash


def require_l3a_manifest(manifest: object = L3A_MANIFEST) -> AuthorityManifest:
    if type(manifest) is not AuthorityManifest or manifest.manifest_hash != L3A_AUTHORITY_MANIFEST_HASH:
        raise LaneIIIRefused("Lane III authority manifest is missing, altered, or ambiguous.")
    return manifest
