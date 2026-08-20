"""Lane III Phase C deterministic hypothesis and confidence engine.

This module is intentionally downstream of the frozen L3-B observation
substrate.  It turns replayable observations into inspectable evidence and
relative-support assessments.  It has no transport, account, sizing, risk,
or scientific dependencies.  A score in this module is interpretation state,
not a probability or a command.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from typing import Iterable

from .contracts import (
    EvidenceFamily,
    EvidenceRelation,
    HypothesisDirection,
    HypothesisKind,
    L3A_AUTHORITY_MANIFEST_HASH,
    LaneIIIInstrument,
    LaneIIIRefused,
    canonical_hash,
    normalized_utc,
)
from .market_data import (
    AggressorSide,
    BookApplication,
    BookApplyOutcome,
    BookChange,
    BookDeltaEvent,
    BookSide,
    BookSnapshotEvent,
    CanonicalMarketEvent,
    DataQuality,
    DerivativesContextEvent,
    LiquidityBehavior,
    L3B_L3A_CONSTITUTION,
    L3B_VERSION,
    MNQContract,
    MarketDataPipeline,
    MarketDataSource,
    OrderingOutcome,
    PipelineResult,
    SessionContext,
    TradeEvent,
)


L3C_SCHEMA = "lane-iii-phase-c-hypothesis-confidence-v1"
L3C_VERSION = "lane-iii-phase-c-v1"
# L3-B already fails closed against L3-A. Retaining this identity makes the
# Phase C boundary explicit in configuration and commissioning inspection.
L3C_L3B_CONSTITUTION = L3B_L3A_CONSTITUTION
L3C_L3A_AUTHORITY_MANIFEST_HASH = L3A_AUTHORITY_MANIFEST_HASH


class HypothesisEngineRefused(LaneIIIRefused):
    """An input cannot be evaluated safely by the interpretation layer."""


class EvidenceKind(StrEnum):
    """Machine-testable evidence labels.  None asserts participant intent."""

    ABOVE_VWAP = "ABOVE_VWAP"
    BELOW_VWAP = "BELOW_VWAP"
    RANGE_EXPANSION_UP = "RANGE_EXPANSION_UP"
    RANGE_EXPANSION_DOWN = "RANGE_EXPANSION_DOWN"
    RANGE_RECLAIM_UP = "RANGE_RECLAIM_UP"
    RANGE_RECLAIM_DOWN = "RANGE_RECLAIM_DOWN"
    AGGRESSIVE_BUY_IMBALANCE = "AGGRESSIVE_BUY_IMBALANCE"
    AGGRESSIVE_SELL_IMBALANCE = "AGGRESSIVE_SELL_IMBALANCE"
    SELLING_WITHOUT_DOWNWARD_PROGRESS = "SELLING_WITHOUT_DOWNWARD_PROGRESS"
    BUYING_WITHOUT_UPWARD_PROGRESS = "BUYING_WITHOUT_UPWARD_PROGRESS"
    BID_REPLENISHMENT = "BID_REPLENISHMENT"
    OFFER_REPLENISHMENT = "OFFER_REPLENISHMENT"
    BID_LIQUIDITY_PULL = "BID_LIQUIDITY_PULL"
    OFFER_LIQUIDITY_PULL = "OFFER_LIQUIDITY_PULL"
    SESSION_PHASE = "SESSION_PHASE"
    DERIVATIVES_VINTAGE = "DERIVATIVES_VINTAGE"


class EvidenceDerivation(StrEnum):
    RAW_DERIVED = "RAW_DERIVED"
    HIGHER_ORDER_DERIVED = "HIGHER_ORDER_DERIVED"


class EvidenceUsability(StrEnum):
    """Whether an evidence object may contribute at the current evaluation."""

    AUTHORITATIVE = "AUTHORITATIVE"
    DEGRADED = "DEGRADED"
    UNUSABLE = "UNUSABLE"
    EXPIRED = "EXPIRED"


class EvidenceDeactivationReason(StrEnum):
    EXPIRED = "EXPIRED"
    SOURCE_STALE = "SOURCE_STALE"
    SOURCE_GAPPED = "SOURCE_GAPPED"
    SOURCE_RECOVERING = "SOURCE_RECOVERING"
    SOURCE_INCOMPLETE = "SOURCE_INCOMPLETE"
    SOURCE_INVALID = "SOURCE_INVALID"
    RETENTION_BOUND = "RETENTION_BOUND"


class HypothesisState(StrEnum):
    FORMING = "FORMING"
    ACTIVE = "ACTIVE"
    CONFLICTED = "CONFLICTED"
    DECAYING = "DECAYING"
    INVALIDATED = "INVALIDATED"
    EXPIRED = "EXPIRED"


def _decimal(value: object, field: str, *, unit_interval: bool = False) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{field} must be a finite decimal.") from exc
    if not result.is_finite():
        raise ValueError(f"{field} must be a finite decimal.")
    if unit_interval and not Decimal(0) <= result <= Decimal(1):
        raise ValueError(f"{field} must be in [0, 1].")
    return result


def _timestamp(value: object, field: str) -> datetime:
    return datetime.fromisoformat(normalized_utc(value, field).replace("Z", "+00:00"))


def _timestamp_text(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _positive_duration(value: object, field: str) -> timedelta:
    if not isinstance(value, timedelta) or value <= timedelta(0) or value.total_seconds() % 1:
        raise ValueError(f"{field} must be a positive whole-second timedelta.")
    return value


@dataclass(frozen=True)
class HypothesisIdentity:
    """The deliberately narrow set of directional market interpretations."""

    kind: HypothesisKind
    direction: HypothesisDirection

    def __post_init__(self) -> None:
        if type(self.kind) is not HypothesisKind or type(self.direction) is not HypothesisDirection:
            raise ValueError("Hypothesis identity requires explicit kind and direction.")
        if self.kind is HypothesisKind.UNRESOLVED or self.direction is HypothesisDirection.NEUTRAL:
            raise ValueError("L3-C directional identities are reversal or continuation only.")

    @property
    def key(self) -> str:
        return f"{self.direction.value.lower()}-{self.kind.value.lower()}"

    def payload(self) -> dict[str, str]:
        return {"kind": self.kind.value, "direction": self.direction.value}


BULLISH_REVERSAL = HypothesisIdentity(HypothesisKind.REVERSAL, HypothesisDirection.BULLISH)
BEARISH_REVERSAL = HypothesisIdentity(HypothesisKind.REVERSAL, HypothesisDirection.BEARISH)
BULLISH_CONTINUATION = HypothesisIdentity(HypothesisKind.CONTINUATION, HypothesisDirection.BULLISH)
BEARISH_CONTINUATION = HypothesisIdentity(HypothesisKind.CONTINUATION, HypothesisDirection.BEARISH)


@dataclass(frozen=True)
class SourceProvenance:
    """A traceable L3-B observation window behind one L3-C evidence object."""

    source_event_ids: tuple[str, ...]
    source_payload_hashes: tuple[str, ...]
    window_start: str
    window_end: str
    source_quality: DataQuality
    source_observation_id: str

    def __post_init__(self) -> None:
        if not isinstance(self.source_event_ids, tuple) or not self.source_event_ids or any(
            not isinstance(value, str) or not value for value in self.source_event_ids
        ):
            raise ValueError("Source provenance requires one or more canonical event identities.")
        if len(self.source_event_ids) != len(set(self.source_event_ids)):
            raise ValueError("Source provenance may not repeat a canonical event identity.")
        if not isinstance(self.source_payload_hashes, tuple) or len(self.source_payload_hashes) != len(self.source_event_ids):
            raise ValueError("Source provenance must retain one payload hash per event identity.")
        if any(not isinstance(value, str) or len(value) != 64 for value in self.source_payload_hashes):
            raise ValueError("Source payload hashes must be SHA-256 values.")
        start = _timestamp(self.window_start, "Source window start")
        end = _timestamp(self.window_end, "Source window end")
        if end < start:
            raise ValueError("Source window end may not precede its start.")
        if type(self.source_quality) is not DataQuality:
            raise ValueError("Source quality must be an explicit L3-B state.")
        if not isinstance(self.source_observation_id, str) or not self.source_observation_id:
            raise ValueError("Source observation identity is required.")

    def payload(self) -> dict[str, object]:
        return {
            "source_event_ids": list(self.source_event_ids),
            "source_payload_hashes": list(self.source_payload_hashes),
            "window_start": normalized_utc(self.window_start, "Source window start"),
            "window_end": normalized_utc(self.window_end, "Source window end"),
            "source_quality": self.source_quality.value,
            "source_observation_id": self.source_observation_id,
        }


@dataclass(frozen=True)
class EvidenceImpact:
    """One explicit support or contradiction relationship, with no decision state."""

    hypothesis: HypothesisIdentity
    relation: EvidenceRelation
    strength: Decimal
    invalidates: bool = False

    def __post_init__(self) -> None:
        if type(self.hypothesis) is not HypothesisIdentity or type(self.relation) is not EvidenceRelation:
            raise ValueError("Evidence impact requires explicit hypothesis and relation.")
        if self.relation is EvidenceRelation.INCONCLUSIVE:
            raise ValueError("Inconclusive observations must not create a hypothesis impact.")
        _decimal(self.strength, "Evidence impact strength", unit_interval=True)
        if self.strength == 0:
            raise ValueError("Evidence impact strength must be positive.")
        if self.invalidates and self.relation is not EvidenceRelation.CONTRADICTS:
            raise ValueError("Only contradictory evidence may invalidate a hypothesis.")

    def payload(self) -> dict[str, object]:
        return {
            "hypothesis": self.hypothesis.payload(),
            "relation": self.relation.value,
            "strength": str(self.strength),
            "invalidates": self.invalidates,
        }


@dataclass(frozen=True)
class EvidenceObject:
    """An immutable derived market observation with complete source provenance."""

    evidence_id: str
    family: EvidenceFamily
    kind: EvidenceKind
    derivation: EvidenceDerivation
    created_at: str
    expires_at: str
    source: SourceProvenance
    impacts: tuple[EvidenceImpact, ...]
    correlation_key: str
    measurement: Decimal | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.evidence_id, str) or not self.evidence_id.startswith("l3c-e-"):
            raise ValueError("Evidence identity must be a deterministic L3-C identity.")
        if type(self.family) is not EvidenceFamily or type(self.kind) is not EvidenceKind:
            raise ValueError("Evidence family and kind must be explicit.")
        if type(self.derivation) is not EvidenceDerivation or type(self.source) is not SourceProvenance:
            raise ValueError("Evidence derivation and source must be explicit.")
        created = _timestamp(self.created_at, "Evidence creation time")
        expires = _timestamp(self.expires_at, "Evidence expiry time")
        if expires <= created:
            raise ValueError("Evidence expiry must follow creation time.")
        if not isinstance(self.impacts, tuple) or any(type(value) is not EvidenceImpact for value in self.impacts):
            raise ValueError("Evidence impacts must be an immutable tuple.")
        keys = [(value.hypothesis.key, value.relation.value) for value in self.impacts]
        if len(keys) != len(set(keys)):
            raise ValueError("An evidence object may not repeat a hypothesis/relation impact.")
        if not isinstance(self.correlation_key, str) or not self.correlation_key:
            raise ValueError("Evidence correlation key is required.")
        if self.measurement is not None:
            _decimal(self.measurement, "Evidence measurement")

    def payload_without_identity(self) -> dict[str, object]:
        return {
            "family": self.family.value,
            "kind": self.kind.value,
            "derivation": self.derivation.value,
            "created_at": normalized_utc(self.created_at, "Evidence creation time"),
            "expires_at": normalized_utc(self.expires_at, "Evidence expiry time"),
            "source": self.source.payload(),
            "impacts": [value.payload() for value in self.impacts],
            "correlation_key": self.correlation_key,
            "measurement": None if self.measurement is None else str(self.measurement),
        }

    @property
    def snapshot_hash(self) -> str:
        return canonical_hash({"evidence_id": self.evidence_id, **self.payload_without_identity()})


@dataclass(frozen=True)
class EvidenceState:
    evidence: EvidenceObject
    usability: EvidenceUsability
    deactivated_at: str | None = None
    deactivation_reason: EvidenceDeactivationReason | None = None

    def __post_init__(self) -> None:
        if type(self.evidence) is not EvidenceObject or type(self.usability) is not EvidenceUsability:
            raise ValueError("Evidence state requires an evidence object and explicit usability.")
        if self.usability is EvidenceUsability.AUTHORITATIVE and self.deactivated_at is not None:
            raise ValueError("Authoritative evidence may not have a deactivation time.")
        if self.usability is not EvidenceUsability.AUTHORITATIVE:
            if self.deactivated_at is None or type(self.deactivation_reason) is not EvidenceDeactivationReason:
                raise ValueError("Non-authoritative evidence requires a deterministic deactivation reason.")
            _timestamp(self.deactivated_at, "Evidence deactivation time")


@dataclass(frozen=True)
class RejectedObservation:
    """Visible record that a source observation was not allowed to create evidence."""

    event_id: str
    observed_at: str
    family: EvidenceFamily
    source_quality: DataQuality
    reason: EvidenceDeactivationReason

    def __post_init__(self) -> None:
        if not isinstance(self.event_id, str) or not self.event_id:
            raise ValueError("Rejected observation event identity is required.")
        _timestamp(self.observed_at, "Rejected observation time")
        if type(self.family) is not EvidenceFamily or type(self.source_quality) is not DataQuality:
            raise ValueError("Rejected observation requires explicit family and quality.")
        if type(self.reason) is not EvidenceDeactivationReason:
            raise ValueError("Rejected observation requires an explicit reason.")


@dataclass(frozen=True)
class FamilyContribution:
    """One family-level balance; evidence count never becomes an extra vote."""

    family: EvidenceFamily
    supporting_evidence_ids: tuple[str, ...]
    contradictory_evidence_ids: tuple[str, ...]
    strongest_support: Decimal
    strongest_contradiction: Decimal
    balance: Decimal

    def __post_init__(self) -> None:
        if type(self.family) is not EvidenceFamily:
            raise ValueError("Family contribution requires an explicit family.")
        for field, value in (
            ("Strongest support", self.strongest_support),
            ("Strongest contradiction", self.strongest_contradiction),
        ):
            _decimal(value, field, unit_interval=True)
        if self.balance != self.strongest_support - self.strongest_contradiction:
            raise ValueError("Family balance must equal strongest support minus strongest contradiction.")
        if not Decimal(-1) <= self.balance <= Decimal(1):
            raise ValueError("Family balance must be in [-1, 1].")


@dataclass(frozen=True)
class ConfidenceAssessment:
    """Explainable non-probabilistic relative support assessment for a hypothesis."""

    evaluated_at: str
    relative_support: Decimal
    family_contributions: tuple[FamilyContribution, ...]

    def __post_init__(self) -> None:
        _timestamp(self.evaluated_at, "Confidence evaluation time")
        _decimal(self.relative_support, "Relative support", unit_interval=True)
        if not isinstance(self.family_contributions, tuple) or any(type(value) is not FamilyContribution for value in self.family_contributions):
            raise ValueError("Confidence assessments require immutable family contributions.")
        families = [value.family for value in self.family_contributions]
        if len(families) != len(set(families)):
            raise ValueError("A confidence assessment may include each family once.")


@dataclass(frozen=True)
class HypothesisRecord:
    hypothesis_id: str
    identity: HypothesisIdentity
    created_at: str
    last_updated_at: str
    expires_at: str
    state: HypothesisState
    confidence: ConfidenceAssessment
    supporting_evidence_ids: tuple[str, ...]
    contradictory_evidence_ids: tuple[str, ...]
    invalidated_by_evidence_ids: tuple[str, ...]
    configuration_hash: str

    def __post_init__(self) -> None:
        if not isinstance(self.hypothesis_id, str) or not self.hypothesis_id.startswith("l3c-h-"):
            raise ValueError("Hypothesis identity must be a deterministic L3-C identity.")
        if type(self.identity) is not HypothesisIdentity or type(self.state) is not HypothesisState:
            raise ValueError("Hypothesis requires explicit identity and lifecycle state.")
        created = _timestamp(self.created_at, "Hypothesis creation time")
        updated = _timestamp(self.last_updated_at, "Hypothesis update time")
        expires = _timestamp(self.expires_at, "Hypothesis expiry time")
        if updated < created or expires <= created:
            raise ValueError("Hypothesis timestamps are inconsistent.")
        if type(self.confidence) is not ConfidenceAssessment:
            raise ValueError("Hypothesis requires an explicit confidence assessment.")
        if not isinstance(self.configuration_hash, str) or len(self.configuration_hash) != 64:
            raise ValueError("Hypothesis configuration hash must be SHA-256 text.")


@dataclass(frozen=True)
class ConfidenceUpdate:
    hypothesis_id: str
    evaluated_at: str
    previous_relative_support: Decimal | None
    current_relative_support: Decimal
    added_evidence_ids: tuple[str, ...]
    removed_evidence_ids: tuple[str, ...]
    family_contributions: tuple[FamilyContribution, ...]


@dataclass(frozen=True)
class HypothesisEngineMetrics:
    observations_processed: int
    evidence_objects_created: int
    evidence_expired: int
    active_hypotheses: int
    confidence_updates: int
    invalid_source_observations: int
    retained_evidence_objects: int
    retained_hypothesis_records: int


@dataclass(frozen=True)
class HypothesisEngineSnapshot:
    evaluated_at: str
    configuration_hash: str
    evidence: tuple[EvidenceState, ...]
    hypotheses: tuple[HypothesisRecord, ...]
    dominant_hypothesis_id: str | None
    rejected_observations: tuple[RejectedObservation, ...]
    confidence_updates: tuple[ConfidenceUpdate, ...]
    metrics: HypothesisEngineMetrics

    @property
    def snapshot_hash(self) -> str:
        return canonical_hash(
            {
                "evaluated_at": normalized_utc(self.evaluated_at, "Snapshot evaluation time"),
                "configuration_hash": self.configuration_hash,
                "evidence": [state.evidence.snapshot_hash + state.usability.value for state in self.evidence],
                "hypotheses": [
                    {
                        "id": item.hypothesis_id,
                        "state": item.state.value,
                        "score": str(item.confidence.relative_support),
                        "support": list(item.supporting_evidence_ids),
                        "contradiction": list(item.contradictory_evidence_ids),
                    }
                    for item in self.hypotheses
                ],
                "dominant_hypothesis_id": self.dominant_hypothesis_id,
            }
        )


@dataclass(frozen=True)
class HypothesisEngineConfig:
    """Versioned evaluation policy, separate from mutable market interpretation state."""

    version: str = L3C_VERSION
    flow_window_events: int = 8
    structural_window_events: int = 8
    minimum_flow_events: int = 3
    replenishment_minimum_count: int = 2
    structural_lifetime: timedelta = timedelta(seconds=90)
    flow_lifetime: timedelta = timedelta(seconds=30)
    liquidity_lifetime: timedelta = timedelta(seconds=20)
    timing_lifetime: timedelta = timedelta(minutes=5)
    derivatives_lifetime: timedelta = timedelta(days=1)
    hypothesis_idle_lifetime: timedelta = timedelta(seconds=90)
    hypothesis_maximum_lifetime: timedelta = timedelta(minutes=10)
    trade_maximum_age: timedelta = timedelta(seconds=30)
    quote_maximum_age: timedelta = timedelta(seconds=10)
    book_maximum_age: timedelta = timedelta(seconds=15)
    dominance_threshold: Decimal = Decimal("0.65")
    dominance_margin: Decimal = Decimal("0.10")
    maximum_evidence_objects: int = 256
    maximum_history: int = 128
    confidence_families: tuple[EvidenceFamily, ...] = (
        EvidenceFamily.STRUCTURAL_CONTEXT,
        EvidenceFamily.ORDER_FLOW,
        EvidenceFamily.RESTING_LIQUIDITY,
        EvidenceFamily.DERIVATIVES_CONTEXT,
        EvidenceFamily.TIMING_SESSION_CONTEXT,
    )

    def __post_init__(self) -> None:
        if not isinstance(self.version, str) or not self.version:
            raise ValueError("Configuration version is required.")
        for field, value in (
            ("Flow window events", self.flow_window_events),
            ("Structural window events", self.structural_window_events),
            ("Minimum flow events", self.minimum_flow_events),
            ("Replenishment minimum count", self.replenishment_minimum_count),
            ("Maximum evidence objects", self.maximum_evidence_objects),
            ("Maximum history", self.maximum_history),
        ):
            if type(value) is not int or value <= 0:
                raise ValueError(f"{field} must be a positive integer.")
        if self.minimum_flow_events > self.flow_window_events:
            raise ValueError("Minimum flow events may not exceed the flow window.")
        for field, value in (
            ("Structural lifetime", self.structural_lifetime),
            ("Flow lifetime", self.flow_lifetime),
            ("Liquidity lifetime", self.liquidity_lifetime),
            ("Timing lifetime", self.timing_lifetime),
            ("Derivatives lifetime", self.derivatives_lifetime),
            ("Hypothesis idle lifetime", self.hypothesis_idle_lifetime),
            ("Hypothesis maximum lifetime", self.hypothesis_maximum_lifetime),
            ("Trade maximum age", self.trade_maximum_age),
            ("Quote maximum age", self.quote_maximum_age),
            ("Book maximum age", self.book_maximum_age),
        ):
            _positive_duration(value, field)
        if self.hypothesis_maximum_lifetime < self.hypothesis_idle_lifetime:
            raise ValueError("Hypothesis maximum lifetime must not be shorter than idle lifetime.")
        _decimal(self.dominance_threshold, "Dominance threshold", unit_interval=True)
        _decimal(self.dominance_margin, "Dominance margin", unit_interval=True)
        if not isinstance(self.confidence_families, tuple) or not self.confidence_families:
            raise ValueError("Confidence families must be a non-empty immutable tuple.")
        if any(type(item) is not EvidenceFamily for item in self.confidence_families):
            raise ValueError("Confidence families must be explicit evidence families.")
        if len(self.confidence_families) != len(set(self.confidence_families)):
            raise ValueError("Confidence families may not repeat a family.")

    def payload(self) -> dict[str, object]:
        duration_fields = (
            "structural_lifetime", "flow_lifetime", "liquidity_lifetime", "timing_lifetime",
            "derivatives_lifetime", "hypothesis_idle_lifetime", "hypothesis_maximum_lifetime",
            "trade_maximum_age", "quote_maximum_age", "book_maximum_age",
        )
        result: dict[str, object] = {
            "schema": L3C_SCHEMA,
            "version": self.version,
            "l3b_version": L3B_VERSION,
            "l3a_authority_manifest_hash": L3C_L3A_AUTHORITY_MANIFEST_HASH,
            "flow_window_events": self.flow_window_events,
            "structural_window_events": self.structural_window_events,
            "minimum_flow_events": self.minimum_flow_events,
            "replenishment_minimum_count": self.replenishment_minimum_count,
            "dominance_threshold": str(self.dominance_threshold),
            "dominance_margin": str(self.dominance_margin),
            "maximum_evidence_objects": self.maximum_evidence_objects,
            "maximum_history": self.maximum_history,
            "confidence_families": [item.value for item in self.confidence_families],
        }
        result.update({field: int(getattr(self, field).total_seconds()) for field in duration_fields})
        return result

    @property
    def configuration_hash(self) -> str:
        return canonical_hash(self.payload())


@dataclass(frozen=True)
class _TradeWindowItem:
    event_id: str
    payload_hash: str
    at: str
    price: Decimal
    size: int
    side: AggressorSide


class HypothesisEngine:
    """Single-threaded deterministic L3-B observation interpreter.

    The caller applies a canonical event through the frozen ``MarketDataPipeline``
    first, then supplies the event, result, and pipeline to :meth:`observe`.
    L3-C never constructs an alternate observation path.
    """

    def __init__(self, source: MarketDataSource, instrument: MNQContract, config: HypothesisEngineConfig | None = None) -> None:
        if type(source) is not MarketDataSource or type(instrument) is not MNQContract:
            raise ValueError("L3-C requires explicit L3-B source and concrete MNQ contract.")
        if instrument.strategy_instrument is not LaneIIIInstrument.MNQ:
            raise HypothesisEngineRefused("L3-C admits MNQ observations only.")
        self.source = source
        self.instrument = instrument
        self.config = config or HypothesisEngineConfig()
        if type(self.config) is not HypothesisEngineConfig:
            raise ValueError("L3-C requires an immutable HypothesisEngineConfig.")
        self._evidence: dict[str, EvidenceState] = {}
        self._hypotheses: dict[str, HypothesisRecord] = {}
        self._current_hypotheses: dict[HypothesisIdentity, str] = {}
        self._trade_window: deque[_TradeWindowItem] = deque(maxlen=max(self.config.flow_window_events, self.config.structural_window_events))
        self._replenishments: dict[tuple[BookSide, Decimal], deque[tuple[str, str, str]]] = {}
        self._rejected: deque[RejectedObservation] = deque(maxlen=self.config.maximum_history)
        self._updates: deque[ConfidenceUpdate] = deque(maxlen=self.config.maximum_history)
        self._last_evaluated_at: datetime | None = None
        self._observations_processed = 0
        self._evidence_created = 0
        self._evidence_expired = 0
        self._confidence_updates = 0
        self._invalid_source_observations = 0

    def observe(self, event: CanonicalMarketEvent, result: PipelineResult, pipeline: MarketDataPipeline) -> HypothesisEngineSnapshot:
        """Interpret one just-applied L3-B event through the same live/replay path."""
        if type(pipeline) is not MarketDataPipeline or pipeline.source != self.source or pipeline.instrument != self.instrument:
            raise HypothesisEngineRefused("L3-C may consume only its declared L3-B pipeline.")
        if event.header.source != self.source or event.header.instrument != self.instrument:
            raise HypothesisEngineRefused("L3-C event source or instrument does not match the engine scope.")
        if type(result) is not PipelineResult or result.event_id != event.header.event_id:
            raise HypothesisEngineRefused("L3-C requires the matching L3-B pipeline result.")
        at = _timestamp(event.header.timestamps.ordering_time, "L3-C observation time")
        self._require_monotonic(at)
        self._observations_processed += 1
        self._retire_unusable(at, pipeline)
        if isinstance(event, TradeEvent):
            self._observe_trade(event, result, pipeline, at)
        elif isinstance(event, (BookSnapshotEvent, BookDeltaEvent)):
            self._observe_book(event, result.book_application, at)
        elif isinstance(event, DerivativesContextEvent):
            self._observe_derivatives(event, at)
        self._rebuild_hypotheses(at)
        self._last_evaluated_at = at
        return self.snapshot(at)

    def advance(self, as_of: str, pipeline: MarketDataPipeline) -> HypothesisEngineSnapshot:
        """Apply expiry/staleness deterministically at an explicit replay time."""
        if type(pipeline) is not MarketDataPipeline or pipeline.source != self.source or pipeline.instrument != self.instrument:
            raise HypothesisEngineRefused("L3-C may advance only with its declared L3-B pipeline.")
        at = _timestamp(as_of, "L3-C advance time")
        self._require_monotonic(at)
        self._retire_unusable(at, pipeline)
        self._rebuild_hypotheses(at)
        self._last_evaluated_at = at
        return self.snapshot(at)

    def snapshot(self, as_of: datetime | None = None) -> HypothesisEngineSnapshot:
        at = as_of or self._last_evaluated_at
        if at is None:
            raise HypothesisEngineRefused("L3-C has no snapshot before its first observation.")
        hypotheses = tuple(sorted(self._hypotheses.values(), key=lambda item: (item.created_at, item.hypothesis_id)))
        active = tuple(item for item in hypotheses if item.state in {HypothesisState.FORMING, HypothesisState.ACTIVE, HypothesisState.CONFLICTED, HypothesisState.DECAYING})
        dominant = self._dominant(active)
        metrics = HypothesisEngineMetrics(
            observations_processed=self._observations_processed,
            evidence_objects_created=self._evidence_created,
            evidence_expired=self._evidence_expired,
            active_hypotheses=len(active),
            confidence_updates=self._confidence_updates,
            invalid_source_observations=self._invalid_source_observations,
            retained_evidence_objects=len(self._evidence),
            retained_hypothesis_records=len(self._hypotheses),
        )
        return HypothesisEngineSnapshot(
            evaluated_at=_timestamp_text(at),
            configuration_hash=self.config.configuration_hash,
            evidence=tuple(sorted(self._evidence.values(), key=lambda item: item.evidence.evidence_id)),
            hypotheses=hypotheses,
            dominant_hypothesis_id=dominant.hypothesis_id if dominant else None,
            rejected_observations=tuple(self._rejected),
            confidence_updates=tuple(self._updates),
            metrics=metrics,
        )

    def _require_monotonic(self, at: datetime) -> None:
        if self._last_evaluated_at is not None and at < self._last_evaluated_at:
            raise HypothesisEngineRefused("L3-C evaluation time must not move backward; late L3-B events are not reinterpreted.")

    def _qualities(self, at: datetime, pipeline: MarketDataPipeline) -> dict[str, DataQuality]:
        return pipeline.staleness(
            _timestamp_text(at),
            trade_maximum_age=self.config.trade_maximum_age,
            quote_maximum_age=self.config.quote_maximum_age,
            book_maximum_age=self.config.book_maximum_age,
        )

    @staticmethod
    def _reason_for_quality(quality: DataQuality) -> EvidenceDeactivationReason:
        return {
            DataQuality.STALE: EvidenceDeactivationReason.SOURCE_STALE,
            DataQuality.GAPPED: EvidenceDeactivationReason.SOURCE_GAPPED,
            DataQuality.RECOVERING: EvidenceDeactivationReason.SOURCE_RECOVERING,
            DataQuality.INCOMPLETE: EvidenceDeactivationReason.SOURCE_INCOMPLETE,
            DataQuality.INVALID: EvidenceDeactivationReason.SOURCE_INVALID,
        }.get(quality, EvidenceDeactivationReason.SOURCE_INVALID)

    def _retire_unusable(self, at: datetime, pipeline: MarketDataPipeline) -> None:
        qualities = self._qualities(at, pipeline)
        for evidence_id, state in tuple(self._evidence.items()):
            if state.usability is not EvidenceUsability.AUTHORITATIVE:
                continue
            evidence = state.evidence
            expiry = _timestamp(evidence.expires_at, "Evidence expiry time")
            reason: EvidenceDeactivationReason | None = None
            if at >= expiry:
                reason = EvidenceDeactivationReason.EXPIRED
            elif evidence.family in {EvidenceFamily.STRUCTURAL_CONTEXT, EvidenceFamily.ORDER_FLOW, EvidenceFamily.TIMING_SESSION_CONTEXT} and qualities["trade"] is not DataQuality.HEALTHY:
                reason = self._reason_for_quality(qualities["trade"])
            elif evidence.family is EvidenceFamily.RESTING_LIQUIDITY and qualities["book"] is not DataQuality.HEALTHY:
                reason = self._reason_for_quality(qualities["book"])
            if reason is not None:
                usability = EvidenceUsability.EXPIRED if reason is EvidenceDeactivationReason.EXPIRED else EvidenceUsability.UNUSABLE
                self._evidence[evidence_id] = replace(
                    state, usability=usability, deactivated_at=_timestamp_text(at), deactivation_reason=reason
                )
                self._evidence_expired += 1

    def _observe_trade(self, event: TradeEvent, result: PipelineResult, pipeline: MarketDataPipeline, at: datetime) -> None:
        if result.ordering is not OrderingOutcome.ACCEPTED:
            return
        qualities = self._qualities(at, pipeline)
        if qualities["trade"] is not DataQuality.HEALTHY:
            self._reject(event, EvidenceFamily.ORDER_FLOW, qualities["trade"])
            return
        item = _TradeWindowItem(
            event_id=event.header.event_id,
            payload_hash=canonical_hash(event.payload()),
            at=_timestamp_text(at),
            price=event.price,
            size=event.size,
            side=event.aggressor_side,
        )
        self._trade_window.append(item)
        if result.session_context is not None and result.session_context.complete:
            self._emit_vwap_evidence(event, result.session_context, at)
            self._emit_session_phase(event, result.session_context, at)
        if result.trade_flow is None or not result.trade_flow.complete:
            self._reject(event, EvidenceFamily.ORDER_FLOW, DataQuality.INCOMPLETE)
            return
        self._emit_structural_window(at)
        self._emit_flow_window(at)

    def _observe_book(self, event: BookSnapshotEvent | BookDeltaEvent, application: BookApplication | None, at: datetime) -> None:
        if application is None:
            raise HypothesisEngineRefused("A depth observation must include its L3-B book application.")
        if application.state.quality is not DataQuality.HEALTHY:
            self._reject(event, EvidenceFamily.RESTING_LIQUIDITY, application.state.quality)
            return
        if application.outcome is not BookApplyOutcome.DELTA_APPLIED:
            return
        for change in application.changes:
            self._observe_book_change(change, event, at)

    def _observe_book_change(self, change: BookChange, event: BookSnapshotEvent | BookDeltaEvent, at: datetime) -> None:
        if change.behavior is LiquidityBehavior.REPLENISH:
            key = (change.side, change.price)
            history = self._replenishments.setdefault(key, deque(maxlen=self.config.replenishment_minimum_count))
            history.append((change.source_event_id, canonical_hash(event.payload()), _timestamp_text(at)))
            if len(history) < self.config.replenishment_minimum_count:
                return
            ids = tuple(value[0] for value in history)
            hashes = tuple(value[1] for value in history)
            kind = EvidenceKind.BID_REPLENISHMENT if change.side is BookSide.BID else EvidenceKind.OFFER_REPLENISHMENT
            impacts = (
                EvidenceImpact(BULLISH_REVERSAL, EvidenceRelation.SUPPORTS, Decimal("0.50")),
                EvidenceImpact(BEARISH_CONTINUATION, EvidenceRelation.CONTRADICTS, Decimal("0.50")),
            ) if change.side is BookSide.BID else (
                EvidenceImpact(BEARISH_REVERSAL, EvidenceRelation.SUPPORTS, Decimal("0.50")),
                EvidenceImpact(BULLISH_CONTINUATION, EvidenceRelation.CONTRADICTS, Decimal("0.50")),
            )
            self._emit(
                EvidenceFamily.RESTING_LIQUIDITY, kind, EvidenceDerivation.HIGHER_ORDER_DERIVED, at,
                self.config.liquidity_lifetime, self._provenance_from_event_ids(ids, hashes, at, DataQuality.HEALTHY, f"book-replenishment:{change.side.value}:{change.price}"),
                impacts, f"book:{change.side.value}:{change.price}:replenishment", Decimal(len(history)),
            )
        elif change.behavior is LiquidityBehavior.PULL:
            kind = EvidenceKind.BID_LIQUIDITY_PULL if change.side is BookSide.BID else EvidenceKind.OFFER_LIQUIDITY_PULL
            impacts = (
                EvidenceImpact(BEARISH_CONTINUATION, EvidenceRelation.SUPPORTS, Decimal("0.50")),
                EvidenceImpact(BULLISH_CONTINUATION, EvidenceRelation.CONTRADICTS, Decimal("0.50"), invalidates=True),
            ) if change.side is BookSide.BID else (
                EvidenceImpact(BULLISH_CONTINUATION, EvidenceRelation.SUPPORTS, Decimal("0.50")),
                EvidenceImpact(BEARISH_CONTINUATION, EvidenceRelation.CONTRADICTS, Decimal("0.50"), invalidates=True),
            )
            self._emit(
                EvidenceFamily.RESTING_LIQUIDITY, kind, EvidenceDerivation.RAW_DERIVED, at,
                self.config.liquidity_lifetime, self._provenance_from_event_ids((event.header.event_id,), (canonical_hash(event.payload()),), at, DataQuality.HEALTHY, f"book-pull:{change.side.value}:{change.price}"),
                impacts, f"book:{change.side.value}:{change.price}:pull", Decimal("0.50"),
            )

    def _observe_derivatives(self, event: DerivativesContextEvent, at: datetime) -> None:
        vintage = _timestamp(event.data_vintage_time, "Derivatives vintage")
        fresh = at - vintage <= self.config.derivatives_lifetime
        source_quality = DataQuality.HEALTHY if fresh else DataQuality.STALE
        provenance = self._provenance_from_event_ids((event.header.event_id,), (canonical_hash(event.payload()),), at, source_quality, f"derivatives-vintage:{event.expiry}:{event.strike}")
        if not fresh:
            # The record is intentionally preserved as non-directional stale context;
            # it contributes to no hypothesis and cannot be mistaken for fresh input.
            self._emit(
                EvidenceFamily.DERIVATIVES_CONTEXT, EvidenceKind.DERIVATIVES_VINTAGE, EvidenceDerivation.RAW_DERIVED,
                at, self.config.derivatives_lifetime, provenance, (), f"derivatives:{event.expiry}:{event.strike}", None,
                initial_usability=EvidenceUsability.DEGRADED, deactivation_reason=EvidenceDeactivationReason.SOURCE_STALE,
            )
            return
        self._emit(
            EvidenceFamily.DERIVATIVES_CONTEXT, EvidenceKind.DERIVATIVES_VINTAGE, EvidenceDerivation.RAW_DERIVED,
            at, self.config.derivatives_lifetime, provenance, (), f"derivatives:{event.expiry}:{event.strike}", None,
        )

    def _emit_vwap_evidence(self, event: TradeEvent, session: SessionContext, at: datetime) -> None:
        if session.vwap is None:
            return
        relation = EvidenceKind.ABOVE_VWAP if event.price > session.vwap else EvidenceKind.BELOW_VWAP if event.price < session.vwap else None
        if relation is None:
            return
        if relation is EvidenceKind.ABOVE_VWAP:
            impacts = (
                EvidenceImpact(BULLISH_CONTINUATION, EvidenceRelation.SUPPORTS, Decimal("0.50")),
                EvidenceImpact(BEARISH_CONTINUATION, EvidenceRelation.CONTRADICTS, Decimal("0.50")),
            )
        else:
            impacts = (
                EvidenceImpact(BEARISH_CONTINUATION, EvidenceRelation.SUPPORTS, Decimal("0.50")),
                EvidenceImpact(BULLISH_CONTINUATION, EvidenceRelation.CONTRADICTS, Decimal("0.50")),
            )
        session_key = canonical_hash(
            {"session_id": session.session_id, "vwap": str(session.vwap), "high": str(session.session_high), "low": str(session.session_low), "volume": session.total_volume}
        )
        source = SourceProvenance(
            (event.header.event_id,), (canonical_hash(event.payload()),), session.session_start, _timestamp_text(at), DataQuality.HEALTHY,
            f"l3b-session:{session.session_id}:{session_key}",
        )
        self._emit(
            EvidenceFamily.STRUCTURAL_CONTEXT, relation, EvidenceDerivation.HIGHER_ORDER_DERIVED, at,
            self.config.structural_lifetime, source, impacts, f"session-vwap:{session.session_id}", event.price - session.vwap,
        )

    def _emit_session_phase(self, event: TradeEvent, session: SessionContext, at: datetime) -> None:
        source = SourceProvenance(
            (event.header.event_id,), (canonical_hash(event.payload()),), _timestamp_text(at), _timestamp_text(at), DataQuality.HEALTHY,
            f"l3b-session-phase:{session.session_id}",
        )
        self._emit(
            EvidenceFamily.TIMING_SESSION_CONTEXT, EvidenceKind.SESSION_PHASE, EvidenceDerivation.HIGHER_ORDER_DERIVED,
            at, self.config.timing_lifetime, source, (), f"session-phase:{session.session_id}", None,
        )

    def _emit_structural_window(self, at: datetime) -> None:
        values = tuple(self._trade_window)[-self.config.structural_window_events:]
        if len(values) < 2:
            return
        prior = values[:-1]
        current = values[-1]
        prior_high = max(item.price for item in prior)
        prior_low = min(item.price for item in prior)
        first = values[0].price
        source = self._provenance_from_trade_window(values, DataQuality.HEALTHY, "structural-window")
        if current.price > prior_high:
            self._emit(
                EvidenceFamily.STRUCTURAL_CONTEXT, EvidenceKind.RANGE_EXPANSION_UP, EvidenceDerivation.HIGHER_ORDER_DERIVED,
                at, self.config.structural_lifetime, source,
                (EvidenceImpact(BULLISH_CONTINUATION, EvidenceRelation.SUPPORTS, Decimal("0.50")), EvidenceImpact(BEARISH_CONTINUATION, EvidenceRelation.CONTRADICTS, Decimal("0.50"))),
                "structural:range-expansion", current.price - prior_high,
            )
        elif current.price < prior_low:
            self._emit(
                EvidenceFamily.STRUCTURAL_CONTEXT, EvidenceKind.RANGE_EXPANSION_DOWN, EvidenceDerivation.HIGHER_ORDER_DERIVED,
                at, self.config.structural_lifetime, source,
                (EvidenceImpact(BEARISH_CONTINUATION, EvidenceRelation.SUPPORTS, Decimal("0.50")), EvidenceImpact(BULLISH_CONTINUATION, EvidenceRelation.CONTRADICTS, Decimal("0.50"))),
                "structural:range-expansion", prior_low - current.price,
            )
        if prior_low < first and current.price >= first:
            self._emit(
                EvidenceFamily.STRUCTURAL_CONTEXT, EvidenceKind.RANGE_RECLAIM_UP, EvidenceDerivation.HIGHER_ORDER_DERIVED,
                at, self.config.structural_lifetime, source,
                (EvidenceImpact(BULLISH_REVERSAL, EvidenceRelation.SUPPORTS, Decimal("0.50")), EvidenceImpact(BEARISH_CONTINUATION, EvidenceRelation.CONTRADICTS, Decimal("0.50"))),
                "structural:range-reclaim", current.price - first,
            )
        elif prior_high > first and current.price <= first:
            self._emit(
                EvidenceFamily.STRUCTURAL_CONTEXT, EvidenceKind.RANGE_RECLAIM_DOWN, EvidenceDerivation.HIGHER_ORDER_DERIVED,
                at, self.config.structural_lifetime, source,
                (EvidenceImpact(BEARISH_REVERSAL, EvidenceRelation.SUPPORTS, Decimal("0.50")), EvidenceImpact(BULLISH_CONTINUATION, EvidenceRelation.CONTRADICTS, Decimal("0.50"))),
                "structural:range-reclaim", first - current.price,
            )

    def _emit_flow_window(self, at: datetime) -> None:
        values = tuple(self._trade_window)[-self.config.flow_window_events:]
        if len(values) < self.config.minimum_flow_events or any(item.side is AggressorSide.UNKNOWN for item in values):
            return
        buy = sum(item.size for item in values if item.side is AggressorSide.BUY)
        sell = sum(item.size for item in values if item.side is AggressorSide.SELL)
        total = buy + sell
        if total == 0 or buy == sell:
            return
        strength = Decimal(abs(buy - sell)) / Decimal(total)
        source = self._provenance_from_trade_window(values, DataQuality.HEALTHY, "flow-window")
        current, first = values[-1], values[0]
        if sell > buy:
            self._emit(
                EvidenceFamily.ORDER_FLOW, EvidenceKind.AGGRESSIVE_SELL_IMBALANCE, EvidenceDerivation.HIGHER_ORDER_DERIVED,
                at, self.config.flow_lifetime, source,
                (EvidenceImpact(BEARISH_CONTINUATION, EvidenceRelation.SUPPORTS, strength), EvidenceImpact(BULLISH_CONTINUATION, EvidenceRelation.CONTRADICTS, strength)),
                "flow:sell-aggression", strength,
            )
            if current.price >= first.price:
                self._emit(
                    EvidenceFamily.ORDER_FLOW, EvidenceKind.SELLING_WITHOUT_DOWNWARD_PROGRESS, EvidenceDerivation.HIGHER_ORDER_DERIVED,
                    at, self.config.flow_lifetime, source,
                    (EvidenceImpact(BULLISH_REVERSAL, EvidenceRelation.SUPPORTS, strength), EvidenceImpact(BEARISH_CONTINUATION, EvidenceRelation.CONTRADICTS, strength)),
                    "flow:sell-effort-result", strength,
                )
        else:
            self._emit(
                EvidenceFamily.ORDER_FLOW, EvidenceKind.AGGRESSIVE_BUY_IMBALANCE, EvidenceDerivation.HIGHER_ORDER_DERIVED,
                at, self.config.flow_lifetime, source,
                (EvidenceImpact(BULLISH_CONTINUATION, EvidenceRelation.SUPPORTS, strength), EvidenceImpact(BEARISH_CONTINUATION, EvidenceRelation.CONTRADICTS, strength)),
                "flow:buy-aggression", strength,
            )
            if current.price <= first.price:
                self._emit(
                    EvidenceFamily.ORDER_FLOW, EvidenceKind.BUYING_WITHOUT_UPWARD_PROGRESS, EvidenceDerivation.HIGHER_ORDER_DERIVED,
                    at, self.config.flow_lifetime, source,
                    (EvidenceImpact(BEARISH_REVERSAL, EvidenceRelation.SUPPORTS, strength), EvidenceImpact(BULLISH_CONTINUATION, EvidenceRelation.CONTRADICTS, strength)),
                    "flow:buy-effort-result", strength,
                )

    def _provenance_from_trade_window(self, values: tuple[_TradeWindowItem, ...], quality: DataQuality, observation_id: str) -> SourceProvenance:
        return SourceProvenance(
            tuple(item.event_id for item in values), tuple(item.payload_hash for item in values), values[0].at, values[-1].at,
            quality, f"l3b:{observation_id}:{values[0].event_id}:{values[-1].event_id}",
        )

    def _provenance_from_event_ids(self, event_ids: tuple[str, ...], payload_hashes: tuple[str, ...], at: datetime, quality: DataQuality, observation_id: str) -> SourceProvenance:
        # A depth history records the canonical IDs that mechanically established
        # the behavior; the exact events remain recoverable from L3-B capture.
        return SourceProvenance(
            event_ids, payload_hashes, _timestamp_text(at), _timestamp_text(at),
            quality, f"l3b:{observation_id}",
        )

    def _emit(
        self,
        family: EvidenceFamily,
        kind: EvidenceKind,
        derivation: EvidenceDerivation,
        at: datetime,
        lifetime: timedelta,
        source: SourceProvenance,
        impacts: tuple[EvidenceImpact, ...],
        correlation_key: str,
        measurement: Decimal | None,
        *,
        initial_usability: EvidenceUsability = EvidenceUsability.AUTHORITATIVE,
        deactivation_reason: EvidenceDeactivationReason | None = None,
    ) -> None:
        expires = at + lifetime
        payload = {
            "family": family.value,
            "kind": kind.value,
            "derivation": derivation.value,
            "created_at": _timestamp_text(at),
            "expires_at": _timestamp_text(expires),
            "source": source.payload(),
            "impacts": [value.payload() for value in impacts],
            "correlation_key": correlation_key,
            "measurement": None if measurement is None else str(measurement),
        }
        evidence = EvidenceObject("l3c-e-" + canonical_hash(payload)[:32], family, kind, derivation, _timestamp_text(at), _timestamp_text(expires), source, impacts, correlation_key, measurement)
        if evidence.evidence_id in self._evidence:
            return
        if initial_usability is EvidenceUsability.AUTHORITATIVE:
            state = EvidenceState(evidence, initial_usability)
        else:
            state = EvidenceState(evidence, initial_usability, _timestamp_text(at), deactivation_reason)
        self._evidence[evidence.evidence_id] = state
        self._evidence_created += 1
        self._bound_evidence(at)

    def _bound_evidence(self, at: datetime) -> None:
        while len(self._evidence) > self.config.maximum_evidence_objects:
            candidates = sorted(
                self._evidence.values(),
                key=lambda item: (item.usability is EvidenceUsability.AUTHORITATIVE, item.evidence.created_at, item.evidence.evidence_id),
            )
            state = candidates[0]
            if state.usability is EvidenceUsability.AUTHORITATIVE:
                self._evidence[state.evidence.evidence_id] = replace(
                    state, usability=EvidenceUsability.EXPIRED, deactivated_at=_timestamp_text(at),
                    deactivation_reason=EvidenceDeactivationReason.RETENTION_BOUND,
                )
                self._evidence_expired += 1
                continue
            del self._evidence[state.evidence.evidence_id]

    def _reject(self, event: CanonicalMarketEvent, family: EvidenceFamily, quality: DataQuality) -> None:
        if quality is DataQuality.HEALTHY:
            return
        self._invalid_source_observations += 1
        self._rejected.append(RejectedObservation(
            event.header.event_id, event.header.timestamps.ordering_time, family, quality, self._reason_for_quality(quality)
        ))

    def _rebuild_hypotheses(self, at: datetime) -> None:
        active = tuple(state.evidence for state in self._evidence.values() if state.usability is EvidenceUsability.AUTHORITATIVE)
        affected = {impact.hypothesis for evidence in active for impact in evidence.impacts}
        for identity, record_id in tuple(self._current_hypotheses.items()):
            record = self._hypotheses[record_id]
            if identity not in affected:
                expired_assessment = ConfidenceAssessment(_timestamp_text(at), Decimal("0.50"), ())
                self._hypotheses[record_id] = replace(
                    record, state=HypothesisState.EXPIRED, last_updated_at=_timestamp_text(at), confidence=expired_assessment,
                    supporting_evidence_ids=(), contradictory_evidence_ids=(),
                )
                self._record_update(record, expired_assessment, (), (), at)
                del self._current_hypotheses[identity]
        for identity in sorted(affected, key=lambda item: item.key):
            matching = tuple(evidence for evidence in active if any(impact.hypothesis == identity for impact in evidence.impacts))
            assessment = self._assess(identity, matching, at)
            supports = tuple(sorted(
                evidence.evidence_id for evidence in matching
                if any(impact.hypothesis == identity and impact.relation is EvidenceRelation.SUPPORTS for impact in evidence.impacts)
            ))
            contradictions = tuple(sorted(
                evidence.evidence_id for evidence in matching
                if any(impact.hypothesis == identity and impact.relation is EvidenceRelation.CONTRADICTS for impact in evidence.impacts)
            ))
            invalidators = tuple(sorted(
                evidence.evidence_id for evidence in matching
                if any(impact.hypothesis == identity and impact.invalidates for impact in evidence.impacts)
            ))
            old_id = self._current_hypotheses.get(identity)
            old = self._hypotheses.get(old_id) if old_id else None
            if invalidators:
                if old is None:
                    old = self._new_hypothesis(identity, at, assessment)
                self._hypotheses[old.hypothesis_id] = replace(
                    old, last_updated_at=_timestamp_text(at), state=HypothesisState.INVALIDATED, confidence=assessment,
                    supporting_evidence_ids=supports, contradictory_evidence_ids=contradictions, invalidated_by_evidence_ids=invalidators,
                )
                self._current_hypotheses.pop(identity, None)
                self._record_update(old, assessment, supports, contradictions, at)
                continue
            if old is None:
                old = self._new_hypothesis(identity, at, assessment)
                self._current_hypotheses[identity] = old.hypothesis_id
            maximum_expiry = _timestamp(old.created_at, "Hypothesis creation time") + self.config.hypothesis_maximum_lifetime
            if at >= maximum_expiry:
                self._hypotheses[old.hypothesis_id] = replace(old, last_updated_at=_timestamp_text(at), state=HypothesisState.EXPIRED)
                self._current_hypotheses.pop(identity, None)
                self._record_update(old, assessment, supports, contradictions, at)
                continue
            state = HypothesisState.CONFLICTED if supports and contradictions else HypothesisState.ACTIVE if supports else HypothesisState.DECAYING
            updated = replace(
                old, last_updated_at=_timestamp_text(at), expires_at=_timestamp_text(min(at + self.config.hypothesis_idle_lifetime, maximum_expiry)),
                state=state, confidence=assessment, supporting_evidence_ids=supports, contradictory_evidence_ids=contradictions,
                invalidated_by_evidence_ids=(),
            )
            self._hypotheses[updated.hypothesis_id] = updated
            self._record_update(old, assessment, supports, contradictions, at)
        self._bound_hypotheses()

    def _new_hypothesis(self, identity: HypothesisIdentity, at: datetime, assessment: ConfidenceAssessment) -> HypothesisRecord:
        created = _timestamp_text(at)
        identifier = "l3c-h-" + identity.key + "-" + canonical_hash({"identity": identity.payload(), "created_at": created, "config": self.config.configuration_hash})[:20]
        record = HypothesisRecord(
            identifier, identity, created, created, _timestamp_text(at + self.config.hypothesis_idle_lifetime), HypothesisState.FORMING,
            assessment, (), (), (), self.config.configuration_hash,
        )
        self._hypotheses[identifier] = record
        return record

    def _assess(self, identity: HypothesisIdentity, evidence: tuple[EvidenceObject, ...], at: datetime) -> ConfidenceAssessment:
        contributions: list[FamilyContribution] = []
        for family in self.config.confidence_families:
            support: list[tuple[Decimal, str]] = []
            contradiction: list[tuple[Decimal, str]] = []
            for item in evidence:
                if item.family is not family:
                    continue
                for impact in item.impacts:
                    if impact.hypothesis != identity:
                        continue
                    target = support if impact.relation is EvidenceRelation.SUPPORTS else contradiction
                    target.append((impact.strength, item.evidence_id))
            if not support and not contradiction:
                continue
            # Max-within-family is the deliberate dependence control.  An
            # additional order-flow label can refine diagnostics but cannot
            # increase this family beyond its strongest support/contradiction.
            strongest_support = max((value for value, _ in support), default=Decimal(0))
            strongest_contradiction = max((value for value, _ in contradiction), default=Decimal(0))
            contributions.append(FamilyContribution(
                family,
                tuple(sorted(identifier for value, identifier in support if value == strongest_support)),
                tuple(sorted(identifier for value, identifier in contradiction if value == strongest_contradiction)),
                strongest_support, strongest_contradiction, strongest_support - strongest_contradiction,
            ))
        total_balance = sum((item.balance for item in contributions), Decimal(0))
        denominator = Decimal(2 * len(self.config.confidence_families))
        score = max(Decimal(0), min(Decimal(1), Decimal("0.5") + total_balance / denominator))
        return ConfidenceAssessment(_timestamp_text(at), score, tuple(contributions))

    def _record_update(self, old: HypothesisRecord, assessment: ConfidenceAssessment, support: tuple[str, ...], contradiction: tuple[str, ...], at: datetime) -> None:
        old_ids = set(old.supporting_evidence_ids) | set(old.contradictory_evidence_ids)
        new_ids = set(support) | set(contradiction)
        if old.confidence.relative_support == assessment.relative_support and old_ids == new_ids:
            return
        self._updates.append(ConfidenceUpdate(
            old.hypothesis_id, _timestamp_text(at), old.confidence.relative_support, assessment.relative_support,
            tuple(sorted(new_ids - old_ids)), tuple(sorted(old_ids - new_ids)), assessment.family_contributions,
        ))
        self._confidence_updates += 1

    def _bound_hypotheses(self) -> None:
        while len(self._hypotheses) > self.config.maximum_history:
            candidates = sorted(
                (item for item in self._hypotheses.values() if item.hypothesis_id not in self._current_hypotheses.values()),
                key=lambda item: (item.last_updated_at, item.hypothesis_id),
            )
            if not candidates:
                return
            del self._hypotheses[candidates[0].hypothesis_id]

    def _dominant(self, active: tuple[HypothesisRecord, ...]) -> HypothesisRecord | None:
        candidates = [item for item in active if item.state is HypothesisState.ACTIVE]
        if not candidates:
            return None
        ranked = sorted(candidates, key=lambda item: (-item.confidence.relative_support, item.hypothesis_id))
        first = ranked[0]
        if first.confidence.relative_support < self.config.dominance_threshold:
            return None
        if len(ranked) > 1 and first.confidence.relative_support - ranked[1].confidence.relative_support < self.config.dominance_margin:
            return None
        return first


@dataclass(frozen=True)
class HypothesisReplayReport:
    events_processed: int
    final_snapshot_hash: str
    snapshots: tuple[HypothesisEngineSnapshot, ...]


class DeterministicHypothesisReplay:
    """Replay facade that deliberately invokes the normal L3-B then L3-C path."""

    def __init__(self, pipeline: MarketDataPipeline, engine: HypothesisEngine) -> None:
        if pipeline.source != engine.source or pipeline.instrument != engine.instrument:
            raise HypothesisEngineRefused("Replay pipeline and hypothesis engine must share source and instrument.")
        self.pipeline = pipeline
        self.engine = engine

    def replay(self, events: Iterable[CanonicalMarketEvent]) -> HypothesisReplayReport:
        snapshots: list[HypothesisEngineSnapshot] = []
        for event in events:
            result = self.pipeline.apply(event)
            snapshots.append(self.engine.observe(event, result, self.pipeline))
        final = snapshots[-1].snapshot_hash if snapshots else canonical_hash({"empty": True, "config": self.engine.config.configuration_hash})
        return HypothesisReplayReport(len(snapshots), final, tuple(snapshots))
