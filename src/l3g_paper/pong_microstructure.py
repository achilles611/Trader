"""Pure causal microstructure adapters for execution-disabled Pong research.

The types in this module describe evidence, never execution authority.  They
accept already authenticated/canonical observations, preserve their causal
timestamps and hashes, and do not open files, sockets, accounts, or transports.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from enum import StrEnum
import re
from typing import Iterable

from src.lane_iii.contracts import canonical_hash, normalized_utc
from src.l3g_paper.discernment import (
    DISCERNMENT_INSTRUMENT,
    DiscernmentEvidence,
    DiscernmentPolicy,
    VolumeRegime as DiscernmentVolumeRegime,
)


PONG_MICROSTRUCTURE_SCHEMA = "beelzebub-pong-microstructure-research-v1"


class AggressorSide(StrEnum):
    BUY = "BUY"
    SELL = "SELL"
    UNKNOWN = "UNKNOWN"


class AggressorProvenance(StrEnum):
    AUTHENTICATED_PROVIDER = "AUTHENTICATED_PROVIDER"
    AUTHENTICATED_DERIVED = "AUTHENTICATED_DERIVED"
    UNAVAILABLE = "UNAVAILABLE"


class FlowBehavior(StrEnum):
    BUY_PROGRESS = "BUY_PROGRESS"
    BUY_ABSORBED = "BUY_ABSORBED"
    SELL_PROGRESS = "SELL_PROGRESS"
    SELL_ABSORBED = "SELL_ABSORBED"
    BALANCED = "BALANCED"
    UNAVAILABLE = "UNAVAILABLE"


class BookSide(StrEnum):
    BID = "BID"
    ASK = "ASK"


class DepthEventKind(StrEnum):
    SNAPSHOT = "SNAPSHOT"
    UPDATE = "UPDATE"
    OUT_OF_RANGE = "OUT_OF_RANGE"
    RECONNECT_RESET = "RECONNECT_RESET"


class PriceResponse(StrEnum):
    ACCEPTANCE = "ACCEPTANCE"
    REJECTION = "REJECTION"
    UNAVAILABLE = "UNAVAILABLE"


class LiquidityChangeKind(StrEnum):
    REDUCE = "REDUCE"
    REPLENISH = "REPLENISH"
    UNCHANGED = "UNCHANGED"


class ReductionAttribution(StrEnum):
    NOT_APPLICABLE = "NOT_APPLICABLE"
    TRADE_MATCHED_CONSUMPTION = "TRADE_MATCHED_CONSUMPTION"
    SUPPORTED_CANCELLATION = "SUPPORTED_CANCELLATION"
    AMBIGUOUS = "AMBIGUOUS"


class VolumeRegime(StrEnum):
    LOW = "LOW"
    NORMAL = "NORMAL"
    HIGH = "HIGH"
    UNAVAILABLE = "UNAVAILABLE"


class VolumeWindowKind(StrEnum):
    COMPLETED = "COMPLETED"
    AS_OF = "AS_OF"


def _text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} is required.")
    return value


def _sha256(value: object, field: str) -> str:
    if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise ValueError(f"{field} must be a lowercase SHA-256 digest.")
    return value


def _decimal(value: object, field: str, *, minimum: Decimal | None = None) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be a finite decimal.") from exc
    if not result.is_finite() or (minimum is not None and result < minimum):
        raise ValueError(f"{field} must be a finite decimal >= {minimum}.")
    return result


def _dtext(value: Decimal) -> str:
    normalized = value.normalize()
    return "0" if normalized == 0 else format(normalized, "f")


def _time(value: str, field: str) -> datetime:
    return datetime.fromisoformat(normalized_utc(value, field).replace("Z", "+00:00"))


@dataclass(frozen=True)
class CausalEventRef:
    """Stable identity and causal availability envelope for a canonical event."""

    event_id: str
    payload_hash: str
    event_at: str
    received_at: str
    available_at: str
    sequence: int
    sequence_authority: str
    continuity_domain: str

    def __post_init__(self) -> None:
        _text(self.event_id, "Event ID")
        _sha256(self.payload_hash, "Event payload hash")
        event = _time(self.event_at, "Event time")
        received = _time(self.received_at, "Event receipt time")
        available = _time(self.available_at, "Event available-at time")
        if event > received or received > available:
            raise ValueError("Event provenance is non-causal.")
        if type(self.sequence) is not int or self.sequence < 0:
            raise ValueError("Event sequence must be a non-negative integer.")
        _text(self.sequence_authority, "Sequence authority")
        _text(self.continuity_domain, "Continuity domain")

    def payload(self) -> dict[str, object]:
        return {
            "event_id": self.event_id,
            "payload_hash": self.payload_hash,
            "event_at": normalized_utc(self.event_at, "Event time"),
            "received_at": normalized_utc(self.received_at, "Event receipt time"),
            "available_at": normalized_utc(self.available_at, "Event available-at time"),
            "sequence": self.sequence,
            "sequence_authority": self.sequence_authority,
            "continuity_domain": self.continuity_domain,
        }


def _ordered_refs(refs: Iterable[CausalEventRef], as_of: datetime) -> tuple[CausalEventRef, ...]:
    ordered = tuple(sorted(refs, key=lambda item: (
        _time(item.available_at, "Event available-at time"),
        _time(item.event_at, "Event time"),
        item.sequence,
        item.event_id,
    )))
    if len({item.event_id for item in ordered}) != len(ordered):
        raise ValueError("Duplicate event IDs are forbidden.")
    sequence_keys = {(item.continuity_domain, item.sequence) for item in ordered}
    if len(sequence_keys) != len(ordered):
        raise ValueError("Duplicate sequence coordinates are forbidden.")
    if any(_time(item.available_at, "Event available-at time") > as_of for item in ordered):
        raise ValueError("Future-available evidence cannot enter an earlier result.")
    return ordered


@dataclass(frozen=True)
class ClassifiedTrade:
    event: CausalEventRef
    instrument: str
    price: Decimal
    volume: Decimal
    aggressor_side: AggressorSide
    classification_provenance: AggressorProvenance

    def __post_init__(self) -> None:
        _text(self.instrument, "Trade instrument")
        _decimal(self.price, "Trade price", minimum=Decimal("0.000000001"))
        _decimal(self.volume, "Trade volume", minimum=Decimal("0.000000001"))
        if type(self.aggressor_side) is not AggressorSide:
            raise ValueError("Aggressor side is required.")
        if type(self.classification_provenance) is not AggressorProvenance:
            raise ValueError("Aggressor classification provenance is required.")
        if (
            self.aggressor_side is AggressorSide.UNKNOWN
            and self.classification_provenance is not AggressorProvenance.UNAVAILABLE
        ):
            raise ValueError("Unknown aggressor classification must retain UNAVAILABLE provenance.")
        if (
            self.aggressor_side is not AggressorSide.UNKNOWN
            and self.classification_provenance is AggressorProvenance.UNAVAILABLE
        ):
            raise ValueError("Classified aggressor side requires authenticated provenance.")


@dataclass(frozen=True)
class ClassifiedFlowPolicy:
    policy_id: str = "pong-symmetric-classified-flow-research"
    version: str = "1"
    minimum_classified_count: int = 4
    minimum_classified_volume: Decimal = Decimal("4")
    minimum_classified_coverage: Decimal = Decimal("0.70")
    dominance_threshold: Decimal = Decimal("0.20")
    tick_size: Decimal = Decimal("0.25")
    minimum_progress_ticks: Decimal = Decimal("1")
    maximum_staleness_seconds: int = 5

    def __post_init__(self) -> None:
        _text(self.policy_id, "Flow policy ID")
        _text(self.version, "Flow policy version")
        if type(self.minimum_classified_count) is not int or self.minimum_classified_count < 1:
            raise ValueError("Minimum classified count must be positive.")
        _decimal(self.minimum_classified_volume, "Minimum classified volume", minimum=Decimal("0.000000001"))
        coverage = _decimal(self.minimum_classified_coverage, "Minimum classified coverage")
        dominance = _decimal(self.dominance_threshold, "Dominance threshold")
        if coverage <= 0 or coverage > 1 or dominance <= 0 or dominance >= 1:
            raise ValueError("Flow coverage and dominance thresholds are outside range.")
        _decimal(self.tick_size, "Tick size", minimum=Decimal("0.000000001"))
        _decimal(self.minimum_progress_ticks, "Minimum progress ticks", minimum=Decimal("0"))
        if type(self.maximum_staleness_seconds) is not int or self.maximum_staleness_seconds <= 0:
            raise ValueError("Maximum flow staleness must be positive.")

    def payload(self) -> dict[str, object]:
        return {
            "policy_id": self.policy_id,
            "version": self.version,
            "minimum_classified_count": self.minimum_classified_count,
            "minimum_classified_volume": _dtext(self.minimum_classified_volume),
            "minimum_classified_coverage": _dtext(self.minimum_classified_coverage),
            "dominance_threshold": _dtext(self.dominance_threshold),
            "tick_size": _dtext(self.tick_size),
            "minimum_progress_ticks": _dtext(self.minimum_progress_ticks),
            "maximum_staleness_seconds": self.maximum_staleness_seconds,
            "production_configured": False,
            "execution_authority": "DENIED",
        }

    @property
    def configuration_hash(self) -> str:
        return canonical_hash(self.payload())


@dataclass(frozen=True)
class ClassifiedFlowResult:
    window_start: str
    as_of: str
    instrument: str
    behavior: FlowBehavior
    valid: bool
    reason_codes: tuple[str, ...]
    sample_count: int
    buy_count: int
    sell_count: int
    unknown_count: int
    classification_provenance_counts: tuple[tuple[str, int], ...]
    buy_volume: Decimal
    sell_volume: Decimal
    unknown_volume: Decimal
    total_volume: Decimal
    classified_coverage: Decimal
    imbalance: Decimal | None
    arrival_rate_per_second: Decimal
    price_change: Decimal | None
    source_event_at: str
    received_at: str
    available_at: str
    continuity_domain: str
    source_ids: tuple[str, ...]
    source_hashes: tuple[str, ...]
    dependency_group_id: str
    policy_hash: str

    def payload(self) -> dict[str, object]:
        return {
            "schema": PONG_MICROSTRUCTURE_SCHEMA,
            "kind": "CLASSIFIED_FLOW",
            "window_start": normalized_utc(self.window_start, "Flow window start"),
            "as_of": normalized_utc(self.as_of, "Flow as-of time"),
            "instrument": self.instrument,
            "behavior": self.behavior.value,
            "valid": self.valid,
            "reason_codes": list(self.reason_codes),
            "sample_count": self.sample_count,
            "counts": {"buy": self.buy_count, "sell": self.sell_count, "unknown": self.unknown_count},
            "classification_provenance_counts": dict(self.classification_provenance_counts),
            "volumes": {
                "buy": _dtext(self.buy_volume), "sell": _dtext(self.sell_volume),
                "unknown": _dtext(self.unknown_volume), "total": _dtext(self.total_volume),
            },
            "classified_coverage": _dtext(self.classified_coverage),
            "imbalance": None if self.imbalance is None else _dtext(self.imbalance),
            "arrival_rate_per_second": _dtext(self.arrival_rate_per_second),
            "price_change": None if self.price_change is None else _dtext(self.price_change),
            "source_event_at": normalized_utc(self.source_event_at, "Flow source event time"),
            "received_at": normalized_utc(self.received_at, "Flow receipt time"),
            "available_at": normalized_utc(self.available_at, "Flow availability time"),
            "continuity_domain": self.continuity_domain,
            "source_ids": list(self.source_ids),
            "source_hashes": list(self.source_hashes),
            "dependency_group_id": self.dependency_group_id,
            "policy_hash": self.policy_hash,
            "execution_authority": "DENIED",
        }

    @property
    def result_hash(self) -> str:
        return canonical_hash(self.payload())


class ClassifiedFlowAdapter:
    def __init__(self, policy: ClassifiedFlowPolicy = ClassifiedFlowPolicy()) -> None:
        self.policy = policy

    def evaluate(
        self,
        trades: Iterable[ClassifiedTrade],
        *,
        window_start: str,
        as_of: str,
        feed_continuity_valid: bool,
    ) -> ClassifiedFlowResult:
        if type(feed_continuity_valid) is not bool:
            raise ValueError("Flow continuity state must be boolean.")
        start = _time(window_start, "Flow window start")
        end = _time(as_of, "Flow as-of time")
        if start >= end:
            raise ValueError("Flow window must have positive duration.")
        values = tuple(trades)
        refs = _ordered_refs((item.event for item in values), end)
        by_id = {item.event.event_id: item for item in values}
        ordered = tuple(by_id[item.event_id] for item in refs)
        if any(_time(item.event.event_at, "Trade event time") < start for item in ordered):
            raise ValueError("Trade precedes the declared flow window.")
        instruments = {item.instrument for item in ordered}
        if len(instruments) > 1:
            raise ValueError("A flow result cannot mix instruments.")
        if len({item.event.continuity_domain for item in ordered}) > 1:
            raise ValueError("A flow result cannot bridge continuity domains.")
        buy = tuple(item for item in ordered if item.aggressor_side is AggressorSide.BUY)
        sell = tuple(item for item in ordered if item.aggressor_side is AggressorSide.SELL)
        unknown = tuple(item for item in ordered if item.aggressor_side is AggressorSide.UNKNOWN)
        buy_volume = sum((item.volume for item in buy), Decimal("0"))
        sell_volume = sum((item.volume for item in sell), Decimal("0"))
        unknown_volume = sum((item.volume for item in unknown), Decimal("0"))
        classified = buy_volume + sell_volume
        total = classified + unknown_volume
        coverage = classified / total if total else Decimal("0")
        imbalance = (buy_volume - sell_volume) / classified if classified else None
        duration = Decimal(str((end - start).total_seconds()))
        rate = Decimal(len(ordered)) / duration
        price_change = ordered[-1].price - ordered[0].price if len(ordered) >= 2 else None
        reasons: list[str] = []
        if not feed_continuity_valid:
            reasons.append("CLASSIFIED_FLOW_BROKEN_FEED")
        if not ordered:
            reasons.append("CLASSIFIED_FLOW_EMPTY")
        elif end - _time(ordered[-1].event.available_at, "Trade available-at time") > timedelta(
            seconds=self.policy.maximum_staleness_seconds
        ):
            reasons.append("CLASSIFIED_FLOW_STALE")
        if len(buy) + len(sell) < self.policy.minimum_classified_count:
            reasons.append("CLASSIFIED_FLOW_SPARSE_COUNT")
        if classified < self.policy.minimum_classified_volume:
            reasons.append("CLASSIFIED_FLOW_SPARSE_VOLUME")
        if coverage < self.policy.minimum_classified_coverage:
            reasons.append("CLASSIFIED_FLOW_LOW_CLASSIFIED_COVERAGE")
        valid = not reasons
        behavior = FlowBehavior.UNAVAILABLE
        if valid and imbalance is not None:
            progress = self.policy.tick_size * self.policy.minimum_progress_ticks
            if imbalance >= self.policy.dominance_threshold:
                behavior = (
                    FlowBehavior.BUY_PROGRESS
                    if price_change is not None and price_change >= progress
                    else FlowBehavior.BUY_ABSORBED
                )
            elif imbalance <= -self.policy.dominance_threshold:
                behavior = (
                    FlowBehavior.SELL_PROGRESS
                    if price_change is not None and price_change <= -progress
                    else FlowBehavior.SELL_ABSORBED
                )
            else:
                behavior = FlowBehavior.BALANCED
        source_ids = tuple(item.event.event_id for item in ordered)
        source_hashes = tuple(item.event.payload_hash for item in ordered)
        provenance_counts = tuple(
            (provenance.value, sum(item.classification_provenance is provenance for item in ordered))
            for provenance in AggressorProvenance
        )
        dependency = canonical_hash({
            "kind": "CLASSIFIED_FLOW_WINDOW_INPUTS",
            "window_start": normalized_utc(window_start, "Flow window start"),
            "as_of": normalized_utc(as_of, "Flow as-of time"),
            "source_ids": source_ids,
            "source_hashes": source_hashes,
        })
        source_event_at = ordered[-1].event.event_at if ordered else as_of
        received_at = ordered[-1].event.received_at if ordered else as_of
        continuity_domain = ordered[-1].event.continuity_domain if ordered else "UNESTABLISHED"
        return ClassifiedFlowResult(
            window_start=window_start,
            as_of=as_of,
            instrument=next(iter(instruments), "UNAVAILABLE"),
            behavior=behavior,
            valid=valid,
            reason_codes=tuple(reasons),
            sample_count=len(ordered),
            buy_count=len(buy),
            sell_count=len(sell),
            unknown_count=len(unknown),
            classification_provenance_counts=provenance_counts,
            buy_volume=buy_volume,
            sell_volume=sell_volume,
            unknown_volume=unknown_volume,
            total_volume=total,
            classified_coverage=coverage,
            imbalance=imbalance,
            arrival_rate_per_second=rate,
            price_change=price_change,
            source_event_at=source_event_at,
            received_at=received_at,
            available_at=as_of,
            continuity_domain=continuity_domain,
            source_ids=source_ids,
            source_hashes=source_hashes,
            dependency_group_id="flow-dependency-" + dependency[:32],
            policy_hash=self.policy.configuration_hash,
        )


@dataclass(frozen=True)
class DepthObservation:
    event: CausalEventRef
    instrument: str
    side: BookSide
    price: Decimal
    displayed_quantity: Decimal
    nearby_same_side_quantity: Decimal
    touch_price: Decimal
    event_kind: DepthEventKind
    matched_trade_volume: Decimal = Decimal("0")
    interaction_volume: Decimal = Decimal("0")
    cancellation_evidence_supported: bool = False
    observed_price_response: PriceResponse = PriceResponse.UNAVAILABLE

    def __post_init__(self) -> None:
        _text(self.instrument, "Depth instrument")
        if type(self.side) is not BookSide or type(self.event_kind) is not DepthEventKind:
            raise ValueError("Depth side and event kind are required.")
        _decimal(self.price, "Depth price", minimum=Decimal("0.000000001"))
        _decimal(self.displayed_quantity, "Displayed quantity", minimum=Decimal("0"))
        _decimal(self.nearby_same_side_quantity, "Nearby depth", minimum=Decimal("0"))
        _decimal(self.touch_price, "Touch price", minimum=Decimal("0.000000001"))
        _decimal(self.matched_trade_volume, "Matched trade volume", minimum=Decimal("0"))
        _decimal(self.interaction_volume, "Interaction volume", minimum=Decimal("0"))
        if type(self.cancellation_evidence_supported) is not bool:
            raise ValueError("Cancellation evidence state must be boolean.")
        if type(self.observed_price_response) is not PriceResponse:
            raise ValueError("Observed price response is required.")
        if self.side is BookSide.BID and self.price > self.touch_price:
            raise ValueError("Bid cluster cannot be above its bid touch.")
        if self.side is BookSide.ASK and self.price < self.touch_price:
            raise ValueError("Ask cluster cannot be below its ask touch.")


@dataclass(frozen=True)
class LiquidityClusterPolicy:
    policy_id: str = "pong-symmetric-liquidity-cluster-research"
    version: str = "1"
    tick_size: Decimal = Decimal("0.25")
    minimum_relative_size: Decimal = Decimal("0.25")
    maximum_staleness_seconds: int = 5

    def __post_init__(self) -> None:
        _text(self.policy_id, "Liquidity policy ID")
        _text(self.version, "Liquidity policy version")
        _decimal(self.tick_size, "Liquidity tick size", minimum=Decimal("0.000000001"))
        threshold = _decimal(self.minimum_relative_size, "Minimum relative size")
        if threshold <= 0:
            raise ValueError("Minimum relative size must be positive.")
        if type(self.maximum_staleness_seconds) is not int or self.maximum_staleness_seconds <= 0:
            raise ValueError("Maximum liquidity staleness must be positive.")

    def payload(self) -> dict[str, object]:
        return {
            "policy_id": self.policy_id, "version": self.version,
            "tick_size": _dtext(self.tick_size),
            "minimum_relative_size": _dtext(self.minimum_relative_size),
            "maximum_staleness_seconds": self.maximum_staleness_seconds,
            "book_fidelity": "AGGREGATE_MARKET_BY_PRICE_NOT_INDIVIDUAL_ORDER",
            "production_configured": False, "execution_authority": "DENIED",
        }

    @property
    def configuration_hash(self) -> str:
        return canonical_hash(self.payload())


@dataclass(frozen=True)
class LiquidityTransition:
    prior_event_id: str
    event_id: str
    kind: LiquidityChangeKind
    quantity: Decimal
    reduction_attribution: ReductionAttribution

    def payload(self) -> dict[str, object]:
        return {
            "prior_event_id": self.prior_event_id, "event_id": self.event_id,
            "kind": self.kind.value, "quantity": _dtext(self.quantity),
            "reduction_attribution": self.reduction_attribution.value,
        }


@dataclass(frozen=True)
class LiquidityClusterResult:
    cluster_id: str
    instrument: str
    side: BookSide
    price: Decimal
    as_of: str
    valid: bool
    reason_codes: tuple[str, ...]
    current_displayed_quantity: Decimal
    nearby_same_side_quantity: Decimal
    relative_size: Decimal | None
    distance_from_touch_ticks: Decimal
    persistence_seconds: Decimal
    interaction_count: int
    interaction_volume: Decimal
    acceptance_count: int
    rejection_count: int
    depleted_quantity: Decimal
    replenished_quantity: Decimal
    transitions: tuple[LiquidityTransition, ...]
    source_event_at: str
    received_at: str
    available_at: str
    continuity_domain: str
    source_ids: tuple[str, ...]
    source_hashes: tuple[str, ...]
    dependency_group_id: str
    policy_hash: str

    def payload(self) -> dict[str, object]:
        return {
            "schema": PONG_MICROSTRUCTURE_SCHEMA, "kind": "LIQUIDITY_CLUSTER",
            "cluster_id": self.cluster_id, "instrument": self.instrument,
            "side": self.side.value, "price": _dtext(self.price),
            "as_of": normalized_utc(self.as_of, "Liquidity as-of time"),
            "valid": self.valid, "reason_codes": list(self.reason_codes),
            "current_displayed_quantity": _dtext(self.current_displayed_quantity),
            "nearby_same_side_quantity": _dtext(self.nearby_same_side_quantity),
            "relative_size": None if self.relative_size is None else _dtext(self.relative_size),
            "distance_from_touch_ticks": _dtext(self.distance_from_touch_ticks),
            "persistence_seconds": _dtext(self.persistence_seconds),
            "interaction_count": self.interaction_count,
            "interaction_volume": _dtext(self.interaction_volume),
            "acceptance_count": self.acceptance_count, "rejection_count": self.rejection_count,
            "depleted_quantity": _dtext(self.depleted_quantity),
            "replenished_quantity": _dtext(self.replenished_quantity),
            "transitions": [item.payload() for item in self.transitions],
            "source_event_at": normalized_utc(self.source_event_at, "Liquidity source event time"),
            "received_at": normalized_utc(self.received_at, "Liquidity receipt time"),
            "available_at": normalized_utc(self.available_at, "Liquidity availability time"),
            "continuity_domain": self.continuity_domain,
            "source_ids": list(self.source_ids), "source_hashes": list(self.source_hashes),
            "dependency_group_id": self.dependency_group_id, "policy_hash": self.policy_hash,
            "book_fidelity": "AGGREGATE_MARKET_BY_PRICE_NOT_INDIVIDUAL_ORDER",
            "execution_authority": "DENIED",
        }

    @property
    def result_hash(self) -> str:
        return canonical_hash(self.payload())


class LiquidityClusterAdapter:
    def __init__(self, policy: LiquidityClusterPolicy = LiquidityClusterPolicy()) -> None:
        self.policy = policy

    def evaluate(
        self,
        observations: Iterable[DepthObservation],
        *,
        as_of: str,
        feed_continuity_valid: bool,
    ) -> LiquidityClusterResult:
        if type(feed_continuity_valid) is not bool:
            raise ValueError("Liquidity continuity state must be boolean.")
        end = _time(as_of, "Liquidity as-of time")
        values = tuple(observations)
        if not values:
            raise ValueError("A liquidity cluster requires at least one observation.")
        refs = _ordered_refs((item.event for item in values), end)
        by_id = {item.event.event_id: item for item in values}
        ordered = tuple(by_id[item.event_id] for item in refs)
        identities = {(item.instrument, item.side, item.price) for item in ordered}
        if len(identities) != 1:
            raise ValueError("A liquidity cluster cannot mix instruments, sides, or prices.")
        if len({item.event.continuity_domain for item in ordered}) != 1:
            raise ValueError("A liquidity cluster cannot bridge continuity domains.")
        instrument, side, price = next(iter(identities))
        transitions: list[LiquidityTransition] = []
        depleted = Decimal("0")
        replenished = Decimal("0")
        for prior, current in zip(ordered, ordered[1:]):
            change = current.displayed_quantity - prior.displayed_quantity
            if change < 0:
                quantity = -change
                depleted += quantity
                if current.event_kind in {DepthEventKind.OUT_OF_RANGE, DepthEventKind.RECONNECT_RESET}:
                    attribution = ReductionAttribution.AMBIGUOUS
                elif current.matched_trade_volume >= quantity:
                    attribution = ReductionAttribution.TRADE_MATCHED_CONSUMPTION
                elif current.cancellation_evidence_supported:
                    attribution = ReductionAttribution.SUPPORTED_CANCELLATION
                else:
                    attribution = ReductionAttribution.AMBIGUOUS
                kind = LiquidityChangeKind.REDUCE
            elif change > 0:
                quantity = change
                replenished += quantity
                attribution = ReductionAttribution.NOT_APPLICABLE
                kind = LiquidityChangeKind.REPLENISH
            else:
                quantity = Decimal("0")
                attribution = ReductionAttribution.NOT_APPLICABLE
                kind = LiquidityChangeKind.UNCHANGED
            transitions.append(LiquidityTransition(
                prior.event.event_id, current.event.event_id, kind, quantity, attribution,
            ))
        latest = ordered[-1]
        relative = (
            latest.displayed_quantity / latest.nearby_same_side_quantity
            if latest.nearby_same_side_quantity > 0 else None
        )
        distance = (
            latest.touch_price - latest.price if side is BookSide.BID
            else latest.price - latest.touch_price
        ) / self.policy.tick_size
        persistence = Decimal(str((
            _time(latest.event.event_at, "Latest depth event time")
            - _time(ordered[0].event.event_at, "First depth event time")
        ).total_seconds()))
        reasons: list[str] = []
        if not feed_continuity_valid:
            reasons.append("LIQUIDITY_CLUSTER_BROKEN_FEED")
        if end - _time(latest.event.available_at, "Depth available-at time") > timedelta(
            seconds=self.policy.maximum_staleness_seconds
        ):
            reasons.append("LIQUIDITY_CLUSTER_STALE")
        if latest.event_kind in {DepthEventKind.OUT_OF_RANGE, DepthEventKind.RECONNECT_RESET}:
            reasons.append("LIQUIDITY_CLUSTER_RANGE_OR_RESET_AMBIGUOUS")
        if relative is None:
            reasons.append("LIQUIDITY_CLUSTER_NEARBY_DEPTH_UNAVAILABLE")
        elif relative < self.policy.minimum_relative_size:
            reasons.append("LIQUIDITY_CLUSTER_BELOW_RELATIVE_SIZE")
        source_ids = tuple(item.event.event_id for item in ordered)
        source_hashes = tuple(item.event.payload_hash for item in ordered)
        group_hash = canonical_hash({
            "kind": "LIQUIDITY_CLUSTER_INPUTS", "instrument": instrument,
            "side": side.value, "price": _dtext(price), "source_ids": source_ids,
            "source_hashes": source_hashes,
        })
        cluster_hash = canonical_hash({
            "continuity_domain": ordered[0].event.continuity_domain,
            "instrument": instrument, "side": side.value, "price": _dtext(price),
            "first_event_id": ordered[0].event.event_id,
        })
        interactions = tuple(item for item in ordered if item.interaction_volume > 0)
        return LiquidityClusterResult(
            cluster_id="liquidity-cluster-" + cluster_hash[:32],
            instrument=instrument,
            side=side,
            price=price,
            as_of=as_of,
            valid=not reasons,
            reason_codes=tuple(reasons),
            current_displayed_quantity=latest.displayed_quantity,
            nearby_same_side_quantity=latest.nearby_same_side_quantity,
            relative_size=relative,
            distance_from_touch_ticks=distance,
            persistence_seconds=persistence,
            interaction_count=len(interactions),
            interaction_volume=sum(
                (item.interaction_volume for item in interactions), Decimal("0")
            ),
            acceptance_count=sum(
                item.observed_price_response is PriceResponse.ACCEPTANCE
                for item in interactions
            ),
            rejection_count=sum(
                item.observed_price_response is PriceResponse.REJECTION
                for item in interactions
            ),
            depleted_quantity=depleted,
            replenished_quantity=replenished,
            transitions=tuple(transitions),
            source_event_at=latest.event.event_at,
            received_at=latest.event.received_at,
            available_at=as_of,
            continuity_domain=latest.event.continuity_domain,
            source_ids=source_ids,
            source_hashes=source_hashes,
            dependency_group_id="liquidity-dependency-" + group_hash[:32],
            policy_hash=self.policy.configuration_hash,
        )


@dataclass(frozen=True)
class VolumeWindow:
    window_id: str
    payload_hash: str
    instrument: str
    normalization_key: str
    window_start: str
    window_end: str
    available_at: str
    volume: Decimal
    kind: VolumeWindowKind
    continuity_valid: bool
    feed_fresh: bool

    def __post_init__(self) -> None:
        _text(self.window_id, "Volume window ID")
        _sha256(self.payload_hash, "Volume window hash")
        _text(self.instrument, "Volume window instrument")
        _text(self.normalization_key, "Volume normalization key")
        start = _time(self.window_start, "Volume window start")
        end = _time(self.window_end, "Volume window end")
        available = _time(self.available_at, "Volume window available-at time")
        if start >= end or end > available:
            raise ValueError("Volume window timing is non-causal.")
        _decimal(self.volume, "Window volume", minimum=Decimal("0"))
        if type(self.kind) is not VolumeWindowKind:
            raise ValueError("Volume window kind is required.")
        if any(type(item) is not bool for item in (self.continuity_valid, self.feed_fresh)):
            raise ValueError("Volume feed states must be booleans.")


@dataclass(frozen=True)
class VolumeRegimePolicy:
    policy_id: str = "pong-prior-only-volume-regime-research"
    version: str = "1"
    minimum_prior_windows: int = 3
    low_ratio: Decimal = Decimal("0.75")
    high_ratio: Decimal = Decimal("1.25")

    def __post_init__(self) -> None:
        _text(self.policy_id, "Volume regime policy ID")
        _text(self.version, "Volume regime policy version")
        if type(self.minimum_prior_windows) is not int or self.minimum_prior_windows < 1:
            raise ValueError("Minimum prior volume windows must be positive.")
        low = _decimal(self.low_ratio, "Low volume ratio")
        high = _decimal(self.high_ratio, "High volume ratio")
        if low <= 0 or low >= 1 or high <= 1:
            raise ValueError("Volume regime ratios are outside range.")

    def payload(self) -> dict[str, object]:
        return {
            "policy_id": self.policy_id, "version": self.version,
            "minimum_prior_windows": self.minimum_prior_windows,
            "low_ratio": _dtext(self.low_ratio), "high_ratio": _dtext(self.high_ratio),
            "reference_rule": "COMPLETED_PRIOR_WINDOWS_AVAILABLE_BEFORE_CURRENT_START",
            "production_configured": False, "execution_authority": "DENIED",
        }

    @property
    def configuration_hash(self) -> str:
        return canonical_hash(self.payload())


@dataclass(frozen=True)
class VolumeRegimeResult:
    instrument: str
    regime: VolumeRegime
    valid: bool
    reason_codes: tuple[str, ...]
    current_window_id: str
    current_volume: Decimal
    prior_median_volume: Decimal | None
    volume_ratio: Decimal | None
    as_of: str
    source_event_at: str
    received_at: str
    available_at: str
    source_ids: tuple[str, ...]
    source_hashes: tuple[str, ...]
    dependency_group_id: str
    policy_hash: str

    def __post_init__(self) -> None:
        _text(self.instrument, "Volume regime instrument")

    def payload(self) -> dict[str, object]:
        return {
            "schema": PONG_MICROSTRUCTURE_SCHEMA, "kind": "VOLUME_REGIME",
            "instrument": self.instrument,
            "regime": self.regime.value, "valid": self.valid,
            "reason_codes": list(self.reason_codes),
            "current_window_id": self.current_window_id,
            "current_volume": _dtext(self.current_volume),
            "prior_median_volume": None if self.prior_median_volume is None else _dtext(self.prior_median_volume),
            "volume_ratio": None if self.volume_ratio is None else _dtext(self.volume_ratio),
            "as_of": normalized_utc(self.as_of, "Volume regime as-of time"),
            "source_event_at": normalized_utc(self.source_event_at, "Volume source event time"),
            "received_at": normalized_utc(self.received_at, "Volume receipt time"),
            "available_at": normalized_utc(self.available_at, "Volume availability time"),
            "source_ids": list(self.source_ids), "source_hashes": list(self.source_hashes),
            "dependency_group_id": self.dependency_group_id, "policy_hash": self.policy_hash,
            "execution_authority": "DENIED",
        }

    @property
    def result_hash(self) -> str:
        return canonical_hash(self.payload())


class VolumeRegimeAdapter:
    def __init__(self, policy: VolumeRegimePolicy = VolumeRegimePolicy()) -> None:
        self.policy = policy

    def evaluate(
        self,
        current: VolumeWindow,
        prior_windows: Iterable[VolumeWindow],
        *,
        as_of: str,
    ) -> VolumeRegimeResult:
        decision = _time(as_of, "Volume regime as-of time")
        if _time(current.available_at, "Current volume availability") > decision:
            raise ValueError("Current volume was not available at the regime decision.")
        prior = tuple(sorted(prior_windows, key=lambda item: (
            _time(item.window_end, "Prior window end"), item.window_id,
        )))
        if len({item.window_id for item in prior}) != len(prior):
            raise ValueError("Duplicate prior volume window IDs are forbidden.")
        if any(item.instrument != current.instrument for item in prior):
            raise ValueError("Reference volume window instrument mismatch.")
        current_start = _time(current.window_start, "Current volume start")
        for item in prior:
            if item.kind is not VolumeWindowKind.COMPLETED:
                raise ValueError("Reference volume windows must be completed.")
            if item.normalization_key != current.normalization_key:
                raise ValueError("Reference window normalization key mismatch.")
            if _time(item.window_end, "Prior volume end") >= current_start:
                raise ValueError("Reference volume window is not strictly prior.")
            if _time(item.available_at, "Prior volume availability") > current_start:
                raise ValueError("Reference volume was not available before the current window.")
        source = (current, *prior)
        source_ids = tuple(item.window_id for item in source)
        source_hashes = tuple(item.payload_hash for item in source)
        group_hash = canonical_hash({
            "kind": "PRIOR_ONLY_VOLUME_REGIME_INPUTS",
            "instrument": current.instrument,
            "source_ids": source_ids, "source_hashes": source_hashes,
        })
        reasons: list[str] = []
        if not current.continuity_valid or not current.feed_fresh:
            reasons.append("VOLUME_REGIME_BROKEN_FEED")
        usable_prior = tuple(item for item in prior if item.continuity_valid and item.feed_fresh)
        if len(usable_prior) < self.policy.minimum_prior_windows:
            reasons.append("VOLUME_REGIME_INSUFFICIENT_PRIOR_HISTORY")
        median: Decimal | None = None
        ratio: Decimal | None = None
        regime = VolumeRegime.UNAVAILABLE
        if not reasons:
            ordered_volumes = sorted(item.volume for item in usable_prior)
            middle = len(ordered_volumes) // 2
            median = (
                ordered_volumes[middle] if len(ordered_volumes) % 2
                else (ordered_volumes[middle - 1] + ordered_volumes[middle]) / Decimal("2")
            )
            if median <= 0:
                reasons.append("VOLUME_REGIME_ZERO_REFERENCE_MEDIAN")
            else:
                ratio = current.volume / median
                regime = (
                    VolumeRegime.LOW if ratio < self.policy.low_ratio
                    else VolumeRegime.HIGH if ratio > self.policy.high_ratio
                    else VolumeRegime.NORMAL
                )
        return VolumeRegimeResult(
            instrument=current.instrument,
            regime=regime,
            valid=not reasons,
            reason_codes=tuple(reasons),
            current_window_id=current.window_id,
            current_volume=current.volume,
            prior_median_volume=median,
            volume_ratio=ratio,
            as_of=as_of,
            source_event_at=current.window_end,
            received_at=current.available_at,
            available_at=as_of,
            source_ids=source_ids,
            source_hashes=source_hashes,
            dependency_group_id="volume-regime-dependency-" + group_hash[:32],
            policy_hash=self.policy.configuration_hash,
        )


PONG_FLOW_COMPONENT = ("pong-symmetric-classified-flow", "mapping-v1")
PONG_BID_LIQUIDITY_COMPONENT = ("pong-bid-liquidity-interaction", "mapping-v1")
PONG_ASK_LIQUIDITY_COMPONENT = ("pong-ask-liquidity-interaction", "mapping-v1")
PONG_VOLUME_REGIME_COMPONENT = ("pong-prior-only-volume-regime", "mapping-v1")
PONG_FLOW_GROUP = "PONG_MICROSTRUCTURE_FLOW"
PONG_LIQUIDITY_GROUP = "PONG_MICROSTRUCTURE_LIQUIDITY"
PONG_VOLUME_REGIME_GROUP = "PONG_VOLUME_REGIME"


@dataclass(frozen=True)
class MicrostructureEvidenceMappingPolicy:
    """Frozen hypotheses for research replay; not calibrated trading weights."""

    mapping_id: str = "pong-microstructure-to-discernment-research"
    version: str = "1"
    flow_progress_effect: Decimal = Decimal("0.70")
    flow_absorption_effect: Decimal = Decimal("0.55")
    interacting_liquidity_effect: Decimal = Decimal("0.65")
    displayed_wall_effect: Decimal = Decimal("0.10")
    low_volume_multiplier: Decimal = Decimal("1.25")
    normal_volume_multiplier: Decimal = Decimal("1")
    high_volume_multiplier: Decimal = Decimal("0.85")
    flow_group_bound: Decimal = Decimal("0.80")
    liquidity_group_bound: Decimal = Decimal("1.00")
    regime_group_bound: Decimal = Decimal("0.01")
    validity_seconds: int = 5

    def __post_init__(self) -> None:
        _text(self.mapping_id, "Evidence mapping ID")
        _text(self.version, "Evidence mapping version")
        for value, field in (
            (self.flow_progress_effect, "Flow progress effect"),
            (self.flow_absorption_effect, "Flow absorption effect"),
            (self.interacting_liquidity_effect, "Interacting liquidity effect"),
            (self.displayed_wall_effect, "Displayed wall effect"),
            (self.low_volume_multiplier, "Low-volume multiplier"),
            (self.normal_volume_multiplier, "Normal-volume multiplier"),
            (self.high_volume_multiplier, "High-volume multiplier"),
            (self.flow_group_bound, "Flow group bound"),
            (self.liquidity_group_bound, "Liquidity group bound"),
            (self.regime_group_bound, "Regime group bound"),
        ):
            if _decimal(value, field) <= 0:
                raise ValueError(f"{field} must be positive.")
        if self.displayed_wall_effect >= self.interacting_liquidity_effect:
            raise ValueError("Mere displayed liquidity must weigh less than an interaction.")
        if type(self.validity_seconds) is not int or self.validity_seconds <= 0:
            raise ValueError("Mapped evidence validity must be positive.")

    def payload(self) -> dict[str, object]:
        return {
            "mapping_id": self.mapping_id,
            "version": self.version,
            "components": [
                list(PONG_FLOW_COMPONENT),
                list(PONG_BID_LIQUIDITY_COMPONENT),
                list(PONG_ASK_LIQUIDITY_COMPONENT),
                list(PONG_VOLUME_REGIME_COMPONENT),
            ],
            "effects": {
                "flow_progress": _dtext(self.flow_progress_effect),
                "flow_absorption": _dtext(self.flow_absorption_effect),
                "interacting_liquidity": _dtext(self.interacting_liquidity_effect),
                "displayed_wall": _dtext(self.displayed_wall_effect),
            },
            "regime_multipliers": {
                "LOW": _dtext(self.low_volume_multiplier),
                "NORMAL": _dtext(self.normal_volume_multiplier),
                "HIGH": _dtext(self.high_volume_multiplier),
            },
            "dependency_group_bounds": [
                [PONG_FLOW_GROUP, _dtext(self.flow_group_bound)],
                [PONG_LIQUIDITY_GROUP, _dtext(self.liquidity_group_bound)],
                [PONG_VOLUME_REGIME_GROUP, _dtext(self.regime_group_bound)],
            ],
            "validity_seconds": self.validity_seconds,
            "calibration_status": "EXPERIMENTAL_DIRECTIONAL_SUPPORT",
            "production_configured": False,
            "execution_authority": "DENIED",
        }

    @property
    def mapping_hash(self) -> str:
        return canonical_hash(self.payload())

    def discernment_policy(self) -> DiscernmentPolicy:
        return DiscernmentPolicy(
            required_components=(
                PONG_FLOW_COMPONENT,
                PONG_BID_LIQUIDITY_COMPONENT,
                PONG_ASK_LIQUIDITY_COMPONENT,
                PONG_VOLUME_REGIME_COMPONENT,
            ),
            dependency_group_bounds=(
                (PONG_FLOW_GROUP, float(self.flow_group_bound)),
                (PONG_LIQUIDITY_GROUP, float(self.liquidity_group_bound)),
                (PONG_VOLUME_REGIME_GROUP, float(self.regime_group_bound)),
            ),
            evidence_mapping_hash=self.mapping_hash,
        )


@dataclass(frozen=True)
class MappedMicrostructureEvidence:
    evidence: tuple[DiscernmentEvidence, ...]
    volume_regime: DiscernmentVolumeRegime
    mapping_hash: str
    source_artifacts: tuple[tuple[str, str], ...]
    blockers: tuple[str, ...]

    def __post_init__(self) -> None:
        _sha256(self.mapping_hash, "Evidence mapping hash")
        if tuple(sorted(self.source_artifacts)) != self.source_artifacts:
            raise ValueError("Mapped source artifacts must be deterministically ordered.")
        if len({item[0] for item in self.source_artifacts}) != len(self.source_artifacts):
            raise ValueError("Mapped source artifact IDs must be unique.")
        for artifact_id, artifact_hash in self.source_artifacts:
            _text(artifact_id, "Mapped source artifact ID")
            _sha256(artifact_hash, "Mapped source artifact hash")
        if self.blockers:
            if self.evidence or self.volume_regime is not DiscernmentVolumeRegime.UNAVAILABLE:
                raise ValueError("Blocked microstructure mapping cannot expose usable evidence.")
        elif not self.evidence or self.volume_regime is DiscernmentVolumeRegime.UNAVAILABLE:
            raise ValueError("Usable microstructure mapping requires evidence and volume regime.")

    def payload(self) -> dict[str, object]:
        return {
            "schema": PONG_MICROSTRUCTURE_SCHEMA,
            "kind": "DISCERNMENT_EVIDENCE_MAPPING",
            "evidence_hashes": [item.evidence_hash for item in self.evidence],
            "volume_regime": self.volume_regime.value,
            "mapping_hash": self.mapping_hash,
            "source_artifacts": [list(item) for item in self.source_artifacts],
            "blockers": list(self.blockers),
            "production_configured": False,
            "execution_authority": "DENIED",
        }

    @property
    def result_hash(self) -> str:
        return canonical_hash(self.payload())


class MicrostructureEvidenceMapper:
    """Map one causal microstructure frame into bounded Discernment evidence."""

    def __init__(
        self,
        policy: MicrostructureEvidenceMappingPolicy = MicrostructureEvidenceMappingPolicy(),
    ) -> None:
        self.policy = policy

    def _multiplier(self, regime: VolumeRegime) -> Decimal:
        return {
            VolumeRegime.LOW: self.policy.low_volume_multiplier,
            VolumeRegime.NORMAL: self.policy.normal_volume_multiplier,
            VolumeRegime.HIGH: self.policy.high_volume_multiplier,
        }[regime]

    def _flow_effect(self, result: ClassifiedFlowResult) -> Decimal:
        signed = {
            FlowBehavior.BUY_PROGRESS: self.policy.flow_progress_effect,
            FlowBehavior.BUY_ABSORBED: -self.policy.flow_absorption_effect,
            FlowBehavior.SELL_PROGRESS: -self.policy.flow_progress_effect,
            FlowBehavior.SELL_ABSORBED: self.policy.flow_absorption_effect,
            FlowBehavior.BALANCED: Decimal("0"),
        }
        return signed[result.behavior]

    def _liquidity_effect(self, result: LiquidityClusterResult) -> Decimal:
        side_sign = Decimal("1") if result.side is BookSide.BID else Decimal("-1")
        consumption = any(
            item.reduction_attribution is ReductionAttribution.TRADE_MATCHED_CONSUMPTION
            for item in result.transitions
        )
        if result.interaction_count and result.rejection_count > result.acceptance_count:
            return side_sign * self.policy.interacting_liquidity_effect
        if consumption and result.acceptance_count:
            return -side_sign * self.policy.interacting_liquidity_effect
        return side_sign * self.policy.displayed_wall_effect

    @staticmethod
    def _source_pairs(
        artifact_id: str,
        artifact_hash: str,
        source_ids: tuple[str, ...],
        source_hashes: tuple[str, ...],
    ) -> tuple[tuple[str, str], ...]:
        return tuple(sorted(((*zip(source_ids, source_hashes), (artifact_id, artifact_hash)))))

    def map_frame(
        self,
        flow: ClassifiedFlowResult,
        bid_liquidity: LiquidityClusterResult,
        ask_liquidity: LiquidityClusterResult,
        volume_regime: VolumeRegimeResult,
        *,
        first_evidence_revision: int,
    ) -> MappedMicrostructureEvidence:
        if type(first_evidence_revision) is not int or first_evidence_revision <= 0:
            raise ValueError("First mapped evidence revision must be positive.")
        blockers: list[str] = []
        if not flow.valid:
            blockers.extend(f"FLOW:{item}" for item in flow.reason_codes)
        if not bid_liquidity.valid:
            blockers.extend(f"BID_LIQUIDITY:{item}" for item in bid_liquidity.reason_codes)
        if not ask_liquidity.valid:
            blockers.extend(f"ASK_LIQUIDITY:{item}" for item in ask_liquidity.reason_codes)
        if not volume_regime.valid:
            blockers.extend(f"VOLUME_REGIME:{item}" for item in volume_regime.reason_codes)
        if bid_liquidity.side is not BookSide.BID:
            blockers.append("BID_LIQUIDITY_SIDE_MISMATCH")
        if ask_liquidity.side is not BookSide.ASK:
            blockers.append("ASK_LIQUIDITY_SIDE_MISMATCH")
        instruments = {
            flow.instrument,
            bid_liquidity.instrument,
            ask_liquidity.instrument,
            volume_regime.instrument,
        }
        if len(instruments) != 1:
            blockers.append("MICROSTRUCTURE_INSTRUMENT_MISMATCH")
        elif next(iter(instruments)) != DISCERNMENT_INSTRUMENT:
            blockers.append("MICROSTRUCTURE_INSTRUMENT_NOT_DISCERNMENT_INSTRUMENT")
        as_of_values = {
            normalized_utc(item, "Mapped microstructure as-of time")
            for item in (flow.available_at, bid_liquidity.available_at,
                         ask_liquidity.available_at, volume_regime.available_at)
        }
        if len(as_of_values) != 1:
            blockers.append("MICROSTRUCTURE_FRAME_AVAILABILITY_MISMATCH")
        continuity_domains = {flow.continuity_domain, bid_liquidity.continuity_domain,
                              ask_liquidity.continuity_domain}
        if len(continuity_domains) != 1:
            blockers.append("MICROSTRUCTURE_CONTINUITY_DOMAIN_MISMATCH")
        artifacts = tuple(sorted((
            ("classified-flow-result", flow.result_hash),
            (f"{bid_liquidity.side.value.lower()}-liquidity-result", bid_liquidity.result_hash),
            (f"{ask_liquidity.side.value.lower()}-liquidity-result", ask_liquidity.result_hash),
            ("volume-regime-result", volume_regime.result_hash),
        )))
        if blockers:
            return MappedMicrostructureEvidence(
                (), DiscernmentVolumeRegime.UNAVAILABLE, self.policy.mapping_hash,
                artifacts, tuple(blockers),
            )
        available_at = next(iter(as_of_values))
        valid_until = (
            _time(available_at, "Mapped evidence availability")
            + timedelta(seconds=self.policy.validity_seconds)
        ).isoformat().replace("+00:00", "Z")
        multiplier = self._multiplier(volume_regime.regime)
        specifications = (
            (
                PONG_FLOW_COMPONENT, PONG_FLOW_GROUP,
                self._flow_effect(flow) * multiplier, flow.source_event_at,
                flow.received_at, "classified-flow-result", flow.result_hash,
                flow.source_ids, flow.source_hashes,
            ),
            (
                PONG_BID_LIQUIDITY_COMPONENT, PONG_LIQUIDITY_GROUP,
                self._liquidity_effect(bid_liquidity) * multiplier,
                bid_liquidity.source_event_at, bid_liquidity.received_at,
                "bid-liquidity-result", bid_liquidity.result_hash,
                bid_liquidity.source_ids, bid_liquidity.source_hashes,
            ),
            (
                PONG_ASK_LIQUIDITY_COMPONENT, PONG_LIQUIDITY_GROUP,
                self._liquidity_effect(ask_liquidity) * multiplier,
                ask_liquidity.source_event_at, ask_liquidity.received_at,
                "ask-liquidity-result", ask_liquidity.result_hash,
                ask_liquidity.source_ids, ask_liquidity.source_hashes,
            ),
            (
                PONG_VOLUME_REGIME_COMPONENT, PONG_VOLUME_REGIME_GROUP,
                Decimal("0"), volume_regime.source_event_at, volume_regime.received_at,
                "volume-regime-result", volume_regime.result_hash,
                volume_regime.source_ids, volume_regime.source_hashes,
            ),
        )
        evidence: list[DiscernmentEvidence] = []
        continuity_domain = next(iter(continuity_domains))
        for offset, specification in enumerate(specifications):
            (
                component, group, effect, source_event_at, received_at,
                artifact_id, artifact_hash, source_ids, source_hashes,
            ) = specification
            pairs = self._source_pairs(artifact_id, artifact_hash, source_ids, source_hashes)
            revision = first_evidence_revision + offset
            event_material = {
                "mapping_hash": self.policy.mapping_hash,
                "component": list(component),
                "dependency_group": group,
                "effect": _dtext(effect),
                "revision": revision,
                "source_pairs": [list(item) for item in pairs],
            }
            evidence.append(DiscernmentEvidence(
                event_id="pong-mapped-evidence-" + canonical_hash(event_material)[:32],
                component_id=component[0],
                component_version=component[1],
                dependency_group=group,
                log_odds_effect=float(effect),
                required=True,
                source_event_at=source_event_at,
                received_at=received_at,
                available_at=available_at,
                valid_until=valid_until,
                evidence_revision=revision,
                continuity_domain=continuity_domain,
                continuity_valid=True,
                source_ids=tuple(item[0] for item in pairs),
                source_hashes=tuple(item[1] for item in pairs),
            ))
        return MappedMicrostructureEvidence(
            tuple(evidence), DiscernmentVolumeRegime(volume_regime.regime.value),
            self.policy.mapping_hash, artifacts, (),
        )
