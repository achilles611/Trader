"""Lane III Phase B deterministic MNQ market-observation substrate.

This module deliberately models observations and mechanical reconstructions
only.  It has no network, broker, account, signal, hypothesis, confidence, or
execution dependency.  Provider adapters live behind the small protocol at
the end of this file and must turn their packets into these explicit types.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone, tzinfo
from decimal import Decimal, InvalidOperation
from enum import StrEnum
import json
import re
from types import MappingProxyType
from typing import Iterable, Mapping, Protocol, TypeAlias
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .contracts import LaneIIIInstrument, LaneIIIRefused, canonical_hash, normalized_utc, require_l3a_manifest


L3B_SCHEMA = "lane-iii-phase-b-market-intelligence-v1"
L3B_VERSION = "lane-iii-phase-b-v1"
MNQ_TICK_SIZE = Decimal("0.25")
_MNQ_MONTHS = {"F": 1, "G": 2, "H": 3, "J": 4, "K": 5, "M": 6, "N": 7, "Q": 8, "U": 9, "V": 10, "X": 11, "Z": 12}
# Loading Phase B fails closed if the frozen Phase A authority manifest is
# absent or altered.  Phase B adds no authority to that constitution.
L3B_L3A_CONSTITUTION = require_l3a_manifest()


class _USCentralFallback(tzinfo):
    """Current US Central IANA rules when a Windows Python lacks tzdata.

    The deployment remains explicitly America/Chicago.  This narrow fallback
    exists so deterministic session boundaries do not silently degrade to a
    fixed offset in environments without an installed IANA zone database.
    """

    @staticmethod
    def _bounds(year: int) -> tuple[datetime, datetime, datetime, datetime]:
        march_eighth = datetime(year, 3, 8)
        dst_start = march_eighth + timedelta(days=(6 - march_eighth.weekday()) % 7, hours=2)
        november_first = datetime(year, 11, 1)
        dst_end = november_first + timedelta(days=(6 - november_first.weekday()) % 7, hours=2)
        return dst_start, dst_end, dst_start + timedelta(hours=6), dst_end + timedelta(hours=5)

    def utcoffset(self, value: datetime | None) -> timedelta:
        return timedelta(hours=-6) + self.dst(value)

    def dst(self, value: datetime | None) -> timedelta:
        if value is None:
            return timedelta(0)
        naive = value.replace(tzinfo=None)
        start, end, _, _ = self._bounds(naive.year)
        return timedelta(hours=1) if start <= naive < end else timedelta(0)

    def tzname(self, value: datetime | None) -> str:
        return "CDT" if self.dst(value) else "CST"

    def fromutc(self, value: datetime) -> datetime:
        utc = value.replace(tzinfo=None)
        _, _, start_utc, end_utc = self._bounds(utc.year)
        offset = timedelta(hours=-5) if start_utc <= utc < end_utc else timedelta(hours=-6)
        return (utc + offset).replace(tzinfo=self)


class MarketDataRefused(LaneIIIRefused):
    """A market event is malformed or exceeds L3-B's observation authority."""


class BackpressureRefused(MarketDataRefused):
    """A bounded hand-off queue filled before an event could be accepted."""


def _text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} is required.")
    return value


def _finite_decimal(value: object, field: str, *, positive: bool = False, nonnegative: bool = False) -> Decimal:
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{field} must be a finite decimal.") from exc
    if not number.is_finite():
        raise ValueError(f"{field} must be a finite decimal.")
    if positive and number <= 0:
        raise ValueError(f"{field} must be positive.")
    if nonnegative and number < 0:
        raise ValueError(f"{field} must be non-negative.")
    return number


def _quantity(value: object, field: str, *, allow_zero: bool = False) -> int:
    if type(value) is not int or value < 0 or (value == 0 and not allow_zero):
        qualifier = "non-negative" if allow_zero else "positive"
        raise ValueError(f"{field} must be a {qualifier} integer quantity.")
    return value


def _price(value: object, field: str) -> Decimal:
    price = _finite_decimal(value, field, positive=True)
    if price % MNQ_TICK_SIZE != 0:
        raise ValueError(f"{field} must align to the MNQ {MNQ_TICK_SIZE} tick size.")
    return price


def _hash(value: object, field: str) -> str:
    if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise ValueError(f"{field} must be a lowercase SHA-256 digest.")
    return value


def _json_value(value: object, field: str) -> None:
    try:
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be JSON serializable without NaN or infinity.") from exc


class MarketStream(StrEnum):
    TRADE = "TRADE"
    QUOTE = "QUOTE"
    DEPTH = "DEPTH"
    SESSION_CONTEXT = "SESSION_CONTEXT"
    DERIVATIVES_CONTEXT = "DERIVATIVES_CONTEXT"


class AggressorSide(StrEnum):
    BUY = "BUY"
    SELL = "SELL"
    UNKNOWN = "UNKNOWN"


class AggressorProvenance(StrEnum):
    PROVIDER = "PROVIDER"
    QUOTE_DERIVED = "QUOTE_DERIVED"
    UNAVAILABLE = "UNAVAILABLE"


class BookSide(StrEnum):
    BID = "BID"
    ASK = "ASK"


class DepthOperation(StrEnum):
    UPSERT = "UPSERT"
    REMOVE = "REMOVE"


class DataQuality(StrEnum):
    """Truthfulness state, not an indicator or trading readiness score."""

    HEALTHY = "HEALTHY"
    STALE = "STALE"
    GAPPED = "GAPPED"
    RECOVERING = "RECOVERING"
    INCOMPLETE = "INCOMPLETE"
    INVALID = "INVALID"


class OrderingOutcome(StrEnum):
    ACCEPTED = "ACCEPTED"
    DUPLICATE = "DUPLICATE"
    LATE = "LATE"
    GAP = "GAP"
    UNSEQUENCED = "UNSEQUENCED"


class BookApplyOutcome(StrEnum):
    SNAPSHOT_APPLIED = "SNAPSHOT_APPLIED"
    DELTA_APPLIED = "DELTA_APPLIED"
    DUPLICATE = "DUPLICATE"
    LATE = "LATE"
    GAP = "GAP"
    RECOVERY_REQUIRED = "RECOVERY_REQUIRED"
    SOURCE_MISMATCH = "SOURCE_MISMATCH"


class LiquidityBehavior(StrEnum):
    """Mechanical displayed-liquidity measurements; never an intent claim."""

    ADD = "ADD"
    REDUCE = "REDUCE"
    PULL = "PULL"
    REPLENISH = "REPLENISH"
    PERSIST = "PERSIST"
    EXECUTE = "EXECUTE"


@dataclass(frozen=True)
class MarketDataSource:
    """Identity of a market-data source, distinct from an execution provider."""

    provider: str
    feed: str

    def __post_init__(self) -> None:
        _text(self.provider, "Market-data provider")
        _text(self.feed, "Market-data feed")

    def payload(self) -> dict[str, str]:
        return {"provider": self.provider, "feed": self.feed}


@dataclass(frozen=True)
class MNQContract:
    """An observed CME MNQ expiry; it is not a continuous execution symbol."""

    contract_symbol: str
    contract_expiry: str
    exchange: str = "CME"
    strategy_instrument: LaneIIIInstrument = LaneIIIInstrument.MNQ

    def __post_init__(self) -> None:
        if self.strategy_instrument is not LaneIIIInstrument.MNQ:
            raise MarketDataRefused("L3-B observes only the MNQ strategy root.")
        if self.exchange != "CME":
            raise MarketDataRefused("L3-B MNQ observations must identify CME.")
        match = re.fullmatch(r"MNQ([FGHJKMNQUVXZ])(\d{1,2})", self.contract_symbol)
        if match is None:
            raise MarketDataRefused("Observed instrument must be a concrete MNQ CME expiry such as MNQU6.")
        if not isinstance(self.contract_expiry, str) or re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", self.contract_expiry) is None:
            raise ValueError("Contract expiry must be YYYY-MM.")
        year, month = self.contract_expiry.split("-")
        if int(month) != _MNQ_MONTHS[match.group(1)] or int(year) % (10 if len(match.group(2)) == 1 else 100) != int(match.group(2)):
            raise ValueError("Contract symbol month/year must agree with contract expiry.")

    def payload(self) -> dict[str, str]:
        return {
            "strategy_instrument": self.strategy_instrument.value,
            "contract_symbol": self.contract_symbol,
            "contract_expiry": self.contract_expiry,
            "exchange": self.exchange,
        }


@dataclass(frozen=True)
class EventTimestamps:
    """Separate source clocks.  Receipt time is required; source clocks may be absent."""

    local_receipt_time: str
    exchange_time: str | None = None
    provider_time: str | None = None

    def __post_init__(self) -> None:
        normalized_utc(self.local_receipt_time, "Local receipt time")
        if self.exchange_time is not None:
            normalized_utc(self.exchange_time, "Exchange event time")
        if self.provider_time is not None:
            normalized_utc(self.provider_time, "Provider time")

    @property
    def authoritative_event_time(self) -> str | None:
        return self.exchange_time or self.provider_time

    @property
    def ordering_time(self) -> str:
        """A deterministic fallback, not an assertion of exchange ordering."""
        return self.exchange_time or self.provider_time or self.local_receipt_time

    def payload(self) -> dict[str, str | None]:
        return {
            "exchange_time": self.exchange_time and normalized_utc(self.exchange_time, "Exchange event time"),
            "provider_time": self.provider_time and normalized_utc(self.provider_time, "Provider time"),
            "local_receipt_time": normalized_utc(self.local_receipt_time, "Local receipt time"),
        }


@dataclass(frozen=True)
class EventHeader:
    """Common provenance and ordering metadata for one canonical event."""

    event_id: str
    source: MarketDataSource
    instrument: MNQContract
    timestamps: EventTimestamps
    stream: MarketStream
    raw_event_id: str
    raw_payload_hash: str
    provider_sequence: int | None = None
    provider_event_id: str | None = None

    def __post_init__(self) -> None:
        _text(self.event_id, "Canonical event identity")
        if type(self.source) is not MarketDataSource or type(self.instrument) is not MNQContract:
            raise ValueError("Canonical event requires explicit source and concrete MNQ contract.")
        if type(self.timestamps) is not EventTimestamps or type(self.stream) is not MarketStream:
            raise ValueError("Canonical event requires explicit timestamps and stream.")
        _text(self.raw_event_id, "Raw event identity")
        _hash(self.raw_payload_hash, "Raw payload hash")
        if self.provider_sequence is not None and (type(self.provider_sequence) is not int or self.provider_sequence < 0):
            raise ValueError("Provider sequence must be a non-negative integer when available.")
        if self.provider_event_id is not None:
            _text(self.provider_event_id, "Provider event identity")

    def payload(self) -> dict[str, object]:
        return {
            "event_id": self.event_id,
            "source": self.source.payload(),
            "instrument": self.instrument.payload(),
            "timestamps": self.timestamps.payload(),
            "stream": self.stream.value,
            "raw_event_id": self.raw_event_id,
            "raw_payload_hash": self.raw_payload_hash,
            "provider_sequence": self.provider_sequence,
            "provider_event_id": self.provider_event_id,
        }


@dataclass(frozen=True)
class TradeEvent:
    header: EventHeader
    price: Decimal
    size: int
    aggressor_side: AggressorSide = AggressorSide.UNKNOWN
    aggressor_provenance: AggressorProvenance = AggressorProvenance.UNAVAILABLE
    derivation_quote_event_id: str | None = None

    def __post_init__(self) -> None:
        if type(self.header) is not EventHeader or self.header.stream is not MarketStream.TRADE:
            raise ValueError("Trade event requires a TRADE header.")
        _price(self.price, "Trade price")
        _quantity(self.size, "Trade size")
        if type(self.aggressor_side) is not AggressorSide or type(self.aggressor_provenance) is not AggressorProvenance:
            raise ValueError("Trade aggressor values must be explicit.")
        if self.aggressor_side is AggressorSide.UNKNOWN and self.aggressor_provenance is not AggressorProvenance.UNAVAILABLE:
            raise ValueError("Unknown aggressor side must retain UNAVAILABLE provenance.")
        if self.aggressor_side is not AggressorSide.UNKNOWN and self.aggressor_provenance is AggressorProvenance.UNAVAILABLE:
            raise ValueError("Known aggressor side requires provider or quote-derived provenance.")
        if self.aggressor_provenance is AggressorProvenance.QUOTE_DERIVED:
            _text(self.derivation_quote_event_id, "Aggressor derivation quote identity")
        elif self.derivation_quote_event_id is not None:
            raise ValueError("Only quote-derived aggressor side may carry a derivation quote identity.")

    def payload(self) -> dict[str, object]:
        return {
            "kind": "TRADE",
            "header": self.header.payload(),
            "price": str(self.price),
            "size": self.size,
            "aggressor_side": self.aggressor_side.value,
            "aggressor_provenance": self.aggressor_provenance.value,
            "derivation_quote_event_id": self.derivation_quote_event_id,
        }


@dataclass(frozen=True)
class QuoteEvent:
    header: EventHeader
    bid_price: Decimal
    ask_price: Decimal
    bid_quantity: int
    ask_quantity: int

    def __post_init__(self) -> None:
        if type(self.header) is not EventHeader or self.header.stream is not MarketStream.QUOTE:
            raise ValueError("Quote event requires a QUOTE header.")
        bid = _price(self.bid_price, "Best bid")
        ask = _price(self.ask_price, "Best ask")
        if bid > ask:
            raise ValueError("Best bid may not exceed best ask.")
        _quantity(self.bid_quantity, "Best bid quantity")
        _quantity(self.ask_quantity, "Best ask quantity")

    @property
    def spread_ticks(self) -> int:
        return int((self.ask_price - self.bid_price) / MNQ_TICK_SIZE)

    def payload(self) -> dict[str, object]:
        return {
            "kind": "QUOTE",
            "header": self.header.payload(),
            "bid_price": str(self.bid_price),
            "ask_price": str(self.ask_price),
            "bid_quantity": self.bid_quantity,
            "ask_quantity": self.ask_quantity,
        }


@dataclass(frozen=True)
class BookLevel:
    price: Decimal
    quantity: int

    def __post_init__(self) -> None:
        _price(self.price, "Depth price")
        _quantity(self.quantity, "Depth quantity")

    def payload(self) -> dict[str, object]:
        return {"price": str(self.price), "quantity": self.quantity}


def _validate_levels(levels: tuple[BookLevel, ...], side: BookSide) -> None:
    if not isinstance(levels, tuple) or any(type(level) is not BookLevel for level in levels):
        raise ValueError("Depth levels must be an immutable tuple of BookLevel values.")
    prices = [level.price for level in levels]
    if len(prices) != len(set(prices)):
        raise ValueError("Depth snapshot may not repeat a price level.")
    ordered = sorted(prices, reverse=side is BookSide.BID)
    if prices != ordered:
        direction = "descending" if side is BookSide.BID else "ascending"
        raise ValueError(f"{side.value} snapshot levels must be {direction} by price.")


@dataclass(frozen=True)
class BookSnapshotEvent:
    header: EventHeader
    bids: tuple[BookLevel, ...]
    asks: tuple[BookLevel, ...]

    def __post_init__(self) -> None:
        if type(self.header) is not EventHeader or self.header.stream is not MarketStream.DEPTH:
            raise ValueError("Book snapshot requires a DEPTH header.")
        _validate_levels(self.bids, BookSide.BID)
        _validate_levels(self.asks, BookSide.ASK)
        if self.bids and self.asks and self.bids[0].price > self.asks[0].price:
            raise ValueError("Depth snapshot best bid may not exceed best ask.")

    def payload(self) -> dict[str, object]:
        return {
            "kind": "BOOK_SNAPSHOT",
            "header": self.header.payload(),
            "bids": [level.payload() for level in self.bids],
            "asks": [level.payload() for level in self.asks],
        }


@dataclass(frozen=True)
class BookDeltaEvent:
    header: EventHeader
    side: BookSide
    operation: DepthOperation
    price: Decimal
    quantity: int | None = None

    def __post_init__(self) -> None:
        if type(self.header) is not EventHeader or self.header.stream is not MarketStream.DEPTH:
            raise ValueError("Book delta requires a DEPTH header.")
        if type(self.side) is not BookSide or type(self.operation) is not DepthOperation:
            raise ValueError("Book delta requires explicit side and operation.")
        _price(self.price, "Depth delta price")
        if self.operation is DepthOperation.UPSERT:
            _quantity(self.quantity, "Depth upsert quantity")
        elif self.quantity is not None:
            raise ValueError("Depth removal must not assert a residual quantity.")

    def payload(self) -> dict[str, object]:
        return {
            "kind": "BOOK_DELTA",
            "header": self.header.payload(),
            "side": self.side.value,
            "operation": self.operation.value,
            "price": str(self.price),
            "quantity": self.quantity,
        }


class OptionRight(StrEnum):
    CALL = "CALL"
    PUT = "PUT"


@dataclass(frozen=True)
class DerivativesContextEvent:
    """Optional extension record.  OI vintage is explicit and may be old."""

    header: EventHeader
    underlying: str
    expiry: str
    strike: Decimal
    right: OptionRight
    open_interest: int | None
    volume: int | None
    data_vintage_time: str

    def __post_init__(self) -> None:
        if type(self.header) is not EventHeader or self.header.stream is not MarketStream.DERIVATIVES_CONTEXT:
            raise ValueError("Derivatives context requires a DERIVATIVES_CONTEXT header.")
        _text(self.underlying, "Derivatives underlying")
        if not isinstance(self.expiry, str) or re.fullmatch(r"\d{4}-\d{2}-\d{2}", self.expiry) is None:
            raise ValueError("Options expiry must be YYYY-MM-DD.")
        _finite_decimal(self.strike, "Options strike", positive=True)
        if type(self.right) is not OptionRight:
            raise ValueError("Options right must be explicit.")
        if self.open_interest is not None:
            _quantity(self.open_interest, "Open interest", allow_zero=True)
        if self.volume is not None:
            _quantity(self.volume, "Options volume", allow_zero=True)
        normalized_utc(self.data_vintage_time, "Derivatives data vintage time")

    def payload(self) -> dict[str, object]:
        return {
            "kind": "DERIVATIVES_CONTEXT",
            "header": self.header.payload(),
            "underlying": self.underlying,
            "expiry": self.expiry,
            "strike": str(self.strike),
            "right": self.right.value,
            "open_interest": self.open_interest,
            "volume": self.volume,
            "data_vintage_time": normalized_utc(self.data_vintage_time, "Derivatives data vintage time"),
        }


CanonicalMarketEvent: TypeAlias = TradeEvent | QuoteEvent | BookSnapshotEvent | BookDeltaEvent | DerivativesContextEvent


@dataclass(frozen=True)
class RawProviderEvent:
    """Append-only provider payload plus identity; it is intentionally untrusted."""

    raw_event_id: str
    source: MarketDataSource
    received_at: str
    payload: Mapping[str, object]
    provider_event_id: str | None = None

    def __post_init__(self) -> None:
        _text(self.raw_event_id, "Raw event identity")
        if type(self.source) is not MarketDataSource:
            raise ValueError("Raw event requires an explicit source.")
        normalized_utc(self.received_at, "Raw receipt time")
        if not isinstance(self.payload, Mapping):
            raise ValueError("Raw payload must be a mapping.")
        _json_value(dict(self.payload), "Raw payload")
        object.__setattr__(self, "payload", MappingProxyType(dict(self.payload)))
        if self.provider_event_id is not None:
            _text(self.provider_event_id, "Provider event identity")

    @property
    def payload_hash(self) -> str:
        return canonical_hash(dict(self.payload))

    def payload_record(self) -> dict[str, object]:
        return {
            "raw_event_id": self.raw_event_id,
            "source": self.source.payload(),
            "received_at": normalized_utc(self.received_at, "Raw receipt time"),
            "provider_event_id": self.provider_event_id,
            "payload": dict(self.payload),
            "payload_hash": self.payload_hash,
        }


@dataclass(frozen=True)
class SequenceAssessment:
    outcome: OrderingOutcome
    previous_sequence: int | None
    received_sequence: int | None


class SequenceGuard:
    """Per-stream sequence guard.  Unsequenced data is usable only as incomplete."""

    def __init__(self) -> None:
        self._last_sequence: int | None = None

    @property
    def last_sequence(self) -> int | None:
        return self._last_sequence

    def assess(self, sequence: int | None) -> SequenceAssessment:
        previous = self._last_sequence
        if sequence is None:
            return SequenceAssessment(OrderingOutcome.UNSEQUENCED, previous, None)
        if previous is None:
            self._last_sequence = sequence
            return SequenceAssessment(OrderingOutcome.ACCEPTED, None, sequence)
        if sequence == previous:
            return SequenceAssessment(OrderingOutcome.DUPLICATE, previous, sequence)
        if sequence < previous:
            return SequenceAssessment(OrderingOutcome.LATE, previous, sequence)
        self._last_sequence = sequence
        if sequence > previous + 1:
            return SequenceAssessment(OrderingOutcome.GAP, previous, sequence)
        return SequenceAssessment(OrderingOutcome.ACCEPTED, previous, sequence)

    def reset_for_reconnect(self) -> None:
        self._last_sequence = None


@dataclass(frozen=True)
class BookChange:
    side: BookSide
    price: Decimal
    prior_quantity: int
    current_quantity: int
    behavior: LiquidityBehavior
    source_event_id: str
    supporting_trade_event_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if type(self.side) is not BookSide or type(self.behavior) is not LiquidityBehavior:
            raise ValueError("Book change must have explicit side and mechanical behavior.")
        _price(self.price, "Book change price")
        _quantity(self.prior_quantity, "Prior book quantity", allow_zero=True)
        _quantity(self.current_quantity, "Current book quantity", allow_zero=True)
        _text(self.source_event_id, "Book change source event identity")
        if self.behavior is LiquidityBehavior.ADD and self.current_quantity <= self.prior_quantity:
            raise ValueError("ADD must increase displayed quantity.")
        if self.behavior in {LiquidityBehavior.REDUCE, LiquidityBehavior.PULL, LiquidityBehavior.EXECUTE} and self.current_quantity >= self.prior_quantity:
            raise ValueError("Reduction behavior must reduce displayed quantity.")


@dataclass(frozen=True)
class ReconstructedBookState:
    source: MarketDataSource
    instrument: MNQContract
    bids: tuple[BookLevel, ...]
    asks: tuple[BookLevel, ...]
    quality: DataQuality
    snapshot_event_id: str | None
    latest_event_id: str | None
    latest_receipt_time: str | None
    latest_sequence: int | None
    source_event_ids: tuple[str, ...]
    gap_detected: bool

    def __post_init__(self) -> None:
        if type(self.source) is not MarketDataSource or type(self.instrument) is not MNQContract:
            raise ValueError("Reconstructed book must retain source and concrete contract provenance.")
        _validate_levels(self.bids, BookSide.BID)
        _validate_levels(self.asks, BookSide.ASK)
        if self.bids and self.asks and self.bids[0].price > self.asks[0].price:
            raise ValueError("Reconstructed book best bid may not exceed best ask.")
        if type(self.quality) is not DataQuality:
            raise ValueError("Reconstructed book quality must be explicit.")
        if self.latest_sequence is not None and (type(self.latest_sequence) is not int or self.latest_sequence < 0):
            raise ValueError("Reconstructed book sequence is invalid.")
        if self.latest_receipt_time is not None:
            normalized_utc(self.latest_receipt_time, "Reconstructed book receipt time")

    def payload(self) -> dict[str, object]:
        return {
            "source": self.source.payload(),
            "instrument": self.instrument.payload(),
            "bids": [level.payload() for level in self.bids],
            "asks": [level.payload() for level in self.asks],
            "quality": self.quality.value,
            "snapshot_event_id": self.snapshot_event_id,
            "latest_event_id": self.latest_event_id,
            "latest_receipt_time": self.latest_receipt_time,
            "latest_sequence": self.latest_sequence,
            "source_event_ids": list(self.source_event_ids),
            "gap_detected": self.gap_detected,
        }

    @property
    def state_hash(self) -> str:
        return canonical_hash(self.payload())


@dataclass(frozen=True)
class BookApplication:
    outcome: BookApplyOutcome
    state: ReconstructedBookState
    changes: tuple[BookChange, ...] = ()


class OrderBookReconstructor:
    """Single-threaded deterministic snapshot-plus-delta order-book reconstruction."""

    def __init__(self, source: MarketDataSource, instrument: MNQContract) -> None:
        self._source = source
        self._instrument = instrument
        self._bids: dict[Decimal, int] = {}
        self._asks: dict[Decimal, int] = {}
        self._quality = DataQuality.INCOMPLETE
        self._snapshot_event_id: str | None = None
        self._latest_event_id: str | None = None
        self._latest_receipt_time: str | None = None
        self._latest_sequence: int | None = None
        self._source_event_ids: list[str] = []
        self._gap_detected = False
        self._removed: dict[tuple[BookSide, Decimal], str] = {}
        self._first_seen: dict[tuple[BookSide, Decimal], str] = {}

    def _state(self) -> ReconstructedBookState:
        bids = tuple(BookLevel(price, quantity) for price, quantity in sorted(self._bids.items(), reverse=True))
        asks = tuple(BookLevel(price, quantity) for price, quantity in sorted(self._asks.items()))
        return ReconstructedBookState(
            source=self._source,
            instrument=self._instrument,
            bids=bids,
            asks=asks,
            quality=self._quality,
            snapshot_event_id=self._snapshot_event_id,
            latest_event_id=self._latest_event_id,
            latest_receipt_time=self._latest_receipt_time,
            latest_sequence=self._latest_sequence,
            source_event_ids=tuple(self._source_event_ids),
            gap_detected=self._gap_detected,
        )

    def _matches(self, event: BookSnapshotEvent | BookDeltaEvent) -> bool:
        return event.header.source == self._source and event.header.instrument == self._instrument

    def mark_invalid(self) -> ReconstructedBookState:
        """An adapter uses this after rejecting a malformed provider depth packet."""
        self._quality = DataQuality.INVALID
        return self._state()

    def notify_reconnect(self) -> ReconstructedBookState:
        """Incremental state can never bridge a disconnect."""
        self._quality = DataQuality.RECOVERING
        self._latest_sequence = None
        self._gap_detected = True
        return self._state()

    def mark_stale(self, as_of: str, maximum_age: timedelta) -> ReconstructedBookState:
        now = datetime.fromisoformat(normalized_utc(as_of, "Book staleness assessment time").replace("Z", "+00:00"))
        if not isinstance(maximum_age, timedelta) or maximum_age <= timedelta(0):
            raise ValueError("Book staleness maximum age must be positive.")
        if self._latest_receipt_time is not None and self._quality is DataQuality.HEALTHY and now - datetime.fromisoformat(normalized_utc(self._latest_receipt_time, "Book receipt time").replace("Z", "+00:00")) > maximum_age:
            self._quality = DataQuality.STALE
        return self._state()

    def apply(self, event: BookSnapshotEvent | BookDeltaEvent) -> BookApplication:
        if not self._matches(event):
            return BookApplication(BookApplyOutcome.SOURCE_MISMATCH, self._state())
        sequence = event.header.provider_sequence
        if sequence is not None and self._latest_sequence is not None:
            if sequence == self._latest_sequence:
                return BookApplication(BookApplyOutcome.DUPLICATE, self._state())
            if sequence < self._latest_sequence:
                return BookApplication(BookApplyOutcome.LATE, self._state())
        if isinstance(event, BookDeltaEvent):
            if sequence is None or self._quality not in {DataQuality.HEALTHY, DataQuality.STALE}:
                return BookApplication(BookApplyOutcome.RECOVERY_REQUIRED, self._state())
            if self._latest_sequence is not None and sequence > self._latest_sequence + 1:
                self._latest_sequence = sequence
                self._gap_detected = True
                self._quality = DataQuality.GAPPED
                return BookApplication(BookApplyOutcome.GAP, self._state())
            return self._apply_delta(event)
        return self._apply_snapshot(event)

    def _apply_snapshot(self, event: BookSnapshotEvent) -> BookApplication:
        sequence = event.header.provider_sequence
        self._bids = {level.price: level.quantity for level in event.bids}
        self._asks = {level.price: level.quantity for level in event.asks}
        self._snapshot_event_id = event.header.event_id
        self._latest_event_id = event.header.event_id
        self._latest_receipt_time = event.header.timestamps.local_receipt_time
        self._latest_sequence = sequence
        self._source_event_ids = [event.header.event_id]
        self._gap_detected = False
        self._removed.clear()
        self._first_seen = {
            (BookSide.BID, level.price): event.header.timestamps.ordering_time for level in event.bids
        } | {
            (BookSide.ASK, level.price): event.header.timestamps.ordering_time for level in event.asks
        }
        self._quality = DataQuality.HEALTHY if sequence is not None else DataQuality.INCOMPLETE
        return BookApplication(BookApplyOutcome.SNAPSHOT_APPLIED, self._state())

    def _apply_delta(self, event: BookDeltaEvent) -> BookApplication:
        assert event.header.provider_sequence is not None
        levels = self._bids if event.side is BookSide.BID else self._asks
        prior = levels.get(event.price, 0)
        current = 0 if event.operation is DepthOperation.REMOVE else event.quantity
        assert current is not None
        if current == 0:
            levels.pop(event.price, None)
            self._removed[(event.side, event.price)] = event.header.event_id
            self._first_seen.pop((event.side, event.price), None)
        else:
            levels[event.price] = current
            self._first_seen.setdefault((event.side, event.price), event.header.timestamps.ordering_time)
        self._latest_event_id = event.header.event_id
        self._latest_receipt_time = event.header.timestamps.local_receipt_time
        self._latest_sequence = event.header.provider_sequence
        self._source_event_ids.append(event.header.event_id)
        self._quality = DataQuality.HEALTHY
        behavior: LiquidityBehavior | None = None
        if current > prior:
            behavior = LiquidityBehavior.REPLENISH if (event.side, event.price) in self._removed else LiquidityBehavior.ADD
        elif current < prior:
            # A disappearance is only a reduction at this point.  It becomes
            # a pull only after the caller provides the relevant observed
            # trade window and establishes that no matching execution exists.
            behavior = LiquidityBehavior.REDUCE
        changes = () if behavior is None else (BookChange(event.side, event.price, prior, current, behavior, event.header.event_id),)
        return BookApplication(BookApplyOutcome.DELTA_APPLIED, self._state(), changes)

    def classify_reduction_with_trades(self, change: BookChange, trades: Iterable[TradeEvent]) -> BookChange:
        """Promote only a matching, observed execution to the mechanical EXECUTE label."""
        if change.behavior not in {LiquidityBehavior.REDUCE, LiquidityBehavior.PULL}:
            return change
        required_side = AggressorSide.SELL if change.side is BookSide.BID else AggressorSide.BUY
        matches = tuple(
            trade.header.event_id
            for trade in trades
            if trade.price == change.price and trade.aggressor_side is required_side
        )
        if not matches:
            if change.current_quantity == 0:
                return BookChange(
                    change.side, change.price, change.prior_quantity, change.current_quantity,
                    LiquidityBehavior.PULL, change.source_event_id,
                )
            return change
        return BookChange(
            change.side, change.price, change.prior_quantity, change.current_quantity,
            LiquidityBehavior.EXECUTE, change.source_event_id, matches,
        )

    def persistence_measurements(self, as_of: str, minimum_duration: timedelta) -> tuple[BookChange, ...]:
        """Report displayed persistence, without deciding why liquidity remains."""
        as_of_utc = datetime.fromisoformat(normalized_utc(as_of, "Persistence assessment time").replace("Z", "+00:00"))
        if not isinstance(minimum_duration, timedelta) or minimum_duration <= timedelta(0):
            raise ValueError("Persistence minimum duration must be positive.")
        measurements: list[BookChange] = []
        for (side, price), first_seen in sorted(self._first_seen.items(), key=lambda item: (item[0][0].value, item[0][1])):
            seen_utc = datetime.fromisoformat(normalized_utc(first_seen, "Depth first-seen time").replace("Z", "+00:00"))
            if as_of_utc - seen_utc >= minimum_duration:
                quantity = (self._bids if side is BookSide.BID else self._asks)[price]
                measurements.append(BookChange(side, price, quantity, quantity, LiquidityBehavior.PERSIST, self._latest_event_id or "snapshot"))
        return tuple(measurements)


@dataclass(frozen=True)
class TradeFlowMeasurements:
    buy_volume: int
    sell_volume: int
    unknown_volume: int
    total_volume: int
    signed_volume: int | None
    cumulative_delta: int | None
    complete: bool
    trade_count: int


class TradeFlowAccumulator:
    """Mechanical trade-flow totals.  A sequence gap makes signed totals incomplete."""

    def __init__(self) -> None:
        self.reset()

    def reset(self) -> None:
        self._buy = 0
        self._sell = 0
        self._unknown = 0
        self._count = 0
        self._complete = True

    def mark_gap(self) -> None:
        self._complete = False

    def ingest(self, trade: TradeEvent) -> TradeFlowMeasurements:
        self._count += 1
        if trade.aggressor_side is AggressorSide.BUY:
            self._buy += trade.size
        elif trade.aggressor_side is AggressorSide.SELL:
            self._sell += trade.size
        else:
            self._unknown += trade.size
            self._complete = False
        return self.measurements()

    def measurements(self) -> TradeFlowMeasurements:
        signed = self._buy - self._sell if self._complete else None
        return TradeFlowMeasurements(
            buy_volume=self._buy,
            sell_volume=self._sell,
            unknown_volume=self._unknown,
            total_volume=self._buy + self._sell + self._unknown,
            signed_volume=signed,
            cumulative_delta=signed,
            complete=self._complete,
            trade_count=self._count,
        )


@dataclass(frozen=True)
class OHLCBar:
    instrument: MNQContract
    start_time: str
    end_time: str
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int
    trade_count: int
    time_basis: str
    complete: bool

    def __post_init__(self) -> None:
        if type(self.instrument) is not MNQContract:
            raise ValueError("OHLC bar requires concrete MNQ contract identity.")
        start = normalized_utc(self.start_time, "Bar start time")
        end = normalized_utc(self.end_time, "Bar end time")
        if end <= start:
            raise ValueError("Bar end must be after start.")
        high = _price(self.high, "Bar high")
        low = _price(self.low, "Bar low")
        open_ = _price(self.open, "Bar open")
        close = _price(self.close, "Bar close")
        if not low <= min(open_, close) <= high or not low <= max(open_, close) <= high:
            raise ValueError("OHLC bar prices are inconsistent.")
        _quantity(self.volume, "Bar volume")
        _quantity(self.trade_count, "Bar trade count")
        if self.time_basis not in {"exchange_time", "provider_time", "local_receipt_time"}:
            raise ValueError("OHLC bar must disclose its time basis.")


class BarAccumulator:
    """Fixed UTC interval bars from trade observations, with disclosed clock fallback."""

    def __init__(self, instrument: MNQContract, interval: timedelta) -> None:
        if not isinstance(interval, timedelta) or interval <= timedelta(0) or interval.total_seconds() % 1:
            raise ValueError("Bar interval must be a positive whole number of seconds.")
        self._instrument = instrument
        self._interval = interval
        self._current: OHLCBar | None = None

    @staticmethod
    def _event_time(trade: TradeEvent) -> tuple[datetime, str]:
        times = trade.header.timestamps
        value = times.exchange_time or times.provider_time or times.local_receipt_time
        basis = "exchange_time" if times.exchange_time else "provider_time" if times.provider_time else "local_receipt_time"
        return datetime.fromisoformat(normalized_utc(value, "Trade bar time").replace("Z", "+00:00")), basis

    def ingest(self, trade: TradeEvent) -> tuple[OHLCBar, ...]:
        if trade.header.instrument != self._instrument:
            raise MarketDataRefused("A bar accumulator cannot mix concrete MNQ contracts.")
        occurred_at, basis = self._event_time(trade)
        seconds = int(self._interval.total_seconds())
        epoch = int(occurred_at.timestamp())
        start = datetime.fromtimestamp(epoch - epoch % seconds, timezone.utc)
        end = start + self._interval
        if self._current is None:
            self._current = self._new_bar(trade, start, end, basis)
            return ()
        if start < datetime.fromisoformat(self._current.start_time.replace("Z", "+00:00")):
            self._current = OHLCBar(**{**self._current.__dict__, "complete": False})
            return ()
        if start == datetime.fromisoformat(self._current.start_time.replace("Z", "+00:00")):
            self._current = OHLCBar(
                self._instrument, self._current.start_time, self._current.end_time, self._current.open,
                max(self._current.high, trade.price), min(self._current.low, trade.price), trade.price,
                self._current.volume + trade.size, self._current.trade_count + 1, self._current.time_basis,
                self._current.complete and self._current.time_basis == basis,
            )
            return ()
        completed = self._current
        self._current = self._new_bar(trade, start, end, basis)
        return (completed,)

    def _new_bar(self, trade: TradeEvent, start: datetime, end: datetime, basis: str) -> OHLCBar:
        start_text = start.isoformat().replace("+00:00", "Z")
        end_text = end.isoformat().replace("+00:00", "Z")
        return OHLCBar(self._instrument, start_text, end_text, trade.price, trade.price, trade.price, trade.price, trade.size, 1, basis, True)

    def current(self) -> OHLCBar | None:
        return self._current


@dataclass(frozen=True)
class SessionContext:
    session_id: str
    session_start: str
    session_end: str
    session_open: Decimal | None
    session_high: Decimal | None
    session_low: Decimal | None
    vwap: Decimal | None
    total_volume: int
    prior_session_high: Decimal | None
    prior_session_low: Decimal | None
    prior_settlement: Decimal | None
    overnight_high: Decimal | None
    overnight_low: Decimal | None
    cash_open_price: Decimal | None
    opening_range_high: Decimal | None
    opening_range_low: Decimal | None
    time_basis: str
    complete: bool


class SessionContextAccumulator:
    """CME-session measurements using America/Chicago boundaries and DST-aware dates."""

    def __init__(self, instrument: MNQContract, *, exchange_timezone: str = "America/Chicago", session_start: time = time(17), cash_open: time = time(8, 30), opening_range: timedelta = timedelta(minutes=30)) -> None:
        self._instrument = instrument
        try:
            self._timezone: tzinfo = ZoneInfo(exchange_timezone)
        except ZoneInfoNotFoundError as exc:
            if exchange_timezone != "America/Chicago":
                raise MarketDataRefused("The requested IANA exchange timezone is unavailable in this environment.") from exc
            self._timezone = _USCentralFallback()
        self._session_start = session_start
        self._cash_open = cash_open
        self._opening_range = opening_range
        self._prior: SessionContext | None = None
        self._reset(None)

    def _reset(self, identity: tuple[datetime, datetime, str] | None) -> None:
        self._identity = identity
        self._open: Decimal | None = None
        self._high: Decimal | None = None
        self._low: Decimal | None = None
        self._price_volume = Decimal(0)
        self._volume = 0
        self._overnight_high: Decimal | None = None
        self._overnight_low: Decimal | None = None
        self._cash_open_price: Decimal | None = None
        self._opening_high: Decimal | None = None
        self._opening_low: Decimal | None = None
        self._time_basis: str | None = None
        self._complete = True

    def _identity_for(self, utc_time: str) -> tuple[datetime, datetime, str]:
        local = datetime.fromisoformat(normalized_utc(utc_time, "Session event time").replace("Z", "+00:00")).astimezone(self._timezone)
        start_date = local.date() if local.timetz().replace(tzinfo=None) >= self._session_start else (local - timedelta(days=1)).date()
        start = datetime.combine(start_date, self._session_start, self._timezone)
        end = start + timedelta(days=1)
        return start, end, f"CME-{start.date().isoformat()}"

    def mark_incomplete(self) -> None:
        self._complete = False

    def ingest(self, trade: TradeEvent) -> SessionContext:
        if trade.header.instrument != self._instrument:
            raise MarketDataRefused("A session accumulator cannot mix concrete MNQ contracts.")
        event_time = trade.header.timestamps.exchange_time or trade.header.timestamps.provider_time or trade.header.timestamps.local_receipt_time
        identity = self._identity_for(event_time)
        if self._identity is not None and identity[0] < self._identity[0]:
            # A late timestamp must not roll a completed session backward.
            self._complete = False
            return self.snapshot()
        if self._identity is not None and identity[2] != self._identity[2]:
            self._prior = self.snapshot()
            self._reset(identity)
        elif self._identity is None:
            self._reset(identity)
        local = datetime.fromisoformat(normalized_utc(event_time, "Session event time").replace("Z", "+00:00")).astimezone(self._timezone)
        time_basis = "exchange_time" if trade.header.timestamps.exchange_time else "provider_time" if trade.header.timestamps.provider_time else "local_receipt_time"
        if self._time_basis is None:
            self._time_basis = time_basis
        elif self._time_basis != time_basis:
            self._time_basis = "mixed_source_times"
            self._complete = False
        # The CME trading session starts at 17:00 on the prior local date;
        # its cash open is on the following local date.  Deriving from the
        # session start, rather than the trade's calendar date, survives DST.
        assert self._identity is not None
        cash_date = (self._identity[0] + timedelta(days=1)).date()
        cash_start = datetime.combine(cash_date, self._cash_open, self._timezone)
        if local < cash_start:
            self._overnight_high = trade.price if self._overnight_high is None else max(self._overnight_high, trade.price)
            self._overnight_low = trade.price if self._overnight_low is None else min(self._overnight_low, trade.price)
        elif self._cash_open_price is None:
            self._cash_open_price = trade.price
        if cash_start <= local < cash_start + self._opening_range:
            self._opening_high = trade.price if self._opening_high is None else max(self._opening_high, trade.price)
            self._opening_low = trade.price if self._opening_low is None else min(self._opening_low, trade.price)
        self._open = trade.price if self._open is None else self._open
        self._high = trade.price if self._high is None else max(self._high, trade.price)
        self._low = trade.price if self._low is None else min(self._low, trade.price)
        self._price_volume += trade.price * trade.size
        self._volume += trade.size
        return self.snapshot()

    def snapshot(self) -> SessionContext:
        if self._identity is None:
            raise MarketDataRefused("No session context exists before a trade observation.")
        start, end, session_id = self._identity
        vwap = (self._price_volume / self._volume) if self._volume else None
        return SessionContext(
            session_id=session_id,
            session_start=start.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
            session_end=end.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
            session_open=self._open,
            session_high=self._high,
            session_low=self._low,
            vwap=vwap,
            total_volume=self._volume,
            prior_session_high=self._prior.session_high if self._prior else None,
            prior_session_low=self._prior.session_low if self._prior else None,
            # Settlement is not derivable from the tape alone.  An adapter may
            # add it later through an explicit authoritative context record.
            prior_settlement=None,
            overnight_high=self._overnight_high,
            overnight_low=self._overnight_low,
            cash_open_price=self._cash_open_price,
            opening_range_high=self._opening_high,
            opening_range_low=self._opening_low,
            time_basis=self._time_basis or "unavailable",
            complete=self._complete,
        )


@dataclass(frozen=True)
class PipelineResult:
    event_id: str
    ordering: OrderingOutcome | None
    book_application: BookApplication | None
    trade_flow: TradeFlowMeasurements | None
    session_context: SessionContext | None
    completed_bars: tuple[OHLCBar, ...]


@dataclass(frozen=True)
class PipelineMetrics:
    """Operational counters; they are observability, never strategy inputs."""

    events_processed: int
    events_rejected: int
    events_duplicated: int
    sequence_gaps: int
    latest_book_quality: DataQuality
    buffer_pressure: float | None


class MarketDataPipeline:
    """Synchronous shared normalization/reconstruction path for live adapters and replay."""

    def __init__(self, source: MarketDataSource, instrument: MNQContract, *, bar_interval: timedelta = timedelta(minutes=1)) -> None:
        self.source = source
        self.instrument = instrument
        self.book = OrderBookReconstructor(source, instrument)
        self.trade_flow = TradeFlowAccumulator()
        self.session = SessionContextAccumulator(instrument)
        self.bars = BarAccumulator(instrument, bar_interval)
        self._sequence_guards = {MarketStream.TRADE: SequenceGuard(), MarketStream.QUOTE: SequenceGuard()}
        self._flow_session_id: str | None = None
        self.latest_quote: QuoteEvent | None = None
        self.latest_quote_quality = DataQuality.INCOMPLETE
        self.latest_trade: TradeEvent | None = None
        self.latest_trade_quality = DataQuality.INCOMPLETE
        self._events_processed = 0
        self._events_rejected = 0
        self._events_duplicated = 0
        self._sequence_gaps = 0

    def notify_reconnect(self) -> None:
        self.book.notify_reconnect()
        for guard in self._sequence_guards.values():
            guard.reset_for_reconnect()
        self.latest_quote_quality = DataQuality.RECOVERING
        self.latest_trade_quality = DataQuality.RECOVERING
        self.trade_flow.mark_gap()
        self.session.mark_incomplete()

    def _assert_scope(self, event: CanonicalMarketEvent) -> None:
        if event.header.source != self.source or event.header.instrument != self.instrument:
            self._events_rejected += 1
            raise MarketDataRefused("A pipeline accepts exactly one source and concrete MNQ contract.")

    def note_rejected_provider_event(self) -> None:
        """An adapter records a refused raw packet without converting it into state."""
        self._events_rejected += 1

    def metrics(self, buffer: "BoundedMarketDataBuffer | None" = None) -> PipelineMetrics:
        if buffer is not None and type(buffer) is not BoundedMarketDataBuffer:
            raise ValueError("Pipeline metrics require a BoundedMarketDataBuffer or None.")
        return PipelineMetrics(
            events_processed=self._events_processed,
            events_rejected=self._events_rejected,
            events_duplicated=self._events_duplicated,
            sequence_gaps=self._sequence_gaps,
            latest_book_quality=self.book._state().quality,
            buffer_pressure=buffer.pressure if buffer is not None else None,
        )

    def apply(self, event: CanonicalMarketEvent) -> PipelineResult:
        self._assert_scope(event)
        self._events_processed += 1
        if isinstance(event, (BookSnapshotEvent, BookDeltaEvent)):
            application = self.book.apply(event)
            if application.outcome is BookApplyOutcome.DUPLICATE:
                self._events_duplicated += 1
            elif application.outcome is BookApplyOutcome.GAP:
                self._sequence_gaps += 1
            elif application.outcome in {BookApplyOutcome.RECOVERY_REQUIRED, BookApplyOutcome.SOURCE_MISMATCH}:
                self._events_rejected += 1
            return PipelineResult(event.header.event_id, None, application, None, None, ())
        if isinstance(event, QuoteEvent):
            ordering = self._sequence_guards[MarketStream.QUOTE].assess(event.header.provider_sequence).outcome
            if ordering is OrderingOutcome.DUPLICATE:
                self._events_duplicated += 1
            elif ordering is OrderingOutcome.GAP:
                self._sequence_gaps += 1
            if ordering not in {OrderingOutcome.DUPLICATE, OrderingOutcome.LATE}:
                self.latest_quote = event
            self.latest_quote_quality = DataQuality.HEALTHY if ordering is OrderingOutcome.ACCEPTED else DataQuality.GAPPED if ordering is OrderingOutcome.GAP else DataQuality.INCOMPLETE if ordering is OrderingOutcome.UNSEQUENCED else self.latest_quote_quality
            return PipelineResult(event.header.event_id, ordering, None, None, None, ())
        if isinstance(event, TradeEvent):
            ordering = self._sequence_guards[MarketStream.TRADE].assess(event.header.provider_sequence).outcome
            if ordering is OrderingOutcome.DUPLICATE:
                self._events_duplicated += 1
            elif ordering is OrderingOutcome.GAP:
                self._sequence_gaps += 1
            flow_incomplete = ordering in {OrderingOutcome.GAP, OrderingOutcome.UNSEQUENCED}
            if flow_incomplete:
                self.session.mark_incomplete()
            if ordering not in {OrderingOutcome.DUPLICATE, OrderingOutcome.LATE}:
                self.latest_trade = event
                session = self.session.ingest(event)
                if self._flow_session_id != session.session_id:
                    self.trade_flow.reset()
                    self._flow_session_id = session.session_id
                if flow_incomplete:
                    self.trade_flow.mark_gap()
                flow = self.trade_flow.ingest(event)
                completed = self.bars.ingest(event)
            else:
                flow = self.trade_flow.measurements()
                session = self.session.snapshot() if self.latest_trade else None
                completed = ()
            self.latest_trade_quality = DataQuality.HEALTHY if ordering is OrderingOutcome.ACCEPTED else DataQuality.GAPPED if ordering is OrderingOutcome.GAP else DataQuality.INCOMPLETE if ordering is OrderingOutcome.UNSEQUENCED else self.latest_trade_quality
            return PipelineResult(event.header.event_id, ordering, None, flow, session, completed)
        return PipelineResult(event.header.event_id, None, None, None, None, ())

    def staleness(self, as_of: str, *, trade_maximum_age: timedelta, quote_maximum_age: timedelta, book_maximum_age: timedelta) -> dict[str, DataQuality]:
        """Caller-selected lifetimes; L3-B intentionally sets no strategy thresholds."""
        now = datetime.fromisoformat(normalized_utc(as_of, "Staleness assessment time").replace("Z", "+00:00"))
        for maximum in (trade_maximum_age, quote_maximum_age, book_maximum_age):
            if not isinstance(maximum, timedelta) or maximum <= timedelta(0):
                raise ValueError("Staleness maximum age must be positive.")
        def age_quality(event: TradeEvent | QuoteEvent | None, quality: DataQuality, maximum: timedelta) -> DataQuality:
            if event is None:
                return DataQuality.INCOMPLETE
            received = datetime.fromisoformat(normalized_utc(event.header.timestamps.local_receipt_time, "Receipt time").replace("Z", "+00:00"))
            return DataQuality.STALE if quality is DataQuality.HEALTHY and now - received > maximum else quality
        self.book.mark_stale(as_of, book_maximum_age)
        return {
            "trade": age_quality(self.latest_trade, self.latest_trade_quality, trade_maximum_age),
            "quote": age_quality(self.latest_quote, self.latest_quote_quality, quote_maximum_age),
            "book": self.book._state().quality,
            "context": DataQuality.HEALTHY if self.latest_trade and self.session.snapshot().complete else DataQuality.INCOMPLETE,
        }


class MarketDataProviderAdapter(Protocol):
    """Provider-specific code has this narrow one-way normalization boundary."""

    @property
    def source(self) -> MarketDataSource: ...

    def normalize(self, raw_event: RawProviderEvent) -> tuple[CanonicalMarketEvent, ...]: ...


class DerivativesProviderAdapter(Protocol):
    @property
    def source(self) -> MarketDataSource: ...

    def normalize_derivatives(self, raw_event: RawProviderEvent) -> tuple[DerivativesContextEvent, ...]: ...


class BoundedMarketDataBuffer:
    """Explicit bounded buffering: full capacity raises and never discards silently."""

    def __init__(self, capacity: int) -> None:
        if type(capacity) is not int or capacity <= 0:
            raise ValueError("Buffer capacity must be a positive integer.")
        self._capacity = capacity
        self._events: deque[CanonicalMarketEvent] = deque()
        self.quality = DataQuality.HEALTHY
        self.rejected = 0

    @property
    def pressure(self) -> float:
        return len(self._events) / self._capacity

    def publish(self, event: CanonicalMarketEvent) -> None:
        if len(self._events) >= self._capacity:
            self.rejected += 1
            self.quality = DataQuality.INVALID
            raise BackpressureRefused("Market-data buffer is full; no event was silently discarded.")
        self._events.append(event)

    def drain(self) -> tuple[CanonicalMarketEvent, ...]:
        values = tuple(self._events)
        self._events.clear()
        return values
