"""L3-F2 NinjaTrader Desktop observation boundary.

The bridge accepts only newline-delimited observation records sent by a local
NinjaTrader AddOn.  It binds only ``127.0.0.1`` and has no write, command,
request, execution, or account-control operation.  It is a one-way ingress
boundary, not a NinjaTrader automation API.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from enum import StrEnum
import json
import socket
from types import MappingProxyType
from typing import Iterable, Mapping

from src.lane_iii.contracts import LaneIIIInstrument, canonical_hash, normalized_utc
from src.lane_iii.market_data import (
    BookLevel, BookSnapshotEvent, EventHeader, EventTimestamps, MNQContract,
    MarketDataProviderAdapter, MarketDataSource, MarketStream, QuoteEvent,
    RawProviderEvent, TradeEvent, AggressorSide, AggressorProvenance,
)
from .tradovate_observation import (
    AccountObservation, ObservationTruth, OrderObservation, PositionObservation,
    ProviderErrorCode, ProviderHealth, ProviderHealthTracker,
    ProviderObservationRefused, ProviderOrderStatus, ProviderPositionDirection,
    ProviderStream, ReconciliationResult, StreamHealth, reconcile_provider_truth,
)


L3F2_SCHEMA = "lane-iii-phase-f2-ninjatrader-observation-v1"
NINJATRADER_SOURCE = "NINJATRADER"
LUCID_CQG_PROVIDER = "LUCID_CQG"
PROP_SIM_ENVIRONMENT = "PROP_SIM"
LOOPBACK_HOST = "127.0.0.1"
_TYPES = {"CONNECTION", "INSTRUMENT", "TRADE", "QUOTE", "DEPTH", "ACCOUNT", "POSITION", "ORDER", "EXECUTION", "HEALTH", "SNAPSHOT_COMPLETE"}
_WIRE_FIELDS = frozenset({
    "schema", "observation_id", "session_id", "observation_type",
    "ninja_receipt_time", "local_monotonic_sequence", "provider_timestamp",
    "provider_sequence", "exchange_timestamp", "account", "payload",
})
_ACCOUNT_ALIASES = frozenset({"Lucid25kflex01", "Sim101"})
_ACCOUNT_SCOPED_TYPES = frozenset({"ACCOUNT", "POSITION", "ORDER", "EXECUTION", "SNAPSHOT_COMPLETE"})


class NinjaTraderObservationError(ProviderObservationRefused):
    """A NinjaTrader observation cannot safely become Lane III truth."""


class NinjaTraderProvider(StrEnum):
    TRADOVATE_DIRECT = "TRADOVATE_DIRECT"
    NINJATRADER_DESKTOP = "NINJATRADER_DESKTOP"


class AccountClass(StrEnum):
    PROVIDER_EVALUATION = "PROVIDER_EVALUATION"
    LOCAL_SIMULATION = "LOCAL_SIMULATION"


class NinjaTraderHealthStream(StrEnum):
    NINJATRADER_PROCESS = "NINJATRADER_PROCESS"
    LUCID_CONNECTION = "LUCID_CONNECTION"
    MARKET_DATA_STREAM = "MARKET_DATA_STREAM"
    DEPTH_STREAM = "DEPTH_STREAM"
    ACCOUNT_STREAM = "ACCOUNT_STREAM"
    POSITION_STREAM = "POSITION_STREAM"
    ORDER_STREAM = "ORDER_STREAM"
    LOCAL_BRIDGE = "LOCAL_BRIDGE"


@dataclass(frozen=True)
class NinjaTraderProviderHealth:
    streams: Mapping[NinjaTraderHealthStream, StreamHealth]
    updated_at: Mapping[NinjaTraderHealthStream, str | None]

    @property
    def authoritative(self) -> bool:
        required = {NinjaTraderHealthStream.NINJATRADER_PROCESS, NinjaTraderHealthStream.LUCID_CONNECTION, NinjaTraderHealthStream.MARKET_DATA_STREAM, NinjaTraderHealthStream.DEPTH_STREAM, NinjaTraderHealthStream.ACCOUNT_STREAM, NinjaTraderHealthStream.POSITION_STREAM, NinjaTraderHealthStream.ORDER_STREAM, NinjaTraderHealthStream.LOCAL_BRIDGE}
        return all(self.streams.get(stream) is StreamHealth.HEALTHY for stream in required)


class NinjaTraderHealthTracker:
    def __init__(self) -> None:
        self._states = {stream: StreamHealth.UNKNOWN for stream in NinjaTraderHealthStream}
        self._times: dict[NinjaTraderHealthStream, str | None] = {stream: None for stream in NinjaTraderHealthStream}

    def mark(self, stream: NinjaTraderHealthStream, state: StreamHealth, at: str) -> NinjaTraderProviderHealth:
        if type(stream) is not NinjaTraderHealthStream or type(state) is not StreamHealth:
            raise ValueError("NinjaTrader health state must be explicit.")
        self._states[stream], self._times[stream] = state, normalized_utc(at, "NinjaTrader health time")
        return self.snapshot()

    def assess_stale(self, at: str, maximum_age: timedelta) -> NinjaTraderProviderHealth:
        now = datetime.fromisoformat(normalized_utc(at, "NinjaTrader health check").replace("Z", "+00:00"))
        for stream, previous in self._times.items():
            if self._states[stream] is StreamHealth.HEALTHY and previous is not None and now - datetime.fromisoformat(previous.replace("Z", "+00:00")) > maximum_age:
                self._states[stream] = StreamHealth.STALE
        return self.snapshot()

    def snapshot(self) -> NinjaTraderProviderHealth:
        return NinjaTraderProviderHealth(MappingProxyType(dict(self._states)), MappingProxyType(dict(self._times)))


def _text(value: object, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} is required.")
    return value.strip()


def _decimal(value: object, name: str, *, positive: bool = False) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{name} must be finite.") from exc
    if not result.is_finite() or (positive and result <= 0):
        raise ValueError(f"{name} is invalid.")
    return result


def _quantity(value: object, name: str, *, zero: bool = False) -> int:
    if type(value) is not int or value < 0 or (not zero and value == 0):
        raise ValueError(f"{name} must be a {'non-negative' if zero else 'positive'} integer.")
    return value


def _mapping(value: object, name: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise NinjaTraderObservationError(ProviderErrorCode.MALFORMED_PROVIDER_PAYLOAD, name)
    return value


def _wire_integer(value: object, name: str) -> int:
    """Reject coercion at the transport boundary (including JSON booleans)."""
    if type(value) is not int or value < 0:
        raise ValueError(f"{name} must be a non-negative integer.")
    return value


def _wire_json_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    """Do not let duplicate JSON keys silently alter an observation."""
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate bridge JSON key")
        result[key] = value
    return result


def _contains_prohibited_identity(value: object) -> bool:
    forbidden = {"account_id", "accountid", "password", "token", "secret", "authorization"}
    if isinstance(value, Mapping):
        return any(str(key).replace("-", "_").lower() in forbidden or _contains_prohibited_identity(item) for key, item in value.items())
    if isinstance(value, list):
        return any(_contains_prohibited_identity(item) for item in value)
    return False


@dataclass(frozen=True)
class NinjaTraderContract:
    master_instrument: str
    native_full_name: str
    expiration: str
    exchange: str
    tick_size: Decimal
    internal_contract_id: str
    point_value: Decimal | None = None

    def __post_init__(self) -> None:
        if self.master_instrument != "MNQ" or self.exchange != "CME" or self.expiration != "2026-09":
            raise NinjaTraderObservationError(ProviderErrorCode.CONTRACT_NOT_FOUND, "mnq_sep26_required")
        _text(self.native_full_name, "Native contract name")
        _text(self.internal_contract_id, "NinjaTrader contract identity")
        if _decimal(self.tick_size, "Tick size", positive=True) != Decimal("0.25"):
            raise NinjaTraderObservationError(ProviderErrorCode.CONTRACT_NOT_FOUND, "mnq_tick_size")
        if self.point_value is not None:
            _decimal(self.point_value, "Point value", positive=True)

    @property
    def canonical(self) -> MNQContract:
        return MNQContract("MNQU6", "2026-09", "CME", LaneIIIInstrument.MNQ)


@dataclass(frozen=True)
class NinjaTraderObservation:
    """One safe bridge record.  No account ID or credential field exists."""

    observation_id: str
    session_id: str
    observation_type: str
    ninja_receipt_time: str
    local_monotonic_sequence: int
    payload: Mapping[str, object]
    account_alias: str | None = None
    account_class: AccountClass | None = None
    provider_timestamp: str | None = None
    provider_sequence: int | None = None
    exchange_timestamp: str | None = None
    source: str = NINJATRADER_SOURCE
    provider: str = LUCID_CQG_PROVIDER
    environment: str = PROP_SIM_ENVIRONMENT

    def __post_init__(self) -> None:
        _text(self.observation_id, "Observation ID")
        _text(self.session_id, "NinjaTrader session ID")
        if self.observation_type not in _TYPES:
            raise NinjaTraderObservationError(ProviderErrorCode.MALFORMED_PROVIDER_PAYLOAD, "observation_type")
        normalized_utc(self.ninja_receipt_time, "NinjaTrader receipt timestamp")
        if type(self.local_monotonic_sequence) is not int or self.local_monotonic_sequence < 0:
            raise ValueError("Local ordering sequence must be non-negative.")
        if not isinstance(self.payload, Mapping):
            raise ValueError("Observation payload must be a mapping.")
        object.__setattr__(self, "payload", MappingProxyType(dict(self.payload)))
        if (self.source, self.provider, self.environment) != (NINJATRADER_SOURCE, LUCID_CQG_PROVIDER, PROP_SIM_ENVIRONMENT):
            raise NinjaTraderObservationError(ProviderErrorCode.ENVIRONMENT_MISMATCH, "ninjatrader_provenance")
        if self.account_alias is None:
            if self.account_class is not None:
                raise ValueError("Account class requires account alias.")
        elif self.account_class is None:
            raise ValueError("Account alias requires explicit account class.")
        elif self.account_alias not in _ACCOUNT_ALIASES:
            raise NinjaTraderObservationError(ProviderErrorCode.ACCOUNT_NOT_FOUND, "unrecognized_account_alias")
        if self.observation_type in _ACCOUNT_SCOPED_TYPES and self.account_alias is None:
            raise NinjaTraderObservationError(ProviderErrorCode.ACCOUNT_NOT_FOUND, "account_provenance_required")
        if self.provider_timestamp is not None:
            normalized_utc(self.provider_timestamp, "Provider timestamp")
        if self.exchange_timestamp is not None:
            normalized_utc(self.exchange_timestamp, "Exchange timestamp")
        if self.provider_sequence is not None and (type(self.provider_sequence) is not int or self.provider_sequence < 0):
            raise ValueError("Provider sequence must be non-negative when exposed.")

    @classmethod
    def from_wire(cls, text: str) -> "NinjaTraderObservation":
        try:
            raw = json.loads(text, object_pairs_hook=_wire_json_object)
        except (json.JSONDecodeError, ValueError):
            raise NinjaTraderObservationError(ProviderErrorCode.MALFORMED_PROVIDER_PAYLOAD, "bridge_json") from None
        value = _mapping(raw, "bridge_record")
        if value.get("schema") != L3F2_SCHEMA:
            raise NinjaTraderObservationError(ProviderErrorCode.MALFORMED_PROVIDER_PAYLOAD, "bridge_schema")
        if set(value) != _WIRE_FIELDS:
            raise NinjaTraderObservationError(ProviderErrorCode.MALFORMED_PROVIDER_PAYLOAD, "bridge_record_shape")
        if _contains_prohibited_identity(value):
            raise NinjaTraderObservationError(ProviderErrorCode.MALFORMED_PROVIDER_PAYLOAD, "secret_or_provider_account_identifier")
        aliases = value.get("account")
        account = _mapping(aliases, "account") if aliases is not None else None
        try:
            return cls(
                observation_id=str(value["observation_id"]), session_id=str(value["session_id"]),
                observation_type=str(value["observation_type"]), ninja_receipt_time=str(value["ninja_receipt_time"]),
                local_monotonic_sequence=_wire_integer(value["local_monotonic_sequence"], "Local ordering sequence"), payload=_mapping(value["payload"], "payload"),
                account_alias=None if account is None else str(account["alias"]),
                account_class=None if account is None else AccountClass(str(account["class"])),
                provider_timestamp=None if value.get("provider_timestamp") is None else str(value["provider_timestamp"]),
                provider_sequence=None if value.get("provider_sequence") is None else _wire_integer(value["provider_sequence"], "Provider sequence"),
                exchange_timestamp=None if value.get("exchange_timestamp") is None else str(value["exchange_timestamp"]),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise NinjaTraderObservationError(ProviderErrorCode.MALFORMED_PROVIDER_PAYLOAD, "bridge_record_fields") from exc

    def wire_hash(self) -> str:
        return canonical_hash({"schema": L3F2_SCHEMA, "observation_id": self.observation_id, "session_id": self.session_id, "observation_type": self.observation_type, "ninja_receipt_time": self.ninja_receipt_time, "local_monotonic_sequence": self.local_monotonic_sequence, "payload": dict(self.payload), "account_alias": self.account_alias, "account_class": None if self.account_class is None else self.account_class.value, "provider_timestamp": self.provider_timestamp, "provider_sequence": self.provider_sequence, "exchange_timestamp": self.exchange_timestamp})


@dataclass(frozen=True)
class LoopbackBridgeConfig:
    host: str = LOOPBACK_HOST
    port: int = 48135
    maximum_frame_bytes: int = 65536

    def __post_init__(self) -> None:
        if self.host != LOOPBACK_HOST:
            raise NinjaTraderObservationError(ProviderErrorCode.ENVIRONMENT_MISMATCH, "loopback_only")
        if type(self.port) is not int or not 1024 <= self.port <= 65535:
            raise ValueError("Bridge port must be a non-privileged TCP port.")
        if type(self.maximum_frame_bytes) is not int or not 1024 <= self.maximum_frame_bytes <= 1048576:
            raise ValueError("Maximum bridge frame size is invalid.")


class NinjaTraderSessionLedger:
    """Reject duplicate and prior-session records; nothing writes back to NT."""
    _MARKET_TYPES = frozenset({"TRADE", "QUOTE", "DEPTH"})
    _ACCOUNT_TYPES = frozenset({"ACCOUNT", "POSITION", "ORDER", "EXECUTION"})

    def __init__(self) -> None:
        self._current_session_ids: dict[str, str] = {}
        self._last_sequences: dict[str, int] = {}
        self._seen: dict[str, set[str]] = {}
        self._retired_sessions: dict[str, set[str]] = {}

    @classmethod
    def _channel(cls, observation: NinjaTraderObservation) -> str:
        # NinjaTrader can retain a chart indicator across a Custom-project
        # recompile while the AddOn has already reloaded. Their independent
        # read-only session IDs must not retire each other.
        if observation.observation_type in cls._MARKET_TYPES:
            return "MARKET_DATA"
        if observation.observation_type in cls._ACCOUNT_TYPES:
            return "ACCOUNT_STATE"
        return "CONTROL"

    def admit(self, observation: NinjaTraderObservation) -> bool:
        channel = self._channel(observation)
        current_session_id = self._current_session_ids.get(channel)
        if current_session_id is None:
            self._current_session_ids[channel] = observation.session_id
            self._last_sequences[channel] = -1
            self._seen[channel] = set()
            self._retired_sessions[channel] = set()
        elif observation.session_id != current_session_id:
            # A new session on the same semantic channel begins a fresh
            # ordered stream. A prior session must never overwrite it.
            retired = self._retired_sessions[channel]
            if observation.session_id in retired:
                raise NinjaTraderObservationError(ProviderErrorCode.STALE_PROVIDER_STATE, "prior_session_callback")
            retired.add(current_session_id)
            self._current_session_ids[channel] = observation.session_id
            self._last_sequences[channel] = -1
            self._seen[channel] = set()
        identity = observation.wire_hash()
        seen = self._seen[channel]
        if identity in seen:
            return False
        if observation.local_monotonic_sequence <= self._last_sequences[channel]:
            raise NinjaTraderObservationError(ProviderErrorCode.STALE_PROVIDER_STATE, "out_of_order_or_prior_session")
        seen.add(identity)
        self._last_sequences[channel] = observation.local_monotonic_sequence
        return True


class NinjaTraderMarketDataAdapter(MarketDataProviderAdapter):
    def __init__(self, contract: NinjaTraderContract) -> None:
        self.contract = contract
        self._source = MarketDataSource(NINJATRADER_SOURCE, LUCID_CQG_PROVIDER)
        # A quote-derived aggressor may reference only the immediately
        # preceding quote emitted from the same NinjaTrader Last callback.
        # Retaining one value is sufficient and prevents retrospective lookup.
        self._latest_quote_observation_id: str | None = None
        self._latest_quote: QuoteEvent | None = None

    @property
    def source(self) -> MarketDataSource:
        return self._source

    def normalize(self, raw_event: RawProviderEvent) -> tuple[QuoteEvent | TradeEvent | BookSnapshotEvent, ...]:
        packet = _mapping(raw_event.payload, "ninjatrader_packet")
        record = NinjaTraderObservation.from_wire(json.dumps(dict(packet), sort_keys=True, separators=(",", ":")))
        if record.observation_type == "QUOTE":
            return (self._quote(raw_event, record),)
        if record.observation_type == "TRADE":
            return (self._trade(raw_event, record),)
        if record.observation_type == "DEPTH":
            return (self._depth(raw_event, record),)
        raise NinjaTraderObservationError(ProviderErrorCode.MARKET_DATA_UNAVAILABLE, "non_market_record")

    def _header(self, raw: RawProviderEvent, record: NinjaTraderObservation, stream: MarketStream) -> EventHeader:
        provider_time = record.provider_timestamp
        return EventHeader("l3f2-" + canonical_hash({"wire": record.wire_hash(), "stream": stream.value})[:32], self.source, self.contract.canonical, EventTimestamps(raw.received_at, exchange_time=record.exchange_timestamp, provider_time=provider_time), stream, record.observation_id, raw.payload_hash, record.provider_sequence, None)

    def _contract_check(self, record: NinjaTraderObservation) -> None:
        value = record.payload.get("contract_id")
        if value != self.contract.internal_contract_id:
            raise NinjaTraderObservationError(ProviderErrorCode.CONTRACT_NOT_FOUND, "ninjatrader_contract")

    def _quote(self, raw: RawProviderEvent, record: NinjaTraderObservation) -> QuoteEvent:
        self._contract_check(record)
        try:
            quote = QuoteEvent(self._header(raw, record, MarketStream.QUOTE), _decimal(record.payload["bid"], "Bid", positive=True), _decimal(record.payload["ask"], "Ask", positive=True), _quantity(record.payload["bid_size"], "Bid size"), _quantity(record.payload["ask_size"], "Ask size"))
            self._latest_quote_observation_id = record.observation_id
            self._latest_quote = quote
            return quote
        except (KeyError, ValueError) as exc:
            raise NinjaTraderObservationError(ProviderErrorCode.MALFORMED_PROVIDER_PAYLOAD, "quote") from exc

    def _trade(self, raw: RawProviderEvent, record: NinjaTraderObservation) -> TradeEvent:
        self._contract_check(record)
        try:
            header = self._header(raw, record, MarketStream.TRADE)
            price = _decimal(record.payload["price"], "Trade price", positive=True)
            size = _quantity(record.payload["size"], "Trade size")
            source = str(record.payload.get("aggressor_source", "UNKNOWN"))
            if source == "PROVIDER_NATIVE":
                side_name = str(record.payload.get("aggressor_side", "UNKNOWN"))
                side = AggressorSide(side_name) if side_name in {"BUY", "SELL"} else AggressorSide.UNKNOWN
                return TradeEvent(
                    header, price, size, side,
                    AggressorProvenance.PROVIDER if side is not AggressorSide.UNKNOWN else AggressorProvenance.UNAVAILABLE,
                )
            if source == "BID_ASK_CLASSIFICATION":
                quote_observation_id = record.payload.get("derivation_quote_observation_id")
                quote = self._latest_quote
                if (
                    isinstance(quote_observation_id, str)
                    and quote_observation_id == self._latest_quote_observation_id
                    and quote is not None
                    and quote.header.timestamps.ordering_time == header.timestamps.ordering_time
                ):
                    try:
                        bid = _decimal(record.payload["bid_at_trade"], "Bid at trade", positive=True)
                        ask = _decimal(record.payload["ask_at_trade"], "Ask at trade", positive=True)
                    except (KeyError, ValueError):
                        return TradeEvent(header, price, size)
                    if bid == quote.bid_price and ask == quote.ask_price and bid < ask:
                        side = AggressorSide.BUY if price >= ask else AggressorSide.SELL if price <= bid else AggressorSide.UNKNOWN
                        if side is not AggressorSide.UNKNOWN:
                            return TradeEvent(
                                header, price, size, side, AggressorProvenance.QUOTE_DERIVED,
                                quote.header.event_id,
                            )
            return TradeEvent(header, price, size)
        except (KeyError, ValueError) as exc:
            raise NinjaTraderObservationError(ProviderErrorCode.MALFORMED_PROVIDER_PAYLOAD, "trade") from exc

    def _depth(self, raw: RawProviderEvent, record: NinjaTraderObservation) -> BookSnapshotEvent:
        self._contract_check(record)
        try:
            self._validate_depth_mutation(record.payload)
            bids = self._levels(record.payload["bids"], True)
            asks = self._levels(record.payload["asks"], False)
            return BookSnapshotEvent(self._header(raw, record, MarketStream.DEPTH), bids, asks)
        except (KeyError, ValueError) as exc:
            raise NinjaTraderObservationError(ProviderErrorCode.MALFORMED_PROVIDER_PAYLOAD, "depth") from exc

    def _validate_depth_mutation(self, payload: Mapping[str, object]) -> None:
        mutation_fields = {
            "operation", "side", "mutation_price", "mutation_volume",
            "mutation_position", "is_reset",
        }
        # Legacy snapshot frames carried only operation/side. They remain
        # snapshots. New mutation provenance is accepted only as a complete,
        # internally valid unit and is still not promoted to a BookDeltaEvent.
        if not any(name in payload for name in mutation_fields - {"operation", "side"}):
            return
        if not mutation_fields.issubset(payload):
            raise ValueError("incomplete_depth_mutation_provenance")
        if payload["operation"] not in {"Add", "Update", "Remove"}:
            raise ValueError("depth_mutation_operation")
        if payload["side"] not in {"Bid", "Ask"}:
            raise ValueError("depth_mutation_side")
        price = _decimal(payload["mutation_price"], "Depth mutation price", positive=True)
        if price % self.contract.tick_size != 0:
            raise ValueError("depth_mutation_tick_alignment")
        volume = _quantity(payload["mutation_volume"], "Depth mutation volume", zero=True)
        position = _wire_integer(payload["mutation_position"], "Depth mutation position")
        if type(payload["is_reset"]) is not bool:
            raise ValueError("depth_mutation_reset")
        rows = payload["bids" if payload["side"] == "Bid" else "asks"]
        if not isinstance(rows, list):
            raise ValueError("depth_mutation_book_side")
        levels = tuple(
            (
                _decimal(_mapping(row, "depth_mutation_level")["price"], "Depth mutation level price", positive=True),
                _quantity(_mapping(row, "depth_mutation_level")["size"], "Depth mutation level size"),
            )
            for row in rows
        )
        matches = tuple(item for item in levels if item[0] == price)
        if payload["operation"] == "Remove":
            if matches:
                raise ValueError("removed_depth_level_still_present")
        elif volume <= 0 or matches != ((price, volume),):
            raise ValueError("depth_upsert_not_reflected_in_snapshot")
        elif position >= len(levels) or levels[position][0] != price:
            raise ValueError("depth_mutation_position_mismatch")

    @staticmethod
    def _levels(value: object, reverse: bool) -> tuple[BookLevel, ...]:
        if not isinstance(value, list):
            raise ValueError("depth_levels")
        levels = tuple(BookLevel(_decimal(_mapping(row, "depth_level")["price"], "Depth price", positive=True), _quantity(_mapping(row, "depth_level")["size"], "Depth size")) for row in value)
        return tuple(sorted(levels, key=lambda level: level.price, reverse=reverse))


class NinjaTraderAccountIsolation:
    """Only the Lucid alias may become authoritative provider truth."""
    def __init__(self, lucid_alias: str = "Lucid25kflex01") -> None:
        self.lucid_alias = _text(lucid_alias, "Lucid account alias")
        self._states: dict[str, tuple[AccountClass, object]] = {}

    def record(self, observation: NinjaTraderObservation, state: object) -> None:
        if observation.account_alias is None or observation.account_class is None:
            raise NinjaTraderObservationError(ProviderErrorCode.ACCOUNT_NOT_FOUND, "account_provenance_required")
        if observation.account_alias == self.lucid_alias and observation.account_class is not AccountClass.PROVIDER_EVALUATION:
            raise NinjaTraderObservationError(ProviderErrorCode.ENVIRONMENT_MISMATCH, "lucid_account_class")
        if observation.account_alias != self.lucid_alias and observation.account_class is AccountClass.PROVIDER_EVALUATION:
            raise NinjaTraderObservationError(ProviderErrorCode.ACCOUNT_AMBIGUOUS, "unexpected_provider_account")
        self._states[observation.account_alias] = (observation.account_class, state)

    def authoritative(self) -> object:
        value = self._states.get(self.lucid_alias)
        if value is None or value[0] is not AccountClass.PROVIDER_EVALUATION:
            raise NinjaTraderObservationError(ProviderErrorCode.ACCOUNT_NOT_FOUND, "lucid_alias_not_bound")
        return value[1]

    def local_simulation(self) -> object | None:
        value = self._states.get("Sim101")
        return None if value is None else value[1]


class LoopbackNinjaTraderBridge:
    """A localhost-only observation receiver. It deliberately has no send API."""
    def __init__(self, config: LoopbackBridgeConfig = LoopbackBridgeConfig()) -> None:
        self.config = config
        self.ledger = NinjaTraderSessionLedger()

    def open_listener(self) -> socket.socket:
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # Windows SO_REUSEADDR permits another process to bind this endpoint,
        # which defeats the single-owner listener invariant. Prefer exclusive
        # ownership there; retain the conventional restart-friendly setting on
        # platforms that do not expose the Windows socket option.
        if hasattr(socket, "SO_EXCLUSIVEADDRUSE"):
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
        else:
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            listener.bind((self.config.host, self.config.port))
            listener.listen(16)
        except Exception:
            listener.close()
            raise
        return listener

    def decode_frame(self, frame: bytes) -> NinjaTraderObservation:
        if not isinstance(frame, bytes) or not frame or len(frame) > self.config.maximum_frame_bytes:
            raise NinjaTraderObservationError(ProviderErrorCode.MALFORMED_PROVIDER_PAYLOAD, "bridge_frame")
        try:
            text = frame.decode("utf-8")
        except UnicodeDecodeError:
            raise NinjaTraderObservationError(ProviderErrorCode.MALFORMED_PROVIDER_PAYLOAD, "bridge_encoding") from None
        return NinjaTraderObservation.from_wire(text.rstrip("\r\n"))

    def accept_observation(self, frame: bytes) -> NinjaTraderObservation | None:
        observation = self.decode_frame(frame)
        return observation if self.ledger.admit(observation) else None
