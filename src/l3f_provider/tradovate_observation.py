"""Lane III Phase F: Tradovate / Lucid read-only observation boundary.

This module intentionally has *no* order, cancel, replace, liquidate, flatten,
or generic-provider-request surface.  It is allowed to authenticate and observe
only.  Any future execution phase must be a new module with an explicit and
separately-reviewed authority grant.

The adapter normalizes a deliberately small, documented subset of Tradovate
payloads into the frozen L3-B canonical event types.  Provider payloads remain
untrusted until all identity, timestamp, price, and contract checks succeed.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, time, timedelta, timezone
from decimal import Decimal, InvalidOperation
from enum import StrEnum
import hashlib
import json
import os
import re
from types import MappingProxyType
from typing import Callable, Iterable, Mapping, Protocol, Sequence

import requests

from src.lane_iii.contracts import LaneIIIInstrument, canonical_hash, normalized_utc
from src.lane_iii.market_data import (
    AggressorProvenance,
    AggressorSide,
    BookLevel,
    BookSnapshotEvent,
    EventHeader,
    EventTimestamps,
    MNQContract,
    MarketDataProviderAdapter,
    MarketDataSource,
    MarketStream,
    QuoteEvent,
    RawProviderEvent,
    TradeEvent,
)


L3F_SCHEMA = "lane-iii-phase-f-tradovate-observation-v1"
L3F_VERSION = "lane-iii-phase-f-v1"
TRADOVATE_PROVIDER = "TRADOVATE"
TRADOVATE_FEED = "CQG_TRADOVATE"
_IDENTIFIER = re.compile(r"[A-Za-z0-9_.:@-]{1,128}")
_MNQ_SYMBOL = re.compile(r"MNQ([FGHJKMNQUVXZ])(\d{1,2})")
_MONTHS = {"F": 1, "G": 2, "H": 3, "J": 4, "K": 5, "M": 6, "N": 7, "Q": 8, "U": 9, "V": 10, "X": 11, "Z": 12}


class TradovateObservationError(RuntimeError):
    """A provider observation cannot safely become Lane III truth."""


class ProviderErrorCode(StrEnum):
    AUTH_FAILED = "AUTH_FAILED"
    TOKEN_EXPIRED = "TOKEN_EXPIRED"
    ACCOUNT_NOT_FOUND = "ACCOUNT_NOT_FOUND"
    ACCOUNT_AMBIGUOUS = "ACCOUNT_AMBIGUOUS"
    MARKET_DATA_UNAVAILABLE = "MARKET_DATA_UNAVAILABLE"
    DOM_UNAVAILABLE = "DOM_UNAVAILABLE"
    CONTRACT_NOT_FOUND = "CONTRACT_NOT_FOUND"
    STALE_PROVIDER_STATE = "STALE_PROVIDER_STATE"
    POSITION_UNKNOWN = "POSITION_UNKNOWN"
    ORDER_STATE_UNKNOWN = "ORDER_STATE_UNKNOWN"
    RECONCILIATION_MISMATCH = "RECONCILIATION_MISMATCH"
    PROVIDER_DISCONNECTED = "PROVIDER_DISCONNECTED"
    ENVIRONMENT_MISMATCH = "ENVIRONMENT_MISMATCH"
    READ_ONLY_VIOLATION = "READ_ONLY_VIOLATION"
    MALFORMED_PROVIDER_PAYLOAD = "MALFORMED_PROVIDER_PAYLOAD"


class ProviderObservationRefused(TradovateObservationError):
    def __init__(self, code: ProviderErrorCode, detail: str = "") -> None:
        self.code = code
        self.detail = detail
        super().__init__(code.value if not detail else f"{code.value}:{detail}")


class CapabilityStatus(StrEnum):
    SUPPORTED = "SUPPORTED"
    UNSUPPORTED = "UNSUPPORTED"
    UNAVAILABLE = "UNAVAILABLE"
    UNKNOWN = "UNKNOWN"


class TradovateEnvironment(StrEnum):
    DEMO = "TRADOVATE_DEMO"
    LIVE = "TRADOVATE_LIVE"


class ObservationMode(StrEnum):
    OBSERVE_ONLY = "OBSERVE_ONLY"


class ProviderStream(StrEnum):
    AUTH = "AUTH"
    MARKET_DATA = "MARKET_DATA"
    USER_DATA = "USER_DATA"


class StreamHealth(StrEnum):
    UNKNOWN = "UNKNOWN"
    CONNECTING = "CONNECTING"
    HEALTHY = "HEALTHY"
    STALE = "STALE"
    DISCONNECTED = "DISCONNECTED"
    AUTH_EXPIRED = "AUTH_EXPIRED"


class ObservationTruth(StrEnum):
    FLAT_CONFIRMED = "FLAT_CONFIRMED"
    POSITION_CONFIRMED = "POSITION_CONFIRMED"
    ORDER_WORKING = "ORDER_WORKING"
    MISMATCH = "MISMATCH"
    UNKNOWN = "UNKNOWN"
    STALE = "STALE"


class ProviderPositionDirection(StrEnum):
    FLAT = "FLAT"
    LONG = "LONG"
    SHORT = "SHORT"
    UNKNOWN = "UNKNOWN"


class ProviderOrderStatus(StrEnum):
    WORKING = "WORKING"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"
    UNKNOWN = "UNKNOWN"


def _required(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} is required.")
    return value.strip()


def _identifier(value: object, field_name: str) -> str:
    result = _required(value, field_name)
    if _IDENTIFIER.fullmatch(result) is None:
        raise ValueError(f"{field_name} has invalid characters.")
    return result


def _decimal(value: object, field_name: str, *, positive: bool = False, nonnegative: bool = False) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{field_name} must be a finite decimal.") from exc
    if not result.is_finite() or (positive and result <= 0) or (nonnegative and result < 0):
        raise ValueError(f"{field_name} is outside its permitted range.")
    return result


def _quantity(value: object, field_name: str, *, allow_zero: bool = False) -> int:
    if type(value) is not int or value < 0 or (value == 0 and not allow_zero):
        raise ValueError(f"{field_name} must be a {'non-negative' if allow_zero else 'positive'} integer.")
    return value


def _utc(value: object, field_name: str) -> str:
    return normalized_utc(value, field_name)


def _date_month(value: object, field_name: str) -> str:
    text = _required(value, field_name)
    if re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", text) is None:
        raise ValueError(f"{field_name} must be YYYY-MM.")
    return text


def _read_mapping(value: object, field_name: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise ProviderObservationRefused(ProviderErrorCode.MALFORMED_PROVIDER_PAYLOAD, field_name)
    return value


def redact_sensitive(value: object) -> object:
    """Return a safe diagnostic representation without modifying original data."""
    sensitive = ("password", "secret", "token", "authorization", "cid", "accesskey", "apikey")
    if isinstance(value, Mapping):
        return {
            str(key): "<redacted>" if any(marker in str(key).lower() for marker in sensitive) else redact_sensitive(item)
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [redact_sensitive(item) for item in value]
    return value


@dataclass(frozen=True)
class TradovateEndpoints:
    """Exact endpoint fence.  Demo failure can never select Live."""

    environment: TradovateEnvironment
    rest_base_url: str
    websocket_url: str
    market_data_websocket_url: str

    @staticmethod
    def _values(environment: TradovateEnvironment) -> tuple[str, str, str]:
        if environment is TradovateEnvironment.DEMO:
            return ("https://demo.tradovateapi.com/v1", "wss://demo.tradovateapi.com/v1/websocket", "wss://md.tradovateapi.com/v1/websocket")
        if environment is TradovateEnvironment.LIVE:
            return ("https://live.tradovateapi.com/v1", "wss://live.tradovateapi.com/v1/websocket", "wss://md.tradovateapi.com/v1/websocket")
        raise ProviderObservationRefused(ProviderErrorCode.ENVIRONMENT_MISMATCH, "environment")

    @classmethod
    def for_environment(cls, environment: TradovateEnvironment) -> "TradovateEndpoints":
        return cls(environment, *cls._values(environment))

    def __post_init__(self) -> None:
        if type(self.environment) is not TradovateEnvironment:
            raise ValueError("Tradovate environment must be explicit.")
        if (self.rest_base_url, self.websocket_url, self.market_data_websocket_url) != type(self)._values(self.environment):
            raise ProviderObservationRefused(ProviderErrorCode.ENVIRONMENT_MISMATCH, "endpoint_fence")


@dataclass(frozen=True)
class TradovateCredentials:
    """Runtime-only credentials.  ``repr`` is deliberately secret-free."""

    username: str
    password: str
    client_id: str
    client_secret: str

    def __post_init__(self) -> None:
        _required(self.username, "Tradovate username")
        _required(self.password, "Tradovate password")
        _required(self.client_id, "Tradovate client ID")
        _required(self.client_secret, "Tradovate client secret")

    def __repr__(self) -> str:
        return "TradovateCredentials(username=<runtime-only>, password=<redacted>, client_id=<redacted>, client_secret=<redacted>)"


class TradovateSecretProvider(Protocol):
    def credentials(self) -> TradovateCredentials: ...


@dataclass(frozen=True)
class EnvironmentTradovateSecretProvider:
    """Read credentials only from process environment at connection time."""

    username_variable: str = "L3F_TRADOVATE_USERNAME"
    password_variable: str = "L3F_TRADOVATE_PASSWORD"
    client_id_variable: str = "L3F_TRADOVATE_CID"
    client_secret_variable: str = "L3F_TRADOVATE_SECRET"

    def credentials(self) -> TradovateCredentials:
        values = (os.getenv(self.username_variable), os.getenv(self.password_variable), os.getenv(self.client_id_variable), os.getenv(self.client_secret_variable))
        if not all(values):
            raise ProviderObservationRefused(ProviderErrorCode.AUTH_FAILED, "runtime_credentials_missing")
        return TradovateCredentials(*[str(value) for value in values])

    def __repr__(self) -> str:
        return "EnvironmentTradovateSecretProvider(runtime_variables=<redacted>)"


@dataclass(frozen=True)
class ProviderCapability:
    name: str
    status: CapabilityStatus
    observed_at: str | None = None
    evidence: str | None = None

    def __post_init__(self) -> None:
        _identifier(self.name, "Capability name")
        if type(self.status) is not CapabilityStatus:
            raise ValueError("Capability status must be explicit.")
        if self.observed_at is not None:
            _utc(self.observed_at, "Capability observation time")
        if self.evidence is not None:
            _required(self.evidence, "Capability evidence")

    def payload(self) -> dict[str, str | None]:
        return {"name": self.name, "status": self.status.value, "observed_at": self.observed_at, "evidence": self.evidence}


@dataclass(frozen=True)
class TradovateContract:
    provider_contract_id: int
    symbol: str
    expiry: str
    exchange: str
    tick_size: Decimal
    point_value: Decimal | None = None

    def __post_init__(self) -> None:
        if type(self.provider_contract_id) is not int or self.provider_contract_id <= 0:
            raise ValueError("Provider contract ID must be positive.")
        match = _MNQ_SYMBOL.fullmatch(_required(self.symbol, "Provider contract symbol"))
        if match is None:
            raise ProviderObservationRefused(ProviderErrorCode.CONTRACT_NOT_FOUND, "not_concrete_mnq")
        expiry = _date_month(self.expiry, "Provider contract expiry")
        if int(expiry[-2:]) != _MONTHS[match.group(1)] or int(expiry[:4]) % (10 if len(match.group(2)) == 1 else 100) != int(match.group(2)):
            raise ProviderObservationRefused(ProviderErrorCode.CONTRACT_NOT_FOUND, "symbol_expiry_mismatch")
        if self.exchange != "CME":
            raise ProviderObservationRefused(ProviderErrorCode.CONTRACT_NOT_FOUND, "exchange_not_cme")
        tick = _decimal(self.tick_size, "Tick size", positive=True)
        if tick != Decimal("0.25"):
            raise ProviderObservationRefused(ProviderErrorCode.CONTRACT_NOT_FOUND, "mnq_tick_size_mismatch")
        if self.point_value is not None:
            _decimal(self.point_value, "Point value", positive=True)

    @property
    def canonical_contract(self) -> MNQContract:
        return MNQContract(self.symbol, self.expiry, self.exchange, LaneIIIInstrument.MNQ)

    def payload(self) -> dict[str, str | int | None]:
        return {"provider_contract_id": self.provider_contract_id, "symbol": self.symbol, "expiry": self.expiry, "exchange": self.exchange, "tick_size": str(self.tick_size), "point_value": None if self.point_value is None else str(self.point_value)}


@dataclass(frozen=True)
class ProviderAccount:
    provider_account_id: int
    name: str
    active: bool
    environment: TradovateEnvironment

    def __post_init__(self) -> None:
        if type(self.provider_account_id) is not int or self.provider_account_id <= 0:
            raise ValueError("Provider account ID must be positive.")
        _required(self.name, "Provider account name")
        if type(self.active) is not bool or type(self.environment) is not TradovateEnvironment:
            raise ValueError("Provider account metadata is malformed.")

    def safe_payload(self) -> dict[str, object]:
        return {"provider_account_id_hash": hashlib.sha256(str(self.provider_account_id).encode()).hexdigest(), "active": self.active, "environment": self.environment.value}


@dataclass(frozen=True)
class AccountObservation:
    account_alias: str
    provider_account_id: int
    active: bool
    observed_at: str
    balance: Decimal | None = None
    realized_pnl: Decimal | None = None
    unrealized_pnl: Decimal | None = None
    margin_state: str | None = None
    restrictions: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _identifier(self.account_alias, "Account alias")
        if type(self.provider_account_id) is not int or self.provider_account_id <= 0 or type(self.active) is not bool:
            raise ValueError("Account observation identity is malformed.")
        _utc(self.observed_at, "Account observation time")
        for value, name in ((self.balance, "Balance"), (self.realized_pnl, "Realized P&L"), (self.unrealized_pnl, "Unrealized P&L")):
            if value is not None:
                _decimal(value, name)
        if self.margin_state is not None:
            _required(self.margin_state, "Margin state")
        if not isinstance(self.restrictions, tuple) or any(not isinstance(item, str) or not item.strip() for item in self.restrictions):
            raise ValueError("Account restrictions must be an immutable text tuple.")


@dataclass(frozen=True)
class PositionObservation:
    contract: TradovateContract | None
    quantity: int | None
    direction: ProviderPositionDirection
    average_price: Decimal | None
    account_alias: str
    observed_at: str

    def __post_init__(self) -> None:
        if type(self.direction) is not ProviderPositionDirection:
            raise ValueError("Position direction must be explicit.")
        _identifier(self.account_alias, "Position account alias")
        _utc(self.observed_at, "Position observation time")
        if self.direction is ProviderPositionDirection.UNKNOWN:
            if any(value is not None for value in (self.contract, self.quantity, self.average_price)):
                raise ValueError("Unknown position cannot assert exposure fields.")
            return
        if self.direction is ProviderPositionDirection.FLAT:
            if self.contract is not None or self.quantity not in {0, None} or self.average_price is not None:
                raise ValueError("Flat position cannot assert a contract or average price.")
            return
        if type(self.contract) is not TradovateContract or self.quantity is None or self.quantity <= 0:
            raise ValueError("Non-flat position requires concrete contract and positive quantity.")
        _decimal(self.average_price, "Position average price", positive=True)


@dataclass(frozen=True)
class OrderObservation:
    provider_order_id: int
    contract: TradovateContract
    side: str
    quantity: int
    filled_quantity: int
    remaining_quantity: int
    status: ProviderOrderStatus
    account_alias: str
    observed_at: str
    created_at: str | None = None

    def __post_init__(self) -> None:
        if type(self.provider_order_id) is not int or self.provider_order_id <= 0:
            raise ValueError("Provider order ID must be positive.")
        if type(self.contract) is not TradovateContract or self.side not in {"BUY", "SELL"}:
            raise ValueError("Order contract or side is invalid.")
        _quantity(self.quantity, "Order quantity")
        _quantity(self.filled_quantity, "Filled quantity", allow_zero=True)
        _quantity(self.remaining_quantity, "Remaining quantity", allow_zero=True)
        if self.filled_quantity + self.remaining_quantity != self.quantity:
            raise ValueError("Order quantities must reconcile exactly.")
        if type(self.status) is not ProviderOrderStatus:
            raise ValueError("Order status must be explicit.")
        if self.status is ProviderOrderStatus.UNKNOWN:
            raise ValueError("Unknown provider order must not be modeled as a concrete order.")
        _identifier(self.account_alias, "Order account alias")
        _utc(self.observed_at, "Order observation time")
        if self.created_at is not None:
            _utc(self.created_at, "Order creation time")

    @property
    def is_working(self) -> bool:
        return self.status in {ProviderOrderStatus.WORKING, ProviderOrderStatus.PARTIALLY_FILLED}


@dataclass(frozen=True)
class LocalObservedState:
    """Non-authoritative persisted observation used only to detect disagreement."""

    position_direction: ProviderPositionDirection
    position_quantity: int | None
    working_order_ids: tuple[int, ...]
    observed_at: str | None = None

    def __post_init__(self) -> None:
        if type(self.position_direction) is not ProviderPositionDirection:
            raise ValueError("Local position direction must be explicit.")
        if self.position_direction in {ProviderPositionDirection.LONG, ProviderPositionDirection.SHORT}:
            _quantity(self.position_quantity, "Local position quantity")
        elif self.position_quantity not in {None, 0}:
            raise ValueError("Only non-flat local position may declare quantity.")
        if not isinstance(self.working_order_ids, tuple) or any(type(item) is not int or item <= 0 for item in self.working_order_ids):
            raise ValueError("Local working order IDs must be immutable positive integers.")
        if self.observed_at is not None:
            _utc(self.observed_at, "Local observation time")


@dataclass(frozen=True)
class ReconciliationResult:
    state: ObservationTruth
    provider_position: PositionObservation
    provider_orders: tuple[OrderObservation, ...]
    observed_at: str
    reasons: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if type(self.state) is not ObservationTruth or type(self.provider_position) is not PositionObservation:
            raise ValueError("Reconciliation state is malformed.")
        if not isinstance(self.provider_orders, tuple) or any(type(item) is not OrderObservation for item in self.provider_orders):
            raise ValueError("Reconciliation orders must be an immutable tuple.")
        _utc(self.observed_at, "Reconciliation time")
        if not isinstance(self.reasons, tuple):
            raise ValueError("Reconciliation reasons must be immutable.")

    @property
    def authoritative(self) -> bool:
        return self.state in {ObservationTruth.FLAT_CONFIRMED, ObservationTruth.POSITION_CONFIRMED, ObservationTruth.ORDER_WORKING}


def reconcile_provider_truth(*, local: LocalObservedState | None, position: PositionObservation, orders: Sequence[OrderObservation], observed_at: str, maximum_age: timedelta = timedelta(seconds=30)) -> ReconciliationResult:
    """Provider truth wins; missing/stale/non-conforming data never becomes flat."""
    now = _utc(observed_at, "Reconciliation time")
    provider_time = datetime.fromisoformat(position.observed_at.replace("Z", "+00:00"))
    current_time = datetime.fromisoformat(now.replace("Z", "+00:00"))
    if current_time - provider_time > maximum_age:
        return ReconciliationResult(ObservationTruth.STALE, position, tuple(orders), now, ("provider_position_stale",))
    if position.direction is ProviderPositionDirection.UNKNOWN:
        return ReconciliationResult(ObservationTruth.UNKNOWN, position, tuple(orders), now, ("provider_position_unknown",))
    if any(order.observed_at != position.observed_at for order in orders):
        return ReconciliationResult(ObservationTruth.STALE, position, tuple(orders), now, ("provider_order_snapshot_not_coherent",))
    working = tuple(sorted((order.provider_order_id for order in orders if order.is_working)))
    if local is not None:
        if local.position_direction is not position.direction or (local.position_quantity or 0) != (position.quantity or 0):
            return ReconciliationResult(ObservationTruth.MISMATCH, position, tuple(orders), now, ("local_position_disagrees_with_provider",))
        if tuple(sorted(local.working_order_ids)) != working:
            return ReconciliationResult(ObservationTruth.MISMATCH, position, tuple(orders), now, ("local_orders_disagree_with_provider",))
    if working:
        return ReconciliationResult(ObservationTruth.ORDER_WORKING, position, tuple(orders), now)
    if position.direction is ProviderPositionDirection.FLAT:
        return ReconciliationResult(ObservationTruth.FLAT_CONFIRMED, position, tuple(orders), now)
    return ReconciliationResult(ObservationTruth.POSITION_CONFIRMED, position, tuple(orders), now)


@dataclass(frozen=True)
class ProviderHealth:
    streams: Mapping[ProviderStream, StreamHealth]
    updated_at: Mapping[ProviderStream, str | None]

    def __post_init__(self) -> None:
        if set(self.streams) != set(ProviderStream) or set(self.updated_at) != set(ProviderStream):
            raise ValueError("Provider health requires every independent stream.")
        if any(type(value) is not StreamHealth for value in self.streams.values()):
            raise ValueError("Provider stream health must be explicit.")
        for value in self.updated_at.values():
            if value is not None:
                _utc(value, "Provider stream update time")

    @property
    def account_truth_available(self) -> bool:
        return self.streams[ProviderStream.AUTH] is StreamHealth.HEALTHY and self.streams[ProviderStream.USER_DATA] is StreamHealth.HEALTHY

    @property
    def market_data_available(self) -> bool:
        return self.streams[ProviderStream.AUTH] is StreamHealth.HEALTHY and self.streams[ProviderStream.MARKET_DATA] is StreamHealth.HEALTHY


class ProviderHealthTracker:
    def __init__(self) -> None:
        self._streams = {stream: StreamHealth.UNKNOWN for stream in ProviderStream}
        self._updated: dict[ProviderStream, str | None] = {stream: None for stream in ProviderStream}

    def mark(self, stream: ProviderStream, state: StreamHealth, at: str) -> ProviderHealth:
        if type(stream) is not ProviderStream or type(state) is not StreamHealth:
            raise ValueError("Provider stream and health state must be explicit.")
        self._streams[stream] = state
        self._updated[stream] = _utc(at, "Provider stream update time")
        return self.snapshot()

    def mark_disconnected(self, stream: ProviderStream, at: str) -> ProviderHealth:
        return self.mark(stream, StreamHealth.DISCONNECTED, at)

    def mark_token_expired(self, at: str) -> ProviderHealth:
        self.mark(ProviderStream.AUTH, StreamHealth.AUTH_EXPIRED, at)
        self._streams[ProviderStream.MARKET_DATA] = StreamHealth.DISCONNECTED
        self._streams[ProviderStream.USER_DATA] = StreamHealth.DISCONNECTED
        self._updated[ProviderStream.MARKET_DATA] = _utc(at, "Provider stream update time")
        self._updated[ProviderStream.USER_DATA] = _utc(at, "Provider stream update time")
        return self.snapshot()

    def assess_staleness(self, at: str, maximum_age: timedelta) -> ProviderHealth:
        now = datetime.fromisoformat(_utc(at, "Provider health check time").replace("Z", "+00:00"))
        for stream, previous in self._updated.items():
            if self._streams[stream] is StreamHealth.HEALTHY and previous is not None:
                then = datetime.fromisoformat(previous.replace("Z", "+00:00"))
                if now - then > maximum_age:
                    self._streams[stream] = StreamHealth.STALE
        return self.snapshot()

    def snapshot(self) -> ProviderHealth:
        return ProviderHealth(MappingProxyType(dict(self._streams)), MappingProxyType(dict(self._updated)))


@dataclass(frozen=True)
class LucidRiskProfile:
    provider_name: str
    platform: str
    routing_data_family: str
    account_program: str
    account_stage: str
    nominal_account_size: Decimal
    account_alias: str
    strategy_instrument: LaneIIIInstrument
    firm_max_mnq_contracts: int
    internal_max_mnq_contracts: int
    firm_max_loss_limit: Decimal
    drawdown_type: str
    drawdown_behavior: str
    firm_liquidation_balance: Decimal | None
    daily_loss_limit_enabled: bool
    daily_loss_limit: Decimal | None
    internal_daily_loss_ceiling: Decimal
    high_impact_news_restriction: CapabilityStatus
    required_flat_before_news: str | None
    required_flat_after_news: str | None
    firm_flat_time: time
    internal_flat_time: time
    timezone_name: str
    firm_reopen_time: time
    allowed_session_start: time
    allowed_session_end: time
    automated_strategy_allowed: CapabilityStatus
    rule_provenance_url: str
    rule_provenance_observed_at: str

    def __post_init__(self) -> None:
        if (self.provider_name, self.platform, self.routing_data_family) != ("Lucid Trading", "Tradovate", "CQG / Tradovate"):
            raise ValueError("Lucid / Tradovate provider identity must be explicit and exact.")
        if self.account_program not in {"LucidDaily", "LucidPro", "LucidFlex", "LucidDirect", "LucidMaxx", "LucidLive"}:
            raise ValueError("Lucid account program is invalid.")
        if self.account_stage not in {"Evaluation", "Sim Funded", "Live"}:
            raise ValueError("Lucid account stage is invalid.")
        _decimal(self.nominal_account_size, "Nominal account size", positive=True)
        _identifier(self.account_alias, "Account alias")
        if self.strategy_instrument is not LaneIIIInstrument.MNQ:
            raise ValueError("L3-F observes MNQ only.")
        _quantity(self.firm_max_mnq_contracts, "Firm max MNQ contracts")
        _quantity(self.internal_max_mnq_contracts, "Internal max MNQ contracts")
        if self.internal_max_mnq_contracts > self.firm_max_mnq_contracts:
            raise ValueError("Internal maximum quantity may not exceed Lucid maximum.")
        firm_loss = _decimal(self.firm_max_loss_limit, "Firm maximum loss", positive=True)
        internal_loss = _decimal(self.internal_daily_loss_ceiling, "Internal daily loss ceiling", positive=True)
        if internal_loss >= firm_loss:
            raise ValueError("Internal daily loss ceiling must be stricter than firm maximum loss.")
        if self.drawdown_type not in {"Intraday", "EOD", "Other"}:
            raise ValueError("Drawdown type is invalid.")
        if self.drawdown_behavior not in {"Trailing", "Locked", "Other", "Unknown"}:
            raise ValueError("Drawdown behavior is invalid.")
        if self.firm_liquidation_balance is not None:
            _decimal(self.firm_liquidation_balance, "Firm liquidation balance", positive=True)
        if type(self.daily_loss_limit_enabled) is not bool:
            raise ValueError("Daily-loss enabled state must be boolean.")
        if self.daily_loss_limit_enabled and self.daily_loss_limit is None:
            raise ValueError("Enabled daily loss limit requires a configured value.")
        if not self.daily_loss_limit_enabled and self.daily_loss_limit is not None:
            raise ValueError("Disabled daily loss limit cannot declare a value.")
        if self.daily_loss_limit is not None:
            _decimal(self.daily_loss_limit, "Daily loss limit", positive=True)
        if type(self.high_impact_news_restriction) is not CapabilityStatus or type(self.automated_strategy_allowed) is not CapabilityStatus:
            raise ValueError("Rule knowledge state must be explicit.")
        # This phase fixes the supplied Lucid profile to New York time.  Do
        # not use the workstation timezone (and do not require the optional
        # Windows ``tzdata`` package merely to validate this canonical name).
        if self.timezone_name != "America/New_York":
            raise ValueError("Lucid session rules must use canonical America/New_York timezone semantics.")
        if self.internal_flat_time >= self.firm_flat_time:
            raise ValueError("Internal flat time must precede firm flat time.")
        if self.allowed_session_start >= self.allowed_session_end:
            raise ValueError("Allowed session must have a positive same-day interval.")
        _required(self.rule_provenance_url, "Rule provenance URL")
        _utc(self.rule_provenance_observed_at, "Rule provenance observation time")

    @property
    def future_live_readiness_blockers(self) -> tuple[str, ...]:
        blockers: list[str] = []
        if self.high_impact_news_restriction is CapabilityStatus.UNKNOWN:
            blockers.append("high_impact_news_restriction_unknown")
        if self.drawdown_behavior == "Unknown":
            blockers.append("drawdown_behavior_unknown")
        if self.automated_strategy_allowed is not CapabilityStatus.SUPPORTED:
            blockers.append("automation_policy_not_confirmed")
        return tuple(blockers)

    def payload(self) -> dict[str, object]:
        return {"provider_name": self.provider_name, "platform": self.platform, "routing_data_family": self.routing_data_family, "account_program": self.account_program, "account_stage": self.account_stage, "nominal_account_size": str(self.nominal_account_size), "account_alias": self.account_alias, "instrument": self.strategy_instrument.value, "firm_max_mnq_contracts": self.firm_max_mnq_contracts, "internal_max_mnq_contracts": self.internal_max_mnq_contracts, "firm_max_loss_limit": str(self.firm_max_loss_limit), "drawdown_type": self.drawdown_type, "drawdown_behavior": self.drawdown_behavior, "firm_liquidation_balance": None if self.firm_liquidation_balance is None else str(self.firm_liquidation_balance), "daily_loss_limit_enabled": self.daily_loss_limit_enabled, "daily_loss_limit": None if self.daily_loss_limit is None else str(self.daily_loss_limit), "internal_daily_loss_ceiling": str(self.internal_daily_loss_ceiling), "high_impact_news_restriction": self.high_impact_news_restriction.value, "firm_flat_time": self.firm_flat_time.isoformat(), "internal_flat_time": self.internal_flat_time.isoformat(), "timezone": self.timezone_name, "firm_reopen_time": self.firm_reopen_time.isoformat(), "allowed_session": [self.allowed_session_start.isoformat(), self.allowed_session_end.isoformat()], "automated_strategy_allowed": self.automated_strategy_allowed.value, "rule_provenance_url": self.rule_provenance_url, "rule_provenance_observed_at": self.rule_provenance_observed_at}


class FutureRateAction(StrEnum):
    """Diagnostic-only action categories for a separately authorized future phase."""

    ENTRY_ATTEMPT = "ENTRY_ATTEMPT"
    CHANGE_ATTEMPT = "CHANGE_ATTEMPT"
    DUPLICATE_DECISION = "DUPLICATE_DECISION"


@dataclass(frozen=True)
class FutureExecutionRatePolicy:
    interval: timedelta = timedelta(minutes=1)
    maximum_entry_attempts: int = 5
    maximum_change_attempts: int = 10

    def __post_init__(self) -> None:
        if not isinstance(self.interval, timedelta) or self.interval <= timedelta(0):
            raise ValueError("Future rate-limit interval must be positive.")
        if type(self.maximum_entry_attempts) is not int or self.maximum_entry_attempts <= 0 or type(self.maximum_change_attempts) is not int or self.maximum_change_attempts <= 0:
            raise ValueError("Future rate limits must be positive integers.")


class FutureExecutionRateGuard:
    """A future-safe diagnostic gate with no provider or execution dependency."""

    def __init__(self, policy: FutureExecutionRatePolicy = FutureExecutionRatePolicy()) -> None:
        self.policy = policy
        self._accepted: list[tuple[FutureRateAction, str, datetime]] = []
        self._decision_ids: set[str] = set()

    def assess_attempt(self, action: FutureRateAction, decision_id: str, at: str) -> tuple[bool, str]:
        if type(action) is not FutureRateAction:
            raise ValueError("Future rate action must be explicit.")
        identity = _identifier(decision_id, "Future decision identity")
        current = datetime.fromisoformat(_utc(at, "Future rate observation time").replace("Z", "+00:00"))
        self._accepted = [item for item in self._accepted if current - item[2] <= self.policy.interval]
        if identity in self._decision_ids:
            return False, "DUPLICATE_DECISION_ATTEMPT"
        category = FutureRateAction.ENTRY_ATTEMPT if action is FutureRateAction.ENTRY_ATTEMPT else FutureRateAction.CHANGE_ATTEMPT
        count = sum(1 for prior, _, _ in self._accepted if prior is category)
        limit = self.policy.maximum_entry_attempts if category is FutureRateAction.ENTRY_ATTEMPT else self.policy.maximum_change_attempts
        if count >= limit:
            return False, "RATE_LIMIT_EXCEEDED"
        self._accepted.append((category, identity, current))
        self._decision_ids.add(identity)
        return True, "FUTURE_RATE_GUARD_ACCEPTED"


@dataclass(frozen=True)
class TradeDurationDiagnosticRecord:
    """Post-trade diagnostic data only; L3-F does not create the trades."""

    opened_at: str
    closed_at: str
    realized_profit: Decimal

    def __post_init__(self) -> None:
        opened = datetime.fromisoformat(_utc(self.opened_at, "Trade open time").replace("Z", "+00:00"))
        closed = datetime.fromisoformat(_utc(self.closed_at, "Trade close time").replace("Z", "+00:00"))
        if closed < opened:
            raise ValueError("Trade close time cannot precede open time.")
        _decimal(self.realized_profit, "Trade realized profit")

    @property
    def duration(self) -> timedelta:
        return datetime.fromisoformat(self.closed_at.replace("Z", "+00:00")) - datetime.fromisoformat(self.opened_at.replace("Z", "+00:00"))


@dataclass(frozen=True)
class MicroscalpingDiagnostic:
    profitable_trade_count: int
    profitable_short_duration_trade_count: int
    profitable_amount: Decimal
    short_duration_profitable_amount: Decimal
    short_duration_profit_fraction: Decimal | None


def microscalping_diagnostic(records: Iterable[TradeDurationDiagnosticRecord], maximum_duration: timedelta = timedelta(seconds=5)) -> MicroscalpingDiagnostic:
    if not isinstance(maximum_duration, timedelta) or maximum_duration <= timedelta(0):
        raise ValueError("Microscalping duration threshold must be positive.")
    values = tuple(records)
    if any(type(record) is not TradeDurationDiagnosticRecord for record in values):
        raise ValueError("Microscalping diagnostic requires exact records.")
    profitable = tuple(record for record in values if record.realized_profit > 0)
    short = tuple(record for record in profitable if record.duration <= maximum_duration)
    total = sum((record.realized_profit for record in profitable), Decimal("0"))
    short_total = sum((record.realized_profit for record in short), Decimal("0"))
    return MicroscalpingDiagnostic(len(profitable), len(short), total, short_total, None if total == 0 else short_total / total)


COMMISSIONED_LUCID_PROFILE = LucidRiskProfile(
    provider_name="Lucid Trading", platform="Tradovate", routing_data_family="CQG / Tradovate", account_program="LucidFlex", account_stage="Evaluation", nominal_account_size=Decimal("25000"), account_alias="Lucid25kflex01", strategy_instrument=LaneIIIInstrument.MNQ, firm_max_mnq_contracts=20, internal_max_mnq_contracts=1, firm_max_loss_limit=Decimal("1000"), drawdown_type="EOD", drawdown_behavior="Unknown", firm_liquidation_balance=Decimal("24000"), daily_loss_limit_enabled=False, daily_loss_limit=None, internal_daily_loss_ceiling=Decimal("200"), high_impact_news_restriction=CapabilityStatus.UNKNOWN, required_flat_before_news=None, required_flat_after_news=None, firm_flat_time=time(16, 45), internal_flat_time=time(15, 58), timezone_name="America/New_York", firm_reopen_time=time(18, 0), allowed_session_start=time(9, 35), allowed_session_end=time(15, 30), automated_strategy_allowed=CapabilityStatus.SUPPORTED, rule_provenance_url="https://lucidtrading.com/general-faq/", rule_provenance_observed_at="2026-08-20T00:00:00Z",
)


@dataclass(frozen=True)
class TradovateObservationConfig:
    environment: TradovateEnvironment
    mode: ObservationMode
    account_alias: str
    provider_account_id: int | None
    contract_symbol: str | None
    risk_profile: LucidRiskProfile
    stale_after: timedelta = timedelta(seconds=30)

    def __post_init__(self) -> None:
        if type(self.environment) is not TradovateEnvironment or self.mode is not ObservationMode.OBSERVE_ONLY:
            raise ProviderObservationRefused(ProviderErrorCode.READ_ONLY_VIOLATION, "observe_only_mode_required")
        _identifier(self.account_alias, "Configured account alias")
        if self.account_alias != self.risk_profile.account_alias:
            raise ValueError("Configured account alias must match Lucid risk profile.")
        if self.provider_account_id is not None and (type(self.provider_account_id) is not int or self.provider_account_id <= 0):
            raise ValueError("Configured provider account ID must be positive when provided.")
        if self.contract_symbol is not None and _MNQ_SYMBOL.fullmatch(self.contract_symbol) is None:
            raise ValueError("Configured contract must be a concrete MNQ expiry.")
        if not isinstance(self.stale_after, timedelta) or self.stale_after <= timedelta(0):
            raise ValueError("Provider stale interval must be positive.")

    @property
    def endpoints(self) -> TradovateEndpoints:
        return TradovateEndpoints.for_environment(self.environment)


@dataclass(frozen=True)
class TradovateSession:
    access_token: str
    expires_at: str
    environment: TradovateEnvironment
    user_id: int

    def __post_init__(self) -> None:
        _required(self.access_token, "Access token")
        _utc(self.expires_at, "Token expiry")
        if type(self.environment) is not TradovateEnvironment or type(self.user_id) is not int or self.user_id <= 0:
            raise ValueError("Tradovate session is malformed.")

    def __repr__(self) -> str:
        return f"TradovateSession(access_token=<redacted>, expires_at={self.expires_at!r}, environment={self.environment.value!r}, user_id={self.user_id!r})"


class TradovateReadOnlyClient(Protocol):
    """No write method is permitted in the L3-F transport protocol."""

    def authenticate(self, credentials: TradovateCredentials, endpoints: TradovateEndpoints) -> TradovateSession: ...
    def discover_capabilities(self, session: TradovateSession, endpoints: TradovateEndpoints) -> Mapping[str, CapabilityStatus]: ...
    def list_accounts(self, session: TradovateSession, endpoints: TradovateEndpoints) -> Sequence[ProviderAccount]: ...
    def resolve_contract(self, session: TradovateSession, endpoints: TradovateEndpoints, symbol: str) -> TradovateContract: ...
    def observe_account(self, session: TradovateSession, endpoints: TradovateEndpoints, account: ProviderAccount, alias: str, observed_at: str) -> AccountObservation: ...
    def observe_position(self, session: TradovateSession, endpoints: TradovateEndpoints, account: ProviderAccount, alias: str, contract: TradovateContract, observed_at: str) -> PositionObservation: ...
    def observe_orders(self, session: TradovateSession, endpoints: TradovateEndpoints, account: ProviderAccount, alias: str, contract: TradovateContract, observed_at: str) -> Sequence[OrderObservation]: ...


class _HttpSession(Protocol):
    def post(self, url: str, *, json: Mapping[str, object], timeout: float) -> object: ...
    def get(self, url: str, *, headers: Mapping[str, str], params: Mapping[str, object] | None, timeout: float) -> object: ...


def _response_json(response: object, operation: str) -> object:
    try:
        response.raise_for_status()  # type: ignore[attr-defined]
        return response.json()  # type: ignore[attr-defined]
    except Exception:
        # Provider response text can contain diagnostic PII or auth material;
        # retain only the fixed operation identifier in the failure.
        raise ProviderObservationRefused(ProviderErrorCode.MALFORMED_PROVIDER_PAYLOAD, operation) from None


def _provider_time(value: object, field_name: str) -> str:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return datetime.fromtimestamp(float(value) / 1000, tz=timezone.utc).isoformat().replace("+00:00", "Z")
    if isinstance(value, str) and re.fullmatch(r"\d{13}", value):
        return datetime.fromtimestamp(int(value) / 1000, tz=timezone.utc).isoformat().replace("+00:00", "Z")
    return _utc(value, field_name)


class RequestsTradovateReadOnlyClient:
    """Concrete REST transport restricted to authentication and named reads.

    The API also has order endpoints, but this class deliberately does not
    contain their route names or a generic request method.  Its application
    identifier/version/device name are non-secret deployment metadata supplied
    by the registered Tradovate API application owner.
    """

    def __init__(self, *, application_id: str, application_version: str, device_id: str, request_timeout_seconds: float = 10.0, http_session: _HttpSession | None = None) -> None:
        self.application_id = _identifier(application_id, "Tradovate application ID")
        self.application_version = _identifier(application_version, "Tradovate application version")
        self.device_id = _identifier(device_id, "Tradovate device ID")
        if not isinstance(request_timeout_seconds, (int, float)) or isinstance(request_timeout_seconds, bool) or request_timeout_seconds <= 0:
            raise ValueError("Tradovate request timeout must be positive.")
        self.request_timeout_seconds = float(request_timeout_seconds)
        self._http = http_session or requests.Session()

    def __repr__(self) -> str:
        return f"RequestsTradovateReadOnlyClient(application_id={self.application_id!r}, application_version={self.application_version!r}, device_id={self.device_id!r}, authority='OBSERVE_ONLY')"

    def authenticate(self, credentials: TradovateCredentials, endpoints: TradovateEndpoints) -> TradovateSession:
        payload = {"name": credentials.username, "password": credentials.password, "appId": self.application_id, "appVersion": self.application_version, "cid": credentials.client_id, "sec": credentials.client_secret, "deviceId": self.device_id}
        response = self._http.post(f"{endpoints.rest_base_url}/auth/accesstokenrequest", json=payload, timeout=self.request_timeout_seconds)
        body = _read_mapping(_response_json(response, "auth_accesstokenrequest"), "auth_response")
        token = body.get("accessToken")
        user_id = body.get("userId")
        expiry = body.get("expirationTime", body.get("accessTokenExpiration"))
        if not isinstance(token, str) or not token or type(user_id) is not int or expiry is None:
            raise ProviderObservationRefused(ProviderErrorCode.AUTH_FAILED, "auth_response_incomplete")
        try:
            return TradovateSession(token, _provider_time(expiry, "Token expiry"), endpoints.environment, user_id)
        except ValueError:
            raise ProviderObservationRefused(ProviderErrorCode.AUTH_FAILED, "token_expiry_invalid") from None

    def discover_capabilities(self, session: TradovateSession, endpoints: TradovateEndpoints) -> Mapping[str, CapabilityStatus]:
        self._assert_session_environment(session, endpoints)
        # Presence of public API documentation is not account entitlement.
        # Actual quote/DOM/user-sync status is set only by a successful scoped
        # subscription in a future read-only commissioning invocation.
        return MappingProxyType({"authentication": CapabilityStatus.SUPPORTED, "accounts": CapabilityStatus.UNKNOWN, "contract_discovery": CapabilityStatus.UNKNOWN, "quotes": CapabilityStatus.UNKNOWN, "trades": CapabilityStatus.UNKNOWN, "dom": CapabilityStatus.UNKNOWN, "user_sync": CapabilityStatus.UNKNOWN, "positions": CapabilityStatus.UNKNOWN, "orders": CapabilityStatus.UNKNOWN, "market_data": CapabilityStatus.UNKNOWN})

    def list_accounts(self, session: TradovateSession, endpoints: TradovateEndpoints) -> Sequence[ProviderAccount]:
        rows = self._accounts_read(session, endpoints)
        result: list[ProviderAccount] = []
        for row in rows:
            try:
                result.append(ProviderAccount(int(row["id"]), str(row["name"]), bool(row.get("active", True)), endpoints.environment))
            except (KeyError, TypeError, ValueError) as exc:
                raise ProviderObservationRefused(ProviderErrorCode.ACCOUNT_NOT_FOUND, "account_payload") from exc
        return tuple(result)

    def resolve_contract(self, session: TradovateSession, endpoints: TradovateEndpoints, symbol: str) -> TradovateContract:
        self._assert_session_environment(session, endpoints)
        found = _read_mapping(self._contract_find_read(session, endpoints, symbol), "contract_find_response")
        try:
            contract_id = int(found["id"])
            if str(found["name"]) != symbol:
                raise ValueError("symbol")
            details = _read_mapping(self._contract_item_read(session, endpoints, contract_id), "contract_item_response")
            maturity_id = details.get("contractMaturityId", found.get("contractMaturityId"))
            if type(maturity_id) is not int:
                raise ValueError("maturity")
            maturity = _read_mapping(self._contract_maturity_read(session, endpoints, maturity_id), "contract_maturity_response")
            raw_expiry = maturity.get("expirationDate", maturity.get("expiration"))
            expiry = _date_month(str(raw_expiry)[:7], "Provider contract expiry")
            exchange = str(details.get("exchange", details.get("exchangeName", found.get("exchange"))))
            tick_size = _decimal(details.get("tickSize"), "Provider tick size", positive=True)
            point_raw = details.get("pointValue")
            return TradovateContract(contract_id, symbol, expiry, exchange, tick_size, None if point_raw is None else _decimal(point_raw, "Provider point value", positive=True))
        except (KeyError, TypeError, ValueError, ProviderObservationRefused) as exc:
            if isinstance(exc, ProviderObservationRefused):
                raise
            raise ProviderObservationRefused(ProviderErrorCode.CONTRACT_NOT_FOUND, "contract_metadata") from None

    def observe_account(self, session: TradovateSession, endpoints: TradovateEndpoints, account: ProviderAccount, alias: str, observed_at: str) -> AccountObservation:
        row = _read_mapping(self._account_item_read(session, endpoints, account.provider_account_id), "account_item_response")
        if row.get("id") != account.provider_account_id:
            raise ProviderObservationRefused(ProviderErrorCode.RECONCILIATION_MISMATCH, "account_id")
        def optional_decimal(*names: str) -> Decimal | None:
            for name in names:
                if name in row and row[name] is not None:
                    return _decimal(row[name], name)
            return None
        restrictions = tuple(str(item) for item in row.get("restrictions", ()) if isinstance(item, str))
        try:
            return AccountObservation(alias, account.provider_account_id, bool(row.get("active", account.active)), observed_at, optional_decimal("cashBalance", "balance"), optional_decimal("realizedPnL", "realizedPnl"), optional_decimal("unrealizedPnL", "unrealizedPnl"), None if row.get("marginState") is None else str(row["marginState"]), restrictions)
        except ValueError:
            raise ProviderObservationRefused(ProviderErrorCode.STALE_PROVIDER_STATE, "account_state") from None

    def observe_position(self, session: TradovateSession, endpoints: TradovateEndpoints, account: ProviderAccount, alias: str, contract: TradovateContract, observed_at: str) -> PositionObservation:
        rows = self._positions_read(session, endpoints, account.provider_account_id)
        matching = [row for row in rows if row.get("accountId") == account.provider_account_id and row.get("contractId") == contract.provider_contract_id]
        if not matching:
            return PositionObservation(None, 0, ProviderPositionDirection.FLAT, None, alias, observed_at)
        if len(matching) != 1:
            raise ProviderObservationRefused(ProviderErrorCode.POSITION_UNKNOWN, "multiple_contract_positions")
        row = matching[0]
        try:
            signed = int(row.get("netPos", row.get("quantity")))
            if signed == 0:
                return PositionObservation(None, 0, ProviderPositionDirection.FLAT, None, alias, observed_at)
            average = _decimal(row.get("netPrice", row.get("averagePrice")), "Provider average price", positive=True)
            return PositionObservation(contract, abs(signed), ProviderPositionDirection.LONG if signed > 0 else ProviderPositionDirection.SHORT, average, alias, observed_at)
        except (TypeError, ValueError):
            raise ProviderObservationRefused(ProviderErrorCode.POSITION_UNKNOWN, "position_payload") from None

    def observe_orders(self, session: TradovateSession, endpoints: TradovateEndpoints, account: ProviderAccount, alias: str, contract: TradovateContract, observed_at: str) -> Sequence[OrderObservation]:
        result: list[OrderObservation] = []
        states = {"working": ProviderOrderStatus.WORKING, "partiallyfilled": ProviderOrderStatus.PARTIALLY_FILLED, "filled": ProviderOrderStatus.FILLED, "cancelled": ProviderOrderStatus.CANCELLED, "canceled": ProviderOrderStatus.CANCELLED, "rejected": ProviderOrderStatus.REJECTED}
        for row in self._orders_read(session, endpoints, account.provider_account_id):
            if row.get("accountId") != account.provider_account_id or row.get("contractId") != contract.provider_contract_id:
                continue
            try:
                status = states.get(str(row.get("ordStatus", row.get("status", ""))).replace(" ", "").lower())
                if status is None:
                    raise ValueError("status")
                quantity = int(row.get("orderQty", row.get("quantity")))
                filled = int(row.get("filledQty", row.get("filledQuantity", 0)))
                remaining = int(row.get("remainingQty", quantity - filled))
                action = str(row.get("action", row.get("side", ""))).upper()
                side = "BUY" if action in {"BUY", "B"} else "SELL" if action in {"SELL", "S"} else ""
                created = row.get("timestamp", row.get("createdAt"))
                result.append(OrderObservation(int(row["id"]), contract, side, quantity, filled, remaining, status, alias, observed_at, None if created is None else _provider_time(created, "Order creation time")))
            except (KeyError, TypeError, ValueError):
                raise ProviderObservationRefused(ProviderErrorCode.ORDER_STATE_UNKNOWN, "order_payload") from None
        return tuple(result)

    def _headers(self, session: TradovateSession, endpoints: TradovateEndpoints) -> Mapping[str, str]:
        self._assert_session_environment(session, endpoints)
        return MappingProxyType({"Accept": "application/json", "Authorization": f"Bearer {session.access_token}"})

    @staticmethod
    def _assert_session_environment(session: TradovateSession, endpoints: TradovateEndpoints) -> None:
        if session.environment is not endpoints.environment:
            raise ProviderObservationRefused(ProviderErrorCode.ENVIRONMENT_MISMATCH, "session_endpoint")

    def _accounts_read(self, session: TradovateSession, endpoints: TradovateEndpoints) -> Sequence[Mapping[str, object]]:
        response = self._http.get(f"{endpoints.rest_base_url}/account/list", headers=self._headers(session, endpoints), params=None, timeout=self.request_timeout_seconds)
        body = _response_json(response, "account_list")
        if not isinstance(body, list):
            raise ProviderObservationRefused(ProviderErrorCode.ACCOUNT_NOT_FOUND, "account_list_payload")
        return tuple(_read_mapping(row, "account_list_row") for row in body)

    def _account_item_read(self, session: TradovateSession, endpoints: TradovateEndpoints, account_id: int) -> object:
        response = self._http.get(f"{endpoints.rest_base_url}/account/item", headers=self._headers(session, endpoints), params={"id": account_id}, timeout=self.request_timeout_seconds)
        return _response_json(response, "account_item")

    def _contract_find_read(self, session: TradovateSession, endpoints: TradovateEndpoints, symbol: str) -> object:
        response = self._http.get(f"{endpoints.rest_base_url}/contract/find", headers=self._headers(session, endpoints), params={"name": symbol}, timeout=self.request_timeout_seconds)
        return _response_json(response, "contract_find")

    def _contract_item_read(self, session: TradovateSession, endpoints: TradovateEndpoints, contract_id: int) -> object:
        response = self._http.get(f"{endpoints.rest_base_url}/contract/item", headers=self._headers(session, endpoints), params={"id": contract_id}, timeout=self.request_timeout_seconds)
        return _response_json(response, "contract_item")

    def _contract_maturity_read(self, session: TradovateSession, endpoints: TradovateEndpoints, maturity_id: int) -> object:
        response = self._http.get(f"{endpoints.rest_base_url}/contractMaturity/item", headers=self._headers(session, endpoints), params={"id": maturity_id}, timeout=self.request_timeout_seconds)
        return _response_json(response, "contract_maturity_item")

    def _positions_read(self, session: TradovateSession, endpoints: TradovateEndpoints, account_id: int) -> Sequence[Mapping[str, object]]:
        response = self._http.get(f"{endpoints.rest_base_url}/position/list", headers=self._headers(session, endpoints), params={"accountId": account_id}, timeout=self.request_timeout_seconds)
        body = _response_json(response, "position_list")
        if not isinstance(body, list):
            raise ProviderObservationRefused(ProviderErrorCode.POSITION_UNKNOWN, "position_list_payload")
        return tuple(_read_mapping(row, "position_list_row") for row in body)

    def _orders_read(self, session: TradovateSession, endpoints: TradovateEndpoints, account_id: int) -> Sequence[Mapping[str, object]]:
        response = self._http.get(f"{endpoints.rest_base_url}/order/list", headers=self._headers(session, endpoints), params={"accountId": account_id}, timeout=self.request_timeout_seconds)
        body = _response_json(response, "order_list")
        if not isinstance(body, list):
            raise ProviderObservationRefused(ProviderErrorCode.ORDER_STATE_UNKNOWN, "order_list_payload")
        return tuple(_read_mapping(row, "order_list_row") for row in body)


class _WebSocket(Protocol):
    def send(self, message: str) -> object: ...
    def recv(self, timeout: float | None = None) -> object: ...
    def close(self) -> object: ...


class TradovateReadOnlyWebSocket:
    """Authenticated read-only Tradovate WebSocket client.

    The only application messages it can create are authorization, market-data
    subscription, tick-chart subscription, and user synchronization requests.
    No order mutation route is present in either this public surface or its
    internal frame formatter.  A caller supplies all non-secret app metadata
    through the HTTP client and the runtime-only token through ``session``.
    """

    def __init__(self, session: TradovateSession, endpoints: TradovateEndpoints, *, websocket_factory: Callable[[str, float], _WebSocket] | None = None, timeout_seconds: float = 10.0) -> None:
        if session.environment is not endpoints.environment:
            raise ProviderObservationRefused(ProviderErrorCode.ENVIRONMENT_MISMATCH, "websocket_session_endpoint")
        if not isinstance(timeout_seconds, (int, float)) or isinstance(timeout_seconds, bool) or timeout_seconds <= 0:
            raise ValueError("WebSocket timeout must be positive.")
        self.session, self.endpoints, self.timeout_seconds = session, endpoints, float(timeout_seconds)
        self._factory = websocket_factory or self._connect_websocket
        self._socket: _WebSocket | None = None
        self._request_id = 0
        self._opened = False

    def __repr__(self) -> str:
        return f"TradovateReadOnlyWebSocket(environment={self.endpoints.environment.value!r}, access_token=<redacted>, authority='OBSERVE_ONLY')"

    @staticmethod
    def _connect_websocket(url: str, timeout: float) -> _WebSocket:
        try:
            from websockets.sync.client import connect
            return connect(url, open_timeout=timeout, close_timeout=timeout)
        except Exception as exc:
            raise ProviderObservationRefused(ProviderErrorCode.PROVIDER_DISCONNECTED, type(exc).__name__) from None

    def open(self) -> None:
        if self._opened:
            return
        try:
            self._socket = self._factory(self.endpoints.websocket_url, self.timeout_seconds)
            # Tradovate's framed WebSocket protocol authorizes once per socket.
            self._send("authorize", self.session.access_token)
            self._require_success(self._receive_frame(), "authorize")
        except ProviderObservationRefused:
            self.close()
            raise
        except Exception as exc:
            self.close()
            raise ProviderObservationRefused(ProviderErrorCode.AUTH_FAILED, type(exc).__name__) from None
        self._opened = True

    def subscribe_quotes(self, contract: TradovateContract) -> None:
        self._require_open()
        self._send("md/subscribeQuote", {"symbol": contract.provider_contract_id})

    def subscribe_dom(self, contract: TradovateContract) -> None:
        self._require_open()
        self._send("md/subscribeDOM", {"symbol": contract.provider_contract_id})

    def subscribe_tick_chart(self, contract: TradovateContract) -> None:
        self._require_open()
        self._send("md/getChart", {"symbol": contract.provider_contract_id, "chartDescription": {"underlyingType": "Tick", "elementSize": 1, "elementSizeUnit": "UnderlyingUnits"}, "timeRange": {"asMuchAsElements": 1}})

    def synchronize_user(self, account: ProviderAccount) -> None:
        self._require_open()
        self._send("user/syncrequest", {"users": [self.session.user_id], "accounts": [account.provider_account_id], "splitResponses": True})

    def next_market_packet(self, received_at: str) -> RawProviderEvent:
        self._require_open()
        frame = self._receive_frame()
        payload = self._event_payload(frame, {"md", "chart"})
        payload_dict = dict(payload)
        receipt = _utc(received_at, "Market packet receipt time")
        event_id = "tradovate-ws-" + canonical_hash({"payload": payload_dict, "received_at": receipt})[:32]
        return RawProviderEvent(event_id, MarketDataSource(TRADOVATE_PROVIDER, TRADOVATE_FEED), receipt, payload_dict, None if payload.get("id") is None else str(payload["id"]))

    def next_user_packet(self) -> Mapping[str, object]:
        self._require_open()
        return MappingProxyType(dict(self._event_payload(self._receive_frame(), {"props", "user"})))

    def close(self) -> None:
        socket, self._socket, self._opened = self._socket, None, False
        if socket is not None:
            try:
                socket.close()
            except Exception:
                pass

    def _require_open(self) -> None:
        if not self._opened or self._socket is None:
            raise ProviderObservationRefused(ProviderErrorCode.PROVIDER_DISCONNECTED, "websocket_not_authorized")

    def _send(self, endpoint: str, body: object) -> None:
        if self._socket is None:
            raise ProviderObservationRefused(ProviderErrorCode.PROVIDER_DISCONNECTED, "websocket_missing")
        request_id, self._request_id = self._request_id, self._request_id + 1
        encoded_body = body if isinstance(body, str) else json.dumps(body, sort_keys=True, separators=(",", ":"))
        self._socket.send(f"{endpoint}\n{request_id}\n\n{encoded_body}")

    def _receive_frame(self) -> object:
        if self._socket is None:
            raise ProviderObservationRefused(ProviderErrorCode.PROVIDER_DISCONNECTED, "websocket_missing")
        try:
            return self._socket.recv(timeout=self.timeout_seconds)
        except TypeError:  # Some compliant test/different clients omit timeout.
            try:
                return self._socket.recv()
            except Exception as exc:
                raise ProviderObservationRefused(ProviderErrorCode.PROVIDER_DISCONNECTED, type(exc).__name__) from None
        except Exception as exc:
            raise ProviderObservationRefused(ProviderErrorCode.PROVIDER_DISCONNECTED, type(exc).__name__) from None

    @staticmethod
    def _frame_records(frame: object) -> tuple[Mapping[str, object], ...]:
        if not isinstance(frame, str) or not frame:
            raise ProviderObservationRefused(ProviderErrorCode.MALFORMED_PROVIDER_PAYLOAD, "websocket_frame")
        # SockJS open/heartbeat frames are not evidence; a caller must read
        # another packet rather than treating either as fresh market state.
        if frame in {"o", "h"}:
            raise ProviderObservationRefused(ProviderErrorCode.STALE_PROVIDER_STATE, "non_event_websocket_frame")
        encoded = frame[1:] if frame.startswith("a") else frame
        try:
            decoded = json.loads(encoded)
        except (TypeError, json.JSONDecodeError):
            raise ProviderObservationRefused(ProviderErrorCode.MALFORMED_PROVIDER_PAYLOAD, "websocket_json") from None
        values = decoded if isinstance(decoded, list) else [decoded]
        return tuple(_read_mapping(item, "websocket_record") for item in values)

    @classmethod
    def _require_success(cls, frame: object, operation: str) -> None:
        records = cls._frame_records(frame)
        if not any(record.get("s") in {200, 201, 202} for record in records):
            raise ProviderObservationRefused(ProviderErrorCode.AUTH_FAILED, operation)

    @classmethod
    def _event_payload(cls, frame: object, event_types: set[str]) -> Mapping[str, object]:
        records = cls._frame_records(frame)
        for record in records:
            if record.get("e") in event_types and isinstance(record.get("d"), Mapping):
                return MappingProxyType(dict(record))
        raise ProviderObservationRefused(ProviderErrorCode.MALFORMED_PROVIDER_PAYLOAD, "unexpected_websocket_event")


@dataclass(frozen=True)
class CommissioningSnapshot:
    environment: TradovateEnvironment
    capabilities: tuple[ProviderCapability, ...]
    account: AccountObservation | None
    contract: TradovateContract | None
    reconciliation: ReconciliationResult | None
    health: ProviderHealth
    startup_ready: bool
    blockers: tuple[str, ...]

    @property
    def payload(self) -> dict[str, object]:
        return {"schema": L3F_SCHEMA, "environment": self.environment.value, "capabilities": [item.payload() for item in self.capabilities], "account_alias": None if self.account is None else self.account.account_alias, "contract": None if self.contract is None else self.contract.payload(), "reconciliation": None if self.reconciliation is None else self.reconciliation.state.value, "health": {key.value: value.value for key, value in self.health.streams.items()}, "startup_ready": self.startup_ready, "blockers": list(self.blockers)}


class TradovateObservationService:
    """Orchestrates authenticated observation; it owns no execution authority."""

    def __init__(self, config: TradovateObservationConfig, client: TradovateReadOnlyClient, secrets: TradovateSecretProvider) -> None:
        if type(config) is not TradovateObservationConfig:
            raise ValueError("Exact L3-F observation configuration is required.")
        self.config, self.client, self.secrets = config, client, secrets
        self.health = ProviderHealthTracker()
        self.session: TradovateSession | None = None
        self.account: ProviderAccount | None = None
        self.contract: TradovateContract | None = None
        self.capabilities: tuple[ProviderCapability, ...] = ()
        self.reconciliation: ReconciliationResult | None = None

    def authenticate_and_discover(self, at: str) -> tuple[ProviderCapability, ...]:
        stamp = _utc(at, "Authentication time")
        try:
            session = self.client.authenticate(self.secrets.credentials(), self.config.endpoints)
        except ProviderObservationRefused:
            self.health.mark(ProviderStream.AUTH, StreamHealth.DISCONNECTED, stamp)
            raise
        except Exception as exc:
            self.health.mark(ProviderStream.AUTH, StreamHealth.DISCONNECTED, stamp)
            raise ProviderObservationRefused(ProviderErrorCode.AUTH_FAILED, type(exc).__name__) from None
        if session.environment is not self.config.environment:
            self.health.mark(ProviderStream.AUTH, StreamHealth.DISCONNECTED, stamp)
            raise ProviderObservationRefused(ProviderErrorCode.ENVIRONMENT_MISMATCH, "session_environment")
        self.session = session
        self.health.mark(ProviderStream.AUTH, StreamHealth.HEALTHY, stamp)
        try:
            raw = self.client.discover_capabilities(session, self.config.endpoints)
        except Exception as exc:
            raw = {"authentication": CapabilityStatus.SUPPORTED, "capability_discovery": CapabilityStatus.UNAVAILABLE}
            discovery_failure = type(exc).__name__
        else:
            discovery_failure = None
        defaults = ("authentication", "accounts", "contract_discovery", "quotes", "trades", "dom", "user_sync", "positions", "orders", "market_data")
        capabilities = [ProviderCapability(name, raw.get(name, CapabilityStatus.UNKNOWN), stamp, discovery_failure if name == "capability_discovery" else None) for name in defaults]
        self.capabilities = tuple(capabilities)
        return self.capabilities

    def select_account_and_contract(self, at: str) -> tuple[ProviderAccount, TradovateContract]:
        if self.session is None:
            raise ProviderObservationRefused(ProviderErrorCode.AUTH_FAILED, "authentication_required")
        stamp = _utc(at, "Provider discovery time")
        try:
            accounts = tuple(self.client.list_accounts(self.session, self.config.endpoints))
        except Exception as exc:
            raise ProviderObservationRefused(ProviderErrorCode.ACCOUNT_NOT_FOUND, type(exc).__name__) from None
        candidates = [account for account in accounts if self.config.provider_account_id is not None and account.provider_account_id == self.config.provider_account_id]
        if self.config.provider_account_id is None:
            raise ProviderObservationRefused(ProviderErrorCode.ACCOUNT_AMBIGUOUS, "explicit_provider_account_id_required")
        if not candidates:
            raise ProviderObservationRefused(ProviderErrorCode.ACCOUNT_NOT_FOUND, "configured_account_absent")
        if len(candidates) != 1:
            raise ProviderObservationRefused(ProviderErrorCode.ACCOUNT_AMBIGUOUS, "configured_account_not_unique")
        account = candidates[0]
        if account.environment is not self.config.environment:
            raise ProviderObservationRefused(ProviderErrorCode.ENVIRONMENT_MISMATCH, "account_environment")
        if not account.active:
            raise ProviderObservationRefused(ProviderErrorCode.ACCOUNT_NOT_FOUND, "configured_account_inactive")
        if self.config.contract_symbol is None:
            raise ProviderObservationRefused(ProviderErrorCode.CONTRACT_NOT_FOUND, "explicit_contract_required")
        try:
            contract = self.client.resolve_contract(self.session, self.config.endpoints, self.config.contract_symbol)
        except ProviderObservationRefused:
            raise
        except Exception as exc:
            raise ProviderObservationRefused(ProviderErrorCode.CONTRACT_NOT_FOUND, type(exc).__name__) from None
        if contract.symbol != self.config.contract_symbol:
            raise ProviderObservationRefused(ProviderErrorCode.CONTRACT_NOT_FOUND, "configured_contract_mismatch")
        self.account, self.contract = account, contract
        return account, contract

    def startup_reconcile(self, at: str, local: LocalObservedState | None = None) -> CommissioningSnapshot:
        stamp = _utc(at, "Startup reconciliation time")
        try:
            if self.session is None:
                self.authenticate_and_discover(stamp)
            if self.account is None or self.contract is None:
                self.select_account_and_contract(stamp)
            assert self.session is not None and self.account is not None and self.contract is not None
            account = self.client.observe_account(self.session, self.config.endpoints, self.account, self.config.account_alias, stamp)
            position = self.client.observe_position(self.session, self.config.endpoints, self.account, self.config.account_alias, self.contract, stamp)
            orders = tuple(self.client.observe_orders(self.session, self.config.endpoints, self.account, self.config.account_alias, self.contract, stamp))
            if account.provider_account_id != self.account.provider_account_id or position.account_alias != self.config.account_alias or any(item.account_alias != self.config.account_alias for item in orders):
                raise ProviderObservationRefused(ProviderErrorCode.RECONCILIATION_MISMATCH, "account_scope")
            self.health.mark(ProviderStream.USER_DATA, StreamHealth.HEALTHY, stamp)
            self.reconciliation = reconcile_provider_truth(local=local, position=position, orders=orders, observed_at=stamp, maximum_age=self.config.stale_after)
            blockers = self.config.risk_profile.future_live_readiness_blockers + (() if self.reconciliation.authoritative else (self.reconciliation.state.value.lower(),))
            return CommissioningSnapshot(self.config.environment, self.capabilities, account, self.contract, self.reconciliation, self.health.snapshot(), self.reconciliation.authoritative, blockers)
        except ProviderObservationRefused:
            self.health.mark(ProviderStream.USER_DATA, StreamHealth.DISCONNECTED, stamp)
            raise
        except Exception as exc:
            self.health.mark(ProviderStream.USER_DATA, StreamHealth.DISCONNECTED, stamp)
            raise ProviderObservationRefused(ProviderErrorCode.STALE_PROVIDER_STATE, type(exc).__name__) from None

    def mark_market_data_healthy(self, at: str) -> ProviderHealth:
        return self.health.mark(ProviderStream.MARKET_DATA, StreamHealth.HEALTHY, at)

    def renew_session(self, at: str) -> None:
        """Reauthenticate explicitly; a renewal never keeps an old token authoritative."""
        self.session = None
        self.health.mark_token_expired(at)
        self.authenticate_and_discover(at)


class TradovateMarketDataAdapter(MarketDataProviderAdapter):
    """Strict conversion from sanitized Tradovate MD packets to frozen L3-B events."""

    def __init__(self, contract: TradovateContract) -> None:
        if type(contract) is not TradovateContract:
            raise ValueError("Tradovate market-data adapter requires resolved contract.")
        self.contract = contract
        self._source = MarketDataSource(TRADOVATE_PROVIDER, TRADOVATE_FEED)

    @property
    def source(self) -> MarketDataSource:
        return self._source

    def normalize(self, raw_event: RawProviderEvent) -> tuple[QuoteEvent | TradeEvent | BookSnapshotEvent, ...]:
        if type(raw_event) is not RawProviderEvent or raw_event.source != self.source:
            raise ProviderObservationRefused(ProviderErrorCode.MALFORMED_PROVIDER_PAYLOAD, "raw_source")
        payload = raw_event.payload
        body = payload.get("d", payload)
        body = _read_mapping(body, "market_data_body")
        result: list[QuoteEvent | TradeEvent | BookSnapshotEvent] = []
        for index, row in enumerate(self._rows(body, "quotes", "quote")):
            result.append(self._quote(raw_event, row, index))
        offset = len(result)
        for index, row in enumerate(self._rows(body, "trades", "trade")):
            result.append(self._trade(raw_event, row, offset + index))
        offset = len(result)
        for index, row in enumerate(self._rows(body, "doms", "dom", "depth")):
            result.append(self._depth(raw_event, row, offset + index))
        if not result:
            raise ProviderObservationRefused(ProviderErrorCode.MARKET_DATA_UNAVAILABLE, "no_supported_market_records")
        return tuple(result)

    @staticmethod
    def _rows(body: Mapping[str, object], *names: str) -> tuple[Mapping[str, object], ...]:
        for name in names:
            if name not in body:
                continue
            value = body[name]
            values = value if isinstance(value, list) else [value]
            return tuple(_read_mapping(item, f"{name}_record") for item in values)
        return ()

    def _require_contract(self, row: Mapping[str, object]) -> None:
        provided = row.get("contractId", row.get("contract_id"))
        if provided != self.contract.provider_contract_id:
            raise ProviderObservationRefused(ProviderErrorCode.CONTRACT_NOT_FOUND, "market_data_contract_mismatch")

    def _header(self, raw: RawProviderEvent, stream: MarketStream, row: Mapping[str, object], index: int) -> EventHeader:
        self._require_contract(row)
        timestamp = row.get("timestamp", row.get("time", row.get("providerTime")))
        if timestamp is None:
            raise ProviderObservationRefused(ProviderErrorCode.MALFORMED_PROVIDER_PAYLOAD, "provider_timestamp_missing")
        provider_time = _utc(timestamp, "Provider event time")
        sequence_raw = row.get("seq", row.get("sequence"))
        sequence = None if sequence_raw is None else int(sequence_raw)
        if sequence is not None and sequence < 0:
            raise ProviderObservationRefused(ProviderErrorCode.MALFORMED_PROVIDER_PAYLOAD, "negative_sequence")
        provider_event_id = row.get("id", row.get("eventId"))
        event_identity = f"{raw.raw_event_id}:{stream.value}:{index}"
        return EventHeader(event_id="l3f-" + canonical_hash({"raw": raw.raw_event_id, "stream": stream.value, "index": index})[:32], source=self.source, instrument=self.contract.canonical_contract, timestamps=EventTimestamps(raw.received_at, provider_time=provider_time), stream=stream, raw_event_id=event_identity, raw_payload_hash=raw.payload_hash, provider_sequence=sequence, provider_event_id=None if provider_event_id is None else str(provider_event_id))

    def _quote(self, raw: RawProviderEvent, row: Mapping[str, object], index: int) -> QuoteEvent:
        try:
            return QuoteEvent(self._header(raw, MarketStream.QUOTE, row, index), _decimal(row.get("bid"), "Bid", positive=True), _decimal(row.get("ask"), "Ask", positive=True), _quantity(row.get("bidSize", row.get("bid_quantity")), "Bid size"), _quantity(row.get("askSize", row.get("ask_quantity")), "Ask size"))
        except (ValueError, TypeError) as exc:
            raise ProviderObservationRefused(ProviderErrorCode.MALFORMED_PROVIDER_PAYLOAD, "quote") from exc

    def _trade(self, raw: RawProviderEvent, row: Mapping[str, object], index: int) -> TradeEvent:
        side_text = str(row.get("aggressorSide", row.get("side", "UNKNOWN"))).upper()
        side = AggressorSide.BUY if side_text in {"BUY", "B", "ASK"} else AggressorSide.SELL if side_text in {"SELL", "S", "BID"} else AggressorSide.UNKNOWN
        provenance = AggressorProvenance.PROVIDER if side is not AggressorSide.UNKNOWN else AggressorProvenance.UNAVAILABLE
        try:
            return TradeEvent(self._header(raw, MarketStream.TRADE, row, index), _decimal(row.get("price"), "Trade price", positive=True), _quantity(row.get("size", row.get("qty")), "Trade size"), side, provenance)
        except (ValueError, TypeError) as exc:
            raise ProviderObservationRefused(ProviderErrorCode.MALFORMED_PROVIDER_PAYLOAD, "trade") from exc

    def _depth(self, raw: RawProviderEvent, row: Mapping[str, object], index: int) -> BookSnapshotEvent:
        try:
            bids = self._levels(row.get("bids"), reverse=True)
            asks = self._levels(row.get("asks"), reverse=False)
            if not bids and not asks:
                raise ValueError("no depth levels")
            return BookSnapshotEvent(self._header(raw, MarketStream.DEPTH, row, index), bids, asks)
        except (ValueError, TypeError) as exc:
            raise ProviderObservationRefused(ProviderErrorCode.MALFORMED_PROVIDER_PAYLOAD, "aggregated_dom") from exc

    @staticmethod
    def _levels(value: object, *, reverse: bool) -> tuple[BookLevel, ...]:
        if not isinstance(value, list):
            raise ValueError("depth side must be list")
        levels = tuple(BookLevel(_decimal(_read_mapping(item, "depth_level").get("price"), "Depth price", positive=True), _quantity(_read_mapping(item, "depth_level").get("size", _read_mapping(item, "depth_level").get("quantity")), "Depth size")) for item in value)
        return tuple(sorted(levels, key=lambda level: level.price, reverse=reverse))


def capture_sanitized_fixture(raw_event: RawProviderEvent) -> dict[str, object]:
    """Safe deterministic fixture representation: hashes and sanitized payload only."""
    return {"schema": "lane-iii-phase-f-provider-fixture-v1", "raw_event_id": raw_event.raw_event_id, "source": raw_event.source.payload(), "received_at": raw_event.received_at, "provider_event_id": raw_event.provider_event_id, "payload": redact_sensitive(dict(raw_event.payload)), "payload_hash": raw_event.payload_hash}


def live_execution_artifact_refused() -> None:
    """A sentinel for callers: L3-F cannot label anything LIVE_EXECUTION."""
    raise ProviderObservationRefused(ProviderErrorCode.READ_ONLY_VIOLATION, "live_execution_authority_absent")
