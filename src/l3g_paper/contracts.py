"""Immutable Lane III-G experimental-paper authority contracts.

Nothing in this module can represent live-capital authority.  The concrete
account binding and capability manifest are intentionally closed over Sim101,
MNQ SEP26, and one contract; widening any of them requires a source change.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from enum import StrEnum
import json
from types import MappingProxyType
from typing import Mapping, Protocol, runtime_checkable

from src.lane_iii.contracts import canonical_hash, normalized_utc
from .sessions import (
    PaperSessionKind,
    UNSPECIFIED_OFF_SESSION_CONTEXT,
    context_from_identity,
    session_family,
)


PAPER_POLICY_SCHEMA = "lane-iii-phase-g-paper-policy-v3"
PAPER_RISK_SCHEMA = "lane-iii-phase-g-paper-risk-v2"
PAPER_RECORD_SCHEMA = "lane-iii-phase-g-paper-record-v1"
PAPER_POLICY_ID = "l3g-ny-high-confluence-commissioning-policy-v1"
PAPER_ENTRY_PROFILE = "NY_HIGH_CONFLUENCE_COMMISSIONING"
PAPER_ENTRY_PROFILE_VERSION = "NY_HIGH_CONFLUENCE_COMMISSIONING_V1"
PAPER_RISK_PROFILE_ID = "l3g-ny-high-confluence-commissioning-risk-v1"
PAPER_MODE = "PAPER_SIM101"
PAPER_ACCOUNT = "Sim101"
PAPER_ACCOUNT_CLASS = "LOCAL_SIMULATION"
PAPER_INSTRUMENT = "MNQ SEP26"
PAPER_CANONICAL_CONTRACT = "MNQU6"
PAPER_NATIVE_CONTRACT = "MNQ SEP26"
PAPER_MAXIMUM_QUANTITY = 1


class PaperDecisionKind(StrEnum):
    NO_TRADE = "NO_TRADE"
    LONG = "LONG"
    SHORT = "SHORT"
    EXIT = "EXIT"


class HypothesisKind(StrEnum):
    BULLISH_REVERSAL = "BULLISH_REVERSAL"
    BEARISH_CONTINUATION = "BEARISH_CONTINUATION"


class PaperDirection(StrEnum):
    LONG = "LONG"
    SHORT = "SHORT"
    FLAT = "FLAT"


class PaperEntryOwner(StrEnum):
    """Authority currently reserved at the one paper-entry admission boundary."""

    NONE = "NONE"
    STRATEGY = "STRATEGY"
    COMMISSIONING = "COMMISSIONING"


class PaperSourceQuality(StrEnum):
    PROVISIONAL_CONTIGUOUS_LOCAL_CALLBACKS = "PROVISIONAL_CONTIGUOUS_LOCAL_CALLBACKS"
    UNUSABLE = "UNUSABLE"


class SequenceAuthority(StrEnum):
    LOCAL_CALLBACK_ORDER_ONLY = "LOCAL_CALLBACK_ORDER_ONLY"


class BookCompleteness(StrEnum):
    UNVERIFIED = "UNVERIFIED"


class ExecutionAction(StrEnum):
    ENTER_LONG = "ENTER_LONG"
    ENTER_SHORT = "ENTER_SHORT"
    EXIT = "EXIT"
    EMERGENCY_FLATTEN = "EMERGENCY_FLATTEN"
    CANCEL_OWNED_ORDERS = "CANCEL_OWNED_ORDERS"
    HEARTBEAT = "HEARTBEAT"
    RECONCILE = "RECONCILE"


class PaperRuntimeState(StrEnum):
    DISABLED = "DISABLED"
    STARTING = "STARTING"
    WAITING_FOR_EXECUTION_BRIDGE = "WAITING_FOR_EXECUTION_BRIDGE"
    RECONCILING = "RECONCILING"
    READY_DISARMED = "READY_DISARMED"
    PAPER_RUNNING = "PAPER_RUNNING"
    ARMED_FLAT = "ARMED_FLAT"
    ENTRY_PENDING = "ENTRY_PENDING"
    LONG = "LONG"
    SHORT = "SHORT"
    EXIT_PENDING = "EXIT_PENDING"
    PAUSED = "PAUSED"
    LOCKED_OUT = "LOCKED_OUT"
    FAULTED = "FAULTED"
    STOPPING = "STOPPING"
    STOPPED = "STOPPED"


class EvidenceFamily(StrEnum):
    STRUCTURAL_CONTEXT = "STRUCTURAL_CONTEXT"
    ORDER_FLOW = "ORDER_FLOW"
    RESTING_LIQUIDITY = "RESTING_LIQUIDITY"
    VOLATILITY_CONTEXT = "VOLATILITY_CONTEXT"
    MARKET_REGIME = "MARKET_REGIME"


def _utc(value: str, field_name: str) -> str:
    return normalized_utc(value, field_name)


def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _jsonable(value: object) -> object:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, StrEnum):
        return value.value
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_jsonable(item) for item in value]
    if isinstance(value, list):
        return [_jsonable(item) for item in value]
    return value


def canonical_json(payload: Mapping[str, object]) -> bytes:
    return json.dumps(_jsonable(payload), sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")


def _validate_session_identity(
    session_kind: PaperSessionKind,
    session_id: str,
    trade_date: str,
    session_profile_hash: str,
    session_generation: int,
) -> None:
    context_from_identity(session_kind, session_id, trade_date, session_profile_hash, session_generation)


@dataclass(frozen=True)
class PaperPolicyArtifact:
    schema: str = PAPER_POLICY_SCHEMA
    policy_id: str = PAPER_POLICY_ID
    entry_profile: str = PAPER_ENTRY_PROFILE
    entry_profile_version: str = PAPER_ENTRY_PROFILE_VERSION
    authority: str = "EXPERIMENTAL_PAPER_DIRECTION_ONLY"
    instrument: str = "MNQ"
    native_contract: str = PAPER_NATIVE_CONTRACT
    canonical_contract: str = PAPER_CANONICAL_CONTRACT
    scientific_eligibility: bool = False
    entry_session_kind: PaperSessionKind = PaperSessionKind.NEW_YORK_RTH
    sequence_authority: SequenceAuthority = SequenceAuthority.LOCAL_CALLBACK_ORDER_ONLY
    book_completeness: BookCompleteness = BookCompleteness.UNVERIFIED
    allowed_hypotheses: tuple[HypothesisKind, ...] = (
        HypothesisKind.BULLISH_REVERSAL,
        HypothesisKind.BEARISH_CONTINUATION,
    )
    classified_flow_window: int = 8
    minimum_classified_trades: int = 3
    structural_window: int = 8
    replenishment_count: int = 2
    structural_evidence_lifetime_seconds: int = 90
    flow_evidence_lifetime_seconds: int = 30
    liquidity_evidence_lifetime_seconds: int = 20
    hypothesis_idle_lifetime_seconds: int = 90
    hypothesis_maximum_lifetime_seconds: int = 600
    entry_support_threshold: Decimal = Decimal("0.675")
    entry_dominance_margin: Decimal = Decimal("0.10")
    retention_support_threshold: Decimal = Decimal("0.55")
    retention_dominance_margin: Decimal = Decimal("0.025")
    entry_family_count: int = 3
    retention_family_count: int = 2
    decision_ttl_seconds: int = 5
    reentry_cooldown_seconds: int = 3600
    structural_strength: Decimal = Decimal("0.50")
    score_denominator: Decimal = Decimal("10")
    session_timezone: str = "America/New_York"
    provisional_session_boundary: str = "00:00"

    def __post_init__(self) -> None:
        if (
            self.schema != PAPER_POLICY_SCHEMA
            or self.policy_id != PAPER_POLICY_ID
            or self.entry_profile != PAPER_ENTRY_PROFILE
            or self.entry_profile_version != PAPER_ENTRY_PROFILE_VERSION
        ):
            raise ValueError("The paper policy identity is immutable.")
        if self.authority != "EXPERIMENTAL_PAPER_DIRECTION_ONLY" or self.scientific_eligibility:
            raise ValueError("The paper policy cannot acquire scientific or execution authority.")
        if self.native_contract != PAPER_NATIVE_CONTRACT or self.canonical_contract != PAPER_CANONICAL_CONTRACT:
            raise ValueError("The paper policy requires exact MNQ SEP26 identity.")
        if self.allowed_hypotheses != (
            HypothesisKind.BULLISH_REVERSAL,
            HypothesisKind.BEARISH_CONTINUATION,
        ):
            raise ValueError("The paper directional universe is fixed.")
        if (
            self.entry_session_kind is not PaperSessionKind.NEW_YORK_RTH
            or self.entry_support_threshold != Decimal("0.675")
            or self.entry_dominance_margin != Decimal("0.10")
            or self.retention_support_threshold != Decimal("0.55")
            or self.retention_dominance_margin != Decimal("0.025")
            or self.entry_family_count != 3
            or self.retention_family_count != 2
            or self.reentry_cooldown_seconds != 3600
        ):
            raise ValueError("The NY high-confluence commissioning policy tuning is immutable.")

    def payload(self) -> dict[str, object]:
        return _jsonable(asdict(self))  # type: ignore[return-value]

    @property
    def configuration_hash(self) -> str:
        return canonical_hash(self.payload())


@dataclass(frozen=True)
class PaperRiskProfile:
    schema: str = PAPER_RISK_SCHEMA
    profile_id: str = PAPER_RISK_PROFILE_ID
    mode: str = PAPER_MODE
    account_name: str = PAPER_ACCOUNT
    account_class: str = PAPER_ACCOUNT_CLASS
    instrument: str = PAPER_INSTRUMENT
    canonical_contract: str = PAPER_CANONICAL_CONTRACT
    entry_session_kind: PaperSessionKind = PaperSessionKind.NEW_YORK_RTH
    maximum_absolute_position: int = 1
    maximum_entry_quantity: int = 1
    maximum_pending_entries: int = 1
    maximum_simultaneous_thesis: int = 1
    pyramiding: bool = False
    averaging: bool = False
    same_event_reversal: bool = False
    entry_order_type: str = "MARKET"
    normal_exit: str = "FLATTEN_OWNED_INSTRUMENT"
    protective_order_type: str = "STOP_MARKET"
    protective_stop_distance_points: Decimal = Decimal("25.00")
    maximum_trade_risk_dollars: Decimal = Decimal("50.00")
    daily_loss_limit_dollars: Decimal = Decimal("200.00")
    maximum_position_age_seconds: int = 3600
    entry_session_start: str = "09:35"
    entry_session_end: str = "15:30"
    hard_flat_deadline: str = "15:58"
    session_timezone: str = "America/New_York"
    reentry_cooldown_seconds: int = 3600
    maximum_session_entries: int = 1
    maximum_consecutive_losses: int = 1
    maximum_entry_slippage_points: Decimal = Decimal("2.00")
    quote_maximum_age_seconds: int = 2
    classified_trade_maximum_age_seconds: int = 5
    depth_mutation_maximum_age_seconds: int = 5
    point_value_dollars: Decimal = Decimal("2.00")
    tick_size: Decimal = Decimal("0.25")
    tick_value_dollars: Decimal = Decimal("0.50")
    paper_only: bool = True
    approved_for_live: bool = False

    def __post_init__(self) -> None:
        identity = (self.schema, self.profile_id, self.mode, self.account_name, self.account_class, self.instrument, self.canonical_contract)
        required = (PAPER_RISK_SCHEMA, PAPER_RISK_PROFILE_ID, PAPER_MODE, PAPER_ACCOUNT, PAPER_ACCOUNT_CLASS, PAPER_INSTRUMENT, PAPER_CANONICAL_CONTRACT)
        if identity != required:
            raise ValueError("The Lane III-G risk identity is sealed to Sim101/MNQ SEP26.")
        if any((self.maximum_absolute_position != 1, self.maximum_entry_quantity != 1, not self.paper_only, self.approved_for_live)):
            raise ValueError("The paper risk profile cannot represent live or multi-contract authority.")
        if self.pyramiding or self.averaging or self.same_event_reversal:
            raise ValueError("Pyramiding, averaging, and same-event reversal are forbidden.")
        if (
            self.entry_session_kind is not PaperSessionKind.NEW_YORK_RTH
            or self.maximum_position_age_seconds != 3600
            or self.reentry_cooldown_seconds != 3600
            or self.maximum_session_entries != 1
            or self.maximum_consecutive_losses != 1
        ):
            raise ValueError("The NY high-confluence commissioning risk limits are immutable.")

    def payload(self) -> dict[str, object]:
        return _jsonable(asdict(self))  # type: ignore[return-value]

    @property
    def configuration_hash(self) -> str:
        return canonical_hash(self.payload())


@dataclass(frozen=True)
class ExecutionAccountBinding:
    account_name: str = PAPER_ACCOUNT
    account_class: str = PAPER_ACCOUNT_CLASS
    instrument: str = PAPER_INSTRUMENT
    canonical_contract: str = PAPER_CANONICAL_CONTRACT
    maximum_quantity: int = PAPER_MAXIMUM_QUANTITY
    paper_only: bool = True
    live_capital: bool = False

    def __post_init__(self) -> None:
        if (
            self.account_name,
            self.account_class,
            self.instrument,
            self.canonical_contract,
            self.maximum_quantity,
            self.paper_only,
            self.live_capital,
        ) != (PAPER_ACCOUNT, PAPER_ACCOUNT_CLASS, PAPER_INSTRUMENT, PAPER_CANONICAL_CONTRACT, 1, True, False):
            raise ValueError("No configurable or live account binding exists in Lane III-G.")

    def payload(self) -> dict[str, object]:
        return _jsonable(asdict(self))  # type: ignore[return-value]

    @property
    def binding_hash(self) -> str:
        return canonical_hash(self.payload())


@dataclass(frozen=True)
class ExecutionCapabilityManifest:
    adapter: str = "NinjaTraderSim101PaperAdapter"
    mode: str = PAPER_MODE
    account_class: str = PAPER_ACCOUNT_CLASS
    account_name: str = PAPER_ACCOUNT
    instrument: str = PAPER_INSTRUMENT
    maximum_quantity: int = 1
    order_mutation: str = "PAPER_ONLY"
    live_capital: bool = False

    def __post_init__(self) -> None:
        if asdict(self) != {
            "adapter": "NinjaTraderSim101PaperAdapter",
            "mode": PAPER_MODE,
            "account_class": PAPER_ACCOUNT_CLASS,
            "account_name": PAPER_ACCOUNT,
            "instrument": PAPER_INSTRUMENT,
            "maximum_quantity": 1,
            "order_mutation": "PAPER_ONLY",
            "live_capital": False,
        }:
            raise ValueError("Only the compiled Sim101 paper capability may be registered.")


@dataclass(frozen=True)
class PaperEvidence:
    evidence_id: str
    hypothesis_kind: HypothesisKind
    family: EvidenceFamily
    label: str
    strength: Decimal
    supports: bool
    observed_at: str
    expires_at: str
    source_observation_ids: tuple[str, ...]
    source_local_sequences: tuple[int, ...]
    source_payload_hashes: tuple[str, ...]
    quality: PaperSourceQuality = PaperSourceQuality.PROVISIONAL_CONTIGUOUS_LOCAL_CALLBACKS
    sequence_authority: SequenceAuthority = SequenceAuthority.LOCAL_CALLBACK_ORDER_ONLY
    book_completeness: BookCompleteness = BookCompleteness.UNVERIFIED
    scientific_eligibility: bool = False
    blocking: bool = False
    session_kind: PaperSessionKind = UNSPECIFIED_OFF_SESSION_CONTEXT.session_kind
    session_id: str = UNSPECIFIED_OFF_SESSION_CONTEXT.session_id
    trade_date: str = UNSPECIFIED_OFF_SESSION_CONTEXT.trade_date
    session_profile_hash: str = UNSPECIFIED_OFF_SESSION_CONTEXT.session_profile_hash
    session_generation: int = UNSPECIFIED_OFF_SESSION_CONTEXT.session_generation
    source_session_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _utc(self.observed_at, "Paper evidence time")
        _utc(self.expires_at, "Paper evidence expiry")
        if not self.evidence_id.startswith("l3g-pe-"):
            raise ValueError("Paper evidence requires the l3g namespace.")
        if not Decimal("0") < self.strength <= Decimal("1"):
            raise ValueError("Evidence strength must be within (0, 1].")
        if not self.source_observation_ids or len(self.source_observation_ids) != len(self.source_local_sequences) or len(self.source_observation_ids) != len(self.source_payload_hashes):
            raise ValueError("Paper evidence provenance arrays must be non-empty and aligned.")
        if self.scientific_eligibility or self.sequence_authority is not SequenceAuthority.LOCAL_CALLBACK_ORDER_ONLY or self.book_completeness is not BookCompleteness.UNVERIFIED:
            raise ValueError("Provisional paper evidence cannot be promoted to scientific truth.")
        _validate_session_identity(self.session_kind, self.session_id, self.trade_date, self.session_profile_hash, self.session_generation)
        source_sessions = self.source_session_ids or tuple(self.session_id for _ in self.source_observation_ids)
        if len(source_sessions) != len(self.source_observation_ids) or set(source_sessions) != {self.session_id}:
            raise ValueError("CROSS_SESSION_SOURCE_SET")
        object.__setattr__(self, "source_session_ids", tuple(source_sessions))

    def payload(self) -> dict[str, object]:
        result = asdict(self)
        result["session_family"] = session_family(self.session_kind).value
        return _jsonable(result)  # type: ignore[return-value]


@dataclass(frozen=True)
class PaperDecision:
    paper_decision_id: str
    paper_policy_id: str
    paper_policy_hash: str
    decision: PaperDecisionKind
    created_at: str
    expires_at: str
    hypothesis_kind: HypothesisKind | None
    direction: PaperDirection
    relative_support: Decimal
    family_summary: Mapping[str, object]
    source_observation_ids: tuple[str, ...]
    source_local_sequences: tuple[int, ...]
    source_payload_hashes: tuple[str, ...]
    sequence_authority: SequenceAuthority
    book_completeness: BookCompleteness
    scientific_eligibility: bool
    reason_code: str
    session_kind: PaperSessionKind = UNSPECIFIED_OFF_SESSION_CONTEXT.session_kind
    session_id: str = UNSPECIFIED_OFF_SESSION_CONTEXT.session_id
    trade_date: str = UNSPECIFIED_OFF_SESSION_CONTEXT.trade_date
    session_profile_hash: str = UNSPECIFIED_OFF_SESSION_CONTEXT.session_profile_hash
    session_generation: int = UNSPECIFIED_OFF_SESSION_CONTEXT.session_generation
    commissioning: bool = False
    strategy_generated: bool = True
    scientific_evidence: bool = False

    def __post_init__(self) -> None:
        if not self.paper_decision_id.startswith("l3g-pd-"):
            raise ValueError("Paper decisions require the l3g-pd namespace.")
        if self.paper_policy_id != PAPER_POLICY_ID or not self.paper_policy_hash:
            raise ValueError("Paper decision policy identity is required.")
        _utc(self.created_at, "Paper decision time")
        _utc(self.expires_at, "Paper decision expiry")
        if self.scientific_eligibility or self.sequence_authority is not SequenceAuthority.LOCAL_CALLBACK_ORDER_ONLY or self.book_completeness is not BookCompleteness.UNVERIFIED:
            raise ValueError("Paper decisions are never scientifically eligible.")
        if self.commissioning:
            if self.strategy_generated or self.scientific_evidence or self.hypothesis_kind is not None:
                raise ValueError("Commissioning decisions must remain non-strategy and non-scientific.")
            if self.decision in {PaperDecisionKind.LONG, PaperDecisionKind.SHORT}:
                expected_direction = PaperDirection.LONG if self.decision is PaperDecisionKind.LONG else PaperDirection.SHORT
                if self.direction is not expected_direction:
                    raise ValueError("Commissioning decision direction must match its sealed entry action.")
            elif self.decision is not PaperDecisionKind.EXIT or self.direction is not PaperDirection.FLAT:
                raise ValueError("Commissioning decisions may only create a sealed entry or a flat closing exit.")
        else:
            if not self.strategy_generated or self.scientific_evidence:
                raise ValueError("Strategy decisions cannot claim scientific evidence or lose their strategy provenance.")
            if self.decision is PaperDecisionKind.LONG and (self.hypothesis_kind is not HypothesisKind.BULLISH_REVERSAL or self.direction is not PaperDirection.LONG):
                raise ValueError("Only bullish reversal may create a long paper decision.")
            if self.decision is PaperDecisionKind.SHORT and (self.hypothesis_kind is not HypothesisKind.BEARISH_CONTINUATION or self.direction is not PaperDirection.SHORT):
                raise ValueError("Only bearish continuation may create a short paper decision.")
        if self.decision in {PaperDecisionKind.NO_TRADE, PaperDecisionKind.EXIT} and self.direction is not PaperDirection.FLAT:
            raise ValueError("NO_TRADE and EXIT have a flat target direction.")
        _validate_session_identity(self.session_kind, self.session_id, self.trade_date, self.session_profile_hash, self.session_generation)
        object.__setattr__(self, "family_summary", MappingProxyType(dict(self.family_summary)))

    def payload(self) -> dict[str, object]:
        result = dict(self.__dict__)
        result["family_summary"] = dict(self.family_summary)
        result["session_family"] = session_family(self.session_kind).value
        return _jsonable(result)  # type: ignore[return-value]


@dataclass(frozen=True)
class PaperExecutionIntent:
    intent_id: str
    paper_decision_id: str
    target_position: PaperDirection
    requested_quantity: int
    instrument: str
    created_at: str
    expires_at: str
    policy_hash: str
    reference_bid: Decimal | None
    reference_ask: Decimal | None
    reference_last: Decimal | None
    session_kind: PaperSessionKind = UNSPECIFIED_OFF_SESSION_CONTEXT.session_kind
    session_id: str = UNSPECIFIED_OFF_SESSION_CONTEXT.session_id
    trade_date: str = UNSPECIFIED_OFF_SESSION_CONTEXT.trade_date
    session_profile_hash: str = UNSPECIFIED_OFF_SESSION_CONTEXT.session_profile_hash
    session_generation: int = UNSPECIFIED_OFF_SESSION_CONTEXT.session_generation
    commissioning: bool = False
    strategy_generated: bool = True
    scientific_evidence: bool = False

    def __post_init__(self) -> None:
        if not self.intent_id.startswith("l3g-pi-") or not self.paper_decision_id.startswith("l3g-pd-"):
            raise ValueError("Paper intent identity is invalid.")
        if self.instrument != PAPER_INSTRUMENT or self.requested_quantity != 1:
            raise ValueError("Paper intents are sealed to one MNQ SEP26 contract.")
        _utc(self.created_at, "Paper intent time")
        _utc(self.expires_at, "Paper intent expiry")
        if self.target_position not in {PaperDirection.LONG, PaperDirection.SHORT, PaperDirection.FLAT}:
            raise ValueError("Unsupported target position.")
        if self.commissioning:
            if self.strategy_generated or self.scientific_evidence:
                raise ValueError("Commissioning intents must remain non-strategy and non-scientific.")
        elif not self.strategy_generated or self.scientific_evidence:
            raise ValueError("Strategy intents must retain non-scientific strategy provenance.")
        _validate_session_identity(self.session_kind, self.session_id, self.trade_date, self.session_profile_hash, self.session_generation)

    def payload(self) -> dict[str, object]:
        result = asdict(self)
        result["session_family"] = session_family(self.session_kind).value
        return _jsonable(result)  # type: ignore[return-value]


@dataclass(frozen=True)
class PaperRiskGrant:
    grant_id: str
    intent_id: str
    risk_profile_hash: str
    account_binding_hash: str
    granted: bool
    reason_codes: tuple[str, ...]
    evaluated_at: str
    expires_at: str
    current_position: PaperDirection
    current_working_orders: int
    daily_realized_pnl: Decimal
    daily_unrealized_pnl: Decimal
    session_entry_count: int
    consecutive_losses: int
    paper_only: bool = True
    approved_for_live: bool = False
    session_kind: PaperSessionKind = UNSPECIFIED_OFF_SESSION_CONTEXT.session_kind
    session_id: str = UNSPECIFIED_OFF_SESSION_CONTEXT.session_id
    trade_date: str = UNSPECIFIED_OFF_SESSION_CONTEXT.trade_date
    session_profile_hash: str = UNSPECIFIED_OFF_SESSION_CONTEXT.session_profile_hash
    session_generation: int = UNSPECIFIED_OFF_SESSION_CONTEXT.session_generation
    commissioning: bool = False
    strategy_generated: bool = True
    scientific_evidence: bool = False

    def __post_init__(self) -> None:
        if not self.grant_id.startswith("l3g-pg-") or not self.intent_id.startswith("l3g-pi-"):
            raise ValueError("Paper risk-grant identity is invalid.")
        _utc(self.evaluated_at, "Paper risk evaluation time")
        _utc(self.expires_at, "Paper risk grant expiry")
        if not self.reason_codes or not self.paper_only or self.approved_for_live:
            raise ValueError("A risk grant must retain explicit paper-only reasons and authority.")
        if min(self.current_working_orders, self.session_entry_count, self.consecutive_losses) < 0:
            raise ValueError("Paper risk counters cannot be negative.")
        if self.commissioning:
            if self.strategy_generated or self.scientific_evidence:
                raise ValueError("Commissioning grants must remain non-strategy and non-scientific.")
        elif not self.strategy_generated or self.scientific_evidence:
            raise ValueError("Strategy grants must retain non-scientific strategy provenance.")
        _validate_session_identity(self.session_kind, self.session_id, self.trade_date, self.session_profile_hash, self.session_generation)

    def payload(self) -> dict[str, object]:
        result = asdict(self)
        result["session_family"] = session_family(self.session_kind).value
        return _jsonable(result)  # type: ignore[return-value]

    def valid_at(self, at: str) -> bool:
        use_time = datetime.fromisoformat(_utc(at, "Grant use time").replace("Z", "+00:00"))
        expiry = datetime.fromisoformat(self.expires_at.replace("Z", "+00:00"))
        return self.granted and use_time <= expiry


@dataclass(frozen=True)
class PaperSessionArmGrant:
    """An operator grant scoped to one exact market session, never a day."""

    session_kind: PaperSessionKind
    session_id: str
    trade_date: str
    session_profile_hash: str
    session_generation: int
    granted_at: str
    expires_at: str

    def __post_init__(self) -> None:
        _validate_session_identity(self.session_kind, self.session_id, self.trade_date, self.session_profile_hash, self.session_generation)
        _utc(self.granted_at, "Paper session arm grant time")
        _utc(self.expires_at, "Paper session arm grant expiry")
        if self.session_kind is PaperSessionKind.OFF_SESSION:
            raise ValueError("OFF_SESSION cannot be armed.")

    def valid_at(self, at: str) -> bool:
        moment = datetime.fromisoformat(_utc(at, "Paper session arm use time").replace("Z", "+00:00"))
        return moment < datetime.fromisoformat(self.expires_at.replace("Z", "+00:00"))

    def payload(self) -> dict[str, object]:
        result = asdict(self)
        result["session_family"] = session_family(self.session_kind).value
        return _jsonable(result)  # type: ignore[return-value]


@dataclass(frozen=True)
class PaperExecutionCommand:
    command_id: str
    command_sequence: int
    session_id: str
    intent_id: str
    decision_id: str
    action: ExecutionAction
    account_name: str
    account_class: str
    instrument: str
    quantity: int
    expected_position: PaperDirection
    created_at: str
    expires_at: str
    policy_hash: str
    risk_profile_hash: str
    account_binding_hash: str
    reason_code: str
    risk_grant_id: str
    signature: str = ""
    session_kind: PaperSessionKind = UNSPECIFIED_OFF_SESSION_CONTEXT.session_kind
    trade_date: str = UNSPECIFIED_OFF_SESSION_CONTEXT.trade_date
    session_profile_hash: str = UNSPECIFIED_OFF_SESSION_CONTEXT.session_profile_hash
    session_generation: int = UNSPECIFIED_OFF_SESSION_CONTEXT.session_generation
    execution_session_id: str = ""
    commissioning: bool = False
    strategy_generated: bool = True
    scientific_evidence: bool = False

    def __post_init__(self) -> None:
        if not self.command_id.startswith("l3g-pc-") or type(self.command_sequence) is not int or self.command_sequence <= 0:
            raise ValueError("Paper command identity or sequence is invalid.")
        if not self.session_id or not self.intent_id or not self.decision_id or not self.risk_grant_id:
            raise ValueError("Paper command provenance is incomplete.")
        if (self.account_name, self.account_class, self.instrument) != (PAPER_ACCOUNT, PAPER_ACCOUNT_CLASS, PAPER_INSTRUMENT):
            raise ValueError("Paper commands cannot target another account or instrument.")
        expected_quantity = 0 if self.action in {ExecutionAction.HEARTBEAT, ExecutionAction.RECONCILE, ExecutionAction.CANCEL_OWNED_ORDERS} else 1
        if self.quantity != expected_quantity:
            raise ValueError("Paper command quantity is fixed by its closed action.")
        expected_target = {
            ExecutionAction.ENTER_LONG: PaperDirection.LONG,
            ExecutionAction.ENTER_SHORT: PaperDirection.SHORT,
            ExecutionAction.EXIT: PaperDirection.FLAT,
            ExecutionAction.EMERGENCY_FLATTEN: PaperDirection.FLAT,
            ExecutionAction.CANCEL_OWNED_ORDERS: PaperDirection.FLAT,
            ExecutionAction.HEARTBEAT: PaperDirection.FLAT,
            ExecutionAction.RECONCILE: PaperDirection.FLAT,
        }[self.action]
        if self.expected_position is not expected_target:
            raise ValueError("Paper command action and target position do not match.")
        if not all((self.policy_hash, self.risk_profile_hash, self.account_binding_hash, self.reason_code)):
            raise ValueError("Paper command authority hashes and reason are required.")
        if self.commissioning:
            if self.strategy_generated or self.scientific_evidence:
                raise ValueError("Commissioning commands must remain non-strategy and non-scientific.")
        elif not self.strategy_generated or self.scientific_evidence:
            raise ValueError("Strategy commands must retain non-scientific strategy provenance.")
        _utc(self.created_at, "Paper command time")
        _utc(self.expires_at, "Paper command expiry")
        # Version-G commands called session_id the authenticated bridge
        # session.  A legacy object is harmless (the compiled AddOn refuses
        # its OFF_SESSION context); retain construction compatibility while
        # all runtime-created commands use a fully qualified market session.
        legacy_execution_session = (
            self.session_kind is PaperSessionKind.OFF_SESSION
            and self.session_id != UNSPECIFIED_OFF_SESSION_CONTEXT.session_id
            and (not self.execution_session_id or self.execution_session_id == self.session_id)
        )
        if legacy_execution_session:
            object.__setattr__(self, "execution_session_id", self.session_id)
        else:
            _validate_session_identity(self.session_kind, self.session_id, self.trade_date, self.session_profile_hash, self.session_generation)
        if not self.execution_session_id:
            object.__setattr__(self, "execution_session_id", self.session_id)

    def unsigned_payload(self) -> dict[str, object]:
        result = asdict(self)
        result.pop("signature")
        result["session_family"] = session_family(self.session_kind).value
        return _jsonable(result)  # type: ignore[return-value]

    def payload(self) -> dict[str, object]:
        result = asdict(self)
        result["session_family"] = session_family(self.session_kind).value
        return _jsonable(result)  # type: ignore[return-value]

    def with_signature(self, signature: str) -> "PaperExecutionCommand":
        if not signature:
            raise ValueError("A command signature is required.")
        return replace(self, signature=signature)


@runtime_checkable
class ExecutionVenueAdapter(Protocol):
    @property
    def capability(self) -> ExecutionCapabilityManifest: ...

    def submit(self, command: PaperExecutionCommand, grant: PaperRiskGrant) -> None: ...


@runtime_checkable
class ExecutionReconciler(Protocol):
    def request_reconciliation(self) -> None: ...


@runtime_checkable
class ExecutionSecretProvider(Protocol):
    def load_key(self) -> bytes: ...


@runtime_checkable
class ExecutionAuditSink(Protocol):
    def append(self, kind: str, payload: Mapping[str, object], *, identity: str | None = None) -> str: ...


@dataclass(frozen=True)
class PaperAuthorityBundle:
    policy: PaperPolicyArtifact = field(default_factory=PaperPolicyArtifact)
    risk: PaperRiskProfile = field(default_factory=PaperRiskProfile)
    binding: ExecutionAccountBinding = field(default_factory=ExecutionAccountBinding)
    capability: ExecutionCapabilityManifest = field(default_factory=ExecutionCapabilityManifest)

    def authority_payload(self) -> dict[str, object]:
        return {
            "mode": PAPER_MODE,
            "scientific_eligibility": False,
            "paper_execution": "AVAILABLE",
            "account": PAPER_ACCOUNT,
            "account_class": PAPER_ACCOUNT_CLASS,
            "instrument": PAPER_INSTRUMENT,
            "maximum_quantity": 1,
            "live_capital": "DENIED",
            "paper_policy_hash": self.policy.configuration_hash,
            "risk_profile_hash": self.risk.configuration_hash,
            "account_binding_hash": self.binding.binding_hash,
        }


def expires_at(created_at: str, seconds: int) -> str:
    created = datetime.fromisoformat(_utc(created_at, "Creation time").replace("Z", "+00:00"))
    return (created + timedelta(seconds=seconds)).isoformat().replace("+00:00", "Z")


def deterministic_id(prefix: str, payload: Mapping[str, object]) -> str:
    if prefix not in {"l3g-pe-", "l3g-pd-", "l3g-pi-", "l3g-pg-", "l3g-pc-", "l3g-es-"}:
        raise ValueError("Unknown Lane III-G deterministic namespace.")
    return prefix + canonical_hash(dict(payload))[:32]


def refuse_execution_target(value: object) -> None:
    """Validate the only startup execution target without permissive parsing."""
    if value != PAPER_MODE:
        raise ValueError("Lane III-G supports only the compiled PAPER_SIM101 execution target.")


POLICY = PaperPolicyArtifact()
RISK_PROFILE = PaperRiskProfile()
ACCOUNT_BINDING = ExecutionAccountBinding()
CAPABILITY = ExecutionCapabilityManifest()
AUTHORITY = PaperAuthorityBundle(POLICY, RISK_PROFILE, ACCOUNT_BINDING, CAPABILITY)
