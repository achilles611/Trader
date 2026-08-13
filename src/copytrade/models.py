from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field, is_dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any
from uuid import uuid4


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def as_utc(value: datetime | int | float | str | None) -> datetime:
    if value is None:
        return utc_now()
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value / 1000 if value > 10_000_000_000 else value, timezone.utc)
    normalized = value.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    return parsed.astimezone(timezone.utc) if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def iso(value: datetime | int | float | str | None) -> str:
    return as_utc(value).isoformat()


def ms(value: datetime | int | float | str) -> int:
    return int(as_utc(value).timestamp() * 1000)


def jsonable(value: Any) -> Any:
    if isinstance(value, datetime):
        return iso(value)
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Path):
        return str(value)
    if is_dataclass(value):
        return {key: jsonable(item) for key, item in asdict(value).items()}
    if isinstance(value, dict):
        return {str(key): jsonable(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [jsonable(item) for item in value]
    return value


def stable_id(prefix: str, *parts: Any) -> str:
    canonical = json.dumps([jsonable(part) for part in parts], sort_keys=True, separators=(",", ":"), default=str)
    return f"{prefix}_{hashlib.sha256(canonical.encode('utf-8')).hexdigest()[:32]}"


class PositionEventType(str, Enum):
    OPEN = "OPEN"
    ADD = "ADD"
    REDUCE = "REDUCE"
    CLOSE = "CLOSE"
    FLIP = "FLIP"


class TargetStatus(str, Enum):
    NEW = "new"
    QUEUED = "queued"
    PENDING = "pending"
    APPROVED = "approved"
    SHADOW = "shadow"
    ACTIVE = "active"
    MUTED = "muted"
    REJECTED = "rejected"


class CandidateAnalysisState(str, Enum):
    """Machine-owned research lifecycle kept separate from operator target status."""

    NEW = "new"
    PREFILTER_REJECTED = "prefilter_rejected"
    BACKFILL_PENDING = "backfill_pending"
    BACKFILL_FAILED = "backfill_failed"
    ANALYSIS_PENDING = "analysis_pending"
    ANALYZED = "analyzed"
    QUALIFIED = "qualified"
    QUARANTINED = "quarantined"


class ConnectionState(str, Enum):
    CONNECTED = "CONNECTED"
    RECONNECTING = "RECONNECTING"
    STALE = "STALE"
    ERROR = "ERROR"
    STOPPED = "STOPPED"


@dataclass(frozen=True)
class Target:
    wallet: str
    source: str = "hyperliquid"
    venue: str = "hyperliquid"
    status: str = TargetStatus.PENDING.value
    label: str = ""
    created_at: datetime = field(default_factory=utc_now)
    updated_at: datetime = field(default_factory=utc_now)
    metadata: dict[str, Any] = field(default_factory=dict)

    def normalized_wallet(self) -> str:
        return self.wallet.lower()


@dataclass(frozen=True)
class DiscoveryObservation:
    """One source-preserving public observation of a potentially useful wallet."""

    wallet: str
    source: str
    observed_at: datetime
    recent_activity_at: datetime | None = None
    discovery_rank: int | None = None
    source_score: float | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    raw_evidence: dict[str, Any] = field(default_factory=dict)
    evidence_id: str | None = None

    def normalized_wallet(self) -> str:
        return self.wallet.lower()


@dataclass(frozen=True)
class DiscoveryRun:
    run_id: str
    started_at: datetime
    sources: tuple[str, ...]
    configuration: dict[str, Any]
    finished_at: datetime | None = None
    status: str = "running"
    wallets_seen: int = 0
    eligible_wallets: int = 0
    limit_deferred_wallets: int = 0
    new_wallets: int = 0
    existing_wallets_refreshed: int = 0
    filtered_wallets: int = 0
    queued_for_analysis: int = 0
    errors: tuple[str, ...] = ()
    valid_events: int = 0
    normalized_observations: int = 0
    duplicate_events: int = 0
    invalid_wallets: int = 0
    malformed_events: int = 0
    unsupported_records: int = 0
    fatal_source_errors: int = 0


@dataclass(frozen=True)
class DiscoverySummary:
    run_id: str
    status: str
    sources: tuple[str, ...]
    wallets_seen: int
    eligible_wallets: int
    limit_deferred_wallets: int
    new_wallets: int
    existing_wallets_refreshed: int
    filtered_wallets: int
    queued_for_analysis: int
    errors: tuple[str, ...] = ()
    valid_events: int = 0
    normalized_observations: int = 0
    duplicate_events: int = 0
    invalid_wallets: int = 0
    malformed_events: int = 0
    unsupported_records: int = 0
    fatal_source_errors: int = 0


@dataclass(frozen=True)
class AnalysisRun:
    """Auditable Phase B orchestration run; execution state remains out of scope."""

    run_id: str
    started_at: datetime
    configuration: dict[str, Any]
    finished_at: datetime | None = None
    status: str = "running"
    wallets_considered: int = 0
    cheap_rejected: int = 0
    backfill_attempted: int = 0
    backfill_failed: int = 0
    reconstructed: int = 0
    scored: int = 0
    eligible: int = 0
    rejected: int = 0
    deferred: int = 0
    errors: tuple[str, ...] = ()


@dataclass(frozen=True)
class CandidateAnalysis:
    wallet: str
    lifecycle_status: str
    last_run_id: str | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None
    prefilter_reasons: tuple[str, ...] = ()
    errors: tuple[str, ...] = ()
    summary: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class RawFill:
    """Source-preserving public fill.  The deterministic ID makes ingestion idempotent."""

    event_id: str
    source: str
    venue: str
    target_wallet: str
    symbol: str
    side: str
    direction: str
    price: float
    base_quantity: float
    notional: float
    event_timestamp: datetime
    ingestion_timestamp: datetime
    chain_network: str | None = None
    target_order_id: str | None = None
    target_trade_id: str | None = None
    transaction_hash: str | None = None
    fee: float = 0.0
    fee_token: str | None = None
    target_account_equity: float | None = None
    target_position_before: float | None = None
    confirmation: str | None = None
    raw_payload: dict[str, Any] = field(default_factory=dict)
    # These source values are deliberately kept distinct from reconstructed
    # accounting.  Hyperliquid's closedPnl is a reconciliation input, never an
    # instruction to overwrite our calculation.
    source_closed_pnl: float | None = None
    is_liquidation: bool = False

    @property
    def signed_quantity(self) -> float:
        side = self.side.upper()
        if side in {"B", "BUY", "LONG"}:
            return abs(self.base_quantity)
        if side in {"A", "S", "SELL", "SHORT"}:
            return -abs(self.base_quantity)
        # Hyperliquid normally supplies B/A.  Direction provides a stable fallback.
        return -abs(self.base_quantity) if "short" in self.direction.lower() or "sell" in self.direction.lower() else abs(self.base_quantity)

    @classmethod
    def from_hyperliquid(
        cls,
        payload: dict[str, Any],
        wallet: str,
        *,
        network: str = "mainnet",
        ingested_at: datetime | None = None,
    ) -> "RawFill":
        price = float(payload.get("px") or payload.get("price") or 0.0)
        quantity = abs(float(payload.get("sz") or payload.get("size") or 0.0))
        event_time = as_utc(payload.get("time") or payload.get("timestamp"))
        order_id = payload.get("oid") or payload.get("orderId")
        trade_id = payload.get("tid") or payload.get("tradeId")
        tx_hash = payload.get("hash") or payload.get("transactionHash")
        symbol = str(payload.get("coin") or payload.get("symbol") or "").upper()
        side = str(payload.get("side") or "")
        event_id = stable_id(
            "hlfill",
            network,
            wallet.lower(),
            tx_hash or "",
            str(trade_id or ""),
            str(order_id or ""),
            symbol,
            side,
            ms(event_time),
            f"{price:.12g}",
            f"{quantity:.12g}",
        )
        position = payload.get("startPosition")
        equity = payload.get("accountValue") or payload.get("accountEquity")
        closed_pnl = payload.get("closedPnl")
        liquidation = bool(payload.get("liquidation") or payload.get("liquidated")) or "liquidat" in str(payload.get("dir") or "").lower()
        return cls(
            event_id=event_id,
            source="hyperliquid",
            venue="hyperliquid",
            chain_network=network,
            target_wallet=wallet.lower(),
            target_order_id=str(order_id) if order_id is not None else None,
            target_trade_id=str(trade_id) if trade_id is not None else None,
            transaction_hash=str(tx_hash) if tx_hash else None,
            symbol=symbol,
            side=side,
            direction=str(payload.get("dir") or payload.get("direction") or ""),
            price=price,
            base_quantity=quantity,
            notional=price * quantity,
            fee=float(payload.get("fee") or 0.0),
            fee_token=str(payload.get("feeToken")) if payload.get("feeToken") else None,
            target_account_equity=float(equity) if equity not in (None, "") else None,
            target_position_before=float(position) if position not in (None, "") else None,
            event_timestamp=event_time,
            ingestion_timestamp=ingested_at or utc_now(),
            confirmation=str(payload.get("block") or payload.get("blockNumber") or "") or None,
            raw_payload=dict(payload),
            source_closed_pnl=float(closed_pnl) if closed_pnl not in (None, "") else None,
            is_liquidation=liquidation,
        )


@dataclass(frozen=True)
class PositionEvent:
    event_id: str
    target_wallet: str
    symbol: str
    event_type: PositionEventType
    direction: str
    delta_quantity: float
    before_quantity: float
    after_quantity: float
    price: float
    notional: float
    event_timestamp: datetime
    campaign_id: str | None
    raw_fill_ids: tuple[str, ...]
    target_equity: float | None = None
    initial_delta_notional: float = 0.0
    equity_source: str = "missing"
    equity_age_seconds: float | None = None
    # A crossing source fill emits two events, one per economic campaign.
    # The fields make that intentional double attribution auditable.
    source_event_type: str | None = None
    split_role: str | None = None
    split_quantity: float | None = None
    split_notional: float | None = None
    split_fee: float | None = None
    source_closed_pnl: float | None = None


@dataclass
class PositionCampaign:
    campaign_id: str
    target_wallet: str
    symbol: str
    direction: str
    opened_at: datetime
    closed_at: datetime | None = None
    entry_quantity: float = 0.0
    open_quantity: float = 0.0
    entry_notional: float = 0.0
    remaining_entry_notional: float = 0.0
    exit_notional: float = 0.0
    realized_pnl: float = 0.0
    target_fees: float = 0.0
    event_count: int = 0
    raw_fill_ids: list[str] = field(default_factory=list)
    max_open_quantity: float = 0.0
    adverse_add_count: int = 0
    history_complete: bool = True
    entry_basis_quality: str = "observed"
    source_closed_pnl: float = 0.0
    source_closed_pnl_observed: bool = False
    reconciliation_gross_difference: float | None = None
    liquidation_count: int = 0

    @property
    def average_entry_price(self) -> float:
        return self.entry_notional / self.entry_quantity if self.entry_quantity else 0.0

    @property
    def is_closed(self) -> bool:
        return self.closed_at is not None

    @property
    def holding_seconds(self) -> float:
        return max(0.0, ((self.closed_at or utc_now()) - self.opened_at).total_seconds())


@dataclass(frozen=True)
class TraderSnapshot:
    snapshot_id: str
    target_wallet: str
    snapshot_timestamp: datetime
    account_value: float | None
    withdrawable: float | None
    total_notional_position: float | None
    positions: dict[str, Any]
    source: str = "hyperliquid"
    raw_payload: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class TraderMetrics:
    target_wallet: str
    calculated_at: datetime
    history_days: float
    campaign_count: int
    closed_campaign_count: int
    realized_pnl: float
    net_pnl: float
    wins: int
    losses: int
    win_rate: float
    shrunk_win_rate: float
    average_winner: float
    average_loser: float
    median_winner: float
    median_loser: float
    profit_factor: float
    payoff_ratio: float
    expectancy: float
    median_holding_seconds: float
    mean_holding_seconds: float
    max_drawdown: float
    longest_losing_streak: int
    longest_winning_streak: int
    best_campaign: float
    worst_campaign: float
    fifth_percentile: float
    ninety_fifth_percentile: float
    pnl_concentration_best: float
    pnl_concentration_best_five: float
    average_entry_size_fraction: float
    median_entry_size_fraction: float
    entry_size_variance: float
    martingale_indicator: bool
    adverse_averaging_indicator: bool
    activity_recency_days: float | None
    by_symbol: dict[str, dict[str, float]] = field(default_factory=dict)
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class CopySignal:
    signal_id: str
    target_wallet: str
    campaign_id: str | None
    source_event_id: str
    symbol: str
    action: str
    direction: str
    target_price: float
    target_quantity: float
    target_notional: float
    allocation_fraction: float
    requested_capital: float
    created_at: datetime
    source_event_timestamp: datetime
    size_ratio: float | None = None
    reason: str = ""
    target_position_before: float = 0.0
    target_leverage: float | None = None
    target_equity: float | None = None
    equity_source: str = "missing"
    equity_age_seconds: float | None = None


@dataclass
class VirtualTargetPosition:
    sleeve_id: str
    target_wallet: str
    campaign_id: str | None
    symbol: str
    direction: str
    quantity: float
    entry_price: float
    allocated_capital: float
    remaining_capital: float
    entry_fee: float
    realized_pnl: float = 0.0
    exit_fee: float = 0.0
    opened_at: datetime = field(default_factory=utc_now)
    updated_at: datetime = field(default_factory=utc_now)
    closed_at: datetime | None = None
    target_entry_price: float | None = None
    max_drawdown: float = 0.0
    current_mark: float | None = None
    unrealized_pnl: float = 0.0

    @property
    def is_open(self) -> bool:
        return self.closed_at is None and self.quantity > 0


@dataclass(frozen=True)
class ExecutionAttempt:
    attempt_id: str
    signal_id: str
    target_wallet: str
    symbol: str
    action: str
    requested_capital: float
    status: str
    reason: str
    source_event_timestamp: datetime
    received_at: datetime
    decided_at: datetime
    paper_order_at: datetime | None = None
    simulated_execution_at: datetime | None = None
    detection_latency_ms: float = 0.0
    decision_latency_ms: float = 0.0


@dataclass(frozen=True)
class ExecutionFill:
    execution_fill_id: str
    attempt_id: str
    sleeve_id: str | None
    price: float
    quantity: float
    notional: float
    fee: float
    slippage_bps: float
    timestamp: datetime
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class BacktestRun:
    run_id: str
    started_at: datetime
    finished_at: datetime | None
    target_wallets: tuple[str, ...]
    start_timestamp: datetime | None
    end_timestamp: datetime | None
    initial_capital: float
    ending_capital: float
    seed: int
    configuration: dict[str, Any]
    summary: dict[str, Any]
    git_commit: str | None = None


@dataclass(frozen=True)
class CandidateScore:
    target_wallet: str
    calculated_at: datetime
    total_score: float
    component_scores: dict[str, float]
    penalties: dict[str, float]
    eligible: bool
    reasons: tuple[str, ...] = ()
    source_quality: float = 1.0
    # Scores can be produced by the legacy one-wallet command or by a
    # complete Phase B analysis.  Selection must never confuse the two.
    provenance: str = "legacy"
    analysis_run_id: str | None = None
    config_fingerprint: str | None = None
    confidence_score: float = 0.0
    hard_gates: tuple[str, ...] = ()
    score_version: str = "phase_b_suitability_v2"


def new_run_id(prefix: str = "copyrun") -> str:
    return f"{prefix}_{utc_now().strftime('%Y%m%dT%H%M%SZ')}_{uuid4().hex[:8]}"
