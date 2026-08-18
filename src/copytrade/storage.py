from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass, field
from itertools import islice
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping, Protocol

from .contracts import (
    PHASE_A_EVIDENCE_SCHEMA_VERSION,
    PHASE_B_RECOMMENDATION_SCHEMA_VERSION,
    PHASE_D_EXECUTION_CONTRACT_VERSION,
)
from .execution_contracts import (
    ExecutionIntent,
    ExecutionRiskDecision,
    ExecutionState,
    ExecutionSubmission,
    ExposureEffect,
    ReconciliationState,
    TERMINAL_EXECUTION_STATES,
    VenueFill,
    VenueOrderStatus,
    validate_execution_transition,
)
from .models import (
    BacktestRun,
    AnalysisRun,
    CandidateAnalysis,
    CandidateScore,
    CopySignal,
    DiscoveryObservation,
    DiscoveryRun,
    DiscoverySummary,
    ExecutionAttempt,
    ExecutionFill,
    PositionCampaign,
    PositionEvent,
    PositionEventType,
    RawFill,
    Target,
    TargetStatus,
    TraderMetrics,
    TraderSnapshot,
    VirtualTargetPosition,
    as_utc,
    iso,
    jsonable,
    stable_id,
)


@dataclass
class DiscoveryStageStats:
    """Mutable bounded-run accounting, kept even if a source later fails."""

    normalized_observations: int = 0
    duplicate_events: int = 0
    invalid_wallets: int = 0
    rejections: list[dict[str, Any]] = field(default_factory=list)


RECONSTRUCTION_SCHEMA_VERSION = 1


@dataclass(frozen=True)
class ReconstructionCursor:
    target_wallet: str
    schema_version: int
    revision: int
    last_seen_timestamp: object | None = None
    last_seen_event_id: str | None = None
    last_processed_timestamp: object | None = None
    last_processed_event_id: str | None = None
    pending_fill_ids: tuple[str, ...] = ()
    pending_event_ids: tuple[str, ...] = ()
    recovery_state: str = "CONTINUOUS"
    recovery_anchor_event_id: str | None = None
    recovery_anchor_timestamp: object | None = None
    recovery_detail: dict[str, Any] = field(default_factory=dict)
    updated_at: object | None = None


class CopyTradeStore(Protocol):
    """Small storage contract; a PostgreSQL implementation can replace SQLite later."""

    def initialize(self) -> None: ...
    def upsert_target(self, target: Target) -> None: ...
    def insert_raw_fill(self, fill: RawFill) -> bool: ...
    def list_raw_fills(self, wallet: str | None = None) -> list[RawFill]: ...


def _dump(value: Any) -> str:
    return json.dumps(jsonable(value), sort_keys=True, separators=(",", ":"))


def _load(value: str | None, default: Any) -> Any:
    return json.loads(value) if value else default


def _is_wallet(value: str) -> bool:
    return value.startswith("0x") and len(value) == 42 and all(character in "0123456789abcdef" for character in value[2:].lower())


def _discovery_source_event_id(observation: DiscoveryObservation) -> str:
    """Identify one source event deterministically before per-run aggregation.

    Provider-supplied evidence IDs are preferred.  The fallback deliberately
    includes every source field that can distinguish fills/trades when an
    archive omits a dedicated ID, so overlapping files cannot manufacture
    activity while different real events remain distinguishable.
    """
    raw = observation.raw_evidence
    source_identifier = observation.evidence_id or next(
        (
            raw.get(key)
            for key in ("tid", "tradeId", "fillId", "id", "hash", "transactionHash")
            if raw.get(key) not in (None, "")
        ),
        None,
    )
    if source_identifier is not None:
        return stable_id(
            "discovery_source_event", observation.source, observation.normalized_wallet(), str(source_identifier),
        )
    return stable_id(
        "discovery_source_event",
        observation.source,
        observation.normalized_wallet(),
        raw.get("hash") or raw.get("transactionHash") or "",
        raw.get("oid") or raw.get("orderId") or "",
        raw.get("time") or raw.get("timestamp") or iso(observation.recent_activity_at or observation.observed_at),
        raw.get("coin") or raw.get("symbol") or "",
        raw.get("px") or raw.get("price") or "",
        raw.get("sz") or raw.get("size") or "",
        observation.discovery_rank,
        observation.source_score,
    )


class CopyTradeDatabase:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.path)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=NORMAL")
        connection.execute("PRAGMA busy_timeout=5000")
        try:
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def initialize(self) -> None:
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS copy_targets (
                    wallet TEXT PRIMARY KEY, source TEXT NOT NULL, venue TEXT NOT NULL,
                    status TEXT NOT NULL, label TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL, metadata_json TEXT NOT NULL DEFAULT '{}'
                );
                CREATE INDEX IF NOT EXISTS idx_copy_targets_status ON copy_targets(status);
                CREATE TABLE IF NOT EXISTS copy_raw_fills (
                    event_id TEXT PRIMARY KEY, source TEXT NOT NULL, venue TEXT NOT NULL,
                    chain_network TEXT, target_wallet TEXT NOT NULL, target_order_id TEXT,
                    target_trade_id TEXT, transaction_hash TEXT, symbol TEXT NOT NULL,
                    side TEXT NOT NULL, direction TEXT NOT NULL, price REAL NOT NULL,
                    base_quantity REAL NOT NULL, notional REAL NOT NULL, fee REAL NOT NULL DEFAULT 0,
                    fee_token TEXT, target_account_equity REAL, target_position_before REAL,
                    event_timestamp TEXT NOT NULL, ingestion_timestamp TEXT NOT NULL,
                    confirmation TEXT, raw_payload_json TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_copy_raw_fills_wallet_time
                    ON copy_raw_fills(target_wallet, event_timestamp);
                CREATE INDEX IF NOT EXISTS idx_copy_raw_fills_order
                    ON copy_raw_fills(target_wallet, target_order_id);
                CREATE TABLE IF NOT EXISTS copy_reconstruction_cursors (
                    target_wallet TEXT PRIMARY KEY,
                    schema_version INTEGER NOT NULL,
                    revision INTEGER NOT NULL DEFAULT 0,
                    last_seen_timestamp TEXT,
                    last_seen_event_id TEXT,
                    last_processed_timestamp TEXT,
                    last_processed_event_id TEXT,
                    pending_fill_ids_json TEXT NOT NULL DEFAULT '[]',
                    pending_event_ids_json TEXT NOT NULL DEFAULT '[]',
                    recovery_state TEXT NOT NULL DEFAULT 'CONTINUOUS',
                    recovery_anchor_event_id TEXT,
                    recovery_anchor_timestamp TEXT,
                    recovery_detail_json TEXT NOT NULL DEFAULT '{}',
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_copy_reconstruction_cursors_recovery
                    ON copy_reconstruction_cursors(recovery_state, updated_at);
                CREATE TABLE IF NOT EXISTS copy_position_events (
                    event_id TEXT PRIMARY KEY, target_wallet TEXT NOT NULL, symbol TEXT NOT NULL,
                    event_type TEXT NOT NULL, direction TEXT NOT NULL, delta_quantity REAL NOT NULL,
                    before_quantity REAL NOT NULL, after_quantity REAL NOT NULL, price REAL NOT NULL,
                    notional REAL NOT NULL, event_timestamp TEXT NOT NULL, campaign_id TEXT,
                    raw_fill_ids_json TEXT NOT NULL, target_equity REAL,
                    initial_delta_notional REAL NOT NULL DEFAULT 0
                );
                CREATE INDEX IF NOT EXISTS idx_copy_position_events_wallet_time
                    ON copy_position_events(target_wallet, event_timestamp);
                CREATE TABLE IF NOT EXISTS copy_campaigns (
                    campaign_id TEXT PRIMARY KEY, target_wallet TEXT NOT NULL, symbol TEXT NOT NULL,
                    direction TEXT NOT NULL, opened_at TEXT NOT NULL, closed_at TEXT,
                    entry_quantity REAL NOT NULL, open_quantity REAL NOT NULL,
                    entry_notional REAL NOT NULL, exit_notional REAL NOT NULL,
                    realized_pnl REAL NOT NULL, target_fees REAL NOT NULL,
                    event_count INTEGER NOT NULL, raw_fill_ids_json TEXT NOT NULL,
                    max_open_quantity REAL NOT NULL, adverse_add_count INTEGER NOT NULL DEFAULT 0
                );
                CREATE INDEX IF NOT EXISTS idx_copy_campaigns_wallet ON copy_campaigns(target_wallet, opened_at);
                CREATE TABLE IF NOT EXISTS copy_trader_snapshots (
                    snapshot_id TEXT PRIMARY KEY, target_wallet TEXT NOT NULL, snapshot_timestamp TEXT NOT NULL,
                    account_value REAL, withdrawable REAL, total_notional_position REAL,
                    positions_json TEXT NOT NULL, source TEXT NOT NULL, raw_payload_json TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_copy_snapshots_wallet_time ON copy_trader_snapshots(target_wallet, snapshot_timestamp);
                CREATE TABLE IF NOT EXISTS copy_daily_metrics (
                    target_wallet TEXT NOT NULL, calculated_at TEXT NOT NULL, payload_json TEXT NOT NULL,
                    PRIMARY KEY(target_wallet, calculated_at)
                );
                CREATE TABLE IF NOT EXISTS copy_candidate_scores (
                    target_wallet TEXT NOT NULL, calculated_at TEXT NOT NULL, total_score REAL NOT NULL,
                    component_scores_json TEXT NOT NULL, penalties_json TEXT NOT NULL, eligible INTEGER NOT NULL,
                    reasons_json TEXT NOT NULL, source_quality REAL NOT NULL,
                    provenance TEXT NOT NULL DEFAULT 'legacy', analysis_run_id TEXT,
                    config_fingerprint TEXT, confidence_score REAL NOT NULL DEFAULT 0,
                    hard_gates_json TEXT NOT NULL DEFAULT '[]', score_version TEXT NOT NULL DEFAULT 'phase_b_suitability_v3',
                    PRIMARY KEY(target_wallet, calculated_at)
                );
                CREATE INDEX IF NOT EXISTS idx_copy_scores_wallet_time ON copy_candidate_scores(target_wallet, calculated_at DESC);
                CREATE TABLE IF NOT EXISTS copy_signals (
                    signal_id TEXT PRIMARY KEY, target_wallet TEXT NOT NULL, campaign_id TEXT,
                    source_event_id TEXT NOT NULL, symbol TEXT NOT NULL, action TEXT NOT NULL,
                    direction TEXT NOT NULL, target_price REAL NOT NULL, target_quantity REAL NOT NULL,
                    target_notional REAL NOT NULL, allocation_fraction REAL NOT NULL,
                    requested_capital REAL NOT NULL, created_at TEXT NOT NULL,
                    source_event_timestamp TEXT NOT NULL, size_ratio REAL, reason TEXT NOT NULL,
                    target_position_before REAL NOT NULL DEFAULT 0, target_leverage REAL
                );
                CREATE TABLE IF NOT EXISTS copy_virtual_positions (
                    sleeve_id TEXT PRIMARY KEY, target_wallet TEXT NOT NULL, campaign_id TEXT,
                    symbol TEXT NOT NULL, direction TEXT NOT NULL, quantity REAL NOT NULL,
                    entry_price REAL NOT NULL, allocated_capital REAL NOT NULL,
                    remaining_capital REAL NOT NULL, entry_fee REAL NOT NULL,
                    realized_pnl REAL NOT NULL, exit_fee REAL NOT NULL, opened_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL, closed_at TEXT, target_entry_price REAL,
                    max_drawdown REAL NOT NULL DEFAULT 0
                );
                CREATE INDEX IF NOT EXISTS idx_copy_sleeves_wallet_open ON copy_virtual_positions(target_wallet, closed_at);
                CREATE TABLE IF NOT EXISTS copy_execution_attempts (
                    attempt_id TEXT PRIMARY KEY, signal_id TEXT NOT NULL, target_wallet TEXT NOT NULL,
                    symbol TEXT NOT NULL, action TEXT NOT NULL, requested_capital REAL NOT NULL,
                    status TEXT NOT NULL, reason TEXT NOT NULL, source_event_timestamp TEXT NOT NULL,
                    received_at TEXT NOT NULL, decided_at TEXT NOT NULL, paper_order_at TEXT,
                    simulated_execution_at TEXT, detection_latency_ms REAL NOT NULL,
                    decision_latency_ms REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS copy_execution_claims (
                    signal_id TEXT PRIMARY KEY, status TEXT NOT NULL,
                    claimed_at TEXT NOT NULL, attempt_id TEXT
                );
                CREATE TABLE IF NOT EXISTS copy_execution_fills (
                    execution_fill_id TEXT PRIMARY KEY, attempt_id TEXT NOT NULL, sleeve_id TEXT,
                    price REAL NOT NULL, quantity REAL NOT NULL, notional REAL NOT NULL, fee REAL NOT NULL,
                    slippage_bps REAL NOT NULL, timestamp TEXT NOT NULL, raw_json TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS copy_backtest_runs (
                    run_id TEXT PRIMARY KEY, started_at TEXT NOT NULL, finished_at TEXT,
                    target_wallets_json TEXT NOT NULL, start_timestamp TEXT, end_timestamp TEXT,
                    initial_capital REAL NOT NULL, ending_capital REAL NOT NULL, seed INTEGER NOT NULL,
                    configuration_json TEXT NOT NULL, summary_json TEXT NOT NULL, git_commit TEXT
                );
                CREATE TABLE IF NOT EXISTS copy_portfolio_snapshots (
                    snapshot_id TEXT PRIMARY KEY, timestamp TEXT NOT NULL, cash REAL NOT NULL,
                    equity REAL NOT NULL, committed_capital REAL NOT NULL,
                    drawdown_fraction REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS copy_backfill_coverage (
                    coverage_id TEXT PRIMARY KEY, target_wallet TEXT NOT NULL, requested_start TEXT NOT NULL,
                    requested_end TEXT NOT NULL, earliest_observed_fill TEXT, latest_observed_fill TEXT,
                    source_limit_detected INTEGER NOT NULL, coverage_complete INTEGER NOT NULL, coverage_quality TEXT NOT NULL,
                    coverage_state TEXT NOT NULL DEFAULT 'UNPROVEN'
                );
                CREATE INDEX IF NOT EXISTS idx_copy_portfolio_snapshot_time ON copy_portfolio_snapshots(timestamp);
                CREATE TABLE IF NOT EXISTS copy_discovery_runs (
                    run_id TEXT PRIMARY KEY, started_at TEXT NOT NULL, finished_at TEXT,
                    status TEXT NOT NULL, sources_json TEXT NOT NULL, configuration_json TEXT NOT NULL,
                    wallets_seen INTEGER NOT NULL DEFAULT 0, eligible_wallets INTEGER NOT NULL DEFAULT 0,
                    limit_deferred_wallets INTEGER NOT NULL DEFAULT 0, new_wallets INTEGER NOT NULL DEFAULT 0,
                    existing_wallets_refreshed INTEGER NOT NULL DEFAULT 0, filtered_wallets INTEGER NOT NULL DEFAULT 0,
                    queued_for_analysis INTEGER NOT NULL DEFAULT 0, errors_json TEXT NOT NULL DEFAULT '[]',
                    valid_events INTEGER NOT NULL DEFAULT 0, normalized_observations INTEGER NOT NULL DEFAULT 0,
                    duplicate_events INTEGER NOT NULL DEFAULT 0, invalid_wallets INTEGER NOT NULL DEFAULT 0,
                    malformed_events INTEGER NOT NULL DEFAULT 0, unsupported_records INTEGER NOT NULL DEFAULT 0,
                    fatal_source_errors INTEGER NOT NULL DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS copy_discovery_candidates (
                    wallet TEXT PRIMARY KEY, discovered_at TEXT NOT NULL, last_seen_at TEXT NOT NULL,
                    recent_activity_at TEXT, discovery_rank INTEGER, source_score REAL,
                    source_count INTEGER NOT NULL DEFAULT 0, discovery_status TEXT NOT NULL DEFAULT 'new',
                    last_discovery_run_id TEXT NOT NULL, metadata_json TEXT NOT NULL DEFAULT '{}'
                );
                CREATE INDEX IF NOT EXISTS idx_copy_discovery_candidates_seen ON copy_discovery_candidates(last_seen_at DESC);
                CREATE INDEX IF NOT EXISTS idx_copy_discovery_candidates_activity ON copy_discovery_candidates(recent_activity_at DESC);
                CREATE INDEX IF NOT EXISTS idx_copy_discovery_candidates_run ON copy_discovery_candidates(last_discovery_run_id);
                CREATE TABLE IF NOT EXISTS copy_discovery_observations (
                    observation_id TEXT PRIMARY KEY, run_id TEXT NOT NULL, wallet TEXT NOT NULL, source TEXT NOT NULL,
                    observed_at TEXT NOT NULL, recent_activity_at TEXT, discovery_rank INTEGER, source_score REAL,
                    metadata_json TEXT NOT NULL DEFAULT '{}', raw_evidence_json TEXT NOT NULL DEFAULT '{}', evidence_id TEXT,
                    source_event_id TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_copy_discovery_observations_wallet ON copy_discovery_observations(wallet, observed_at DESC);
                CREATE INDEX IF NOT EXISTS idx_copy_discovery_observations_run ON copy_discovery_observations(run_id, source);
                CREATE INDEX IF NOT EXISTS idx_copy_discovery_observations_source_wallet ON copy_discovery_observations(source, wallet);
                CREATE UNIQUE INDEX IF NOT EXISTS idx_copy_discovery_observations_event ON copy_discovery_observations(run_id, source_event_id);
                CREATE TABLE IF NOT EXISTS copy_discovery_rejections (
                    rejection_id INTEGER PRIMARY KEY AUTOINCREMENT, run_id TEXT NOT NULL, category TEXT NOT NULL,
                    message TEXT NOT NULL, source TEXT, record_index INTEGER, event_index INTEGER,
                    raw_record_json TEXT NOT NULL DEFAULT '{}', recorded_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_copy_discovery_rejections_run ON copy_discovery_rejections(run_id, rejection_id);
                CREATE TABLE IF NOT EXISTS copy_analysis_runs (
                    run_id TEXT PRIMARY KEY, started_at TEXT NOT NULL, finished_at TEXT,
                    status TEXT NOT NULL, configuration_json TEXT NOT NULL,
                    wallets_considered INTEGER NOT NULL DEFAULT 0, cheap_rejected INTEGER NOT NULL DEFAULT 0,
                    backfill_attempted INTEGER NOT NULL DEFAULT 0, backfill_failed INTEGER NOT NULL DEFAULT 0,
                    reconstructed INTEGER NOT NULL DEFAULT 0, scored INTEGER NOT NULL DEFAULT 0,
                    eligible INTEGER NOT NULL DEFAULT 0, rejected INTEGER NOT NULL DEFAULT 0,
                    deferred INTEGER NOT NULL DEFAULT 0, errors_json TEXT NOT NULL DEFAULT '[]'
                );
                CREATE TABLE IF NOT EXISTS copy_analysis_run_wallets (
                    run_id TEXT NOT NULL, wallet TEXT NOT NULL, stage TEXT NOT NULL, status TEXT NOT NULL,
                    attempts INTEGER NOT NULL DEFAULT 0, error TEXT, payload_json TEXT NOT NULL DEFAULT '{}',
                    updated_at TEXT NOT NULL, PRIMARY KEY(run_id, wallet)
                );
                CREATE INDEX IF NOT EXISTS idx_copy_analysis_run_wallets_status ON copy_analysis_run_wallets(run_id, status);
                CREATE TABLE IF NOT EXISTS copy_analysis_run_wallet_events (
                    event_id INTEGER PRIMARY KEY AUTOINCREMENT, run_id TEXT NOT NULL, wallet TEXT NOT NULL,
                    stage TEXT NOT NULL, status TEXT NOT NULL, attempt INTEGER NOT NULL DEFAULT 0,
                    recorded_at TEXT NOT NULL, error TEXT, payload_json TEXT NOT NULL DEFAULT '{}'
                );
                CREATE INDEX IF NOT EXISTS idx_copy_analysis_wallet_events_run_wallet
                    ON copy_analysis_run_wallet_events(run_id, wallet, event_id);
                CREATE INDEX IF NOT EXISTS idx_copy_analysis_wallet_events_run_stage
                    ON copy_analysis_run_wallet_events(run_id, stage, status);
                CREATE TABLE IF NOT EXISTS copy_analysis_market_evidence (
                    analysis_run_id TEXT NOT NULL, symbol TEXT NOT NULL, bucket_timestamp TEXT NOT NULL,
                    price REAL, source TEXT NOT NULL, quality TEXT NOT NULL, market_timestamp TEXT,
                    requested_for_timestamp TEXT NOT NULL, resolution TEXT NOT NULL, recorded_at TEXT NOT NULL,
                    PRIMARY KEY(analysis_run_id, symbol, bucket_timestamp)
                );
                CREATE TABLE IF NOT EXISTS copy_analysis_finalist_recommendations (
                    analysis_run_id TEXT NOT NULL, config_fingerprint TEXT NOT NULL, wallet TEXT NOT NULL,
                    recommendation_schema_version INTEGER NOT NULL DEFAULT 1,
                    finalist_eligible INTEGER NOT NULL, finalist_rejection_reasons_json TEXT NOT NULL DEFAULT '[]',
                    diversification_penalty REAL, final_selection_score REAL, selection_rank INTEGER,
                    evaluated_at TEXT NOT NULL,
                    PRIMARY KEY(analysis_run_id, config_fingerprint, wallet)
                );
                CREATE INDEX IF NOT EXISTS idx_copy_finalist_recommendations_run
                    ON copy_analysis_finalist_recommendations(analysis_run_id, config_fingerprint, selection_rank);
                CREATE TABLE IF NOT EXISTS copy_candidate_analyses (
                    wallet TEXT PRIMARY KEY, lifecycle_status TEXT NOT NULL, last_run_id TEXT,
                    started_at TEXT, completed_at TEXT, prefilter_reasons_json TEXT NOT NULL DEFAULT '[]',
                    errors_json TEXT NOT NULL DEFAULT '[]', summary_json TEXT NOT NULL DEFAULT '{}'
                );
                CREATE INDEX IF NOT EXISTS idx_copy_candidate_analyses_state ON copy_candidate_analyses(lifecycle_status, completed_at DESC);
                CREATE INDEX IF NOT EXISTS idx_copy_candidate_analyses_run ON copy_candidate_analyses(last_run_id);

                -- Phase D owns a separate execution ledger.  The historical
                -- copy_execution_* tables above remain PAPER research records.
                CREATE TABLE IF NOT EXISTS phase_d_execution_intents (
                    intent_id TEXT PRIMARY KEY, contract_version INTEGER NOT NULL,
                    signal_id TEXT NOT NULL UNIQUE, source_event_id TEXT NOT NULL,
                    target_wallet TEXT NOT NULL, campaign_id TEXT, symbol TEXT NOT NULL,
                    action TEXT NOT NULL, direction TEXT NOT NULL,
                    requested_quantity REAL NOT NULL, requested_capital REAL NOT NULL,
                    source_event_timestamp TEXT NOT NULL, accepted_at TEXT NOT NULL,
                    provenance_json TEXT NOT NULL, exposure_effect TEXT NOT NULL,
                    supersedes_intent_id TEXT, execution_domain TEXT NOT NULL DEFAULT 'SIMULATOR',
                    execution_account_id TEXT NOT NULL DEFAULT 'SIMULATOR:default',
                    state TEXT NOT NULL, updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_phase_d_execution_intents_state
                    ON phase_d_execution_intents(state, updated_at);
                CREATE INDEX IF NOT EXISTS idx_phase_d_execution_intents_symbol
                    ON phase_d_execution_intents(symbol, target_wallet);
                CREATE TABLE IF NOT EXISTS phase_d_execution_state_events (
                    event_id TEXT PRIMARY KEY, intent_id TEXT NOT NULL, sequence INTEGER NOT NULL,
                    previous_state TEXT, next_state TEXT NOT NULL, reason TEXT NOT NULL,
                    source TEXT NOT NULL, occurred_at TEXT NOT NULL, raw_evidence_json TEXT NOT NULL DEFAULT '{}',
                    UNIQUE(intent_id, sequence),
                    FOREIGN KEY(intent_id) REFERENCES phase_d_execution_intents(intent_id)
                );
                CREATE INDEX IF NOT EXISTS idx_phase_d_execution_state_events_intent
                    ON phase_d_execution_state_events(intent_id, sequence);
                CREATE TABLE IF NOT EXISTS phase_d_execution_risk_decisions (
                    decision_id TEXT PRIMARY KEY, intent_id TEXT NOT NULL, allowed INTEGER NOT NULL,
                    reason TEXT NOT NULL, evaluated_at TEXT NOT NULL, evidence_json TEXT NOT NULL DEFAULT '{}',
                    FOREIGN KEY(intent_id) REFERENCES phase_d_execution_intents(intent_id)
                );
                CREATE INDEX IF NOT EXISTS idx_phase_d_execution_risk_decisions_intent
                    ON phase_d_execution_risk_decisions(intent_id, evaluated_at);
                CREATE TABLE IF NOT EXISTS phase_d_execution_submissions (
                    submission_id TEXT PRIMARY KEY, intent_id TEXT NOT NULL UNIQUE,
                    client_order_id TEXT NOT NULL UNIQUE, requested_quantity REAL NOT NULL,
                    side TEXT NOT NULL, execution_domain TEXT NOT NULL DEFAULT 'SIMULATOR',
                    execution_account_id TEXT NOT NULL DEFAULT 'SIMULATOR:default', state TEXT NOT NULL, venue_order_id TEXT,
                    filled_quantity REAL NOT NULL DEFAULT 0, created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL, raw_evidence_json TEXT NOT NULL DEFAULT '{}',
                    FOREIGN KEY(intent_id) REFERENCES phase_d_execution_intents(intent_id)
                );
                CREATE INDEX IF NOT EXISTS idx_phase_d_execution_submissions_state
                    ON phase_d_execution_submissions(state, updated_at);
                CREATE TABLE IF NOT EXISTS phase_d_execution_fills (
                    execution_fill_id TEXT PRIMARY KEY, intent_id TEXT NOT NULL,
                    submission_id TEXT NOT NULL, venue_fill_id TEXT NOT NULL,
                    execution_domain TEXT NOT NULL DEFAULT 'SIMULATOR',
                    execution_account_id TEXT NOT NULL DEFAULT 'SIMULATOR:default', side TEXT NOT NULL DEFAULT 'BUY',
                    quantity REAL NOT NULL, price REAL NOT NULL, fee REAL NOT NULL,
                    venue_timestamp TEXT NOT NULL, received_at TEXT NOT NULL,
                    raw_evidence_json TEXT NOT NULL DEFAULT '{}',
                    UNIQUE(submission_id, venue_fill_id),
                    FOREIGN KEY(intent_id) REFERENCES phase_d_execution_intents(intent_id),
                    FOREIGN KEY(submission_id) REFERENCES phase_d_execution_submissions(submission_id)
                );
                CREATE INDEX IF NOT EXISTS idx_phase_d_execution_fills_intent
                    ON phase_d_execution_fills(intent_id, venue_timestamp);
                CREATE INDEX IF NOT EXISTS idx_phase_d_execution_fills_domain_account
                    ON phase_d_execution_fills(execution_domain, execution_account_id, venue_timestamp);
                CREATE UNIQUE INDEX IF NOT EXISTS idx_phase_d_execution_fills_venue_fill_id
                    ON phase_d_execution_fills(venue_fill_id);
                CREATE TABLE IF NOT EXISTS phase_d_execution_reconciliation_runs (
                    reconciliation_run_id TEXT PRIMARY KEY, scope TEXT NOT NULL,
                    execution_domain TEXT NOT NULL DEFAULT 'SIMULATOR',
                    execution_account_id TEXT NOT NULL DEFAULT 'SIMULATOR:default',
                    state TEXT NOT NULL, started_at TEXT NOT NULL, completed_at TEXT,
                    evidence_json TEXT NOT NULL DEFAULT '{}'
                );
                CREATE TABLE IF NOT EXISTS phase_d_execution_reconciliation_items (
                    item_id TEXT PRIMARY KEY, reconciliation_run_id TEXT NOT NULL,
                    intent_id TEXT, submission_id TEXT, item_type TEXT NOT NULL,
                    state TEXT NOT NULL, reason TEXT NOT NULL, local_json TEXT NOT NULL DEFAULT '{}',
                    venue_json TEXT NOT NULL DEFAULT '{}', recorded_at TEXT NOT NULL,
                    FOREIGN KEY(reconciliation_run_id) REFERENCES phase_d_execution_reconciliation_runs(reconciliation_run_id)
                );
                CREATE INDEX IF NOT EXISTS idx_phase_d_execution_reconciliation_items_run
                    ON phase_d_execution_reconciliation_items(reconciliation_run_id, state);
                CREATE TABLE IF NOT EXISTS phase_d_execution_position_observations (
                    observation_id TEXT PRIMARY KEY, reconciliation_run_id TEXT NOT NULL,
                    execution_domain TEXT NOT NULL DEFAULT 'SIMULATOR',
                    execution_account_id TEXT NOT NULL DEFAULT 'SIMULATOR:default', symbol TEXT NOT NULL, local_signed_quantity REAL NOT NULL,
                    venue_signed_quantity REAL, state TEXT NOT NULL, observed_at TEXT NOT NULL,
                    raw_evidence_json TEXT NOT NULL DEFAULT '{}',
                    FOREIGN KEY(reconciliation_run_id) REFERENCES phase_d_execution_reconciliation_runs(reconciliation_run_id)
                );
                CREATE INDEX IF NOT EXISTS idx_phase_d_execution_position_observations_scope_symbol
                    ON phase_d_execution_position_observations(execution_domain, execution_account_id, symbol, observed_at);
                CREATE TABLE IF NOT EXISTS phase_d_execution_integrity_issues (
                    issue_id TEXT PRIMARY KEY, execution_domain TEXT NOT NULL,
                    execution_account_id TEXT NOT NULL, intent_id TEXT, submission_id TEXT,
                    category TEXT NOT NULL, reason TEXT NOT NULL, existing_json TEXT NOT NULL DEFAULT '{}',
                    received_json TEXT NOT NULL DEFAULT '{}', recorded_at TEXT NOT NULL,
                    FOREIGN KEY(intent_id) REFERENCES phase_d_execution_intents(intent_id),
                    FOREIGN KEY(submission_id) REFERENCES phase_d_execution_submissions(submission_id)
                );
                CREATE INDEX IF NOT EXISTS idx_phase_d_execution_integrity_issues_scope
                    ON phase_d_execution_integrity_issues(execution_domain, execution_account_id, recorded_at);
                -- D.4 observations are independent, append-only read-only
                -- evidence. They are never execution authority by themselves.
                CREATE TABLE IF NOT EXISTS phase_d_shadow_observations (
                    observation_id TEXT PRIMARY KEY, execution_domain TEXT NOT NULL,
                    execution_account_id TEXT NOT NULL, venue TEXT NOT NULL, account_id TEXT NOT NULL,
                    state TEXT NOT NULL, freshness TEXT NOT NULL, observed_at TEXT, attempted_at TEXT NOT NULL,
                    received_at TEXT NOT NULL,
                    reason TEXT NOT NULL, components_json TEXT NOT NULL DEFAULT '{}',
                    normalized_json TEXT NOT NULL DEFAULT '{}', comparison_json TEXT NOT NULL DEFAULT '{}',
                    raw_evidence_json TEXT NOT NULL DEFAULT '{}'
                );
                CREATE INDEX IF NOT EXISTS idx_phase_d_shadow_observations_scope
                    ON phase_d_shadow_observations(execution_domain, execution_account_id, received_at, observation_id);
                """
            )
            self._ensure_column(connection, "copy_signals", "target_position_before", "REAL NOT NULL DEFAULT 0")
            self._ensure_column(connection, "phase_d_shadow_observations", "attempted_at", "TEXT")
            connection.execute(
                "UPDATE phase_d_shadow_observations SET attempted_at=received_at WHERE attempted_at IS NULL"
            )
            connection.execute(
                """CREATE INDEX IF NOT EXISTS idx_phase_d_shadow_observations_current
                   ON phase_d_shadow_observations(execution_domain, execution_account_id, attempted_at, received_at, observation_id)"""
            )
            self._ensure_column(connection, "phase_d_execution_intents", "execution_domain", "TEXT NOT NULL DEFAULT 'SIMULATOR'")
            self._ensure_column(connection, "phase_d_execution_intents", "execution_account_id", "TEXT NOT NULL DEFAULT 'SIMULATOR:default'")
            self._ensure_column(connection, "phase_d_execution_submissions", "execution_domain", "TEXT NOT NULL DEFAULT 'SIMULATOR'")
            self._ensure_column(connection, "phase_d_execution_submissions", "execution_account_id", "TEXT NOT NULL DEFAULT 'SIMULATOR:default'")
            self._ensure_column(connection, "phase_d_execution_fills", "execution_domain", "TEXT NOT NULL DEFAULT 'SIMULATOR'")
            self._ensure_column(connection, "phase_d_execution_fills", "execution_account_id", "TEXT NOT NULL DEFAULT 'SIMULATOR:default'")
            fill_side_added = self._ensure_column(
                connection, "phase_d_execution_fills", "side", "TEXT NOT NULL DEFAULT 'BUY'",
            )
            self._ensure_column(connection, "phase_d_execution_reconciliation_runs", "execution_domain", "TEXT NOT NULL DEFAULT 'SIMULATOR'")
            self._ensure_column(connection, "phase_d_execution_reconciliation_runs", "execution_account_id", "TEXT NOT NULL DEFAULT 'SIMULATOR:default'")
            self._ensure_column(connection, "phase_d_execution_position_observations", "execution_domain", "TEXT NOT NULL DEFAULT 'SIMULATOR'")
            self._ensure_column(connection, "phase_d_execution_position_observations", "execution_account_id", "TEXT NOT NULL DEFAULT 'SIMULATOR:default'")
            connection.execute(
                """UPDATE phase_d_execution_intents SET execution_domain='PAPER_COMPAT',
                   execution_account_id='PAPER_COMPAT:legacy_paper'
                   WHERE intent_id IN (
                       SELECT intent_id FROM phase_d_execution_state_events
                       WHERE source IN ('paper_execution_commit', 'paper_bridge')
                   )"""
            )
            connection.execute(
                """UPDATE phase_d_execution_submissions
                   SET execution_domain=(SELECT execution_domain FROM phase_d_execution_intents
                                         WHERE intent_id=phase_d_execution_submissions.intent_id),
                       execution_account_id=(SELECT execution_account_id FROM phase_d_execution_intents
                                             WHERE intent_id=phase_d_execution_submissions.intent_id)"""
            )
            connection.execute(
                """UPDATE phase_d_execution_fills
                   SET execution_domain=(SELECT execution_domain FROM phase_d_execution_submissions
                                         WHERE submission_id=phase_d_execution_fills.submission_id),
                       execution_account_id=(SELECT execution_account_id FROM phase_d_execution_submissions
                                             WHERE submission_id=phase_d_execution_fills.submission_id)"""
            )
            if fill_side_added:
                # Only pre-side-contract rows derive this unavailable field
                # from their submission.  Later reconciliation may record an
                # executed-side conflict, which must remain immutable.
                connection.execute(
                    """UPDATE phase_d_execution_fills
                       SET side=(SELECT side FROM phase_d_execution_submissions
                                 WHERE submission_id=phase_d_execution_fills.submission_id)"""
                )
            self._ensure_column(connection, "copy_signals", "target_leverage", "REAL")
            self._ensure_column(connection, "copy_raw_fills", "source_closed_pnl", "REAL")
            self._ensure_column(connection, "copy_raw_fills", "is_liquidation", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column(connection, "copy_reconstruction_cursors", "schema_version", "INTEGER NOT NULL DEFAULT 1")
            self._ensure_column(connection, "copy_reconstruction_cursors", "revision", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column(connection, "copy_reconstruction_cursors", "last_seen_timestamp", "TEXT")
            self._ensure_column(connection, "copy_reconstruction_cursors", "last_seen_event_id", "TEXT")
            self._ensure_column(connection, "copy_reconstruction_cursors", "last_processed_timestamp", "TEXT")
            self._ensure_column(connection, "copy_reconstruction_cursors", "last_processed_event_id", "TEXT")
            self._ensure_column(connection, "copy_reconstruction_cursors", "pending_fill_ids_json", "TEXT NOT NULL DEFAULT '[]'")
            self._ensure_column(connection, "copy_reconstruction_cursors", "pending_event_ids_json", "TEXT NOT NULL DEFAULT '[]'")
            self._ensure_column(connection, "copy_reconstruction_cursors", "recovery_state", "TEXT NOT NULL DEFAULT 'CONTINUOUS'")
            self._ensure_column(connection, "copy_reconstruction_cursors", "recovery_anchor_event_id", "TEXT")
            self._ensure_column(connection, "copy_reconstruction_cursors", "recovery_anchor_timestamp", "TEXT")
            self._ensure_column(connection, "copy_reconstruction_cursors", "recovery_detail_json", "TEXT NOT NULL DEFAULT '{}'")
            self._ensure_column(connection, "copy_analysis_finalist_recommendations", "recommendation_schema_version", "INTEGER NOT NULL DEFAULT 1")
            self._ensure_column(connection, "copy_position_events", "equity_source", "TEXT NOT NULL DEFAULT 'missing'")
            self._ensure_column(connection, "copy_position_events", "equity_age_seconds", "REAL")
            self._ensure_column(connection, "copy_position_events", "source_event_type", "TEXT")
            self._ensure_column(connection, "copy_position_events", "split_role", "TEXT")
            self._ensure_column(connection, "copy_position_events", "split_quantity", "REAL")
            self._ensure_column(connection, "copy_position_events", "split_notional", "REAL")
            self._ensure_column(connection, "copy_position_events", "split_fee", "REAL")
            self._ensure_column(connection, "copy_position_events", "source_closed_pnl", "REAL")
            self._ensure_column(connection, "copy_campaigns", "history_complete", "INTEGER NOT NULL DEFAULT 1")
            self._ensure_column(connection, "copy_campaigns", "entry_basis_quality", "TEXT NOT NULL DEFAULT 'observed'")
            self._ensure_column(connection, "copy_campaigns", "source_closed_pnl", "REAL NOT NULL DEFAULT 0")
            self._ensure_column(connection, "copy_campaigns", "source_closed_pnl_observed", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column(connection, "copy_campaigns", "reconciliation_gross_difference", "REAL")
            self._ensure_column(connection, "copy_campaigns", "liquidation_count", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column(connection, "copy_campaigns", "remaining_entry_notional", "REAL NOT NULL DEFAULT 0")
            self._ensure_column(connection, "copy_signals", "target_equity", "REAL")
            self._ensure_column(connection, "copy_signals", "equity_source", "TEXT NOT NULL DEFAULT 'missing'")
            self._ensure_column(connection, "copy_signals", "equity_age_seconds", "REAL")
            self._ensure_column(connection, "copy_virtual_positions", "current_mark", "REAL")
            self._ensure_column(connection, "copy_virtual_positions", "unrealized_pnl", "REAL NOT NULL DEFAULT 0")
            self._ensure_column(connection, "copy_portfolio_snapshots", "peak_equity", "REAL")
            self._ensure_column(connection, "copy_portfolio_snapshots", "max_drawdown_fraction", "REAL NOT NULL DEFAULT 0")
            self._ensure_column(connection, "copy_backfill_coverage", "coverage_state", "TEXT NOT NULL DEFAULT 'UNPROVEN'")
            self._ensure_column(connection, "copy_candidate_scores", "provenance", "TEXT NOT NULL DEFAULT 'legacy'")
            self._ensure_column(connection, "copy_candidate_scores", "analysis_run_id", "TEXT")
            self._ensure_column(connection, "copy_candidate_scores", "config_fingerprint", "TEXT")
            self._ensure_column(connection, "copy_candidate_scores", "confidence_score", "REAL NOT NULL DEFAULT 0")
            self._ensure_column(connection, "copy_candidate_scores", "hard_gates_json", "TEXT NOT NULL DEFAULT '[]'")
            self._ensure_column(connection, "copy_candidate_scores", "score_version", "TEXT NOT NULL DEFAULT 'phase_b_suitability_v3'")
            self._ensure_column(connection, "copy_discovery_runs", "eligible_wallets", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column(connection, "copy_discovery_runs", "limit_deferred_wallets", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column(connection, "copy_discovery_runs", "valid_events", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column(connection, "copy_discovery_runs", "normalized_observations", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column(connection, "copy_discovery_runs", "duplicate_events", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column(connection, "copy_discovery_runs", "invalid_wallets", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column(connection, "copy_discovery_runs", "malformed_events", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column(connection, "copy_discovery_runs", "unsupported_records", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column(connection, "copy_discovery_runs", "fatal_source_errors", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column(connection, "copy_discovery_observations", "source_event_id", "TEXT")
            connection.execute("CREATE INDEX IF NOT EXISTS idx_copy_targets_status ON copy_targets(status)")
            connection.execute("CREATE INDEX IF NOT EXISTS idx_copy_discovery_candidates_run ON copy_discovery_candidates(last_discovery_run_id)")
            connection.execute("CREATE INDEX IF NOT EXISTS idx_copy_discovery_observations_source_wallet ON copy_discovery_observations(source, wallet)")
            connection.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_copy_discovery_observations_event ON copy_discovery_observations(run_id, source_event_id)"
            )
            # B.1 could leave an orphan score if a process died after scoring
            # and before qualification. Preserve superseded records in a
            # migration archive before enforcing the B.2 run-level invariant.
            # The archive makes existing-database upgrades auditable instead
            # of silently deleting historical evidence.
            connection.execute(
                """CREATE TABLE IF NOT EXISTS copy_candidate_score_archive (
                    archive_id INTEGER PRIMARY KEY AUTOINCREMENT, original_score_rowid INTEGER NOT NULL UNIQUE,
                    archived_at TEXT NOT NULL, archive_reason TEXT NOT NULL, target_wallet TEXT NOT NULL,
                    calculated_at TEXT NOT NULL, total_score REAL NOT NULL, component_scores_json TEXT NOT NULL,
                    penalties_json TEXT NOT NULL, eligible INTEGER NOT NULL, reasons_json TEXT NOT NULL,
                    source_quality REAL NOT NULL, provenance TEXT NOT NULL, analysis_run_id TEXT,
                    config_fingerprint TEXT, confidence_score REAL NOT NULL DEFAULT 0,
                    hard_gates_json TEXT NOT NULL DEFAULT '[]', score_version TEXT NOT NULL DEFAULT 'phase_b_suitability_v3'
                )"""
            )
            self._ensure_column(connection, "copy_candidate_score_archive", "confidence_score", "REAL NOT NULL DEFAULT 0")
            self._ensure_column(connection, "copy_candidate_score_archive", "hard_gates_json", "TEXT NOT NULL DEFAULT '[]'")
            self._ensure_column(connection, "copy_candidate_score_archive", "score_version", "TEXT NOT NULL DEFAULT 'phase_b_suitability_v3'")
            connection.execute(
                """INSERT OR IGNORE INTO copy_candidate_score_archive(
                    original_score_rowid, archived_at, archive_reason, target_wallet, calculated_at, total_score,
                    component_scores_json, penalties_json, eligible, reasons_json, source_quality, provenance,
                    analysis_run_id, config_fingerprint, confidence_score, hard_gates_json, score_version)
                   SELECT score.rowid, ?, 'duplicate_phase_b_authority_migration', score.target_wallet, score.calculated_at, score.total_score,
                    score.component_scores_json, score.penalties_json, score.eligible, score.reasons_json, score.source_quality, score.provenance,
                    score.analysis_run_id, score.config_fingerprint, score.confidence_score, score.hard_gates_json, score.score_version
                   FROM copy_candidate_scores AS score
                   WHERE score.provenance='phase_b' AND score.analysis_run_id IS NOT NULL AND EXISTS (
                     SELECT 1 FROM copy_candidate_scores AS newer
                     WHERE newer.target_wallet=score.target_wallet AND newer.analysis_run_id=score.analysis_run_id
                       AND newer.provenance='phase_b' AND (
                         newer.calculated_at > score.calculated_at OR
                         (newer.calculated_at=score.calculated_at AND newer.rowid > score.rowid)
                       )
                   )""",
                (iso(None),),
            )
            connection.execute(
                """DELETE FROM copy_candidate_scores
                   WHERE provenance='phase_b' AND analysis_run_id IS NOT NULL AND EXISTS (
                     SELECT 1 FROM copy_candidate_scores AS newer
                     WHERE newer.target_wallet=copy_candidate_scores.target_wallet
                       AND newer.analysis_run_id=copy_candidate_scores.analysis_run_id
                       AND newer.provenance='phase_b' AND (
                         newer.calculated_at > copy_candidate_scores.calculated_at OR
                         (newer.calculated_at=copy_candidate_scores.calculated_at AND newer.rowid > copy_candidate_scores.rowid)
                       )
                   )"""
            )
            connection.execute(
                """CREATE UNIQUE INDEX IF NOT EXISTS idx_copy_phase_b_score_authority
                   ON copy_candidate_scores(target_wallet, analysis_run_id, provenance)
                   WHERE provenance='phase_b' AND analysis_run_id IS NOT NULL"""
            )
            duplicate_attempt = connection.execute(
                "SELECT 1 FROM copy_execution_attempts GROUP BY signal_id HAVING COUNT(*) > 1 LIMIT 1"
            ).fetchone()
            # Fresh and already-correct databases get an explicit database
            # uniqueness guard.  A legacy duplicate is retained for audit; the
            # new claim table still prevents any further duplicate execution.
            if not duplicate_attempt:
                connection.execute(
                    "CREATE UNIQUE INDEX IF NOT EXISTS idx_copy_execution_attempts_signal ON copy_execution_attempts(signal_id)"
                )

    @staticmethod
    def _ensure_column(connection: sqlite3.Connection, table: str, column: str, definition: str) -> bool:
        present = {str(row["name"]) for row in connection.execute(f"PRAGMA table_info({table})").fetchall()}
        if column not in present:
            connection.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
            return True
        return False

    def upsert_target(self, target: Target) -> None:
        with self._connect() as connection:
            connection.execute(
                """INSERT INTO copy_targets(wallet, source, venue, status, label, created_at, updated_at, metadata_json)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(wallet) DO UPDATE SET source=excluded.source, venue=excluded.venue,
                   status=excluded.status, label=excluded.label, updated_at=excluded.updated_at,
                   metadata_json=excluded.metadata_json""",
                (target.normalized_wallet(), target.source, target.venue, getattr(target.status, "value", str(target.status)), target.label,
                 iso(target.created_at), iso(target.updated_at), _dump(target.metadata)),
            )

    def list_targets(self, status: str | None = None) -> list[Target]:
        query = "SELECT * FROM copy_targets"
        values: tuple[Any, ...] = ()
        if status:
            query += " WHERE status = ?"
            values = (status,)
        query += " ORDER BY created_at, wallet"
        with self._connect() as connection:
            rows = connection.execute(query, values).fetchall()
        return [
            Target(wallet=row["wallet"], source=row["source"], venue=row["venue"], status=row["status"],
                   label=row["label"], created_at=as_utc(row["created_at"]), updated_at=as_utc(row["updated_at"]),
                   metadata=_load(row["metadata_json"], {}))
            for row in rows
        ]

    def get_target(self, wallet: str) -> Target | None:
        targets = self.list_targets()
        normalized = wallet.lower()
        return next((target for target in targets if target.wallet.lower() == normalized), None)

    def set_target_status(self, wallet: str, status: str) -> bool:
        """Low-level persistence primitive for internal migrations and fixtures.

        Application-level callers must use ``CopyTradeService.set_status`` for
        ordinary non-Active transitions or ``CopyControlCenter.activate_wallet``
        for the privileged Active transition. This storage layer deliberately
        does not duplicate Phase-C authority policy.
        """
        with self._connect() as connection:
            cursor = connection.execute(
                "UPDATE copy_targets SET status=?, updated_at=? WHERE wallet=?",
                (status, iso(None), wallet.lower()),
            )
            if cursor.rowcount:
                connection.execute("UPDATE copy_discovery_candidates SET discovery_status=? WHERE wallet=?", (status, wallet.lower()))
        return cursor.rowcount > 0

    def start_discovery_run(self, run: DiscoveryRun) -> None:
        with self._connect() as connection:
            connection.execute(
                """INSERT INTO copy_discovery_runs(run_id, started_at, finished_at, status, sources_json, configuration_json,
                wallets_seen, eligible_wallets, limit_deferred_wallets, new_wallets, existing_wallets_refreshed,
                filtered_wallets, queued_for_analysis, errors_json, valid_events, normalized_observations,
                duplicate_events, invalid_wallets, malformed_events, unsupported_records, fatal_source_errors)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (run.run_id, iso(run.started_at), iso(run.finished_at) if run.finished_at else None, run.status,
                 _dump(run.sources), _dump(run.configuration), run.wallets_seen, run.eligible_wallets,
                 run.limit_deferred_wallets, run.new_wallets, run.existing_wallets_refreshed,
                 run.filtered_wallets, run.queued_for_analysis, _dump(run.errors), run.valid_events,
                 run.normalized_observations, run.duplicate_events, run.invalid_wallets, run.malformed_events,
                 run.unsupported_records, run.fatal_source_errors),
            )

    def finish_discovery_run(
        self, run_id: str, *, status: str, errors: tuple[str, ...] = (), wallets_seen: int = 0,
        eligible_wallets: int = 0, limit_deferred_wallets: int = 0, new_wallets: int = 0,
        existing_wallets_refreshed: int = 0, filtered_wallets: int = 0, queued_for_analysis: int = 0,
        valid_events: int = 0, normalized_observations: int = 0, duplicate_events: int = 0,
        invalid_wallets: int = 0, malformed_events: int = 0, unsupported_records: int = 0,
        fatal_source_errors: int = 0,
    ) -> None:
        with self._connect() as connection:
            connection.execute(
                """UPDATE copy_discovery_runs SET finished_at=?, status=?, wallets_seen=?, eligible_wallets=?,
                limit_deferred_wallets=?, new_wallets=?, existing_wallets_refreshed=?, filtered_wallets=?,
                queued_for_analysis=?, errors_json=?, valid_events=?, normalized_observations=?, duplicate_events=?,
                invalid_wallets=?, malformed_events=?, unsupported_records=?, fatal_source_errors=? WHERE run_id=?""",
                (iso(None), status, wallets_seen, eligible_wallets, limit_deferred_wallets, new_wallets,
                 existing_wallets_refreshed, filtered_wallets, queued_for_analysis, _dump(errors), valid_events,
                 normalized_observations, duplicate_events, invalid_wallets, malformed_events, unsupported_records,
                 fatal_source_errors, run_id),
            )

    @staticmethod
    def new_discovery_stage_stats() -> DiscoveryStageStats:
        return DiscoveryStageStats()

    def stage_discovery_observations(
        self, run_id: str, observations: Iterable[DiscoveryObservation], *, batch_size: int = 500,
        statistics: DiscoveryStageStats | None = None,
    ) -> DiscoveryStageStats:
        """Append de-duplicated run evidence in bounded transactions before candidate selection."""
        if batch_size <= 0:
            raise ValueError("Discovery observation batch size must be positive.")
        statistics = statistics or DiscoveryStageStats()
        batch: list[tuple[Any, ...]] = []

        def flush() -> None:
            if not batch:
                return
            with self._connect() as connection:
                for row in batch:
                    cursor = connection.execute(
                        """INSERT OR IGNORE INTO copy_discovery_observations(observation_id, run_id, wallet, source, observed_at,
                        recent_activity_at, discovery_rank, source_score, metadata_json, raw_evidence_json, evidence_id, source_event_id)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        row,
                    )
                    if cursor.rowcount == 0:
                        statistics.duplicate_events += 1
            batch.clear()

        for observation in observations:
            statistics.normalized_observations += 1
            wallet = observation.normalized_wallet()
            if not _is_wallet(wallet) or not observation.source:
                statistics.invalid_wallets += 1
                statistics.rejections.append({
                    "category": "invalid_wallet", "message": "Discovery observation has an invalid wallet or missing source.",
                    "source": observation.source, "record_index": None, "event_index": None,
                    "raw_record": observation.raw_evidence,
                })
                continue
            source_event_id = _discovery_source_event_id(observation)
            batch.append((
                stable_id("discovery_observation", run_id, source_event_id), run_id, wallet, observation.source,
                iso(observation.observed_at), iso(observation.recent_activity_at) if observation.recent_activity_at else None,
                observation.discovery_rank, observation.source_score, _dump(observation.metadata),
                _dump(observation.raw_evidence), observation.evidence_id, source_event_id,
            ))
            if len(batch) >= batch_size:
                flush()
        flush()
        return statistics

    def record_discovery_rejections(self, run_id: str, rejections: Iterable[object]) -> None:
        rows: list[tuple[Any, ...]] = []
        for rejection in rejections:
            if isinstance(rejection, Mapping):
                value = rejection
            else:
                value = {
                    "category": getattr(rejection, "category", "malformed_event"),
                    "message": getattr(rejection, "message", str(rejection)),
                    "source": getattr(rejection, "source", None),
                    "record_index": getattr(rejection, "record_index", None),
                    "event_index": getattr(rejection, "event_index", None),
                    "raw_record": getattr(rejection, "raw_record", {}),
                }
            rows.append((
                run_id, str(value.get("category") or "malformed_event"), str(value.get("message") or "rejected discovery record"),
                value.get("source"), value.get("record_index"), value.get("event_index"),
                _dump(value.get("raw_record") or {}), iso(None),
            ))
        if not rows:
            return
        with self._connect() as connection:
            connection.executemany(
                """INSERT INTO copy_discovery_rejections(run_id, category, message, source, record_index, event_index,
                raw_record_json, recorded_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                rows,
            )

    def discard_discovery_observations(self, run_id: str) -> None:
        with self._connect() as connection:
            connection.execute("DELETE FROM copy_discovery_observations WHERE run_id=?", (run_id,))

    def complete_discovery_run(
        self, run: DiscoveryRun, *, limit: int, min_activity: int, max_activity_age_seconds: float | None,
        valid_events: int = 0, normalized_observations: int = 0, duplicate_events: int = 0,
        invalid_wallets: int = 0, malformed_events: int = 0, unsupported_records: int = 0,
    ) -> DiscoverySummary:
        """Aggregate staged evidence without retaining the raw input in process memory."""
        cutoff = run.started_at.timestamp() - max_activity_age_seconds if max_activity_age_seconds is not None else None
        with self._connect() as connection:
            # Keep raw Phase A evidence in SQLite.  Completion scales with the
            # candidate-wallet result set rather than constructing a Python
            # list for every staged observation in a Deep scan.  JSON1 is part
            # of the supported SQLite runtime used by this application.
            rows = connection.execute(
                """SELECT wallet, COUNT(*) AS activity_count, MIN(observed_at) AS first_seen, MAX(observed_at) AS last_seen,
                MAX(recent_activity_at) AS recent_activity_at, MIN(discovery_rank) AS discovery_rank,
                MAX(source_score) AS source_score, COUNT(DISTINCT source) AS independent_source_count,
                COUNT(DISTINCT strftime('%Y-%m-%dT%H', recent_activity_at)) AS distinct_active_hours,
                COUNT(DISTINCT date(recent_activity_at)) AS distinct_active_days,
                COALESCE((julianday(MAX(recent_activity_at)) - julianday(MIN(recent_activity_at))) * 24.0, 0.0) AS observation_span_hours,
                COUNT(DISTINCT CASE WHEN json_valid(metadata_json)
                                    THEN NULLIF(UPPER(json_extract(metadata_json, '$.coin')), '') END) AS distinct_symbols,
                GROUP_CONCAT(DISTINCT CASE WHEN json_valid(metadata_json)
                                           THEN NULLIF(UPPER(json_extract(metadata_json, '$.coin')), '') END) AS symbols_csv,
                SUM(CASE WHEN source_score > 0 THEN source_score ELSE 0 END) AS approximate_observed_notional,
                MIN(recent_activity_at) AS first_observed_activity,
                MAX(recent_activity_at) AS last_observed_activity,
                GROUP_CONCAT(DISTINCT source) AS latest_sources_csv
                FROM copy_discovery_observations WHERE run_id=? GROUP BY wallet""",
                (run.run_id,),
            ).fetchall()
            aggregates = [dict(row) for row in rows]
            # `source_count` is lifetime evidence, unlike the `latest_*`
            # metadata above.  Aggregate it once instead of executing a full
            # observation-table query for every selected candidate.
            source_counts = {
                str(row["wallet"]): int(row["source_count"])
                for row in connection.execute(
                    "SELECT wallet, COUNT(DISTINCT source) AS source_count FROM copy_discovery_observations GROUP BY wallet"
                ).fetchall()
            }
            eligible = [
                row for row in aggregates
                if int(row["activity_count"]) >= min_activity
                and (cutoff is None or (
                    row["recent_activity_at"] is not None
                    and as_utc(row["recent_activity_at"]).timestamp() >= cutoff
                ))
            ]
            eligible.sort(key=lambda row: (
                -int(row["activity_count"]),
                -as_utc(row["recent_activity_at"] or row["last_seen"]).timestamp(),
                row["wallet"],
            ))
            selected = eligible[:limit]
            wallets_seen = len(aggregates)
            eligible_wallets = len(eligible)
            limit_deferred_wallets = eligible_wallets - len(selected)
            filtered_wallets = wallets_seen - eligible_wallets
            errors = tuple(
                f"{name}:{count}" for name, count in (
                    ("duplicate_events", duplicate_events), ("invalid_wallets_rejected", invalid_wallets),
                    ("malformed_events_rejected", malformed_events), ("unsupported_records_rejected", unsupported_records),
                ) if count
            )
            status = "completed_with_warnings" if errors else "completed"
            new_wallets = 0
            existing_wallets_refreshed = 0
            queued_for_analysis = 0
            for aggregate in selected:
                wallet = str(aggregate["wallet"])
                target = connection.execute("SELECT status FROM copy_targets WHERE wallet=?", (wallet,)).fetchone()
                existing = connection.execute("SELECT * FROM copy_discovery_candidates WHERE wallet=?", (wallet,)).fetchone()
                candidate_status = str(target["status"]) if target is not None else TargetStatus.NEW.value
                first_seen = as_utc(aggregate["first_seen"])
                last_seen = as_utc(aggregate["last_seen"])
                activity = as_utc(aggregate["recent_activity_at"]) if aggregate["recent_activity_at"] else None
                source_count = source_counts.get(wallet, 0)
                symbols = sorted(filter(None, str(aggregate["symbols_csv"] or "").split(",")))
                metadata = {
                    "evidence_schema_version": PHASE_A_EVIDENCE_SCHEMA_VERSION,
                    "latest_sources": sorted(filter(None, str(aggregate["latest_sources_csv"] or "").split(","))),
                    "latest_activity_observations": int(aggregate["activity_count"]),
                    "cheap_stats": {
                        "distinct_observed_events": int(aggregate["activity_count"]),
                        "distinct_active_hours": int(aggregate["distinct_active_hours"]),
                        "distinct_active_days": int(aggregate["distinct_active_days"]),
                        "observation_span_hours": max(0.0, float(aggregate["observation_span_hours"] or 0.0)),
                        "distinct_symbols": int(aggregate["distinct_symbols"]),
                        "symbols": symbols,
                        "approximate_observed_notional": float(aggregate["approximate_observed_notional"] or 0.0),
                        "independent_source_count": int(aggregate["independent_source_count"]),
                        "first_observed_activity": aggregate["first_observed_activity"],
                        "last_observed_activity": aggregate["last_observed_activity"],
                    },
                }
                if target is None:
                    connection.execute(
                        """INSERT INTO copy_targets(wallet, source, venue, status, label, created_at, updated_at, metadata_json)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                        (wallet, "hyperliquid", "hyperliquid", TargetStatus.NEW.value, "", iso(first_seen), iso(last_seen),
                         _dump({"discovery_run_id": run.run_id, "discovery_sources": metadata["latest_sources"]})),
                    )
                if existing is None:
                    new_wallets += 1
                    connection.execute(
                        """INSERT INTO copy_discovery_candidates(wallet, discovered_at, last_seen_at, recent_activity_at,
                        discovery_rank, source_score, source_count, discovery_status, last_discovery_run_id, metadata_json)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (wallet, iso(first_seen), iso(last_seen), iso(activity) if activity else None,
                         aggregate["discovery_rank"], aggregate["source_score"], source_count,
                         candidate_status, run.run_id, _dump(metadata)),
                    )
                else:
                    existing_wallets_refreshed += 1
                    prior_seen = as_utc(existing["last_seen_at"])
                    prior_activity = as_utc(existing["recent_activity_at"]) if existing["recent_activity_at"] else None
                    connection.execute(
                        """UPDATE copy_discovery_candidates SET last_seen_at=?, recent_activity_at=?, discovery_rank=?,
                        source_score=?, source_count=?, discovery_status=?, last_discovery_run_id=?, metadata_json=? WHERE wallet=?""",
                        (iso(max(prior_seen, last_seen)),
                         iso(max(filter(None, (prior_activity, activity)))) if prior_activity or activity else None,
                         min([int(existing["discovery_rank"]), int(aggregate["discovery_rank"])] if existing["discovery_rank"] is not None and aggregate["discovery_rank"] is not None else [value for value in (existing["discovery_rank"], aggregate["discovery_rank"]) if value is not None]) if existing["discovery_rank"] is not None or aggregate["discovery_rank"] is not None else None,
                         max([float(existing["source_score"]), float(aggregate["source_score"])] if existing["source_score"] is not None and aggregate["source_score"] is not None else [value for value in (existing["source_score"], aggregate["source_score"]) if value is not None]) if existing["source_score"] is not None or aggregate["source_score"] is not None else None,
                         source_count, candidate_status, run.run_id, _dump(metadata), wallet),
                    )
                if candidate_status in {TargetStatus.NEW.value, TargetStatus.QUEUED.value, TargetStatus.PENDING.value}:
                    queued_for_analysis += 1
            connection.execute(
                """UPDATE copy_discovery_runs SET finished_at=?, status=?, wallets_seen=?, eligible_wallets=?,
                limit_deferred_wallets=?, new_wallets=?, existing_wallets_refreshed=?, filtered_wallets=?,
                queued_for_analysis=?, errors_json=?, valid_events=?, normalized_observations=?, duplicate_events=?,
                invalid_wallets=?, malformed_events=?, unsupported_records=?, fatal_source_errors=0 WHERE run_id=?""",
                (iso(None), status, wallets_seen, eligible_wallets, limit_deferred_wallets, new_wallets,
                 existing_wallets_refreshed, filtered_wallets, queued_for_analysis, _dump(errors), valid_events,
                 normalized_observations, duplicate_events, invalid_wallets, malformed_events, unsupported_records, run.run_id),
            )
        return DiscoverySummary(
            run_id=run.run_id, status=status, sources=run.sources, wallets_seen=wallets_seen,
            eligible_wallets=eligible_wallets, limit_deferred_wallets=limit_deferred_wallets,
            new_wallets=new_wallets, existing_wallets_refreshed=existing_wallets_refreshed,
            filtered_wallets=filtered_wallets, queued_for_analysis=queued_for_analysis, errors=errors,
            valid_events=valid_events, normalized_observations=normalized_observations, duplicate_events=duplicate_events,
            invalid_wallets=invalid_wallets, malformed_events=malformed_events, unsupported_records=unsupported_records,
        )

    def list_discovery_candidates(self, *, limit: int = 100, source: str | None = None) -> list[dict[str, Any]]:
        clauses = []
        values: list[Any] = []
        if source:
            clauses.append("EXISTS (SELECT 1 FROM copy_discovery_observations observation WHERE observation.wallet=candidate.wallet AND observation.source=?)")
            values.append(source)
        query = """SELECT candidate.*, target.status AS current_status FROM copy_discovery_candidates candidate
                   LEFT JOIN copy_targets target ON target.wallet=candidate.wallet"""
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY candidate.recent_activity_at DESC, candidate.last_seen_at DESC LIMIT ?"
        values.append(limit)
        with self._connect() as connection:
            rows = connection.execute(query, values).fetchall()
        return [dict(row) for row in rows]

    def list_discovery_runs(self, *, limit: int = 50) -> list[dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute("SELECT * FROM copy_discovery_runs ORDER BY started_at DESC LIMIT ?", (limit,)).fetchall()
        return [dict(row) for row in rows]

    def start_analysis_run(self, run: AnalysisRun) -> None:
        with self._connect() as connection:
            connection.execute(
                """INSERT INTO copy_analysis_runs(run_id, started_at, finished_at, status, configuration_json,
                wallets_considered, cheap_rejected, backfill_attempted, backfill_failed, reconstructed, scored,
                eligible, rejected, deferred, errors_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (run.run_id, iso(run.started_at), iso(run.finished_at) if run.finished_at else None, run.status,
                 _dump(run.configuration), run.wallets_considered, run.cheap_rejected, run.backfill_attempted,
                 run.backfill_failed, run.reconstructed, run.scored, run.eligible, run.rejected, run.deferred,
                 _dump(run.errors)),
            )

    def finish_analysis_run(
        self, run_id: str, *, status: str, wallets_considered: int, cheap_rejected: int,
        backfill_attempted: int, backfill_failed: int, reconstructed: int, scored: int, eligible: int,
        rejected: int, deferred: int, errors: tuple[str, ...] = (),
    ) -> None:
        with self._connect() as connection:
            connection.execute(
                """UPDATE copy_analysis_runs SET finished_at=?, status=?, wallets_considered=?, cheap_rejected=?,
                backfill_attempted=?, backfill_failed=?, reconstructed=?, scored=?, eligible=?, rejected=?,
                deferred=?, errors_json=? WHERE run_id=?""",
                (iso(None), status, wallets_considered, cheap_rejected, backfill_attempted, backfill_failed,
                 reconstructed, scored, eligible, rejected, deferred, _dump(errors), run_id),
            )

    def get_analysis_run(self, run_id: str) -> dict[str, Any] | None:
        with self._connect() as connection:
            row = connection.execute("SELECT * FROM copy_analysis_runs WHERE run_id=?", (run_id,)).fetchone()
        return dict(row) if row else None

    def set_analysis_run_configuration(self, run_id: str, configuration: dict[str, Any]) -> None:
        """Finalize an initial immutable run manifest before any wallet work."""
        with self._connect() as connection:
            connection.execute(
                "UPDATE copy_analysis_runs SET configuration_json=? WHERE run_id=? AND status='running'",
                (_dump(configuration), run_id),
            )

    def latest_resumable_analysis_run(self) -> dict[str, Any] | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM copy_analysis_runs WHERE status='running' ORDER BY started_at DESC LIMIT 1"
            ).fetchone()
        return dict(row) if row else None

    def list_analysis_runs(self, *, limit: int = 50) -> list[dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute("SELECT * FROM copy_analysis_runs ORDER BY started_at DESC LIMIT ?", (limit,)).fetchall()
        return [dict(row) for row in rows]

    def record_analysis_wallet(
        self, run_id: str, wallet: str, *, stage: str, status: str, attempts: int = 0,
        error: str | None = None, payload: dict[str, Any] | None = None,
    ) -> None:
        with self._connect() as connection:
            # The projection makes a current run easy to inspect; the append-only
            # stream preserves every prefilter/backfill/retry/analysis decision.
            connection.execute(
                """INSERT INTO copy_analysis_run_wallet_events(run_id, wallet, stage, status, attempt, recorded_at, error, payload_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (run_id, wallet.lower(), stage, status, attempts, iso(None), error, _dump(payload or {})),
            )
            connection.execute(
                """INSERT INTO copy_analysis_run_wallets(run_id, wallet, stage, status, attempts, error, payload_json, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(run_id, wallet) DO UPDATE SET stage=excluded.stage, status=excluded.status,
                attempts=excluded.attempts, error=excluded.error, payload_json=excluded.payload_json, updated_at=excluded.updated_at""",
                (run_id, wallet.lower(), stage, status, attempts, error, _dump(payload or {}), iso(None)),
            )

    def list_analysis_wallet_events(self, run_id: str, wallet: str | None = None) -> list[dict[str, Any]]:
        query = "SELECT * FROM copy_analysis_run_wallet_events WHERE run_id=?"
        values: list[Any] = [run_id]
        if wallet:
            query += " AND wallet=?"
            values.append(wallet.lower())
        query += " ORDER BY event_id"
        with self._connect() as connection:
            rows = connection.execute(query, values).fetchall()
        return [
            {
                **dict(row),
                "payload": _load(row["payload_json"], {}),
            }
            for row in rows
        ]

    def analysis_run_counters(self, run_id: str) -> dict[str, int]:
        """Rebuild final counters from durable events rather than RAM state.

        A wallet is counted once per terminal/current stage.  This makes a
        process interruption harmless: a later resume appends additional events
        and the current projection determines deferred/qualification state.
        """
        events = self.list_analysis_wallet_events(run_id)
        latest: dict[str, dict[str, Any]] = {}
        seen_prefilter: set[str] = set()
        cheap_rejected: set[str] = set()
        backfill_attempted: set[str] = set()
        backfill_failed: set[str] = set()
        reconstructed: set[str] = set()
        scored: set[str] = set()
        for event in events:
            wallet = str(event["wallet"])
            latest[wallet] = event
            stage, status, payload = str(event["stage"]), str(event["status"]), event["payload"]
            if stage == "prefilter":
                seen_prefilter.add(wallet)
                if status == "rejected":
                    cheap_rejected.add(wallet)
            if stage == "backfill" and status in {"started", "completed", "failed", "quarantined"}:
                backfill_attempted.add(wallet)
                if status == "failed":
                    backfill_failed.add(wallet)
            if stage == "analysis" and status == "completed":
                if payload.get("reconstructed", True):
                    reconstructed.add(wallet)
                if payload.get("scored", True):
                    scored.add(wallet)
        eligible: set[str] = set()
        rejected: set[str] = set()
        deferred: set[str] = set()
        for wallet, event in latest.items():
            stage, status, payload = str(event["stage"]), str(event["status"]), event["payload"]
            if stage == "prefilter" and status == "rejected":
                rejected.add(wallet)
            elif stage == "backfill" and status == "quarantined":
                rejected.add(wallet)
            elif stage == "analysis" and status == "completed":
                (eligible if bool(payload.get("eligible")) else rejected).add(wallet)
            elif status in {"deferred", "failed", "skipped"}:
                deferred.add(wallet)
        return {
            "wallets_considered": len(seen_prefilter), "cheap_rejected": len(cheap_rejected),
            "backfill_attempted": len(backfill_attempted), "backfill_failed": len(backfill_failed),
            "reconstructed": len(reconstructed), "scored": len(scored), "eligible": len(eligible),
            "rejected": len(rejected), "deferred": len(deferred),
        }

    def analysis_funnel(
        self, run_id: str, *, high_suitability_score: float, config_fingerprint: str | None = None,
    ) -> dict[str, dict[str, float | int]]:
        """Durable funnel projection for an immutable analysis run."""
        counters = self.analysis_run_counters(run_id)
        with self._connect() as connection:
            quarantined = int(connection.execute(
                """SELECT COUNT(DISTINCT wallet) FROM copy_analysis_run_wallet_events
                   WHERE run_id=? AND stage='backfill' AND status='quarantined'""", (run_id,),
            ).fetchone()[0])
            high_suitability = int(connection.execute(
                """SELECT COUNT(*) FROM copy_candidate_scores
                   WHERE provenance='phase_b' AND analysis_run_id=? AND eligible=1 AND total_score>=?""",
                (run_id, high_suitability_score),
            ).fetchone()[0])
            selection_query = """SELECT COUNT(*) FROM copy_analysis_finalist_recommendations
                                 WHERE analysis_run_id=? AND selection_rank IS NOT NULL"""
            selection_values: list[Any] = [run_id]
            if config_fingerprint is not None:
                selection_query += " AND config_fingerprint=?"
                selection_values.append(config_fingerprint)
            diversification_selected = int(connection.execute(selection_query, selection_values).fetchone()[0])
        observed = int(counters["wallets_considered"])
        stages = {
            "wallets_observed": observed,
            "cheap_eligible": max(0, observed - int(counters["cheap_rejected"])),
            "cheap_rejected": int(counters["cheap_rejected"]),
            "backfill_attempted": int(counters["backfill_attempted"]),
            "coverage_quarantined": quarantined,
            "reconstructed": int(counters["reconstructed"]),
            "scored": int(counters["scored"]),
            "eligible": int(counters["eligible"]),
            "high_suitability": high_suitability,
            "diversification_selected": diversification_selected,
            "shadow_finalists": diversification_selected,
        }
        return {
            name: {"count": count, "percent_of_observed": 100.0 * count / observed if observed else 0.0}
            for name, count in stages.items()
        }

    def get_analysis_market_evidence(
        self, analysis_run_id: str, symbol: str, bucket_timestamp: str,
    ) -> dict[str, Any] | None:
        with self._connect() as connection:
            row = connection.execute(
                """SELECT symbol, bucket_timestamp, price, source, quality, market_timestamp AS timestamp,
                          requested_for_timestamp, resolution
                   FROM copy_analysis_market_evidence
                   WHERE analysis_run_id=? AND symbol=? AND bucket_timestamp=?""",
                (analysis_run_id, symbol.upper(), bucket_timestamp),
            ).fetchone()
        return dict(row) if row else None

    def insert_analysis_market_evidence(self, analysis_run_id: str, evidence: Mapping[str, object]) -> dict[str, Any]:
        """Persist the first selected market observation; never rewrite it in a completed run."""
        symbol = str(evidence["symbol"]).upper()
        bucket = str(evidence["bucket_timestamp"])
        with self._connect() as connection:
            connection.execute(
                """INSERT OR IGNORE INTO copy_analysis_market_evidence(
                    analysis_run_id, symbol, bucket_timestamp, price, source, quality, market_timestamp,
                    requested_for_timestamp, resolution, recorded_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (analysis_run_id, symbol, bucket, evidence.get("price"), str(evidence.get("source") or "unavailable"),
                 str(evidence.get("quality") or "missing_historical_price"), evidence.get("timestamp"),
                 str(evidence.get("requested_for_timestamp") or bucket), str(evidence.get("resolution") or "unspecified"), iso(None)),
            )
            row = connection.execute(
                """SELECT symbol, bucket_timestamp, price, source, quality, market_timestamp AS timestamp,
                          requested_for_timestamp, resolution
                   FROM copy_analysis_market_evidence
                   WHERE analysis_run_id=? AND symbol=? AND bucket_timestamp=?""",
                (analysis_run_id, symbol, bucket),
            ).fetchone()
        assert row is not None
        return dict(row)

    def upsert_finalist_recommendations(
        self, config_fingerprint: str, recommendations: Iterable[Mapping[str, object]],
    ) -> None:
        """Persist recommendation-only finalist evidence without changing any target state or score."""
        with self._connect() as connection:
            for item in recommendations:
                connection.execute(
                    """INSERT INTO copy_analysis_finalist_recommendations(
                        analysis_run_id, config_fingerprint, wallet, recommendation_schema_version, finalist_eligible,
                        finalist_rejection_reasons_json, diversification_penalty, final_selection_score,
                        selection_rank, evaluated_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                       ON CONFLICT(analysis_run_id, config_fingerprint, wallet) DO UPDATE SET
                         recommendation_schema_version=excluded.recommendation_schema_version,
                         finalist_eligible=excluded.finalist_eligible,
                         finalist_rejection_reasons_json=excluded.finalist_rejection_reasons_json,
                         diversification_penalty=excluded.diversification_penalty,
                         final_selection_score=excluded.final_selection_score,
                         selection_rank=excluded.selection_rank,
                         evaluated_at=excluded.evaluated_at""",
                    (str(item["analysis_run_id"]), config_fingerprint, str(item["wallet"]).lower(),
                     PHASE_B_RECOMMENDATION_SCHEMA_VERSION, int(bool(item.get("finalist_eligible"))), _dump(item.get("finalist_rejection_reasons", ())),
                     item.get("diversification_penalty"), item.get("final_selection_score"), item.get("selection_rank"), iso(None)),
                )

    def get_finalist_recommendation(
        self, analysis_run_id: str | None, config_fingerprint: str, wallet: str,
    ) -> dict[str, Any] | None:
        if not analysis_run_id:
            return None
        with self._connect() as connection:
            row = connection.execute(
                """SELECT * FROM copy_analysis_finalist_recommendations
                   WHERE analysis_run_id=? AND config_fingerprint=? AND wallet=?""",
                (analysis_run_id, config_fingerprint, wallet.lower()),
            ).fetchone()
        if not row:
            return None
        value = dict(row)
        value["finalist_eligible"] = bool(value["finalist_eligible"])
        value["finalist_rejection_reasons"] = _load(value.pop("finalist_rejection_reasons_json"), [])
        return value

    def list_finalist_recommendations(
        self, config_fingerprint: str, *, selected_only: bool = True,
    ) -> list[dict[str, Any]]:
        """Read Phase B's persisted authority without recomputing a cohort.

        ``selected_only`` returns the diversified final selection in Phase-B
        rank order.  Phase C deliberately uses this API rather than scores so
        the two phases cannot disagree about eligibility or diversification.
        """
        where = "config_fingerprint=?"
        if selected_only:
            where += " AND finalist_eligible=1 AND selection_rank IS NOT NULL"
        with self._connect() as connection:
            rows = connection.execute(
                f"""SELECT * FROM copy_analysis_finalist_recommendations
                    WHERE {where}
                    ORDER BY CASE WHEN selection_rank IS NULL THEN 1 ELSE 0 END,
                             selection_rank ASC, wallet ASC""",
                (config_fingerprint,),
            ).fetchall()
        results: list[dict[str, Any]] = []
        for row in rows:
            value = dict(row)
            value["finalist_eligible"] = bool(value["finalist_eligible"])
            value["finalist_rejection_reasons"] = _load(value.pop("finalist_rejection_reasons_json"), [])
            results.append(value)
        return results

    def get_analysis_wallet(self, run_id: str, wallet: str) -> dict[str, Any] | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM copy_analysis_run_wallets WHERE run_id=? AND wallet=?", (run_id, wallet.lower())
            ).fetchone()
        return dict(row) if row else None

    def upsert_candidate_analysis(self, analysis: CandidateAnalysis) -> None:
        with self._connect() as connection:
            connection.execute(
                """INSERT INTO copy_candidate_analyses(wallet, lifecycle_status, last_run_id, started_at, completed_at,
                prefilter_reasons_json, errors_json, summary_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(wallet) DO UPDATE SET lifecycle_status=excluded.lifecycle_status,
                last_run_id=excluded.last_run_id, started_at=excluded.started_at, completed_at=excluded.completed_at,
                prefilter_reasons_json=excluded.prefilter_reasons_json, errors_json=excluded.errors_json,
                summary_json=excluded.summary_json""",
                (analysis.wallet.lower(), analysis.lifecycle_status, analysis.last_run_id,
                 iso(analysis.started_at) if analysis.started_at else None,
                 iso(analysis.completed_at) if analysis.completed_at else None,
                 _dump(analysis.prefilter_reasons), _dump(analysis.errors), _dump(analysis.summary)),
            )

    def get_candidate_analysis(self, wallet: str) -> CandidateAnalysis | None:
        with self._connect() as connection:
            row = connection.execute("SELECT * FROM copy_candidate_analyses WHERE wallet=?", (wallet.lower(),)).fetchone()
        if not row:
            return None
        return CandidateAnalysis(
            wallet=row["wallet"], lifecycle_status=row["lifecycle_status"], last_run_id=row["last_run_id"],
            started_at=as_utc(row["started_at"]) if row["started_at"] else None,
            completed_at=as_utc(row["completed_at"]) if row["completed_at"] else None,
            prefilter_reasons=tuple(_load(row["prefilter_reasons_json"], [])), errors=tuple(_load(row["errors_json"], [])),
            summary=_load(row["summary_json"], {}),
        )

    def list_analysis_candidates(
        self, *, status: str | None = None, lifecycle_status: str | None = None, limit: int = 1000,
        wallets: Iterable[str] | None = None,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        values: list[Any] = []
        if status:
            clauses.append("target.status=?")
            values.append(status)
        if lifecycle_status:
            clauses.append("analysis.lifecycle_status=?")
            values.append(lifecycle_status)
        normalized_wallets = [str(wallet).lower() for wallet in wallets] if wallets is not None else []
        if wallets is not None:
            if not normalized_wallets:
                return []
            clauses.append("candidate.wallet IN (" + ",".join("?" for _ in normalized_wallets) + ")")
            values.extend(normalized_wallets)
        query = """SELECT candidate.*, target.status AS current_status, analysis.lifecycle_status,
                   analysis.last_run_id AS analysis_run_id, analysis.completed_at AS analysis_completed_at,
                   analysis.prefilter_reasons_json, analysis.errors_json, analysis.summary_json,
                   phase_score.total_score, phase_score.eligible AS score_eligible, phase_score.reasons_json,
                   phase_score.provenance AS score_provenance, phase_score.analysis_run_id AS score_analysis_run_id,
                   phase_score.config_fingerprint AS candidate_config_fingerprint, phase_score.confidence_score,
                   phase_score.hard_gates_json, phase_score.score_version,
                   legacy_score.total_score AS legacy_total_score, legacy_score.eligible AS legacy_score_eligible,
                   legacy_score.reasons_json AS legacy_reasons_json, legacy_score.calculated_at AS legacy_calculated_at
                   FROM copy_discovery_candidates candidate
                   JOIN copy_targets target ON target.wallet=candidate.wallet
                   LEFT JOIN copy_candidate_analyses analysis ON analysis.wallet=candidate.wallet
                   LEFT JOIN copy_candidate_scores phase_score ON phase_score.target_wallet=candidate.wallet
                     AND phase_score.analysis_run_id=analysis.last_run_id AND phase_score.provenance='phase_b'
                     AND phase_score.rowid=(SELECT current_score.rowid FROM copy_candidate_scores current_score
                                            WHERE current_score.target_wallet=candidate.wallet
                                              AND current_score.analysis_run_id=analysis.last_run_id
                                              AND current_score.provenance='phase_b'
                                            ORDER BY current_score.calculated_at DESC, current_score.rowid DESC LIMIT 1)
                   LEFT JOIN copy_candidate_scores legacy_score ON legacy_score.target_wallet=candidate.wallet
                     AND legacy_score.provenance<>'phase_b'
                     AND legacy_score.rowid=(SELECT current_legacy.rowid FROM copy_candidate_scores current_legacy
                                              WHERE current_legacy.target_wallet=candidate.wallet
                                                AND current_legacy.provenance<>'phase_b'
                                              ORDER BY current_legacy.calculated_at DESC, current_legacy.rowid DESC LIMIT 1)"""
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY phase_score.total_score DESC NULLS LAST, candidate.recent_activity_at DESC, candidate.wallet LIMIT ?"
        values.append(limit)
        with self._connect() as connection:
            rows = connection.execute(query, values).fetchall()
        result: list[dict[str, Any]] = []
        for row in rows:
            value = dict(row)
            value["prefilter_reasons"] = _load(value.pop("prefilter_reasons_json", None), [])
            value["analysis_errors"] = _load(value.pop("errors_json", None), [])
            value["analysis_summary"] = _load(value.pop("summary_json", None), {})
            value["score_reasons"] = _load(value.pop("reasons_json", None), [])
            value["score_hard_gates"] = _load(value.pop("hard_gates_json", None), [])
            value["legacy_score_reasons"] = _load(value.pop("legacy_reasons_json", None), [])
            result.append(value)
        return result

    def count_analysis_candidates_by_state(self) -> dict[str, int]:
        with self._connect() as connection:
            rows = connection.execute(
                """SELECT COALESCE(analysis.lifecycle_status, 'new') AS lifecycle_status, COUNT(*) AS count
                   FROM copy_discovery_candidates candidate
                   LEFT JOIN copy_candidate_analyses analysis ON analysis.wallet=candidate.wallet
                   GROUP BY COALESCE(analysis.lifecycle_status, 'new')"""
            ).fetchall()
        return {str(row["lifecycle_status"]): int(row["count"]) for row in rows}

    def insert_raw_fill(self, fill: RawFill) -> bool:
        return self.insert_raw_fills((fill,)) == 1

    @staticmethod
    def _raw_fill_values(fill: RawFill) -> tuple[Any, ...]:
        return (
            fill.event_id, fill.source, fill.venue, fill.chain_network, fill.target_wallet.lower(), fill.target_order_id,
            fill.target_trade_id, fill.transaction_hash, fill.symbol, fill.side, fill.direction, fill.price,
            fill.base_quantity, fill.notional, fill.fee, fill.fee_token, fill.target_account_equity,
            fill.target_position_before, iso(fill.event_timestamp), iso(fill.ingestion_timestamp), fill.confirmation,
            _dump(fill.raw_payload), fill.source_closed_pnl, int(fill.is_liquidation),
        )

    def insert_raw_fills_returning_new(self, fills: Iterable[RawFill], *, batch_size: int = 1_000) -> tuple[RawFill, ...]:
        """Insert de-duplicated source fills in one bounded SQLite transaction.

        Network acquisition can stay parallel while persistence is a single
        WAL-friendly write transaction.  Returning the exact newly inserted
        evidence lets the watcher distinguish an old replay from a genuinely
        late arrival without relying on timestamps alone.
        """
        if batch_size < 1:
            raise ValueError("Raw-fill batch size must be positive.")
        inserted: list[RawFill] = []
        iterator = iter(fills)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            while batch := list(islice(iterator, batch_size)):
                # De-duplicate within a frame first.  The IMMEDIATE writer
                # transaction then makes the read/insert decision atomic with
                # respect to other local SQLite writers.
                unique = list({fill.event_id: fill for fill in batch}.values())
                if not unique:
                    continue
                known: set[str] = set()
                for offset in range(0, len(unique), 900):
                    identifiers = [fill.event_id for fill in unique[offset:offset + 900]]
                    placeholders = ",".join("?" for _ in identifiers)
                    rows = connection.execute(
                        f"SELECT event_id FROM copy_raw_fills WHERE event_id IN ({placeholders})", identifiers,
                    ).fetchall()
                    known.update(str(row["event_id"]) for row in rows)
                fresh = [fill for fill in unique if fill.event_id not in known]
                if not fresh:
                    continue
                connection.executemany(
                    """INSERT OR IGNORE INTO copy_raw_fills(
                        event_id, source, venue, chain_network, target_wallet, target_order_id, target_trade_id,
                        transaction_hash, symbol, side, direction, price, base_quantity, notional, fee, fee_token,
                        target_account_equity, target_position_before, event_timestamp, ingestion_timestamp,
                        confirmation, raw_payload_json, source_closed_pnl, is_liquidation)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    [self._raw_fill_values(fill) for fill in fresh],
                )
                inserted.extend(fresh)
        return tuple(inserted)

    def insert_raw_fills(self, fills: Iterable[RawFill], *, batch_size: int = 1_000) -> int:
        return len(self.insert_raw_fills_returning_new(fills, batch_size=batch_size))

    def list_raw_fills(
        self, wallet: str | None = None, *, start: object | None = None, end: object | None = None
    ) -> list[RawFill]:
        clauses: list[str] = []
        values: list[Any] = []
        if wallet:
            clauses.append("target_wallet = ?")
            values.append(wallet.lower())
        if start is not None:
            clauses.append("event_timestamp >= ?")
            values.append(iso(start))
        if end is not None:
            clauses.append("event_timestamp <= ?")
            values.append(iso(end))
        query = "SELECT * FROM copy_raw_fills"
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY event_timestamp, event_id"
        with self._connect() as connection:
            rows = connection.execute(query, values).fetchall()
        return [self._raw_fill_from_row(row) for row in rows]

    def list_raw_fills_after(self, wallet: str, timestamp: object | None, event_id: str | None) -> list[RawFill]:
        """Return only source evidence strictly beyond a durable composite cursor."""
        if timestamp is None or event_id is None:
            return self.list_raw_fills(wallet)
        with self._connect() as connection:
            rows = connection.execute(
                """SELECT * FROM copy_raw_fills WHERE target_wallet=?
                   AND (event_timestamp>? OR (event_timestamp=? AND event_id>?))
                   ORDER BY event_timestamp, event_id""",
                (wallet.lower(), iso(timestamp), iso(timestamp), event_id),
            ).fetchall()
        return [self._raw_fill_from_row(row) for row in rows]

    def list_raw_fills_by_ids(self, wallet: str, event_ids: Iterable[str]) -> list[RawFill]:
        identifiers = list(dict.fromkeys(event_ids))
        if not identifiers:
            return []
        rows: list[sqlite3.Row] = []
        with self._connect() as connection:
            for offset in range(0, len(identifiers), 900):
                batch = identifiers[offset:offset + 900]
                placeholders = ",".join("?" for _ in batch)
                rows.extend(connection.execute(
                    f"SELECT * FROM copy_raw_fills WHERE target_wallet=? AND event_id IN ({placeholders})",
                    [wallet.lower(), *batch],
                ).fetchall())
        return [self._raw_fill_from_row(row) for row in sorted(rows, key=lambda row: (row["event_timestamp"], row["event_id"]))]

    def latest_raw_fill(self, wallet: str) -> RawFill | None:
        with self._connect() as connection:
            row = connection.execute(
                """SELECT * FROM copy_raw_fills WHERE target_wallet=?
                   ORDER BY event_timestamp DESC, event_id DESC LIMIT 1""", (wallet.lower(),),
            ).fetchone()
        return self._raw_fill_from_row(row) if row else None

    def latest_fill_time(self, wallet: str) -> object | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT MAX(event_timestamp) AS event_timestamp FROM copy_raw_fills WHERE target_wallet=?", (wallet.lower(),)
            ).fetchone()
        return as_utc(row["event_timestamp"]) if row and row["event_timestamp"] else None

    def earliest_fill_time(self, wallet: str) -> object | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT MIN(event_timestamp) AS event_timestamp FROM copy_raw_fills WHERE target_wallet=?", (wallet.lower(),)
            ).fetchone()
        return as_utc(row["event_timestamp"]) if row and row["event_timestamp"] else None

    @staticmethod
    def _cursor_from_row(row: sqlite3.Row | None, wallet: str) -> ReconstructionCursor:
        if row is None:
            return ReconstructionCursor(target_wallet=wallet.lower(), schema_version=RECONSTRUCTION_SCHEMA_VERSION, revision=0)
        return ReconstructionCursor(
            target_wallet=row["target_wallet"], schema_version=int(row["schema_version"]), revision=int(row["revision"]),
            last_seen_timestamp=as_utc(row["last_seen_timestamp"]) if row["last_seen_timestamp"] else None,
            last_seen_event_id=row["last_seen_event_id"],
            last_processed_timestamp=as_utc(row["last_processed_timestamp"]) if row["last_processed_timestamp"] else None,
            last_processed_event_id=row["last_processed_event_id"],
            pending_fill_ids=tuple(_load(row["pending_fill_ids_json"], [])),
            pending_event_ids=tuple(_load(row["pending_event_ids_json"], [])),
            recovery_state=row["recovery_state"], recovery_anchor_event_id=row["recovery_anchor_event_id"],
            recovery_anchor_timestamp=as_utc(row["recovery_anchor_timestamp"]) if row["recovery_anchor_timestamp"] else None,
            recovery_detail=_load(row["recovery_detail_json"], {}),
            updated_at=as_utc(row["updated_at"]) if row["updated_at"] else None,
        )

    def reconstruction_cursor(self, wallet: str) -> ReconstructionCursor:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM copy_reconstruction_cursors WHERE target_wallet=?", (wallet.lower(),),
            ).fetchone()
        return self._cursor_from_row(row, wallet)

    def has_reconstruction_cursor(self, wallet: str) -> bool:
        with self._connect() as connection:
            return connection.execute(
                "SELECT 1 FROM copy_reconstruction_cursors WHERE target_wallet=?", (wallet.lower(),),
            ).fetchone() is not None

    def reconstruction_cursors(self, wallets: Iterable[str] | None = None) -> list[ReconstructionCursor]:
        requested = [wallet.lower() for wallet in wallets] if wallets is not None else None
        with self._connect() as connection:
            if requested:
                placeholders = ",".join("?" for _ in requested)
                rows = connection.execute(
                    f"SELECT * FROM copy_reconstruction_cursors WHERE target_wallet IN ({placeholders}) ORDER BY target_wallet",
                    requested,
                ).fetchall()
            else:
                rows = connection.execute("SELECT * FROM copy_reconstruction_cursors ORDER BY target_wallet").fetchall()
        return [self._cursor_from_row(row, row["target_wallet"]) for row in rows]

    @staticmethod
    def _cursor_values(cursor: ReconstructionCursor, *, revision: int) -> tuple[Any, ...]:
        return (
            cursor.target_wallet.lower(), cursor.schema_version, revision,
            iso(cursor.last_seen_timestamp) if cursor.last_seen_timestamp else None, cursor.last_seen_event_id,
            iso(cursor.last_processed_timestamp) if cursor.last_processed_timestamp else None, cursor.last_processed_event_id,
            _dump(cursor.pending_fill_ids), _dump(cursor.pending_event_ids), cursor.recovery_state,
            cursor.recovery_anchor_event_id,
            iso(cursor.recovery_anchor_timestamp) if cursor.recovery_anchor_timestamp else None,
            _dump(cursor.recovery_detail), iso(cursor.updated_at),
        )

    def set_recovery_state(
        self, wallet: str, state: str, *, anchor: RawFill | None = None, detail: Mapping[str, Any] | None = None,
    ) -> ReconstructionCursor:
        """Persist continuity state without manufacturing source history."""
        if state not in {"CONTINUOUS", "RECOVERING", "RECOVERY_INCOMPLETE"}:
            raise ValueError(f"Unsupported recovery state: {state}")
        normalized = wallet.lower()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = connection.execute(
                    "SELECT * FROM copy_reconstruction_cursors WHERE target_wallet=?", (normalized,),
                ).fetchone()
                cursor = self._cursor_from_row(row, normalized)
                replacement = ReconstructionCursor(
                    target_wallet=normalized, schema_version=cursor.schema_version, revision=cursor.revision,
                    last_seen_timestamp=cursor.last_seen_timestamp, last_seen_event_id=cursor.last_seen_event_id,
                    last_processed_timestamp=cursor.last_processed_timestamp, last_processed_event_id=cursor.last_processed_event_id,
                    pending_fill_ids=cursor.pending_fill_ids, pending_event_ids=cursor.pending_event_ids,
                    recovery_state=state,
                    recovery_anchor_event_id=anchor.event_id if anchor else cursor.recovery_anchor_event_id,
                    recovery_anchor_timestamp=anchor.event_timestamp if anchor else cursor.recovery_anchor_timestamp,
                    recovery_detail=dict(detail or {}), updated_at=iso(None),
                )
                if row is None:
                    connection.execute(
                        """INSERT INTO copy_reconstruction_cursors(target_wallet, schema_version, revision,
                        last_seen_timestamp, last_seen_event_id, last_processed_timestamp, last_processed_event_id,
                        pending_fill_ids_json, pending_event_ids_json, recovery_state, recovery_anchor_event_id,
                        recovery_anchor_timestamp, recovery_detail_json, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        self._cursor_values(replacement, revision=1),
                    )
                    return ReconstructionCursor(**{**replacement.__dict__, "revision": 1})
                changed = connection.execute(
                    """UPDATE copy_reconstruction_cursors SET schema_version=?, revision=?, last_seen_timestamp=?,
                       last_seen_event_id=?, last_processed_timestamp=?, last_processed_event_id=?, pending_fill_ids_json=?,
                       pending_event_ids_json=?, recovery_state=?, recovery_anchor_event_id=?, recovery_anchor_timestamp=?,
                       recovery_detail_json=?, updated_at=? WHERE target_wallet=? AND revision=?""",
                    (*self._cursor_values(replacement, revision=cursor.revision + 1)[1:], normalized, cursor.revision),
                ).rowcount
                if changed != 1:
                    raise RuntimeError("Concurrent reconstruction cursor update detected.")
                return ReconstructionCursor(**{**replacement.__dict__, "revision": cursor.revision + 1})
            except Exception:
                connection.rollback()
                raise

    def clear_pending_reconstruction_events(self, wallet: str, event_ids: Iterable[str]) -> ReconstructionCursor:
        consumed = set(event_ids)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = connection.execute(
                    "SELECT * FROM copy_reconstruction_cursors WHERE target_wallet=?", (wallet.lower(),),
                ).fetchone()
                cursor = self._cursor_from_row(row, wallet)
                if row is None:
                    return cursor
                replacement = ReconstructionCursor(
                    target_wallet=cursor.target_wallet, schema_version=cursor.schema_version, revision=cursor.revision,
                    last_seen_timestamp=cursor.last_seen_timestamp, last_seen_event_id=cursor.last_seen_event_id,
                    last_processed_timestamp=cursor.last_processed_timestamp, last_processed_event_id=cursor.last_processed_event_id,
                    pending_fill_ids=cursor.pending_fill_ids,
                    pending_event_ids=tuple(item for item in cursor.pending_event_ids if item not in consumed),
                    recovery_state=cursor.recovery_state, recovery_anchor_event_id=cursor.recovery_anchor_event_id,
                    recovery_anchor_timestamp=cursor.recovery_anchor_timestamp, recovery_detail=cursor.recovery_detail,
                    updated_at=iso(None),
                )
                changed = connection.execute(
                    """UPDATE copy_reconstruction_cursors SET schema_version=?, revision=?, last_seen_timestamp=?,
                       last_seen_event_id=?, last_processed_timestamp=?, last_processed_event_id=?, pending_fill_ids_json=?,
                       pending_event_ids_json=?, recovery_state=?, recovery_anchor_event_id=?, recovery_anchor_timestamp=?,
                       recovery_detail_json=?, updated_at=? WHERE target_wallet=? AND revision=?""",
                    (*self._cursor_values(replacement, revision=cursor.revision + 1)[1:], wallet.lower(), cursor.revision),
                ).rowcount
                if changed != 1:
                    raise RuntimeError("Concurrent reconstruction cursor update detected.")
                return ReconstructionCursor(**{**replacement.__dict__, "revision": cursor.revision + 1})
            except Exception:
                connection.rollback()
                raise

    def latest_prior_equity_observation(self, wallet: str, timestamp: object) -> dict[str, Any] | None:
        """Return the latest non-future account-value sample for enrichment."""
        with self._connect() as connection:
            row = connection.execute(
                """SELECT * FROM copy_trader_snapshots WHERE target_wallet=? AND account_value IS NOT NULL
                   AND snapshot_timestamp <= ? ORDER BY snapshot_timestamp DESC LIMIT 1""",
                (wallet.lower(), iso(timestamp)),
            ).fetchone()
        if not row:
            return None
        return {
            "account_value": float(row["account_value"]), "timestamp": as_utc(row["snapshot_timestamp"]),
            "source": row["source"], "positions": _load(row["positions_json"], {}),
        }

    @staticmethod
    def _raw_fill_from_row(row: sqlite3.Row) -> RawFill:
        return RawFill(
            event_id=row["event_id"], source=row["source"], venue=row["venue"], chain_network=row["chain_network"],
            target_wallet=row["target_wallet"], target_order_id=row["target_order_id"], target_trade_id=row["target_trade_id"],
            transaction_hash=row["transaction_hash"], symbol=row["symbol"], side=row["side"], direction=row["direction"],
            price=float(row["price"]), base_quantity=float(row["base_quantity"]), notional=float(row["notional"]),
            fee=float(row["fee"]), fee_token=row["fee_token"], target_account_equity=row["target_account_equity"],
            target_position_before=row["target_position_before"], event_timestamp=as_utc(row["event_timestamp"]),
            ingestion_timestamp=as_utc(row["ingestion_timestamp"]), confirmation=row["confirmation"],
            raw_payload=_load(row["raw_payload_json"], {}), source_closed_pnl=row["source_closed_pnl"],
            is_liquidation=bool(row["is_liquidation"]),
        )

    def upsert_position_event(self, event: PositionEvent) -> None:
        with self._connect() as connection:
            connection.execute(
                """INSERT INTO copy_position_events(event_id, target_wallet, symbol, event_type, direction,
                   delta_quantity, before_quantity, after_quantity, price, notional, event_timestamp,
                   campaign_id, raw_fill_ids_json, target_equity, initial_delta_notional, equity_source,
                   equity_age_seconds, source_event_type, split_role, split_quantity, split_notional, split_fee, source_closed_pnl)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(event_id) DO UPDATE SET campaign_id=excluded.campaign_id, target_equity=excluded.target_equity,
                   equity_source=excluded.equity_source, equity_age_seconds=excluded.equity_age_seconds""",
                (event.event_id, event.target_wallet, event.symbol, event.event_type.value, event.direction,
                 event.delta_quantity, event.before_quantity, event.after_quantity, event.price, event.notional,
                 iso(event.event_timestamp), event.campaign_id, _dump(event.raw_fill_ids), event.target_equity,
                 event.initial_delta_notional, event.equity_source, event.equity_age_seconds, event.source_event_type,
                 event.split_role, event.split_quantity, event.split_notional, event.split_fee, event.source_closed_pnl),
            )

    def list_position_events(self, wallet: str | None = None) -> list[PositionEvent]:
        query = "SELECT * FROM copy_position_events"
        values: tuple[Any, ...] = ()
        if wallet:
            query += " WHERE target_wallet=?"
            values = (wallet.lower(),)
        query += " ORDER BY event_timestamp, event_id"
        with self._connect() as connection:
            rows = connection.execute(query, values).fetchall()
        return [
            PositionEvent(event_id=row["event_id"], target_wallet=row["target_wallet"], symbol=row["symbol"],
                          event_type=PositionEventType(row["event_type"]), direction=row["direction"],
                          delta_quantity=float(row["delta_quantity"]), before_quantity=float(row["before_quantity"]),
                          after_quantity=float(row["after_quantity"]), price=float(row["price"]),
                          notional=float(row["notional"]), event_timestamp=as_utc(row["event_timestamp"]),
                          campaign_id=row["campaign_id"], raw_fill_ids=tuple(_load(row["raw_fill_ids_json"], [])),
                          target_equity=row["target_equity"], initial_delta_notional=float(row["initial_delta_notional"]),
                          equity_source=row["equity_source"], equity_age_seconds=row["equity_age_seconds"],
                          source_event_type=row["source_event_type"], split_role=row["split_role"],
                          split_quantity=row["split_quantity"], split_notional=row["split_notional"], split_fee=row["split_fee"],
                          source_closed_pnl=row["source_closed_pnl"])
            for row in rows
        ]

    def upsert_campaign(self, campaign: PositionCampaign) -> None:
        with self._connect() as connection:
            connection.execute(
                """INSERT INTO copy_campaigns(campaign_id, target_wallet, symbol, direction, opened_at, closed_at,
                   entry_quantity, open_quantity, entry_notional, remaining_entry_notional, exit_notional, realized_pnl, target_fees,
                   event_count, raw_fill_ids_json, max_open_quantity, adverse_add_count, history_complete,
                   entry_basis_quality, source_closed_pnl, source_closed_pnl_observed, reconciliation_gross_difference, liquidation_count)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(campaign_id) DO UPDATE SET closed_at=excluded.closed_at, entry_quantity=excluded.entry_quantity,
                   open_quantity=excluded.open_quantity, entry_notional=excluded.entry_notional, remaining_entry_notional=excluded.remaining_entry_notional, exit_notional=excluded.exit_notional,
                   realized_pnl=excluded.realized_pnl, target_fees=excluded.target_fees, event_count=excluded.event_count,
                   raw_fill_ids_json=excluded.raw_fill_ids_json, max_open_quantity=excluded.max_open_quantity,
                   adverse_add_count=excluded.adverse_add_count, history_complete=excluded.history_complete,
                   entry_basis_quality=excluded.entry_basis_quality, source_closed_pnl=excluded.source_closed_pnl,
                   source_closed_pnl_observed=excluded.source_closed_pnl_observed,
                   reconciliation_gross_difference=excluded.reconciliation_gross_difference, liquidation_count=excluded.liquidation_count""",
                (campaign.campaign_id, campaign.target_wallet, campaign.symbol, campaign.direction, iso(campaign.opened_at),
                 iso(campaign.closed_at) if campaign.closed_at else None, campaign.entry_quantity, campaign.open_quantity,
                 campaign.entry_notional, campaign.remaining_entry_notional, campaign.exit_notional, campaign.realized_pnl, campaign.target_fees,
                 campaign.event_count, _dump(campaign.raw_fill_ids), campaign.max_open_quantity, campaign.adverse_add_count,
                 int(campaign.history_complete), campaign.entry_basis_quality, campaign.source_closed_pnl,
                 int(campaign.source_closed_pnl_observed), campaign.reconciliation_gross_difference, campaign.liquidation_count),
            )

    @staticmethod
    def _position_event_values(event: PositionEvent) -> tuple[Any, ...]:
        return (
            event.event_id, event.target_wallet, event.symbol, event.event_type.value, event.direction,
            event.delta_quantity, event.before_quantity, event.after_quantity, event.price, event.notional,
            iso(event.event_timestamp), event.campaign_id, _dump(event.raw_fill_ids), event.target_equity,
            event.initial_delta_notional, event.equity_source, event.equity_age_seconds, event.source_event_type,
            event.split_role, event.split_quantity, event.split_notional, event.split_fee, event.source_closed_pnl,
        )

    @staticmethod
    def _campaign_values(campaign: PositionCampaign) -> tuple[Any, ...]:
        return (
            campaign.campaign_id, campaign.target_wallet, campaign.symbol, campaign.direction, iso(campaign.opened_at),
            iso(campaign.closed_at) if campaign.closed_at else None, campaign.entry_quantity, campaign.open_quantity,
            campaign.entry_notional, campaign.remaining_entry_notional, campaign.exit_notional, campaign.realized_pnl,
            campaign.target_fees, campaign.event_count, _dump(campaign.raw_fill_ids), campaign.max_open_quantity,
            campaign.adverse_add_count, int(campaign.history_complete), campaign.entry_basis_quality,
            campaign.source_closed_pnl, int(campaign.source_closed_pnl_observed), campaign.reconciliation_gross_difference,
            campaign.liquidation_count,
        )

    def persist_reconstruction_batch(
        self, wallet: str, events: Iterable[PositionEvent], campaigns: Iterable[PositionCampaign], cursor: ReconstructionCursor,
        *, replace_wallet_history: bool = False,
    ) -> ReconstructionCursor:
        """Commit reconstructed rows and the cursor as one recovery boundary.

        Raw evidence is committed first.  This second transaction is all or
        nothing: the cursor can never point beyond events/campaign accounting
        that reached SQLite.  A crash before it commits leaves the cursor behind
        durable evidence, which is intentionally safe to replay.
        """
        event_rows = [self._position_event_values(item) for item in events]
        campaign_rows = [self._campaign_values(item) for item in campaigns]
        normalized = wallet.lower()
        if cursor.target_wallet.lower() != normalized:
            raise ValueError("Reconstruction cursor wallet does not match persisted rows.")
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                current = connection.execute(
                    "SELECT revision FROM copy_reconstruction_cursors WHERE target_wallet=?", (normalized,),
                ).fetchone()
                current_revision = int(current["revision"]) if current else 0
                if current_revision != cursor.revision:
                    raise RuntimeError("Concurrent reconstruction cursor update detected.")
                if replace_wallet_history:
                    connection.execute("DELETE FROM copy_position_events WHERE target_wallet=?", (normalized,))
                    connection.execute("DELETE FROM copy_campaigns WHERE target_wallet=?", (normalized,))
                if event_rows:
                    connection.executemany(
                        """INSERT INTO copy_position_events(event_id, target_wallet, symbol, event_type, direction,
                           delta_quantity, before_quantity, after_quantity, price, notional, event_timestamp,
                           campaign_id, raw_fill_ids_json, target_equity, initial_delta_notional, equity_source,
                           equity_age_seconds, source_event_type, split_role, split_quantity, split_notional, split_fee, source_closed_pnl)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                           ON CONFLICT(event_id) DO UPDATE SET campaign_id=excluded.campaign_id,
                           target_equity=excluded.target_equity, equity_source=excluded.equity_source,
                           equity_age_seconds=excluded.equity_age_seconds""", event_rows,
                    )
                if campaign_rows:
                    connection.executemany(
                        """INSERT INTO copy_campaigns(campaign_id, target_wallet, symbol, direction, opened_at, closed_at,
                           entry_quantity, open_quantity, entry_notional, remaining_entry_notional, exit_notional, realized_pnl, target_fees,
                           event_count, raw_fill_ids_json, max_open_quantity, adverse_add_count, history_complete,
                           entry_basis_quality, source_closed_pnl, source_closed_pnl_observed, reconciliation_gross_difference, liquidation_count)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                           ON CONFLICT(campaign_id) DO UPDATE SET closed_at=excluded.closed_at,
                           entry_quantity=excluded.entry_quantity, open_quantity=excluded.open_quantity,
                           entry_notional=excluded.entry_notional, remaining_entry_notional=excluded.remaining_entry_notional,
                           exit_notional=excluded.exit_notional, realized_pnl=excluded.realized_pnl,
                           target_fees=excluded.target_fees, event_count=excluded.event_count,
                           raw_fill_ids_json=excluded.raw_fill_ids_json, max_open_quantity=excluded.max_open_quantity,
                           adverse_add_count=excluded.adverse_add_count, history_complete=excluded.history_complete,
                           entry_basis_quality=excluded.entry_basis_quality, source_closed_pnl=excluded.source_closed_pnl,
                           source_closed_pnl_observed=excluded.source_closed_pnl_observed,
                           reconciliation_gross_difference=excluded.reconciliation_gross_difference,
                           liquidation_count=excluded.liquidation_count""", campaign_rows,
                    )
                replacement = ReconstructionCursor(
                    target_wallet=normalized, schema_version=cursor.schema_version, revision=current_revision + 1,
                    last_seen_timestamp=cursor.last_seen_timestamp, last_seen_event_id=cursor.last_seen_event_id,
                    last_processed_timestamp=cursor.last_processed_timestamp,
                    last_processed_event_id=cursor.last_processed_event_id,
                    pending_fill_ids=cursor.pending_fill_ids, pending_event_ids=cursor.pending_event_ids,
                    recovery_state=cursor.recovery_state, recovery_anchor_event_id=cursor.recovery_anchor_event_id,
                    recovery_anchor_timestamp=cursor.recovery_anchor_timestamp, recovery_detail=cursor.recovery_detail,
                    updated_at=iso(None),
                )
                if current is None:
                    connection.execute(
                        """INSERT INTO copy_reconstruction_cursors(target_wallet, schema_version, revision,
                        last_seen_timestamp, last_seen_event_id, last_processed_timestamp, last_processed_event_id,
                        pending_fill_ids_json, pending_event_ids_json, recovery_state, recovery_anchor_event_id,
                        recovery_anchor_timestamp, recovery_detail_json, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        self._cursor_values(replacement, revision=replacement.revision),
                    )
                else:
                    connection.execute(
                        """UPDATE copy_reconstruction_cursors SET schema_version=?, revision=?, last_seen_timestamp=?,
                           last_seen_event_id=?, last_processed_timestamp=?, last_processed_event_id=?, pending_fill_ids_json=?,
                           pending_event_ids_json=?, recovery_state=?, recovery_anchor_event_id=?, recovery_anchor_timestamp=?,
                           recovery_detail_json=?, updated_at=? WHERE target_wallet=? AND revision=?""",
                        (*self._cursor_values(replacement, revision=replacement.revision)[1:], normalized, current_revision),
                    )
                connection.commit()
                return replacement
            except Exception:
                connection.rollback()
                raise

    def list_open_campaigns(self, wallet: str) -> list[PositionCampaign]:
        with self._connect() as connection:
            rows = connection.execute(
                """SELECT * FROM copy_campaigns WHERE target_wallet=? AND closed_at IS NULL
                   ORDER BY opened_at, campaign_id""", (wallet.lower(),),
            ).fetchall()
        return [
            PositionCampaign(campaign_id=row["campaign_id"], target_wallet=row["target_wallet"], symbol=row["symbol"],
                             direction=row["direction"], opened_at=as_utc(row["opened_at"]),
                             closed_at=None, entry_quantity=float(row["entry_quantity"]), open_quantity=float(row["open_quantity"]),
                             entry_notional=float(row["entry_notional"]), remaining_entry_notional=float(row["remaining_entry_notional"]),
                             exit_notional=float(row["exit_notional"]), realized_pnl=float(row["realized_pnl"]),
                             target_fees=float(row["target_fees"]), event_count=int(row["event_count"]),
                             raw_fill_ids=list(_load(row["raw_fill_ids_json"], [])), max_open_quantity=float(row["max_open_quantity"]),
                             adverse_add_count=int(row["adverse_add_count"]), history_complete=bool(row["history_complete"]),
                             entry_basis_quality=row["entry_basis_quality"], source_closed_pnl=float(row["source_closed_pnl"]),
                             source_closed_pnl_observed=bool(row["source_closed_pnl_observed"]),
                             reconciliation_gross_difference=row["reconciliation_gross_difference"], liquidation_count=int(row["liquidation_count"]))
            for row in rows
        ]

    def list_position_events_by_ids(self, wallet: str, event_ids: Iterable[str]) -> list[PositionEvent]:
        identifiers = list(dict.fromkeys(event_ids))
        if not identifiers:
            return []
        rows: list[sqlite3.Row] = []
        with self._connect() as connection:
            for offset in range(0, len(identifiers), 900):
                batch = identifiers[offset:offset + 900]
                placeholders = ",".join("?" for _ in batch)
                rows.extend(connection.execute(
                    f"SELECT * FROM copy_position_events WHERE target_wallet=? AND event_id IN ({placeholders})",
                    [wallet.lower(), *batch],
                ).fetchall())
        return [
            PositionEvent(event_id=row["event_id"], target_wallet=row["target_wallet"], symbol=row["symbol"],
                          event_type=PositionEventType(row["event_type"]), direction=row["direction"],
                          delta_quantity=float(row["delta_quantity"]), before_quantity=float(row["before_quantity"]),
                          after_quantity=float(row["after_quantity"]), price=float(row["price"]),
                          notional=float(row["notional"]), event_timestamp=as_utc(row["event_timestamp"]),
                          campaign_id=row["campaign_id"], raw_fill_ids=tuple(_load(row["raw_fill_ids_json"], [])),
                          target_equity=row["target_equity"], initial_delta_notional=float(row["initial_delta_notional"]),
                          equity_source=row["equity_source"], equity_age_seconds=row["equity_age_seconds"],
                          source_event_type=row["source_event_type"], split_role=row["split_role"],
                          split_quantity=row["split_quantity"], split_notional=row["split_notional"], split_fee=row["split_fee"],
                          source_closed_pnl=row["source_closed_pnl"])
            for row in sorted(rows, key=lambda row: (row["event_timestamp"], row["event_id"]))
        ]

    def list_position_events_for_raw_fills(self, wallet: str, raw_fill_ids: Iterable[str]) -> list[PositionEvent]:
        """Find only events attributable to a delivered source-frame overlap.

        This supports a real-time callback that repeats evidence first seen in
        a startup snapshot without scanning a wallet's historical event table.
        SQLite's built-in JSON table function expands the immutable raw-fill
        attribution stored on each reconstructed event.
        """
        identifiers = list(dict.fromkeys(raw_fill_ids))
        if not identifiers:
            return []
        rows: list[sqlite3.Row] = []
        with self._connect() as connection:
            for offset in range(0, len(identifiers), 900):
                batch = identifiers[offset:offset + 900]
                placeholders = ",".join("?" for _ in batch)
                rows.extend(connection.execute(
                    f"""SELECT DISTINCT event.* FROM copy_position_events AS event
                        JOIN json_each(event.raw_fill_ids_json) AS raw
                        WHERE event.target_wallet=? AND raw.value IN ({placeholders})""",
                    [wallet.lower(), *batch],
                ).fetchall())
        return [
            PositionEvent(event_id=row["event_id"], target_wallet=row["target_wallet"], symbol=row["symbol"],
                          event_type=PositionEventType(row["event_type"]), direction=row["direction"],
                          delta_quantity=float(row["delta_quantity"]), before_quantity=float(row["before_quantity"]),
                          after_quantity=float(row["after_quantity"]), price=float(row["price"]),
                          notional=float(row["notional"]), event_timestamp=as_utc(row["event_timestamp"]),
                          campaign_id=row["campaign_id"], raw_fill_ids=tuple(_load(row["raw_fill_ids_json"], [])),
                          target_equity=row["target_equity"], initial_delta_notional=float(row["initial_delta_notional"]),
                          equity_source=row["equity_source"], equity_age_seconds=row["equity_age_seconds"],
                          source_event_type=row["source_event_type"], split_role=row["split_role"],
                          split_quantity=row["split_quantity"], split_notional=row["split_notional"], split_fee=row["split_fee"],
                          source_closed_pnl=row["source_closed_pnl"])
            for row in sorted(rows, key=lambda row: (row["event_timestamp"], row["event_id"]))
        ]

    def list_campaigns(self, wallet: str | None = None, *, closed_only: bool = False) -> list[PositionCampaign]:
        clauses: list[str] = []
        values: list[Any] = []
        if wallet:
            clauses.append("target_wallet=?")
            values.append(wallet.lower())
        if closed_only:
            clauses.append("closed_at IS NOT NULL")
        query = "SELECT * FROM copy_campaigns"
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY opened_at, campaign_id"
        with self._connect() as connection:
            rows = connection.execute(query, values).fetchall()
        return [
            PositionCampaign(campaign_id=row["campaign_id"], target_wallet=row["target_wallet"], symbol=row["symbol"],
                             direction=row["direction"], opened_at=as_utc(row["opened_at"]),
                             closed_at=as_utc(row["closed_at"]) if row["closed_at"] else None,
                             entry_quantity=float(row["entry_quantity"]), open_quantity=float(row["open_quantity"]),
                             entry_notional=float(row["entry_notional"]), remaining_entry_notional=float(row["remaining_entry_notional"]), exit_notional=float(row["exit_notional"]),
                             realized_pnl=float(row["realized_pnl"]), target_fees=float(row["target_fees"]),
                             event_count=int(row["event_count"]), raw_fill_ids=list(_load(row["raw_fill_ids_json"], [])),
                             max_open_quantity=float(row["max_open_quantity"]), adverse_add_count=int(row["adverse_add_count"]),
                             history_complete=bool(row["history_complete"]), entry_basis_quality=row["entry_basis_quality"],
                             source_closed_pnl=float(row["source_closed_pnl"]), source_closed_pnl_observed=bool(row["source_closed_pnl_observed"]),
                             reconciliation_gross_difference=row["reconciliation_gross_difference"], liquidation_count=int(row["liquidation_count"]))
            for row in rows
        ]

    def insert_snapshot(self, snapshot: TraderSnapshot) -> bool:
        with self._connect() as connection:
            cursor = connection.execute(
                """INSERT OR IGNORE INTO copy_trader_snapshots(snapshot_id, target_wallet, snapshot_timestamp,
                account_value, withdrawable, total_notional_position, positions_json, source, raw_payload_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (snapshot.snapshot_id, snapshot.target_wallet, iso(snapshot.snapshot_timestamp), snapshot.account_value,
                 snapshot.withdrawable, snapshot.total_notional_position, _dump(snapshot.positions), snapshot.source,
                 _dump(snapshot.raw_payload)),
            )
        return cursor.rowcount == 1

    def upsert_metrics(self, metrics: TraderMetrics) -> None:
        with self._connect() as connection:
            connection.execute(
                "INSERT OR REPLACE INTO copy_daily_metrics(target_wallet, calculated_at, payload_json) VALUES (?, ?, ?)",
                (metrics.target_wallet, iso(metrics.calculated_at), _dump(metrics)),
            )

    def latest_metrics(self, wallet: str) -> TraderMetrics | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT payload_json FROM copy_daily_metrics WHERE target_wallet=? ORDER BY calculated_at DESC LIMIT 1",
                (wallet.lower(),),
            ).fetchone()
        if not row:
            return None
        payload = _load(row["payload_json"], {})
        payload["calculated_at"] = as_utc(payload["calculated_at"])
        return TraderMetrics(**payload)

    def upsert_candidate_score(self, score: CandidateScore) -> None:
        with self._connect() as connection:
            values = (
                score.target_wallet, iso(score.calculated_at), score.total_score, _dump(score.component_scores),
                _dump(score.penalties), int(score.eligible), _dump(score.reasons), score.source_quality,
                score.provenance, score.analysis_run_id, score.config_fingerprint, score.confidence_score,
                _dump(score.hard_gates), score.score_version,
            )
            if score.provenance == "phase_b" and score.analysis_run_id:
                connection.execute(
                    """INSERT INTO copy_candidate_scores(target_wallet, calculated_at, total_score,
                    component_scores_json, penalties_json, eligible, reasons_json, source_quality,
                    provenance, analysis_run_id, config_fingerprint, confidence_score, hard_gates_json, score_version)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(target_wallet, analysis_run_id, provenance)
                    WHERE provenance='phase_b' AND analysis_run_id IS NOT NULL DO UPDATE SET
                      calculated_at=excluded.calculated_at, total_score=excluded.total_score,
                      component_scores_json=excluded.component_scores_json, penalties_json=excluded.penalties_json,
                      eligible=excluded.eligible, reasons_json=excluded.reasons_json,
                      source_quality=excluded.source_quality, config_fingerprint=excluded.config_fingerprint,
                      confidence_score=excluded.confidence_score, hard_gates_json=excluded.hard_gates_json,
                      score_version=excluded.score_version""",
                    values,
                )
            else:
                connection.execute(
                    """INSERT OR REPLACE INTO copy_candidate_scores(target_wallet, calculated_at, total_score,
                    component_scores_json, penalties_json, eligible, reasons_json, source_quality,
                    provenance, analysis_run_id, config_fingerprint, confidence_score, hard_gates_json, score_version)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    values,
                )

    def latest_scores(self) -> list[CandidateScore]:
        with self._connect() as connection:
            rows = connection.execute(
                """SELECT s.* FROM copy_candidate_scores s
                   WHERE s.rowid=(SELECT current_score.rowid FROM copy_candidate_scores current_score
                                  WHERE current_score.target_wallet=s.target_wallet
                                  ORDER BY current_score.calculated_at DESC, current_score.rowid DESC LIMIT 1)
                   ORDER BY s.total_score DESC, s.target_wallet"""
            ).fetchall()
        return [
            CandidateScore(target_wallet=row["target_wallet"], calculated_at=as_utc(row["calculated_at"]),
                           total_score=float(row["total_score"]), component_scores=_load(row["component_scores_json"], {}),
                           penalties=_load(row["penalties_json"], {}), eligible=bool(row["eligible"]),
                           reasons=tuple(_load(row["reasons_json"], [])), source_quality=float(row["source_quality"]),
                           provenance=str(row["provenance"] or "legacy"), analysis_run_id=row["analysis_run_id"],
                           config_fingerprint=row["config_fingerprint"], confidence_score=float(row["confidence_score"] or 0),
                           hard_gates=tuple(_load(row["hard_gates_json"], [])), score_version=str(row["score_version"] or "phase_b_suitability_v3"))
            for row in rows
        ]

    def latest_legacy_scores(self) -> list[CandidateScore]:
        """Latest research-only score per wallet, explicitly excluding Phase B evidence."""
        with self._connect() as connection:
            rows = connection.execute(
                """SELECT score.* FROM copy_candidate_scores score
                   WHERE score.provenance<>'phase_b'
                     AND score.rowid=(SELECT current_score.rowid FROM copy_candidate_scores current_score
                                      WHERE current_score.target_wallet=score.target_wallet
                                        AND current_score.provenance<>'phase_b'
                                      ORDER BY current_score.calculated_at DESC, current_score.rowid DESC LIMIT 1)
                   ORDER BY score.total_score DESC, score.target_wallet"""
            ).fetchall()
        return [
            CandidateScore(target_wallet=row["target_wallet"], calculated_at=as_utc(row["calculated_at"]),
                           total_score=float(row["total_score"]), component_scores=_load(row["component_scores_json"], {}),
                           penalties=_load(row["penalties_json"], {}), eligible=bool(row["eligible"]),
                           reasons=tuple(_load(row["reasons_json"], [])), source_quality=float(row["source_quality"]),
                           provenance=str(row["provenance"] or "legacy"), analysis_run_id=row["analysis_run_id"],
                           config_fingerprint=row["config_fingerprint"], confidence_score=float(row["confidence_score"] or 0),
                           hard_gates=tuple(_load(row["hard_gates_json"], [])), score_version=str(row["score_version"] or "phase_b_suitability_v3"))
            for row in rows
        ]

    def phase_b_qualified_scores(self, *, config_fingerprint: str | None = None) -> list[CandidateScore]:
        """Current qualified Phase B scores, one authoritative row per wallet."""
        fingerprint_clause = " AND score.config_fingerprint=?" if config_fingerprint is not None else ""
        values: tuple[Any, ...] = (config_fingerprint,) if config_fingerprint is not None else ()
        with self._connect() as connection:
            rows = connection.execute(
                """SELECT score.* FROM copy_candidate_analyses analysis
                   JOIN copy_targets target ON target.wallet=analysis.wallet
                   JOIN copy_analysis_runs run ON run.run_id=analysis.last_run_id
                   JOIN copy_candidate_scores score ON score.target_wallet=analysis.wallet
                     AND score.analysis_run_id=analysis.last_run_id AND score.provenance='phase_b'
                   WHERE analysis.lifecycle_status='qualified' AND analysis.completed_at IS NOT NULL
                     AND run.status IN ('completed', 'completed_with_errors')
                     AND target.status NOT IN ('muted', 'rejected', 'active') AND score.eligible=1
                     AND score.rowid=(SELECT current_score.rowid FROM copy_candidate_scores current_score
                                      WHERE current_score.target_wallet=score.target_wallet
                                        AND current_score.analysis_run_id=score.analysis_run_id
                                        AND current_score.provenance='phase_b'
                                      ORDER BY current_score.calculated_at DESC, current_score.rowid DESC LIMIT 1)""" + fingerprint_clause +
                " ORDER BY score.total_score DESC, score.target_wallet",
                values,
            ).fetchall()
        return [
            CandidateScore(
                target_wallet=row["target_wallet"], calculated_at=as_utc(row["calculated_at"]), total_score=float(row["total_score"]),
                component_scores=_load(row["component_scores_json"], {}), penalties=_load(row["penalties_json"], {}),
                eligible=bool(row["eligible"]), reasons=tuple(_load(row["reasons_json"], [])),
                source_quality=float(row["source_quality"]), provenance=str(row["provenance"] or "legacy"),
                analysis_run_id=row["analysis_run_id"], config_fingerprint=row["config_fingerprint"],
                confidence_score=float(row["confidence_score"] or 0), hard_gates=tuple(_load(row["hard_gates_json"], [])),
                score_version=str(row["score_version"] or "phase_b_suitability_v3"),
            )
            for row in rows
        ]

    def count_stale_qualified_candidates(self, current_config_fingerprint: str) -> int:
        with self._connect() as connection:
            row = connection.execute(
                """SELECT COUNT(DISTINCT analysis.wallet) AS count FROM copy_candidate_analyses analysis
                   JOIN copy_candidate_scores score ON score.target_wallet=analysis.wallet
                     AND score.analysis_run_id=analysis.last_run_id AND score.provenance='phase_b'
                   WHERE analysis.lifecycle_status='qualified' AND analysis.completed_at IS NOT NULL
                     AND score.config_fingerprint<>?""",
                (current_config_fingerprint,),
            ).fetchone()
        return int(row["count"] if row else 0)

    def insert_signal(self, signal: CopySignal) -> bool:
        with self._connect() as connection:
            cursor = connection.execute(
                """INSERT OR IGNORE INTO copy_signals(signal_id, target_wallet, campaign_id, source_event_id,
                symbol, action, direction, target_price, target_quantity, target_notional, allocation_fraction,
                requested_capital, created_at, source_event_timestamp, size_ratio, reason, target_position_before, target_leverage,
                target_equity, equity_source, equity_age_seconds)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (signal.signal_id, signal.target_wallet, signal.campaign_id, signal.source_event_id, signal.symbol,
                 signal.action, signal.direction, signal.target_price, signal.target_quantity, signal.target_notional,
                 signal.allocation_fraction, signal.requested_capital, iso(signal.created_at),
                 iso(signal.source_event_timestamp), signal.size_ratio, signal.reason, signal.target_position_before, signal.target_leverage,
                 signal.target_equity, signal.equity_source, signal.equity_age_seconds),
            )
        return cursor.rowcount == 1

    def has_signal(self, signal_id: str) -> bool:
        with self._connect() as connection:
            row = connection.execute("SELECT 1 FROM copy_signals WHERE signal_id=?", (signal_id,)).fetchone()
        return row is not None

    @staticmethod
    def _signal_from_row(row: sqlite3.Row) -> CopySignal:
        return CopySignal(
            signal_id=row["signal_id"], target_wallet=row["target_wallet"], campaign_id=row["campaign_id"],
            source_event_id=row["source_event_id"], symbol=row["symbol"], action=row["action"], direction=row["direction"],
            target_price=float(row["target_price"]), target_quantity=float(row["target_quantity"]),
            target_notional=float(row["target_notional"]), allocation_fraction=float(row["allocation_fraction"]),
            requested_capital=float(row["requested_capital"]), created_at=as_utc(row["created_at"]),
            source_event_timestamp=as_utc(row["source_event_timestamp"]), size_ratio=row["size_ratio"], reason=row["reason"],
            target_position_before=float(row["target_position_before"]), target_leverage=row["target_leverage"],
            target_equity=row["target_equity"], equity_source=row["equity_source"], equity_age_seconds=row["equity_age_seconds"],
        )

    def get_signal(self, signal_id: str) -> CopySignal | None:
        with self._connect() as connection:
            row = connection.execute("SELECT * FROM copy_signals WHERE signal_id=?", (signal_id,)).fetchone()
        return self._signal_from_row(row) if row else None

    def sizing_history(self, wallet: str) -> list[dict[str, Any]]:
        """Prior reconstructed entry evidence for one-time classifier restore.

        This is intentionally sourced from PositionEvents rather than PAPER
        signals: a paused or recovery-blocked entry must still preserve the
        target's prior-only sizing context for later live source events.
        """
        with self._connect() as connection:
            rows = connection.execute(
                """SELECT event_id, event_type,
                          CASE WHEN initial_delta_notional>0 THEN initial_delta_notional ELSE notional END AS target_notional,
                          target_equity, equity_source, equity_age_seconds
                   FROM copy_position_events WHERE target_wallet=? AND event_type IN ('OPEN', 'ADD')
                   ORDER BY event_timestamp, event_id""", (wallet.lower(),),
            ).fetchall()
        return [dict(row) for row in rows]

    def upsert_virtual_position(self, sleeve: VirtualTargetPosition) -> None:
        with self._connect() as connection:
            connection.execute(
                """INSERT INTO copy_virtual_positions(sleeve_id, target_wallet, campaign_id, symbol, direction,
                quantity, entry_price, allocated_capital, remaining_capital, entry_fee, realized_pnl, exit_fee,
                opened_at, updated_at, closed_at, target_entry_price, max_drawdown, current_mark, unrealized_pnl)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(sleeve_id) DO UPDATE SET quantity=excluded.quantity, remaining_capital=excluded.remaining_capital,
                realized_pnl=excluded.realized_pnl, exit_fee=excluded.exit_fee, updated_at=excluded.updated_at,
                closed_at=excluded.closed_at, max_drawdown=excluded.max_drawdown, current_mark=excluded.current_mark,
                unrealized_pnl=excluded.unrealized_pnl""",
                (sleeve.sleeve_id, sleeve.target_wallet, sleeve.campaign_id, sleeve.symbol, sleeve.direction,
                 sleeve.quantity, sleeve.entry_price, sleeve.allocated_capital, sleeve.remaining_capital, sleeve.entry_fee,
                 sleeve.realized_pnl, sleeve.exit_fee, iso(sleeve.opened_at), iso(sleeve.updated_at),
                 iso(sleeve.closed_at) if sleeve.closed_at else None, sleeve.target_entry_price, sleeve.max_drawdown,
                 sleeve.current_mark, sleeve.unrealized_pnl),
            )

    def list_virtual_positions(self, *, open_only: bool = False) -> list[VirtualTargetPosition]:
        query = "SELECT * FROM copy_virtual_positions"
        if open_only:
            query += " WHERE closed_at IS NULL"
        query += " ORDER BY opened_at, sleeve_id"
        with self._connect() as connection:
            rows = connection.execute(query).fetchall()
        return [
            VirtualTargetPosition(sleeve_id=row["sleeve_id"], target_wallet=row["target_wallet"], campaign_id=row["campaign_id"],
                                  symbol=row["symbol"], direction=row["direction"], quantity=float(row["quantity"]),
                                  entry_price=float(row["entry_price"]), allocated_capital=float(row["allocated_capital"]),
                                  remaining_capital=float(row["remaining_capital"]), entry_fee=float(row["entry_fee"]),
                                  realized_pnl=float(row["realized_pnl"]), exit_fee=float(row["exit_fee"]),
                                  opened_at=as_utc(row["opened_at"]), updated_at=as_utc(row["updated_at"]),
                                  closed_at=as_utc(row["closed_at"]) if row["closed_at"] else None,
                                  target_entry_price=row["target_entry_price"], max_drawdown=float(row["max_drawdown"]),
                                  current_mark=row["current_mark"], unrealized_pnl=float(row["unrealized_pnl"]))
            for row in rows
        ]

    def list_realized_results(self) -> list[tuple[str, float, object]]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT timestamp, raw_json FROM copy_execution_fills ORDER BY timestamp, execution_fill_id"
            ).fetchall()
        results: list[tuple[str, float, object]] = []
        for row in rows:
            raw = _load(row["raw_json"], {})
            if "target_wallet" in raw and ("risk_realized_pnl" in raw or "realized_pnl" in raw):
                value = raw["risk_realized_pnl"] if "risk_realized_pnl" in raw else raw["realized_pnl"]
                results.append((str(raw["target_wallet"]), float(value), as_utc(row["timestamp"])))
        return results

    def insert_execution_attempt(self, attempt: ExecutionAttempt) -> None:
        with self._connect() as connection:
            connection.execute(
                """INSERT INTO copy_execution_attempts(attempt_id, signal_id, target_wallet, symbol,
                action, requested_capital, status, reason, source_event_timestamp, received_at, decided_at,
                paper_order_at, simulated_execution_at, detection_latency_ms, decision_latency_ms)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (attempt.attempt_id, attempt.signal_id, attempt.target_wallet, attempt.symbol, attempt.action,
                 attempt.requested_capital, attempt.status, attempt.reason, iso(attempt.source_event_timestamp),
                 iso(attempt.received_at), iso(attempt.decided_at), iso(attempt.paper_order_at) if attempt.paper_order_at else None,
                 iso(attempt.simulated_execution_at) if attempt.simulated_execution_at else None,
                 attempt.detection_latency_ms, attempt.decision_latency_ms),
            )

    def get_execution_attempt(self, signal_id: str) -> ExecutionAttempt | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM copy_execution_attempts WHERE signal_id=? ORDER BY decided_at LIMIT 1", (signal_id,)
            ).fetchone()
        if not row:
            return None
        return ExecutionAttempt(
            attempt_id=row["attempt_id"], signal_id=row["signal_id"], target_wallet=row["target_wallet"],
            symbol=row["symbol"], action=row["action"], requested_capital=float(row["requested_capital"]),
            status=row["status"], reason=row["reason"], source_event_timestamp=as_utc(row["source_event_timestamp"]),
            received_at=as_utc(row["received_at"]), decided_at=as_utc(row["decided_at"]),
            paper_order_at=as_utc(row["paper_order_at"]) if row["paper_order_at"] else None,
            simulated_execution_at=as_utc(row["simulated_execution_at"]) if row["simulated_execution_at"] else None,
            detection_latency_ms=float(row["detection_latency_ms"]), decision_latency_ms=float(row["decision_latency_ms"]),
        )

    def has_execution_attempt_for_signal(self, signal_id: str) -> bool:
        with self._connect() as connection:
            row = connection.execute("SELECT 1 FROM copy_execution_attempts WHERE signal_id=?", (signal_id,)).fetchone()
        return row is not None

    def insert_execution_fill(self, fill: ExecutionFill) -> None:
        with self._connect() as connection:
            connection.execute(
                """INSERT OR IGNORE INTO copy_execution_fills(execution_fill_id, attempt_id, sleeve_id, price,
                quantity, notional, fee, slippage_bps, timestamp, raw_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (fill.execution_fill_id, fill.attempt_id, fill.sleeve_id, fill.price, fill.quantity, fill.notional,
                 fill.fee, fill.slippage_bps, iso(fill.timestamp), _dump(fill.raw)),
            )

    def list_execution_fills_for_attempt(self, attempt_id: str) -> list[ExecutionFill]:
        """Compatibility read of historical PAPER fills for the D.2 bridge."""
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM copy_execution_fills WHERE attempt_id=? ORDER BY timestamp, execution_fill_id", (attempt_id,)
            ).fetchall()
        return [ExecutionFill(
            execution_fill_id=row["execution_fill_id"], attempt_id=row["attempt_id"], sleeve_id=row["sleeve_id"],
            price=float(row["price"]), quantity=float(row["quantity"]), notional=float(row["notional"]),
            fee=float(row["fee"]), slippage_bps=float(row["slippage_bps"]), timestamp=as_utc(row["timestamp"]),
            raw=_load(row["raw_json"], {}),
        ) for row in rows]

    def insert_backtest_run(self, run: BacktestRun) -> None:
        with self._connect() as connection:
            connection.execute(
                """INSERT OR REPLACE INTO copy_backtest_runs(run_id, started_at, finished_at, target_wallets_json,
                start_timestamp, end_timestamp, initial_capital, ending_capital, seed, configuration_json,
                summary_json, git_commit) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (run.run_id, iso(run.started_at), iso(run.finished_at) if run.finished_at else None, _dump(run.target_wallets),
                 iso(run.start_timestamp) if run.start_timestamp else None, iso(run.end_timestamp) if run.end_timestamp else None,
                run.initial_capital, run.ending_capital, run.seed, _dump(run.configuration), _dump(run.summary), run.git_commit),
            )

    def insert_portfolio_snapshot(
        self, *, timestamp: object, cash: float, equity: float, committed_capital: float, drawdown_fraction: float,
        peak_equity: float | None = None, max_drawdown_fraction: float = 0.0,
    ) -> None:
        snapshot_id = stable_id("portfolio", iso(timestamp), cash, equity, committed_capital, drawdown_fraction)
        with self._connect() as connection:
            connection.execute(
                """INSERT OR IGNORE INTO copy_portfolio_snapshots(snapshot_id, timestamp, cash, equity,
                   committed_capital, drawdown_fraction, peak_equity, max_drawdown_fraction) VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (snapshot_id, iso(timestamp), cash, equity, committed_capital, drawdown_fraction, peak_equity, max_drawdown_fraction),
            )

    def persist_portfolio_mark(
        self, sleeves: Iterable[VirtualTargetPosition], snapshot: dict[str, float], *, timestamp: object,
    ) -> int:
        """Persist observational marks for sleeves that are still open.

        Marking has deliberately weaker authority than execution.  It can
        never create a sleeve or rewrite economic state (quantity, capital,
        realized P&L, fees, or closure).  This makes a stale in-memory engine
        harmless after another execution path has committed a close.
        """
        persisted = 0
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                for sleeve in sleeves:
                    cursor = connection.execute(
                        """UPDATE copy_virtual_positions
                           SET updated_at=?, max_drawdown=?, current_mark=?, unrealized_pnl=?
                           WHERE sleeve_id=? AND closed_at IS NULL""",
                        (iso(sleeve.updated_at), sleeve.max_drawdown, sleeve.current_mark, sleeve.unrealized_pnl,
                         sleeve.sleeve_id),
                    )
                    persisted += cursor.rowcount
                # Never checkpoint the caller's portfolio total: it may be a
                # stale engine that just lost a race to a committed close.
                # Derive every economic aggregate from database truth instead.
                if persisted:
                    latest = connection.execute(
                        """SELECT cash, peak_equity, max_drawdown_fraction
                           FROM copy_portfolio_snapshots ORDER BY timestamp DESC LIMIT 1"""
                    ).fetchone()
                    if latest:
                        aggregates = connection.execute(
                            """SELECT COALESCE(SUM(remaining_capital), 0) AS committed_capital,
                                      COALESCE(SUM(unrealized_pnl), 0) AS unrealized_pnl
                               FROM copy_virtual_positions WHERE closed_at IS NULL"""
                        ).fetchone()
                        cash = float(latest["cash"])
                        committed = float(aggregates["committed_capital"])
                        equity = cash + committed + float(aggregates["unrealized_pnl"])
                        peak = max(float(latest["peak_equity"] or latest["cash"]), equity)
                        drawdown = max(0.0, peak - equity) / max(peak, 1e-12)
                        maximum = max(float(latest["max_drawdown_fraction"] or 0.0), drawdown)
                        snapshot_id = stable_id("portfolio_mark", iso(timestamp), cash, equity, committed, drawdown)
                        connection.execute(
                            """INSERT OR IGNORE INTO copy_portfolio_snapshots(snapshot_id, timestamp, cash, equity, committed_capital,
                            drawdown_fraction, peak_equity, max_drawdown_fraction) VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                            (snapshot_id, iso(timestamp), cash, equity, committed, drawdown, peak, maximum),
                        )
                connection.commit()
                return persisted
            except Exception:
                connection.rollback()
                raise

    def latest_portfolio_snapshot(self) -> dict[str, Any] | None:
        with self._connect() as connection:
            row = connection.execute("SELECT * FROM copy_portfolio_snapshots ORDER BY timestamp DESC LIMIT 1").fetchone()
        return dict(row) if row else None

    def insert_backfill_coverage(self, wallet: str, coverage: Any) -> None:
        coverage_id = stable_id("coverage", wallet.lower(), coverage.requested_start, coverage.requested_end, coverage.coverage_quality)
        with self._connect() as connection:
            connection.execute(
                """INSERT OR IGNORE INTO copy_backfill_coverage(coverage_id, target_wallet, requested_start, requested_end,
                earliest_observed_fill, latest_observed_fill, source_limit_detected, coverage_complete, coverage_quality, coverage_state)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (coverage_id, wallet.lower(), iso(coverage.requested_start), iso(coverage.requested_end),
                 iso(coverage.earliest_observed_fill) if coverage.earliest_observed_fill else None,
                 iso(coverage.latest_observed_fill) if coverage.latest_observed_fill else None,
                 int(coverage.source_limit_detected), int(coverage.coverage_complete), coverage.coverage_quality, coverage.coverage_state),
            )

    def latest_backfill_coverage(self, wallet: str) -> dict[str, Any] | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM copy_backfill_coverage WHERE target_wallet=? ORDER BY requested_end DESC LIMIT 1", (wallet.lower(),)
            ).fetchone()
        return dict(row) if row else None

    def analysis_window_coverage(self, wallet: str, required_start: object, required_end: object) -> dict[str, Any]:
        """Evaluate source-proof coverage over the whole requested interval.

        Raw fill timestamps are deliberately excluded: data presence cannot
        prove the absence of omitted fills.  Only continuous
        ``PROVEN_COMPLETE`` request segments establish complete coverage.
        """
        start, end = as_utc(required_start), as_utc(required_end)
        with self._connect() as connection:
            rows = connection.execute(
                """SELECT * FROM copy_backfill_coverage
                   WHERE target_wallet=? AND requested_end>=? AND requested_start<=?
                   ORDER BY requested_start, requested_end""",
                (wallet.lower(), iso(start), iso(end)),
            ).fetchall()
        segments = [dict(row) for row in rows]
        response: dict[str, Any] = {
            "required_start": iso(start), "required_end": iso(end), "coverage_state": "UNPROVEN",
            "coverage_quality": "analysis_window", "segment_count": len(segments),
            "segments": [
                {"requested_start": item["requested_start"], "requested_end": item["requested_end"],
                 "coverage_state": item.get("coverage_state", "UNPROVEN"), "coverage_quality": item["coverage_quality"]}
                for item in segments
            ],
        }
        if any(str(item.get("coverage_state")) == "KNOWN_INCOMPLETE" for item in segments):
            response["coverage_state"] = "KNOWN_INCOMPLETE"
            return response
        proven = [item for item in segments if str(item.get("coverage_state")) == "PROVEN_COMPLETE"]
        if not proven:
            return response
        cursor = start
        for item in proven:
            segment_start, segment_end = as_utc(item["requested_start"]), as_utc(item["requested_end"])
            if segment_end < cursor:
                continue
            # SQLite timestamps have microsecond precision.  No undocumented
            # adjacency tolerance is used: a gap must be explicitly covered.
            if segment_start > cursor:
                return response
            cursor = max(cursor, segment_end)
            if cursor >= end:
                response["coverage_state"] = "PROVEN_COMPLETE"
                response["coverage_complete"] = True
                return response
        return response

    def commit_execution(
        self, signal: CopySignal, attempt: ExecutionAttempt, sleeves: Iterable[VirtualTargetPosition],
        fills: Iterable[ExecutionFill], *, snapshot: dict[str, float] | None, fault_hook: Any = None,
        phase_d_projection: bool = False,
    ) -> bool:
        """The paper execution idempotency boundary.

        A signal claim, attempt, sleeve mutations, fills, and portfolio snapshot
        live in one SQLite transaction.  A process death before commit leaves no
        claim or economic side effect; after commit, the primary-key claim makes
        replay a no-op.
        """
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                fill_rows = tuple(fills)
                exists = connection.execute("SELECT 1 FROM copy_execution_claims WHERE signal_id=?", (signal.signal_id,)).fetchone()
                if exists:
                    connection.rollback()
                    return False
                connection.execute(
                    """INSERT OR IGNORE INTO copy_signals(signal_id, target_wallet, campaign_id, source_event_id,
                    symbol, action, direction, target_price, target_quantity, target_notional, allocation_fraction,
                    requested_capital, created_at, source_event_timestamp, size_ratio, reason, target_position_before,
                    target_leverage, target_equity, equity_source, equity_age_seconds)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (signal.signal_id, signal.target_wallet, signal.campaign_id, signal.source_event_id, signal.symbol,
                     signal.action, signal.direction, signal.target_price, signal.target_quantity, signal.target_notional,
                     signal.allocation_fraction, signal.requested_capital, iso(signal.created_at), iso(signal.source_event_timestamp),
                     signal.size_ratio, signal.reason, signal.target_position_before, signal.target_leverage, signal.target_equity,
                     signal.equity_source, signal.equity_age_seconds),
                )
                connection.execute(
                    "INSERT INTO copy_execution_claims(signal_id, status, claimed_at, attempt_id) VALUES (?, 'committed', ?, ?)",
                    (signal.signal_id, iso(attempt.decided_at), attempt.attempt_id),
                )
                if fault_hook:
                    fault_hook("after_claim")
                connection.execute(
                    """INSERT INTO copy_execution_attempts(attempt_id, signal_id, target_wallet, symbol, action,
                    requested_capital, status, reason, source_event_timestamp, received_at, decided_at, paper_order_at,
                    simulated_execution_at, detection_latency_ms, decision_latency_ms)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (attempt.attempt_id, attempt.signal_id, attempt.target_wallet, attempt.symbol, attempt.action,
                     attempt.requested_capital, attempt.status, attempt.reason, iso(attempt.source_event_timestamp),
                     iso(attempt.received_at), iso(attempt.decided_at), iso(attempt.paper_order_at) if attempt.paper_order_at else None,
                     iso(attempt.simulated_execution_at) if attempt.simulated_execution_at else None,
                     attempt.detection_latency_ms, attempt.decision_latency_ms),
                )
                if fault_hook:
                    fault_hook("after_attempt")
                for sleeve in sleeves:
                    connection.execute(
                        """INSERT INTO copy_virtual_positions(sleeve_id, target_wallet, campaign_id, symbol, direction,
                        quantity, entry_price, allocated_capital, remaining_capital, entry_fee, realized_pnl, exit_fee,
                        opened_at, updated_at, closed_at, target_entry_price, max_drawdown, current_mark, unrealized_pnl)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(sleeve_id) DO UPDATE SET quantity=excluded.quantity, remaining_capital=excluded.remaining_capital,
                        realized_pnl=excluded.realized_pnl, exit_fee=excluded.exit_fee, updated_at=excluded.updated_at,
                        closed_at=excluded.closed_at, max_drawdown=excluded.max_drawdown, current_mark=excluded.current_mark,
                        unrealized_pnl=excluded.unrealized_pnl""",
                        (sleeve.sleeve_id, sleeve.target_wallet, sleeve.campaign_id, sleeve.symbol, sleeve.direction,
                         sleeve.quantity, sleeve.entry_price, sleeve.allocated_capital, sleeve.remaining_capital,
                         sleeve.entry_fee, sleeve.realized_pnl, sleeve.exit_fee, iso(sleeve.opened_at), iso(sleeve.updated_at),
                         iso(sleeve.closed_at) if sleeve.closed_at else None, sleeve.target_entry_price, sleeve.max_drawdown,
                         sleeve.current_mark, sleeve.unrealized_pnl),
                    )
                if fault_hook:
                    fault_hook("after_sleeves")
                for fill in fill_rows:
                    connection.execute(
                        """INSERT INTO copy_execution_fills(execution_fill_id, attempt_id, sleeve_id, price, quantity,
                        notional, fee, slippage_bps, timestamp, raw_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (fill.execution_fill_id, fill.attempt_id, fill.sleeve_id, fill.price, fill.quantity, fill.notional,
                         fill.fee, fill.slippage_bps, iso(fill.timestamp), _dump(fill.raw)),
                    )
                if snapshot:
                    snapshot_id = stable_id("portfolio", iso(attempt.decided_at), snapshot["cash"], snapshot["equity"], snapshot["committed_capital"], snapshot["drawdown_fraction"])
                    connection.execute(
                        """INSERT INTO copy_portfolio_snapshots(snapshot_id, timestamp, cash, equity, committed_capital,
                        drawdown_fraction, peak_equity, max_drawdown_fraction) VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                        (snapshot_id, iso(attempt.decided_at), snapshot["cash"], snapshot["equity"], snapshot["committed_capital"],
                         snapshot["drawdown_fraction"], snapshot.get("peak_equity"), snapshot.get("max_drawdown_fraction", 0.0)),
                    )
                if phase_d_projection:
                    self._commit_phase_d_paper_projection(connection, signal, attempt, fill_rows)
                    if fault_hook:
                        fault_hook("after_phase_d_projection")
                if fault_hook:
                    fault_hook("before_commit")
                connection.commit()
                return True
            except Exception:
                connection.rollback()
                raise

    def _commit_phase_d_paper_projection(
        self, connection: sqlite3.Connection, signal: CopySignal, attempt: ExecutionAttempt, fills: tuple[ExecutionFill, ...],
    ) -> None:
        """Write the D.2 compatibility projection in the legacy paper transaction.

        This is deliberately a compact SQL projection rather than a call to
        the public D methods, avoiding extra connection/commit latency on the
        Phase-C market-evidence path. Existing D.0 rows are never rewritten.
        """
        existing = connection.execute(
            "SELECT 1 FROM phase_d_execution_intents WHERE signal_id=?", (signal.signal_id,)
        ).fetchone()
        if existing:
            return
        intent = ExecutionIntent.from_copy_signal(
            signal, accepted_at=attempt.decided_at, execution_domain="PAPER_COMPAT",
            execution_account_id="PAPER_COMPAT:legacy_paper",
        )
        allowed = attempt.status == "filled" and bool(fills)
        projection_reason = attempt.reason if allowed or attempt.status != "filled" else "paper_filled_without_fill_evidence"
        state = ExecutionState.FILLED if allowed and fills else ExecutionState.BLOCKED
        connection.execute(
            """INSERT INTO phase_d_execution_intents(
                intent_id, contract_version, signal_id, source_event_id, target_wallet, campaign_id, symbol,
                action, direction, requested_quantity, requested_capital, source_event_timestamp, accepted_at,
                provenance_json, exposure_effect, supersedes_intent_id, execution_domain, execution_account_id, state, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (intent.intent_id, intent.contract_version, intent.signal_id, intent.source_event_id, intent.target_wallet,
             intent.campaign_id, intent.symbol, intent.action, intent.direction, intent.requested_quantity,
             intent.requested_capital, iso(intent.source_event_timestamp), iso(attempt.decided_at), _dump(intent.provenance),
             intent.exposure_effect.value, intent.supersedes_intent_id, intent.execution_domain,
             intent.execution_account_id, state.value, iso(attempt.decided_at)),
        )
        # PAPER has already completed its economic lifecycle inside this
        # transaction.  Store one immutable terminal projection event rather
        # than replaying intermediate D transitions one SQLite write at a
        # time.  The evidence preserves the semantic path for audit while the
        # live state machine remains the authority for future venue adapters.
        reason = "paper_execution_committed" if allowed else projection_reason
        source = "paper_execution_commit"
        connection.execute(
            """INSERT INTO phase_d_execution_state_events(
                event_id, intent_id, sequence, previous_state, next_state, reason, source, occurred_at, raw_evidence_json)
               VALUES (?, ?, 1, NULL, ?, ?, ?, ?, ?)""",
            (stable_id("phase_d_execution_state", intent.intent_id, 1, None, state.value, reason, source),
             intent.intent_id, state.value, reason, source, iso(attempt.decided_at),
             _dump({
                 "paper_compatibility": True, "legacy_attempt_id": attempt.attempt_id,
                 "projected_lifecycle": (["CREATED", "VALIDATING", "READY", "SUBMITTING", "FILLED"]
                                         if allowed else ["CREATED", "VALIDATING", "BLOCKED"]),
             })),
        )
        connection.execute(
            """INSERT INTO phase_d_execution_risk_decisions(decision_id, intent_id, allowed, reason, evaluated_at, evidence_json)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (stable_id("phase_d_paper_risk", intent.intent_id, attempt.attempt_id, attempt.status, attempt.reason),
             intent.intent_id, int(allowed), projection_reason, iso(attempt.decided_at),
             _dump({"paper_compatibility": True, "paper_attempt_id": attempt.attempt_id, "paper_status": attempt.status})),
        )
        if not allowed or not fills:
            return
        quantity = sum(fill.quantity for fill in fills)
        submission_id = stable_id("phase_d_paper_submission", intent.intent_id)
        client_order_id = stable_id("phase_d_paper_client_order", intent.intent_id)
        connection.execute(
            """INSERT INTO phase_d_execution_submissions(
                submission_id, intent_id, client_order_id, requested_quantity, side, state, venue_order_id,
                execution_domain, execution_account_id, filled_quantity, created_at, updated_at, raw_evidence_json)
               VALUES (?, ?, ?, ?, ?, 'FILLED', ?, ?, ?, ?, ?, ?, ?)""",
            (submission_id, intent.intent_id, client_order_id, quantity,
             "BUY" if (intent.direction == "long") == (intent.action in {"open", "add"}) else "SELL",
             stable_id("phase_d_paper_order", attempt.attempt_id), intent.execution_domain, intent.execution_account_id,
             quantity, iso(attempt.decided_at), iso(attempt.decided_at),
             _dump({"paper_compatibility": True, "legacy_attempt_id": attempt.attempt_id})),
        )
        for fill in fills:
            connection.execute(
                """INSERT INTO phase_d_execution_fills(
                    execution_fill_id, intent_id, submission_id, venue_fill_id, quantity, price, fee,
                    execution_domain, execution_account_id, side, venue_timestamp, received_at, raw_evidence_json)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (stable_id("phase_d_execution_fill_v1", submission_id,
                           stable_id("phase_d_paper_fill", attempt.attempt_id, fill.execution_fill_id)),
                 intent.intent_id, submission_id, stable_id("phase_d_paper_fill", attempt.attempt_id, fill.execution_fill_id),
                 fill.quantity, fill.price, fill.fee, intent.execution_domain, intent.execution_account_id,
                 "BUY" if (intent.direction == "long") == (intent.action in {"open", "add"}) else "SELL",
                 iso(fill.timestamp), iso(attempt.decided_at),
                 _dump({"paper_compatibility": True, "legacy_execution_fill_id": fill.execution_fill_id, **fill.raw})),
            )

    # ------------------------------------------------------------------
    # Phase D execution ledger.  This is deliberately separate from the
    # historical paper tables and is not called by PaperExecutionEngine.

    @staticmethod
    def _phase_d_intent_from_row(row: sqlite3.Row) -> ExecutionIntent:
        version = int(row["contract_version"])
        if version != PHASE_D_EXECUTION_CONTRACT_VERSION:
            raise ValueError(
                f"Unsupported Phase-D execution contract version {version}; "
                f"reader supports {PHASE_D_EXECUTION_CONTRACT_VERSION}."
            )
        return ExecutionIntent(
            intent_id=row["intent_id"], signal_id=row["signal_id"], source_event_id=row["source_event_id"],
            target_wallet=row["target_wallet"], campaign_id=row["campaign_id"], symbol=row["symbol"],
            action=row["action"], direction=row["direction"], requested_quantity=float(row["requested_quantity"]),
            requested_capital=float(row["requested_capital"]), source_event_timestamp=as_utc(row["source_event_timestamp"]),
            accepted_at=as_utc(row["accepted_at"]), contract_version=version,
            provenance=_load(row["provenance_json"], {}), exposure_effect=ExposureEffect(row["exposure_effect"]),
            supersedes_intent_id=row["supersedes_intent_id"], execution_domain=row["execution_domain"],
            execution_account_id=row["execution_account_id"], state=ExecutionState(row["state"]),
            updated_at=as_utc(row["updated_at"]),
        )

    @staticmethod
    def _phase_d_submission_from_row(row: sqlite3.Row) -> ExecutionSubmission:
        return ExecutionSubmission(
            submission_id=row["submission_id"], intent_id=row["intent_id"], client_order_id=row["client_order_id"],
            requested_quantity=float(row["requested_quantity"]), side=row["side"], state=row["state"],
            venue_order_id=row["venue_order_id"], filled_quantity=float(row["filled_quantity"]),
            created_at=as_utc(row["created_at"]), updated_at=as_utc(row["updated_at"]),
            raw_evidence=_load(row["raw_evidence_json"], {}), execution_domain=row["execution_domain"],
            execution_account_id=row["execution_account_id"],
        )

    @staticmethod
    def _same_phase_d_intent(existing: ExecutionIntent, requested: ExecutionIntent) -> bool:
        """Do not use an upsert to turn an existing intent into revised history."""
        return (
            existing.intent_id == requested.intent_id and existing.contract_version == requested.contract_version
            and existing.signal_id == requested.signal_id and existing.source_event_id == requested.source_event_id
            and existing.target_wallet == requested.target_wallet and existing.campaign_id == requested.campaign_id
            and existing.symbol == requested.symbol and existing.action == requested.action
            and existing.direction == requested.direction and existing.requested_quantity == requested.requested_quantity
            and existing.requested_capital == requested.requested_capital
            and iso(existing.source_event_timestamp) == iso(requested.source_event_timestamp)
            and existing.exposure_effect == requested.exposure_effect
            and existing.supersedes_intent_id == requested.supersedes_intent_id
            and existing.execution_domain == requested.execution_domain
            and existing.execution_account_id == requested.execution_account_id
            and _dump(existing.provenance) == _dump(requested.provenance)
        )

    def create_or_get_execution_intent(self, intent: ExecutionIntent) -> ExecutionIntent:
        """Atomically claim a Phase-C signal and write immutable D provenance."""
        if intent.contract_version != PHASE_D_EXECUTION_CONTRACT_VERSION:
            raise ValueError(f"Unsupported Phase-D execution contract version {intent.contract_version}.")
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            existing_row = connection.execute(
                "SELECT * FROM phase_d_execution_intents WHERE signal_id=?", (intent.signal_id,)
            ).fetchone()
            if existing_row:
                existing = self._phase_d_intent_from_row(existing_row)
                if not self._same_phase_d_intent(existing, intent):
                    raise ValueError("Phase-C signal already has an immutable, non-equivalent Phase-D intent.")
                return existing
            connection.execute(
                """INSERT INTO phase_d_execution_intents(
                    intent_id, contract_version, signal_id, source_event_id, target_wallet, campaign_id, symbol,
                    action, direction, requested_quantity, requested_capital, source_event_timestamp, accepted_at,
                    provenance_json, exposure_effect, supersedes_intent_id, execution_domain, execution_account_id, state, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (intent.intent_id, intent.contract_version, intent.signal_id, intent.source_event_id, intent.target_wallet,
                 intent.campaign_id, intent.symbol, intent.action, intent.direction, intent.requested_quantity,
                 intent.requested_capital, iso(intent.source_event_timestamp), iso(intent.accepted_at),
                 _dump(intent.provenance), intent.exposure_effect.value, intent.supersedes_intent_id,
                 intent.execution_domain, intent.execution_account_id, intent.state.value,
                 iso(intent.updated_at or intent.accepted_at)),
            )
            self._append_phase_d_state_event(
                connection, intent.intent_id, None, intent.state, "intent_accepted",
                "lane_ii_bridge" if intent.provenance.get("lane_ii", {}).get("source") == "LANE_II" else "phase_c",
                intent.accepted_at, {},
            )
        return intent

    def get_execution_intent(self, intent_id: str) -> ExecutionIntent | None:
        with self._connect() as connection:
            row = connection.execute("SELECT * FROM phase_d_execution_intents WHERE intent_id=?", (intent_id,)).fetchone()
        return self._phase_d_intent_from_row(row) if row else None

    def get_execution_intent_for_signal(self, signal_id: str) -> ExecutionIntent | None:
        with self._connect() as connection:
            row = connection.execute("SELECT * FROM phase_d_execution_intents WHERE signal_id=?", (signal_id,)).fetchone()
        return self._phase_d_intent_from_row(row) if row else None

    def list_execution_intents(self, *, states: Iterable[ExecutionState] | None = None) -> list[ExecutionIntent]:
        values = [state.value for state in states] if states else []
        query = "SELECT * FROM phase_d_execution_intents"
        if values:
            query += " WHERE state IN (" + ",".join("?" for _ in values) + ")"
        query += " ORDER BY accepted_at, intent_id"
        with self._connect() as connection:
            rows = connection.execute(query, values).fetchall()
        return [self._phase_d_intent_from_row(row) for row in rows]

    @staticmethod
    def _append_phase_d_state_event(
        connection: sqlite3.Connection, intent_id: str, previous: ExecutionState | None, next_state: ExecutionState,
        reason: str, source: str, occurred_at: object, raw_evidence: Mapping[str, Any],
    ) -> None:
        sequence = int(connection.execute(
            "SELECT COUNT(*) FROM phase_d_execution_state_events WHERE intent_id=?", (intent_id,)
        ).fetchone()[0]) + 1
        event_id = stable_id(
            "phase_d_execution_state", intent_id, sequence, previous.value if previous else None,
            next_state.value, reason, source,
        )
        connection.execute(
            """INSERT INTO phase_d_execution_state_events(
                event_id, intent_id, sequence, previous_state, next_state, reason, source, occurred_at, raw_evidence_json)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (event_id, intent_id, sequence, previous.value if previous else None, next_state.value,
             reason, source, iso(occurred_at), _dump(dict(raw_evidence))),
        )

    def transition_execution_intent(
        self, intent_id: str, next_state: ExecutionState, *, reason: str, source: str,
        occurred_at: object | None = None, raw_evidence: Mapping[str, Any] | None = None,
    ) -> ExecutionIntent:
        """Append a legal transition, ignoring stale observations after terminal truth."""
        at = as_utc(occurred_at)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute("SELECT * FROM phase_d_execution_intents WHERE intent_id=?", (intent_id,)).fetchone()
            if not row:
                raise KeyError(f"Unknown Phase-D execution intent: {intent_id}")
            current = self._phase_d_intent_from_row(row)
            if current.state == next_state:
                return current
            try:
                validate_execution_transition(current.state, next_state)
            except ValueError:
                # A stale acknowledgement/cancel notification must not undo a
                # terminal state.  Actual fill truth is explicitly modeled by
                # the one legal CANCELLED -> FILLED transition above.
                if current.state in TERMINAL_EXECUTION_STATES:
                    return current
                raise
            connection.execute(
                "UPDATE phase_d_execution_intents SET state=?, updated_at=? WHERE intent_id=?",
                (next_state.value, iso(at), intent_id),
            )
            self._append_phase_d_state_event(
                connection, intent_id, current.state, next_state, reason, source, at, raw_evidence or {},
            )
            row = connection.execute("SELECT * FROM phase_d_execution_intents WHERE intent_id=?", (intent_id,)).fetchone()
        return self._phase_d_intent_from_row(row)

    def list_execution_state_events(self, intent_id: str) -> list[dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM phase_d_execution_state_events WHERE intent_id=? ORDER BY sequence", (intent_id,)
            ).fetchall()
        return [
            {**dict(row), "raw_evidence": _load(row["raw_evidence_json"], {})}
            for row in rows
        ]

    def record_execution_risk_decision(self, decision: ExecutionRiskDecision) -> None:
        with self._connect() as connection:
            connection.execute(
                """INSERT OR IGNORE INTO phase_d_execution_risk_decisions(
                    decision_id, intent_id, allowed, reason, evaluated_at, evidence_json)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (decision.decision_id, decision.intent_id, int(decision.allowed), decision.reason,
                 iso(decision.evaluated_at), _dump(decision.evidence)),
            )

    def latest_execution_risk_decision(self, intent_id: str) -> dict[str, Any] | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM phase_d_execution_risk_decisions WHERE intent_id=? ORDER BY evaluated_at DESC, decision_id DESC LIMIT 1",
                (intent_id,),
            ).fetchone()
        return ({**dict(row), "allowed": bool(row["allowed"]), "evidence": _load(row["evidence_json"], {})} if row else None)

    def prepare_execution_submission(
        self, intent_id: str, *, submission_id: str, client_order_id: str, side: str, requested_quantity: float,
        created_at: object | None = None,
    ) -> tuple[ExecutionSubmission, bool]:
        """Persist the idempotent submission identity before adapter invocation."""
        at = as_utc(created_at)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                "SELECT * FROM phase_d_execution_submissions WHERE intent_id=?", (intent_id,)
            ).fetchone()
            if existing:
                return self._phase_d_submission_from_row(existing), False
            intent_row = connection.execute(
                "SELECT * FROM phase_d_execution_intents WHERE intent_id=?", (intent_id,)
            ).fetchone()
            if not intent_row:
                raise KeyError(f"Unknown Phase-D execution intent: {intent_id}")
            intent = self._phase_d_intent_from_row(intent_row)
            if intent.state is not ExecutionState.READY:
                raise ValueError(f"Cannot prepare submission from {intent.state.value}.")
            connection.execute(
                """INSERT INTO phase_d_execution_submissions(
                    submission_id, intent_id, client_order_id, requested_quantity, side, state, venue_order_id,
                    execution_domain, execution_account_id, filled_quantity, created_at, updated_at, raw_evidence_json)
                   VALUES (?, ?, ?, ?, ?, 'PREPARED', NULL, ?, ?, 0, ?, ?, '{}')""",
                (submission_id, intent_id, client_order_id, requested_quantity, side, intent.execution_domain,
                 intent.execution_account_id, iso(at), iso(at)),
            )
            connection.execute(
                "UPDATE phase_d_execution_intents SET state=?, updated_at=? WHERE intent_id=?",
                (ExecutionState.SUBMITTING.value, iso(at), intent_id),
            )
            self._append_phase_d_state_event(
                connection, intent_id, intent.state, ExecutionState.SUBMITTING,
                "submission_identity_persisted", "execution_engine", at,
                {"submission_id": submission_id, "client_order_id": client_order_id},
            )
            row = connection.execute(
                "SELECT * FROM phase_d_execution_submissions WHERE intent_id=?", (intent_id,)
            ).fetchone()
        return self._phase_d_submission_from_row(row), True

    def get_execution_submission(self, intent_id: str) -> ExecutionSubmission | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM phase_d_execution_submissions WHERE intent_id=?", (intent_id,)
            ).fetchone()
        return self._phase_d_submission_from_row(row) if row else None

    def update_execution_submission(
        self, intent_id: str, *, state: str, venue_order_id: str | None, filled_quantity: float,
        raw_evidence: Mapping[str, Any], updated_at: object | None = None,
    ) -> ExecutionSubmission:
        """Persist only monotonic venue evidence; stale reads cannot regress it."""
        at = as_utc(updated_at)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT * FROM phase_d_execution_submissions WHERE intent_id=?", (intent_id,)
            ).fetchone()
            if not row:
                raise KeyError(f"No Phase-D submission for intent {intent_id}")
            current = self._phase_d_submission_from_row(row)
            if current.venue_order_id and venue_order_id and current.venue_order_id != venue_order_id:
                self._record_execution_integrity_issue(
                    connection, execution_domain=current.execution_domain, execution_account_id=current.execution_account_id,
                    intent_id=intent_id, submission_id=current.submission_id,
                    category="CONFLICTING_VENUE_ORDER_ID", reason="submission_venue_order_id_conflict",
                    existing={"venue_order_id": current.venue_order_id, "state": current.state,
                              "filled_quantity": current.filled_quantity},
                    received={"venue_order_id": venue_order_id, "state": state, "filled_quantity": filled_quantity,
                              "raw_evidence": dict(raw_evidence)}, recorded_at=at,
                )
                return current
            current_rank = self._submission_state_rank(current.state)
            incoming_rank = self._submission_state_rank(state)
            next_state = state if incoming_rank > current_rank or current.state == state else current.state
            next_filled = max(current.filled_quantity, filled_quantity)
            next_order_id = current.venue_order_id or venue_order_id
            next_updated_at = max(as_utc(current.updated_at), at)
            next_evidence = (
                dict(raw_evidence)
                if incoming_rank > current_rank or (state == current.state and at >= as_utc(current.updated_at))
                else current.raw_evidence
            )
            connection.execute(
                """UPDATE phase_d_execution_submissions
                   SET state=?, venue_order_id=COALESCE(?, venue_order_id), filled_quantity=?, updated_at=?, raw_evidence_json=?
                   WHERE intent_id=?""",
                (next_state, next_order_id, next_filled, iso(next_updated_at), _dump(next_evidence), intent_id),
            )
            row = connection.execute(
                "SELECT * FROM phase_d_execution_submissions WHERE intent_id=?", (intent_id,)
            ).fetchone()
        return self._phase_d_submission_from_row(row)

    @staticmethod
    def _submission_state_rank(state: str) -> int:
        return {
            "PREPARED": 0,
            VenueOrderStatus.ACKNOWLEDGED.value: 1,
            VenueOrderStatus.PARTIALLY_FILLED.value: 2,
            VenueOrderStatus.CANCELLED.value: 3,
            VenueOrderStatus.REJECTED.value: 3,
            VenueOrderStatus.EXPIRED.value: 3,
            VenueOrderStatus.FILLED.value: 4,
        }.get(state, 0)

    @staticmethod
    def _record_execution_integrity_issue(
        connection: sqlite3.Connection, *, execution_domain: str, execution_account_id: str,
        intent_id: str | None, submission_id: str | None, category: str, reason: str,
        existing: Mapping[str, Any] | None, received: Mapping[str, Any] | None, recorded_at: object,
    ) -> None:
        issue_id = stable_id(
            "phase_d_execution_integrity", execution_domain, execution_account_id, intent_id, submission_id,
            category, reason, _dump(dict(existing or {})), _dump(dict(received or {})),
        )
        connection.execute(
            """INSERT OR IGNORE INTO phase_d_execution_integrity_issues(
                issue_id, execution_domain, execution_account_id, intent_id, submission_id, category, reason,
                existing_json, received_json, recorded_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (issue_id, execution_domain, execution_account_id, intent_id, submission_id, category, reason,
             _dump(dict(existing or {})), _dump(dict(received or {})), iso(recorded_at)),
        )

    def record_execution_venue_fill(
        self, intent_id: str, submission_id: str, fill: VenueFill, *, received_at: object | None = None,
    ) -> bool:
        """Persist immutable venue evidence and fail closed on identity collisions."""
        execution_fill_id = stable_id("phase_d_execution_fill_v1", submission_id, fill.venue_fill_id)
        at = as_utc(received_at)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            submission_row = connection.execute(
                "SELECT * FROM phase_d_execution_submissions WHERE submission_id=?", (submission_id,)
            ).fetchone()
            if not submission_row:
                raise KeyError(f"Unknown Phase-D submission: {submission_id}")
            submission = self._phase_d_submission_from_row(submission_row)
            if submission.intent_id != intent_id:
                self._record_execution_integrity_issue(
                    connection, execution_domain=submission.execution_domain, execution_account_id=submission.execution_account_id,
                    intent_id=intent_id, submission_id=submission_id, category="FILL_SUBMISSION_INTENT_CONFLICT",
                    reason="fill_submission_does_not_belong_to_intent",
                    existing={"submission_intent_id": submission.intent_id},
                    received={"intent_id": intent_id, "venue_fill_id": fill.venue_fill_id}, recorded_at=at,
                )
                return False
            side = (fill.side or submission.side).upper()
            incoming = {
                "intent_id": intent_id, "submission_id": submission_id, "venue_fill_id": fill.venue_fill_id,
                "execution_domain": submission.execution_domain, "execution_account_id": submission.execution_account_id,
                "side": side, "quantity": float(fill.quantity), "price": float(fill.price), "fee": float(fill.fee),
                "venue_order_id": fill.raw_payload.get("venue_order_id"), "client_order_id": fill.client_order_id,
            }
            if fill.client_order_id != submission.client_order_id or side not in {"BUY", "SELL"}:
                self._record_execution_integrity_issue(
                    connection, execution_domain=submission.execution_domain, execution_account_id=submission.execution_account_id,
                    intent_id=intent_id, submission_id=submission_id, category="FILL_IDENTITY_CONFLICT",
                    reason="fill_side_or_client_order_conflicts_with_submission",
                    existing={"client_order_id": submission.client_order_id, "side": submission.side},
                    received=incoming, recorded_at=at,
                )
                return False
            if side != submission.side:
                self._record_execution_integrity_issue(
                    connection, execution_domain=submission.execution_domain, execution_account_id=submission.execution_account_id,
                    intent_id=intent_id, submission_id=submission_id, category="FILL_SIDE_CONFLICT",
                    reason="fill_side_conflicts_with_submission",
                    existing={"side": submission.side}, received=incoming, recorded_at=at,
                )
            existing = connection.execute(
                "SELECT * FROM phase_d_execution_fills WHERE venue_fill_id=?", (fill.venue_fill_id,)
            ).fetchone()
            if existing:
                existing_raw = _load(existing["raw_evidence_json"], {})
                equivalent = (
                    existing["intent_id"] == intent_id and existing["submission_id"] == submission_id
                    and existing["execution_domain"] == submission.execution_domain
                    and existing["execution_account_id"] == submission.execution_account_id
                    and existing["side"] == side and float(existing["quantity"]) == float(fill.quantity)
                    and float(existing["price"]) == float(fill.price) and float(existing["fee"]) == float(fill.fee)
                    and existing_raw.get("venue_order_id") == fill.raw_payload.get("venue_order_id")
                )
                if not equivalent:
                    self._record_execution_integrity_issue(
                        connection, execution_domain=submission.execution_domain, execution_account_id=submission.execution_account_id,
                        intent_id=intent_id, submission_id=submission_id, category="CONFLICTING_VENUE_FILL_ID",
                        reason="venue_fill_id_conflicts_with_immutable_evidence",
                        existing={
                            "intent_id": existing["intent_id"], "submission_id": existing["submission_id"],
                            "execution_domain": existing["execution_domain"],
                            "execution_account_id": existing["execution_account_id"], "side": existing["side"],
                            "quantity": existing["quantity"], "price": existing["price"], "fee": existing["fee"],
                            "venue_order_id": existing_raw.get("venue_order_id"),
                        }, received=incoming, recorded_at=at,
                    )
                return False
            connection.execute(
                """INSERT INTO phase_d_execution_fills(
                    execution_fill_id, intent_id, submission_id, venue_fill_id, execution_domain, execution_account_id,
                    side, quantity, price, fee, venue_timestamp, received_at, raw_evidence_json)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (execution_fill_id, intent_id, submission_id, fill.venue_fill_id, submission.execution_domain,
                 submission.execution_account_id, side, fill.quantity, fill.price, fill.fee,
                 iso(fill.venue_timestamp), iso(at), _dump(fill.raw_payload)),
            )
            total = float(connection.execute(
                "SELECT COALESCE(SUM(quantity), 0) FROM phase_d_execution_fills WHERE submission_id=?", (submission_id,)
            ).fetchone()[0])
            if total > submission.requested_quantity + 1e-12:
                self._record_execution_integrity_issue(
                    connection, execution_domain=submission.execution_domain, execution_account_id=submission.execution_account_id,
                    intent_id=intent_id, submission_id=submission_id, category="OVERFILL_DETECTED",
                    reason="deduplicated_fill_quantity_exceeds_requested_quantity",
                    existing={"requested_quantity": submission.requested_quantity},
                    received={"deduplicated_fill_quantity": total, "venue_fill_id": fill.venue_fill_id}, recorded_at=at,
                )
        return True

    def list_execution_fills(
        self, intent_id: str | None = None, *, execution_domain: str | None = None,
        execution_account_id: str | None = None,
    ) -> list[dict[str, Any]]:
        query = "SELECT * FROM phase_d_execution_fills"
        values: list[Any] = []
        clauses: list[str] = []
        if intent_id:
            clauses.append("intent_id=?")
            values.append(intent_id)
        if execution_domain:
            clauses.append("execution_domain=?")
            values.append(execution_domain)
        if execution_account_id:
            clauses.append("execution_account_id=?")
            values.append(execution_account_id)
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY venue_timestamp, execution_fill_id"
        with self._connect() as connection:
            rows = connection.execute(query, values).fetchall()
        return [{**dict(row), "raw_evidence": _load(row["raw_evidence_json"], {})} for row in rows]

    def list_execution_integrity_issues(
        self, *, execution_domain: str | None = None, execution_account_id: str | None = None,
        intent_id: str | None = None,
    ) -> list[dict[str, Any]]:
        query = "SELECT * FROM phase_d_execution_integrity_issues"
        clauses: list[str] = []
        values: list[Any] = []
        for column, value in (("execution_domain", execution_domain), ("execution_account_id", execution_account_id),
                              ("intent_id", intent_id)):
            if value is not None:
                clauses.append(f"{column}=?")
                values.append(value)
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY recorded_at, issue_id"
        with self._connect() as connection:
            rows = connection.execute(query, values).fetchall()
        return [{**dict(row), "existing": _load(row["existing_json"], {}), "received": _load(row["received_json"], {})}
                for row in rows]

    def execution_has_integrity_issue(
        self, *, intent_id: str | None = None, execution_domain: str = "SIMULATOR",
        execution_account_id: str = "SIMULATOR:default",
    ) -> bool:
        query = """SELECT 1 FROM phase_d_execution_integrity_issues
                   WHERE execution_domain=? AND execution_account_id=?"""
        values: list[Any] = [execution_domain, execution_account_id]
        if intent_id:
            query += " AND intent_id=?"
            values.append(intent_id)
        query += " LIMIT 1"
        with self._connect() as connection:
            return connection.execute(query, values).fetchone() is not None

    def start_execution_reconciliation(
        self, reconciliation_run_id: str, *, scope: str, started_at: object, evidence: Mapping[str, Any] | None = None,
        execution_domain: str = "SIMULATOR", execution_account_id: str = "SIMULATOR:default",
    ) -> None:
        with self._connect() as connection:
            connection.execute(
                """INSERT OR IGNORE INTO phase_d_execution_reconciliation_runs(
                    reconciliation_run_id, scope, execution_domain, execution_account_id, state, started_at, completed_at, evidence_json)
                   VALUES (?, ?, ?, ?, 'RECONCILING', ?, NULL, ?)""",
                (reconciliation_run_id, scope, execution_domain, execution_account_id, iso(started_at), _dump(dict(evidence or {}))),
            )

    def record_execution_reconciliation_item(
        self, *, reconciliation_run_id: str, item_id: str, item_type: str, state: str, reason: str,
        intent_id: str | None = None, submission_id: str | None = None, local: Mapping[str, Any] | None = None,
        venue: Mapping[str, Any] | None = None, recorded_at: object | None = None,
    ) -> None:
        with self._connect() as connection:
            connection.execute(
                """INSERT OR IGNORE INTO phase_d_execution_reconciliation_items(
                    item_id, reconciliation_run_id, intent_id, submission_id, item_type, state, reason,
                    local_json, venue_json, recorded_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (item_id, reconciliation_run_id, intent_id, submission_id, item_type, state, reason,
                 _dump(dict(local or {})), _dump(dict(venue or {})), iso(recorded_at)),
            )

    def record_execution_position_observation(
        self, *, observation_id: str, reconciliation_run_id: str, symbol: str, local_signed_quantity: float,
        venue_signed_quantity: float | None, state: str, observed_at: object, raw_evidence: Mapping[str, Any] | None = None,
        execution_domain: str = "SIMULATOR", execution_account_id: str = "SIMULATOR:default",
    ) -> None:
        with self._connect() as connection:
            connection.execute(
                """INSERT OR IGNORE INTO phase_d_execution_position_observations(
                    observation_id, reconciliation_run_id, execution_domain, execution_account_id, symbol,
                    local_signed_quantity, venue_signed_quantity, state, observed_at, raw_evidence_json)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (observation_id, reconciliation_run_id, execution_domain, execution_account_id, symbol,
                 local_signed_quantity, venue_signed_quantity, state, iso(observed_at), _dump(dict(raw_evidence or {}))),
            )

    def complete_execution_reconciliation(
        self, reconciliation_run_id: str, *, state: str, completed_at: object, evidence: Mapping[str, Any] | None = None,
    ) -> None:
        with self._connect() as connection:
            connection.execute(
                """UPDATE phase_d_execution_reconciliation_runs
                   SET state=?, completed_at=?, evidence_json=? WHERE reconciliation_run_id=?""",
                (state, iso(completed_at), _dump(dict(evidence or {})), reconciliation_run_id),
            )

    def latest_execution_reconciliation(
        self, *, scopes: Iterable[str] | None = None, execution_domain: str = "SIMULATOR",
        execution_account_id: str = "SIMULATOR:default",
    ) -> dict[str, Any] | None:
        values: list[Any] = [execution_domain, execution_account_id]
        query = """SELECT * FROM phase_d_execution_reconciliation_runs
                   WHERE execution_domain=? AND execution_account_id=?"""
        if scopes:
            scope_values = list(scopes)
            query += " AND scope IN (" + ",".join("?" for _ in scope_values) + ")"
            values.extend(scope_values)
        query += " ORDER BY started_at DESC, reconciliation_run_id DESC LIMIT 1"
        with self._connect() as connection:
            row = connection.execute(query, values).fetchone()
        return {**dict(row), "evidence": _load(row["evidence_json"], {})} if row else None

    def phase_d_local_positions(
        self, *, execution_domain: str = "SIMULATOR", execution_account_id: str = "SIMULATOR:default",
    ) -> dict[str, float]:
        """Reconstruct scoped exposure from executed fill side, never intent wishes."""
        with self._connect() as connection:
            rows = connection.execute(
                """SELECT intent.symbol, fill.side, fill.quantity
                   FROM phase_d_execution_fills AS fill
                   JOIN phase_d_execution_intents AS intent ON intent.intent_id=fill.intent_id
                   WHERE fill.execution_domain=? AND fill.execution_account_id=?
                   ORDER BY fill.venue_timestamp, fill.execution_fill_id"""
                , (execution_domain, execution_account_id)
            ).fetchall()
        positions: dict[str, float] = {}
        for row in rows:
            signed = float(row["quantity"]) if row["side"] == "BUY" else -float(row["quantity"])
            positions[row["symbol"]] = positions.get(row["symbol"], 0.0) + signed
        return {symbol: quantity for symbol, quantity in positions.items() if abs(quantity) > 1e-12}

    def execution_position_reconciliation_unhealthy(
        self, *, execution_domain: str = "SIMULATOR", execution_account_id: str = "SIMULATOR:default",
    ) -> bool:
        """Whether the latest authoritative position observation is degraded.

        ``verified_flat`` contains a fresh position observation as well as the
        aggregate recovery result, so it may clear a prior position failure.
        It deliberately cannot clear the independent open-order authority.
        """
        position_run = self.latest_execution_reconciliation(
            scopes=("account_positions", "positions", "verified_flat"), execution_domain=execution_domain,
            execution_account_id=execution_account_id,
        )
        return bool(position_run and position_run["state"] not in {
            ReconciliationState.MATCHED.value, ReconciliationState.VERIFIED_FLAT.value,
        })

    def execution_open_order_reconciliation_unhealthy(
        self, *, execution_domain: str = "SIMULATOR", execution_account_id: str = "SIMULATOR:default",
    ) -> bool:
        """Whether the latest authoritative open-order observation is degraded.

        This selector intentionally excludes position runs.  An external open
        order remains an entry risk until a later ``open_orders`` observation,
        rather than a positions-only reconciliation, reports it absent.
        """
        order_run = self.latest_execution_reconciliation(
            scopes=("open_orders",), execution_domain=execution_domain,
            execution_account_id=execution_account_id,
        )
        return bool(order_run and order_run["state"] != ReconciliationState.MATCHED.value)

    def execution_account_reconciliation_unhealthy(
        self, *, execution_domain: str = "SIMULATOR", execution_account_id: str = "SIMULATOR:default",
    ) -> bool:
        """Compatibility name for the position-authority health check."""
        return self.execution_position_reconciliation_unhealthy(
            execution_domain=execution_domain, execution_account_id=execution_account_id,
        )

    def execution_safety_health(
        self, *, execution_domain: str = "SIMULATOR", execution_account_id: str = "SIMULATOR:default",
    ) -> dict[str, Any]:
        """Return the combined, independently-authoritative execution safety state.

        Integrity records are immutable contradictory evidence in D.3.2 and
        remain unresolved until a future explicitly-authorized remediation
        workflow exists.  They therefore fail closed alongside reconciliation
        authority rather than being treated as an entry-only warning.
        """
        position_authority = self.latest_execution_reconciliation(
            scopes=("account_positions", "positions", "verified_flat"), execution_domain=execution_domain,
            execution_account_id=execution_account_id,
        )
        open_order_authority = self.latest_execution_reconciliation(
            scopes=("open_orders",), execution_domain=execution_domain,
            execution_account_id=execution_account_id,
        )
        integrity_issues = self.list_execution_integrity_issues(
            execution_domain=execution_domain, execution_account_id=execution_account_id,
        )
        position_unhealthy = bool(position_authority and position_authority["state"] not in {
            ReconciliationState.MATCHED.value, ReconciliationState.VERIFIED_FLAT.value,
        })
        open_order_unhealthy = bool(
            open_order_authority and open_order_authority["state"] != ReconciliationState.MATCHED.value
        )
        integrity_unhealthy = bool(integrity_issues)
        reasons = [
            *(["position_reconciliation_unhealthy"] if position_unhealthy else []),
            *(["open_order_reconciliation_unhealthy"] if open_order_unhealthy else []),
            *(["execution_integrity_failure"] if integrity_unhealthy else []),
        ]
        return {
            "healthy": not reasons,
            "unhealthy": bool(reasons),
            "reasons": reasons,
            "position_authority": position_authority,
            "open_order_authority": open_order_authority,
            "position_unhealthy": position_unhealthy,
            "open_order_unhealthy": open_order_unhealthy,
            "integrity_unhealthy": integrity_unhealthy,
            "integrity_issue_count": len(integrity_issues),
        }

    def execution_has_unresolved_entry_risk(
        self, *, execution_domain: str = "SIMULATOR", execution_account_id: str = "SIMULATOR:default",
    ) -> bool:
        """Return scoped evidence that must fail closed for exposure increases."""
        with self._connect() as connection:
            unresolved = connection.execute(
                """SELECT 1 FROM phase_d_execution_intents
                   WHERE exposure_effect='INCREASE' AND state IN (
                       'SUBMITTING', 'SUBMISSION_UNKNOWN', 'ACKNOWLEDGED', 'PARTIALLY_FILLED',
                       'CANCEL_PENDING', 'RECONCILIATION_REQUIRED'
                   ) AND execution_domain=? AND execution_account_id=?
                   LIMIT 1"""
                , (execution_domain, execution_account_id)
            ).fetchone()
            if unresolved:
                return True
        return self.execution_safety_health(
            execution_domain=execution_domain, execution_account_id=execution_account_id,
        )["unhealthy"]

    def execution_unresolved_submissions(
        self, *, execution_domain: str = "SIMULATOR", execution_account_id: str = "SIMULATOR:default",
    ) -> list[dict[str, Any]]:
        """Durable ambiguity that prevents a verified-flat declaration."""
        with self._connect() as connection:
            rows = connection.execute(
                """SELECT intent_id, state, updated_at FROM phase_d_execution_intents
                   WHERE state IN ('SUBMITTING', 'SUBMISSION_UNKNOWN', 'RECONCILIATION_REQUIRED', 'CANCEL_PENDING')
                     AND execution_domain=? AND execution_account_id=?
                   ORDER BY updated_at, intent_id"""
                , (execution_domain, execution_account_id)
            ).fetchall()
        return [dict(row) for row in rows]

    def record_shadow_observation(self, observation: Mapping[str, Any]) -> bool:
        """Append one already-sanitized D.4 observation without rewriting history."""
        required = (
            "observation_id", "execution_domain", "execution_account_id", "venue", "account_id", "state",
            "freshness", "attempted_at", "received_at", "reason", "components", "normalized", "comparison", "raw_evidence",
        )
        missing = [name for name in required if name not in observation]
        if missing:
            raise ValueError("Shadow observation missing required fields: " + ", ".join(missing))
        with self._connect() as connection:
            cursor = connection.execute(
                """INSERT OR IGNORE INTO phase_d_shadow_observations(
                    observation_id, execution_domain, execution_account_id, venue, account_id, state, freshness,
                    observed_at, attempted_at, received_at, reason, components_json, normalized_json, comparison_json, raw_evidence_json)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    str(observation["observation_id"]), str(observation["execution_domain"]),
                    str(observation["execution_account_id"]), str(observation["venue"]), str(observation["account_id"]),
                    str(observation["state"]), str(observation["freshness"]), observation.get("observed_at"),
                    str(observation["attempted_at"]), str(observation["received_at"]), str(observation["reason"]),
                    _dump(dict(observation["components"])), _dump(dict(observation["normalized"])),
                    _dump(dict(observation["comparison"])), _dump(dict(observation["raw_evidence"])),
                ),
            )
        return cursor.rowcount == 1

    def latest_shadow_observation(
        self, *, execution_domain: str, execution_account_id: str,
    ) -> dict[str, Any] | None:
        with self._connect() as connection:
            row = connection.execute(
                """SELECT * FROM phase_d_shadow_observations
                   WHERE execution_domain=? AND execution_account_id=?
                   ORDER BY COALESCE(attempted_at, received_at) DESC,
                            CASE state WHEN 'INCOMPLETE' THEN 1 ELSE 0 END DESC,
                            received_at DESC, observation_id DESC LIMIT 1""",
                (execution_domain, execution_account_id),
            ).fetchone()
        if row is None:
            return None
        value = dict(row)
        for name, default in (
            ("components_json", {}), ("normalized_json", {}), ("comparison_json", {}), ("raw_evidence_json", {}),
        ):
            value[name[:-5]] = _load(value.pop(name), default)
        return value

    def list_shadow_observations(
        self, *, execution_domain: str, execution_account_id: str, limit: int = 50,
    ) -> list[dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute(
                """SELECT * FROM phase_d_shadow_observations
                   WHERE execution_domain=? AND execution_account_id=?
                   ORDER BY COALESCE(attempted_at, received_at) DESC,
                            CASE state WHEN 'INCOMPLETE' THEN 1 ELSE 0 END DESC,
                            received_at DESC, observation_id DESC LIMIT ?""",
                (execution_domain, execution_account_id, limit),
            ).fetchall()
        values: list[dict[str, Any]] = []
        for row in rows:
            value = dict(row)
            for name, default in (
                ("components_json", {}), ("normalized_json", {}), ("comparison_json", {}), ("raw_evidence_json", {}),
            ):
                value[name[:-5]] = _load(value.pop(name), default)
            values.append(value)
        return values

    def shadow_read_model(
        self, *, configured: bool = False, venue: str | None = None, account_id: str | None = None,
        execution_domain: str | None = None, execution_account_id: str | None = None,
    ) -> dict[str, Any]:
        """D.4 operator visibility; it never feeds simulator execution authority."""
        if not configured:
            return {
                "configured": False, "state": "NOT_CONFIGURED", "read_only": True,
                "reason": "shadow_observation_not_configured", "latest_observation": None, "history": [],
            }
        if not all((venue, account_id, execution_domain, execution_account_id)):
            raise ValueError("Configured shadow read model requires venue, account, domain, and account scope.")
        latest = self.latest_shadow_observation(
            execution_domain=str(execution_domain), execution_account_id=str(execution_account_id),
        )
        return {
            "configured": True, "read_only": True, "venue": str(venue), "account_id": str(account_id),
            "execution_domain": str(execution_domain), "execution_account_id": str(execution_account_id),
            "state": latest["state"] if latest else "NOT_YET_OBSERVED",
            "freshness": latest["freshness"] if latest else "UNKNOWN",
            "reason": latest["reason"] if latest else "shadow_observation_pending",
            "latest_observation": latest,
            "history": self.list_shadow_observations(
                execution_domain=str(execution_domain), execution_account_id=str(execution_account_id), limit=20,
            ),
        }

    def execution_read_model(self, *, limit: int = 50) -> dict[str, Any]:
        """Stable read model for the future control center; no mutable engine access."""
        execution_domain, execution_account_id = "SIMULATOR", "SIMULATOR:default"
        with self._connect() as connection:
            state_rows = connection.execute(
                """SELECT state, COUNT(*) AS count FROM phase_d_execution_intents
                   WHERE execution_domain=? AND execution_account_id=? GROUP BY state""",
                (execution_domain, execution_account_id),
            ).fetchall()
            intents = connection.execute(
                """SELECT intent_id, signal_id, target_wallet, campaign_id, symbol, action, direction,
                          requested_quantity, requested_capital, exposure_effect, state, accepted_at, updated_at,
                          execution_domain, execution_account_id
                   FROM phase_d_execution_intents WHERE execution_domain=? AND execution_account_id=?
                   ORDER BY updated_at DESC, intent_id DESC LIMIT ?""",
                (execution_domain, execution_account_id, limit),
            ).fetchall()
            submissions = connection.execute(
                """SELECT submission_id, intent_id, client_order_id, state, venue_order_id, requested_quantity,
                          filled_quantity, updated_at, execution_domain, execution_account_id
                   FROM phase_d_execution_submissions WHERE execution_domain=? AND execution_account_id=?
                   ORDER BY updated_at DESC, submission_id DESC LIMIT ?""",
                (execution_domain, execution_account_id, limit),
            ).fetchall()
            fills = connection.execute(
                """SELECT execution_fill_id, intent_id, submission_id, venue_fill_id, side, quantity, price, fee,
                          venue_timestamp, received_at, execution_domain, execution_account_id
                   FROM phase_d_execution_fills WHERE execution_domain=? AND execution_account_id=?
                   ORDER BY venue_timestamp DESC, execution_fill_id DESC LIMIT ?""",
                (execution_domain, execution_account_id, limit),
            ).fetchall()
            discrepancies = connection.execute(
                """SELECT item.* FROM phase_d_execution_reconciliation_items AS item
                   JOIN phase_d_execution_reconciliation_runs AS run
                     ON run.reconciliation_run_id=item.reconciliation_run_id
                   WHERE item.state NOT IN ('MATCHED', 'VERIFIED_FLAT')
                     AND run.execution_domain=? AND run.execution_account_id=?
                   ORDER BY item.recorded_at DESC LIMIT ?""",
                (execution_domain, execution_account_id, limit),
            ).fetchall()
            paper_counts = connection.execute(
                """SELECT COUNT(*) AS intents,
                          (SELECT COUNT(*) FROM phase_d_execution_fills
                           WHERE execution_domain='PAPER_COMPAT' AND execution_account_id='PAPER_COMPAT:legacy_paper') AS fills
                   FROM phase_d_execution_intents
                   WHERE execution_domain='PAPER_COMPAT' AND execution_account_id='PAPER_COMPAT:legacy_paper'"""
            ).fetchone()
        current_states = {str(row["state"]): int(row["count"]) for row in state_rows}
        position_reconciliation = self.latest_execution_reconciliation(
            scopes=("account_positions", "positions", "verified_flat"), execution_domain=execution_domain,
            execution_account_id=execution_account_id,
        )
        open_order_reconciliation = self.latest_execution_reconciliation(
            scopes=("open_orders",), execution_domain=execution_domain,
            execution_account_id=execution_account_id,
        )
        intent_reconciliation = self.latest_execution_reconciliation(
            scopes=("intent_order", "order"), execution_domain=execution_domain,
            execution_account_id=execution_account_id,
        )
        safety = self.execution_safety_health(
            execution_domain=execution_domain, execution_account_id=execution_account_id,
        )
        reconciliation_state = position_reconciliation["state"] if position_reconciliation else "NOT_YET_RUN"
        open_order_state = open_order_reconciliation["state"] if open_order_reconciliation else "NOT_YET_RUN"
        if safety["integrity_unhealthy"]:
            health_state = "INTEGRITY_FAILURE"
        elif safety["open_order_unhealthy"]:
            health_state = "OPEN_ORDER_RECONCILIATION_INCOMPLETE"
        elif reconciliation_state in {ReconciliationState.INCOMPLETE.value, ReconciliationState.RECONCILING.value}:
            health_state = "RECONCILIATION_INCOMPLETE"
        elif reconciliation_state == ReconciliationState.MISMATCH.value:
            health_state = "POSITION_MISMATCH"
        elif reconciliation_state == ReconciliationState.VERIFIED_FLAT.value:
            health_state = "VERIFIED_FLAT"
        else:
            health_state = "CONTINUOUS"
        entry_blocked = self.execution_has_unresolved_entry_risk(
            execution_domain=execution_domain, execution_account_id=execution_account_id,
        )
        return {
            "execution_mode": "SIMULATOR_ONLY",
            "live_order_transmission": False,
            "entry_enabled": False,
            "entry_inhibit": {"active": True, "reason": "phase_d_d0_simulator_only"},
            "hard_transport_stop": {"active": False, "reason": "no_live_transport_exists"},
            "adapter_state": "SIMULATOR_ONLY",
            "reconciliation": position_reconciliation,
            "reconciliation_authorities": {
                "account_positions": position_reconciliation,
                "position": position_reconciliation,
                "open_orders": open_order_reconciliation,
                "intent_order": intent_reconciliation,
            },
            "execution_health": {
                "state": health_state,
                "entry_inhibited": entry_blocked,
                "reason": safety["reasons"] or [reconciliation_state, open_order_state],
                "safety": safety,
            },
            "entry_blocked_by_unresolved_execution": entry_blocked,
            "state_counts": current_states,
            "unknown_submissions": current_states.get(ExecutionState.SUBMISSION_UNKNOWN.value, 0),
            "outstanding_orders": [dict(row) for row in submissions if row["state"] not in {"FILLED", "CANCELLED", "REJECTED", "EXPIRED"}],
            "partial_orders": [dict(row) for row in submissions if 0 < float(row["filled_quantity"]) < float(row["requested_quantity"])],
            "position_mismatches": [
                {**dict(row), "local": _load(row["local_json"], {}), "venue": _load(row["venue_json"], {})}
                for row in discrepancies
            ],
            "local_positions": self.phase_d_local_positions(
                execution_domain=execution_domain, execution_account_id=execution_account_id,
            ),
            "recent_intents": [dict(row) for row in intents],
            "recent_fills": [dict(row) for row in fills],
            "integrity_issues": self.list_execution_integrity_issues(
                execution_domain=execution_domain, execution_account_id=execution_account_id,
            )[:limit],
            "shadow": self.shadow_read_model(),
            "paper_compatibility_audit": {
                "execution_domain": "PAPER_COMPAT",
                "execution_account_id": "PAPER_COMPAT:legacy_paper",
                "intent_count": int(paper_counts["intents"]),
                "fill_count": int(paper_counts["fills"]),
            },
        }

    def dashboard_snapshot(self) -> dict[str, Any]:
        targets = self.list_targets()
        positions = self.list_virtual_positions(open_only=True)
        all_positions = self.list_virtual_positions()
        metrics = [self.latest_metrics(target.wallet) for target in targets]
        return {
            "targets": [jsonable(target) for target in targets],
            "scores": [jsonable(score) for score in self.latest_scores()],
            "positions": [jsonable(position) for position in positions],
            "all_positions": [jsonable(position) for position in all_positions],
            "metrics": [jsonable(metric) for metric in metrics if metric],
            "latest_fills": [jsonable(fill) for fill in self.list_raw_fills()[-20:]],
            "execution_attempts": self._recent_execution_attempts(),
            "execution_fills": self._recent_execution_fills(),
            "target_snapshots": self._latest_target_snapshots(),
            "portfolio_curve": self._portfolio_curve(),
            "backfill_coverage": self._backfill_coverage(),
        }

    def _recent_execution_attempts(self, limit: int = 20) -> list[dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM copy_execution_attempts ORDER BY decided_at DESC LIMIT ?", (limit,)
            ).fetchall()
        return [dict(row) for row in rows]

    def _recent_execution_fills(self, limit: int = 20) -> list[dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM copy_execution_fills ORDER BY timestamp DESC LIMIT ?", (limit,)
            ).fetchall()
        return [dict(row) for row in rows]

    def _latest_target_snapshots(self) -> list[dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute(
                """SELECT s.* FROM copy_trader_snapshots s JOIN (
                    SELECT target_wallet, MAX(snapshot_timestamp) AS snapshot_timestamp
                    FROM copy_trader_snapshots GROUP BY target_wallet
                ) newest ON newest.target_wallet=s.target_wallet AND newest.snapshot_timestamp=s.snapshot_timestamp"""
            ).fetchall()
        snapshots = []
        for row in rows:
            item = dict(row)
            item["positions"] = _load(item.pop("positions_json"), {})
            item.pop("raw_payload_json", None)
            snapshots.append(item)
        return snapshots

    def _portfolio_curve(self, limit: int = 200) -> list[dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM copy_portfolio_snapshots ORDER BY timestamp DESC LIMIT ?", (limit,)
            ).fetchall()
        return [dict(row) for row in reversed(rows)]

    def _backfill_coverage(self) -> list[dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute(
                """SELECT c.* FROM copy_backfill_coverage c JOIN (
                    SELECT target_wallet, MAX(requested_end) requested_end FROM copy_backfill_coverage GROUP BY target_wallet
                ) newest ON newest.target_wallet=c.target_wallet AND newest.requested_end=c.requested_end"""
            ).fetchall()
        return [dict(row) for row in rows]
