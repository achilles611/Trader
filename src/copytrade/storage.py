from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterable, Iterator, Protocol

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
                    config_fingerprint TEXT,
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
                    queued_for_analysis INTEGER NOT NULL DEFAULT 0, errors_json TEXT NOT NULL DEFAULT '[]'
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
                CREATE TABLE IF NOT EXISTS copy_candidate_analyses (
                    wallet TEXT PRIMARY KEY, lifecycle_status TEXT NOT NULL, last_run_id TEXT,
                    started_at TEXT, completed_at TEXT, prefilter_reasons_json TEXT NOT NULL DEFAULT '[]',
                    errors_json TEXT NOT NULL DEFAULT '[]', summary_json TEXT NOT NULL DEFAULT '{}'
                );
                CREATE INDEX IF NOT EXISTS idx_copy_candidate_analyses_state ON copy_candidate_analyses(lifecycle_status, completed_at DESC);
                CREATE INDEX IF NOT EXISTS idx_copy_candidate_analyses_run ON copy_candidate_analyses(last_run_id);
                """
            )
            self._ensure_column(connection, "copy_signals", "target_position_before", "REAL NOT NULL DEFAULT 0")
            self._ensure_column(connection, "copy_signals", "target_leverage", "REAL")
            self._ensure_column(connection, "copy_raw_fills", "source_closed_pnl", "REAL")
            self._ensure_column(connection, "copy_raw_fills", "is_liquidation", "INTEGER NOT NULL DEFAULT 0")
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
            self._ensure_column(connection, "copy_discovery_runs", "eligible_wallets", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column(connection, "copy_discovery_runs", "limit_deferred_wallets", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column(connection, "copy_discovery_observations", "source_event_id", "TEXT")
            connection.execute("CREATE INDEX IF NOT EXISTS idx_copy_targets_status ON copy_targets(status)")
            connection.execute("CREATE INDEX IF NOT EXISTS idx_copy_discovery_candidates_run ON copy_discovery_candidates(last_discovery_run_id)")
            connection.execute("CREATE INDEX IF NOT EXISTS idx_copy_discovery_observations_source_wallet ON copy_discovery_observations(source, wallet)")
            connection.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_copy_discovery_observations_event ON copy_discovery_observations(run_id, source_event_id)"
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
    def _ensure_column(connection: sqlite3.Connection, table: str, column: str, definition: str) -> None:
        present = {str(row["name"]) for row in connection.execute(f"PRAGMA table_info({table})").fetchall()}
        if column not in present:
            connection.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")

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
                filtered_wallets, queued_for_analysis, errors_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (run.run_id, iso(run.started_at), iso(run.finished_at) if run.finished_at else None, run.status,
                 _dump(run.sources), _dump(run.configuration), run.wallets_seen, run.eligible_wallets,
                 run.limit_deferred_wallets, run.new_wallets, run.existing_wallets_refreshed,
                 run.filtered_wallets, run.queued_for_analysis, _dump(run.errors)),
            )

    def finish_discovery_run(
        self, run_id: str, *, status: str, errors: tuple[str, ...] = (), wallets_seen: int = 0,
        eligible_wallets: int = 0, limit_deferred_wallets: int = 0, new_wallets: int = 0,
        existing_wallets_refreshed: int = 0, filtered_wallets: int = 0, queued_for_analysis: int = 0,
    ) -> None:
        with self._connect() as connection:
            connection.execute(
                """UPDATE copy_discovery_runs SET finished_at=?, status=?, wallets_seen=?, eligible_wallets=?,
                limit_deferred_wallets=?, new_wallets=?, existing_wallets_refreshed=?, filtered_wallets=?,
                queued_for_analysis=?, errors_json=? WHERE run_id=?""",
                (iso(None), status, wallets_seen, eligible_wallets, limit_deferred_wallets, new_wallets,
                 existing_wallets_refreshed, filtered_wallets, queued_for_analysis, _dump(errors), run_id),
            )

    def stage_discovery_observations(
        self, run_id: str, observations: Iterable[DiscoveryObservation], *, batch_size: int = 500,
    ) -> int:
        """Append de-duplicated run evidence in bounded transactions before candidate selection."""
        if batch_size <= 0:
            raise ValueError("Discovery observation batch size must be positive.")
        invalid_wallets = 0
        batch: list[tuple[Any, ...]] = []

        def flush() -> None:
            if not batch:
                return
            with self._connect() as connection:
                connection.executemany(
                    """INSERT OR IGNORE INTO copy_discovery_observations(observation_id, run_id, wallet, source, observed_at,
                    recent_activity_at, discovery_rank, source_score, metadata_json, raw_evidence_json, evidence_id, source_event_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    batch,
                )
            batch.clear()

        for observation in observations:
            wallet = observation.normalized_wallet()
            if not _is_wallet(wallet) or not observation.source:
                invalid_wallets += 1
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
        return invalid_wallets

    def discard_discovery_observations(self, run_id: str) -> None:
        with self._connect() as connection:
            connection.execute("DELETE FROM copy_discovery_observations WHERE run_id=?", (run_id,))

    def complete_discovery_run(
        self, run: DiscoveryRun, *, limit: int, min_activity: int, max_activity_age_seconds: float | None,
        invalid_wallets: int = 0,
    ) -> DiscoverySummary:
        """Aggregate staged evidence without retaining the raw input in process memory."""
        cutoff = run.started_at.timestamp() - max_activity_age_seconds if max_activity_age_seconds is not None else None
        with self._connect() as connection:
            rows = connection.execute(
                """SELECT wallet, COUNT(*) AS activity_count, MIN(observed_at) AS first_seen, MAX(observed_at) AS last_seen,
                MAX(recent_activity_at) AS recent_activity_at, MIN(discovery_rank) AS discovery_rank,
                MAX(source_score) AS source_score FROM copy_discovery_observations WHERE run_id=? GROUP BY wallet""",
                (run.run_id,),
            ).fetchall()
            aggregates = [dict(row) for row in rows]
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
            errors = (f"invalid_wallets_rejected:{invalid_wallets}",) if invalid_wallets else ()
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
                source_count = int(connection.execute(
                    "SELECT COUNT(DISTINCT source) FROM copy_discovery_observations WHERE wallet=?", (wallet,)
                ).fetchone()[0])
                metadata = {
                    "latest_sources": sorted(row["source"] for row in connection.execute(
                        "SELECT DISTINCT source FROM copy_discovery_observations WHERE run_id=? AND wallet=?", (run.run_id, wallet)
                    ).fetchall()),
                    "latest_activity_observations": int(aggregate["activity_count"]),
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
                """UPDATE copy_discovery_runs SET finished_at=?, status='completed', wallets_seen=?, eligible_wallets=?,
                limit_deferred_wallets=?, new_wallets=?, existing_wallets_refreshed=?, filtered_wallets=?,
                queued_for_analysis=?, errors_json=? WHERE run_id=?""",
                (iso(None), wallets_seen, eligible_wallets, limit_deferred_wallets, new_wallets,
                 existing_wallets_refreshed, filtered_wallets, queued_for_analysis, _dump(errors), run.run_id),
            )
        return DiscoverySummary(
            run_id=run.run_id, status="completed", sources=run.sources, wallets_seen=wallets_seen,
            eligible_wallets=eligible_wallets, limit_deferred_wallets=limit_deferred_wallets,
            new_wallets=new_wallets, existing_wallets_refreshed=existing_wallets_refreshed,
            filtered_wallets=filtered_wallets, queued_for_analysis=queued_for_analysis, errors=errors,
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
                   score.total_score, score.eligible AS score_eligible, score.reasons_json
                   FROM copy_discovery_candidates candidate
                   JOIN copy_targets target ON target.wallet=candidate.wallet
                   LEFT JOIN copy_candidate_analyses analysis ON analysis.wallet=candidate.wallet
                   LEFT JOIN copy_candidate_scores score ON score.target_wallet=candidate.wallet
                     AND score.calculated_at=(SELECT MAX(calculated_at) FROM copy_candidate_scores WHERE target_wallet=candidate.wallet)"""
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY score.total_score DESC NULLS LAST, candidate.recent_activity_at DESC, candidate.wallet LIMIT ?"
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
        with self._connect() as connection:
            cursor = connection.execute(
                """INSERT OR IGNORE INTO copy_raw_fills(
                    event_id, source, venue, chain_network, target_wallet, target_order_id, target_trade_id,
                    transaction_hash, symbol, side, direction, price, base_quantity, notional, fee, fee_token,
                    target_account_equity, target_position_before, event_timestamp, ingestion_timestamp,
                    confirmation, raw_payload_json, source_closed_pnl, is_liquidation) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (fill.event_id, fill.source, fill.venue, fill.chain_network, fill.target_wallet.lower(), fill.target_order_id,
                 fill.target_trade_id, fill.transaction_hash, fill.symbol, fill.side, fill.direction, fill.price,
                 fill.base_quantity, fill.notional, fill.fee, fill.fee_token, fill.target_account_equity,
                 fill.target_position_before, iso(fill.event_timestamp), iso(fill.ingestion_timestamp), fill.confirmation,
                 _dump(fill.raw_payload), fill.source_closed_pnl, int(fill.is_liquidation)),
            )
        return cursor.rowcount == 1

    def insert_raw_fills(self, fills: Iterable[RawFill]) -> int:
        return sum(1 for fill in fills if self.insert_raw_fill(fill))

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
            connection.execute(
                """INSERT OR REPLACE INTO copy_candidate_scores(target_wallet, calculated_at, total_score,
                component_scores_json, penalties_json, eligible, reasons_json, source_quality,
                provenance, analysis_run_id, config_fingerprint)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (score.target_wallet, iso(score.calculated_at), score.total_score, _dump(score.component_scores),
                 _dump(score.penalties), int(score.eligible), _dump(score.reasons), score.source_quality,
                 score.provenance, score.analysis_run_id, score.config_fingerprint),
            )

    def latest_scores(self) -> list[CandidateScore]:
        with self._connect() as connection:
            rows = connection.execute(
                """SELECT s.* FROM copy_candidate_scores s JOIN (
                       SELECT target_wallet, MAX(calculated_at) calculated_at FROM copy_candidate_scores GROUP BY target_wallet
                   ) newest ON newest.target_wallet=s.target_wallet AND newest.calculated_at=s.calculated_at
                   ORDER BY s.total_score DESC, s.target_wallet"""
            ).fetchall()
        return [
            CandidateScore(target_wallet=row["target_wallet"], calculated_at=as_utc(row["calculated_at"]),
                           total_score=float(row["total_score"]), component_scores=_load(row["component_scores_json"], {}),
                           penalties=_load(row["penalties_json"], {}), eligible=bool(row["eligible"]),
                           reasons=tuple(_load(row["reasons_json"], [])), source_quality=float(row["source_quality"]),
                           provenance=str(row["provenance"] or "legacy"), analysis_run_id=row["analysis_run_id"],
                           config_fingerprint=row["config_fingerprint"])
            for row in rows
        ]

    def phase_b_qualified_scores(self) -> list[CandidateScore]:
        """Current qualified analysis scores, immune to later legacy scoring."""
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
                   ORDER BY score.total_score DESC, score.target_wallet"""
            ).fetchall()
        return [
            CandidateScore(
                target_wallet=row["target_wallet"], calculated_at=as_utc(row["calculated_at"]), total_score=float(row["total_score"]),
                component_scores=_load(row["component_scores_json"], {}), penalties=_load(row["penalties_json"], {}),
                eligible=bool(row["eligible"]), reasons=tuple(_load(row["reasons_json"], [])),
                source_quality=float(row["source_quality"]), provenance=str(row["provenance"] or "legacy"),
                analysis_run_id=row["analysis_run_id"], config_fingerprint=row["config_fingerprint"],
            )
            for row in rows
        ]

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
    ) -> None:
        """Durably checkpoint marks; unlike execution, this creates no attempt."""
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
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
                snapshot_id = stable_id("portfolio_mark", iso(timestamp), snapshot["cash"], snapshot["equity"], snapshot["committed_capital"], snapshot["drawdown_fraction"])
                connection.execute(
                    """INSERT OR IGNORE INTO copy_portfolio_snapshots(snapshot_id, timestamp, cash, equity, committed_capital,
                    drawdown_fraction, peak_equity, max_drawdown_fraction) VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                    (snapshot_id, iso(timestamp), snapshot["cash"], snapshot["equity"], snapshot["committed_capital"],
                     snapshot["drawdown_fraction"], snapshot.get("peak_equity"), snapshot.get("max_drawdown_fraction", 0.0)),
                )
                connection.commit()
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
                for fill in fills:
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
                if fault_hook:
                    fault_hook("before_commit")
                connection.commit()
                return True
            except Exception:
                connection.rollback()
                raise

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
