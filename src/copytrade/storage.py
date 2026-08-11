from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any, Iterable, Protocol

from .models import (
    BacktestRun,
    CandidateScore,
    CopySignal,
    ExecutionAttempt,
    ExecutionFill,
    PositionCampaign,
    PositionEvent,
    PositionEventType,
    RawFill,
    Target,
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


class CopyTradeDatabase:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)

    def _connect(self) -> sqlite3.Connection:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.path)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=NORMAL")
        return connection

    def initialize(self) -> None:
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS copy_targets (
                    wallet TEXT PRIMARY KEY, source TEXT NOT NULL, venue TEXT NOT NULL,
                    status TEXT NOT NULL, label TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL, metadata_json TEXT NOT NULL DEFAULT '{}'
                );
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
        return cursor.rowcount > 0

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
                component_scores_json, penalties_json, eligible, reasons_json, source_quality)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (score.target_wallet, iso(score.calculated_at), score.total_score, _dump(score.component_scores),
                 _dump(score.penalties), int(score.eligible), _dump(score.reasons), score.source_quality),
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
                           reasons=tuple(_load(row["reasons_json"], [])), source_quality=float(row["source_quality"]))
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
