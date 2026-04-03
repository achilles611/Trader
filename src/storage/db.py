from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from .models import AIAnalysisRecord, BotRunRecord, PatchAttemptRecord, RunCycleRecord


def _dump_json(payload: Any) -> str:
    return json.dumps(payload, sort_keys=True)


class SwarmDatabase:
    def __init__(self, path: Path) -> None:
        self.path = path

    def _connect(self) -> sqlite3.Connection:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.path)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=NORMAL")
        return connection

    def initialize(self) -> None:
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS run_cycles (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    cycle_id TEXT NOT NULL UNIQUE,
                    started_at TEXT NOT NULL,
                    expected_trigger_at TEXT NOT NULL,
                    actual_trigger_at TEXT NOT NULL,
                    finished_at TEXT,
                    git_sha TEXT,
                    status TEXT NOT NULL,
                    total_pnl REAL DEFAULT 0,
                    total_drawdown REAL DEFAULT 0,
                    total_trades INTEGER DEFAULT 0,
                    summary_artifact_path TEXT NOT NULL,
                    analysis_artifact_path TEXT NOT NULL DEFAULT '',
                    drift_seconds REAL DEFAULT 0,
                    duration_seconds REAL DEFAULT 0,
                    error_message TEXT
                );

                CREATE TABLE IF NOT EXISTS bot_runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    cycle_id INTEGER NOT NULL REFERENCES run_cycles(id) ON DELETE CASCADE,
                    bot_id TEXT NOT NULL,
                    profile_name TEXT NOT NULL,
                    config_hash TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    pnl REAL NOT NULL,
                    win_rate REAL NOT NULL,
                    drawdown REAL NOT NULL,
                    trade_count INTEGER NOT NULL,
                    avg_hold_sec REAL NOT NULL,
                    expectancy REAL NOT NULL,
                    sharpe_like REAL NOT NULL,
                    max_adverse_excursion REAL NOT NULL,
                    max_favorable_excursion REAL NOT NULL,
                    block_reason_counts TEXT NOT NULL,
                    artifact_path TEXT NOT NULL,
                    repo_sha TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    ended_at TEXT NOT NULL,
                    family TEXT NOT NULL,
                    signal_diagnostics TEXT NOT NULL,
                    genome TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS ai_analyses (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    cycle_id INTEGER NOT NULL REFERENCES run_cycles(id) ON DELETE CASCADE,
                    model TEXT NOT NULL,
                    prompt_cache_key TEXT NOT NULL,
                    request_tokens_est INTEGER NOT NULL,
                    response_tokens_est INTEGER NOT NULL,
                    json_result TEXT NOT NULL,
                    recommendation_grade TEXT NOT NULL,
                    patch_request_artifact_path TEXT NOT NULL,
                    response_id TEXT NOT NULL DEFAULT '',
                    request_id TEXT NOT NULL DEFAULT ''
                );

                CREATE TABLE IF NOT EXISTS patch_attempts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    cycle_id INTEGER NOT NULL REFERENCES run_cycles(id) ON DELETE CASCADE,
                    branch_name TEXT NOT NULL,
                    diff_artifact_path TEXT NOT NULL,
                    validation_status TEXT NOT NULL,
                    merged_to_develop INTEGER NOT NULL DEFAULT 0,
                    promoted_to_main INTEGER NOT NULL DEFAULT 0,
                    notes TEXT NOT NULL DEFAULT ''
                );

                CREATE INDEX IF NOT EXISTS idx_bot_runs_cycle_id ON bot_runs(cycle_id);
                CREATE INDEX IF NOT EXISTS idx_ai_analyses_cycle_id ON ai_analyses(cycle_id);
                CREATE INDEX IF NOT EXISTS idx_patch_attempts_cycle_id ON patch_attempts(cycle_id);
                """
            )

    def ping(self) -> bool:
        with self._connect() as connection:
            connection.execute("SELECT 1")
        return True

    def insert_cycle(self, record: RunCycleRecord) -> int:
        with self._connect() as connection:
            cursor = connection.execute(
                """
                INSERT INTO run_cycles (
                    cycle_id,
                    started_at,
                    expected_trigger_at,
                    actual_trigger_at,
                    finished_at,
                    git_sha,
                    status,
                    total_pnl,
                    total_drawdown,
                    total_trades,
                    summary_artifact_path,
                    analysis_artifact_path,
                    drift_seconds,
                    duration_seconds,
                    error_message
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record.cycle_id,
                    record.started_at,
                    record.expected_trigger_at,
                    record.actual_trigger_at,
                    record.finished_at,
                    record.git_sha,
                    record.status,
                    record.total_pnl,
                    record.total_drawdown,
                    record.total_trades,
                    record.summary_artifact_path,
                    record.analysis_artifact_path,
                    record.drift_seconds,
                    record.duration_seconds,
                    record.error_message,
                ),
            )
            return int(cursor.lastrowid)

    def update_cycle(self, cycle_db_id: int, record: RunCycleRecord) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE run_cycles
                SET
                    finished_at = ?,
                    git_sha = ?,
                    status = ?,
                    total_pnl = ?,
                    total_drawdown = ?,
                    total_trades = ?,
                    summary_artifact_path = ?,
                    analysis_artifact_path = ?,
                    drift_seconds = ?,
                    duration_seconds = ?,
                    error_message = ?
                WHERE id = ?
                """,
                (
                    record.finished_at,
                    record.git_sha,
                    record.status,
                    record.total_pnl,
                    record.total_drawdown,
                    record.total_trades,
                    record.summary_artifact_path,
                    record.analysis_artifact_path,
                    record.drift_seconds,
                    record.duration_seconds,
                    record.error_message,
                    cycle_db_id,
                ),
            )

    def insert_bot_runs(self, cycle_db_id: int, records: list[BotRunRecord]) -> None:
        with self._connect() as connection:
            connection.executemany(
                """
                INSERT INTO bot_runs (
                    cycle_id,
                    bot_id,
                    profile_name,
                    config_hash,
                    symbol,
                    pnl,
                    win_rate,
                    drawdown,
                    trade_count,
                    avg_hold_sec,
                    expectancy,
                    sharpe_like,
                    max_adverse_excursion,
                    max_favorable_excursion,
                    block_reason_counts,
                    artifact_path,
                    repo_sha,
                    started_at,
                    ended_at,
                    family,
                    signal_diagnostics,
                    genome
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        cycle_db_id,
                        record.bot_id,
                        record.profile_name,
                        record.config_hash,
                        record.symbol,
                        record.pnl,
                        record.win_rate,
                        record.drawdown,
                        record.trade_count,
                        record.avg_hold_sec,
                        record.expectancy,
                        record.sharpe_like,
                        record.max_adverse_excursion,
                        record.max_favorable_excursion,
                        _dump_json(record.block_reason_counts),
                        record.artifact_path,
                        record.repo_sha,
                        record.started_at,
                        record.ended_at,
                        record.family,
                        _dump_json(record.signal_diagnostics),
                        _dump_json(record.genome),
                    )
                    for record in records
                ],
            )

    def insert_ai_analysis(self, cycle_db_id: int, record: AIAnalysisRecord) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO ai_analyses (
                    cycle_id,
                    model,
                    prompt_cache_key,
                    request_tokens_est,
                    response_tokens_est,
                    json_result,
                    recommendation_grade,
                    patch_request_artifact_path,
                    response_id,
                    request_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    cycle_db_id,
                    record.model,
                    record.prompt_cache_key,
                    record.request_tokens_est,
                    record.response_tokens_est,
                    _dump_json(record.json_result),
                    record.recommendation_grade,
                    record.patch_request_artifact_path,
                    record.response_id,
                    record.request_id,
                ),
            )

    def insert_patch_attempt(self, cycle_db_id: int, record: PatchAttemptRecord) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO patch_attempts (
                    cycle_id,
                    branch_name,
                    diff_artifact_path,
                    validation_status,
                    merged_to_develop,
                    promoted_to_main,
                    notes
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    cycle_db_id,
                    record.branch_name,
                    record.diff_artifact_path,
                    record.validation_status,
                    int(record.merged_to_develop),
                    int(record.promoted_to_main),
                    record.notes,
                ),
            )

    def get_recent_cycles(self, limit: int = 5) -> list[dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT cycle_id, started_at, finished_at, git_sha, status, total_pnl, total_drawdown, total_trades
                FROM run_cycles
                WHERE finished_at IS NOT NULL
                ORDER BY finished_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [dict(row) for row in rows]

    def get_latest_bot_hashes(self) -> dict[str, str]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT br.bot_id, br.config_hash, rc.finished_at
                FROM bot_runs br
                JOIN run_cycles rc ON rc.id = br.cycle_id
                WHERE rc.status = 'completed'
                ORDER BY rc.finished_at DESC
                """
            ).fetchall()
        hashes: dict[str, str] = {}
        for row in rows:
            bot_id = str(row["bot_id"])
            if bot_id not in hashes:
                hashes[bot_id] = str(row["config_hash"])
        return hashes

    def latest_cycle(self) -> dict[str, Any] | None:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT cycle_id, started_at, finished_at, git_sha, status, total_pnl, total_drawdown, total_trades
                FROM run_cycles
                ORDER BY id DESC
                LIMIT 1
                """
            ).fetchone()
        return dict(row) if row is not None else None
