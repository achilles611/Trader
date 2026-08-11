"""Phase C paper-only control-center API and durable operator controls.

This module deliberately consumes the persisted discovery/research/execution
tables.  It adds only operator state, control state, and an audit feed; it
does not alter Phase A discovery or Phase B scoring/reconstruction logic.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import sqlite3
from contextlib import contextmanager
from datetime import timedelta
from pathlib import Path
from typing import Any, Iterator

from .config import CopyTradeConfig
from .models import CopySignal, TargetStatus, as_utc, iso, jsonable, stable_id, utc_now
from .paper import PaperExecutionEngine
from .storage import CopyTradeDatabase


CONTROL_RUNNING = "RUNNING"
CONTROL_ENTRIES_PAUSED = "ENTRIES_PAUSED"
CONTROL_EXITING = "EXITING"
CONTROL_PAUSED = "PAUSED"
CONTROL_STATES = {CONTROL_RUNNING, CONTROL_ENTRIES_PAUSED, CONTROL_EXITING, CONTROL_PAUSED}
OPERATOR_STATES = {"new", "approved", "shadow", "active", "muted", "rejected"}


def _load(value: str | None, default: Any) -> Any:
    return json.loads(value) if value else default


def _dump(value: Any) -> str:
    return json.dumps(jsonable(value), sort_keys=True, separators=(",", ":"))


def _config_fingerprint(snapshot: dict[str, Any]) -> str:
    """Match Phase B's immutable configuration fingerprint without importing its pipeline at module load."""
    payload = json.dumps(snapshot, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


class ControlCenterStore:
    """Small additive schema for durable Phase C operator state and audit data."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.path)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA journal_mode=WAL")
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
                CREATE TABLE IF NOT EXISTS copy_control_center_state (
                    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                    state TEXT NOT NULL, updated_at TEXT NOT NULL,
                    updated_by TEXT NOT NULL DEFAULT 'operator',
                    note TEXT NOT NULL DEFAULT ''
                );
                INSERT OR IGNORE INTO copy_control_center_state(singleton, state, updated_at, updated_by, note)
                    VALUES (1, 'RUNNING', datetime('now'), 'system', 'paper entries enabled by default');
                CREATE TABLE IF NOT EXISTS copy_control_center_activity (
                    event_id TEXT PRIMARY KEY, occurred_at TEXT NOT NULL, category TEXT NOT NULL,
                    severity TEXT NOT NULL, wallet TEXT, symbol TEXT, message TEXT NOT NULL,
                    payload_json TEXT NOT NULL DEFAULT '{}'
                );
                CREATE INDEX IF NOT EXISTS idx_copy_control_activity_time
                    ON copy_control_center_activity(occurred_at DESC);
                CREATE INDEX IF NOT EXISTS idx_copy_control_activity_wallet
                    ON copy_control_center_activity(wallet, occurred_at DESC);
                """
            )

    def control_state(self) -> dict[str, Any]:
        self.initialize()
        with self._connect() as connection:
            row = connection.execute("SELECT state, updated_at, updated_by, note FROM copy_control_center_state WHERE singleton=1").fetchone()
        assert row is not None
        result = dict(row)
        result["entries_allowed"] = result["state"] == CONTROL_RUNNING
        result["paper_only"] = True
        return result

    def set_control_state(self, state: str, *, by: str = "operator", note: str = "") -> dict[str, Any]:
        if state not in CONTROL_STATES:
            raise ValueError(f"Unsupported paper control state: {state}")
        self.initialize()
        now = iso(None)
        with self._connect() as connection:
            connection.execute(
                "UPDATE copy_control_center_state SET state=?, updated_at=?, updated_by=?, note=? WHERE singleton=1",
                (state, now, by, note),
            )
        self.record_activity(
            category="control", severity="warning" if state != CONTROL_RUNNING else "info",
            message=f"Paper control state changed to {state}", payload={"state": state, "by": by, "note": note},
        )
        return self.control_state()

    def record_activity(
        self, *, category: str, severity: str, message: str, wallet: str | None = None,
        symbol: str | None = None, payload: dict[str, Any] | None = None, occurred_at: object | None = None,
    ) -> None:
        self.initialize()
        at = iso(occurred_at)
        event_id = stable_id("control_activity", at, category, severity, wallet or "", symbol or "", message, payload or {})
        with self._connect() as connection:
            connection.execute(
                """INSERT OR IGNORE INTO copy_control_center_activity(event_id, occurred_at, category, severity, wallet, symbol, message, payload_json)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (event_id, at, category, severity, wallet.lower() if wallet else None, symbol, message, _dump(payload or {})),
            )

    def activities(self, *, limit: int = 100, wallet: str | None = None) -> list[dict[str, Any]]:
        self.initialize()
        query = "SELECT * FROM copy_control_center_activity"
        values: list[Any] = []
        if wallet:
            query += " WHERE wallet=?"
            values.append(wallet.lower())
        query += " ORDER BY occurred_at DESC LIMIT ?"
        values.append(max(1, min(int(limit), 500)))
        with self._connect() as connection:
            rows = connection.execute(query, values).fetchall()
        return [{**dict(row), "payload": _load(row["payload_json"], {})} for row in rows]

    def entry_block_reason(self, wallet: str, action: str) -> str | None:
        """Return an auditable paper-entry gate reason; exits are never gated here."""
        if action not in {"open", "add"}:
            return None
        state = self.control_state()
        if not state["entries_allowed"]:
            return "paper_entries_paused"
        with self._connect() as connection:
            target = connection.execute("SELECT status FROM copy_targets WHERE wallet=?", (wallet.lower(),)).fetchone()
            active_count = int(connection.execute("SELECT COUNT(*) FROM copy_targets WHERE status='active'").fetchone()[0])
        status = str(target["status"]) if target else None
        if status == "muted":
            return "wallet_muted"
        # Retain compatibility for established paper users until an operator
        # deliberately builds an active cohort.  Once one exists, it is the
        # authoritative paper entry allow-list.
        if active_count and status != "active":
            return "not_active_paper_cohort"
        return None


class CopyControlCenter:
    """Read-model and command service for the Phase C control surface."""

    def __init__(self, config: CopyTradeConfig, database: CopyTradeDatabase | None = None) -> None:
        self.config = config
        self.database = database or CopyTradeDatabase(config.artifacts.database_path)
        self.database.initialize()
        self.store = ControlCenterStore(config.artifacts.database_path)
        self.store.initialize()

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        with self.store._connect() as connection:
            yield connection

    def health(self, watcher_health: dict[str, Any] | None = None) -> dict[str, Any]:
        now = utc_now()
        with self._connect() as connection:
            db_ok = bool(connection.execute("SELECT 1").fetchone())
            latest_discovery = connection.execute("SELECT * FROM copy_discovery_runs ORDER BY started_at DESC LIMIT 1").fetchone()
            latest_analysis = connection.execute("SELECT * FROM copy_analysis_runs ORDER BY started_at DESC LIMIT 1").fetchone()
            mark = connection.execute("SELECT MAX(updated_at) AS updated_at FROM copy_virtual_positions WHERE closed_at IS NULL").fetchone()
            fill = connection.execute("SELECT MAX(event_timestamp) AS event_timestamp FROM copy_raw_fills").fetchone()
        last_mark = mark["updated_at"] if mark and mark["updated_at"] else None
        mark_age_ms = (now - as_utc(last_mark)).total_seconds() * 1000 if last_mark else None
        return {
            "mode": self.config.mode,
            "paper_only": True,
            "database": {"connected": db_ok, "path": str(self.config.artifacts.database_path)},
            "watcher": watcher_health or {"state": "NOT_ATTACHED", "detail": "Control center is not running a watcher."},
            "market_data": {"last_mark_at": last_mark, "age_ms": mark_age_ms,
                            "fresh": bool(mark_age_ms is not None and mark_age_ms <= self.config.paper_execution.market_data_max_age_ms)},
            "source": {"last_public_fill_at": fill["event_timestamp"] if fill else None},
            "last_discovery_run": self._run_payload(latest_discovery),
            "last_phase_b_run": self._run_payload(latest_analysis),
            "websocket": {"available": True, "endpoint": "/ws"},
            "kill_switch": {"active": self.config.risk.kill_switch_path.exists(), "path": str(self.config.risk.kill_switch_path)},
            "control": self.store.control_state(),
            "timestamp": iso(now),
        }

    @staticmethod
    def _run_payload(row: sqlite3.Row | None) -> dict[str, Any] | None:
        if not row:
            return None
        value = dict(row)
        for key in list(value):
            if key.endswith("_json"):
                value[key[:-5]] = _load(value.pop(key), {} if key == "configuration_json" else [])
        return value

    def overview(self) -> dict[str, Any]:
        counts = self._counts()
        portfolio = self.portfolio_summary()
        return {
            "paper_only": True,
            "counts": counts,
            "funnel": self.funnel(),
            "portfolio": portfolio,
            "control": self.store.control_state(),
            "top_candidates": self.candidates(page_size=8, status="")['items'],
            "active_cohort": self.active_cohort(),
            "recent_activity": self.activity(limit=8),
        }

    def _counts(self) -> dict[str, int]:
        with self._connect() as connection:
            discovered = int(connection.execute("SELECT COUNT(*) FROM copy_discovery_candidates").fetchone()[0])
            statuses = {str(row["status"]): int(row["count"]) for row in connection.execute(
                "SELECT status, COUNT(*) AS count FROM copy_targets GROUP BY status"
            ).fetchall()}
            lifecycle = {str(row["state"]): int(row["count"]) for row in connection.execute(
                """SELECT COALESCE(lifecycle_status, 'new') AS state, COUNT(*) AS count
                   FROM copy_discovery_candidates candidate LEFT JOIN copy_candidate_analyses analysis ON analysis.wallet=candidate.wallet
                   GROUP BY COALESCE(lifecycle_status, 'new')"""
            ).fetchall()}
            stale = int(connection.execute(
                """SELECT COUNT(*) FROM copy_candidate_analyses analysis JOIN copy_candidate_scores score
                   ON score.target_wallet=analysis.wallet AND score.analysis_run_id=analysis.last_run_id AND score.provenance='phase_b'
                   WHERE analysis.lifecycle_status='qualified' AND score.config_fingerprint<>?""",
                (_config_fingerprint(self.config.snapshot()),),
            ).fetchone()[0])
            open_positions = int(connection.execute("SELECT COUNT(*) FROM copy_virtual_positions WHERE closed_at IS NULL").fetchone()[0])
        return {
            "total_discovered": discovered, "new": statuses.get("new", 0), "queued": statuses.get("queued", 0),
            "prefilter_rejected": lifecycle.get("prefilter_rejected", 0),
            "analyzed": lifecycle.get("analyzed", 0) + lifecycle.get("qualified", 0),
            "qualified": lifecycle.get("qualified", 0), "shadow": statuses.get("shadow", 0),
            "active": statuses.get("active", 0), "muted": statuses.get("muted", 0), "rejected": statuses.get("rejected", 0),
            "stale_analyses": stale, "open_paper_positions": open_positions,
        }

    def funnel(self) -> list[dict[str, Any]]:
        counts = self._counts()
        # Each value is a directly persisted-state count; unavailable stages are
        # intentionally absent rather than inferred from unrelated totals.
        return [
            {"key": "discovered", "label": "Discovered", "count": counts["total_discovered"], "filter": {}},
            {"key": "prefilter_rejected", "label": "Prefilter rejected", "count": counts["prefilter_rejected"], "filter": {"lifecycle": "prefilter_rejected"}},
            {"key": "analyzed", "label": "Analyzed", "count": counts["analyzed"], "filter": {"lifecycle": "analyzed"}},
            {"key": "qualified", "label": "Qualified", "count": counts["qualified"], "filter": {"lifecycle": "qualified"}},
            {"key": "shadow", "label": "Shadow finalists", "count": counts["shadow"], "filter": {"status": "shadow"}},
            {"key": "active", "label": "Active paper traders", "count": counts["active"], "filter": {"status": "active"}},
        ]

    def candidates(
        self, *, page: int = 1, page_size: int = 50, sort: str = "score", direction: str = "desc",
        search: str = "", status: str = "", lifecycle: str = "", min_score: float | None = None,
        max_score: float | None = None, min_win_rate: float | None = None, max_win_rate: float | None = None,
        min_profit_factor: float | None = None, max_profit_factor: float | None = None, max_drawdown: float | None = None,
        max_follower_drawdown: float | None = None, coverage: str = "", copyability_available: bool | None = None,
        recent_days: int | None = None, current_only: bool = False,
    ) -> dict[str, Any]:
        page = max(1, int(page))
        page_size = max(1, min(int(page_size), 200))
        sortable = {
            "score": "score.total_score", "wallet": "candidate.wallet", "last_active": "candidate.recent_activity_at",
            "campaigns": "json_extract(analysis.summary_json, '$.target_metrics.campaign_count')",
            "win_rate": "json_extract(analysis.summary_json, '$.target_metrics.win_rate')",
            "profit_factor": "json_extract(analysis.summary_json, '$.target_metrics.profit_factor')",
            "target_pnl": "json_extract(analysis.summary_json, '$.target_metrics.net_pnl')",
            "follower_pnl": "json_extract(analysis.summary_json, '$.follower.net_pnl')",
            "target_drawdown": "json_extract(analysis.summary_json, '$.target_metrics.max_drawdown')",
            "follower_drawdown": "json_extract(analysis.summary_json, '$.follower.max_drawdown')",
        }
        order = sortable.get(sort, sortable["score"])
        descending = "ASC" if direction.lower() == "asc" else "DESC"
        clauses: list[str] = []
        values: list[Any] = []
        if search:
            clauses.append("candidate.wallet LIKE ?")
            values.append(f"%{search.lower()}%")
        if status:
            clauses.append("target.status=?")
            values.append(status)
        if lifecycle:
            clauses.append("COALESCE(analysis.lifecycle_status, 'new')=?")
            values.append(lifecycle)
        if min_score is not None:
            clauses.append("COALESCE(score.total_score, -999999)>=?")
            values.append(float(min_score))
        if max_score is not None:
            clauses.append("COALESCE(score.total_score, 999999)<=?")
            values.append(float(max_score))
        if min_win_rate is not None:
            clauses.append("COALESCE(json_extract(analysis.summary_json, '$.target_metrics.win_rate'), -1)>=?")
            values.append(float(min_win_rate))
        if max_win_rate is not None:
            clauses.append("COALESCE(json_extract(analysis.summary_json, '$.target_metrics.win_rate'), 999999)<=?")
            values.append(float(max_win_rate))
        if min_profit_factor is not None:
            clauses.append("COALESCE(json_extract(analysis.summary_json, '$.target_metrics.profit_factor'), -1)>=?")
            values.append(float(min_profit_factor))
        if max_profit_factor is not None:
            clauses.append("COALESCE(json_extract(analysis.summary_json, '$.target_metrics.profit_factor'), 999999)<=?")
            values.append(float(max_profit_factor))
        if max_drawdown is not None:
            clauses.append("COALESCE(json_extract(analysis.summary_json, '$.target_metrics.max_drawdown'), 999999)<=?")
            values.append(float(max_drawdown))
        if max_follower_drawdown is not None:
            clauses.append("COALESCE(json_extract(analysis.summary_json, '$.follower.max_drawdown'), 999999)<=?")
            values.append(float(max_follower_drawdown))
        if coverage:
            clauses.append("COALESCE(json_extract(analysis.summary_json, '$.coverage.coverage_state'), 'UNPROVEN')=?")
            values.append(coverage)
        if copyability_available is not None:
            if copyability_available:
                clauses.append("COALESCE(json_extract(analysis.summary_json, '$.copyability.status'), '') NOT IN ('', 'unavailable')")
            else:
                clauses.append("COALESCE(json_extract(analysis.summary_json, '$.copyability.status'), 'unavailable') IN ('', 'unavailable')")
        if recent_days is not None:
            cutoff = iso(utc_now() - timedelta(days=max(0, int(recent_days))))
            clauses.append("candidate.recent_activity_at>=?")
            values.append(cutoff)
        fingerprint = _config_fingerprint(self.config.snapshot())
        if current_only:
            clauses.append("phase_score.config_fingerprint=?")
            values.append(fingerprint)
        base = """
            FROM copy_discovery_candidates candidate
            JOIN copy_targets target ON target.wallet=candidate.wallet
            LEFT JOIN copy_candidate_analyses analysis ON analysis.wallet=candidate.wallet
            LEFT JOIN copy_candidate_scores score ON score.target_wallet=candidate.wallet
              AND score.calculated_at=(SELECT MAX(calculated_at) FROM copy_candidate_scores WHERE target_wallet=candidate.wallet)
            LEFT JOIN copy_candidate_scores phase_score ON phase_score.target_wallet=candidate.wallet
              AND phase_score.analysis_run_id=analysis.last_run_id AND phase_score.provenance='phase_b'
              AND phase_score.rowid=(SELECT MAX(current_score.rowid) FROM copy_candidate_scores current_score
                WHERE current_score.target_wallet=candidate.wallet AND current_score.analysis_run_id=analysis.last_run_id
                AND current_score.provenance='phase_b')
        """
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        with self._connect() as connection:
            total = int(connection.execute("SELECT COUNT(*) " + base + where, values).fetchone()[0])
            rows = connection.execute(
                """SELECT candidate.wallet, candidate.discovered_at, candidate.last_seen_at, candidate.recent_activity_at,
                   candidate.source_count, target.label, target.status AS operator_state,
                   COALESCE(analysis.lifecycle_status, 'new') AS lifecycle_status, analysis.completed_at AS analysis_timestamp,
                   analysis.summary_json, analysis.prefilter_reasons_json, score.total_score, score.eligible,
                   score.component_scores_json, score.penalties_json, score.reasons_json, phase_score.config_fingerprint
                """ + base + where + f" ORDER BY {order} {descending} NULLS LAST, candidate.wallet ASC LIMIT ? OFFSET ?",
                [*values, page_size, (page - 1) * page_size],
            ).fetchall()
        items = [self._candidate_row(dict(row), fingerprint) for row in rows]
        return {"items": items, "page": page, "page_size": page_size, "total": total,
                "pages": max(1, (total + page_size - 1) // page_size), "current_config_fingerprint": fingerprint}

    def _candidate_row(self, row: dict[str, Any], fingerprint: str) -> dict[str, Any]:
        summary = _load(row.pop("summary_json", None), {})
        target = summary.get("target_metrics", {}) if isinstance(summary, dict) else {}
        follower = summary.get("follower", {}) if isinstance(summary, dict) else {}
        coverage = summary.get("coverage", {}) if isinstance(summary, dict) else {}
        copyability = summary.get("copyability", {}) if isinstance(summary, dict) else {}
        walk_forward = summary.get("walk_forward", {}) if isinstance(summary, dict) else {}
        score_fingerprint = row.pop("config_fingerprint", None)
        return {
            "wallet": row["wallet"], "label": row.get("label", ""), "operator_state": row["operator_state"],
            "research_state": row["lifecycle_status"], "score": row.get("total_score"), "qualified": bool(row.get("eligible")),
            "analysis_timestamp": row.get("analysis_timestamp"), "stale_analysis": bool(score_fingerprint and score_fingerprint != fingerprint),
            "last_active": row.get("recent_activity_at"), "history_days": target.get("history_days"),
            "campaigns": target.get("campaign_count"), "win_rate": target.get("win_rate"),
            "profit_factor": target.get("profit_factor"), "expectancy": target.get("expectancy"),
            "target_net_pnl": target.get("net_pnl"), "target_max_drawdown": target.get("max_drawdown"),
            "follower_net_pnl": follower.get("net_pnl"), "follower_max_drawdown": follower.get("max_drawdown"),
            "follower_expectancy": follower.get("expectancy"), "follower_profit_factor": follower.get("profit_factor"),
            "copyability": copyability.get("score", copyability.get("status")),
            "missed_trade_rate": follower.get("missed_trade_rate"), "slippage_robustness": summary.get("slippage", {}).get("robustness") if isinstance(summary.get("slippage"), dict) else None,
            "walk_forward": walk_forward.get("status"), "coverage": coverage.get("coverage_state", coverage.get("status", "UNPROVEN")),
            "concentration": target.get("pnl_concentration_best_five"), "liquidation_frequency": target.get("liquidation_frequency"),
            "recency_days": target.get("activity_recency_days"), "source_count": row.get("source_count", 0),
            "score_reasons": _load(row.get("reasons_json"), []),
            "prefilter_reasons": _load(row.get("prefilter_reasons_json"), []),
        }

    def candidate_detail(self, wallet: str) -> dict[str, Any] | None:
        rows = self.candidates(page=1, page_size=200, search=wallet, sort="wallet", direction="asc")["items"]
        row = next((item for item in rows if item["wallet"].lower() == wallet.lower()), None)
        if not row:
            return None
        target = self.database.get_target(wallet)
        analysis = self.database.get_candidate_analysis(wallet)
        latest_metrics = self.database.latest_metrics(wallet)
        latest_score = next((score for score in self.database.latest_scores() if score.target_wallet == wallet.lower()), None)
        summary = analysis.summary if analysis else {}
        return {
            "identity": {"wallet": wallet.lower(), "label": target.label if target else "", "operator_state": target.status if target else "new",
                         "research_state": analysis.lifecycle_status if analysis else "new", "first_discovered": row.get("discovered_at"),
                         "last_activity": row.get("last_active"), "analysis_timestamp": row.get("analysis_timestamp"),
                         "coverage": summary.get("coverage", {}), "source_count": row.get("source_count", 0)},
            "score": {"total": row.get("score"), "eligible": row.get("qualified"),
                      "reason_codes": list(latest_score.reasons) if latest_score else row.get("score_reasons", []),
                      "component_scores": latest_score.component_scores if latest_score else {},
                      "penalties": latest_score.penalties if latest_score else {}, "hard_gate_failures": row.get("prefilter_reasons", [])},
            "target_performance": summary.get("target_metrics", jsonable(latest_metrics) if latest_metrics else {}),
            "follower_performance": summary.get("follower", {}), "copyability": summary.get("copyability", {}),
            "slippage": summary.get("slippage", {}), "latency": summary.get("latency", {"status": "unavailable", "message": "Historical latency evidence unavailable"}),
            "walk_forward": summary.get("walk_forward", {}), "concentration": summary.get("concentration", {}),
            "analysis_window": summary.get("analysis_window", {}), "diversification": summary.get("diversification_input", {}),
            "table_row": row, "open_paper_positions": self.positions(wallet=wallet), "activity": self.activity(wallet=wallet, limit=50),
        }

    def shadow_finalists(self) -> list[dict[str, Any]]:
        # Deferred to avoid coupling the service's watcher path to Phase B's
        # orchestration module during import.
        from .analysis import CandidateAnalysisPipeline
        finalists = CandidateAnalysisPipeline(type("Service", (), {"config": self.config, "database": self.database})()).shadow_finalists()
        positions = {item["wallet"]: item for item in self._pnl_by_trader()}
        for item in finalists:
            target = self.database.get_target(str(item["wallet"]))
            item["operator_state"] = target.status if target else "new"
            item["paper_pnl"] = positions.get(str(item["wallet"]), {})
        return finalists

    def set_operator_state(self, wallet: str, state: str, *, by: str = "operator") -> dict[str, Any]:
        if state not in OPERATOR_STATES:
            raise ValueError("Operator state must be one of: " + ", ".join(sorted(OPERATOR_STATES)))
        target = self.database.get_target(wallet)
        if not target:
            raise KeyError("Wallet was not found in the candidate universe.")
        analysis = self.database.get_candidate_analysis(wallet)
        if state == "active" and target.status != "shadow" and (not analysis or analysis.lifecycle_status != "qualified"):
            raise ValueError("Only a qualified or shadow trader can enter the active PAPER cohort.")
        before = target.status
        if not self.database.set_target_status(wallet, state):
            raise KeyError("Wallet was not found in the candidate universe.")
        self.store.record_activity(category="operator", severity="info", wallet=wallet,
            message=f"Trader state changed from {before} to {state}", payload={"from": before, "to": state, "by": by})
        return {"wallet": wallet.lower(), "operator_state": state, "previous_state": before, "paper_only": True}

    def active_cohort(self) -> dict[str, Any]:
        active = [target for target in self.database.list_targets("active")]
        pnl = {row["wallet"]: row for row in self._pnl_by_trader()}
        members = []
        for target in active:
            detail = self.candidate_detail(target.wallet)
            row = detail["table_row"] if detail else {}
            members.append({"wallet": target.wallet, "label": target.label, "score": row.get("score"), "open_pnl": pnl.get(target.wallet, {}).get("open_pnl", 0.0),
                            "total_pnl": pnl.get(target.wallet, {}).get("total_pnl", 0.0), "drawdown": pnl.get(target.wallet, {}).get("max_drawdown", 0.0),
                            "allocation_policy": "Dynamic 5/10/20", "operator_state": "active", "research_state": row.get("research_state")})
        return {"target_size": "5–7", "count": len(members), "members": members, "paper_only": True}

    def positions(self, *, wallet: str | None = None, symbol: str | None = None, direction: str | None = None) -> list[dict[str, Any]]:
        positions = self.database.list_virtual_positions(open_only=True)
        now = utc_now()
        result = []
        for position in positions:
            if wallet and position.target_wallet != wallet.lower():
                continue
            if symbol and position.symbol.upper() != symbol.upper():
                continue
            if direction and position.direction.lower() != direction.lower():
                continue
            age = max(0.0, (now - position.opened_at).total_seconds())
            mark_age = max(0.0, (now - position.updated_at).total_seconds() * 1000)
            result.append({
                "paper": True, "sleeve_id": position.sleeve_id, "target_wallet": position.target_wallet, "symbol": position.symbol,
                "direction": position.direction, "quantity": position.quantity, "entry_price": position.entry_price, "current_mark": position.current_mark,
                "target_entry_price": position.target_entry_price, "allocated_capital": position.allocated_capital,
                "remaining_capital": position.remaining_capital, "allocation_bucket": self._bucket(position.allocated_capital),
                "unrealized_pnl": position.unrealized_pnl, "realized_pnl": position.realized_pnl, "fees": position.entry_fee + position.exit_fee,
                "opened_at": iso(position.opened_at), "age_seconds": age, "campaign_id": position.campaign_id,
                "mark_fresh": mark_age <= self.config.paper_execution.market_data_max_age_ms, "mark_age_ms": mark_age,
                "max_drawdown": position.max_drawdown,
            })
        return result

    def _bucket(self, capital: float) -> str:
        base = self.config.capital.initial_capital
        ratio = capital / max(base, 1e-9)
        candidates = [(0.05, "5%"), (0.10, "10%"), (0.20, "20%")]
        return min(candidates, key=lambda item: abs(item[0] - ratio))[1] if any(abs(ratio - x[0]) < 0.025 for x in candidates) else "fallback"

    def portfolio_summary(self) -> dict[str, Any]:
        snapshot = self.database.latest_portfolio_snapshot()
        open_positions = self.database.list_virtual_positions(open_only=True)
        all_positions = self.database.list_virtual_positions()
        committed = sum(position.remaining_capital for position in open_positions)
        open_pnl = sum(position.unrealized_pnl for position in open_positions)
        fees = sum(position.entry_fee + position.exit_fee for position in all_positions)
        realized_total = sum(position.realized_pnl - position.entry_fee for position in all_positions)
        today = utc_now().date()
        realized_today = sum(position.realized_pnl - position.entry_fee for position in all_positions if position.closed_at and position.closed_at.date() == today)
        equity = float(snapshot["equity"]) if snapshot else self.config.capital.initial_capital + realized_total + open_pnl
        cash = float(snapshot["cash"]) if snapshot else self.config.capital.initial_capital - committed + realized_total
        curve = self._portfolio_curve()
        current_dd = float(snapshot["drawdown_fraction"]) if snapshot else 0.0
        max_dd = max([float(point.get("drawdown_fraction") or 0.0) for point in curve] + [current_dd])
        return {
            "paper_only": True, "equity": equity, "cash": cash, "committed_capital": committed, "open_pnl": open_pnl,
            "realized_pnl_today": realized_today, "realized_pnl_total": realized_total, "fees": fees,
            "current_drawdown": current_dd, "max_drawdown": max_dd, "active_wallets": len({item.target_wallet for item in open_positions}),
            "open_positions": len(open_positions), "equity_curve": curve, "drawdown_curve": [{"timestamp": point["timestamp"], "value": point.get("drawdown_fraction", 0.0)} for point in curve],
            "pnl_by_trader": self._pnl_by_trader(), "pnl_by_symbol": self._pnl_by_symbol(), "pnl_by_bucket": self._pnl_by_bucket(),
        }

    def _portfolio_curve(self) -> list[dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute("SELECT * FROM copy_portfolio_snapshots ORDER BY timestamp DESC LIMIT 500").fetchall()
        return [dict(row) for row in reversed(rows)]

    def _pnl_by_trader(self) -> list[dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute(
                """SELECT target_wallet, SUM(CASE WHEN closed_at IS NULL THEN unrealized_pnl ELSE 0 END) AS open_pnl,
                   SUM(realized_pnl-entry_fee) AS realized_pnl, SUM(entry_fee+exit_fee) AS fees,
                   SUM(CASE WHEN closed_at IS NULL THEN remaining_capital ELSE 0 END) AS capital_usage,
                   MAX(max_drawdown) AS max_drawdown FROM copy_virtual_positions GROUP BY target_wallet ORDER BY (SUM(realized_pnl-entry_fee)+SUM(CASE WHEN closed_at IS NULL THEN unrealized_pnl ELSE 0 END)) DESC"""
            ).fetchall()
        return [{**dict(row), "total_pnl": float(row["open_pnl"] or 0) + float(row["realized_pnl"] or 0)} for row in rows]

    def _pnl_by_symbol(self) -> list[dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute(
                """SELECT symbol, SUM(CASE WHEN closed_at IS NULL THEN unrealized_pnl ELSE 0 END) AS open_pnl,
                   SUM(realized_pnl-entry_fee) AS realized_pnl, SUM(CASE WHEN closed_at IS NULL THEN remaining_capital ELSE 0 END) AS exposure,
                   SUM(CASE WHEN closed_at IS NULL THEN 1 ELSE 0 END) AS position_count FROM copy_virtual_positions GROUP BY symbol ORDER BY exposure DESC"""
            ).fetchall()
        return [{**dict(row), "total_pnl": float(row["open_pnl"] or 0) + float(row["realized_pnl"] or 0)} for row in rows]

    def _pnl_by_bucket(self) -> list[dict[str, Any]]:
        values: dict[str, dict[str, float]] = {}
        for position in self.database.list_virtual_positions():
            bucket = self._bucket(position.allocated_capital)
            row = values.setdefault(bucket, {"bucket": bucket, "open_pnl": 0.0, "realized_pnl": 0.0, "capital_usage": 0.0, "position_count": 0.0})
            row["position_count"] += 1
            row["realized_pnl"] += position.realized_pnl - position.entry_fee
            if position.is_open:
                row["open_pnl"] += position.unrealized_pnl
                row["capital_usage"] += position.remaining_capital
        return [{**item, "total_pnl": item["open_pnl"] + item["realized_pnl"]} for item in values.values()]

    def risk_panel(self) -> dict[str, Any]:
        portfolio = self.portfolio_summary()
        capital = max(self.config.capital.initial_capital, 1e-12)
        symbol = portfolio["pnl_by_symbol"]
        top_symbol = max([float(row.get("exposure") or 0.0) for row in symbol], default=0.0)
        return {"paper_only": True, "kill_switch": self.config.risk.kill_switch_path.exists(), "entry_control": self.store.control_state(),
                "limits": [
                    {"label": "Capital committed", "current": portfolio["committed_capital"] / capital, "limit": self.config.risk.max_total_committed_fraction},
                    {"label": "Largest symbol concentration", "current": top_symbol / capital, "limit": self.config.risk.max_capital_per_symbol_fraction},
                    {"label": "Portfolio drawdown", "current": portfolio["current_drawdown"], "limit": self.config.risk.max_copy_drawdown_fraction},
                    {"label": "Daily realized loss", "current": max(0.0, -portfolio["realized_pnl_today"]) / capital, "limit": self.config.risk.daily_loss_stop_fraction},
                ]}

    def activity(self, *, limit: int = 100, wallet: str | None = None) -> list[dict[str, Any]]:
        manual = self.store.activities(limit=limit, wallet=wallet)
        with self._connect() as connection:
            filters, values = [], []
            if wallet:
                filters.append("target_wallet=?")
                values.append(wallet.lower())
            where = " WHERE " + " AND ".join(filters) if filters else ""
            attempts = connection.execute(
                "SELECT * FROM copy_execution_attempts" + where + " ORDER BY decided_at DESC LIMIT ?", [*values, limit]
            ).fetchall()
        generated = []
        for attempt in attempts:
            item = dict(attempt)
            verb = "filled" if item["status"] == "filled" else item["status"]
            generated.append({"event_id": item["attempt_id"], "occurred_at": item["decided_at"], "category": "execution",
                              "severity": "info" if item["status"] == "filled" else "warning", "wallet": item["target_wallet"], "symbol": item["symbol"],
                              "message": f"Paper {item['action']} {verb}: {item['symbol']}", "payload": {"reason": item["reason"], "paper": True}})
        return sorted([*manual, *generated], key=lambda item: str(item["occurred_at"]), reverse=True)[:limit]

    def pause_entries(self) -> dict[str, Any]:
        return self.store.set_control_state(CONTROL_ENTRIES_PAUSED, note="New PAPER entries paused; exits remain enabled.")

    def resume_entries(self) -> dict[str, Any]:
        return self.store.set_control_state(CONTROL_RUNNING, note="New PAPER entries resumed.")

    def close_all_paper_positions(self, *, pause_after: bool = False) -> dict[str, Any]:
        """Flatten only fresh-mark paper sleeves through the existing paper engine."""
        prior_state = self.store.control_state()["state"]
        self.store.set_control_state(CONTROL_EXITING, note="Flattening open PAPER positions.")
        engine = PaperExecutionEngine(self.config, self.database)
        engine.restore(self.database.list_virtual_positions(), self.database.latest_portfolio_snapshot(), self.database.list_realized_results())
        now = utc_now()
        groups: dict[tuple[str, str], list[Any]] = {}
        for sleeve in engine.portfolio.sleeves.values():
            if sleeve.is_open:
                groups.setdefault((sleeve.target_wallet, sleeve.symbol), []).append(sleeve)
        closed, skipped = [], []
        for (wallet, symbol), sleeves in groups.items():
            mark = next((item.current_mark for item in sleeves if item.current_mark and (now - item.updated_at).total_seconds() * 1000 <= self.config.paper_execution.market_data_max_age_ms), None)
            if not mark:
                skipped.append({"wallet": wallet, "symbol": symbol, "reason": "no_fresh_market_reference"})
                self.store.record_activity(category="control", severity="warning", wallet=wallet, symbol=symbol,
                    message="Could not close PAPER position: no fresh market reference", payload={"paper": True})
                continue
            signal = CopySignal(
                signal_id=stable_id("manual_paper_close", wallet, symbol, now), target_wallet=wallet, campaign_id=None,
                source_event_id=stable_id("manual_close_source", wallet, symbol, now), symbol=symbol, action="close",
                direction=sleeves[0].direction, target_price=float(mark), target_quantity=sum(item.quantity for item in sleeves),
                target_notional=sum(item.quantity for item in sleeves) * float(mark), allocation_fraction=0.0, requested_capital=0.0,
                created_at=now, source_event_timestamp=now, reason="manual_close_all_paper_positions",
            )
            attempt = engine.process_signal(signal, received_at=now, market_price=float(mark), market_metadata={"market_reference_source": "persisted_live_mark", "paper_control": "close_all"})
            closed.append({"wallet": wallet, "symbol": symbol, "status": attempt.status, "reason": attempt.reason})
            self.store.record_activity(category="control", severity="warning", wallet=wallet, symbol=symbol,
                message=f"Close-all PAPER action {attempt.status} for {symbol}", payload={"reason": attempt.reason, "paper": True})
        final = CONTROL_PAUSED if pause_after else str(prior_state)
        note = "Exit + pause completed." if pause_after else "Close-all PAPER positions completed; entry state retained." if not skipped else "Close-all finished with stale-reference skips."
        # A close-all must not silently change entry semantics. Restore its prior
        # state unless the action explicitly requested a pause.
        return {"closed": closed, "skipped": skipped, "control": self.store.set_control_state(final, note=note), "paper_only": True}

    def exit_and_pause(self) -> dict[str, Any]:
        return self.close_all_paper_positions(pause_after=True)


def create_control_center_app(config: CopyTradeConfig, database: CopyTradeDatabase | None = None, watcher_health: dict[str, Any] | None = None) -> Any:
    """Create the local FastAPI Phase C application; no live-trading routes exist."""
    try:
        from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
        from fastapi.responses import FileResponse, HTMLResponse
        from fastapi.staticfiles import StaticFiles
    except ImportError as exc:  # pragma: no cover - dependency guidance
        raise RuntimeError("copy-control-center requires fastapi and uvicorn; install requirements.txt.") from exc

    center = CopyControlCenter(config, database)
    app = FastAPI(title="Trader Copy Control Center", version="1.0", docs_url=None, redoc_url=None)

    def required_wallet(wallet: str) -> str:
        if not wallet.startswith("0x") or len(wallet) != 42:
            raise HTTPException(status_code=400, detail="Invalid wallet address.")
        return wallet.lower()

    @app.get("/api/health")
    async def api_health() -> dict[str, Any]:
        return center.health(watcher_health)

    @app.get("/api/overview")
    async def api_overview() -> dict[str, Any]:
        return center.overview()

    @app.get("/api/candidates")
    async def api_candidates(
        page: int = 1, page_size: int = 50, sort: str = "score", direction: str = "desc", search: str = "",
        status: str = "", lifecycle: str = "", min_score: float | None = None, max_score: float | None = None,
        min_win_rate: float | None = None, max_win_rate: float | None = None, min_profit_factor: float | None = None,
        max_profit_factor: float | None = None, max_drawdown: float | None = None, max_follower_drawdown: float | None = None,
        coverage: str = "", copyability_available: bool | None = None, recent_days: int | None = None, current_only: bool = False,
    ) -> dict[str, Any]:
        return center.candidates(page=page, page_size=page_size, sort=sort, direction=direction, search=search, status=status,
                                 lifecycle=lifecycle, min_score=min_score, max_score=max_score, min_win_rate=min_win_rate,
                                 max_win_rate=max_win_rate, min_profit_factor=min_profit_factor, max_profit_factor=max_profit_factor,
                                 max_drawdown=max_drawdown, max_follower_drawdown=max_follower_drawdown, coverage=coverage,
                                 copyability_available=copyability_available, recent_days=recent_days, current_only=current_only)

    @app.get("/api/candidates/{wallet}")
    async def api_candidate(wallet: str) -> dict[str, Any]:
        detail = center.candidate_detail(required_wallet(wallet))
        if not detail:
            raise HTTPException(status_code=404, detail="Candidate not found.")
        return detail

    @app.post("/api/candidates/{wallet}/operator-state")
    async def api_operator_state(wallet: str, body: dict[str, Any]) -> dict[str, Any]:
        try:
            return center.set_operator_state(required_wallet(wallet), str(body.get("state", "")))
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    @app.get("/api/shadow-finalists")
    async def api_shadow_finalists() -> dict[str, Any]:
        return {"items": center.shadow_finalists(), "paper_only": True}

    @app.get("/api/active-cohort")
    async def api_active() -> dict[str, Any]:
        return center.active_cohort()

    @app.get("/api/portfolio")
    async def api_portfolio() -> dict[str, Any]:
        return center.portfolio_summary()

    @app.get("/api/positions")
    async def api_positions(wallet: str | None = None, symbol: str | None = None, direction: str | None = None) -> dict[str, Any]:
        return {"items": center.positions(wallet=wallet, symbol=symbol, direction=direction), "paper_only": True}

    @app.get("/api/activity")
    async def api_activity(limit: int = Query(100, ge=1, le=500), wallet: str | None = None) -> dict[str, Any]:
        return {"items": center.activity(limit=limit, wallet=wallet)}

    @app.get("/api/system")
    async def api_system() -> dict[str, Any]:
        return {"health": center.health(watcher_health), "risk": center.risk_panel(), "paper_only": True}

    @app.get("/api/controls")
    async def api_controls() -> dict[str, Any]:
        return center.store.control_state()

    @app.post("/api/controls/pause-entries")
    async def api_pause_entries() -> dict[str, Any]:
        return center.pause_entries()

    @app.post("/api/controls/resume-entries")
    async def api_resume_entries() -> dict[str, Any]:
        return center.resume_entries()

    @app.post("/api/controls/close-all-paper-positions")
    async def api_close_all() -> dict[str, Any]:
        return center.close_all_paper_positions()

    @app.post("/api/controls/exit-and-pause")
    async def api_exit_pause() -> dict[str, Any]:
        return center.exit_and_pause()

    @app.websocket("/ws")
    async def ws_updates(websocket: WebSocket) -> None:
        await websocket.accept()
        try:
            previous: dict[str, str] = {}
            while True:
                events = {
                    "control_state": center.store.control_state(),
                    "portfolio_update": center.portfolio_summary(),
                    "position_update": {"items": center.positions(), "paper_only": True},
                    "watcher_health": center.health(watcher_health)["watcher"],
                }
                for name, payload in events.items():
                    signature = _dump(payload)
                    if previous.get(name) != signature:
                        await websocket.send_json({"type": name, "data": payload, "paper_only": True})
                        previous[name] = signature
                await asyncio.sleep(1)
        except WebSocketDisconnect:
            return

    frontend_dist = Path(__file__).resolve().parents[2] / "control-center-ui" / "dist"
    if frontend_dist.exists():
        assets = frontend_dist / "assets"
        if assets.exists():
            app.mount("/assets", StaticFiles(directory=assets), name="assets")

        @app.get("/{path:path}", response_class=FileResponse)
        async def frontend(path: str) -> Any:
            requested = frontend_dist / path if path and (frontend_dist / path).is_file() else frontend_dist / "index.html"
            return FileResponse(requested)
    else:
        @app.get("/", response_class=HTMLResponse)
        async def frontend_missing() -> str:
            return "<h1>Copy Control Center</h1><p>Frontend build not found. Run <code>npm run build</code> in control-center-ui.</p>"

    return app


def serve_control_center(config: CopyTradeConfig, database: CopyTradeDatabase | None = None, *, host: str | None = None, port: int | None = None) -> None:
    try:
        import uvicorn
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("copy-control-center requires uvicorn; install requirements.txt.") from exc
    uvicorn.run(create_control_center_app(config, database), host=host or config.artifacts.dashboard_host, port=port or config.artifacts.dashboard_port)
