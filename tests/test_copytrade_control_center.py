from __future__ import annotations

import tempfile
import unittest
import asyncio
from datetime import timedelta
from pathlib import Path

from src.copytrade.config import ArtifactConfig, CopyTradeConfig, PaperExecutionConfig, RiskConfig
from src.copytrade.control_center import (
    CONTROL_ENTRIES_PAUSED,
    CONTROL_RUNNING,
    CopyControlCenter,
    create_control_center_app,
)
from src.copytrade.models import (
    CandidateAnalysis,
    CandidateScore,
    CopySignal,
    DiscoveryObservation,
    DiscoveryRun,
    utc_now,
    RawFill,
    Target,
)
from src.copytrade.paper import PaperExecutionEngine
from src.copytrade.service import CopyTradeService


WALLET_A = "0x1111111111111111111111111111111111111111"
WALLET_B = "0x2222222222222222222222222222222222222222"


def config(root: Path) -> CopyTradeConfig:
    return CopyTradeConfig(
        artifacts=ArtifactConfig(database_path=root / "copy.sqlite3", obsidian_root=root / "obsidian"),
        paper_execution=PaperExecutionConfig(fee_rate=0.0, slippage_bps=0.0, min_order_notional=1.0, market_data_max_age_ms=60_000),
        risk=RiskConfig(kill_switch_path=root / "kill.txt", max_signal_age_seconds=60_000),
    )


def seed_candidate(service: CopyTradeService, wallet: str, *, score: float, status: str = "new") -> None:
    now = utc_now()
    run = DiscoveryRun(run_id=f"discover_{wallet[-2:]}", started_at=now, sources=("fixture",), configuration={})
    service.database.start_discovery_run(run)
    service.database.stage_discovery_observations(run.run_id, [DiscoveryObservation(wallet=wallet, source="fixture", observed_at=now, recent_activity_at=now)])
    service.database.complete_discovery_run(run, limit=10, min_activity=1, max_activity_age_seconds=None)
    service.database.set_target_status(wallet, status)
    summary = {
        "target_metrics": {"history_days": 180, "campaign_count": 120, "net_pnl": score, "win_rate": 0.62, "profit_factor": 1.8, "max_drawdown": 0.08},
        "follower": {"net_pnl": score * 0.8, "expectancy": 1.2, "profit_factor": 1.5, "max_drawdown": 0.1, "missed_trade_rate": 0.02},
        "coverage": {"coverage_state": "PROVEN_COMPLETE"},
        "copyability": {"status": "available", "score": 0.88},
        "latency": {"status": "unavailable"},
        "analysis_window": {"required_start": (now - timedelta(days=180)).isoformat(), "required_end": now.isoformat(), "boundary_policy": "strict"},
        "diversification_input": {"daily_return_series": {}, "symbols": ["BTC"], "directions": ["long"]},
    }
    service.database.upsert_candidate_analysis(CandidateAnalysis(wallet=wallet, lifecycle_status="qualified", last_run_id="phase_b_fixture", completed_at=now, summary=summary))
    from src.copytrade.control_center import _config_fingerprint
    service.database.upsert_candidate_score(CandidateScore(wallet, now, score, {"consistency": 9.0}, {}, True, ("fixture",), provenance="phase_b", analysis_run_id="phase_b_fixture", config_fingerprint=_config_fingerprint(service.config.snapshot())))


def open_signal(wallet: str, action: str = "open") -> CopySignal:
    now = utc_now()
    return CopySignal(
        signal_id=f"signal_{wallet[-4:]}_{action}_{int(now.timestamp() * 1000000)}", target_wallet=wallet,
        campaign_id="campaign", source_event_id=f"event_{wallet[-4:]}_{action}", symbol="BTC", action=action,
        direction="long", target_price=100.0, target_quantity=1.0, target_notional=100.0,
        allocation_fraction=0.1, requested_capital=10.0, created_at=now, source_event_timestamp=now,
    )


class CopyControlCenterTests(unittest.TestCase):
    def test_candidate_filters_sorting_pagination_and_detail(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            service = CopyTradeService(config(Path(temp)))
            seed_candidate(service, WALLET_A, score=88.0, status="shadow")
            seed_candidate(service, WALLET_B, score=72.0, status="new")
            center = CopyControlCenter(service.config, service.database)

            page = center.candidates(page=1, page_size=1, sort="score", direction="desc", lifecycle="qualified")
            self.assertEqual((page["total"], len(page["items"])), (2, 1))
            self.assertEqual(page["items"][0]["wallet"], WALLET_A)
            self.assertEqual(center.candidates(status="shadow")["items"][0]["wallet"], WALLET_A)
            detail = center.candidate_detail(WALLET_A)
            self.assertIsNotNone(detail)
            self.assertEqual(detail["identity"]["research_state"], "qualified")  # type: ignore[index]
            self.assertEqual(detail["latency"]["status"], "unavailable")  # type: ignore[index]

    def test_operator_state_and_control_state_are_durable_and_pause_preserves_exits(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            service = CopyTradeService(config(Path(temp)))
            seed_candidate(service, WALLET_A, score=88.0)
            center = CopyControlCenter(service.config, service.database)
            self.assertEqual(center.set_operator_state(WALLET_A, "active")["operator_state"], "active")
            self.assertEqual(CopyControlCenter(service.config).active_cohort()["count"], 1)
            paused = center.pause_entries()
            self.assertEqual(paused["state"], CONTROL_ENTRIES_PAUSED)
            self.assertEqual(center.store.entry_block_reason(WALLET_A, "open"), "paper_entries_paused")
            self.assertIsNone(center.store.entry_block_reason(WALLET_A, "reduce"))
            self.assertEqual(center.resume_entries()["state"], CONTROL_RUNNING)
            center.set_operator_state(WALLET_A, "muted")
            self.assertEqual(center.store.entry_block_reason(WALLET_A, "open"), "wallet_muted")
            self.assertIsNone(center.store.entry_block_reason(WALLET_A, "close"))

    def test_close_all_affects_only_fresh_paper_sleeves_and_exit_pause_persists(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            service = CopyTradeService(config(Path(temp)))
            engine = PaperExecutionEngine(service.config, service.database)
            self.assertEqual(engine.process_signal(open_signal(WALLET_A), market_price=100.0).status, "filled")
            engine.mark_to_market("BTC", 101.0, utc_now())
            engine.persist_mark(utc_now())
            center = CopyControlCenter(service.config, service.database)
            result = center.close_all_paper_positions()
            self.assertEqual(len(result["closed"]), 1)
            self.assertFalse(service.database.list_virtual_positions(open_only=True))
            self.assertEqual(center.store.control_state()["state"], CONTROL_RUNNING)

            engine = PaperExecutionEngine(service.config, service.database)
            engine.restore(service.database.list_virtual_positions(), service.database.latest_portfolio_snapshot(), service.database.list_realized_results())
            self.assertEqual(engine.process_signal(open_signal(WALLET_B), market_price=100.0).status, "filled")
            engine.mark_to_market("BTC", 99.0, utc_now())
            engine.persist_mark(utc_now())
            result = center.exit_and_pause()
            self.assertEqual(result["control"]["state"], "PAUSED")
            self.assertFalse(service.database.list_virtual_positions(open_only=True))

    def test_persisted_pause_blocks_open_but_not_close_and_resume_allows_open(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            service = CopyTradeService(config(Path(temp)))
            service.database.upsert_target(Target(wallet=WALLET_A))
            service.database.upsert_target(Target(wallet=WALLET_B))
            center = CopyControlCenter(service.config, service.database)
            now_ms = int(utc_now().timestamp() * 1000)
            asyncio.run(service.ingest_market_update({"mids": {"BTC": "100"}}))
            center.pause_entries()
            blocked = RawFill.from_hyperliquid({"coin": "BTC", "px": "100", "sz": "1", "side": "B", "dir": "Open Long", "time": now_ms, "startPosition": "0", "tid": 1}, WALLET_A)
            asyncio.run(service.ingest_watched_fills(WALLET_A, [blocked], False))
            self.assertEqual(service.database.dashboard_snapshot()["execution_attempts"][0]["reason"], "paper_entries_paused")

            center.resume_entries()
            opened = RawFill.from_hyperliquid({"coin": "BTC", "px": "100", "sz": "1", "side": "B", "dir": "Open Long", "time": now_ms + 1, "startPosition": "0", "tid": 2}, WALLET_B)
            asyncio.run(service.ingest_watched_fills(WALLET_B, [opened], False))
            self.assertTrue(service.database.list_virtual_positions(open_only=True))

            center.pause_entries()
            closed = RawFill.from_hyperliquid({"coin": "BTC", "px": "101", "sz": "1", "side": "A", "dir": "Close Long", "time": now_ms + 2, "startPosition": "1", "tid": 3}, WALLET_B)
            asyncio.run(service.ingest_watched_fills(WALLET_B, [closed], False))
            attempts = service.database.dashboard_snapshot()["execution_attempts"]
            self.assertTrue(any(item["target_wallet"] == WALLET_B and item["action"] == "close" and item["status"] == "filled" for item in attempts))
            self.assertEqual(center.store.control_state()["state"], CONTROL_ENTRIES_PAUSED)

    def test_api_surface_is_paper_only_and_exposes_required_routes(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            app = create_control_center_app(config(Path(temp)))
            paths = {route.path for route in app.routes}
            self.assertTrue({"/api/health", "/api/overview", "/api/candidates", "/api/shadow-finalists", "/api/portfolio", "/api/positions", "/api/activity", "/api/system", "/ws"}.issubset(paths))
            banned = ("private", "secret", "credential", "live", "order", "key")
            self.assertFalse(any(any(word in path.lower() for word in banned) for path in paths))
            self.assertTrue(all("paper" in path or not path.startswith("/api/controls/") or path.endswith(("pause-entries", "resume-entries", "exit-and-pause")) for path in paths))
