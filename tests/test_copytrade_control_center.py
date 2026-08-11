from __future__ import annotations

import tempfile
import unittest
import asyncio
from dataclasses import replace
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
    AnalysisRun,
    CopySignal,
    DiscoveryObservation,
    DiscoveryRun,
    utc_now,
    iso,
    RawFill,
    Target,
)
from src.copytrade.paper import PaperExecutionEngine
from src.copytrade.service import CopyTradeService
from fastapi import HTTPException


WALLET_A = "0x1111111111111111111111111111111111111111"
WALLET_B = "0x2222222222222222222222222222222222222222"


def config(root: Path) -> CopyTradeConfig:
    return CopyTradeConfig(
        artifacts=ArtifactConfig(database_path=root / "copy.sqlite3", obsidian_root=root / "obsidian"),
        paper_execution=PaperExecutionConfig(fee_rate=0.0, slippage_bps=0.0, min_order_notional=1.0, market_data_max_age_ms=60_000),
        risk=RiskConfig(kill_switch_path=root / "kill.txt", max_signal_age_seconds=60_000),
    )


def phase_b_summary(now: object, score: float) -> dict[str, object]:
    """A persisted B.2 summary fixture, including every nested evidence group."""
    return {
        "target_metrics": {
            "activity": {"fills": 240, "campaigns": 120, "completed_campaigns": 115, "active_days": 90,
                         "trades_per_day": 2.6, "median_holding_seconds": 3600, "mean_holding_seconds": 4200,
                         "recent_activity_days": 1.0},
            "profitability": {"gross_pnl": score * 1.2, "net_pnl": score, "fees": score * 0.2, "expectancy": 1.1,
                              "median_campaign_pnl": 0.5, "average_win": 2.0, "average_loss": -1.0,
                              "win_rate": 0.62, "profit_factor": 1.8},
            "risk": {"max_drawdown_fraction": 0.08, "average_drawdown_dollars": 5.0, "worst_campaign": -8.0,
                     "tail_loss_percentile": -4.0, "largest_loss_relative_to_equity": 0.03,
                     "liquidation_frequency": 0.0, "loss_streak": 2, "adverse_averaging": False},
            "stability": {"profitable_day_fraction": 0.62, "daily_pnl": {"2026-01-01": 1.0},
                          "daily_pnl_variance": 0.2, "performance_concentration": 0.2,
                          "recent_vs_historical_pnl": 1.1},
            "concentration": {"symbol_count": 2, "largest_symbol_exposure_fraction": 0.55,
                              "top_campaign_pnl_fraction": 0.10, "top_five_campaign_pnl_fraction": 0.30,
                              "long_campaign_count": 70, "short_campaign_count": 50},
            "sizing": {"opening_observations": 120, "usable_equity_observations": 110,
                       "fallback_or_missing_observations": 10, "equity_quality_counts": {"exact": 110},
                       "martingale_indicator": False, "adverse_add_indicator": False},
        },
        "follower": {"net_pnl": score * 0.8, "return_fraction": 0.04, "expectancy": 0.8,
                     "profit_factor": 1.5, "max_drawdown": 0.1, "attempted_entries": 120,
                     "filled_attempts": 100, "skipped_attempts": 20, "missed_trade_rate": 0.02,
                     "pnl_by_wallet": {}, "pnl_by_symbol": {}, "pnl_by_bucket": {"10%": score * 0.8}, "fees": 1.0,
                     "sizing": {"bucket": "10%"}, "slippage_cost_at_baseline": 1.0, "slippage_robust": True,
                     "slippage_robustness_score": 0.9, "latency_status": "available", "price_assumption": "midpoint"},
        "copyability": {"status": "available", "score": 0.88},
        "coverage": {"coverage_state": "PROVEN_COMPLETE", "coverage_quality": "complete"},
        "analysis_window": {"required_start": (utc_now() - timedelta(days=180)).isoformat(), "required_end": utc_now().isoformat(), "boundary_policy": "strict"},
        "diversification_input": {"daily_return_series": {"2026-01-01": 0.01}, "symbols": ["BTC", "ETH"], "directions": ["long", "short"], "campaign_ids": ["c1", "c2"]},
        "slippage_scenarios": [{"bps": 5, "net_pnl": score * 0.8}, {"bps": 10, "net_pnl": score * 0.7}],
        "latency": {"status": "available", "curve": [{"latency_ms": 100, "net_pnl": score * 0.8}]},
        "walk_forward": [{"train_start": "2026-01-01", "forward_end": "2026-01-31", "net_pnl": score * 0.1}],
        "walk_forward_evaluation": {"status": "available", "score": 0.85, "window_count": 1},
        "score": {"total": score, "eligible": True}, "eligible": True,
    }


def seed_candidate(service: CopyTradeService, wallet: str, *, score: float, status: str = "new") -> None:
    now = utc_now()
    run = DiscoveryRun(run_id=f"discover_{wallet[-2:]}", started_at=now, sources=("fixture",), configuration={})
    service.database.start_discovery_run(run)
    service.database.stage_discovery_observations(run.run_id, [DiscoveryObservation(wallet=wallet, source="fixture", observed_at=now, recent_activity_at=now)])
    service.database.complete_discovery_run(run, limit=10, min_activity=1, max_activity_age_seconds=None)
    service.database.set_target_status(wallet, status)
    summary = phase_b_summary(now, score)
    run_id = f"phase_b_fixture_{wallet[-2:]}"
    service.database.start_analysis_run(AnalysisRun(run_id=run_id, started_at=now, finished_at=now, status="completed", configuration={}))
    service.database.upsert_candidate_analysis(CandidateAnalysis(wallet=wallet, lifecycle_status="qualified", last_run_id=run_id, completed_at=now, summary=summary))
    from src.copytrade.control_center import _config_fingerprint
    service.database.upsert_candidate_score(CandidateScore(wallet, now, score, {"consistency": 9.0}, {}, True, ("fixture",), provenance="phase_b", analysis_run_id=run_id, config_fingerprint=_config_fingerprint(service.config.snapshot())))


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
            self.assertEqual(detail["latency"]["status"], "available")  # type: ignore[index]
            self.assertEqual(detail["target_performance"]["activity"]["campaigns"], 120)  # type: ignore[index]
            self.assertEqual(len(detail["walk_forward"]["windows"]), 1)  # type: ignore[index]

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

    def test_phase_b_score_is_canonical_and_legacy_score_is_display_only(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            service = CopyTradeService(config(Path(temp)))
            seed_candidate(service, WALLET_A, score=88.0, status="shadow")
            now = utc_now() + timedelta(seconds=1)
            service.database.upsert_candidate_score(CandidateScore(
                WALLET_A, now, 999.0, {"legacy": 99.0}, {}, True, ("legacy",), provenance="legacy",
            ))
            center = CopyControlCenter(service.config, service.database)
            row = center.candidates(search=WALLET_A)["items"][0]
            detail = center.candidate_detail(WALLET_A)
            self.assertEqual(row["score"], 88.0)
            self.assertEqual(detail["score"]["total"], 88.0)  # type: ignore[index]
            self.assertEqual(detail["legacy_compatibility_score"]["total"], 999.0)  # type: ignore[index]
            self.assertEqual(center.set_operator_state(WALLET_A, "active")["operator_state"], "active")

    def test_shadow_finalists_use_persisted_phase_b_scores_and_real_b2_shape(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            service = CopyTradeService(config(Path(temp)))
            seed_candidate(service, WALLET_A, score=88.0, status="shadow")
            finalists = CopyControlCenter(service.config, service.database).shadow_finalists()
            self.assertEqual(finalists[0]["wallet"], WALLET_A)
            self.assertEqual(finalists[0]["score"], 88.0)
            self.assertEqual(finalists[0]["walk_forward"]["windows"][0]["net_pnl"], 8.8)

    def test_phase_c_reads_do_not_mutate_phase_b_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            service = CopyTradeService(config(Path(temp)))
            seed_candidate(service, WALLET_A, score=88.0, status="shadow")
            with service.database._connect() as connection:
                before = connection.execute("SELECT COUNT(*) FROM copy_candidate_scores").fetchone()[0]
                analysis_before = connection.execute("SELECT summary_json FROM copy_candidate_analyses WHERE wallet=?", (WALLET_A,)).fetchone()[0]
            center = CopyControlCenter(service.config, service.database)
            center.candidates()
            center.candidate_detail(WALLET_A)
            center.shadow_finalists()
            with service.database._connect() as connection:
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM copy_candidate_scores").fetchone()[0], before)
                self.assertEqual(connection.execute("SELECT summary_json FROM copy_candidate_analyses WHERE wallet=?", (WALLET_A,)).fetchone()[0], analysis_before)

    def test_activation_requires_current_qualified_phase_b_evidence_without_state_change(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            service = CopyTradeService(config(Path(temp)))
            seed_candidate(service, WALLET_A, score=88.0, status="shadow")
            with service.database._connect() as connection:
                connection.execute("UPDATE copy_candidate_scores SET config_fingerprint='stale' WHERE target_wallet=? AND provenance='phase_b'", (WALLET_A,))
            center = CopyControlCenter(service.config, service.database)
            with self.assertRaisesRegex(ValueError, "stale"):
                center.set_operator_state(WALLET_A, "active")
            self.assertEqual(service.database.get_target(WALLET_A).status, "shadow")  # type: ignore[union-attr]

    def test_entry_gate_allows_only_active_entries_and_always_allows_exits(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            service = CopyTradeService(config(Path(temp)))
            center = CopyControlCenter(service.config, service.database)
            self.assertEqual(center.store.entry_block_reason(WALLET_B, "open"), "wallet_not_active")
            states = {"approved": "wallet_not_active", "shadow": "wallet_not_active", "muted": "wallet_muted", "rejected": "wallet_not_active"}
            for index, (status, expected) in enumerate(states.items(), 3):
                wallet = f"0x{index:040x}"
                service.database.upsert_target(Target(wallet=wallet, status=status))
                self.assertEqual(center.store.entry_block_reason(wallet, "open"), expected)
                self.assertIsNone(center.store.entry_block_reason(wallet, "close"))
            service.database.upsert_target(Target(wallet=WALLET_A, status="active"))
            self.assertIsNone(center.store.entry_block_reason(WALLET_A, "add"))

    def test_execution_watcher_wallets_exclude_shadow_unless_a_sleeve_needs_exit_monitoring(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            service = CopyTradeService(config(Path(temp)))
            active = [f"0x{index:040x}" for index in range(10, 15)]
            shadow = [f"0x{index:040x}" for index in range(20, 40)]
            for wallet in active:
                service.database.upsert_target(Target(wallet=wallet, status="active"))
            for wallet in shadow:
                service.database.upsert_target(Target(wallet=wallet, status="shadow"))
            engine = PaperExecutionEngine(service.config, service.database)
            self.assertEqual(engine.process_signal(open_signal(shadow[0]), market_price=100.0).status, "filled")
            watched = service.monitored_execution_wallets()
            self.assertEqual(set(watched), set(active) | {shadow[0]})
            self.assertEqual(len(watched), 6)

    def test_config_bootstrap_preserves_durable_operator_state(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            initial = config(Path(temp))
            service = CopyTradeService(initial)
            service.database.upsert_target(Target(wallet=WALLET_A, status="active"))
            restarted = CopyTradeService(replace(initial, targets=({"wallet": WALLET_A, "status": "pending", "label": "bootstrap"},)))
            self.assertEqual(restarted.database.get_target(WALLET_A).status, "active")  # type: ignore[union-attr]

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

    def test_partial_close_pauses_entries_and_reports_skipped_groups(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            service = CopyTradeService(config(Path(temp)))
            engine = PaperExecutionEngine(service.config, service.database)
            self.assertEqual(engine.process_signal(open_signal(WALLET_A), market_price=100.0).status, "filled")
            eth = replace(open_signal(WALLET_B), symbol="ETH")
            self.assertEqual(engine.process_signal(eth, market_price=100.0).status, "filled")
            engine.mark_to_market("BTC", 101.0, utc_now())
            engine.persist_mark(utc_now())
            with service.database._connect() as connection:
                connection.execute("UPDATE copy_virtual_positions SET updated_at=? WHERE symbol='ETH'", (iso(utc_now() - timedelta(days=1)),))
            result = CopyControlCenter(service.config, service.database).close_all_paper_positions()
            self.assertEqual(result["status"], "partial")
            self.assertEqual(result["control"]["state"], "PAUSED")
            self.assertTrue(result["skipped"])

    def test_sizing_bucket_is_persisted_and_legacy_sleeves_are_not_inferred(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            service = CopyTradeService(config(Path(temp)))
            engine = PaperExecutionEngine(service.config, service.database)
            signal = replace(open_signal(WALLET_A), sizing_bucket="20%", allocation_fraction=0.2, requested_capital=20.0)
            self.assertEqual(engine.process_signal(signal, market_price=100.0).status, "filled")
            center = CopyControlCenter(service.config, service.database)
            self.assertEqual(center.positions()[0]["allocation_bucket"], "20%")
            sleeve = service.database.list_virtual_positions(open_only=True)[0]
            sleeve.sizing_bucket = "unknown_legacy"
            sleeve.sizing_allocation_fraction = None
            service.database.upsert_virtual_position(sleeve)
            self.assertEqual(center.positions()[0]["allocation_bucket"], "unknown_legacy")

    def test_persisted_pause_blocks_open_but_not_close_and_resume_allows_open(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            service = CopyTradeService(config(Path(temp)))
            service.database.upsert_target(Target(wallet=WALLET_A))
            service.database.upsert_target(Target(wallet=WALLET_B, status="active"))
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

    def test_operator_state_api_uses_explicit_400_404_and_409_responses(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            service = CopyTradeService(config(Path(temp)))
            seed_candidate(service, WALLET_A, score=88.0, status="shadow")
            app = create_control_center_app(service.config, service.database)
            endpoint = next(route.endpoint for route in app.routes if route.path == "/api/candidates/{wallet}/operator-state")
            with self.assertRaises(HTTPException) as invalid:
                asyncio.run(endpoint("invalid", {"state": "active"}))
            self.assertEqual(invalid.exception.status_code, 400)
            with self.assertRaises(HTTPException) as missing:
                asyncio.run(endpoint(WALLET_B, {"state": "shadow"}))
            self.assertEqual(missing.exception.status_code, 404)
            with service.database._connect() as connection:
                connection.execute("UPDATE copy_candidate_scores SET config_fingerprint='stale' WHERE target_wallet=?", (WALLET_A,))
            with self.assertRaises(HTTPException) as stale:
                asyncio.run(endpoint(WALLET_A, {"state": "active"}))
            self.assertEqual(stale.exception.status_code, 409)

    def test_control_center_watcher_lifecycle_uses_one_fake_watcher(self) -> None:
        class FakeHealth:
            def as_dict(self) -> dict[str, object]:
                return {"state": "CONNECTED", "per_target": {WALLET_A: "CONNECTED"}}

        class FakeWatcher:
            def __init__(self, _: object) -> None:
                self.health = FakeHealth()
                self.calls: list[list[str]] = []
                self.stopped = False

            async def run(self, wallets: list[str], *_: object) -> dict[str, int]:
                self.calls.append(wallets)
                await asyncio.Event().wait()
                return {}

            def stop(self) -> None:
                self.stopped = True

        class FakeService:
            adapter = object()

            def monitored_execution_wallets(self) -> list[str]:
                return [WALLET_A]

            async def ingest_watched_fills(self, *_: object) -> None: pass
            async def ingest_watched_state(self, *_: object) -> None: pass
            async def ingest_market_update(self, *_: object) -> None: pass
            async def reconcile_monitored_wallets(self) -> dict[str, int]: return {}

        with tempfile.TemporaryDirectory() as temp:
            service = CopyTradeService(config(Path(temp)))
            watcher = FakeWatcher(object())
            app = create_control_center_app(service.config, service.database, watcher_service=FakeService(), watcher_factory=lambda _: watcher)

            async def exercise_lifecycle() -> None:
                async with app.router.lifespan_context(app):
                    await asyncio.sleep(0)
                    health_endpoint = next(route.endpoint for route in app.routes if route.path == "/api/health")
                    health = await health_endpoint()
                    self.assertEqual(health["watcher"]["state"], "CONNECTED")
                    self.assertEqual(health["watcher"]["subscribed_target_count"], 1)
                self.assertTrue(watcher.stopped)

            asyncio.run(exercise_lifecycle())
            self.assertEqual(watcher.calls, [[WALLET_A]])
