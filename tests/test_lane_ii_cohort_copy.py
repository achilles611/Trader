from __future__ import annotations

import asyncio
import json
import tempfile
import unittest
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from src.copytrade.config import (
    ArtifactConfig,
    CopyTradeConfig,
    PaperExecutionConfig,
    PaperStrategyConfig,
    RiskConfig,
    ScientificExecutionConfig,
    SizingConfig,
)
from src.copytrade.control_center import create_control_center_app
from src.copytrade.lane_ii import freeze_lane_ii_deep_batch, freeze_lane_ii_research_pass, lane_ii_status, refresh_public_cohort
from src.copytrade.models import RawFill, Target, utc_now
from src.copytrade.service import CopyTradeService


WALLET = "0x1212121212121212121212121212121212121212"


def _config(root: Path, strategy: str = "COHORT_COPY_V1") -> CopyTradeConfig:
    return CopyTradeConfig(
        artifacts=ArtifactConfig(database_path=root / "lane-ii.sqlite3", obsidian_root=root / "obsidian"),
        capital=replace(CopyTradeConfig().capital, initial_capital=100),
        sizing=SizingConfig(
            small_fraction=.12, medium_fraction=.16, large_fraction=.20,
            min_history=10, fallback_fraction=.12, copy_target_adds=True,
        ),
        risk=RiskConfig(
            max_total_committed_fraction=.60, max_capital_per_target_fraction=.25,
            max_capital_per_symbol_fraction=.30, max_simultaneous_virtual_campaigns=5,
            kill_switch_path=root / "kill", max_signal_age_seconds=86_400,
        ),
        paper_execution=PaperExecutionConfig(
            fee_rate=0, slippage_bps=0, min_order_notional=10,
            quantity_precision=6, market_data_max_age_ms=60_000,
        ),
        paper_strategy=PaperStrategyConfig(strategy_id=strategy, version=1),
        scientific_execution=ScientificExecutionConfig(enabled=True),
    )


def _fill(index: int, side: str = "B", before: float = 0, *, wallet: str = WALLET) -> RawFill:
    at = utc_now() - timedelta(milliseconds=10 - index)
    return RawFill.from_hyperliquid(
        {
            "coin": "BTC", "px": "100", "sz": "1", "side": side,
            "time": int(at.timestamp() * 1000), "startPosition": str(before),
            "oid": index, "tid": f"lane-ii-{index}", "hash": f"0x{index:064x}",
            "fee": "0", "accountValue": "1000",
        },
        wallet,
        ingested_at=at,
    )


class LaneIICohortCopyTests(unittest.TestCase):
    def test_cohort_strategy_is_distinct_from_scientific_sensor_gate(self) -> None:
        with tempfile.TemporaryDirectory() as first, tempfile.TemporaryDirectory() as second:
            sensor = CopyTradeService(_config(Path(first), "SCIENTIFIC_SENSOR_V1"))
            sensor.database.upsert_target(Target(wallet=WALLET, status="active"))
            asyncio.run(sensor.ingest_market_update({"mids": {"BTC": "100"}}))
            asyncio.run(sensor.ingest_watched_fills(WALLET, [_fill(1)], False))
            self.assertEqual(sensor.database.dashboard_snapshot()["execution_attempts"][0]["reason"], "scientific_decision_required")
            self.assertEqual(sensor.database.list_virtual_positions(open_only=True), [])

            cohort = CopyTradeService(_config(Path(second)))
            cohort.database.upsert_target(Target(wallet=WALLET, status="active"))
            asyncio.run(cohort.ingest_market_update({"mids": {"BTC": "100"}}))
            asyncio.run(cohort.ingest_watched_fills(WALLET, [_fill(2)], False))
            attempts = cohort.database.dashboard_snapshot()["execution_attempts"]
            self.assertEqual((attempts[0]["status"], attempts[0]["reason"]), ("filled", "approved"))
            position = cohort.database.list_virtual_positions(open_only=True)[0]
            self.assertEqual(position.allocated_capital, 12)
            self.assertEqual(position.target_wallet, WALLET)

    def test_shadow_wallets_are_observed_but_never_gain_entry_authority(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            service = CopyTradeService(_config(Path(folder)))
            service.database.upsert_target(Target(wallet=WALLET, status="shadow"))
            self.assertEqual(service.monitored_observation_wallets(), [WALLET])
            self.assertEqual(service.monitored_execution_wallets(), [])
            asyncio.run(service.ingest_market_update({"mids": {"BTC": "100"}}))
            asyncio.run(service.ingest_watched_fills(WALLET, [_fill(3)], False))
            self.assertEqual(service.database.dashboard_snapshot()["execution_attempts"][0]["reason"], "wallet_not_active")
            self.assertEqual(service.database.list_virtual_positions(open_only=True), [])

    def test_adds_partial_exits_and_conflicting_leaders_preserve_virtual_ownership(self) -> None:
        other = "0x3434343434343434343434343434343434343434"
        with tempfile.TemporaryDirectory() as folder:
            config = _config(Path(folder))
            config = replace(
                config,
                sizing=replace(config.sizing, fallback_fraction=.20),
                risk=replace(
                    config.risk,
                    max_total_committed_fraction=1,
                    max_capital_per_target_fraction=1,
                    max_capital_per_symbol_fraction=1,
                ),
            )
            service = CopyTradeService(config)
            service.database.upsert_target(Target(wallet=WALLET, status="active"))
            service.database.upsert_target(Target(wallet=other, status="active"))
            asyncio.run(service.ingest_market_update({"mids": {"BTC": "100"}}))
            asyncio.run(service.ingest_watched_fills(WALLET, [_fill(10, "B", 0)], False))
            asyncio.run(service.ingest_watched_fills(other, [_fill(11, "A", 0, wallet=other)], False))
            asyncio.run(service.ingest_watched_fills(WALLET, [_fill(12, "B", 1)], False))
            owned = service.database.list_virtual_positions(open_only=True)
            self.assertEqual(len(owned), 2)
            long_sleeve = next(item for item in owned if item.target_wallet == WALLET)
            self.assertGreater(long_sleeve.allocated_capital, 20)

            asyncio.run(service.ingest_watched_fills(WALLET, [_fill(13, "A", 2)], False))
            asyncio.run(service.ingest_watched_fills(other, [_fill(14, "B", -1, wallet=other)], False))
            remaining = service.database.list_virtual_positions(open_only=True)
            self.assertEqual(len(remaining), 1)
            self.assertEqual(remaining[0].target_wallet, WALLET)
            self.assertGreater(remaining[0].quantity, 0)

    def test_status_never_labels_cash_app_btc_as_connected_capital(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            service = CopyTradeService(_config(Path(folder)))
            status = lane_ii_status(service, cohort_path=Path(folder) / "missing.json")
            self.assertEqual(status["display_account"], "PAPER — $100 simulated")
            self.assertFalse(status["portfolio"]["connected_capital"])
            self.assertFalse(status["connection_setup"]["cash_app_connected"])
            self.assertEqual(status["readiness"]["live"]["state"], "UNAVAILABLE")
            self.assertEqual(status["readiness"]["paper"]["state"], "BLOCKED_COHORT_SHORTFALL")

    def test_terminal_shortfall_explains_disabled_paper_start(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            cohort = root / "latest-cohort.json"
            cohort.write_text(json.dumps({
                "schema": "beelzebub-lane-ii-cohort-v1",
                "selection_status": "COHORT_SHORTFALL_EVIDENCE_DEPENDENCY",
                "selected": [],
                "research_watchlist": [],
                "screening": {"available": 100},
                "shortfall": {
                    "minimum_required": 5,
                    "summary": "Zero qualified wallets after the bounded source pilots.",
                    "paper_start_blocker": "Start is disabled: 0 of 5 required finalists qualified.",
                    "next_action": "Supply a verified zero-cost historical source or keep Lane II disarmed.",
                },
            }), encoding="utf-8")
            status = lane_ii_status(CopyTradeService(_config(root)), cohort_path=cohort)
            self.assertEqual(status["overall"]["state"], "RESEARCH_SHORTFALL")
            self.assertEqual(status["overall"]["next_action"], "Supply a verified zero-cost historical source or keep Lane II disarmed.")
            self.assertEqual(status["readiness"]["paper"]["blocker"], "Start is disabled: 0 of 5 required finalists qualified.")
            self.assertEqual(status["cohort"]["shortfall"]["minimum_required"], 5)
            self.assertFalse(status["controls"]["start_paper_available"])

    def test_frozen_no_spend_screen_is_deterministic_and_does_not_start_phase_b(self) -> None:
        class PublicOnlyAdapter:
            def __init__(self) -> None:
                self.calls: list[str] = []

            def fetch_quantity_precisions(self) -> dict[str, int]:
                self.calls.append("meta")
                return {"BTC": 6}

            def fetch_user_fills(self, wallet: str, *, aggregate_by_time: bool = False) -> list[RawFill]:
                self.calls.append(f"fills:{wallet}:{aggregate_by_time}")
                return [_fill(20, wallet=wallet)]

            def fetch_portfolio(self, wallet: str) -> list[dict[str, str]]:
                self.calls.append(f"portfolio:{wallet}")
                return [{"time": "now", "accountValue": "100"}]

            def fetch_clearinghouse_state(self, wallet: str) -> SimpleNamespace:
                self.calls.append(f"state:{wallet}")
                return SimpleNamespace(
                    account_value=100.0,
                    withdrawable=100.0,
                    total_notional_position=0.0,
                    positions={"asset_positions": []},
                )

        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            retained = root / "retained.sqlite3"
            import sqlite3
            connection = sqlite3.connect(retained)
            try:
                connection.executescript(
                    """
                    CREATE TABLE copy_discovery_candidates (
                        wallet TEXT PRIMARY KEY, discovered_at TEXT, last_seen_at TEXT, recent_activity_at TEXT,
                        discovery_rank INTEGER, source_score REAL, source_count INTEGER,
                        discovery_status TEXT, last_discovery_run_id TEXT, metadata_json TEXT
                    );
                    CREATE TABLE copy_candidate_analyses (
                        wallet TEXT PRIMARY KEY, lifecycle_status TEXT, last_run_id TEXT, started_at TEXT,
                        completed_at TEXT, prefilter_reasons_json TEXT, errors_json TEXT, summary_json TEXT
                    );
                    """
                )
                connection.execute(
                    "INSERT INTO copy_discovery_candidates VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (WALLET, "now", "now", "now", 1, 10.0, 1, "new", None, "{}"),
                )
                connection.execute(
                    "INSERT INTO copy_discovery_candidates VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    ("0x5656565656565656565656565656565656565656", "now", "now", "now", 2, 99.0, 1, "new", None, "{}"),
                )
                connection.execute(
                    "INSERT INTO copy_candidate_analyses VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    ("0x5656565656565656565656565656565656565656", "backfill_failed", "old", "now", "now", "[]", "[]", "{}"),
                )
                connection.commit()
            finally:
                connection.close()
            service = CopyTradeService(_config(root))
            adapter = PublicOnlyAdapter()
            service.adapter = adapter  # type: ignore[assignment]
            frozen = freeze_lane_ii_research_pass(
                service,
                retained_database=retained,
                output=root / "research-pass.json",
                seed_limit=50,
                planned_deep_limit=12,
                public_request_budget=4,
            )
            self.assertEqual([item["wallet"] for item in frozen["seeds"]], [WALLET])
            screened = refresh_public_cohort(
                service,
                retained_database=retained,
                output_directory=root / "evidence",
                analysis_limit=0,
                frozen_research_pass=frozen["research_pass_path"],
                include_public_account_state=True,
                public_request_budget=4,
            )
            self.assertEqual(screened["selection_status"], "NOT_RUN_SCREEN_ONLY")
            self.assertEqual(screened["selected"], [])
            self.assertEqual(screened["screening"]["requests_consumed"], 4)
            self.assertEqual(adapter.calls, ["meta", f"fills:{WALLET}:False", f"portfolio:{WALLET}", f"state:{WALLET}"])
            self.assertEqual(service.database.list_targets(), [])
            progress_files = list((root / "evidence").glob("screen-progress-*.json"))
            self.assertEqual(len(progress_files), 1)
            progress = json.loads(progress_files[0].read_text(encoding="utf-8"))
            self.assertEqual((progress["state"], progress["completed_wallets"], progress["requests_consumed"]), ("COMPLETED", 1, 4))
            status = lane_ii_status(service, cohort_path=screened["latest_cohort_path"])
            funnel = {item["label"]: item["count"] for item in status["funnel"]}
            self.assertEqual(status["overall"]["state"], "RESEARCH_SCREENED")
            self.assertEqual((funnel["Screened"], funnel["Acquiring"], funnel["Deferred"]), (1, 0, 0))
            self.assertFalse(status["controls"]["start_paper_available"])

    def test_deep_batch_is_pinned_to_completed_parent_screen(self) -> None:
        class PublicOnlyAdapter:
            def fetch_quantity_precisions(self) -> dict[str, int]:
                return {"BTC": 6}

            def fetch_user_fills(self, wallet: str, *, aggregate_by_time: bool = False) -> list[RawFill]:
                return [_fill(41, wallet=wallet)]

        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            retained = root / "retained.sqlite3"
            import sqlite3
            connection = sqlite3.connect(retained)
            try:
                connection.execute(
                    "CREATE TABLE copy_discovery_candidates (wallet TEXT PRIMARY KEY, discovered_at TEXT, last_seen_at TEXT, recent_activity_at TEXT, discovery_rank INTEGER, source_score REAL, source_count INTEGER, discovery_status TEXT, last_discovery_run_id TEXT, metadata_json TEXT)"
                )
                connection.execute(
                    "INSERT INTO copy_discovery_candidates VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (WALLET, "now", "now", "now", 1, 1.0, 1, "new", None, "{}"),
                )
                connection.commit()
            finally:
                connection.close()
            config = _config(root)
            service = CopyTradeService(replace(config, analysis=replace(config.analysis, default_workers=1)))
            service.adapter = PublicOnlyAdapter()  # type: ignore[assignment]
            parent = freeze_lane_ii_research_pass(
                service, retained_database=retained, output=root / "parent.json", seed_limit=1,
                planned_deep_limit=1, public_request_budget=2,
            )
            screened = refresh_public_cohort(
                service, retained_database=retained, output_directory=root / "screen", analysis_limit=0,
                frozen_research_pass=parent["research_pass_path"], public_request_budget=2,
            )
            child = freeze_lane_ii_deep_batch(
                service, parent_research_pass=parent["research_pass_path"],
                candidate_universe=screened["candidate_universe_path"], wallets=[WALLET], output=root / "deep.json",
            )
            self.assertEqual(child["pass_kind"], "deep_batch")
            self.assertEqual(child["policy"]["public_request_budget"], 2)
            self.assertEqual([item["wallet"] for item in child["seeds"]], [WALLET])

            # The immutable deep batch keeps its policy and exact wallet, but
            # controlled recovery must be able to serialize Phase-B history
            # acquisition without changing the frozen evidence.
            with patch("src.copytrade.lane_ii.CandidateAnalysisPipeline.run", return_value={"status": "fixture"}) as analysis_run, patch(
                "src.copytrade.lane_ii.CandidateAnalysisPipeline.shadow_finalists", return_value=[],
            ):
                resumed = refresh_public_cohort(
                    service, retained_database=retained, output_directory=root / "deep-run", analysis_limit=1,
                    frozen_research_pass=child["research_pass_path"], public_request_budget=2, analysis_workers=1,
                )
            self.assertEqual(resumed["selection_status"], "COMPLETED")
            self.assertEqual(analysis_run.call_args.kwargs["workers"], 1)
            self.assertEqual(analysis_run.call_args.kwargs["candidate_wallets"], [WALLET])

            # A caller cannot turn the recovery cap into a concurrency
            # increase above the configured single-owner limit.
            with patch("src.copytrade.lane_ii.CandidateAnalysisPipeline.run", return_value={"status": "fixture"}) as capped_run, patch(
                "src.copytrade.lane_ii.CandidateAnalysisPipeline.shadow_finalists", return_value=[],
            ):
                refresh_public_cohort(
                    service, retained_database=retained, output_directory=root / "capped-run", seed_limit=1,
                    analysis_limit=2, public_request_budget=2, analysis_workers=99,
                )
            self.assertEqual(capped_run.call_args.kwargs["workers"], 1)

    def test_lane_ii_only_lifecycle_never_starts_lane_iii_and_live_is_backend_denied(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            calls: list[str] = []

            def forbidden(*_: object, **__: object) -> object:
                calls.append("lane-iii-factory")
                raise AssertionError("Lane III factory must not run")

            app = create_control_center_app(
                _config(Path(folder)),
                lane_ii_only=True,
                ninjatrader_listener_factory=forbidden,  # type: ignore[arg-type]
                lane_iii_shadow_factory=forbidden,  # type: ignore[arg-type]
                lane_iii_paper_factory=forbidden,  # type: ignore[arg-type]
            )

            async def exercise() -> dict[str, object]:
                async with app.router.lifespan_context(app):
                    endpoint = next(route.endpoint for route in app.routes if route.path == "/api/lane-ii/live/start")
                    response = await endpoint()
                    return json.loads(response.body)

            payload = asyncio.run(exercise())
            self.assertEqual(calls, [])
            self.assertEqual(payload["code"], "LANE_II_LIVE_UNAVAILABLE")
            self.assertFalse(payload["exchange_authority"])


if __name__ == "__main__":
    unittest.main()
