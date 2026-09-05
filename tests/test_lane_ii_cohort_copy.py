from __future__ import annotations

import asyncio
import json
import tempfile
import unittest
from dataclasses import replace
from datetime import timedelta
from pathlib import Path

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
from src.copytrade.lane_ii import lane_ii_status
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
