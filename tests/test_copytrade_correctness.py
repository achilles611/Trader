from __future__ import annotations

import asyncio
import tempfile
import unittest
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

from src.copytrade.analytics import calculate_trader_metrics
from src.copytrade.backtest import CopyTradeBacktester
from src.copytrade.config import ArtifactConfig, CopyTradeConfig, PaperExecutionConfig, RiskConfig, SizingConfig
from src.copytrade.hyperliquid import HyperliquidPublicAdapter, HyperliquidWatcher
from src.copytrade.market import HyperliquidMarketData, MarketPrice
from src.copytrade.models import RawFill, TraderSnapshot, as_utc, stable_id
from src.copytrade.paper import PaperExecutionEngine, TargetSizeClassifier
from src.copytrade.reconstruction import PositionReconstructor
from src.copytrade.scoring import FollowerMetrics, pairwise_correlation_details, score_candidate
from src.copytrade.service import CopyTradeService
from src.copytrade.storage import CopyTradeDatabase


WALLET = "0x1111111111111111111111111111111111111111"
T0 = 1_700_000_000_000


def raw(
    tid: int, side: str, qty: float, before: float, *, price: float = 100.0, fee: float = 0.0,
    closed_pnl: float | None = None, account_value: float | None = None, time_ms: int | None = None,
) -> RawFill:
    payload = {"coin": "BTC", "px": str(price), "sz": str(qty), "side": side, "time": time_ms or T0 + tid,
               "startPosition": str(before), "oid": tid, "tid": tid, "hash": f"0x{tid:064x}", "fee": str(fee)}
    if closed_pnl is not None:
        payload["closedPnl"] = str(closed_pnl)
    if account_value is not None:
        payload["accountValue"] = str(account_value)
    return RawFill.from_hyperliquid(payload, WALLET)


def config(path: Path, **changes: object) -> CopyTradeConfig:
    base = CopyTradeConfig(
        artifacts=ArtifactConfig(database_path=path / "paper.sqlite3", obsidian_root=path / "obsidian"),
        sizing=SizingConfig(min_history=2, max_equity_age_seconds=60),
        paper_execution=PaperExecutionConfig(fee_rate=0.0, slippage_bps=0, min_order_notional=1, random_seed=1),
        risk=RiskConfig(max_signal_age_seconds=10_000_000_000, kill_switch_path=path / "kill", max_total_committed_fraction=1,
                        max_capital_per_target_fraction=1, max_capital_per_symbol_fraction=1),
    )
    return replace(base, **changes)


def signal(name: str, *, action: str = "open", direction: str = "long", price: float = 100.0, capital: float = 100.0,
           qty: float = 1.0, before: float = 0.0, at: object = T0):
    from src.copytrade.models import CopySignal
    timestamp = as_utc(at)
    return CopySignal(stable_id("signal", name), WALLET, "campaign", name, "BTC", action, direction, price, qty, qty * price,
                      0.1, capital, timestamp, timestamp, target_position_before=before)


class FixtureMarket:
    def __init__(self, base: object) -> None:
        self.base = as_utc(base)

    def historical_price(self, symbol: str, timestamp: object) -> MarketPrice:
        seconds = (as_utc(timestamp) - self.base).total_seconds()
        return MarketPrice(symbol, 100 + seconds, as_utc(timestamp), "fixture_time_price", "fixture_exact")

    def current_price(self, symbol: str):  # pragma: no cover - protocol completeness
        return self.historical_price(symbol, self.base)

    def current_order_book(self, symbol: str):  # pragma: no cover - protocol completeness
        raise NotImplementedError


class CandleAdapter:
    def fetch_candle_snapshot(self, symbol, start, end, interval):
        at = int(as_utc(end).timestamp() * 1000)
        minute = at - at % 60_000
        return [{"t": minute - 60_000, "c": "100"}, {"t": minute, "c": "999"}]


class CopytradeCorrectnessTests(unittest.TestCase):
    def test_long_to_short_flip_splits_fee_notional_and_pnl(self) -> None:
        result = PositionReconstructor().reconstruct([raw(1, "B", 1, 0, price=90, fee=.3), raw(2, "A", 3, 1, price=100, fee=.9, closed_pnl=10)])
        old, new = result.campaigns
        self.assertEqual((old.direction, new.direction), ("long", "short"))
        self.assertAlmostEqual(old.realized_pnl, 10)
        self.assertAlmostEqual(old.target_fees, .6)  # .3 entry + one-third of crossing fee
        self.assertAlmostEqual(new.target_fees, .6)  # two-thirds of crossing fee
        self.assertEqual((old.event_count, new.event_count), (2, 1))
        self.assertEqual((result.events[-2].split_role, result.events[-1].split_role), ("closing", "opening"))
        self.assertAlmostEqual(result.events[-1].initial_delta_notional, 200)
        self.assertAlmostEqual(old.source_closed_pnl, 10)
        self.assertFalse(new.source_closed_pnl_observed)

    def test_short_to_long_flip_and_negative_rebate(self) -> None:
        result = PositionReconstructor().reconstruct([raw(1, "A", 2, 0, price=110), raw(2, "B", 5, -2, price=100, fee=-1, closed_pnl=20)])
        old, new = result.campaigns
        self.assertEqual((old.direction, new.direction), ("short", "long"))
        self.assertAlmostEqual(old.realized_pnl, 20)
        self.assertAlmostEqual(old.target_fees, -.4)
        self.assertAlmostEqual(new.target_fees, -.6)
        self.assertAlmostEqual(result.events[-1].initial_delta_notional, 300)

    def test_truncated_entry_is_not_used_for_metrics(self) -> None:
        result = PositionReconstructor().reconstruct([raw(1, "A", 2, 2, price=100, closed_pnl=15)])
        campaign = result.campaigns[0]
        self.assertFalse(campaign.history_complete)
        self.assertEqual(campaign.entry_basis_quality, "unknown_truncated")
        self.assertEqual(campaign.realized_pnl, 0)
        metrics = calculate_trader_metrics(WALLET, result.campaigns, result.events)
        self.assertEqual(metrics.closed_campaign_count, 0)
        self.assertEqual(metrics.raw["truncated_campaign_count"], 1)

    def test_reconciliation_reports_difference_without_overwriting(self) -> None:
        result = PositionReconstructor().reconstruct([raw(1, "B", 1, 0, price=90), raw(2, "A", 1, 1, price=100, closed_pnl=11)])
        campaign = result.campaigns[0]
        self.assertAlmostEqual(campaign.realized_pnl, 10)
        self.assertAlmostEqual(campaign.source_closed_pnl, 11)
        self.assertAlmostEqual(campaign.reconciliation_gross_difference or 0, -1)
        self.assertEqual(result.reconciliation["mismatched_campaigns"], 1)

    def test_hyperliquid_closed_pnl_and_liquidation_are_normalized(self) -> None:
        item = RawFill.from_hyperliquid({"coin": "BTC", "px": "100", "sz": "1", "side": "A", "time": T0,
                                         "closedPnl": "-12.5", "dir": "Liquidated Long", "fee": "0"}, WALLET)
        self.assertEqual(item.source_closed_pnl, -12.5)
        self.assertTrue(item.is_liquidation)

    def test_prior_only_equity_enrichment_and_staleness(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            service = CopyTradeService(config(Path(temp)))
            service.database.insert_snapshot(TraderSnapshot("prior", WALLET, as_utc(T0 - 1_000), 1_000, None, None, {}, "live", {}))
            service.database.insert_raw_fill(raw(1, "B", 1, 0, account_value=None))
            event = service.reconstruct(WALLET)["events"][0]
            self.assertEqual((event.target_equity, event.equity_source, event.equity_age_seconds), (1_000, "recent_live_snapshot", 1.001))
            service.database.insert_snapshot(TraderSnapshot("old", WALLET, as_utc(T0 - 100_000), 2_000, None, None, {}, "live", {}))
            # A different target avoids using the recent observation above.
            second = raw(2, "B", 1, 0, time_ms=T0 + 200_000)
            service.database.insert_raw_fill(second)
            enriched = service.reconstruct(WALLET)["events"][-1]
            self.assertEqual(enriched.equity_source, "missing")
            self.assertIsNone(enriched.target_equity)

    def test_equity_classifier_creates_five_ten_twenty_buckets(self) -> None:
        classifier = TargetSizeClassifier(SizingConfig(min_history=2, max_equity_age_seconds=60))
        classifier.classify(WALLET, 100, 1_000, "exact")
        classifier.classify(WALLET, 100, 1_000, "exact")
        self.assertEqual(classifier.classify(WALLET, 30, 1_000, "exact").allocation_fraction, .05)
        self.assertEqual(classifier.classify(WALLET, 100, 1_000, "exact").allocation_fraction, .10)
        self.assertEqual(classifier.classify(WALLET, 200, 1_000, "exact").allocation_fraction, .20)
        before = list(classifier._history[WALLET])
        classifier.classify(WALLET, 1_000, 1_000, "recent_live_snapshot", 61)
        self.assertEqual(before, classifier._history[WALLET])

    def test_latency_without_market_data_is_unavailable_not_flat(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            cfg = config(Path(temp))
            fills = [raw(1, "B", 1, 0), raw(2, "A", 1, 1, time_ms=T0 + 1_000)]
            self.assertEqual(CopyTradeBacktester(cfg).latency_decay_curve(fills), [])
            metrics = calculate_trader_metrics(WALLET, PositionReconstructor().reconstruct(fills).campaigns)
            scored = score_candidate(metrics, cfg.candidates, FollowerMetrics(expectancy=1))
            self.assertIn("latency_unavailable", scored.reasons)
            self.assertNotIn("latency_survivability", scored.component_scores)

    def test_time_sensitive_latency_changes_fixture_execution(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            cfg = config(Path(temp))
            fills = [raw(1, "B", 1, 0), raw(2, "A", 1, 1, time_ms=T0 + 20_000)]
            curve = CopyTradeBacktester(cfg, market_data=FixtureMarket(T0)).latency_decay_curve(fills)
            self.assertEqual(len(curve), len(cfg.backtest.detection_delays_ms))
            self.assertNotEqual(curve[0]["net_pnl"], curve[-1]["net_pnl"])

    def test_candle_proxy_never_uses_current_minute_close(self) -> None:
        market = HyperliquidMarketData(CandleAdapter())  # type: ignore[arg-type]
        price = market.historical_price("BTC", as_utc(T0 + 5_000))
        self.assertIsNotNone(price)
        self.assertEqual(price.price, 100)
        self.assertIn("prior_candle", price.quality)

    def test_mark_to_market_long_short_partial_loss_and_current_equity_cap(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            risk = replace(config(root).risk, max_capital_per_target_fraction=.3, risk_cap_base="current_equity")
            engine = PaperExecutionEngine(config(root, risk=risk))
            self.assertEqual(engine.process_signal(signal("long", capital=60)).status, "filled")
            engine.mark_to_market("BTC", 50, T0 + 1_000)
            self.assertAlmostEqual(engine.portfolio.equity, 170)
            self.assertAlmostEqual(next(iter(engine.portfolio.sleeves.values())).unrealized_pnl, -30)
            self.assertEqual(engine.process_signal(signal("second", capital=60, at=T0 + 2_000)).reason, "insufficient_capital")
            self.assertEqual(engine.process_signal(signal("partial", action="reduce", qty=50, before=100, price=50, at=T0 + 3_000)).status, "filled")
            self.assertLess(engine.portfolio.target_realized(WALLET), 0)
            short = PaperExecutionEngine(config(root))
            short.process_signal(signal("short", direction="short", capital=100))
            short.mark_to_market("BTC", 80, T0 + 1_000)
            self.assertAlmostEqual(short.portfolio.equity, 220)

    def test_atomic_replay_fault_and_restart_preserve_drawdown(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            cfg = config(Path(temp))
            database = CopyTradeDatabase(cfg.artifacts.database_path)
            database.initialize()
            engine = PaperExecutionEngine(cfg, database)
            item = signal("atomic", capital=100)
            with self.assertRaisesRegex(RuntimeError, "boom"):
                engine.process_signal(item, fault_hook=lambda phase: (_ for _ in ()).throw(RuntimeError("boom")) if phase == "after_sleeves" else None)
            self.assertFalse(database.has_execution_attempt_for_signal(item.signal_id))
            self.assertEqual(engine.process_signal(item).status, "filled")
            self.assertEqual(engine.process_signal(item).status, "filled")
            self.assertEqual(len(database.dashboard_snapshot()["execution_attempts"]), 1)
            engine.mark_to_market("BTC", 50, T0 + 1_000)
            engine.process_signal(signal("close", action="close", qty=1, before=1, price=50, at=T0 + 2_000))
            restarted = PaperExecutionEngine(cfg, database)
            restarted.restore(database.list_virtual_positions(), database.latest_portfolio_snapshot())
            self.assertAlmostEqual(restarted.portfolio.peak_equity or 0, 200)
            self.assertGreaterEqual(restarted.portfolio.max_drawdown_fraction, .25)

    def test_every_execution_transaction_fault_replays_once(self) -> None:
        # A: signal exists before execution; B/C: each in-transaction phase;
        # D: calling again after commit represents acknowledgement loss.
        with tempfile.TemporaryDirectory() as temp:
            for phase in ("after_claim", "after_attempt", "after_sleeves", "before_commit"):
                root = Path(temp) / phase
                cfg = config(root)
                database = CopyTradeDatabase(cfg.artifacts.database_path)
                database.initialize()
                item = signal(phase, capital=20)
                database.insert_signal(item)  # A
                engine = PaperExecutionEngine(cfg, database)
                with self.assertRaisesRegex(RuntimeError, phase):
                    engine.process_signal(item, fault_hook=lambda current, expected=phase: (_ for _ in ()).throw(RuntimeError(expected)) if current == expected else None)
                self.assertFalse(database.has_execution_attempt_for_signal(item.signal_id))
                self.assertEqual(engine.process_signal(item).status, "filled")
                self.assertEqual(engine.process_signal(item).status, "filled")  # D
                self.assertEqual(len(database.dashboard_snapshot()["execution_attempts"]), 1)

    def test_time_aligned_correlation_and_walk_forward_boundary_policy(self) -> None:
        dates = {f"2024-01-{day:02d}": float(day) for day in range(1, 8)}
        corr, count = pairwise_correlation_details(dates, {key: value * 2 for key, value in dates.items()})
        self.assertEqual(count, 7)
        self.assertAlmostEqual(corr, 1)
        with tempfile.TemporaryDirectory() as temp:
            cfg = config(Path(temp), backtest=replace(CopyTradeConfig().backtest, default_training_days=1, default_forward_days=1))
            fills = [raw(1, "B", 1, 0, time_ms=T0), raw(2, "A", 1, 1, time_ms=T0 + 86_400_000 + 1),
                     raw(3, "B", 1, 0, time_ms=T0 + 172_800_000 + 1)]
            windows = CopyTradeBacktester(cfg).walk_forward(fills, training_days=1, forward_days=1)
            self.assertEqual(windows[0]["boundary_policy"], "exclude_campaigns_open_at_forward_start")
            self.assertEqual(windows[0]["boundary_campaigns_excluded"], 1)

    def test_websocket_auxiliary_array_and_coverage_are_safe(self) -> None:
        adapter = HyperliquidPublicAdapter(config(Path(tempfile.gettempdir())).source)
        watcher = HyperliquidWatcher(adapter)
        received: list[str] = []
        async def handler(wallet, fills, snapshot): received.append(wallet)
        async def state(wallet, payload): raise AssertionError("ambiguous auxiliary payload must not be forwarded")
        asyncio.run(watcher._handle_message({"channel": "orderUpdates", "data": [{"status": "open"}]}, handler, state))
        asyncio.run(watcher._handle_message({"channel": "allDexsClearinghouseState", "data": []}, handler, state))
        asyncio.run(watcher._handle_message({"channel": "userFills", "data": {"user": WALLET, "fills": [], "isSnapshot": True}}, handler, state))
        self.assertEqual(received, [WALLET])
        adapter.fetch_fills_by_time = lambda *args, **kwargs: []  # type: ignore[method-assign]
        adapter.backfill_fills(WALLET, T0, T0 + 1_000)
        self.assertFalse(adapter.last_backfill_coverage.coverage_complete)  # type: ignore[union-attr]
