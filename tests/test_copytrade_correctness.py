from __future__ import annotations

import asyncio
import argparse
import json
import sys
import tempfile
import types
import unittest
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from src.copytrade.analytics import calculate_trader_metrics
from src.copytrade.backtest import CopyTradeBacktester
from src.copytrade.config import ArtifactConfig, CandidateConfig, CopyTradeConfig, PaperExecutionConfig, RiskConfig, SizingConfig
from src.copytrade.cli import run_copytrade_command
from src.copytrade.hyperliquid import HyperliquidPublicAdapter, HyperliquidWatcher
from src.copytrade.market import HyperliquidMarketData, MarketPrice
from src.copytrade.models import RawFill, Target, TraderSnapshot, as_utc, stable_id, utc_now
from src.copytrade.paper import PaperExecutionEngine, TargetSizeClassifier
from src.copytrade.reconstruction import PositionReconstructor
from src.copytrade.scoring import FollowerMetrics, pairwise_correlation_details, score_candidate, select_diverse_targets
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
    def test_copy_backtest_does_not_mutate_operational_paper_tables(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            cfg = config(Path(temp))
            service = CopyTradeService(cfg)
            service.database.insert_raw_fill(raw(1, "B", 1, 0))
            self.assertEqual(PaperExecutionEngine(cfg, service.database).process_signal(signal("operational")).status, "filled")

            operational_tables = (
                "copy_signals", "copy_execution_claims", "copy_execution_attempts", "copy_execution_fills",
                "copy_virtual_positions", "copy_portfolio_snapshots",
            )
            def counts() -> dict[str, int]:
                with service.database._connect() as connection:  # type: ignore[attr-defined]
                    return {table: int(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]) for table in operational_tables}

            before = counts()
            args = argparse.Namespace(
                command="copy-backtest", config="ignored.yaml", wallet=[WALLET], walk_forward=False,
                export=False, market_price_proxy=False,
            )
            with patch("src.copytrade.cli.CopyTradeConfig.from_yaml", return_value=cfg), patch("src.copytrade.cli._print"):
                self.assertEqual(run_copytrade_command(args), 0)
            self.assertEqual(counts(), before)
            with service.database._connect() as connection:  # type: ignore[attr-defined]
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM copy_backtest_runs").fetchone()[0], 1)

    def test_watcher_warms_market_cache_once_before_initial_reconcile(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            adapter = HyperliquidPublicAdapter(config(Path(temp)).source)
            calls: list[str] = []
            adapter.fetch_user_fills = lambda wallet: (_ for _ in ()).throw(AssertionError("watcher fallback reconcile must not run"))  # type: ignore[method-assign]
            watcher = HyperliquidWatcher(adapter)

            class Socket:
                async def __aenter__(self):
                    return self

                async def __aexit__(self, *args):
                    return None

                async def send(self, message):
                    return None

                async def recv(self):
                    return json.dumps({"channel": "allMids", "data": {"mids": {"BTC": "100"}}})

                async def ping(self):
                    return None

            async def on_fills(wallet, fills, is_snapshot):
                calls.append("fills")
                watcher.stop()

            async def on_market(payload):
                calls.append("market")

            async def gap_reconcile():
                calls.append("reconcile")
                await on_fills(WALLET, [], True)
                return {WALLET: 0}

            fake_websockets = types.SimpleNamespace(connect=lambda *args, **kwargs: Socket())
            with patch.dict(sys.modules, {"websockets": fake_websockets}):
                reconciled = asyncio.run(watcher.run(
                    [WALLET], on_fills, on_market=on_market, on_reconcile=gap_reconcile, duration_seconds=1,
                ))
            self.assertEqual(calls, ["market", "reconcile", "fills"])
            self.assertEqual(reconciled, {WALLET: 0})

    def test_enriched_events_drive_backtest_five_ten_twenty_and_ignore_future(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            cfg = config(Path(temp))
            service = CopyTradeService(cfg)
            times = [T0 + offset for offset in (1_000, 2_000, 3_000, 4_000, 5_000)]
            notionals = [100, 100, 30, 100, 200]
            for index, (when, notional) in enumerate(zip(times, notionals), 1):
                service.database.insert_snapshot(TraderSnapshot(f"equity{index}", WALLET, as_utc(when - 1), 1_000, None, None, {}, "live", {}))
                service.database.insert_raw_fill(raw(index, "B", notional / 100, 0, price=100, time_ms=when))
            events = service.reconstruct(WALLET)["events"]
            run = CopyTradeBacktester(cfg).run(events=events)
            self.assertEqual([item["allocation_fraction"] for item in run.summary["sizing_decisions"]], [.10, .10, .05, .10, .20])
            self.assertEqual(run.summary["equity_enrichment"]["usable_entry_count"], 5)

            future_wallet = "0x2222222222222222222222222222222222222222"
            service.database.insert_snapshot(TraderSnapshot("future", future_wallet, as_utc(T0 + 10_000), 1_000, None, None, {}, "live", {}))
            future = RawFill.from_hyperliquid({"coin": "BTC", "px": "100", "sz": "1", "side": "B", "time": T0,
                                                "startPosition": "0", "oid": 88, "tid": 88, "fee": "0"}, future_wallet)
            service.database.insert_raw_fill(future)
            self.assertEqual(service.reconstruct(future_wallet)["events"][0].equity_source, "missing")

    def test_historical_seed_rejects_stale_and_disallowed_equity(self) -> None:
        from dataclasses import replace as replace_dataclass
        from src.copytrade.models import PositionEvent, PositionEventType
        cfg = config(Path(tempfile.gettempdir()), sizing=SizingConfig(min_history=0, max_equity_age_seconds=10, accepted_equity_sources=("exact",)))
        base = PositionEvent("one", WALLET, "BTC", PositionEventType.OPEN, "long", 1, 0, 1, 100, 100,
                             as_utc(T0), "campaign", (), 1_000, 100, "exact", 0)
        stale = replace_dataclass(base, event_id="two", equity_age_seconds=11)
        disallowed = replace_dataclass(base, event_id="three", equity_source="recent_live_snapshot")
        classifier = TargetSizeClassifier(cfg.sizing)
        CopyTradeBacktester._seed_prior_size_history(classifier, [base, stale, disallowed])
        self.assertEqual(classifier._history[WALLET], [.1])

    def test_official_live_state_parses_and_enriches_following_open(self) -> None:
        from src.copytrade.service import _live_state_equity
        with tempfile.TemporaryDirectory() as temp:
            service = CopyTradeService(config(Path(temp)))
            payload = {"user": WALLET, "clearinghouseStates": {"": {"marginSummary": {"accountValue": "1234"}}}}
            asyncio.run(service.ingest_watched_state(WALLET, payload))
            observation = service.database.latest_prior_equity_observation(WALLET, utc_now() + timedelta(seconds=1))
            self.assertEqual(observation["account_value"], 1234)
            self.assertEqual(observation["positions"]["equity_parse_status"], "ok")
            current_ms = int((utc_now() + timedelta(milliseconds=20)).timestamp() * 1000)
            service.database.insert_raw_fill(raw(1, "B", 1, 0, time_ms=current_ms))
            event = service.reconstruct(WALLET)["events"][0]
            self.assertEqual((event.target_equity, event.equity_source), (1234, "recent_live_snapshot"))
            decision = TargetSizeClassifier(service.config.sizing).classify(
                WALLET, event.initial_delta_notional, event.target_equity, event.equity_source, event.equity_age_seconds,
            )
            self.assertAlmostEqual(decision.target_size_fraction or 0, 100 / 1234)

            self.assertEqual(_live_state_equity({"clearinghouseStates": {"": {"marginSummary": {"accountValue": "10"}}, "dex2": {"marginSummary": {"accountValue": "20"}}}})[0], 10)
            self.assertEqual(_live_state_equity({"clearinghouseStates": {"dex1": {"marginSummary": {"accountValue": "10"}}, "dex2": {"marginSummary": {"accountValue": "20"}}}})[2], "ambiguous_multiple_states")
            self.assertEqual(_live_state_equity({"clearinghouseStates": {"": {}}})[2], "missing_margin_account_value")
            asyncio.run(service.ingest_watched_state(WALLET, {"user": WALLET, "clearinghouseStates": {"a": {}, "b": {}}}))
            asyncio.run(service.ingest_watched_state(WALLET, {"user": WALLET, "clearinghouseStates": []}))
            asyncio.run(service.ingest_watched_state("0x3333333333333333333333333333333333333333", payload))
            latest = service.database.latest_prior_equity_observation(WALLET, utc_now() + timedelta(seconds=1))
            self.assertEqual(latest["account_value"], 1234)

    def test_unproven_coverage_warns_but_known_incomplete_blocks(self) -> None:
        candidate = CandidateConfig(history_days_min=0, closed_campaigns_min=0, require_positive_expectancy=False,
                                    require_positive_follower_expectancy=False, activity_max_age_days=99_999)
        metrics = calculate_trader_metrics(WALLET, [])
        metrics.raw["coverage_state"] = "PROVEN_COMPLETE"
        self.assertTrue(score_candidate(metrics, candidate, FollowerMetrics(expectancy=1)).eligible)
        metrics.raw["coverage_state"] = "UNPROVEN"
        unproven = score_candidate(metrics, candidate, FollowerMetrics(expectancy=1))
        self.assertTrue(unproven.eligible)
        self.assertIn("coverage_unproven", unproven.reasons)
        self.assertEqual(select_diverse_targets([unproven], {WALLET: {}}, target_count=1)[0].target_wallet, WALLET)
        metrics.raw["coverage_state"] = "KNOWN_INCOMPLETE"
        self.assertFalse(score_candidate(metrics, candidate, FollowerMetrics(expectancy=1)).eligible)

    def test_live_market_reference_mark_persistence_and_stale_skip(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            cfg = config(Path(temp), risk=replace(config(Path(temp)).risk, max_price_deviation_bps=200))
            service = CopyTradeService(cfg)
            service.database.upsert_target(Target(wallet=WALLET, status="active"))
            asyncio.run(service.ingest_market_update({"mids": {"BTC": "101"}}))
            current_ms = int(utc_now().timestamp() * 1000)
            asyncio.run(service.ingest_watched_fills(WALLET, [raw(1, "B", 1, 0, price=100, time_ms=current_ms)], False))
            execution = service.database.dashboard_snapshot()["execution_fills"][0]
            details = json.loads(execution["raw_json"])
            self.assertEqual(execution["price"], 101)
            self.assertEqual(details["market_reference_price"], 101)
            self.assertAlmostEqual(details["entry_deterioration_bps"], 100)
            attempts_before = len(service.database.dashboard_snapshot()["execution_attempts"])
            asyncio.run(service.ingest_market_update({"mids": {"BTC": "90"}}))
            sleeve = service.database.list_virtual_positions(open_only=True)[0]
            self.assertEqual(sleeve.current_mark, 90)
            self.assertLess(sleeve.unrealized_pnl, 0)
            self.assertGreater(service.database.latest_portfolio_snapshot()["max_drawdown_fraction"], 0)
            self.assertEqual(len(service.database.dashboard_snapshot()["execution_attempts"]), attempts_before)
            restarted = PaperExecutionEngine(cfg, service.database)
            restarted.restore(service.database.list_virtual_positions(), service.database.latest_portfolio_snapshot(), service.database.list_realized_results())
            self.assertEqual(next(iter(restarted.portfolio.sleeves.values())).current_mark, 90)

            short_service = CopyTradeService(config(Path(temp) / "short"))
            short_service.database.upsert_target(Target(wallet=WALLET, status="active"))
            asyncio.run(short_service.ingest_market_update({"mids": {"BTC": "100"}}))
            asyncio.run(short_service.ingest_watched_fills(WALLET, [raw(7, "A", 1, 0, price=100, time_ms=int(utc_now().timestamp() * 1000))], False))
            asyncio.run(short_service.ingest_market_update({"mids": {"BTC": "90"}}))
            self.assertGreater(short_service.database.list_virtual_positions(open_only=True)[0].unrealized_pnl, 0)

            stale = CopyTradeService(config(Path(temp) / "stale", paper_execution=replace(cfg.paper_execution, market_data_max_age_ms=0)))
            stale.database.upsert_target(Target(wallet=WALLET, status="active"))
            later = int(utc_now().timestamp() * 1000)
            asyncio.run(stale.ingest_watched_fills(WALLET, [raw(2, "B", 1, 0, price=100, time_ms=later)], False))
            self.assertEqual(stale.database.dashboard_snapshot()["execution_attempts"][0]["reason"], "stale_market_data")

    def test_entry_fee_is_in_risk_ledger_and_restores_without_double_count(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            cfg = config(root, paper_execution=replace(config(root).paper_execution, fee_rate=.01),
                         risk=replace(config(root).risk, target_loss_stop_fraction=.005))
            database = CopyTradeDatabase(cfg.artifacts.database_path)
            database.initialize()
            engine = PaperExecutionEngine(cfg, database)
            self.assertEqual(engine.process_signal(signal("fee-open", capital=100)).status, "filled")
            self.assertAlmostEqual(engine.portfolio.target_realized(WALLET), -1)
            self.assertAlmostEqual(engine.portfolio.daily_realized(T0), -1)
            self.assertEqual(engine.process_signal(signal("fee-stop", capital=10, at=T0 + 1)).reason, "target_loss_stop")
            engine.process_signal(signal("fee-reduce", action="reduce", price=50, qty=50, before=100, at=T0 + 2))
            engine.process_signal(signal("fee-close", action="close", price=50, qty=50, before=50, at=T0 + 3))
            sleeve = database.list_virtual_positions()[0]
            self.assertAlmostEqual(sleeve.realized_pnl - sleeve.entry_fee, -51.5)
            restarted = PaperExecutionEngine(cfg, database)
            ledger = database.list_realized_results()
            restarted.restore(database.list_virtual_positions(), database.latest_portfolio_snapshot(), ledger)
            self.assertAlmostEqual(restarted.portfolio.target_realized(WALLET), -51.5)
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
