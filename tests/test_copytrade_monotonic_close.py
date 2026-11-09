from __future__ import annotations

import asyncio
import copy
import sqlite3
import tempfile
import threading
import unittest
from dataclasses import replace
from pathlib import Path

from src.copytrade.config import ArtifactConfig, CopyTradeConfig, PaperExecutionConfig, RiskConfig, SizingConfig
from src.copytrade.control_center import CopyControlCenter
from src.copytrade.discovery import DiscoveryPipeline
from src.copytrade.analysis import _config_fingerprint
from src.copytrade.models import AnalysisRun, CandidateAnalysis, CandidateScore, CopySignal, DiscoveryObservation, RawFill, Target, as_utc, stable_id, utc_now
from src.copytrade.paper import PaperExecutionEngine
from src.copytrade.service import CopyTradeService
from src.copytrade.storage import CopyTradeDatabase


WALLET = "0x7777777777777777777777777777777777777777"


def config(root: Path) -> CopyTradeConfig:
    return CopyTradeConfig(
        artifacts=ArtifactConfig(database_path=root / "monotonic.sqlite3", obsidian_root=root / "obsidian"),
        sizing=SizingConfig(min_history=1),
        paper_execution=PaperExecutionConfig(fee_rate=0, slippage_bps=0, min_order_notional=1, random_seed=1),
        risk=RiskConfig(
            kill_switch_path=root / "kill", max_total_committed_fraction=1, max_capital_per_target_fraction=1,
            max_capital_per_symbol_fraction=1, max_signal_age_seconds=86_400,
        ),
    )


def signal(name: str, *, action: str = "open", quantity: float = 1, before: float = 0) -> CopySignal:
    now = utc_now()
    return CopySignal(
        signal_id=stable_id("monotonic", name, action, now), target_wallet=WALLET, campaign_id="campaign",
        source_event_id=name, symbol="BTC", action=action, direction="long", target_price=100,
        target_quantity=quantity, target_notional=quantity * 100, allocation_fraction=0.1, requested_capital=100,
        created_at=now, source_event_timestamp=now, target_position_before=before,
    )


def raw(tid: int, side: str, position_before: float) -> RawFill:
    now = utc_now()
    return RawFill.from_hyperliquid({
        "coin": "BTC", "px": "100", "sz": "1", "side": side, "time": int(now.timestamp() * 1000),
        "startPosition": str(position_before), "oid": tid, "tid": tid, "fee": "0", "accountValue": "1000",
    }, WALLET)


class MonotonicPaperCloseTests(unittest.TestCase):
    def _service_with_open_sleeve(self, root: Path) -> tuple[CopyTradeService, CopyControlCenter]:
        service = CopyTradeService(config(root))
        # Fixture-only state: these tests exercise execution persistence, not
        # Phase-C activation authority (covered separately).
        service.database.upsert_target(Target(wallet=WALLET, status="active"))
        asyncio.run(service.ingest_market_update({"mids": {"BTC": "100"}}))
        asyncio.run(service.ingest_watched_fills(WALLET, [raw(1, "B", 0)], False))
        self.assertEqual(len(service.database.list_virtual_positions(open_only=True)), 1)
        return service, CopyControlCenter(service.config, service.database, execution_service=service)

    @staticmethod
    def _closed_row(database: CopyTradeDatabase) -> sqlite3.Row:
        connection = sqlite3.connect(database.path)
        try:
            connection.row_factory = sqlite3.Row
            row = connection.execute("SELECT * FROM copy_virtual_positions").fetchone()
        finally:
            connection.close()
        assert row is not None
        return row

    def test_mark_storage_cannot_resurrect_or_distort_a_closed_sleeve(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            cfg = config(Path(temp))
            database = CopyTradeDatabase(cfg.artifacts.database_path)
            database.initialize()
            engine = PaperExecutionEngine(cfg, database)
            self.assertEqual(engine.process_signal(signal("open")).status, "filled")
            stale = copy.deepcopy(next(iter(engine.portfolio.sleeves.values())))
            self.assertEqual(engine.process_signal(signal("close", action="close", quantity=1, before=1)).status, "filled")
            before = self._closed_row(database)
            stale.current_mark, stale.unrealized_pnl, stale.max_drawdown = 80, -20, 20
            self.assertEqual(database.persist_portfolio_mark([stale], engine._snapshot(), timestamp=utc_now()), 0)
            after = self._closed_row(database)
            self.assertIsNotNone(after["closed_at"])
            self.assertEqual(after["quantity"], 0)
            for field in ("remaining_capital", "realized_pnl", "exit_fee", "closed_at"):
                self.assertEqual(after[field], before[field])
            self.assertEqual(database.list_virtual_positions(open_only=True), [])
            self.assertEqual(database.latest_portfolio_snapshot()["committed_capital"], 0)  # type: ignore[index]

    def test_stale_service_marks_after_control_close_all_never_reopen(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            service, center = self._service_with_open_sleeve(Path(temp))
            self.assertEqual(center.close_all_paper_positions()["status"], "completed")
            for price in (99, 101, 98, 102, 97):
                asyncio.run(service.ingest_market_update({"mids": {"BTC": str(price)}}))
            row = self._closed_row(service.database)
            self.assertIsNotNone(row["closed_at"])
            self.assertEqual((row["quantity"], row["remaining_capital"]), (0, 0))
            self.assertEqual(service.database.list_virtual_positions(open_only=True), [])

    def test_in_process_control_center_fallback_shares_existing_execution_authority(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            service, _ = self._service_with_open_sleeve(Path(temp))
            fallback = CopyControlCenter(service.config, service.database)
            fallback_service = fallback._paper_service()
            self.assertIs(fallback_service._live_engine, service._live_engine)
            self.assertEqual(fallback.close_all_paper_positions()["status"], "completed")
            self.assertEqual(service.database.list_virtual_positions(open_only=True), [])

    def test_restart_and_repeated_close_all_preserve_database_truth(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            service, center = self._service_with_open_sleeve(root)
            first = center.close_all_paper_positions()
            second = center.close_all_paper_positions()
            self.assertEqual((first["status"], second["status"]), ("completed", "completed"))
            before = self._closed_row(service.database)
            restarted = CopyTradeService(config(root))
            restarted.reload_execution_state()
            for price in (103, 96, 100):
                asyncio.run(restarted.ingest_market_update({"mids": {"BTC": str(price)}}))
            after = self._closed_row(restarted.database)
            self.assertEqual(after["closed_at"], before["closed_at"])
            self.assertEqual((after["quantity"], after["remaining_capital"], after["realized_pnl"]),
                             (before["quantity"], before["remaining_capital"], before["realized_pnl"]))
            self.assertEqual(restarted.database.list_virtual_positions(open_only=True), [])

    def test_mark_and_close_race_is_monotonic(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            service, center = self._service_with_open_sleeve(Path(temp))
            barrier = threading.Barrier(2)
            failures: list[BaseException] = []

            def mark() -> None:
                try:
                    barrier.wait()
                    asyncio.run(service.ingest_market_update({"mids": {"BTC": "95"}}))
                except BaseException as exc:  # collect worker failures for the test thread
                    failures.append(exc)

            def close() -> None:
                try:
                    barrier.wait()
                    center.close_all_paper_positions()
                except BaseException as exc:
                    failures.append(exc)

            workers = [threading.Thread(target=mark), threading.Thread(target=close)]
            for worker in workers: worker.start()
            for worker in workers: worker.join()
            self.assertEqual(failures, [])
            self.assertEqual(service.database.list_virtual_positions(open_only=True), [])
            self.assertIsNotNone(self._closed_row(service.database)["closed_at"])

    def test_source_fill_and_close_race_leaves_no_open_sleeve(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            service, center = self._service_with_open_sleeve(Path(temp))
            barrier = threading.Barrier(2)
            failures: list[BaseException] = []

            def source_close() -> None:
                try:
                    barrier.wait()
                    asyncio.run(service.ingest_watched_fills(WALLET, [raw(2, "A", 1)], False))
                except BaseException as exc:
                    failures.append(exc)

            def control_close() -> None:
                try:
                    barrier.wait()
                    center.close_all_paper_positions()
                except BaseException as exc:
                    failures.append(exc)

            workers = [threading.Thread(target=source_close), threading.Thread(target=control_close)]
            for worker in workers: worker.start()
            for worker in workers: worker.join()
            self.assertEqual(failures, [])
            self.assertEqual(service.database.list_virtual_positions(open_only=True), [])
            connection = sqlite3.connect(service.database.path)
            try:
                open_count = connection.execute("SELECT COUNT(*) FROM copy_virtual_positions WHERE closed_at IS NULL").fetchone()[0]
                duplicates = connection.execute("SELECT COUNT(*) FROM copy_virtual_positions").fetchone()[0]
            finally:
                connection.close()
            self.assertEqual((open_count, duplicates), (0, 1))

    def test_temporary_database_paper_rehearsal_survives_authority_restart_and_traffic(self) -> None:
        """Deterministic Phase A -> authority -> PAPER -> restart rehearsal."""
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            service = CopyTradeService(config(root))
            now = utc_now()

            class FixtureDiscovery:
                source_name = "rehearsal"

                def discover(self, *, refresh: bool = False):
                    return iter((DiscoveryObservation(WALLET, self.source_name, now, now, evidence_id="rehearsal-a"),))

            DiscoveryPipeline(service.database).run(FixtureDiscovery(), limit=10, min_activity=1, max_activity_age=None)
            fingerprint = _config_fingerprint(service.config.research_snapshot())
            run_id = "rehearsal-phase-b"
            service.database.start_analysis_run(AnalysisRun(run_id, now, {"fixture": "rehearsal"}))
            service.database.finish_analysis_run(
                run_id, status="completed", wallets_considered=1, cheap_rejected=0, backfill_attempted=1,
                backfill_failed=0, reconstructed=1, scored=1, eligible=1, rejected=0, deferred=0,
            )
            service.database.upsert_candidate_analysis(CandidateAnalysis(
                WALLET, "qualified", run_id, now, now, summary={"coverage": {"coverage_state": "PROVEN_COMPLETE"}},
            ))
            service.database.upsert_candidate_score(CandidateScore(
                WALLET, now, 90, {"fixture": 90}, {}, True, provenance="phase_b", analysis_run_id=run_id,
                config_fingerprint=fingerprint, confidence_score=90,
            ))
            service.database.upsert_finalist_recommendations(fingerprint, ({
                "analysis_run_id": run_id, "wallet": WALLET, "finalist_eligible": True,
                "finalist_rejection_reasons": (), "diversification_penalty": 0, "final_selection_score": 90,
                "selection_rank": 1,
            },))
            center = CopyControlCenter(service.config, service.database, execution_service=service)
            self.assertEqual(center.set_operator_state(WALLET, "active")["operator_state"], "active")

            asyncio.run(service.ingest_market_update({"mids": {"BTC": "100"}}))
            asyncio.run(service.ingest_watched_fills(WALLET, [raw(10, "B", 0)], False))
            self.assertEqual(len(service.database.list_virtual_positions(open_only=True)), 1)
            center.pause_entries()
            # Exit signals remain permitted while entry control is paused.
            asyncio.run(service.ingest_watched_fills(WALLET, [raw(11, "A", 1)], False))
            self.assertEqual(service.database.list_virtual_positions(open_only=True), [])

            center.resume_entries()
            asyncio.run(service.ingest_market_update({"mids": {"BTC": "100"}}))
            asyncio.run(service.ingest_watched_fills(WALLET, [raw(12, "B", 0)], False))
            center.pause_entries()
            self.assertEqual(center.close_all_paper_positions()["status"], "completed")

            restarted = CopyTradeService(config(root))
            restarted.reload_execution_state()
            asyncio.run(restarted.ingest_market_update({"mids": {"BTC": "95"}}))
            asyncio.run(restarted.ingest_watched_fills(WALLET, [raw(11, "A", 1)], False))
            self.assertEqual(restarted.database.list_virtual_positions(open_only=True), [])
            self.assertEqual(CopyControlCenter(restarted.config, restarted.database, execution_service=restarted).portfolio_summary()["committed_capital"], 0)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
