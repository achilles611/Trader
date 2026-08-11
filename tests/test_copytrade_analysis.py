from __future__ import annotations

import json
import tempfile
import unittest
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from unittest.mock import patch

from src.copytrade.analysis import CandidateAnalysisPipeline
from src.copytrade.config import AnalysisConfig, ArtifactConfig, CandidateConfig, CopyTradeConfig, PaperExecutionConfig, RiskConfig, SizingConfig
from src.copytrade.discovery import DiscoveryPipeline
from src.copytrade.hyperliquid import BackfillCoverage
from src.copytrade.models import AnalysisRun, DiscoveryObservation, RawFill, as_utc, utc_now
from src.copytrade.service import CopyTradeService


GOOD = "0x1111111111111111111111111111111111111111"
FAILED = "0x2222222222222222222222222222222222222222"
STALE = "0x3333333333333333333333333333333333333333"
INCOMPLETE = "0x4444444444444444444444444444444444444444"
FILL_AT = utc_now() - timedelta(minutes=10)


class StaticProvider:
    source_name = "phase_b_fixture"

    def __init__(self, observations: list[DiscoveryObservation]) -> None:
        self.observations = observations

    def discover(self, *, refresh: bool = False):
        return iter(self.observations)


def config(root: Path) -> CopyTradeConfig:
    return CopyTradeConfig(
        artifacts=ArtifactConfig(database_path=root / "analysis.sqlite3", obsidian_root=root / "obsidian"),
        sizing=SizingConfig(min_history=1, max_equity_age_seconds=86_400),
        paper_execution=PaperExecutionConfig(fee_rate=0, slippage_bps=0, min_order_notional=1, random_seed=1),
        risk=RiskConfig(kill_switch_path=root / "kill", max_total_committed_fraction=1, max_capital_per_target_fraction=1,
                        max_capital_per_symbol_fraction=1, max_signal_age_seconds=86_400),
        candidates=CandidateConfig(history_days_min=0, closed_campaigns_min=0, max_drawdown_hard=1,
                                   require_positive_expectancy=False, require_positive_follower_expectancy=False,
                                   activity_max_age_days=30),
        analysis=AnalysisConfig(default_workers=2, retry_attempts=2, retry_initial_seconds=0, history_days=90,
                                min_discovery_activity=1, shadow_finalist_count=2),
    )


def fills(wallet: str) -> list[RawFill]:
    start = FILL_AT
    return [
        RawFill.from_hyperliquid({"coin": "BTC", "px": "100", "sz": "1", "side": "B", "time": int(start.timestamp() * 1000),
                                  "startPosition": "0", "oid": 1, "tid": 1, "fee": "0", "accountValue": "1000"}, wallet),
        RawFill.from_hyperliquid({"coin": "BTC", "px": "110", "sz": "1", "side": "A", "time": int((start + timedelta(minutes=1)).timestamp() * 1000),
                                  "startPosition": "1", "oid": 2, "tid": 2, "fee": "0", "accountValue": "1000"}, wallet),
    ]


def seed_candidates(service: CopyTradeService, wallets: list[str], *, stale: set[str] | None = None) -> None:
    stale = stale or set()
    now = utc_now()
    observations = [
        DiscoveryObservation(wallet, "phase_b_fixture", now, now - timedelta(days=31) if wallet in stale else now,
                             evidence_id=f"{wallet}-event")
        for wallet in wallets
    ]
    DiscoveryPipeline(service.database).run(StaticProvider(observations), limit=100, min_activity=1, max_activity_age=None)


def execution_counts(service: CopyTradeService) -> dict[str, int]:
    tables = ("copy_signals", "copy_execution_claims", "copy_execution_attempts", "copy_execution_fills", "copy_virtual_positions", "copy_portfolio_snapshots")
    with service.database._connect() as connection:  # type: ignore[attr-defined]
        return {table: int(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]) for table in tables}


class PhaseBAnalysisTests(unittest.TestCase):
    def test_prefilter_rejects_stale_candidate_with_explainable_reason(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            service = CopyTradeService(config(Path(temp)))
            seed_candidates(service, [STALE], stale={STALE})
            result = CandidateAnalysisPipeline(service).run(limit=10, cheap_only=True)
            self.assertEqual((result["wallets_considered"], result["cheap_rejected"], result["rejected"]), (1, 1, 1))
            analysis = service.database.get_candidate_analysis(STALE)
            self.assertEqual(analysis.lifecycle_status, "prefilter_rejected")  # type: ignore[union-attr]
            self.assertIn("inactive", analysis.prefilter_reasons)  # type: ignore[union-attr]

    def test_analysis_is_resumable_forceable_and_isolated_from_execution_tables(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            service = CopyTradeService(config(Path(temp)))
            seed_candidates(service, [GOOD])
            calls: list[str] = []

            def backfill(wallet: str, start: object) -> dict[str, object]:
                calls.append(wallet)
                service.database.insert_raw_fills(fills(wallet))
                return {"new_raw_fills": 2}

            before = execution_counts(service)
            pipeline = CandidateAnalysisPipeline(service, backfill_wallet=backfill, sleep=lambda _: None)
            first = pipeline.run(limit=10, workers=2)
            self.assertEqual(first["eligible"], 1)
            self.assertEqual(calls, [GOOD])
            self.assertEqual(service.database.get_target(GOOD).status, "new")  # type: ignore[union-attr]
            analysis = service.database.get_candidate_analysis(GOOD)
            self.assertEqual(analysis.lifecycle_status, "qualified")  # type: ignore[union-attr]
            self.assertEqual(analysis.summary["latency"]["status"], "unavailable")  # type: ignore[union-attr]
            self.assertEqual(analysis.summary["follower"]["sizing"]["usable_entry_count"], 1)  # type: ignore[union-attr]
            self.assertEqual(execution_counts(service), before)

            second = pipeline.run(limit=10)
            self.assertEqual(second["deferred"], 1)
            self.assertEqual(calls, [GOOD])
            forced = pipeline.run(limit=10, force=True)
            self.assertEqual(forced["scored"], 1)
            self.assertEqual(calls, [GOOD, GOOD])
            self.assertEqual(len(service.database.list_raw_fills(GOOD)), 2)

    def test_failed_wallet_does_not_abort_other_candidates_and_can_retry(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            service = CopyTradeService(config(Path(temp)))
            seed_candidates(service, [GOOD, FAILED])
            failures = {FAILED: 2}

            def backfill(wallet: str, start: object) -> dict[str, object]:
                if failures.get(wallet, 0):
                    failures[wallet] -= 1
                    raise RuntimeError("temporary public API failure")
                service.database.insert_raw_fills(fills(wallet))
                return {"new_raw_fills": 2}

            pipeline = CandidateAnalysisPipeline(service, backfill_wallet=backfill, sleep=lambda _: None)
            result = pipeline.run(limit=10, workers=2)
            self.assertEqual((result["backfill_failed"], result["eligible"]), (1, 1))
            self.assertEqual(service.database.get_candidate_analysis(FAILED).lifecycle_status, "backfill_failed")  # type: ignore[union-attr]
            retry = pipeline.run(limit=10)
            self.assertEqual(retry["eligible"], 1)
            self.assertEqual(service.database.get_candidate_analysis(FAILED).lifecycle_status, "qualified")  # type: ignore[union-attr]

    def test_known_incomplete_coverage_quarantines_without_follower_replay(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            service = CopyTradeService(config(Path(temp)))
            seed_candidates(service, [INCOMPLETE])

            def backfill(wallet: str, start: object) -> dict[str, object]:
                service.database.insert_backfill_coverage(wallet, BackfillCoverage(
                    requested_start=start, requested_end=utc_now(), earliest_observed_fill=None, latest_observed_fill=None,
                    source_limit_detected=True, coverage_complete=False, coverage_quality="fixture", coverage_state="KNOWN_INCOMPLETE",
                ))
                raise RuntimeError("dense public interval")

            result = CandidateAnalysisPipeline(service, backfill_wallet=backfill).run(limit=10)
            self.assertEqual((result["rejected"], result["scored"]), (1, 0))
            analysis = service.database.get_candidate_analysis(INCOMPLETE)
            self.assertEqual(analysis.lifecycle_status, "quarantined")  # type: ignore[union-attr]
            self.assertIn("known_incomplete", analysis.prefilter_reasons)  # type: ignore[union-attr]

    def test_resume_uses_interrupted_run_and_cli_status_is_machine_readable(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            cfg = config(Path(temp))
            service = CopyTradeService(cfg)
            seed_candidates(service, [GOOD])
            interrupted = AnalysisRun("analysis_interrupted", utc_now(), {"fixture": True})
            service.database.start_analysis_run(interrupted)

            def backfill(wallet: str, start: object) -> dict[str, object]:
                service.database.insert_raw_fills(fills(wallet))
                return {"new_raw_fills": 2}

            result = CandidateAnalysisPipeline(service, backfill_wallet=backfill).run(limit=10, resume=True)
            self.assertEqual(result["run_id"], interrupted.run_id)
            with patch("src.copytrade.cli.CopyTradeConfig.from_yaml", return_value=cfg), patch("src.copytrade.cli._print") as printed:
                from src.copytrade.cli import run_copytrade_command
                import argparse
                self.assertEqual(run_copytrade_command(argparse.Namespace(command="copy-analysis-status", config="ignored", limit=10)), 0)
            payload = printed.call_args.args[0]
            self.assertIn("candidates", payload)
            self.assertEqual(payload["candidates"][0]["wallet"], GOOD)


if __name__ == "__main__":
    unittest.main()
