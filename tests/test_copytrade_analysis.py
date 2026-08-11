from __future__ import annotations

import json
import tempfile
import unittest
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from unittest.mock import patch

from src.copytrade.analysis import CandidateAnalysisPipeline, _copyability, _walk_forward_evidence
from src.copytrade.config import AnalysisConfig, ArtifactConfig, CandidateConfig, CopyTradeConfig, PaperExecutionConfig, RiskConfig, SizingConfig
from src.copytrade.discovery import DiscoveryPipeline
from src.copytrade.hyperliquid import BackfillCoverage
from src.copytrade.models import CandidateScore, DiscoveryObservation, RawFill, as_utc, utc_now
from src.copytrade.scoring import FollowerMetrics, pairwise_correlation_status, score_candidate
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

            reconstruction_calls = 0
            def reconstruct(wallet: str) -> dict[str, object]:
                nonlocal reconstruction_calls
                reconstruction_calls += 1
                return service.reconstruct(wallet)

            before = execution_counts(service)
            pipeline = CandidateAnalysisPipeline(service, backfill_wallet=backfill, reconstruct_wallet=reconstruct, sleep=lambda _: None)
            first = pipeline.run(limit=10, workers=2)
            self.assertEqual(first["eligible"], 1)
            self.assertEqual(calls, [GOOD])
            self.assertEqual(reconstruction_calls, 1)
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
            interrupted = {"value": True}

            def backfill(wallet: str, start: object) -> dict[str, object]:
                if interrupted["value"]:
                    interrupted["value"] = False
                    raise KeyboardInterrupt("simulated interruption")
                service.database.insert_raw_fills(fills(wallet))
                return {"new_raw_fills": 2}

            pipeline = CandidateAnalysisPipeline(service, backfill_wallet=backfill)
            with self.assertRaises(KeyboardInterrupt):
                pipeline.run(limit=10)
            run_id = service.database.latest_resumable_analysis_run()["run_id"]  # type: ignore[index]
            result = pipeline.run(limit=999, status="all", workers=1, resume=True)
            self.assertEqual(result["run_id"], run_id)
            with patch("src.copytrade.cli.CopyTradeConfig.from_yaml", return_value=cfg), patch("src.copytrade.cli._print") as printed:
                from src.copytrade.cli import run_copytrade_command
                import argparse
                self.assertEqual(run_copytrade_command(argparse.Namespace(command="copy-analysis-status", config="ignored", limit=10)), 0)
            payload = printed.call_args.args[0]
            self.assertIn("candidates", payload)
            self.assertEqual(payload["candidates"][0]["wallet"], GOOD)

    def test_finalists_require_current_qualified_phase_b_score_and_respect_operator_state(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            service = CopyTradeService(config(Path(temp)))
            seed_candidates(service, [GOOD, STALE, INCOMPLETE], stale={STALE})

            def backfill(wallet: str, start: object) -> dict[str, object]:
                if wallet == INCOMPLETE:
                    service.database.insert_backfill_coverage(wallet, BackfillCoverage(
                        requested_start=start, requested_end=utc_now(), earliest_observed_fill=None, latest_observed_fill=None,
                        source_limit_detected=True, coverage_complete=False, coverage_quality="fixture", coverage_state="KNOWN_INCOMPLETE",
                    ))
                    raise RuntimeError("dense interval")
                service.database.insert_raw_fills(fills(wallet))
                return {"new_raw_fills": 2}

            pipeline = CandidateAnalysisPipeline(service, backfill_wallet=backfill)
            first = pipeline.run(limit=10)
            self.assertEqual([row["wallet"] for row in first["shadow_finalists"]], [GOOD])
            # A legacy one-wallet score is not Phase B proof and cannot enter.
            service.database.upsert_candidate_score(CandidateScore(STALE, utc_now(), 99, {}, {}, True))
            self.assertEqual([row["wallet"] for row in pipeline.shadow_finalists()], [GOOD])
            # A later legacy score cannot replace the run-stamped finalist score.
            prior_score = first["shadow_finalists"][0]["score"]
            service.database.upsert_candidate_score(CandidateScore(GOOD, utc_now(), 99, {}, {}, True))
            self.assertEqual(pipeline.shadow_finalists()[0]["score"], prior_score)
            service.set_status(GOOD, "muted")
            self.assertEqual(pipeline.shadow_finalists(), [])

    def test_window_coverage_requires_contiguous_proven_segments(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            service = CopyTradeService(config(Path(temp)))
            start, end = utc_now() - timedelta(days=10), utc_now()
            def coverage(begin: object, finish: object, state: str) -> None:
                service.database.insert_backfill_coverage(GOOD, BackfillCoverage(
                    requested_start=begin, requested_end=finish, earliest_observed_fill=None, latest_observed_fill=None,
                    source_limit_detected=state == "KNOWN_INCOMPLETE", coverage_complete=state == "PROVEN_COMPLETE",
                    coverage_quality="fixture", coverage_state=state,
                ))
            coverage(start, end, "PROVEN_COMPLETE")
            self.assertEqual(service.database.analysis_window_coverage(GOOD, start, end)["coverage_state"], "PROVEN_COMPLETE")
            # A fresh database proves neither an old raw fill nor a small recent segment.
            service2 = CopyTradeService(config(Path(temp) / "other"))
            coverage_db = service2.database
            coverage_db.insert_backfill_coverage(GOOD, BackfillCoverage(
                requested_start=end - timedelta(minutes=5), requested_end=end, earliest_observed_fill=None, latest_observed_fill=None,
                source_limit_detected=False, coverage_complete=True, coverage_quality="fixture", coverage_state="PROVEN_COMPLETE",
            ))
            self.assertEqual(coverage_db.analysis_window_coverage(GOOD, start, end)["coverage_state"], "UNPROVEN")
            coverage_db.insert_backfill_coverage(GOOD, BackfillCoverage(
                requested_start=start, requested_end=end - timedelta(days=2), earliest_observed_fill=None, latest_observed_fill=None,
                source_limit_detected=False, coverage_complete=False, coverage_quality="fixture", coverage_state="UNPROVEN",
            ))
            self.assertEqual(coverage_db.analysis_window_coverage(GOOD, start, end)["coverage_state"], "UNPROVEN")
            service3 = CopyTradeService(config(Path(temp) / "disjoint"))
            for left, right in ((start, start + timedelta(days=4)), (start + timedelta(days=6), end)):
                service3.database.insert_backfill_coverage(GOOD, BackfillCoverage(
                    requested_start=left, requested_end=right, earliest_observed_fill=None, latest_observed_fill=None,
                    source_limit_detected=False, coverage_complete=True, coverage_quality="fixture", coverage_state="PROVEN_COMPLETE",
                ))
            self.assertEqual(service3.database.analysis_window_coverage(GOOD, start, end)["coverage_state"], "UNPROVEN")
            coverage_db.insert_backfill_coverage(GOOD, BackfillCoverage(
                requested_start=start + timedelta(days=3), requested_end=start + timedelta(days=4), earliest_observed_fill=None, latest_observed_fill=None,
                source_limit_detected=True, coverage_complete=False, coverage_quality="fixture", coverage_state="KNOWN_INCOMPLETE",
            ))
            self.assertEqual(coverage_db.analysis_window_coverage(GOOD, start, end)["coverage_state"], "KNOWN_INCOMPLETE")

    def test_resume_manifest_counters_and_append_only_events_survive_crash(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            cfg = config(Path(temp))
            service = CopyTradeService(cfg)
            seed_candidates(service, [GOOD, FAILED])
            def backfill(wallet: str, start: object) -> dict[str, object]:
                service.database.insert_raw_fills(fills(wallet))
                return {"new_raw_fills": 2}
            def crash_on_second(wallet: str) -> dict[str, object]:
                if wallet == FAILED:
                    raise KeyboardInterrupt("crash after first completed wallet")
                return service.reconstruct(wallet)
            first = CandidateAnalysisPipeline(service, backfill_wallet=backfill, reconstruct_wallet=crash_on_second)
            with self.assertRaises(KeyboardInterrupt):
                first.run(limit=10, workers=1)
            run_id = service.database.latest_resumable_analysis_run()["run_id"]  # type: ignore[index]
            resumed = CandidateAnalysisPipeline(service, backfill_wallet=backfill).run(resume=True, limit=1, status="all", workers=1)
            self.assertEqual(resumed["run_id"], run_id)
            self.assertEqual((resumed["wallets_considered"], resumed["backfill_attempted"], resumed["reconstructed"], resumed["scored"], resumed["eligible"], resumed["deferred"]), (2, 2, 2, 2, 2, 0))
            events = service.database.list_analysis_wallet_events(run_id, GOOD)
            self.assertEqual([event["stage"] for event in events], ["prefilter", "backfill", "backfill", "analysis"])
            self.assertEqual(resumed["shadow_finalists"][1]["diversification"]["symbol_overlap"], 1.0)

    def test_resume_uses_original_manifest_and_rejects_changed_configuration(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            cfg = config(root)
            service = CopyTradeService(cfg)
            seed_candidates(service, [GOOD])
            service.set_status(GOOD, "pending")
            def crash(wallet: str, start: object) -> dict[str, object]:
                raise KeyboardInterrupt("stop")
            with self.assertRaises(KeyboardInterrupt):
                CandidateAnalysisPipeline(service, backfill_wallet=crash).run(status="pending", limit=7, workers=1)
            run = service.database.latest_resumable_analysis_run()
            manifest = json.loads(run["configuration_json"])  # type: ignore[index]
            self.assertEqual((manifest["invocation"]["status"], manifest["invocation"]["limit"]), ("pending", 7))
            incompatible = CopyTradeService(replace(cfg, analysis=replace(cfg.analysis, history_days=91)))
            with self.assertRaisesRegex(ValueError, "changed copy-trading configuration"):
                CandidateAnalysisPipeline(incompatible).run(resume=True)

    def test_follower_risk_copyability_walk_forward_liquidation_and_correlation_semantics(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            service = CopyTradeService(config(Path(temp)))
            service.import_wallets([GOOD])
            service.database.insert_raw_fills(fills(GOOD))
            metrics = service.reconstruct(GOOD)["metrics"]
            strict = replace(service.config.candidates, max_follower_drawdown_hard=0.10, max_follower_drawdown_preferred=0.05)
            unsafe = score_candidate(metrics, strict, FollowerMetrics(expectancy=1, max_drawdown=0.11))
            self.assertFalse(unsafe.eligible)
            self.assertIn("follower_drawdown_hard_limit", unsafe.reasons)
            unavailable = _copyability(100, {"drawdown_denominator": 1000}, {"filled_attempts": 1, "return_fraction": 0.1}, {})
            self.assertEqual(unavailable["reason"], "target_equity_denominator_unavailable")
            available = _copyability(100, {"copyability_capital_denominator": 1000, "copyability_capital_source": "source_fill", "copyability_capital_quality": "genuine_usable_target_equity"}, {"filled_attempts": 1, "return_fraction": 0.1}, {})
            self.assertEqual(available["status"], "available")
            stable = _walk_forward_evidence([{"forward_return_fraction": 0.04}, {"forward_return_fraction": 0.03}], 2)
            short = _walk_forward_evidence([{"forward_return_fraction": 0.04}], 2)
            self.assertEqual((stable["status"], short["status"]), ("available", "unavailable"))
            scored_forward = score_candidate(metrics, service.config.candidates, FollowerMetrics(expectancy=1, walk_forward_score=stable["score"], walk_forward_status="available", walk_forward_window_count=2))
            scored_short = score_candidate(metrics, service.config.candidates, FollowerMetrics(expectancy=1))
            self.assertIn("walk_forward", scored_forward.component_scores)
            self.assertNotIn("walk_forward", scored_short.component_scores)
            self.assertIn("walk_forward_unavailable", scored_short.reasons)
            metrics.raw["liquidation_frequency"] = 0.2
            liquidation = score_candidate(metrics, service.config.candidates, FollowerMetrics(expectancy=1))
            self.assertIn("liquidation", liquidation.penalties)
            self.assertIn("liquidation_frequency_hard_limit", liquidation.reasons)
            correlation = pairwise_correlation_status({"2026-01-01": 0.1}, {"2026-01-01": 0.1})
            self.assertEqual((correlation["status"], correlation["correlation"]), ("insufficient_history", None))

    def test_global_status_counts_and_explicit_reporting_metrics(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            service = CopyTradeService(config(Path(temp)))
            extra = "0x5555555555555555555555555555555555555555"
            seed_candidates(service, [GOOD, FAILED, extra])
            status = CandidateAnalysisPipeline(service).status(limit=1)
            self.assertEqual((status["total_candidates"], len(status["candidates"])), (3, 1))
            service.import_wallets([GOOD])
            service.database.insert_raw_fills(fills(GOOD))
            metrics = service.reconstruct(GOOD)["metrics"]
            from src.copytrade.analysis import _target_summary
            summary = _target_summary(metrics, service.database.list_campaigns(GOOD), service.database.list_position_events(GOOD))
            self.assertIn("max_drawdown_fraction", summary["risk"])
            self.assertIn("average_drawdown_dollars", summary["risk"])
            self.assertEqual(summary["profitability"]["median_campaign_pnl"], 10.0)


if __name__ == "__main__":
    unittest.main()
