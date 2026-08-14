from __future__ import annotations

import tempfile
import unittest
from dataclasses import replace
from datetime import timedelta
from pathlib import Path

from src.copytrade.analysis import CandidateAnalysisPipeline, _friction_evidence, _regime_evidence
from src.copytrade.analytics import calculate_trader_metrics
from src.copytrade.config import AnalysisConfig, ArtifactConfig, CandidateConfig, ConfidenceConfig, CopyTradeConfig, FinalistRequirementsConfig, PaperExecutionConfig, PrefilterConfig, RegimeConfig, RiskConfig, SizingConfig
from src.copytrade.discovery import DiscoveryPipeline
from src.copytrade.models import AnalysisRun, CandidateAnalysis, CandidateScore, DiscoveryObservation, PositionCampaign, RawFill, TraderMetrics, utc_now
from src.copytrade.scoring import FollowerMetrics, score_candidate, suitability_confidence
from src.copytrade.service import CopyTradeService


STEADY = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
JACKPOT = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
WHALE = "0xcccccccccccccccccccccccccccccccccccccccc"


class StaticProvider:
    source_name = "suitability_fixture"

    def __init__(self, observations):
        self.observations = observations

    def discover(self, *, refresh: bool = False):
        return iter(self.observations)


def test_config(root: Path) -> CopyTradeConfig:
    return CopyTradeConfig(
        artifacts=ArtifactConfig(database_path=root / "suitability.sqlite3", obsidian_root=root / "obsidian"),
        sizing=SizingConfig(min_history=1, max_equity_age_seconds=86_400),
        paper_execution=PaperExecutionConfig(fee_rate=0, slippage_bps=0, min_order_notional=1, random_seed=1),
        risk=RiskConfig(kill_switch_path=root / "kill", max_total_committed_fraction=1, max_capital_per_target_fraction=1,
                        max_capital_per_symbol_fraction=1, max_signal_age_seconds=86_400),
        candidates=CandidateConfig(history_days_min=0, closed_campaigns_min=0, max_drawdown_hard=1,
                                   max_follower_drawdown_hard=1, require_positive_expectancy=False,
                                   require_positive_follower_expectancy=False, pnl_concentration_hard=1),
        prefilter=PrefilterConfig(min_activity_observations=1),
        confidence=ConfidenceConfig(closed_campaigns_reference=4, active_days_reference=4, history_days_reference=4,
                                    walk_forward_windows_reference=2, regimes_reference=2),
        regimes=RegimeConfig(minimum_campaigns_per_regime=2),
        analysis=AnalysisConfig(default_workers=1, retry_attempts=1, retry_initial_seconds=0, history_days=30,
                                min_discovery_activity=1, shadow_finalist_count=2, high_suitability_score=50,
                                market_evidence_enabled=False),
        finalist_requirements=FinalistRequirementsConfig(minimum_confidence_score=0, require_copyability_evidence=False),
    )


def campaigns(wallet: str, pnls: list[float], *, symbols: tuple[str, ...] = ("BTC", "ETH")) -> list[PositionCampaign]:
    start = utc_now() - timedelta(days=len(pnls) + 2)
    result: list[PositionCampaign] = []
    for index, pnl in enumerate(pnls):
        opened = start + timedelta(days=index)
        entry_price = 100.0
        quantity = 1.0
        exit_price = entry_price + pnl
        result.append(PositionCampaign(
            campaign_id=f"{wallet[-4:]}-{index}", target_wallet=wallet, symbol=symbols[index % len(symbols)],
            direction="long", opened_at=opened, closed_at=opened + timedelta(hours=2),
            entry_quantity=quantity, open_quantity=0, entry_notional=entry_price,
            remaining_entry_notional=0, exit_notional=exit_price, realized_pnl=pnl, target_fees=0,
            event_count=2, raw_fill_ids=[str(index)], max_open_quantity=1,
        ))
    return result


class SuitabilityEvidenceTests(unittest.TestCase):
    def test_relative_weights_normalize_perfect_scores_and_never_exceed_fixed_scale(self) -> None:
        cfg = test_config(Path(tempfile.gettempdir()))
        metrics = TraderMetrics(
            target_wallet=STEADY, calculated_at=utc_now(), history_days=365, campaign_count=100,
            closed_campaign_count=100, realized_pnl=1_000, net_pnl=1_000, wins=100, losses=0,
            win_rate=1, shrunk_win_rate=1, average_winner=10, average_loser=-1, median_winner=10,
            median_loser=-1, profit_factor=2, payoff_ratio=2, expectancy=100,
            median_holding_seconds=60, mean_holding_seconds=60, max_drawdown=0,
            longest_losing_streak=0, longest_winning_streak=100, best_campaign=10,
            worst_campaign=1, fifth_percentile=1, ninety_fifth_percentile=10,
            pnl_concentration_best=0.1, pnl_concentration_best_five=0.1,
            average_entry_size_fraction=0.1, median_entry_size_fraction=0.1,
            entry_size_variance=0, martingale_indicator=False, adverse_averaging_indicator=False,
            activity_recency_days=0, raw={"coverage_state": "PROVEN_COMPLETE", "active_days": 30},
        )
        follower = FollowerMetrics(
            expectancy=1, return_fraction=0.20, max_drawdown=0, copyability_score=1,
            slippage_robustness=1, walk_forward_score=1, walk_forward_status="available",
            friction_robustness=1, regime_robustness=1,
            latency_curve=({"latency_ms": 100, "return_fraction": 0.20},), latency_status="available",
        )
        perfect = score_candidate(metrics, cfg.candidates, follower, confidence_score=100)
        multiplied = score_candidate(
            metrics, replace(cfg.candidates, score_weights={key: value * 11 for key, value in cfg.candidates.score_weights.items()}),
            follower, confidence_score=100,
        )
        missing = score_candidate(metrics, cfg.candidates, FollowerMetrics(expectancy=1), confidence_score=0)
        self.assertEqual(perfect.total_score, 100.0)
        self.assertEqual(perfect.total_score, multiplied.total_score)
        self.assertLessEqual(sum(missing.component_scores.values()), 100.0)
        self.assertLessEqual(missing.total_score, 100.0)

    def test_finalist_policy_rejects_low_confidence_and_missing_copyability_without_mutating_score(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            cfg = replace(
                test_config(Path(temp)),
                finalist_requirements=FinalistRequirementsConfig(
                    minimum_confidence_score=60, require_copyability_evidence=True,
                ),
            )
            service = CopyTradeService(cfg)
            DiscoveryPipeline(service.database).run(
                StaticProvider([DiscoveryObservation(STEADY, "suitability_fixture", utc_now(), utc_now(), evidence_id="policy")]),
                limit=10, min_activity=1, max_activity_age=None,
            )
            fingerprint = CandidateAnalysisPipeline(service).config.research_snapshot()
            from src.copytrade.analysis import _config_fingerprint
            config_fingerprint = _config_fingerprint(fingerprint)
            run = AnalysisRun("analysis_policy", utc_now(), {"fixture": True})
            service.database.start_analysis_run(run)
            service.database.finish_analysis_run(run.run_id, status="completed", wallets_considered=1, cheap_rejected=0,
                                                 backfill_attempted=0, backfill_failed=0, reconstructed=1, scored=1,
                                                 eligible=1, rejected=0, deferred=0)
            service.database.upsert_candidate_analysis(CandidateAnalysis(
                wallet=STEADY, lifecycle_status="qualified", last_run_id=run.run_id,
                started_at=utc_now(), completed_at=utc_now(), summary={
                    "copyability": {"status": "unavailable"}, "walk_forward_evaluation": {"status": "unavailable"},
                    "latency": {"status": "unavailable"}, "regime": {"status": "insufficient_sample"},
                },
            ))
            service.database.upsert_candidate_score(CandidateScore(
                STEADY, utc_now(), 95, {"fixture": 95}, {}, True, provenance="phase_b",
                analysis_run_id=run.run_id, config_fingerprint=config_fingerprint, confidence_score=30,
            ))
            pipeline = CandidateAnalysisPipeline(service)
            self.assertEqual(pipeline.shadow_finalists(), [])
            report = pipeline.suitability_report(STEADY)
            self.assertEqual(report["score"]["base_suitability_score"], 95)
            self.assertFalse(report["score"]["finalist_eligible"])
            self.assertEqual(
                report["score"]["finalist_rejection_reasons"],
                ["confidence_below_minimum", "copyability_evidence_required"],
            )

    def test_steady_repeatable_trader_beats_jackpot_and_uncopyable_trader_is_hard_gated(self) -> None:
        cfg = test_config(Path(tempfile.gettempdir()))
        steady_metrics = calculate_trader_metrics(STEADY, campaigns(STEADY, [8, 9, 7, 8, 9, 8]), (), cfg.sizing)
        jackpot_metrics = calculate_trader_metrics(JACKPOT, campaigns(JACKPOT, [2, 2, 2, 2, 2, 200]), (), cfg.sizing)
        follower = FollowerMetrics(expectancy=1, return_fraction=0.08, max_drawdown=0.05, copyability_score=0.8,
                                   friction_robustness=0.8, regime_robustness=0.8)
        steady = score_candidate(steady_metrics, cfg.candidates, follower, confidence_score=80)
        steady_with_diversification_argument = score_candidate(
            steady_metrics, cfg.candidates, follower, confidence_score=80, diversification=0.0,
        )
        jackpot = score_candidate(jackpot_metrics, cfg.candidates, follower, confidence_score=80)
        whale = score_candidate(steady_metrics, cfg.candidates, replace(follower, copyability_score=0.01), confidence_score=80)
        self.assertGreater(steady.total_score, jackpot.total_score)
        self.assertEqual(steady.total_score, steady_with_diversification_argument.total_score)
        self.assertIn("jackpot_concentration", jackpot.reasons)
        self.assertFalse(whale.eligible)
        self.assertIn("copyability_hard_limit", whale.hard_gates)

    def test_confidence_regime_and_friction_are_deterministic_and_separate_from_suitability(self) -> None:
        cfg = test_config(Path(tempfile.gettempdir()))
        strong = calculate_trader_metrics(STEADY, campaigns(STEADY, [5, 6, 7, 8]), (), cfg.sizing)
        weak = calculate_trader_metrics(STEADY, campaigns(STEADY, [5]), (), cfg.sizing)
        strong.raw["active_days"] = 4
        weak.raw["active_days"] = 1
        high = suitability_confidence(strong, cfg.confidence, coverage_state="PROVEN_COMPLETE", walk_forward_windows=2, represented_regimes=2)
        low = suitability_confidence(weak, cfg.confidence, coverage_state="UNPROVEN", walk_forward_windows=0, represented_regimes=0)
        self.assertGreater(high["score"], low["score"])
        regime = _regime_evidence(campaigns(STEADY, [10, 8, -5, -4]), cfg)
        self.assertEqual(regime["status"], "available")
        self.assertEqual(regime["campaign_count"], 4)
        self.assertEqual(regime["represented_dimensions"], 2)
        self.assertEqual(sum(item["campaign_count"] for item in regime["directional"]["regimes"].values()), 4)
        self.assertEqual(sum(item["campaign_count"] for item in regime["volatility"]["regimes"].values()), 4)
        friction = _friction_evidence([
            {"slippage_bps": 0, "return_fraction": 0.10}, {"slippage_bps": 5, "return_fraction": 0.04},
            {"slippage_bps": 10, "return_fraction": -0.01},
        ])
        self.assertEqual((friction["status"], friction["break_even_slippage_bps"]), ("available", 10.0))
        self.assertLess(friction["score"], 1.0)

    def test_end_to_end_pipeline_stores_funnel_report_and_never_promotes_operator_status(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            service = CopyTradeService(test_config(root))
            observed_at = utc_now()
            DiscoveryPipeline(service.database).run(
                StaticProvider([DiscoveryObservation(STEADY, "suitability_fixture", observed_at, observed_at, evidence_id="seed")]),
                limit=10, min_activity=1, max_activity_age=None,
            )
            start = utc_now() - timedelta(days=10)
            fills = []
            for index, price in enumerate((100, 110, 100, 111, 100, 112, 100, 113)):
                fills.append(RawFill.from_hyperliquid({
                    "coin": "BTC" if index < 4 else "ETH", "px": str(price), "sz": "1",
                    "side": "B" if index % 2 == 0 else "A", "startPosition": "0" if index % 2 == 0 else "1",
                    "time": int((start + timedelta(days=index)).timestamp() * 1000), "oid": index, "tid": index,
                    "fee": "0", "accountValue": "1000",
                }, STEADY))
            def backfill(wallet: str, requested_start: object):
                service.database.insert_raw_fills(fills)
                return {"new_raw_fills": len(fills)}
            result = CandidateAnalysisPipeline(service, backfill_wallet=backfill).run(limit=10)
            self.assertEqual(result["status"], "completed")
            self.assertIn("high_suitability", result["funnel"])
            report = CandidateAnalysisPipeline(service).suitability_report(STEADY)
            self.assertEqual(report["operator_action"], "recommendation_only; no target status was changed")
            self.assertEqual(service.database.get_target(STEADY).status, "new")  # type: ignore[union-attr]
            self.assertIn("confidence_score", report["score"])
            self.assertIn("friction_robustness", report)


if __name__ == "__main__":
    unittest.main()
