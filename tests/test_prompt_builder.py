from __future__ import annotations

import json
import unittest
from types import SimpleNamespace

from src.analysis.prompt_builder import build_analysis_input


class _Timing:
    def to_dict(self) -> dict[str, object]:
        return {
            "cycle_id": "cycle_1",
            "expected_trigger_at": "2026-04-03T05:00:00+00:00",
            "actual_trigger_at": "2026-04-03T05:00:01+00:00",
            "started_at": "2026-04-03T05:00:01+00:00",
            "finished_at": "2026-04-03T05:01:01+00:00",
            "drift_seconds": 1.0,
            "duration_seconds": 60.0,
        }


class PromptBuilderTests(unittest.TestCase):
    def test_top_events_are_compacted_for_analysis(self) -> None:
        bundle = SimpleNamespace(
            cycle_id="cycle_1",
            status="completed",
            dry_run=True,
            run_mode="dry_run",
            git_sha="abc123",
            repo_branch="main",
            market="ETH-USD",
            generation=1,
            timing=_Timing(),
            total_pnl=1.0,
            total_drawdown=0.1,
            total_trades=2,
            guardrails={"live_trading_enabled": False},
            compile_test_result={"overall_status": "pass"},
            rolling_comparison={"window_cycles": 1},
            profile_deltas=[],
            bot_runs=[
                SimpleNamespace(
                    bot_id="tr1",
                    profile_name="baseline",
                    family="tr",
                    config_hash="hash1",
                    market="ETH-USD",
                    started_at="2026-04-03T05:00:01+00:00",
                    ended_at="2026-04-03T05:01:01+00:00",
                    pnl=1.0,
                    win_rate=1.0,
                    drawdown=0.0,
                    trade_count=1,
                    avg_hold_sec=12.0,
                    expectancy=1.0,
                    sharpe_like=0.5,
                    max_adverse_excursion=0.0,
                    max_favorable_excursion=1.2,
                    block_reason_counts={},
                    signal_diagnostics={"event_count": 2},
                    per_trade_summary=[{"id": 1}],
                    genome={"archetype": "baseline"},
                )
            ],
            top_events=[
                {
                    "timestamp": "2026-04-03T05:00:00+00:00",
                    "instance_id": "tr1",
                    "family": "tr",
                    "generation": 1,
                    "profile_name": "baseline",
                    "action_candidate": "hold",
                    "executed": False,
                    "block_reason": "blocked_choppy_market",
                    "entry_quality_score": 1,
                    "price": 2050.0,
                    "move_from_previous_pct": 0.1,
                    "market_state": "CHOPPY",
                    "position_side": None,
                    "long_score": 0,
                    "short_score": 1,
                    "long_rsi_ok": False,
                    "short_rsi_ok": True,
                    "missed_trend": False,
                    "indicators": {
                        "market_state": "CHOPPY",
                        "rsi": 48.0,
                        "trend_up": False,
                        "trend_down": True,
                        "network_prob_win_long": 0.52,
                        "network_prob_win_short": 0.55,
                        "feature_vector": [1, 2, 3],
                        "feature_names": ["a", "b", "c"],
                    },
                }
            ],
        )

        payload = json.loads(
            build_analysis_input(
                bundle,
                max_signal_events=20,
                log_excerpts={"analysis": ["x" * 400]},
            )
        )
        event = payload["top_signals_events"][0]

        self.assertEqual(event["instance_id"], "tr1")
        self.assertEqual(event["indicators"]["rsi"], 48.0)
        self.assertNotIn("feature_vector", json.dumps(payload))
        self.assertNotIn("feature_names", json.dumps(payload))
        self.assertEqual(len(payload["log_excerpts"]["analysis"][0]), 240)


if __name__ == "__main__":
    unittest.main()
