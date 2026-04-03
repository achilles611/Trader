from __future__ import annotations

import json
from pathlib import Path


def load_system_prompt(path: Path) -> str:
    return path.read_text(encoding="utf-8").strip()


def _compact_bot_summary(bot) -> dict:
    return {
        "bot_id": bot.bot_id,
        "profile_name": bot.profile_name,
        "family": bot.family,
        "config_hash": bot.config_hash,
        "market": bot.market,
        "started_at": bot.started_at,
        "ended_at": bot.ended_at,
        "pnl": bot.pnl,
        "win_rate": bot.win_rate,
        "drawdown": bot.drawdown,
        "trade_count": bot.trade_count,
        "avg_hold_sec": bot.avg_hold_sec,
        "expectancy": bot.expectancy,
        "sharpe_like": bot.sharpe_like,
        "max_adverse_excursion": bot.max_adverse_excursion,
        "max_favorable_excursion": bot.max_favorable_excursion,
        "block_reason_counts": bot.block_reason_counts,
        "signal_diagnostics": bot.signal_diagnostics,
        "per_trade_summary": bot.per_trade_summary[-3:],
        "genome": bot.genome,
    }


def build_analysis_input(bundle, *, max_signal_events: int, log_excerpts: dict[str, list[str]]) -> str:
    compact_payload = {
        "cycle_metadata": {
            "cycle_id": bundle.cycle_id,
            "status": bundle.status,
            "dry_run": bundle.dry_run,
            "run_mode": bundle.run_mode,
            "git_sha": bundle.git_sha,
            "repo_branch": bundle.repo_branch,
            "market": bundle.market,
            "generation": bundle.generation,
            "timing": bundle.timing.to_dict(),
            "totals": {
                "pnl": bundle.total_pnl,
                "drawdown": bundle.total_drawdown,
                "trades": bundle.total_trades,
            },
        },
        "guardrails": bundle.guardrails,
        "compile_test_result": bundle.compile_test_result,
        "rolling_comparison": bundle.rolling_comparison,
        "profile_deltas": bundle.profile_deltas,
        "bot_summaries": [_compact_bot_summary(bot) for bot in bundle.bot_runs],
        "top_signals_events": bundle.top_events[:max_signal_events],
        "log_excerpts": log_excerpts,
    }
    return json.dumps(compact_payload, indent=2, sort_keys=True)
