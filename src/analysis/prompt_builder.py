from __future__ import annotations

import json
from pathlib import Path


def load_system_prompt(path: Path) -> str:
    return path.read_text(encoding="utf-8").strip()


def _compact_indicator_snapshot(indicators: dict) -> dict:
    keep = (
        "market_state",
        "rsi",
        "trend_up",
        "trend_down",
        "bullish_cross",
        "bearish_cross",
        "momentum_resume_up",
        "momentum_resume_down",
        "pullback_detected_long",
        "pullback_detected_short",
        "near_recent_high",
        "near_recent_low",
        "network_prob_win_long",
        "network_prob_win_short",
        "final_long_score",
        "final_short_score",
        "long_score",
        "short_score",
        "entry_market_state_ok",
        "long_entry_market_state_ok",
        "short_entry_market_state_ok",
    )
    return {key: indicators[key] for key in keep if key in indicators}


def _compact_top_event(event: dict) -> dict:
    indicators = event.get("indicators", {}) if isinstance(event, dict) else {}
    return {
        "timestamp": event.get("timestamp"),
        "instance_id": event.get("instance_id"),
        "family": event.get("family"),
        "generation": event.get("generation"),
        "profile_name": event.get("profile_name"),
        "action_candidate": event.get("action_candidate"),
        "executed": event.get("executed"),
        "reason": event.get("reason") or event.get("block_reason"),
        "entry_quality_score": event.get("entry_quality_score"),
        "price": event.get("price"),
        "move_from_previous_pct": event.get("move_from_previous_pct"),
        "market_state": event.get("market_state") or indicators.get("market_state"),
        "position_side": event.get("position_side"),
        "long_score": event.get("long_score"),
        "short_score": event.get("short_score"),
        "long_rsi_ok": event.get("long_rsi_ok"),
        "short_rsi_ok": event.get("short_rsi_ok"),
        "missed_trend": event.get("missed_trend"),
        "indicators": _compact_indicator_snapshot(indicators),
    }


def _compact_log_excerpts(log_excerpts: dict[str, list[str]], *, max_lines_per_stream: int = 12, max_chars_per_line: int = 240) -> dict[str, list[str]]:
    compacted: dict[str, list[str]] = {}
    for name, lines in log_excerpts.items():
        trimmed_lines = []
        for line in lines[-max_lines_per_stream:]:
            text = str(line)
            trimmed_lines.append(text[:max_chars_per_line])
        compacted[name] = trimmed_lines
    return compacted


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
        "top_signals_events": [_compact_top_event(event) for event in bundle.top_events[:max_signal_events]],
        "log_excerpts": _compact_log_excerpts(log_excerpts),
    }
    return json.dumps(compact_payload, indent=2, sort_keys=True)
