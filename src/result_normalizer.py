from __future__ import annotations

import statistics
from collections import Counter
from datetime import datetime, timezone
from typing import Any

from .safety.position_limits import evaluate_cycle_guardrails
from .storage.artifacts import append_jsonl, read_jsonl
from .storage.models import BotRunRecord, CycleTiming, NormalizedBotRun, NormalizedCycleBundle


def _mean(values: list[float]) -> float:
    return statistics.mean(values) if values else 0.0


def _sharpe_like(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    deviation = statistics.pstdev(values)
    if deviation == 0:
        return 0.0
    return statistics.mean(values) / deviation


def _trade_summary(trades: list[dict[str, Any]]) -> dict[str, Any]:
    pnl_values = [float(trade.get("pnl", 0.0)) for trade in trades]
    holds = [float(trade.get("trade_duration_seconds", 0.0)) for trade in trades]
    mae = [float(trade.get("max_adverse_excursion_pct", 0.0)) for trade in trades]
    mfe = [float(trade.get("max_favorable_excursion_pct", 0.0)) for trade in trades]
    return {
        "avg_hold_sec": _mean(holds),
        "expectancy": _mean(pnl_values),
        "sharpe_like": _sharpe_like(pnl_values),
        "max_adverse_excursion": max(mae, default=0.0),
        "max_favorable_excursion": max(mfe, default=0.0),
        "sample_trades": trades[-5:],
    }


def _signal_diagnostics(events: list[dict[str, Any]]) -> dict[str, Any]:
    action_counts = Counter(str(event.get("action_candidate", "hold")) for event in events)
    block_counts = Counter(str(event.get("block_reason")) for event in events if event.get("block_reason"))
    reason_counts = Counter(str(event.get("reason")) for event in events if event.get("reason"))
    return {
        "action_counts": dict(action_counts),
        "block_reason_counts": dict(block_counts),
        "top_reasons": dict(reason_counts.most_common(5)),
        "executed_count": sum(1 for event in events if event.get("executed")),
        "missed_trend_count": sum(1 for event in events if event.get("missed_trend")),
        "event_count": len(events),
    }


def _rank_events(events: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    ranked = sorted(
        events,
        key=lambda item: (
            1 if item.get("executed") else 0,
            1 if item.get("block_reason") else 0,
            1 if item.get("missed_trend") else 0,
            float(item.get("entry_quality_score", 0.0)),
        ),
        reverse=True,
    )
    return ranked[:limit]


def _build_order_events(bot_id: str, trades: list[dict[str, Any]]) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for trade in trades:
        events.append(
            {
                "timestamp": trade.get("opened_at"),
                "bot_id": bot_id,
                "event": "order_submitted",
                "side": trade.get("side"),
                "price": trade.get("entry_price"),
                "quantity": trade.get("quantity"),
                "reason": trade.get("entry_reason"),
            }
        )
        events.append(
            {
                "timestamp": trade.get("closed_at"),
                "bot_id": bot_id,
                "event": "order_closed",
                "side": trade.get("side"),
                "price": trade.get("exit_price"),
                "quantity": trade.get("quantity"),
                "reason": trade.get("reason"),
                "pnl": trade.get("pnl"),
            }
        )
    return events


def normalize_cycle(
    *,
    settings,
    repo_state,
    artifact_layout,
    timing_context: dict[str, Any],
    swarm_result,
    bot_definitions,
    recent_cycles: list[dict[str, Any]],
    previous_hashes: dict[str, str],
    validation_report,
    log_excerpts: dict[str, list[str]],
) -> tuple[NormalizedCycleBundle, list[BotRunRecord]]:
    total_pnl = 0.0
    total_drawdown = 0.0
    total_trades = 0
    all_events: list[dict[str, Any]] = []
    bot_runs: list[NormalizedBotRun] = []
    db_records: list[BotRunRecord] = []

    for definition in bot_definitions:
        report = swarm_result.instance_reports[definition.bot_id]
        trades = list(report.get("trades", []))
        signal_events = read_jsonl(artifact_layout.bot_signal_log(definition.bot_id))
        diagnostics = _signal_diagnostics(signal_events)
        summary = _trade_summary(trades)
        pnl = float(report.get("final_pnl", 0.0))
        drawdown = float(report.get("max_drawdown_pct", 0.0)) / 100.0
        win_rate = float(report.get("win_rate", 0.0)) / 100.0
        total_pnl += pnl
        total_drawdown = max(total_drawdown, drawdown)
        total_trades += int(report.get("total_trades", 0))

        for event in signal_events:
            append_jsonl(artifact_layout.combined_signals_path, event)
        for order_event in _build_order_events(definition.bot_id, trades):
            append_jsonl(artifact_layout.combined_orders_path, order_event)
        all_events.extend(signal_events)

        normalized_bot = NormalizedBotRun(
            bot_id=definition.bot_id,
            profile_name=definition.profile_name,
            family=definition.family,
            config_hash=definition.config_hash,
            repo_sha=repo_state.git_sha,
            market=str(report.get("product_id", definition.base_config.product_id)),
            started_at=str(report.get("started_at", swarm_result.started_at)),
            ended_at=str(report.get("ended_at", swarm_result.ended_at)),
            pnl=pnl,
            win_rate=win_rate,
            drawdown=drawdown,
            trade_count=int(report.get("total_trades", 0)),
            avg_hold_sec=float(summary["avg_hold_sec"]),
            expectancy=float(summary["expectancy"]),
            sharpe_like=float(summary["sharpe_like"]),
            max_adverse_excursion=float(summary["max_adverse_excursion"]),
            max_favorable_excursion=float(summary["max_favorable_excursion"]),
            block_reason_counts=dict(report.get("block_reason_histogram", {})),
            signal_diagnostics=diagnostics,
            per_trade_summary=list(summary["sample_trades"]),
            artifact_path=str(artifact_layout.bot_root(definition.bot_id)),
            genome=dict(definition.genome),
        )
        bot_runs.append(normalized_bot)
        db_records.append(
            BotRunRecord(
                bot_id=normalized_bot.bot_id,
                profile_name=normalized_bot.profile_name,
                config_hash=normalized_bot.config_hash,
                symbol=normalized_bot.market,
                pnl=normalized_bot.pnl,
                win_rate=normalized_bot.win_rate,
                drawdown=normalized_bot.drawdown,
                trade_count=normalized_bot.trade_count,
                avg_hold_sec=normalized_bot.avg_hold_sec,
                expectancy=normalized_bot.expectancy,
                sharpe_like=normalized_bot.sharpe_like,
                max_adverse_excursion=normalized_bot.max_adverse_excursion,
                max_favorable_excursion=normalized_bot.max_favorable_excursion,
                block_reason_counts=normalized_bot.block_reason_counts,
                artifact_path=normalized_bot.artifact_path,
                repo_sha=normalized_bot.repo_sha,
                started_at=normalized_bot.started_at,
                ended_at=normalized_bot.ended_at,
                family=normalized_bot.family,
                signal_diagnostics=normalized_bot.signal_diagnostics,
                genome=normalized_bot.genome,
            )
        )

    timing = CycleTiming(
        cycle_id=timing_context["cycle_id"],
        expected_trigger_at=timing_context["expected_trigger_at"],
        actual_trigger_at=timing_context["actual_trigger_at"],
        started_at=timing_context["started_at"],
        finished_at=timing_context["finished_at"],
        drift_seconds=float(timing_context["drift_seconds"]),
        duration_seconds=float(timing_context["duration_seconds"]),
    )
    top_events = _rank_events(all_events, settings.analysis.max_signal_events)
    rolling_count = len(recent_cycles)
    average_pnl = _mean([float(item.get("total_pnl", 0.0)) for item in recent_cycles])
    average_drawdown = _mean([float(item.get("total_drawdown", 0.0)) for item in recent_cycles])
    average_trades = _mean([float(item.get("total_trades", 0.0)) for item in recent_cycles])
    profile_deltas = [
        {
            "bot_id": definition.bot_id,
            "current_config_hash": definition.config_hash,
            "previous_config_hash": previous_hashes.get(definition.bot_id),
            "changed": previous_hashes.get(definition.bot_id) != definition.config_hash if previous_hashes.get(definition.bot_id) else False,
        }
        for definition in bot_definitions
    ]
    bundle = NormalizedCycleBundle(
        cycle_id=timing.cycle_id,
        status="completed",
        dry_run=settings.dry_run,
        run_mode="dry_run" if settings.dry_run else "live" if settings.live_trading_enabled else "paper",
        git_sha=repo_state.git_sha,
        repo_branch=repo_state.branch,
        market=bot_definitions[0].base_config.product_id,
        generation=settings.generation,
        timing=timing,
        total_pnl=total_pnl,
        total_drawdown=total_drawdown,
        total_trades=total_trades,
        guardrails={
            "global_kill_switch": settings.safety.global_kill_switch,
            "dry_run": settings.dry_run,
            "live_trading_enabled": settings.live_trading_enabled,
            "cycle_guardrail_flags": evaluate_cycle_guardrails(
                type("BundleProxy", (), {"total_drawdown": total_drawdown})(),
                settings,
            ),
        },
        compile_test_result=validation_report.to_dict(),
        top_events=top_events,
        rolling_comparison={
            "window_cycles": rolling_count,
            "average_total_pnl": average_pnl,
            "average_total_drawdown": average_drawdown,
            "average_total_trades": average_trades,
            "delta_total_pnl": total_pnl - average_pnl if rolling_count else total_pnl,
            "delta_total_drawdown": total_drawdown - average_drawdown if rolling_count else total_drawdown,
            "delta_total_trades": total_trades - average_trades if rolling_count else total_trades,
        },
        profile_deltas=profile_deltas,
        bot_runs=bot_runs,
        log_excerpts=log_excerpts,
        dashboard_path=swarm_result.dashboard_path,
    )
    return bundle, db_records
