from __future__ import annotations

import html
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from .analytics import campaign_return_series, calculate_trader_metrics, drawdown_curve, equity_curve
from .backtest import CopyTradeBacktester
from .config import CopyTradeConfig
from .models import BacktestRun, CandidateScore, PositionCampaign, TraderMetrics
from .storage import CopyTradeDatabase


class ObsidianExporter:
    def __init__(self, config: CopyTradeConfig, database: CopyTradeDatabase) -> None:
        self.config = config
        self.database = database
        self.root = config.artifacts.obsidian_root

    def export_target(self, wallet: str) -> Path:
        target = self.database.get_target(wallet)
        if not target:
            raise KeyError(f"Target not found: {wallet}")
        campaigns = self.database.list_campaigns(wallet)
        events = self.database.list_position_events(wallet)
        metrics = self.database.latest_metrics(wallet) or calculate_trader_metrics(wallet, campaigns, events)
        score = next((item for item in self.database.latest_scores() if item.target_wallet == wallet.lower()), None)
        charts = self._target_charts(wallet, campaigns, metrics)
        note = self.root / "Targets" / f"{wallet.lower()}.md"
        note.parent.mkdir(parents=True, exist_ok=True)
        note.write_text(self._target_markdown(target.status, wallet, metrics, score, charts), encoding="utf-8")
        return note

    def export_dashboard(self) -> Path:
        root = self.root
        root.mkdir(parents=True, exist_ok=True)
        scores = self.database.latest_scores()
        targets = {item.wallet: item for item in self.database.list_targets()}
        rows = []
        for score in scores:
            metrics = self.database.latest_metrics(score.target_wallet)
            target = targets.get(score.target_wallet)
            rows.append(
                f"| [{score.target_wallet}](Targets/{score.target_wallet}.md) | {target.status if target else 'unknown'} | "
                f"{score.total_score:.1f} | {metrics.campaign_count if metrics else 0} | "
                f"{metrics.profit_factor if metrics else 0:.2f} | {metrics.expectancy if metrics else 0:.2f} | "
                f"{metrics.max_drawdown if metrics else 0:.2%} | {'yes' if score.eligible else ', '.join(score.reasons)} |"
            )
        path = root / "Copy Trading Dashboard.md"
        path.write_text(
            "---\ntype: copy-trading-dashboard\nupdated_at: " + datetime.now(timezone.utc).isoformat() +
            "\n---\n\n# Copy Trading Dashboard\n\n"
            "| Target | Status | Score | Campaigns | Profit factor | Expectancy | Max drawdown | Eligible |\n"
            "|---|---:|---:|---:|---:|---:|---:|---|\n" + ("\n".join(rows) or "| No candidates scored | | | | | | | |") +
            "\n\nTarget performance is distinct from simulated follower performance. Historical price assumptions are documented in each backtest.\n",
            encoding="utf-8",
        )
        return path

    def export_backtest(self, run: BacktestRun) -> Path:
        path = self.root / "Backtests" / f"{run.run_id}.md"
        path.parent.mkdir(parents=True, exist_ok=True)
        summary = run.summary
        path.write_text(
            "---\nrun_id: " + run.run_id + "\nstarting_capital: " + f"{run.initial_capital:.2f}" +
            "\nending_capital: " + f"{run.ending_capital:.2f}" + "\nseed: " + str(run.seed) +
            "\ngit_commit: " + str(run.git_commit or "unknown") + "\n---\n\n# Copy Backtest " + run.run_id +
            "\n\n## Simulated follower performance\n\n" +
            f"- Net P&L: {float(summary.get('net_pnl', 0)):.2f}\n- Return: {float(summary.get('return_fraction', 0)):.2%}\n"
            f"- Filled attempts: {summary.get('filled_attempts', 0)}\n- {summary.get('price_assumption', '')}\n\n"
            "## Reproducibility\n\n```json\n" + _pretty(run.configuration) + "\n```\n",
            encoding="utf-8",
        )
        return path

    def export_report(self) -> Path:
        dashboard = self.export_dashboard()
        all_targets = self.database.list_targets()
        target_notes = [self.export_target(target.wallet) for target in all_targets]
        path = self.root / "Reports" / f"{datetime.now(timezone.utc).date().isoformat()}.md"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            "# Copy-trading research report\n\n"
            f"- Dashboard: [[{dashboard.stem}]]\n"
            f"- Targets refreshed: {len(target_notes)}\n"
            "- This report contains public-source observations and paper simulations only; no live orders are enabled.\n",
            encoding="utf-8",
        )
        return path

    def _target_charts(self, wallet: str, campaigns: list[PositionCampaign], metrics: TraderMetrics) -> dict[str, Path]:
        chart_dir = self.root / "charts"
        chart_dir.mkdir(parents=True, exist_ok=True)
        closed = [campaign for campaign in campaigns if campaign.closed_at]
        returns = [campaign.realized_pnl - campaign.target_fees for campaign in closed]
        curve = equity_curve(closed)
        rolling = [sum(returns[max(0, index - 9): index + 1]) for index in range(len(returns))]
        holding = [campaign.holding_seconds / 60 for campaign in closed]
        size = [campaign.entry_notional for campaign in campaigns]
        symbol_pnl = metrics.by_symbol
        fills = self.database.list_raw_fills(wallet)
        follower_curve: list[float] = []
        latency_values: list[float] = []
        if fills:
            backtester = CopyTradeBacktester(self.config)
            follower_run = backtester.run(fills)
            follower_curve = [float(value) for value in follower_run.summary.get("follower_equity_curve", [])]
            latency_values = [float(value["net_pnl"]) for value in backtester.latency_decay_curve(fills)]
        paths = {
            "equity_curve": chart_dir / f"{wallet}_equity.svg",
            "drawdown_curve": chart_dir / f"{wallet}_drawdown.svg",
            "rolling_pnl": chart_dir / f"{wallet}_rolling_pnl.svg",
            "win_loss": chart_dir / f"{wallet}_win_loss.svg",
            "holding_time": chart_dir / f"{wallet}_holding_time.svg",
            "position_size": chart_dir / f"{wallet}_position_size.svg",
            "pnl_by_symbol": chart_dir / f"{wallet}_pnl_by_symbol.svg",
            "latency_decay": chart_dir / f"{wallet}_latency_decay.svg",
            "follower_equity": chart_dir / f"{wallet}_follower_equity.svg",
        }
        _line_svg(paths["equity_curve"], "Target equity curve (closed campaigns)", curve)
        _line_svg(paths["drawdown_curve"], "Target drawdown", drawdown_curve(curve))
        _line_svg(paths["rolling_pnl"], "Rolling P&L (10 campaigns)", rolling)
        _bar_svg(paths["win_loss"], "Win/loss distribution", {"wins": sum(value > 0 for value in returns), "losses": sum(value < 0 for value in returns)})
        _histogram_svg(paths["holding_time"], "Holding-time distribution (minutes)", holding)
        _histogram_svg(paths["position_size"], "Position-size distribution (USD notional)", size)
        _bar_svg(paths["pnl_by_symbol"], "P&L by symbol", {symbol: item["net_pnl"] for symbol, item in symbol_pnl.items()})
        _line_svg(paths["latency_decay"], "Latency-decay curve (simulated follower net P&L)", latency_values)
        _line_svg(paths["follower_equity"], "Simulated follower equity", follower_curve)
        return paths

    def _target_markdown(
        self, status: str, wallet: str, metrics: TraderMetrics, score: CandidateScore | None, charts: dict[str, Path]
    ) -> str:
        score_value = score.total_score if score else 0.0
        frontmatter = {
            "venue": "hyperliquid", "wallet": wallet.lower(), "status": status, "copyability_score": round(score_value, 2),
            "history_days": round(metrics.history_days, 2), "campaign_count": metrics.campaign_count,
            "win_rate": round(metrics.win_rate, 6), "profit_factor": round(metrics.profit_factor, 6),
            "expectancy": round(metrics.expectancy, 6), "max_drawdown": round(metrics.max_drawdown, 6),
            "median_holding_minutes": round(metrics.median_holding_seconds / 60, 2),
            "last_activity": metrics.activity_recency_days, "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        frontmatter_text = "\n".join(f"{key}: {value}" for key, value in frontmatter.items())
        embeds = "\n".join(
            f"### {name.replace('_', ' ').title()}\n\n![]({(Path('..') / path.relative_to(self.root)).as_posix()})"
            for name, path in charts.items()
        )
        return (
            "---\n" + frontmatter_text + "\n---\n\n# " + wallet.lower() +
            "\n\n## Target performance\n\n"
            f"- Net P&L after target fees: {metrics.net_pnl:.2f}\n- Campaigns: {metrics.closed_campaign_count} closed / {metrics.campaign_count} total\n"
            f"- Excluded truncated campaigns: {metrics.raw.get('truncated_campaign_count', 0)}; P&L reconciliation mismatches: {metrics.raw.get('reconciliation_mismatch_count', 0)}\n"
            f"- Win rate: {metrics.win_rate:.2%}; profit factor: {metrics.profit_factor:.2f}; expectancy: {metrics.expectancy:.2f}\n"
            f"- Martingale indicator: {metrics.martingale_indicator}; adverse averaging: {metrics.adverse_averaging_indicator}\n\n"
            "## Simulated follower performance\n\nLatency and follower-equity charts below are deterministic paper simulations using the report configuration. These are deliberately not inferred from target fills alone.\n\n" + embeds + "\n"
        )


def _line_svg(path: Path, title: str, values: Iterable[float]) -> None:
    values = list(values)
    width, height, padding = 760, 280, 35
    if not values:
        content = f'<text x="{width / 2}" y="{height / 2}" text-anchor="middle">No data yet</text>'
    else:
        low, high = min(values), max(values)
        scale = high - low or 1.0
        points = []
        for index, value in enumerate(values):
            x = padding + (width - 2 * padding) * (index / max(len(values) - 1, 1))
            y = height - padding - (height - 2 * padding) * ((value - low) / scale)
            points.append(f"{x:.1f},{y:.1f}")
        content = f'<polyline fill="none" stroke="#47b8e0" stroke-width="3" points="{" ".join(points)}"/>'
    _write_svg(path, title, content, width, height)


def _bar_svg(path: Path, title: str, values: dict[str, float]) -> None:
    width, height, padding = 760, 280, 35
    if not values:
        content = '<text x="380" y="140" text-anchor="middle">No data yet</text>'
    else:
        maximum = max(max(abs(value) for value in values.values()), 1.0)
        bar_width = (width - 2 * padding) / len(values) * 0.65
        content_parts = []
        for index, (label, value) in enumerate(values.items()):
            x = padding + index * (width - 2 * padding) / len(values) + 10
            bar_height = (height - 2 * padding) * abs(value) / maximum
            y = height - padding - bar_height
            color = "#3ecf8e" if value >= 0 else "#f97068"
            content_parts.append(f'<rect x="{x:.1f}" y="{y:.1f}" width="{bar_width:.1f}" height="{bar_height:.1f}" fill="{color}"/>')
            content_parts.append(f'<text x="{x + bar_width / 2:.1f}" y="{height - 12}" text-anchor="middle" font-size="11">{html.escape(label)}</text>')
        content = "".join(content_parts)
    _write_svg(path, title, content, width, height)


def _histogram_svg(path: Path, title: str, values: list[float]) -> None:
    if not values:
        _bar_svg(path, title, {})
        return
    minimum, maximum = min(values), max(values)
    bins = 8
    width = (maximum - minimum) / bins or 1.0
    counts = {f"{minimum + index * width:.1f}": 0.0 for index in range(bins)}
    for value in values:
        index = min(bins - 1, int((value - minimum) / width))
        key = list(counts)[index]
        counts[key] += 1
    _bar_svg(path, title, counts)


def _write_svg(path: Path, title: str, content: str, width: int, height: int) -> None:
    path.write_text(
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">'
        '<rect width="100%" height="100%" fill="#10131a"/><text x="20" y="24" fill="#e8eef7" font-size="16">'
        f'{html.escape(title)}</text><g fill="#c7d1df" stroke="#c7d1df">{content}</g></svg>', encoding="utf-8"
    )


def _pretty(value: object) -> str:
    import json
    return json.dumps(value, indent=2, sort_keys=True, default=str)
