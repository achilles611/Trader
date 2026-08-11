from __future__ import annotations

import math
from collections import defaultdict
from statistics import fmean, median, pvariance
from datetime import timezone
from typing import Iterable

from .models import PositionCampaign, PositionEvent, PositionEventType, TraderMetrics, utc_now


def _percentile(values: list[float], fraction: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = (len(ordered) - 1) * fraction
    lower = math.floor(index)
    upper = math.ceil(index)
    if lower == upper:
        return ordered[lower]
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (index - lower)


def equity_curve(campaigns: Iterable[PositionCampaign]) -> list[float]:
    equity = 0.0
    values = [equity]
    for campaign in sorted((item for item in campaigns if item.closed_at), key=lambda item: item.closed_at or item.opened_at):
        equity += campaign.realized_pnl - campaign.target_fees
        values.append(equity)
    return values


def drawdown_curve(values: Iterable[float]) -> list[float]:
    peak = -math.inf
    curve: list[float] = []
    for value in values:
        peak = max(peak, value)
        curve.append(max(0.0, peak - value))
    return curve


def campaign_return_series(campaigns: Iterable[PositionCampaign]) -> dict[str, float]:
    """UTC-day buckets; no positional pairing of unrelated campaign sequences."""
    buckets: dict[str, float] = defaultdict(float)
    for campaign in campaigns:
        if not campaign.closed_at or not campaign.history_complete:
            continue
        day = campaign.closed_at.astimezone(timezone.utc).date().isoformat()
        buckets[day] += (campaign.realized_pnl - campaign.target_fees) / max(campaign.entry_notional, 1e-12)
    return dict(sorted(buckets.items()))


def calculate_trader_metrics(
    target_wallet: str,
    campaigns: Iterable[PositionCampaign],
    events: Iterable[PositionEvent] = (),
) -> TraderMetrics:
    all_campaigns = sorted(campaigns, key=lambda item: item.opened_at)
    event_list = list(events)
    truncated = [campaign for campaign in all_campaigns if not campaign.history_complete]
    closed = [campaign for campaign in all_campaigns if campaign.closed_at and campaign.history_complete]
    pnl = [campaign.realized_pnl - campaign.target_fees for campaign in closed]
    winners = [value for value in pnl if value > 0]
    losers = [value for value in pnl if value < 0]
    wins = len(winners)
    losses = len(losers)
    decided = wins + losses
    win_rate = wins / decided if decided else 0.0
    # Beta(1,1) posterior mean keeps early scorecards from treating one win as 100%.
    shrunk_win_rate = (wins + 1) / (decided + 2)
    gross_profit = sum(winners)
    gross_loss = abs(sum(losers))
    profit_factor = gross_profit / gross_loss if gross_loss else (float("inf") if gross_profit else 0.0)
    average_winner = fmean(winners) if winners else 0.0
    average_loser = abs(fmean(losers)) if losers else 0.0
    payoff_ratio = average_winner / average_loser if average_loser else 0.0
    curve = equity_curve(closed)
    drawdowns = drawdown_curve(curve)
    streaks = _streaks(pnl)
    symbols: dict[str, list[float]] = defaultdict(list)
    long_count = 0
    short_count = 0
    for campaign, result in zip(closed, pnl):
        symbols[campaign.symbol].append(result)
        if campaign.direction == "long":
            long_count += 1
        else:
            short_count += 1
    by_symbol = {
        symbol: {
            "net_pnl": sum(results), "campaign_count": float(len(results)),
            "win_rate": sum(value > 0 for value in results) / len(results) if results else 0.0,
        }
        for symbol, results in symbols.items()
    }
    size_fractions = [
        abs(event.initial_delta_notional) / event.target_equity
        for event in event_list
        if event.event_type in {PositionEventType.OPEN, PositionEventType.FLIP}
        and event.target_equity is not None and event.target_equity > 0
    ]
    equity_observations = [
        event.target_equity for event in event_list if event.target_equity is not None and event.target_equity > 0
    ]
    max_drawdown_dollars = max(drawdowns, default=0.0)
    # Candidate gates are configured as fractions.  Prefer actual account value;
    # if the public fill stream did not expose it, retain a conservative
    # notional-normalized proxy and label its dollar denominator in raw metrics.
    drawdown_denominator = equity_observations[0] if equity_observations else max(
        (campaign.entry_notional for campaign in all_campaigns), default=1.0
    )
    start = min((campaign.opened_at for campaign in all_campaigns), default=utc_now())
    latest = max(((campaign.closed_at or campaign.opened_at) for campaign in all_campaigns), default=None)
    history_days = max(0.0, ((latest or start) - start).total_seconds() / 86400)
    concentration_base = max(abs(sum(pnl)), 1e-12)
    best_sorted = sorted(pnl, reverse=True)
    total_fees = sum(campaign.target_fees for campaign in closed)
    return TraderMetrics(
        target_wallet=target_wallet.lower(), calculated_at=utc_now(), history_days=history_days,
        campaign_count=len(all_campaigns), closed_campaign_count=len(closed), realized_pnl=sum(campaign.realized_pnl for campaign in closed),
        net_pnl=sum(pnl), wins=wins, losses=losses, win_rate=win_rate, shrunk_win_rate=shrunk_win_rate,
        average_winner=average_winner, average_loser=average_loser,
        median_winner=median(winners) if winners else 0.0, median_loser=abs(median(losers)) if losers else 0.0,
        profit_factor=profit_factor, payoff_ratio=payoff_ratio, expectancy=fmean(pnl) if pnl else 0.0,
        median_holding_seconds=median([campaign.holding_seconds for campaign in closed]) if closed else 0.0,
        mean_holding_seconds=fmean([campaign.holding_seconds for campaign in closed]) if closed else 0.0,
        max_drawdown=max_drawdown_dollars / max(drawdown_denominator, 1e-12), longest_losing_streak=streaks["loss"], longest_winning_streak=streaks["win"],
        best_campaign=max(pnl, default=0.0), worst_campaign=min(pnl, default=0.0),
        fifth_percentile=_percentile(pnl, 0.05), ninety_fifth_percentile=_percentile(pnl, 0.95),
        pnl_concentration_best=max(pnl, default=0.0) / concentration_base,
        pnl_concentration_best_five=sum(best_sorted[:5]) / concentration_base,
        average_entry_size_fraction=fmean(size_fractions) if size_fractions else 0.0,
        median_entry_size_fraction=median(size_fractions) if size_fractions else 0.0,
        entry_size_variance=pvariance(size_fractions) if len(size_fractions) > 1 else 0.0,
        martingale_indicator=_is_martingale(closed),
        adverse_averaging_indicator=any(campaign.adverse_add_count > 0 for campaign in all_campaigns),
        activity_recency_days=((utc_now() - latest).total_seconds() / 86400) if latest else None,
        by_symbol=by_symbol,
        raw={
            "equity_curve": curve, "drawdown_curve": drawdowns, "long_campaign_count": long_count,
            "short_campaign_count": short_count, "target_fees": total_fees,
            "trade_frequency_per_day": len(all_campaigns) / max(history_days, 1.0),
            "rolling_drawdowns": drawdowns, "max_drawdown_dollars": max_drawdown_dollars,
            "drawdown_denominator": drawdown_denominator,
            "liquidation_frequency": sum(campaign.liquidation_count for campaign in all_campaigns) / max(len(all_campaigns), 1),
            "truncated_campaign_count": len(truncated), "excluded_closed_campaign_count": sum(item.closed_at is not None for item in truncated),
            "eligible_campaign_count": len(all_campaigns) - len(truncated),
            "source_closed_pnl": sum(item.source_closed_pnl for item in all_campaigns if item.source_closed_pnl_observed),
            "reconstructed_gross_realized_pnl": sum(item.realized_pnl for item in closed),
            "reconciliation_mismatch_count": sum(
                item.reconciliation_gross_difference is not None and abs(item.reconciliation_gross_difference) > 1e-8
                for item in all_campaigns
            ),
        },
    )


def _streaks(pnl: list[float]) -> dict[str, int]:
    win = loss = current_win = current_loss = 0
    for value in pnl:
        if value > 0:
            current_win += 1
            current_loss = 0
        elif value < 0:
            current_loss += 1
            current_win = 0
        else:
            current_win = current_loss = 0
        win = max(win, current_win)
        loss = max(loss, current_loss)
    return {"win": win, "loss": loss}


def _is_martingale(campaigns: list[PositionCampaign]) -> bool:
    closed = [campaign for campaign in campaigns if campaign.closed_at]
    comparisons: list[bool] = []
    for previous, current in zip(closed, closed[1:]):
        previous_result = previous.realized_pnl - previous.target_fees
        if previous_result < 0:
            comparisons.append(current.entry_notional > previous.entry_notional * 1.10)
    return len(comparisons) >= 3 and sum(comparisons) / len(comparisons) >= 0.60
