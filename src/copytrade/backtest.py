from __future__ import annotations

import subprocess
from dataclasses import replace
from datetime import timedelta
from typing import Iterable

from .analytics import calculate_trader_metrics
from .config import CopyTradeConfig
from .models import BacktestRun, PositionEvent, PositionEventType, RawFill, as_utc, new_run_id, utc_now
from .market import MarketDataProvider
from .paper import PaperExecutionEngine, SignalFactory, TargetSizeClassifier
from .reconstruction import PositionReconstructor
from .storage import CopyTradeStore


class CopyTradeBacktester:
    """Chronological event replay; it uses no future fills, metrics, or sizing observations."""

    def __init__(
        self, config: CopyTradeConfig, store: CopyTradeStore | None = None, market_data: MarketDataProvider | None = None
    ) -> None:
        self.config = config
        self.store = store
        self.market_data = market_data

    def run(
        self, fills: Iterable[RawFill] | None = None, *, events: Iterable[PositionEvent] | None = None,
        prior_events: Iterable[PositionEvent] = (), run_id: str | None = None,
    ) -> BacktestRun:
        started = utc_now()
        if events is None:
            reconstruction = PositionReconstructor().reconstruct(fills or ())
            replay_events = list(reconstruction.events)
        else:
            replay_events = list(events)
        replay_events.sort(key=lambda event: (event.event_timestamp, event.event_id))
        classifier = TargetSizeClassifier(self.config.sizing)
        self._seed_prior_size_history(classifier, prior_events)
        factory = SignalFactory(classifier, self.config)
        engine = PaperExecutionEngine(self.config, self.store)
        attempts = []
        market_observations: list[dict[str, object]] = []
        signals_created = 0
        for event in replay_events:
            signals = factory.from_position_event(event, engine.portfolio.cash or 0.0)
            for signal in signals:
                # In a flip, the close is processed first.  Recalculate the entry
                # request from then-current free cash rather than pre-close equity.
                if signal.action in {"open", "add"}:
                    signal = replace(
                        signal,
                        requested_capital=(engine.portfolio.cash or 0.0) * signal.allocation_fraction,
                    )
                signals_created += 1
                if self.store:
                    self.store.insert_signal(signal)  # type: ignore[attr-defined]
                received = event.event_timestamp + timedelta(milliseconds=self.config.paper_execution.detection_latency_ms)
                execution_at = received + timedelta(milliseconds=self.config.paper_execution.order_latency_ms)
                market_price = event.price
                market_metadata: dict[str, object] = {"source": "target_fill", "quality": "not_latency_sensitive", "market_timestamp": event.event_timestamp.isoformat()}
                if self.market_data:
                    # The simulated executable reference must be available no
                    # earlier than detection plus order latency.
                    historical = self.market_data.historical_price(event.symbol, execution_at)
                    if historical is not None:
                        market_price = historical.price
                        market_metadata = {"source": historical.source, "quality": historical.quality,
                                           "market_timestamp": as_utc(historical.timestamp).isoformat(), "reference_price": historical.price}
                    else:
                        market_metadata = {"source": "unavailable", "quality": "missing_historical_price", "market_timestamp": None}
                market_observations.append({"event_id": event.event_id, "target_fill_price": event.price, "execution_price": market_price, **market_metadata})
                attempts.append(engine.process_signal(signal, received_at=received, market_price=market_price, market_metadata=market_metadata))
        ending = engine.portfolio.equity
        summary = {
            "events_replayed": len(replay_events), "signals_created": signals_created,
            "attempts": len(attempts), "filled_attempts": sum(item.status == "filled" for item in attempts),
            "skipped_attempts": sum(item.status in {"skipped", "missed"} for item in attempts),
            "ending_cash": engine.portfolio.cash, "ending_equity": ending,
            "net_pnl": ending - self.config.capital.initial_capital,
            "return_fraction": (ending - self.config.capital.initial_capital) / self.config.capital.initial_capital,
            "committed_capital": engine.portfolio.committed_capital,
            "open_virtual_positions": len([item for item in engine.portfolio.sleeves.values() if item.is_open]),
            "max_drawdown_fraction": engine.portfolio.max_drawdown_fraction,
            "follower_equity_curve": engine.equity_history,
            "price_assumption": (
                "Historical market-price provider plus configured deterministic slippage; historical L2 was not used."
                if self.market_data else "Target fill price plus configured deterministic slippage; historical L2 was not used."
            ),
            "skip_reasons": _count([item.reason for item in attempts if item.status != "filled"]),
            "market_observations": market_observations,
            "market_data_complete": bool(self.market_data) and all(item.get("quality") != "missing_historical_price" for item in market_observations),
        }
        run = BacktestRun(
            run_id=run_id or new_run_id(), started_at=started, finished_at=utc_now(),
            target_wallets=tuple(sorted({event.target_wallet for event in replay_events})),
            start_timestamp=replay_events[0].event_timestamp if replay_events else None,
            end_timestamp=replay_events[-1].event_timestamp if replay_events else None,
            initial_capital=self.config.capital.initial_capital, ending_capital=ending,
            seed=self.config.paper_execution.random_seed, configuration=self.config.snapshot(), summary=summary,
            git_commit=_git_commit(),
        )
        if self.store:
            self.store.insert_backtest_run(run)  # type: ignore[attr-defined]
        return run

    def latency_decay_curve(self, fills: Iterable[RawFill]) -> list[dict[str, float]]:
        values: list[dict[str, float]] = []
        if self.market_data is None:
            return values
        fills = list(fills)
        for latency_ms in self.config.backtest.detection_delays_ms:
            config = replace(
                self.config,
                paper_execution=replace(self.config.paper_execution, detection_latency_ms=latency_ms),
            )
            run = CopyTradeBacktester(config, market_data=self.market_data).run(fills)
            if not run.summary.get("market_data_complete"):
                return []
            values.append({"latency_ms": float(latency_ms), "net_pnl": float(run.summary["net_pnl"]),
                           "return_fraction": float(run.summary["return_fraction"])})
        return values

    def slippage_scenarios(self, fills: Iterable[RawFill]) -> list[dict[str, float]]:
        values: list[dict[str, float]] = []
        fills = list(fills)
        for slippage_bps in self.config.backtest.slippage_scenarios_bps:
            config = replace(self.config, paper_execution=replace(self.config.paper_execution, slippage_bps=slippage_bps))
            run = CopyTradeBacktester(config, market_data=self.market_data).run(fills)
            values.append({"slippage_bps": float(slippage_bps), "net_pnl": float(run.summary["net_pnl"]),
                           "return_fraction": float(run.summary["return_fraction"])})
        return values

    def walk_forward(self, fills: Iterable[RawFill], *, training_days: int | None = None, forward_days: int | None = None) -> list[dict[str, object]]:
        ordered = sorted(fills, key=lambda fill: (fill.event_timestamp, fill.event_id))
        if not ordered:
            return []
        train_days = training_days or self.config.backtest.default_training_days
        test_days = forward_days or self.config.backtest.default_forward_days
        cursor = ordered[0].event_timestamp
        final = ordered[-1].event_timestamp
        windows: list[dict[str, object]] = []
        while cursor + timedelta(days=train_days + test_days) <= final + timedelta(milliseconds=1):
            training_end = cursor + timedelta(days=train_days)
            forward_end = training_end + timedelta(days=test_days)
            training = [fill for fill in ordered if cursor <= fill.event_timestamp < training_end]
            forward = [fill for fill in ordered if training_end <= fill.event_timestamp < forward_end]
            training_reconstruction = PositionReconstructor().reconstruct(training)
            training_metrics = calculate_trader_metrics(
                training[0].target_wallet if training else "unknown", training_reconstruction.campaigns, training_reconstruction.events
            )
            # Boundary policy: exclude campaigns already open at forward start.
            # Reconstructing all data up to the boundary identifies them without
            # injecting training P&L or using any future source evidence.
            state_at_boundary = PositionReconstructor().reconstruct([fill for fill in ordered if fill.event_timestamp < training_end])
            open_campaign_ids = {campaign.campaign_id for campaign in state_at_boundary.campaigns if not campaign.is_closed}
            full_through_forward = PositionReconstructor().reconstruct([fill for fill in ordered if fill.event_timestamp < forward_end])
            forward_events = [event for event in full_through_forward.events if training_end <= event.event_timestamp < forward_end and event.campaign_id not in open_campaign_ids]
            run = self.run(events=forward_events, prior_events=training_reconstruction.events)
            windows.append({
                "training_start": cursor.isoformat(), "training_end": training_end.isoformat(),
                "forward_end": forward_end.isoformat(), "training_campaigns": training_metrics.closed_campaign_count,
                "training_expectancy": training_metrics.expectancy, "forward_run_id": run.run_id,
                "forward_net_pnl": run.summary["net_pnl"],
                "boundary_policy": "exclude_campaigns_open_at_forward_start",
                "boundary_campaigns_excluded": len(open_campaign_ids),
            })
            cursor += timedelta(days=test_days)
        return windows

    @staticmethod
    def _seed_prior_size_history(classifier: TargetSizeClassifier, events: Iterable[PositionEvent]) -> None:
        groups: dict[str, list[float]] = {}
        for event in sorted(events, key=lambda item: (item.event_timestamp, item.event_id)):
            if event.event_type not in {PositionEventType.OPEN, PositionEventType.FLIP}:
                continue
            if event.target_equity and event.target_equity > 0 and event.initial_delta_notional > 0:
                groups.setdefault(event.target_wallet, []).append(event.initial_delta_notional / event.target_equity)
        for wallet, fractions in groups.items():
            classifier.seed(wallet, fractions)


def _count(values: Iterable[str]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for value in values:
        counts[value] = counts.get(value, 0) + 1
    return counts


def _git_commit() -> str | None:
    try:
        result = subprocess.run(["git", "rev-parse", "HEAD"], check=True, capture_output=True, text=True, timeout=2)
    except (OSError, subprocess.SubprocessError):
        return None
    return result.stdout.strip() or None
