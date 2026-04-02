from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from .bot import (
    calculate_quote_size,
    cooldown_active,
    count_recent_entries,
    current_drawdown_pct,
    current_equity,
    daily_loss_limit_hit,
    decision_quality_score,
    estimated_spread_pct,
    expected_move_covers_costs,
    latest_closed_trade,
    move_pct_from_entry,
    paper_exit_fill,
    paper_fill,
    parse_timestamp,
    reset_daily_guard_if_needed,
)
from .config import BotConfig
from .market_data import CoinbasePublicClient
from .models import BotState, ClosedTrade, Position
from .storage import create_initial_state
from .strategy import MomentumStrategy


@dataclass
class BacktestSummary:
    starting_cash: float
    ending_equity: float
    total_return_pct: float
    trades: int
    win_rate_pct: float
    max_drawdown_pct: float


def run_backtest(config: BotConfig, candles: int) -> tuple[BacktestSummary, list[ClosedTrade]]:
    client = CoinbasePublicClient(
        timeout_seconds=config.market_data_timeout_seconds,
        max_retries=config.market_data_max_retries,
        retry_backoff_seconds=config.market_data_retry_backoff_seconds,
    )
    strategy = MomentumStrategy(config)
    state = create_initial_state(config.starting_cash)
    history = client.get_candles(
        product_id=config.product_id,
        granularity=config.granularity,
        limit=candles,
    )
    if len(history) < config.lookback_candles:
        raise RuntimeError("Not enough historical candles returned for backtest.")

    product = client.get_product_info(config.product_id)
    max_drawdown = 0.0

    for index in range(config.lookback_candles, len(history)):
        window = history[: index + 1]
        candle = history[index]
        now = candle.start
        _clear_expired_pause(state, now)

        mark_price = candle.close
        equity = current_equity(state, mark_price)
        reset_daily_guard_if_needed(state, now, equity)
        state.peak_equity = max(state.peak_equity, equity)
        drawdown = current_drawdown_pct(state, equity)
        state.max_drawdown_pct_seen = max(state.max_drawdown_pct_seen, drawdown)
        max_drawdown = max(max_drawdown, drawdown)

        _enforce_circuit_breakers(state, now, equity, config)
        _enforce_trade_rate_limits(state, now, config)

        if state.position is not None:
            decision = strategy.evaluate(window, state.position)
            _simulate_exit_if_needed(state, candle, config, now.isoformat(), decision.market_state)
            if state.position is None:
                continue

            if (
                (state.position.side == "long" and decision.action == "sell")
                or (state.position.side == "short" and decision.action == "cover")
            ):
                exit_fill = (
                    paper_exit_fill(
                        price=mark_price,
                        quantity=state.position.quantity,
                        fee_rate=config.fee_rate,
                        slippage_bps=config.slippage_bps,
                    )
                    if state.position.side == "long"
                    else paper_fill(
                        price=mark_price,
                        quantity=state.position.quantity,
                        fee_rate=config.fee_rate,
                        slippage_bps=config.slippage_bps,
                    )
                )
                _close_position(state, exit_fill, "trend_reversal", now.isoformat(), config)
            continue

        decision = strategy.evaluate(window, None)
        if decision.action not in {"buy", "short"}:
            continue
        if _current_halt_reason(state, now, config) is not None:
            continue

        cooldown_is_active, _ = cooldown_active(state, now, config)
        if cooldown_is_active:
            continue

        if daily_loss_limit_hit(state, equity, config.daily_max_loss_pct):
            continue

        last_trade = latest_closed_trade(state)
        new_side = "long" if decision.action == "buy" else "short"
        if last_trade is not None and last_trade.side != new_side:
            last_closed_at = parse_timestamp(last_trade.closed_at)
            if last_closed_at is not None:
                flip_deadline = last_closed_at + timedelta(seconds=config.flip_cooldown_seconds)
                if now < flip_deadline:
                    continue

        if config.max_spread_threshold > 0 and estimated_spread_pct(candle) > config.max_spread_threshold:
            continue

        quote_size = calculate_quote_size(state.cash, equity, mark_price, product, config)
        if quote_size <= 0:
            continue
        if not expected_move_covers_costs(
            current_price=mark_price,
            target_price=decision.take_profit,
            notional=quote_size,
            config=config,
        ):
            continue

        quantity = quote_size / mark_price
        entry_fill = (
            paper_fill(
                price=mark_price,
                quantity=quantity,
                fee_rate=config.fee_rate,
                slippage_bps=config.slippage_bps,
            )
            if decision.action == "buy"
            else paper_exit_fill(
                price=mark_price,
                quantity=quantity,
                fee_rate=config.fee_rate,
                slippage_bps=config.slippage_bps,
            )
        )
        if decision.action == "buy":
            total_cost = (entry_fill.price * entry_fill.quantity) + entry_fill.fees_paid
            state.cash -= total_cost
        else:
            gross_proceeds = entry_fill.price * entry_fill.quantity
            state.cash += gross_proceeds - entry_fill.fees_paid

        position_size = entry_fill.price * entry_fill.quantity
        state.position = Position(
            side=new_side,
            quantity=entry_fill.quantity,
            entry_price=entry_fill.price,
            position_size=position_size,
            stop_loss=decision.stop_loss
            or (
                mark_price * (1 - config.stop_loss_pct)
                if decision.action == "buy"
                else mark_price * (1 + config.stop_loss_pct)
            ),
            take_profit=decision.take_profit
            or (
                mark_price * (1 + config.take_profit_pct)
                if decision.action == "buy"
                else mark_price * (1 - config.take_profit_pct)
            ),
            trailing_stop=decision.trailing_stop
            or (
                mark_price * (1 - config.trailing_stop_pct_for_market_state(decision.market_state))
                if decision.action == "buy"
                else mark_price * (1 + config.trailing_stop_pct_for_market_state(decision.market_state))
            ),
            highest_price=entry_fill.price,
            lowest_price=entry_fill.price,
            opened_at=now.isoformat(),
            mode="paper",
            entry_fees_paid=entry_fill.fees_paid,
            entry_reason=decision.reason,
            market_state=decision.market_state,
            entry_indicators=decision.indicators,
            entry_quality_score=decision_quality_score(decision.action, decision.indicators),
        )
        state.entry_timestamps.append(now.isoformat())

    if state.position is not None:
        final_price = history[-1].close
        exit_fill = (
            paper_exit_fill(
                price=final_price,
                quantity=state.position.quantity,
                fee_rate=config.fee_rate,
                slippage_bps=config.slippage_bps,
            )
            if state.position.side == "long"
            else paper_fill(
                price=final_price,
                quantity=state.position.quantity,
                fee_rate=config.fee_rate,
                slippage_bps=config.slippage_bps,
            )
        )
        _close_position(state, exit_fill, "end_of_backtest", history[-1].start.isoformat(), config)

    ending_equity = current_equity(state, history[-1].close)
    wins = sum(1 for trade in state.closed_trades if trade.result == "WIN")
    trades = len(state.closed_trades)
    win_rate = (wins / trades * 100) if trades else 0.0
    total_return_pct = ((ending_equity / config.starting_cash) - 1) * 100 if config.starting_cash else 0.0
    summary = BacktestSummary(
        starting_cash=config.starting_cash,
        ending_equity=ending_equity,
        total_return_pct=total_return_pct,
        trades=trades,
        win_rate_pct=win_rate,
        max_drawdown_pct=max_drawdown * 100,
    )
    return summary, state.closed_trades


def _clear_expired_pause(state: BotState, now) -> None:
    paused_until = parse_timestamp(state.trading_paused_until)
    if paused_until is not None and now >= paused_until:
        state.trading_paused_until = None
        if not state.trading_disabled:
            state.trading_disabled_reason = None


def _disable_trading(state: BotState, reason: str) -> None:
    state.trading_disabled = True
    state.trading_disabled_reason = reason
    state.trading_paused_until = None


def _pause_trading(state: BotState, until, reason: str) -> None:
    existing_pause = parse_timestamp(state.trading_paused_until)
    if existing_pause is None or until > existing_pause:
        state.trading_paused_until = until.isoformat()
        if not state.trading_disabled:
            state.trading_disabled_reason = reason


def _current_halt_reason(state: BotState, now, config: BotConfig) -> str | None:
    if not config.trading_enabled:
        return "manual_disable_env"

    paused_until = parse_timestamp(state.trading_paused_until)
    if paused_until is not None and now < paused_until:
        return state.trading_disabled_reason or f"trading_paused_until:{paused_until.isoformat()}"

    if state.trading_disabled:
        return state.trading_disabled_reason or "trading_disabled"
    return None


def _enforce_circuit_breakers(state: BotState, now, equity: float, config: BotConfig) -> None:
    if current_drawdown_pct(state, equity) >= config.max_drawdown_pct:
        _disable_trading(state, f"max_drawdown_pct:{current_drawdown_pct(state, equity):.4f}")
        return
    if state.consecutive_losses >= config.max_consecutive_losses:
        _disable_trading(state, f"max_consecutive_losses:{state.consecutive_losses}")
        return
    if daily_loss_limit_hit(state, equity, config.daily_max_loss_pct):
        _pause_trading(state, now + timedelta(seconds=config.trade_rate_pause_seconds), "daily_loss_limit_hit")


def _enforce_trade_rate_limits(state: BotState, now, config: BotConfig) -> None:
    if len(state.entry_timestamps) >= config.max_trades_total:
        _disable_trading(state, f"max_trades_total:{len(state.entry_timestamps)}")
        return
    trades_last_hour = count_recent_entries(state, now, 3600)
    if trades_last_hour >= config.max_trades_per_hour:
        _pause_trading(
            state,
            now + timedelta(seconds=config.trade_rate_pause_seconds),
            f"max_trades_per_hour:{trades_last_hour}",
        )


def _simulate_exit_if_needed(state: BotState, candle, config: BotConfig, now: str, market_state: str) -> None:
    position = state.position
    assert position is not None

    opened_at = parse_timestamp(position.opened_at) or candle.start

    position.highest_price = max(position.highest_price, candle.high)
    if position.lowest_price is None:
        position.lowest_price = position.entry_price
    position.lowest_price = min(position.lowest_price, candle.low)

    unrealized_move = move_pct_from_entry(position, candle.close)
    if market_state == "CHOPPY" and unrealized_move >= config.chop_profit_lock_trigger_pct:
        if position.side == "long":
            position.stop_loss = max(
                position.stop_loss,
                position.entry_price * (1 + config.chop_profit_lock_stop_buffer_pct),
            )
        else:
            position.stop_loss = min(
                position.stop_loss,
                position.entry_price * (1 - config.chop_profit_lock_stop_buffer_pct),
            )

    trail_pct = config.trailing_stop_pct_for_market_state(market_state)
    elapsed = candle.start - opened_at

    if position.side == "long":
        position.trailing_stop = max(
            position.trailing_stop,
            position.highest_price * (1 - trail_pct),
        )
        effective_stop = max(position.stop_loss, position.trailing_stop)

        if candle.low <= effective_stop:
            exit_fill = paper_exit_fill(
                price=effective_stop,
                quantity=position.quantity,
                fee_rate=config.fee_rate,
                slippage_bps=config.slippage_bps,
            )
            _close_position(state, exit_fill, "stop_loss", now, config)
            return

        if candle.high >= position.take_profit:
            exit_fill = paper_exit_fill(
                price=position.take_profit,
                quantity=position.quantity,
                fee_rate=config.fee_rate,
                slippage_bps=config.slippage_bps,
            )
            _close_position(state, exit_fill, "take_profit", now, config)
            return

        if market_state == "CHOPPY" and elapsed >= timedelta(minutes=config.chop_stall_minutes):
            distance_from_entry = abs(candle.close - position.entry_price) / position.entry_price
            if distance_from_entry <= config.chop_stall_exit_band_pct:
                exit_fill = paper_exit_fill(
                    price=candle.close,
                    quantity=position.quantity,
                    fee_rate=config.fee_rate,
                    slippage_bps=config.slippage_bps,
                )
                _close_position(state, exit_fill, "chop_stall_exit", now, config)
                return

        if elapsed >= timedelta(minutes=config.max_trade_duration_minutes):
            exit_fill = paper_exit_fill(
                price=candle.close,
                quantity=position.quantity,
                fee_rate=config.fee_rate,
                slippage_bps=config.slippage_bps,
            )
            _close_position(state, exit_fill, "duration_watchdog", now, config)
            return
        return

    position.trailing_stop = min(
        position.trailing_stop,
        position.lowest_price * (1 + trail_pct),
    )
    effective_stop = min(position.stop_loss, position.trailing_stop)

    if candle.high >= effective_stop:
        exit_fill = paper_fill(
            price=effective_stop,
            quantity=position.quantity,
            fee_rate=config.fee_rate,
            slippage_bps=config.slippage_bps,
        )
        _close_position(state, exit_fill, "stop_loss", now, config)
        return

    if candle.low <= position.take_profit:
        exit_fill = paper_fill(
            price=position.take_profit,
            quantity=position.quantity,
            fee_rate=config.fee_rate,
            slippage_bps=config.slippage_bps,
        )
        _close_position(state, exit_fill, "take_profit", now, config)
        return

    if market_state == "CHOPPY" and elapsed >= timedelta(minutes=config.chop_stall_minutes):
        distance_from_entry = abs(candle.close - position.entry_price) / position.entry_price
        if distance_from_entry <= config.chop_stall_exit_band_pct:
            exit_fill = paper_fill(
                price=candle.close,
                quantity=position.quantity,
                fee_rate=config.fee_rate,
                slippage_bps=config.slippage_bps,
            )
            _close_position(state, exit_fill, "chop_stall_exit", now, config)
            return

    if elapsed >= timedelta(minutes=config.max_trade_duration_minutes):
        exit_fill = paper_fill(
            price=candle.close,
            quantity=position.quantity,
            fee_rate=config.fee_rate,
            slippage_bps=config.slippage_bps,
        )
        _close_position(state, exit_fill, "duration_watchdog", now, config)


def _close_position(state: BotState, fill, reason: str, now: str, config: BotConfig) -> None:
    position = state.position
    assert position is not None

    closed_at = parse_timestamp(now) or parse_timestamp(position.opened_at) or position.opened_at

    if position.side == "long":
        gross_proceeds = fill.price * fill.quantity
        state.cash += gross_proceeds - fill.fees_paid
        pnl = (fill.price - position.entry_price) * fill.quantity
    else:
        total_cover_cost = (fill.price * fill.quantity) + fill.fees_paid
        state.cash -= total_cover_cost
        pnl = (position.entry_price - fill.price) * fill.quantity

    total_fees = fill.fees_paid + position.entry_fees_paid
    pnl -= total_fees
    pnl_pct = pnl / position.position_size if position.position_size > 0 else 0.0
    opened_at = parse_timestamp(position.opened_at)
    duration_seconds = (
        max(0.0, (closed_at - opened_at).total_seconds()) if opened_at is not None and isinstance(closed_at, type(opened_at)) else 0.0
    )
    result = "WIN" if pnl > 0 else "LOSS" if pnl < 0 else "FLAT"

    state.closed_trades.append(
        ClosedTrade(
            opened_at=position.opened_at,
            closed_at=now,
            side=position.side,
            quantity=fill.quantity,
            position_size=position.position_size,
            entry_price=position.entry_price,
            exit_price=fill.price,
            pnl=pnl,
            pnl_pct=pnl_pct,
            reason=reason,
            reason_tag=reason,
            result=result,
            trade_duration_seconds=duration_seconds,
            fees_paid=total_fees,
            mode="paper",
            entry_reason=position.entry_reason,
            market_state=position.market_state,
            entry_indicators=position.entry_indicators,
            entry_quality_score=position.entry_quality_score,
        )
    )
    if pnl < 0:
        state.last_loss_at = now
        state.consecutive_losses += 1
    else:
        state.consecutive_losses = 0
    state.position = None
