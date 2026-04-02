from __future__ import annotations

import logging
import time
from collections import Counter
from datetime import datetime, timedelta
from typing import Any

from .config import BotConfig
from .exchange import CoinbaseLiveTrader
from .market_data import CoinbasePublicClient, FatalMarketDataError, TransientMarketDataError
from .models import BotState, Candle, ClosedTrade, Position, ProductInfo, TradeResult, utc_now
from .storage import append_jsonl, append_trade, load_state, save_state
from .strategy import MomentumStrategy


LOGGER = logging.getLogger("eth_bot")


def parse_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value)


def current_equity(state: BotState, mark_price: float) -> float:
    if state.position is None:
        position_value = 0.0
    elif state.position.side == "long":
        position_value = state.position.quantity * mark_price
    else:
        position_value = -(state.position.quantity * mark_price)
    return state.cash + position_value


def current_drawdown_pct(state: BotState, equity: float) -> float:
    peak_equity = max(state.peak_equity, equity)
    if peak_equity <= 0:
        return 0.0
    return max(0.0, (peak_equity - equity) / peak_equity)


def reset_daily_guard_if_needed(state: BotState, now: datetime, equity: float) -> None:
    day_marker = now.date().isoformat()
    if state.day_marker != day_marker:
        state.day_marker = day_marker
        state.day_start_equity = equity


def latest_closed_trade(state: BotState) -> ClosedTrade | None:
    return state.closed_trades[-1] if state.closed_trades else None


def cooldown_active(state: BotState, now: datetime, config: BotConfig) -> tuple[bool, str | None]:
    last_trade = latest_closed_trade(state)
    if last_trade is None:
        return False, None

    closed_at = parse_timestamp(last_trade.closed_at)
    if closed_at is None:
        return False, None

    cooldown_seconds = 0
    if last_trade.result == "LOSS":
        cooldown_seconds = config.cooldown_after_loss_seconds
    elif last_trade.result == "WIN":
        cooldown_seconds = config.cooldown_after_win_seconds

    if cooldown_seconds <= 0:
        return False, None

    blocked_until = closed_at + timedelta(seconds=cooldown_seconds)
    if now < blocked_until:
        return True, f"trade_cooldown_until:{blocked_until.isoformat()}"
    return False, None


def daily_loss_limit_hit(state: BotState, equity: float, daily_max_loss_pct: float) -> bool:
    threshold = state.day_start_equity * (1 - daily_max_loss_pct)
    return equity <= threshold


def count_recent_entries(state: BotState, now: datetime, window_seconds: int) -> int:
    if window_seconds <= 0:
        return len(state.entry_timestamps)
    cutoff = now - timedelta(seconds=window_seconds)
    return sum(
        1
        for item in state.entry_timestamps
        if (timestamp := parse_timestamp(item)) is not None and timestamp >= cutoff
    )


def estimated_spread_pct(candle: Candle) -> float:
    if candle.close <= 0:
        return 0.0
    return max(0.0, (candle.high - candle.low) / candle.close)


def move_pct_from_entry(position: Position, current_price: float) -> float:
    if position.entry_price <= 0:
        return 0.0
    if position.side == "long":
        return (current_price - position.entry_price) / position.entry_price
    return (position.entry_price - current_price) / position.entry_price


def decision_quality_score(action: str, indicators: dict[str, Any]) -> int:
    if action == "buy":
        return int(indicators.get("long_score", 0) or 0)
    if action == "short":
        return int(indicators.get("short_score", 0) or 0)
    return int(max(indicators.get("long_score", 0) or 0, indicators.get("short_score", 0) or 0))


def move_from_previous_pct(indicators: dict[str, Any]) -> float:
    close = indicators.get("close")
    previous_close = indicators.get("previous_close")
    if close in (None, 0) or previous_close in (None, 0):
        return 0.0
    return abs(float(close) - float(previous_close)) / float(previous_close)


def is_missed_trend(
    indicators: dict[str, Any],
    *,
    action_candidate: str,
    position_side: str | None,
    config: BotConfig,
) -> bool:
    if action_candidate != "hold" or position_side is not None:
        return False

    close = indicators.get("close")
    previous_close = indicators.get("previous_close")
    if close in (None, 0) or previous_close in (None, 0):
        return False

    moved_enough = move_from_previous_pct(indicators) >= config.missed_trend_move_pct
    moved_with_trend = (
        bool(indicators.get("trend_up")) and float(close) > float(previous_close)
    ) or (
        bool(indicators.get("trend_down")) and float(close) < float(previous_close)
    )
    return moved_enough and moved_with_trend


def signal_reason_histogram(events: list[dict[str, Any]], *, field: str, require_hold: bool = False) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for event in events:
        if require_hold and event.get("action_candidate") != "hold":
            continue
        value = event.get(field)
        if value:
            counts[str(value)] += 1
    return dict(counts)


def expected_move_covers_costs(
    current_price: float,
    target_price: float | None,
    notional: float,
    config: BotConfig,
) -> bool:
    if target_price is None or current_price <= 0 or notional <= 0:
        return False
    expected_move_ratio = abs(target_price - current_price) / current_price
    expected_move_value = notional * expected_move_ratio
    one_way_cost = notional * (config.fee_rate + (config.slippage_bps / 10_000))
    minimum_expected_move = one_way_cost * config.min_expected_move_multiple
    return expected_move_value >= minimum_expected_move


def calculate_quote_size(
    cash: float,
    equity: float,
    price: float,
    product: ProductInfo,
    config: BotConfig,
) -> float:
    usable_cash = max(0.0, cash - config.min_cash_reserve)
    if usable_cash <= 0:
        return 0.0

    risk_budget = equity * config.risk_per_trade_pct
    stop_distance = price * config.stop_loss_pct
    quantity_from_risk = risk_budget / stop_distance if stop_distance > 0 else 0.0
    notional_from_risk = quantity_from_risk * price
    estimated_trade_cost_multiplier = 1 + config.fee_rate + (config.slippage_bps / 10_000)
    cash_limited_notional = usable_cash / estimated_trade_cost_multiplier
    max_notional = min(cash_limited_notional, equity * config.max_notional_pct)
    scaled_notional = min(notional_from_risk, max_notional) * config.aggressiveness
    position_cap = min(max_notional, config.max_position_size)
    minimum_notional = max(config.min_order_notional, product.quote_min_size, config.min_position_size)

    if position_cap < minimum_notional:
        return 0.0
    if scaled_notional < minimum_notional:
        return 0.0
    return min(scaled_notional, position_cap)


def paper_fill(price: float, quantity: float, fee_rate: float, slippage_bps: float) -> TradeResult:
    slippage_multiplier = 1 + (slippage_bps / 10_000)
    execution_price = price * slippage_multiplier
    notional = execution_price * quantity
    fees = notional * fee_rate
    return TradeResult(price=execution_price, quantity=quantity, fees_paid=fees)


def paper_exit_fill(price: float, quantity: float, fee_rate: float, slippage_bps: float) -> TradeResult:
    slippage_multiplier = 1 - (slippage_bps / 10_000)
    execution_price = price * slippage_multiplier
    notional = execution_price * quantity
    fees = notional * fee_rate
    return TradeResult(price=execution_price, quantity=quantity, fees_paid=fees)


class TradingBot:
    def __init__(self, config: BotConfig) -> None:
        self.config = config
        self.market_data = CoinbasePublicClient(
            timeout_seconds=config.market_data_timeout_seconds,
            max_retries=config.market_data_max_retries,
            retry_backoff_seconds=config.market_data_retry_backoff_seconds,
        )
        self.strategy = MomentumStrategy(config)
        self.live_exchange = None
        if config.mode == "live":
            self.live_exchange = CoinbaseLiveTrader(
                api_key=config.coinbase_api_key or "",
                api_secret=config.coinbase_api_secret or "",
            )
            if config.enable_shorts:
                LOGGER.warning(
                    "BOT_ENABLE_SHORTS is on, but live short entries are disabled for the current spot adapter."
                )
        if config.max_concurrent_trades > 1:
            LOGGER.warning(
                "BOT_MAX_CONCURRENT_TRADES=%s requested, but the current runtime is still single-position. "
                "Re-entry after exit is supported; true parallel positions need a state refactor.",
                config.max_concurrent_trades,
            )

    def load_state(self) -> BotState:
        return load_state(self.config.state_path, self.config.starting_cash)

    def snapshot(self) -> dict[str, Any]:
        state = self.load_state()
        product = self.market_data.get_product_info(self.config.product_id)
        price = product.price
        equity = current_equity(state, price)
        now = utc_now()
        self._clear_expired_pause(state, now)
        return {
            "captured_at": now.isoformat(),
            "price": price,
            "cash": state.cash,
            "equity": equity,
            "closed_trades": len(state.closed_trades),
            "entry_count": len(state.entry_timestamps),
            "consecutive_losses": state.consecutive_losses,
            "max_drawdown_pct": state.max_drawdown_pct_seen * 100,
            "trading_halted": self._current_halt_reason(state, now) is not None,
            "halt_reason": self._current_halt_reason(state, now),
            "open_position_side": state.position.side if state.position else None,
            "open_position_quantity": state.position.quantity if state.position else 0.0,
            "open_position_entry_price": state.position.entry_price if state.position else None,
            "open_position_entry_reason": state.position.entry_reason if state.position else None,
            "open_position_market_state": state.position.market_state if state.position else None,
        }

    def run_forever(self) -> None:
        LOGGER.info("Starting bot in %s mode for %s", self.config.mode, self.config.product_id)
        while True:
            try:
                self.run_once()
            except TransientMarketDataError as exc:
                LOGGER.warning("Transient market-data failure: %s", exc)
            except KeyboardInterrupt:
                raise
            except Exception as exc:
                self._handle_system_error(exc)
                LOGGER.exception("Loop failed.")
            time.sleep(self.config.loop_seconds)

    def run_once(self) -> dict[str, Any]:
        state = self.load_state()
        now = utc_now()
        self._clear_expired_pause(state, now)

        product = self.market_data.get_product_info(self.config.product_id)
        if product.trading_disabled:
            raise RuntimeError(f"{product.product_id} is currently disabled for trading.")

        candles = self.market_data.get_candles(
            product_id=self.config.product_id,
            granularity=self.config.granularity,
            limit=self.config.lookback_candles,
        )
        if not candles:
            raise RuntimeError("No candles returned from Coinbase public market data.")

        current_price = candles[-1].close
        equity = current_equity(state, current_price)
        reset_daily_guard_if_needed(state, now, equity)
        state.peak_equity = max(state.peak_equity, equity)
        state.max_drawdown_pct_seen = max(state.max_drawdown_pct_seen, current_drawdown_pct(state, equity))
        self._enforce_global_circuit_breakers(state, now, equity)

        if state.position is not None:
            signal_event = self._manage_open_position(state, product, current_price, candles, now)
        else:
            self._enforce_trade_rate_limits(state, now)
            signal_event = self._maybe_open_position(state, product, candles, current_price, equity, now)

        refreshed_equity = current_equity(state, current_price)
        state.peak_equity = max(state.peak_equity, refreshed_equity)
        state.max_drawdown_pct_seen = max(state.max_drawdown_pct_seen, current_drawdown_pct(state, refreshed_equity))
        save_state(self.config.state_path, state)

        halt_reason = self._current_halt_reason(state, now)
        LOGGER.info(
            "mode=%s price=%.2f cash=%.2f equity=%.2f position=%s halted=%s",
            self.config.mode,
            current_price,
            state.cash,
            refreshed_equity,
            f"{state.position.side}:{state.position.quantity:.6f}" if state.position else "flat",
            halt_reason or "no",
        )
        return {
            "captured_at": now.isoformat(),
            "price": current_price,
            "equity": refreshed_equity,
            "halt_reason": halt_reason,
            "signal_event": signal_event,
        }

    def _manual_kill_switch_reason(self) -> str | None:
        path = self.config.kill_switch_path
        if not path.exists():
            return None
        reason = path.read_text(encoding="utf-8").strip()
        return reason or f"manual_kill_switch:{path}"

    def _clear_expired_pause(self, state: BotState, now: datetime) -> None:
        paused_until = parse_timestamp(state.trading_paused_until)
        if paused_until is not None and now >= paused_until:
            state.trading_paused_until = None
            if not state.trading_disabled:
                state.trading_disabled_reason = None

    def _disable_trading(self, state: BotState, reason: str) -> None:
        if not state.trading_disabled or state.trading_disabled_reason != reason:
            LOGGER.warning("Trading disabled: %s", reason)
        state.trading_disabled = True
        state.trading_disabled_reason = reason
        state.trading_paused_until = None

    def _pause_trading(self, state: BotState, until: datetime, reason: str) -> None:
        existing_pause = parse_timestamp(state.trading_paused_until)
        if existing_pause is None or until > existing_pause:
            LOGGER.warning("Trading paused until %s: %s", until.isoformat(), reason)
            state.trading_paused_until = until.isoformat()
            if not state.trading_disabled:
                state.trading_disabled_reason = reason

    def _current_halt_reason(self, state: BotState, now: datetime) -> str | None:
        if not self.config.trading_enabled:
            return "manual_disable_env"

        kill_switch_reason = self._manual_kill_switch_reason()
        if kill_switch_reason:
            return kill_switch_reason

        paused_until = parse_timestamp(state.trading_paused_until)
        if paused_until is not None and now < paused_until:
            return state.trading_disabled_reason or f"trading_paused_until:{paused_until.isoformat()}"

        if state.trading_disabled:
            return state.trading_disabled_reason or "trading_disabled"
        return None

    def _handle_system_error(self, exc: Exception) -> None:
        try:
            state = self.load_state()
            self._disable_trading(state, f"system_error:{type(exc).__name__}")
            save_state(self.config.state_path, state)
        except Exception:
            LOGGER.exception("Failed to persist system-error halt.")

    def _enforce_global_circuit_breakers(self, state: BotState, now: datetime, equity: float) -> None:
        drawdown_pct = current_drawdown_pct(state, equity)
        if drawdown_pct >= self.config.max_drawdown_pct:
            self._disable_trading(state, f"max_drawdown_pct:{drawdown_pct:.4f}")
            return

        if state.consecutive_losses >= self.config.max_consecutive_losses:
            self._disable_trading(state, f"max_consecutive_losses:{state.consecutive_losses}")
            return

        if daily_loss_limit_hit(state, equity, self.config.daily_max_loss_pct):
            self._pause_trading(
                state,
                now + timedelta(seconds=self.config.trade_rate_pause_seconds),
                "daily_loss_limit_hit",
            )

    def _enforce_trade_rate_limits(self, state: BotState, now: datetime) -> None:
        if len(state.entry_timestamps) >= self.config.max_trades_total:
            self._disable_trading(state, f"max_trades_total:{len(state.entry_timestamps)}")
            return

        trades_last_hour = count_recent_entries(state, now, 3600)
        if trades_last_hour >= self.config.max_trades_per_hour:
            self._pause_trading(
                state,
                now + timedelta(seconds=self.config.trade_rate_pause_seconds),
                f"max_trades_per_hour:{trades_last_hour}",
            )

    def _entry_block_reason(
        self,
        state: BotState,
        candles: list[Candle],
        equity: float,
        now: datetime,
        new_side: str,
    ) -> str | None:
        if state.position is not None or self.config.max_concurrent_trades <= 0:
            return "position_already_open"
        if self.config.max_concurrent_trades <= 1 and state.position is not None:
            return "max_concurrent_trades"
        if self._current_halt_reason(state, now) is not None:
            return self._current_halt_reason(state, now)
        if daily_loss_limit_hit(state, equity, self.config.daily_max_loss_pct):
            return "daily_loss_limit_hit"

        cooldown_is_active, cooldown_reason = cooldown_active(state, now, self.config)
        if cooldown_is_active:
            return cooldown_reason

        last_trade = latest_closed_trade(state)
        if last_trade is not None and last_trade.side != new_side:
            last_closed_at = parse_timestamp(last_trade.closed_at)
            if last_closed_at is not None:
                flip_deadline = last_closed_at + timedelta(seconds=self.config.flip_cooldown_seconds)
                if now < flip_deadline:
                    return f"flip_cooldown_until:{flip_deadline.isoformat()}"

        current_spread = estimated_spread_pct(candles[-1])
        if self.config.max_spread_threshold > 0 and current_spread > self.config.max_spread_threshold:
            return f"spread_filter:{current_spread:.4f}"
        return None

    def _log_signal_event(
        self,
        now: datetime,
        decision,
        *,
        action_candidate: str | None = None,
        reason: str | None = None,
        block_reason: str | None = None,
        executed: bool = False,
        position_side: str | None = None,
        price: float | None = None,
    ) -> dict[str, Any]:
        indicators = decision.indicators
        resolved_action_candidate = action_candidate or decision.action
        event = {
            "timestamp": now.isoformat(),
            "action_candidate": resolved_action_candidate,
            "reason": reason or decision.reason,
            "market_state": decision.market_state,
            "block_reason": block_reason,
            "executed": executed,
            "position_side": position_side,
            "price": price,
            "entry_quality_score": decision_quality_score(resolved_action_candidate, indicators),
            "long_score": int(indicators.get("long_score", 0) or 0),
            "short_score": int(indicators.get("short_score", 0) or 0),
            "trend_up": indicators.get("trend_up"),
            "trend_down": indicators.get("trend_down"),
            "momentum_resume_up": indicators.get("momentum_resume_up"),
            "momentum_resume_down": indicators.get("momentum_resume_down"),
            "strong_push": indicators.get("strong_push"),
            "pullback_detected_long": indicators.get("pullback_detected_long"),
            "pullback_detected_short": indicators.get("pullback_detected_short"),
            "long_rsi_ok": indicators.get("long_rsi_ok"),
            "short_rsi_ok": indicators.get("short_rsi_ok"),
            "near_recent_high": indicators.get("near_recent_high"),
            "near_recent_low": indicators.get("near_recent_low"),
            "entry_market_state_ok": indicators.get("entry_market_state_ok"),
            "move_from_previous_pct": move_from_previous_pct(indicators) * 100,
            "missed_trend": is_missed_trend(
                indicators,
                action_candidate=resolved_action_candidate,
                position_side=position_side,
                config=self.config,
            ),
            "indicators": indicators,
        }
        append_jsonl(self.config.signal_log_path, event)
        return event

    def _maybe_open_position(
        self,
        state: BotState,
        product: ProductInfo,
        candles: list[Candle],
        current_price: float,
        equity: float,
        now: datetime,
    ) -> dict[str, Any]:
        decision = self.strategy.evaluate(candles, None)
        if decision.action not in {"buy", "short"}:
            return self._log_signal_event(
                now,
                decision,
                block_reason=decision.reason if decision.action == "hold" else None,
                price=current_price,
            )

        new_side = "long" if decision.action == "buy" else "short"
        block_reason = self._entry_block_reason(state, candles, equity, now, new_side)
        if block_reason is not None:
            LOGGER.info("Entry blocked: %s", block_reason)
            return self._log_signal_event(now, decision, block_reason=block_reason, price=current_price)

        quote_size = calculate_quote_size(state.cash, equity, current_price, product, self.config)
        if quote_size <= 0:
            LOGGER.info("Position sizing blocked the trade. Cash or risk budget is too small.")
            return self._log_signal_event(now, decision, block_reason="position_sizing", price=current_price)
        if not expected_move_covers_costs(
            current_price=current_price,
            target_price=decision.take_profit,
            notional=quote_size,
            config=self.config,
        ):
            LOGGER.info("Entry blocked: expected move does not cover modeled fees.")
            return self._log_signal_event(
                now,
                decision,
                block_reason="expected_move_below_modeled_cost",
                price=current_price,
            )

        if decision.action == "short" and self.config.mode == "live":
            LOGGER.warning("Short entries are only supported in paper mode with the current spot adapter.")
            return self._log_signal_event(
                now,
                decision,
                block_reason="live_short_unsupported",
                price=current_price,
            )

        if self.config.mode == "live":
            assert self.live_exchange is not None
            trade = self.live_exchange.place_market_buy(product, quote_size)
        else:
            quantity = quote_size / current_price
            trade = (
                paper_fill(
                    price=current_price,
                    quantity=quantity,
                    fee_rate=self.config.fee_rate,
                    slippage_bps=self.config.slippage_bps,
                )
                if decision.action == "buy"
                else paper_exit_fill(
                    price=current_price,
                    quantity=quantity,
                    fee_rate=self.config.fee_rate,
                    slippage_bps=self.config.slippage_bps,
                )
            )

        if decision.action == "buy":
            total_cost = (trade.price * trade.quantity) + trade.fees_paid
            state.cash -= total_cost
        else:
            gross_proceeds = trade.price * trade.quantity
            state.cash += gross_proceeds - trade.fees_paid

        position_size = trade.price * trade.quantity
        state.position = Position(
            side=new_side,
            quantity=trade.quantity,
            entry_price=trade.price,
            position_size=position_size,
            stop_loss=decision.stop_loss
            or (
                current_price * (1 - self.config.stop_loss_pct)
                if decision.action == "buy"
                else current_price * (1 + self.config.stop_loss_pct)
            ),
            take_profit=decision.take_profit
            or (
                current_price * (1 + self.config.take_profit_pct)
                if decision.action == "buy"
                else current_price * (1 - self.config.take_profit_pct)
            ),
            trailing_stop=decision.trailing_stop
            or (
                current_price * (1 - self.config.trailing_stop_pct_for_market_state(decision.market_state))
                if decision.action == "buy"
                else current_price * (1 + self.config.trailing_stop_pct_for_market_state(decision.market_state))
            ),
            highest_price=trade.price,
            lowest_price=trade.price,
            opened_at=now.isoformat(),
            mode=self.config.mode,
            entry_order_id=trade.order_id,
            entry_fees_paid=trade.fees_paid,
            entry_reason=decision.reason,
            market_state=decision.market_state,
            entry_indicators=decision.indicators,
            entry_quality_score=decision_quality_score(decision.action, decision.indicators),
        )
        state.entry_timestamps.append(now.isoformat())
        state.last_signal_at = now.isoformat()
        LOGGER.info(
            "Opened %s position qty=%.6f notional=%.2f entry=%.2f stop=%.2f take=%.2f reason=%s market_state=%s",
            state.position.side,
            trade.quantity,
            position_size,
            trade.price,
            state.position.stop_loss,
            state.position.take_profit,
            decision.reason,
            decision.market_state,
        )
        return self._log_signal_event(
            now,
            decision,
            executed=True,
            position_side=state.position.side,
            price=current_price,
        )

    def _manage_open_position(
        self,
        state: BotState,
        product: ProductInfo,
        current_price: float,
        candles: list[Candle],
        now: datetime,
    ) -> dict[str, Any]:
        position = state.position
        assert position is not None

        opened_at = parse_timestamp(position.opened_at) or now
        decision = self.strategy.evaluate(candles, position)
        market_state = decision.market_state
        trail_pct = self.config.trailing_stop_pct_for_market_state(market_state)

        position.highest_price = max(position.highest_price, current_price)
        if position.lowest_price is None:
            position.lowest_price = position.entry_price
        position.lowest_price = min(position.lowest_price, current_price)

        unrealized_move = move_pct_from_entry(position, current_price)
        if market_state == "CHOPPY" and unrealized_move >= self.config.chop_profit_lock_trigger_pct:
            if position.side == "long":
                position.stop_loss = max(
                    position.stop_loss,
                    position.entry_price * (1 + self.config.chop_profit_lock_stop_buffer_pct),
                )
            else:
                position.stop_loss = min(
                    position.stop_loss,
                    position.entry_price * (1 - self.config.chop_profit_lock_stop_buffer_pct),
                )

        if position.side == "long":
            position.trailing_stop = max(
                position.trailing_stop,
                position.highest_price * (1 - trail_pct),
            )
            effective_stop = max(position.stop_loss, position.trailing_stop)
        else:
            position.trailing_stop = min(
                position.trailing_stop,
                position.lowest_price * (1 + trail_pct),
            )
            effective_stop = min(position.stop_loss, position.trailing_stop)

        exit_reason = None
        exit_price = None
        elapsed = now - opened_at
        max_duration = timedelta(minutes=self.config.max_trade_duration_minutes)
        if position.side == "long":
            if current_price <= effective_stop:
                exit_reason = "stop_loss"
                exit_price = effective_stop
            elif current_price >= position.take_profit:
                exit_reason = "take_profit"
                exit_price = position.take_profit
        else:
            if current_price >= effective_stop:
                exit_reason = "stop_loss"
                exit_price = effective_stop
            elif current_price <= position.take_profit:
                exit_reason = "take_profit"
                exit_price = position.take_profit

        if exit_reason is None:
            if market_state == "CHOPPY" and elapsed >= timedelta(minutes=self.config.chop_stall_minutes):
                distance_from_entry = abs(current_price - position.entry_price) / position.entry_price
                if distance_from_entry <= self.config.chop_stall_exit_band_pct:
                    exit_reason = "chop_stall_exit"
                    exit_price = current_price

        if exit_reason is None and elapsed >= max_duration:
            exit_reason = "duration_watchdog"
            exit_price = current_price

        if exit_reason is None:
            if position.side == "long" and decision.action == "sell":
                exit_reason = decision.reason
                exit_price = current_price
            elif position.side == "short" and decision.action == "cover":
                exit_reason = decision.reason
                exit_price = current_price

        if exit_reason is None or exit_price is None:
            return self._log_signal_event(
                now,
                decision,
                action_candidate="hold",
                executed=False,
                position_side=position.side,
                price=current_price,
            )

        if self.config.mode == "live":
            assert self.live_exchange is not None
            trade = self.live_exchange.place_market_sell(product, position.quantity)
        else:
            trade = (
                paper_exit_fill(
                    price=exit_price,
                    quantity=position.quantity,
                    fee_rate=self.config.fee_rate,
                    slippage_bps=self.config.slippage_bps,
                )
                if position.side == "long"
                else paper_fill(
                    price=exit_price,
                    quantity=position.quantity,
                    fee_rate=self.config.fee_rate,
                    slippage_bps=self.config.slippage_bps,
                )
            )

        if position.side == "long":
            gross_proceeds = trade.price * trade.quantity
            state.cash += gross_proceeds - trade.fees_paid
            pnl = (trade.price - position.entry_price) * trade.quantity
        else:
            total_cover_cost = (trade.price * trade.quantity) + trade.fees_paid
            state.cash -= total_cover_cost
            pnl = (position.entry_price - trade.price) * trade.quantity

        total_fees = trade.fees_paid + position.entry_fees_paid
        pnl -= total_fees
        pnl_pct = pnl / position.position_size if position.position_size > 0 else 0.0
        duration_seconds = max(0.0, (now - opened_at).total_seconds())
        result = "WIN" if pnl > 0 else "LOSS" if pnl < 0 else "FLAT"

        closed_trade = ClosedTrade(
            opened_at=position.opened_at,
            closed_at=now.isoformat(),
            side=position.side,
            quantity=trade.quantity,
            position_size=position.position_size,
            entry_price=position.entry_price,
            exit_price=trade.price,
            pnl=pnl,
            pnl_pct=pnl_pct,
            reason=exit_reason,
            reason_tag=exit_reason,
            result=result,
            trade_duration_seconds=duration_seconds,
            fees_paid=total_fees,
            mode=self.config.mode,
            entry_order_id=position.entry_order_id,
            exit_order_id=trade.order_id,
            entry_reason=position.entry_reason,
            market_state=position.market_state,
            entry_indicators=position.entry_indicators,
            entry_quality_score=position.entry_quality_score,
        )
        state.closed_trades.append(closed_trade)
        append_trade(self.config.trade_log_path, closed_trade)
        if pnl < 0:
            state.last_loss_at = now.isoformat()
            state.consecutive_losses += 1
        elif pnl > 0:
            state.consecutive_losses = 0
        else:
            state.consecutive_losses = 0

        LOGGER.info(
            "Closed %s position qty=%.6f exit=%.2f pnl=%.2f result=%s reason=%s",
            position.side,
            trade.quantity,
            trade.price,
            pnl,
            result,
            exit_reason,
        )
        state.position = None
        self._enforce_global_circuit_breakers(state, now, current_equity(state, trade.price))
        self._enforce_trade_rate_limits(state, now)
        return self._log_signal_event(
            now,
            decision,
            action_candidate="sell" if position.side == "long" else "cover",
            reason=exit_reason,
            executed=True,
            position_side=position.side,
            price=trade.price,
        )

    def run_session(self, minutes: float) -> dict[str, Any]:
        if minutes <= 0:
            raise ValueError("Session duration must be greater than 0 minutes.")

        started_at = utc_now()
        starting_snapshot = self.snapshot()
        starting_state = self.load_state()
        starting_trade_count = len(starting_state.closed_trades)
        deadline = time.monotonic() + (minutes * 60)
        cycles = 0
        errors = 0
        session_peak_equity = starting_snapshot["equity"]
        session_max_drawdown = 0.0
        signal_events: list[dict[str, Any]] = []
        transient_market_data_errors = 0

        while True:
            try:
                cycle = self.run_once()
                cycles += 1
                if "signal_event" in cycle:
                    signal_events.append(cycle["signal_event"])
                session_peak_equity = max(session_peak_equity, cycle["equity"])
                if session_peak_equity > 0:
                    drawdown = (session_peak_equity - cycle["equity"]) / session_peak_equity
                    session_max_drawdown = max(session_max_drawdown, drawdown)
            except TransientMarketDataError as exc:
                transient_market_data_errors += 1
                LOGGER.warning("Transient market-data failure: %s", exc)
            except KeyboardInterrupt:
                raise
            except Exception as exc:
                errors += 1
                self._handle_system_error(exc)
                LOGGER.exception("Session cycle failed.")
                break

            remaining_seconds = deadline - time.monotonic()
            if remaining_seconds <= 0:
                break
            time.sleep(min(self.config.loop_seconds, remaining_seconds))

        ending_snapshot = self.snapshot()
        ending_state = self.load_state()
        session_trades = ending_state.closed_trades[starting_trade_count:]
        realized_pnl = sum(trade.pnl for trade in session_trades)
        wins = sum(1 for trade in session_trades if trade.result == "WIN")
        losses = sum(1 for trade in session_trades if trade.result == "LOSS")
        total_trades = len(session_trades)
        final_pnl = ending_snapshot["equity"] - starting_snapshot["equity"]
        halt_reason = self._current_halt_reason(ending_state, utc_now())
        reason_histogram = signal_reason_histogram(signal_events, field="reason")
        hold_reason_histogram = signal_reason_histogram(signal_events, field="reason", require_hold=True)
        block_reason_histogram = signal_reason_histogram(signal_events, field="block_reason")
        market_state_histogram = signal_reason_histogram(signal_events, field="market_state")
        missed_trend_count = sum(1 for event in signal_events if event.get("missed_trend"))

        return {
            "started_at": started_at.isoformat(),
            "ended_at": utc_now().isoformat(),
            "duration_minutes": minutes,
            "mode": self.config.mode,
            "product_id": self.config.product_id,
            "granularity": self.config.granularity,
            "loop_seconds": self.config.loop_seconds,
            "cycles": cycles,
            "errors": errors,
            "transient_market_data_errors": transient_market_data_errors,
            "starting": starting_snapshot,
            "ending": ending_snapshot,
            "total_trades": total_trades,
            "session_closed_trades": total_trades,
            "wins": wins,
            "losses": losses,
            "win_rate": (wins / total_trades * 100) if total_trades else 0.0,
            "max_drawdown": session_max_drawdown * 100,
            "max_drawdown_pct": session_max_drawdown * 100,
            "realized_pnl": realized_pnl,
            "final_pnl": final_pnl,
            "equity_change": final_pnl,
            "signal_events": len(signal_events),
            "reason_histogram": reason_histogram,
            "hold_reason_histogram": hold_reason_histogram,
            "block_reason_histogram": block_reason_histogram,
            "market_state_histogram": market_state_histogram,
            "missed_trend_count": missed_trend_count,
            "return_pct": (
                ((ending_snapshot["equity"] / starting_snapshot["equity"]) - 1) * 100
                if starting_snapshot["equity"]
                else 0.0
            ),
            "halt_reason": halt_reason,
            "trades": [trade.to_json() for trade in session_trades],
        }
