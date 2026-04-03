from __future__ import annotations


def validate_profile_limits(settings, bot_definitions) -> list[str]:
    issues: list[str] = []
    total_max_notional = 0.0
    estimated_orders_per_minute = 0.0

    for definition in bot_definitions:
        config = definition.base_config
        profile = definition.strategy_profile
        total_max_notional += config.max_position_size
        estimated_orders_per_minute += config.max_trades_per_hour / 60.0

        if config.max_position_size > settings.safety.max_notional_per_bot:
            issues.append(
                f"{definition.bot_id} max_position_size={config.max_position_size:.2f} exceeds "
                f"MAX_NOTIONAL_PER_BOT={settings.safety.max_notional_per_bot:.2f}"
            )

        allowed_hold = settings.safety.max_hold_sec_per_profile.get(
            definition.bot_id,
            settings.safety.max_hold_sec_per_profile.get(definition.family),
        )
        if allowed_hold is not None and profile.max_hold_seconds > allowed_hold:
            issues.append(
                f"{definition.bot_id} max_hold_seconds={profile.max_hold_seconds} exceeds allowed_hold={allowed_hold}"
            )

    if total_max_notional > settings.safety.max_portfolio_notional:
        issues.append(
            f"aggregate max_position_size={total_max_notional:.2f} exceeds "
            f"MAX_PORTFOLIO_NOTIONAL={settings.safety.max_portfolio_notional:.2f}"
        )

    if estimated_orders_per_minute > settings.safety.max_order_rate_per_minute:
        issues.append(
            f"estimated order rate={estimated_orders_per_minute:.2f}/min exceeds "
            f"MAX_ORDER_RATE_PER_MINUTE={settings.safety.max_order_rate_per_minute:.2f}"
        )

    if len(bot_definitions) > settings.safety.max_open_positions:
        issues.append(
            f"configured bots={len(bot_definitions)} exceeds MAX_OPEN_POSITIONS={settings.safety.max_open_positions}"
        )

    return issues


def evaluate_cycle_guardrails(bundle, settings) -> list[str]:
    flags: list[str] = []
    if bundle.total_drawdown > settings.safety.max_drawdown_per_cycle:
        flags.append(
            f"cycle drawdown {bundle.total_drawdown:.2%} exceeded {settings.safety.max_drawdown_per_cycle:.2%}"
        )
    return flags
