from __future__ import annotations


def ema(values: list[float], period: int) -> list[float]:
    if period <= 0:
        raise ValueError("EMA period must be positive.")
    if not values:
        return []

    multiplier = 2 / (period + 1)
    ema_values = [values[0]]
    for price in values[1:]:
        ema_values.append((price - ema_values[-1]) * multiplier + ema_values[-1])
    return ema_values


def rsi(values: list[float], period: int) -> list[float | None]:
    if period <= 0:
        raise ValueError("RSI period must be positive.")
    if len(values) < 2:
        return [None for _ in values]

    gains = [0.0]
    losses = [0.0]
    for index in range(1, len(values)):
        delta = values[index] - values[index - 1]
        gains.append(max(delta, 0.0))
        losses.append(abs(min(delta, 0.0)))

    avg_gain = sum(gains[1 : period + 1]) / period if len(values) > period else 0.0
    avg_loss = sum(losses[1 : period + 1]) / period if len(values) > period else 0.0

    output: list[float | None] = [None] * len(values)
    for index in range(period, len(values)):
        if index > period:
            avg_gain = ((avg_gain * (period - 1)) + gains[index]) / period
            avg_loss = ((avg_loss * (period - 1)) + losses[index]) / period
        if avg_loss == 0:
            output[index] = 100.0
            continue
        rs = avg_gain / avg_loss
        output[index] = 100.0 - (100.0 / (1.0 + rs))
    return output
