"""Shared conservative governance for Hyperliquid's public ``/info`` budget."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from math import ceil
from random import uniform
from threading import Condition, Lock
from time import monotonic, time
from typing import Any, Callable, Deque, Mapping


HYPERLIQUID_REST_WEIGHT_PER_MINUTE = 1_200
DEFAULT_OPERATING_WEIGHT_BUDGET = 900
_WINDOW_SECONDS = 60.0
_VARIABLE_RESPONSE_RESERVE = 100
_TWO_WEIGHT_INFO_TYPES = {
    "l2Book", "allMids", "clearinghouseState", "orderStatus", "spotClearinghouseState", "exchangeStatus",
}
_TWENTY_ITEM_RESPONSE_TYPES = {
    "recentTrades", "historicalOrders", "userFills", "userFillsByTime", "fundingHistory", "userFunding",
    "nonUserFundingUpdates", "twapHistory", "userTwapSliceFills", "userTwapSliceFillsByTime",
    "delegatorHistory", "delegatorRewards", "validatorStats",
}


@dataclass
class _Reservation:
    request_type: str
    created_at: float
    weight: int
    reservation_weight: int


class HyperliquidInfoRateLimiter:
    """Thread-safe sliding-window limiter with a process-wide cooldown.

    Variable-size endpoints reserve their documented maximum additional
    response weight before issuing a request.  Once a valid response arrives,
    unused reservation is released while the actual documented weight remains
    in the rolling minute.  This makes parallel backfill safe without reducing
    Phase B to a single worker.
    """

    def __init__(
        self, *, operating_budget: int = DEFAULT_OPERATING_WEIGHT_BUDGET,
        documented_limit: int = HYPERLIQUID_REST_WEIGHT_PER_MINUTE,
        backoff_initial_seconds: float = 2.0, backoff_max_seconds: float = 30.0,
        jitter_seconds: float = 0.5, clock: Callable[[], float] = monotonic,
        wall_clock: Callable[[], float] = time, jitter: Callable[[float, float], float] = uniform,
    ) -> None:
        if not 1 <= operating_budget <= documented_limit:
            raise ValueError("Hyperliquid operating budget must be between 1 and the documented IP limit.")
        if backoff_initial_seconds <= 0 or backoff_max_seconds < backoff_initial_seconds or jitter_seconds < 0:
            raise ValueError("Hyperliquid rate-limit backoff configuration is invalid.")
        self.operating_budget = operating_budget
        self.documented_limit = documented_limit
        self.backoff_initial_seconds = backoff_initial_seconds
        self.backoff_max_seconds = backoff_max_seconds
        self.jitter_seconds = jitter_seconds
        self._clock = clock
        self._wall_clock = wall_clock
        self._jitter = jitter
        self._reservations: Deque[_Reservation] = deque()
        self._condition = Condition(Lock())
        self._throttle_until = 0.0
        self._throttle_until_epoch: float | None = None
        self._consecutive_429 = 0
        self._429_count = 0
        self._retry_count = 0

    def acquire(self, payload: Mapping[str, Any]) -> _Reservation:
        request_type = str(payload.get("type") or "")
        reservation_weight = self.reserved_weight(request_type)
        if reservation_weight > self.operating_budget:
            raise ValueError(
                f"Hyperliquid operating budget {self.operating_budget} is below the required reservation "
                f"for {request_type or 'info'} ({reservation_weight})."
            )
        with self._condition:
            while True:
                now = self._clock()
                self._expire(now)
                used_weight = sum(item.weight for item in self._reservations)
                if now >= self._throttle_until and used_weight + reservation_weight <= self.operating_budget:
                    reservation = _Reservation(request_type, now, reservation_weight, reservation_weight)
                    self._reservations.append(reservation)
                    return reservation
                waits: list[float] = []
                if now < self._throttle_until:
                    waits.append(self._throttle_until - now)
                if used_weight + reservation_weight > self.operating_budget and self._reservations:
                    waits.append(max(0.01, self._reservations[0].created_at + _WINDOW_SECONDS - now))
                self._condition.wait(timeout=max(0.01, min(waits) if waits else 0.01))

    def settle(self, reservation: _Reservation, response_payload: object | None) -> None:
        """Replace a conservative reservation with the documented actual weight."""
        with self._condition:
            if reservation not in self._reservations:
                return
            if response_payload is not None:
                reservation.weight = self.actual_weight(reservation.request_type, response_payload)
                self._consecutive_429 = 0
            self._condition.notify_all()

    def register_429(self, retry_after: str | None = None) -> float:
        """Apply a cooldown shared by every worker and return its remaining delay."""
        with self._condition:
            now = self._clock()
            self._429_count += 1
            self._consecutive_429 += 1
            delay = _retry_after_seconds(retry_after, self._wall_clock())
            if delay is None:
                exponential = min(self.backoff_max_seconds, self.backoff_initial_seconds * (2 ** (self._consecutive_429 - 1)))
                delay = min(self.backoff_max_seconds, exponential + (self._jitter(0.0, self.jitter_seconds) if self.jitter_seconds else 0.0))
            self._throttle_until = max(self._throttle_until, now + delay)
            self._throttle_until_epoch = max(self._throttle_until_epoch or 0.0, self._wall_clock() + delay)
            self._condition.notify_all()
            return max(0.0, self._throttle_until - now)

    def record_retry(self) -> None:
        with self._condition:
            self._retry_count += 1

    def telemetry(self) -> dict[str, object]:
        with self._condition:
            now = self._clock()
            self._expire(now)
            estimated_weight = sum(item.weight for item in self._reservations)
            throttled = now < self._throttle_until
            throttle_until = None
            if throttled and self._throttle_until_epoch is not None:
                throttle_until = datetime.fromtimestamp(self._throttle_until_epoch, tz=timezone.utc).isoformat()
            return {
                "state": "THROTTLED" if throttled else "READY",
                "requests_last_minute": len(self._reservations),
                "estimated_weight_last_minute": estimated_weight,
                "limiter_utilization": round(estimated_weight / self.operating_budget, 4),
                "currently_throttled": throttled,
                "throttle_until": throttle_until,
                "429_count": self._429_count,
                "retry_count": self._retry_count,
                "operating_budget": self.operating_budget,
                "documented_limit": self.documented_limit,
            }

    @staticmethod
    def reserved_weight(request_type: str) -> int:
        return _base_weight(request_type) + (_VARIABLE_RESPONSE_RESERVE if _is_variable_response(request_type) else 0)

    @staticmethod
    def actual_weight(request_type: str, response_payload: object) -> int:
        base = _base_weight(request_type)
        if not isinstance(response_payload, list):
            return base
        if request_type in _TWENTY_ITEM_RESPONSE_TYPES:
            return base + ceil(len(response_payload) / 20)
        if request_type == "candleSnapshot":
            return base + ceil(len(response_payload) / 60)
        return base

    def _expire(self, now: float) -> None:
        while self._reservations and now - self._reservations[0].created_at >= _WINDOW_SECONDS:
            self._reservations.popleft()
        if now >= self._throttle_until:
            self._throttle_until_epoch = None


def _base_weight(request_type: str) -> int:
    if request_type in _TWO_WEIGHT_INFO_TYPES:
        return 2
    if request_type == "userRole":
        return 60
    return 20


def _is_variable_response(request_type: str) -> bool:
    return request_type in _TWENTY_ITEM_RESPONSE_TYPES or request_type == "candleSnapshot"


def _retry_after_seconds(value: str | None, now_epoch: float) -> float | None:
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        try:
            return max(0.0, parsedate_to_datetime(value).timestamp() - now_epoch)
        except (TypeError, ValueError, IndexError):
            return None


_shared_limiters: dict[str, HyperliquidInfoRateLimiter] = {}
_shared_limiters_lock = Lock()


def shared_hyperliquid_info_limiter(
    info_url: str, *, operating_budget: int = DEFAULT_OPERATING_WEIGHT_BUDGET,
    backoff_initial_seconds: float = 2.0, backoff_max_seconds: float = 30.0, jitter_seconds: float = 0.5,
) -> HyperliquidInfoRateLimiter:
    """Return the one process-wide limiter for an API URL.

    If differently configured services share the same process, use the lower
    budget so coordination remains safe rather than creating an independent
    allowance for either caller.
    """
    key = info_url.rstrip("/").lower()
    with _shared_limiters_lock:
        limiter = _shared_limiters.get(key)
        if limiter is None:
            limiter = HyperliquidInfoRateLimiter(
                operating_budget=operating_budget, backoff_initial_seconds=backoff_initial_seconds,
                backoff_max_seconds=backoff_max_seconds, jitter_seconds=jitter_seconds,
            )
            _shared_limiters[key] = limiter
        elif operating_budget < limiter.operating_budget:
            limiter.operating_budget = operating_budget
        return limiter
