"""Shared conservative governance for Hyperliquid's public ``/info`` budget."""

from __future__ import annotations

from collections import deque
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from math import ceil
from pathlib import Path
from random import uniform
import sqlite3
from threading import Condition, Lock
from time import monotonic, sleep, time
from typing import Any, Callable, Deque, Iterator, Mapping
from uuid import uuid4


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
_coordinator_initialization_lock = Lock()


@dataclass
class _Reservation:
    request_type: str
    created_at: float
    weight: int
    reservation_weight: int
    coordination_id: str | None = None


class _SQLiteRateLimitCoordinator:
    """Atomic host-local /info budget coordination for independent processes.

    The shared artifacts database is intentionally used as a small local
    control plane, not as a network dependency.  Every reservation is made in
    one ``BEGIN IMMEDIATE`` transaction, so separate CLI and Control Center
    processes on the same machine consume one conservative allowance.
    """

    def __init__(self, path: Path, endpoint: str, *, operating_budget: int, documented_limit: int,
                 wall_clock: Callable[[], float], sleeper: Callable[[float], None] = sleep) -> None:
        self.path = path
        self.endpoint = endpoint
        self.operating_budget = operating_budget
        self.documented_limit = documented_limit
        self._wall_clock = wall_clock
        self._sleep = sleeper
        # SQLite schema initialization itself obtains a write lock.  Serialize
        # same-process constructors so concurrent startup cannot turn a safe
        # conservative merge into a transient "database is locked" failure.
        with _coordinator_initialization_lock:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with self._connect() as connection:
                connection.executescript("""
                CREATE TABLE IF NOT EXISTS copy_hyperliquid_rate_limit_reservations (
                    reservation_id TEXT PRIMARY KEY, endpoint TEXT NOT NULL, request_type TEXT NOT NULL,
                    created_at_epoch REAL NOT NULL, reservation_weight INTEGER NOT NULL, actual_weight INTEGER
                );
                CREATE INDEX IF NOT EXISTS idx_copy_hl_rate_limit_window
                    ON copy_hyperliquid_rate_limit_reservations(endpoint, created_at_epoch);
                CREATE TABLE IF NOT EXISTS copy_hyperliquid_rate_limit_cooldowns (
                    endpoint TEXT PRIMARY KEY, throttle_until_epoch REAL NOT NULL DEFAULT 0,
                    consecutive_429 INTEGER NOT NULL DEFAULT 0, rate_limit_count INTEGER NOT NULL DEFAULT 0,
                    retry_count INTEGER NOT NULL DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS copy_hyperliquid_rate_limit_budgets (
                    endpoint TEXT PRIMARY KEY, operating_budget INTEGER NOT NULL,
                    updated_at_epoch REAL NOT NULL
                );
                """)
            self.operating_budget = self.set_operating_budget(operating_budget)

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self.path, timeout=5.0, check_same_thread=False)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout=5000")
        connection.execute("PRAGMA journal_mode=WAL")
        try:
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def acquire(self, request_type: str, reservation_weight: int) -> _Reservation:
        while True:
            now = self._wall_clock()
            wait_seconds = 0.01
            reservation_id: str | None = None
            with self._connect() as connection:
                connection.execute("BEGIN IMMEDIATE")
                try:
                    connection.execute(
                        "DELETE FROM copy_hyperliquid_rate_limit_reservations WHERE endpoint=? AND created_at_epoch<=?",
                        (self.endpoint, now - _WINDOW_SECONDS),
                    )
                    connection.execute(
                        """INSERT OR IGNORE INTO copy_hyperliquid_rate_limit_cooldowns(endpoint)
                           VALUES (?)""", (self.endpoint,),
                    )
                    cooldown = connection.execute(
                        "SELECT throttle_until_epoch FROM copy_hyperliquid_rate_limit_cooldowns WHERE endpoint=?",
                        (self.endpoint,),
                    ).fetchone()
                    budget = int(connection.execute(
                        "SELECT operating_budget FROM copy_hyperliquid_rate_limit_budgets WHERE endpoint=?", (self.endpoint,),
                    ).fetchone()["operating_budget"])
                    # Another local process may have initialized more
                    # conservatively after this object was created.  Never
                    # retain a larger in-memory allowance.
                    self.operating_budget = min(self.operating_budget, budget)
                    used = int(connection.execute(
                        """SELECT COALESCE(SUM(COALESCE(actual_weight, reservation_weight)), 0)
                           FROM copy_hyperliquid_rate_limit_reservations WHERE endpoint=?""", (self.endpoint,),
                    ).fetchone()[0])
                    throttle_until = float(cooldown["throttle_until_epoch"])
                    if now >= throttle_until and used + reservation_weight <= budget:
                        reservation_id = uuid4().hex
                        connection.execute(
                            """INSERT INTO copy_hyperliquid_rate_limit_reservations(
                                reservation_id, endpoint, request_type, created_at_epoch, reservation_weight)
                               VALUES (?, ?, ?, ?, ?)""",
                            (reservation_id, self.endpoint, request_type, now, reservation_weight),
                        )
                    else:
                        waits: list[float] = []
                        if now < throttle_until:
                            waits.append(throttle_until - now)
                        if used + reservation_weight > budget:
                            oldest = connection.execute(
                                """SELECT MIN(created_at_epoch) FROM copy_hyperliquid_rate_limit_reservations
                                   WHERE endpoint=?""", (self.endpoint,),
                            ).fetchone()[0]
                            if oldest is not None:
                                waits.append(max(0.01, float(oldest) + _WINDOW_SECONDS - now))
                        wait_seconds = max(0.01, min(waits) if waits else 0.01)
                    connection.commit()
                except Exception:
                    connection.rollback()
                    raise
            if reservation_id is not None:
                return _Reservation(request_type, now, reservation_weight, reservation_weight, reservation_id)
            self._sleep(wait_seconds)

    def set_operating_budget(self, requested_budget: int) -> int:
        """Atomically adopt the minimum host-local configured allowance.

        There is deliberately no automatic increase: relaxing a conservative
        budget requires an explicit coordinator reset/reconfiguration rather
        than process-start order.
        """
        if not 1 <= requested_budget <= self.documented_limit:
            raise ValueError("Hyperliquid operating budget must be between 1 and the documented IP limit.")
        now = self._wall_clock()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                connection.execute(
                    """INSERT INTO copy_hyperliquid_rate_limit_budgets(endpoint, operating_budget, updated_at_epoch)
                       VALUES (?, ?, ?)
                       ON CONFLICT(endpoint) DO UPDATE SET
                         operating_budget=MIN(copy_hyperliquid_rate_limit_budgets.operating_budget, excluded.operating_budget),
                         updated_at_epoch=excluded.updated_at_epoch""",
                    (self.endpoint, requested_budget, now),
                )
                effective = int(connection.execute(
                    "SELECT operating_budget FROM copy_hyperliquid_rate_limit_budgets WHERE endpoint=?", (self.endpoint,),
                ).fetchone()["operating_budget"])
                connection.commit()
                self.operating_budget = effective
                return effective
            except Exception:
                connection.rollback()
                raise

    def current_budget(self) -> int:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT operating_budget FROM copy_hyperliquid_rate_limit_budgets WHERE endpoint=?", (self.endpoint,),
            ).fetchone()
        if row is not None:
            self.operating_budget = min(self.operating_budget, int(row["operating_budget"]))
        return self.operating_budget

    def settle(self, reservation: _Reservation, response_payload: object | None) -> None:
        if not reservation.coordination_id or response_payload is None:
            return
        actual_weight = HyperliquidInfoRateLimiter.actual_weight(reservation.request_type, response_payload)
        with self._connect() as connection:
            connection.execute(
                """UPDATE copy_hyperliquid_rate_limit_reservations SET actual_weight=?
                   WHERE reservation_id=? AND endpoint=?""",
                (actual_weight, reservation.coordination_id, self.endpoint),
            )
            connection.execute(
                """UPDATE copy_hyperliquid_rate_limit_cooldowns SET consecutive_429=0 WHERE endpoint=?""",
                (self.endpoint,),
            )

    def register_429(self, retry_after: str | None, *, backoff_initial_seconds: float,
                     backoff_max_seconds: float, jitter_seconds: float,
                     jitter: Callable[[float, float], float]) -> float:
        now = self._wall_clock()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                connection.execute("INSERT OR IGNORE INTO copy_hyperliquid_rate_limit_cooldowns(endpoint) VALUES (?)", (self.endpoint,))
                state = connection.execute(
                    """SELECT throttle_until_epoch, consecutive_429 FROM copy_hyperliquid_rate_limit_cooldowns
                       WHERE endpoint=?""", (self.endpoint,),
                ).fetchone()
                consecutive = int(state["consecutive_429"]) + 1
                delay = _retry_after_seconds(retry_after, now)
                if delay is None:
                    exponential = min(backoff_max_seconds, backoff_initial_seconds * (2 ** (consecutive - 1)))
                    delay = min(backoff_max_seconds, exponential + (jitter(0.0, jitter_seconds) if jitter_seconds else 0.0))
                throttle_until = max(float(state["throttle_until_epoch"]), now + delay)
                connection.execute(
                    """UPDATE copy_hyperliquid_rate_limit_cooldowns
                       SET throttle_until_epoch=?, consecutive_429=?, rate_limit_count=rate_limit_count+1
                       WHERE endpoint=?""",
                    (throttle_until, consecutive, self.endpoint),
                )
                connection.commit()
            except Exception:
                connection.rollback()
                raise
        return max(0.0, throttle_until - now)

    def record_retry(self) -> None:
        with self._connect() as connection:
            connection.execute("INSERT OR IGNORE INTO copy_hyperliquid_rate_limit_cooldowns(endpoint) VALUES (?)", (self.endpoint,))
            connection.execute(
                "UPDATE copy_hyperliquid_rate_limit_cooldowns SET retry_count=retry_count+1 WHERE endpoint=?", (self.endpoint,),
            )

    def telemetry(self) -> dict[str, object]:
        now = self._wall_clock()
        with self._connect() as connection:
            connection.execute("DELETE FROM copy_hyperliquid_rate_limit_reservations WHERE endpoint=? AND created_at_epoch<=?", (self.endpoint, now - _WINDOW_SECONDS))
            connection.execute("INSERT OR IGNORE INTO copy_hyperliquid_rate_limit_cooldowns(endpoint) VALUES (?)", (self.endpoint,))
            state = connection.execute(
                """SELECT throttle_until_epoch, rate_limit_count, retry_count FROM copy_hyperliquid_rate_limit_cooldowns
                   WHERE endpoint=?""", (self.endpoint,),
            ).fetchone()
            rows = connection.execute(
                """SELECT COUNT(*) AS requests, COALESCE(SUM(COALESCE(actual_weight, reservation_weight)), 0) AS weight
                   FROM copy_hyperliquid_rate_limit_reservations WHERE endpoint=?""", (self.endpoint,),
            ).fetchone()
            budget = int(connection.execute(
                "SELECT operating_budget FROM copy_hyperliquid_rate_limit_budgets WHERE endpoint=?", (self.endpoint,),
            ).fetchone()["operating_budget"])
        self.operating_budget = min(self.operating_budget, budget)
        throttle_until = float(state["throttle_until_epoch"])
        throttled = now < throttle_until
        estimated_weight = int(rows["weight"])
        return {
            "state": "THROTTLED" if throttled else "READY", "requests_last_minute": int(rows["requests"]),
            "estimated_weight_last_minute": estimated_weight,
            "limiter_utilization": round(estimated_weight / self.operating_budget, 4),
            "currently_throttled": throttled,
            "throttle_until": datetime.fromtimestamp(throttle_until, tz=timezone.utc).isoformat() if throttled else None,
            "429_count": int(state["rate_limit_count"]), "retry_count": int(state["retry_count"]),
            "operating_budget": self.operating_budget, "documented_limit": self.documented_limit,
            "coordination_scope": "host_sqlite",
        }


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
        coordination_path: Path | None = None, sleeper: Callable[[float], None] = sleep,
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
        self._coordinator = (
            _SQLiteRateLimitCoordinator(
                coordination_path, "hyperliquid-info:" + "default", operating_budget=operating_budget,
                documented_limit=documented_limit, wall_clock=wall_clock, sleeper=sleeper,
            ) if coordination_path is not None else None
        )

    def acquire(self, payload: Mapping[str, Any]) -> _Reservation:
        request_type = str(payload.get("type") or "")
        reservation_weight = self.reserved_weight(request_type)
        if self._coordinator is not None:
            self.operating_budget = self._coordinator.current_budget()
        if reservation_weight > self.operating_budget:
            raise ValueError(
                f"Hyperliquid operating budget {self.operating_budget} is below the required reservation "
                f"for {request_type or 'info'} ({reservation_weight})."
            )
        if self._coordinator is not None:
            return self._coordinator.acquire(request_type, reservation_weight)
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

    def set_operating_budget(self, operating_budget: int) -> int:
        """Monotonically lower the local and coordinated operating budget."""
        if not 1 <= operating_budget <= self.documented_limit:
            raise ValueError("Hyperliquid operating budget must be between 1 and the documented IP limit.")
        if self._coordinator is not None:
            self.operating_budget = self._coordinator.set_operating_budget(operating_budget)
            return self.operating_budget
        with self._condition:
            self.operating_budget = min(self.operating_budget, operating_budget)
            self._condition.notify_all()
            return self.operating_budget

    def settle(self, reservation: _Reservation, response_payload: object | None) -> None:
        """Replace a conservative reservation with the documented actual weight."""
        if self._coordinator is not None:
            self._coordinator.settle(reservation, response_payload)
            return
        with self._condition:
            if reservation not in self._reservations:
                return
            if response_payload is not None:
                reservation.weight = self.actual_weight(reservation.request_type, response_payload)
                self._consecutive_429 = 0
            self._condition.notify_all()

    def register_429(self, retry_after: str | None = None) -> float:
        """Apply a cooldown shared by every worker and return its remaining delay."""
        if self._coordinator is not None:
            return self._coordinator.register_429(
                retry_after, backoff_initial_seconds=self.backoff_initial_seconds,
                backoff_max_seconds=self.backoff_max_seconds, jitter_seconds=self.jitter_seconds, jitter=self._jitter,
            )
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
        if self._coordinator is not None:
            self._coordinator.record_retry()
            return
        with self._condition:
            self._retry_count += 1

    def telemetry(self) -> dict[str, object]:
        if self._coordinator is not None:
            telemetry = self._coordinator.telemetry()
            self.operating_budget = int(telemetry["operating_budget"])
            return telemetry
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
    coordination_path: Path | None = None,
) -> HyperliquidInfoRateLimiter:
    """Return the one process-wide limiter for an API URL.

    If differently configured services share the same process, use the lower
    budget so coordination remains safe rather than creating an independent
    allowance for either caller.
    """
    key = info_url.rstrip("/").lower() + "|" + (str(coordination_path.resolve()) if coordination_path else "process")
    with _shared_limiters_lock:
        limiter = _shared_limiters.get(key)
        if limiter is None:
            limiter = HyperliquidInfoRateLimiter(
                operating_budget=operating_budget, backoff_initial_seconds=backoff_initial_seconds,
                backoff_max_seconds=backoff_max_seconds, jitter_seconds=jitter_seconds,
                coordination_path=coordination_path,
            )
            _shared_limiters[key] = limiter
        elif operating_budget < limiter.operating_budget:
            limiter.set_operating_budget(operating_budget)
        return limiter
