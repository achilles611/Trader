from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any, Awaitable, Callable, Iterable

import requests

from .config import SourceConfig
from .models import ConnectionState, RawFill, TraderSnapshot, as_utc, stable_id, utc_now


class HyperliquidAPIError(RuntimeError):
    pass


@dataclass(frozen=True)
class BackfillCoverage:
    requested_start: object
    requested_end: object
    earliest_observed_fill: object | None
    latest_observed_fill: object | None
    source_limit_detected: bool
    coverage_complete: bool
    coverage_quality: str
    coverage_state: str = "UNPROVEN"


class HyperliquidPublicAdapter:
    """Unauthenticated Hyperliquid public information and websocket adapter."""

    def __init__(self, config: SourceConfig, session: requests.Session | None = None) -> None:
        self.config = config
        self.session = session or requests.Session()
        self.last_backfill_coverage: BackfillCoverage | None = None

    def info(self, payload: dict[str, Any]) -> Any:
        response = self.session.post(
            self.config.info_url, json=payload, headers={"Content-Type": "application/json"},
            timeout=self.config.request_timeout_seconds,
        )
        try:
            response.raise_for_status()
        except requests.RequestException as exc:
            raise HyperliquidAPIError(f"Hyperliquid info request failed: {exc}") from exc
        try:
            return response.json()
        except ValueError as exc:
            raise HyperliquidAPIError("Hyperliquid info endpoint returned non-JSON content.") from exc

    def fetch_user_fills(self, wallet: str, *, aggregate_by_time: bool = False) -> list[RawFill]:
        payload = {"type": "userFills", "user": wallet.lower(), "aggregateByTime": aggregate_by_time}
        response = self.info(payload)
        if not isinstance(response, list):
            raise HyperliquidAPIError("Unexpected userFills response.")
        return self._parse_fills(response, wallet)

    def fetch_fills_by_time(
        self, wallet: str, start: object, end: object | None = None, *, aggregate_by_time: bool = False
    ) -> list[RawFill]:
        payload: dict[str, Any] = {
            "type": "userFillsByTime", "user": wallet.lower(), "startTime": int(as_utc(start).timestamp() * 1000),
            "aggregateByTime": aggregate_by_time,
        }
        if end is not None:
            payload["endTime"] = int(as_utc(end).timestamp() * 1000)
        response = self.info(payload)
        if not isinstance(response, list):
            raise HyperliquidAPIError("Unexpected userFillsByTime response.")
        return self._parse_fills(response, wallet)

    def backfill_fills(self, wallet: str, start: object, end: object | None = None) -> list[RawFill]:
        """Fetch a bounded historical range, splitting dense intervals around API limits.

        Hyperliquid returns at most 2,000 fills per response.  A range that still
        contains 2,000 fills at a one-minute interval is surfaced as an error so
        a caller cannot silently claim a complete backfill.
        """
        start_at = as_utc(start)
        end_at = as_utc(end or utc_now())
        if end_at < start_at:
            raise ValueError("backfill end must not precede start")
        collected: dict[str, RawFill] = {}
        source_limit_detected = False

        def fetch_range(range_start: object, range_end: object) -> None:
            nonlocal source_limit_detected
            fills = self.fetch_fills_by_time(wallet, range_start, range_end)
            if len(fills) >= 2000:
                source_limit_detected = True
                left = as_utc(range_start)
                right = as_utc(range_end)
                if (right - left) <= timedelta(minutes=1):
                    self.last_backfill_coverage = BackfillCoverage(
                        requested_start=start_at, requested_end=end_at, earliest_observed_fill=None,
                        latest_observed_fill=None, source_limit_detected=True, coverage_complete=False,
                        coverage_quality="incomplete_dense_interval_public_cap", coverage_state="KNOWN_INCOMPLETE",
                    )
                    raise HyperliquidAPIError(
                        "A one-minute interval reached the public API's 2,000-fill cap; complete backfill is unavailable."
                    )
                middle = left + (right - left) / 2
                fetch_range(left, middle)
                # Advance one millisecond so a boundary fill is not requested twice;
                # deterministic IDs still protect against endpoint inclusivity.
                fetch_range(middle + timedelta(milliseconds=1), right)
                return
            for fill in fills:
                collected[fill.event_id] = fill

        fetch_range(start_at, end_at)
        result = sorted(collected.values(), key=lambda fill: (fill.event_timestamp, fill.event_id))
        # The public endpoint retains only the latest 10,000 user fills.  It has
        # no archive watermark, so a deep history request can never be claimed
        # complete merely because recursive 2,000-result pages were exhausted.
        self.last_backfill_coverage = BackfillCoverage(
            requested_start=start_at, requested_end=end_at,
            earliest_observed_fill=result[0].event_timestamp if result else None,
            latest_observed_fill=result[-1].event_timestamp if result else None,
            source_limit_detected=source_limit_detected,
            coverage_complete=False,
            coverage_quality="unproven_public_10000_fill_retention",
            coverage_state="UNPROVEN",
        )
        return result

    def fetch_portfolio(self, wallet: str) -> Any:
        return self.info({"type": "portfolio", "user": wallet.lower()})

    def fetch_clearinghouse_state(self, wallet: str) -> TraderSnapshot:
        payload = self.info({"type": "clearinghouseState", "user": wallet.lower()})
        if not isinstance(payload, dict):
            raise HyperliquidAPIError("Unexpected clearinghouseState response.")
        margin = payload.get("marginSummary") or {}
        positions = payload.get("assetPositions") or []
        timestamp = utc_now()
        return TraderSnapshot(
            snapshot_id=stable_id("snapshot", wallet.lower(), timestamp, payload), target_wallet=wallet.lower(),
            snapshot_timestamp=timestamp, account_value=_optional_float(margin.get("accountValue")),
            withdrawable=_optional_float(payload.get("withdrawable")), total_notional_position=_optional_float(margin.get("totalNtlPos")),
            positions={"asset_positions": positions}, source="hyperliquid", raw_payload=payload,
        )

    def fetch_spot_state(self, wallet: str) -> Any:
        return self.info({"type": "spotClearinghouseState", "user": wallet.lower()})

    def fetch_candle_snapshot(self, coin: str, start: object, end: object, interval: str = "1m") -> Any:
        return self.info({
            "type": "candleSnapshot",
            "req": {"coin": coin, "interval": interval, "startTime": int(as_utc(start).timestamp() * 1000),
                    "endTime": int(as_utc(end).timestamp() * 1000)},
        })

    def _parse_fills(self, payloads: Iterable[dict[str, Any]], wallet: str) -> list[RawFill]:
        return [RawFill.from_hyperliquid(payload, wallet, network=self.config.network) for payload in payloads if isinstance(payload, dict)]


def _optional_float(value: Any) -> float | None:
    return float(value) if value not in (None, "") else None


FillHandler = Callable[[str, list[RawFill], bool], Awaitable[None] | None]
StateHandler = Callable[[str, dict[str, Any]], Awaitable[None] | None]
MarketHandler = Callable[[dict[str, Any]], Awaitable[None] | None]


@dataclass
class WatchHealth:
    state: ConnectionState = ConnectionState.STOPPED
    last_message_at: object | None = None
    reconnects: int = 0
    error: str = ""
    per_target: dict[str, ConnectionState] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "state": self.state.value, "last_message_at": self.last_message_at.isoformat() if self.last_message_at else None,
            "reconnects": self.reconnects, "error": self.error,
            "per_target": {wallet: state.value for wallet, state in self.per_target.items()},
        }


class HyperliquidWatcher:
    """Reconnectable public websocket watcher.  Persistence is delegated to its handlers."""

    def __init__(self, adapter: HyperliquidPublicAdapter) -> None:
        self.adapter = adapter
        self.health = WatchHealth()
        self._stopped = asyncio.Event()

    def stop(self) -> None:
        self._stopped.set()

    async def reconcile(self, wallets: Iterable[str], on_fills: FillHandler) -> None:
        """Use a small overlap so restart/disconnect reconciliation is idempotent."""
        for wallet in wallets:
            try:
                fills = await asyncio.to_thread(self.adapter.fetch_user_fills, wallet)
                result = on_fills(wallet.lower(), fills, True)
                if asyncio.iscoroutine(result):
                    await result
            except Exception as exc:  # A watcher should continue monitoring other targets.
                self.health.per_target[wallet.lower()] = ConnectionState.ERROR
                self.health.error = str(exc)

    async def run(
        self, wallets: Iterable[str], on_fills: FillHandler, on_state: StateHandler | None = None,
        on_market: MarketHandler | None = None,
        *, duration_seconds: float | None = None,
    ) -> None:
        target_wallets = [wallet.lower() for wallet in wallets]
        if not target_wallets:
            raise ValueError("No approved copy-trade targets to watch.")
        if len(set(target_wallets)) > 10:
            raise ValueError("Hyperliquid allows at most 10 unique users across user-specific websocket subscriptions.")
        await self.reconcile(target_wallets, on_fills)
        deadline = asyncio.get_running_loop().time() + duration_seconds if duration_seconds else None
        backoff = self.adapter.config.reconnect_initial_seconds
        self.health.state = ConnectionState.RECONNECTING
        self.health.per_target = {wallet: ConnectionState.RECONNECTING for wallet in target_wallets}
        try:
            import websockets
        except ImportError as exc:
            raise RuntimeError("websockets is required for copy-watch; install requirements.txt.") from exc

        while not self._stopped.is_set() and (deadline is None or asyncio.get_running_loop().time() < deadline):
            try:
                async with websockets.connect(self.adapter.config.websocket_url, ping_interval=20, ping_timeout=20) as socket:
                    if on_market and self.adapter.config.subscribe_market_data:
                        # allMids is a single public stream.  The service filters
                        # it to active/open symbols and labels it a midpoint.
                        await socket.send(json.dumps({"method": "subscribe", "subscription": {"type": "allMids"}}))
                    for wallet in target_wallets:
                        await socket.send(json.dumps({"method": "subscribe", "subscription": {"type": "userFills", "user": wallet}}))
                        # Shared userFills carries its `user` field.  Do not
                        # subscribe to userEvents: unlike fills it is not a
                        # safely attributable shared multiwallet contract.
                        if self.adapter.config.subscribe_position_state:
                            await socket.send(json.dumps({"method": "subscribe", "subscription": {"type": "allDexsClearinghouseState", "user": wallet}}))
                    self.health.state = ConnectionState.CONNECTED
                    self.health.per_target = {wallet: ConnectionState.CONNECTED for wallet in target_wallets}
                    self.health.error = ""
                    backoff = self.adapter.config.reconnect_initial_seconds
                    while not self._stopped.is_set() and (deadline is None or asyncio.get_running_loop().time() < deadline):
                        try:
                            message = await asyncio.wait_for(socket.recv(), timeout=self.adapter.config.stale_after_seconds)
                        except TimeoutError:
                            self.health.state = ConnectionState.STALE
                            self.health.per_target = {wallet: ConnectionState.STALE for wallet in target_wallets}
                            await socket.ping()
                            continue
                        self.health.last_message_at = utc_now()
                        self.health.state = ConnectionState.CONNECTED
                        self.health.per_target = {wallet: ConnectionState.CONNECTED for wallet in target_wallets}
                        await self._handle_message(json.loads(message), on_fills, on_state, on_market)
            except Exception as exc:
                self.health.state = ConnectionState.RECONNECTING
                self.health.per_target = {wallet: ConnectionState.RECONNECTING for wallet in target_wallets}
                self.health.reconnects += 1
                self.health.error = str(exc)
                await asyncio.sleep(backoff)
                backoff = min(self.adapter.config.reconnect_max_seconds, backoff * 2)
                await self.reconcile(target_wallets, on_fills)
        self.health.state = ConnectionState.STOPPED

    async def _handle_message(
        self, message: dict[str, Any], on_fills: FillHandler, on_state: StateHandler | None,
        on_market: MarketHandler | None = None,
    ) -> None:
        if not isinstance(message, dict):
            return
        channel = message.get("channel")
        data = message.get("data")
        if channel == "userFills":
            if not isinstance(data, dict):
                return
            wallet = str(data.get("user") or "").lower()
            fills = self.adapter._parse_fills(data.get("fills") or [], wallet)
            result = on_fills(wallet, fills, bool(data.get("isSnapshot")))
            if asyncio.iscoroutine(result):
                await result
            return
        if on_state and channel == "allDexsClearinghouseState" and isinstance(data, dict):
            wallet = str(data.get("user") or "").lower()
            if not wallet:
                return
            result = on_state(wallet, data)
            if asyncio.iscoroutine(result):
                await result
            return
        if on_market and channel == "allMids" and isinstance(data, dict):
            result = on_market(data)
            if asyncio.iscoroutine(result):
                await result
