from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from typing import Iterable

from .analytics import calculate_trader_metrics
from .config import CopyTradeConfig
from .discovery import CandidateDiscoveryAdapter
from .hyperliquid import HyperliquidPublicAdapter
from .models import PositionEventType, RawFill, Target, TargetStatus, TraderSnapshot, utc_now
from .paper import PaperExecutionEngine, SignalFactory, TargetSizeClassifier
from .reconstruction import PositionReconstructor
from .storage import CopyTradeDatabase


class CopyTradeService:
    """Application service joining ingestion, traceable reconstruction, and paper copying."""

    def __init__(self, config: CopyTradeConfig, database: CopyTradeDatabase | None = None) -> None:
        self.config = config
        self.database = database or CopyTradeDatabase(config.artifacts.database_path)
        self.database.initialize()
        self.adapter = HyperliquidPublicAdapter(config.source)
        for target in config.targets:
            wallet = str(target.get("wallet", "")).strip()
            if wallet:
                self.database.upsert_target(Target(wallet=wallet, label=str(target.get("label", "")), status=str(target.get("status", "pending"))))

    def import_wallets(self, wallets: Iterable[str], *, label_prefix: str = "") -> list[Target]:
        imported: list[Target] = []
        for wallet in wallets:
            normalized = wallet.strip().lower()
            if not normalized:
                continue
            if not _is_wallet(normalized):
                raise ValueError(f"Invalid Hyperliquid wallet address: {wallet}")
            target = Target(wallet=normalized, label=f"{label_prefix}{normalized}" if label_prefix else "")
            self.database.upsert_target(target)
            imported.append(target)
        return imported

    def import_wallet_file(self, path: str | Path) -> list[Target]:
        source = Path(path)
        if not source.exists():
            raise FileNotFoundError(source)
        text = source.read_text(encoding="utf-8")
        # CSV header/extra columns and whitespace-delimited research lists are both accepted.
        tokens: list[str] = []
        for line in text.splitlines():
            first = line.split(",", 1)[0].strip()
            if first.lower() not in {"wallet", "address", ""}:
                tokens.append(first)
        return self.import_wallets(tokens)

    def import_discovered(self, adapter: CandidateDiscoveryAdapter) -> list[Target]:
        """Register future discovery-adapter results through the same audit path."""
        imported: list[Target] = []
        for target in adapter.discover():
            if not _is_wallet(target.wallet.lower()):
                raise ValueError(f"Discovery adapter emitted an invalid wallet: {target.wallet}")
            self.database.upsert_target(target)
            imported.append(target)
        return imported

    def set_status(self, wallet: str, status: str) -> None:
        if status not in {item.value for item in TargetStatus}:
            raise ValueError(f"Unsupported target status: {status}")
        if not self.database.set_target_status(wallet, status):
            raise KeyError(f"Target not found: {wallet}")

    def backfill(self, wallet: str, *, start: object | None = None, end: object | None = None) -> dict[str, object]:
        target = self.database.get_target(wallet)
        if not target:
            raise KeyError(f"Target must be imported before backfill: {wallet}")
        start_at = start or self.database.latest_fill_time(wallet) or (utc_now() - timedelta(days=90))
        fills = self.adapter.backfill_fills(wallet, start_at, end)
        coverage = self.adapter.last_backfill_coverage
        if coverage:
            self.database.insert_backfill_coverage(wallet, coverage)
        inserted = self.database.insert_raw_fills(fills)
        snapshot = self.adapter.fetch_clearinghouse_state(wallet)
        self.database.insert_snapshot(snapshot)
        portfolio = self.adapter.fetch_portfolio(wallet)
        self._store_portfolio_snapshot(wallet, portfolio)
        reconstruction = self.reconstruct(wallet)
        return {
            "wallet": wallet.lower(), "fetched_fills": len(fills), "new_raw_fills": inserted,
            "position_events": len(reconstruction["events"]), "campaigns": len(reconstruction["campaigns"]),
            "snapshot_id": snapshot.snapshot_id,
            "coverage": {
                "coverage_complete": coverage.coverage_complete,
                "coverage_quality": coverage.coverage_quality,
                "source_limit_detected": coverage.source_limit_detected,
            } if coverage else None,
        }

    def reconstruct(self, wallet: str) -> dict[str, object]:
        fills = self.database.list_raw_fills(wallet)
        result = PositionReconstructor().reconstruct(fills)
        enriched_events = tuple(self._enrich_equity(event) for event in result.events)
        for event in enriched_events:
            self.database.upsert_position_event(event)
        for campaign in result.campaigns:
            self.database.upsert_campaign(campaign)
        metrics = calculate_trader_metrics(wallet, result.campaigns, enriched_events)
        coverage = self.database.latest_backfill_coverage(wallet)
        if coverage:
            metrics.raw["coverage_complete"] = bool(coverage["coverage_complete"])
            metrics.raw["coverage_quality"] = coverage["coverage_quality"]
        self.database.upsert_metrics(metrics)
        return {"events": enriched_events, "campaigns": result.campaigns, "metrics": metrics, "reconciliation": result.reconciliation}

    async def ingest_watched_fills(self, wallet: str, fills: list[RawFill], is_snapshot: bool) -> None:
        # This happens before reconstruction/signal generation, preserving source
        # evidence even if a later process crashes.
        for fill in fills:
            self.database.insert_raw_fill(fill)
        reconstructed = self.reconstruct(wallet)
        events = reconstructed["events"]
        assert isinstance(events, tuple)
        engine = PaperExecutionEngine(self.config, self.database)
        engine.restore(self.database.list_virtual_positions(), self.database.latest_portfolio_snapshot(), self.database.list_realized_results())
        classifier = TargetSizeClassifier(self.config.sizing)
        factory = SignalFactory(classifier, self.config)
        for event in events:
            signals = factory.from_position_event(event, engine.portfolio.cash or 0.0)
            for signal in signals:
                # A persisted attempt is the idempotency boundary.  If a crash
                # happened after raw/signal persistence but before an attempt,
                # the same deterministic signal is recovered on the next pass.
                if not self.database.has_signal(signal.signal_id):
                    self.database.insert_signal(signal)
                received_at = utc_now()
                engine.process_signal(signal, received_at=received_at, market_price=event.price)

    async def ingest_watched_state(self, wallet: str, payload: dict[str, object]) -> None:
        state = payload.get("clearinghouseState") if isinstance(payload.get("clearinghouseState"), dict) else payload
        margin = state.get("marginSummary") if isinstance(state, dict) and isinstance(state.get("marginSummary"), dict) else {}
        snapshot = TraderSnapshot(
            snapshot_id=f"wsstate_{wallet.lower()}_{int(utc_now().timestamp() * 1000)}",
            target_wallet=wallet.lower(), snapshot_timestamp=utc_now(), account_value=_float_or_none(margin.get("accountValue")), withdrawable=None,
            total_notional_position=None, positions={"websocket": payload}, source="hyperliquid", raw_payload=payload,
        )
        self.database.insert_snapshot(snapshot)

    async def reconcile_wallet(self, wallet: str) -> int:
        """Fetch the gap from durable local time before websocket subscription.

        An overlap protects endpoint-boundary ambiguity; raw event IDs make it
        harmless.  This is stronger than relying on a websocket snapshot alone
        after a restart.
        """
        latest = self.database.latest_fill_time(wallet)
        if latest:
            fills = await asyncio.to_thread(self.adapter.backfill_fills, wallet, latest - timedelta(milliseconds=1), utc_now())
        else:
            fills = await asyncio.to_thread(self.adapter.fetch_user_fills, wallet)
        await self.ingest_watched_fills(wallet, fills, True)
        return len(fills)

    async def reconcile_approved_wallets(self) -> dict[str, int]:
        result: dict[str, int] = {}
        for wallet in self.approved_wallets():
            result[wallet] = await self.reconcile_wallet(wallet)
        return result

    def approved_wallets(self) -> list[str]:
        return [target.wallet for target in self.database.list_targets(TargetStatus.APPROVED.value)]

    def _store_portfolio_snapshot(self, wallet: str, portfolio: object) -> None:
        if not isinstance(portfolio, list):
            return
        for period, data in portfolio:
            if not isinstance(data, dict):
                continue
            history = data.get("accountValueHistory") or []
            for timestamp, account_value in history:
                snapshot = TraderSnapshot(
                    snapshot_id=f"portfolio_{wallet.lower()}_{period}_{timestamp}", target_wallet=wallet.lower(),
                    snapshot_timestamp=timestamp, account_value=float(account_value), withdrawable=None,
                    total_notional_position=None, positions={"period": period}, source="hyperliquid", raw_payload=data,
                )
                self.database.insert_snapshot(snapshot)

    def _enrich_equity(self, event: object):
        """Join entry semantics to only a prior account-value observation."""
        from .models import PositionEvent
        if not isinstance(event, PositionEvent):
            return event
        if event.target_equity is not None and event.target_equity > 0:
            return replace(event, equity_source="exact", equity_age_seconds=0.0)
        observation = self.database.latest_prior_equity_observation(event.target_wallet, event.event_timestamp)
        if not observation:
            return replace(event, target_equity=None, equity_source="missing", equity_age_seconds=None)
        age = max(0.0, (event.event_timestamp - observation["timestamp"]).total_seconds())
        if age > self.config.sizing.max_equity_age_seconds:
            return replace(event, target_equity=None, equity_source="missing", equity_age_seconds=age)
        positions = observation["positions"]
        quality = "sampled_prior_proxy" if isinstance(positions, dict) and "period" in positions else "recent_live_snapshot"
        return replace(event, target_equity=observation["account_value"], equity_source=quality, equity_age_seconds=age)


def _is_wallet(value: str) -> bool:
    if not value.startswith("0x") or len(value) != 42:
        return False
    return all(character in "0123456789abcdef" for character in value[2:].lower())


def _float_or_none(value: object) -> float | None:
    return float(value) if value not in (None, "") else None
