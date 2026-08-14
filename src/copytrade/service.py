from __future__ import annotations

import asyncio
from threading import Lock, RLock
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from typing import Any, Iterable

from .analytics import calculate_trader_metrics
from .config import CopyTradeConfig
from .control_center import ControlCenterStore
from .discovery import CandidateDiscoveryAdapter, DiscoveryPipeline
from .hyperliquid import HyperliquidPublicAdapter
from .equity import is_equity_observation_usable
from .models import CopySignal, PositionCampaign, PositionEvent, PositionEventType, RawFill, Target, TargetStatus, TraderSnapshot, as_utc, stable_id, utc_now
from .market import LiveMarketCache
from .paper import PaperExecutionEngine, SignalFactory, TargetSizeClassifier
from .rate_limit import shared_hyperliquid_info_limiter
from .reconstruction import FillAggregate, PositionReconstructor, ReconstructionResult, aggregate_partial_fills
from .storage import CopyTradeDatabase, RECONSTRUCTION_SCHEMA_VERSION, ReconstructionCursor


class _PaperExecutionAuthority:
    """One in-process mutable PAPER portfolio per durable database."""

    def __init__(self) -> None:
        self.lock = RLock()
        self.reconstruction_lock = RLock()
        self.engine: PaperExecutionEngine | None = None
        self.last_mark_persist_at: dict[str, object] = {}
        self.classifiers: dict[str, tuple[object, TargetSizeClassifier]] = {}
        self.classifier_signal_ids: dict[str, set[str]] = {}
        self.incremental_work: dict[str, dict[str, int | str]] = {}


_paper_execution_authorities: dict[str, _PaperExecutionAuthority] = {}
_paper_execution_authorities_lock = Lock()


def _paper_execution_authority(database_path: Path) -> _PaperExecutionAuthority:
    key = str(database_path.resolve())
    with _paper_execution_authorities_lock:
        return _paper_execution_authorities.setdefault(key, _PaperExecutionAuthority())


class CopyTradeService:
    """Application service joining ingestion, traceable reconstruction, and paper copying."""

    def __init__(self, config: CopyTradeConfig, database: CopyTradeDatabase | None = None) -> None:
        self.config = config
        self.database = database or CopyTradeDatabase(config.artifacts.database_path)
        self.database.initialize()
        # Phase C owns the durable operator/control state.  Keeping its gate
        # beside signal processing means a watcher cannot bypass the UI's
        # paper-entry controls, while raw evidence and exit handling remain
        # available regardless of entry eligibility.
        self.control_store = ControlCenterStore(config.artifacts.database_path)
        self.control_store.initialize()
        self.api_limiter = shared_hyperliquid_info_limiter(
            config.source.info_url, operating_budget=config.analysis.api_weight_budget_per_minute,
            backoff_initial_seconds=config.analysis.rate_limit_backoff_initial_seconds,
            backoff_max_seconds=config.analysis.rate_limit_backoff_max_seconds,
            jitter_seconds=config.analysis.rate_limit_jitter_seconds,
            coordination_path=config.artifacts.database_path,
        )
        self.adapter = HyperliquidPublicAdapter(config.source, limiter=self.api_limiter)
        self.market_cache = LiveMarketCache()
        # All services against one SQLite portfolio share this mutable engine
        # and lock.  The application passes one service to the Control Center;
        # this registry additionally makes an in-process fallback unable to
        # create a competing stale authority.
        self._execution_authority = _paper_execution_authority(self.database.path)
        self._execution_lock = self._execution_authority.lock
        for target in config.targets:
            wallet = str(target.get("wallet", "")).strip()
            if wallet:
                status = str(target.get("status", "pending"))
                if status == TargetStatus.ACTIVE.value:
                    raise ValueError(
                        "Configured targets cannot enter Active directly. Use the canonical Phase C activation path "
                        "so current Phase-B finalist authority can be validated."
                    )
                self.database.upsert_target(Target(wallet=wallet, label=str(target.get("label", "")), status=status))

    @property
    def _live_engine(self) -> PaperExecutionEngine | None:
        return self._execution_authority.engine

    @_live_engine.setter
    def _live_engine(self, engine: PaperExecutionEngine | None) -> None:
        self._execution_authority.engine = engine

    @property
    def _last_mark_persist_at(self) -> dict[str, object]:
        return self._execution_authority.last_mark_persist_at

    def reload_execution_state(self) -> PaperExecutionEngine:
        """Replace the live PAPER engine with the current durable state.

        Callers must hold no external database transaction.  The service lock
        makes the replacement atomic with respect to watcher mutations.
        """
        with self._execution_lock:
            engine = PaperExecutionEngine(self.config, self.database)
            engine.restore(
                self.database.list_virtual_positions(), self.database.latest_portfolio_snapshot(),
                self.database.list_realized_results(),
            )
            self._live_engine = engine
            self._last_mark_persist_at.clear()
            return engine

    def _execution_engine(self) -> PaperExecutionEngine:
        return self._live_engine or self.reload_execution_state()

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

    def discover_candidates(
        self, provider: CandidateDiscoveryAdapter, *, limit: int, min_activity: int, refresh: bool = False,
        max_activity_age: timedelta | None = timedelta(days=30), configuration: dict[str, object] | None = None,
    ):
        """Register cheap, public discovery evidence only; no scoring or paper execution occurs here."""
        return DiscoveryPipeline(self.database).run(
            provider, limit=limit, min_activity=min_activity, refresh=refresh,
            max_activity_age=max_activity_age, configuration=configuration,
        )

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

    def set_status(self, wallet: str, status: str) -> None:
        """Apply a generic non-Active target status transition.

        Active is a privileged paper-execution state.  Only Phase C's
        canonical activation path may enter it after validating current Phase-B
        finalist authority; callers must never be able to bypass that policy
        through this general-purpose helper.
        """
        if status not in {item.value for item in TargetStatus}:
            raise ValueError(f"Unsupported target status: {status}")
        if status == TargetStatus.ACTIVE.value:
            raise ValueError(
                "Direct Active transition is prohibited. Use the canonical Phase C activation path so "
                "current Phase-B finalist authority can be validated."
            )
        if not self.database.set_target_status(wallet, status):
            raise KeyError(f"Target not found: {wallet}")

    def backfill(self, wallet: str, *, start: object | None = None, end: object | None = None) -> dict[str, object]:
        return self._backfill_with_adapter(wallet, self.adapter, start=start, end=end)

    def backfill_for_analysis(self, wallet: str, *, start: object, end: object | None = None) -> dict[str, object]:
        """Phase B worker-safe public backfill using a dedicated adapter/session.

        The public adapter stores coverage from its most recent request, so a
        separate instance prevents concurrent analysis workers from crossing
        that bookkeeping.  This remains source ingestion only.
        """
        return self._backfill_with_adapter(
            wallet, HyperliquidPublicAdapter(self.config.source, limiter=self.api_limiter), start=start, end=end,
            reconstruct_after_ingestion=False,
        )

    def _backfill_with_adapter(
        self, wallet: str, adapter: HyperliquidPublicAdapter, *, start: object | None, end: object | None,
        reconstruct_after_ingestion: bool = True,
    ) -> dict[str, object]:
        target = self.database.get_target(wallet)
        if not target:
            raise KeyError(f"Target must be imported before backfill: {wallet}")
        start_at = start or self.database.latest_fill_time(wallet) or (utc_now() - timedelta(days=90))
        try:
            fills = adapter.backfill_fills(wallet, start_at, end)
        except Exception:
            # Dense public-history intervals explicitly mark coverage known
            # incomplete before raising. Persist that evidence for Phase B's
            # hard quarantine instead of collapsing it into a generic retry.
            coverage = adapter.last_backfill_coverage
            if coverage:
                self.database.insert_backfill_coverage(wallet, coverage)
            raise
        coverage = adapter.last_backfill_coverage
        if coverage:
            self.database.insert_backfill_coverage(wallet, coverage)
        inserted = self.database.insert_raw_fills(fills)
        snapshot = adapter.fetch_clearinghouse_state(wallet)
        self.database.insert_snapshot(snapshot)
        portfolio = adapter.fetch_portfolio(wallet)
        self._store_portfolio_snapshot(wallet, portfolio)
        payload: dict[str, object] = {
            "wallet": wallet.lower(), "fetched_fills": len(fills), "new_raw_fills": inserted,
            "snapshot_id": snapshot.snapshot_id,
            "coverage": {
                "coverage_complete": coverage.coverage_complete,
                "coverage_quality": coverage.coverage_quality,
                "coverage_state": coverage.coverage_state,
                "source_limit_detected": coverage.source_limit_detected,
            } if coverage else None,
        }
        if reconstruct_after_ingestion:
            reconstruction = self.reconstruct(wallet)
            payload.update({"position_events": len(reconstruction["events"]), "campaigns": len(reconstruction["campaigns"])})
        else:
            payload["reconstruction_deferred"] = True
        return payload

    def reconstruct(self, wallet: str) -> dict[str, object]:
        with self._execution_authority.reconstruction_lock:
            return self._reconstruct_full(wallet)

    def _reconstruct_full(self, wallet: str) -> dict[str, object]:
        """Explicit full-history rebuild for Phase B, repair, and validation.

        It is intentionally separate from the Phase C watcher hot path.  The
        cursor is reset to the complete durable evidence only after the rebuilt
        events and campaigns have committed in the same transaction.
        """
        fills = self.database.list_raw_fills(wallet)
        result = PositionReconstructor().reconstruct(fills)
        enriched_events = tuple(self._enrich_equity(event) for event in result.events)
        current = self.database.reconstruction_cursor(wallet)
        last = fills[-1] if fills else None
        cursor = ReconstructionCursor(
            target_wallet=wallet.lower(), schema_version=RECONSTRUCTION_SCHEMA_VERSION, revision=current.revision,
            last_seen_timestamp=last.event_timestamp if last else None, last_seen_event_id=last.event_id if last else None,
            last_processed_timestamp=last.event_timestamp if last else None, last_processed_event_id=last.event_id if last else None,
            recovery_state=current.recovery_state, recovery_anchor_event_id=current.recovery_anchor_event_id,
            recovery_anchor_timestamp=current.recovery_anchor_timestamp, recovery_detail=current.recovery_detail,
            updated_at=utc_now(),
        )
        self.database.persist_reconstruction_batch(wallet, enriched_events, result.campaigns, cursor, replace_wallet_history=True)
        self._execution_authority.classifiers.pop(wallet.lower(), None)
        self._execution_authority.classifier_signal_ids.pop(wallet.lower(), None)
        metrics = calculate_trader_metrics(wallet, result.campaigns, enriched_events, self.config.sizing)
        coverage = self.database.latest_backfill_coverage(wallet)
        if coverage:
            metrics.raw["coverage_complete"] = bool(coverage["coverage_complete"])
            metrics.raw["coverage_quality"] = coverage["coverage_quality"]
            metrics.raw["coverage_state"] = coverage.get("coverage_state", "UNPROVEN")
        self.database.upsert_metrics(metrics)
        return {"events": enriched_events, "campaigns": result.campaigns, "metrics": metrics, "reconciliation": result.reconciliation}

    async def ingest_watched_fills(self, wallet: str, fills: list[RawFill], is_snapshot: bool) -> None:
        # SQLite reconstruction can be substantial during startup/recovery.
        # Keep it out of both the asyncio event loop and the mutable PAPER
        # authority lock; execution itself remains strictly serialized below.
        await asyncio.to_thread(self._stage_and_incrementally_reconstruct, wallet, fills, is_snapshot)
        await asyncio.to_thread(self._drain_pending_reconstruction_events, wallet)

    def _ingest_watched_fills(self, wallet: str, fills: list[RawFill], is_snapshot: bool) -> None:
        self._stage_and_incrementally_reconstruct(wallet, fills, is_snapshot)
        self._drain_pending_reconstruction_events(wallet)

    @staticmethod
    def _fill_key(fill: RawFill) -> tuple[object, str]:
        return fill.event_timestamp, fill.event_id

    @staticmethod
    def _same_partial_order(first: RawFill, second: RawFill) -> bool:
        return (
            first.target_order_id is not None and first.target_order_id == second.target_order_id
            and first.target_wallet.lower() == second.target_wallet.lower() and first.symbol == second.symbol
            and (first.signed_quantity >= 0) == (second.signed_quantity >= 0)
        )

    def _full_rebuild_cursor(
        self, wallet: str, cursor: ReconstructionCursor, *, pending_event_ids: tuple[str, ...] = (),
        recovery_state: str | None = None, recovery_detail: dict[str, object] | None = None,
        fills: list[RawFill] | None = None, reconstruction: ReconstructionResult | None = None,
    ) -> tuple[tuple[PositionEvent, ...], dict[str, int | str]]:
        fills = fills if fills is not None else self.database.list_raw_fills(wallet)
        result = reconstruction if reconstruction is not None else PositionReconstructor().reconstruct(fills)
        events = tuple(self._enrich_equity(event) for event in result.events)
        last = fills[-1] if fills else None
        replacement = ReconstructionCursor(
            target_wallet=wallet.lower(), schema_version=RECONSTRUCTION_SCHEMA_VERSION, revision=cursor.revision,
            last_seen_timestamp=last.event_timestamp if last else None, last_seen_event_id=last.event_id if last else None,
            last_processed_timestamp=last.event_timestamp if last else None, last_processed_event_id=last.event_id if last else None,
            pending_event_ids=pending_event_ids,
            recovery_state=recovery_state or cursor.recovery_state,
            recovery_anchor_event_id=cursor.recovery_anchor_event_id,
            recovery_anchor_timestamp=cursor.recovery_anchor_timestamp,
            recovery_detail=recovery_detail if recovery_detail is not None else cursor.recovery_detail,
            updated_at=utc_now(),
        )
        self.database.persist_reconstruction_batch(wallet, events, result.campaigns, replacement, replace_wallet_history=True)
        self._execution_authority.classifiers.pop(wallet.lower(), None)
        self._execution_authority.classifier_signal_ids.pop(wallet.lower(), None)
        return events, {
            "mode": "full_rebuild", "fills_loaded": len(fills), "new_raw_fills": 0,
            "events_produced": len(events), "campaign_rows_written": len(result.campaigns),
        }

    def _stage_and_incrementally_reconstruct(self, wallet: str, fills: list[RawFill], is_snapshot: bool) -> None:
        """Persist source evidence then advance only finalized new aggregates.

        Cursor movement is in the same transaction as event/campaign writes.
        An interrupted source insert is harmless (no evidence committed); an
        interrupted reconstruction leaves evidence but an older cursor, which
        safely replays through deterministic IDs on the next call.
        """
        normalized = wallet.lower()
        with self._execution_authority.reconstruction_lock:
            fresh = self.database.insert_raw_fills_returning_new(fills)
            cursor_exists = self.database.has_reconstruction_cursor(normalized)
            cursor = self.database.reconstruction_cursor(normalized)
            cursor_initialized = cursor_exists and (
                cursor.last_seen_event_id is not None or self.database.latest_raw_fill(normalized) is None
            )
            if not cursor_initialized:
                # Pre-hotfix databases -- or a recovery-status row written
                # before its first reconstruction -- have durable raw evidence
                # and possibly persisted historical campaigns, but no cursor
                # provenance.
                # Continuing from those campaigns would reapply the whole
                # ledger against its final source position.  One explicit safe
                # rebuild establishes the first cursor transaction boundary.
                historical = self.database.list_raw_fills(normalized)
                rebuilt = PositionReconstructor().reconstruct(historical)
                fresh_ids = {item.event_id for item in fresh}
                pending = () if is_snapshot else tuple(
                    event.event_id for event in rebuilt.events if set(event.raw_fill_ids) & fresh_ids
                )
                events, work = self._full_rebuild_cursor(
                    normalized, cursor, pending_event_ids=pending, fills=historical, reconstruction=rebuilt,
                )
                work["new_raw_fills"] = len(fresh)
                self._execution_authority.incremental_work[normalized] = work
                return
            late = [fill for fill in fresh if cursor.last_seen_timestamp is not None and cursor.last_seen_event_id is not None
                    and self._fill_key(fill) <= (cursor.last_seen_timestamp, cursor.last_seen_event_id)]
            if cursor.schema_version != RECONSTRUCTION_SCHEMA_VERSION or late:
                # New evidence preceding the durable cursor changes causal
                # historical transitions.  Rebuild accounting, fail closed for
                # entries, and only queue direct new exits for PAPER handling.
                selected = ()
                if late:
                    late_ids = {fill.event_id for fill in late}
                    all_fills = self.database.list_raw_fills(normalized)
                    rebuilt = PositionReconstructor().reconstruct(all_fills)
                    selected = tuple(
                        event.event_id for event in rebuilt.events
                        if set(event.raw_fill_ids) & late_ids and event.event_type in {PositionEventType.REDUCE, PositionEventType.CLOSE}
                    )
                events, work = self._full_rebuild_cursor(
                    normalized, cursor, pending_event_ids=selected,
                    recovery_state="RECOVERY_INCOMPLETE" if late else cursor.recovery_state,
                    recovery_detail={"reason": "out_of_order_source_evidence", "late_fill_ids": [item.event_id for item in late]}
                    if late else None, fills=all_fills if late else None, reconstruction=rebuilt if late else None,
                )
                if late:
                    self.control_store.record_activity(
                        category="recovery", severity="warning", wallet=normalized,
                        message="Out-of-order source evidence forced a fail-closed reconstruction rebuild.",
                        payload={"recovery_state": "RECOVERY_INCOMPLETE", "late_fill_ids": [item.event_id for item in late]},
                    )
                work["new_raw_fills"] = len(fresh)
                self._execution_authority.incremental_work[normalized] = work
                return

            new_fills = self.database.list_raw_fills_after(normalized, cursor.last_seen_timestamp, cursor.last_seen_event_id)
            pending = self.database.list_raw_fills_by_ids(normalized, cursor.pending_fill_ids)
            ordered = sorted({item.event_id: item for item in [*pending, *new_fills]}.values(), key=self._fill_key)
            if not ordered:
                # The subscription snapshot establishes accounting and sizing
                # history without replaying a potentially large backlog into
                # PAPER.  If the source later delivers one of those exact
                # fills as a *live* frame, it is an explicit real-time event:
                # queue only the fully represented event(s), via indexed JSON
                # attribution, rather than scanning historic position events.
                replay_ids: tuple[str, ...] = ()
                if not is_snapshot and fills:
                    delivered_ids = {item.event_id for item in fills}
                    replay_ids = tuple(
                        event.event_id for event in self.database.list_position_events_for_raw_fills(normalized, delivered_ids)
                        if set(event.raw_fill_ids).issubset(delivered_ids) and event.event_id not in cursor.pending_event_ids
                    )
                if replay_ids:
                    replacement = ReconstructionCursor(
                        target_wallet=normalized, schema_version=RECONSTRUCTION_SCHEMA_VERSION, revision=cursor.revision,
                        last_seen_timestamp=cursor.last_seen_timestamp, last_seen_event_id=cursor.last_seen_event_id,
                        last_processed_timestamp=cursor.last_processed_timestamp,
                        last_processed_event_id=cursor.last_processed_event_id,
                        pending_fill_ids=cursor.pending_fill_ids,
                        pending_event_ids=tuple([*cursor.pending_event_ids, *replay_ids]),
                        recovery_state=cursor.recovery_state, recovery_anchor_event_id=cursor.recovery_anchor_event_id,
                        recovery_anchor_timestamp=cursor.recovery_anchor_timestamp, recovery_detail=cursor.recovery_detail,
                        updated_at=utc_now(),
                    )
                    self.database.persist_reconstruction_batch(normalized, (), (), replacement)
                self._execution_authority.incremental_work[normalized] = {
                    "mode": "replay", "fills_loaded": 0, "new_raw_fills": len(fresh),
                    "events_produced": 0, "campaign_rows_written": 0, "events_requeued": len(replay_ids),
                }
                return

            groups: list[list[RawFill]] = []
            for fill in ordered:
                if groups and self._same_partial_order(groups[-1][-1], fill):
                    groups[-1].append(fill)
                else:
                    groups.append([fill])
            # Hyperliquid delivers a complete userFills frame as the smallest
            # public execution unit.  Finalize its tail immediately so a lone
            # realtime fill is not held indefinitely; partials within the
            # frame still aggregate exactly.  Durable overlap/recovery frames
            # are likewise finalized as a snapshot.
            pending_group: list[RawFill] = []
            aggregates: list[FillAggregate] = []
            for group in groups:
                aggregates.extend(aggregate_partial_fills(group))

            open_campaigns = self.database.list_open_campaigns(normalized)
            rebaseline_after = cursor.recovery_detail.get("safe_rebaseline_after")
            if rebaseline_after:
                baseline = as_utc(rebaseline_after)
                # Preserve incomplete historical campaigns for audit, but they
                # cannot become the source state for a deliberately verified
                # zero-position baseline.
                open_campaigns = [campaign for campaign in open_campaigns if campaign.opened_at > baseline]
            state = PositionReconstructor.incremental_state(open_campaigns)
            reconstructor = PositionReconstructor()
            generated: list[PositionEvent] = []
            changed_campaigns: dict[str, PositionCampaign] = {}
            last_processed: RawFill | None = None
            for aggregate in aggregates:
                events, changed = reconstructor.apply_aggregate(state, aggregate)
                generated.extend(self._enrich_equity(event) for event in events)
                changed_campaigns.update({campaign.campaign_id: campaign for campaign in changed})
                last_processed = aggregate.fills[-1]
            last_seen = ordered[-1]
            replacement = ReconstructionCursor(
                target_wallet=normalized, schema_version=RECONSTRUCTION_SCHEMA_VERSION, revision=cursor.revision,
                last_seen_timestamp=last_seen.event_timestamp, last_seen_event_id=last_seen.event_id,
                last_processed_timestamp=last_processed.event_timestamp if last_processed else cursor.last_processed_timestamp,
                last_processed_event_id=last_processed.event_id if last_processed else cursor.last_processed_event_id,
                pending_fill_ids=tuple(item.event_id for item in pending_group),
                pending_event_ids=tuple([*cursor.pending_event_ids, *(event.event_id for event in generated)]),
                recovery_state=cursor.recovery_state, recovery_anchor_event_id=cursor.recovery_anchor_event_id,
                recovery_anchor_timestamp=cursor.recovery_anchor_timestamp, recovery_detail=cursor.recovery_detail,
                updated_at=utc_now(),
            )
            self.database.persist_reconstruction_batch(normalized, generated, changed_campaigns.values(), replacement)
            self._execution_authority.incremental_work[normalized] = {
                "mode": "incremental", "fills_loaded": len(ordered), "new_raw_fills": len(fresh),
                "events_produced": len(generated), "campaign_rows_written": len(changed_campaigns),
            }

    def incremental_work(self, wallet: str) -> dict[str, int | str]:
        return dict(self._execution_authority.incremental_work.get(wallet.lower(), {}))

    def _classifier_for_wallet(self, wallet: str, *, exclude_event_ids: Iterable[str] = ()) -> TargetSizeClassifier:
        key = wallet.lower()
        cached = self._execution_authority.classifiers.get(key)
        if cached is not None and cached[0] == self.config.sizing:
            return cached[1]
        classifier = TargetSizeClassifier(self.config.sizing)
        fractions: list[float] = []
        seeded_signal_ids: set[str] = set()
        excluded = set(exclude_event_ids)
        for item in self.database.sizing_history(key):
            if item.get("event_id") in excluded:
                continue
            event_type = item.get("event_type")
            if event_type == PositionEventType.OPEN.value and not self.config.sizing.copy_initial_entries:
                continue
            if event_type == PositionEventType.ADD.value and not self.config.sizing.copy_target_adds:
                continue
            equity = item.get("target_equity")
            if equity is None or float(equity) <= 0:
                continue
            if is_equity_observation_usable(
                self.config.sizing, float(equity), str(item.get("equity_source") or "missing"), item.get("equity_age_seconds"),
            ):
                fractions.append(float(item["target_notional"]) / float(equity))
                action = "open" if event_type == PositionEventType.OPEN.value else "add"
                seeded_signal_ids.add(stable_id("signal", str(item["event_id"]), action))
        classifier.seed(key, fractions)
        self._execution_authority.classifiers[key] = (self.config.sizing, classifier)
        self._execution_authority.classifier_signal_ids[key] = seeded_signal_ids
        return classifier

    def _signals_for_event(self, event: PositionEvent, engine: PaperExecutionEngine) -> list[CopySignal]:
        action = {
            PositionEventType.OPEN: "open", PositionEventType.ADD: "add",
            PositionEventType.REDUCE: "reduce", PositionEventType.CLOSE: "close",
        }.get(event.event_type)
        if action is not None:
            identifier = (stable_id("signal", event.event_id, action) if action in {"open", "add"}
                          else stable_id("signal", event.event_id, action, event.direction))
            existing = self.database.get_signal(identifier)
            if existing:
                if action in {"open", "add"}:
                    known = self._execution_authority.classifier_signal_ids.setdefault(event.target_wallet.lower(), set())
                    if existing.signal_id not in known:
                        equity = existing.target_equity
                        if equity and is_equity_observation_usable(
                            self.config.sizing, equity, existing.equity_source, existing.equity_age_seconds,
                        ):
                            self._classifier_for_wallet(event.target_wallet).record(
                                event.target_wallet, existing.target_notional / equity,
                            )
                        known.add(existing.signal_id)
                return [existing]
        factory = SignalFactory(self._classifier_for_wallet(event.target_wallet), self.config)
        signals = factory.from_position_event(event, engine.portfolio.cash or 0.0)
        for signal in signals:
            self.database.insert_signal(signal)
            if signal.action in {"open", "add"}:
                self._execution_authority.classifier_signal_ids.setdefault(event.target_wallet.lower(), set()).add(signal.signal_id)
        return signals

    def _execute_reconstructed_signal(
        self, engine: PaperExecutionEngine, event: PositionEvent, signal: CopySignal, recovery_state: str,
    ) -> None:
        received_at = utc_now()
        observation, age_ms = self.market_cache.latest_available(
            event.symbol, received_at, self.config.paper_execution.market_data_max_age_ms,
        )
        metadata: dict[str, object] = {
            "target_fill_price": event.price, "source_fill_timestamp": event.event_timestamp.isoformat(),
            "local_receive_timestamp": received_at.isoformat(), "market_reference_age_ms": age_ms,
        }
        entry_block = (
            "source_recovery_not_continuous" if recovery_state != "CONTINUOUS" and signal.action in {"open", "add"}
            else self.control_store.entry_block_reason(signal.target_wallet, signal.action)
        )
        if entry_block:
            metadata.update({"entry_gate": entry_block, "market_reference_source": "not_used"})
            engine.process_signal(signal, received_at=received_at, market_metadata=metadata, forced_reason=entry_block)
            return
        if observation:
            metadata.update({
                "market_reference_price": observation.price,
                "market_reference_timestamp": observation.timestamp.isoformat(),
                "market_reference_source": observation.source, "market_reference_quality": observation.quality,
                "source_to_receive_latency_ms": max(0.0, (received_at - event.event_timestamp).total_seconds() * 1000),
            })
            engine.process_signal(signal, received_at=received_at, market_price=observation.price, market_metadata=metadata)
        elif signal.action in {"open", "add"}:
            metadata.update({"market_reference_source": "unavailable", "market_reference_quality": "stale_or_missing"})
            engine.process_signal(signal, received_at=received_at, market_metadata=metadata, forced_reason="stale_market_data")
        elif self.config.paper_execution.stale_exit_market_policy == "skip":
            metadata.update({"market_reference_source": "unavailable", "market_reference_quality": "stale_or_missing"})
            engine.process_signal(signal, received_at=received_at, market_metadata=metadata, forced_reason="stale_market_data_exit")
        else:
            metadata.update({"market_reference_price": event.price, "market_reference_source": "target_fill_fallback",
                             "market_reference_quality": "exit_only_not_contemporaneous"})
            engine.process_signal(signal, received_at=received_at, market_price=event.price, market_metadata=metadata)

    def _drain_pending_reconstruction_events(self, wallet: str) -> None:
        """Execute only the cursor's durable pending economic events.

        Signal rows are inserted before the PAPER attempt.  Therefore a crash
        after reconstruction or signal persistence can restart from the same
        deterministic signal, and a crash after execution is absorbed by the
        transaction-backed execution claim before the queue entry is cleared.
        """
        normalized = wallet.lower()
        with self._execution_lock:
            cursor = self.database.reconstruction_cursor(normalized)
            events = self.database.list_position_events_by_ids(normalized, cursor.pending_event_ids)
            if not events:
                return
            engine = self._execution_engine()
            # On a restart, the event transaction has already committed but
            # their signals may not have.  Seed only from prior events so the
            # first pending OPEN/ADD still sees prior-only sizing history.
            self._classifier_for_wallet(normalized, exclude_event_ids=cursor.pending_event_ids)
            completed: list[str] = []
            for event in events:
                for signal in self._signals_for_event(event, engine):
                    self._execute_reconstructed_signal(engine, event, signal, cursor.recovery_state)
                completed.append(event.event_id)
            if completed:
                self.database.clear_pending_reconstruction_events(normalized, completed)

    async def ingest_watched_state(self, wallet: str, payload: dict[str, object]) -> None:
        attributed = str(payload.get("user") or "").lower()
        if attributed and attributed != wallet.lower():
            return
        account_value, state_key, parse_status = _live_state_equity(payload)
        snapshot = TraderSnapshot(
            snapshot_id=f"wsstate_{wallet.lower()}_{int(utc_now().timestamp() * 1000)}",
            target_wallet=wallet.lower(), snapshot_timestamp=utc_now(), account_value=account_value, withdrawable=None,
            total_notional_position=None, positions={"websocket": payload, "equity_state_key": state_key,
                                                      "equity_parse_status": parse_status}, source="hyperliquid", raw_payload=payload,
        )
        self.database.insert_snapshot(snapshot)

    async def ingest_market_update(self, payload: dict[str, object]) -> None:
        with self._execution_lock:
            self._ingest_market_update(payload)

    def _ingest_market_update(self, payload: dict[str, object]) -> None:
        mids = payload.get("mids") if isinstance(payload.get("mids"), dict) else payload
        if not isinstance(mids, dict):
            return
        received = utc_now()
        observed_at = payload.get("time") or received
        for symbol, value in mids.items():
            price = _float_or_none(value)
            if price is not None and price > 0:
                self.market_cache.update_mid(str(symbol), price, timestamp=observed_at, received_at=received)
        engine = self._live_engine
        if engine is None and self.database.list_virtual_positions(open_only=True):
            engine = self._execution_engine()
        if engine is None:
            return
        open_symbols = {sleeve.symbol for sleeve in engine.portfolio.sleeves.values() if sleeve.is_open}
        for symbol in open_symbols:
            observation, _ = self.market_cache.latest_available(symbol, received, self.config.paper_execution.market_data_max_age_ms)
            if not observation:
                continue
            engine.mark_to_market(symbol, observation.price, received)
            previous = self._last_mark_persist_at.get(symbol)
            if previous is None or (received - as_utc(previous)).total_seconds() * 1000 >= self.config.paper_execution.mark_persist_interval_ms:
                engine.persist_mark(received)
                self._last_mark_persist_at[symbol] = received

    def close_all_paper_positions(self, *, pause_after: bool = False) -> dict[str, Any]:
        """Close fresh-mark sleeves through the one serialized PAPER engine.

        The engine is first reconstructed from database truth, then retained as
        the service's live instance after the committed closes.  A later mark
        therefore sees closed sleeves in memory as well as in persistence.
        """
        with self._execution_lock:
            prior_state = self.control_store.control_state()["state"]
            self.control_store.set_control_state("EXITING", note="Flattening open PAPER positions.")
            engine = self.reload_execution_state()
            now = utc_now()
            groups: dict[tuple[str, str], list[Any]] = {}
            for sleeve in engine.portfolio.sleeves.values():
                if sleeve.is_open:
                    groups.setdefault((sleeve.target_wallet, sleeve.symbol), []).append(sleeve)
            attempted: list[dict[str, Any]] = []
            closed: list[dict[str, Any]] = []
            failed: list[dict[str, Any]] = []
            skipped: list[dict[str, Any]] = []
            for (wallet, symbol), sleeves in groups.items():
                mark = next((
                    item.current_mark for item in sleeves
                    if item.current_mark and (now - item.updated_at).total_seconds() * 1000
                    <= self.config.paper_execution.market_data_max_age_ms
                ), None)
                if not mark:
                    result = {"wallet": wallet, "symbol": symbol, "status": "skipped", "reason": "no_fresh_market_reference"}
                    attempted.append(result)
                    skipped.append(result)
                    self.control_store.record_activity(
                        category="control", severity="warning", wallet=wallet, symbol=symbol,
                        message="Could not close PAPER position: no fresh market reference",
                        payload={"paper": True, "attempt_status": "skipped", "remaining_open": True},
                    )
                    continue
                signal = CopySignal(
                    signal_id=stable_id("manual_paper_close", wallet, symbol, now), target_wallet=wallet, campaign_id=None,
                    source_event_id=stable_id("manual_close_source", wallet, symbol, now), symbol=symbol, action="close",
                    direction=sleeves[0].direction, target_price=float(mark), target_quantity=sum(item.quantity for item in sleeves),
                    target_notional=sum(item.quantity for item in sleeves) * float(mark), allocation_fraction=0.0,
                    requested_capital=0.0, created_at=now, source_event_timestamp=now,
                    reason="manual_close_all_paper_positions",
                )
                attempt = engine.process_signal(
                    signal, received_at=now, market_price=float(mark),
                    market_metadata={"market_reference_source": "persisted_live_mark", "paper_control": "close_all"},
                )
                group_remaining = any(
                    position.target_wallet == wallet and position.symbol == symbol
                    for position in self.database.list_virtual_positions(open_only=True)
                )
                result = {"wallet": wallet, "symbol": symbol, "status": attempt.status, "reason": attempt.reason}
                attempted.append(result)
                if attempt.status == "filled" and not group_remaining:
                    closed.append(result)
                else:
                    if attempt.status == "skipped":
                        skipped.append(result)
                    failed.append({**result, "reason": attempt.reason if attempt.status != "filled" else "position_remains_open"})
                self.control_store.record_activity(
                    category="control", severity="info" if attempt.status == "filled" and not group_remaining else "warning",
                    wallet=wallet, symbol=symbol, message=f"Close-all PAPER action {attempt.status} for {symbol}",
                    payload={"reason": attempt.reason, "paper": True, "attempt_status": attempt.status,
                             "remaining_open": group_remaining},
                )
            remaining_positions = self.database.list_virtual_positions(open_only=True)
            remaining_open_positions = [
                {"sleeve_id": position.sleeve_id, "wallet": position.target_wallet, "symbol": position.symbol,
                 "direction": position.direction, "quantity": position.quantity}
                for position in remaining_positions
            ]
            partial = bool(remaining_open_positions)
            final = "PAUSED" if pause_after or partial else str(prior_state)
            note = (
                "Exit + pause completed." if pause_after else
                "Close-all partially completed; new PAPER entries remain paused until explicitly resumed."
                if partial else "Close-all PAPER positions completed; entry state retained."
            )
            if partial:
                self.control_store.record_activity(
                    category="control", severity="warning", wallet=None, symbol=None,
                    message="Close-all PAPER positions incomplete; new PAPER entries remain paused.",
                    payload={"paper": True, "remaining_open_positions": remaining_open_positions, "failed": failed},
                )
            return {
                "status": "partial" if partial else "completed", "attempted": attempted, "closed": closed,
                "failed": failed, "skipped": skipped, "remaining_open_positions": remaining_open_positions,
                "control": self.control_store.set_control_state(final, note=note), "paper_only": True,
            }

    async def reconcile_wallet(self, wallet: str) -> int:
        """Reconcile retained public history while proving the local overlap.

        Receiving a nonempty response is not proof that the public retention
        window still joins the durable local ledger.  A durable source fill ID
        must appear in the returned range; otherwise entry copying fails closed
        until an operator performs a verified flat-source rebaseline.
        """
        normalized = wallet.lower()
        anchor = self.database.latest_raw_fill(normalized)
        prior = self.database.reconstruction_cursor(normalized)
        baseline_detail = {
            "safe_rebaseline_after": prior.recovery_detail["safe_rebaseline_after"],
        } if "safe_rebaseline_after" in prior.recovery_detail else {}
        self.database.set_recovery_state(
            normalized, "RECOVERING", anchor=anchor,
            detail={"reason": "watcher_reconcile", "prior_state": prior.recovery_state, **baseline_detail},
        )
        fills = await asyncio.to_thread(self.adapter.fetch_user_fills, normalized)
        returned = {fill.event_id for fill in fills}
        continuous = anchor is None or anchor.event_id in returned
        if not continuous:
            snapshot = await asyncio.to_thread(self.adapter.fetch_clearinghouse_state, normalized)
            self.database.insert_snapshot(snapshot)
            self.database.set_recovery_state(
                normalized, "RECOVERY_INCOMPLETE", anchor=anchor,
                detail={
                    "reason": "recovery_anchor_missing", "anchor_event_id": anchor.event_id,
                    "anchor_timestamp": anchor.event_timestamp.isoformat(), "returned_fill_count": len(fills),
                    "clearinghouse_snapshot_id": snapshot.snapshot_id,
                },
            )
            self.control_store.record_activity(
                category="recovery", severity="warning", wallet=normalized,
                message="Source continuity could not be proven; PAPER entries are fail-closed.",
                payload={"recovery_state": "RECOVERY_INCOMPLETE", "anchor_event_id": anchor.event_id,
                         "returned_fill_count": len(fills), "paper_entries_blocked": True, "exits_allowed": True},
            )
        elif prior.recovery_state == "RECOVERY_INCOMPLETE":
            # A later overlap cannot prove the unobserved historical gap did
            # not contain an economic transition.  Only safe rebaseline below
            # can leave the incomplete state.
            self.database.set_recovery_state(
                normalized, "RECOVERY_INCOMPLETE", anchor=anchor,
                detail={"reason": "prior_recovery_gap_requires_safe_rebaseline", "anchor_event_id": anchor.event_id if anchor else None},
            )
        else:
            self.database.set_recovery_state(
                normalized, "CONTINUOUS", anchor=anchor,
                detail={"reason": "recovery_anchor_verified" if anchor else "initial_source_baseline",
                        "anchor_event_id": anchor.event_id if anchor else None, **baseline_detail},
            )
            self.control_store.record_activity(
                category="recovery", severity="info", wallet=normalized,
                message="Source continuity verified for watcher reconciliation.",
                payload={"recovery_state": "CONTINUOUS", "anchor_event_id": anchor.event_id if anchor else None},
            )
        await self.ingest_watched_fills(normalized, fills, True)
        return len(fills)

    async def safe_rebaseline_recovery(self, wallet: str) -> dict[str, object]:
        """Explicitly permit a new source baseline only after a verified flat state.

        Existing raw evidence and incomplete campaigns are retained unchanged.
        The cursor records a zero-source baseline for future incremental
        transitions; it never fabricates the missing close, price, or P&L, and
        it never mutates PAPER sleeves.
        """
        normalized = wallet.lower()
        cursor = self.database.reconstruction_cursor(normalized)
        if cursor.recovery_state != "RECOVERY_INCOMPLETE":
            return {"wallet": normalized, "accepted": False, "reason": "recovery_not_incomplete"}
        snapshot = await asyncio.to_thread(self.adapter.fetch_clearinghouse_state, normalized)
        self.database.insert_snapshot(snapshot)
        if not _clearinghouse_snapshot_is_flat(snapshot):
            self.control_store.record_activity(
                category="recovery", severity="warning", wallet=normalized,
                message="Safe rebaseline rejected because source clearinghouse exposure is not provably flat.",
                payload={"recovery_state": "RECOVERY_INCOMPLETE", "snapshot_id": snapshot.snapshot_id},
            )
            return {"wallet": normalized, "accepted": False, "reason": "source_not_provably_flat", "snapshot_id": snapshot.snapshot_id}
        rebased_at = snapshot.snapshot_timestamp
        updated = self.database.set_recovery_state(
            normalized, "CONTINUOUS", anchor=None,
            detail={
                "reason": "operator_acknowledged_flat_rebaseline", "snapshot_id": snapshot.snapshot_id,
                "safe_rebaseline_after": rebased_at.isoformat(), "prior_recovery_anchor": cursor.recovery_anchor_event_id,
            },
        )
        self.control_store.record_activity(
            category="recovery", severity="warning", wallet=normalized,
            message="Operator acknowledged a verified flat source state; future campaigns use a new zero baseline.",
            payload={"recovery_state": updated.recovery_state, "snapshot_id": snapshot.snapshot_id,
                     "safe_rebaseline_after": rebased_at.isoformat(), "paper_only": True},
        )
        return {"wallet": normalized, "accepted": True, "snapshot_id": snapshot.snapshot_id,
                "recovery_state": updated.recovery_state, "safe_rebaseline_after": rebased_at.isoformat()}

    def recovery_status(self, wallet: str | None = None) -> dict[str, object]:
        cursors = self.database.reconstruction_cursors([wallet] if wallet else None)
        cursor_by_wallet = {item.target_wallet: item for item in cursors}
        expected = [wallet.lower()] if wallet else self.monitored_execution_wallets()
        rows: list[dict[str, object]] = []
        for expected_wallet in expected:
            item = cursor_by_wallet.pop(expected_wallet, None)
            if item is None:
                rows.append({
                    "wallet": expected_wallet, "state": "NOT_STARTED", "anchor_event_id": None,
                    "anchor_timestamp": None, "detail": {"reason": "no_durable_recovery_checkpoint"},
                    "updated_at": None, "entries_blocked": True, "exits_allowed": True,
                })
                continue
            rows.append({
                "wallet": item.target_wallet, "state": item.recovery_state,
                "anchor_event_id": item.recovery_anchor_event_id,
                "anchor_timestamp": item.recovery_anchor_timestamp.isoformat() if item.recovery_anchor_timestamp else None,
                "detail": item.recovery_detail,
                "updated_at": item.updated_at.isoformat() if hasattr(item.updated_at, "isoformat") else item.updated_at,
                "entries_blocked": item.recovery_state != "CONTINUOUS", "exits_allowed": True,
            })
        for item in cursor_by_wallet.values():
            rows.append({
                "wallet": item.target_wallet, "state": item.recovery_state,
                "anchor_event_id": item.recovery_anchor_event_id,
                "anchor_timestamp": item.recovery_anchor_timestamp.isoformat() if item.recovery_anchor_timestamp else None,
                "detail": item.recovery_detail,
                "updated_at": item.updated_at.isoformat() if hasattr(item.updated_at, "isoformat") else item.updated_at,
                "entries_blocked": item.recovery_state != "CONTINUOUS", "exits_allowed": True,
            })
        return {
            "wallets": sorted(rows, key=lambda item: str(item["wallet"])),
            "paper_only": True,
        }

    async def reconcile_approved_wallets(self) -> dict[str, int]:
        """Compatibility alias for the former approved-target watcher API."""
        return await self.reconcile_monitored_wallets()

    def approved_wallets(self) -> list[str]:
        """Compatibility alias; new entry monitoring is Active-only."""
        return self.monitored_execution_wallets()

    def monitored_execution_wallets(self) -> list[str]:
        """Watch active entry targets and any wallet with an open paper sleeve for exits."""
        active = {target.wallet for target in self.database.list_targets(TargetStatus.ACTIVE.value)}
        exits = {position.target_wallet for position in self.database.list_virtual_positions(open_only=True)}
        return sorted(active | exits)

    async def reconcile_monitored_wallets(self) -> dict[str, int]:
        result: dict[str, int] = {}
        for wallet in self.monitored_execution_wallets():
            result[wallet] = await self.reconcile_wallet(wallet)
        return result

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
    try:
        return float(value) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None


def _clearinghouse_snapshot_is_flat(snapshot: TraderSnapshot) -> bool:
    """Accept only an explicitly parseable zero-position public state."""
    positions = snapshot.positions.get("asset_positions") if isinstance(snapshot.positions, dict) else None
    if not isinstance(positions, list):
        return False
    for item in positions:
        position = item.get("position") if isinstance(item, dict) and isinstance(item.get("position"), dict) else item
        if not isinstance(position, dict):
            return False
        value = position.get("szi", position.get("size", position.get("position")))
        try:
            if value is None or abs(float(value)) > 1e-12:
                return False
        except (TypeError, ValueError):
            return False
    return True


def _live_state_equity(payload: dict[str, object]) -> tuple[float | None, str | None, str]:
    """Select only an unambiguous canonical perp state from official payloads.

    The official `clearinghouseStates` is a record keyed by dex.  A single
    state is safe.  With several, only the empty/default key (the same implicit
    first-perp choice used by the public adapter) is accepted; otherwise the
    snapshot remains deliberately unpriced instead of summing unrelated dexes.
    """
    states = payload.get("clearinghouseStates")
    if isinstance(states, dict):
        usable = {str(key): value for key, value in states.items() if isinstance(value, dict)}
        if len(usable) == 1:
            key, state = next(iter(usable.items()))
        elif "" in usable:
            key, state = "", usable[""]
        elif len(usable) > 1:
            return None, None, "ambiguous_multiple_states"
        else:
            return None, None, "missing_clearinghouse_state"
        margin = state.get("marginSummary") if isinstance(state.get("marginSummary"), dict) else {}
        value = _float_or_none(margin.get("accountValue"))
        return value, key, "ok" if value is not None else "missing_margin_account_value"
    # A direct clearinghouseState/top-level marginSummary is retained for
    # compatibility with older fixtures and explicit single-dex subscriptions.
    state = payload.get("clearinghouseState") if isinstance(payload.get("clearinghouseState"), dict) else payload
    margin = state.get("marginSummary") if isinstance(state, dict) and isinstance(state.get("marginSummary"), dict) else {}
    value = _float_or_none(margin.get("accountValue"))
    return value, None, "legacy_ok" if value is not None else "missing_clearinghouse_states"
