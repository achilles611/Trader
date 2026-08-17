"""D.7 public-data ignition, provenance, coverage, and observer services.

This module deliberately does not add an alternate research engine.  It turns
verified official source objects and public websocket messages into the D.6
``ScientificWorker.ingest_observation`` bridge, then lets the durable D.6
queue materialize features, labels, discovery, experiments, and shadow work.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import shutil
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

from .config import CopyTradeConfig
from .discovery import DiscoveryProviderError, HyperCoreNodeTradeDiscoveryProvider, LocalNodeTradeFileTransport
from .models import as_utc, iso, stable_id, utc_now
from .science_repository import ScientificRepository, canonical_hash
from .science_storage import StorageRoots
from .scientific_worker import ScientificWorker, WorkerStage
from .source_acquisition import (
    HistoricalHourPlan,
    HyperCoreObject,
    HyperCoreSourceAcquisition,
    HyperCoreSourceError,
    historical_hour_slots,
)


OFFICIAL_ARCHIVE_SOURCE = "HISTORICAL_OFFICIAL_ARCHIVE"
LIVE_PUBLIC_SOURCE = "LIVE_PUBLIC_OBSERVATION"
OFFICIAL_SOURCE_NAME = "hyperliquid_hypercore_node_fills_by_block"
PARSER_VERSION = "d7-hypercore-normalizer-v1"
SCHEMA_VERSION = 1


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(value: datetime | str) -> str:
    return iso(as_utc(value)).replace("+00:00", "Z")


def _time(value: str | datetime) -> datetime:
    return as_utc(value)


def _number(value: object) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if result == result and abs(result) != float("inf") else None


def _side(value: object) -> str | None:
    normalized = str(value or "").strip().lower()
    if normalized in {"b", "buy", "long", "open_long"}:
        return "buy"
    if normalized in {"a", "s", "sell", "short", "open_short"}:
        return "sell"
    return None


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def source_capabilities() -> list[dict[str, str]]:
    """The D.7 capability audit, intentionally distinguishing trade prints from mids."""
    return [
        {"evidence": "wallet fills", "historical": "yes", "live_current": "yes (configured public wallets)", "source": "official HyperCore fills / public userFills"},
        {"evidence": "trade price", "historical": "yes (observed fills, not midpoint)", "live_current": "yes (allMids midpoint)", "source": "official HyperCore fills / public allMids"},
        {"evidence": "volume", "historical": "yes (observed fill quantity only)", "live_current": "unavailable", "source": "official HyperCore fills"},
        {"evidence": "spread", "historical": "unavailable", "live_current": "unavailable", "source": "not synthesized"},
        {"evidence": "depth", "historical": "unavailable", "live_current": "unavailable", "source": "not synthesized"},
        {"evidence": "liquidations", "historical": "unavailable", "live_current": "unavailable", "source": "not synthesized"},
    ]


@dataclass(frozen=True)
class CoveragePolicy:
    minimum_fraction: float
    require_state: str = "PROVEN_COMPLETE"


class DataCoverage:
    """First-class D.7 data-quality calculation over durable hour manifests."""

    def __init__(self, repository: ScientificRepository, policy: CoveragePolicy) -> None:
        self.repository = repository
        self.policy = policy

    def calculate(self, start: str, end: str, *, computed_at: str | None = None, persist: bool = True) -> dict[str, Any]:
        rows = self.repository.list_acquisition_manifest(start=start, end=end)
        expected = len(historical_hour_slots(start, end, maximum_hours=max(1, 100_000)))
        verified_states = {"VERIFIED", "PARSED", "INGESTED"}
        parsed_states = {"PARSED", "INGESTED"}
        ingested_states = {"INGESTED"}
        verified = sum(row["state"] in verified_states for row in rows)
        parsed = sum(row["state"] in parsed_states for row in rows)
        ingested = sum(row["state"] in ingested_states for row in rows)
        missing = sum(row["state"] == "MISSING_SOURCE" for row in rows)
        corrupt = sum(row["state"] == "FAILED" or int(row["malformed_count"] or 0) > 0 for row in rows)
        observations = sum(int(row["observation_count"] or 0) for row in rows)
        duplicates = sum(int(row["duplicate_count"] or 0) for row in rows)
        first = min((str(row["first_event_at"]) for row in rows if row["first_event_at"]), default=None)
        last = max((str(row["last_event_at"]) for row in rows if row["last_event_at"]), default=None)
        timestamp_anomalies = sum(
            int(bool(row["first_event_at"] and (str(row["first_event_at"]) < str(row["expected_start"]) or str(row["first_event_at"]) >= str(row["expected_end"]))))
            + int(bool(row["last_event_at"] and (str(row["last_event_at"]) < str(row["expected_start"]) or str(row["last_event_at"]) >= str(row["expected_end"]))))
            for row in rows
        )
        fraction = ingested / expected if expected else 0.0
        if corrupt:
            state = "CORRUPT"
        elif missing:
            state = "KNOWN_GAP"
        elif len(rows) < expected or not rows:
            state = "UNPROVEN"
        elif ingested == expected and fraction >= self.policy.minimum_fraction:
            state = "PROVEN_COMPLETE"
        elif verified or parsed or ingested:
            state = "PROVEN_PARTIAL"
        else:
            state = "UNPROVEN"
        payload = {
            "coverage_id": "coverage-" + canonical_hash({"start": start, "end": end, "rows": [(row["expected_start"], row["state"], row["local_sha256"], row["observation_count"]) for row in rows]})[:28],
            "interval_start": start, "interval_end": end, "source_name": OFFICIAL_SOURCE_NAME,
            "state": state, "coverage_fraction": fraction, "expected_hours": expected, "verified_hours": verified,
            "missing_hours": missing, "malformed_hours": corrupt, "parsed_hours": parsed,
            "observation_count": observations, "duplicate_count": duplicates,
            "timestamp_anomalies": timestamp_anomalies, "first_event_at": first, "last_event_at": last,
            "wallet_attribution_quality": "PROVEN_PER_FILL" if parsed and not corrupt else "UNPROVEN",
            "market_evidence_availability": "OFFICIAL_TRADE_PRINTS_ONLY" if parsed else "UNAVAILABLE",
            "details": {
                "ingested_hours": ingested, "manifest_rows": len(rows), "source_capabilities": source_capabilities(),
                "policy": {"minimum_fraction": self.policy.minimum_fraction, "require_state": self.policy.require_state},
            },
            "computed_at": computed_at or _iso(_now()),
        }
        return self.repository.record_coverage(payload) if persist else payload

    def eligible(self, coverage: Mapping[str, Any] | None) -> bool:
        return bool(coverage and coverage.get("state") == self.policy.require_state
                    and float(coverage.get("coverage_fraction") or 0.0) >= self.policy.minimum_fraction)


class DataIgnitionCommissioner:
    """Bounded orchestration for historical source data and D.6 commissioning."""

    def __init__(
        self, repository: ScientificRepository, worker: ScientificWorker, config: CopyTradeConfig,
        *, source: HyperCoreSourceAcquisition | None = None,
    ) -> None:
        self.repository = repository
        self.worker = worker
        self.config = config
        self.settings = config.commissioning
        self.roots = StorageRoots(home=Path.cwd(), hot_root=config.artifacts.database_path.parent, cold_root=config.storage.cold_root)
        self.source = source or HyperCoreSourceAcquisition(config.artifacts.database_path.parent / "hypercore-cache")
        self.coverage = DataCoverage(repository, CoveragePolicy(self.settings.min_coverage_fraction))
        self.repository.initialize()

    @property
    def range(self) -> tuple[str, str]:
        return _iso(self.settings.historical_start), _iso(self.settings.historical_end)

    def source_status(self, *, test_access: bool = False) -> dict[str, Any]:
        return {**self.source.source_status(test_access=test_access), "capabilities": source_capabilities(), "execution_mode": "SIMULATION_SHADOW_ONLY"}

    def plan_history(self, start: str | None = None, end: str | None = None) -> dict[str, Any]:
        requested_start, requested_end = _iso(start or self.settings.historical_start), _iso(end or self.settings.historical_end)
        slots = historical_hour_slots(requested_start, requested_end, maximum_hours=self.settings.max_hours_per_run)
        now = _iso(_now())
        for slot in slots:
            self.repository.record_acquisition_hour(
                expected_start=slot.start, expected_end=slot.end, source_name=OFFICIAL_SOURCE_NAME,
                state="PLANNED", updated_at=now, parser_version=PARSER_VERSION, schema_version=SCHEMA_VERSION,
            )
        self.repository.set_watermark("historical_acquisition_cursor", slots[0].start, updated_at=now,
                                      status="PLANNED", details={"start": requested_start, "end": requested_end, "hours": len(slots)})
        return {"state": "PLANNED", "start": requested_start, "end": requested_end, "hours": len(slots),
                "max_download_bytes": self.settings.max_download_bytes, "max_hours_per_run": self.settings.max_hours_per_run}

    def cancel_history(self, reason: str = "operator cancelled historical acquisition") -> dict[str, Any]:
        self.repository.set_acquisition_cancelled(True, reason=reason, updated_at=_iso(_now()))
        return {"state": "CANCELLATION_REQUESTED", **self.repository.acquisition_control()}

    def acquire_history(self, start: str | None = None, end: str | None = None) -> dict[str, Any]:
        plan = self.plan_history(start, end)
        requested_start, requested_end = str(plan["start"]), str(plan["end"])
        self.repository.set_acquisition_cancelled(False, reason="", updated_at=_iso(_now()))
        # A bounded single-prefix requester-pays probe is required for this
        # command.  Source-status is intentionally non-mutating by default,
        # but acquisition must not mistake an untested credential for access.
        source_state = self.source.source_status(test_access=True)
        if source_state.get("connection_state") != "READY":
            return {"state": "SOURCE_SETUP_REQUIRED", "plan": plan, "source": source_state, "coverage": self.coverage.calculate(requested_start, requested_end)}
        slots = historical_hour_slots(requested_start, requested_end, maximum_hours=self.settings.max_hours_per_run)
        try:
            resolved = self.source.resolve_historical_slots(slots, cancelled=lambda: self.repository.acquisition_control()["cancel_requested"])
        except HyperCoreSourceError as exc:
            return {"state": "SOURCE_UNAVAILABLE", "plan": plan, "reason": str(exc), "source": self.source.source_status()}
        available: list[HyperCoreObject] = []
        now = _iso(_now())
        for slot in slots:
            if self.repository.acquisition_control()["cancel_requested"]:
                return {"state": "CANCELLED", "plan": plan, "coverage": self.coverage.calculate(requested_start, requested_end)}
            item = resolved.get(slot.start)
            if item is None:
                self.repository.record_acquisition_hour(expected_start=slot.start, expected_end=slot.end, source_name=OFFICIAL_SOURCE_NAME,
                                                        state="MISSING_SOURCE", updated_at=now, failure_reason="official source object absent")
            else:
                available.append(item)
                self.repository.record_acquisition_hour(expected_start=slot.start, expected_end=slot.end, source_name=OFFICIAL_SOURCE_NAME,
                                                        state="AVAILABLE", updated_at=now, source_identifier=item.identifier,
                                                        expected_bytes=item.size, object_checksum=item.etag)
        forecast = sum(item.size for item in available)
        if forecast > self.settings.max_download_bytes:
            return {"state": "BYTE_CAP_EXCEEDED", "plan": plan, "forecast_bytes": forecast,
                    "max_download_bytes": self.settings.max_download_bytes, "coverage": self.coverage.calculate(requested_start, requested_end)}
        if not available:
            return {"state": "NO_SOURCE_OBJECTS", "plan": plan, "forecast_bytes": 0, "coverage": self.coverage.calculate(requested_start, requested_end)}
        try:
            preflight = self.source.preflight(available)
        except HyperCoreSourceError as exc:
            return {"state": "PREFLIGHT_FAILED", "plan": plan, "forecast_bytes": forecast, "reason": str(exc), "coverage": self.coverage.calculate(requested_start, requested_end)}
        completed: list[dict[str, Any]] = []
        for item in available:
            if self.repository.acquisition_control()["cancel_requested"]:
                return {"state": "CANCELLED", "plan": plan, "completed": completed, "coverage": self.coverage.calculate(requested_start, requested_end)}
            hour_start, hour_end = _iso(str(item.data_hour_start)), _iso(str(item.data_hour_end))
            manifest = self.repository.acquisition_hour(hour_start)
            if manifest and manifest["state"] == "INGESTED":
                completed.append({"hour": item.data_hour_start, "state": "REUSED_INGESTED"})
                continue
            try:
                self.repository.record_acquisition_hour(expected_start=hour_start, expected_end=hour_end, source_name=OFFICIAL_SOURCE_NAME,
                                                        state="DOWNLOADING", updated_at=_iso(_now()))
                path, metadata = self.source.acquire(item, protected_paths=preflight["protected_paths"])
                self.repository.record_acquisition_hour(
                    expected_start=hour_start, expected_end=hour_end, source_name=OFFICIAL_SOURCE_NAME,
                    state="VERIFIED", updated_at=_iso(_now()), source_identifier=item.identifier, local_path=str(path),
                    expected_bytes=item.size, downloaded_bytes=path.stat().st_size, object_checksum=item.etag,
                    local_sha256=str(metadata["sha256"]), acquired_at=str(metadata["acquired_at"]),
                    parser_version=PARSER_VERSION, schema_version=SCHEMA_VERSION,
                )
                completed.append(self.ingest_verified_hour(hour_start))
            except (HyperCoreSourceError, DiscoveryProviderError, OSError, ValueError) as exc:
                self.repository.record_acquisition_hour(
                    expected_start=hour_start, expected_end=hour_end, source_name=OFFICIAL_SOURCE_NAME,
                    state="FAILED", updated_at=_iso(_now()), failure_reason=f"{type(exc).__name__}: {str(exc)[:400]}", increment_retry=True,
                )
                completed.append({"hour": item.data_hour_start, "state": "FAILED", "reason": str(exc)})
        coverage = self.coverage.calculate(requested_start, requested_end)
        selection = self._schedule_selected_corpus(requested_start, requested_end) if self.coverage.eligible(coverage) else None
        self.repository.set_watermark("historical_acquisition_cursor", requested_end, updated_at=_iso(_now()), status=coverage["state"],
                                      details={"coverage_id": coverage["coverage_id"], "forecast_bytes": forecast})
        return {"state": "COMPLETED" if coverage["state"] == "PROVEN_COMPLETE" else "PARTIAL", "plan": plan,
                "forecast_bytes": forecast, "preflight": preflight, "hours": completed, "coverage": coverage, "selection": selection}

    def ingest_verified_hour(self, expected_start: str) -> dict[str, Any]:
        manifest = self.repository.acquisition_hour(expected_start)
        if not manifest or manifest["state"] not in {"VERIFIED", "PARSED", "INGESTED"}:
            raise ValueError("Historical ingestion requires a verified source-hour manifest.")
        if manifest["state"] == "INGESTED":
            return {"hour": expected_start, "state": "REUSED_INGESTED", "observations": int(manifest["observation_count"])}
        path = Path(str(manifest["local_path"] or ""))
        if not path.exists() or _sha256(path) != str(manifest["local_sha256"]):
            raise ValueError("Verified source cache is missing or checksum-corrupt; refusing historical ingestion.")
        self.repository.record_acquisition_hour(expected_start=manifest["expected_start"], expected_end=manifest["expected_end"],
                                                source_name=OFFICIAL_SOURCE_NAME, state="PARSED", updated_at=_iso(_now()))
        provider = HyperCoreNodeTradeDiscoveryProvider(LocalNodeTradeFileTransport((path,)))
        normalized, duplicates, first, last, malformed = 0, 0, None, None, 0
        pending: list[dict[str, Any]] = []

        def flush_pending() -> None:
            nonlocal normalized, duplicates
            if not pending:
                return
            rows = self.worker.ingest_observations(pending, schedule_features=False)
            normalized += len(rows)
            duplicates += sum(not bool(row.get("inserted")) for row in rows)
            pending.clear()

        try:
            for observed in provider.discover():
                event_at = _iso(observed.recent_activity_at or observed.observed_at)
                first = min(first, event_at) if first else event_at
                last = max(last, event_at) if last else event_at
                raw = dict(observed.raw_evidence)
                symbol = str(observed.metadata.get("coin") or raw.get("coin") or raw.get("symbol") or "")
                price = _number(raw.get("px", raw.get("price")))
                quantity = _number(raw.get("sz", raw.get("size")))
                side = _side(raw.get("side") or raw.get("dir"))
                if not symbol or price is None or price <= 0 or quantity is None or quantity == 0 or side is None:
                    malformed += 1
                    continue
                source_ref = str(manifest["source_identifier"])
                # Some official fills use ``tid=0`` for independent dust
                # conversions.  A trade-id-only key would therefore merge
                # distinct evidence.  Preserve the documented block/event
                # position when available and still deduplicate exact replay.
                legacy_wallet_event_id = f"wallet:{source_ref}:{observed.evidence_id or canonical_hash(raw)}"
                ambiguous_zero_tid = str(raw.get("tid") or "") in {"", "0"}
                source_record_identity = canonical_hash({
                    "provider_event_id": observed.evidence_id, "wallet": observed.wallet.lower(), "raw": raw,
                    "block_number": observed.metadata.get("block_number"), "block_event_index": observed.metadata.get("block_event_index"),
                })
                prior_legacy = self.repository.observation_for_source_event(source=OFFICIAL_ARCHIVE_SOURCE, source_event_id=legacy_wallet_event_id) if ambiguous_zero_tid else None
                # Maintain exact identities from an interrupted pre-hardening
                # pass when the legacy ID still names the same immutable raw
                # event.  Only ambiguous tid=0 records receive block-position
                # suffixes, avoiding replay duplicates while separating them.
                if prior_legacy and prior_legacy.get("payload", {}).get("raw_event") == raw:
                    wallet_event_id = legacy_wallet_event_id
                elif not ambiguous_zero_tid:
                    wallet_event_id = legacy_wallet_event_id
                else:
                    wallet_event_id = f"wallet:{source_ref}:{source_record_identity}"
                market_event_id = f"market:{source_ref}:{canonical_hash({'raw': raw, 'symbol': symbol, 'event_at': event_at})}"
                payload = {
                    "origin": OFFICIAL_ARCHIVE_SOURCE, "source_object": source_ref, "source_block_event": observed.metadata,
                    "price": price, "quantity": abs(quantity), "notional": abs(quantity) * price, "side": side,
                    "estimated_cost": self.config.paper_execution.fee_rate, "raw_event": raw,
                }
                pending.extend((
                    {"kind": "WALLET_FILL", "source": OFFICIAL_ARCHIVE_SOURCE, "source_event_id": wallet_event_id,
                     "wallet": observed.wallet, "symbol": symbol, "event_at": event_at,
                     "received_at": str(manifest["acquired_at"] or _iso(_now())), "payload": payload,
                     "quality_flags": {"origin": OFFICIAL_ARCHIVE_SOURCE, "wallet_attribution": "official_per_fill", "historical": True}},
                    {"kind": "MARKET_PRICE", "source": OFFICIAL_ARCHIVE_SOURCE, "source_event_id": market_event_id,
                     "symbol": symbol, "event_at": event_at, "received_at": str(manifest["acquired_at"] or _iso(_now())),
                     "payload": {"origin": OFFICIAL_ARCHIVE_SOURCE, "source_object": source_ref, "price": price,
                                 "volume": abs(quantity), "market_evidence_kind": "OFFICIAL_TRADE_PRICE_NOT_MID"},
                     "quality_flags": {"origin": OFFICIAL_ARCHIVE_SOURCE, "historical": True, "is_midpoint": False,
                                       "spread": "unavailable", "depth": "unavailable", "liquidations": "unavailable"}},
                ))
                if len(pending) >= self.config.scientific_worker.max_sqlite_write_batch:
                    flush_pending()
            flush_pending()
        except DiscoveryProviderError:
            raise
        malformed += int(provider.discovery_stats.get("malformed_events", 0)) + int(provider.discovery_stats.get("unsupported_records", 0))
        self.repository.record_acquisition_hour(
            expected_start=manifest["expected_start"], expected_end=manifest["expected_end"], source_name=OFFICIAL_SOURCE_NAME,
            state="INGESTED", updated_at=_iso(_now()), parser_version=PARSER_VERSION, schema_version=SCHEMA_VERSION,
            observation_count=normalized, duplicate_count=duplicates, malformed_count=malformed, first_event_at=first, last_event_at=last,
        )
        archive = self._archive_verified_source(manifest, path) if self.settings.archive_verified_sources else {"state": "HOT_RETAINED"}
        return {"hour": expected_start, "state": "INGESTED", "observations": normalized, "duplicates": duplicates,
                "malformed": malformed, "first_event_at": first, "last_event_at": last, "archive": archive}

    def _schedule_selected_corpus(self, start: str, end: str) -> dict[str, Any]:
        """Select a bounded, chronological, deterministic D.6 working corpus.

        All verified raw observations remain in the immutable repository.  The
        selection merely bounds feature/outcome work to the configured hot
        corpus, whose IDs are persisted in a watermark and later incorporated
        into the immutable corpus snapshot.
        """
        available, rows = self.repository.select_observations_evenly(
            source=OFFICIAL_ARCHIVE_SOURCE, kind="WALLET_FILL", start=start, end=end,
            maximum=self.settings.max_corpus_observations,
        )
        if not rows:
            return {"selected": 0, "available": 0, "superseded_work": 0}
        selected = rows
        selected_ids = tuple(str(row["observation_id"]) for row in selected)
        superseded = self.repository.supersede_pending_source_observation_work_except(
            work_types=(WorkerStage.FEATURE_MATERIALIZATION.value, WorkerStage.OUTCOME_LABEL.value),
            source=OFFICIAL_ARCHIVE_SOURCE, start=start, end=end, observation_ids=selected_ids,
            reason="D.7 bounded historical corpus selection",
        )
        superseded += self.repository.supersede_pending_historical_observation_work_except(
            work_types=(WorkerStage.FEATURE_MATERIALIZATION.value, WorkerStage.OUTCOME_LABEL.value),
            source=OFFICIAL_ARCHIVE_SOURCE, observation_ids=selected_ids,
            reason="D.7 current historical corpus supersedes out-of-interval archive projection work",
        )
        for observation in selected:
            fingerprint = str(observation["raw_fingerprint"])
            self.worker._enqueue(WorkerStage.FEATURE_MATERIALIZATION, "observation", str(observation["observation_id"]), 1, fingerprint)
            market_fingerprint = canonical_hash({"source": OFFICIAL_ARCHIVE_SOURCE, "symbol": observation.get("symbol"),
                                                 "anchor": observation["raw_fingerprint"], "range": [start, end]})
            self.worker._enqueue(WorkerStage.OUTCOME_LABEL, "observation", str(observation["observation_id"]), 1, market_fingerprint,
                                 supersede_available=True)
        detail = {"start": start, "end": end, "available_wallet_observations": available, "selected_observation_ids": list(selected_ids),
                  "selection_method": "chronological_time_stratified", "max_corpus_observations": self.settings.max_corpus_observations,
                  "superseded_pending_projection_work": superseded}
        self.repository.set_watermark("d7_historical_corpus_selection", canonical_hash(detail), updated_at=_iso(_now()), status="READY", details=detail)
        return {"selected": len(selected_ids), "available": available, "superseded_work": superseded, "fingerprint": canonical_hash(detail)}

    def _archive_verified_source(self, manifest: Mapping[str, Any], hot_path: Path) -> dict[str, Any]:
        """Copy verified raw source to D: before any optional hot eviction."""
        self.roots.ensure_cold()
        status = self.roots.cold_status()
        if not status["cold_available"]:
            return {"state": "HOT_RETAINED_COLD_UNAVAILABLE", **status}
        when = _time(str(manifest["expected_start"]))
        destination = self.roots.cold_root / "source-cache" / when.strftime("%Y/%m/%d") / hot_path.name
        destination.parent.mkdir(parents=True, exist_ok=True)
        partial = destination.with_suffix(destination.suffix + ".partial")
        shutil.copy2(hot_path, partial)
        if _sha256(partial) != str(manifest["local_sha256"]):
            partial.unlink(missing_ok=True)
            raise OSError("Cold source archive checksum mismatch; preserving the hot verified copy.")
        os.replace(partial, destination)
        # The manifest is updated before eviction.  The source-acquisition
        # cache metadata remains useful provenance but its absent hot file is
        # never treated as a reusable verified cache object.
        self.repository.record_acquisition_hour(
            expected_start=str(manifest["expected_start"]), expected_end=str(manifest["expected_end"]), source_name=OFFICIAL_SOURCE_NAME,
            state="INGESTED", updated_at=_iso(_now()), local_path=str(destination), local_sha256=str(manifest["local_sha256"]),
        )
        hot_path.unlink(missing_ok=True)
        return {"state": "ARCHIVED_COLD", "destination": str(destination), "checksum_sha256": str(manifest["local_sha256"]), **status}

    def corpus_snapshot(self, start: str | None = None, end: str | None = None) -> dict[str, Any]:
        begin, finish = _iso(start or self.settings.historical_start), _iso(end or self.settings.historical_end)
        coverage = self.coverage.calculate(begin, finish)
        if not self.coverage.eligible(coverage):
            raise ValueError(f"Research corpus is not eligible: coverage state is {coverage['state']} ({coverage['coverage_fraction']:.3f}).")
        selection = self.repository.get_watermark("d7_historical_corpus_selection")
        selected_ids = set(selection.get("details", {}).get("selected_observation_ids", [])) if selection else set()
        observations = self.repository.observations_by_ids(tuple(sorted(selected_ids))) if selected_ids else []
        observations = [row for row in observations if begin <= str(row["normalized_at"]) < finish
                        and row["source"] == OFFICIAL_ARCHIVE_SOURCE]
        fingerprint = canonical_hash({"range": [begin, finish], "coverage": coverage["coverage_id"],
                                      "observations": [(row["observation_id"], row["payload_hash"]) for row in observations],
                                      "features": [(row["feature_id"], row["version"]) for row in self.repository.list_features()]})
        payload = {
            "corpus_fingerprint": "corpus-" + fingerprint[:28], "interval_start": begin, "interval_end": finish,
            "coverage_id": coverage["coverage_id"], "observation_fingerprint": canonical_hash([(row["observation_id"], row["payload_hash"]) for row in observations]),
            "feature_versions": [{"feature_id": row["feature_id"], "version": row["version"]} for row in self.repository.list_features()],
            "symbols": sorted({str(row["symbol"]) for row in observations if row.get("symbol")}),
            "code_sha": "d7-data-ignition-v1", "config_sha": canonical_hash(self.config.research_snapshot()),
            "created_at": _iso(_now()), "coverage": coverage, "source_hours": [row["expected_start"] for row in self.repository.list_acquisition_manifest(start=begin, end=finish)],
        }
        return self.repository.record_corpus_snapshot(payload)

    def commission(self, start: str | None = None, end: str | None = None, *, max_cycles: int = 128) -> dict[str, Any]:
        begin, finish = _iso(start or self.settings.historical_start), _iso(end or self.settings.historical_end)
        acquisition = self.acquire_history(begin, finish)
        coverage = self.coverage.calculate(begin, finish)
        if not self.coverage.eligible(coverage):
            return {"state": "COVERAGE_NOT_READY", "acquisition": acquisition, "coverage": coverage, "science": self.science_counts()}
        selection = self._schedule_selected_corpus(begin, finish)
        snapshot = self.corpus_snapshot(begin, finish)
        science = self.worker.run_until_idle(max_cycles=max_cycles)
        return {"state": "COMMISSIONED", "acquisition": acquisition, "coverage": coverage, "corpus_snapshot": snapshot,
                "selection": selection, "science": {**self.science_counts(), "worker": science}}

    def catch_up(self) -> dict[str, Any]:
        rows = self.repository.list_acquisition_manifest()
        completed = [row for row in rows if row["state"] == "INGESTED"]
        if not completed:
            return {"state": "UNPROVEN", "reason": "no proven historical observation interval"}
        last_end = max(str(row["expected_end"]) for row in completed)
        available_end = _iso((_now() - timedelta(hours=1)).replace(minute=0, second=0, microsecond=0))
        if _time(last_end) >= _time(available_end):
            return {"state": "HEALTHY", "last_proven_interval_end": last_end, "source_available_through": available_end}
        max_end = min(_time(available_end), _time(last_end) + timedelta(hours=self.settings.max_hours_per_run))
        plan = self.plan_history(last_end, _iso(max_end))
        self.repository.set_watermark("historical_catch_up", _iso(max_end), updated_at=_iso(_now()), status="CATCHING_UP",
                                      details={"gap_start": last_end, "available_end": available_end, "planned_hours": plan["hours"]})
        return {"state": "CATCHING_UP", "gap_start": last_end, "source_available_through": available_end, "plan": plan}

    def science_counts(self) -> dict[str, int]:
        kinds = self.repository.observation_counts()
        hypotheses = self.repository.list_hypotheses()
        forward = self.repository.list_forward_records()
        return {
            "observations": sum(kinds.values()), "wallet_observations": kinds.get("WALLET_FILL", 0),
            "market_observations": kinds.get("MARKET_PRICE", 0), "feature_values": len(self.repository.list_feature_values()),
            "outcome_labels": len(self.repository.list_outcome_labels()), "candidate_patterns": len(self.repository.list_discoveries()),
            "registered_hypotheses": sum(item["state"] == "REGISTERED" for item in hypotheses),
            "historical_rejects": len(self.repository.list_graveyard()),
            "historical_survivors": sum(item["state"] in {"FORWARD_SHADOW", "PROMOTED"} for item in hypotheses),
            "forward_predictions": len(forward), "forward_resolved": sum(item["outcome"] is not None for item in forward),
        }

    def status(self) -> dict[str, Any]:
        start, end = self.range
        # Status powers the read-only Control Center surface.  It derives the
        # current truth without turning a dashboard refresh into a provenance
        # write; acquisition, explicit coverage, and commissioning persist it.
        coverage = self.coverage.calculate(start, end, persist=False)
        wallet, market = self.repository.latest_observation(kind="WALLET_FILL"), self.repository.latest_observation(kind="MARKET_PRICE")
        manifest = self.repository.list_acquisition_manifest(start=start, end=end)
        verified = [row for row in manifest if row["state"] in {"VERIFIED", "PARSED", "INGESTED"}]
        now = _now()
        market_lag = (now - _time(str(market["received_at"]))).total_seconds() if market else None
        historic_lag = (now - _time(max(str(row["expected_end"]) for row in verified))).total_seconds() if verified else None
        queue = self.repository.work_queue_status(now=_iso(now))
        if coverage["state"] in {"CORRUPT", "KNOWN_GAP"}:
            health, readiness = "HISTORICAL_GAP", "DEGRADED"
        elif self.repository.get_watermark("historical_catch_up") and self.repository.get_watermark("historical_catch_up")["status"] == "CATCHING_UP":
            health, readiness = "CATCHING_UP", "PARTIAL_COVERAGE"
        elif market_lag is not None and market_lag > self.settings.max_live_receive_lag_seconds:
            health, readiness = "SOURCE_STALE", "DEGRADED"
        elif not self.coverage.eligible(coverage):
            health, readiness = "SOURCE_UNAVAILABLE" if not verified else "HISTORICAL_GAP", "PARTIAL_COVERAGE"
        elif self.repository.list_forward_records():
            health, readiness = "HEALTHY", "FORWARD_SHADOW_ACTIVE"
        elif self.repository.list_hypotheses() or self.repository.list_experiments():
            health, readiness = "HEALTHY", "RESEARCH_ACTIVE"
        else:
            health, readiness = "HEALTHY", "HISTORICAL_READY"
        return {
            "execution_mode": "SIMULATION_SHADOW_ONLY", "health": health, "readiness": readiness,
            "requested_range": {"start": start, "end": end}, "coverage": coverage,
            "manifest": {"planned": len(manifest), "verified": len(verified), "missing": sum(row["state"] == "MISSING_SOURCE" for row in manifest),
                         "failed": sum(row["state"] == "FAILED" for row in manifest), "bytes_downloaded": sum(int(row["downloaded_bytes"] or 0) for row in manifest)},
            "freshness": {"last_wallet_observation": wallet["received_at"] if wallet else None, "last_market_observation": market["received_at"] if market else None,
                          "last_historical_hour_verified": max((str(row["expected_start"]) for row in verified), default=None),
                          "coverage_lag_seconds": historic_lag, "live_receive_lag_seconds": market_lag,
                          "science_queue_oldest": queue.get("oldest_pending_at")},
            "counts": self.science_counts(), "queue": queue, "source_capabilities": source_capabilities(),
            "storage": {**self.roots.cold_status(), "hot_root": str(self.roots.hot_root),
                        "hot_free_bytes": shutil.disk_usage(self.roots.hot_root).free if self.roots.hot_root.exists() else None},
            "acquisition_control": self.repository.acquisition_control(),
        }


class PublicObservationService:
    """Restart-safe public allMids/userFills observer; it never executes trades."""

    def __init__(self, commissioner: DataIgnitionCommissioner) -> None:
        self.commissioner = commissioner
        self.config = commissioner.config
        self._stop = asyncio.Event()

    def stop(self) -> None:
        self._stop.set()

    def ingest_market(self, payload: Mapping[str, Any], *, received_at: datetime | None = None) -> int:
        received = received_at or _now()
        mids = payload.get("mids") if isinstance(payload.get("mids"), Mapping) else payload
        if not isinstance(mids, Mapping):
            return 0
        source_at = payload.get("time") or received
        records: list[dict[str, Any]] = []
        for symbol, value in mids.items():
            price = _number(value)
            if not price or price <= 0:
                continue
            event_id = stable_id("d7_live_mid", str(symbol), str(source_at), price, int(received.timestamp() * 1000))
            records.append({"kind": "MARKET_PRICE", "source": LIVE_PUBLIC_SOURCE, "source_event_id": event_id,
                            "symbol": str(symbol), "event_at": received, "received_at": received,
                            "payload": {"origin": LIVE_PUBLIC_SOURCE, "price": price, "source_timestamp": str(source_at),
                                        "market_evidence_kind": "PUBLIC_MIDPOINT"},
                            "quality_flags": {"origin": LIVE_PUBLIC_SOURCE, "is_midpoint": True}})
        # One public websocket frame contains many markets.  Persist the frame
        # atomically through the normal D.6 bridge rather than opening a
        # SQLite transaction and work item per symbol.
        rows = self.commissioner.worker.ingest_observations(records) if records else []
        count = len(rows)
        if count:
            self.commissioner.repository.set_watermark("live_public_market", _iso(received), updated_at=_iso(received), status="HEALTHY", details={"observations": count})
        return count

    def ingest_wallet_fill(self, wallet: str, fill: Mapping[str, Any], *, received_at: datetime | None = None) -> bool:
        received = received_at or _now()
        symbol = str(fill.get("coin") or fill.get("symbol") or "")
        price, quantity, side = _number(fill.get("px", fill.get("price"))), _number(fill.get("sz", fill.get("size"))), _side(fill.get("side") or fill.get("dir"))
        event_at = fill.get("time") or fill.get("timestamp") or received
        if not wallet or not symbol or not price or not quantity or side is None:
            return False
        source_event_id = stable_id("d7_live_wallet_fill", wallet.lower(), fill.get("tid") or fill.get("tradeId") or fill.get("oid"), event_at, symbol, price, quantity, side)
        self.commissioner.worker.ingest_observation(
            kind="WALLET_FILL", source=LIVE_PUBLIC_SOURCE, source_event_id=source_event_id, wallet=wallet.lower(), symbol=symbol,
            event_at=event_at, received_at=received, payload={"origin": LIVE_PUBLIC_SOURCE, "price": price, "quantity": abs(quantity),
                "notional": abs(quantity) * price, "side": side, "estimated_cost": self.config.paper_execution.fee_rate, "raw_event": dict(fill)},
            quality_flags={"origin": LIVE_PUBLIC_SOURCE, "wallet_attribution": "public_userFills", "historical": False},
        )
        self.commissioner.repository.set_watermark("live_public_wallet", _iso(received), updated_at=_iso(received), status="HEALTHY", details={"wallet": wallet.lower()})
        return True

    async def run(self, *, duration_seconds: float | None = None) -> dict[str, Any]:
        try:
            import websockets
        except ImportError as exc:  # pragma: no cover - dependency guidance
            raise RuntimeError("websockets is required for public observation.") from exc
        wallets = tuple(sorted({str(item).lower() for item in self.config.commissioning.observation_wallets}))
        deadline = asyncio.get_running_loop().time() + duration_seconds if duration_seconds else None
        backoff, reconnects, observed = self.config.commissioning.observer_reconnect_initial_seconds, 0, 0
        self.commissioner.catch_up()
        while not self._stop.is_set() and (deadline is None or asyncio.get_running_loop().time() < deadline):
            try:
                async with websockets.connect(self.config.source.websocket_url, ping_interval=20, ping_timeout=20) as socket:
                    await socket.send(json.dumps({"method": "subscribe", "subscription": {"type": "allMids"}}))
                    for wallet in wallets:
                        await socket.send(json.dumps({"method": "subscribe", "subscription": {"type": "userFills", "user": wallet}}))
                    self.commissioner.repository.set_watermark("public_observer", _iso(_now()), updated_at=_iso(_now()), status="HEALTHY", details={"wallets": len(wallets), "reconnects": reconnects})
                    backoff = self.config.commissioning.observer_reconnect_initial_seconds
                    while not self._stop.is_set() and (deadline is None or asyncio.get_running_loop().time() < deadline):
                        try:
                            message = await asyncio.wait_for(socket.recv(), timeout=self.config.source.stale_after_seconds)
                        except TimeoutError:
                            self.commissioner.repository.set_watermark("public_observer", _iso(_now()), updated_at=_iso(_now()), status="SOURCE_STALE", details={"reason": "websocket receive timeout"})
                            await socket.ping()
                            continue
                        decoded = json.loads(message)
                        if not isinstance(decoded, Mapping):
                            continue
                        if decoded.get("channel") == "allMids" and isinstance(decoded.get("data"), Mapping):
                            observed += self.ingest_market(decoded["data"])
                        elif decoded.get("channel") == "userFills" and isinstance(decoded.get("data"), Mapping):
                            data = decoded["data"]
                            wallet = str(data.get("user") or "").lower()
                            for fill in data.get("fills", ()) if isinstance(data.get("fills"), list) else ():
                                if isinstance(fill, Mapping):
                                    observed += int(self.ingest_wallet_fill(wallet, fill))
            except Exception as exc:
                reconnects += 1
                self.commissioner.repository.set_watermark("public_observer", _iso(_now()), updated_at=_iso(_now()), status="RECONNECTING", details={"error_class": type(exc).__name__, "reconnects": reconnects})
                await asyncio.sleep(backoff)
                backoff = min(self.config.commissioning.observer_reconnect_max_seconds, backoff * 2)
                self.commissioner.catch_up()
        self.commissioner.repository.set_watermark("public_observer", _iso(_now()), updated_at=_iso(_now()), status="STOPPED", details={"observations": observed, "reconnects": reconnects})
        return {"state": "STOPPED", "observations": observed, "reconnects": reconnects, "paper_only": True}
