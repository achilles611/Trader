"""Offline-only recovery analysis over an immutable Lane II SQLite snapshot.

This module is deliberately separate from live acquisition.  It copies an
already-verified snapshot into the caller's recovery workspace, protects the
process from network I/O, and invokes the existing reconstruction, follower
replay, and Phase-B scoring implementation against that derived database.
"""

from __future__ import annotations

import hashlib
import json
import socket
import sqlite3
import time
from contextlib import contextmanager
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from .analysis import CandidateAnalysisPipeline, _config_fingerprint
from .config import CopyTradeConfig
from .market import MarketPrice
from .reconstruction import SourcePositionContinuityError
from .service import CopyTradeService
from .storage import CopyTradeDatabase


SAVED_EVIDENCE_SCHEMA = "beelzebub-lane-ii-saved-evidence-evaluation-v1"


class SavedEvidenceOnlyError(RuntimeError):
    """Raised when an offline recovery invariant would be violated."""


class _NoNetworkMarketData:
    """A historical-price seam that records no substitute and cannot fetch."""

    def historical_price(self, symbol: str, timestamp: object) -> MarketPrice | None:
        del symbol, timestamp
        return None

    def current_price(self, symbol: str) -> MarketPrice:
        raise SavedEvidenceOnlyError(f"Network disabled: current market price requested for {symbol}.")

    def current_order_book(self, symbol: str) -> object:
        raise SavedEvidenceOnlyError(f"Network disabled: order book requested for {symbol}.")


class _NoNetworkAdapter:
    """Fail closed if a service path tries to acquire after construction."""

    def __getattr__(self, name: str) -> object:
        raise SavedEvidenceOnlyError(f"Network acquisition disabled: adapter.{name} was requested.")


class _NetworkGuard:
    """Process-local socket guard used as a second line of defense."""

    def __init__(self) -> None:
        self.blocked_attempts = 0
        self._socket_connect: object | None = None
        self._create_connection: object | None = None

    def _blocked_connect(self, *_args: object, **_kwargs: object) -> None:
        self.blocked_attempts += 1
        raise SavedEvidenceOnlyError("Network disabled for saved-evidence-only analysis.")

    @contextmanager
    def active(self) -> Iterator["_NetworkGuard"]:
        self._socket_connect = socket.socket.connect
        self._create_connection = socket.create_connection
        socket.socket.connect = self._blocked_connect  # type: ignore[method-assign]
        socket.create_connection = self._blocked_connect  # type: ignore[assignment]
        try:
            yield self
        finally:
            socket.socket.connect = self._socket_connect  # type: ignore[method-assign]
            socket.create_connection = self._create_connection  # type: ignore[assignment]


def evaluate_saved_evidence(
    *,
    config: CopyTradeConfig,
    snapshot_database: str | Path,
    output_directory: str | Path,
    original_run_id: str,
    replay_completed: bool = False,
    workspace: str | Path | None = None,
) -> dict[str, Any]:
    """Evaluate one frozen candidate run without any acquisition capability.

    The source snapshot is opened ``mode=ro&immutable=1`` only.  All mutable
    reconstruction output is confined to a SQLite backup made under
    ``output_directory``.  This function intentionally never resumes or edits
    the original run, so a still-running acquisition worker retains ownership
    of its lifecycle rows.
    """

    root = Path(workspace or Path.cwd()).resolve()
    snapshot = Path(snapshot_database).resolve()
    output = Path(output_directory).resolve()
    _require_within(snapshot, root, "snapshot database")
    _require_within(output, root, "output directory")
    if not snapshot.is_file():
        raise FileNotFoundError(f"Saved-evidence snapshot not found: {snapshot}")
    if output.exists():
        raise SavedEvidenceOnlyError(f"Refusing to overwrite recovery output: {output}")

    source_sha_before = _sha256(snapshot)
    source_run, configuration, run_wallets, coverage_records, saved_analyses = _load_saved_inputs(snapshot, original_run_id)
    expected_fingerprint = str(configuration.get("config_fingerprint") or "")
    actual_fingerprint = _config_fingerprint(config.research_snapshot())
    if not expected_fingerprint or expected_fingerprint != actual_fingerprint:
        raise SavedEvidenceOnlyError(
            "Saved-evidence analysis requires the original scoring configuration fingerprint; "
            f"saved={expected_fingerprint or 'missing'} current={actual_fingerprint}."
        )
    manifest = list(configuration.get("candidate_manifest") or ())
    if not manifest:
        raise SavedEvidenceOnlyError("Original analysis run has no immutable candidate manifest.")
    window = dict(configuration.get("analysis_window") or {})
    required_start, required_end = window.get("required_start"), window.get("required_end")
    if not required_start or not required_end:
        raise SavedEvidenceOnlyError("Original analysis run has no immutable analysis window.")

    output.mkdir(parents=True)
    derived_database = output / "derived-analysis.sqlite3"
    _sqlite_backup(snapshot, derived_database, deadline_seconds=120)
    _copy_saved_market_evidence(derived_database, original_run_id, _recovery_run_id(original_run_id))

    original_run_rows = _original_run_rows(derived_database, original_run_id)
    runtime_config = replace(
        config,
        artifacts=replace(config.artifacts, database_path=derived_database),
        scientific_execution=replace(config.scientific_execution, enabled=False),
        scientific_worker=replace(config.scientific_worker, enabled=False),
    )
    service = CopyTradeService(runtime_config, database=CopyTradeDatabase(derived_database))
    # Reconstruction/backtesting use the original Phase-B policy.  The
    # alternate runtime config above only routes service-owned local state to
    # the recovery workspace and prevents unrelated science-worker setup.
    service.config = config
    service.adapter = _NoNetworkAdapter()  # type: ignore[assignment]
    pipeline = CandidateAnalysisPipeline(service, market_data_factory=_NoNetworkMarketData)
    recovery_run_id = _recovery_run_id(original_run_id)
    by_wallet_status = {str(row["wallet"]).lower(): row for row in run_wallets}
    by_wallet_coverage = {wallet: rows for wallet, rows in coverage_records.items()}
    by_wallet_analysis = {str(row["wallet"]).lower(): row for row in saved_analyses}

    guard = _NetworkGuard()
    wallet_results: list[dict[str, Any]] = []
    with guard.active():
        for entry in manifest:
            wallet = str(entry.get("wallet") or "").lower()
            if not wallet:
                raise SavedEvidenceOnlyError("Candidate manifest contains an empty wallet.")
            run_wallet = by_wallet_status.get(wallet, {})
            evidence = _saved_evidence_summary(derived_database, wallet)
            coverage = service.database.analysis_window_coverage(wallet, required_start, required_end)
            result = _base_wallet_result(
                wallet=wallet,
                manifest_entry=entry,
                run_wallet=run_wallet,
                evidence=evidence,
                coverage=coverage,
                requested_start=str(required_start),
                requested_end=str(required_end),
                minimum_history_days=config.candidates.history_days_min,
            )
            result["available_history"]["source_coverage_records"] = by_wallet_coverage.get(wallet, [])
            coverage_state = str(coverage.get("coverage_state") or "UNPROVEN")
            stage = str(run_wallet.get("stage") or "")
            state = str(run_wallet.get("status") or "")
            if coverage_state == "KNOWN_INCOMPLETE":
                result.update(_quarantined_result())
            elif stage == "backfill" and state == "started":
                # A raw-fill count is deliberately not considered a completed
                # acquisition transaction.  Only the saved lifecycle record
                # can move a candidate out of this pending state.
                result.update(_pending_result())
            elif stage == "analysis" and state == "completed":
                if replay_completed:
                    try:
                        result.update(_replay_saved_wallet(
                            pipeline, wallet, coverage, recovery_run_id, expected_fingerprint, required_start, required_end,
                        ))
                    except SourcePositionContinuityError as error:
                        result.update(_integrity_unresolved_result(error))
                else:
                    result.update(_completed_snapshot_result(by_wallet_analysis.get(wallet), run_wallet, result))
            elif stage == "backfill" and state == "completed":
                try:
                    result.update(_replay_saved_wallet(
                        pipeline, wallet, coverage, recovery_run_id, expected_fingerprint,
                        required_start, required_end,
                    ))
                except SourcePositionContinuityError as error:
                    result.update(_integrity_unresolved_result(error))
            else:
                result.update({
                    "assessment": "PENDING_SAVED_ACQUISITION",
                    "assessment_reasons": ["no_completed_saved_backfill_lifecycle_record"],
                    "additional_evidence_required": ["completed backfill lifecycle record and coverage record"],
                    "follower": _unavailable_follower("saved_acquisition_not_completed"),
                    "campaigns": {"status": "not_reconstructed", "closed_campaigns": None},
                })
            wallet_results.append(result)

    preserved_rows = _original_run_rows(derived_database, original_run_id)
    if preserved_rows != original_run_rows:
        raise SavedEvidenceOnlyError("Recovery analysis attempted to rewrite the original analysis run rows.")
    source_sha_after = _sha256(snapshot)
    if source_sha_after != source_sha_before:
        raise SavedEvidenceOnlyError("Saved-evidence snapshot changed during offline analysis.")

    report = {
        "schema": SAVED_EVIDENCE_SCHEMA,
        "mode": "SAVED_EVIDENCE_ONLY",
        "replay_completed": replay_completed,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "authority": {
            "cohort_selection_completed": False,
            "activated_wallets": [],
            "trading_enabled": False,
            "network_acquisition_enabled": False,
        },
        "input_snapshot": {
            "database": str(snapshot),
            "sha256_before": source_sha_before,
            "sha256_after": source_sha_after,
            "unchanged": source_sha_before == source_sha_after,
            "read_mode": "sqlite mode=ro&immutable=1",
        },
        "derived_analysis_database": str(derived_database),
        "network_guard": {"enabled": True, "blocked_attempts": guard.blocked_attempts, "network_requests": 0},
        "original_run": {
            "record": source_run,
            "run_id": original_run_id,
            "configuration_fingerprint": expected_fingerprint,
            "analysis_window": window,
            "candidate_manifest": manifest,
            "run_wallet_rows": run_wallets,
            "coverage_records": coverage_records,
        },
        "requirements": {
            "hard_minimum_history_days": config.candidates.history_days_min,
            "preferred_history_days": config.candidates.history_days_preferred,
            "requested_analysis_window_days": config.analysis.history_days,
            "history_measurement": (
                "Existing score_candidate gates on TraderMetrics.history_days, calculated as elapsed time from the first "
                "reconstructed campaign opening to the latest campaign close/open. This report separately shows raw-fill "
                "observed span and never converts unobserved gaps into zero returns or inactive observations."
            ),
            "scoring_policy": "phase_b_suitability_v3",
        },
        "wallet_results": wallet_results,
        "summary": {
            "wallet_count": len(wallet_results),
            "replayed_saved_evidence_wallets": sum(item["assessment"] == "INSUFFICIENT_EVIDENCE_TO_QUALIFY" and item.get("replay_performed") for item in wallet_results),
            "integrity_unresolved_wallets": sum(item["assessment"] == "INTEGRITY_UNRESOLVED_SOURCE_POSITION_CONTINUITY" for item in wallet_results),
            "pending_wallets": sum(item["assessment"] == "PENDING_SAVED_ACQUISITION" for item in wallet_results),
            "quarantined_wallets": sum(item["assessment"] == "QUARANTINED_KNOWN_INCOMPLETE" for item in wallet_results),
            "cohort_selection": "NOT_RUN_PROVISIONAL_INDIVIDUAL_ASSESSMENTS_ONLY",
        },
    }
    _write_json(output / "saved-evidence-evaluation.json", report)
    return {**report, "report_path": str((output / "saved-evidence-evaluation.json").resolve())}


def _integrity_unresolved_result(error: SourcePositionContinuityError) -> dict[str, Any]:
    """Preserve an integrity boundary without mislabelling trader performance."""
    return {
        "assessment": "INTEGRITY_UNRESOLVED_SOURCE_POSITION_CONTINUITY",
        "assessment_reasons": [error.reason],
        "replay_performed": False,
        "integrity": {
            "status": "unresolved_source_position_continuity",
            "reason": error.reason,
            "wallet": error.wallet,
            "symbol": error.symbol,
            "timestamp": error.timestamp.isoformat(),
            "detail": error.detail,
            "affected_metrics": [
                "campaign_count", "pnl", "drawdown", "concentration", "copyability", "liquidation_frequency",
            ],
            "operator_note": "Saved raw evidence is retained; this is not a measured poor-performance finding.",
        },
        "campaigns": {"status": "integrity_unresolved", "closed_campaigns": None},
        "follower": _unavailable_follower("source_position_continuity_unresolved"),
        "additional_evidence_required": ["source-proven position continuation or a separately validated normalizer"],
    }


def _replay_saved_wallet(
    pipeline: CandidateAnalysisPipeline,
    wallet: str,
    coverage: dict[str, Any],
    recovery_run_id: str,
    fingerprint: str,
    required_start: object,
    required_end: object,
) -> dict[str, Any]:
    analysis = pipeline._analyze_wallet(  # Existing Phase-B reconstruction/replay/scoring; no acquisition callback is involved.
        wallet, coverage, run_id=recovery_run_id, config_fingerprint=fingerprint,
        required_start=required_start, required_end=required_end,
    )
    target = dict(analysis.get("target_metrics") or {})
    activity = dict(target.get("activity") or {})
    score = dict(analysis.get("score") or {})
    coverage_state = str(coverage.get("coverage_state") or "UNPROVEN")
    # A canonical score remains visible without silently converting incomplete
    # provenance into a qualification or a cohort-selection decision.
    if coverage_state != "PROVEN_COMPLETE":
        assessment = "INSUFFICIENT_EVIDENCE_TO_QUALIFY"
        assessment_reasons = ["coverage_not_proven_for_requested_window", *list(score.get("reasons") or ())]
    elif bool(analysis.get("eligible")):
        assessment = "CANONICAL_SCORE_ELIGIBLE_PROVISIONAL_ONLY"
        assessment_reasons = ["individual_score_only_no_cohort_selection"]
    else:
        assessment = "MEASURED_POOR_PERFORMANCE"
        assessment_reasons = list(score.get("hard_gates") or score.get("reasons") or ())
    follower = dict(analysis.get("follower") or {})
    market_missing = _market_missing(follower)
    return {
        "assessment": assessment,
        "assessment_reasons": sorted(set(assessment_reasons)),
        "replay_performed": True,
        "campaigns": {
            "status": "reconstructed_saved_evidence",
            "closed_campaigns": activity.get("completed_campaigns"),
            "all_campaigns": activity.get("campaigns"),
        },
        "follower": {
            **follower,
            "modeled_costs": {"fees": follower.get("fees"), "configured_slippage_scenarios": True},
            "omitted_or_missing": {
                "historical_market_evidence": "missing" if market_missing else "saved_evidence_used",
                "latency_evidence": dict(analysis.get("stress_tests") or {}).get("latency", {}).get("status", "unavailable"),
                "funding": "not_modeled_by_existing_follower_replay",
            },
        },
        "canonical_score": score,
        "additional_evidence_required": _additional_evidence(coverage, market_missing=market_missing),
    }


def _base_wallet_result(
    *, wallet: str, manifest_entry: dict[str, Any], run_wallet: dict[str, Any], evidence: dict[str, Any],
    coverage: dict[str, Any], requested_start: str, requested_end: str, minimum_history_days: int,
) -> dict[str, Any]:
    observed_days = evidence.get("observed_span_days")
    history_short = observed_days is None or float(observed_days) < minimum_history_days
    return {
        "wallet": wallet,
        "manifest_source_timestamps": {
            "recent_activity_at": manifest_entry.get("recent_activity_at"),
            "first_observed_activity": ((manifest_entry.get("metadata") or {}).get("cheap_stats") or {}).get("first_observed_activity"),
            "last_observed_activity": ((manifest_entry.get("metadata") or {}).get("cheap_stats") or {}).get("last_observed_activity"),
        },
        "original_run_status": {key: run_wallet.get(key) for key in ("stage", "status", "attempts", "error", "updated_at")},
        "analysis_window": {"requested_start": requested_start, "requested_end": requested_end},
        "available_history": {
            **evidence,
            "minimum_history_days": minimum_history_days,
            "meets_minimum_by_observed_span": not history_short,
            "coverage_status": coverage,
            "observation_note": "Observed span is a timestamp envelope only; gaps are not treated as zero-return or inactive evidence.",
        },
    }


def _pending_result() -> dict[str, Any]:
    return {
        "assessment": "PENDING_SAVED_ACQUISITION",
        "assessment_reasons": ["backfill_lifecycle_status_started_not_completed"],
        "replay_performed": False,
        "campaigns": {"status": "not_reconstructed", "closed_campaigns": None},
        "follower": _unavailable_follower("saved_acquisition_pending"),
        "additional_evidence_required": ["completed backfill lifecycle record", "bounded coverage record for requested analysis window"],
    }


def _quarantined_result() -> dict[str, Any]:
    return {
        "assessment": "QUARANTINED_KNOWN_INCOMPLETE",
        "assessment_reasons": ["known_incomplete"],
        "replay_performed": False,
        "campaigns": {"status": "quarantined_not_reconstructed", "closed_campaigns": None},
        "follower": _unavailable_follower("known_incomplete_history"),
        "additional_evidence_required": ["complete, independently bounded source history replacing the known-incomplete interval"],
    }


def _completed_snapshot_result(
    saved_analysis: dict[str, Any] | None, run_wallet: dict[str, Any], result: dict[str, Any],
) -> dict[str, Any]:
    summary = dict((saved_analysis or {}).get("summary") or {})
    score = dict((run_wallet.get("payload") or {}).get("score") or summary.get("score") or {})
    target = dict(summary.get("target_metrics") or {})
    follower = dict(summary.get("follower") or {})
    market_missing = _market_missing(follower)
    if follower:
        follower = {
            **follower,
            "modeled_costs": {
                "fees": follower.get("fees"),
                "configured_slippage_scenarios": bool(summary.get("slippage_scenarios")),
            },
            "omitted_or_missing": {
                "historical_market_evidence": "missing" if market_missing else "saved_evidence_used",
                "latency_evidence": dict(summary.get("stress_tests") or {}).get("latency", {}).get("status", "unavailable"),
                "funding": "not_modeled_by_existing_follower_replay",
            },
        }
    return {
        "assessment": "INSUFFICIENT_EVIDENCE_TO_QUALIFY",
        "assessment_reasons": ["saved_analysis_completed_but_observed_history_or_coverage_is_insufficient", *list(score.get("reasons") or ())],
        "replay_performed": False,
        "campaigns": {
            "status": "saved_original_analysis_preserved_not_replayed",
            "closed_campaigns": dict(target.get("activity") or {}).get("completed_campaigns"),
            "all_campaigns": dict(target.get("activity") or {}).get("campaigns"),
        },
        "follower": follower or _unavailable_follower("no_saved_follower_summary"),
        "canonical_score": score,
        "additional_evidence_required": _additional_evidence(
            dict(result["available_history"]["coverage_status"]), market_missing=market_missing,
        ),
    }


def _additional_evidence(coverage: dict[str, Any], *, market_missing: bool) -> list[str]:
    required: list[str] = []
    if coverage.get("coverage_state") != "PROVEN_COMPLETE":
        required.append("continuous provenance for the original requested analysis window")
    if market_missing:
        required.append("historical executable-price evidence before latency/capture claims")
    required.append("funding evidence or an explicitly approved funding model")
    return required


def _market_missing(follower: dict[str, Any]) -> bool:
    return str(follower.get("latency_status") or "unavailable") == "unavailable"


def _unavailable_follower(reason: str) -> dict[str, Any]:
    return {
        "status": "unavailable",
        "reason": reason,
        "modeled_costs": {"fees": None, "configured_slippage_scenarios": False},
        "omitted_or_missing": {
            "historical_market_evidence": "missing", "latency_evidence": "unavailable",
            "funding": "not_modeled_by_existing_follower_replay",
        },
    }


def _load_saved_inputs(
    snapshot: Path, run_id: str,
) -> tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]], dict[str, list[dict[str, Any]]], list[dict[str, Any]]]:
    connection = _readonly(snapshot)
    try:
        run = connection.execute("SELECT * FROM copy_analysis_runs WHERE run_id=?", (run_id,)).fetchone()
        if run is None:
            raise SavedEvidenceOnlyError(f"Analysis run not found in snapshot: {run_id}")
        record = dict(run)
        configuration = _json(record.pop("configuration_json", "{}"))
        record["errors"] = _json(record.pop("errors_json", "[]"))
        run_wallets = []
        for row in connection.execute("SELECT * FROM copy_analysis_run_wallets WHERE run_id=? ORDER BY wallet", (run_id,)):
            item = dict(row)
            item["payload"] = _json(item.pop("payload_json", "{}"))
            run_wallets.append(item)
        wallet_list = [str(item.get("wallet") or "").lower() for item in configuration.get("candidate_manifest") or ()]
        coverages: dict[str, list[dict[str, Any]]] = {}
        for wallet in wallet_list:
            coverages[wallet] = [dict(row) for row in connection.execute(
                "SELECT requested_start, requested_end, earliest_observed_fill, latest_observed_fill, "
                "source_limit_detected, coverage_complete, coverage_quality, coverage_state "
                "FROM copy_backfill_coverage WHERE target_wallet=? ORDER BY coverage_id", (wallet,)
            )]
        analyses = []
        for wallet in wallet_list:
            row = connection.execute("SELECT wallet, lifecycle_status, last_run_id, started_at, completed_at, summary_json "
                                     "FROM copy_candidate_analyses WHERE wallet=?", (wallet,)).fetchone()
            if row is not None:
                item = dict(row)
                item["summary"] = _json(item.pop("summary_json", "{}"))
                analyses.append(item)
        return record, configuration, run_wallets, coverages, analyses
    finally:
        connection.close()


def _saved_evidence_summary(database: Path, wallet: str) -> dict[str, Any]:
    connection = _readonly(database)
    try:
        row = connection.execute(
            "SELECT COUNT(*) AS fill_count, MIN(event_timestamp) AS first_raw_fill, MAX(event_timestamp) AS last_raw_fill, "
            "MIN(ingestion_timestamp) AS first_ingested_at, MAX(ingestion_timestamp) AS last_ingested_at, "
            "COUNT(DISTINCT symbol) AS symbol_count FROM copy_raw_fills WHERE target_wallet=?", (wallet,)
        ).fetchone()
        result = dict(row)
        first, last = result.get("first_raw_fill"), result.get("last_raw_fill")
        if first and last:
            result["observed_span_days"] = max(0.0, (_as_datetime(last) - _as_datetime(first)).total_seconds() / 86_400)
        else:
            result["observed_span_days"] = None
        return result
    finally:
        connection.close()


def _copy_saved_market_evidence(database: Path, original_run_id: str, recovery_run_id: str) -> None:
    connection = sqlite3.connect(database)
    try:
        connection.execute(
            "INSERT OR IGNORE INTO copy_analysis_market_evidence "
            "(analysis_run_id, symbol, bucket_timestamp, price, source, quality, market_timestamp, requested_for_timestamp, resolution, recorded_at) "
            "SELECT ?, symbol, bucket_timestamp, price, source, quality, market_timestamp, requested_for_timestamp, resolution, recorded_at "
            "FROM copy_analysis_market_evidence WHERE analysis_run_id=?", (recovery_run_id, original_run_id),
        )
        connection.commit()
    finally:
        connection.close()


def _original_run_rows(database: Path, run_id: str) -> list[tuple[Any, ...]]:
    connection = _readonly(database)
    try:
        return [tuple(row) for row in connection.execute(
            "SELECT wallet, stage, status, attempts, error, payload_json, updated_at "
            "FROM copy_analysis_run_wallets WHERE run_id=? ORDER BY wallet", (run_id,)
        )]
    finally:
        connection.close()


def _sqlite_backup(source: Path, destination: Path, *, deadline_seconds: int) -> None:
    if destination.exists():
        raise SavedEvidenceOnlyError(f"Recovery analysis database already exists: {destination}")
    started = time.monotonic()
    source_connection = _readonly(source)
    destination_connection = sqlite3.connect(destination)
    try:
        def progress(_status: int, _remaining: int, _total: int) -> None:
            if time.monotonic() - started > deadline_seconds:
                raise SavedEvidenceOnlyError(f"SQLite backup exceeded {deadline_seconds} seconds.")
        source_connection.backup(destination_connection, pages=1024, progress=progress, sleep=0.05)
    finally:
        destination_connection.close()
        source_connection.close()


def _readonly(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(path.as_uri() + "?mode=ro&immutable=1", uri=True)
    connection.row_factory = sqlite3.Row
    return connection


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _write_json(path: Path, value: dict[str, Any]) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")


def _json(value: object) -> dict[str, Any] | list[Any]:
    if isinstance(value, (dict, list)):
        return value
    try:
        parsed = json.loads(str(value or "{}"))
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, (dict, list)) else {}


def _as_datetime(value: object) -> datetime:
    text = str(value).replace("Z", "+00:00")
    parsed = datetime.fromisoformat(text)
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _recovery_run_id(original_run_id: str) -> str:
    return f"saved_evidence_{original_run_id}"


def _require_within(path: Path, root: Path, label: str) -> None:
    try:
        path.relative_to(root)
    except ValueError as exc:
        raise SavedEvidenceOnlyError(f"{label} must be inside the isolated recovery workspace: {path}") from exc
