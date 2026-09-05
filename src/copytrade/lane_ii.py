"""Lane II public-cohort evidence and paper-only read model.

This module deliberately contains no exchange client, signer, secret lookup, or
order-submission path. Public ``/info`` evidence, local PAPER state, and
operator activation remain separate authority domains.
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from dataclasses import asdict, replace
from datetime import timedelta
from pathlib import Path
from typing import Any, Iterable

from .analysis import CandidateAnalysisPipeline, _config_fingerprint
from .control_center import CopyControlCenter
from .discovery import DiscoveryPipeline
from .hyperliquid import PUBLIC_BACKFILL_MAX_REQUESTS_PER_WALLET
from .models import CopySignal, DiscoveryObservation, RawFill, as_utc, jsonable, stable_id, utc_now
from .backtest import CopyTradeBacktester
from .paper import PaperExecutionEngine
from .service import CopyTradeService


COHORT_SCHEMA = "beelzebub-lane-ii-cohort-v1"
UNIVERSE_SCHEMA = "beelzebub-lane-ii-candidate-universe-v1"
RESEARCH_PASS_SCHEMA = "beelzebub-lane-ii-research-pass-v1"


class _ObservationProvider:
    source_name = "hyperliquid_public_user_fills"

    def __init__(self, observations: Iterable[DiscoveryObservation]) -> None:
        self._observations = tuple(observations)

    def discover(self, *, refresh: bool = False) -> Iterable[DiscoveryObservation]:
        del refresh
        yield from self._observations


def _json_sha256(value: object) -> str:
    payload = json.dumps(jsonable(value), sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _write_evidence(path: Path, value: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {**value, "payload_sha256": _json_sha256(value)}
    path.write_text(json.dumps(jsonable(payload), indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    return path


def _load_evidence(path: str | Path, *, schema: str) -> dict[str, Any]:
    """Load a self-digesting local evidence artifact without trusting its path."""
    resolved = Path(path).expanduser().resolve()
    try:
        value = json.loads(resolved.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ValueError(f"Unable to load Lane II evidence artifact: {resolved}") from exc
    if not isinstance(value, dict) or value.get("schema") != schema:
        raise ValueError(f"Unexpected Lane II evidence schema in {resolved}")
    digest = value.pop("payload_sha256", None)
    if not isinstance(digest, str) or digest != _json_sha256(value):
        raise ValueError(f"Lane II evidence digest mismatch: {resolved}")
    return value


def _retained_seed_rows(path: Path, limit: int) -> list[dict[str, Any]]:
    """Read the retained discovery database without initializing or migrating it."""
    resolved = path.expanduser().resolve()
    if not resolved.is_file():
        raise FileNotFoundError(f"Retained candidate database not found: {resolved}")
    uri = f"file:{resolved.as_posix()}?mode=ro&immutable=1"
    connection = sqlite3.connect(uri, uri=True)
    connection.row_factory = sqlite3.Row
    cursor = connection.cursor()
    try:
        tables = {str(row[0]) for row in cursor.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        if "copy_candidate_analyses" in tables:
            rows = [dict(row) for row in cursor.execute(
                """SELECT candidate.wallet, candidate.discovered_at, candidate.last_seen_at, candidate.recent_activity_at,
                          candidate.discovery_rank, candidate.source_score, candidate.source_count, candidate.metadata_json,
                          analysis.lifecycle_status AS prior_lifecycle_status
                     FROM copy_discovery_candidates AS candidate
                LEFT JOIN copy_candidate_analyses AS analysis ON analysis.wallet = candidate.wallet
                    WHERE COALESCE(analysis.lifecycle_status, '') NOT IN
                          ('prefilter_rejected', 'backfill_failed', 'quarantined')
                    ORDER BY COALESCE(candidate.source_score, 0) DESC,
                             COALESCE(candidate.recent_activity_at, candidate.last_seen_at) DESC,
                             candidate.wallet ASC
                    LIMIT ?""",
                (max(1, int(limit)),),
            ).fetchall()]
        else:
            rows = [dict(row) for row in cursor.execute(
                """SELECT wallet, discovered_at, last_seen_at, recent_activity_at,
                          discovery_rank, source_score, source_count, metadata_json,
                          NULL AS prior_lifecycle_status
                     FROM copy_discovery_candidates
                    ORDER BY COALESCE(source_score, 0) DESC,
                             COALESCE(recent_activity_at, last_seen_at) DESC,
                             wallet ASC
                    LIMIT ?""",
                (max(1, int(limit)),),
            ).fetchall()]
    finally:
        cursor.close()
        connection.close()
    return [
        {
            **row,
            "metadata": json.loads(str(row["metadata_json"] or "{}")),
        }
        for row in rows
    ]


def freeze_lane_ii_research_pass(
    service: CopyTradeService,
    *,
    retained_database: str | Path,
    output: str | Path,
    seed_limit: int = 50,
    planned_deep_limit: int = 12,
    target_count: int = 7,
    public_request_budget: int | None = None,
) -> dict[str, Any]:
    """Pin a bounded research batch before public responses can influence it.

    This is deliberately an evidence artifact rather than an operator action:
    it records the current policy fingerprint, fixed cutoff, exact lead order,
    and a no-spend request budget.  The retained database remains read-only.
    """
    if service.config.mode != "paper" or service.config.live_enabled:
        raise ValueError("Lane II research pass requires a paper-only configuration.")
    if seed_limit < 1 or planned_deep_limit < 0 or target_count < 5 or target_count > 7:
        raise ValueError("Invalid bounded Lane II research-pass limits.")
    if public_request_budget is not None and public_request_budget < 1:
        raise ValueError("Lane II public request budget must be positive when specified.")
    retained_path = Path(retained_database).expanduser().resolve()
    seeds = _retained_seed_rows(retained_path, seed_limit)
    cutoff = utc_now()
    payload = {
        "schema": RESEARCH_PASS_SCHEMA,
        "created_at": cutoff.isoformat(),
        "data_cutoff": cutoff.isoformat(),
        "retained_database": str(retained_path),
        "retained_database_mode": "sqlite mode=ro",
        "retained_lead_count": len(seeds),
        "configuration_fingerprint": _config_fingerprint(service.config.research_snapshot()),
        "policy": {
            "strategy_id": service.config.paper_strategy.strategy_id,
            "strategy_version": service.config.paper_strategy.version,
            "minimum_history_days": service.config.candidates.history_days_min,
            "preferred_history_days": service.config.candidates.history_days_preferred,
            "target_count": target_count,
            "planned_deep_limit": planned_deep_limit,
            "public_request_budget": public_request_budget,
            "new_monetary_spend_authorized_usd": 0,
            "paper_execution_authority": False,
            "live_execution_authority": False,
        },
        "selection_rule": {
            "description": (
                "Read-only retained discovery leads sorted by existing source score, recent activity, and wallet; "
                "prior terminal prefilter/backfill/quarantine outcomes are excluded from this batch."
            ),
            "qualification_rule": "unchanged_phase_b_policy_only",
        },
        "seeds": [
            {
                "seed_rank": rank,
                "wallet": str(seed["wallet"]).lower(),
                "retained_source_score": seed.get("source_score"),
                "retained_recent_activity_at": seed.get("recent_activity_at"),
                "prior_lifecycle_status": seed.get("prior_lifecycle_status"),
            }
            for rank, seed in enumerate(seeds, 1)
        ],
    }
    path = _write_evidence(Path(output), payload)
    return {**payload, "research_pass_path": str(path.resolve())}


def freeze_lane_ii_deep_batch(
    service: CopyTradeService,
    *,
    parent_research_pass: str | Path,
    candidate_universe: str | Path,
    wallets: Iterable[str],
    output: str | Path,
) -> dict[str, Any]:
    """Freeze an exact Phase-B subset from already completed public screening.

    This creates a child of the original retained-universe pass.  The explicit
    wallet ordering and the source screen are both self-digesting artifacts;
    later public acquisition therefore cannot silently replace the planned
    cohort with a higher-ranked mutable database row.
    """
    if service.config.mode != "paper" or service.config.live_enabled:
        raise ValueError("Lane II deep research requires a paper-only configuration.")
    parent_path = Path(parent_research_pass).expanduser().resolve()
    parent = _load_evidence(parent_path, schema=RESEARCH_PASS_SCHEMA)
    universe_path = Path(candidate_universe).expanduser().resolve()
    universe = _load_evidence(universe_path, schema=UNIVERSE_SCHEMA)
    fingerprint = _config_fingerprint(service.config.research_snapshot())
    if parent.get("configuration_fingerprint") != fingerprint:
        raise ValueError("Parent Lane II research pass has a stale configuration fingerprint.")
    frozen_in_universe = universe.get("frozen_research_pass")
    if not frozen_in_universe or Path(str(frozen_in_universe)).expanduser().resolve() != parent_path:
        raise ValueError("Candidate universe is not cryptographically linked to the supplied parent research pass.")
    normalized = tuple(dict.fromkeys(str(wallet).strip().lower() for wallet in wallets if str(wallet).strip()))
    planned_limit = int((parent.get("policy") or {}).get("planned_deep_limit") or 0)
    if not normalized or len(normalized) > planned_limit:
        raise ValueError("Deep-batch wallet count must be between one and the parent planned deep limit.")
    parent_seeds = {str(item["wallet"]).lower(): item for item in list(parent.get("seeds") or ())}
    screen_rows = {str(item["wallet"]).lower(): item for item in list(universe.get("candidates") or ())}
    missing = [wallet for wallet in normalized if wallet not in parent_seeds or wallet not in screen_rows]
    if missing:
        raise ValueError(f"Deep-batch wallet(s) are absent from the frozen parent screen: {', '.join(missing)}")
    unavailable = [wallet for wallet in normalized if screen_rows[wallet].get("fresh_public_status") != "AVAILABLE"]
    if unavailable:
        raise ValueError(f"Deep-batch wallet(s) lack available fresh public evidence: {', '.join(unavailable)}")
    cutoff = utc_now()
    parent_policy = dict(parent.get("policy") or {})
    target_count = int(parent_policy.get("target_count") or 7)
    # One fresh ``meta`` response plus one current fills response per fixed
    # wallet.  Deeper acquisition is separately bounded in configuration.
    request_budget = len(normalized) + 1
    payload = {
        "schema": RESEARCH_PASS_SCHEMA,
        "pass_kind": "deep_batch",
        "created_at": cutoff.isoformat(),
        "data_cutoff": cutoff.isoformat(),
        "retained_database": parent["retained_database"],
        "retained_database_mode": parent.get("retained_database_mode", "sqlite mode=ro"),
        "configuration_fingerprint": fingerprint,
        "parent_research_pass": str(parent_path),
        "parent_research_pass_payload_sha256": _json_sha256(parent),
        "candidate_universe": str(universe_path),
        "candidate_universe_payload_sha256": _json_sha256(universe),
        "policy": {
            **parent_policy,
            "planned_deep_limit": len(normalized),
            "public_request_budget": request_budget,
            "backfill_max_requests_per_wallet": PUBLIC_BACKFILL_MAX_REQUESTS_PER_WALLET,
            "new_monetary_spend_authorized_usd": 0,
            "paper_execution_authority": False,
            "live_execution_authority": False,
        },
        "selection_rule": {
            "description": "Exact wallet subset selected from the completed parent public screen by recorded observed-span and diversity triage; Phase B qualification policy is unchanged.",
            "qualification_rule": "unchanged_phase_b_policy_only",
            "target_count": target_count,
        },
        "seeds": [
            {
                "seed_rank": rank,
                "wallet": wallet,
                "retained_source_score": parent_seeds[wallet].get("retained_source_score"),
                "retained_recent_activity_at": parent_seeds[wallet].get("retained_recent_activity_at"),
                "prior_lifecycle_status": parent_seeds[wallet].get("prior_lifecycle_status"),
                "screening_evidence": {
                    "fresh_public_status": screen_rows[wallet].get("fresh_public_status"),
                    "fresh_public_fill_count": screen_rows[wallet].get("fresh_public_fill_count"),
                    "oldest_public_fill_at": screen_rows[wallet].get("oldest_public_fill_at"),
                    "newest_public_fill_at": screen_rows[wallet].get("newest_public_fill_at"),
                    "distinct_symbols": screen_rows[wallet].get("distinct_symbols"),
                    "requests_consumed": screen_rows[wallet].get("requests_consumed"),
                },
            }
            for rank, wallet in enumerate(normalized, 1)
        ],
    }
    path = _write_evidence(Path(output), payload)
    return {**payload, "research_pass_path": str(path.resolve())}


def _fill_observation(fill: RawFill, *, rank: int, retained_score: float | None) -> DiscoveryObservation:
    return DiscoveryObservation(
        wallet=fill.target_wallet,
        source="hyperliquid_public_user_fills",
        observed_at=utc_now(),
        recent_activity_at=fill.event_timestamp,
        discovery_rank=rank,
        source_score=fill.notional,
        metadata={
            "coin": fill.symbol,
            "public_endpoint": "userFills",
            "retained_source_score": retained_score,
        },
        raw_evidence={
            "event_id": fill.event_id,
            "coin": fill.symbol,
            "side": fill.side,
            "px": fill.price,
            "sz": fill.base_quantity,
            "time": fill.event_timestamp.isoformat(),
            "closed_pnl": fill.source_closed_pnl,
            "liquidation": fill.is_liquidation,
        },
        evidence_id=fill.event_id,
    )


def refresh_public_cohort(
    service: CopyTradeService,
    *,
    retained_database: str | Path,
    output_directory: str | Path,
    seed_limit: int = 24,
    analysis_limit: int = 10,
    target_count: int = 7,
    frozen_research_pass: str | Path | None = None,
    include_public_account_state: bool = False,
    public_request_budget: int | None = None,
) -> dict[str, Any]:
    """Refresh the current public universe and run the canonical Phase B path.

    The retained database supplies candidate *leads* only. Every registered
    observation in this run comes from a fresh unauthenticated ``userFills``
    response, and Phase B independently backfills/reconstructs those wallets.
    """
    if service.config.mode != "paper" or service.config.live_enabled:
        raise ValueError("Lane II cohort refresh requires a paper-only configuration.")
    if analysis_limit < 0:
        raise ValueError("Lane II analysis limit must not be negative.")
    research_pass: dict[str, Any] | None = None
    if frozen_research_pass is not None:
        research_pass = _load_evidence(frozen_research_pass, schema=RESEARCH_PASS_SCHEMA)
        fingerprint = _config_fingerprint(service.config.research_snapshot())
        if research_pass.get("configuration_fingerprint") != fingerprint:
            raise ValueError("Frozen Lane II research pass has a stale configuration fingerprint.")
        retained_path = Path(str(research_pass["retained_database"])).expanduser().resolve()
        seeds = [
            {
                "wallet": str(seed["wallet"]).lower(),
                "source_score": seed.get("retained_source_score"),
                "recent_activity_at": seed.get("retained_recent_activity_at"),
                "prior_lifecycle_status": seed.get("prior_lifecycle_status"),
                "metadata": {},
            }
            for seed in list(research_pass.get("seeds") or ())
        ]
        if not seeds:
            raise ValueError("Frozen Lane II research pass has no candidate seeds.")
        planned_budget = (research_pass.get("policy") or {}).get("public_request_budget")
        if public_request_budget is not None and planned_budget is not None and public_request_budget != planned_budget:
            raise ValueError("Public request budget differs from the frozen Lane II research pass.")
        public_request_budget = planned_budget if planned_budget is not None else public_request_budget
    else:
        retained_path = Path(retained_database).expanduser().resolve()
        seeds = _retained_seed_rows(retained_path, seed_limit)
    if public_request_budget is not None and int(public_request_budget) < 1:
        raise ValueError("Lane II public request budget must be positive when specified.")
    output_root = Path(output_directory)
    started_at = utc_now()
    run_stamp = started_at.strftime("%Y%m%dT%H%M%SZ")
    progress_path = output_root / f"screen-progress-{run_stamp}.json"

    def write_progress(state: str, *, completed_wallets: int, requests: int) -> None:
        _write_evidence(progress_path, {
            "schema": "beelzebub-lane-ii-screen-progress-v1",
            "state": state,
            "started_at": started_at.isoformat(),
            "updated_at": utc_now().isoformat(),
            "frozen_research_pass": str(Path(frozen_research_pass).expanduser().resolve()) if frozen_research_pass else None,
            "candidate_count": len(seeds),
            "completed_wallets": completed_wallets,
            "available": sum(item["fresh_public_status"] == "AVAILABLE" for item in public_rows),
            "empty": sum(item["fresh_public_status"] == "EMPTY" for item in public_rows),
            "errors": sum(item["fresh_public_status"] == "ERROR" for item in public_rows),
            "deferred": sum(item["fresh_public_status"] == "DEFERRED_REQUEST_BUDGET" for item in public_rows),
            "requests_consumed": requests,
            "request_budget": public_request_budget,
            "last_useful_progress_at": utc_now().isoformat() if completed_wallets else None,
        })

    public_rows: list[dict[str, Any]] = []
    write_progress("STARTING", completed_wallets=0, requests=0)
    instrument_metadata = service.refresh_instrument_metadata()
    requests_consumed = 1  # ``meta`` inside refresh_instrument_metadata.
    budget = int(public_request_budget) if public_request_budget is not None else None
    if budget is not None and requests_consumed > budget:
        raise ValueError("Frozen Lane II request budget cannot fund required instrument metadata.")
    observations: list[DiscoveryObservation] = []
    for rank, seed in enumerate(seeds, 1):
        wallet = str(seed["wallet"]).lower()
        row: dict[str, Any] = {
            "wallet": wallet,
            "retained_rank": rank,
            "retained_source_score": seed.get("source_score"),
            "retained_metadata": seed.get("metadata", {}),
            "prior_lifecycle_status": seed.get("prior_lifecycle_status"),
            "fresh_public_status": "NOT_REQUESTED",
            "fresh_public_fill_count": None,
            "oldest_public_fill_at": None,
            "newest_public_fill_at": None,
            "distinct_symbols": None,
            "symbols": [],
            "error": None,
            "portfolio": {"status": "NOT_REQUESTED"},
            "current_account": {"status": "NOT_REQUESTED"},
            "requests_consumed": 0,
        }
        if budget is not None and requests_consumed >= budget:
            row["fresh_public_status"] = "DEFERRED_REQUEST_BUDGET"
            row["error"] = "public_request_budget_exhausted"
            public_rows.append(row)
            write_progress("RUNNING", completed_wallets=len(public_rows), requests=requests_consumed)
            continue
        try:
            requests_consumed += 1
            row["requests_consumed"] += 1
            fills = service.adapter.fetch_user_fills(wallet, aggregate_by_time=False)
            newest = max((item.event_timestamp for item in fills), default=None)
            oldest = min((item.event_timestamp for item in fills), default=None)
            symbols = sorted({item.symbol for item in fills})
            observations.extend(
                _fill_observation(item, rank=rank, retained_score=seed.get("source_score"))
                for item in fills
            )
            row.update({
                "fresh_public_fill_count": len(fills),
                "oldest_public_fill_at": oldest.isoformat() if oldest else None,
                "newest_public_fill_at": newest.isoformat() if newest else None,
                "distinct_symbols": len(symbols),
                "symbols": symbols,
                "fresh_public_status": "AVAILABLE" if fills else "EMPTY",
            })
        except Exception as exc:
            row.update({"fresh_public_status": "ERROR", "error": f"{type(exc).__name__}: {exc}"})
        if include_public_account_state and (budget is None or requests_consumed < budget):
            try:
                requests_consumed += 1
                row["requests_consumed"] += 1
                portfolio = service.adapter.fetch_portfolio(wallet)
                row["portfolio"] = {
                    "status": "AVAILABLE",
                    "response_type": type(portfolio).__name__,
                    "response_digest": _json_sha256(portfolio),
                    "series_count": len(portfolio) if isinstance(portfolio, list) else None,
                }
            except Exception as exc:
                row["portfolio"] = {"status": "ERROR", "error": f"{type(exc).__name__}: {exc}"}
        elif include_public_account_state:
            row["portfolio"] = {"status": "DEFERRED_REQUEST_BUDGET"}
        if include_public_account_state and (budget is None or requests_consumed < budget):
            try:
                requests_consumed += 1
                row["requests_consumed"] += 1
                account = service.adapter.fetch_clearinghouse_state(wallet)
                row["current_account"] = {
                    "status": "AVAILABLE",
                    "account_value": account.account_value,
                    "withdrawable": account.withdrawable,
                    "total_notional_position": account.total_notional_position,
                    "open_position_count": len(account.positions.get("asset_positions") or ()),
                }
            except Exception as exc:
                row["current_account"] = {"status": "ERROR", "error": f"{type(exc).__name__}: {exc}"}
        elif include_public_account_state:
            row["current_account"] = {"status": "DEFERRED_REQUEST_BUDGET"}
        public_rows.append(row)
        write_progress("RUNNING", completed_wallets=len(public_rows), requests=requests_consumed)

    cutoff = utc_now()
    universe = {
        "schema": UNIVERSE_SCHEMA,
        "started_at": started_at.isoformat(),
        "data_cutoff": cutoff.isoformat(),
        "source": {
            "retained_database": str(retained_path.expanduser().resolve()),
            "retained_database_mode": "read-only",
            "fresh_endpoint": service.config.source.info_url,
            "fresh_request_type": "userFills",
            "documented_response_limit": 2000,
            "documented_retention_limit": 10000,
        },
        "instrument_metadata": instrument_metadata,
        "seed_limit": seed_limit,
        "frozen_research_pass": str(Path(frozen_research_pass).expanduser().resolve()) if frozen_research_pass else None,
        "screening": {
            "public_account_state_requested": include_public_account_state,
            "request_budget": budget,
            "requests_consumed": requests_consumed,
            "deferred_by_budget": sum(item["fresh_public_status"] == "DEFERRED_REQUEST_BUDGET" for item in public_rows),
            "available": sum(item["fresh_public_status"] == "AVAILABLE" for item in public_rows),
            "empty": sum(item["fresh_public_status"] == "EMPTY" for item in public_rows),
            "errors": sum(item["fresh_public_status"] == "ERROR" for item in public_rows),
        },
        "fresh_observations": len(observations),
        "candidates": public_rows,
    }
    universe_path = _write_evidence(output_root / f"candidate-universe-{run_stamp}.json", universe)
    _write_evidence(output_root / "latest-candidate-universe.json", universe)

    if observations:
        discovery = DiscoveryPipeline(service.database).run(
            _ObservationProvider(observations),
            limit=max(1, len(seeds)),
            min_activity=max(1, service.config.analysis.min_discovery_activity),
            refresh=True,
            max_activity_age=timedelta(days=service.config.candidates.activity_max_age_days),
            configuration={
                "lane": "II",
                "universe_path": str(universe_path.resolve()),
                "fresh_public_only": True,
            },
        )
        if analysis_limit:
            deep_wallets = [str(seed["wallet"]).lower() for seed in seeds] if research_pass and research_pass.get("pass_kind") == "deep_batch" else None
            if deep_wallets and analysis_limit != len(deep_wallets):
                raise ValueError("A frozen Lane II deep batch must analyze exactly its pinned wallet count.")
            analysis = CandidateAnalysisPipeline(service).run(
                limit=analysis_limit, status="new", force=True,
                workers=min(max(1, service.config.analysis.default_workers), analysis_limit),
                candidate_wallets=deep_wallets,
            )
            pipeline = CandidateAnalysisPipeline(service)
            selected = pipeline.shadow_finalists(count=max(1, target_count), persist=True, wallets=deep_wallets)
        else:
            analysis = {"status": "SCREEN_ONLY_NO_DEEP_ANALYSIS", "planned_deep_limit": 0}
            selected = []
    else:
        discovery = None
        analysis = None
        selected = []

    center = CopyControlCenter(service.config, service.database, execution_service=service)
    candidate_view = center.candidates(page=1, page_size=200, sort="score", direction="desc", current_only=True)
    items = list(candidate_view.get("items", []))
    selected_wallets = {str(item["wallet"]).lower() for item in selected}
    for wallet in sorted(selected_wallets):
        # Shadow is observation authority only. Active remains a separate,
        # canonical operator decision and is never granted by refresh.
        center.set_operator_state(wallet, "shadow", by="lane-ii-public-refresh")

    watchlist = [
        {
            **item,
            "roster": "selected" if str(item["wallet"]).lower() in selected_wallets else "research_watchlist",
            "qualification_warning": None if str(item["wallet"]).lower() in selected_wallets else "NOT_SELECTED_NOT_ACTIVE",
        }
        for item in items[: max(target_count, 10)]
    ]
    cohort = {
        "schema": COHORT_SCHEMA,
        "strategy": {"strategy_id": service.config.paper_strategy.strategy_id, "version": service.config.paper_strategy.version},
        "generated_at": utc_now().isoformat(),
        "data_cutoff": cutoff.isoformat(),
        "candidate_universe_path": str(universe_path.resolve()),
        "frozen_research_pass": str(Path(frozen_research_pass).expanduser().resolve()) if frozen_research_pass else None,
        "paper_account": {
            "display": "PAPER — $100 simulated",
            "initial_capital": service.config.capital.initial_capital,
            "connected_capital": False,
            "cash_app_connected": False,
        },
        "coverage_policy": {
            "public_history": "UNPROVEN unless the stored Phase B record proves otherwise",
            "user_fills_response_limit": 2000,
            "user_fills_retention_limit": 10000,
        },
        "requested_target_count": target_count,
        "selected_count": len(selected),
        "shortfall": max(0, 5 - len(selected)),
        "paper_start_eligible": len(selected) >= 5,
        "selection_status": "COMPLETED" if analysis_limit else "NOT_RUN_SCREEN_ONLY",
        "screening": universe["screening"],
        "selected": selected,
        "research_watchlist": [item for item in watchlist if item["roster"] == "research_watchlist"],
        "all_evaluated": watchlist,
        "discovery": asdict(discovery) if discovery else None,
        "analysis": analysis,
        "authority": {
            "selection_is_not_activation": True,
            "activated_wallets": [],
            "exchange_order_authority": False,
            "live_eligibility": "UNRESOLVED_US_VENUE_ELIGIBILITY",
        },
    }
    cohort_path = _write_evidence(output_root / f"cohort-{run_stamp}.json", cohort)
    latest_path = _write_evidence(output_root / "latest-cohort.json", cohort)
    write_progress("COMPLETED", completed_wallets=len(public_rows), requests=requests_consumed)
    return {
        **cohort,
        "candidate_universe_path": str(universe_path.resolve()),
        "cohort_path": str(cohort_path.resolve()),
        "latest_cohort_path": str(latest_path.resolve()),
    }


def load_cohort_evidence(path: str | Path) -> dict[str, Any] | None:
    resolved = Path(path)
    if not resolved.is_file():
        return None
    try:
        value = json.loads(resolved.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return value if isinstance(value, dict) and value.get("schema") == COHORT_SCHEMA else None


def replay_lane_ii_shared_cohort(
    service: CopyTradeService, *, cohort_path: str | Path, output: str | Path,
) -> dict[str, Any]:
    """Run a single $100 historical PAPER replay for an already selected cohort.

    This intentionally has no discovery, acquisition, target-state, watcher, or
    execution side effect.  It reuses only persisted Phase-B event history
    inside each selected wallet's immutable analysis window.
    """
    cohort_source = Path(cohort_path).expanduser().resolve()
    cohort = _load_evidence(cohort_source, schema=COHORT_SCHEMA)
    selected = list(cohort.get("selected") or ())
    wallets = tuple(str(item.get("wallet") or "").lower() for item in selected if str(item.get("wallet") or ""))
    payload: dict[str, Any] = {
        "schema": "beelzebub-lane-ii-shared-cohort-replay-v1",
        "generated_at": utc_now().isoformat(),
        "cohort_path": str(cohort_source),
        "cohort_payload_sha256": _json_sha256(cohort),
        "paper_account": {"initial_capital": service.config.capital.initial_capital, "currency": service.config.capital.currency},
        "selected_wallets": list(wallets),
        "authority": {
            "paper_execution_authority": False,
            "exchange_order_authority": False,
            "live_execution_authority": False,
            "cash_app_connected": False,
        },
    }
    if cohort.get("selection_status") != "COMPLETED" or len(wallets) < 5:
        payload.update({
            "status": "NOT_RUN_COHORT_SHORTFALL",
            "reason": "The canonical cohort did not contain the required five selected Phase B finalists.",
            "shortfall": max(0, 5 - len(wallets)),
            "shared_100_results": None,
            "stresses": None,
        })
        path = _write_evidence(Path(output), payload)
        return {**payload, "replay_path": str(path.resolve())}

    events: list[Any] = []
    per_wallet: list[dict[str, Any]] = []
    for wallet in wallets:
        candidate = next(iter(service.database.list_analysis_candidates(wallets=[wallet], limit=1)), None)
        if not candidate:
            raise ValueError(f"Selected cohort wallet is absent from the local Phase B database: {wallet}")
        summary = candidate.get("analysis_summary") or {}
        window = summary.get("analysis_window") if isinstance(summary, dict) else None
        diversification = summary.get("diversification_input") if isinstance(summary, dict) else None
        if not isinstance(window, dict) or not window.get("required_start") or not window.get("required_end"):
            raise ValueError(f"Selected cohort wallet has no immutable Phase B analysis window: {wallet}")
        campaign_ids = set((diversification or {}).get("campaign_ids") or ()) if isinstance(diversification, dict) else set()
        start, end = as_utc(window["required_start"]), as_utc(window["required_end"])
        source_events = service.database.list_position_events(wallet)
        bounded = [
            event for event in source_events
            if start <= as_utc(event.event_timestamp) <= end and (not campaign_ids or event.campaign_id in campaign_ids)
        ]
        events.extend(bounded)
        coverage = summary.get("coverage") if isinstance(summary, dict) else {}
        per_wallet.append({
            "wallet": wallet, "events_replayed": len(bounded), "analysis_window": window,
            "coverage": coverage,
        })
    events.sort(key=lambda event: (as_utc(event.event_timestamp), event.event_id))
    backtester = CopyTradeBacktester(service.config)
    baseline = backtester.run(events=events, coverage_metadata={"wallet_coverages": per_wallet})
    slippage = backtester.slippage_scenarios(events=events)
    payload.update({
        "status": "COMPLETED_PAPER_ONLY",
        "shared_100_results": baseline.summary,
        "per_wallet_windows": per_wallet,
        "stresses": {
            "slippage_scenarios": slippage,
            "latency": {
                "status": "UNAVAILABLE_HISTORICAL_L2_NOT_COLLECTED",
                "note": "No historical order-book data was purchased or inferred; baseline uses target fill price plus configured deterministic slippage.",
            },
            "portfolio_constraints": {
                "max_total_committed_fraction": service.config.risk.max_total_committed_fraction,
                "max_capital_per_target_fraction": service.config.risk.max_capital_per_target_fraction,
                "max_capital_per_symbol_fraction": service.config.risk.max_capital_per_symbol_fraction,
                "max_simultaneous_virtual_campaigns": service.config.risk.max_simultaneous_virtual_campaigns,
            },
        },
    })
    path = _write_evidence(Path(output), payload)
    return {**payload, "replay_path": str(path.resolve())}


def run_paper_account_demo(service: CopyTradeService, *, output: str | Path) -> dict[str, Any]:
    """Exercise $100 sizing against current public mids and lot precision.

    This is a clearly synthetic signal matrix. It demonstrates mechanics and
    cannot be interpreted as natural observation or a profitability result.
    """
    metadata = service.refresh_instrument_metadata()
    mids_payload = service.adapter.info({"type": "allMids"})
    if not isinstance(mids_payload, dict):
        raise ValueError("Unexpected allMids response")
    preferred = ["BTC", "ETH", "SOL", "HYPE", "DOGE", "XRP"]
    symbols = [symbol for symbol in preferred if symbol in service.instrument_quantity_precision and symbol in mids_payload]
    if not symbols:
        raise ValueError("No current instrument metadata overlapped current public mids")
    now = utc_now()

    def signal(symbol: str, fraction: float, *, sequence: int, action: str = "open", direction: str = "long", wallet: str = "0x1111111111111111111111111111111111111111", campaign: str | None = None, target_quantity: float = 1, target_position_before: float = 0) -> CopySignal:
        price = float(mids_payload[symbol])
        identifier = stable_id("lane_ii_demo_signal", symbol, fraction, sequence, action, direction, wallet)
        return CopySignal(
            signal_id=identifier, target_wallet=wallet, campaign_id=campaign or f"demo-{wallet[-4:]}-{symbol}",
            source_event_id=identifier, symbol=symbol, action=action, direction=direction,
            target_price=price, target_quantity=target_quantity, target_notional=price * target_quantity,
            allocation_fraction=fraction, requested_capital=100 * fraction,
            created_at=now, source_event_timestamp=now, reason="synthetic_lane_ii_demo",
            target_position_before=target_position_before, target_equity=1000,
            equity_source="exact", equity_age_seconds=0,
        )

    policies = {
        "legacy_5_10_20": (.05, .10, .20),
        "proposed_paper_12_16_20": (.12, .16, .20),
    }
    matrices: dict[str, Any] = {}
    for name, fractions in policies.items():
        rows: list[dict[str, Any]] = []
        for symbol in symbols:
            for sequence, fraction in enumerate(fractions, 1):
                engine = PaperExecutionEngine(
                    service.config,
                    quantity_precision_by_symbol=service.instrument_quantity_precision,
                )
                attempt = engine.process_signal(signal(symbol, fraction, sequence=sequence), received_at=now, market_price=float(mids_payload[symbol]))
                position = next(iter(engine.portfolio.sleeves.values()), None)
                rows.append({
                    "symbol": symbol,
                    "public_mid": float(mids_payload[symbol]),
                    "quantity_precision": service.instrument_quantity_precision[symbol],
                    "intended_fraction": fraction,
                    "intended_capital": 100 * fraction,
                    "status": attempt.status,
                    "reason": attempt.reason,
                    "simulated_quantity": position.quantity if position else 0,
                    "simulated_notional": (position.quantity * position.entry_price) if position else 0,
                })
        matrices[name] = {
            "attempts": len(rows),
            "filled": sum(row["status"] == "filled" for row in rows),
            "skipped": sum(row["status"] != "filled" for row in rows),
            "rows": rows,
        }

    # One deterministic ownership lifecycle: opposing leaders retain separate
    # sleeves, an add aggregates only its owner's sleeve, and partial/complete
    # exits cannot close the other wallet's ownership.
    lifecycle_engine = PaperExecutionEngine(
        service.config,
        quantity_precision_by_symbol=service.instrument_quantity_precision,
    )
    symbol = symbols[0]
    price = float(mids_payload[symbol])
    long_wallet = "0x1111111111111111111111111111111111111111"
    short_wallet = "0x2222222222222222222222222222222222222222"
    lifecycle_signals = [
        signal(symbol, .12, sequence=100, direction="long", wallet=long_wallet, campaign="leader-a"),
        signal(symbol, .12, sequence=101, direction="short", wallet=short_wallet, campaign="leader-b"),
        signal(symbol, .12, sequence=102, action="add", direction="long", wallet=long_wallet, campaign="leader-a"),
    ]
    lifecycle_attempts = [
        lifecycle_engine.process_signal(item, received_at=now, market_price=price)
        for item in lifecycle_signals
    ]
    long_position = next(item for item in lifecycle_engine.portfolio.sleeves.values() if item.target_wallet == long_wallet)
    reduce_signal = signal(
        symbol, 0, sequence=103, action="reduce", direction="long", wallet=long_wallet,
        campaign="leader-a", target_quantity=long_position.quantity / 2,
        target_position_before=long_position.quantity,
    )
    close_signal = signal(
        symbol, 0, sequence=104, action="close", direction="short", wallet=short_wallet,
        campaign="leader-b", target_quantity=1, target_position_before=1,
    )
    lifecycle_attempts.extend([
        lifecycle_engine.process_signal(reduce_signal, received_at=now, market_price=price),
        lifecycle_engine.process_signal(close_signal, received_at=now, market_price=price),
    ])
    open_sleeves = [item for item in lifecycle_engine.portfolio.sleeves.values() if item.is_open]
    lifecycle = {
        "actions": [
            {"action": item.action, "wallet": item.target_wallet, "status": attempt.status, "reason": attempt.reason}
            for item, attempt in zip([*lifecycle_signals, reduce_signal, close_signal], lifecycle_attempts)
        ],
        "open_sleeves_after": [
            {"wallet": item.target_wallet, "direction": item.direction, "quantity": item.quantity, "allocated_capital": item.allocated_capital}
            for item in open_sleeves
        ],
        "opposing_leader_exit_preserved_other_owner": (
            len(open_sleeves) == 1 and open_sleeves[0].target_wallet == long_wallet
        ),
    }
    payload = {
        "schema": "beelzebub-lane-ii-paper-demo-v1",
        "generated_at": utc_now().isoformat(),
        "classification": "SYNTHETIC_PAPER_MECHANICS_NOT_NATURAL_TRADING",
        "display_account": "PAPER — $100 simulated",
        "connected_capital": False,
        "real_exchange_orders": 0,
        "real_funds_moved": 0,
        "assumptions": {
            "fee_rate": service.config.paper_execution.fee_rate,
            "funding": "NOT_MODELED_IN_SYNTHETIC_MECHANICS_DEMO",
            "slippage_bps": service.config.paper_execution.slippage_bps,
            "detection_latency_ms": service.config.paper_execution.detection_latency_ms,
            "order_latency_ms": service.config.paper_execution.order_latency_ms,
            "minimum_order_notional": service.config.paper_execution.min_order_notional,
            "source_leverage_ignored": True,
        },
        "instrument_metadata": metadata,
        "matrices": matrices,
        "ownership_lifecycle": lifecycle,
    }
    path = _write_evidence(Path(output), payload)
    return {**payload, "output": str(path.resolve())}


def lane_ii_status(
    service: CopyTradeService,
    *,
    cohort_path: str | Path,
    watcher_health: dict[str, Any] | None = None,
) -> dict[str, Any]:
    center = CopyControlCenter(service.config, service.database, execution_service=service)
    evidence = load_cohort_evidence(cohort_path)
    screening = dict((evidence or {}).get("screening") or {})
    analysis_summary = dict((evidence or {}).get("analysis") or {})
    portfolio = center.portfolio_summary()
    selected = list((evidence or {}).get("selected") or [])
    watchlist = list((evidence or {}).get("research_watchlist") or [])
    selected_wallets = {str(item.get("wallet", "")).lower() for item in selected}
    targets = service.database.list_targets()
    observing = [target.wallet for target in targets if target.status in {"shadow", "active"}]
    active = [target.wallet for target in targets if target.status == "active"]
    net_exposure: dict[str, dict[str, float]] = {}
    for position in service.database.list_virtual_positions(open_only=True):
        item = net_exposure.setdefault(position.symbol, {"long_quantity": 0.0, "short_quantity": 0.0, "net_quantity": 0.0})
        if position.direction == "long":
            item["long_quantity"] += position.quantity
            item["net_quantity"] += position.quantity
        else:
            item["short_quantity"] += position.quantity
            item["net_quantity"] -= position.quantity
    credentials = {
        "testnet_account_address_present": bool(os.getenv("HYPERLIQUID_TESTNET_ACCOUNT_ADDRESS")),
        "testnet_account_kind_present": bool(os.getenv("HYPERLIQUID_TESTNET_ACCOUNT_KIND")),
        "testnet_api_wallet_key_present": bool(os.getenv("HYPERLIQUID_TESTNET_API_WALLET_PRIVATE_KEY")),
    }
    counts = center._counts()
    public_state = str((watcher_health or {}).get("state") or ("NOT_STARTED" if not observing else "STARTING"))
    screened = sum(int(screening.get(key) or 0) for key in ("available", "empty", "errors", "deferred_by_budget"))
    acquired = max(0, int(analysis_summary.get("backfill_attempted") or 0) - int(analysis_summary.get("backfill_failed") or 0))
    deferred = int(screening.get("deferred_by_budget") or 0) + int(analysis_summary.get("deferred") or 0)
    selection_completed = str((evidence or {}).get("selection_status") or "") == "COMPLETED"
    has_screen = screened > 0
    generated_at = utc_now()
    return {
        "schema": "beelzebub-lane-ii-slim-status-v1",
        "generated_at": generated_at.isoformat(),
        "display_account": "PAPER — $100 simulated",
        "strategy": {"strategy_id": service.config.paper_strategy.strategy_id, "version": service.config.paper_strategy.version},
        "overall": {
            "state": (
                "PAPER_READY_DISARMED" if len(selected) >= 5 and selection_completed else
                "RESEARCH_SCREENED" if has_screen else
                "RESEARCH_SHORTFALL"
            ),
            "next_action": (
                "Observe the frozen selected cohort; activate PAPER separately when continuity is proven."
                if len(selected) >= 5 and selection_completed else
                "Review the frozen public screen and run the separately bounded deep-evaluation batch."
                if has_screen else
                "Continue public observation; fewer than five candidates meet the unchanged evidence gates."
            ),
        },
        "readiness": {
            "public_observation": {"state": public_state, "read_only": True},
            "paper": {
                "state": "READY_DISARMED" if len(selected) >= 5 and selection_completed else "BLOCKED_COHORT_SHORTFALL",
                "simulated": True,
                "selected_count": len(selected),
                "minimum_selected": 5,
            },
            "testnet": {
                "state": "BLOCKED_PREREQUISITES" if not all(credentials.values()) else "CONFIG_PRESENT_NOT_COMMISSIONED",
                "configuration_presence": credentials,
                "real_order_count_this_pass": 0,
            },
            "live": {
                "state": "UNAVAILABLE",
                "eligibility": "UNRESOLVED_US_VENUE_ELIGIBILITY",
                "adapter": "NOT_IMPLEMENTED",
                "backend_denied": True,
                "real_order_count_this_pass": 0,
            },
        },
        "cohort": {
            "selected_at": (evidence or {}).get("generated_at"),
            "data_cutoff": (evidence or {}).get("data_cutoff"),
            "selected": selected,
            "research_watchlist": watchlist,
            "selected_wallets": sorted(selected_wallets),
            "observing_wallets": sorted(observing),
            "active_wallets": sorted(active),
        },
        "portfolio": {**portfolio, "display": "PAPER — $100 simulated", "connected_capital": False},
        "paper_policy": {
            "initial_capital": service.config.capital.initial_capital,
            "sizing_fractions": [
                service.config.sizing.small_fraction,
                service.config.sizing.medium_fraction,
                service.config.sizing.large_fraction,
            ],
            "maximum_total_committed": (
                service.config.capital.initial_capital * service.config.risk.max_total_committed_fraction
            ),
            "fee_rate": service.config.paper_execution.fee_rate,
            "funding_model": "NOT_MODELED",
            "source_leverage_ignored": True,
        },
        "positions": center.positions(),
        "net_exchange_exposure": [
            {"symbol": symbol, **values}
            for symbol, values in sorted(net_exposure.items())
        ],
        "recent_actions": center.activity(limit=30),
        "funnel": [
            {"label": "Discovered", "count": counts["total_discovered"]},
            {"label": "Screened", "count": screened},
            {"label": "Acquiring", "count": 0},
            {"label": "Acquired", "count": acquired},
            {"label": "Analyzed", "count": counts["analyzed"]},
            {"label": "Qualified", "count": counts["qualified"]},
            {"label": "Selected", "count": len(selected)},
            {"label": "Deferred", "count": deferred},
        ],
        "controls": {
            "refresh_candidates_available": not selected,
            "observe_available": bool(selected or watchlist),
            "start_paper_available": (
                len(selected) >= 5 and selection_completed and
                service.config.paper_strategy.strategy_id == "COHORT_COPY_V1"
            ),
            "pause_new_entries_available": True,
            "close_paper_positions_available": bool(service.database.list_virtual_positions(open_only=True)),
            "live_available": False,
        },
        "connection_setup": {
            "operator_location": "Colorado, United States",
            "future_budget_source": "Cash App BTC (not connected, not verified exchange capital)",
            "cash_app_credentials_requested": False,
            "cash_app_connected": False,
            "trading_account_address": None,
            "account_kind": None,
            "agent_signer": None,
            "live_eligibility": "UNRESOLVED_US_VENUE_ELIGIBILITY",
        },
    }
