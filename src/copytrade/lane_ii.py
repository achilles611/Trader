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

from .analysis import CandidateAnalysisPipeline
from .control_center import CopyControlCenter
from .discovery import DiscoveryPipeline
from .models import CopySignal, DiscoveryObservation, RawFill, jsonable, stable_id, utc_now
from .paper import PaperExecutionEngine
from .service import CopyTradeService


COHORT_SCHEMA = "beelzebub-lane-ii-cohort-v1"
UNIVERSE_SCHEMA = "beelzebub-lane-ii-candidate-universe-v1"


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


def _retained_seed_rows(path: Path, limit: int) -> list[dict[str, Any]]:
    """Read the retained discovery database without initializing or migrating it."""
    resolved = path.expanduser().resolve()
    if not resolved.is_file():
        raise FileNotFoundError(f"Retained candidate database not found: {resolved}")
    uri = f"file:{resolved.as_posix()}?mode=ro"
    connection = sqlite3.connect(uri, uri=True)
    connection.row_factory = sqlite3.Row
    try:
        rows = connection.execute(
            """SELECT wallet, discovered_at, last_seen_at, recent_activity_at,
                      discovery_rank, source_score, source_count, metadata_json
                 FROM copy_discovery_candidates
                ORDER BY COALESCE(source_score, 0) DESC,
                         COALESCE(recent_activity_at, last_seen_at) DESC,
                         wallet ASC
                LIMIT ?""",
            (max(1, int(limit)),),
        ).fetchall()
    finally:
        connection.close()
    return [
        {
            **dict(row),
            "metadata": json.loads(str(row["metadata_json"] or "{}")),
        }
        for row in rows
    ]


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
) -> dict[str, Any]:
    """Refresh the current public universe and run the canonical Phase B path.

    The retained database supplies candidate *leads* only. Every registered
    observation in this run comes from a fresh unauthenticated ``userFills``
    response, and Phase B independently backfills/reconstructs those wallets.
    """
    if service.config.mode != "paper" or service.config.live_enabled:
        raise ValueError("Lane II cohort refresh requires a paper-only configuration.")
    started_at = utc_now()
    instrument_metadata = service.refresh_instrument_metadata()
    retained_path = Path(retained_database)
    output_root = Path(output_directory)
    seeds = _retained_seed_rows(retained_path, seed_limit)
    observations: list[DiscoveryObservation] = []
    public_rows: list[dict[str, Any]] = []
    for rank, seed in enumerate(seeds, 1):
        wallet = str(seed["wallet"]).lower()
        try:
            fills = service.adapter.fetch_user_fills(wallet, aggregate_by_time=False)
            newest = max((item.event_timestamp for item in fills), default=None)
            oldest = min((item.event_timestamp for item in fills), default=None)
            symbols = sorted({item.symbol for item in fills})
            observations.extend(
                _fill_observation(item, rank=rank, retained_score=seed.get("source_score"))
                for item in fills
            )
            public_rows.append({
                "wallet": wallet,
                "retained_rank": rank,
                "retained_source_score": seed.get("source_score"),
                "retained_metadata": seed.get("metadata", {}),
                "fresh_public_fill_count": len(fills),
                "oldest_public_fill_at": oldest.isoformat() if oldest else None,
                "newest_public_fill_at": newest.isoformat() if newest else None,
                "distinct_symbols": len(symbols),
                "symbols": symbols,
                "fresh_public_status": "AVAILABLE" if fills else "EMPTY",
                "error": None,
            })
        except Exception as exc:
            public_rows.append({
                "wallet": wallet,
                "retained_rank": rank,
                "retained_source_score": seed.get("source_score"),
                "fresh_public_fill_count": None,
                "oldest_public_fill_at": None,
                "newest_public_fill_at": None,
                "distinct_symbols": None,
                "symbols": [],
                "fresh_public_status": "ERROR",
                "error": f"{type(exc).__name__}: {exc}",
            })

    cutoff = utc_now()
    run_stamp = cutoff.strftime("%Y%m%dT%H%M%SZ")
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
        "fresh_observations": len(observations),
        "candidates": public_rows,
    }
    universe_path = _write_evidence(output_root / f"candidate-universe-{run_stamp}.json", universe)
    _write_evidence(output_root / "latest-candidate-universe.json", universe)

    if observations:
        discovery = DiscoveryPipeline(service.database).run(
            _ObservationProvider(observations),
            limit=max(1, analysis_limit),
            min_activity=max(1, service.config.analysis.min_discovery_activity),
            refresh=True,
            max_activity_age=timedelta(days=service.config.candidates.activity_max_age_days),
            configuration={
                "lane": "II",
                "universe_path": str(universe_path.resolve()),
                "fresh_public_only": True,
            },
        )
        analysis = CandidateAnalysisPipeline(service).run(
            limit=max(1, analysis_limit), status="new", force=True,
            workers=min(max(1, service.config.analysis.default_workers), max(1, analysis_limit)),
        )
        pipeline = CandidateAnalysisPipeline(service)
        selected = pipeline.shadow_finalists(count=max(1, target_count), persist=True)
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
    generated_at = utc_now()
    return {
        "schema": "beelzebub-lane-ii-slim-status-v1",
        "generated_at": generated_at.isoformat(),
        "display_account": "PAPER — $100 simulated",
        "strategy": {"strategy_id": service.config.paper_strategy.strategy_id, "version": service.config.paper_strategy.version},
        "overall": {
            "state": "PAPER_READY_DISARMED" if len(selected) >= 5 else "RESEARCH_SHORTFALL",
            "next_action": (
                "Observe the frozen selected cohort; activate PAPER separately when continuity is proven."
                if len(selected) >= 5 else
                "Continue public observation; fewer than five candidates meet the unchanged evidence gates."
            ),
        },
        "readiness": {
            "public_observation": {"state": public_state, "read_only": True},
            "paper": {
                "state": "READY_DISARMED" if len(selected) >= 5 else "BLOCKED_COHORT_SHORTFALL",
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
            {"label": "Analyzed", "count": counts["analyzed"]},
            {"label": "Qualified", "count": counts["qualified"]},
            {"label": "Selected", "count": len(selected)},
            {"label": "Observing", "count": len(observing)},
            {"label": "Active", "count": len(active)},
        ],
        "controls": {
            "refresh_candidates_available": True,
            "observe_available": bool(selected or watchlist),
            "start_paper_available": len(selected) >= 5 and service.config.paper_strategy.strategy_id == "COHORT_COPY_V1",
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
