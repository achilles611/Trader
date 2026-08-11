from __future__ import annotations

import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import timedelta
from statistics import fmean
from typing import Any, Callable, Iterable

from .analytics import campaign_return_series
from .backtest import CopyTradeBacktester
from .config import CopyTradeConfig
from .models import AnalysisRun, CandidateAnalysis, CandidateAnalysisState, PositionCampaign, PositionEvent, as_utc, new_run_id, utc_now
from .scoring import FollowerMetrics, score_candidate, select_diverse_targets
from .service import CopyTradeService


_FINAL_STATES = {
    CandidateAnalysisState.PREFILTER_REJECTED.value,
    CandidateAnalysisState.ANALYZED.value,
    CandidateAnalysisState.QUALIFIED.value,
    CandidateAnalysisState.QUARANTINED.value,
}
_OPERATOR_CONTROLLED_TARGET_STATES = {"approved", "shadow", "active", "muted", "rejected"}


class CandidateAnalysisPipeline:
    """Resumable, research-only Phase B orchestration around existing components.

    It deliberately keeps its lifecycle in ``copy_candidate_analyses`` rather
    than promoting or changing the operator-managed target state.  Historical
    replays use an in-memory paper engine and never touch operational execution
    tables.
    """

    def __init__(
        self, service: CopyTradeService, *,
        backfill_wallet: Callable[[str, object], dict[str, object]] | None = None,
        reconstruct_wallet: Callable[[str], dict[str, object]] | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.service = service
        self.config = service.config
        self.database = service.database
        self._backfill_wallet = backfill_wallet or self._default_backfill
        self._reconstruct_wallet = reconstruct_wallet or service.reconstruct
        self._sleep = sleep

    def run(
        self, *, limit: int = 500, status: str | None = "new", resume: bool = False,
        force: bool = False, workers: int | None = None, cheap_only: bool = False,
    ) -> dict[str, object]:
        if limit <= 0:
            raise ValueError("--limit must be positive.")
        worker_count = workers or self.config.analysis.default_workers
        if worker_count <= 0:
            raise ValueError("--workers must be positive.")
        run, resumed = self._start_or_resume(
            resume=resume, configuration={
                "limit": limit, "status": status, "force": force, "workers": worker_count,
                "cheap_only": cheap_only, "history_days": self.config.analysis.history_days,
                "min_discovery_activity": self.config.analysis.min_discovery_activity,
                "copytrade_config": self.config.snapshot(),
            },
        )
        counters = self._counters(run) if resumed else _empty_counters()
        errors: list[str] = list(_json_list(run.get("errors_json"))) if resumed else []
        candidates = self.database.list_analysis_candidates(status=status, limit=limit)
        if status is None:
            candidates = [row for row in candidates if row.get("current_status") not in _OPERATOR_CONTROLLED_TARGET_STATES]
        pending: list[dict[str, Any]] = []
        try:
            for candidate in candidates:
                wallet = str(candidate["wallet"]).lower()
                existing = self.database.get_candidate_analysis(wallet)
                if not force and existing and existing.completed_at and existing.lifecycle_status in _FINAL_STATES:
                    counters["deferred"] += 1
                    self.database.record_analysis_wallet(run["run_id"], wallet, stage="resume", status="skipped", payload={
                        "reason": "already_complete", "lifecycle_status": existing.lifecycle_status,
                    })
                    continue
                counters["wallets_considered"] += 1
                reasons = self._cheap_prefilter(candidate)
                if reasons:
                    counters["cheap_rejected"] += 1
                    counters["rejected"] += 1
                    self._save_candidate(
                        wallet, CandidateAnalysisState.PREFILTER_REJECTED.value, run["run_id"],
                        reasons=reasons, summary={"prefilter": {"accepted": False, "reasons": reasons}}, completed=True,
                    )
                    self.database.record_analysis_wallet(run["run_id"], wallet, stage="prefilter", status="rejected", payload={"reasons": reasons})
                    continue
                self._save_candidate(
                    wallet, CandidateAnalysisState.BACKFILL_PENDING.value, run["run_id"],
                    summary={"prefilter": {"accepted": True, "reasons": []}}, completed=False,
                )
                self.database.record_analysis_wallet(run["run_id"], wallet, stage="prefilter", status="accepted")
                pending.append(candidate)

            if cheap_only:
                counters["deferred"] += len(pending)
                for candidate in pending:
                    self.database.record_analysis_wallet(
                        run["run_id"], str(candidate["wallet"]), stage="backfill", status="deferred",
                        payload={"reason": "cheap_only"},
                    )
                return self._finish(run["run_id"], counters, errors, status="completed")

            required_start = utc_now() - timedelta(days=self.config.analysis.history_days)
            outcomes = self._backfill_all(pending, required_start, worker_count)
            for wallet, attempts, result, error in outcomes:
                counters["backfill_attempted"] += 1
                if error:
                    counters["backfill_failed"] += 1
                    counters["deferred"] += 1
                    errors.append(f"{wallet}: {error}")
                    self._save_candidate(
                        wallet, CandidateAnalysisState.BACKFILL_FAILED.value, run["run_id"], errors=(error,), completed=False,
                    )
                    self.database.record_analysis_wallet(
                        run["run_id"], wallet, stage="backfill", status="failed", attempts=attempts, error=error,
                    )
                    continue
                coverage = self.database.latest_backfill_coverage(wallet) or {}
                coverage_state = str(coverage.get("coverage_state", "UNPROVEN"))
                if coverage_state == "KNOWN_INCOMPLETE":
                    counters["rejected"] += 1
                    self._save_candidate(
                        wallet, CandidateAnalysisState.QUARANTINED.value, run["run_id"],
                        reasons=("known_incomplete",), summary={"coverage": coverage, "backfill": result}, completed=True,
                    )
                    self.database.record_analysis_wallet(
                        run["run_id"], wallet, stage="backfill", status="quarantined", attempts=attempts,
                        payload={"reason": "known_incomplete", "coverage": coverage},
                    )
                    continue
                self.database.record_analysis_wallet(
                    run["run_id"], wallet, stage="backfill", status="completed", attempts=attempts,
                    payload={"coverage": coverage, "backfill": result},
                )
                self._save_candidate(wallet, CandidateAnalysisState.ANALYSIS_PENDING.value, run["run_id"], completed=False)
                try:
                    analysis = self._analyze_wallet(wallet, coverage)
                except Exception as exc:  # reconstruction/simulation failure remains per-wallet and resumable
                    message = str(exc)
                    counters["deferred"] += 1
                    errors.append(f"{wallet}: analysis failed: {message}")
                    self._save_candidate(
                        wallet, CandidateAnalysisState.ANALYSIS_PENDING.value, run["run_id"], errors=(message,), completed=False,
                    )
                    self.database.record_analysis_wallet(run["run_id"], wallet, stage="analysis", status="failed", error=message)
                    continue
                counters["reconstructed"] += 1
                counters["scored"] += 1
                if analysis["eligible"]:
                    counters["eligible"] += 1
                    lifecycle = CandidateAnalysisState.QUALIFIED.value
                else:
                    counters["rejected"] += 1
                    lifecycle = CandidateAnalysisState.ANALYZED.value
                self._save_candidate(
                    wallet, lifecycle, run["run_id"], reasons=tuple(analysis["score"]["reasons"]),
                    summary=analysis, completed=True,
                )
                self.database.record_analysis_wallet(
                    run["run_id"], wallet, stage="analysis", status="completed", payload={
                        "eligible": analysis["eligible"], "score": analysis["score"], "copyability": analysis["copyability"],
                    },
                )
        except Exception as exc:
            errors.append(f"run failure: {exc}")
            return self._finish(run["run_id"], counters, errors, status="failed")
        return self._finish(run["run_id"], counters, errors, status="completed_with_errors" if errors else "completed")

    def status(self, *, limit: int = 1000) -> dict[str, object]:
        rows = self.database.list_analysis_candidates(limit=limit)
        state_counts: dict[str, int] = {}
        for row in rows:
            state = str(row.get("lifecycle_status") or CandidateAnalysisState.NEW.value)
            state_counts[state] = state_counts.get(state, 0) + 1
        return {"runs": self.database.list_analysis_runs(), "state_counts": state_counts, "candidates": rows}

    def shadow_finalists(self, *, count: int | None = None) -> list[dict[str, object]]:
        target_count = count or self.config.analysis.shadow_finalist_count
        scores = self.database.latest_scores()
        series = {
            score.target_wallet: campaign_return_series(self.database.list_campaigns(score.target_wallet, closed_only=True))
            for score in scores
        }
        selected = select_diverse_targets(scores, series, target_count=target_count)
        candidates = {row["wallet"]: row for row in self.database.list_analysis_candidates(limit=10_000)}
        finalists = []
        for rank, score in enumerate(selected, 1):
            candidate = candidates.get(score.target_wallet, {})
            analysis = candidate.get("analysis_summary", {}) if isinstance(candidate, dict) else {}
            target = analysis.get("target_metrics", {}) if isinstance(analysis, dict) else {}
            follower = analysis.get("follower", {}) if isinstance(analysis, dict) else {}
            finalists.append({
                "rank": rank, "wallet": score.target_wallet, "score": score.total_score,
                "target": target, "follower": follower, "copyability": analysis.get("copyability", {}),
                "data_quality": analysis.get("coverage", {}), "principal_risks": score.reasons,
                "selection_reason": "eligible score with lower time-aligned return correlation to already selected finalists",
            })
        return finalists

    def _default_backfill(self, wallet: str, start: object) -> dict[str, object]:
        earliest = self.database.earliest_fill_time(wallet)
        if earliest and as_utc(earliest) <= as_utc(start):
            latest = self.database.latest_fill_time(wallet)
            # Existing history satisfies the requested left boundary.  Fetch
            # only the forward gap when it is materially stale; fill IDs make
            # the one-millisecond overlap harmless at the boundary.
            if latest and as_utc(latest) < utc_now() - timedelta(minutes=1):
                return self.service.backfill_for_analysis(wallet, start=as_utc(latest) - timedelta(milliseconds=1))
            return {"skipped_existing_history": True, "earliest_fill": as_utc(earliest).isoformat()}
        return self.service.backfill_for_analysis(wallet, start=start)

    def _start_or_resume(self, *, resume: bool, configuration: dict[str, object]) -> tuple[dict[str, Any], bool]:
        if resume:
            existing = self.database.latest_resumable_analysis_run()
            if existing:
                return existing, True
        new = AnalysisRun(run_id=new_run_id("analysis"), started_at=utc_now(), configuration=configuration)
        self.database.start_analysis_run(new)
        created = self.database.get_analysis_run(new.run_id)
        assert created is not None
        return created, False

    def _backfill_all(
        self, candidates: Iterable[dict[str, Any]], start: object, workers: int,
    ) -> list[tuple[str, int, dict[str, object], str | None]]:
        items = [str(candidate["wallet"]).lower() for candidate in candidates]
        if not items:
            return []
        outcomes: list[tuple[str, int, dict[str, object], str | None]] = []
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="copy-analysis") as pool:
            futures = {pool.submit(self._retry_backfill, wallet, start): wallet for wallet in items}
            for future in as_completed(futures):
                outcomes.append(future.result())
        return sorted(outcomes, key=lambda item: item[0])

    def _retry_backfill(self, wallet: str, start: object) -> tuple[str, int, dict[str, object], str | None]:
        attempts = self.config.analysis.retry_attempts
        for attempt in range(1, attempts + 1):
            try:
                return wallet, attempt, self._backfill_wallet(wallet, start), None
            except Exception as exc:
                coverage = self.database.latest_backfill_coverage(wallet)
                if coverage and str(coverage.get("coverage_state")) == "KNOWN_INCOMPLETE":
                    return wallet, attempt, {"coverage": coverage}, None
                if attempt == attempts:
                    return wallet, attempt, {}, str(exc)
                self._sleep(self.config.analysis.retry_initial_seconds * (2 ** (attempt - 1)))
        raise AssertionError("retry loop must return")  # pragma: no cover

    def _cheap_prefilter(self, candidate: dict[str, Any]) -> tuple[str, ...]:
        reasons: list[str] = []
        wallet = str(candidate.get("wallet") or "").lower()
        if not _is_wallet(wallet):
            reasons.append("invalid_wallet")
        status = str(candidate.get("current_status") or "")
        if status in _OPERATOR_CONTROLLED_TARGET_STATES:
            reasons.append("operator_managed_status")
        activity = candidate.get("recent_activity_at")
        if not activity or (utc_now() - as_utc(activity)).total_seconds() > self.config.candidates.activity_max_age_days * 86_400:
            reasons.append("inactive")
        metadata = _json_object(candidate.get("metadata_json"))
        observed = int(metadata.get("latest_activity_observations", 0) or 0)
        if observed < self.config.analysis.min_discovery_activity:
            reasons.append("insufficient_activity")
        coverage = self.database.latest_backfill_coverage(wallet) if wallet else None
        if coverage and str(coverage.get("coverage_state")) == "KNOWN_INCOMPLETE":
            reasons.append("known_incomplete")
        return tuple(sorted(set(reasons)))

    def _analyze_wallet(self, wallet: str, coverage: dict[str, Any]) -> dict[str, object]:
        reconstructed = self._reconstruct_wallet(wallet)
        events = tuple(reconstructed["events"])
        metrics = reconstructed["metrics"]
        if metrics is None:
            raise RuntimeError("reconstruction returned no trader metrics")
        campaigns = self.database.list_campaigns(wallet)
        backtester = CopyTradeBacktester(self.config)
        baseline = backtester.run(events=events, coverage_metadata=coverage)
        slippage = backtester.slippage_scenarios(events=events)
        latency = backtester.latency_decay_curve(events=events)
        walk_forward = (
            backtester.walk_forward(events=events)
            if metrics.closed_campaign_count >= self.config.candidates.closed_campaigns_min else []
        )
        follower = _follower_summary(baseline.summary, slippage, latency)
        copyability = _copyability(metrics.net_pnl, metrics.raw, baseline.summary, coverage)
        follower_metrics = FollowerMetrics(
            net_pnl=float(baseline.summary["net_pnl"]),
            expectancy=float(follower["expectancy"]),
            profit_factor=float(follower["profit_factor"] or 0.0),
            max_drawdown=float(baseline.summary["max_drawdown_fraction"]),
            missed_trade_rate=float(follower["missed_trade_rate"]),
            latency_curve=tuple(latency), latency_status="available" if latency else "unavailable",
            return_fraction=float(follower["return_fraction"]),
            copyability_score=copyability["score"] if copyability["status"] == "available" else None,
            slippage_robustness=follower.get("slippage_robustness_score"),
        )
        score = score_candidate(metrics, self.config.candidates, follower_metrics)
        self.database.upsert_candidate_score(score)
        return {
            "target_metrics": _target_summary(metrics, campaigns, events),
            "follower": follower,
            "copyability": copyability,
            "coverage": coverage,
            "slippage_scenarios": slippage,
            "latency": {"status": "available" if latency else "unavailable", "curve": latency},
            "walk_forward": walk_forward,
            "score": {
                "total": score.total_score, "eligible": score.eligible, "components": score.component_scores,
                "penalties": score.penalties, "reasons": list(score.reasons), "source_quality": score.source_quality,
            },
            "eligible": score.eligible,
        }

    def _save_candidate(
        self, wallet: str, lifecycle: str, run_id: str, *, reasons: tuple[str, ...] = (),
        errors: tuple[str, ...] = (), summary: dict[str, object] | None = None, completed: bool,
    ) -> None:
        existing = self.database.get_candidate_analysis(wallet)
        self.database.upsert_candidate_analysis(CandidateAnalysis(
            wallet=wallet, lifecycle_status=lifecycle, last_run_id=run_id,
            started_at=existing.started_at if existing and existing.started_at else utc_now(),
            completed_at=utc_now() if completed else None, prefilter_reasons=reasons, errors=errors,
            summary=summary if summary is not None else (existing.summary if existing else {}),
        ))

    def _finish(self, run_id: str, counters: dict[str, int], errors: list[str], *, status: str) -> dict[str, object]:
        self.database.finish_analysis_run(run_id, status=status, errors=tuple(sorted(errors)), **counters)
        return {"run_id": run_id, "status": status, **counters, "errors": sorted(errors), "shadow_finalists": self.shadow_finalists()}

    @staticmethod
    def _counters(run: dict[str, Any]) -> dict[str, int]:
        return {key: int(run.get(key, 0)) for key in _empty_counters()}


def _empty_counters() -> dict[str, int]:
    return {
        "wallets_considered": 0, "cheap_rejected": 0, "backfill_attempted": 0, "backfill_failed": 0,
        "reconstructed": 0, "scored": 0, "eligible": 0, "rejected": 0, "deferred": 0,
    }


def _follower_summary(
    baseline: dict[str, Any], slippage: list[dict[str, float]], latency: list[dict[str, float]],
) -> dict[str, object]:
    attempts = max(int(baseline.get("attempts", 0)), 1)
    net = float(baseline.get("net_pnl", 0.0))
    zero = next((item for item in slippage if float(item["slippage_bps"]) == 0.0), None)
    max_friction = slippage[-1] if slippage else None
    sizing_decisions = baseline.get("sizing_decisions", [])
    bucket_counts: dict[str, int] = {}
    for decision in sizing_decisions:
        if isinstance(decision, dict):
            bucket = str(decision.get("bucket") or "fallback")
            bucket_counts[bucket] = bucket_counts.get(bucket, 0) + 1
    slippage_robustness = None
    if zero and float(zero["return_fraction"]) > 0 and max_friction:
        slippage_robustness = max(0.0, min(1.0, float(max_friction["return_fraction"]) / float(zero["return_fraction"])))
    return {
        "net_pnl": net, "return_fraction": float(baseline.get("return_fraction", 0.0)),
        "max_drawdown": float(baseline.get("max_drawdown_fraction", 0.0)),
        "profit_factor": baseline.get("follower_profit_factor"),
        "expectancy": float(baseline.get("follower_expectancy", net / attempts)),
        "attempted_entries": int(baseline.get("attempts", 0)), "filled_attempts": int(baseline.get("filled_attempts", 0)),
        "skipped_attempts": int(baseline.get("skipped_attempts", 0)),
        "missed_trade_rate": float(baseline.get("skipped_attempts", 0)) / attempts,
        "pnl_by_wallet": baseline.get("follower_pnl_by_wallet", {}),
        "pnl_by_symbol": baseline.get("follower_pnl_by_symbol", {}),
        "pnl_by_sizing_bucket": baseline.get("follower_pnl_by_sizing_bucket", {}),
        "fees": float(baseline.get("follower_fees", 0.0)),
        "sizing": {
            **dict(baseline.get("equity_enrichment", {})), "bucket_counts": bucket_counts,
            "bucket_fractions": {bucket: count / len(sizing_decisions) for bucket, count in bucket_counts.items()} if sizing_decisions else {},
        },
        "slippage_cost_at_baseline": (float(zero["net_pnl"]) - net) if zero else None,
        "slippage_robust": bool(max_friction and float(max_friction["net_pnl"]) > 0),
        "slippage_robustness_score": slippage_robustness,
        "latency_status": "available" if latency else "unavailable",
        "price_assumption": baseline.get("price_assumption"),
    }


def _copyability(target_net_pnl: float, raw: dict[str, Any], follower: dict[str, Any], coverage: dict[str, Any]) -> dict[str, object]:
    denominator = float(raw.get("drawdown_denominator") or 0.0)
    target_return = target_net_pnl / denominator if denominator > 0 else None
    follower_return = float(follower.get("return_fraction", 0.0))
    if int(follower.get("filled_attempts", 0)) <= 0:
        return {"status": "unavailable", "score": None, "reason": "no_filled_follower_entries"}
    if target_return is None or target_return <= 0:
        return {"status": "unavailable", "score": None, "reason": "non_positive_or_unavailable_target_return"}
    ratio = follower_return / target_return
    return {
        "status": "available", "score": max(0.0, min(1.0, ratio)), "normalized_return_ratio": ratio,
        "target_return_on_capital": target_return, "follower_return_on_capital": follower_return,
        "coverage_state": coverage.get("coverage_state", "UNPROVEN"),
        "basis": "follower return on initial follower capital divided by target net P&L over documented capital denominator",
    }


def _target_summary(metrics: Any, campaigns: Iterable[PositionCampaign], events: Iterable[PositionEvent]) -> dict[str, object]:
    campaigns = list(campaigns)
    events = list(events)
    closed = [campaign for campaign in campaigns if campaign.closed_at and campaign.history_complete]
    daily: dict[str, float] = {}
    for campaign in closed:
        day = as_utc(campaign.closed_at).date().isoformat()
        daily[day] = daily.get(day, 0.0) + campaign.realized_pnl - campaign.target_fees
    values = list(daily.values())
    symbols = {campaign.symbol for campaign in campaigns}
    notionals = {symbol: sum(c.entry_notional for c in campaigns if c.symbol == symbol) for symbol in symbols}
    total_notional = sum(notionals.values())
    opens = [event for event in events if event.event_type.value in {"OPEN", "FLIP"}]
    usable = [event for event in opens if event.target_equity is not None and event.equity_source != "missing"]
    bucket_counts: dict[str, int] = {}
    for event in opens:
        bucket = event.equity_source if event.target_equity is None else "usable"
        bucket_counts[bucket] = bucket_counts.get(bucket, 0) + 1
    return {
        "activity": {
            "fills": len({fill_id for event in events for fill_id in event.raw_fill_ids}), "campaigns": len(campaigns),
            "completed_campaigns": len(closed), "active_days": len({as_utc(event.event_timestamp).date().isoformat() for event in events}),
            "trades_per_day": metrics.raw.get("trade_frequency_per_day", 0.0),
            "median_holding_seconds": metrics.median_holding_seconds, "mean_holding_seconds": metrics.mean_holding_seconds,
            "recent_activity_days": metrics.activity_recency_days,
        },
        "profitability": {
            "gross_pnl": metrics.realized_pnl, "net_pnl": metrics.net_pnl, "fees": metrics.raw.get("target_fees", 0.0),
            "expectancy": metrics.expectancy, "median_campaign_pnl": metrics.median_winner - metrics.median_loser,
            "average_win": metrics.average_winner, "average_loss": metrics.average_loser,
            "win_rate": metrics.win_rate, "profit_factor": metrics.profit_factor,
        },
        "risk": {
            "max_drawdown": metrics.max_drawdown, "average_drawdown": fmean(metrics.raw.get("drawdown_curve", []) or [0.0]),
            "worst_campaign": metrics.worst_campaign, "tail_loss_percentile": metrics.fifth_percentile,
            "largest_loss_relative_to_equity": abs(min(metrics.worst_campaign, 0.0)) / max(float(metrics.raw.get("drawdown_denominator") or 1.0), 1e-12),
            "liquidation_frequency": metrics.raw.get("liquidation_frequency", 0.0), "loss_streak": metrics.longest_losing_streak,
            "adverse_averaging": metrics.adverse_averaging_indicator,
        },
        "stability": {
            "profitable_day_fraction": sum(value > 0 for value in values) / len(values) if values else 0.0,
            "daily_pnl": dict(sorted(daily.items())), "daily_pnl_variance": _variance(values),
            "performance_concentration": metrics.pnl_concentration_best,
            "recent_vs_historical_pnl": _recent_vs_historical(values),
        },
        "concentration": {
            "symbol_count": len(symbols), "largest_symbol_exposure_fraction": max(notionals.values(), default=0.0) / max(total_notional, 1e-12),
            "top_campaign_pnl_fraction": metrics.pnl_concentration_best, "top_five_campaign_pnl_fraction": metrics.pnl_concentration_best_five,
            "long_campaign_count": metrics.raw.get("long_campaign_count", 0), "short_campaign_count": metrics.raw.get("short_campaign_count", 0),
        },
        "sizing": {
            "opening_observations": len(opens), "usable_equity_observations": len(usable),
            "fallback_or_missing_observations": len(opens) - len(usable), "equity_quality_counts": bucket_counts,
            "martingale_indicator": metrics.martingale_indicator, "adverse_add_indicator": metrics.adverse_averaging_indicator,
        },
    }


def _json_object(value: object) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _json_list(value: object) -> list[str]:
    if not isinstance(value, str):
        return []
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return []
    return [str(item) for item in parsed] if isinstance(parsed, list) else []


def _variance(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    mean = fmean(values)
    return sum((value - mean) ** 2 for value in values) / len(values)


def _recent_vs_historical(values: list[float]) -> float | None:
    if len(values) < 2:
        return None
    split = max(1, len(values) // 2)
    historical = fmean(values[:split])
    recent = fmean(values[split:])
    return recent - historical


def _is_wallet(value: str) -> bool:
    return value.startswith("0x") and len(value) == 42 and all(character in "0123456789abcdef" for character in value[2:])
