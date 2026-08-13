from __future__ import annotations

import hashlib
import inspect
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import replace
from datetime import timedelta
from statistics import fmean
from typing import Any, Callable, Iterable

from .analytics import calculate_trader_metrics, campaign_return_series
from .backtest import CopyTradeBacktester
from .config import CopyTradeConfig
from .hyperliquid import HyperliquidPublicAdapter
from .market import CachedHistoricalMarketData, HyperliquidMarketData, MarketDataProvider
from .models import AnalysisRun, CandidateAnalysis, CandidateAnalysisState, CandidateScore, PositionCampaign, PositionEvent, as_utc, new_run_id, utc_now
from .scoring import FollowerMetrics, score_candidate, select_diverse_targets_with_metadata, suitability_confidence
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
        backfill_wallet: Callable[..., dict[str, object]] | None = None,
        reconstruct_wallet: Callable[[str], dict[str, object]] | None = None,
        market_data_factory: Callable[[], MarketDataProvider] | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.service = service
        self.config = service.config
        self.database = service.database
        self._backfill_wallet = backfill_wallet or self._default_backfill
        self._reconstruct_wallet = reconstruct_wallet or service.reconstruct
        self._market_data_factory = market_data_factory or (
            lambda: HyperliquidMarketData(HyperliquidPublicAdapter(self.config.source))
        )
        self._sleep = sleep

    def run(
        self, *, limit: int = 500, status: str | None = "new", resume: bool = False,
        force: bool = False, workers: int | None = None, cheap_only: bool = False,
    ) -> dict[str, object]:
        existing_run = self.database.latest_resumable_analysis_run() if resume else None
        if existing_run:
            run = existing_run
            configuration = _json_object(run.get("configuration_json"))
            self._validate_resumed_configuration(configuration)
            invocation = _json_object(configuration.get("invocation"))
            if not invocation:
                raise ValueError("Cannot resume a pre-Phase-B.1 run without an immutable invocation manifest.")
            candidate_wallets = [str(wallet).lower() for wallet in configuration.get("candidate_wallets", [])]
            if not candidate_wallets:
                raise ValueError("Cannot resume analysis run without its immutable candidate manifest.")
            candidate_rows = self.database.list_analysis_candidates(wallets=candidate_wallets, limit=len(candidate_wallets))
            rows_by_wallet = {str(row["wallet"]).lower(): row for row in candidate_rows}
            candidates = [rows_by_wallet[wallet] for wallet in candidate_wallets if wallet in rows_by_wallet]
            worker_count = int(invocation["workers"])
            force = bool(invocation["force"])
            cheap_only = bool(invocation["cheap_only"])
            required_start = as_utc(configuration["analysis_window"]["required_start"])
            required_end = as_utc(configuration["analysis_window"]["required_end"])
        else:
            if limit <= 0:
                raise ValueError("--limit must be positive.")
            worker_count = workers or self.config.analysis.default_workers
            if worker_count <= 0:
                raise ValueError("--workers must be positive.")
            candidates = self.database.list_analysis_candidates(status=status, limit=limit)
            if status is None:
                candidates = [row for row in candidates if row.get("current_status") not in _OPERATOR_CONTROLLED_TARGET_STATES]
            required_end = utc_now()
            required_start = required_end - timedelta(days=self.config.analysis.history_days)
            configuration = {
                "invocation": {"limit": limit, "status": status, "force": force, "workers": worker_count, "cheap_only": cheap_only},
                "analysis_window": {"required_start": required_start.isoformat(), "required_end": required_end.isoformat()},
                "history_days": self.config.analysis.history_days,
                "min_discovery_activity": self.config.analysis.min_discovery_activity,
                "copytrade_config": self.config.snapshot(), "config_fingerprint": _config_fingerprint(self.config.snapshot()),
                "candidate_wallets": [str(row["wallet"]).lower() for row in candidates],
            }
            run, _ = self._start_or_resume(resume=False, configuration=configuration)
        errors: list[str] = list(_json_list(run.get("errors_json")))
        pending: list[dict[str, Any]] = []
        ready_for_analysis: list[str] = []
        try:
            for candidate in candidates:
                wallet = str(candidate["wallet"]).lower()
                existing = self.database.get_candidate_analysis(wallet)
                run_wallet = self.database.get_analysis_wallet(run["run_id"], wallet)
                if not force and existing and existing.completed_at and existing.lifecycle_status in _FINAL_STATES:
                    # A resume of this very run must not rewrite its successful
                    # terminal state as "deferred".  A new run records why it
                    # deliberately skipped a result from a prior run.
                    if existing.last_run_id != run["run_id"]:
                        self.database.record_analysis_wallet(run["run_id"], wallet, stage="resume", status="skipped", payload={
                            "reason": "already_complete", "lifecycle_status": existing.lifecycle_status,
                        })
                    continue
                if run_wallet and run_wallet["stage"] == "backfill" and run_wallet["status"] == "completed":
                    ready_for_analysis.append(wallet)
                    continue
                if run_wallet and run_wallet["stage"] == "analysis" and run_wallet["status"] == "failed":
                    ready_for_analysis.append(wallet)
                    continue
                if run_wallet and run_wallet["stage"] == "analysis" and run_wallet["status"] == "completed":
                    continue
                if run_wallet and run_wallet["stage"] == "backfill" and run_wallet["status"] == "quarantined":
                    continue
                if run_wallet and run_wallet["stage"] == "prefilter" and run_wallet["status"] == "rejected":
                    continue
                reasons = self._cheap_prefilter(candidate, required_start, required_end)
                if reasons:
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
                for candidate in pending:
                    self.database.record_analysis_wallet(
                        run["run_id"], str(candidate["wallet"]), stage="backfill", status="deferred",
                        payload={"reason": "cheap_only"},
                    )
                return self._finish(run["run_id"], errors, status="completed")

            for candidate in pending:
                self.database.record_analysis_wallet(run["run_id"], str(candidate["wallet"]), stage="backfill", status="started")
            outcomes = self._backfill_all(pending, required_start, required_end, worker_count)
            for wallet, attempts, result, error in outcomes:
                if error:
                    errors.append(f"{wallet}: {error}")
                    self._save_candidate(
                        wallet, CandidateAnalysisState.BACKFILL_FAILED.value, run["run_id"], errors=(error,), completed=False,
                    )
                    self.database.record_analysis_wallet(
                        run["run_id"], wallet, stage="backfill", status="failed", attempts=attempts, error=error,
                    )
                    continue
                coverage = self.database.analysis_window_coverage(wallet, required_start, required_end)
                coverage_state = str(coverage.get("coverage_state", "UNPROVEN"))
                if coverage_state == "KNOWN_INCOMPLETE":
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
                self._complete_analysis_wallet(wallet, run["run_id"], coverage, configuration, required_start, required_end, errors)
            for wallet in ready_for_analysis:
                coverage = self.database.analysis_window_coverage(wallet, required_start, required_end)
                if coverage.get("coverage_state") == "KNOWN_INCOMPLETE":
                    self._save_candidate(wallet, CandidateAnalysisState.QUARANTINED.value, run["run_id"], reasons=("known_incomplete",), summary={"coverage": coverage}, completed=True)
                    self.database.record_analysis_wallet(run["run_id"], wallet, stage="backfill", status="quarantined", payload={"reason": "known_incomplete", "coverage": coverage})
                else:
                    self._complete_analysis_wallet(wallet, run["run_id"], coverage, configuration, required_start, required_end, errors)
        except Exception as exc:
            errors.append(f"run failure: {exc}")
            return self._finish(run["run_id"], errors, status="failed")
        return self._finish(run["run_id"], errors, status="completed_with_errors" if errors else "completed")

    def status(self, *, limit: int = 1000) -> dict[str, object]:
        rows = self.database.list_analysis_candidates(limit=limit)
        state_counts = self.database.count_analysis_candidates_by_state()
        fingerprint = _config_fingerprint(self.config.snapshot())
        finalists = self.shadow_finalists()
        for row in rows:
            candidate_fingerprint = row.get("candidate_config_fingerprint")
            row["stale_for_current_config"] = bool(candidate_fingerprint and candidate_fingerprint != fingerprint)
        return {
            "runs": self.database.list_analysis_runs(), "state_counts": state_counts,
            "total_candidates": sum(state_counts.values()), "candidates": rows,
            "current_config_fingerprint": fingerprint,
            "stale_qualified_candidates": self.database.count_stale_qualified_candidates(fingerprint),
            "run_funnels": {
                str(run["run_id"]): self.database.analysis_funnel(
                    str(run["run_id"]), high_suitability_score=self.config.analysis.high_suitability_score,
                    config_fingerprint=fingerprint,
                )
                for run in self.database.list_analysis_runs(limit=20)
            },
            "shadow_finalists": finalists,
        }

    def shadow_finalists(self, *, count: int | None = None) -> list[dict[str, object]]:
        target_count = count or self.config.analysis.shadow_finalist_count
        current_fingerprint = _config_fingerprint(self.config.snapshot())
        scores = self.database.phase_b_qualified_scores(config_fingerprint=current_fingerprint)
        candidates = {row["wallet"]: row for row in self.database.list_analysis_candidates(limit=10_000)}
        analysis_by_wallet = {
            score.target_wallet: _json_object(candidates.get(score.target_wallet, {}).get("analysis_summary", {}))
            for score in scores
        }
        # Never derive selection inputs from mutable all-time tables.  The
        # summary is the bounded evidence population which generated this score.
        series = {
            score.target_wallet: _json_object(analysis_by_wallet[score.target_wallet].get("diversification_input", {})).get("daily_return_series", {})
            for score in scores
        }
        exposures = {
            score.target_wallet: {
                "symbols": _json_object(analysis_by_wallet[score.target_wallet].get("diversification_input", {})).get("symbols", []),
                "directions": _json_object(analysis_by_wallet[score.target_wallet].get("diversification_input", {})).get("directions", []),
            }
            for score in scores
        }
        assessments: dict[str, tuple[bool, tuple[str, ...]]] = {
            score.target_wallet: self._finalist_policy(score, analysis_by_wallet[score.target_wallet], current_fingerprint)
            for score in scores
        }
        eligible_scores = [score for score in scores if assessments[score.target_wallet][0]]
        selected = select_diverse_targets_with_metadata(eligible_scores, series, target_count=target_count, exposures=exposures)
        selected_by_wallet = {
            score.target_wallet: (rank, diversification)
            for rank, (score, diversification) in enumerate(selected, 1)
        }
        self.database.upsert_finalist_recommendations(
            current_fingerprint,
            (
                {
                    "analysis_run_id": score.analysis_run_id,
                    "wallet": score.target_wallet,
                    "finalist_eligible": assessments[score.target_wallet][0],
                    "finalist_rejection_reasons": assessments[score.target_wallet][1],
                    "diversification_penalty": selected_by_wallet.get(score.target_wallet, (None, {}))[1].get("penalty"),
                    "final_selection_score": (
                        score.total_score - float(selected_by_wallet[score.target_wallet][1]["penalty"])
                        if score.target_wallet in selected_by_wallet else None
                    ),
                    "selection_rank": selected_by_wallet.get(score.target_wallet, (None, {}))[0],
                }
                for score in scores if score.analysis_run_id
            ),
        )
        finalists = []
        for rank, (score, diversification) in enumerate(selected, 1):
            candidate = candidates.get(score.target_wallet, {})
            analysis = candidate.get("analysis_summary", {}) if isinstance(candidate, dict) else {}
            target = analysis.get("target_metrics", {}) if isinstance(analysis, dict) else {}
            follower = analysis.get("follower", {}) if isinstance(analysis, dict) else {}
            finalists.append({
                "rank": rank, "wallet": score.target_wallet, "score": score.total_score,
                "base_suitability_score": score.total_score, "confidence_score": score.confidence_score,
                "finalist_eligible": True, "finalist_rejection_reasons": [],
                "diversification_penalty": diversification["penalty"],
                "final_selection_score": score.total_score - float(diversification["penalty"]),
                "target": target, "follower": follower, "copyability": analysis.get("copyability", {}),
                "data_quality": analysis.get("coverage", {}), "principal_risks": score.reasons,
                "hard_gates": score.hard_gates, "pathology": analysis.get("pathology", {}),
                "regime": analysis.get("regime", {}), "stress_tests": analysis.get("stress_tests", {}),
                "diversification": diversification,
                "current_config_fingerprint": current_fingerprint,
                "candidate_config_fingerprint": score.config_fingerprint,
                "stale_for_current_config": score.config_fingerprint != current_fingerprint,
                "selection_reason": "current qualified Phase B score with return-correlation and exposure-overlap diversification penalties",
            })
        return finalists

    def _finalist_policy(
        self, score: object, analysis: dict[str, object], current_fingerprint: str,
    ) -> tuple[bool, tuple[str, ...]]:
        """Apply configurable evidence policy without mutating Phase-B score evidence."""
        reasons: list[str] = []
        requirements = self.config.finalist_requirements
        if getattr(score, "provenance", None) != "phase_b" or not getattr(score, "analysis_run_id", None):
            reasons.append("not_current_phase_b_finalist_candidate")
        if not getattr(score, "eligible"):
            reasons.append("base_suitability_ineligible")
        if getattr(score, "config_fingerprint") != current_fingerprint:
            reasons.append("stale_config_fingerprint")
        if float(getattr(score, "confidence_score")) < requirements.minimum_confidence_score:
            reasons.append("confidence_below_minimum")
        copyability = _json_object(analysis.get("copyability", {}))
        if requirements.require_copyability_evidence and copyability.get("status") != "available":
            reasons.append("copyability_evidence_required")
        walk_forward = _json_object(analysis.get("walk_forward_evaluation", {}))
        if requirements.require_walk_forward_evidence and walk_forward.get("status") != "available":
            reasons.append("walk_forward_evidence_required")
        latency = _json_object(analysis.get("latency", {}))
        if requirements.require_latency_evidence and latency.get("status") != "available":
            reasons.append("latency_evidence_required")
        regime = _json_object(analysis.get("regime", {}))
        if requirements.require_regime_evidence and regime.get("status") != "available":
            reasons.append("regime_evidence_required")
        return not reasons, tuple(sorted(set(reasons)))

    def suitability_report(self, wallet: str) -> dict[str, object]:
        """Deterministic operator report from the immutable completed analysis summary."""
        candidate = next(iter(self.database.list_analysis_candidates(wallets=[wallet], limit=1)), None)
        if not candidate:
            raise KeyError(f"Candidate not found: {wallet}")
        summary = _json_object(candidate.get("analysis_summary", {}))
        current_fingerprint = _config_fingerprint(self.config.snapshot())
        recommendation = self.database.get_finalist_recommendation(
            candidate.get("score_analysis_run_id"), current_fingerprint, str(candidate["wallet"]),
        )
        if recommendation is None:
            transient_score = CandidateScore(
                target_wallet=str(candidate["wallet"]), calculated_at=utc_now(),
                total_score=float(candidate.get("total_score") or 0), component_scores={}, penalties={},
                eligible=bool(candidate.get("score_eligible")), provenance=str(candidate.get("score_provenance") or "legacy"),
                analysis_run_id=candidate.get("score_analysis_run_id"), config_fingerprint=candidate.get("candidate_config_fingerprint"),
                confidence_score=float(candidate.get("confidence_score") or 0),
                hard_gates=tuple(candidate.get("score_hard_gates") or ()), score_version=str(candidate.get("score_version") or "phase_b_suitability_v3"),
            )
            finalist_eligible, policy_reasons = self._finalist_policy(transient_score, summary, current_fingerprint)
            finalist_reasons = list(policy_reasons)
        else:
            finalist_reasons = recommendation["finalist_rejection_reasons"]
            finalist_eligible = bool(recommendation["finalist_eligible"])
        score = {
            "suitability_score": candidate.get("total_score"), "confidence_score": candidate.get("confidence_score"),
            "base_suitability_score": candidate.get("total_score"),
            "eligibility": bool(candidate.get("score_eligible")), "hard_gates": candidate.get("score_hard_gates", []),
            "principal_risks": candidate.get("score_reasons", []), "score_version": candidate.get("score_version"),
            "analysis_run_id": candidate.get("score_analysis_run_id"),
            "config_fingerprint": candidate.get("candidate_config_fingerprint"),
            "finalist_eligible": finalist_eligible, "finalist_rejection_reasons": finalist_reasons,
            "diversification_penalty": recommendation.get("diversification_penalty") if recommendation else None,
            "final_selection_score": recommendation.get("final_selection_score") if recommendation else None,
        }
        return {
            "wallet": str(candidate["wallet"]), "operator_status": candidate.get("current_status"),
            "lifecycle_status": candidate.get("lifecycle_status"), "score": score,
            "target_performance": summary.get("target_metrics", {}), "follower_performance": summary.get("follower", {}),
            "copyability": summary.get("copyability", {}), "data_quality": summary.get("coverage", {}),
            "walk_forward": summary.get("walk_forward_evaluation", {}), "regime_robustness": summary.get("regime", {}),
            "friction_robustness": summary.get("stress_tests", {}), "pathology": summary.get("pathology", {}),
            "analysis_window": summary.get("analysis_window", {}),
            "why_it_might_fail": candidate.get("score_reasons", []),
            "operator_action": "recommendation_only; no target status was changed",
        }

    def _default_backfill(self, wallet: str, start: object, end: object) -> dict[str, object]:
        earliest = self.database.earliest_fill_time(wallet)
        if earliest and as_utc(earliest) <= as_utc(start):
            latest = self.database.latest_fill_time(wallet)
            # Existing history satisfies the requested left boundary.  Fetch
            # only the forward gap.  The immutable end is always forwarded;
            # fill IDs make the one-millisecond overlap harmless at the gap.
            if latest and as_utc(latest) < as_utc(end):
                return self.service.backfill_for_analysis(
                    wallet, start=max(as_utc(start), as_utc(latest) - timedelta(milliseconds=1)), end=end,
                )
            return {"skipped_existing_history": True, "earliest_fill": as_utc(earliest).isoformat()}
        return self.service.backfill_for_analysis(wallet, start=start, end=end)

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

    def _validate_resumed_configuration(self, configuration: dict[str, Any]) -> None:
        stored_fingerprint = str(configuration.get("config_fingerprint") or "")
        if not stored_fingerprint:
            raise ValueError("Cannot resume analysis run without a Phase B.1 configuration fingerprint.")
        current_fingerprint = _config_fingerprint(self.config.snapshot())
        if stored_fingerprint != current_fingerprint:
            raise ValueError(
                "Refusing to resume with a changed copy-trading configuration; restore the original configuration or start a new run."
            )

    def _backfill_all(
        self, candidates: Iterable[dict[str, Any]], start: object, end: object, workers: int,
    ) -> list[tuple[str, int, dict[str, object], str | None]]:
        items = [str(candidate["wallet"]).lower() for candidate in candidates]
        if not items:
            return []
        outcomes: list[tuple[str, int, dict[str, object], str | None]] = []
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="copy-analysis") as pool:
            futures = {pool.submit(self._retry_backfill, wallet, start, end): wallet for wallet in items}
            for future in as_completed(futures):
                outcomes.append(future.result())
        return sorted(outcomes, key=lambda item: item[0])

    def _retry_backfill(self, wallet: str, start: object, end: object) -> tuple[str, int, dict[str, object], str | None]:
        attempts = self.config.analysis.retry_attempts
        for attempt in range(1, attempts + 1):
            try:
                return wallet, attempt, self._invoke_backfill(wallet, start, end), None
            except Exception as exc:
                coverage = self.database.analysis_window_coverage(wallet, start, end)
                if str(coverage.get("coverage_state")) == "KNOWN_INCOMPLETE":
                    return wallet, attempt, {"coverage": coverage}, None
                if attempt == attempts:
                    return wallet, attempt, {}, str(exc)
                self._sleep(self.config.analysis.retry_initial_seconds * (2 ** (attempt - 1)))
        raise AssertionError("retry loop must return")  # pragma: no cover

    def _invoke_backfill(self, wallet: str, start: object, end: object) -> dict[str, object]:
        """Pass immutable bounds to modern callbacks while retaining test/plugin compatibility."""
        try:
            inspect.signature(self._backfill_wallet).bind(wallet, start, end)
        except TypeError:
            return self._backfill_wallet(wallet, start)
        return self._backfill_wallet(wallet, start, end)

    def _cheap_prefilter(self, candidate: dict[str, Any], required_start: object, required_end: object) -> tuple[str, ...]:
        reasons: list[str] = []
        wallet = str(candidate.get("wallet") or "").lower()
        if not _is_wallet(wallet):
            reasons.append("invalid_wallet")
        status = str(candidate.get("current_status") or "")
        if self.config.prefilter.exclude_operator_managed and status in _OPERATOR_CONTROLLED_TARGET_STATES:
            reasons.append("operator_managed_status")
        activity = candidate.get("recent_activity_at")
        if not activity or (as_utc(required_end) - as_utc(activity)).total_seconds() > self.config.candidates.activity_max_age_days * 86_400:
            reasons.append("inactive")
        metadata = _json_object(candidate.get("metadata_json"))
        cheap_stats = _json_object(metadata.get("cheap_stats"))
        observed = int(cheap_stats.get("distinct_observed_events", metadata.get("latest_activity_observations", 0)) or 0)
        minimum_activity = self.config.prefilter.min_activity_observations or self.config.analysis.min_discovery_activity
        if observed < minimum_activity:
            reasons.append("insufficient_activity")
        if int(cheap_stats.get("distinct_active_days", 0) or 0) < self.config.prefilter.min_distinct_active_days:
            reasons.append("insufficient_temporal_diversity")
        if int(cheap_stats.get("distinct_active_hours", 0) or 0) < self.config.prefilter.min_distinct_active_hours:
            reasons.append("insufficient_temporal_diversity")
        if float(cheap_stats.get("observation_span_hours", 0) or 0) < self.config.prefilter.min_observation_span_hours:
            reasons.append("insufficient_temporal_span")
        if float(cheap_stats.get("approximate_observed_notional", 0) or 0) < self.config.prefilter.min_observed_notional:
            reasons.append("insufficient_observed_notional")
        if int(cheap_stats.get("distinct_symbols", 0) or 0) < self.config.prefilter.min_distinct_symbols:
            reasons.append("insufficient_symbol_diversity")
        coverage = self.database.analysis_window_coverage(wallet, required_start, required_end) if wallet else None
        if coverage and str(coverage.get("coverage_state")) == "KNOWN_INCOMPLETE":
            reasons.append("known_incomplete")
        return tuple(sorted(set(reasons)))

    def _complete_analysis_wallet(
        self, wallet: str, run_id: str, coverage: dict[str, Any], configuration: dict[str, Any],
        required_start: object, required_end: object, errors: list[str],
    ) -> None:
        try:
            analysis = self._analyze_wallet(
                wallet, coverage, run_id=run_id, config_fingerprint=str(configuration["config_fingerprint"]),
                required_start=required_start, required_end=required_end,
            )
        except Exception as exc:  # reconstruction/simulation failure remains per-wallet and resumable
            message = str(exc)
            errors.append(f"{wallet}: analysis failed: {message}")
            self._save_candidate(wallet, CandidateAnalysisState.ANALYSIS_PENDING.value, run_id, errors=(message,), completed=False)
            self.database.record_analysis_wallet(run_id, wallet, stage="analysis", status="failed", error=message)
            return
        lifecycle = CandidateAnalysisState.QUALIFIED.value if analysis["eligible"] else CandidateAnalysisState.ANALYZED.value
        self._save_candidate(
            wallet, lifecycle, run_id, reasons=tuple(analysis["score"]["reasons"]), summary=analysis, completed=True,
        )
        self.database.record_analysis_wallet(
            run_id, wallet, stage="analysis", status="completed", payload={
                "eligible": analysis["eligible"], "score": analysis["score"], "copyability": analysis["copyability"],
                "reconstructed": True, "scored": True,
            },
        )

    def _analyze_wallet(
        self, wallet: str, coverage: dict[str, Any], *, run_id: str, config_fingerprint: str,
        required_start: object, required_end: object,
    ) -> dict[str, object]:
        reconstructed = self._reconstruct_wallet(wallet)
        all_events = tuple(reconstructed["events"])
        all_campaigns = tuple(reconstructed["campaigns"])
        start, end = as_utc(required_start), as_utc(required_end)
        boundary_campaigns = [
            campaign for campaign in all_campaigns
            if as_utc(campaign.opened_at) < start and (campaign.closed_at is None or as_utc(campaign.closed_at) >= start)
        ]
        # Reconstructing all source fills is only used to identify the state at
        # the left boundary.  No campaign whose economic entry predates the
        # immutable window is given a fabricated entry basis.  To prevent a
        # future close from leaking P&L into the run, score only campaigns that
        # both open and close inside the window.
        campaigns = tuple(
            campaign for campaign in all_campaigns
            if start <= as_utc(campaign.opened_at) <= end
            and campaign.closed_at is not None and as_utc(campaign.closed_at) <= end
            and campaign.history_complete
        )
        campaign_ids = {campaign.campaign_id for campaign in campaigns}
        events = tuple(
            event for event in all_events
            if start <= as_utc(event.event_timestamp) <= end and event.campaign_id in campaign_ids
        )
        metrics = calculate_trader_metrics(wallet, campaigns, events, self.config.sizing)
        latest_activity = max(((campaign.closed_at or campaign.opened_at) for campaign in campaigns), default=None)
        metrics = replace(
            metrics,
            activity_recency_days=((end - as_utc(latest_activity)).total_seconds() / 86_400) if latest_activity else None,
        )
        # ``service.reconstruct`` reports the latest physical request.  Phase B
        # eligibility instead uses the immutable full analysis interval.
        metrics.raw["coverage_state"] = coverage.get("coverage_state", "UNPROVEN")
        metrics.raw["coverage_complete"] = coverage.get("coverage_state") == "PROVEN_COMPLETE"
        metrics.raw["coverage_quality"] = coverage.get("coverage_quality", "analysis_window")
        metrics.raw["analysis_window_coverage"] = coverage
        self.database.upsert_metrics(metrics)
        market_evidence = self._historical_market_evidence(run_id, events)
        backtester = CopyTradeBacktester(self.config, market_data=market_evidence)
        baseline = backtester.run(events=events, coverage_metadata=coverage)
        slippage = backtester.slippage_scenarios(events=events)
        latency = backtester.latency_decay_curve(events=events)
        walk_forward = (
            backtester.walk_forward(events=events)
            if metrics.closed_campaign_count >= self.config.candidates.closed_campaigns_min else []
        )
        walk_forward_evaluation = _walk_forward_evidence(walk_forward, self.config.analysis.walk_forward_min_windows)
        follower = _follower_summary(baseline.summary, slippage, latency)
        copyability = _copyability(metrics.net_pnl, metrics.raw, baseline.summary, coverage)
        friction = _friction_evidence(slippage)
        latency_evidence = _latency_evidence(latency)
        regime = _regime_evidence(campaigns, self.config)
        pathology = _pathology_evidence(metrics, campaigns, self.config)
        metrics.raw["pathology"] = pathology
        metrics.raw["regime"] = regime
        metrics.raw["source_quality"] = 1.0
        confidence = suitability_confidence(
            metrics, self.config.confidence, coverage_state=str(coverage.get("coverage_state", "UNPROVEN")),
            walk_forward_windows=int(walk_forward_evaluation["window_count"]),
            represented_regimes=int(regime.get("represented_dimensions", 0)),
        )
        self.database.upsert_metrics(metrics)
        follower_metrics = FollowerMetrics(
            net_pnl=float(baseline.summary["net_pnl"]),
            expectancy=float(follower["expectancy"]),
            profit_factor=float(follower["profit_factor"] or 0.0),
            max_drawdown=float(baseline.summary["max_drawdown_fraction"]),
            missed_trade_rate=float(follower["missed_trade_rate"]),
            latency_curve=tuple(latency), latency_status="available" if latency else "unavailable",
            return_fraction=float(follower["return_fraction"]),
            copyability_score=copyability["score"] if copyability["status"] == "available" else None,
            slippage_robustness=friction.get("retention_score"), friction_robustness=friction.get("score"),
            walk_forward_score=walk_forward_evaluation["score"],
            walk_forward_status=str(walk_forward_evaluation["status"]),
            walk_forward_window_count=int(walk_forward_evaluation["window_count"]),
            regime_robustness=regime.get("combined_score") if regime.get("status") == "available" else None,
        )
        score = replace(
            score_candidate(metrics, self.config.candidates, follower_metrics, confidence_score=float(confidence["score"])), provenance="phase_b",
            analysis_run_id=run_id, config_fingerprint=config_fingerprint,
        )
        self.database.upsert_candidate_score(score)
        diversification_input = {
            "daily_return_series": campaign_return_series(campaigns),
            "symbols": sorted({campaign.symbol for campaign in campaigns}),
            "directions": sorted({campaign.direction for campaign in campaigns}),
            "campaign_ids": sorted(campaign_ids),
        }
        boundary_metadata = {
            "required_start": start.isoformat(), "required_end": end.isoformat(),
            "boundary_policy": "reconstruct_prior_state_then_exclude_campaigns_opened_before_required_start; score_only_campaigns_opened_and_closed_within_window",
            "campaigns_excluded_at_left_boundary": len(boundary_campaigns),
            "events_excluded_before_window": sum(as_utc(event.event_timestamp) < start for event in all_events),
            "events_excluded_after_window": sum(as_utc(event.event_timestamp) > end for event in all_events),
            "events_excluded_boundary_campaigns": sum(
                event.campaign_id in {campaign.campaign_id for campaign in boundary_campaigns}
                for event in all_events if start <= as_utc(event.event_timestamp) <= end
            ),
            "campaigns_analyzed": len(campaigns),
            "analyzed_first_event": events[0].event_timestamp.isoformat() if events else None,
            "analyzed_last_event": events[-1].event_timestamp.isoformat() if events else None,
        }
        return {
            "target_metrics": _target_summary(metrics, campaigns, events),
            "follower": follower,
            "copyability": copyability,
            "coverage": coverage,
            "analysis_window": boundary_metadata,
            **boundary_metadata,
            "diversification_input": diversification_input,
            "slippage_scenarios": slippage,
            "stress_tests": {"slippage": friction, "latency": latency_evidence},
            "latency": {"status": "available" if latency else "unavailable", "curve": latency, **latency_evidence},
            "market_evidence": {
                "status": "available" if market_evidence and any(item.get("price") is not None for item in market_evidence.evidence_metadata()) else "unavailable",
                "resolution": f"{self.config.analysis.market_evidence_bucket_seconds}s" if market_evidence else None,
                "observations": market_evidence.evidence_metadata() if market_evidence else [],
                "quality_note": "Public candle-close proxy only; this is not historical L2/order-book execution evidence.",
            },
            "walk_forward": walk_forward,
            "walk_forward_evaluation": walk_forward_evaluation,
            "regime": regime,
            "pathology": pathology,
            "confidence": confidence,
            "score": {
                "total": score.total_score, "eligible": score.eligible, "components": score.component_scores,
                "penalties": score.penalties, "reasons": list(score.reasons), "hard_gates": list(score.hard_gates),
                "confidence": score.confidence_score, "score_version": score.score_version, "source_quality": score.source_quality,
            },
            "eligible": score.eligible,
        }

    def _historical_market_evidence(
        self, run_id: str, events: Iterable[PositionEvent],
    ) -> CachedHistoricalMarketData | None:
        """Prime bounded, immutable candle evidence before any replay scenarios run."""
        event_list = tuple(events)
        if not self.config.analysis.market_evidence_enabled or not event_list:
            return None
        evidence = CachedHistoricalMarketData(
            self._market_data_factory(), bucket_seconds=self.config.analysis.market_evidence_bucket_seconds,
            load=lambda symbol, bucket: self.database.get_analysis_market_evidence(run_id, symbol, bucket),
            store=lambda item: self.database.insert_analysis_market_evidence(run_id, item),
        )
        delays = set(self.config.backtest.detection_delays_ms)
        delays.add(self.config.paper_execution.detection_latency_ms)
        requests = [
            (
                event.symbol,
                event.event_timestamp + timedelta(milliseconds=delay + self.config.paper_execution.order_latency_ms),
            )
            for event in event_list for delay in delays
        ]
        evidence.prime(requests)
        return evidence

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

    def _finish(self, run_id: str, errors: list[str], *, status: str) -> dict[str, object]:
        counters = self.database.analysis_run_counters(run_id)
        self.database.finish_analysis_run(run_id, status=status, errors=tuple(sorted(errors)), **counters)
        finalists = self.shadow_finalists()
        return {
            "run_id": run_id, "status": status, **counters, "errors": sorted(errors),
            "funnel": self.database.analysis_funnel(
                run_id, high_suitability_score=self.config.analysis.high_suitability_score,
                config_fingerprint=_config_fingerprint(self.config.snapshot()),
            ),
            "diversification_selected": len(finalists), "shadow_finalists": finalists,
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


def _friction_evidence(scenarios: Iterable[dict[str, object]]) -> dict[str, object]:
    """Summarize deterministic slippage scenarios without inferring liquidity."""
    values = sorted((dict(item) for item in scenarios), key=lambda item: float(item.get("slippage_bps", 0)))
    if not values:
        return {"status": "unavailable", "score": None, "retention_score": None, "break_even_slippage_bps": None}
    baseline = next((item for item in values if float(item.get("slippage_bps", 0)) == 0), values[0])
    baseline_return = float(baseline.get("return_fraction", 0.0))
    returns = [float(item.get("return_fraction", 0.0)) for item in values]
    retained = max(0.0, min(1.0, min(returns) / baseline_return)) if baseline_return > 1e-12 else 0.0
    positive_fraction = sum(value > 0 for value in returns) / len(returns)
    break_even = next((float(item.get("slippage_bps", 0)) for item in values if float(item.get("return_fraction", 0)) <= 0), None)
    return {
        "status": "available", "baseline_return_fraction": baseline_return,
        "worst_return_fraction": min(returns), "retention_score": retained,
        "positive_scenario_fraction": positive_fraction, "score": (retained + positive_fraction) / 2,
        "break_even_slippage_bps": break_even, "scenarios": values,
    }


def _latency_evidence(curve: Iterable[dict[str, object]]) -> dict[str, object]:
    values = sorted((dict(item) for item in curve), key=lambda item: float(item.get("latency_ms", 0)))
    if not values:
        return {
            "status": "unavailable", "reason": "missing_or_insufficient_historical_price_evidence",
            "retention_score": None, "break_even_latency_ms": None,
        }
    baseline = float(values[0].get("return_fraction", 0.0))
    returns = [float(item.get("return_fraction", 0.0)) for item in values]
    retention = max(0.0, min(1.0, returns[-1] / baseline)) if baseline > 1e-12 else 0.0
    break_even = next((float(item.get("latency_ms", 0)) for item in values if float(item.get("return_fraction", 0)) <= 0), None)
    return {"status": "available", "baseline_return_fraction": baseline, "worst_return_fraction": min(returns),
            "retention_score": retention, "break_even_latency_ms": break_even, "curve": values}


def _regime_evidence(campaigns: Iterable[PositionCampaign], config: CopyTradeConfig) -> dict[str, object]:
    """Independent directional and volatility evidence using a transparent price proxy."""
    method = "campaign entry-to-exit observed-price proxy"
    if not config.regimes.enabled:
        disabled = {"status": "disabled", "represented_regime_count": 0, "score": None, "regimes": {}}
        return {
            "status": "disabled", "directional": disabled, "volatility": disabled,
            "combined_score": None, "score": None, "represented_dimensions": 0,
            "represented_regime_count": 0, "method": "disabled",
        }
    directional_groups: dict[str, list[float]] = {}
    volatility_groups: dict[str, list[float]] = {}
    threshold = config.regimes.volatility_move_threshold
    campaign_count = 0
    for campaign in campaigns:
        if not campaign.closed_at or campaign.entry_quantity <= 0 or campaign.average_entry_price <= 0:
            continue
        exit_price = campaign.exit_notional / campaign.entry_quantity if campaign.exit_notional else 0.0
        if exit_price <= 0:
            continue
        campaign_count += 1
        move = exit_price / campaign.average_entry_price - 1
        pnl = campaign.realized_pnl - campaign.target_fees
        directional_groups.setdefault(
            "rising" if move > threshold else ("falling" if move < -threshold else "sideways"), []
        ).append(pnl)
        volatility_groups.setdefault("high_volatility" if abs(move) >= threshold else "low_volatility", []).append(pnl)

    directional = _regime_dimension(directional_groups, config.regimes.minimum_campaigns_per_regime)
    volatility = _regime_dimension(volatility_groups, config.regimes.minimum_campaigns_per_regime)
    available_scores = [
        float(dimension["score"]) for dimension in (directional, volatility)
        if dimension["status"] == "available" and dimension["score"] is not None
    ]
    combined = fmean(available_scores) if available_scores else None
    represented_dimensions = len(available_scores)
    return {
        "status": "available" if available_scores else "insufficient_sample",
        "directional": directional,
        "volatility": volatility,
        "combined_score": combined,
        # ``score`` and ``represented_regime_count`` retain a stable public
        # surface while counting independent dimensions, never overlapping bins.
        "score": combined,
        "represented_dimensions": represented_dimensions,
        "represented_regime_count": represented_dimensions,
        "campaign_count": campaign_count,
        "method": method,
    }


def _regime_dimension(groups: dict[str, list[float]], minimum_campaigns: int) -> dict[str, object]:
    summaries = {
        name: {
            "campaign_count": len(values), "net_pnl": sum(values),
            "profitable_fraction": sum(value > 0 for value in values) / len(values),
        }
        for name, values in sorted(groups.items())
    }
    represented = {
        name: value for name, value in summaries.items()
        if int(value["campaign_count"]) >= minimum_campaigns
    }
    if not represented:
        return {"status": "insufficient_sample", "represented_regime_count": 0, "score": None, "regimes": summaries}
    positive = [max(float(item["net_pnl"]), 0.0) for item in represented.values()]
    total_positive = sum(positive)
    strongest_share = max(positive, default=0.0) / total_positive if total_positive else 1.0
    profitable_fraction = sum(float(item["net_pnl"]) > 0 for item in represented.values()) / len(represented)
    return {
        "status": "available", "represented_regime_count": len(represented),
        "score": max(0.0, min(1.0, (profitable_fraction + (1 - strongest_share)) / 2)),
        "strongest_regime_pnl_share": strongest_share, "regimes": summaries,
    }


def _pathology_evidence(
    metrics: object, campaigns: Iterable[PositionCampaign], config: CopyTradeConfig,
) -> dict[str, object]:
    values = [campaign.realized_pnl - campaign.target_fees for campaign in campaigns if campaign.closed_at]
    winners = [value for value in values if value > 0]
    worst_to_average_win = abs(min(values, default=0.0)) / max(fmean(winners) if winners else 0.0, 1e-12)
    reasons: list[str] = []
    if getattr(metrics, "martingale_indicator"):
        reasons.append("martingale_like")
    if getattr(metrics, "adverse_averaging_indicator"):
        reasons.append("adverse_averaging")
    concentration = float(getattr(metrics, "pnl_concentration_best"))
    if concentration > config.candidates.pnl_concentration_preferred:
        reasons.append("jackpot_concentration")
    liquidation_frequency = float(getattr(metrics, "raw").get("liquidation_frequency", 0.0) or 0.0)
    if liquidation_frequency > 0:
        reasons.append("liquidation_observed")
    if worst_to_average_win >= 3:
        reasons.append("tail_loss_asymmetry")
    if len(getattr(metrics, "by_symbol")) == 1 and values:
        reasons.append("one_symbol_concentration")
    return {"reason_codes": sorted(set(reasons)), "top_campaign_pnl_fraction": concentration,
            "worst_to_average_win_ratio": worst_to_average_win, "liquidation_frequency": liquidation_frequency,
            "campaign_count": len(values)}


def _copyability(target_net_pnl: float, raw: dict[str, Any], follower: dict[str, Any], coverage: dict[str, Any]) -> dict[str, object]:
    denominator = float(raw.get("copyability_capital_denominator") or 0.0)
    denominator_source = raw.get("copyability_capital_source")
    denominator_quality = raw.get("copyability_capital_quality", "unavailable")
    target_return = target_net_pnl / denominator if denominator > 0 else None
    follower_return = float(follower.get("return_fraction", 0.0))
    if int(follower.get("filled_attempts", 0)) <= 0:
        return {"status": "unavailable", "score": None, "reason": "no_filled_follower_entries",
                "denominator": denominator or None, "denominator_source": denominator_source, "denominator_quality": denominator_quality}
    if denominator <= 0 or denominator_quality != "genuine_usable_target_equity":
        return {"status": "unavailable", "score": None, "reason": "target_equity_denominator_unavailable",
                "denominator": None, "denominator_source": denominator_source, "denominator_quality": denominator_quality}
    if target_return is None or target_return <= 0:
        return {"status": "unavailable", "score": None, "reason": "non_positive_or_unavailable_target_return",
                "denominator": denominator, "denominator_source": denominator_source, "denominator_quality": denominator_quality}
    ratio = follower_return / target_return
    return {
        "status": "available", "score": max(0.0, min(1.0, ratio)), "normalized_return_ratio": ratio,
        "target_return_on_capital": target_return, "follower_return_on_capital": follower_return,
        "denominator": denominator, "denominator_source": denominator_source, "denominator_quality": denominator_quality,
        "coverage_state": coverage.get("coverage_state", "UNPROVEN"),
        "basis": "follower return on initial follower capital divided by target net P&L over a genuine usable target-equity observation",
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
            "expectancy": metrics.expectancy,
            "median_campaign_pnl": _median_campaign_pnl(closed),
            "average_win": metrics.average_winner, "average_loss": metrics.average_loser,
            "win_rate": metrics.win_rate, "profit_factor": metrics.profit_factor,
        },
        "risk": {
            "max_drawdown_fraction": metrics.max_drawdown,
            "average_drawdown_dollars": fmean(metrics.raw.get("drawdown_curve", []) or [0.0]),
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


def _config_fingerprint(snapshot: dict[str, Any]) -> str:
    payload = json.dumps(snapshot, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _walk_forward_evidence(windows: Iterable[dict[str, object]], minimum_windows: int) -> dict[str, object]:
    values = [float(window.get("forward_return_fraction", 0.0)) for window in windows]
    if len(values) < minimum_windows:
        return {"status": "unavailable", "score": None, "window_count": len(values), "reason": "insufficient_walk_forward_windows"}
    profitable_fraction = sum(value > 0 for value in values) / len(values)
    mean_return = fmean(values)
    worst_return = min(values)
    # Positive, repeated forward performance is modest evidence.  A string of
    # weak/negative windows reduces this component but never becomes a hard
    # rejection solely because forward history is short.
    score = max(0.0, min(1.0, (profitable_fraction + max(0.0, min(1.0, 0.5 + mean_return / 0.10))) / 2))
    return {
        "status": "available", "score": score, "window_count": len(values),
        "profitable_window_fraction": profitable_fraction, "mean_forward_return": mean_return,
        "worst_forward_return": worst_return,
    }


def _median_campaign_pnl(campaigns: Iterable[PositionCampaign]) -> float:
    values = sorted(campaign.realized_pnl - campaign.target_fees for campaign in campaigns)
    if not values:
        return 0.0
    middle = len(values) // 2
    return values[middle] if len(values) % 2 else (values[middle - 1] + values[middle]) / 2


def _exposure_profile(campaigns: Iterable[PositionCampaign]) -> dict[str, object]:
    items = list(campaigns)
    return {
        "symbols": sorted({campaign.symbol for campaign in items}),
        "directions": sorted({campaign.direction for campaign in items}),
    }
